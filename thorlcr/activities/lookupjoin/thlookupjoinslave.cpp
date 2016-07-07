/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include "thactivityutil.ipp"
#include "thcompressutil.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "jbuff.hpp"
#include "jset.hpp"
#include "jisem.hpp"

#include "thorxmlwrite.hpp"
#include "../hashdistrib/thhashdistribslave.ipp"
#include "thsortu.hpp"

#ifdef _DEBUG
#define _TRACEBROADCAST
#endif


enum join_t { JT_Undefined, JT_Inner, JT_LeftOuter, JT_RightOuter, JT_LeftOnly, JT_RightOnly, JT_LeftOnlyTransform };


#define MAX_SEND_SIZE 0x100000 // 1MB
#define MAX_QUEUE_BLOCKS 10

enum broadcast_code { bcast_none, bcast_send, bcast_sendStopping, bcast_stop };
enum broadcast_flags { bcastflag_null=0, bcastflag_spilt=0x100, bcastflag_stop=0x200 };
#define BROADCAST_CODE_MASK 0x00FF
#define BROADCAST_FLAG_MASK 0xFF00

//#define HIGHFREQ_TIMING
#ifdef HIGHFREQ_TIMING
#define ADDHFTIME(base, inc) base += inc
#define ADDHFTIMERRESET(timer) timer.reset()
#define INCHFSTATE(stat) ++stat;
#else
#define ADDHFTIME(base, inc)
#define ADDHFTIMERRESET(timer)
#define INCHFSTATE(stat)
#endif

template <typename T>
class CCycleAddTimerType
{
    T &time;
    CCycleTimer timer;
public:
    inline CCycleAddTimerType(T &_time) : time(_time) { }
    inline ~CCycleAddTimerType()
    {
        time += timer.elapsedCycles();
    }
};
typedef CCycleAddTimerType<cycle_t> CCycleAddTimer;
typedef CCycleAddTimerType<std::atomic<cycle_t>> CAtomicCycleAddTimer;



class CSendItem : public CSimpleInterface
{
    CMessageBuffer msg;
    unsigned slave, targetChannel, headerLen;
    unsigned short info;
public:
    CSendItem(broadcast_code _code, unsigned _slave, unsigned _targetChannel) : info((unsigned short)_code), slave(_slave), targetChannel(_targetChannel)
    {
        msg.append(info);
        msg.append(slave);
        msg.append(targetChannel);
        headerLen = msg.length();
    }
    CSendItem(CMessageBuffer &_msg)
    {
        msg.swapWith(_msg);
        msg.read(info);
        msg.read(slave);
        msg.read(targetChannel);
        headerLen = msg.getPos();
    }
    unsigned length() const { return msg.length(); }
    void reset() { msg.setLength(headerLen); }
    CMessageBuffer &queryMsg() { return msg; }
    broadcast_code queryCode() const { return (broadcast_code)(info & BROADCAST_CODE_MASK); }
    unsigned querySlave() const { return slave; } // 0 based
    unsigned queryTargetChannel() const { return targetChannel; } // 0 based
    void setTargetChannel(unsigned _targetChannel) { targetChannel = _targetChannel; }
    broadcast_flags queryFlags() const { return (broadcast_flags)(info & BROADCAST_FLAG_MASK); }
    void setFlag(broadcast_flags _flag)
    {
        info = (info & ~BROADCAST_FLAG_MASK) | ((short)_flag);
        msg.writeDirect(0, sizeof(info), &info); // update
    }
};


interface IBCastReceive
{
    virtual void bCastReceive(CSendItem *sendItem, bool stop) = 0;
};

/*
 * CBroadcaster, is a utility class, that sends CSendItem packets to sibling nodes, which in turn resend to others,
 * ensuring the data is broadcast to all other nodes.
 * sender and receiver threads are employed to handle the receipt/resending of packets.
 * CBroadcaster should be started on all receiving nodes, each receiver will receive CSendItem packets
 * through IBCastReceive::bCastReceive calls.
 */
class CBroadcaster : public CSimpleInterface
{
    ICommunicator *comm = nullptr;
    CActivityBase &activity;
    mptag_t mpTag;
    unsigned myNode, nodes, mySlave, slaves, senders, mySender;
    IBCastReceive *recvInterface;
    CriticalSection stopCrit;
    CriticalSection broadcastLock;
    bool allRequestStop, stopping, stopRecv, receiving, nodeBroadcast;
    broadcast_flags stopFlag;
    Owned<IBitSet> sendersDone, broadcastersStopping;
    cycle_t receiveMsgTime = 0;

    class CRecv : implements IThreaded
    {
        CBroadcaster &broadcaster;
        CThreadedPersistent threaded;
        bool aborted;
    public:
        CRecv(CBroadcaster &_broadcaster) : threaded("CBroadcaster::CRecv", this), broadcaster(_broadcaster)
        {
            aborted = false;
        }
        void start()
        {
            aborted = false;
            threaded.start();
        }
        void abort(bool join)
        {
            if (aborted)
                return;
            aborted = true;
            broadcaster.cancelReceive();
            if (join)
                threaded.join();
        }
        void wait()
        {
            threaded.join();
        }
    // IThreaded
        virtual void main()
        {
            try
            {
                broadcaster.recvLoop();
            }
            catch (IException *e)
            {
                EXCLOG(e, "CRecv");
                abort(false);
                broadcaster.cancel(e);
                e->Release();
            }
        }
    } receiver;
    class CSend : implements IThreaded
    {
        CBroadcaster &broadcaster;
        CThreadedPersistent threaded;
        SimpleInterThreadQueueOf<CSendItem, true> broadcastQueue;
        Owned<IException> exception;
        bool aborted;
        void clearQueue()
        {
            loop
            {
                Owned<CSendItem> sendItem = broadcastQueue.dequeueNow();
                if (NULL == sendItem)
                    break;
            }
        }
    public:
        CSend(CBroadcaster &_broadcaster) : threaded("CBroadcaster::CSend", this), broadcaster(_broadcaster)
        {
            aborted = false;
        }
        ~CSend()
        {
            clearQueue();
        }
        void addBlock(CSendItem *sendItem)
        {
            if (exception)
            {
                if (sendItem)
                    sendItem->Release();
                throw exception.getClear();
            }
            broadcastQueue.enqueue(sendItem); // will block if queue full
        }
        void start()
        {
            aborted = false;
            exception.clear();
            threaded.start();
        }
        void abort(bool join)
        {
            if (aborted)
                return;
            aborted = true;
            broadcastQueue.stop();
            clearQueue();
            if (join)
                threaded.join();
        }
        void wait()
        {
            ActPrintLog(&broadcaster.activity, "CSend::wait(), messages to send: %d", broadcastQueue.ordinality());
            addBlock(NULL);
            threaded.join();
        }
    // IThreaded
        virtual void main()
        {
            try
            {
                while (!broadcaster.activity.queryAbortSoon())
                {
                    Owned<CSendItem> sendItem = broadcastQueue.dequeue();
                    if (NULL == sendItem)
                        break;
                    broadcaster.broadcastToOthers(sendItem);
                }
            }
            catch (IException *e)
            {
                EXCLOG(e, "CSend");
                exception.setown(e);
                abort(false);
                broadcaster.cancel(e);
            }
            ActPrintLog(&broadcaster.activity, "Sender stopped");
        }
    } sender;

    // NB: returns true if all done. Sets allDoneExceptSelf if all except this sender are done
    bool senderStop(unsigned sender)
    {
        activity.ActPrintLog("senderStop(sender=%u", sender);
        bool done = sendersDone->testSet(sender, true);
        assertex(false == done);
        unsigned which = sendersDone->scan(0, false);
        if (which != senders)
            return false;
        return true;
    }
    bool senderStop(CSendItem &sendItem)
    {
        unsigned sender = sendItem.querySlave();
        if (nodeBroadcast)
            sender = sender % nodes;
        return senderStop(sender);
    }
    unsigned target(unsigned i, unsigned node)
    {
        // For a tree broadcast, calculate the next node to send the data to. i represents the ith copy sent from this node.
        // node is a 0 based node number.
        // It returns a 0 based node number of the next node to send the data to.
        unsigned n = node;
        unsigned j=0;
        while (n)
        {
            j++;
            n /= 2;
        }
        return ((1<<(i+j))+node);
    }
    void broadcastToOthers(CSendItem *sendItem)
    {
        mptag_t rt = activity.queryMPServer().createReplyTag();
        unsigned origin = sendItem->querySlave() % nodes;
        unsigned pseudoNode = (myNode<origin) ? nodes-origin+myNode : myNode-origin;
        CMessageBuffer replyMsg;
        // sends to all in 1st pass, then waits for ack from all

        unsigned loopCnt = (bcast_stop == sendItem->queryCode()) ? 2 : 1;
        for (unsigned sendRecv=0; sendRecv<loopCnt && !activity.queryAbortSoon(); sendRecv++)
        {
            unsigned i = 0;
            while (!activity.queryAbortSoon())
            {
                unsigned t = target(i++, pseudoNode);
                if (t>=nodes)
                    break;
                t += origin;
                if (t>=nodes)
                    t -= nodes;
                t += 1; // adjust 0 based to 1 based, i.e. excluding master at 0
                unsigned sendLen = sendItem->length();
                unsigned ot=t;
                if (!nodeBroadcast)
                {
                    // map target node+channel to slave #, to propagate to other nodes on same channel
                    t = activity.queryJob().querySlaveForNodeChannel(t-1, sendItem->queryTargetChannel()) + 1;
                }
                if (bcast_stop == sendItem->queryCode())
                    activity.ActPrintLog("broadcastToOthers: sending stop - ot=%u, t=%u, origin node=%u, sendItem->slave=%u", ot, t, origin, sendItem->querySlave());
                if (0 == sendRecv) // send
                {
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Sending to node %d, origin node %d, origin slave %d, size %d, code=%d", myNode+1, t, origin+1, sendItem->querySlave()+1, sendLen, (unsigned)sendItem->queryCode());
#endif
                    CMessageBuffer &msg = sendItem->queryMsg();
                    msg.setReplyTag(rt); // simulate sendRecv

                    /* send to slave on node t, on my channel #
                     * It doesn't matter which channel receives, but this is spreading the load
                     */
                    CriticalBlock b(broadcastLock); // prevent other channels overlapping, otherwise causes queue ordering issues with MP multi packet messages to same dst.
                    comm->send(msg, t, mpTag);
                }
                else // recv reply
                {
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Waiting for reply from node %d, origin node %d, origin slave %d, size %d, code=%d, replyTag=%d", myNode+1, t, origin+1, sendItem->querySlave()+1, sendLen, (unsigned)sendItem->queryCode(), (unsigned)rt);
#endif
                    if (!activity.receiveMsg(*comm, replyMsg, t, rt))
                        break;
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Sent to node %d, origin node %d, origin slave %d, size %d, code=%d - received ack", myNode+1, t, origin+1, sendItem->querySlave()+1, sendLen, (unsigned)sendItem->queryCode());
#endif
                }
            }
        }
    }
    void cancelReceive()
    {
        stopRecv = true;
        if (receiving)
            comm->cancel(RANK_ALL, mpTag);
    }
    bool receiveMsg(CMessageBuffer &mb, rank_t *sender)
    {
        BooleanOnOff onOff(receiving);
        // check 'cancelledReceive' every 10 secs
        while (!stopRecv)
        {
            if (comm->recv(mb, RANK_ALL, mpTag, sender, 10000))
                return true;
        }
        return false;
    }
    void recvLoop()
    {
        receiveMsgTime = 0;
        // my sender is implicitly stopped (never sends to self)
        if (senderStop(mySender))
            return;
        ActPrintLog(&activity, "Start of recvLoop()");
        CMessageBuffer msg;
        while (!stopRecv && !activity.queryAbortSoon())
        {
            rank_t sendRank;
            {
                CCycleAddTimer tb(receiveMsgTime);
                if (!receiveMsg(msg, &sendRank))
                {
                    ActPrintLog(&activity, "recvLoop() - receiveMsg cancelled");
                    break;
                }
            }
            mptag_t replyTag = msg.getReplyTag();
            Owned<CSendItem> sendItem = new CSendItem(msg);
            unsigned origin = sendItem->querySlave() % nodes;
#ifdef _TRACEBROADCAST
            ActPrintLog(&activity, "Broadcast node %d received from node %d, origin node %d, origin slave %d, size %d, code=%d", myNode+1, (unsigned)sendRank, origin+1, sendItem->querySlave()+1, sendItem->length(), (unsigned)sendItem->queryCode());
#endif
#ifdef _TRACEBROADCAST
            ActPrintLog(&activity, "Broadcast node %d, sent ack to node %d, replyTag=%d", myNode+1, (unsigned)sendRank, (unsigned)replyTag);
#endif
            assertex(mySlave != sendItem->querySlave());
            switch (sendItem->queryCode())
            {
                case bcast_stop:
                {
                    bool stopResent = (0 != (sendItem->queryFlags() & bcastflag_stop));
                    if (!stopResent)
                    {
                        CMessageBuffer ackMsg;
                        comm->send(ackMsg, sendRank, replyTag); // send ack
                    }
                    if (0 == (sendItem->queryFlags() & bcastflag_stop))
                        sender.addBlock(sendItem.getLink());
                    bool stop = senderStop(*sendItem);
                    recvInterface->bCastReceive(sendItem.getLink(), stop);
                    if (stop)
                    {
                        ActPrintLog(&activity, "recvLoop, received last senderStop, node=%u, slave=%u", origin+1, sendItem->querySlave()+1);
                        // NB: this slave has nothing more to receive.
                        // However the sender will still be re-broadcasting some packets, including these stop packets
                        return;
                    }
                    break;
                }
                case bcast_sendStopping:
                {
                    setStopping(sendItem->querySlave());
                    // fall through
                }
                case bcast_send:
                {
                    sender.addBlock(sendItem.getLink());
                    if (!allRequestStop) // don't care if all stopping
                        recvInterface->bCastReceive(sendItem.getClear(), false);
                    break;
                }
                default:
                    throwUnexpected();
            }
        }
        ActPrintLog(&activity, "End of recvLoop()");
    }
    inline void _setStopping(unsigned sender)
    {
        broadcastersStopping->set(sender, true);
        // allRequestStop=true, if I'm stopping and all others have requested also
        allRequestStop = broadcastersStopping->scan(0, false) == senders;
    }
public:
    CBroadcaster(CActivityBase &_activity) : activity(_activity), receiver(*this), sender(*this)
    {
        allRequestStop = stopping = stopRecv = false;
        myNode = activity.queryJob().queryMyNode();
        mySlave = activity.queryJobChannel().queryMySlave();
        nodes = activity.queryJob().queryNodes();
        slaves = activity.queryJob().querySlaves();
        mySender = mySlave;
        senders = slaves;
        sendersDone.setown(createThreadSafeBitSet());
        broadcastersStopping.setown(createThreadSafeBitSet());
        mpTag = TAG_NULL;
        recvInterface = NULL;
        stopFlag = bcastflag_null;
        receiving = false;
        nodeBroadcast = false;
    }
    cycle_t getReceiveMsgTime() const { return receiveMsgTime; }
    void start(IBCastReceive *_recvInterface, mptag_t _mpTag, bool _stopping, bool _nodeBroadcast)
    {
        nodeBroadcast = _nodeBroadcast;
        if (nodeBroadcast)
        {
            mySender = myNode;
            senders = nodes;
            comm = &activity.queryJob().queryNodeComm();
        }
        else
        {
            mySender = mySlave;
            senders = slaves;
            comm = &activity.queryJobChannel().queryJobComm();
        }
        stopping = _stopping;
        recvInterface = _recvInterface;
        stopRecv = false;
        mpTag = _mpTag;
        if (recvInterface)
        {
            receiver.start();
            sender.start();
        }
    }
    void reset()
    {
        allRequestStop = stopping = false;
        stopFlag = bcastflag_null;
        sendersDone->reset();
        broadcastersStopping->reset();
    }
    CSendItem *newSendItem(broadcast_code code)
    {
        if (stopping && (bcast_send==code))
            code = bcast_sendStopping;
        return new CSendItem(code, mySlave, activity.queryJobChannelNumber());
    }
    void end()
    {
        receiver.wait(); // terminates when received stop from all others
        sender.wait(); // terminates when any remaining packets, including final stop packets have been re-broadcast
    }
    void cancel(IException *e=NULL)
    {
        receiver.abort(true);
        sender.abort(true);
        if (e)
            activity.fireException(e);
    }
    bool send(CSendItem *sendItem)
    {
        if (bcast_stop == sendItem->queryCode())
            sendLocalStop(sendItem);
        broadcastToOthers(sendItem);
        return !allRequestStop;
    }
    broadcast_flags queryStopFlag() { return stopFlag; }
    bool stopRequested()
    {
        if (bcastflag_null != queryStopFlag()) // if this node has requested to stop immediately
            return true;
        return allRequestStop; // if not, if all have request to stop
    }
    void setStopping(unsigned sender)
    {
        if (nodeBroadcast)
            sender = sender % nodes;
        CriticalBlock b(stopCrit); // multiple channels could call
        _setStopping(sender);
    }
    void stop(unsigned sender, broadcast_flags flag)
    {
        if (nodeBroadcast)
            sender = sender % nodes;
        CriticalBlock b(stopCrit); // multiple channels could call
        _setStopping(sender);
        stopFlag = flag;
    }
    void sendLocalStop(CSendItem *sendItem)
    {
        if (nodeBroadcast)
            return;
        Owned<CSendItem> stopSendItem = new CSendItem(bcast_stop, sendItem->querySlave(), sendItem->queryTargetChannel());
        stopSendItem->setFlag(bcastflag_stop);
        unsigned myNodeNum = activity.queryJob().queryMyNode();
        for (unsigned ch=0; ch<activity.queryJob().queryJobChannels(); ch++)
        {
            if (ch != activity.queryJobChannelNumber())
            {
                unsigned dst = activity.queryJob().querySlaveForNodeChannel(myNodeNum, ch) + 1;
                CriticalBlock b(broadcastLock); // prevent other channels overlapping, otherwise causes queue ordering issues with MP multi packet messages to same dst.
                comm->send(stopSendItem->queryMsg(), dst, mpTag);
            }
        }
    }
};

/* CMarker processes a sorted set of rows, comparing every adjacent row.
 * It creates a bitmap, where 1 represents row N mismatches N+1
 * Multiple threads are used to process blocks of the table in parallel
 * When complete, it knows how many unique values there are in the table
 * which are subsequently stepped through via findNextBoundary() to build
 * up a hash table
 */
class CMarker
{
    CActivityBase &activity;
    NonReentrantSpinLock lock;
    ICompare *cmp;
    OwnedConstThorRow bitSetMem; // for thread unsafe version
    Owned<IBitSet> bitSet;
    const void **base;
    rowidx_t nextChunkStartRow; // Updated as threads request next chunk
    rowidx_t rowCount, chunkSize; // There are configured at start of calculate()
    rowidx_t parallelMinChunkSize, parallelChunkSize; // Constant, possibly configurable in future
    unsigned threadCount;

    class CCompareThread : public CInterface, implements IThreaded
    {
        CMarker &parent;
        CThreaded threaded;
        rowidx_t startRow, endRow, chunkUnique;
    public:
        CCompareThread(CMarker &_parent, rowidx_t _startRow, rowidx_t _endRow)
            : parent(_parent), startRow(_startRow), endRow(_endRow), threaded("CCompareThread", this)
        {
            chunkUnique = 0;
        }
        rowidx_t getUnique() const { return chunkUnique; }
        void start() { threaded.start(); }
        void join() { threaded.join(); }
    // IThreaded
        virtual void main()
        {
            chunkUnique = parent.run(startRow, endRow);
        }
    };

    rowidx_t getMore(rowidx_t &startRow) // NB: returns end row #
    {
        //NOTE: If we could guarantee that nextChunkStartRow could not overflow then the spin lock could be replaced
        //with a atomic fetch_add().
        NonReentrantSpinBlock block(lock);
        if (nextChunkStartRow == rowCount)
            return 0;
        startRow = nextChunkStartRow;
        if (rowCount-nextChunkStartRow <= chunkSize)
            nextChunkStartRow = rowCount;
        else
            nextChunkStartRow += chunkSize;
        return nextChunkStartRow; // and end row for this particular chunk request
    }
    inline void mark(rowidx_t i)
    {
        // NB: Thread safe, because markers are dealing with discrete parts of bitSetMem (alighted to bits_t boundaries)
        bitSet->set(i); // mark boundary
    }
    rowidx_t doMarking(rowidx_t myStart, rowidx_t myEnd)
    {
        // myStart must be on bits_t boundary
        dbgassertex(0 == (myStart % BitsPerItem));

        rowidx_t chunkUnique = 0;
        const void **rows = base+myStart;
        rowidx_t i=myStart;
        for (; i<(myEnd-1); i++, rows++)
        {
            int r = cmp->docompare(*rows, *(rows+1));
            if (r)
            {
                ++chunkUnique;
                mark(i);
            }
            /* JCSMORE - could I binchop ahead somehow, to process duplicates more quickly..
             * i.e. if same cur+mid = cur, then no need to check intermediates..
             */
        }
        if (myEnd != rowCount)
        {
            // final row, cross boundary with next chunk, i.e. { last-row-of-my-chunk , first-row-of-next }
            int r = cmp->docompare(*rows, *(rows+1));
            if (r)
            {
                ++chunkUnique;
                mark(i);
            }
        }
        return chunkUnique;
    }
    rowidx_t run(rowidx_t myStart, rowidx_t myEnd)
    {
        rowidx_t chunkUnique = 0;
        loop
        {
            chunkUnique += doMarking(myStart, myEnd);
            myEnd = getMore(myStart);
            if (0 == myEnd)
                break; // done
        }
        return chunkUnique;
    }
public:
    CMarker(CActivityBase &_activity) : activity(_activity)
    {
        cmp = NULL;
        base = NULL;
        nextChunkStartRow = rowCount = chunkSize = 0;
        // perhaps should make these configurable..
        parallelMinChunkSize = 1024;
        parallelChunkSize = 10*parallelMinChunkSize;
        threadCount = activity.getOptInt(THOROPT_JOINHELPER_THREADS, 0);
        if (0 == threadCount)
            threadCount = activity.queryMaxCores();

        activity.ActPrintLog("CMarker: threadCount = %u", threadCount);
    }
    bool init(rowidx_t rowCount, roxiemem::IRowManager *rowManager)
    {
        bool threadSafeBitSet = activity.getOptBool("threadSafeBitSet", false); // for testing only
        if (threadSafeBitSet)
        {
            DBGLOG("Using Thread safe variety of IBitSet");
            bitSet.setown(createThreadSafeBitSet());
        }
        else
        {
            size32_t bitSetMemSz = getBitSetMemoryRequirement(rowCount);
            void *pBitSetMem = rowManager->allocate(bitSetMemSz, activity.queryContainer().queryId(), SPILL_PRIORITY_LOW);
            if (!pBitSetMem)
                return false;

            bitSetMem.setown(pBitSetMem);
            bitSet.setown(createBitSet(bitSetMemSz, pBitSetMem));
        }
        return true;
    }
    void reset()
    {
        bitSet.clear();
    }
    rowidx_t calculate(CThorExpandingRowArray &rows, ICompare *_cmp, bool doSort)
    {
        assertex(bitSet);
        cmp = _cmp;
        if (doSort)
            rows.sort(*cmp, threadCount);
        rowCount = rows.ordinality();
        if (0 == rowCount)
            return 0;
        base = rows.getRowArray();

        rowidx_t uniqueTotal = 0;
        if ((1 == threadCount) || (rowCount < parallelMinChunkSize))
            uniqueTotal = doMarking(0, rowCount);
        else
        {
            nextChunkStartRow = 0;
            chunkSize = rowCount / threadCount;
            if (chunkSize > parallelChunkSize)
                chunkSize = parallelChunkSize;
            else if (chunkSize < parallelMinChunkSize)
            {
                chunkSize = parallelMinChunkSize;
                threadCount = rowCount / chunkSize;
            }
            // Must be multiple of sizeof BitsPerItem
            chunkSize = ((chunkSize + (BitsPerItem-1)) / BitsPerItem) * BitsPerItem; // round up to nearest multiple of BitsPerItem

            /* This is yet another case of requiring a set of small worker threads
             * Thor should really use a common pool of lightweight threadlets made available to all
             * where any particular instances (e.g. lookup) can stipulate min/max it requires etc.
             */
            CIArrayOf<CCompareThread> threads;
            for (unsigned t=0; t<threadCount; t++)
            {
                if (nextChunkStartRow+chunkSize >= rowCount)
                {
                    threads.append(* new CCompareThread(*this, nextChunkStartRow, rowCount));
                    nextChunkStartRow = rowCount;
                    break;
                }
                else
                {
                    rowidx_t s = nextChunkStartRow;
                    nextChunkStartRow += chunkSize;
                    threads.append(* new CCompareThread(*this, s, nextChunkStartRow));
                }
            }
            ForEachItemIn(t, threads)
                threads.item(t).start();
            ForEachItemIn(t2, threads)
            {
                CCompareThread &compareThread = threads.item(t2);
                compareThread.join();
                uniqueTotal += compareThread.getUnique();
            }
        }
        ++uniqueTotal;
        mark(rowCount-1); // last row is implicitly end of group
        cmp = NULL;
        return uniqueTotal;
    }
    rowidx_t findNextBoundary(rowidx_t start)
    {
        if (start==rowCount)
            return 0;
        return bitSet->scan(start, true)+1;
    }
};

#ifdef _TRACEBROADCAST
#define InterChannelBarrier() interChannelBarrier(__LINE__)
#else
#define InterChannelBarrier() interChannelBarrier();
#endif


struct HtEntry { rowidx_t index, count; unsigned hash; };

interface ITableLookup : extends IInterface
{
    virtual const void *getNextRHS(HtEntry &currentHashEntry) = 0;
    virtual const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry) = 0;
};

class CTableCommon : public CSimpleInterfaceOf<ITableLookup>
{
protected:
    rowidx_t tableSize = 0;
    rowidx_t tableMask = 0;
public:
    virtual void reset()
    {
        tableSize = 0;
    }
    virtual void setup(roxiemem::IRowManager *rowManager, rowidx_t size, IHash *_leftHash, IHash *_rightHash, ICompare *_compareLeftRight) = 0;
    virtual void addRows(CThorExpandingRowArray &_rows, CMarker *marker) = 0;
};


/* 
    These activities load the RHS into a table, therefore
    the right hand stream -should- contain the fewer records

    Inner, left outer and left only joins supported
*/

/* Base common to:
 * 1) Lookup Many
 * 2) Lookup
 * 3) All
 *
 * Handles the initialization, broadcast and processing (decompression, deserialization, population) of RHS
 * and base common functionality for all and lookup varieties
 */
class CInMemJoinBase : public CSlaveActivity, implements ILookAheadStopNotify, implements IBCastReceive
{
    typedef CSlaveActivity PARENT;

    Owned<IException> leftexception;

    bool eos, eog, someSinceEog;
    IHThorAnyJoinBaseArg *helper;

protected:
    static int sortBySize(void * const *_i1, void * const *_i2)
    {
        CThorSpillableRowArray *i1 = (CThorSpillableRowArray *)*_i1;
        CThorSpillableRowArray *i2 = (CThorSpillableRowArray *)*_i2;
        return i2->numCommitted()-i1->numCommitted();
    }

    /* Utility class, that is called from the broadcaster to queue up received blocks
     * It will block if it has > MAX_QUEUE_BLOCKS to process (on the queue)
     * Processing will decompress the incoming blocks and add them to the local channel row array
     */

    class CRowProcessor : public CSimpleInterfaceOf<IThreaded>
    {
        CThreadedPersistent threaded;
        CInMemJoinBase &owner;
        bool stopped;
        SimpleInterThreadQueueOf<CSendItem, true> blockQueue;
        Owned<IException> exception;

        CThorExpandingRowArray rhsInRowsTemp;
        CThorExpandingRowArray pending;


        CCycleTimer deserializationTimer;
        cycle_t deserializationTime = 0;
        rowcount_t rowCount = 0;


        void clearQueue()
        {
            loop
            {
                Owned<CSendItem> sendItem = blockQueue.dequeueNow();
                if (NULL == sendItem)
                    break;
            }
        }
    public:
        CRowProcessor(CInMemJoinBase &_owner) : threaded("CRowProcessor", this), owner(_owner), pending(_owner, _owner.sharedRightRowInterfaces), rhsInRowsTemp(_owner, _owner.sharedRightRowInterfaces)
        {
            stopped = false;
            blockQueue.setLimit(MAX_QUEUE_BLOCKS);
            deserializationTime = 0;
            rowCount = 0;
        }
        ~CRowProcessor()
        {
            blockQueue.stop();
            clearQueue();
            wait();
        }
        void start()
        {
            stopped = false;
            clearQueue();
            exception.clear();
            threaded.start();
        }
        void abort()
        {
            if (!stopped)
            {
                stopped = true;
                blockQueue.enqueue(NULL);
            }
        }
        void wait()
        {
            threaded.join();
            if (exception)
                throw exception.getClear();
        }
        void addBlock(CSendItem *sendItem)
        {
            if (exception)
            {
                if (sendItem)
                    sendItem->Release();
                throw exception.getClear();
            }
            blockQueue.enqueue(sendItem); // will block if queue full
        }
        void processRHSRows(MemoryBuffer &mb)
        {
            RtlDynamicRowBuilder rowBuilder(owner.rightAllocator); // NB: rightAllocator is the shared allocator
            CThorStreamDeserializerSource memDeserializer(mb.length(), mb.toByteArray());

            while (!memDeserializer.eos())
            {
                deserializationTimer.reset();
                size32_t sz = owner.rightDeserializer->deserialize(rowBuilder, memDeserializer);
                deserializationTime += deserializationTimer.elapsedCycles();
                pending.append(rowBuilder.finalizeRowClear(sz));
                if (pending.ordinality() >= 100)
                {
                    // NB: If spilt, addRHSRows will filter out non-locals
                    if (!owner.addRHSRows(pending, rhsInRowsTemp)) // NB: in SMART case, must succeed
                        throw MakeActivityException(&owner, 0, "Out of memory: Unable to add any more rows to RHS");
                    rowCount += 100;
                }
            }
            if (pending.ordinality())
            {
                rowCount += pending.ordinality();
                // NB: If spilt, addRHSRows will filter out non-locals
                if (!owner.addRHSRows(pending, rhsInRowsTemp)) // NB: in SMART case, must succeed
                    throw MakeActivityException(&owner, 0, "Out of memory: Unable to add any more rows to RHS");
            }
        }
    // IThreaded
        virtual void main()
        {
            CCycleTimer timer;
            cycle_t rpDequeueTime = 0;
            cycle_t rpExpandTime = 0;
            cycle_t rpProcessRHSRowsTime = 0;
            unsigned blocks = 0;
            try
            {
                while (!stopped)
                {
                    timer.reset();
                    Owned<CSendItem> sendItem = blockQueue.dequeue(); // NB: will be rows for this channel only
                    rpDequeueTime += timer.elapsedCycles();
                    if (stopped || (NULL == sendItem))
                        break;
                    timer.reset();
                    MemoryBuffer expandedMb;
                    ThorExpand(sendItem->queryMsg(), expandedMb);
                    rpExpandTime += timer.elapsedCycles();
                    timer.reset();
                    processRHSRows(expandedMb);
                    rpProcessRHSRowsTime += timer.elapsedCycles();
                    ++blocks;
                }
            }
            catch (IException *e)
            {
                exception.setown(e);
                EXCLOG(e, "CRowProcessor");
            }
            VStringBuffer msg("rpDequeueTime (blocks=%u, rowCount=%" I64F "u)", blocks, rowCount);
            owner.logTiming(msg, rpDequeueTime);
            owner.logTiming("rpExpandTime", rpExpandTime);
            owner.logTiming("rpProcessRHSRowsTime", rpProcessRHSRowsTime);
            owner.logTiming("deserializationTime", deserializationTime);
        }
    } *rowProcessor;

    Owned<CBroadcaster> broadcaster;
    rowidx_t rhsTableLen;
    Owned<CTableCommon> table;
    Owned<ITableLookup> tableProxy; // will be used if global
    HtEntry currentHashEntry; // Used for lookup,many only
    OwnedConstThorRow leftRow;

    IThorDataLink *leftITDL, *rightITDL;
    Owned<IRowStream> left, right;
    IThorAllocator *rightThorAllocator;
    roxiemem::IRowManager *rightRowManager;
    Owned<IThorRowInterfaces> sharedRightRowInterfaces;
    Owned<IEngineRowAllocator> rightAllocator;
    Owned<IEngineRowAllocator> leftAllocator;
    Owned<IEngineRowAllocator> allocator; // allocator for output transform
    Owned<IOutputRowSerializer> rightSerializer;
    Owned<IOutputRowDeserializer> rightDeserializer;
    enum LKJState : byte { lkj_start, lkj_gotrhs, lkj_stdjoin, lkj_locallookup } lkjState = lkj_start;
    join_t joinType;
    OwnedConstThorRow defaultRight;
    bool local;
    unsigned flags;
    bool exclude;
    const void *rhsNext;
    CThorExpandingRowArray rhs;
    Owned<IOutputMetaData> outputMeta;
    IOutputMetaData *rightOutputMeta;
    CriticalSection rhsRowLock, clearNonLocalLock;
    Owned<IThorRowCollector> rightCollector;
    IArrayOf<IRowStream> gatheredRHSNodeStreams;

    rowidx_t nextRhsRow;
    unsigned keepLimit;
    unsigned joined;
    unsigned joinCounter;
    OwnedConstThorRow defaultLeft;

    bool leftMatch, grouped;
    bool fuzzyMatch, returnMany;
    unsigned myNodeNum, mySlaveNum;
    unsigned myChannelNum;
    unsigned numNodes, numSlaves;
    OwnedMalloc<CInMemJoinBase *> channels;
    bool localGathered = false;
    bool postBroadcast = false;
    bool localGatheredAndSorted = false;
    bool allInMemory = false;
    rowidx_t totalRHSCount = 0;

    atomic_t interChannelToNotifyCount; // only used on channel 0
    InterruptableSemaphore interChannelBarrierSem;
    bool channelActivitiesAssigned;

    std::atomic<cycle_t> addRHSRowTime = {0}; // avoid hitting deleted copy constructor
    cycle_t nextRowTime = 0;



    inline bool isLocal() const { return local; }
    inline bool isGlobal() const { return !local; }
    inline void signalInterChannelBarrier()
    {
        interChannelBarrierSem.signal();
    }
    inline bool incNotifyCountAndCheck()
    {
        if (atomic_add_and_read(&interChannelToNotifyCount, 1) == queryJob().queryJobChannels())
        {
            atomic_set(&interChannelToNotifyCount, 0); // reset for next barrier
            return true;
        }
        return false;
    }
    inline void interChannelBarrier()
    {
        if (queryJob().queryJobChannels()>1)
        {
            if (channels[0]->incNotifyCountAndCheck())
            {
                for (unsigned ch=0; ch<queryJob().queryJobChannels(); ch++)
                {
                    if (channels[ch] != this)
                        channels[ch]->signalInterChannelBarrier();
                }
            }
            else
                interChannelBarrierSem.wait();
        }
    }
#ifdef _TRACEBROADCAST
    void interChannelBarrier(unsigned lineNo)
    {
        ActPrintLog("waiting on interChannelBarrier, lineNo = %d", lineNo);
        interChannelBarrier();
        ActPrintLog("past interChannelBarrier, lineNo = %d", lineNo);
    }
#endif
    inline bool isGroupOp() const
    {
        switch (container.getKind())
        {
            case TAKlookupdenormalizegroup:
            case TAKsmartdenormalizegroup:
            case TAKalldenormalizegroup:
                return true;
        }
        return false;
    }
    inline bool isDenormalize() const
    {
        switch (container.getKind())
        {
            case TAKlookupdenormalize:
            case TAKlookupdenormalizegroup:
            case TAKalldenormalize:
            case TAKalldenormalizegroup:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
                return true;
        }
        return false;
    }
    StringBuffer &getJoinTypeStr(StringBuffer &str)
    {
        switch(joinType)
        {
            case JT_Undefined:  return str.append("UNDEFINED");
            case JT_Inner:      return str.append("INNER");
            case JT_LeftOuter:  return str.append("LEFT OUTER");
            case JT_RightOuter: return str.append("RIGHT OUTER");
            case JT_LeftOnly:   return str.append("LEFT ONLY");
            case JT_RightOnly:  return str.append("RIGHT ONLY");
        }
        return str.append("---> Unknown Join Type <---");
    }
    bool inline gotRHS() const { return lkjState >= lkj_gotrhs; }
    void clearRHS()
    {
        rightCollector->reset();
        rhs.clearRows();
    }
    void clearHT()
    {
        rhsTableLen = 0;
        table->reset();
    }
    virtual unsigned queryChannelDestination(const void *row)
    {
        /* default (for ALL join). Could send to any remote channel, but
         * choose to send to slave on same channel as me.
         */
        return mySlaveNum / numNodes;
    }
    void broadcastRHS() // broadcasting local rhs
    {
        ActPrintLog("start broadcastRHS()");
        Owned<CSendItem> sendItem = broadcaster->newSendItem(bcast_send);
        CThorExpandingRowArray rhsInRowsTemp(*this, sharedRightRowInterfaces);

        OwnedPointerArrayOf<CThorExpandingRowArray> pendingPerChannel;
        SafePointerArrayOf<MemoryBuffer> serializationBuffers;
        SafePointerArrayOf<CMemoryRowSerializer> rowSerializers;
        for (unsigned ch=0; ch<queryJob().queryJobChannels(); ch++)
        {
            pendingPerChannel.append(new CThorExpandingRowArray(*this, sharedRightRowInterfaces));
            MemoryBuffer *mb = new MemoryBuffer;
            serializationBuffers.append(mb);
            rowSerializers.append(new CMemoryRowSerializer(*mb));
        }

        CThorExpandingRowArray **pendingChannels = pendingPerChannel.getArray();

        cycle_t serializationTime = 0, compressionTime = 0, broadcastToOthersTime = 0, rightNextRowTime = 0;
        CCycleTimer serializationTimer, compressionTimer, broadcastToOthersTimer, rightNextRowTimer, broadcastRHSTime;
        try
        {
            while (!abortSoon)
            {
                unsigned channelToSend = NotFound;
                while (!abortSoon)
                {
                    rightNextRowTimer.reset();
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;
                    rightNextRowTime += rightNextRowTimer.elapsedCycles();

                    unsigned channelDst = queryChannelDestination(row);

                    CThorExpandingRowArray &pendingChannel = *pendingChannels[channelDst];
                    MemoryBuffer &mb = *serializationBuffers[channelDst];
                    CMemoryRowSerializer &mbser = *rowSerializers[channelDst];

                    if (numNodes>1)
                    {
                        serializationTimer.reset();
                        rightSerializer->serialize(mbser, (const byte *)row.get());
                        serializationTime += serializationTimer.elapsedCycles();
                        pendingChannel.append(row.getClear());
                        if (mb.length() >= MAX_SEND_SIZE)
                        {
                            channelToSend = channelDst;
                            break;
                        }
                    }
                    else
                        pendingChannel.append(row.getClear());
                    if (pendingChannel.ordinality() >= 100)
                    {
                        if (!channels[channelDst]->addRHSRows(pendingChannel, rhsInRowsTemp))
                            throw MakeActivityException(this, 0, "Out of memory: Unable to add any more rows to RHS");
                    }
                    if (broadcaster->stopRequested())
                        break;
                }
                if (NotFound == channelToSend) // either at end or stopRequested
                {
                    ForEachItemIn(p, pendingPerChannel)
                    {
                        if (pendingChannels[p]->ordinality())
                        {
                            if (!channels[p]->addRHSRows(*pendingChannels[p], rhsInRowsTemp))
                                throw MakeActivityException(this, 0, "Out of memory: Unable to add any more rows to RHS");
                        }
                        MemoryBuffer &mb = *serializationBuffers[p];
                        if (mb.length())
                        {
                            compressionTimer.reset();
                            ThorCompress(mb, sendItem->queryMsg());
                            compressionTime += compressionTimer.elapsedCycles();
                            broadcastToOthersTimer.reset();

                            sendItem->setTargetChannel(p);
                            if (!broadcaster->send(sendItem))
                                break;
                            broadcastToOthersTime += broadcastToOthersTimer.elapsedCycles();
                            sendItem->reset();
                            mb.clear();
                        }
                    }
                    break;
                }
                else
                {
                    if (!channels[channelToSend]->addRHSRows(*pendingChannels[channelToSend], rhsInRowsTemp))
                        throw MakeActivityException(this, 0, "Out of memory: Unable to add any more rows to RHS");
                    compressionTimer.reset();
                    MemoryBuffer &mb = *serializationBuffers[channelToSend];
                    ThorCompress(mb, sendItem->queryMsg());
                    compressionTime += compressionTimer.elapsedCycles();
                    broadcastToOthersTimer.reset();

                    sendItem->setTargetChannel(channelToSend);
                    if (!broadcaster->send(sendItem))
                        break;
                    broadcastToOthersTime += broadcastToOthersTimer.elapsedCycles();
                    sendItem->reset();
                    mb.clear();
                }
            }
        }
        catch (IException *e)
        {
            ActPrintLog(e, "CInMemJoinBase::broadcastRHS: exception");
            throw;
        }


        logTiming("serializationTime", serializationTime);
        logTiming("compressionTime", compressionTime);
        logTiming("broadcastToOthersTime", broadcastToOthersTime);
        logTiming("rightNextRowTime", rightNextRowTime);
        logTiming("broadcastRHSTime", broadcastRHSTime.elapsedCycles());

        sendItem.setown(broadcaster->newSendItem(bcast_stop));
        if (broadcaster->stopRequested())
            sendItem->setFlag(broadcaster->queryStopFlag());
        ActPrintLog("Sending final RHS broadcast packet");
        broadcaster->send(sendItem); // signals stop to others
        ActPrintLog("end broadcastRHS()");
    }
    inline void resetRhsNext()
    {
        nextRhsRow = 0;
        joined = 0;
        leftMatch = false;
    }
    inline const void *denormalizeNextRow()
    {
        ConstPointerArray filteredRhs;
        while (rhsNext)
        {
            if (abortSoon)
                return NULL;
            if (!fuzzyMatch || (match(leftRow, rhsNext)))
            {
                leftMatch = true;
                if (exclude)
                {
                    rhsNext = NULL;
                    break;
                }
                ++joined;
                filteredRhs.append(rhsNext);
            }
            if (!returnMany || joined == keepLimit)
            {
                rhsNext = NULL;
                break;
            }
            rhsNext = tableProxy->getNextRHS(currentHashEntry); // NB: currentHashEntry only used for Lookup,Many case
        }
        if (filteredRhs.ordinality() || (!leftMatch && 0!=(flags & JFleftouter)))
        {
            unsigned rcCount = 0;
            OwnedConstThorRow ret;
            RtlDynamicRowBuilder rowBuilder(allocator);
            unsigned numRows = filteredRhs.ordinality();
            const void *rightRow = numRows ? filteredRhs.item(0) : defaultRight.get();
            if (isGroupOp())
            {
                size32_t sz = joinTransform(rowBuilder, leftRow, rightRow, numRows, filteredRhs.getArray());
                if (sz)
                    ret.setown(rowBuilder.finalizeRowClear(sz));
            }
            else
            {
                ret.set(leftRow);
                if (filteredRhs.ordinality())
                {
                    size32_t rowSize = 0;
                    loop
                    {
                        const void *rightRow = filteredRhs.item(rcCount);
                        size32_t sz = joinTransform(rowBuilder, ret, rightRow, ++rcCount);
                        if (sz)
                        {
                            rowSize = sz;
                            ret.setown(rowBuilder.finalizeRowClear(sz));
                        }
                        if (rcCount == filteredRhs.ordinality())
                            break;
                        rowBuilder.ensureRow();
                    }
                    if (!rowSize)
                        ret.clear();
                }
            }
            return ret.getClear();
        }
        else
            return NULL;
    }
    const void *lookupNextRow()
    {
        CCycleAddTimer tb(nextRowTime);
        if (!abortSoon && !eos)
        {
            loop
            {
                if (NULL == rhsNext)
                {
                    leftRow.setown(left->nextRow());
                    joinCounter = 0;
                    if (leftRow)
                    {
                        eog = false;
                        if (totalRHSCount)
                        {
                            resetRhsNext();
                            const void *failRow = NULL;
                            // NB: currentHashEntry only used for Lookup,Many case
                            rhsNext = tableProxy->getFirstRHSMatch(leftRow, failRow, currentHashEntry); // also checks abortLimit/atMost
                            if (failRow)
                                return failRow;
                        }
                    }
                    else
                    {
                        if (eog)
                            eos = true;
                        else
                        {
                            eog = true;
                            if (!someSinceEog)
                                continue; // skip empty 'group'
                            someSinceEog = false;
                        }
                        break;
                    }
                }
                OwnedConstThorRow ret;
                if (isDenormalize())
                    ret.setown(denormalizeNextRow());
                else
                {
                    RtlDynamicRowBuilder rowBuilder(allocator);
                    while (rhsNext)
                    {
                        if (!fuzzyMatch || match(leftRow, rhsNext))
                        {
                            leftMatch = true;
                            if (!exclude)
                            {
                                size32_t sz = joinTransform(rowBuilder, leftRow, rhsNext, ++joinCounter);
                                if (sz)
                                {
                                    OwnedConstThorRow row = rowBuilder.finalizeRowClear(sz);
                                    someSinceEog = true;
                                    if (++joined == keepLimit)
                                        rhsNext = NULL;
                                    else if (!returnMany)
                                        rhsNext = NULL;
                                    else
                                        rhsNext = tableProxy->getNextRHS(currentHashEntry); // NB: currentHashEntry only used for Lookup,Many case
                                    return row.getClear();
                                }
                            }
                        }
                        rhsNext = tableProxy->getNextRHS(currentHashEntry); // NB: currentHashEntry only used for Lookup,Many case
                    }
                    if (!leftMatch && NULL == rhsNext && 0!=(flags & JFleftouter))
                    {
                        size32_t sz = joinTransform(rowBuilder, leftRow, defaultRight, 0);
                        if (sz)
                            ret.setown(rowBuilder.finalizeRowClear(sz));
                    }
                }
                if (ret)
                {
                    someSinceEog = true;
                    return ret.getClear();
                }
            }
        }
        return NULL;
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CInMemJoinBase(CGraphElementBase *_container) : CSlaveActivity(_container), rhs(*this)
    {
        helper = static_cast <IHThorAnyJoinBaseArg *> (queryHelper());

        nextRhsRow = 0;
        rhsNext = NULL;
        myNodeNum = queryJob().queryMyNode();
        mySlaveNum = queryJobChannel().queryMySlave();
        myChannelNum = queryJobChannelNumber();
        numNodes = queryJob().queryNodes();
        numSlaves = queryJob().querySlaves();
        local = container.queryLocal() || (1 == numSlaves);
        rowProcessor = NULL;

        atomic_set(&interChannelToNotifyCount, 0);
        rhsTableLen = 0;
        leftITDL = rightITDL = NULL;

        joined = 0;
        joinCounter = 0;
        leftMatch = false;
        returnMany = false;

        eos = eog = someSinceEog = false;

        flags = helper->getJoinFlags();
        grouped = helper->queryOutputMeta()->isGrouped();
        fuzzyMatch = 0 != (JFmatchrequired & flags);
        exclude = 0 != (JFexclude & flags);
        keepLimit = helper->getKeepLimit();
        if (0 == keepLimit)
            keepLimit = (unsigned)-1;
        if (flags & JFleftouter)
            joinType = exclude ? JT_LeftOnly : JT_LeftOuter;
        else
            joinType = JT_Inner;
        channelActivitiesAssigned = false;
        rightOutputMeta = NULL;
        if (isGlobal() && getOptBool("lkjoinUseSharedAllocator", true))
        {
            ActPrintLog("Using shared row manager for RHS");
            rightThorAllocator = queryJob().querySharedAllocator();
        }
        else
            rightThorAllocator = queryJobChannel().queryThorAllocator();
        rightRowManager = rightThorAllocator->queryRowManager();
        appendOutputLinked(this);
    }
    ~CInMemJoinBase()
    {
        if (isGlobal())
            ::Release(rowProcessor);
    }
    void logTiming(const char *msg, cycle_t cycles)
    {
        ActPrintLog("TIME: %s - %s = %u", queryJob().queryWuid(), msg, static_cast<unsigned>(cycle_to_millisec(cycles)));
    }
    ITableLookup *queryTable() { return table; }
    void startLeftInput()
    {
        try
        {
            startInput(0);
        }
        catch(IException *e)
        {
            leftexception.setown(e);
        }
    }

    virtual bool match(const void *lhs, const void *rhsrow) = 0;
    virtual const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned numRows, const void **rows) = 0;
    virtual const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned count) = 0;

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        StringBuffer str;
        ActPrintLog("Join type is %s", getJoinTypeStr(str).str());

        leftITDL = queryInput(0);
        rightITDL = queryInput(1);
        rightOutputMeta = rightITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
        rightAllocator.setown(rightThorAllocator->getRowAllocator(rightOutputMeta, container.queryId(), (roxiemem::RoxieHeapFlags)(roxiemem::RHFpacked|roxiemem::RHFunique)));
        sharedRightRowInterfaces.setown(createThorRowInterfaces(rightRowManager, rightOutputMeta, queryId(), &queryJobChannel().querySharedMemCodeContext()));

        if (isGlobal())
        {
            rhs.setup(sharedRightRowInterfaces);

            mpTag = container.queryJobChannel().deserializeMPTag(data);

            channels.allocateN(queryJob().queryJobChannels());
            broadcaster.setown(new CBroadcaster(*this));
            rowProcessor = new CRowProcessor(*this);
        }
    }
    CInMemJoinBase *queryChannel(unsigned channel)
    {
        return channels[channel];
    }
    virtual void setInputStream(unsigned index, CThorInput &_input, bool consumerOrdered) override
    {
        PARENT::setInputStream(index, _input, consumerOrdered);
        if (0 == index)
            setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), LOOKUPJOINL_SMART_BUFFER_SIZE, isSmartBufferSpillNeeded(input->queryFromActivity()), grouped, RCUNBOUND, this, &container.queryJob().queryIDiskUsage()));
    }
    virtual void start() override
    {
        assertex(inputs.ordinality() == 2);

        lkjState = lkj_start;
        nextRhsRow = 0;
        joined = 0;
        joinCounter = 0;
        leftMatch = false;
        rhsNext = NULL;
        rhsTableLen = 0;

        if (isGlobal())
        {
            // It is not until here, that it is guaranteed all channel slave activities have been initialized.
            if (!channelActivitiesAssigned)
            {
                channelActivitiesAssigned = true;
                for (unsigned c=0; c<queryJob().queryJobChannels(); c++)
                {
                    CInMemJoinBase &channel = (CInMemJoinBase &)queryChannelActivity(c);
                    channels[c] = &channel;
                }
            }
        }
        allocator.set(queryRowAllocator());
        leftAllocator.set(::queryRowAllocator(leftITDL));
        outputMeta.set(leftITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta());

        eos = eog = someSinceEog = false;
        atomic_set(&interChannelToNotifyCount, 0);
        currentHashEntry.index = 0;
        currentHashEntry.count = 0;
        currentHashEntry.hash = 0;

        rightSerializer.set(::queryRowSerializer(rightITDL));
        rightDeserializer.set(::queryRowDeserializer(rightITDL));

        if ((flags & JFonfail) || (flags & JFleftouter))
        {
            RtlDynamicRowBuilder rr(rightAllocator);
            rr.ensureRow();
            size32_t rrsz = helper->createDefaultRight(rr);
            defaultRight.setown(rr.finalizeRowClear(rrsz));
        }

        CAsyncCallStart asyncLeftStart(std::bind(&CInMemJoinBase::startLeftInput, this));
        try
        {
            startInput(1);
        }
        catch (CATCHALL)
        {
            asyncLeftStart.wait();
            left->stop();
            throw;
        }
        asyncLeftStart.wait();
        left.set(inputStream);
        right.set(queryInputStream(1));
        if (leftexception)
        {
            right->stop();
            throw leftexception.getClear();
        }
        dataLinkStart();
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        if (isGlobal())
        {
            cancelReceiveMsg(queryJob().queryNodeComm(), RANK_ALL, mpTag);
            interChannelBarrierSem.interrupt(NULL);
            broadcaster->cancel();
            if (rowProcessor)
                rowProcessor->abort();
        }
    }
    virtual void stop()
    {
        // JCS->GH - if in a child query, it would be good to preserve RHS.. would need tip/flag from codegen that constant
        clearRHS();
        clearHT();
        if (right)
        {
            stopInput(1, "(R)");
            right.clear();
        }
        if (broadcaster)
            broadcaster->reset();
        stopInput(0, "(L)");
        left.clear();
        logTiming("nextRow", nextRowTime);
        dataLinkStop();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canStall = true;
    }

// The methods below are only used if activity is global
    void doBroadcastRHS(bool stopping)
    {
        if (stopping)
            broadcaster->setStopping(myNodeNum);
        rowProcessor->start();
        broadcaster->start(this, mpTag, stopping, false); // slaves broadcasting
        broadcastRHS();
        broadcaster->end();

        InterChannelBarrier();
        rowProcessor->wait();
        InterChannelBarrier();

        rightCollector->flush();

        logTiming("receiveMsgTime", broadcaster->getReceiveMsgTime());
    }
    void doBroadcastStop(mptag_t tag, broadcast_flags flag) // only called on channel 0
    {
        broadcaster->reset();
        broadcaster->start(this, tag, false, true); // nodes broadcasting
        Owned<CSendItem> sendItem = broadcaster->newSendItem(bcast_stop);
        if (flag)
            sendItem->setFlag(flag);
        broadcaster->send(sendItem); // signals stop to others
        broadcaster->end();
    }
    rowidx_t getLocalCount() const
    {
        CThorArrayLockBlock b(*rightCollector);
        return rightCollector->numRows();
    }
    rowidx_t getGlobalRHSTotal()
    {
        rowcount_t rhsRows = 0;
        for (unsigned ch=0; ch<queryJob().queryJobChannels(); ch++)
        {
            rhsRows += channels[ch]->getLocalCount();
            if (rhsRows > RIMAX)
                throw MakeActivityException(this, 0, "Too many RHS rows: %" RCPF "u", rhsRows);
        }
        return (rowidx_t)rhsRows;
    }
    virtual bool addRHSRows(CThorExpandingRowArray &inRows, CThorExpandingRowArray &rhsInRowsTemp)
    {
        CriticalBlock b(rhsRowLock);
        return rightCollector->addRows(inRows, true);
    }

// IBCastReceive (only used if global)
    virtual void bCastReceive(CSendItem *sendItem, bool stop)
    {
        if (sendItem)
        {
            if (bcast_stop == sendItem->queryCode())
            {
                if (0 == (sendItem->queryFlags() & bcastflag_stop))
                    broadcaster->sendLocalStop(sendItem);
                sendItem->Release();
                if (!stop)
                    return;
                sendItem = NULL; // fall through, base signals stop to rowProcessor
            }
        }
        dbgassertex((sendItem==NULL) == stop); // if sendItem==NULL stop must = true, if sendItem != NULL stop must = false;
        rowProcessor->addBlock(sendItem);
    }
    virtual void onInputFinished(rowcount_t count)
    {
        ActPrintLog("LHS input finished, %" RCPF "d rows read", count);
    }
};


/* Base class for:
 * 1) Lookup Many
 * 2) Lookup
 * They use different hash table representations, Lookup Many represents entries as {index, count}
 * Where as Lookup represents as simply hash table to rows
 * The main activity class
 * Both varieties do common work in this base class
 *
 * It performs the join, by 1st ensuring all RHS data is on all nodes, creating a hash table of this gathered set
 * then it streams the LHS through, matching against the RHS hash table entries.
 * It also handles match conditions where there is no hard match (, ALL), in those cases no hash table is needed.
 *
 * This base class also handles the 'SMART' functionality.
 * If RHS doesn't fit into memory, this class handles failover to local lookupjoin, by hashing the RHS to local
 * and hash distributing the LHS.
 * If the local RHS table still doesn't fit into memory, it will failover to a standard hash join, i.e. it will
 * need to sort both sides
 *
 * TODO: right outer/only joins
 */

interface IChannelDistributor
{
    virtual void putRow(const void *row) = 0;
    virtual bool spill(bool critical) = 0;
    virtual roxiemem::IBufferedRowCallback *queryCallback() = 0;
};

class CLookupJoinActivityBase : public CInMemJoinBase, implements roxiemem::IBufferedRowCallback
{
    typedef CInMemJoinBase PARENT;
    IHThorHashJoinArg *helper;
    bool oomCallbackInstalled = false;

protected:
    IHash *leftHash, *rightHash;
    ICompare *compareRight, *compareLeftRight;

    unsigned abortLimit, atMost;
    bool dedup, stable;

    mptag_t lhsDistributeTag, rhsDistributeTag, broadcast2MpTag, broadcast3MpTag;

    // Handling failover to a) hashed local lookupjoin b) hash distributed standard join
    bool smart;
    bool rhsCompacted;
    unsigned spillCompInfo;
    Owned<IHashDistributor> lhsDistributor, rhsDistributor;
    ICompare *compareLeft;
    atomic_t failedOverToLocal, failedOverToStandard;
    Owned<IJoinHelper> joinHelper;

    inline bool isSmart() const { return smart; }
    inline void setFailoverToLocal(bool tf) { atomic_set(&failedOverToLocal, (int)tf); }
    inline void setFailoverToStandard(bool tf) { atomic_set(&failedOverToStandard, (int)tf); }
    inline bool hasFailedOverToLocal() const { return 0 != atomic_read(&failedOverToLocal); }
    inline bool checkFailedOverToLocal() { return atomic_cas(&failedOverToLocal, 1, 0); }
    inline bool hasFailedOverToStandard() const { return 0 != atomic_read(&failedOverToStandard); }
    rowidx_t clearMyNonLocals()
    {
        ActPrintLog("#3 clearMyNonLocals");
        CriticalBlock b(rhsRowLock); // must prevent this interfering with writers. NB: could be re-entrant if addRHSRows caused resize that cause this callback to be called.
        ActPrintLog("#4 clearMyNonLocals");
        CThorArrayLockBlock b2(*rightCollector);
        ActPrintLog("#5 clearMyNonLocals");
        setFailoverToLocal(true); // ensure it is
        ActPrintLog("#6 clearMyNonLocals");
        if (!broadcaster->stopRequested())
            broadcaster->stop(mySlaveNum, bcastflag_spilt); // signals to broadcast to start stopping immediately and to signal spilt to others
        ActPrintLog("#7 clearMyNonLocals");
        CThorExpandingRowArray channelRows(*this, sharedRightRowInterfaces);
        ActPrintLog("#8 clearMyNonLocals");
        rightCollector->transferRowsOut(channelRows, false); // don't sort
        ActPrintLog("#9 clearMyNonLocals");
        rowidx_t clearedRows = 0;
        rowidx_t numRows = channelRows.ordinality();

        ActPrintLog("#10 clearMyNonLocals");
        CCycleTimer timer;
        cycle_t hashCycles = 0;
        cycle_t releaseRowCycles = 0;
        for (rowidx_t r=0; r<numRows; r++)
        {
            timer.reset();
            unsigned hv = rightHash->hash(channelRows.query(r));
            hashCycles += timer.elapsedCycles();
            if (myNodeNum != (hv % numNodes))
            {
                timer.reset();
                {
                    OwnedConstThorRow row = channelRows.getClear(r); // dispose of
                }
                releaseRowCycles += timer.elapsedCycles(); 
                ++clearedRows;
            }
        }
        rowidx_t preCompactCount = channelRows.ordinality();
        ActPrintLog("Channel cleared %" RIPF "u, about to compacted from %" RIPF "u", clearedRows, preCompactCount);
        logTiming("Channel clear hashTime", hashCycles);
        logTiming("Channel clear rowRelease", releaseRowCycles);
        if (clearedRows)
            channelRows.compact();
        rightCollector->transferRowsIn(channelRows);
        ActPrintLog("Channel cleared %" RIPF "u, compacted from %" RIPF "u to %" RIPF "u", clearedRows, preCompactCount, (rowidx_t)rightCollector->numRows());
        return clearedRows;
    }
    rowidx_t clearNonLocalRows(const char *msg, bool allowSpill)
    {
        if (allInMemory)
            return false; // if here, implies CB was called, whilst final handover was being done and CB being removed.
        {
            ActPrintLog("#1 Clearing non-local rows - cause: %s, allowSpill=%s", msg, allowSpill?"true":"false");
            CLookupJoinActivityBase *lkjoinCh0 = (CLookupJoinActivityBase *)channels[0];
            CriticalBlock b(lkjoinCh0->clearNonLocalLock);
            ActPrintLog("#2 Clearing non-local rows - cause: %s, allowSpill=%s", msg, allowSpill?"true":"false");
            if (lkjoinCh0->checkFailedOverToLocal()) // I have flagged channel0 failover
            {
                // JCSMORE: As all need clearing, would benefit from doing in parallel
                std::atomic<rowidx_t> clearedRows(0);

                class CClearChannelRows : public CAsyncFor
                {
                    CLookupJoinActivityBase &activity;
                    std::atomic<rowidx_t> &clearedRows;
                public:
                    CClearChannelRows(CLookupJoinActivityBase &_activity, std::atomic<rowidx_t> &_clearedRows) : activity(_activity), clearedRows(_clearedRows)
                    {
                    }
                    void Do(unsigned i)
                    {
                        clearedRows += ((CLookupJoinActivityBase *)activity.queryChannel(i))->clearMyNonLocals();
                    }
                } afor(*this, clearedRows);

                unsigned numThreads = queryJob().queryJobChannels(); // not sure you'll ever want to change this
                CCycleTimer timer;
                afor.For(queryJob().queryJobChannels(), numThreads);
                VStringBuffer msg("Cleared %" RIPF "u on %u threads", clearedRows.load(), numThreads);
                logTiming(msg, timer.elapsedCycles());
                if (clearedRows)
                    return clearedRows;
            }
        }
        if (!allowSpill)
            return 0;
        rowidx_t spiltRows = rightCollector->spill(localGathered); // passes localGathered, if true, can clear row array too
        ActPrintLog("clearNonLocalRows spilt %" RIPF "u rows, arrayCleared=%s", spiltRows, localGathered?"true":"false");
        return spiltRows;
    }
    void checkSmartMemException(IException *e)
    {
        if (!isSmart())
            throw e;
        switch (e->errorCode())
        {
        case ROXIEMM_MEMORY_POOL_EXHAUSTED:
        case ROXIEMM_MEMORY_LIMIT_EXCEEDED:
            break;
        default:
            throw e;
        }
    }
    bool setupHT(rowidx_t size)
    {
        if (size < 10)
            size = 16;
        else
        {
//            rowcount_t res = size/3*4; // make HT 1/3 bigger than # rows
            double htFactor = ((float)getOptInt("htperc", 133)) / 100.0;
            rowcount_t res = size*htFactor;
            rowcount_t roundedRes = 1;
            while (roundedRes < res)
                roundedRes <<= 1;
            ActPrintLog("htFactor = %f, size=%u, htsize=%" I64F "u, roundedRes=%" I64F "u", htFactor, size, res, roundedRes);
            res = roundedRes;
            if ((res < size) || (res > RIMAX)) // check for overflow, or result bigger than rowidx_t size
                throw MakeActivityException(this, 0, "Too many rows on RHS for hash table: %" RCPF "d", res);
            size = (rowidx_t)res;
        }
        try
        {
            table->setup(rightRowManager, size, leftHash, rightHash, compareLeftRight);
        }
        catch (IException *e)
        {
            checkSmartMemException(e);
            e->Release();
            return false;
        }
        rhsTableLen = size;
        return true;
    }
    void load(IRowStream *stream, IRowWriter *writer, bool grouped)
    {
        while (!abortSoon)
        {
            const void *next = stream->nextRow();
            if (!next)
            {
                if (!grouped)
                    break;
                else
                {
                    next = stream->nextRow();
                    if (!next)
                        break;
                }
            }
            writer->putRow(next);
        }
    }
    virtual unsigned queryChannelDestination(const void *row) override
    {
        unsigned hv = rightHash->hash(row);
        unsigned slaveDst = hv % numSlaves;
        return slaveDst / numNodes;
    }
    bool prepareLocalHT(CMarker &marker)
    {
        CCycleTimer timer;
        try
        {
            if (!marker.init(rightCollector->numRows(), queryRowManager()))
                return false;
        }
        catch (IException *e)
        {
            checkSmartMemException(e);
            e->Release();
            return false;
        }

        rowidx_t uniqueKeys = 0;
        {
            CThorArrayLockBlock b(*rightCollector);
            if (rightCollector->hasSpilt())
                return false;
            /* transfer rows out of collector to perform calc, but we'll keep lock,
             * so that a request to spill, will block delay, but can still proceed after calculate is done
             */
            CThorExpandingRowArray temp(*this, sharedRightRowInterfaces);
            rightCollector->transferRowsOut(temp, false);
            CCycleTimer timer;
            uniqueKeys = marker.calculate(temp, compareRight, false);
            VStringBuffer msg("CMarker::calculate - uniqueKeys=%" RIPF "u", uniqueKeys);
            logTiming(msg, timer.elapsedCycles());
            rightCollector->transferRowsIn(temp);
        }
        if (!setupHT(uniqueKeys)) // could cause spilling
        {
            if (!isSmart())
                throw MakeActivityException(this, 0, "Failed to allocate [LOCAL] hash table");
            return false;
        }
        logTiming("prepareLocalHT", timer.elapsedCycles());
        return true;
    }
    /*
     * handleGlobalRHS() attempts to broadcast and gather RHS rows and setup HT on channel 0
     * Checks at various stages if spilt and bails out.
     * Side effect of setting 'rhsCollated' based on ch0 value on all channels
     * and setting setFailoverToLocal(true) if fails over.
     */
    bool handleGlobalRHS(CMarker &marker, bool stopping)
    {
        addRHSRowTime = 0;
        CCycleTimer timer;
        doBroadcastRHS(stopping);
        logTiming("doBroadcastRHS", timer.elapsedCycles());
        postBroadcast = true;
        if (!hasFailedOverToLocal())
        {
            localGathered = true;

            rowidx_t rhsRows = rightCollector->numRows();
            bool success=false;
            try
            {
                if (marker.init(rhsRows, rightRowManager)) // May fail if insufficient memory available
                    success = true;
            }
            catch (IException *e)
            {
                checkSmartMemException(e);
                e->Release();
            }
            if (!success)
            {
                ActPrintLog("Out of memory trying to size the global RHS row table for a SMART join, will now attempt a distributed local lookup join");
                if (!hasFailedOverToLocal())
                {
                    // NB: someone else could have provoked callback already
                    clearNonLocalRows("OOM on sizing global row table", false); // NB: triggers on all channels
                    dbgassertex(hasFailedOverToLocal());
                }
            }
            // For testing purposes only
            if (isSmart() && !hasFailedOverToLocal() && getOptBool(THOROPT_LKJOIN_LOCALFAILOVER, getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER)))
                clearNonLocalRows("testing", false);
        }
        rowidx_t uniqueKeys = 0;
        {
            /* NB: This does not allocate/will not provoke spilling, but spilling callback still active
             * and need to protect access
             */
            CThorArrayLockBlock b(*rightCollector);
            if (!hasFailedOverToLocal())
            {
                CThorExpandingRowArray channelRows(*this, sharedRightRowInterfaces);
                rightCollector->transferRowsOut(channelRows, false);
                CCycleTimer timer;
                uniqueKeys = marker.calculate(channelRows, compareRight, true);
                VStringBuffer msg("CMarker::calculate - uniqueKeys=%" RIPF "u", uniqueKeys);
                logTiming(msg, timer.elapsedCycles());
                rightCollector->transferRowsIn(channelRows);
                localGatheredAndSorted = true;
            }
        }
        if (!hasFailedOverToLocal()) // check again after processing above
        {
            if (!setupHT(uniqueKeys)) // NB: Sizing can cause spilling callback to be triggered or OOM in case of !smart
            {
                ActPrintLog("Out of memory trying to size the global hash table for a SMART join, will now attempt a distributed local lookup join");
                clearNonLocalRows("OOM on sizing global hash table", false); // NB: setupHT should have provoked callback already
                dbgassertex(hasFailedOverToLocal());
            }
        }
        if (isSmart())
        {
            /* NB: Potentially one of the slaves spilt late after broadcast and rowprocessor finished, in final phase above
             * So remove callback, sync channels and check again, then broadcast state to everyone.
             */

            // don't re-sort (if there's a spill event) if already gathered and sorted (during calculate above)
            ICompare *cmp = localGatheredAndSorted ? nullptr : compareRight;
            rightCollector->setup(cmp, stableSort_none, rc_mixed, SPILL_PRIORITY_DISABLE);

            InterChannelBarrier(); // keep window informing other nodes of state to a minimum
            {
                ActPrintLog("Blocking rightCollector and synching channels to determine global spill state");
                CThorArrayLockBlock b(*rightCollector);
                ActPrintLog("synching channels to determine global spill state");
                InterChannelBarrier(); // Intentional, all channels blocking spill handling whilst communicating if all succeeded without spilling
                ActPrintLog("All channels now blocking rightCollector");
                if (0 == queryJobChannelNumber()) // All channels broadcast, but only channel 0 receives
                {
                    ActPrintLog("Broadcasting final spilt status: %s", hasFailedOverToLocal() ? "spilt" : "did not spill");
                    // NB: Will cause other slaves to flush non-local if any have and failedOverToLocal will be set on all

                    CCycleTimer timer;
                    doBroadcastStop(broadcast2MpTag, hasFailedOverToLocal() ? bcastflag_spilt : bcastflag_null);
                    logTiming("doBroadcastStop()", timer.elapsedCycles());
                    ActPrintLog("Done broadcasting final spilt status: %s", hasFailedOverToLocal() ? "spilt" : "did not spill");
                }
                InterChannelBarrier();

                // Before releasing the lock mark allInMemory=true, which will make any clearNonLocalRows callback in progress a NOP
                if (!hasFailedOverToLocal()) // NB: all channels will have marked failover if any node sent bcastflag_spilt
                    allInMemory = true;
            }
        }

        logTiming("addRHSRowTime", addRHSRowTime);

        return allInMemory;
    }
    /*
     * NB: if global attempt fails.
     */
    void handleFailoverToLocalRHS()
    {
        Owned<IRowWriter> writer = rightCollector->getWriter();

        CCycleTimer timer;
        Owned<IRowStream> rightDistStream = rhsDistributor->connect(queryRowInterfaces(rightITDL), right.getClear(), rightHash, NULL);
        load(rightDistStream, writer, false);
        logTiming("load right distribute time", timer.elapsedCycles());

        if (getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER)) // for testing only (force to disk, as if spilt)
            rightCollector->spill(false);
    }
    void setupStandardJoin(IRowStream *right)
    {
        // NB: lhs ordering and grouping lost from here on.. (will have been caught earlier if global)
        if (grouped)
            throw MakeActivityException(this, 0, "Degraded to standard join, LHS order cannot be preserved");

        Owned<IThorRowLoader> rowLoader = createThorRowLoader(*this, queryRowInterfaces(leftITDL), helper->isLeftAlreadyLocallySorted() ? NULL : compareLeft);
        left.setown(rowLoader->load(left, abortSoon, false));
        leftITDL = queryInput(0); // reset
        ActPrintLog("LHS loaded/sorted");

        // rightStream is sorted
        // so now going to do a std. join on distributed sorted streams
        switch(container.getKind())
        {
            case TAKlookupjoin:
            case TAKsmartjoin:
            {
                bool hintunsortedoutput = getOptBool(THOROPT_UNSORTED_OUTPUT, TAKsmartjoin == container.getKind());
                bool hintparallelmatch = getOptBool(THOROPT_PARALLEL_MATCH, hintunsortedoutput); // i.e. unsorted, implies use parallel by default, otherwise no point
                joinHelper.setown(createJoinHelper(*this, helper, this, hintparallelmatch, hintunsortedoutput));
                break;
            }
            case TAKlookupdenormalize:
            case TAKlookupdenormalizegroup:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
                joinHelper.setown(createDenormalizeHelper(*this, helper, this));
                break;
            default:
                throwUnexpected();
        }
        joinHelper->init(left, right, leftAllocator, rightAllocator, ::queryRowMetaData(leftITDL));
    }
    void installOOMCallback()
    {
        if (!oomCallbackInstalled)
        {
            oomCallbackInstalled = true;
            rightRowManager->addRowBuffer(this);
        }
    }
    void removeOOMCallback()
    {
        if (oomCallbackInstalled)
        {
            oomCallbackInstalled = false;
            rightRowManager->removeRowBuffer(this);
        }
    }
    void getRHS(bool stopping)
    {
        if (gotRHS())
            return;
        lkjState = lkj_gotrhs;

        /* Global Lookup:
         * ==============
         * See : handleGlobalRHS()
         * Distributes RHS (using broadcaster) all channels broadcast, but only channel 0 receives/collates.
         * Whilst broadcasting, rows are kept in separate row arrays for each slave.
         * The separate slave row arrays are combined into a single RHS table and a HT is built.
         * If an OOM occurs, it fails over to local lookup 'Failover Local Lookup' (see below)
         * If succeeds, shares HT between channels and each channel works independently, streaming LHS to match RHS HT table.
         *
         * Failover Local Lookup:
         * ======================
         * On OOM event, all non-local (that don't hash partition to local node) are purged (see clearNonLocalRows())
         * See : handleGlobalRHS()
         * Row arrays are compacted.
         * A spillable stream of all local row arrays is formed (see getGatheredRHSStream())
         * The collected local RHS rows are partitioned with channel distributor - splitting them into channels.
         * Remaining non-local nodes are read from global RHS distributor and partitioned by channel distributor also.
         * LHS is co-distributed, i.e. to channels.
         * If an OOM occurs during this partitioning, it fails over to 'Failover Standard Join' (see below)
         * If succeeds, falls through to Local Lookup Handling
         *
         * Local Lookup
         * ============
         * Gathers local dataset into RHS table.
         * If succeeds proceeds to 'Local Lookup Handling'
         *
         * Local Lookup Handling
         * =====================
         * Prepares HT based on local channel rows
         * IF OOM's fails over to 'Failover Standard Join' (see below)
         * If succeeds, adds channel rows to HT (in global case channels >0 share table and HT)
         *
         * Failover Standard Join:
         * =======================
         * The LHS side is loaded and spilt and sorted if necessary
         * A regular join helper is created to perform a local join against the two hash distributed sorted sides.
         */

        try
        {
            if (isSmart())
                installOOMCallback();

            CMarker marker(*this);
            Owned<IRowStream> rightStream;
            if (isGlobal())
            {
                /* All slaves on all channels now know whether any one spilt or not, i.e. whether to perform local hash join or not
                 * If any have spilt, still need to distribute rest of RHS..
                 */
                CCycleTimer timer;
                bool ok = handleGlobalRHS(marker, stopping);
                logTiming("handleGlobalRHS", timer.elapsedCycles());
                if (stopping)
                {
                    ActPrintLog("Global getRHS stopped");
                    return;
                }
                if (ok)
                    ActPrintLog("RHS global rows fitted in memory in this channel, my channel count: %" RCPF "u, totalCount = %" RIPF "u", rightCollector->numRows(), getGlobalRHSTotal());
                else
                {
                    ActPrintLog("Some nodes Spilt whilst broadcasting or preparing HT. Will attempt distributed local lookup join");

                    // NB: lhs ordering and grouping lost from here on..
                    if (grouped)
                        throw MakeActivityException(this, 0, "Degraded to Distributed Local Lookup, but input is marked as grouped and cannot preserve LHS order");

                    // If HT sized already and now spilt, it's too big. Clear for re-use by handleLocalRHS()
                    clearHT();
                    marker.reset();

                    CCycleTimer timer;
                    handleFailoverToLocalRHS();
                    localGathered = true;
                    logTiming("handlFailoverToLocalRHS", timer.elapsedCycles());
                    if (rightCollector->hasSpilt())
                    {
                        ActPrintLog("Global SMART JOIN spilt to disk during Distributed Local Lookup handling. Failing over to Standard Join");
                        rightStream.setown(rightCollector->getStream());
                        setFailoverToStandard(true);
                    }

                    // start LHS distributor, needed by local lookup or full join
                    CCycleTimer timer2;
                    left.setown(lhsDistributor->connect(queryRowInterfaces(leftITDL), left.getClear(), leftHash, NULL));

                    // NB: Some channels in this or other slave processes may have fallen over to hash join
                }
            }
            else
            {
                if (stopping)
                {
                    ActPrintLog("Local getRHS stopped");
                    return;
                }
                Owned<IRowWriter> writer = rightCollector->getWriter();
                load(right, writer, true); // right may be grouped
                if (rightCollector->hasSpilt())
                {
                    rightStream.setown(rightCollector->getStream());
                    ActPrintLog("Local SMART JOIN spilt to disk. Failing over to regular local join");
                    setFailoverToStandard(true);
                }
                else
                    ActPrintLog("RHS local rows fitted in memory in this channel, count: %" RCPF "u", rightCollector->numRows());
            }
            if (!rightStream)
            {
                /* All RHS rows fitted in memory
                 * NB: rightCollector callback only enabled if local.
                 */
                if (isLocal() || hasFailedOverToLocal())
                {
                    ActPrintLog("Preparing [LOCAL] hashtable for a SMART join (%" RCPF "u rows)", rightCollector->numRows());
                    if (hasFailedOverToLocal())
                        marker.reset();
                    if (prepareLocalHT(marker))
                    {
                        rightStream.setown(rightCollector->getStream(false, &rhs));
                        if (!rightStream)
                            totalRHSCount = rhs.ordinality();
                        else
                            ActPrintLog("Preparing [LOCAL] hashtable succeeded, but caused spill, will now failover to a std hash join");
                    }
                    else
                    {
                        ActPrintLog("Out of memory trying to prepare [LOCAL] hashtable for a SMART join (%" RCPF "u rows), will now failover to a std hash join", rightCollector->numRows());
                        rightStream.setown(rightCollector->getStream());
                    }
                }
                else
                {
                    totalRHSCount = getGlobalRHSTotal();
                    rightStream.setown(rightCollector->getStream(false, &rhs));
                    dbgassertex(!rightStream);
                }
            }
            if (rightStream)
            {
                marker.reset();
                ActPrintLog("Performing STANDARD JOIN");
                setupStandardJoin(rightStream); // NB: rightStream is sorted
                lkjState = lkj_stdjoin;
            }
            else
            {
                // NB: No spilling here on in
                if (isSmart())
                    removeOOMCallback();
                {
                    CCycleTimer timer;
                    table->addRows(rhs, &marker);
                    logTiming("addHTTime", timer.elapsedCycles());
                }

                // NB: No spilling from here on in
                if (isLocal() || hasFailedOverToLocal())
                {
                    ActPrintLog("Performing LOCAL LOOKUP JOIN: rhs size=%u, lookup table size = %" RIPF "u", rhs.ordinality(), rhsTableLen);
                    tableProxy.set(table);
                }
                else
                {
                    ActPrintLog("Performing GLOBAL LOOKUP JOIN: rhs size=%u, my channel lookup table size = %" RIPF "u", rhs.ordinality(), rhsTableLen);
                    InterChannelBarrier(); // wait for all channels to prep. their table

                    tableProxy.setown(createGlobalProxy());
                }
                lkjState = lkj_locallookup;
            }
        }
        catch (IException *e)
        {
            if (!isOOMException(e))
                throw e;
            IOutputMetaData *inputOutputMeta = rightITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
            // rows may either be in separate slave row arrays or in single rhs array, or split.
            rowcount_t total = isGlobal() ? getGlobalRHSTotal() : rightCollector->numRows();
            throw checkAndCreateOOMContextException(this, e, "gathering RHS rows for lookup join", total, inputOutputMeta, NULL);
        }
    }
// pity these are identical in both All and Lookup varieties, but helper doesn't have a common base
    virtual bool match(const void *lhs, const void *rhsrow) override
    {
        return helper->match(lhs, rhsrow);
    }
    virtual const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned numRows, const void **rows) override
    {
        return helper->transform(rowBuilder, left, right, numRows, rows);
    }
    virtual const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned count) override
    {
        return helper->transform(rowBuilder, left, right, count);
    }
public:
    static bool needDedup(IHThorHashJoinArg *helper)
    {
        unsigned flags = helper->getJoinFlags();
        bool dedup = false;
        if (0 == (flags & (JFtransformMaySkip|JFmatchrequired)))
        {
            if (0 == (JFmanylookup & flags))
                dedup = true;
            else
            {
                unsigned keepLimit = helper->getKeepLimit();
                unsigned abortLimit = helper->getMatchAbortLimit();
                unsigned atMost = helper->getJoinLimit();
                dedup = (1==keepLimit) && (0==atMost) && (0==abortLimit);
            }
        }
        return dedup;
    }
    CLookupJoinActivityBase(CGraphElementBase *_container) : PARENT(_container)
    {
        helper = static_cast <IHThorHashJoinArg *> (queryHelper());

        rhsCompacted = false;
        broadcast2MpTag = broadcast3MpTag = lhsDistributeTag = rhsDistributeTag = TAG_NULL;
        setFailoverToLocal(false);
        setFailoverToStandard(false);
        leftHash = helper->queryHashLeft();
        rightHash = helper->queryHashRight();
        compareRight = helper->queryCompareRight();
        compareLeft = helper->queryCompareLeft();
        compareLeftRight = helper->queryCompareLeftRight();
        stable = 0 == (JFunstable & flags);
        dedup = false;
        abortLimit = helper->getMatchAbortLimit();
        atMost = helper->getJoinLimit();
        if (0 == abortLimit)
            abortLimit = (unsigned)-1;
        if (0 == atMost)
            atMost = (unsigned)-1;
        if (abortLimit < atMost)
            atMost = abortLimit;
        oomCallbackInstalled = false;

        switch (container.getKind())
        {
            case TAKsmartjoin:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
                smart = true;
                break;
            default:
                smart = false;
                break;
        }
        spillCompInfo = 0x0;
        if (getOptBool(THOROPT_COMPRESS_SPILLS, true))
        {
            StringBuffer compType;
            getOpt(THOROPT_COMPRESS_SPILL_TYPE, compType);
            setCompFlag(compType, spillCompInfo);
        }
        ActPrintLog("Smart join = %s", smart?"true":"false");
    }
    bool exceedsLimit(rowidx_t count, const void *left, const void *right, const void *&failRow)
    {
        if ((unsigned)-1 != atMost)
        {
            failRow = NULL;
            if (count>abortLimit)
            {
                if (0 == (JFmatchAbortLimitSkips & flags))
                {
                    Owned<IException> e;
                    try
                    {
                        if (helper)
                            helper->onMatchAbortLimitExceeded();
                        CommonXmlWriter xmlwrite(0);
                        if (outputMeta && outputMeta->hasXML())
                            outputMeta->toXML((const byte *) leftRow.get(), xmlwrite);
                        throw MakeActivityException(this, 0, "More than %d match candidates in join for row %s", abortLimit, xmlwrite.str());
                    }
                    catch (IException *_e)
                    {
                        if (0 == (JFonfail & flags))
                            throw;
                        e.setown(_e);
                    }
                    RtlDynamicRowBuilder ret(allocator);
                    size32_t transformedSize = helper->onFailTransform(ret, leftRow, defaultRight, e.get());
                    if (transformedSize)
                        failRow = ret.finalizeRowClear(transformedSize);
                }
                else
                    leftMatch = true; // there was a lhs match, even though rhs group exceeded limit. Therefore this lhs will not be considered left only/left outer
                return true;
            }
            else if (count>atMost)
                return true;
        }
        return false;
    }
    IHash *queryLeftHash() const { return leftHash; }

    virtual ITableLookup *createGlobalProxy() = 0;

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        PARENT::init(data, slaveData);

        ICompare *cmp = compareRight;
        if (isGlobal())
        {
            broadcast2MpTag = queryJobChannel().deserializeMPTag(data);
            broadcast3MpTag = queryJobChannel().deserializeMPTag(data);
            lhsDistributeTag = queryJobChannel().deserializeMPTag(data);
            rhsDistributeTag = queryJobChannel().deserializeMPTag(data);
            rhsDistributor.setown(createHashDistributor(this, queryJobChannel().queryJobComm(), rhsDistributeTag, false, NULL, "RHS"));
            lhsDistributor.setown(createHashDistributor(this, queryJobChannel().queryJobComm(), lhsDistributeTag, false, NULL, "LHS"));
            dbgassertex(!stable);

            /* JCS->GH - it used to be the case that a global standard LOOKUP (not SMART), stable order was implemented.
             * Now as rows are coming in a async manner into channel from different slaves, the order will be non-determinitic
             * How much of a problem is that??
             */
        }
        if (isSmart())
            rightCollector.setown(createThorRowCollector(*this, sharedRightRowInterfaces, cmp, stableSort_none, rc_mixed, SPILL_PRIORITY_DISABLE));
        else
            rightCollector.setown(createThorRowCollector(*this, sharedRightRowInterfaces, compareRight, stable ? stableSort_lateAlloc : stableSort_none, rc_allMem, SPILL_PRIORITY_DISABLE));
    }
    virtual void start() override
    {
        ActivityTimer s(totalCycles, timeActivities);
        PARENT::start();
        localGathered = false;
        postBroadcast = false;
        localGatheredAndSorted = false;
        allInMemory = false;
        totalRHSCount = 0;
        if (isSmart())
        {
            if (isGlobal())
            {
                setFailoverToLocal(false);
                rhsCompacted = false;
            }
            setFailoverToStandard(false);
        }
        else
        {
            bool inputGrouped = leftITDL->isGrouped();
            dbgassertex(inputGrouped == grouped); // std. lookup join expects these to match
        }
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        const void *row;
lkjStateSwitch:
        switch (lkjState)
        {
            case lkj_start:
            {
                getRHS(false);
                StringBuffer msg;
                if (isSmart())
                {
                    msg.append("SmartJoin - ");
                    if (hasFailedOverToStandard())
                        msg.append("Failed over to standard join");
                    else if (isGlobal() && hasFailedOverToLocal())
                        msg.append("Failed over to hash distributed local lookup join");
                    else
                        msg.append("All in memory lookup join");
                }
                else
                    msg.append("LookupJoin");
                ActPrintLog("%s", msg.str());
                goto lkjStateSwitch;
            }
            case lkj_stdjoin:
            {
                row = joinHelper->nextRow();
                break;
            }
            case lkj_locallookup:
                row = lookupNextRow();
                break;
            default:
                throwUnexpected();
        }
        if (!row)
            return nullptr;
        dataLinkIncrement();
        return row;
    }
    virtual void abort() override
    {
        PARENT::abort();
        if (rhsDistributor)
            rhsDistributor->abort();
        if (lhsDistributor)
            lhsDistributor->abort();
        if (joinHelper)
            joinHelper->stop();
    }
    virtual void stop() override
    {
        if (isGlobal())
        {
            if (gotRHS())
            {
                // Other channels using HT's. So do not reset until all here
                if (!hasFailedOverToLocal() && queryJob().queryJobChannels()>1)
                    InterChannelBarrier();
            }
            else
                getRHS(true); // If global, need to handle RHS until all are slaves stop
            if (isSmart())
                removeOOMCallback();
        }

        if (rhsDistributor)
        {
            rhsDistributor->disconnect(true);
            rhsDistributor->join();
        }
        if (lhsDistributor)
        {
            lhsDistributor->disconnect(true);
            lhsDistributor->join();
        }
        joinHelper.clear();
        PARENT::stop();
    }
    virtual bool isGrouped() const override
    {
        return isSmart() ? false : queryInput(0)->isGrouped();
    }
    virtual void bCastReceive(CSendItem *sendItem, bool stop) // NB: only called on channel 0
    {
        if (sendItem)
        {
            if (0 != (sendItem->queryFlags() & bcastflag_spilt))
            {
                VStringBuffer msg("Notification that slave %d spilt", sendItem->querySlave());
                clearNonLocalRows(msg.str(), false); // NB: triggers on all channels
            }
        }
        PARENT::bCastReceive(sendItem, stop);
    }
// IBufferedRowCallback
    virtual unsigned getSpillCost() const
    {
        return SPILL_PRIORITY_LOOKUPJOIN;
    }
    virtual unsigned getActivityId() const
    {
        return this->queryActivityId();
    }
    virtual bool freeBufferedRows(bool critical)
    {
        if (isGlobal())
            return clearNonLocalRows("Out of memory callback", true) > 0;
        else
            return rightCollector->spill(false) > 0;
    }
    rowidx_t keepLocal(CThorExpandingRowArray &rows, CThorExpandingRowArray &localRows)
    {
        ForEachItemIn(r, rows)
        {
            unsigned hv = rightHash->hash(rows.query(r));
            if (myNodeNum == (hv % numNodes))
                localRows.append(rows.getClear(r));
        }
        rows.clearRows();
        return localRows.ordinality();
    }
    virtual bool addRHSRows(CThorExpandingRowArray &inRows, CThorExpandingRowArray &rhsInRowsTemp)
    {
        assertex(0 == rhsInRowsTemp.ordinality());

        CAtomicCycleAddTimer tb(addRHSRowTime);
        if (hasFailedOverToLocal())
        {
            if (0 == keepLocal(inRows, rhsInRowsTemp))
                return true;
        }
        CriticalBlock b(rhsRowLock);
        // NB: If PARENT::addRHSRows fails, it will cause clearNonLocalRows() to have been triggered and failedOverToLocal to be set
        if (!hasFailedOverToLocal())
        {
            if (rightCollector->addRows(inRows, true))
                return true;
        }
        dbgassertex(hasFailedOverToLocal());
        if (0 == rhsInRowsTemp.ordinality()) // i.e. not done outside crit above.
        {
            /* Tried to do outside crit above, but hasFailedOverToLocal() could be true now, since gaining lock
             * Will be one off if happens at all.
             */
            if (0 == keepLocal(inRows, rhsInRowsTemp))
                return true;
        }
        /* Could cause rightCollector to spill
         * But it will be last few rows being added if that's the case, broadcaster will be stopping.
         */
        return rightCollector->addRows(rhsInRowsTemp, true);
    }
};

class CHTBase : public CTableCommon
{
protected:
    CLookupJoinActivityBase *activity;
    OwnedConstThorRow htMemory;
    IHash *leftHash, *rightHash;
    ICompare *compareLeftRight;
    size32_t elementSize = sizeof(const void *);

    void _reset()
    {
        htMemory.clear();
        leftHash = rightHash = NULL;
        compareLeftRight = NULL;
    }
public:
    CHTBase(CLookupJoinActivityBase *_activity) : activity(_activity)
    {
        _reset();
    }
    virtual void setup(roxiemem::IRowManager *rowManager, rowidx_t size, IHash *_leftHash, IHash *_rightHash, ICompare *_compareLeftRight) override
    {
        unsigned __int64 _sz = elementSize * ((unsigned __int64)size);
        memsize_t sz = (memsize_t)_sz;
        if (sz != _sz) // treat as OOM exception for handling purposes.
            throw MakeStringException(ROXIEMM_MEMORY_LIMIT_EXCEEDED, "Unsigned overflow, trying to allocate hash table of size: %" I64F "d ", _sz);
        void *ht = rowManager->allocate(sz, activity->queryContainer().queryId(), SPILL_PRIORITY_LOW);
        memset(ht, 0, sz);
        htMemory.setown(ht);
        tableSize = size;
        tableMask = size-1; // size if always a power of 2
        leftHash = _leftHash;
        rightHash = _rightHash;
        compareLeftRight = _compareLeftRight;
    }
    virtual void reset() override
    {
        CTableCommon::reset();
        _reset();
    }
};

class CTableGlobalBase : public CSimpleInterfaceOf<ITableLookup>
{
protected:
    CInMemJoinBase *activity = nullptr;
    unsigned numChannels = 0;
    unsigned myChannel = 0;
    unsigned numNodes = 0;
    unsigned numSlaves = 0;
    OwnedMalloc<ITableLookup *> hTables;
    unsigned currentTableChannel = NotFound;

public:
    CTableGlobalBase(CInMemJoinBase *_activity) : activity(_activity)
    {
        numNodes = activity->queryJob().queryNodes();
        numSlaves = activity->queryJob().querySlaves();
        myChannel = activity->queryJobChannelNumber();
        numChannels = activity->queryJob().queryJobChannels();
        hTables.allocateN(numChannels, true);
        for (unsigned ch=0; ch<numChannels; ch++)
        {
            hTables[ch] = activity->queryChannel(ch)->queryTable();
        }
    }
};

class CLookupHT : public CHTBase
{
    typedef CHTBase PARENT;
    const void **ht;

    void releaseHTRows()
    {
        roxiemem::ReleaseRoxieRowArray(tableSize, ht);
    }
    inline void addEntry(const void *row, unsigned hash)
    {
        loop
        {
            const void *&htRow = ht[hash];
            if (!htRow)
            {
                LinkThorRow(row);
                htRow = row;
                break;
            }
            hash++;
            if (hash>=tableSize)
                hash = 0;
        }
    }
    void _reset()
    {
        releaseHTRows();
        ht = nullptr;
    }
public:
    CLookupHT(CLookupJoinActivityBase *activity) : PARENT(activity)
    {
        _reset();
    }
    ~CLookupHT()
    {
        releaseHTRows();
    }
    const void *findFirst(unsigned hv, const void *left)
    {
        unsigned h = hv%tableSize;
        loop
        {
            const void *right = ht[h];
            if (!right)
                break;
            if (0 == compareLeftRight->docompare(left, right))
                return right;
            h++;
            if (h>=tableSize)
                h = 0;
        }
        return NULL;
    }
    virtual void setup(roxiemem::IRowManager *rowManager, rowidx_t size, IHash *leftHash, IHash *rightHash, ICompare *compareLeftRight) override
    {
        CHTBase::setup(rowManager, size, leftHash, rightHash, compareLeftRight);
        ht = (const void **)htMemory.get();
    }
    virtual void reset() override
    {
        releaseHTRows();
        _reset();
    }
    virtual void addRows(CThorExpandingRowArray &_rows, CMarker *marker) override
    {
        const void **rows = _rows.getRowArray();
        rowidx_t pos=0;
        loop
        {
            rowidx_t nextPos = marker->findNextBoundary(pos);
            if (0 == nextPos)
                break;
            const void *row = rows[pos];
            unsigned h = rightHash->hash(row)%tableSize;
            addEntry(row, h);
            pos = nextPos;
        }
        // Rows now in hash table, rhs arrays no longer needed
        _rows.kill();
        marker->reset();
    }
// ITableLookup
    virtual const void *getNextRHS(HtEntry &currentHashEntry __attribute__((unused))) override
    {
        return nullptr; // no next in LOOKUP without MANY
    }
    virtual const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry __attribute__((unused))) override
    {
        failRow = nullptr;
        unsigned hv = leftHash->hash(leftRow);
        return findFirst(hv, leftRow);
    }
};

class CLookupHTGlobal : public CTableGlobalBase
{
    typedef CTableGlobalBase PARENT;

    IHash *leftHash = nullptr;

public:
    CLookupHTGlobal(CLookupJoinActivityBase *_activity) : PARENT(_activity)
    {
        leftHash = _activity->queryLeftHash();
    }
    virtual const void *getNextRHS(HtEntry &currentHashEntry __attribute__((unused))) override
    {
        return nullptr; // no next in LOOKUP without MANY
    }
    virtual const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry __attribute__((unused))) override
    {
        failRow = nullptr;
        unsigned hv = leftHash->hash(leftRow);
        unsigned s = hv % numSlaves;
        unsigned c = s / numNodes;
        const void *ret = ((CLookupHT *)hTables[c])->findFirst(hv, leftRow);
        return ret;
    }
};


class CLookupJoinSlaveActivity : public CLookupJoinActivityBase
{
    typedef CLookupJoinActivityBase PARENT;

public:
    CLookupJoinSlaveActivity(CGraphElementBase *_container) : PARENT(_container)
    {
        dedup = true;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        PARENT::init(data, slaveData);
        table.setown(new CLookupHT(this));
    }
    virtual ITableLookup *createGlobalProxy() override
    {
        return new CLookupHTGlobal(this);
    }
};

class CLookupManyHT : public CHTBase
{
    typedef CHTBase PARENT;

#ifdef HIGHFREQ_TIMING
    unsigned __int64 misses = 0, lookupMisses = 0;
    CCycleTimer timer, timer2;
    cycle_t findFirstTime = 0;
    cycle_t findFirstCompareTime = 0;
#endif

    HtEntry *ht = nullptr;
    const void **rows = nullptr;

    inline const void *lookup(const void *left, unsigned hash, HtEntry &currentHashEntry)
    {
        unsigned h = hash & tableMask;
        loop
        {
            HtEntry &e = ht[h];
            if (0 == e.count)
                return nullptr;
            if (hash == e.hash)
            {
                const void *right = rows[e.index];
                ADDHFTIMERRESET(timer2);
                bool res = (0 == compareLeftRight->docompare(left, right));
                ADDHFTIME(findFirstCompareTime, timer2.elapsedCycles());
                if (res)
                {
                    currentHashEntry = e;
                    ADDHFTIME(findFirstTime, timer.elapsedCycles());
                    return right;
                }
            }
            h++;
            if (h >= tableSize)
                h = 0;
        }
    }
    inline void addEntry(const void *row, unsigned hash, rowidx_t index, rowidx_t count)
    {
        unsigned h = hash & tableMask;
        loop
        {
            HtEntry &e = ht[h];
            if (!e.count)
            {
                e.index = index;
                e.count = count;
                e.hash = hash;
                break;
            }
            h++;
            if (h >= tableSize)
                h = 0;
        }
    }
    void _reset()
    {
        ht = NULL;
        rows = NULL;
    }
public:
    CLookupManyHT(CLookupJoinActivityBase *_activity) : PARENT(_activity)
    {
        elementSize = sizeof(HtEntry);
        _reset();
    }
    ~CLookupManyHT()
    {
#ifdef HIGHFREQ_TIMING
        activity->logTiming("findFirstTime", findFirstTime);
        VStringBuffer msg("lookupMisses = %" I64F "u, findFirstCompareTime:", lookupMisses);
        activity->logTiming(msg, findFirstCompareTime);
#endif
    }
    const void *findFirst(unsigned hash, const void *left, HtEntry &currentHashEntry)
    {
        ADDHFTIMERRESET(timer);
        const void *right = lookup(left, hash, currentHashEntry);
        if (right)
            return right;
        INCHFSTATE(lookupMisses);
        ADDHFTIME(findFirstTime, timer.elapsedCycles());
        return nullptr;
    }
    virtual void setup(roxiemem::IRowManager *rowManager, rowidx_t size, IHash *leftHash, IHash *rightHash, ICompare *compareLeftRight) override
    {
        CHTBase::setup(rowManager, size, leftHash, rightHash, compareLeftRight);
        ht = (HtEntry *)htMemory.get();
    }
    virtual void reset() override
    {
        CHTBase::reset();
        _reset();
    }
    virtual void addRows(CThorExpandingRowArray &_rows, CMarker *marker) override
    {
#ifdef HIGHFREQ_TIMING
        cycle_t findNextBoundaryTime = 0, addRowsHashTime = 0, addEntryTime = 0;
        CCycleTimer timer;
#endif
        rows = _rows.getRowArray();
        rowidx_t pos=0;
        rowidx_t pos2;
        loop
        {
            ADDHFTIMERRESET(timer);
            pos2 = marker->findNextBoundary(pos);
            ADDHFTIME(findNextBoundaryTime, timer.elapsedCycles());
            if (0 == pos2)
                break;
            rowidx_t count = pos2-pos;
            /* JCS->GH - Could you/do you spot LOOKUP MANY, followed by DEDUP(key) ?
             * It feels like we should only dedup if code gen spots, rather than have LOOKUP without MANY option
             * i.e. feels like LOOKUP without MANY should be deprecated..
            */
            const void *row = rows[pos];
            ADDHFTIMERRESET(timer);
            unsigned hash = rightHash->hash(row);
            ADDHFTIME(addRowsHashTime, timer.elapsedCycles());
            // NB: 'pos' and 'count' won't be used if dedup variety
            ADDHFTIMERRESET(timer);
            addEntry(row, hash, pos, count);
            ADDHFTIME(addEntryTime, timer.elapsedCycles());
            pos = pos2;
        }
#ifdef HIGHFREQ_TIMING
        activity->logTiming("addRows boundaryTime", findNextBoundaryTime);
        activity->logTiming("addRows hashTime", addRowsHashTime);
        activity->logTiming("addRows addEntryTime", addEntryTime);
        activity->ActPrintLog("addRows rows = %u, add misses = %" I64F "u", _rows.ordinality(), misses);
#endif
    }
// ITableLookup
    virtual const void *getNextRHS(HtEntry &currentHashEntry) override
    {
        if (1 == currentHashEntry.count)
            return NULL;
        --currentHashEntry.count;
        return rows[++currentHashEntry.index];
    }
    virtual const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry) override
    {
        unsigned hv=leftHash->hash(leftRow);
        const void *right = findFirst(hv, leftRow, currentHashEntry);
        if (right)
        {
            if (activity->exceedsLimit(currentHashEntry.count, leftRow, right, failRow))
                return NULL;
        }
        return right;
    }
};

class CLookupManyHTGlobal : public CTableGlobalBase
{
protected:
    typedef CTableGlobalBase PARENT;

    CLookupJoinActivityBase *activity;
    IHash *leftHash = nullptr;
#ifdef HIGHFREQ_TIMING
    CCycleTimer timer;
    cycle_t getFirstRHSMatchTime = 0, getNextRHSTime = 0;
#endif

public:
    CLookupManyHTGlobal(CLookupJoinActivityBase *_activity) : PARENT(_activity), activity(_activity)
    {
        leftHash = activity->queryLeftHash();
    }
    ~CLookupManyHTGlobal()
    {
#ifdef HIGHFREQ_TIMING
        activity->logTiming("getFirstRHSMatchTime", getFirstRHSMatchTime);
        activity->logTiming("getNextRHSTime", getNextRHSTime);
#endif
    }
    virtual const void *getNextRHS(HtEntry &currentHashEntry) override
    {
        ADDHFTIMERRESET(timer);
        dbgassertex(NotFound != currentTableChannel);
        const void *ret = hTables[currentTableChannel]->getNextRHS(currentHashEntry);
        ADDHFTIME(getNextRHSTime, timer.elapsedCycles());
        return ret;
    }
    virtual const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry) override
    {
        ADDHFTIMERRESET(timer);
        unsigned hv = leftHash->hash(leftRow);
        unsigned c = (hv % numSlaves) / numNodes;
        const void *right = ((CLookupManyHT *)hTables[c])->findFirst(hv, leftRow, currentHashEntry);
        if (right)
        {
            currentTableChannel = c;
            if (activity->exceedsLimit(currentHashEntry.count, leftRow, right, failRow))
                return nullptr;
        }
        else
            currentTableChannel = NotFound;
        ADDHFTIME(getFirstRHSMatchTime, timer.elapsedCycles());
        return right;
    }
};

class CLookupManyHTGlobalMask : public CLookupManyHTGlobal
{
    typedef CLookupManyHTGlobal PARENT;
    unsigned slavesMask = 0;

public:
    CLookupManyHTGlobalMask(CLookupJoinActivityBase *_activity) : PARENT(_activity)
    {
        slavesMask = numSlaves-1;
        activity->ActPrintLog("Using numSlaves mask: %u", slavesMask);
    }
    virtual const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry) override
    {
        ADDHFTIMERRESET(timer);
        unsigned hv = leftHash->hash(leftRow);
        unsigned c = (hv & slavesMask) / numNodes;
        const void *right = ((CLookupManyHT *)hTables[c])->findFirst(hv, leftRow, currentHashEntry);
        if (right)
        {
            currentTableChannel = c;
            if (activity->exceedsLimit(currentHashEntry.count, leftRow, right, failRow))
                return nullptr;
        }
        else
            currentTableChannel = NotFound;
        ADDHFTIME(getFirstRHSMatchTime, timer.elapsedCycles());
        return right;
    }
};

class CLookupManyJoinSlaveActivity : public CLookupJoinActivityBase
{
    typedef CLookupJoinActivityBase PARENT;

public:
    CLookupManyJoinSlaveActivity(CGraphElementBase *_container) : CLookupJoinActivityBase(_container)
    {
        // NB: could not dedup if JFtransformMaySkip or JFmatchrequired, but returnMany not necessarily true
        returnMany = 0 != (JFmanylookup & flags);
    }
    virtual ITableLookup *createGlobalProxy() override
    {
        if (0 == (numSlaves & (numSlaves -1)))
            return new CLookupManyHTGlobalMask(this);
        else
            return new CLookupManyHTGlobal(this);
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        PARENT::init(data, slaveData);
        table.setown(new CLookupManyHT(this));
    }
};


class CAllTable : public CTableCommon
{
    const void **rows;
    rowidx_t nextRhsRow;

    void _reset()
    {
        rows = NULL;
        nextRhsRow = 0;
    }
public:
    CAllTable(CInMemJoinBase *activity)
    {
        _reset();
    }
    virtual void setup(roxiemem::IRowManager *rowManager, rowidx_t size, IHash *leftHash, IHash *rightHash, ICompare *compareLeftRight) override
    {
    }
    virtual void reset() override
    {
        CTableCommon::reset();
        _reset();
    }
    virtual void addRows(CThorExpandingRowArray &_rows, CMarker *marker __attribute__((unused))) override
    {
        tableSize = _rows.ordinality();
        rows = _rows.getRowArray();
        nextRhsRow = 0;
    }
// ITableLookup
    virtual const void *getNextRHS(HtEntry &currentHashEntry __attribute__((unused))) override
    {
        if (++nextRhsRow<tableSize)
            return rows[nextRhsRow];
        return NULL;
    }
    virtual const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry __attribute__((unused))) override
    {
        nextRhsRow = 0;
        failRow = NULL;
        return rows[0]; // guaranteed to be at least one row
    }
};

class CAllTableGlobal : public CTableGlobalBase
{
    typedef CTableGlobalBase PARENT;

public:
    CAllTableGlobal(CInMemJoinBase *activity) : PARENT(activity)
    {
    }
    virtual const void *getNextRHS(HtEntry &currentHashEntry __attribute__((unused))) override
    {
        loop
        {
            if (currentTableChannel == numChannels)
                return nullptr;
            const void *ret = hTables[currentTableChannel]->getNextRHS(currentHashEntry);
            if (ret)
                return ret;
            currentTableChannel++;
        }
    }
    virtual const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry __attribute__((unused))) override
    {
        currentTableChannel = 0;
        return hTables[0]->getFirstRHSMatch(leftRow, failRow, currentHashEntry);
    }
};

class CAllJoinSlaveActivity : public CInMemJoinBase
{
    typedef CInMemJoinBase PARENT;

    IHThorAllJoinArg *helper;

protected:
    void getRHS(bool stopping)
    {
        if (gotRHS())
            return;
        lkjState = lkj_gotrhs;

        try
        {
            // ALL join must fit into memory
            if (isGlobal())
            {
                doBroadcastRHS(stopping);
                if (stopping) // broadcast done and no-one spilt, this node can now stop
                    return;

                InterChannelBarrier(); // wait for all to setup rhs, since global helper will access all
                totalRHSCount = getGlobalRHSTotal();
                rightCollector->transferRowsOut(rhs, false);
                tableProxy.setown(new CAllTableGlobal(this));
           }
           else
           {
                if (stopping) // if local can stop now
                    return;
                totalRHSCount = rightCollector->numRows();
                // if input counts known, use to presize RHS table
                ThorDataLinkMetaInfo rightMeta;
                rightITDL->getMetaInfo(rightMeta);
                rowcount_t rhsTotalCount = RCUNSET;
                if (rightMeta.totalRowsMin == rightMeta.totalRowsMax)
                {
                    rhsTotalCount = rightMeta.totalRowsMax;
                    if (rhsTotalCount > RIMAX)
                        throw MakeActivityException(this, 0, "Too many rows on RHS for ALL join: %" RCPF "d", rhsTotalCount);
                    rhs.resize((rowidx_t)rhsTotalCount);
                }
                while (!abortSoon)
                {
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;
                    rhs.append(row.getClear());
                }
                if (rhsTotalCount != RCUNSET) // verify matches meta
                    assertex(rhs.ordinality() == rhsTotalCount);
                tableProxy.set(table);
            }
            rhsTableLen = rhs.ordinality();
            table->addRows(rhs, nullptr);
            ActPrintLog("channel rhs table: %d elements", rhsTableLen);
        }
        catch (IException *e)
        {
            if (!isOOMException(e))
                throw e;
            IOutputMetaData *inputOutputMeta = rightITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
            // rows may either be in separate slave row arrays or in single rhs array, or split.
            rowidx_t total = isGlobal() ? getGlobalRHSTotal() : rightCollector->numRows();
            throw checkAndCreateOOMContextException(this, e, "gathering RHS rows for lookup join", total, inputOutputMeta, NULL);
        }
    }
// pity these are identical in both All and Lookup varieties, but helper doesn't have a common base
    virtual bool match(const void *lhs, const void *rhsrow) override
    {
        return helper->match(lhs, rhsrow);
    }
    virtual const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned numRows, const void **rows) override
    {
        return helper->transform(rowBuilder, left, right, numRows, rows);
    }
    virtual const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned count) override
    {
        return helper->transform(rowBuilder, left, right, count);
    }
public:
    CAllJoinSlaveActivity(CGraphElementBase *_container) : PARENT(_container)
    {
        helper = static_cast <IHThorAllJoinArg *> (queryHelper());
        returnMany = true;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        PARENT::init(data, slaveData);
        rightCollector.setown(createThorRowCollector(*this, sharedRightRowInterfaces, nullptr, stableSort_none, rc_allMem, SPILL_PRIORITY_DISABLE));
        table.setown(new CAllTable(this));
    }

// IThorSlaveActivity overloaded methods
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        PARENT::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!gotRHS())
            getRHS(false);
        OwnedConstThorRow row = lookupNextRow();
        if (!row.get())
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void stop()
    {
        if (isGlobal())
        {
            if (gotRHS())
            {
                // Other channels sharing HT. So do not reset until all here
                if (queryJob().queryJobChannels()>1)
                    InterChannelBarrier();
            }
            else
                getRHS(true); // If global, need to handle RHS until all are slaves stop
        }
        PARENT::stop();
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
};


CActivityBase *createLookupJoinSlave(CGraphElementBase *container)
{
    IHThorHashJoinArg *helper = (IHThorHashJoinArg *)container->queryHelper();
    bool local = container->queryLocal() || (1 == container->queryJob().querySlaves());
    if (CLookupManyJoinSlaveActivity::needDedup(helper))
        return new CLookupJoinSlaveActivity(container);
    else
        return new CLookupManyJoinSlaveActivity(container);
}

CActivityBase *createAllJoinSlave(CGraphElementBase *container)
{
    return new CAllJoinSlaveActivity(container);
}


