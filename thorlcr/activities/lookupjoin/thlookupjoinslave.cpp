/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "javahash.hpp"
#include "javahash.tpp"
#include "thorport.hpp"
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
#define MAX_QUEUE_BLOCKS 5

enum broadcast_code { bcast_none, bcast_send, bcast_sendStopping, bcast_stop };
enum broadcast_flags { bcastflag_spilt=0x100 };
#define BROADCAST_CODE_MASK 0x00FF
#define BROADCAST_FLAG_MASK 0xFF00
class CSendItem : public CSimpleInterface
{
    CMessageBuffer msg;
    unsigned info;
    unsigned origin, headerLen;
public:
    CSendItem(broadcast_code _code, unsigned _origin) : info((unsigned)_code), origin(_origin)
    {
        msg.append(info);
        msg.append(origin);
        headerLen = msg.length();
    }
    CSendItem(CMessageBuffer &_msg)
    {
        msg.swapWith(_msg);
        msg.read(info);
        msg.read(origin);
        headerLen = msg.getPos();
    }
    unsigned length() const { return msg.length(); }
    void reset() { msg.setLength(headerLen); }
    CMessageBuffer &queryMsg() { return msg; }
    broadcast_code queryCode() const { return (broadcast_code)(info & BROADCAST_CODE_MASK); }
    unsigned queryOrigin() const { return origin; }
    broadcast_flags queryFlags() const { return (broadcast_flags)(info & BROADCAST_FLAG_MASK); }
    void setFlag(broadcast_flags _flag)
    {
        info = (info & ~BROADCAST_FLAG_MASK) | ((short)_flag);
        msg.writeDirect(0, sizeof(info), &info); // update
    }
};


interface IBCastReceive
{
    virtual void bCastReceive(CSendItem *sendItem) = 0;
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
    ICommunicator &comm;
    CActivityBase &activity;
    mptag_t mpTag;
    unsigned myNode, slaves;
    IBCastReceive *recvInterface;
    InterruptableSemaphore allDoneSem;
    CriticalSection allDoneLock, bcastOtherCrit;
    bool allDone, allDoneWaiting, allRequestStop, stopping, stopRecv;
    Owned<IBitSet> slavesDone, slavesStopping;

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

    // NB: returns true if all except me(myNode) are done
    bool slaveStop(unsigned slave)
    {
        CriticalBlock b(allDoneLock);
        bool done = slavesDone->testSet(slave, true);
        assertex(false == done);
        unsigned which = slavesDone->scan(0, false);
        if (which == slaves) // i.e. got all
        {
            allDone = true;
            if (allDoneWaiting)
            {
                allDoneWaiting = false;
                allDoneSem.signal();
            }
        }
        else if (which == (myNode-1))
        {
            if (slavesDone->scan(which+1, false) == slaves)
                return true; // all done except me
        }
        return false;
    }
    unsigned target(unsigned i, unsigned node)
    {
        // For a tree broadcast, calculate the next node to send the data to. i represents the ith copy sent from this node.
        // node is a 0 based node number.
        // It returns a 1 based node number of the next node to send the data to.
        unsigned n = node;
        unsigned j=0;
        while (n)
        {
            j++;
            n /= 2;
        }
        return ((1<<(i+j))+node)+1;
    }
    void broadcastToOthers(CSendItem *sendItem)
    {
        mptag_t rt = createReplyTag();
        unsigned origin = sendItem->queryOrigin();
        unsigned psuedoNode = (myNode<origin) ? slaves-origin+myNode : myNode-origin;
        CMessageBuffer replyMsg;
        // sends to all in 1st pass, then waits for ack from all
        CriticalBlock b(bcastOtherCrit);
        for (unsigned sendRecv=0; sendRecv<2 && !activity.queryAbortSoon(); sendRecv++)
        {
            unsigned i = 0;
            while (!activity.queryAbortSoon())
            {
                unsigned t = target(i++, psuedoNode);
                if (t>slaves)
                    break;
                t += (origin-1);
                if (t>slaves)
                    t -= slaves;
                unsigned sendLen = sendItem->length();
                if (0 == sendRecv) // send
                {
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Sending to node %d, origin %d, size %d, code=%d", myNode, t, origin, sendLen, (unsigned)sendItem->queryCode());
#endif
                    CMessageBuffer &msg = sendItem->queryMsg();
                    msg.setReplyTag(rt); // simulate sendRecv
                    comm.send(msg, t, mpTag);
                }
                else // recv reply
                {
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Sent to node %d, origin %d, size %d, code=%d - received ack", myNode, t, origin, sendLen, (unsigned)sendItem->queryCode());
#endif
                    if (!activity.receiveMsg(replyMsg, t, rt))
                        break;
                }
            }
        }
    }
    // called by CRecv thread
    void cancelReceive()
    {
        stopRecv = true;
        activity.cancelReceiveMsg(RANK_ALL, mpTag);
    }
    void recvLoop()
    {
        CMessageBuffer msg;
        while (!stopRecv && !activity.queryAbortSoon())
        {
            rank_t sendRank;
            if (!activity.receiveMsg(msg, RANK_ALL, mpTag, &sendRank))
                break;
            mptag_t replyTag = msg.getReplyTag();
            CMessageBuffer ackMsg;
            Owned<CSendItem> sendItem = new CSendItem(msg);
#ifdef _TRACEBROADCAST
            ActPrintLog(&activity, "Broadcast node %d received from node %d, origin node %d, size %d, code=%d", myNode, (unsigned)sendRank, sendItem->queryOrigin(), sendItem->length(), (unsigned)sendItem->queryCode());
#endif
            comm.send(ackMsg, sendRank, replyTag); // send ack
            sender.addBlock(sendItem.getLink());
            assertex(myNode != sendItem->queryOrigin());
            switch (sendItem->queryCode())
            {
                case bcast_stop:
                {
                    CriticalBlock b(allDoneLock);
                    if (slaveStop(sendItem->queryOrigin()-1) || allDone)
                    {
                        recvInterface->bCastReceive(NULL); // signal last
                        ActPrintLog(&activity, "recvLoop, received last slaveStop");
                        // NB: this slave has nothing more to receive.
                        // However the sender will still be re-broadcasting some packets, including these stop packets
                        return;
                    }
                    break;
                }
                case bcast_sendStopping:
                {
                    slavesStopping->set(sendItem->queryOrigin()-1, true);
                    // allRequestStop=true, if I'm stopping and all others have requested also
                    allRequestStop = slavesStopping->scan(0, false) == slaves;
                    // fall through
                }
                case bcast_send:
                {
                    if (!allRequestStop) // don't care if all stopping
                        recvInterface->bCastReceive(sendItem.getClear());
                    break;
                }
                default:
                    throwUnexpected();
            }
        }
    }
public:
    CBroadcaster(CActivityBase &_activity) : activity(_activity), receiver(*this), sender(*this), comm(_activity.queryJob().queryJobComm())
    {
        allDone = allDoneWaiting = allRequestStop = stopping = stopRecv = false;
        myNode = activity.queryJob().queryMyRank();
        slaves = activity.queryJob().querySlaves();
        slavesDone.setown(createBitSet());
        slavesStopping.setown(createBitSet());
        mpTag = TAG_NULL;
        recvInterface = NULL;
    }
    void start(IBCastReceive *_recvInterface, mptag_t _mpTag, bool _stopping)
    {
        stopping = _stopping;
        if (stopping)
            slavesStopping->set(myNode-1, true);
        recvInterface = _recvInterface;
        stopRecv = false;
        mpTag = _mpTag;
        receiver.start();
        sender.start();
    }
    void reset()
    {
        allDone = allDoneWaiting = allRequestStop = stopping = false;
        slavesDone->reset();
        slavesStopping->reset();
    }
    CSendItem *newSendItem(broadcast_code code)
    {
        if (stopping && (bcast_send==code))
            code = bcast_sendStopping;
        return new CSendItem(code, myNode);
    }
    void resetSendItem(CSendItem *sendItem)
    {
        sendItem->reset();
    }
    void waitReceiverDone()
    {
        {
            CriticalBlock b(allDoneLock);
            slaveStop(myNode-1);
            if (allDone)
                return;
            allDoneWaiting = true;
        }
        allDoneSem.wait();
    }
    void end()
    {
        waitReceiverDone();
        receiver.wait(); // terminates when received stop from all others
        sender.wait(); // terminates when any remaining packets, including final stop packets have been re-broadcast
    }
    void cancel(IException *e=NULL)
    {
        allDoneWaiting = false;
        allDone = true;
        receiver.abort(true);
        sender.abort(true);
        if (e)
        {
            allDoneSem.interrupt(LINK(e));
            activity.fireException(e);
        }
        else
            allDoneSem.signal();
    }
    bool send(CSendItem *sendItem)
    {
        broadcastToOthers(sendItem);
        return !allRequestStop;
    }
};


class CMarker
{
    CActivityBase &activity;
    NonReentrantSpinLock lock;
    ICompare *cmp;
    Owned<IBitSet> bitSet; // should be roxiemem, so can cause spilling
    const void **base;
    rowidx_t startRow, endRow, chunkSize, next;
    unsigned unique;
    unsigned minForParallel, parallelMinChunkSize, parallelChunkSize;

    class CCompareThread : public CInterface, implements IThreaded
    {
        CMarker &parent;
        CThreaded threaded;
        rowidx_t startRow, endRow;
    public:
        CCompareThread(CMarker &_parent, rowidx_t _startRow, rowidx_t _endRow)
            : parent(_parent), startRow(_startRow), endRow(_endRow), threaded("CCompareThread", this)
        {
        }
        void start() { threaded.start(); }
        void join() { threaded.join(); }
    // IThreaded
        virtual void main()
        {
            parent.run(startRow, endRow);
        }
    };
    CIArrayOf<CCompareThread> threads;

    rowidx_t getMore(rowidx_t &_startRow)
    {
        NonReentrantSpinBlock block(lock);
        if (startRow == endRow)
            return 0;
        _startRow = startRow;
        if (endRow-startRow <= chunkSize)
            startRow = endRow;
        else
            startRow += chunkSize;
        return startRow;
    }
    inline void mark(rowidx_t i)
    {
        ++unique;
        bitSet->set(i); // mark boundary
    }
    void run(rowidx_t myStart, rowidx_t myEnd)
    {
        loop
        {
            const void **rows = base+myStart;
            rowidx_t i=myStart;
            for (; i<(myStart-1); i++, rows++)
            {
                int r = cmp->docompare(*rows, *(rows+1));
                if (r)
                    mark(i);
                /* JCSMORE - could I binchop ahead somehow, to process duplicates more quickly..
                 * i.e. if same cur+mid = cur, then no need to check intermediates..
                 */
            }
            if (myEnd != endRow)
            {
                // final row, cross boundary with next chunk, i.e. { last-row-of-my-chunk , first-row-of-next }
                int r = cmp->docompare(*rows, *(rows+1));
                if (r)
                    mark(i);
            }
            myEnd = getMore(myStart);
            if (0 == myEnd)
                break; // done
        }
    }
public:
    CMarker(CActivityBase &_activity) : activity(_activity)
    {
        cmp = NULL;
        base = NULL;
        startRow = endRow = chunkSize = next = 0;
        unique = 0;
        // perhaps should make configurable..
        minForParallel = 100;
        parallelMinChunkSize = 100;
        parallelChunkSize = 10000;
   }
    unsigned calculate(CThorExpandingRowArray &rows, ICompare *_cmp)
    {
        cmp = _cmp;
        unsigned threadCount = activity.queryMaxCores();
        rowidx_t rowCount = rows.ordinality();
        unique = 0;
        startRow = next = 0;
        endRow = startRow+rowCount;
        base = rows.getRowArray();
        bitSet.setown(createBitSet());
        if (rowCount < minForParallel)
        {
            chunkSize = minForParallel;
            run(startRow, endRow);
        }
        else
        {
            chunkSize = rowCount / threadCount;
            if (chunkSize > parallelChunkSize)
                chunkSize = parallelChunkSize;
            else if (chunkSize < parallelMinChunkSize)
                chunkSize = parallelMinChunkSize;
            for (unsigned t=0; t<threadCount; t++)
            {
                if (startRow+chunkSize >= endRow)
                {
                    threads.append(* new CCompareThread(*this, startRow, endRow));
                    break;
                }
                else
                {
                    rowidx_t s = startRow;
                    startRow += chunkSize;
                    threads.append(* new CCompareThread(*this, s, startRow));
                }
            }
            ForEachItemIn(t, threads)
                threads.item(t).start();
            ForEachItemIn(t2, threads)
                threads.item(t2).join();
            threads.kill();
        }
        mark(endRow-1); // last row is implicitly end of group
        cmp = NULL;
        return unique;
    }
    rowidx_t findNextBoundary()
    {
        rowidx_t ret = bitSet->scan(next, true);
        ++next;
        return next;
    }
};


/* 
    This activity loads the RIGHT hand stream into the hash table, therefore
    the right hand stream -should- contain the fewer records

    Inner, left outer and left only joins supported

*/

class CInMemJoinBase : public CSlaveActivity, public CThorDataLink, implements ISmartBufferNotify, implements IBCastReceive
{
    Semaphore leftstartsem;
    Owned<IException> leftexception;

protected:
    /* Utility class, that is called from the broadcaster to queue up received blocks
     * It will block if it has > MAX_QUEUE_BLOCKS to process (on the queue)
     * Processing will decompress the incoming blocks and add them to a row array per slave
     */
    class CRowProcessor : implements IThreaded
    {
        CThreadedPersistent threaded;
        CInMemJoinBase &owner;
        bool stopped;
        SimpleInterThreadQueueOf<CSendItem, true> blockQueue;
        Owned<IException> exception;
    public:
        CRowProcessor(CInMemJoinBase &_owner) : threaded("CRowProcessor", this), owner(_owner)
        {
            stopped = false;
            blockQueue.setLimit(MAX_QUEUE_BLOCKS);
        }
        ~CRowProcessor()
        {
            blockQueue.stop();
            loop
            {
                Owned<CSendItem> sendItem = blockQueue.dequeueNow();
                if (NULL == sendItem)
                    break;
            }
            wait();
        }
        void start()
        {
            stopped = false;
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
        void wait() { threaded.join(); }
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
    // IThreaded
        virtual void main()
        {
            try
            {
                while (!stopped)
                {
                    Owned<CSendItem> sendItem = blockQueue.dequeue();
                    if (stopped || (NULL == sendItem))
                        break;
                    MemoryBuffer expandedMb;
                    ThorExpand(sendItem->queryMsg(), expandedMb);
                    owner.processRHSRows(sendItem->queryOrigin(), expandedMb);
                }
            }
            catch (IException *e)
            {
                exception.setown(e);
                EXCLOG(e, "CRowProcessor");
            }
        }
    } rowProcessor;

    CBroadcaster broadcaster;
    rowidx_t rhsTableLen;

    IThorDataLink *leftITDL, *rightITDL;
    Owned<IRowStream> left, right;
    Owned<IEngineRowAllocator> rightAllocator;
    Owned<IEngineRowAllocator> leftAllocator;
    Owned<IEngineRowAllocator> allocator;
    Owned<IOutputRowSerializer> rightSerializer;
    Owned<IOutputRowDeserializer> rightDeserializer;
    bool gotRHS;
    join_t joinType;
    OwnedConstThorRow defaultRight;
    CThorExpandingRowArray rhs;
    bool needGlobal;
    unsigned flags;
    bool exclude;
    const void *rhsNext;
    Owned<IOutputMetaData> outputMeta;

    PointerArrayOf<CThorSpillableRowArray> rhsNodeRows;

    rowidx_t nextRhsRow;
    unsigned keepLimit;
    unsigned joined;
    OwnedConstThorRow defaultLeft;

    bool leftMatch, grouped;
    bool fuzzyMatch, returnMany, dedup;
    rank_t myNode;
    unsigned numNodes;

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
    void clearRHS()
    {
        rhs.kill();
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorSpillableRowArray *rows = rhsNodeRows.item(a);
            if (rows)
                rows->kill();
        }
    }
    inline void resetRhsNext()
    {
        nextRhsRow = 0;
        joined = 0;
        leftMatch = false;
    }
    void processRHSRows(unsigned slave, MemoryBuffer &mb)
    {
        CThorSpillableRowArray &rows = *rhsNodeRows.item(slave-1);
        RtlDynamicRowBuilder rowBuilder(rightAllocator);
        CThorStreamDeserializerSource memDeserializer(mb.length(), mb.toByteArray());
        while (!memDeserializer.eos())
        {
            size32_t sz = rightDeserializer->deserialize(rowBuilder, memDeserializer);
            OwnedConstThorRow fRow = rowBuilder.finalizeRowClear(sz);
            // NB: If spilt, addLocalRHSRow will filter out non-locals
            addLocalRHSRow(rows, fRow);
        }
    }
    void broadcastRHS() // broadcasting local rhs
    {
        bool stopRHSBroadcast = false;
        Owned<CSendItem> sendItem = broadcaster.newSendItem(bcast_send);
        MemoryBuffer mb;
        try
        {
            CThorSpillableRowArray &localRhsRows = *rhsNodeRows.item(queryJob().queryMyRank()-1);
            CMemoryRowSerializer mbser(mb);
            while (!abortSoon)
            {
                while (!abortSoon)
                {
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;

                    if (!addLocalRHSRow(localRhsRows, row))
                        stopRHSBroadcast = true;

                    rightSerializer->serialize(mbser, (const byte *)row.get());
                    if (mb.length() >= MAX_SEND_SIZE || stopRHSBroadcast)
                        break;
                }
                if (0 == mb.length())
                    break;
                if (stopRHSBroadcast)
                    sendItem->setFlag(bcastflag_spilt);
                ThorCompress(mb, sendItem->queryMsg());
                if (!broadcaster.send(sendItem))
                    break;
                if (stopRHSBroadcast)
                    break;
                mb.clear();
                broadcaster.resetSendItem(sendItem);
            }
        }
        catch (IException *e)
        {
            ActPrintLog(e, "CInMemJoinBase::broadcastRHS: exception");
            throw;
        }

        sendItem.setown(broadcaster.newSendItem(bcast_stop));
        if (stopRHSBroadcast)
            sendItem->setFlag(bcastflag_spilt);
        ActPrintLog("Sending final RHS broadcast packet");
        broadcaster.send(sendItem); // signals stop to others
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CInMemJoinBase(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this),
        broadcaster(*this), rhs(*this, NULL), rowProcessor(*this)
    {
        gotRHS = false;
        nextRhsRow = 0;
        rhsNext = NULL;
        myNode = queryJob().queryMyRank();
        numNodes = queryJob().querySlaves();
        needGlobal = !container.queryLocal() && (container.queryJob().querySlaves() > 1);

        rhsTableLen = 0;
        leftITDL = rightITDL = NULL;

        joined = 0;
        leftMatch = false;
        grouped = false;
        fuzzyMatch = returnMany = false;

        exclude = false;
        keepLimit = 0;
    }
    ~CInMemJoinBase()
    {
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorSpillableRowArray *rows = rhsNodeRows.item(a);
            if (rows)
                delete rows;
        }
    }
// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);

        exclude = 0 != (flags & JFexclude);
        if (0 == keepLimit)
            keepLimit = (unsigned)-1;
        if (flags & JFleftouter)
            joinType = exclude ? JT_LeftOnly : JT_LeftOuter;
        else
            joinType = JT_Inner;
        StringBuffer str;
        ActPrintLog("Join type is %s", getJoinTypeStr(str).str());

        if (!container.queryLocal())
            mpTag = container.queryJob().deserializeMPTag(data);

        unsigned slaves = container.queryJob().querySlaves();
        rhsNodeRows.ensure(slaves);
        while (slaves--)
            rhsNodeRows.append(new CThorSpillableRowArray(*this, NULL));
    }
    virtual void start()
    {
        assertex(inputs.ordinality() == 2);
        gotRHS = false;
        nextRhsRow = 0;
        joined = 0;
        leftMatch = false;
        rhsNext = NULL;
        rhsTableLen = 0;
        leftITDL = inputs.item(0);
        rightITDL = inputs.item(1);
        allocator.set(queryRowAllocator());
        leftAllocator.set(::queryRowAllocator(leftITDL));
        outputMeta.set(leftITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta());

        right.set(rightITDL);
        rightAllocator.set(::queryRowAllocator(rightITDL));
        rightSerializer.set(::queryRowSerializer(rightITDL));
        rightDeserializer.set(::queryRowDeserializer(rightITDL));

        leftITDL = createDataLinkSmartBuffer(this,leftITDL,LOOKUPJOINL_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(leftITDL->queryFromActivity()),grouped,RCUNBOUND,this,false,&container.queryJob().queryIDiskUsage());
        left.setown(leftITDL);
        startInput(leftITDL);

        try
        {
            startInput(rightITDL);
        }
        catch (CATCHALL)
        {
            leftstartsem.wait();
            left->stop();
            throw;
        }
        leftstartsem.wait();
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
        cancelReceiveMsg(RANK_ALL, mpTag);
        broadcaster.cancel();
        rowProcessor.abort();
    }
    virtual void stop()
    {
        // JCS->GH - if in a child query, it would be good to preserve RHS.. would need tip/flag from codegen that constant
        clearRHS();
        if (right)
        {
            stopInput(right, "(R)");
            right.clear();
        }
        broadcaster.reset();
        stopInput(left, "(L)");
        left.clear();
        dataLinkStop();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canStall = true;
    }
    void doBroadcastRHS(bool stopping)
    {
        rowProcessor.start();
        broadcaster.start(this, mpTag, stopping);
        broadcastRHS();
        broadcaster.end();
        rowProcessor.wait();
    }
    rowidx_t getGlobalRHSTotal()
    {
        rowcount_t rhsRows = 0;
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
            rows.flush();
            rhsRows += rows.numCommitted();
            if (rhsRows > RIMAX)
                throw MakeActivityException(this, 0, "Too many RHS rows: %"RCPF"d", rhsRows);
        }
        return (rowidx_t)rhsRows;
    }
    virtual bool addLocalRHSRow(CThorSpillableRowArray &localRhsRows, const void *row)
    {
        LinkThorRow(row);
        localRhsRows.append(row);
        return true;
    }
// ISmartBufferNotify
    virtual void onInputStarted(IException *except)
    {
        leftexception.set(except);
        leftstartsem.signal();
    }
    virtual bool startAsync()
    {
        return true;
    }
    virtual void onInputFinished(rowcount_t count)
    {
        ActPrintLog("LHS input finished, %"RCPF"d rows read", count);
    }
};

/*
 * The main activity class
 * It's intended to be used when the RHS globally, is small enough to fit within the memory of a single node.
 * It performs the join, by 1st ensuring all RHS data is on all nodes, creating a hash table of this gathered set
 * then it streams the LHS through, matching against the RHS hash table entries.
 * It also handles match conditions where there is no hard match (, ALL), in those cases no hash table is needed.
 * TODO: right outer/only joins
 */

template <class HTType>
class CLookupJoinActivityBase : public CInMemJoinBase, implements roxiemem::IBufferedRowCallback
{
    IHThorHashJoinArg *hashJoinHelper;
    IHash *leftHash, *rightHash;
    ICompare *compareRight, *compareLeftRight;

    HTType ht;

    unsigned abortLimit, atMost;
    ConstPointerArray candidates;
    unsigned candidateIndex, candidateMatches;

    mptag_t lhsDistributeTag, rhsDistributeTag, broadcast2MpTag;

    // Handling failover to a) hashed local lookupjoin b) hash distributed standard join
    bool localLookupJoin, rhsCollated;
    bool failoverToLocalLookupJoin, failoverToStdJoin;
    Owned<IHashDistributor> lhsDistributor, rhsDistributor;
    ICompare *compareLeft;
    UnsignedArray flushedRowMarkers;

    atomic_t spiltBroadcastingRHS;
    CriticalSection broadcastSpillingLock;

    inline bool isSmart() const
    {
        switch (container.getKind())
        {
            case TAKsmartjoin:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
                return true;
        }
        return false;
    }
    inline bool hasBroadcastSpilt() const { return 0 != atomic_read(&spiltBroadcastingRHS); }
    inline void setBroadcastingSpilt(bool tf) { atomic_set(&spiltBroadcastingRHS, (int)tf); }
    rowidx_t clearNonLocalRows(CThorSpillableRowArray &rows, rowidx_t startPos)
    {
        rowidx_t clearedRows = 0;
        rowidx_t numRows = rows.numCommitted();
        for (rowidx_t r=startPos; r<numRows; r++)
        {
            unsigned hv = rightHash->hash(rows.query(r));
            if ((myNode-1) != (hv % numNodes))
            {
                OwnedConstThorRow row = rows.getClear(r); // dispose of
                ++clearedRows;
            }
        }
        return clearedRows;
    }
    // Annoyingly similar to above, used post broadcast when rhsNodeRows collaed into 'rhs'
    rowidx_t clearNonLocalRows(CThorExpandingRowArray &rows, rowidx_t startPos)
    {
        rowidx_t clearedRows = 0;
        rowidx_t numRows = rows.ordinality();
        for (rowidx_t r=startPos; r<numRows; r++)
        {
            unsigned hv = rightHash->hash(rows.query(r));
            if ((myNode-1) != (hv % numNodes))
            {
                OwnedConstThorRow row = rows.getClear(r); // dispose of
                ++clearedRows;
            }
        }
        return clearedRows;
    }
    bool clearAllNonLocalRows(const char *msg)
    {
        // This is likely to free memory, so block others (threads) until done
        // NB: This will not block appends
        CriticalBlock b(broadcastSpillingLock);
        if (hasBroadcastSpilt())
            return false;
        setBroadcastingSpilt(true);
        ActPrintLog("Clearing non-local rows - cause: %s", msg);

        rowidx_t clearedRows = 0;
        if (rhsCollated)
        {
            // This only needs to be done once, no rows will be added after collated
            clearedRows += clearNonLocalRows(rhs, 0);
            rhs.compact();
        }
        else
        {
            /* NB: It is likely that there will be unflushed rows in the rhsNodeRows arrays after we are done here.
            /* These will need flushing when all is done and clearNonLocalRows will need recalling to process rest
             */
            ForEachItemIn(a, rhsNodeRows)
            {
                CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
                clearedRows += clearNonLocalRows(rows, 0);
            }
        }
        ActPrintLog("handleLowMem: clearedRows = %"RIPF"d", clearedRows);
        return 0 != clearedRows;
    }
    const void *find(const void *left, unsigned &h)
    {
        loop
        {
            const void *right = ht.getRow(h);
            if (0 == compareLeftRight->docompare(left, right))
                return right;
            h++;
            if (h>=rhsTableLen)
                h = 0;
        }
        return NULL;
    }
    const void *findFirst(const void *left, unsigned &h)
    {
        h = leftHash->hash(left)%rhsTableLen;
        return find(left, h);
    }
    const void *findNext(const void *left, unsigned &h)
    {
        h++;
        if (h>=rhsTableLen)
            h = 0;
        return find(left, h);
    }
    void addRowsToHt(CThorExpandingRowArray &rows, CMarker &marker)
    {
        rowidx_t pos=0;
        rowidx_t pos2;
        loop
        {
            pos2 = marker.findNextBoundary();
            rowidx_t count = dedup ? 1 : pos2-pos;
            /* JCS->GH - Could you/do you spot LOOKUP MANY, followed by DEDUP(key) ?
             * It feels like we should only dedup if code gen spots, rather than have LOOKUP without MANY option
             * i.e. feels like LOOKUP without MANY should be deprecated..
            */
            const void *row = rhs.query(pos);
            unsigned h = rightHash->hash(row)%rhsTableLen;
            // NB: 'pos' and 'count' won't be used if dedup variety
            ht.addEntry(row, h, pos, count);
        }
    }
    void setupDistributors()
    {
        if (!rhsDistributor)
        {
            rhsDistributor.setown(createHashDistributor(this, queryJob().queryJobComm(), rhsDistributeTag, false, NULL));
            right.setown(rhsDistributor->connect(queryRowInterfaces(rightITDL), right.getClear(), rightHash, NULL));
            lhsDistributor.setown(createHashDistributor(this, queryJob().queryJobComm(), lhsDistributeTag, false, NULL));
            left.setown(lhsDistributor->connect(queryRowInterfaces(leftITDL), left.getClear(), leftHash, NULL));
        }
    }
    void setupHt(rowidx_t size, const void **rows)
    {
        ht.setup(size, rows);
        rhsTableLen = size;
    }
    void clearHt()
    {
        ht.reset();
        rhsTableLen = 0;
    }

protected:
    Owned<IJoinHelper> joinHelper;

/* This class becomes the base class of template CInMemJoinSlave
 * The following methods are then directly called by CInMemJoinSlave::lookupNextRow() to avoid virtuals being used
 */
    inline void setRhsNext(const void *leftRow)
    {
        if ((unsigned)-1 != atMost)
            rhsNext = candidateIndex < candidates.ordinality() ? candidates.item(candidateIndex++) : NULL;
        else
            rhsNext = findNext(leftRow, nextRhsRow);
    }
    inline const void *setRhsNextFirst(const void *leftRow) // only returns row if onFail row created
    {
        candidateMatches = 0;
        if ((unsigned)-1 != atMost)
        {
            candidates.kill();
            candidateIndex = 1;
        }
        rhsNext = findFirst(leftRow, nextRhsRow);
        if ((unsigned)-1 != atMost) // have to build candidates to know
        {
            while (rhsNext)
            {
                ++candidateMatches;
                if (candidateMatches>abortLimit)
                {
                    if (0 == (JFmatchAbortLimitSkips & flags))
                    {
                        Owned<IException> e;
                        try
                        {
                            if (hashJoinHelper)
                                hashJoinHelper->onMatchAbortLimitExceeded();
                            CommonXmlWriter xmlwrite(0);
                            if (outputMeta && outputMeta->hasXML())
                                outputMeta->toXML((const byte *) leftRow, xmlwrite);
                            throw MakeActivityException(this, 0, "More than %d match candidates in join for row %s", abortLimit, xmlwrite.str());
                        }
                        catch (IException *_e)
                        {
                            if (0 == (JFonfail & flags))
                                throw;
                            e.setown(_e);
                        }
                        RtlDynamicRowBuilder ret(allocator);
                        size32_t transformedSize = hashJoinHelper->onFailTransform(ret, leftRow, defaultRight, e.get());
                        rhsNext = NULL;
                        if (transformedSize)
                        {
                            candidateMatches = 0;
                            return ret.finalizeRowClear(transformedSize);
                        }
                    }
                    else
                        leftMatch = true; // there was a lhs match, even though rhs group exceeded limit. Therefore this lhs will not be considered left only/left outer
                    candidateMatches = 0;
                    break;
                }
                else if (candidateMatches>atMost)
                {
                    candidateMatches = 0;
                    break;
                }
                candidates.append(rhsNext);
                rhsNext = findNext(leftRow, nextRhsRow);
            }
            if (0 == candidateMatches)
                rhsNext = NULL;
            else if (candidates.ordinality())
                rhsNext = candidates.item(0);
        }
        return NULL;
    }
    inline bool match(const void *left, const void *right)
    {
        return hashJoinHelper->match(left, right);
    }
    inline size32_t joinTransform(RtlDynamicRowBuilder &rowBuilder, const void *left, const void *right)
    {
        return hashJoinHelper->transform(rowBuilder, left, right);
    }
    inline const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned numRows, const void **rows)
    {
        return hashJoinHelper->transform(rowBuilder, left, right, numRows, rows);
    }
    inline const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned count)
    {
        return hashJoinHelper->transform(rowBuilder, left, right, count);
    }
// The above methods are used by CInMemJoinSlave::lookupNextRow()
    void getRHS(bool stopping)
    {
        if (gotRHS)
            return;
        gotRHS = true;
/*
 * Global LOOKUP:
 * 1) distributes RHS (using broadcaster)
 * 2) sizes the hash table
 * 3) If there is no OOM event, it is done and the RHS hash table is built.
 *    ELSE -
 * 4) If during 1) or 2) an OOM event occurs, all other slaves are notified.
 *    If in the middle of broadcasting, it will stop sending RHS
 *    The spill event will flush out all rows that do not hash to local slave
 * 5) Hash distributor streams are setup for the [remaining] RHS and unread LHS.
 * 6) The broadcast rows + the remaining RHS distributed stream are consumed into a single row array.
 * 7) When done if it has not spilt, the RHS hash table is sized.
 * 8) If there is no OOM event, the RHS is done and the RHS hash table is built
 *    The distributed LHS stream is used to perform a local lookup join.
 *    ELSE -
 * 9) If during 6) or 7) an OOM event occurs, the stream loader, will spill and sort as necessary.
 * 10) The LHS side is loaded and spilt and sorted if necessary
 * 11) A regular join helper is created to perform a local join against the two hash distributed sorted sides.
 */
        CMarker marker(*this);
        if (needGlobal)
        {
            doBroadcastRHS(stopping);
            rowidx_t rhsRows;
            {
                CriticalBlock b(broadcastSpillingLock);
                rhsRows = getGlobalRHSTotal();
            }
            if (!hasBroadcastSpilt())
                rhs.ensure(rhsRows);

            // NB: no more rows can be added to rhsNodeRows at this point, but they could still be flushed

            if (isSmart() && getOptBool(THOROPT_LKJOIN_LOCALFAILOVER, getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER))) // For testing purposes only
                clearAllNonLocalRows("testing");

            {
                /* NB: This does not allocate/will not provoke spilling, but spilling callback still active
                 * and need to protect rhsNodeRows access
                 */
                CriticalBlock b(broadcastSpillingLock);
                if (!hasBroadcastSpilt())
                {
                    // If spilt, don't size ht table now, if local rhs fits, ht will be sized later
                    ForEachItemIn(a, rhsNodeRows)
                    {
                        CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
                        rhs.appendRows(rows, true);
                        rows.kill(); // free up ptr table asap
                    }
                    rhsCollated = true;
                }
            }
            if (!hasBroadcastSpilt())
            {
                rowidx_t uniqueKeys = marker.calculate(rhs, compareRight);

                // NB: This sizing could cause spilling callback to be triggered
                setupHt(uniqueKeys, rhs.getRowArray());
                /* JCSMORE - failure to size should not be failure condition
                 * It will mark spiltBroadcastingRHS and try to degrade
                 * JCS->GH: However, need to catch OOM somehow..
                 */
            }
            if (failoverToLocalLookupJoin)
            {
                /* NB: Potentially one of the slaves spilt late after broadcast and rowprocessor finished
                 * Need to remove spill callback and broadcast one last message to know.
                 */

                queryJob().queryRowManager()->removeRowBuffer(this);

                ActPrintLog("Broadcasting final split status");
                broadcaster.reset();
                // NB: using a different tag from 1st broadcast, as 2nd on other nodes can start sending before 1st on this has quit receiving
                broadcaster.start(this, broadcast2MpTag, false);
                Owned<CSendItem> sendItem = broadcaster.newSendItem(bcast_stop);
                if (hasBroadcastSpilt())
                    sendItem->setFlag(bcastflag_spilt);
                broadcaster.send(sendItem); // signals stop to others
                broadcaster.end();
            }

            /* All slaves now know whether any one spilt or not, i.e. whether to perform local hash join or not
             * If any have, still need to distribute rest of RHS..
             */

            // flush spillable row arrays, and clear any non-locals if spiltBroadcastingRHS and compact
            if (hasBroadcastSpilt())
            {
                ActPrintLog("Spilt whilst broadcasting, will attempt distribute local lookup join");
                localLookupJoin = true;

                // NB: lhs ordering and grouping lost from here on..
                if (grouped)
                    throw MakeActivityException(this, 0, "Degraded to distributed lookup join, LHS order cannot be preserved");

                // If HT sized already and now spilt, too big clear and size when local size known
                clearHt();

                setupDistributors();

                if (stopping)
                {
                    ActPrintLog("getRHS stopped");
                    /* NB: Can only stop now, after distributors are setup
                     * since other slaves may not be stopping and are dependent on the distributors
                     * The distributor will not actually stop until everyone else does.
                     */
                    return;
                }

                /* NB: The collected broadcast rows thus far (in rhsNodeRows or rhs) were ordered/deterministic.
                 * However, the rest of the rows received via the distributor and non-deterministic.
                 * Therefore the order overall is non-deterministic from this point on.
                 * For that reason, the rest of the RHS (distributed) rows will be processed ahead of the
                 * collected [broadcast] rows in the code below for efficiency reasons.
                 */
                IArrayOf<IRowStream> streams;
                streams.append(*right.getLink()); // what remains of 'right' will be read through distributor

                if (rhsCollated)
                {
                    // NB: If spilt after rhsCollated, callback will have cleared and compacted
                    streams.append(*rhs.createRowStream()); // NB: will kill array when stream exhausted
                }
                else
                {
                    // NB: If cleared before rhsCollated, then need to clear non-locals that were added after spill
                    ForEachItemIn(a, rhsNodeRows)
                    {
                        CThorSpillableRowArray &sRowArray = *rhsNodeRows.item(a);
                        CThorExpandingRowArray rowArray(*this, NULL);
                        rowArray.transferFrom(sRowArray);
                        clearNonLocalRows(rowArray, flushedRowMarkers.item(a));
                        rowArray.compact();
                        streams.append(*rowArray.createRowStream()); // NB: will kill array when stream exhausted
                    }
                }
                right.setown(createConcatRowStream(streams.ordinality(), streams.getArray()));
            }
            else
            {
                if (stopping) // broadcast done and no-one spilt, this node can now stop
                    return;
            }
        }
        else
        {
            if (stopping) // if local can stop now
                return;
            localLookupJoin = true;
        }

        if (localLookupJoin) // NB: Can only be active for LOOKUP
        {
            Owned<IThorRowLoader> rowLoader;
            if (failoverToStdJoin)
            {
                if (getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER)) // for testing only (force to disk, as if spilt)
                    rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(rightITDL), compareRight, false, rc_allDisk, SPILL_PRIORITY_LOOKUPJOIN));
                else
                {
                    rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(rightITDL), compareRight, false, rc_mixed, SPILL_PRIORITY_LOOKUPJOIN));
                    rowLoader->setOptions(rcflag_noAllInMemSort); // If fits into memory, don't want it sorted
                }
            }
            else
            {
                // i.e. will fire OOM if runs out of memory loading local right
                rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(rightITDL), compareRight, false, rc_allMem, SPILL_PRIORITY_DISABLE));
            }

            Owned<IRowStream> rightStream = rowLoader->load(right, abortSoon, false, &rhs);
            if (!rightStream)
            {
                ActPrintLog("RHS local rows fitted in memory, count: %"RIPF"d", rhs.ordinality());
                // all fitted in memory, rows were transferred out back into 'rhs'
                // Will be unsorted because of rcflag_noAllInMemSort

                /* Now need to size HT.
                 * transfer rows back into a spillable container
                 * If HT sizing DOESN'T cause spill, then, row will be transferred back into 'rhs'
                 * If HT sizing DOES cause spill, sorted rightStream will be created.
                 */

                rowLoader.clear();
                Owned<IThorRowCollector> collector = createThorRowCollector(*this, queryRowInterfaces(rightITDL), compareRight,false, rc_mixed, SPILL_PRIORITY_LOOKUPJOIN);
                collector->setOptions(rcflag_noAllInMemSort); // If fits into memory, don't want it sorted
                collector->transferRowsIn(rhs);

                rowidx_t uniqueKeys = marker.calculate(rhs, compareRight);

                // could cause spilling of 'rhs'
                setupHt(uniqueKeys, rhs.getRowArray());
                /* JCSMORE - failure to size should not be failure condition
                 * If it failed, the 'collector' will have spilt and it will not need HT
                 * JCS->GH: However, need to catch OOM somehow..
                 */
                rightStream.setown(collector->getStream(false, &rhs));
            }
            if (rightStream) // NB: returned stream, implies spilt AND sorted, if not, 'rhs' is filled
            {
                ActPrintLog("RHS spilt to disk. Standard Join will be used");

                // NB: lhs ordering and grouping lost from here on.. (will have been caught earlier if global)
                if (grouped)
                    throw MakeActivityException(this, 0, "Degraded to standard join, LHS order cannot be preserved");

                rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(leftITDL), compareLeft));
                left.setown(rowLoader->load(left, abortSoon, false));
                leftITDL = inputs.item(0); // reset
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
                        joinHelper.setown(createJoinHelper(*this, hashJoinHelper, this, hintparallelmatch, hintunsortedoutput));
                        break;
                    }
                    case TAKlookupdenormalize:
                    case TAKlookupdenormalizegroup:
                    case TAKsmartdenormalize:
                    case TAKsmartdenormalizegroup:
                        joinHelper.setown(createDenormalizeHelper(*this, hashJoinHelper, this));
                        break;
                    default:
                        throwUnexpected();
                }
                joinHelper->init(left, rightStream, leftAllocator, rightAllocator, ::queryRowMetaData(leftITDL), &abortSoon);
                return;
            }
            else
                ActPrintLog("Local RHS loaded to memory, performing local lookup join");
            // If got this far, without turning into a standard fully distributed join, then all rows are in rhs
        }

        // JCSMORE - would be nice to make this multi-core, clashes and compares can be expensive

        addRowsToHt(rhs, marker);
        ActPrintLog("rhs table: %d elements", rhsTableLen);
    }
public:
    CLookupJoinActivityBase(CGraphElementBase *_container) : CInMemJoinBase(_container), ht(*this)
    {
        atMost = 0;
        localLookupJoin = rhsCollated = false;
        broadcast2MpTag = lhsDistributeTag = rhsDistributeTag = TAG_NULL;
        setBroadcastingSpilt(false);

        candidateIndex = 0;
        candidateMatches = 0;

        grouped = hashJoinHelper->queryOutputMeta()->isGrouped();

        hashJoinHelper = (IHThorHashJoinArg *)queryHelper();
        flags = hashJoinHelper->getJoinFlags();
        leftHash = hashJoinHelper->queryHashLeft();
        rightHash = hashJoinHelper->queryHashRight();
        compareRight = hashJoinHelper->queryCompareRight();
        compareLeft = hashJoinHelper->queryCompareLeft();
        compareLeftRight = hashJoinHelper->queryCompareLeftRight();
        if (JFmanylookup & flags)
            returnMany = true;
        keepLimit = hashJoinHelper->getKeepLimit();
        abortLimit = hashJoinHelper->getMatchAbortLimit();
        atMost = hashJoinHelper->getJoinLimit();

        fuzzyMatch = 0 != (JFmatchrequired & flags);
        bool maySkip = 0 != (flags & JFtransformMaySkip);
        dedup = false;
        if (compareRight && !maySkip && !fuzzyMatch)
        {
            if (returnMany)
                dedup = (1==keepLimit) && (0==atMost) && (0==abortLimit);
            else
                dedup = true;
        }
        if (0 == abortLimit)
            abortLimit = (unsigned)-1;
        if (0 == atMost)
            atMost = (unsigned)-1;
        if (abortLimit < atMost)
            atMost = abortLimit;

        failoverToLocalLookupJoin = failoverToStdJoin = isSmart();
        ActPrintLog("failoverToLocalLookupJoin=%s, failoverToStdJoin=%s",
                failoverToLocalLookupJoin?"true":"false", failoverToStdJoin?"true":"false");
    }
// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CInMemJoinBase::init(data, slaveData);

        if (!container.queryLocal())
        {
            mpTag = container.queryJob().deserializeMPTag(data);
            broadcast2MpTag = container.queryJob().deserializeMPTag(data);
            lhsDistributeTag = container.queryJob().deserializeMPTag(data);
            rhsDistributeTag = container.queryJob().deserializeMPTag(data);
        }
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);

        CInMemJoinBase::start();

        setBroadcastingSpilt(false);
        localLookupJoin = rhsCollated = false;
        flushedRowMarkers.kill();
        candidateIndex = candidateMatches = 0;

        if (failoverToLocalLookupJoin)
        {
            if (needGlobal)
                queryJob().queryRowManager()->addRowBuffer(this);
        }
        if ((flags & JFonfail) || (flags & JFleftouter))
        {
            RtlDynamicRowBuilder rr(rightAllocator);
            rr.ensureRow();
            size32_t rrsz = hashJoinHelper->createDefaultRight(rr);
            defaultRight.setown(rr.finalizeRowClear(rrsz));
        }
        if (!isSmart())
        {
            bool inputGrouped = leftITDL->isGrouped();
            dbgassertex(inputGrouped == grouped); // std. lookup join expects these to match
        }
    }
    virtual void abort()
    {
    	CInMemJoinBase::abort();
        if (rhsDistributor)
            rhsDistributor->abort();
        if (lhsDistributor)
            lhsDistributor->abort();
    }
    virtual void stop()
    {
        if (!gotRHS && needGlobal)
            getRHS(true); // If global, need to handle RHS until all are slaves stop

        // JCS->GH - if in a child query, it would be good to preserve RHS.. would need tip/flag from codegen that constant
        clearHt();
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
        CInMemJoinBase::stop();
    }
    virtual bool isGrouped()
    {
        return isSmart() ? false : inputs.item(0)->isGrouped();
    }
// IBCastReceive
    virtual void bCastReceive(CSendItem *sendItem)
    {
        if (sendItem)
        {
            if (0 != (sendItem->queryFlags() & bcastflag_spilt))
            {
                VStringBuffer msg("Notification that slave %d spilt", sendItem->queryOrigin());
                clearAllNonLocalRows(msg.str());
            }
        }
        rowProcessor.addBlock(sendItem); // NB: NULL indicates end
    }
// IBufferedRowCallback
    virtual unsigned getPriority() const
    {
        return SPILL_PRIORITY_LOOKUPJOIN;
    }
    virtual bool freeBufferedRows(bool critical)
    {
        // NB: only installed if lookup join and global
        return clearAllNonLocalRows("Out of memory callback");
    }
    virtual bool addLocalRHSRow(CThorSpillableRowArray &localRhsRows, const void *row)
    {
        if (hasBroadcastSpilt())
        {
            // keep it only if it hashes to my node
            unsigned hv = rightHash->hash(row);
            if ((myNode-1) == (hv % numNodes))
                CInMemJoinBase::addLocalRHSRow(localRhsRows, row);
            // ok so switch tactics.
            // clearAllNonLocalRows() will have cleared out non-locals by now
            // Returning false here, will stop the broadcaster

            return false;
        }
        else
        {
            /* NB: It could still spill here, i.e. before appending a non-local row
             * When all is done, a last pass is needed to clear out non-locals
             */
            CInMemJoinBase::addLocalRHSRow(localRhsRows, row);
        }
        return true;
    }
};


class CHTBase
{
protected:
    CInMemJoinBase &activity;
    rowidx_t htSz;
    OwnedConstThorRow htMemory;
public:
    CHTBase(CInMemJoinBase &_activity) : activity(_activity)
    {
        htSz = 0;
    }
    void reset()
    {
        htMemory.clear();
        htSz = 0;
    }
};

class CHTDirect : public CHTBase
{
    const void **ht;
public:
    CHTDirect(CInMemJoinBase &activity) : CHTBase(activity)
    {
        ht = NULL;
    }
    void setup(rowidx_t size, const void **_rows) // _rows unused
    {
        size32_t sz = sizeof(const void *)*size;
        htMemory.setown(activity.queryJob().queryRowManager()->allocate(sz, activity.queryContainer().queryId()));
        ht = (const void **)htMemory.get();
        memset(ht, 0, sz);
    }
    void reset()
    {
        CHTBase::reset();
        ht = NULL;
    }
    inline const void *getRow(unsigned pos)
    {
        return ht[pos];
    }
    inline void addEntry(const void *row, unsigned hash, rowidx_t index, rowidx_t count)
    {
        loop
        {
            const void *&htRow = ht[hash];
            if (!htRow)
            {
                htRow = row;
                break;
            }
            hash++;
            if (hash>=htSz)
                hash = 0;
        }
    }
};

class CHTIndexCount : public CHTBase
{
    struct HtEntry { rowidx_t index, count; } *ht;
    const void **rows;
public:
    CHTIndexCount(CInMemJoinBase &activity) : CHTBase(activity)
    {
        rows = NULL;
        ht = NULL;
    }
    void setup(rowidx_t size, const void **_rows)
    {
        size32_t sz = sizeof(HtEntry)*size;
        htMemory.setown(activity.queryJob().queryRowManager()->allocate(sz, activity.queryContainer().queryId()));
        ht = (HtEntry *)htMemory.get();
        memset(ht, 0, sz);
        rows = _rows;
    }
    void reset()
    {
        CHTBase::reset();
        htMemory.clear();
        rows = NULL;
        htSz = 0;
    }
    inline const void *getRow(unsigned hash)
    {
        HtEntry &e = ht[hash];
        if (e.count)
            return NULL;
        return rows[e.index];
    }
    inline void addEntry(const void *row, unsigned hash, rowidx_t index, rowidx_t count)
    {
        loop
        {
            HtEntry &e = ht[hash];
            if (!e.count)
            {
                e.index = index;
                e.count = count;
                break;
            }
            hash++;
            if (hash>=htSz)
                hash = 0;
        }
    }
};

class CLookupJoinActivity : public CLookupJoinActivityBase<CHTDirect>
{
public:
    CLookupJoinActivity(CGraphElementBase *_container) : CLookupJoinActivityBase(_container)
    {
    }
};

class CLookupManyJoinActivity : public CLookupJoinActivityBase<CHTIndexCount>
{
public:
    CLookupManyJoinActivity(CGraphElementBase *_container) : CLookupJoinActivityBase(_container)
    {
    }
};


class CAllJoinActivity : public CInMemJoinBase
{
    IHThorAllJoinArg *allJoinHelper;
    const void **rhsTable;

protected:
/* This class becomes the base class of template CInMemJoinSlave
 * The following methods are then directly called by CInMemJoinSlave::lookupNextRow() to avoid virtuals being used
 */
    inline void setRhsNext(const void *leftRow)
    {
        if (++nextRhsRow<rhsTableLen)
            rhsNext = rhsTable[nextRhsRow];
        else
            rhsNext = NULL;
    }
    inline const void *setRhsNextFirst(const void *leftRow)
    {
        rhsNext = rhsTable[0];
        return NULL;
    }
    inline bool match(const void *lhs, const void *rhsrow)
    {
        return allJoinHelper->match(lhs, rhsrow);
    }
    inline const size32_t joinTransform(ARowBuilder &rowBuilder, const void *lhs, const void *rhsrow)
    {
        return allJoinHelper->transform(rowBuilder, lhs, rhsrow);
    }
    inline const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned numRows, const void **rows)
    {
        return allJoinHelper->transform(rowBuilder, left, right, numRows, rows);
    }
    inline const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned count)
    {
        return allJoinHelper->transform(rowBuilder, left, right, count);
    }
// The above methods are used by CInMemJoinSlave::lookupNextRow()
    void getRHS(bool stopping)
    {
        if (gotRHS)
            return;
        gotRHS = true;

        // if input counts known, get global aggregate and pre-allocate RHS size
        ThorDataLinkMetaInfo rightMeta;
        rightITDL->getMetaInfo(rightMeta);
        rowcount_t rhsTotalCount;
        if (rightMeta.totalRowsMin == rightMeta.totalRowsMax)
            rhsTotalCount = rightMeta.totalRowsMax;
        else
            rhsTotalCount = RCUNSET;
        // ALL join must fit into memory
        if (needGlobal)
        {
            CMessageBuffer msg;
            msg.append(rhsTotalCount);
            container.queryJob().queryJobComm().send(msg, 0, mpTag);
            if (!receiveMsg(msg, 0, mpTag))
                return;
            msg.read(rhsTotalCount);
            if (RCUNSET != rhsTotalCount)
            {
                if (rhsTotalCount > RIMAX)
                    throw MakeActivityException(this, 0, "Too many rows on RHS for ALL join: %"RCPF"d", rhsTotalCount);
                rhsTableLen = (rowidx_t)rhsTotalCount;
                rhs.ensure(rhsTableLen);
            }
            doBroadcastRHS(stopping);
            if (stopping) // broadcast done and no-one spilt, this node can now stop
                return;

            rowidx_t rhsRows = getGlobalRHSTotal();
            if (rhsTotalCount != RCUNSET)
                assertex(rhsRows == rhsTotalCount);
            else
            {
                rhsTableLen = (rowidx_t)rhsRows;
                rhs.ensure(rhsTableLen);
            }
            ForEachItemIn(a2, rhsNodeRows)
            {
                CThorSpillableRowArray &rows = *rhsNodeRows.item(a2);
                rowidx_t r=0;
                for (; r<rows.numCommitted(); r++)
                    rhs.append(rows.getClear(r));
                rows.kill(); // free up ptr table asap
            }
        }
        else
        {
            if (stopping) // if local can stop now
                return;
            if (RCUNSET != rhsTotalCount)
            {
                if (rhsTotalCount > RIMAX)
                    throw MakeActivityException(this, 0, "Too many rows on RHS for ALL join: %"RCPF"d", rhsTotalCount);
                rhsTableLen = (rowidx_t)rhsTotalCount;
                rhs.ensure(rhsTableLen);
            }
            while (!abortSoon)
            {
                OwnedConstThorRow row = right->ungroupedNextRow();
                if (!row)
                    break;
                rhs.append(row.getClear());
            }
            rhsTableLen = rhs.ordinality();
            if (rhsTotalCount != RCUNSET) // verify matches meta if set/calculated
                assertex(rhsTableLen == rhsTotalCount);
        }
        ActPrintLog("rhs table: %d elements", rhsTableLen);
        rhsTable = rhs.getRowArray();
    }
public:
    CAllJoinActivity(CGraphElementBase *_container) : CInMemJoinBase(_container)
    {
        allJoinHelper = (IHThorAllJoinArg *)queryHelper();
        flags = allJoinHelper->getJoinFlags();
        returnMany = true;
        keepLimit = allJoinHelper->getKeepLimit();
        fuzzyMatch = 0 != (JFmatchrequired & flags);
        grouped = allJoinHelper->queryOutputMeta()->isGrouped();
        rhsTable = NULL;
    }
// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CInMemJoinBase::init(data, slaveData);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);

        CInMemJoinBase::start();

        if ((flags & JFonfail) || (flags & JFleftouter))
        {
            RtlDynamicRowBuilder rr(rightAllocator);
            rr.ensureRow();
            size32_t rrsz = allJoinHelper->createDefaultRight(rr);
            defaultRight.setown(rr.finalizeRowClear(rrsz));
        }
        rhsTable = NULL;
    }
    virtual void stop()
    {
        if (!gotRHS && needGlobal)
            getRHS(true); // If global, need to handle RHS until all are slaves stop
        CInMemJoinBase::stop();
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
// IBCastReceive
    virtual void bCastReceive(CSendItem *sendItem)
    {
        rowProcessor.addBlock(sendItem); // NB: NULL indicates end
    }
};

// template to avoid virtual calls from lookupNextRow()
template <class BASE>
class CInMemJoinSlave : public BASE
{
    using BASE::left;
    using BASE::allocator;
    using BASE::rhsNext;
    using BASE::rhsTableLen;

    using BASE::setRhsNextFirst;
    using BASE::setRhsNext;
    using BASE::match;
    using BASE::joinTransform;

    using BASE::abortSoon;
    using BASE::exclude;
    using BASE::joined;
    using BASE::returnMany;
    using BASE::flags;
    using BASE::fuzzyMatch;
    using BASE::leftMatch;
    using BASE::nextRhsRow;
    using BASE::keepLimit;
    using BASE::defaultRight;

    OwnedConstThorRow leftRow;
    bool eos, eog, someSinceEog;

    inline void resetRhsNext()
    {
        nextRhsRow = 0;
        joined = 0;
        leftMatch = false;
    }
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
protected:
    using BASE::container;
public:
    CInMemJoinSlave(CGraphElementBase *container) : BASE(container)
    {
        eos = eog = someSinceEog = false;
    }
    virtual void start()
    {
        BASE::start();
        eos = eog = someSinceEog = false;
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
            setRhsNext(leftRow);
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
        if (!abortSoon && !eos)
        {
            loop
            {
                if (NULL == rhsNext)
                {
                    leftRow.setown(left->nextRow());
                    if (leftRow)
                    {
                        eog = false;
                        if (rhsTableLen)
                        {
                            resetRhsNext();
                            const void *onFailRow = setRhsNextFirst(leftRow);
                            if (onFailRow)
                                return onFailRow;
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
                                size32_t sz = joinTransform(rowBuilder, leftRow, rhsNext);
                                if (sz)
                                {
                                    OwnedConstThorRow row = rowBuilder.finalizeRowClear(sz);
                                    someSinceEog = true;
                                    if (++joined == keepLimit)
                                        rhsNext = NULL;
                                    else if (!returnMany)
                                        rhsNext = NULL;
                                    else
                                        setRhsNext(leftRow);
                                    return row.getClear();
                                }
                            }
                        }
                        setRhsNext(leftRow);
                    }
                    if (!leftMatch && NULL == rhsNext && 0!=(flags & JFleftouter))
                    {
                        size32_t sz = joinTransform(rowBuilder, leftRow, defaultRight);
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
};


class CLookupManyJoinSlaveActivity : public CInMemJoinSlave<CLookupManyJoinActivity>
{
public:
    CLookupManyJoinSlaveActivity(CGraphElementBase *container) : CInMemJoinSlave<CLookupManyJoinActivity>(container)
    {
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!gotRHS)
            getRHS(false);
        OwnedConstThorRow row;
        if (joinHelper) // regular join (hash join)
            row.setown(joinHelper->nextRow());
        else
            row.setown(lookupNextRow());
        if (!row.get())
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
};

class CLookupJoinSlaveActivity : public CInMemJoinSlave<CLookupJoinActivity>
{
public:
    CLookupJoinSlaveActivity(CGraphElementBase *container) : CInMemJoinSlave<CLookupJoinActivity>(container)
    {
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!gotRHS)
            getRHS(false);
        OwnedConstThorRow row;
        if (joinHelper) // regular join (hash join)
            row.setown(joinHelper->nextRow());
        else
            row.setown(lookupNextRow());
        if (!row.get())
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
};

class CAllJoinSlaveActivity : public CInMemJoinSlave<CAllJoinActivity>
{
public:
    CAllJoinSlaveActivity(CGraphElementBase *container) : CInMemJoinSlave<CAllJoinActivity>(container)
    {
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!gotRHS)
            getRHS(false);
        OwnedConstThorRow row = lookupNextRow();
        if (!row.get())
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
};

CActivityBase *createLookupJoinSlave(CGraphElementBase *container) 
{ 
    IHThorHashJoinArg *hashJoinHelper = (IHThorHashJoinArg *)container->queryHelper();
    unsigned flags = hashJoinHelper->getJoinFlags();
    if (JFmanylookup & flags)
    {
        bool dedup = false;
        if (0 == (flags & (JFtransformMaySkip|JFmatchrequired)))
        {
            unsigned keepLimit = hashJoinHelper->getKeepLimit();
            unsigned abortLimit = hashJoinHelper->getMatchAbortLimit();
            unsigned atMost = hashJoinHelper->getJoinLimit();
            dedup = (1==keepLimit) && (0==atMost) && (0==abortLimit);
        }
        if (dedup)
            return new CLookupJoinSlaveActivity(container);
    }
    return new CLookupManyJoinSlaveActivity(container);
}

CActivityBase *createAllJoinSlave(CGraphElementBase *container)
{
    return new CAllJoinSlaveActivity(container);
}


