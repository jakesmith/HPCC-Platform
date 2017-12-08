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

#include "jlib.hpp"
#include "limits.h"

#include "jbuff.hpp"
#include "jdebug.hpp"
#include "jio.hpp"
#include "jqueue.tpp"
#include "jset.hpp"
#include "jsort.hpp"

#include "thorcommon.ipp"

#include "dadfs.hpp"

#include "jhtree.hpp"

#include "sockfile.hpp"

#include "thorxmlwrite.hpp"

#include "thorport.hpp"
#include "thsortu.hpp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"
#include "thbufdef.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"

#include "slave.ipp"
#include "../fetch/thfetchcommon.hpp"
#include "thkeyedjoinslave.ipp"

#include <vector>
#include <atomic>

//#define TRACE_USAGE

//#define NEWFETCHSTRESS
#ifndef NEWFETCHSTRESS

#define NEWFETCHSENDMAX (0x100000*5)
#define NEWFETCHREPLYMAX (0x100000*5)
#define NEWFETCHPRMEMLIMIT (0x100000*5)
#define NEWFETCHPRBLOCKMEMLIMIT (0x100000*4)

#else

#define NEWFETCHSENDMAX 100
#define NEWFETCHREPLYMAX 50 // want to send back requests of N in chunks of <N
#define NEWFETCHPRMEMLIMIT 1 // low enough to cause 1-by-1

#endif // NEWFETCHSTRESS

#define LOWTHROTTLE_GRANULARITY 10

#define DEFAULT_KEYLOOKUP_QUEUED_BATCHSIZE 100
#define DEFAULT_KEYLOOKUP_MAX_LOOKUP_BATCHSIZE 1000
#define DEFAULT_KEYLOOKUP_MAX_THREADS 10
#define DEFAULT_KEYLOOKUP_MAX_QUEUED 10000

class CJoinGroup;
struct FetchRequestHeader
{
    offset_t fpos;
    CJoinGroup *jg;
    unsigned sequence;
};
struct FetchReplyHeader
{
    static const unsigned fetchMatchedMask = 0x80000000;
    CJoinGroup *jg;
    unsigned sequence; // fetchMatchedMask used to screen top-bit to denote whether reply fetch matched or not
};
struct KeyLookupHeader
{
    CJoinGroup *jg;
};
template <class HeaderStruct>
void getHeaderFromRow(const void *row, HeaderStruct &header)
{
    memcpy(&header, row, sizeof(HeaderStruct));
}


interface IJoinProcessor
{
    virtual void onComplete(CJoinGroup * jg) = 0;
};

class CJoinGroup : public CSimpleInterfaceOf<IInterface>
{
protected:
    CActivityBase &activity;
    OwnedConstThorRow leftRow;
    CThorExpandingRowArray rows; // format of rows = { offset_t, rhs-row }
    Int64Array offsets; // JCSMORE - use roxiemem
    mutable CriticalSection crit;
    std::atomic<unsigned> pending{0}; // e.g. eog, or pending disk fetch triggers
    IJoinProcessor *join = nullptr;
    CJoinGroup *groupStart = nullptr;
    enum LimitType { limit_nothit, limit_atmost, limit_abort } limitHit = limit_nothit;

public:
    CJoinGroup *prev = nullptr;  // Doubly-linked list to allow us to keep track of ones that are still in use
    CJoinGroup *next = nullptr;

public:
    CJoinGroup(CActivityBase &_activity, const void *_leftRow, IJoinProcessor *_join, CJoinGroup *_groupStart)
        : activity(_activity), join(_join), groupStart(_groupStart), rows(_activity, nullptr, ers_allow)
    {
    	leftRow.set(_leftRow);
    }
#undef new
    void *operator new(size_t size, roxiemem::IRowManager *rowManager, activity_id activityId)
    {
        return rowManager->allocate(size, activityId);
    }
#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

    void operator delete(void *ptr, roxiemem::IRowManager *rowManager, activity_id activityId)
    {
        ReleaseRoxieRow(ptr);
    }
    void operator delete(void *ptr)
    {
        ReleaseRoxieRow(ptr);
    }
    inline void setLimitHit(LimitType type)
    {
        CriticalBlock b(crit);
        limitHit = type;
        offsets.kill();
        rows.kill(); // will not need rows anymore
    }
    inline bool hasAbortLimitBeenHit() const
    {
        return limit_abort == limitHit;
    }
    inline void setAbortLimitHit()
    {
        setLimitHit(limit_abort);
    }
    inline bool hasAtMostLimitBeenHit() const
    {
        return limit_atmost == limitHit;
    }
    inline void setAtMostLimitHit()
    {
        setLimitHit(limit_atmost);
    }
    inline const void *queryLeft() const
    {
        return leftRow;
    }
    inline const void *queryRightRow(unsigned idx, offset_t &fpos) const
    {
        CriticalBlock b(crit);
        fpos = offsets.item(idx);
        return rows.query(idx);
    }
    inline bool complete() const { return 0 == pending; }
    inline bool inGroup(CJoinGroup *leader) const
    {
        return groupStart==leader;
    }
    inline void incPending()
    {
        if (groupStart)
            groupStart->incPending();
        pending++;
    }
    inline void decPending()
    {
        if (groupStart)
        {
            --pending;
            groupStart->decPending();
        }
        else if (1 == pending.fetch_sub(1))
            join->onComplete(this);
    }
    inline unsigned addRightMatchPending(offset_t fpos)
    {
        CriticalBlock b(crit);
        offsets.append(fpos);
        return offsets.ordinality()-1;
    }
    inline void addRightMatchCompletePending(unsigned sequence, const void *right)
    {
        CriticalBlock b(crit);
        /* will normally naturally be in sequence, the exception is when fetch processor hands off to multiple targets
         * then join fields results can come back in an indeterminate order. 'sequence' ensures order preserved.
         */
        if (rows.ordinality() == sequence)
            rows.append(right);
        else if (rows.ordinality() < sequence)
        {
            do
            {
                rows.append(nullptr);
            }
            while (rows.ordinality() < sequence);
            rows.append(right);
        }
        else
        {
            dbgassertex(nullptr == rows.query(sequence));
            rows.setRow(sequence, right);
        }
    }
    inline void addRightMatch(const void *right, offset_t fpos)
    {
        dbgassertex(pending);
        CriticalBlock b(crit);
        rows.append(right);
        offsets.append(fpos);
    }
    inline unsigned numRhsMatches() const
    {
        CriticalBlock b(crit);
        return rows.ordinality();
    }
};

class CJoinGroupList
{
    CJoinGroup *head = nullptr, *tail = nullptr;

public:
    CJoinGroupList() { }
    ~CJoinGroupList()
    {
        while (head)
        {
            CJoinGroup *next = head->next;
            head->Release();
            head = next;
        }
    }
    inline CJoinGroup *queryHead() const { return head; }
    CJoinGroup *removeHead()
    {
        if (!head)
            return nullptr;
        CJoinGroup *next = head->next;
        CJoinGroup *ret = head;
        head = next;
        if (head)
            head->prev = nullptr;
        else
            tail = nullptr;
        return ret;
    }
    void remove(CJoinGroup *joinGroup)
    {
        CJoinGroup *prev = joinGroup->prev;
        CJoinGroup *next = joinGroup->next;
        if (joinGroup == tail) // implying next=null also
            tail = prev;
        else
            next->prev = prev;
        if (joinGroup == head) // implying prev=null also
            head = next;
        else
            prev->next = next;
        joinGroup->prev = nullptr;
        joinGroup->next = nullptr;
    }
    void addToTail(CJoinGroup *joinGroup)
    {
        if (!head)
            head = tail = joinGroup;
        else
        {
            tail->next = joinGroup;
            joinGroup->prev = tail;
            tail = joinGroup;
        }
    }
};

enum AdditionStats { AS_Seeks, AS_Scans, AS_Accepted, AS_PostFiltered, AS_PreFiltered,  AS_DiskSeeks, AS_DiskAccepted, AS_DiskRejected };

class CLeavableCriticalBlock // JCSMORE - move to jlib?
{
    CriticalSection &crit;
    bool locked = false;
public:
    inline CLeavableCriticalBlock(CriticalSection &_crit) : crit(_crit)
    {
        enter();
    }
    inline ~CLeavableCriticalBlock()
    {
        if (locked)
            crit.leave();
    }
    inline void enter()
    {
        crit.enter();
        locked = true;
    }
    inline void leave()
    {
        if (locked)
        {
            locked = false;
            crit.leave();
        }
    }
};


class CKeyedJoinSlave : public CSlaveActivity, implements IJoinProcessor
{
    typedef CSlaveActivity PARENT;

    class CKeyedFetchHandler : public CSimpleInterface, implements IThreaded
    {
        CKeyedJoinSlave &activity;
        CThreadedPersistent threaded;
        bool writeWaiting = false, replyWaiting = false, stopped = false, aborted = false;
        unsigned pendingSends = 0, pendingReplies = 0, nodes = 0, maxRequests = 0, blockRequestsAt = 0;
        size32_t minFetchSendSz = NEWFETCHSENDMAX, totalSz = 0;
        PointerArrayOf<CThorExpandingRowArray> dstLists;
        CriticalSection crit, sendCrit;
        Semaphore pendingSendsSem, pendingReplySem;
        mptag_t requestMpTag = TAG_NULL, resultMpTag = TAG_NULL;
        Owned<IEngineRowAllocator> fetchInputAllocator, fetchInputMetaAllocator;
        Owned<IThorRowInterfaces> fetchInputMetaRowIf; // fetch request rows, header + fetch fields
        Owned<IThorRowInterfaces> fetchOutputMetaRowIf; // fetch request reply rows, header + [join fields as child row]
        IHThorKeyedJoinArg *helper = nullptr;

        static int slaveLookup(const void *_key, const void *e)
        {
            offset_t key = *(offset_t *)_key;
            FPosTableEntry &entry = *(FPosTableEntry *)e;
            if (key < entry.base)
                return -1;
            else if (key >= entry.top)
                return 1;
            else
                return 0;
        }
        IFileIO *getFilePartIO(unsigned partNum)
        {
            assertex(partNum<activity.dataParts.ordinality());
            return activity.fetchFiles.item(partNum).getFileIO();
        }
    public:
        class CKeyedFetchResultProcessor : public CSimpleInterface, implements IThreaded
        {
            CKeyedFetchHandler &owner;
            CThreadedPersistent threaded;
            CKeyedJoinSlave &activity;
            ICommunicator &comm;
            mptag_t resultMpTag = TAG_NULL;
            bool aborted = false;
        public:
            CKeyedFetchResultProcessor(CKeyedFetchHandler &_owner, ICommunicator &_comm, mptag_t _mpTag) : threaded("CKeyedFetchResultProcessor", this), owner(_owner), comm(_comm), resultMpTag(_mpTag), activity(owner.activity)
            {
                aborted = false;
                threaded.start();
            }
            ~CKeyedFetchResultProcessor()
            {
                stop();
                threaded.join();
            }
            void stop()
            {
                if (!aborted)
                {
                    aborted = true;
                    comm.cancel(RANK_ALL, resultMpTag);
                }
            }
            bool done() const
            {
                return aborted;
            }
            virtual void threadmain() override
            {
                try
                {
                    unsigned endRequestsCount = activity.container.queryJob().querySlaves();
                    Owned<IBitSet> endRequests = createThreadSafeBitSet(); // NB: verification only
                    while (!aborted)
                    {
                        rank_t sender;
                        CMessageBuffer msg;
                        if (comm.recv(msg, RANK_ALL, resultMpTag, &sender))
                        {
                            if (!msg.length())
                            {
                                unsigned prevVal = endRequests->testSet(((unsigned)sender)-1);
                                assertex(0 == prevVal);
                                if (0 == --endRequestsCount) // wait for all processors to signal end
                                    break;
                                continue;
                            }
                            unsigned count;
                            msg.read(count);

                            CThorExpandingRowArray received(activity, owner.fetchOutputMetaRowIf);
                            size32_t recvSz = msg.remaining();
                            received.deserialize(recvSz, msg.readDirect(recvSz));

                            unsigned c=0, c2=0;
                            while (c<count && !aborted)
                            {
                                OwnedConstThorRow row = received.getClear(c++);
                                const FetchReplyHeader &header = *(FetchReplyHeader *)row.get();
                                CJoinGroup *jg = header.jg;
                                const void *childRow = *(const void **)((byte *)row.get()+sizeof(FetchReplyHeader));
                                if (header.sequence & FetchReplyHeader::fetchMatchedMask)
                                {
                                    if (jg->numRhsMatches() >= activity.abortLimit)
                                        jg->setAbortLimitHit(); // also clears existing rows
                                    else
                                    {
                                        LinkThorRow(childRow);
                                        jg->addRightMatchCompletePending(header.sequence & ~FetchReplyHeader::fetchMatchedMask, childRow);
                                    }
                                }
                                jg->decPending();
                                ++c2;
                                if (LOWTHROTTLE_GRANULARITY == c2) // prevent sender when busy waking up to send very few.
                                {
                                    owner.decPendingReplies(c2);
                                    c2 = 0;
                                }
                            }
                            if (c2)
                                owner.decPendingReplies(c2);
                        }
                    }
                }
                catch (IException *e)
                {
                    activity.fireException(e);
                    e->Release();
                }
                aborted = true;
            }
        } *resultProcessor = nullptr;
        class CKeyedFetchRequestProcessor : public CSimpleInterface, implements IThreaded
        {
            CKeyedFetchHandler &owner;
            CThreadedPersistent threaded;
            CKeyedJoinSlave &activity;
            ICommunicator &comm;
            mptag_t requestMpTag = TAG_NULL, resultMpTag = TAG_NULL;
            bool aborted = false;
            IHThorKeyedJoinArg *helper = nullptr;

            static int partLookup(const void *_key, const void *e)
            {
                FPosTableEntry &entry = *(FPosTableEntry *)e;
                offset_t keyFpos = *(offset_t *)_key;
                if (keyFpos < entry.base)
                    return -1;
                else if (keyFpos >= entry.top)
                    return 1;
                else
                    return 0;
            }

        public:
            CKeyedFetchRequestProcessor(CKeyedFetchHandler &_owner, ICommunicator &_comm, mptag_t _requestMpTag, mptag_t _resultMpTag) : threaded("CKeyedFetchRequestProcessor", this), owner(_owner), comm(_comm), requestMpTag(_requestMpTag), resultMpTag(_resultMpTag), activity(_owner.activity)
            {
                aborted = false;
                helper = owner.helper;
                threaded.start();
            }
            ~CKeyedFetchRequestProcessor()
            {
                stop();
                threaded.join();
            }
            void stop()
            {
                if (!aborted)
                {
                    aborted = true;
                    comm.cancel(RANK_ALL, requestMpTag);
                }
            }
            virtual void threadmain() override
            {
                try
                {
                    rank_t sender;
                    CMessageBuffer msg;
                    unsigned endRequestsCount = activity.container.queryJob().querySlaves();
                    Owned<IBitSet> endRequests = createThreadSafeBitSet(); // NB: verification only

                    Owned<IThorRowInterfaces> fetchDiskRowIf = activity.createRowInterfaces(helper->queryDiskRecordSize());
                    while (!aborted)
                    {
                        CMessageBuffer replyMb;
                        unsigned retCount = 0;
                        DelayedMarker<unsigned> countMarker(replyMb);
                        if (comm.recv(msg, RANK_ALL, requestMpTag, &sender))
                        {
                            if (!msg.length())
                            {
                                unsigned prevVal = endRequests->testSet(((unsigned)sender)-1);
                                assertex(0 == prevVal);
                                // I will not get anymore from 'sender', tell sender I've processed all and there will be no more results.
                                if (!comm.send(msg, sender, resultMpTag, LONGTIMEOUT))
                                    throw MakeActivityException(&activity, 0, "CKeyedFetchRequestProcessor {3} - comm send failed");
                                if (0 == --endRequestsCount)
                                    break;
                                continue;
                            }
                            unsigned count;
                            msg.read(count);

                            CThorExpandingRowArray received(activity, owner.fetchInputMetaRowIf);
                            CThorExpandingRowArray replyRows(activity, owner.fetchOutputMetaRowIf);
                            size32_t recvSz =  msg.remaining();
                            received.deserialize(recvSz, msg.readDirect(recvSz));
                            size32_t replySz = 0;
                            unsigned c = 0;
                            while (count--)
                            {
#ifdef TRACE_JOINGROUPS
                                activity.fetchReadBack++;
#endif
                                if (aborted)
                                    break;
                                OwnedConstThorRow row = received.getClear(c++);
                                FetchRequestHeader &requestHeader = *(FetchRequestHeader *)row.get();
                                const offset_t &fpos = requestHeader.fpos;

                                const void *fetchKey = nullptr;
                                if (owner.fetchInputAllocator)
                                    fetchKey = (const byte *)row.get() + sizeof(FetchRequestHeader);

                                unsigned __int64 localFpos;
                                unsigned files = activity.dataParts.ordinality();
                                unsigned filePartIndex = 0;
                                switch (files)
                                {
                                    case 0:
                                        assertex(false);
                                    case 1:
                                    {
                                        if (isLocalFpos(fpos))
                                            localFpos = getLocalFposOffset(fpos);
                                        else
                                            localFpos = fpos-activity.localFPosToNodeMap[0].base;
                                        break;
                                    }
                                    default:
                                    {
                                        // which of multiple parts this slave is dealing with.
                                        FPosTableEntry *result = (FPosTableEntry *)bsearch(&fpos, activity.localFPosToNodeMap, files, sizeof(FPosTableEntry), partLookup);
                                        if (isLocalFpos(fpos))
                                            localFpos = getLocalFposOffset(fpos);
                                        else
                                            localFpos = fpos-result->base;
                                        filePartIndex = result->index;
                                        break;
                                    }
                                }

                                RtlDynamicRowBuilder fetchReplyBuilder(owner.fetchOutputMetaRowIf->queryRowAllocator());
                                FetchReplyHeader &replyHeader = *(FetchReplyHeader *)fetchReplyBuilder.getUnfinalized();
                                replyHeader.sequence = requestHeader.sequence;
                                replyHeader.jg = requestHeader.jg;
                                const void * &childRow = *(const void **)((byte *)fetchReplyBuilder.getUnfinalized() + sizeof(FetchReplyHeader));

                                Owned<IFileIO> iFileIO = owner.getFilePartIO(filePartIndex);
                                Owned<ISerialStream> stream = createFileSerialStream(iFileIO, localFpos);
                                CThorStreamDeserializerSource ds(stream);

                                RtlDynamicRowBuilder fetchDiskRowBuilder(fetchDiskRowIf->queryRowAllocator());
                                size32_t fetchedLen = fetchDiskRowIf->queryRowDeserializer()->deserialize(fetchDiskRowBuilder, ds);
                                OwnedConstThorRow diskFetchRow = fetchDiskRowBuilder.finalizeRowClear(fetchedLen);

                                size32_t fetchReplySz = sizeof(FetchReplyHeader);
                                if (helper->fetchMatch(fetchKey, diskFetchRow))
                                {
                                    replyHeader.sequence |= FetchReplyHeader::fetchMatchedMask;

                                    RtlDynamicRowBuilder joinFieldsRow(activity.joinFieldsAllocator);
                                    size32_t joinFieldsSz = helper->extractJoinFields(joinFieldsRow, diskFetchRow.get(), (IBlobProvider*)nullptr); // JCSMORE is it right that passing NULL IBlobProvider here??
                                    fetchReplySz += joinFieldsSz;
                                    childRow = joinFieldsRow.finalizeRowClear(joinFieldsSz); // NB: COutputMetaWithChildRow handles serializing/deserializing the child row
                                    activity.statsArr[AS_DiskAccepted]++;
                                }
                                else
                                {
                                    childRow = nullptr;
                                    activity.statsArr[AS_DiskRejected]++;
                                }

                                replySz += fetchReplySz;
                                replyRows.append(fetchReplyBuilder.finalizeRowClear(fetchReplySz));
                                activity.statsArr[AS_DiskSeeks]++;
                                ++retCount;
                                if (replySz>=NEWFETCHREPLYMAX) // send back in chunks
                                {
                                    countMarker.write(retCount);
                                    retCount = 0;
                                    replySz = 0;
                                    replyRows.serialize(replyMb);
                                    replyRows.kill();
                                    if (!comm.send(replyMb, sender, resultMpTag, LONGTIMEOUT))
                                        throw MakeActivityException(&activity, 0, "CKeyedFetchRequestProcessor {1} - comm send failed");
                                    replyMb.rewrite(sizeof(retCount));
                                }
                            }
                            if (retCount)
                            {
                                countMarker.write(retCount);
                                retCount = 0;
                                replyRows.serialize(replyMb);
                                replyRows.kill();
                                if (!comm.send(replyMb, sender, resultMpTag, LONGTIMEOUT))
                                    throw MakeActivityException(&activity, 0, "CKeyedFetchRequestProcessor {2} - comm send failed");
                                replyMb.rewrite(sizeof(retCount));
                            }
                        }
                    }
                }
                catch (IException *e)
                {
                    activity.fireException(e);
                    e->Release();
                }
                aborted = true;
            }
        } *requestProcessor = nullptr;
    public:
        CKeyedFetchHandler(CKeyedJoinSlave &_activity) : threaded("CKeyedFetchHandler", this), activity(_activity)
        {
            helper = activity.helper;
            if (0 != helper->queryFetchInputRecordSize()->getMinRecordSize())
                fetchInputAllocator.setown(activity.getRowAllocator(helper->queryFetchInputRecordSize()));
            Owned<IOutputMetaData> fetchInputMeta = new CPrefixedOutputMeta(sizeof(FetchRequestHeader), helper->queryFetchInputRecordSize());
            fetchInputMetaRowIf.setown(activity.createRowInterfaces(fetchInputMeta));
            fetchInputMetaAllocator.set(fetchInputMetaRowIf->queryRowAllocator());

            Owned<IOutputMetaData> fetchOutputMeta = createOutputMetaDataWithChildRow(activity.joinFieldsAllocator, sizeof(FetchReplyHeader));
            fetchOutputMetaRowIf.setown(activity.createRowInterfaces(fetchOutputMeta));

            totalSz = 0;
            if (minFetchSendSz < fetchInputMeta->getMinRecordSize())
                minFetchSendSz = fetchInputMeta->getMinRecordSize();
            nodes = activity.container.queryJob().querySlaves();
            stopped = aborted = writeWaiting = replyWaiting = false;
            pendingSends = pendingReplies = 0;
            for (unsigned n=0; n<nodes; n++)
                dstLists.append(new CThorExpandingRowArray(activity, fetchInputMetaRowIf));
            size32_t fetchMin = helper->queryJoinFieldsRecordSize()->getMinRecordSize();
            size32_t perRowMin = sizeof(FetchReplyHeader)+fetchMin;
            maxRequests = NEWFETCHPRMEMLIMIT<perRowMin ? 1 : (NEWFETCHPRMEMLIMIT / perRowMin);
            blockRequestsAt = NEWFETCHPRBLOCKMEMLIMIT<perRowMin ? 1 : (NEWFETCHPRBLOCKMEMLIMIT / perRowMin);
            assertex(blockRequestsAt<=maxRequests);

            requestMpTag = (mptag_t)activity.tags.popGet();
            resultMpTag = (mptag_t)activity.tags.popGet();
            requestProcessor = new CKeyedFetchRequestProcessor(*this, activity.queryJobChannel().queryJobComm(), requestMpTag, resultMpTag); // remote receive of fetch fpos'
            resultProcessor = new CKeyedFetchResultProcessor(*this, activity.queryJobChannel().queryJobComm(), resultMpTag); // asynchronously receiving results back

            threaded.start();
        }
        ~CKeyedFetchHandler()
        {
            abort();
            threaded.join();

            ::Release(requestProcessor);
            ::Release(resultProcessor);
            ForEachItemIn(l, dstLists)
            {
                CThorExpandingRowArray *dstList = dstLists.item(l);
                delete dstList;
            }
        }
        bool resultsDone() const
        {
            return resultProcessor->done();
        }
        void addRow(offset_t fpos, unsigned sequence, CJoinGroup *jg)
        {
            // build request row
            RtlDynamicRowBuilder fetchInputRowBuilder(fetchInputMetaAllocator);
            FetchRequestHeader &header = *(FetchRequestHeader *)fetchInputRowBuilder.getUnfinalized();
            header.fpos = fpos;
            header.sequence = sequence;
            header.jg = jg;

            size32_t sz = sizeof(FetchRequestHeader);
            if (fetchInputAllocator)
            {
                CPrefixedRowBuilder prefixBuilder(sizeof(FetchRequestHeader), fetchInputRowBuilder);
                sz += helper->extractFetchFields(prefixBuilder, jg->queryLeft());
            }
            OwnedConstThorRow fetchInputRow = fetchInputRowBuilder.finalizeRowClear(sz);

            if (totalSz + sz > minFetchSendSz)
            {
                sendAll(); // in effect send remaining
                totalSz = 0;
            }
            unsigned dstNode;
            if (activity.remoteDataFiles)
                dstNode = activity.node; // JCSMORE - do directly
            else
            {
                if (1 == activity.filePartTotal)
                    dstNode = activity.globalFPosToNodeMap[0].index;
                else if (isLocalFpos(fpos))
                    dstNode = getLocalFposPart(fpos);
                else
                {
                    const void *result = bsearch(&fpos, activity.globalFPosToNodeMap, activity.filePartTotal, sizeof(FPosTableEntry), slaveLookup);
                    if (!result)
                        throw MakeThorException(TE_FetchOutOfRange, "FETCH: Offset not found in offset table; fpos=%" I64F "d", fpos);
                    dstNode = ((FPosTableEntry *)result)->index;
                }
            }

            {
                CriticalBlock b(crit);
                //must be easier way?
                dstLists.item(dstNode)->append(fetchInputRow.getClear());
                totalSz += sz;
                ++pendingSends;
                if (writeWaiting)
                {
                    writeWaiting = false;
                    pendingSendsSem.signal();
                }
            }
        }
        void stop(bool stopPending)
        {
            CLeavableCriticalBlock b(crit);
            if (!stopped)
            {
                stopped = true;
                if (writeWaiting)
                {
                    writeWaiting = false;
                    pendingSendsSem.signal();
                }
                b.leave();
                threaded.join();
                if (stopPending) // stop groups in progress
                {
                    if (aborted) // don't stop request processor unless aborting, other nodes may depend on it's reply.
                        requestProcessor->stop();
                    resultProcessor->stop();
                }
            }
        }
        void abort()
        {
            if (!aborted)
            {
                aborted = true;
                stop(true);
            }
        }
        void sendStop()
        {
            // signal to request processor that *this* node isn't going to send anymore
            unsigned n=0;
            for (; n<nodes; n++)
            {
                CMessageBuffer msg;
                if (!activity.queryJobChannel().queryJobComm().send(msg, n+1, requestMpTag, LONGTIMEOUT))
                    throw MakeActivityException(&activity, 0, "CKeyedFetchHandler::stop - comm send failed");
            }
        }
        void decPendingReplies(unsigned c=1)
        {
            CriticalBlock b(crit);
            assertex(pendingReplies >= c);
            pendingReplies -= c;
            if (replyWaiting)
            {
                replyWaiting=false;
                pendingReplySem.signal();
            }
        }
        void sendAll()
        {
            CriticalBlock b(sendCrit); // want to block here, if other is replyWaiting
            unsigned n=0;
            for (; n<nodes; n++)
            {
                if (aborted)
                    return;
                CMessageBuffer msg;
                {
                    CriticalBlock b(crit); // keep writer out during flush to this dstNode
                    unsigned total = dstLists.item(n)->ordinality();
                    if (total)
                    {
                        assertex(!replyWaiting);
                        CThorExpandingRowArray dstList(activity, fetchInputMetaRowIf);
                        unsigned dstP=0;
                        for (;;)
                        {
                            // delay if more than max or if few sends and growing # of replies
                            bool throttleBig = pendingReplies >= blockRequestsAt;
                            for (;;)
                            {
                                if (!throttleBig)
                                {
                                    bool throttleSmall = (pendingSends <= LOWTHROTTLE_GRANULARITY) && (pendingReplies >= LOWTHROTTLE_GRANULARITY*2);
                                    if (!throttleSmall)
                                        break;
                                }

                                replyWaiting = true;
                                { CriticalUnblock ub(crit);
                                    while (!pendingReplySem.wait(5000))
                                    {
                                        activity.ActPrintLog("KJ: replyWaiting blocked");
                                    }
                                }
                                if (throttleBig) // break out if some received and reason for blocking was high number of pendingReplies.
                                    break;
                            }
                            if (aborted)
                                return;
                            if (0 == dstP) // delay detach until necessary as may have been blocked and more added.
                            {
                                dstList.swap(*dstLists.item(n));
                                total = dstList.ordinality();
                            }
                            unsigned requests = maxRequests - pendingReplies;
                            assertex(requests);
                            if (total < requests)
                                requests = total;
                            msg.append(requests);
                            unsigned r=0;
                            IOutputRowSerializer *serializer = fetchInputMetaRowIf->queryRowSerializer();
                            CMemoryRowSerializer s(msg);
                            for (; r<requests; r++)
                            {
                                OwnedConstThorRow row = dstList.getClear(dstP++);
                                serializer->serialize(s,(const byte *)row.get());
                            }
                            pendingSends -= requests;
                            pendingReplies += requests;
                            total -= requests;
                            { CriticalUnblock ub(crit);
                                if (!activity.queryJobChannel().queryJobComm().send(msg, n+1, requestMpTag, LONGTIMEOUT))
                                    throw MakeActivityException(&activity, 0, "CKeyedFetchHandler - comm send failed");
                            }
                            if (0 == total)
                                break;
                            msg.clear();
                        }
                    }
                }
            }
        }
    // IThreaded
        virtual void threadmain() override
        {
            try
            {
                CMessageBuffer msg;
                for (;;)
                {
                    crit.enter();
                    if (aborted || stopped)
                    {
                        crit.leave();
                        break;
                    }
                    if (0 == pendingSends)
                    {
                        writeWaiting = true;
                        crit.leave();
                        pendingSendsSem.wait();
                    }
                    else
                        crit.leave();
                    sendAll();
                }
                if (!aborted)
                {
                    sendAll();
                    sendStop();
                }
            }
            catch (IException *e)
            {
                activity.fireException(e);
                e->Release();
            }
        }
    friend class CKeyedFetchRequestProcessor;
    friend class CKeyedFetchResultProcessor;
    };
    Owned<CKeyedFetchHandler> fetchHandler;
    // There is 1 of these per part, but # running is limited
    class CKeyLookupHandler : public CSimpleInterface, implements IThreaded
    {
        CKeyedJoinSlave &activity;
        CThorExpandingRowArray queue;
        CriticalSection queueCrit, batchCrit;
        unsigned partNo = NotFound;
        Owned<IKeyManager> keyManager;
        CThreaded threaded;
        std::atomic<bool> running{false};
        std::atomic<bool> stopping{false};
        enum ThreadStates { ts_initial, ts_starting, ts_running, ts_stopping };
        ThreadStates state = ts_initial;
        IHThorKeyedJoinArg *helper = nullptr;
        CThorExpandingRowArray batchArray;
        bool stopped = false;
    public:
        CKeyLookupHandler(CKeyedJoinSlave &_activity, unsigned _partNo) : activity(_activity), partNo(_partNo), queue(_activity), threaded("CKeyLookupHandler", this), batchArray(_activity)
        {
            helper = activity.helper;
        }
        ~CKeyLookupHandler()
        {
            stop();
        }
        void stop()
        {
            stopped = true;
            threaded.join();
            queue.clearRows();
            batchArray.clearRows();
        }
        void init()
        {
            stopped = false;
            state = ts_initial;
            keyManager.setown(activity.getPartKeyManager(partNo));
        }
        void enqueue(CThorExpandingRowArray &newItems) // NB: enqueue starts thread
        {
            if (!activity.addOrBlock(newItems))
                return; // aborted

            CLeavableCriticalBlock b(queueCrit);
            queue.appendRows(newItems, true);
            do
            {
                if (state == ts_running) // as long as running here, we know thread will process queue
                    break;
                else if (state == ts_starting) // then another thread is dealing with transition (could be blocked in incRunningLookup())
                    break;
                else if (state == ts_initial)
                {
                    state = ts_starting;
                    b.leave();
                    activity.incRunningLookups(); // blocks if hit lookup thread limit
                    if (activity.abortSoon)
                        return;
                    threaded.start();
                    break;
                }
                else if (state == ts_stopping)
                {
                    state = ts_initial;
                    b.leave();
                    // stopping/stopped
                    threaded.join(); // must be sure finished
                    activity.decRunningLookups(); // unblocks any requests to start lookup threads
                    b.enter();
                    // cycle around to start thread again, or bail out if someone else already has.
                }
            }
            while (true);
        }
        void process(CThorExpandingRowArray &processing)
        {
            // NB: TBD This is where remote batch key filtering would be sent/received/processed.
            for (unsigned r=0; r<processing.ordinality() && !stopped; r++)
            {
                OwnedConstThorRow row = processing.getClear(r);
                CJoinGroup *joinGroup = *(CJoinGroup **)row.get();

                const void *keyedFieldsRow = (byte *)row.get() + sizeof(KeyLookupHeader);
                helper->createSegmentMonitors(keyManager, keyedFieldsRow);
                keyManager->finishSegmentMonitors();
                keyManager->reset();
                unsigned hardMatchCandidates = 0;

                // NB: keepLimit is not on hard matches and can only be applied later, since other filtering (e.g. in transform) may keep below keepLimit
                while (keyManager->lookup(true))
                {
                    hardMatchCandidates++;
                    if (hardMatchCandidates > activity.abortLimit)
                    {
                        joinGroup->setAbortLimitHit(); // also clears existing rows
                        break;
                    }
                    else if (hardMatchCandidates > activity.atMost) // atMost - filter out group if > max hard matches
                    {
                        joinGroup->setAtMostLimitHit(); // also clears existing rows
                        break;
                    }
                    KLBlobProviderAdapter adapter(keyManager);
                    byte const * keyRow = keyManager->queryKeyBuffer();
                    size_t fposOffset = keyManager->queryRowSize() - sizeof(offset_t);
                    offset_t fpos = rtlReadBigUInt8(keyRow + fposOffset);
                    if (helper->indexReadMatch(keyedFieldsRow, keyRow,  &adapter))
                    {
                        if (activity.needsDiskRead)
                        {
                            joinGroup->incPending();
                            unsigned sequence = joinGroup->addRightMatchPending(fpos);
                            activity.fetchHandler->addRow(fpos, sequence, joinGroup);
                        }
                        else
                        {
                            RtlDynamicRowBuilder joinFieldsRowBuilder(activity.joinFieldsAllocator);
                            size32_t sz = activity.helper->extractJoinFields(joinFieldsRowBuilder, keyRow, &adapter);
                            const void *joinFieldsRow = joinFieldsRowBuilder.finalizeRowClear(sz);
                            joinGroup->addRightMatch(joinFieldsRow, fpos);
                        }
                    }
                }
                keyManager->releaseSegmentMonitors();
                joinGroup->decPending(); // Every queued lookup row triggered an inc., this is the corresponding dec.
            }
        }
        void queueLookup(const void *keyedFieldsRowWithJGPrefix)
        {
            LinkThorRow(keyedFieldsRowWithJGPrefix);
            CLeavableCriticalBlock b(batchCrit);
            batchArray.append(keyedFieldsRowWithJGPrefix);
            if (batchArray.ordinality() >= activity.keyLookupQueuedBatchSize)
            {
                CThorExpandingRowArray sendPartQueue(activity);
                batchArray.swap(sendPartQueue);
                b.leave(); // unblock crit asap, before starting pool thread
                enqueue(sendPartQueue);
            }
        }
        void flush()
        {
            CThorExpandingRowArray sendPartQueue(activity);
            {
                CriticalBlock b(batchCrit);
                batchArray.swap(sendPartQueue);
            }
            enqueue(sendPartQueue);
        }
        void join()
        {
        	threaded.join();
        }
    // IThreaded
        virtual void threadmain() override
        {
            CThorExpandingRowArray processing(activity);

            {
                CriticalBlock b(queueCrit);
                if (0 == queue.ordinality())
                    return;
                assertex(state == ts_starting);
                state = ts_running; // only this thread can transition between ts_starting and ts_running
                if (queue.ordinality() <= activity.keyLookupMaxLookupBatchSize)
                    queue.swap(processing);
                else
                    queue.transferRows(0, activity.keyLookupMaxLookupBatchSize, processing);
            }
            do
            {
                process(processing);
                activity.decAndRelease(processing.ordinality());

                {
                    CriticalBlock b(queueCrit);
                    if (0 == queue.ordinality())
                    {
                        assertex(state == ts_running);
                        state = ts_stopping; // only this thread can transition between ts_running and ts_stopping
                        break;
                    }
                    processing.clearRows();
                    if (queue.ordinality() <= activity.keyLookupMaxLookupBatchSize)
                        queue.swap(processing);
                    else
                        queue.transferRows(0, activity.keyLookupMaxLookupBatchSize, processing);
                }
            }
            while (true);
        }
    };
    class CLookupThread : implements IThreaded
    {
        CKeyedJoinSlave &owner;
        CThreaded threaded;
    public:
        CLookupThread(CKeyedJoinSlave &_owner) : owner(_owner), threaded("CLookupThread", this)
        {
        }
        void start()
        {
            threaded.start();
        }
        void join()
        {
            threaded.join();
        }
        void stop()
        {
            owner.stopReadAhead();
            join();
        }
    // IThreaded
        virtual void threadmain() override
        {
            owner.readAhead(); // can block
        }
    } lookupThread;

    IHThorKeyedJoinArg *helper = nullptr;
    StringAttr indexName;
    size32_t fixedRecordSize = 0;
    bool localKey = false;
    bool initialized = false;
    bool preserveGroups = false, preserveOrder = false;
    bool needsDiskRead = false;
    bool onFailTransform = false;
    bool keyHasTlk = false;
    UnsignedArray tags;
    std::vector<std::atomic<unsigned __int64>> statsArr; // (seeks, scans, accepted, prefiltered, postfiltered, diskSeeks, diskAccepted, diskRejected)

    bool remoteDataFiles = false;
    SafePointerArrayOf<CKeyLookupHandler> lookupThreads;
    Semaphore keyLookupThreadsBusy, keyLookupExcessiveQueued;
    unsigned blockedStartingLookups = 0;
    unsigned runningLookupThreads = 0;
    rowcount_t totalQueuedLookupRowCount = 0;
    unsigned blockedKeyLookupThreads = 0;

    CPartDescriptorArray dataParts;
    PointerArrayOf<IPartDescriptor> indexParts;
    IArrayOf<IKeyIndex> tlkKeyIndexes;
    Owned<IEngineRowAllocator> joinFieldsAllocator;
    OwnedConstThorRow defaultRight;
    unsigned node = 0;
    unsigned joinFlags = 0;
    unsigned filePartTotal = 0;
    unsigned superWidth = 0;
    IArrayOf<IDelayedFile> fetchFiles;
    OwnedMalloc<FPosTableEntry> localFPosToNodeMap; // maps fpos->local part #
    OwnedMalloc<FPosTableEntry> globalFPosToNodeMap; // maps fpos->node for all parts of file. If file is remote, localFPosToNodeMap will have all parts
    Owned<IExpander> eexp;

    unsigned atMost = 0, keepLimit = 0;
    unsigned abortLimit = 0;
    rowcount_t rowLimit = 0;
    Linked<IHThorArg> inputHelper;
    unsigned keyLookupQueuedBatchSize = DEFAULT_KEYLOOKUP_QUEUED_BATCHSIZE;
    unsigned keyLookupMaxLookupBatchSize = DEFAULT_KEYLOOKUP_MAX_LOOKUP_BATCHSIZE;
    unsigned keyLookupMaxThreads = DEFAULT_KEYLOOKUP_MAX_THREADS;
    unsigned keyLookupMaxQueued = DEFAULT_KEYLOOKUP_MAX_QUEUED;

    Owned<IEngineRowAllocator> keyLookupRowWithJGAllocator, transformAllocator;
    CJoinGroup *currentPendingLhsGroupHead = nullptr;
    bool endOfInput = false; // marked true when input exhausted, but may be groups in flight
    bool eos = false; // marked true when everything processed
    IArrayOf<IKeyManager> tlkKeyManagers;
    CriticalSection onCompleteCrit, queuedCrit, runningLookupThreadsCrit;
    std::atomic<bool> waitingForDoneGroups{false};
    Semaphore waitingForDoneGroupsSem;
    CJoinGroupList pendingJoinGroupList, doneJoinGroupList;
    Owned<IException> abortLimitException;
    Owned<CJoinGroup> currentJoinGroup;
    CJoinGroup *currentActiveLhsGroupHead = nullptr;
    unsigned currentMatchIdx = 0;
    roxiemem::IRowManager *rowManager = nullptr;
    unsigned currentAdded = 0;
    unsigned currentJoinGroupSize = 0;
    MapBetween<unsigned, unsigned, unsigned, unsigned> globalPartNoMap; // for local kj, maps global partNo (in TLK) to locally handled part index numbers

    void doAbortLimit(CJoinGroup *jg)
    {
        helper->onMatchAbortLimitExceeded();
        CommonXmlWriter xmlwrite(0);
        if (inputHelper && inputHelper->queryOutputMeta() && inputHelper->queryOutputMeta()->hasXML())
            inputHelper->queryOutputMeta()->toXML((byte *) jg->queryLeft(), xmlwrite);
        throw MakeActivityException(this, 0, "More than %d match candidates in keyed join for row %s", abortLimit, xmlwrite.str());
    }
    bool checkAbortLimit(CJoinGroup *joinGroup)
    {
        if (joinGroup->hasAbortLimitBeenHit())
        {
            if (0 == (joinFlags & JFmatchAbortLimitSkips))
                doAbortLimit(joinGroup);
            return true;
        }
        return false;
    }
    bool abortLimitAction(CJoinGroup *jg, OwnedConstThorRow &row)
    {
        Owned<IException> abortLimitException;
        try
        {
            return checkAbortLimit(jg);
        }
        catch (IException *_e)
        {
            if (!onFailTransform)
                throw;
            abortLimitException.setown(_e);
        }
        RtlDynamicRowBuilder trow(queryRowAllocator());
        size32_t transformedSize = helper->onFailTransform(trow, jg->queryLeft(), defaultRight, 0, abortLimitException.get());
        if (0 != transformedSize)
            row.setown(trow.finalizeRowClear(transformedSize));
        return true;
    }
    bool addOrBlock(CThorExpandingRowArray &newItems)
    {
        do
        {
            {
                CriticalBlock b(queuedCrit);
                if (totalQueuedLookupRowCount < keyLookupMaxQueued) // NB: permits it to exceed keyLookupMaxQueued by newItems count.
                {
                    totalQueuedLookupRowCount += newItems.ordinality();
                    return true;
                }
                blockedKeyLookupThreads++;
            }
            keyLookupExcessiveQueued.wait();
            if (abortSoon)
                return false;
        }
        while (true);
        return true;
    }
    void decAndRelease(unsigned count)
    {
        CriticalBlock b(queuedCrit);
        totalQueuedLookupRowCount -= count;
        if (totalQueuedLookupRowCount < keyLookupMaxQueued)
        {
            if (blockedKeyLookupThreads)
            {
                unsigned blockedCount = blockedKeyLookupThreads;
                blockedKeyLookupThreads = 0;
                keyLookupExcessiveQueued.signal(blockedCount);
            }
        }
    }
    void incRunningLookups()
    {
        {
            CriticalBlock b(runningLookupThreadsCrit);
            if (runningLookupThreads < keyLookupMaxThreads)
            {
                ++runningLookupThreads;
                return;
            }
            blockedStartingLookups++;
        }
        keyLookupThreadsBusy.wait();
    }
    void decRunningLookups()
    {
        CriticalBlock b(runningLookupThreadsCrit);
        --runningLookupThreads;
        if (blockedStartingLookups)
        {
            keyLookupThreadsBusy.signal(blockedStartingLookups);
            blockedStartingLookups = 0;
        }
    }
    void queueLookupForPart(unsigned partNo, const void *keyedFieldsRowWithJGPrefix)
    {
        KeyLookupHeader lookupKeyHeader;
        getHeaderFromRow(keyedFieldsRowWithJGPrefix, lookupKeyHeader);
        lookupKeyHeader.jg->incPending(); // each queued lookup pending a result

        /* NB: there is 1 batchLookupArray/Thread per part
         * There could be >1 per part, but I'm not sure there's a lot of point.
         */
        CKeyLookupHandler &lookupThread = *lookupThreads.item(partNo);
        lookupThread.queueLookup(keyedFieldsRowWithJGPrefix);
    }
    void flushPartLookups()
    {
    	ForEachItemIn(b, lookupThreads)
    	{
    	    CKeyLookupHandler &lookupThread = *lookupThreads.item(b);
    	    lookupThread.flush();
    	}
    }
    unsigned getTlkKeyManagers(IArrayOf<IKeyManager> &keyManagers)
    {
        keyManagers.clear();
        ForEachItemIn(i, tlkKeyIndexes)
        {
            IKeyIndex *tlkKeyIndex = &tlkKeyIndexes.item(i);
            const RtlRecord &keyRecInfo = helper->queryIndexRecordSize()->queryRecordAccessor(true);
            Owned<IKeyManager> tlkManager = createLocalKeyManager(keyRecInfo, nullptr, nullptr);
            tlkManager->setKey(tlkKeyIndex);
            keyManagers.append(*tlkManager.getClear());
        }
        return tlkKeyIndexes.ordinality();
    }
    IKeyManager *getPartKeyManager(unsigned which)
    {
        IPartDescriptor *filePart = indexParts.item(which);
        assertex(filePart);
        unsigned crc=0;
        filePart->getCrc(crc);
        RemoteFilename rfn;
        filePart->getFilename(0, rfn);
        StringBuffer filename;
        rfn.getPath(filename);

        Owned<IDelayedFile> lfile = queryThor().queryFileCache().lookup(*this, indexName, *filePart);

        Owned<IKeyIndex> keyIndex = createKeyIndex(filename, crc, *lfile, false, false);
        return createLocalKeyManager(helper->queryIndexRecordSize()->queryRecordAccessor(true), keyIndex, nullptr);
    }
    const void *preparePendingLookupRow(void *row, size32_t maxSz, const void *lhsRow, size32_t keySz)
    {
        CJoinGroup *jg = new (rowManager, queryId()) CJoinGroup(*this, lhsRow, this, currentPendingLhsGroupHead);
        memcpy(row, &jg, sizeof(CJoinGroup *)); // NB: row will release joinGroup on destruction
        jg->incPending(); // prevent complete, must be an paired decPending() at some point
        return keyLookupRowWithJGAllocator->finalizeRow(sizeof(KeyLookupHeader)+keySz, row, maxSz);
    }
    CJoinGroup *queueLookup(const void *lhsRow)
    {
        RtlDynamicRowBuilder keyFieldsRowBuilder(keyLookupRowWithJGAllocator);
        CPrefixedRowBuilder keyFieldsPrefixBuilder(sizeof(KeyLookupHeader), keyFieldsRowBuilder);
        size32_t keyedFieldsRowSize = helper->extractIndexReadFields(keyFieldsPrefixBuilder, lhsRow);
        OwnedConstThorRow keyedFieldsRowWithJGPrefix;
        const void *keyedFieldsRow = keyFieldsPrefixBuilder.row();
        if (keyHasTlk)
        {
            ForEachItemIn(whichKm, tlkKeyManagers)
            {
                IKeyManager &keyManager = tlkKeyManagers.item(whichKm);
                helper->createSegmentMonitors(&keyManager, keyedFieldsRow);
                keyManager.finishSegmentMonitors();
                keyManager.reset();
                while (keyManager.lookup(false))
                {
                    offset_t node = extractFpos(&keyManager);
                    if (node) // don't bail out if part0 match, test again for 'real' tlk match.
                    {
                        unsigned partNo = (unsigned)node;
                        partNo = superWidth ? superWidth*whichKm+(partNo-1) : partNo-1;
                        if (container.queryLocalOrGrouped())
                        {
                            unsigned *globalPartNo = globalPartNoMap.getValue(partNo);
                            if (!globalPartNo)
                                continue; // this local kj slave is not dealing with part
                            partNo = *globalPartNo;
                        }
                        if (!keyedFieldsRowWithJGPrefix)
                            keyedFieldsRowWithJGPrefix.setown(preparePendingLookupRow(keyFieldsRowBuilder.getUnfinalizedClear(), keyFieldsRowBuilder.getMaxLength(), lhsRow, keyedFieldsRowSize));
                        queueLookupForPart(partNo, keyedFieldsRowWithJGPrefix);
                    }
                }
                keyManager.releaseSegmentMonitors();
            }
        }
        else
        {
            keyedFieldsRowWithJGPrefix.setown(preparePendingLookupRow(keyFieldsRowBuilder.getUnfinalizedClear(), keyFieldsRowBuilder.getMaxLength(), lhsRow, keyedFieldsRowSize));
            ForEachItemIn(partNo, indexParts)
                queueLookupForPart(partNo, keyedFieldsRowWithJGPrefix);
        }
        if (!keyedFieldsRowWithJGPrefix)
            return nullptr;
        KeyLookupHeader lookupKeyHeader;
        getHeaderFromRow(keyedFieldsRowWithJGPrefix, lookupKeyHeader);
        return LINK(lookupKeyHeader.jg);
    }
    void stopReadAhead()
    {
        {
            CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
            endOfInput = true;
        }
        flushPartLookups();
        bool expectedState = true;
        if (waitingForDoneGroups.compare_exchange_strong(expectedState, false))
            waitingForDoneGroupsSem.signal();
    }
    void readAhead()
    {
        endOfInput = false;
        do
        {
            if (queryAbortSoon())
                break;
            OwnedConstThorRow lhsRow = inputStream->nextRow();
            if (!lhsRow)
            {
                if (preserveGroups && currentPendingLhsGroupHead)
                {
                    currentPendingLhsGroupHead->decPending();
                    currentPendingLhsGroupHead = nullptr;
                }
                lhsRow.setown(inputStream->nextRow());
                if (!lhsRow)
                {
                    stopReadAhead();
                    break;
                }
            }
            Linked<CJoinGroup> jg;
            if (helper->leftCanMatch(lhsRow))
            {
                jg.setown(queueLookup(lhsRow)); // NB: will block if excessive amount queued
                if (jg)
                {
                    if (preserveGroups)
                    {
                        if (!currentPendingLhsGroupHead)
                        {
                            currentPendingLhsGroupHead = jg;  // this row is start of lhs group
                            currentPendingLhsGroupHead->incPending(); // NB: the end of lhs group will cause a decPending complete the joinGroup (if all it's rows are also complete)
                        }
                    }
                    if (preserveOrder)
                    {
                        CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
                        pendingJoinGroupList.addToTail(LINK(jg));
                    }
                }
            }
            else
                statsArr[AS_PreFiltered]++;
            if (jg)
                jg->decPending(); // all lookups queued. joinGroup will complete when all lookups are done (i.e. they're running asynchronously)
            else if ((joinFlags & JFleftonly) || (joinFlags & JFleftouter))
            {
                size32_t maxSz;
                void *unfinalizedRow = keyLookupRowWithJGAllocator->createRow(maxSz);
                OwnedConstThorRow row = preparePendingLookupRow(unfinalizedRow, maxSz, lhsRow, 0);
                KeyLookupHeader lookupKeyHeader;
                getHeaderFromRow(row, lookupKeyHeader);
                jg.set(lookupKeyHeader.jg);
                if (preserveOrder)
                {
                    CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
                    pendingJoinGroupList.addToTail(LINK(jg));
                }
                jg->decPending(); // all lookups queued. joinGroup will complete when all lookups are done (i.e. they're running asynchronously)
            }
        }
        while (!endOfInput);
    }
    const void *doDenormTransform(RtlDynamicRowBuilder &target, CJoinGroup &group)
    {
        offset_t fpos;
        unsigned idx = 0;
        unsigned matched = group.numRhsMatches();
        size32_t retSz = 0;
        OwnedConstThorRow lhs;
        lhs.set(group.queryLeft());
        switch (container.getKind())
        {
            case TAKkeyeddenormalize:
            {
                unsigned added = 0;
                while (idx < matched)
                {
                    const void *rhs = group.queryRightRow(idx++, fpos);
                    if (rhs) // can be null if helper->fetchMatch filtered some out
                    {
                        size32_t transformedSize = helper->transform(target, lhs, rhs, fpos, idx);
                        if (transformedSize)
                        {
                            retSz = transformedSize;
                            added++;
                            lhs.setown(target.finalizeRowClear(transformedSize));
                            if (added==keepLimit)
                                break;
                        }
                    }
                }
                if (retSz)
                    return lhs.getClear();
                break;
            }
            case TAKkeyeddenormalizegroup:
            {
                PointerArray rows;
                while (idx < matched && rows.ordinality() < keepLimit)
                {
                    const void *rhs = group.queryRightRow(idx++, fpos);
                    if (rhs) // can be null if helper->fetchMatch filtered some out
                        rows.append((void *)rhs);
                }
                retSz = helper->transform(target, lhs, rows.item(0), rows.ordinality(), (const void **)rows.getArray());
                if (retSz)
                    return target.finalizeRowClear(retSz);
                break;
            }
            default:
                assertex(false);
        }
        return nullptr;
    }
public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CKeyedJoinSlave(CGraphElementBase *_container) : PARENT(_container), lookupThread(*this), statsArr(8)
    {
        helper = static_cast <IHThorKeyedJoinArg *> (queryHelper());
        reInit = 0 != (helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) || (helper->getJoinFlags() & JFvarindexfilename);

        keyLookupQueuedBatchSize = getOptInt(THOROPT_KEYLOOKUP_QUEUED_BATCHSIZE, DEFAULT_KEYLOOKUP_QUEUED_BATCHSIZE);
        keyLookupMaxThreads = getOptInt(THOROPT_KEYLOOKUP_MAX_THREADS, DEFAULT_KEYLOOKUP_MAX_THREADS);
        keyLookupMaxQueued = getOptInt(THOROPT_KEYLOOKUP_MAX_QUEUED, DEFAULT_KEYLOOKUP_MAX_QUEUED);

        transformAllocator.setown(getRowAllocator(queryOutputMeta(), (roxiemem::RoxieHeapFlags)(queryHeapFlags()|roxiemem::RHFpacked|roxiemem::RHFunique)));
        rowManager = queryJobChannel().queryThorAllocator()->queryRowManager();

        class CKeyLookupRowOutputMetaData : public CPrefixedOutputMeta
        {
        public:
            CKeyLookupRowOutputMetaData(size32_t offset, IOutputMetaData *original) : CPrefixedOutputMeta(offset, original) { }
            virtual unsigned getMetaFlags() { return original->getMetaFlags() | MDFneeddestruct; }
            virtual void destruct(byte * self) override
            {
                CJoinGroup *joinGroup;
                memcpy(&joinGroup, self, sizeof(CJoinGroup *));
                joinGroup->Release();
                CPrefixedOutputMeta::destruct(self);
            }
        };
        Owned<IOutputMetaData> keyLookupRowOutputMetaData = new CKeyLookupRowOutputMetaData(sizeof(KeyLookupHeader), helper->queryIndexReadInputRecordSize());
        keyLookupRowWithJGAllocator.setown(getRowAllocator(keyLookupRowOutputMetaData, (roxiemem::RoxieHeapFlags)(queryHeapFlags()|roxiemem::RHFpacked|roxiemem::RHFunique)));

        appendOutputLinked(this);
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        if (!initialized)
        {
            initialized = true;
            joinFlags = helper->getJoinFlags();
            needsDiskRead = helper->diskAccessRequired();
            fixedRecordSize = helper->queryIndexRecordSize()->getFixedSize(); // 0 if variable and unused
            node = queryJobChannel().queryMyRank()-1;
            onFailTransform = (0 != (joinFlags & JFonfail)) && (0 == (joinFlags & JFmatchAbortLimitSkips));

            joinFieldsAllocator.setown(getRowAllocator(helper->queryJoinFieldsRecordSize()));
            if (onFailTransform || (joinFlags & JFleftouter))
            {
                RtlDynamicRowBuilder rr(joinFieldsAllocator);
                size32_t sz = helper->createDefaultRight(rr);
                defaultRight.setown(rr.finalizeRowClear(sz));
            }
        }
        else
        {
            tags.kill();
            tlkKeyIndexes.kill();
            indexParts.kill();
            dataParts.kill();
            localFPosToNodeMap.clear();
            globalFPosToNodeMap.clear();
            eexp.clear();
            fetchFiles.kill();
            globalPartNoMap.kill();
        }
        for (auto &a : statsArr)
            a = 0;
        // decode data from master. NB: can be resent and differ if in global loop
        data.read(indexName);
        unsigned numTags;
        data.read(numTags);
        unsigned t;
        for (t=0; t<numTags; t++)
        {
            mptag_t tag = container.queryJobChannel().deserializeMPTag(data);
            tags.append(tag);
            queryJobChannel().queryJobComm().flush(tag);
        }

        unsigned numIndexParts;
        data.read(numIndexParts);
        if (numIndexParts)
        {
            unsigned numSuperIndexSubs;
            data.read(superWidth);
            data.read(numSuperIndexSubs);
            data.read(keyHasTlk);
            if (keyHasTlk)
            {
                MemoryBuffer tlkMb;
                unsigned tlks;
                size32_t tlkSz;
                data.read(tlks);
                UnsignedArray posArray, lenArray;
                while (tlks--)
                {
                    data.read(tlkSz);
                    posArray.append(tlkMb.length());
                    lenArray.append(tlkSz);
                    tlkMb.append(tlkSz, data.readDirect(tlkSz));
                }
                ForEachItemIn(p, posArray)
                {
                    Owned<IFileIO> iFileIO = createIFileI(lenArray.item(p), tlkMb.toByteArray()+posArray.item(p));
                    StringBuffer name("TLK");
                    name.append('_').append(container.queryId()).append('_');
                    Owned<IKeyIndex> tlkKeyIndex = createKeyIndex(name.append(p).str(), 0, *iFileIO, true, false); // MORE - not the right crc
                    tlkKeyIndexes.append(*tlkKeyIndex.getClear());
                }
            }

            CPartDescriptorArray _indexParts;
            deserializePartFileDescriptors(data, _indexParts);
            IFileDescriptor &indexFileDesc = _indexParts.item(0).queryOwner();
            localKey = indexFileDesc.queryProperties().getPropBool("@local", false);
            if (numSuperIndexSubs && !localKey) // if numSuperIndexSubs==0 means not interleaved
            {
                IPartDescriptor *currentPart = &_indexParts.item(0);
                ISuperFileDescriptor *superFdesc = currentPart->queryOwner().querySuperFileDescriptor();
                assertex(superFdesc);
                if (container.queryLocalOrGrouped())
                {
                    ForEachItemIn(i, _indexParts)
                    {
                        IPartDescriptor *part = &_indexParts.item(i);
                        unsigned partNo = part->queryPartIndex();
                        unsigned subfile, subpartnum;
                        superFdesc->mapSubPart(partNo, subfile, subpartnum);
                        unsigned globalPartNo = superWidth*subfile+subpartnum;
                        globalPartNoMap.setValue(globalPartNo, i);
                        indexParts.append(LINK(&_indexParts.item(i)));
                    }
                }
                else
                {
                    unsigned s, ip;
                    for (s=0; s<numSuperIndexSubs; s++)
                    {
                        for (ip=0; ip<superWidth; ip++)
                        {
                            unsigned which = ip*numSuperIndexSubs+s;
                            IPartDescriptor &part = _indexParts.item(which);
                            indexParts.append(LINK(&part));
                        }
                    }
                }
            }
            else
            {
                ForEachItemIn(p, _indexParts)
                    indexParts.append(LINK(&_indexParts.item(p)));
            }
            if (needsDiskRead)
            {
                data.read(remoteDataFiles); // if true, all fetch parts will be serialized
                unsigned numDataParts;
                data.read(numDataParts);
                if (numDataParts)
                {
                    deserializePartFileDescriptors(data, dataParts);
                    localFPosToNodeMap.allocateN(numDataParts);

                    unsigned f;
                    FPosTableEntry *e;
                    for (f=0, e=&localFPosToNodeMap[0]; f<numDataParts; f++, e++)
                    {
                        IPartDescriptor &part = dataParts.item(f);
                        e->base = part.queryProperties().getPropInt64("@offset");
                        e->top = e->base + part.queryProperties().getPropInt64("@size");
                        e->index = f; // NB: index == which local part in dataParts
                    }
                }
                if (remoteDataFiles) // global offset map not needed if remote and have all fetch parts inc. map (from above)
                {
                    if (numDataParts)
                        filePartTotal = numDataParts;
                }
                else
                {
                    data.read(filePartTotal);
                    if (filePartTotal)
                    {
                        size32_t offsetMapSz;
                        data.read(offsetMapSz);
                        globalFPosToNodeMap.allocateN(filePartTotal);
                        const void *offsetMapBytes = (FPosTableEntry *)data.readDirect(offsetMapSz);
                        memcpy(globalFPosToNodeMap, offsetMapBytes, offsetMapSz);
                    }
                }
                unsigned encryptedKeyLen;
                void *encryptedKey;
                helper->getFileEncryptKey(encryptedKeyLen,encryptedKey);
                if (0 != encryptedKeyLen)
                {
                    bool dfsEncrypted = numDataParts?dataParts.item(0).queryOwner().queryProperties().getPropBool("@encrypted"):false;
                    if (dfsEncrypted) // otherwise ignore (warning issued by master)
                        eexp.setown(createAESExpander256(encryptedKeyLen, encryptedKey));
                    memset(encryptedKey, 0, encryptedKeyLen);
                    free(encryptedKey);
                }

                fetchHandler.setown(new CKeyedFetchHandler(*this));

                FPosTableEntry *fPosToNodeMap = globalFPosToNodeMap ? globalFPosToNodeMap : localFPosToNodeMap;
                for (unsigned c=0; c<filePartTotal; c++)
                {
                    FPosTableEntry &e = fPosToNodeMap[c];
                    ActPrintLog("Table[%d] : base=%" I64F "d, top=%" I64F "d, slave=%d", c, e.base, e.top, e.index);
                }
                for (unsigned i=0; i<dataParts.ordinality(); i++)
                {
                    Owned<IDelayedFile> dFile = queryThor().queryFileCache().lookup(*this, indexName, dataParts.item(i), eexp);
                    fetchFiles.append(*dFile.getClear());
                }
            }

            unsigned currentNumLookupThreads = lookupThreads.ordinality();
            if (currentNumLookupThreads>numIndexParts)
                lookupThreads.removen(numIndexParts, currentNumLookupThreads-numIndexParts);
            else
            {
                while (currentNumLookupThreads<numIndexParts)
                    lookupThreads.append(new CKeyLookupHandler(*this, currentNumLookupThreads++));
            }
        }
        else
            needsDiskRead = false;
    }
    virtual void abort() override
    {
        PARENT::abort();
    }
// IThorDataLink
    virtual void start() override
    {
        ActivityTimer s(totalCycles, timeActivities);
        assertex(inputs.ordinality() == 1);
        PARENT::start();

        keepLimit = helper->getKeepLimit();
        atMost = helper->getJoinLimit();
        if (atMost == 0)
        {
            if (JFleftonly == (joinFlags & JFleftonly))
                keepLimit = 1; // don't waste time and memory collating and returning record which will be discarded.
            atMost = (unsigned)-1;
        }
        abortLimit = helper->getMatchAbortLimit();
        if (abortLimit == 0) abortLimit = (unsigned)-1;
        if (keepLimit == 0) keepLimit = (unsigned)-1;
        if (abortLimit < atMost)
            atMost = abortLimit;
        rowLimit = (rowcount_t)helper->getRowLimit();
        if (rowLimit < keepLimit)
            keepLimit = rowLimit+1; // if keepLimit is small, let it reach rowLimit+1, but any more is pointless and a waste of time/resources.

        inputHelper.set(input->queryFromActivity()->queryContainer().queryHelper());
        preserveOrder = ((joinFlags & JFreorderable) == 0);
        preserveGroups = input->isGrouped();
        ActPrintLog("KJ: preserveGroups=%s, preserveOrder=%s", preserveGroups?"true":"false", preserveOrder?"true":"false");

        if (keyHasTlk)
            getTlkKeyManagers(tlkKeyManagers);
        currentMatchIdx = 0;
        currentAdded = 0;
        eos = false;
        endOfInput = false;
        currentPendingLhsGroupHead = nullptr;
        ForEachItemIn(b, lookupThreads)
            lookupThreads.item(b)->init();
        lookupThread.start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        OwnedConstThorRow ret;
        while (!abortSoon && !eos)
        {
            if (!currentJoinGroup)
            {
                while (true)
                {
                    {
                        CriticalBlock b(onCompleteCrit);
                        currentJoinGroup.setown(doneJoinGroupList.removeHead());
                        if (currentJoinGroup)
                            break;
                        if (endOfInput && (nullptr == pendingJoinGroupList.queryHead()))
                        {
                            eos = true;
                            // all done
                            return nullptr;
                        }
                    }
                    waitingForDoneGroups = true;
                    waitingForDoneGroupsSem.wait();
                }
                bool eog = false;
                if (preserveGroups)
                {
                    currentJoinGroupSize += currentAdded;
                    if (!currentActiveLhsGroupHead)
                        currentActiveLhsGroupHead = currentJoinGroup;
                    else if (!currentJoinGroup->inGroup(currentActiveLhsGroupHead))
                    {
                        currentActiveLhsGroupHead = currentJoinGroup;
                        eog = 0 != currentJoinGroupSize;
                    }
                    currentJoinGroupSize = 0;
                }
                currentMatchIdx = 0;
                currentAdded = 0;
                if (eog)
                    return nullptr;
            }
            if ((0 == currentMatchIdx) && abortLimitAction(currentJoinGroup, ret)) // only any point in checking 1st Idx
                currentJoinGroup.clear();
            else
            {
                RtlDynamicRowBuilder rowBuilder(transformAllocator, false);
                size32_t transformedSize = 0;
                if (!currentJoinGroup->numRhsMatches() || currentJoinGroup->hasAtMostLimitBeenHit())
                {
                    switch (joinFlags & JFtypemask)
                    {
                        case JFleftouter:
                        case JFleftonly:
                            switch (container.getKind())
                            {
                                case TAKkeyedjoin:
                                {
                                    transformedSize = helper->transform(rowBuilder.ensureRow(), currentJoinGroup->queryLeft(), defaultRight, (__uint64)0, 0U);
                                    if (transformedSize)
                                        ret.setown(rowBuilder.finalizeRowClear(transformedSize));
                                    break;
                                }
                                case TAKkeyeddenormalize:
                                {
                                    // return lhs, already finalized
                                    ret.set(currentJoinGroup->queryLeft());
                                    break;
                                }
                                case TAKkeyeddenormalizegroup:
                                {
                                    transformedSize = helper->transform(rowBuilder.ensureRow(), currentJoinGroup->queryLeft(), NULL, 0, (const void **)NULL); // no dummyrhs (hthor and roxie don't pass)
                                    if (transformedSize)
                                        ret.setown(rowBuilder.finalizeRowClear(transformedSize));
                                    break;
                                }
                            }
                            if (ret)
                                currentAdded++;
                    }
                    currentJoinGroup.clear();
                }
                else if (!(joinFlags & JFexclude))
                {
                    // will be at least 1 rhs match to be in this branch
                    switch (container.getKind())
                    {
                        case TAKkeyedjoin:
                        {
                            rowBuilder.ensureRow();
                            while (currentMatchIdx < currentJoinGroup->numRhsMatches())
                            {
                                offset_t fpos;
                                const void *rhs = currentJoinGroup->queryRightRow(currentMatchIdx++, fpos);
                                if (rhs) // can be null if helper->fetchMatch filtered some out
                                {
                                    transformedSize = helper->transform(rowBuilder, currentJoinGroup->queryLeft(), rhs, fpos, currentMatchIdx);
                                    if (currentMatchIdx >= currentJoinGroup->numRhsMatches())
                                        currentJoinGroup.clear();
                                    if (transformedSize)
                                    {
                                        ret.setown(rowBuilder.finalizeRowClear(transformedSize));
                                        currentAdded++;
                                        if (currentAdded==keepLimit)
                                            currentJoinGroup.clear();
                                        break;
                                    }
                                    if (!currentJoinGroup)
                                        break;
                                }
                            }
                            break;
                        }
                        case TAKkeyeddenormalize:
                        case TAKkeyeddenormalizegroup:
                        {
                            ret.setown(doDenormTransform(rowBuilder, *currentJoinGroup));
                            currentJoinGroup.clear();
                            break;
                        }
                        default:
                            assertex(false);
                    }
                }
                else
                    currentJoinGroup.clear();
            }
            if (ret)
            {
                // NB: If this KJ is a global activity, there will be an associated LIMIT activity beyond the KJ. This check spots if limit exceeded a slave level.
                if (getDataLinkCount()+1 > rowLimit)
                    helper->onLimitExceeded();

                dataLinkIncrement();
                return ret.getClear();
            }
        }
        return nullptr;
    }
    virtual void stop() override
    {
        endOfInput = true; // signals to readAhead which is reading input, that is should stop asap.
        lookupThread.join();
        ForEachItemIn(b, lookupThreads)
            lookupThreads.item(b)->stop();
        PARENT::stop();
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override
    {
        initMetaInfo(info);
        info.canStall = true;
        info.unknownRowsOutput = true;
    }
    virtual void serializeStats(MemoryBuffer &mb) override
    {
        PARENT::serializeStats(mb);
        for (unsigned s : statsArr)
            mb.append(s);
    }
    // IJoinProcessor
    virtual void onComplete(CJoinGroup *joinGroup) override
    {
        // moves complete CJoinGroup's from pending list to done list
        CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
        if (preserveOrder)
        {
            // NB: when preserveGroups, the lhs group will always be complete at same time, so this will traverse whole group
            CJoinGroup *finger = pendingJoinGroupList.queryHead();
            while (finger)
            {
                if (!finger->complete())
                {
                    if (finger == pendingJoinGroupList.queryHead()) // i.e. none ready
                        return;
                }
                CJoinGroup *next = finger->next;
                pendingJoinGroupList.remove(finger);
                doneJoinGroupList.addToTail(LINK(finger));
                finger = next;
            }
        }
        else
            doneJoinGroupList.addToTail(LINK(joinGroup)); // takes ownership
        bool expectedState = true;
        if (waitingForDoneGroups.compare_exchange_strong(expectedState, false))
            waitingForDoneGroupsSem.signal();
    }
};


CActivityBase *createKeyedJoinSlave(CGraphElementBase *container) 
{ 
    return new CKeyedJoinSlave(container);
}

