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
#include "jsorta.hpp"

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
#define DEFAULT_KEYLOOKUP_MAX_THREADS 10
#define DEFAULT_KEYLOOKUP_MAX_QUEUED 10000
#define DEFAULT_KEYLOOKUP_MAX_DONE 10000

class CJoinGroup;
struct KeyLookupHeader
{
    CJoinGroup *jg;
};
struct FetchRequestHeader
{
    offset_t fpos;
    CJoinGroup *jg;
    unsigned __int64 sequence;
};
struct FetchReplyHeader
{
    static const unsigned __int64 fetchMatchedMask = 0x8000000000000000;
    CJoinGroup *jg;
    unsigned __int64 sequence; // fetchMatchedMask used to screen top-bit to denote whether reply fetch matched or not
};
template <class HeaderStruct>
void getHeaderFromRow(const void *row, HeaderStruct &header)
{
    memcpy(&header, row, sizeof(HeaderStruct));
}


enum AllocatorTypes { AT_Transform=1, AT_LookupWithJG, AT_JoinFields, AT_FetchRequest, AT_FetchResponse, AT_JoinGroup, AT_JoinGroupRhsRows, AT_FetchDisk };


struct Row
{
    const void *rhs;
    offset_t fpos;
};
struct RowArray
{
    Row *rows;
    rowidx_t maxRows;
    rowidx_t numRows;
};

interface IJoinProcessor
{
    virtual void onComplete(CJoinGroup * jg) = 0;
    virtual unsigned addRowEntry(unsigned partNo, const void *rhs, offset_t fpos, RowArray *&rowArrays, unsigned &numRowArrays) = 0;
};

class CJoinGroup : public CSimpleInterfaceOf<IInterface>
{
protected:
    OwnedConstThorRow leftRow;
    mutable CriticalSection crit;
    std::atomic<unsigned> pending{0};
    std::atomic<unsigned> candidates{0};
    IJoinProcessor *join = nullptr;
    rowidx_t totalRows = 0;
    unsigned numRowArrays = 0;
    RowArray *rowArrays = nullptr;
    static const unsigned GroupFlagLimitMask = 0x03;
public:
    enum GroupFlags:unsigned { gf_null=0x0, gf_limitatmost=0x01, gf_limitabort=0x02, gf_eog=0x04, gf_head=0x08 } groupFlags = gf_null;
    struct JoinGroupRhsState
    {
        JoinGroupRhsState() { clear(); }
        inline void clear() { arr = nullptr; pos = 0; }
        RowArray *arr;
        rowidx_t pos;
    };
    CJoinGroup *prev = nullptr;  // Doubly-linked list to allow us to keep track of ones that are still in use
    CJoinGroup *next = nullptr;

    inline const void *_queryNextRhs(offset_t &fpos, JoinGroupRhsState &rhsState) const
    {
        while (rhsState.arr != (rowArrays+numRowArrays)) // end of array marker
        {
            if (rhsState.arr->rows)
            {
                while (rhsState.pos < rhsState.arr->numRows)
                {
                    Row &row = rhsState.arr->rows[rhsState.pos++];
                    if (row.rhs)
                    {
                        fpos = row.fpos;
                        return row.rhs;
                    }
                }
            }
            rhsState.arr++;
            rhsState.pos = 0;
        }
        return nullptr;
    }
    void freeRows()
    {
        if (rowArrays)
        {
            RowArray *cur = rowArrays;
            while (numRowArrays--)
            {
                if (cur->rows)
                {
                    const Row *row = cur->rows;
                    while (cur->numRows--)
                    {
                        if (row->rhs)
                            ReleaseRoxieRow(row->rhs);
                        ++row;
                    }
                    ReleaseRoxieRow(cur->rows);
                }
                ++cur;
            }
            ReleaseRoxieRow(rowArrays);
            rowArrays = nullptr;
            totalRows = 0;
        }
    }
public:
    CJoinGroup(CActivityBase &_activity, const void *_leftRow, IJoinProcessor *_join)
        : join(_join)
    {
    	leftRow.set(_leftRow);
    }
    ~CJoinGroup()
    {
        freeRows();
    }
#undef new
    void *operator new(size_t size, roxiemem::IRowManager *rowManager, activity_id activityId)
    {
        return rowManager->allocate(size, createCompoundActSeqId(activityId, AT_JoinGroup));
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
    inline unsigned incCandidates()
    {
        return candidates.fetch_add(1);
    }
    inline bool hasAbortLimitBeenHit() const
    {
        return gf_limitabort == (groupFlags & GroupFlagLimitMask);
    }
    inline void setAbortLimitHit()
    {
        CriticalBlock b(crit);
        addFlag(gf_limitabort);
        freeRows();
    }
    inline bool hasAtMostLimitBeenHit() const
    {
        return gf_limitatmost == (groupFlags & GroupFlagLimitMask);
    }
    inline void setAtMostLimitHit()
    {
        CriticalBlock b(crit);
        addFlag(gf_limitatmost);
        freeRows();
    }
    inline const void *queryLeft() const
    {
        return leftRow;
    }
    inline const void *queryFirstRhs(offset_t &fpos, JoinGroupRhsState &rhsState) const
    {
        CriticalBlock b(crit);
        if (!rowArrays)
            return nullptr;
        rhsState.arr = &rowArrays[0];
        rhsState.pos = 0;
        return _queryNextRhs(fpos, rhsState);
    }
    inline const void *queryNextRhs(offset_t &fpos, JoinGroupRhsState &rhsState) const
    {
        CriticalBlock b(crit);
        return _queryNextRhs(fpos, rhsState);
    }
    inline bool complete() const { return 0 == pending; }
    inline void incPending()
    {
        pending++;
    }
    inline void decPending()
    {
        if (1 == pending.fetch_sub(1))
            join->onComplete(this);
    }
    inline unsigned addRightMatchPending(unsigned partNo, offset_t fpos)
    {
        CriticalBlock b(crit);
        if (gf_null != (groupFlags & GroupFlagLimitMask))
            return NotFound;
        return join->addRowEntry(partNo, nullptr, fpos, rowArrays, numRowArrays);
    }
    inline void addRightMatchCompletePending(unsigned partNo, unsigned sequence, const void *right)
    {
        CriticalBlock b(crit);
        if (gf_null != (groupFlags & GroupFlagLimitMask))
            return;
        RowArray &rowArray = rowArrays[partNo];
        rowArray.rows[sequence].rhs = right;
        ++totalRows;
    }
    inline void addRightMatch(unsigned partNo, const void *right, offset_t fpos)
    {
        CriticalBlock b(crit);
        if (gf_null != (groupFlags & GroupFlagLimitMask))
            return;
        join->addRowEntry(partNo, right, fpos, rowArrays, numRowArrays);
        ++totalRows;
    }
    inline unsigned numRhsMatches() const
    {
        CriticalBlock b(crit);
        return totalRows;
    }
    inline void addFlag(GroupFlags flag)
    {
        groupFlags = static_cast<GroupFlags>(groupFlags | flag);
    }
    inline bool hasFlag(GroupFlags flag) const
    {
        return 0 != (groupFlags & flag);
    }
};

class CJoinGroupList
{
    CJoinGroup *head = nullptr, *tail = nullptr;
    unsigned count = 0;

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
    inline unsigned queryCount() const { return count; }
    inline CJoinGroup *queryHead() const { return head; }
    CJoinGroup *removeHead()
    {
        if (!head)
            return nullptr;
        CJoinGroup *ret = head;
        head = head->next;
        ret->next = nullptr;
        if (head)
        {
            dbgassertex(head->prev == ret);
            head->prev = nullptr;
        }
        else
            tail = nullptr;
        --count;
        return ret;
    }
    CJoinGroup *remove(CJoinGroup *joinGroup)
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
        --count;
        return joinGroup; // now detached
    }
    void addToTail(CJoinGroup *joinGroup)
    {
        dbgassertex(nullptr == joinGroup->next);
        dbgassertex(nullptr == joinGroup->prev);
        if (!head)
        {
            head = tail = joinGroup;
        }
        else
        {
            tail->next = joinGroup;
            joinGroup->prev = tail;
            tail = joinGroup;
        }
        ++count;
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
        Owned<IEngineRowAllocator> fetchInputMetaAllocator;
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
            }
            void stop()
            {
                if (!aborted)
                {
                    aborted = true;
                    comm.cancel(RANK_ALL, resultMpTag);
                }
                threaded.join();
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
                                    LinkThorRow(childRow);
                                    unsigned sequence = header.sequence & 0xffffffff;
                                    unsigned partNo = (header.sequence & ~FetchReplyHeader::fetchMatchedMask) >> 32;
                                    // If !preserverOrder, right rows added to single array in jg, so pass 0
                                    jg->addRightMatchCompletePending(activity.preserveOrder ? partNo : 0, sequence, childRow);
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
            }
            void stop()
            {
                if (!aborted)
                {
                    aborted = true;
                    comm.cancel(RANK_ALL, requestMpTag);
                }
                threaded.join();
            }
            virtual void threadmain() override
            {
                try
                {
                    rank_t sender;
                    CMessageBuffer msg;
                    unsigned endRequestsCount = activity.container.queryJob().querySlaves();
                    Owned<IBitSet> endRequests = createThreadSafeBitSet(); // NB: verification only

                    Owned<IThorRowInterfaces> fetchDiskRowIf = activity.createRowInterfaces(helper->queryDiskRecordSize(), AT_FetchDisk);
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
                                if (0 != helper->queryFetchInputRecordSize()->getMinRecordSize())
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
            Owned<IOutputMetaData> fetchInputMeta = new CPrefixedOutputMeta(sizeof(FetchRequestHeader), helper->queryFetchInputRecordSize());
            fetchInputMetaRowIf.setown(activity.createRowInterfaces(fetchInputMeta, AT_FetchRequest));
            fetchInputMetaAllocator.set(fetchInputMetaRowIf->queryRowAllocator());

            Owned<IOutputMetaData> fetchOutputMeta = createOutputMetaDataWithChildRow(activity.joinFieldsAllocator, sizeof(FetchReplyHeader));
            fetchOutputMetaRowIf.setown(activity.createRowInterfaces(fetchOutputMeta, AT_FetchResponse));

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
        void addRow(offset_t fpos, unsigned __int64 sequence, CJoinGroup *jg)
        {
            // build request row
            RtlDynamicRowBuilder fetchInputRowBuilder(fetchInputMetaAllocator);
            FetchRequestHeader &header = *(FetchRequestHeader *)fetchInputRowBuilder.getUnfinalized();
            header.fpos = fpos;
            header.sequence = sequence;
            header.jg = jg;

            size32_t sz = sizeof(FetchRequestHeader);
            if (0 != helper->queryFetchInputRecordSize()->getMinRecordSize())
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
        CriticalSection queueCrit;
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
        void join()
        {
            {
                CriticalBlock b(queueCrit);
                if (ts_initial == state)
                    return;
            }
            threaded.join();
        }
        void stop()
        {
            stopped = true;
            join();
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
                    activity.lookupThreadLimiter.inc(); // blocks if hit lookup thread limit
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

                // NB: keepLimit is not on hard matches and can only be applied later, since other filtering (e.g. in transform) may keep below keepLimit
                while (keyManager->lookup(true))
                {
                    unsigned candidates = joinGroup->incCandidates()+1;
                    if (candidates > activity.abortLimit)
                    {
                        joinGroup->setAbortLimitHit(); // also clears existing rows
                        break;
                    }
                    else if (candidates > activity.atMost) // atMost - filter out group if > max hard matches
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
                            unsigned __int64 sequence = joinGroup->addRightMatchPending(partNo, fpos);
                            if (NotFound == sequence) // means limit was hit and must have been caused by another handler
                                break;
                            sequence |= (((unsigned __int64)partNo) << 32);
                            activity.fetchHandler->addRow(fpos, sequence, joinGroup);
                        }
                        else
                        {
                            RtlDynamicRowBuilder joinFieldsRowBuilder(activity.joinFieldsAllocator);
                            size32_t sz = activity.helper->extractJoinFields(joinFieldsRowBuilder, keyRow, &adapter);
                            const void *joinFieldsRow = joinFieldsRowBuilder.finalizeRowClear(sz);
                            joinGroup->addRightMatch(partNo, joinFieldsRow, fpos);
                        }
                    }
                }
                keyManager->releaseSegmentMonitors();
                joinGroup->decPending(); // Every queued lookup row triggered an inc., this is the corresponding dec.
            }
        }
        void queueLookup(const void *keyedFieldsRowWithJGPrefix)
        {
            // NB: queueLookup() only called by readAhead() thread, so no protection for batchArray needed
            LinkThorRow(keyedFieldsRowWithJGPrefix);
            batchArray.append(keyedFieldsRowWithJGPrefix);
            if (batchArray.ordinality() >= activity.keyLookupQueuedBatchSize)
                enqueue(batchArray); // NB: enqueue takes ownership of rows, i.e batchArray is cleared after call
        }
        void flush()
        {
            // NB: flush()/flushPartLookups() only called by readAhead() thread, so no protection for batchArray needed
            if (batchArray.ordinality())
                enqueue(batchArray);
        }
    // IThreaded
        virtual void threadmain() override
        {
            CThorExpandingRowArray processing(activity);
            do
            {
                {
                    CriticalBlock b(queueCrit);
                    if (0 == queue.ordinality())
                    {
                        if (state != ts_starting) // 1st time around the loop
                            assertex(state == ts_running);
                        state = ts_stopping; // only this thread can transition between ts_running and ts_stopping
                        break;
                    }
                    else if (ts_starting)
                        state = ts_running; // only this thread can transition between ts_starting and ts_running
                    else
                    {
                        dbgassertex(state == ts_running);
                    }
                    queue.swap(processing);
                }
                process(processing);
                processing.clearRows();
            }
            while (true);
            activity.lookupThreadLimiter.dec(); // unblocks any requests to start lookup threads
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


    class CLimiter
    {
        unsigned max, leeway;
        Semaphore sem;
        CriticalSection crit;
        unsigned count = 0;
        unsigned blocked = 0;
    public:
        CLimiter() : max(0), leeway(0)
        {
        }
        CLimiter(unsigned _max, unsigned _leeway=0) : max(_max), leeway(_leeway)
        {
        }
        void set(unsigned _max, unsigned _leeway=0)
        {
            max = _max;
            leeway = _leeway;
        }
        bool incNonBlocking()
        {
            {
                CriticalBlock b(crit);
                if (count++ < max+leeway)
                    return false;
                ++blocked;
            }
            return true;
        }
        void inc()
        {
            if (!incNonBlocking())
                return;
            sem.wait();
        }
        void dec()
        {
            CriticalBlock b(crit);
            --count;
            if (blocked && (count < max))
            {
                sem.signal(blocked);
                blocked = 0;
            }
        }
        void block()
        {
            sem.wait();
        }
    };

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
    SafePointerArrayOf<CKeyLookupHandler> lookupHandlers;
    CLimiter lookupThreadLimiter;
    CLimiter pendingKeyLookupLimiter;
    CLimiter doneListLimiter;
    rowcount_t totalQueuedLookupRowCount = 0;
    unsigned blockedKeyLookupThreads = 0;
    unsigned blockedDoneGroupThreads = 0;

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
    unsigned keyLookupMaxThreads = DEFAULT_KEYLOOKUP_MAX_THREADS;
    unsigned keyLookupMaxQueued = DEFAULT_KEYLOOKUP_MAX_QUEUED;
    unsigned keyLookupMaxDone = DEFAULT_KEYLOOKUP_MAX_DONE;

    Owned<IEngineRowAllocator> keyLookupRowWithJGAllocator, transformAllocator;
    bool endOfInput = false; // marked true when input exhausted, but may be groups in flight
    bool eos = false; // marked true when everything processed
    IArrayOf<IKeyManager> tlkKeyManagers;
    CriticalSection onCompleteCrit, queuedCrit, runningLookupThreadsCrit;
    std::atomic<bool> waitingForDoneGroups{false};
    Semaphore waitingForDoneGroupsSem, doneGroupsExcessiveSem;
    CJoinGroupList pendingJoinGroupList, doneJoinGroupList;
    Owned<IException> abortLimitException;
    Owned<CJoinGroup> currentJoinGroup;
    unsigned currentMatchIdx = 0;
    CJoinGroup::JoinGroupRhsState rhsState;

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
    void queueLookupForPart(unsigned partNo, const void *keyedFieldsRowWithJGPrefix)
    {
        KeyLookupHeader lookupKeyHeader;
        getHeaderFromRow(keyedFieldsRowWithJGPrefix, lookupKeyHeader);
        lookupKeyHeader.jg->incPending(); // each queued lookup pending a result

        /* NB: there is 1 batchLookupArray/Thread per part
         * There could be >1 per part, but I'm not sure there's a lot of point.
         */
        CKeyLookupHandler &lookupThread = *lookupHandlers.item(partNo);
        lookupThread.queueLookup(keyedFieldsRowWithJGPrefix);
    }
    void flushPartLookups()
    {
    	ForEachItemIn(b, lookupHandlers)
    	{
    	    CKeyLookupHandler &lookupThread = *lookupHandlers.item(b);
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
        CJoinGroup *jg = new (rowManager, queryId()) CJoinGroup(*this, lhsRow, this);
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
                        if (container.queryLocalData())
                        {
                            unsigned *mappedPartNo = globalPartNoMap.getValue(partNo);
                            if (!mappedPartNo)
                                continue; // this local kj slave is not dealing with part
                            partNo = *mappedPartNo;
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
        flushPartLookups();
        ForEachItemIn(h, lookupHandlers)
            lookupHandlers.item(h)->join();

        CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
        endOfInput = true;
        bool expectedState = true;
        if (waitingForDoneGroups.compare_exchange_strong(expectedState, false))
            waitingForDoneGroupsSem.signal();
    }
    void readAhead()
    {
        endOfInput = false;
        CJoinGroup *lastGroupMember = nullptr;
        do
        {
            if (queryAbortSoon())
                break;
            OwnedConstThorRow lhsRow = inputStream->nextRow();
            if (!lhsRow)
            {
                if (preserveGroups && lastGroupMember)
                {
                    lastGroupMember->addFlag(CJoinGroup::GroupFlags::gf_eog);
                    lastGroupMember = nullptr;
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
                jg.setown(queueLookup(lhsRow)); // NB: will block if excessive amount queued
            else
                statsArr[AS_PreFiltered]++;
            if (!jg && ((joinFlags & JFleftonly) || (joinFlags & JFleftouter)))
            {
                size32_t maxSz;
                void *unfinalizedRow = keyLookupRowWithJGAllocator->createRow(maxSz);
                OwnedConstThorRow row = preparePendingLookupRow(unfinalizedRow, maxSz, lhsRow, 0);
                KeyLookupHeader lookupKeyHeader;
                getHeaderFromRow(row, lookupKeyHeader);
                jg.set(lookupKeyHeader.jg);
            }
            if (jg)
            {
                if (preserveGroups)
                {
                    if (!lastGroupMember)
                    {
                        lastGroupMember = jg;
                        lastGroupMember->addFlag(CJoinGroup::GroupFlags::gf_head);
                    }
                }
                bool pendingBlock = false;
                {
                    CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
                    pendingJoinGroupList.addToTail(LINK(jg));
                    pendingBlock = pendingKeyLookupLimiter.incNonBlocking();
                }
                if (pendingBlock)
                {
                    if (preserveOrder || preserveGroups)
                        flushPartLookups(); // some of the batches that are not yet queued may be holding up join groups that are ahead of others that are complete.
                    pendingKeyLookupLimiter.block();
                }
                jg->decPending(); // all lookups queued. joinGroup will complete when all lookups are done (i.e. they're running asynchronously)
            }
        }
        while (!endOfInput);
    }
    const void *doDenormTransform(RtlDynamicRowBuilder &target, CJoinGroup &group)
    {
        offset_t fpos;
        CJoinGroup::JoinGroupRhsState rhsState;
        size32_t retSz = 0;
        OwnedConstThorRow lhs;
        lhs.set(group.queryLeft());
        const void *rhs = group.queryFirstRhs(fpos, rhsState);
        switch (container.getKind())
        {
            case TAKkeyeddenormalize:
            {
                unsigned added = 0;
                unsigned idx = 0;
                while (rhs)
                {
                    ++idx;
                    size32_t transformedSize = helper->transform(target, lhs, rhs, fpos, idx);
                    if (transformedSize)
                    {
                        retSz = transformedSize;
                        added++;
                        lhs.setown(target.finalizeRowClear(transformedSize));
                        if (added==keepLimit)
                            break;
                    }
                    rhs = group.queryNextRhs(fpos, rhsState);
                }
                if (retSz)
                    return lhs.getClear();
                break;
            }
            case TAKkeyeddenormalizegroup:
            {
                PointerArray rows;
                while (rows.ordinality() < keepLimit)
                {
                    if (rhs) // can be null if helper->fetchMatch filtered some out
                        rows.append((void *)rhs);
                    rhs = group.queryNextRhs(fpos, rhsState);
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

    bool transferToDoneList(CJoinGroup *joinGroup)
    {
        doneJoinGroupList.addToTail(joinGroup);
        pendingKeyLookupLimiter.dec();
        return doneListLimiter.incNonBlocking();
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
        keyLookupMaxDone = getOptInt(THOROPT_KEYLOOKUP_MAX_DONE, DEFAULT_KEYLOOKUP_MAX_DONE);

        lookupThreadLimiter.set(keyLookupMaxThreads);
        pendingKeyLookupLimiter.set(keyLookupMaxQueued, 100);
        doneListLimiter.set(keyLookupMaxDone, 100);

        transformAllocator.setown(getRowAllocator(queryOutputMeta(), (roxiemem::RoxieHeapFlags)(queryHeapFlags()|roxiemem::RHFpacked|roxiemem::RHFunique), AT_Transform));
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
        keyLookupRowWithJGAllocator.setown(getRowAllocator(keyLookupRowOutputMetaData, (roxiemem::RoxieHeapFlags)(queryHeapFlags()|roxiemem::RHFpacked|roxiemem::RHFunique), AT_LookupWithJG));

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

            joinFieldsAllocator.setown(getRowAllocator(helper->queryJoinFieldsRecordSize(), roxiemem::RHFnone, AT_JoinFields));
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
                if (container.queryLocalData())
                {
                    ForEachItemIn(p, _indexParts)
                    {
                        IPartDescriptor *part = &_indexParts.item(p);
                        unsigned partNo = part->queryPartIndex();
                        unsigned subfile, subpartnum;
                        superFdesc->mapSubPart(partNo, subfile, subpartnum);
                        unsigned globalPartNo = superWidth*subfile+subpartnum;
                        globalPartNoMap.setValue(globalPartNo, p);
                        indexParts.append(LINK(part));
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
                {
                    IPartDescriptor *part = &_indexParts.item(p);
                    if (container.queryLocalData())
                    {
                        unsigned partNo = part->queryPartIndex();
                        globalPartNoMap.setValue(partNo, p);
                    }
                    indexParts.append(LINK(part));
                }
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

            unsigned currentNumLookupThreads = lookupHandlers.ordinality();
            if (currentNumLookupThreads>numIndexParts)
                lookupHandlers.removen(numIndexParts, currentNumLookupThreads-numIndexParts);
            else
            {
                while (currentNumLookupThreads<numIndexParts)
                    lookupHandlers.append(new CKeyLookupHandler(*this, currentNumLookupThreads++));
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
        preserveOrder = 0 == (joinFlags & JFreorderable);
        preserveGroups = input->isGrouped();
        ActPrintLog("KJ: preserveGroups=%s, preserveOrder=%s", preserveGroups?"true":"false", preserveOrder?"true":"false");

        if (keyHasTlk)
            getTlkKeyManagers(tlkKeyManagers);
        currentMatchIdx = 0;
        rhsState.clear();
        currentAdded = 0;
        eos = false;
        endOfInput = false;
        ForEachItemIn(b, lookupHandlers)
            lookupHandlers.item(b)->init();
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
                        {
                            doneListLimiter.dec();
                            break;
                        }
                        if (endOfInput)
                        {
                            // if disk fetch involved, then there may still be pending rows
                            if (!fetchHandler || !pendingJoinGroupList.queryCount())
                            {
                                eos = true;
                                // all done
                                return nullptr;
                            }
                        }
                        waitingForDoneGroups = true;
                    }
                    waitingForDoneGroupsSem.wait();
                }
                bool eog = false;
                if (preserveGroups)
                {
                    currentJoinGroupSize += currentAdded;
                    if (currentJoinGroup->hasFlag(CJoinGroup::GroupFlags::gf_eog))
                        eog = 0 != currentJoinGroupSize;
                    currentJoinGroupSize = 0;
                }
                currentMatchIdx = 0;
                rhsState.clear();
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
                            offset_t fpos;
                            const void *rhs;
                            if (0 == currentMatchIdx)
                                rhs = currentJoinGroup->queryFirstRhs(fpos, rhsState);
                            else
                                rhs = currentJoinGroup->queryNextRhs(fpos, rhsState);
                            do
                            {
                                if (!rhs)
                                {
                                    currentJoinGroup.clear();
                                    break;
                                }
                                ++currentMatchIdx;
                                transformedSize = helper->transform(rowBuilder, currentJoinGroup->queryLeft(), rhs, fpos, currentMatchIdx);
                                if (transformedSize)
                                {
                                    ret.setown(rowBuilder.finalizeRowClear(transformedSize));
                                    currentAdded++;
                                    if (currentAdded==keepLimit)
                                        currentJoinGroup.clear();
                                    break;
                                }
                                rhs = currentJoinGroup->queryNextRhs(fpos, rhsState);
                            }
                            while (true);
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
        if (fetchHandler)
            fetchHandler->stop(true);
        lookupThread.join();
        ForEachItemIn(b, lookupHandlers)
            lookupHandlers.item(b)->stop();
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
        bool doneListMaxHit = false;
        // moves complete CJoinGroup's from pending list to done list
        {
            CriticalBlock b(onCompleteCrit); // protecting both pendingJoinGroupList and doneJoinGroupList
            if (preserveOrder)
            {
                CJoinGroup *head = pendingJoinGroupList.queryHead();
                if (joinGroup != head)
                    return;
                do
                {
                    if (!head->complete())
                    {
                        if (head == joinGroup) // i.e. none ready
                            return;
                        else
                            break;
                    }
                    head = head->next;
                    CJoinGroup *doneJG = pendingJoinGroupList.removeHead();
                    if (transferToDoneList(doneJG))
                        doneListMaxHit = true;
                }
                while (head);
            }
            else if (preserveGroups)
            {
                // NB: when preserveGroups, the lhs group will always be complete at same time, so this will traverse whole group
                if (!joinGroup->hasFlag(CJoinGroup::GroupFlags::gf_head))
                    return; // intermediate rows are completing, but can't output any of those until head finishes, at which point head marker will shift to next if necessary (see below)
                unsigned numProcessed = 0;
                CJoinGroup *current = joinGroup;
                do
                {
                    if (!current->complete())
                    {
                        dbgassertex(numProcessed); // if onComplete called for a group, there should always be at least 1 complete group ready starting from signalled joinGroup
                        // update current so now marked as new head of group, so that when it completes it will be processed.
                        current->addFlag(CJoinGroup::GroupFlags::gf_head);
                        break;
                    }
                    CJoinGroup *next = current->next;
                    CJoinGroup *doneJG = pendingJoinGroupList.remove(current);
                    if (transferToDoneList(doneJG))
                        doneListMaxHit = true;
                    current = next;
                    ++numProcessed;
                }
                while (current);
            }
            else
            {
                CJoinGroup *doneJG = pendingJoinGroupList.remove(joinGroup);
                doneListMaxHit = transferToDoneList(doneJG);
            }
            bool expectedState = true;
            if (waitingForDoneGroups.compare_exchange_strong(expectedState, false))
                waitingForDoneGroupsSem.signal();
        }
        if (doneListMaxHit) // outside of crit, done group dequeue and signal may already have happened
            doneListLimiter.block();
    }
    virtual unsigned addRowEntry(unsigned partNo, const void *rhs, offset_t fpos, RowArray *&rowArrays, unsigned &numRowArrays) override
    {
        dbgassertex(partNo<lookupHandlers.ordinality());
        if (!rowArrays)
        {
            // If preserving order, a row array per handler/part is used to ensure order is preserved.
            numRowArrays = preserveOrder ? lookupHandlers.ordinality() : 1;
            rowArrays = (RowArray *)rowManager->allocate(sizeof(RowArray)*numRowArrays, createCompoundActSeqId(queryId(), AT_JoinGroupRhsRows));
            memset(rowArrays, 0, sizeof(RowArray)*numRowArrays);
        }
        RowArray &rowArray = rowArrays[preserveOrder ? partNo : 0];
        if (!rowArray.rows)
        {
            rowArray.rows = (Row *)rowManager->allocate(sizeof(Row), createCompoundActSeqId(queryId(), AT_JoinGroupRhsRows));
            rowArray.maxRows = RoxieRowCapacity(rowArray.rows) / sizeof(Row);
            rowArray.numRows = 0;
        }
        else if (rowArray.numRows==rowArray.maxRows)
        {
            if (rowArray.maxRows<4)
                ++rowArray.maxRows;
            else
                rowArray.maxRows += rowArray.maxRows/4;
            memsize_t newCapacity;
            rowManager->resizeRow(newCapacity, (void *&)rowArray.rows, RoxieRowCapacity(rowArray.rows), rowArray.maxRows*sizeof(Row), queryId());
            rowArray.maxRows = newCapacity / sizeof(Row);
        }
        Row &row = rowArray.rows[rowArray.numRows++];
        row.rhs = rhs;
        row.fpos = fpos;
        return rowArray.numRows-1;
    }
};


CActivityBase *createKeyedJoinSlave(CGraphElementBase *container) 
{ 
    return new CKeyedJoinSlave(container);
}

