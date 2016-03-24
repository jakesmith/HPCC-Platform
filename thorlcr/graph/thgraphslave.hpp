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

#ifndef _THGRAPHSLAVE_HPP_
#define _THGRAPHSLAVE_HPP_

#ifdef _WIN32
    #ifdef GRAPHSLAVE_EXPORTS
        #define graphslave_decl __declspec(dllexport)
    #else
        #define graphslave_decl __declspec(dllimport)
    #endif
#else
    #define graphslave_decl
#endif

#include "platform.h"
#include "slave.hpp"
#include "thormisc.hpp"
#include "thorcommon.hpp"
#include "thgraph.hpp"
#include "jdebug.hpp"
#include "traceslave.hpp"
#include "thorstrand.hpp"

interface ILookAheadEngineRowStream : extends IEngineRowStream
{
    virtual void start() = 0;
};
class CSlaveGraphElement;
class graphslave_decl CSlaveActivity : public CActivityBase, implements IThorDataLinkExt, implements IThorSlaveActivity
{
    mutable MemoryBuffer *data;
    mutable CriticalSection crit;

protected:
    Owned<ILookAheadEngineRowStream> lookAheadStream;
    IPointerArrayOf<IThorDataLink> inputs, outputs;
    UnsignedArray inputSourceIdxs;
    IPointerArrayOf<IEngineRowStream> inputStreams;
    IPointerArrayOf<IStrandJunction> inputJunctions;
    BoolArray inputsStopped;
    IThorDataLink *input = nullptr;
    bool inputStopped = true;
    unsigned inputSourceIdx = 0;
    IEngineRowStream *inputStream = NULL;
    MemoryBuffer startCtx;
    bool optStableInput = true; // is the input forced to ordered?
    bool optUnstableInput = false;  // is the input forced to unordered?
    bool optUnordered = false; // is the output specified as unordered?
    Owned<IStrandJunction> junction;
    unsigned outputIdx = 0; // for IThorDataLinkExt

    ActivityTimeAccumulator totalCycles;
    rowcount_t count = 0, icount = 0;

protected:
    inline void dataLinkStart()
    {
#ifdef _TESTING
        ActPrintLog("ITDL starting for output %d", outputIdx);
#endif
#ifdef _TESTING
        assertex(!started() || stopped());      // ITDL started twice
#endif
        icount = 0;
//      count = THORDATALINK_STARTED;
        rowcount_t prevCount = count & THORDATALINK_COUNT_MASK;
        count = prevCount | THORDATALINK_STARTED;
    }
    inline void dataLinkStop()
    {
#ifdef _TESTING
        assertex(started());        // ITDL stopped without being started
#endif
        count |= THORDATALINK_STOPPED;
#ifdef _TESTING
        ActPrintLog("ITDL output %d stopped, count was %" RCPF "d", outputIdx, getDataLinkCount());
#endif
    }
    inline void dataLinkIncrement()
    {
        dataLinkIncrement(1);
    }
    inline void dataLinkIncrement(rowcount_t v)
    {
#ifdef _TESTING
        assertex(started());
#ifdef OUTPUT_RECORDSIZE
        if (count==THORDATALINK_STARTED)
        {
            size32_t rsz = queryRowMetaData(this)->getMinRecordSize();
            ActPrintLog(owner, "Record size %s= %d", queryRowMetaData(this)->isVariableSize()?"(min) ":"",rsz);
        }
#endif
#endif
        icount += v;
        count += v;
    }
    inline bool started() const
    {
        return (count & THORDATALINK_STARTED) ? true : false;
    }
    inline bool stopped() const
    {
        return (count & THORDATALINK_STOPPED) ? true : false;
    }
    inline rowcount_t getDataLinkGlobalCount() const
    {
        return (count & THORDATALINK_COUNT_MASK);
    }
    inline rowcount_t getDataLinkCount() const
    {
        return icount;
    }
public:
//    IMPLEMENT_IINTERFACE;
    virtual void Link(void) const;
    virtual bool Release(void) const;

    CSlaveActivity(CGraphElementBase *container);
    ~CSlaveActivity();
    virtual void clearConnections();
    virtual void releaseIOs();
    virtual MemoryBuffer &queryInitializationData(unsigned slave) const;
    virtual MemoryBuffer &getInitializationData(unsigned slave, MemoryBuffer &mb) const;
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx) override;
    virtual void connectInputStreams(bool consumerOrdered);

    IThorDataLink *queryOutput(unsigned index) const;
    IThorDataLink *queryInput(unsigned index) const;
    IEngineRowStream *queryInputStream(unsigned index) const;
    unsigned queryNumInputs() const { return inputs.ordinality(); }
    void appendOutput(IThorDataLink *itdl);
    void appendOutputLinked(IThorDataLink *itdl);
    void startInput(unsigned index, const char *extra=NULL);
    void startAllInputs();
    void stopInput(unsigned index, const char *extra=NULL);
    void stopAllInputs();
    ActivityTimeAccumulator &getTotalCyclesRef() { return totalCycles; }
    unsigned __int64 queryLocalCycles() const;
    virtual unsigned __int64 queryTotalCycles() const; // some acts. may calculate accumulated total from inputs (e.g. splitter)
    virtual unsigned __int64 queryEndCycles() const;
    virtual void serializeStats(MemoryBuffer &mb);
    void debugRequest(unsigned edgeIdx, MemoryBuffer &msg);

// IThorDataLink
    virtual CSlaveActivity *queryFromActivity() override { return this; }
    virtual IStrandJunction *getOutputStreams(CActivityBase &ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered);
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override { }
    virtual bool isGrouped() const override;
    virtual IOutputMetaData * queryOutputMeta() const;
    virtual unsigned queryOutputIdx() const override { return outputIdx; }
    virtual void dataLinkSerialize(MemoryBuffer &mb) const override;
    virtual bool isInputOrdered(bool consumerOrdered) const override
    {
        if (optStableInput)
            return true;
        if (optUnstableInput)
            return false;
        if (optUnordered)
            return false;
        return consumerOrdered;
    }
    virtual void debugRequest(MemoryBuffer &msg) override;

    virtual IEngineRowStream *queryStream() { return inputStream; }
    virtual void setOutputStream(IEngineRowStream *stream) override { inputStream = stream; }

// IThorDataLink
    virtual void start() override;
    virtual void stop() override;
// IThorDataLinkExt
    virtual void setOutputIdx(unsigned idx) override { outputIdx = idx; }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &in, MemoryBuffer &out) override { }
    virtual void setInputStream(unsigned index, IThorDataLink *input, unsigned inputOutIdx, bool consumerOrdered);
    virtual void processDone(MemoryBuffer &mb) override { };
    virtual void reset() override;
    virtual void abort() override;
};


IEngineRowStream *connectSingleStream(CActivityBase &activity, IThorDataLink *input, unsigned idx, Owned<IStrandJunction> &junction, bool consumerOrdered);
IEngineRowStream *connectSingleStream(CActivityBase &activity, IThorDataLink *input, unsigned idx, bool consumerOrdered);


#define STRAND_CATCH_NEXTROWX_CATCH \
        catch (IException *_e) \
        { \
            parent->processAndThrowOwnedException(_e); \
        }

#define STRAND_CATCH_NEXTROW() \
    virtual const void *nextRow() override \
    { \
        try \
        { \
            return nextRowNoCatch(); \
        } \
        CATCH_NEXTROWX_CATCH \
    } \
    inline const void *nextRowNoCatch() __attribute__((always_inline))


class CThorStrandProcessor : public CInterfaceOf<IEngineRowStream>
{
protected:
    CSlaveActivity &parent;
    IEngineRowStream *inputStream;
    ActivityTimeAccumulator totalCycles;
//    mutable CRuntimeStatisticCollection stats;
    rowcount_t count = 0, icount = 0;
    unsigned numProcessedLastGroup = 0;
    const bool timeActivities;
    bool stopped = false;
    unsigned outputId; // if activity had >1 , this identifies (for tracing purposes) which output this strand belongs to.
    Linked<IHThorArg> baseHelper;

protected:
    inline IHThorArg *queryHelper() const { return baseHelper; }
    inline void dataLinkStart()
    {
#ifdef _TESTING
        parent.ActPrintLog("ITDL starting for output %d", outputId);
#endif
#ifdef _TESTING
        assertex(!hasStarted() || hasStopped());      // ITDL started twice
#endif
        icount = 0;
        rowcount_t prevCount = count & THORDATALINK_COUNT_MASK;
        count = prevCount | THORDATALINK_STARTED;
    }

    inline void dataLinkStop()
    {
#ifdef _TESTING
        assertex(hasStarted());        // ITDL stopped without being started
#endif
        count |= THORDATALINK_STOPPED;
#ifdef _TESTING
        parent.ActPrintLog("ITDL output %d stopped, count was %" RCPF "d", outputId, getDataLinkCount());
#endif
    }

    inline void dataLinkIncrement()
    {
        dataLinkIncrement(1);
    }

    inline void dataLinkIncrement(rowcount_t v)
    {
#ifdef _TESTING
        assertex(hasStarted());
#ifdef OUTPUT_RECORDSIZE
        if (count==THORDATALINK_STARTED)
        {
            size32_t rsz = parent.queryRowMetaData(this)->getMinRecordSize();
            parent.ActPrintLog("Record size %s= %d", parent.queryRowMetaData(this)->isVariableSize()?"(min) ":"",rsz);
        }
#endif
#endif
        icount += v;
        count += v;
    }

    inline bool hasStarted() const { return (count & THORDATALINK_STARTED) ? true : false; }
    inline bool hasStopped() const { return (count & THORDATALINK_STOPPED) ? true : false; }

    inline void dataLinkSerialize(MemoryBuffer &mb) const
    {
        mb.append(count);
    }
    unsigned __int64 queryTotalCycles() const { return parent.queryTotalCycles(); }
    unsigned __int64 queryEndCycles() const  { return parent.queryEndCycles(); }

    inline rowcount_t getDataLinkGlobalCount() { return (count & THORDATALINK_COUNT_MASK); }
    inline rowcount_t getDataLinkCount() const { return icount; }
    virtual void debugRequest(MemoryBuffer &msg) { }

public:
    explicit CThorStrandProcessor(CSlaveActivity &_parent, IEngineRowStream *_inputStream, unsigned _outputId)
      : parent(_parent), inputStream(_inputStream), outputId(_outputId), timeActivities(_parent.queryTimeActivities())
    {
        baseHelper.set(parent.queryHelper());
    }
    ~CThorStrandProcessor()
    {
#ifdef _TESTING
        if(hasStarted() && !hasStopped())
        {
            parent.ActPrintLog("ERROR: ITDL was not stopped before destruction");
            dataLinkStop(); // get some info (even though failed)
        }
#endif
    }
    __declspec(noreturn) void processAndThrowOwnedException(IException *_e) __attribute__((noreturn))
    {
        IThorException *e = QUERYINTERFACE(_e, IThorException);
        if (e)
        {
            if (!e->queryActivityId())
                setExceptionActivityInfo(parent.queryContainer(), e);
        }
        else
        {
            e = MakeActivityException(&parent, _e);
            _e->Release();
        }
        throw e;
    }
    virtual void start()
    {
        count = 0;
        numProcessedLastGroup = 0;
        totalCycles.reset();
//        stats.reset();

        dataLinkStart();
    }
    virtual void reset()
    {
        stopped = false;
    }

// IRowStream
    virtual void stop() override
    {
        if (!stopped)
        {
            if (inputStream)
                inputStream->stop();
//            parent.stop();
//            parent.mergeStrandStats(processed, totalCycles, stats);
        }
        stopped = true;

#ifdef _TESTING
        assertex(hasStarted());        // ITDL stopped without being started
#endif
        count |= THORDATALINK_STOPPED;
    }
// IEngineRowStream
    virtual void resetEOF() override
    {
        inputStream->resetEOF();
    }
};

class CThorStrandedActivity : public CSlaveActivity
{
protected:
    CThorStrandOptions strandOptions;
    IArrayOf<CThorStrandProcessor> strands;
    Owned<IStrandBranch> branch;
    Owned<IStrandJunction> splitter;
    Owned<IStrandJunction> sourceJunction; // A junction applied to the output of a source activity
    std::atomic<unsigned> active;
protected:
    void onStartStrands();
public:
    CThorStrandedActivity(CGraphElementBase *container)
        : CSlaveActivity(container), strandOptions(*container), active(0)
    {
    }

    //This function is pure (But also implemented out of line) to force the derived classes to implement it.
    //After calling the base class start method, and initialising any values from the helper they must call onStartStrands(),
    //this must also happen before any rows are read from the strands (e.g., by a source junction)
//    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) = 0;

    /* JCS - Don't really understand the above pure + defined approach.
     * If defined then, isn't pure meaningless, i.e. derived class does *not* need to define it as there is a definition available
     * in base class
     */
    //For some reason gcc doesn't let you specify a function as pure virtual and define it at the same time.
    virtual void start() override;
    virtual void reset() override;
    virtual void stop() override;
    virtual CThorStrandProcessor *createStrandProcessor(IEngineRowStream *instream) = 0;

    //MORE: Possibly this class should be split into two for sinks and non sinks...
    virtual CThorStrandProcessor *createStrandSourceProcessor(bool inputOrdered) = 0;

    inline unsigned numStrands() const { return strands.ordinality(); }

// IThorDataLink
    virtual IStrandJunction *getOutputStreams(CActivityBase &activity, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered) override;
};



class graphslave_decl CSlaveGraphElement : public CGraphElementBase
{
public:
    CSlaveGraphElement(CGraphBase &owner, IPropertyTree &xgmml) : CGraphElementBase(owner, xgmml)
    {
    }
};

class CJobSlave;
class graphslave_decl CSlaveGraph : public CGraphBase
{
    CJobSlave *jobS;
    Semaphore getDoneSem;
    bool initialized, progressActive, progressToCollect;
    CriticalSection progressCrit;
    SpinLock progressActiveLock;

public:

    CSlaveGraph(CJobChannel &jobChannel);
    ~CSlaveGraph() { }

    void connect();
    void init(MemoryBuffer &mb);
    void recvStartCtx();
    bool recvActivityInitData(size32_t parentExtractSz, const byte *parentExtract);
    void setExecuteReplyTag(mptag_t _executeReplyTag) { executeReplyTag = _executeReplyTag; }
    void initWithActData(MemoryBuffer &in, MemoryBuffer &out);
    void getDone(MemoryBuffer &doneInfoMb);
    void serializeDone(MemoryBuffer &mb);
    IThorResult *getGlobalResult(CActivityBase &activity, IRowInterfaces *rowIf, activity_id ownerId, unsigned id);

    virtual void executeSubGraph(size32_t parentExtractSz, const byte *parentExtract);
    virtual bool serializeStats(MemoryBuffer &mb);
    virtual bool preStart(size32_t parentExtractSz, const byte *parentExtract);
    virtual void start();
    virtual void create(size32_t parentExtractSz, const byte *parentExtract);
    virtual void abort(IException *e);
    virtual void done();
    virtual void end();
    virtual IThorGraphResults *createThorGraphResults(unsigned num);

// IExceptionHandler
    virtual bool fireException(IException *e)
    {
        IThorException *te = QUERYINTERFACE(e, IThorException);
        StringBuffer s;
        if (!te || !te->queryGraphId())
        {
            Owned<IThorException> e2 = MakeGraphException(this, e);
            e2->setAudience(e->errorAudience());
            return CGraphBase::fireException(e2);
        }
        else
            return CGraphBase::fireException(e);
    }
};

interface ISlaveWatchdog;
class graphslave_decl CJobSlave : public CJobBase
{
    ISlaveWatchdog *watchdog;
    Owned<IPropertyTree> workUnitInfo;
    size32_t oldNodeCacheMem;
    unsigned channelMemorySize;

public:
    IMPLEMENT_IINTERFACE;

    CJobSlave(ISlaveWatchdog *_watchdog, IPropertyTree *workUnitInfo, const char *graphName, ILoadedDllEntry *querySo, mptag_t _mptag, mptag_t _slavemptag);

    virtual void addChannel(IMPServer *mpServer);
    virtual void startJob();
    const char *queryFindString() const { return key.get(); } // for string HT

    virtual IGraphTempHandler *createTempHandler(bool errorOnMissing);
    ISlaveWatchdog *queryProgressHandler() { return watchdog; }

    virtual mptag_t deserializeMPTag(MemoryBuffer &mb);
    virtual __int64 getWorkUnitValueInt(const char *prop, __int64 defVal) const;
    virtual StringBuffer &getWorkUnitValue(const char *prop, StringBuffer &str) const;
    virtual bool getWorkUnitValueBool(const char *prop, bool defVal) const;
    virtual IThorAllocator *createThorAllocator();
    virtual void debugRequest(MemoryBuffer &msg, const char *request) const;

// IExceptionHandler
    virtual bool fireException(IException *e)
    {
        return queryJobChannel(0).fireException(e);
    }
// IThreadFactory
    IPooledThread *createNew();
};

class graphslave_decl CJobSlaveChannel : public CJobChannel
{
    CriticalSection graphRunCrit;
public:
    CJobSlaveChannel(CJobBase &job, IMPServer *mpServer, unsigned channel);

    virtual IBarrier *createBarrier(mptag_t tag);
    virtual CGraphBase *createGraph()
    {
        return new CSlaveGraph(*this);
    }
 // IGraphCallback
    virtual void runSubgraph(CGraphBase &graph, size32_t parentExtractSz, const byte *parentExtract);
// IExceptionHandler
    virtual bool fireException(IException *e)
    {
        CMessageBuffer msg;
        msg.append((int)smt_errorMsg);
        msg.append(queryMyRank()-1);
        IThorException *te = QUERYINTERFACE(e, IThorException);
        bool userOrigin = false;
        if (te)
        {
            te->setJobId(queryJob().queryKey());
            te->setSlave(queryMyRank());
            if (!te->queryOrigin())
            {
                VStringBuffer msg("SLAVE #%d", queryMyRank());
                te->setOrigin(msg);
            }
            else if (0 == stricmp("user", te->queryOrigin()))
                userOrigin = true;
        }
        serializeThorException(e, msg);
        if (userOrigin)
        {
            // wait for reply
            if (!queryJobComm().sendRecv(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
                EXCLOG(e, "Failed to sendrecv to master");
        }
        else
        {
            if (!queryJobComm().send(msg, 0, queryJob().querySlaveMpTag(), LONGTIMEOUT))
                EXCLOG(e, "Failed to send to master");
        }
        return true;
    }
};

interface IPartDescriptor;
extern graphslave_decl bool ensurePrimary(CActivityBase *activity, IPartDescriptor &partDesc, OwnedIFile & ifile, unsigned &location, StringBuffer &path);
extern graphslave_decl IReplicatedFile *createEnsurePrimaryPartFile(CActivityBase &activity, const char *logicalFilename, IPartDescriptor *partDesc);
extern graphslave_decl IThorFileCache *createFileCache(unsigned limit);

#endif
