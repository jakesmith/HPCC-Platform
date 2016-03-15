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

class CSlaveActivity;

class CSlaveGraphElement;
class graphslave_decl CSlaveActivity : public CActivityBase
{
    mutable MemoryBuffer *data;
    mutable CriticalSection crit;

protected:
    IPointerArrayOf<IThorDataLink> inputs, outputs;
    ActivityTimeAccumulator totalCycles;
    MemoryBuffer startCtx;
    bool optStableInput = true; // is the input forced to ordered?
    bool optUnstableInput = false;  // is the input forced to unordered?
    bool optUnordered = false; // is the output specified as unordered?

public:
    IMPLEMENT_IINTERFACE;

    CSlaveActivity(CGraphElementBase *container);
    ~CSlaveActivity();
    virtual void clearConnections();
    virtual void releaseIOs();
    virtual void init(MemoryBuffer &in, MemoryBuffer &out) { }
    virtual void processDone(MemoryBuffer &mb) { };
    virtual void abort();
    virtual MemoryBuffer &queryInitializationData(unsigned slave) const;
    virtual MemoryBuffer &getInitializationData(unsigned slave, MemoryBuffer &mb) const;
    virtual void onCreate();
    virtual void onCreateN(unsigned num);

    IThorDataLink *queryOutput(unsigned index);
    IThorDataLink *queryInput(unsigned index);
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx);
    void appendOutput(IThorDataLink *itdl);
    void appendOutputLinked(IThorDataLink *itdl);
    void startInput(IThorDataLink *itdl, const char *extra=NULL);
    void stopInput(IRowStream *itdl, const char *extra=NULL);

    ActivityTimeAccumulator &getTotalCyclesRef() { return totalCycles; }
    unsigned __int64 queryLocalCycles() const;
    virtual unsigned __int64 queryTotalCycles() const; // some acts. may calculate accumulated total from inputs (e.g. splitter)
    virtual unsigned __int64 queryEndCycles() const;
    virtual void serializeStats(MemoryBuffer &mb);
    void debugRequest(unsigned edgeIdx, CMessageBuffer &msg);
    virtual bool isInputOrdered(bool consumerOrdered, unsigned idx) const
    {
        if (optStableInput)
            return true;
        if (optUnstableInput)
            return false;
        if (optUnordered)
            return false;
        return consumerOrdered;
    }
};


#define MAX_SENSIBLE_STRANDS 1024 // Architecture dependent...
class CThorStrandOptions
{
    // Typically set from hints, common to many stranded activities
public:
    explicit CThorStrandOptions(IPropertyTree &_graphNode)
    {
        //PARALLEL(1) can be used to explicitly disable parallel processing.
        numStrands = _graphNode.getPropInt("att[@name='parallel']/@value", 0);
        if ((numStrands == NotFound) || (numStrands > MAX_SENSIBLE_STRANDS))
            numStrands = getAffinityCpus();
        blockSize = _graphNode.getPropInt("hint[@name='strandblocksize']/@value", 0);
    }
    CThorStrandOptions(const CThorStrandOptions &from, CGraphElementBase &container)
    {
        numStrands = from.numStrands;
        blockSize = from.blockSize;

        if (!blockSize)
            blockSize = container.getOptInt("strandBlockSize");
        if (numStrands == 0)
            numStrands = container.getOptInt("forceNumStrands");
    }
public:
    unsigned numStrands = 0; // if 1 it forces single-stranded operations.  (Useful for testing.)
    unsigned blockSize = 0;
};

class CThorStrandedActivity : public CSlaveActivity
{
protected:
    CThorStrandOptions strandOptions;
    IArrayOf<CStrandProcessor> strands;
    Owned<IStrandBranch> branch;
    Owned<IStrandJunction> splitter;
    Owned<IStrandJunction> sourceJunction; // A junction applied to the output of a source activity
    std::atomic<unsigned> active;
public:
    CThorStrandedActivity(CGraphElementBase *container, const CThorStrandOptions &_strandOptions)
        : CSlaveActivity(container), strandOptions(_strandOptions, *container)
    {
        active = 0;
    }

    virtual void onCreate() override
    {
        CSlaveActivity::onCreateN(strands.ordinality());
    }

    //This function is pure (But also implemented out of line) to force the derived classes to implement it.
    //After calling the base class start method, and initialising any values from the helper they must call onStartStrands(),
    //this must also happen before any rows are read from the strands (e.g., by a source junction)
    virtual void start(unsigned parentExtractSize, const byte *parentExtract, bool paused) = 0;

    virtual void reset()
    {
        assertex(active==0);
        ForEachItemIn(idx, strands)
            strands.item(idx).reset();
        resetJunction(splitter);
        CSlaveActivity::reset();
        resetJunction(sourceJunction);
    }

    virtual void stop()
    {
        // Called from the strands... which should ensure that stop is not called more than once per strand
        //The first strand to call
        if (active)
            --active;
        if (!active)
            CRoxieServerActivity::stop();
    }

    virtual IStrandJunction *getOutputStreams(IRoxieSlaveContext *ctx, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered)
    {
        assertex(idx == 0);
        assertex(strands.empty());
        CSlaveActivity::connectDependencies();

        bool inputOrdered = isInputOrdered(consumerOrdered, idx);
        //Note, numStrands == 1 is an explicit request to disable threading
        if (consumerOptions && (consumerOptions->numStrands != 1) && (strandOptions.numStrands != 1))
        {
            //Check to see if the consumer's settings should override
            if (strandOptions.numStrands == 0)
            {
                strandOptions.numStrands = consumerOptions->numStrands;
                strandOptions.blockSize = consumerOptions->blockSize;
            }
            else if (consumerOptions->numStrands > strandOptions.numStrands)
            {
                strandOptions.numStrands = consumerOptions->numStrands;
            }
        }

        Owned <IStrandJunction> recombiner;
        if (input)
        {
            if (strandOptions.numStrands == 1)
            {
                // 1 means explicitly requested single-strand.
                IEngineRowStream *instream = connectSingleStream(ctx, input, sourceIdx, junction, inputOrdered);
                strands.append(*createStrandProcessor(instream));
            }
            else
            {
                PointerArrayOf<IEngineRowStream> instreams;
                recombiner.setown(input->getOutputStreams(ctx, sourceIdx, instreams, &strandOptions, inputOrdered));
                if ((instreams.length() == 1) && (strandOptions.numStrands != 0))  // 0 means did not specify - we should use the strands that our upstream provides
                {
                    assertex(recombiner == NULL);
                    // Create a splitter to split the input into n... and a recombiner if need to preserve sorting
                    if (inputOrdered)
                    {
                        branch.setown(createStrandBranch(ctx->queryRowManager(), strandOptions.numStrands, strandOptions.blockSize, true, input->queryOutputMeta()->isGrouped(), false));
                        splitter.set(branch->queryInputJunction());
                        recombiner.set(branch->queryOutputJunction());
                    }
                    else
                    {
                        splitter.setown(createStrandJunction(ctx->queryRowManager(), 1, strandOptions.numStrands, strandOptions.blockSize, false));
                    }
                    splitter->setInput(0, instreams.item(0));
                    for (unsigned strandNo = 0; strandNo < strandOptions.numStrands; strandNo++)
                        strands.append(*createStrandProcessor(splitter->queryOutput(strandNo)));
                }
                else
                {
                    // Ignore my hint and just use the width already split into...
                    ForEachItemIn(strandNo, instreams)
                        strands.append(*createStrandProcessor(instreams.item(strandNo)));
                }
            }
        }
        else
        {
            unsigned numStrands = strandOptions.numStrands ? strandOptions.numStrands : 1;
            for (unsigned i=0; i < numStrands; i++)
                strands.append(*createStrandSourceProcessor(inputOrdered));

            if (inputOrdered && (numStrands > 1))
            {
                if (consumerOptions)
                {
                    //If the output activities are also stranded then need to create a version of the branch
                    bool isGrouped = queryOutputMeta()->isGrouped();
                    branch.setown(createStrandBranch(ctx->queryRowManager(), strandOptions.numStrands, strandOptions.blockSize, true, isGrouped, true));
                    sourceJunction.set(branch->queryInputJunction());
                    recombiner.set(branch->queryOutputJunction());

                    //This is different from the branch above.  The first "junction" has the source activity as the input, and the outputs as the result of the activity
                    for (unsigned strandNo = 0; strandNo < strandOptions.numStrands; strandNo++)
                    {
                        sourceJunction->setInput(strandNo, &strands.item(strandNo));
                        streams.append(sourceJunction->queryOutput(strandNo));
                    }
#ifdef TRACE_STRANDS
                    if (traceLevel > 2)
                        DBGLOG("Executing activity %u with %u strands", activityId, strands.ordinality());
#endif
                    return recombiner.getClear();
                }
                else
                    recombiner.setown(createStrandJunction(ctx->queryRowManager(), numStrands, 1, strandOptions.blockSize, inputOrdered));
            }
        }
        ForEachItemIn(i, strands)
            streams.append(&strands.item(i));
#ifdef TRACE_STRANDS
        if (traceLevel > 2)
            DBGLOG("Executing activity %u with %u strands", activityId, strands.ordinality());
#endif

        return recombiner.getClear();
    }

    virtual CStrandProcessor *createStrandProcessor(IEngineRowStream *instream) = 0;

    //MORE: Possibly this class should be split into two for sinks and non sinks...
    virtual CStrandProcessor *createStrandSourceProcessor(bool inputOrdered) = 0;

    inline unsigned numStrands() const { return strands.ordinality(); }
protected:

    void onStartStrands()
    {
        ForEachItemIn(idx, strands)
            strands.item(idx).start();
    }
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
    virtual void debugRequest(CMessageBuffer &msg, const char *request) const;

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
