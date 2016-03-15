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


#ifndef _thactivityutil_ipp
#define _thactivityutil_ipp

#include "jlib.hpp"
#include "jlzw.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jbuff.hpp"

#include "thormisc.hpp"
#include "thmem.hpp"
#include "thbuf.hpp"
#include "thgraphslave.hpp"
#include "eclhelper.hpp"
#define NO_BWD_COMPAT_MAXSIZE
#include "thorcommon.ipp"
#include "commonext.hpp"

#define OUTPUT_RECORDSIZE


class CPartHandler : public CSimpleInterface, implements IRowStream
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    virtual ~CPartHandler() { }
    virtual void setPart(IPartDescriptor *partDesc, unsigned partNoSerialized) = 0;
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info, IPartDescriptor *partDesc) { }
    virtual void stop() = 0;
};

IRowStream *createSequentialPartHandler(CPartHandler *partHandler, IArrayOf<IPartDescriptor> &partDescs, bool grouped);

#define CATCH_NEXTROWX_CATCH \
        catch (IException *_e) \
        { \
            this->processAndThrowOwnedException(_e); \
        }

#define CATCH_NEXTROW() \
    virtual const void *nextRow() \
    { \
        try \
        { \
            return nextRowNoCatch(); \
        } \
        CATCH_NEXTROWX_CATCH \
    } \
    inline const void *nextRowNoCatch() __attribute__((always_inline))

class CThorDataLink : implements IThorDataLink
{
    CActivityBase *owner;
    rowcount_t count, icount;
    unsigned outputId;
    unsigned limit;

protected:
    inline void dataLinkStart(unsigned _outputId = 0)
    {
        outputId = _outputId;
#ifdef _TESTING
        ActPrintLog(owner, "ITDL starting for output %d", outputId);
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
        ActPrintLog(owner, "ITDL output %d stopped, count was %" RCPF "d", outputId, getDataLinkCount());
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
        if (count==THORDATALINK_STARTED) {
            size32_t rsz = queryRowMetaData(this)->getMinRecordSize();
            ActPrintLog(owner, "Record size %s= %d", queryRowMetaData(this)->isVariableSize()?"(min) ":"",rsz);
        }   
#endif
#endif
        icount += v;
        count += v; 
    }

    inline bool started()
    {
        return (count & THORDATALINK_STARTED) ? true : false; 
    }

    inline bool stopped()
    {
        return (count & THORDATALINK_STOPPED) ? true : false;
    }


public:
    CThorDataLink(CActivityBase *_owner) : owner(_owner)
    {
        icount = count = 0;
    }
#ifdef _TESTING
    ~CThorDataLink()
    { 
        if(started()&&!stopped())
        {
            ActPrintLog(owner, "ERROR: ITDL was not stopped before destruction");
            dataLinkStop(); // get some info (even though failed)       
        }
    }           
#endif

    void dataLinkSerialize(MemoryBuffer &mb)
    {
        mb.append(count);
    }

    unsigned __int64 queryTotalCycles() const { return ((CSlaveActivity *)owner)->queryTotalCycles(); }
    unsigned __int64 queryEndCycles() const  { return ((CSlaveActivity *)owner)->queryEndCycles(); }

    inline rowcount_t getDataLinkGlobalCount()
    {
        return (count & THORDATALINK_COUNT_MASK); 
    } 
    inline rowcount_t getDataLinkCount()
    {
        return icount; 
    } 
    virtual void debugRequest(MemoryBuffer &msg) { }
    CActivityBase *queryFromActivity() { return owner; }

    void initMetaInfo(ThorDataLinkMetaInfo &info); // for derived children to call from getMetaInfo
    static void calcMetaInfoSize(ThorDataLinkMetaInfo &info,IThorDataLink *input); // for derived children to call from getMetaInfo
    static void calcMetaInfoSize(ThorDataLinkMetaInfo &info,IThorDataLink **link,unsigned ninputs);
    static void calcMetaInfoSize(ThorDataLinkMetaInfo &info, ThorDataLinkMetaInfo *infos,unsigned num);
};

#define STRAND_CATCH_NEXTROWX_CATCH \
        catch (IException *_e) \
        { \
            parent->processAndThrowOwnedException(_e); \
        }

#define STRAND_CATCH_NEXTROW() \
    virtual const void *nextRow() \
    { \
        try \
        { \
            return nextRowNoCatch(); \
        } \
        CATCH_NEXTROWX_CATCH \
    } \
    inline const void *nextRowNoCatch() __attribute__((always_inline))


class CStrandProcessor : public CInterfaceOf<IEngineRowStream>
{
protected:
    CSlaveActivity &parent;
    IEngineRowAllocator *rowAllocator = nullptr;
    IEngineRowStream *inputStream;
    ActivityTimeAccumulator totalCycles;
//    mutable CRuntimeStatisticCollection stats;
    rowcount_t count = 0, icount = 0;
    unsigned numProcessedLastGroup = 0;
    const bool timeActivities;
    bool stopped = false;
    unsigned outputId; // if activity had >1 , this identifies (for tracing purposes) which output this strand belongs to.

public:
    explicit CStrandProcessor(CSlaveActivity &_parent, IEngineRowStream *_inputStream, unsigned _outputId, bool needsAllocator=false)
      : parent(_parent), inputStream(_inputStream), outputId(_outputId), timeActivities(_parent.timeActivities)
    {
/*
        if (needsAllocator)
            rowAllocator = parent.queryCodeContext()->getRowAllocatorEx(parent.queryOutputMeta(), parent.queryId(), roxiemem::RHFunique);
        else
            rowAllocator = NULL;
*/
    }
    ~CStrandProcessor()
    {
        ::Release(rowAllocator);
#ifdef _TESTING
        if(hasStarted() && !hasStopped())
        {
            parent.ActPrintLog("ERROR: ITDL was not stopped before destruction");
            dataLinkStop(); // get some info (even though failed)
        }
#endif
    }

// IRowStream
    virtual void stop()
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
    virtual void resetEOF()
    {
        inputStream->resetEOF();
    }

// IThorStrand
    virtual void start()
    {
        count = 0;
        numProcessedLastGroup = 0;
        totalCycles.reset();
//        stats.reset();

#ifdef _TESTING
        assertex(!hasStarted() || hasStopped());      // ITDL started twice
#endif
        icount = 0;
        rowcount_t prevCount = count & THORDATALINK_COUNT_MASK;
        count = prevCount | THORDATALINK_STARTED;
    }
    virtual void reset()
    {
        stopped = false;
    }

protected:
    inline void dataLinkStart(unsigned _outputId = 0)
    {
        outputId = _outputId;
#ifdef _TESTING
        parent.ActPrintLog("ITDL starting for output %d", outputId);
#endif
#ifdef _TESTING
        assertex(!hasStarted() || hasStopped());      // ITDL started twice
#endif
        icount = 0;
//      count = THORDATALINK_STARTED;
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
    CSlaveActivity *queryFromActivity() { return parent; }
};

interface ISmartBufferNotify
{
    virtual bool startAsync() =0;                       // return true if need to start asynchronously
    virtual void onInputStarted(IException *e) =0;      // e==NULL if start suceeded, NB only called with exception if Async
    virtual void onInputFinished(rowcount_t count) =0;
};

interface IDiskUsage;
IThorDataLink *createDataLinkSmartBuffer(CActivityBase *activity,IThorDataLink *in,size32_t bufsize,bool spillenabled,bool preserveGrouping=true,rowcount_t maxcount=RCUNBOUND,ISmartBufferNotify *notify=NULL, bool inputstarted=false, IDiskUsage *_diskUsage=NULL); //maxcount is maximum rows to read set to RCUNBOUND for all

bool isSmartBufferSpillNeeded(CActivityBase *act);

StringBuffer &locateFilePartPath(CActivityBase *activity, const char *logicalFilename, IPartDescriptor &partDesc, StringBuffer &filePath);
void doReplicate(CActivityBase *activity, IPartDescriptor &partDesc, ICopyFileProgress *iProgress=NULL);
void cancelReplicates(CActivityBase *activity, IPartDescriptor &partDesc);

#define TW_Extend 0x01
#define TW_Direct 0x02
#define TW_External 0x04
#define TW_RenameToPrimary 0x08
#define TW_Temporary 0x10
interface IPartDescriptor;
IFileIO *createMultipleWrite(CActivityBase *activity, IPartDescriptor &partDesc, unsigned recordSize, unsigned twFlags, bool &compress, ICompressor *ecomp, ICopyFileProgress *iProgress, bool *aborted, StringBuffer *_locationName=NULL);


#endif
