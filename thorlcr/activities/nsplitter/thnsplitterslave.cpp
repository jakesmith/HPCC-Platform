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

#include "thnsplitterslave.ipp"

#include "thbuf.hpp"
#include "thormisc.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"

interface ISharedSmartBuffer;
class NSplitterSlaveActivity;

class CSplitterOutputBase : public CSimpleInterfaceOf<IEngineRowStream>
{
protected:
    ActivityTimeAccumulator totalCycles;
public:
    unsigned __int64 queryTotalCycles() const { return totalCycles.totalCycles; }

// IEngineRowStream
    virtual void resetEOF() { throwUnexpected(); }
};

class CSplitterOutput : public CSplitterOutputBase
{
    NSplitterSlaveActivity &activity;
    Semaphore writeBlockSem;

    unsigned output;
    rowcount_t rec, max;

public:
    CSplitterOutput(NSplitterSlaveActivity &_activity, unsigned output);

    virtual void start();
    virtual void stop() override;
    virtual const void *nextRow() override;
};


//
// NSplitterSlaveActivity
//

class NSplitterSlaveActivity : public CSlaveActivity, implements ISharedSmartBufferCallback
{
    bool spill;
    bool eofHit;
    bool inputsConfigured;
    bool writeBlocked, pagedOut;
    CriticalSection startLock, writeAheadCrit;
    PointerArrayOf<Semaphore> stalledWriters;
    unsigned nstopped;
    rowcount_t recsReady;
    bool grouped;
    Owned<IException> startException, writeAheadException;
    Owned<ISharedSmartBuffer> smartBuf;
    bool inputPrepared = false;

    // NB: CWriter only used by 'balanced' splitter, which blocks write when too far ahead
    class CWriter : public CSimpleInterface, IThreaded
    {
        NSplitterSlaveActivity &parent;
        CThreadedPersistent threaded;
        bool stopped;
        rowcount_t current;

    public:
        CWriter(NSplitterSlaveActivity &_parent) : parent(_parent), threaded("CWriter", this)
        {
            current = 0;
            stopped = true;
        }
        ~CWriter() { stop(); }
        virtual void main()
        {
            Semaphore writeBlockSem;
            while (!stopped && !parent.eofHit)
                current = parent.writeahead(current, stopped, writeBlockSem);
        }
        void start()
        {
            stopped = false;
            threaded.start();
        }
        virtual void stop()
        {
            if (!stopped)
            {
                stopped = true;
                threaded.join();
            }
        }
    } writer;
    class CNullInput : public CSplitterOutputBase
    {
    public:
        virtual const void *nextRow() override { throwUnexpected(); return NULL; }
        virtual void stop() override { throwUnexpected(); }
    };
    class CInputWrapper : public CSplitterOutputBase
    {
        IRowStream *inputStream;
        NSplitterSlaveActivity &activity;

    public:
        CInputWrapper(NSplitterSlaveActivity &_activity, IRowStream *_inputStream) : activity(_activity), inputStream(_inputStream) { }
        virtual const void *nextRow()
        {
            ActivityTimer t(totalCycles, activity.queryTimeActivities());
            return inputStream->nextRow();
        }
        virtual void stop() { inputStream->stop(); }
    };
    class CDelayedInput : public CSimpleInterfaceOf<IThorDataLink>
    {
        Owned<IEngineRowStream> inputStream;
        Linked<NSplitterSlaveActivity> activity;
        mutable SpinLock processActiveLock;

        class CStream : public CSimpleInterfaceOf<IEngineRowStream>
        {
            CDelayedInput &owner;
        public:
            CStream(CDelayedInput &_owner) : owner(_owner) { }
        // IEngineRowStream
            virtual const void *nextRow() override { return owner.nextRow(); }
            virtual void stop() override { owner.stop(); }
            virtual void resetEOF() override { owner.resetEOF(); }
        } stream;
    public:
        CDelayedInput(NSplitterSlaveActivity &_activity) : activity(&_activity), stream(*this) { }
        void setInput(IEngineRowStream *_inputStream)
        {
            SpinBlock b(processActiveLock);
            inputStream.setown(_inputStream);
        }
        const void *nextRow()
        {
            OwnedConstThorRow row = inputStream->nextRow();
            if (row)
                activity->dataLinkIncrement();
            return row.getClear();
        }
        void stop()
        {
            inputStream->stop();
            activity->dataLinkStop();
        }
        void resetEOF()
        {
            inputStream->resetEOF();
        }
    // IThorDataLink impl.
        virtual void start()
        {
            activity->ensureInputsConfigured();
            activity->dataLinkStart();
        }
        virtual CSlaveActivity *queryFromActivity() override { return activity; }
        virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override { activity->getMetaInfo(info); }
        virtual unsigned __int64 queryTotalCycles() const override
        {
            SpinBlock b(processActiveLock);
            if (!inputStream)
                return 0;
            return activity->queryTotalCycles();
        }
        virtual unsigned __int64 queryEndCycles() const { return activity->queryEndCycles(); }
        virtual void dataLinkSerialize(MemoryBuffer &mb) const { activity->dataLinkSerialize(mb); }
        virtual void dataLinkStart() { activity->dataLinkStart(); }
        virtual void dataLinkStop() { activity->dataLinkStop(); }
        virtual bool isGrouped() const { return activity->isGrouped(); }
        virtual IOutputMetaData * queryOutputMeta() const { return activity->queryOutputMeta(); }
        virtual unsigned queryOutputIdx() const { return 0; }
        virtual bool isInputOrdered(bool consumerOrdered) const { return activity->isInputOrdered(consumerOrdered); }
        virtual IStrandJunction *getOutputStreams(CActivityBase &_activity, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered)
        {
            return activity->getOutputStreams(_activity, idx, streams, consumerOptions, consumerOrdered);
        }
        virtual void debugRequest(MemoryBuffer &mb) { activity->debugRequest(mb); }

    // Stepping methods
        virtual IInputSteppingMeta *querySteppingMeta() { return NULL; }
        virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return false; }

    // to support non-stranded activities
        virtual IEngineRowStream *querySingleOutput() { return &stream; }
        virtual IEngineRowStream *queryStream() { return &stream; }
        virtual void setSingleOutput(IEngineRowStream *stream) override { throwUnexpected(); }
    };

    IPointerArrayOf<CDelayedInput> delayInputsList;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    NSplitterSlaveActivity(CGraphElementBase *container) : CSlaveActivity(container), writer(*this)
    {
        spill = false;
        nstopped = 0;
        eofHit = inputsConfigured = writeBlocked = pagedOut = false;
        recsReady = 0;
    }
    virtual ~NSplitterSlaveActivity()
    {
        delayInputsList.kill();
    }
    void ensureInputsConfigured()
    {
        CriticalBlock block(startLock);
        if (inputsConfigured)
            return;
        inputsConfigured = true;
        unsigned noutputs = container.connectedOutputs.getCount();
        ActPrintLog("Number of connected outputs: %d", noutputs);
        if (1 == noutputs)
        {
            CIOConnection *io = NULL;
            ForEachItemIn(o, container.connectedOutputs)
            {
                io = container.connectedOutputs.item(o);
                if (io)
                    break;
            }
            assertex(io);
            ForEachItemIn(o2, delayInputsList)
            {
                CDelayedInput *delayedInput = delayInputsList.item(o2);
                if (o2 == o)
                    delayedInput->setInput(new CInputWrapper(*this, inputStream));
                else
                    delayedInput->setInput(new CNullInput());
            }
        }
        else
        {
            ForEachItemIn(o, delayInputsList)
            {
                CDelayedInput *delayedInput = delayInputsList.item(o);
                if (NULL != container.connectedOutputs.queryItem(o))
                    delayedInput->setInput(new CSplitterOutput(*this, o));
                else
                    delayedInput->setInput(new CNullInput());
            }
        }
    }
    void reset()
    {
        CSlaveActivity::reset();
        nstopped = 0;
        grouped = false;
        eofHit = false;
        inputPrepared = false;
        recsReady = 0;
        writeBlocked = false;
        stalledWriters.kill();
        if (inputsConfigured)
        {
            // ensure old inputs cleared, to avoid being reused before re-setup on subsequent executions
            ForEachItemIn(o, delayInputsList)
            {
                CDelayedInput *delayedInput = delayInputsList.item(o);
                delayedInput->setInput(NULL);
            }
            inputsConfigured = false;
        }
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        ForEachItemIn(o, container.outputs)
        {
            Owned<CDelayedInput> delayedInput = new CDelayedInput(*this);
            delayInputsList.append(delayedInput.getLink());
            appendOutput(delayedInput.getClear());
        }
        IHThorSplitArg *helper = (IHThorSplitArg *)queryHelper();
        int dV = getOptInt(THOROPT_SPLITTER_SPILL, -1);
        if (-1 == dV)
            spill = !helper->isBalanced();
        else
            spill = dV>0;
    }
    void prepareInput(unsigned output)
    {
        CriticalBlock block(startLock);
        if (!inputPrepared)
        {
            inputPrepared = true;
            try
            {
                startInput(input);
                grouped = input->isGrouped();
                nstopped = container.connectedOutputs.getCount();
                if (smartBuf)
                    smartBuf->reset();
                else
                {
                    if (spill)
                    {
                        StringBuffer tempname;
                        GetTempName(tempname,"nsplit",true); // use alt temp dir
                        smartBuf.setown(createSharedSmartDiskBuffer(this, tempname.str(), outputs.ordinality(), queryRowInterfaces(input), &container.queryJob().queryIDiskUsage()));
                        ActPrintLog("Using temp spill file: %s", tempname.str());
                    }
                    else
                    {
                        ActPrintLog("Spill is 'balanced'");
                        smartBuf.setown(createSharedSmartMemBuffer(this, outputs.ordinality(), queryRowInterfaces(input), NSPLITTER_SPILL_BUFFER_SIZE));
                    }
                    // mark any unconnected outputs of smartBuf as already stopped.
                    ForEachItemIn(o, outputs)
                    {
                        IThorDataLink *delayedInput = outputs.item(o);
                        if (NULL == container.connectedOutputs.queryItem(o))
                            smartBuf->queryOutput(o)->stop();
                    }
                }
                if (!spill)
                    writer.start(); // writer keeps writing ahead as much as possible, the readahead impl. will block when has too much
            }
            catch (IException *e)
            {
                startException.setown(e); 
            }
        }
    }
    inline const void *nextRow(unsigned output)
    {
        OwnedConstThorRow row = smartBuf->queryOutput(output)->nextRow(); // will block until available
        if (writeAheadException)
            throw LINK(writeAheadException);
        return row.getClear();
    }
    rowcount_t writeahead(rowcount_t current, const bool &stopped, Semaphore &writeBlockSem)
    {
        // NB: readers call writeahead, which will block others
        CriticalBlock b(writeAheadCrit);
        loop
        {
            if (eofHit)
                return recsReady;
            if (current < recsReady)
                return recsReady;
            else if (writeBlocked) // NB: only used by 'balanced' splitter, which blocks write when too far ahead
            {
                stalledWriters.append(&writeBlockSem);
                CriticalUnblock ub(writeAheadCrit);
                writeBlockSem.wait(); // when active writer unblocks, signals all stalledWriters
                // recsReady or eofHit will have been updated by the blocking thread by now, loop and re-check
            }
            else
                break;
        }
        ActivityTimer t(totalCycles, queryTimeActivities());
        pagedOut = false;
        OwnedConstThorRow row;
        loop
        {
            if (abortSoon || stopped || pagedOut)
                break;
            try
            {
                row.setown(inputStream->nextRow());
                if (!row)
                {
                    row.setown(inputStream->nextRow());
                    if (row)
                    {
                        smartBuf->putRow(NULL, this); // may call blocked() (see ISharedSmartBufferCallback impl. below)
                        ++recsReady;
                    }
                }
            }
            catch (IException *e) { writeAheadException.setown(e); }
            if (!row || writeAheadException.get())
            {
                ActPrintLog("Splitter activity, hit end of input @ rec = %" RCPF "d", recsReady);
                eofHit = true;
                smartBuf->flush(); // signals no more rows will be written.
                break;
            }
            smartBuf->putRow(row.getClear(), this); // can block if mem limited, but other readers can progress which is the point
            ++recsReady;
        }
        return recsReady;
    }
    void inputStopped()
    {
        if (nstopped && --nstopped==0) 
        {
            writer.stop();
            stopInput(inputStream);
            inputPrepared = false;
        }
    }
    void abort()
    {
        CSlaveActivity::abort();
        if (smartBuf)
            smartBuf->cancel();
    }
// ISharedSmartBufferCallback impl.
    virtual void paged() { pagedOut = true; }
    virtual void blocked()
    {
        writeBlocked = true; // Prevent other users getting beyond checking recsReady in writeahead()
        writeAheadCrit.leave();
    }
    virtual void unblocked()
    {
        writeAheadCrit.enter();
        writeBlocked = false;
        if (stalledWriters.ordinality())
        {
            ForEachItemInRev(s, stalledWriters)
                stalledWriters.popGet()->signal();
        }
    }

// IThorDataLink (for output 0)
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) override
    {
        calcMetaInfoSize(info, inputs.item(0));
    }
    virtual unsigned __int64 queryTotalCycles() const override
    {
        unsigned __int64 _totalCycles = totalCycles.totalCycles; // more() time
        ForEachItemIn(o, outputs)
        {
            IThorDataLink *delayedInput = outputs.item(o);
            _totalCycles += delayedInput->queryTotalCycles();
        }
        return _totalCycles;
    }

friend class CInputWrapper;
friend class CSplitterOutput;
friend class CWriter;
};

//
// CSplitterOutput
//
CSplitterOutput::CSplitterOutput(NSplitterSlaveActivity &_activity, unsigned _output)
   : activity(_activity), output(_output)
{
    rec = max = 0;
}

// IThorDataLink
void CSplitterOutput::start()
{
    ActivityTimer s(totalCycles, activity.queryTimeActivities());
    rec = max = 0;
    activity.prepareInput(output);
    if (activity.startException)
        throw LINK(activity.startException);
}

void CSplitterOutput::stop()
{ 
    CriticalBlock block(activity.startLock);
    activity.smartBuf->queryOutput(output)->stop();
    activity.inputStopped();
}

const void *CSplitterOutput::nextRow()
{
    if (rec == max)
        max = activity.writeahead(max, activity.queryAbortSoon(), writeBlockSem);
    ActivityTimer t(totalCycles, activity.queryTimeActivities());
    const void *row = activity.nextRow(output); // pass ptr to max if need more
    ++rec;
    return row;
}

CActivityBase *createNSplitterSlave(CGraphElementBase *container)
{
    return new NSplitterSlaveActivity(container);
}

