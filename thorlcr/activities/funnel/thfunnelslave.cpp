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
#include "jset.hpp"
#include "jqueue.tpp"
#include "commonext.hpp"

#include "thormisc.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "thalloc.hpp"
#include "eclrtl_imp.hpp"

#include "thfunnelslave.ipp"

class CParallelFunnel : public CSimpleInterface, implements IRowStream
{
    class CInputHandler : public CInterface, implements IThreaded
    {
        CThreadedPersistent threaded;
        CParallelFunnel &funnel;
        CriticalSection stopCrit;
        StringAttr idStr;
        unsigned inputIndex;
        rowcount_t readThisInput; // purely for tracing
        bool stopping;
    public:
        CInputHandler(CParallelFunnel &_funnel, unsigned _inputIndex)
            : threaded("CInputHandler", this), funnel(_funnel), inputIndex(_inputIndex)
        {
            readThisInput = 0;
            StringBuffer s(funnel.idStr);
            s.append('(').append(inputIndex).append(')');
            idStr.set(s.str());
            stopping = false;
        }
        ~CInputHandler()
        {
            // stop();      too late to call stop I think
        }
        void start()
        {
            // NB don't start in constructor
            threaded.start();
        }
        void stop()
        {
            CriticalBlock b(stopCrit);
            if (stopping) return;
            stopping = true;
        }
        void join()
        {
            threaded.join();
        }

// IThreaded impl.
        virtual void main()
        {
            bool started = false;
            IEngineRowStream *inputStream = nullptr;
            try
            {
                funnel.activity.startInput(inputIndex);
                started = true;
                inputStream = funnel.activity.queryInputStream(inputIndex);
                while (!stopping)
                {
                    OwnedConstThorRow row = inputStream->ungroupedNextRow();
                    if (!row) break;

                    {
                        CriticalBlock b(stopCrit);
                        if (stopping) break;
                    }
                    CriticalBlock b(funnel.crit); // will mean first 'push' could block on fullSem, others on this crit.
                    funnel.push(row.getClear());
                    ++readThisInput;
                }
            }
            catch (IException *e)
            {
                funnel.fireException(e);
                e->Release();
            }
            // Informing EOS before stopping, may allow upstream activities to continue, if input slow to stop
            funnel.informEos(inputIndex);
            if (!started)
                return;
            try
            {
                inputStream->stop();
            }
            catch (IException *e)
            {
                funnel.fireException(e);
                e->Release();
            }
            ActPrintLog(&funnel.activity.queryContainer(), "%s: Read %" I64F "d records", idStr.get(), readThisInput);
        }
    };

    CSlaveActivity &activity;
    CIArrayOf<CInputHandler> inputHandlers;
    bool startInputs;
    Linked<IException> exception;
    unsigned eoss;
    StringAttr idStr;

    CriticalSection fullCrit, crit;
    SimpleInterThreadQueueOf<const void, true> rows;
    Semaphore fullSem;
    size32_t totSize;
    bool full, stopped;
    Linked<IOutputRowSerializer> serializer;

    void push(const void *row)
    {   
        CriticalBlock b2(fullCrit); // exclusivity for totSize / full
        if (stopped) return;
        rows.enqueue(row);
        totSize += thorRowMemoryFootprint(serializer, row);
        while (totSize > FUNNEL_MIN_BUFF_SIZE)
        {
            full = true;
            CriticalUnblock b(fullCrit);
            fullSem.wait(); // block pushers on crit
        }
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CParallelFunnel(CSlaveActivity &_activity) : activity(_activity)
    {
        init();
    }
    ~CParallelFunnel()
    {
        loop
        {
            OwnedConstThorRow row = rows.dequeueNow();
            if (!row) break;
        }
    }
    void init()
    {
        idStr.set(activityKindStr(activity.queryContainer().getKind()));

        stopped = full = false;
        totSize = 0;
        eoss = 0;
        serializer.set(activity.queryRowSerializer());
        for (unsigned i=0; i<activity.queryNumInputs(); i++)
            inputHandlers.append(* new CInputHandler(*this, i));
        // because of the way eos reported make sure started afterwards
        ForEachItemIn(j, inputHandlers)
            inputHandlers.item(j).start();
    }
    bool fireException(IException *e)
    {
        CriticalBlock b(crit);
        if (!exception.get())
        {
            exception.set(e);

            // nextRow() can be blocked on rows.dequeue(), abort needs to abort the SimpleInterThreadQueueOf
            rows.stop();
        }
        return true;
    }
    void informEos(unsigned input)
    {
        CriticalBlock b(crit);
        eoss++;
        if (eoss == inputHandlers.ordinality())
            rows.enqueue(NULL);
    }
    void abort()
    {
        // no action needed here, funnels ends as result of inputs ending prematurely (as before)
    }

// IRowStream impl.
    virtual void stop()
    {
        ForEachItemIn(h, inputHandlers)
        {
            CInputHandler &handler = inputHandlers.item(h);
            handler.stop();
        }
        {
            CriticalBlock b(fullCrit);
            if (full)
            {
                stopped = true; // ensure pending push()'s don't enqueue and if big row potentially block again.
                loop
                {
                    OwnedConstThorRow row = rows.dequeueNow();
                    if (!row) break;
                }
                rows.stop(); // I don't think really needed
                totSize = 0;
                fullSem.signal();
            }
        }
        ForEachItemIn(h2, inputHandlers)
        {
            CInputHandler &handler = inputHandlers.item(h2);
            handler.join();
        }
        if (exception)
            throw exception.getClear();
    }
    virtual const void *nextRow()
    {
        if (exception)
            throw exception.getClear();
        OwnedConstThorRow row = rows.dequeue();
        if (!row) {
            rows.stop();
            return NULL;
        }
        size32_t sz = thorRowMemoryFootprint(serializer, row.get());
        {
            CriticalBlock b(fullCrit);
            assertex(totSize>=sz);
            totSize -= sz;
            if (full)
            {
                full = false;
                fullSem.signal();
            }
        }
        return row.getClear();
    }

friend class CInputHandler;
};


///////////////////
//
// FunnelSlaveActivity
//

//class CParallelFunnel;
//interface IBitSet;
class FunnelSlaveActivity : public CSlaveActivity, public CThorSingleOutput
{
    typedef CSlaveActivity PARENT;

    IRowStream *current;
    unsigned currentMarker;
    bool grouped, *eog, eogNext, parallel;
    rowcount_t readThisInput;
    unsigned stopped;
    Owned<IRowStream> parallelOutput;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    FunnelSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorSingleOutput(this)
    {
        grouped = false;
        eog = NULL;
        current = NULL;
        currentMarker = 0;
        eogNext = false;
        readThisInput = 0;
        stopped = true;
        parallel = false;
    }
    ~FunnelSlaveActivity()
    {
        if (eog) delete [] eog;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        IHThorFunnelArg *helper = (IHThorFunnelArg *)queryHelper();
        parallel = !container.queryGrouped() && !helper->isOrdered() && getOptBool(THOROPT_PARALLEL_FUNNEL, true);
        grouped = container.queryGrouped();
        appendOutputLinked(this);
        ActPrintLog("FUNNEL mode = %s, grouped=%s", parallel?"PARALLEL":"ORDERED", grouped?"GROUPED":"UNGROUPED");
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        if (!grouped && parallel)
        {
            //NB starts inputs on each thread
            parallelOutput.setown(new CParallelFunnel(*this));
        }
        else
        {
            eogNext = false;
            stopped = 0;
            if (grouped)
            {
                if (eog)
                    delete [] eog;
                eog = new bool[inputs.ordinality()];
                unsigned i;
                for (i=0; i<inputs.ordinality(); i++) eog[i] = false;
            }
            current = NULL;
            currentMarker = 0;
            readThisInput = 0;
            ForEachItemIn(i, inputs)
            {
                try { startInput(i); }
                catch (CATCHALL)
                {
                    ActPrintLog("FUNNEL(%" ACTPF "d): Error staring input %d", container.queryId(), i);
                    throw;
                }
                if (!current)
                    current = inputStreams.item(i);
            }
        }
        dataLinkStart();
    }
    virtual void stop()
    {
        if (parallelOutput)
        {
            parallelOutput->stop();
            parallelOutput.clear();
        }
        else
        {
            current = NULL;
            unsigned i = stopped;
            for (;i<inputs.ordinality(); i++)
                stopInput(inputs.item(i));
            stopped = 0;
        }
        dataLinkStop();
    }
    const void * groupedNext()
    {
        if (eogNext)
        {
            eogNext = false;
            return NULL;
        }
        OwnedConstThorRow row = current->nextRow();
        if (!row)
        {
            if (!eog[currentMarker] && readThisInput)
            {
                eog[currentMarker] = true;
                return NULL;
            }
            eog[currentMarker] = true;
            loop
            {
                ActPrintLog("FUNNEL: Read %" RCPF "d records from input %d", readThisInput, currentMarker);
                if (currentMarker + 1 < inputs.ordinality())
                {
                    readThisInput = 0;
                    currentMarker++;
                    ActPrintLog("FUNNEL: changing to input %d", currentMarker);
                    ++stopped;
                    stopInput(current);
                    current = inputStreams.item(currentMarker);
                    // if empty stream, move on (ensuring eog,eog not returned by empty streams)
                    row.setown(current->nextRow());
                    if (row)
                        break;
                }
                else
                {
                    ActPrintLog("FUNNEL: no more inputs");
                    current = NULL;
                    return NULL;
                }
            }
        }
        else
            eog[currentMarker] = false;
        readThisInput++;
        dataLinkIncrement();
        return row.getClear();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (parallelOutput)
        {
            OwnedConstThorRow row = parallelOutput->nextRow();
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        else if (current)
        {
            if (grouped)
                return groupedNext();

            OwnedConstThorRow row = current->ungroupedNextRow();
            if (row)
            {
                readThisInput++;
                dataLinkIncrement();
                return row.getClear();
            }
            loop
            {
                ActPrintLog("FUNNEL: Read %" RCPF "d records from input %d", readThisInput, currentMarker);
                if (currentMarker + 1 < inputs.ordinality())
                {
                    readThisInput = 0;
                    currentMarker++;
                    ActPrintLog("FUNNEL: changing to input %d", currentMarker);
                    ++stopped;
                    stopInput(current);
                    current = inputStreams.item(currentMarker);
                    row.setown(current->ungroupedNextRow());
                    if (row)
                    {
                        readThisInput++;
                        dataLinkIncrement();
                        return row.getClear();
                    }
                }
                else
                {
                    ActPrintLog("FUNNEL: no more inputs");
                    current = NULL;
                    break;
                }
            }
        }
        return NULL;
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info, inputs.getArray(), inputs.ordinality());
    }
    virtual bool isGrouped() { return grouped; }
};

/////
///////////////////
//
// CombineSlaveActivity
//

class CombineSlaveActivity : public CSlaveActivity, public CThorSingleOutput
{
    IHThorCombineArg *helper;
    bool grouped;
    bool eogNext;
    MemoryBuffer recbuf;
    CThorExpandingRowArray rows;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);


    CombineSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorSingleOutput(this), rows(*this, this)
    {
        grouped = container.queryGrouped();
    }
    void init()
    {
        helper = (IHThorCombineArg *) queryHelper();
        appendOutputLinked(this);
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        init();
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        eogNext = false;
        ForEachItemIn(i, inputs)
        {
            try { startInput(i); }
            catch (CATCHALL)
            {
                ActPrintLog("COMBINE(%" ACTPF "d): Error staring input %d", container.queryId(), i);
                throw;
            }
        }
        dataLinkStart();
    }
    virtual void stop()
    {
        for (unsigned i=0;i<inputs.ordinality(); i++)
            stopInput(inputs.item(i));
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        loop
        {
            bool eog = false;
            bool err = false;
            unsigned i;
            unsigned n = inputs.ordinality();
            for (i=0;i<n;i++)
            {
                OwnedConstThorRow row = inputStreams.item(i)->nextRow();
                if (row)
                {
                    if (eog)
                    {
                        err = true;
                        break;
                    }
                    rows.append(row.getClear());
                }
                else
                {
                    if (i&&!eog)
                    {
                        err = true;
                        break;
                    }
                    eog = true;
                }
            }
            if (err)
            {
                eog = true;
                rows.kill();
                throw MakeActivityException(this, -1, "mismatched input row count for Combine");
            }
            if (eog) 
                break;
            RtlDynamicRowBuilder row(queryRowAllocator());
            size32_t sizeGot = helper->transform(row, rows.ordinality(), rows.getRowArray());
            rows.kill();
            if (sizeGot)
            {
                dataLinkIncrement();
                return row.finalizeRowClear(sizeGot);
            }
        }
        rows.kill();
        return NULL;
    }
    virtual bool isGrouped()
    {
        return inputs.item(0)->isGrouped();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        // TBD I think this should say max out = lhs set.
    }
};


/////


class RegroupSlaveActivity : public CSlaveActivity, public CThorSingleOutput
{
    IHThorRegroupArg *helper;
    bool grouped;
    bool eogNext;
    unsigned curinput;
public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    RegroupSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorSingleOutput(this)
    {
        grouped = container.queryGrouped();
    }
    void init()
    {
        helper = (IHThorRegroupArg *) queryHelper();
        appendOutputLinked(this);
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        init();
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        curinput = 0;
        eogNext = false;
        ForEachItemIn(i, inputs)
        {
            try { startInput(i); }
            catch (CATCHALL)
            {
                ActPrintLog("REGROUP(%" ACTPF "d): Error staring input %d", container.queryId(), i);
                throw;
            }
        }
        dataLinkStart();
    }
    virtual void stop()
    {
        for (unsigned i=0;i<inputs.ordinality(); i++)
            stopInput(inputs.item(i));
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        unsigned n = inputs.ordinality();
        IRowStream *current = inputStreams.item(curinput);
        loop
        {
            OwnedConstThorRow row = current->nextRow();
            if (row)
            {
                dataLinkIncrement();
                return row.getClear();
            }
            curinput++;
            if (curinput==n)
            {
                curinput = 0;
                break;
            }
            current = inputStreams.item(curinput);
        }
        return NULL;
    }

    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info, inputs.getArray(), inputs.ordinality());
    }
    virtual bool isGrouped() { return true; }
};

/////

class NonEmptySlaveActivity : public CSlaveActivity, public CThorSingleOutput
{
    IHThorNonEmptyArg *helper;
    bool eogNext, eoi, anyThisGroup, anyThisInput;
    unsigned curinput;
    mptag_t masterMpTag;
    bool sendReceiving;

    bool advance()
    {
        curinput++;
        if (curinput==inputs.ordinality())
        {
            eoi = true;
            return false;
        }
        else
        {
            if (container.queryLocalOrGrouped())
            {
                if (anyThisInput)
                    return false;
            }
            else
            {
                CMessageBuffer msg;
                msg.append(anyThisInput);
                {
                    BooleanOnOff onOff(sendReceiving);
                    if (!queryJobChannel().queryJobComm().sendRecv(msg, 0, masterMpTag, LONGTIMEOUT))
                        return false;
                }
                bool othersRead;
                msg.read(othersRead);
                if (anyThisInput || othersRead)
                    return false;
            }
            anyThisInput = false;
        }
        return true;
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    NonEmptySlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorSingleOutput(this)
    {
        helper = (IHThorNonEmptyArg *) queryHelper();
        sendReceiving = false;
        masterMpTag = TAG_NULL;
    }

// IThorSlaveActivity overloaded methods
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (!container.queryLocalOrGrouped())
            masterMpTag = container.queryJobChannel().deserializeMPTag(data);
        appendOutputLinked(this);
    }
    void abort()
    {
        CSlaveActivity::abort();
        eoi = true;
        if (sendReceiving)
            queryJobChannel().queryJobComm().cancel(0, masterMpTag);
    }

// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        curinput = 0;
        anyThisGroup = anyThisInput = eogNext = false;
        ForEachItemIn(i, inputs)
        {
            try { startInput(i); }
            catch (CATCHALL)
            {
                ActPrintLog("NONEMPTY(%" ACTPF "d): Error staring input %d", container.queryId(), i);
                throw;
            }
        }
        eoi = 0 == inputs.ordinality();
        dataLinkStart();
    }
    virtual void stop()
    {
        for (unsigned i=0;i<inputs.ordinality(); i++)
            stopInput(inputs.item(i));
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eoi) 
            return NULL; 
        if (eogNext) { 
            eogNext = false; 
            return NULL; 
        }
        loop
        {
            OwnedConstThorRow row = inputStreams.item(curinput)->nextRow();
            if (row ) {
                anyThisGroup = true;
                anyThisInput = true;
                dataLinkIncrement();
                return row.getClear();
            }
            if (anyThisGroup && container.queryGrouped())
            {
                anyThisGroup = false;
                break;
            }
            if (!advance())
            {
                eoi = true;
                break;
            }
        }
        return NULL;
    }
    virtual bool isGrouped() { return container.queryGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        calcMetaInfoSize(info, inputs.getArray(), inputs.ordinality());
    }
};


class CNWaySelectActivity : public CSlaveActivity, public CThorSingleOutput, public CThorSteppable
{
    typedef CSlaveActivity PARENT;

    IHThorNWaySelectArg *helper;
    IThorDataLink *selectedInputITDL = nullptr;
    IEngineRowStream *selectedStream = nullptr;
    Owned<IStrandJunction> selectedJunction;
public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CNWaySelectActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorSingleOutput(this), CThorSteppable(this)
    {
        helper = (IHThorNWaySelectArg *)queryHelper();
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);

        PARENT::start();

        unsigned whichInput = helper->getInputIndex();
        selectedInputITDL = NULL;
        selectedStream = NULL;
        if (whichInput--)
        {
            ForEachItemIn(i, inputs)
            {
                IThorDataLink *cur = inputs.item(i);
                IThorNWayInput *nWayInput = dynamic_cast<IThorNWayInput *>(cur);
                if (nWayInput)
                {
                    unsigned numRealInputs = nWayInput->numConcreteOutputs();
                    if (whichInput < numRealInputs)
                    {
                        selectedInputITDL = nWayInput->queryConcreteInput(whichInput);
                        selectedStream = connectSingleStream(*this, selectedInputITDL, 0, selectedJunction, true);  // Should this be passing whichInput??
                        break;
                    }
                    whichInput -= numRealInputs;
                }
            }
        }
        startJunction(selectedJunction);
        dataLinkStart();
    }
    virtual void stop()
    {
        stopInput(inputs.item(0));
        if (selectedStream)
            selectedStream->stop();
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!selectedStream)
            return NULL;
        OwnedConstThorRow ret = selectedStream->nextRow();
        if (ret)
            dataLinkIncrement();
        return ret.getClear();
    }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector &collector)
    { 
        if (!selectedStream)
            return false;
        return selectedInputITDL->gatherConjunctions(collector);
    }
    virtual void resetEOF()
    { 
        if (selectedStream)
            selectedStream->resetEOF();
    }
    virtual const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    virtual const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!selectedStream)
            return NULL;
        return selectedStream->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        if (selectedStream)
            calcMetaInfoSize(info, selectedInputITDL);
        else if (!started())
            info.canStall = true; // unkwown if !started
    }
    virtual bool isGrouped() { return selectedInputITDL ? selectedInputITDL->isGrouped() : false; }
// steppable
    virtual void setInputStream(unsigned index, IThorDataLink *input, unsigned inputOutIdx, bool consumerOrdered) override
    {
        CSlaveActivity::setInputStream(index, input, inputOutIdx, consumerOrdered);
        CThorSteppable::setInputStream(index, input, inputOutIdx, consumerOrdered);
    }
    virtual IInputSteppingMeta *querySteppingMeta()
    {
        if (selectedInputITDL)
            return selectedInputITDL->querySteppingMeta();
        return NULL;
    }
};


class CThorNWayInputSlaveActivity : public CSlaveActivity, public CThorSingleOutput, implements IThorNWayInput
{
    IHThorNWayInputArg *helper;
    PointerArrayOf<IThorDataLink> selectedInputs;
    bool grouped;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CThorNWayInputSlaveActivity(CGraphElementBase *container) : CSlaveActivity(container), CThorSingleOutput(this)
    {
        helper = (IHThorNWayInputArg *)queryHelper();
        grouped = helper->queryOutputMeta()->isGrouped(); // JCSMORE should match graph info, i.e. container.queryGrouped()
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        bool selectionIsAll;
        size32_t selectionLen;
        rtlDataAttr selection;
        helper->getInputSelection(selectionIsAll, selectionLen, selection.refdata());
        selectedInputs.kill();
        if (selectionIsAll)
        {
            ForEachItemIn(i, inputs)
                selectedInputs.append(inputs.item(i));
        }
        else
        {
            const size32_t * selections = (const size32_t *)selection.getdata();
            unsigned max = selectionLen/sizeof(size32_t);
            for (unsigned i = 0; i < max; i++)
            {
                unsigned nextIndex = selections[i];
                //Check there are no duplicates.....  Assumes there are a fairly small number of inputs, so n^2 search is ok.
                for (unsigned j=i+1; j < max; j++)
                {
                    if (nextIndex == selections[j])
                        throw MakeStringException(100, "Selection list for nway input can not contain duplicates");
                }
                if (!inputs.isItem(nextIndex-1))
                    throw MakeStringException(100, "Index %d in RANGE selection list is out of range", nextIndex);

                selectedInputs.append(inputs.item(nextIndex-1));
            }
        }
        // NB: Whatever pulls this IThorNWayInput, starts and stops the selectedInputs
        dataLinkStart();
    }
    virtual void stop()
    {
        // NB: Whatever pulls this IThorNWayInput, starts and stops the selectedInputs
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        throwUnexpected();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info,inputs.item(0));
    }
    virtual bool isGrouped() { return grouped; }
// IThorNWayInput impl.
    virtual unsigned numConcreteOutputs() const
    {
        return selectedInputs.ordinality();
    }
    virtual IThorDataLink *queryConcreteInput(unsigned idx) const
    {
        if (selectedInputs.isItem(idx))
            return selectedInputs.item(idx);
        return NULL;
    }
};



CActivityBase *createFunnelSlave(CGraphElementBase *container)
{
    return new FunnelSlaveActivity(container);
}

CActivityBase *createCombineSlave(CGraphElementBase *container)
{
    return new CombineSlaveActivity(container);
}

CActivityBase *createRegroupSlave(CGraphElementBase *container)
{
    return new RegroupSlaveActivity(container);
}

CActivityBase *createNonEmptySlave(CGraphElementBase *container)
{
    return new NonEmptySlaveActivity(container);
}

CActivityBase *createNWaySelectSlave(CGraphElementBase *container)
{
    return new CNWaySelectActivity(container);
}

CActivityBase *createNWayInputSlave(CGraphElementBase *container)
{
    return new CThorNWayInputSlaveActivity(container);
}
