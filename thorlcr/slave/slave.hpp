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

#ifndef SLAVE_HPP
#define SLAVE_HPP

#ifdef _WIN32
 #ifdef ACTIVITYSLAVES_EXPORTS
  #define activityslaves_decl __declspec(dllexport)
 #else
  #define activityslaves_decl __declspec(dllimport)
 #endif
#else
 #define activityslaves_decl
#endif

#include "jio.hpp"
#include "jsocket.hpp"
#include "slavmain.hpp"
#include "thor.hpp"

#include "eclhelper.hpp"        // for IRecordSize
#include "thgraph.hpp"
#include "thorstep.hpp"
#include "roxiestream.hpp"


/* ---- To implement IThorDataLink you need ----
    virtual const void *nextRow() = 0;
    virtual void stop();
    virtual void start();
    virtual bool isGrouped();
    virtual bool getMetaInfo(ThorDataLinkMetaInfo &info);
*/


struct ThorDataLinkMetaInfo
{
    __int64     totalRowsMin;           // set to 0 if not known
    __int64     totalRowsMax;           // set to -1 if not known
    offset_t    spilled;                // amount "spilled" to disk (approx) (offset_t)-1 for not known

    bool        isSource;
    bool        isSequential;
    bool        canStall;
    bool        fastThrough;
    bool        buffersInput;
    bool        canBufferInput;
    bool        singleRowOutput;
    bool        canIncreaseNumRows;
    bool        canReduceNumRows;
    bool        unknownRowsOutput;      // cannot use input to deduce total
    offset_t    byteTotal;                  // total (uncompressed) byte count of all rows
};

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning( disable : 4275 )
#endif

#define MAX_SENSIBLE_STRANDS 1024 // Architecture dependent...
class CThorStrandOptions
{
    // Typically set from hints, common to many stranded activities
public:
    explicit CThorStrandOptions(CGraphElementBase &container)
    {
        //PARALLEL(1) can be used to explicitly disable parallel processing.
        numStrands = container.queryXGMML().getPropInt("att[@name='parallel']/@value", 0);
        if ((numStrands == NotFound) || (numStrands > MAX_SENSIBLE_STRANDS))
            numStrands = getAffinityCpus();
        if (0 == numStrands)
        	numStrands = container.getOptInt("forceNumStrands");
 		blockSize = container.getOptInt("strandBlockSize");
    }
public:
    unsigned numStrands = 0; // if 1 it forces single-stranded operations.  (Useful for testing.)
    unsigned blockSize = 0;
};


interface IStrandJunction;
class CSlaveActivity;
interface IThorDataLink : extends IInterface
{
    virtual void start() = 0; // prepares input
    virtual CSlaveActivity *queryFromActivity() = 0; // activity that has this as an output
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) = 0;
    virtual unsigned __int64 queryTotalCycles() const = 0;
    virtual unsigned __int64 queryEndCycles() const = 0;
    virtual void dataLinkSerialize(MemoryBuffer &mb) const = 0;
    virtual void dataLinkStart() = 0;
    virtual void dataLinkStop() = 0;
    virtual bool isGrouped() const { return false; }
    virtual IOutputMetaData * queryOutputMeta() const = 0;
    virtual unsigned queryOutputIdx() const = 0;
    virtual bool isInputOrdered(bool consumerOrdered) const = 0;
    virtual IStrandJunction *getOutputStreams(CActivityBase &activity, unsigned idx, PointerArrayOf<IEngineRowStream> &streams, const CThorStrandOptions * consumerOptions, bool consumerOrdered) = 0;  // Use StrandFlags values for flags
    virtual void setOutputStream(unsigned index, IEngineRowStream *stream) = 0;
    virtual void debugRequest(MemoryBuffer &mb) = 0;

// Stepping methods
    virtual IInputSteppingMeta *querySteppingMeta() { return NULL; }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return false; }

// to support non-stranded activities
    virtual IEngineRowStream *queryStream() = 0; // should be const really, but some IEngineRowStream members are not..
};

// helper interface. Used by maintainer of output links
interface IThorDataLinkExt : extends IThorDataLink
{
    virtual void setOutputIdx(unsigned idx) = 0;
};

#if 0
interface IThorDataLink : extends IEngineRowStream // legacy
{
    virtual void start() = 0;
    virtual IInputSteppingMeta *querySteppingMeta() { return NULL; }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return false; }
    virtual void resetEOF() { }

// information routines
    virtual void dataLinkSerialize(MemoryBuffer &mb)=0;
    virtual unsigned __int64 queryTotalCycles() const=0;
    virtual unsigned __int64 queryEndCycles() const=0;
    virtual void debugRequest(MemoryBuffer &mb) = 0;

    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) = 0;
    virtual bool isGrouped() { return false; }
};
#endif

interface IThorSlaveActivity
{
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) = 0;
    virtual void setInputStream(unsigned index, IThorDataLink *input, unsigned inputOutIdx, bool consumerOrdered) = 0;
    virtual void processDone(MemoryBuffer &mb) = 0;
    virtual void reset() = 0;
};
#ifdef _MSC_VER
#pragma warning (pop)
#endif


// utility redirects
extern activityslaves_decl IRowInterfaces * queryRowInterfaces(IThorDataLink *link);
extern activityslaves_decl IEngineRowAllocator * queryRowAllocator(IThorDataLink *link);
extern activityslaves_decl IOutputRowSerializer * queryRowSerializer(IThorDataLink *link);
extern activityslaves_decl IOutputRowDeserializer * queryRowDeserializer(IThorDataLink *link);
extern activityslaves_decl IOutputMetaData *queryRowMetaData(IThorDataLink *link);
extern activityslaves_decl unsigned queryActivityId(IThorDataLink *link);
extern activityslaves_decl ICodeContext *queryCodeContext(IThorDataLink *link);


extern activityslaves_decl void dummyProc();

#endif


