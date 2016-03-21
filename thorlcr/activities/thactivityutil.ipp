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
    virtual const void *nextRow() override \
    { \
        try \
        { \
            return nextRowNoCatch(); \
        } \
        CATCH_NEXTROWX_CATCH \
    } \
    inline const void *nextRowNoCatch() __attribute__((always_inline))

class CThorSingleOutput : public CSimpleInterfaceOf<IEngineRowStream>
{
    CSlaveActivity *owner;
    unsigned outputId = 0;
    unsigned outputIdx = 0;
    bool optStableInput = true; // is the input forced to ordered?
    bool optUnstableInput = false;  // is the input forced to unordered?
    bool optUnordered = false; // is the output specified as unordered?

public:
    CThorSingleOutput(CSlaveActivity *_owner) : owner(_owner) { owner->setSingleOutput(this); }

// IEngineRowStream
    virtual void resetEOF() override { throwUnexpected(); }
};

IThorDataLink *createRowStreamToDataLinkAdapter(IThorDataLink *base, IRowStream *in);

interface ISmartBufferNotify
{
    virtual bool startAsync() =0;                       // return true if need to start asynchronously
    virtual void onInputStarted(IException *e) =0;      // e==NULL if start suceeded, NB only called with exception if Async
    virtual void onInputFinished(rowcount_t count) =0;
};

void initMetaInfo(ThorDataLinkMetaInfo &info);
void calcMetaInfoSize(ThorDataLinkMetaInfo &info, IThorDataLink *link);
void calcMetaInfoSize(ThorDataLinkMetaInfo &info,IThorDataLink **link,unsigned ninputs);
void calcMetaInfoSize(ThorDataLinkMetaInfo &info, ThorDataLinkMetaInfo *infos, unsigned num);

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
