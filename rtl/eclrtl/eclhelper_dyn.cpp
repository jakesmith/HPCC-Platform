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

#include "platform.h"
#include "jptree.hpp"
#include "eclrtl.hpp"
#include "eclhelper.hpp"
#include "rtlds_imp.hpp"
#include "eclhelper_base.hpp"
#include "eclhelper_dyn.hpp"

#include "rtlfield.hpp"
#include "rtlrecord.hpp"
#include "rtldynfield.hpp"
#include "rtlkey.hpp"

//---------------------------------------------------------------------------




const RtlStringTypeInfo ty2(0x4,5);
const RtlFieldStrInfo rf1("f1",NULL,&ty2);
const RtlFieldStrInfo rf2("f2",NULL,&ty2);
const RtlFieldInfo * const tl3[] = { &rf1,&rf2, 0 };
const RtlRecordTypeInfo ty1(0xd,10,tl3);
const RtlStringTypeInfo ty5(0x404,0);
const RtlFieldStrInfo rf3("f2",NULL,&ty5);
const RtlFieldInfo * const tl6[] = { &rf3, 0 };
const RtlRecordTypeInfo ty4(0x40d,4,tl6);

struct mi2 : public CFixedOutputMetaData {
    inline mi2() : CFixedOutputMetaData(10) {}
    virtual const RtlTypeInfo * queryTypeInfo() const override { return &ty1; }
} mx2;


struct dmi1 : public COutputRowDeserializer {
    inline dmi1(unsigned _activityId) : COutputRowDeserializer(_activityId) {}
    virtual size32_t deserialize(ARowBuilder & crSelf, IRowDeserializerSource & in) override {
        UNIMPLEMENTED;
    }
};

extern IOutputRowDeserializer * crdmi1(ICodeContext * ctx, unsigned activityId) {
    dmi1* p = new dmi1(activityId);
    p->onCreate(ctx);
    return p;
}

struct pmi1 : public CSourceRowPrefetcher {
    inline pmi1(unsigned _activityId) : CSourceRowPrefetcher(_activityId) {}
    virtual void readAhead(IRowDeserializerSource & in) override {
        // make sure there is enough data in input stream
        UNIMPLEMENTED;
    }
};

struct mi1 : public CVariableOutputMetaData {
    inline mi1() : CVariableOutputMetaData(4) {}
    virtual const RtlTypeInfo * queryTypeInfo() const override { return &ty4; }
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) override {
        return crdmi1(ctx, activityId);
    }
    virtual CSourceRowPrefetcher * doCreateDiskPrefetcher(unsigned activityId) override {
        return new pmi1(activityId);
    }
    virtual unsigned getMetaFlags() override { return 50; }
} mx1;

class ECLRTL_API CDynamicDiskReadArg : public CThorDiskReadArg
{
public:
    CDynamicDiskReadArg(IPropertyTree &_xgmml) : xgmml(_xgmml)
    {
        translator.setown(createRecordTranslator(queryOutputMeta()->queryRecordAccessor(true), queryDiskRecordSize()->queryRecordAccessor(true)));
        if (xgmml.hasProp("att[@name=\"filter\"]"))
            flags |= TDRkeyed;
    }
    virtual bool needTransform() override
    {
        return true;
        //return translator->needsTranslate();
    }
    virtual unsigned getFlags() override
    {
        return flags;
    }
    virtual void createSegmentMonitors(IIndexReadContext *irc) override
    {
        irc->append(createSingleKeySegmentMonitor(false, 0, 5, "12345"));
        return;
        Owned<IPropertyTreeIterator> filters = xgmml.getElements("att[@name=\"filter\"]");
        if (filters->first())
        {
            flags |= TDRkeyed;
            ForEach(*filters)
            {
                const char *curFilter = filters->query().queryProp("@value");
                assertex(curFilter);
                irc->append(createSingleKeySegmentMonitor(false, 0, 5, "12345"));
            }
        }
    }

    virtual IOutputMetaData * queryOutputMeta() override
    {
        //UNIMPLEMENTED;
        return &mx1;
        return NULL;
    }
    virtual const char * getFileName() override
    {
        return xgmml.queryProp("att[@name=\"_fileName\"]/@value");
    }
    virtual IOutputMetaData * queryDiskRecordSize() override
    {
        //UNIMPLEMENTED;
        return &mx2;
        return NULL;
    }
    virtual unsigned getFormatCrc() override
    {
        return xgmml.getPropInt("att[@name=\"formatCrc\"]/@value", 0);  // engines should treat 0 as 'ignore'
    }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) override
    {
        return translator->translate(rowBuilder, (const byte *) src);
    }
private:
    IPropertyTree &xgmml;
    Owned<const IDynamicTransform> translator;
    unsigned flags = 0;
};

class ECLRTL_API CDynamicWorkUnitWriteArg : public CThorWorkUnitWriteArg
{
    virtual int getSequence() override { return 0; }
    virtual IOutputMetaData * queryOutputMeta() override { return &mx1; }
};

extern ECLRTL_API IHThorArg *createDiskReadArg(IPropertyTree &xgmml)
{
    return new CDynamicDiskReadArg(xgmml);
}

extern ECLRTL_API IHThorArg *createWorkunitWriteArg(IPropertyTree &xgmml)
{
    return new CDynamicWorkUnitWriteArg();
}
