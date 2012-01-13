/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include "jlib.hpp"
#include "thexception.hpp"
#include "thorfile.hpp"
#include "thactivitymaster.ipp"
#include "../diskread/thdiskread.ipp"
#include "thwuidread.ipp"

class CWorkUnitReadMaster : public CMasterActivity
{
    bool global;
    size32_t getResult(MemoryBuffer &msg)
    {
        MemoryBuffer resultBuffer;
        IHThorWorkunitReadArg *helper = (IHThorWorkunitReadArg *)queryHelper();
        size32_t lenData;
        void *tempData;
        const char *wuid = helper->queryWUID();
        if (wuid)
            queryCodeContext()->getExternalResultRaw(lenData, tempData, wuid, helper->queryName(), helper->querySequence(), helper->queryXmlTransformer(), helper->queryCsvTransformer());
        else
            queryCodeContext()->getResultRaw(lenData, tempData, helper->queryName(), helper->querySequence(), helper->queryXmlTransformer(), helper->queryCsvTransformer());
        resultBuffer.setBuffer(lenData, tempData, true);
        msg.append(lenData, tempData);
        return lenData;
    }
public:
    CWorkUnitReadMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        //global = !container.queryOwner().queryOwner() || container.queryOwner().isGlobal();
        global = true; // even if part of a child graph, it should change, e.g. if repeatedly executed in loop
    }

    virtual void handleSlaveMessage(CMessageBuffer &msg)
    {
        assertex(!global);
        msg.clear();
        size32_t len = getResult(msg);
        container.queryJob().queryJobComm().reply(msg);
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (global && 0 == slave)
        {
            size32_t pos = dst.length();
            dst.append((size32_t)0); // placeholder
            size32_t len = getResult(dst);
            dst.writeDirect(pos, sizeof(len), &len);
        }
    }
};

static bool getWorkunitResultFilename(CGraphElementBase &container, StringBuffer & diskFilename, const char * wuid, const char * stepname, int sequence)
{
    try
    {
        ICodeContextExt &codeContext = *QUERYINTERFACE(container.queryCodeContext(), ICodeContextExt);
        Owned<IConstWUResult> result;
        if (wuid)
            result.setown(codeContext.getExternalResult(wuid, stepname, sequence));
        else
            result.setown(codeContext.getResultForGet(stepname, sequence));
        if (!result)
            throw MakeThorException(TE_FailedToRetrieveWorkunitValue, "Failed to find value %s:%d in workunit %s", stepname?stepname:"(null)", sequence, wuid?wuid:"(null)");

        SCMStringBuffer tempFilename;
        result->getResultFilename(tempFilename);
        if (tempFilename.length() == 0)
            return false;

        diskFilename.append("~").append(tempFilename.str());
        return true;
    }
    catch (IException * e) 
    {
        StringBuffer text; 
        e->errorMessage(text); 
        e->Release();
        throw MakeThorException(TE_FailedToRetrieveWorkunitValue, "Failed to find value %s:%d in workunit %s [%s]", stepname?stepname:"(null)", sequence, wuid?wuid:"(null)", text.str());
    }
    return false;
}


CActivityBase *createWorkUnitActivityMaster(CMasterGraphElement *container)
{
    StringBuffer diskFilename;
    IHThorWorkunitReadArg *wuReadHelper = (IHThorWorkunitReadArg *)container->queryHelper();
    if (getWorkunitResultFilename(*container, diskFilename, wuReadHelper->queryWUID(), wuReadHelper->queryName(), wuReadHelper->querySequence()))
    {
        Owned<IHThorDiskReadArg> diskReadHelper = createWorkUnitReadArg(diskFilename, LINK(wuReadHelper));
        Owned<CActivityBase> retAct = createDiskReadActivityMaster(container, diskReadHelper);
        return retAct.getClear();
    }
    else
        return new CWorkUnitReadMaster(container);
}
