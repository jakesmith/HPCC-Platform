/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#pragma warning (disable : 4786)

#include "jflz.hpp"
#include "jstring.hpp"

#include "daaudit.hpp"
#include "dautils.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "esp.hpp"
#include "exception_util.hpp"
#include "package.h"

#include "wsdfs.hpp"
#include "ws_dfsservice.hpp"

using namespace wsdfs;

static IPropertyTree *getLFNMetaData(const char *logicalName)
{
    /*
        A bit kludgy for now, getting file meta data (can be super), from Dali and then getting
        specific copy (cluster) if specified.
        Then return serialized property tree.

        At some point, it could get this info. from somewhere other than Dali
        + be more streamlined / fit-for-purpose.
    */
    Owned<IFileDescriptor> fileDesc = queryDistributedFileDirectory().getFileDescriptor(logicalName, nullptr);
    if (!fileDesc)
        throw makeStringExceptionV(ECLWATCH_FILE_NOT_EXIST, "File %s does not exist.", logicalName);
    StringBuffer cluster;
    CDfsLogicalFileName lfn;
    lfn.set(logicalName);
    if (lfn.getCluster(cluster).length())
    {
        StringArray clusters;
        clusters.append(cluster);
        fileDesc->setClusterOrder(clusters, true);
    }
    Owned<IPropertyTree> fileMetaInfo = createPTree();
    fileDesc->serializeTree(*fileMetaInfo);
    return fileMetaInfo.getClear();
}

void CWsDfsEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    DBGLOG("Initializing %s service [process = %s]", service, process);

    assertex(!m_daliDetached);

#if 0
    VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]", process);
    IPropertyTree *processTree = cfg->queryPropTree(xpath);
    if (!processTree)
        throw MakeStringException(-1, "config not found for process %s", process);

    xpath.clear().appendf("EspService[@name=\"%s\"]", service);
    IPropertyTree *serviceTree = processTree->queryPropTree(xpath);
    if (!serviceTree)
        throw MakeStringException(-1, "config not found for service %s", service);

    serviceTree->getProp("DefaultScope", defaultScope_);
    serviceTree->getProp("User", user_);
    serviceTree->getProp("Password", password_);
#endif
}

bool CWsDfsEx::onDFSFileLookup(IEspContext &context, IEspDFSFileLookupRequest &req, IEspDFSFileLookupResponse &resp)
{
    try
    {
        const char *logicalName = req.getName();

        StringBuffer userID;
        context.getUserID(userID);
        Owned<IUserDescriptor> userDesc;
        if (!userID.isEmpty())
        {
            userDesc.setown(createUserDescriptor());
            userDesc->set(userID.str(), context.queryPassword(), context.querySignature());
        }

        // LDAP scope check
        checkLogicalName(logicalName, userDesc, true, false, false, nullptr); // check for read permissions

        unsigned timeoutSecs = req.getRequestTimeout();

        // 1) establish lock 1st
        unsigned __int64 lockId = 1; // fake! Need to get real lock id from Dali

        // 2) get file meta data
        Owned<IPropertyTree> fileMetaInfo = getLFNMetaData(logicalName);

        Owned<IPropertyTree> responseTree = createPTree();
        responseTree->setPropTree("FileMeta", fileMetaInfo.getClear());
        responseTree->setPropInt64("@lockId", lockId);

        // 3) serialize to blob
        MemoryBuffer respMb, compressedRespMb;
        responseTree->serialize(respMb);
        fastLZCompressToBuffer(compressedRespMb, respMb.length(), respMb.bytes());
        StringBuffer respStr;
        JBASE64_Encode(compressedRespMb.bytes(), compressedRespMb.length(), respStr, false);
        resp.setMeta(respStr.str());

        // 4) update file access.
        //    Really this should be done at end (or at end as well), but this is same as existing DFS lookup.
        CDateTime dt;
        dt.setNow();
        queryDistributedFileDirectory().setFileAccessed(logicalName, dt);

        LOG(MCauditInfo,",FileAccess,EspProcess,READ,%s,%u,%s", logicalName, timeoutSecs, userID.str());
    }
    catch (IException *e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsDfsEx::onDFSKeepAlive(IEspContext &context, IEspDFSLockKeepAliveRequest &req, IEspDFSLockKeepAliveResponse &resp)
{
    return true;
}
