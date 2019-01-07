/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "jliball.hpp"
#include "daclient.hpp"
#include "dautils.hpp"
#include "dafdesc.hpp"
#include "dafsclient.hpp"
#include "seclib.hpp"
#include "environment.hpp"
#include "ws_dfu.hpp"
#include "eclwatch_errorlist.hpp" // only for ECLWATCH_FILE_NOT_EXIST

#include "wsdfuaccess.hpp"
#include "ws_dfu_esp.ipp"


#include <vector>

using namespace dafsclient;

namespace wsdfuaccess
{


CSecAccessType translateToCSecAccessAccessType(SecAccessFlags from)
{
    switch (from)
    {
        case SecAccess_Access:
            return CSecAccessType_Access;
        case SecAccess_Read:
            return CSecAccessType_Read;
        case SecAccess_Write:
            return CSecAccessType_Write;
        case SecAccess_Full:
            return CSecAccessType_Full;
        case SecAccess_None:
        default:
            return CSecAccessType_None;
    }
}


IConstDFUFileAccessInfo *getFileAccess(const char *serviceUrl, const char *jobId, const char *logicalName, SecAccessFlags access, unsigned expirySecs, const char *user, const char *password)
{
    Owned<IClientWsDfu> dfuClient = createWsDfuClient();
    dfuClient->addServiceUrl(serviceUrl);
    dfuClient->setUsernameToken(user, password, "");

    Owned<IClientDFUFileAccessRequest> dfuReq = dfuClient->createDFUFileAccessRequest();
    IEspDFUFileAccessRequestBase &requestBase = dfuReq->updateRequestBase();

    CDfsLogicalFileName lfn;
    lfn.set(logicalName);

    StringBuffer cluster, lfnName;
    lfn.getCluster(cluster);
    lfn.get(lfnName); // remove cluster if present

    requestBase.setName(lfnName);
    requestBase.setCluster(cluster);
    requestBase.setExpirySeconds(expirySecs);
    requestBase.setAccessRole(CFileAccessRole_External);
    requestBase.setAccessType(translateToCSecAccessAccessType(access));
    requestBase.setJobId(jobId);

    Owned<IClientDFUFileAccessResponse> dfuResp = dfuClient->DFUFileAccess(dfuReq);

    const IMultiException *excep = &dfuResp->getExceptions(); // NB: warning despite getXX name, this does not Link
    if (excep->ordinality() > 0)
        throw LINK((IMultiException *)excep); // JCSMORE - const IException.. not caught in general..

    return LINK(&dfuResp->getAccessInfo()); // JCSMORE - getAccessInfo() itself does not link
}



static std::vector<std::string> dfuServiceUrls;
static CriticalSection dfuServiceUrlCrit;
static unsigned currentDfuServiceUrl = 0;

static unsigned getNumDfuServiceURL()
{
    return dfuServiceUrls.size();
}

// JCSMORE something like this should be a utility method in environment.*
void ensureAccessibleDfuServiceURLList()
{
    if (!daliClientActive())
        return;
    CriticalBlock b(dfuServiceUrlCrit);
    if (!dfuServiceUrls.size())
    {
        Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> daliEnv = factory->openEnvironment();
        Owned<IPropertyTree> env = &daliEnv->getPTree();
        if (env.get())
        {
            StringBuffer fileMetaServiceUrl;
            StringBuffer espInstanceComputerName;
            StringBuffer bindingProtocol;
            StringBuffer xpath;
            StringBuffer instanceAddress;
            StringBuffer espServiceType;

            Owned<IPropertyTreeIterator> espProcessIter = env->getElements("Software/EspProcess");
            ForEach(*espProcessIter)
            {
                Owned<IPropertyTreeIterator> espBindingIter = espProcessIter->query().getElements("EspBinding");
                ForEach(*espBindingIter)
                {
                    xpath.setf("Software/EspService[@name=\"%s\"]/Properties/@type",  espBindingIter->query().queryProp("@service"));

                    if (strisame(env->queryProp(xpath), "WsSMC"))
                    {
                        if (espBindingIter->query().getProp("@protocol", bindingProtocol.clear()))
                        {
                            Owned<IPropertyTreeIterator> espInstanceIter = espProcessIter->query().getElements("Instance");
                            ForEach(*espInstanceIter)
                            {
                                if (espInstanceIter->query().getProp("@computer", espInstanceComputerName.clear()))
                                {
                                    xpath.setf("Hardware/Computer[@name=\"%s\"]/@netAddress", espInstanceComputerName.str());
                                    if (env->getProp(xpath.str(), instanceAddress.clear()))
                                    {
                                        fileMetaServiceUrl.setf("%s://%s:%d/WsDfu/DFUFileAccess.xml", bindingProtocol.str(), instanceAddress.str(), espBindingIter->query().getPropInt("@port",8010));
                                        dfuServiceUrls.push_back(fileMetaServiceUrl.str());
                                    }
                                }
                            }
                        }
                    }
                }//ESPBinding
            }//ESPProcess
        }

        if (0 == dfuServiceUrls.size())
            throw MakeStringException(-1,"Could not find any WsSMC services in the target HPCC configuration.");
    }
}

const char *getAvailableDFUServiceURL()
{
    if (0 == dfuServiceUrls.size())
        return nullptr;
    if (currentDfuServiceUrl == dfuServiceUrls.size())
        currentDfuServiceUrl = 0;
    return dfuServiceUrls[currentDfuServiceUrl].c_str();
}


static IDFUFileAccess *doLookupDFUFile(const char *serviceUrl, const char *logicalName, const char *requestId, unsigned expirySecs, const char *user, const char *password)
{
    Owned<IClientWsDfu> dfuClient = createWsDfuClient();
    dfuClient->addServiceUrl(serviceUrl);
    dfuClient->setUsernameToken(user, password, "");

    Owned<IClientDFULookupFileRequest> dfuReq = dfuClient->createDFULookupFileRequest();

    CDfsLogicalFileName lfn;
    lfn.set(logicalName);

    StringBuffer cluster, lfnName;
    lfn.getCluster(cluster);
    lfn.get(lfnName); // remove cluster if present

    dfuReq->setName(lfnName);
    dfuReq->setCluster(cluster);
    dfuReq->setExpirySeconds(expirySecs);
    dfuReq->setRequestId(requestId);

    Owned<IClientDFULookupFileResponse> dfuResp = dfuClient->DFULookupFile(dfuReq);

    const IMultiException *excep = &dfuResp->getExceptions(); // NB: warning despite getXX name, this does not Link
    if (excep->ordinality() > 0)
        throw LINK((IMultiException *)excep); // JCSMORE - const IException.. not caught in general..

    return createDFUFileAccess(lfnName, dfuResp->getMetaInfoBlob());
}

static IDFUFileAccess *doCreateDFUFile(const char *serviceUrl, const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs, const char *user, const char *password)
{
    Owned<IClientWsDfu> dfuClient = createWsDfuClient();
    dfuClient->addServiceUrl(serviceUrl);
    dfuClient->setUsernameToken(user, password, "");

    Owned<IClientDFUCreateFileRequest> dfuReq = dfuClient->createDFUCreateFileRequest();

    dfuReq->setName(logicalName);
    dfuReq->setCluster(cluster);
    dfuReq->setExpirySeconds(expirySecs);
    dfuReq->setRequestId(requestId);
    dfuReq->setRecDef(recDef);
    CFileType serviceType;
    switch (type)
    {
        case dft_flat:
            serviceType = CFileType_Flat;
            break;
        case dft_index:
            serviceType = CFileType_Index;
            break;
    }
    dfuReq->setType(serviceType);

    Owned<IClientDFUCreateFileResponse> dfuResp = dfuClient->DFUCreateFile(dfuReq);

    const IMultiException *excep = &dfuResp->getExceptions(); // NB: warning despite getXX name, this does not Link
    if (excep->ordinality() > 0)
        throw LINK((IMultiException *)excep); // JCSMORE - const IException.. not caught in general..

    return createDFUFileAccess(dfuResp->getFileId(), dfuResp->getMetaInfoBlob());
}

static void doPublishDFUFile(const char *serviceUrl, IDFUFileAccess *dfuFile, bool overwrite, const char *user, const char *password)
{
    Owned<IClientWsDfu> dfuClient = createWsDfuClient();
    dfuClient->addServiceUrl(serviceUrl);
    dfuClient->setUsernameToken(user, password, "");

    Owned<IClientDFUPublishFileRequest> dfuReq = dfuClient->createDFUPublishFileRequest();

    dfuReq->setFileId(dfuFile->queryName());
    dfuReq->setOverwrite(overwrite);
    MemoryBuffer mb;
    dfuFile->queryEngineInterface()->queryFileDescriptor().serialize(mb);
    dfuReq->setFileDescriptorBlob(mb);

    Owned<IClientDFUPublishFileResponse> dfuResp = dfuClient->DFUPublishFile(dfuReq);

    const IMultiException *excep = &dfuResp->getExceptions(); // NB: warning despite getXX name, this does not Link
    if (excep->ordinality() > 0)
        throw LINK((IMultiException *)excep); // JCSMORE - const IException.. not caught in general..
}


// wrapper to the doLookupDFUFile, that discovers and tries DFUService URL's
IDFUFileAccess *lookupDFUFile(const char *logicalName, const char *requestId, unsigned expirySecs, const char *user, const char *password)
{
    ensureAccessibleDfuServiceURLList();
    unsigned c = getNumDfuServiceURL();

    const char *espServiceUrl = getAvailableDFUServiceURL();
    PROGLOG("WsSMC: Targeting ESP WsSMC URL: %s", espServiceUrl);

    while (c)
    {
        try
        {
            /* JCSMORE - where would locking fit in?
             * *IF* Esp established lock, then there'd be no association with this client (no state), and if Esp restarted lock would be lost,
             * if this client died, the lock would remain (on Esp).
             *
             * Idea:
             * 1) Esp establishes lock on behalf of this client.
             * 2) This client sends keep-alive packets every N seconds (To Esp).
             * 3) Esp ensures lock remains alive somehow (something (Esp?) could keep persistent [written] state of active locks?)
             * 4) If no keep-alive for a lock, Esp closes it.
             *
             * Would require the ability (in Dali) to create locks without session association.
             * As long as Dali is the lock manager, Would probably be best if the keep-alive packets were
             * forwarded to Dali, and it managed the live/stale locks.
             */

            return doLookupDFUFile(espServiceUrl, logicalName, requestId, expirySecs, user, password);
        }
        catch (IException *e)
        {
            if (ECLWATCH_FILE_NOT_EXIST == e->errorCode())
            {
                e->Release();
                return nullptr; // not found
            }
            EXCLOG(e, nullptr);
            e->Release();
        }
        --c;
    }
    StringBuffer msg("Failed to contact WsSMC service: { ");
    for (auto &url: dfuServiceUrls)
        msg.append(url.c_str());
    msg.append("}");
    throw makeStringException(0, msg.str());
}

IDFUFileAccess *lookupDFUFile(const char *logicalName, const char *requestId, unsigned expirySecs, IUserDescriptor *userDesc)
{
    assertex(userDesc);
    StringBuffer user, password;
    userDesc->getUserName(user);
    userDesc->getPassword(password);
    return lookupDFUFile(logicalName, requestId, expirySecs, user, password);
}


// wrapper to the doCreateDFUFile, that discovers and tries DFUService URL's
IDFUFileAccess *createDFUFile(const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs, const char *user, const char *password)
{
    ensureAccessibleDfuServiceURLList();
    unsigned c = getNumDfuServiceURL();

    const char *espServiceUrl = getAvailableDFUServiceURL();
    PROGLOG("WsSMC: Targeting ESP WsSMC URL: %s", espServiceUrl);

    while (c)
    {
        try
        {
            return doCreateDFUFile(espServiceUrl, logicalName, cluster, type, recDef, requestId, expirySecs, user, password);
        }
        catch (IException *e)
        {
            EXCLOG(e, nullptr);
            e->Release();
        }
        --c;
    }
    StringBuffer msg("Failed to contact WsSMC service: { ");
    for (auto &url: dfuServiceUrls)
        msg.append(url.c_str());
    msg.append("}");
    throw makeStringException(0, msg.str());
}


IDFUFileAccess *createDFUFile(const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs, IUserDescriptor *userDesc)
{
    assertex(userDesc);
    StringBuffer user, password;
    userDesc->getUserName(user);
    userDesc->getPassword(password);
    return createDFUFile(logicalName, cluster, type, recDef, requestId, expirySecs, user, password);
}

// wrapper to the doPublishDFUFile, that discovers and tries DFUService URL's
void publishDFUFile(IDFUFileAccess *dfuFile, bool overwrite, const char *user, const char *password)
{
    ensureAccessibleDfuServiceURLList();
    unsigned c = getNumDfuServiceURL();

    const char *espServiceUrl = getAvailableDFUServiceURL();
    PROGLOG("WsSMC: Targeting ESP WsSMC URL: %s", espServiceUrl);

    while (c)
    {
        try
        {
            doPublishDFUFile(espServiceUrl, dfuFile, overwrite, user, password);
            return;
        }
        catch (IException *e)
        {
            EXCLOG(e, nullptr);
            e->Release();
        }
        --c;
    }
    StringBuffer msg("Failed to contact WsSMC service: { ");
    for (auto &url: dfuServiceUrls)
        msg.append(url.c_str());
    msg.append("}");
    throw makeStringException(0, msg.str());
}

void publishDFUFile(IDFUFileAccess *dfuFile, bool overwrite, IUserDescriptor *userDesc)
{
    assertex(userDesc);
    StringBuffer user, password;
    userDesc->getUserName(user);
    userDesc->getPassword(password);
    publishDFUFile(dfuFile, overwrite, user, password);
}


} // namespace wsdfuaccess
