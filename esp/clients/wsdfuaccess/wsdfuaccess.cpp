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

//Jlib
#include "jliball.hpp"

#include "ws_dfu.hpp"

#include "wsdfuaccess.hpp"


bool WsDfuAccess_getSecurityInfo(StringBuffer &securityInfoResult, const char *jobId, const char *logicalName, const char *access, unsigned expirySecs, const char *user, const char *token)
{
    // JCSMORE via config
    const char *espHost = "localhost";
    unsigned espServicePort = 8010;
    const char *protocol = "http";
    const char *service = "WsDfu";

    VStringBuffer url("%s://%s:%u/%s", protocol, espHost, espServicePort, service);

    Owned<IClientWsDfu> dfuClient = createWsDfuClient();
    dfuClient->addServiceUrl(url.str());
    dfuClient->setUsernameToken(user, token, "");

    Owned<IClientDFUReadAccessRequest> dfuReq = dfuClient->createDFUReadAccessRequest();

    dfuReq->setName(logicalName);
    //dfuReq->setCluster(const char * val);
    unsigned expiryMins = expirySecs<=90?1:(expirySecs+30)/60;
    dfuReq->setExpiryMinutes(expiryMins);
    dfuReq->setAccessType(CSecAccessType_Read);
    dfuReq->setJobId(jobId);
    //dfuReq->setRefresh(bool val);
    //dfuReq->setReturnJsonTypeInfo(bool val);
    dfuReq->setReturnBinTypeInfo(true);

    Owned<IClientDFUReadAccessResponse> dfuResp = dfuClient->DFUReadAccess(dfuReq);

    const IMultiException* excep = &dfuResp->getExceptions(); // NB: warning despite getXX name, this does not Link
    if (excep->ordinality() > 0)
        throw LINK((IMultiException *)excep); // JCSMORE - const IException.. not caught in general..

    securityInfoResult.append(dfuResp->getMetaInfoBlob());
    return true;
}
