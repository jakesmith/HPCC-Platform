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

#include <vector>

#include "jliball.hpp"
#include "jflz.hpp"
#include "jsecrets.hpp"
#include "seclib.hpp"
#include "ws_dfs.hpp"

#include "eclwatch_errorlist.hpp" // only for ECLWATCH_FILE_NOT_EXIST
#include "soapmessage.hpp"

#include "dautils.hpp"

#include "wsdfs.hpp"

namespace wsdfs
{

class CDFSFile : public CSimpleInterfaceOf<IDFSFile>
{
    Linked<IPropertyTree> fileMeta;
    unsigned __int64 lockId;
    unsigned keepAlivePeriod;

public:
    CDFSFile(IPropertyTree *_fileMeta, unsigned __int64 _lockId, unsigned _keepAlivePeriod) : fileMeta(_fileMeta), lockId(_lockId), keepAlivePeriod(_keepAlivePeriod)
    {
    }
    virtual const IPropertyTree *getFileMeta() const override
    {
        return fileMeta.getLink();
    }
    virtual unsigned __int64 getLocakId() const override
    {
        return lockId;
    }
};

IDFSFile *lookupDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, const char *user, const char *token)
{
    // NB: logicalName can have an option postfix @cluster
    const char *serviceUrl = "http://localhost:8010";
    Owned<IClientWsDfs> dfsClient = createWsDfsClient();
    dfsClient->addServiceUrl(serviceUrl);
    dfsClient->setUsernameToken(user, token, "");

    Owned<IClientDFSFileLookupResponse> dfsResp;

    CTimeMon tm(timeoutSecs);
    while (true)
    {
        try
        {
            Owned<IClientDFSFileLookupRequest> dfsReq = dfsClient->createDFSFileLookupRequest();

            // could validate logicalName here
            // CDfsLogicalFileName lfn;
            // lfn.set(logicalName);

            dfsReq->setName(logicalName);
            dfsReq->setRequestTimeout(timeoutSecs);
            dfsReq->setKeepAliveExpiryFrequency(keepAliveExpiryFrequency);

            dfsResp.setown(dfsClient->DFSFileLookup(dfsReq));

            const IMultiException *excep = &dfsResp->getExceptions(); // NB: warning despite getXX name, this does not Link
            if (excep->ordinality() > 0)
                throw LINK((IMultiException *)excep); // NB - const IException.. not caught in general..

            const char *base64Resp = dfsResp->getMeta();
            MemoryBuffer compressedRespMb;
            JBASE64_Decode(base64Resp, compressedRespMb);
            MemoryBuffer decompressedRespMb;
            fastLZDecompressToBuffer(decompressedRespMb, compressedRespMb);
            Owned<IPropertyTree> resp = createPTree(decompressedRespMb);
            return new CDFSFile(resp->queryPropTree("FileMeta"), resp->getPropInt64("@LockId"), resp->getPropInt("@keepAlivePeriod"));
        }
        catch (IException *e)
        {
            /* NB: there should really be a different IException class and a specific error code
            * The server knows it's an unsupported method.
            */
            if (SOAP_SERVER_ERROR != e->errorCode())
                throw;
            e->Release();
        }

        if (tm.timedout())
            throw makeStringExceptionV(0, "DFSFileLookup timed out: file=%s, timeoutSecs=%u", logicalName, timeoutSecs);
        Sleep(5000); // sanity sleep
    }

    return 0; // placeholder
}

IDFSFile *lookupDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, IUserDescriptor *userDesc)
{
    StringBuffer user, password;
    return lookupDFSFile(logicalName, timeoutSecs, keepAliveExpiryFrequency, userDesc->getUserName(user), userDesc->getPassword(user));
}

} // namespace wsdfs

