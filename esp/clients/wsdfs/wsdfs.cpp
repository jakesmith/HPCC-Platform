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

#include "dafdesc.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"

#include "wsdfs.hpp"

namespace wsdfs
{

class CKeepAliveThread : public CSimpleInterface, implements IThreaded
{
    CThreaded threaded;
    unsigned periodMs;
    Semaphore sem;
public:
    CKeepAliveThread(unsigned _periodSecs) : threaded("CKeepAliveThread", this), periodMs(_periodSecs * 1000)
    {
        threaded.start();
    }
    void stop()
    {
        sem.signal();
    }
    virtual void threadmain() override
    {
        while (true)
        {
            if (sem.wait(periodMs))
                return;
        }
    }
};

#if 0
class CCDistributedFilePart : public CSimpleInterfaceOf<IDistributedFilePart>
{
public:
    CCDistributedFilePart()
    {
    }
    virtual unsigned getPartIndex() override
    {   
    }
    virtual unsigned numCopies() override
    {
    }
    virtual INode *getNode(unsigned copy=0) override { throwUnexpected(); }
    virtual INode *queryNode(unsigned copy=0) override { throwUnexpected(); }
    virtual RemoteFilename &getFilename(RemoteFilename &ret, unsigned copy = 0) override
    {
    }
    virtual StringBuffer &getPartName(StringBuffer &name) override
    {
    }
    virtual StringBuffer &getPartDirectory(StringBuffer &name, unsigned copy = 0) override
    {
    }
    virtual IPropertyTree &queryAttributes() override
    {
    }
    virtual bool lockProperties(unsigned timeoutms=INFINITE) override
    {
    }
    virtual void unlockProperties(DFTransactionState state=TAS_NONE) override
    {
    }
    virtual bool isHost(unsigned copy=0) override
    {
    }
    virtual offset_t getFileSize(bool allowphysical,bool forcephysical) override
    {
    }
    virtual offset_t getDiskSize(bool allowphysical,bool forcephysical) override
    {
    }
    virtual bool getModifiedTime(bool allowphysical,bool forcephysical,CDateTime &dt) override
    {
    }
    virtual bool getCrc(unsigned &crc) override
    {
    }
    virtual unsigned getPhysicalCrc() override
    {
    }
    virtual unsigned bestCopyNum(const IpAddress &ip,unsigned rel=0) override
    {
    }
    virtual unsigned copyClusterNum(unsigned copy,unsigned *replicate=NULL) override
    {
    }
};
#endif


class CServiceDistributeFile : public CSimpleInterfaceOf<IDistributedFile>
{
    Linked<IDFSFile> file;
    Linked<IPropertyTree> meta;
    Owned<IFileDescriptor> fileDesc;
    Owned<IDistributedFile> dfsFile;
public:
    CServiceDistributeFile(IDFSFile *_file) : file(_file)
    {
        meta.set(file->getFileMeta());
        fileDesc.setown(deserializeFileDescriptorTree(meta));
        StringBuffer name;
        fileDesc->getTraceName(name);
        dfsFile.setown(queryDistributedFileDirectory().createNew(fileDesc, name));
    }
// IDistributeFile
    virtual unsigned numParts() override { return dfsFile->numParts(); }
    virtual IDistributedFilePart &queryPart(unsigned idx) override { return dfsFile->queryPart(idx); }
    virtual IDistributedFilePart* getPart(unsigned idx) override { return dfsFile->getPart(idx); }
    virtual StringBuffer &getLogicalName(StringBuffer &name) override { return dfsFile->getLogicalName(name); }
    virtual const char *queryLogicalName() override { return dfsFile->queryLogicalName(); }
    virtual IDistributedFilePartIterator *getIterator(IDFPartFilter *filter=NULL) override { return dfsFile->getIterator(filter); }
    virtual IFileDescriptor *getFileDescriptor(const char *clustername=NULL) override { return fileDesc.getLink(); }
    virtual const char *queryDefaultDir() override { return dfsFile->queryDefaultDir(); }
    virtual const char *queryPartMask() override { return dfsFile->queryPartMask(); }
    virtual IPropertyTree &queryAttributes() override { return dfsFile->queryAttributes(); }
    virtual bool lockProperties(unsigned timeoutms=INFINITE) override
    {
        // TODO: implement. But for now only foreign [read] files are supported, where updates and locking have never been implemented.
        return true;
    }
    virtual void unlockProperties(DFTransactionState state=TAS_NONE) override
    {
        // TODO: implement. But for now only foreign [read] files are supported, where updates and locking have never been implemented.
    }
    virtual bool getModificationTime(CDateTime &dt) override { return dfsFile->getModificationTime(dt); }
    virtual bool getAccessedTime(CDateTime &dt) override { return dfsFile->getAccessedTime(dt); }
    virtual unsigned numCopies(unsigned partno) override { return dfsFile->numCopies(partno); }
    virtual bool existsPhysicalPartFiles(unsigned short port) override
    {
        return dfsFile->existsPhysicalPartFiles(port);
    }
    virtual __int64 getFileSize(bool allowphysical, bool forcephysical) override
    {
        return dfsFile->getFileSize(allowphysical, forcephysical);
        // NB: ignoring allowphysical and forcephysical, because meta data must exist in this implementation.
        //__int64 ret = (__int64)(forcephysical?-1:queryAttributes().getPropInt64("@size", -1));
        //verifyex(ret != -1);
        //return ret;
    }
    virtual __int64 getDiskSize(bool allowphysical, bool forcephysical) override
    {
        return dfsFile->getDiskSize(allowphysical, forcephysical);
    }
    virtual bool getFileCheckSum(unsigned &checksum) override { return dfsFile->getFileCheckSum(checksum); }
    virtual unsigned getPositionPart(offset_t pos,offset_t &base) override { return dfsFile->getPositionPart(pos,base); }
    virtual IDistributedSuperFile *querySuperFile() override
    {
        return dfsFile->querySuperFile();
        UNIMPLEMENTED_X("CServiceDistributeFile::querySuperFile");
    }
    virtual IDistributedSuperFileIterator *getOwningSuperFiles(IDistributedFileTransaction *_transaction=NULL) override { return dfsFile->getOwningSuperFiles(_transaction); }
    virtual bool isCompressed(bool *blocked=NULL) override { return dfsFile->isCompressed(blocked); }
    virtual StringBuffer &getClusterName(unsigned clusternum,StringBuffer &name) override { return dfsFile->getClusterName(clusternum,name); }
    virtual unsigned getClusterNames(StringArray &clusters) override { return dfsFile->getClusterNames(clusters); }                                                                                      // (use findCluster)
    virtual unsigned numClusters() override { return dfsFile->numClusters(); }
    virtual unsigned findCluster(const char *clustername) override { return dfsFile->findCluster(clustername); }
    virtual ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum) override { return dfsFile->queryPartDiskMapping(clusternum); }
    virtual IGroup *queryClusterGroup(unsigned clusternum) override { return dfsFile->queryClusterGroup(clusternum); }
    virtual StringBuffer &getClusterGroupName(unsigned clusternum, StringBuffer &name) override
    {
        return fileDesc->getClusterGroupName(clusternum, name);
    }
    virtual StringBuffer &getECL(StringBuffer &buf) override { return dfsFile->getECL(buf); }

    virtual bool canModify(StringBuffer &reason) override
    {
        return false;
    }
    virtual bool canRemove(StringBuffer &reason,bool ignoresub=false) override
    {
        return false;
    }
    virtual bool checkClusterCompatible(IFileDescriptor &fdesc, StringBuffer &err) override { return dfsFile->checkClusterCompatible(fdesc,err); }

    virtual bool getFormatCrc(unsigned &crc) override { return dfsFile->getFormatCrc(crc); }
    virtual bool getRecordSize(size32_t &rsz) override { return dfsFile->getRecordSize(rsz); }
    virtual bool getRecordLayout(MemoryBuffer &layout, const char *attrname) override { return dfsFile->getRecordLayout(layout,attrname); }
    virtual StringBuffer &getColumnMapping(StringBuffer &mapping) override { return dfsFile->getColumnMapping(mapping); }

    virtual bool isRestrictedAccess() override { return dfsFile->isRestrictedAccess(); }
    virtual unsigned setDefaultTimeout(unsigned timems) override { return dfsFile->setDefaultTimeout(timems); }

    virtual void validate() override { dfsFile->validate(); }

    virtual IPropertyTree *queryHistory() const override { return dfsFile->queryHistory(); }
    virtual bool isExternal() const override { return false; }
    virtual bool getSkewInfo(unsigned &maxSkew, unsigned &minSkew, unsigned &maxSkewPart, unsigned &minSkewPart, bool calculateIfMissing) override { return dfsFile->getSkewInfo(maxSkew, minSkew, maxSkewPart, minSkewPart, calculateIfMissing); }
    virtual int  getExpire() override { return dfsFile->getExpire(); }
    virtual double getCost(const char * cluster) override { return dfsFile->getCost(cluster); }

//////////////

    virtual bool renamePhysicalPartFiles(const char *newlfn,const char *cluster=NULL,IMultiException *exceptions=NULL,const char *newbasedir=NULL) override
    {
        UNIMPLEMENTED_X("CServiceDistributeFile::renamePhysicalPartFiles");
    }
    virtual void rename(const char *logicalname,IUserDescriptor *user) override
    {
        UNIMPLEMENTED_X("CServiceDistributeFile::rename");
    }
    virtual void attach(const char *logicalname,IUserDescriptor *user) override
    {
        UNIMPLEMENTED_X("CServiceDistributeFile::rename");
    }
    virtual void detach(unsigned timeoutms=INFINITE, ICodeContext *ctx=NULL) override
    {
        UNIMPLEMENTED_X("CServiceDistributeFile::detach");
    }
    virtual void enqueueReplicate()
    {
        UNIMPLEMENTED_X("CServiceDistributeFile::enqueueReplicate");
    }


// setters (change file meta data)
    virtual void setPreferredClusters(const char *clusters) override { dfsFile->setPreferredClusters(clusters); }
    virtual void setSingleClusterOnly() override { dfsFile->setSingleClusterOnly(); }
    virtual void addCluster(const char *clustername,const ClusterPartDiskMapSpec &mspec) override { dfsFile->addCluster(clustername, mspec); }
    virtual bool removeCluster(const char *clustername) override { return dfsFile->removeCluster(clustername); }
    virtual void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec) override { dfsFile->updatePartDiskMapping(clustername, spec); }

    virtual void setModificationTime(const CDateTime &dt) override
    {
        // TBD
    }
    virtual void setModified() override
    {
        // TBD
    }
    virtual void setAccessedTime(const CDateTime &dt) override
    {
        // TBD
    }
    virtual void setAccessed() override
    {
        // TBD
    }
    virtual void addAttrValue(const char *attr, unsigned __int64 value) override
    {
        // TBD
    }
    virtual void setExpire(int expireDays) override
    {
        // TBD
    }
    virtual void setECL(const char *ecl) override
    {

    }
    virtual void resetHistory() override
    {
        // TBD
    }
    virtual void setProtect(const char *callerid, bool protect=true, unsigned timeoutms=INFINITE) override
    {
        // TBD
    }
    virtual void setColumnMapping(const char *mapping) override
    {
        // TBD
    }
    virtual void setRestrictedAccess(bool restricted) override
    {
        // TBD
    }
};

class CDFSFile : public CSimpleInterfaceOf<IDFSFile>
{
    Linked<IPropertyTree> fileMeta;
    unsigned __int64 lockId;
    unsigned keepAlivePeriod;

public:
    CDFSFile(IPropertyTree *_fileMeta, unsigned __int64 _lockId, unsigned _keepAlivePeriod) : fileMeta(_fileMeta), lockId(_lockId), keepAlivePeriod(_keepAlivePeriod)
    {
    }
    virtual IPropertyTree *getFileMeta() const override
    {
        return fileMeta.getLink();
    }
    virtual unsigned __int64 getLockId() const override
    {
        return lockId;
    }
};

#if 0
static const char *getEspServiceURL(IConstWorkUnit * wu)
{
    {
#ifdef _CONTAINERIZED
        // Look for 'eclservices' esp service, fallback to 'eclwatch' service.
        Owned<IPropertyTree> match = getGlobalConfigSP().getPropTree("services[@type='eclservices']");
        if (!match)
            throw makeStringException(0, "eclservices is not available");

        const char * espService = match->queryProp("@name");
        const char *protocol = match->getPropBool("@tls") ? "https" : "http";
        unsigned port = match->getPropInt("@port", 8010);

        VStringBuffer espURL("%s://%s:%u/FileSpray", protocol, espService, port);


        StringBuffer credentials;
        if (username && username[0] && userPW && userPW[0])
            credentials.setf("%s:%s@", username, userPW);
        else if (username && username[0])
            credentials.setf("%s@", username);

        const char *protocol = match->getPropBool("@tls") ? "https" : "http";
        unsigned port = match->getPropInt("@port", 8010);

        VStringBuffer espURL("mtls:%s://%s%s:%u", protocol, credentials.str(), espService, port);
        return espURL.detach();
#else
        Owned<IConstEnvironment> daliEnv = openDaliEnvironment();
        Owned<IPropertyTree> env = getEnvironmentTree(daliEnv);

        if (env.get())
        {
            StringBuffer wsFSUrl;
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
                    espBindingIter->query().getProp("@service",wsFSUrl.clear());
                    xpath.setf("Software/EspService[@name=\"%s\"]/Properties/@type", wsFSUrl.str());

                    if(env->getProp(xpath.str(), espServiceType.clear()))
                    {
                        if (!espServiceType.isEmpty() && (strieq(espServiceType.str(),"WsSMC")|| strieq(espServiceType.str(),"FileSpray_Serv")))
                        {
                            if (espBindingIter->query().getProp("@protocol",bindingProtocol.clear()))
                            {
                                Owned<IPropertyTreeIterator> espInstanceIter = espProcessIter->query().getElements("Instance");
                                ForEach(*espInstanceIter)
                                {
                                    if (espInstanceIter->query().getProp("@computer",espInstanceComputerName.clear()))
                                    {
                                        xpath.setf("Hardware/Computer[@name=\"%s\"]/@netAddress",espInstanceComputerName.str());
                                        if (env->getProp(xpath.str(),instanceAddress.clear()))
                                        {
                                            wsFSUrl.setf("%s://%s:%d/FileSpray", bindingProtocol.str(), instanceAddress.str(), espBindingIter->query().getPropInt("@port",8010)); // FileSpray seems to be fixed
                                            addConfiguredWsFSUrl(wsFSUrl.str());
                                        }
                                    }
                                }
                            }
                        }//EclWatch || ws_fs binding
                    }
                }//ESPBinding
            }//ESPProcess
        }
#endif

        if (isUrlListEmpty())
            throw MakeStringException(-1,"Could not find any WS FileSpray in the target HPCC configuration.");
    }

    const char * nextWsFSUrl = getNextAliveWsFSURL(wu);
    if (!nextWsFSUrl||!*nextWsFSUrl)
        throw MakeStringException(-1,"Could not contact any of the configured WS FileSpray instances, check HPCC configuration and system health.");

    PROGLOG("FileServices: Targeting ESP WsFileSpray URL: %s", nextWsFSUrl);

    return nextWsFSUrl;
}
#endif

IClientWsDfs *getDfsClient(const char *serviceEndpoint, const char *user, const char *token)
{
    // JCSMORE - can I reuse these, are they thread safe (AFishbeck?)

    const char *serviceUrl = "http://localhost:8010/WsDfs";
    Owned<IClientWsDfs> dfsClient = createWsDfsClient();
    dfsClient->addServiceUrl(serviceUrl);
    dfsClient->setUsernameToken(user, token, "");
    return dfsClient.getClear();
}

IDFSFile *lookupDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, const char *user, const char *token)
{
    // NB: logicalName can have an option postfix @cluster

    CDfsLogicalFileName lfn;
    lfn.set(logicalName);
    StringBuffer svc, remoteName;
    const char *service = nullptr;
    if (lfn.isRemote())
    {
        verifyex(lfn.getRemoteSpec(svc, remoteName));
        service = svc;
        logicalName = remoteName;
    }
    if (!service)
    {
        // JCSMORE - discover the k8s service from configuration..
        service = "localhost";
    }

    Owned<IClientWsDfs> dfsClient = getDfsClient(service, user, token);

    Owned<IClientDFSFileLookupResponse> dfsResp;

    CTimeMon tm(timeoutSecs);
    while (true)
    {
        try
        {
            Owned<IClientDFSFileLookupRequest> dfsReq = dfsClient->createDFSFileLookupRequest();

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
            unsigned __int64 lockId = resp->getPropInt64("@lockId");
            unsigned keepAlivePeriod = resp->getPropInt("@keepAlivePeriod");
            dbglogXML(resp, 2);
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

IDistributedFile *createLegacyDFSFile(IDFSFile *dfsFile)
{
    return new CServiceDistributeFile(dfsFile);
}

IDistributedFile *lookupLegacyDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, const char *user, const char *token)
{
    Owned<IDFSFile> dfsFile = lookupDFSFile(logicalName, timeoutSecs, keepAliveExpiryFrequency, user, token);
    return new CServiceDistributeFile(dfsFile);
}

IDistributedFile *lookupLegacyDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, IUserDescriptor *userDesc)
{
    Owned<IDFSFile> dfsFile = lookupDFSFile(logicalName, timeoutSecs, keepAliveExpiryFrequency, userDesc);
    return new CServiceDistributeFile(dfsFile);
}


} // namespace wsdfs

