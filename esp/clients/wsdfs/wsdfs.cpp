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


template <class INTERFACE>
class CServiceDistributedFileBase : public CSimpleInterfaceOf<INTERFACE>
{
protected:
    Linked<IDFSFile> dfsFile;
    StringAttr logicalName;
    Owned<IDistributedFile> legacyDFSFile;
    Owned<IFileDescriptor> fileDesc;

    class CDistributedSuperFileIterator: public CSimpleInterfaceOf<IDistributedSuperFileIterator>
    {
        Linked<IDFSFile> source;
        Owned<IDistributedSuperFile> cur;
        std::vector<std::string> owners;
        unsigned which = 0;

        void setCurrent(unsigned w)
        {
            VStringBuffer lfn("~remote::%s::%s", source->queryService(), owners[w].c_str());
            Owned<IDFSFile> dfsFile = lookupDFSFile(lfn, source->queryTimeoutSecs(), keepAliveExpiryFrequency, source->queryUserDescriptor());
            if (!dfsFile->numSubFiles())
                throwUnexpected();
            Owned<IDistributedFile> legacyDFSFile = createLegacyDFSFile(dfsFile);
            IDistributedSuperFile *super = legacyDFSFile->querySuperFile();
            assertex(super);
            cur.set(super);
        }
    public:
        CDistributedSuperFileIterator(IDFSFile *_source, std::vector<std::string> _owners) : source(_source), owners(_owners)
        {
        }
        virtual bool first() override
        {
            if (owners.empty())
                return false;
            which = 0;
            setCurrent(which);
            return true;
        }
        virtual bool next() override
        {
            if (which == (owners.size()-1))
            {
                cur.clear();
                return false;
            }
            ++which;
            setCurrent(which);
            return true;
        }
        virtual bool isValid() override
        {
            return cur != nullptr;
        }
        virtual IDistributedSuperFile &query() override
        {
            return *cur;
        }
        virtual const char *queryName() override
        {
            if (!isValid())
                return nullptr;
            return owners[which].c_str();
        }
    };

public:
    CServiceDistributedFileBase(IDFSFile *_dfsFile) : dfsFile(_dfsFile)
    {
        logicalName.set(dfsFile->queryMeta()->queryProp("FileMeta/@name"));
    }

    virtual unsigned numParts() override { return legacyDFSFile->numParts(); }
    virtual IDistributedFilePart &queryPart(unsigned idx) override { return legacyDFSFile->queryPart(idx); }
    virtual IDistributedFilePart* getPart(unsigned idx) override { return legacyDFSFile->getPart(idx); }
    virtual StringBuffer &getLogicalName(StringBuffer &name) override { return legacyDFSFile->getLogicalName(name); }
    virtual const char *queryLogicalName() override { return legacyDFSFile->queryLogicalName(); }
    virtual IDistributedFilePartIterator *getIterator(IDFPartFilter *filter=NULL) override { return legacyDFSFile->getIterator(filter); }
    virtual IFileDescriptor *getFileDescriptor(const char *clustername=NULL) override { return fileDesc.getLink(); }
    virtual const char *queryDefaultDir() override { return legacyDFSFile->queryDefaultDir(); }
    virtual const char *queryPartMask() override { return legacyDFSFile->queryPartMask(); }
    virtual IPropertyTree &queryAttributes() override { return legacyDFSFile->queryAttributes(); }
    virtual bool lockProperties(unsigned timeoutms=INFINITE) override
    {
        // TODO: implement. But for now only foreign [read] files are supported, where updates and locking have never been implemented.
        return true;
    }
    virtual void unlockProperties(DFTransactionState state=TAS_NONE) override
    {
        // TODO: implement. But for now only foreign [read] files are supported, where updates and locking have never been implemented.
    }
    virtual bool getModificationTime(CDateTime &dt) override { return legacyDFSFile->getModificationTime(dt); }
    virtual bool getAccessedTime(CDateTime &dt) override { return legacyDFSFile->getAccessedTime(dt); }
    virtual unsigned numCopies(unsigned partno) override { return legacyDFSFile->numCopies(partno); }
    virtual bool existsPhysicalPartFiles(unsigned short port) override
    {
        return legacyDFSFile->existsPhysicalPartFiles(port);
    }
    virtual __int64 getFileSize(bool allowphysical, bool forcephysical) override
    {
        return legacyDFSFile->getFileSize(allowphysical, forcephysical);
    }
    virtual __int64 getDiskSize(bool allowphysical, bool forcephysical) override
    {
        return legacyDFSFile->getDiskSize(allowphysical, forcephysical);
    }
    virtual bool getFileCheckSum(unsigned &checksum) override { return legacyDFSFile->getFileCheckSum(checksum); }
    virtual unsigned getPositionPart(offset_t pos,offset_t &base) override { return legacyDFSFile->getPositionPart(pos,base); }
    virtual IDistributedSuperFile *querySuperFile() override
    {
        return nullptr;
    }
    virtual IDistributedSuperFileIterator *getOwningSuperFiles(IDistributedFileTransaction *transaction=NULL) override
    {
        if (transaction)
            throwUnexpected();
        Owned<IPropertyTreeIterator> iter = dfsFile->queryMeta()->getElements("FileMeta/SuperFile/SuperOwner");
        std::vector<std::string> superOwners;
        StringBuffer pname;
        ForEach(*iter)
        {
            iter->query().getProp("@name",pname.clear());
            if (pname.length())
                superOwners.push_back(pname.str());
        }

        return new CDistributedSuperFileIterator(dfsFile, superOwners);
    }
    virtual bool isCompressed(bool *blocked=NULL) override { return legacyDFSFile->isCompressed(blocked); }
    virtual StringBuffer &getClusterName(unsigned clusternum,StringBuffer &name) override { return legacyDFSFile->getClusterName(clusternum,name); }
    virtual unsigned getClusterNames(StringArray &clusters) override { return legacyDFSFile->getClusterNames(clusters); }                                                                                      // (use findCluster)
    virtual unsigned numClusters() override { return legacyDFSFile->numClusters(); }
    virtual unsigned findCluster(const char *clustername) override { return legacyDFSFile->findCluster(clustername); }
    virtual ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum) override { return legacyDFSFile->queryPartDiskMapping(clusternum); }
    virtual IGroup *queryClusterGroup(unsigned clusternum) override { return legacyDFSFile->queryClusterGroup(clusternum); }
    virtual StringBuffer &getClusterGroupName(unsigned clusternum, StringBuffer &name) override
    {
        return fileDesc->getClusterGroupName(clusternum, name);
    }
    virtual StringBuffer &getECL(StringBuffer &buf) override { return legacyDFSFile->getECL(buf); }

    virtual bool canModify(StringBuffer &reason) override
    {
        return false;
    }
    virtual bool canRemove(StringBuffer &reason,bool ignoresub=false) override
    {
        return false;
    }
    virtual bool checkClusterCompatible(IFileDescriptor &fdesc, StringBuffer &err) override { return legacyDFSFile->checkClusterCompatible(fdesc,err); }

    virtual bool getFormatCrc(unsigned &crc) override { return legacyDFSFile->getFormatCrc(crc); }
    virtual bool getRecordSize(size32_t &rsz) override { return legacyDFSFile->getRecordSize(rsz); }
    virtual bool getRecordLayout(MemoryBuffer &layout, const char *attrname) override { return legacyDFSFile->getRecordLayout(layout,attrname); }
    virtual StringBuffer &getColumnMapping(StringBuffer &mapping) override { return legacyDFSFile->getColumnMapping(mapping); }

    virtual bool isRestrictedAccess() override { return legacyDFSFile->isRestrictedAccess(); }
    virtual unsigned setDefaultTimeout(unsigned timems) override { return legacyDFSFile->setDefaultTimeout(timems); }

    virtual void validate() override { legacyDFSFile->validate(); }

    virtual IPropertyTree *queryHistory() const override { return legacyDFSFile->queryHistory(); }
    virtual bool isExternal() const override { return false; }
    virtual bool getSkewInfo(unsigned &maxSkew, unsigned &minSkew, unsigned &maxSkewPart, unsigned &minSkewPart, bool calculateIfMissing) override { return legacyDFSFile->getSkewInfo(maxSkew, minSkew, maxSkewPart, minSkewPart, calculateIfMissing); }
    virtual int getExpire() override { return legacyDFSFile->getExpire(); }
    virtual double getCost(const char * cluster) override { return legacyDFSFile->getCost(cluster); }



// setters (change file meta data)
    virtual void setPreferredClusters(const char *clusters) override { legacyDFSFile->setPreferredClusters(clusters); }
    virtual void setSingleClusterOnly() override { legacyDFSFile->setSingleClusterOnly(); }
    virtual void addCluster(const char *clustername,const ClusterPartDiskMapSpec &mspec) override { legacyDFSFile->addCluster(clustername, mspec); }
    virtual bool removeCluster(const char *clustername) override { return legacyDFSFile->removeCluster(clustername); }
    virtual void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec) override { legacyDFSFile->updatePartDiskMapping(clustername, spec); }

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
    virtual bool renamePhysicalPartFiles(const char *newlfn,const char *cluster=NULL,IMultiException *exceptions=NULL,const char *newbasedir=NULL) override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::renamePhysicalPartFiles");
    }
    virtual void rename(const char *logicalname,IUserDescriptor *user) override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::rename");
    }
    virtual void attach(const char *logicalname,IUserDescriptor *user) override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::rename");
    }
    virtual void detach(unsigned timeoutms=INFINITE, ICodeContext *ctx=NULL) override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::detach");
    }
    virtual void enqueueReplicate() override
    {
        UNIMPLEMENTED_X("CServiceDistributedFileBase::enqueueReplicate");
    }
};

class CServiceDistributedFile : public CServiceDistributedFileBase<IDistributedFile>
{
    typedef CServiceDistributedFileBase<IDistributedFile> PARENT;
public:
    CServiceDistributedFile(IDFSFile *_dfsFile) : PARENT(_dfsFile)
    {
        IPropertyTree *file = dfsFile->queryMeta()->queryPropTree("FileMeta/File");
        const char *remotePlaneName = file->queryProp("@group");
        VStringBuffer planeXPath("planes[@name=\"%s\"]", remotePlaneName);
        IPropertyTree *remotePlane = dfsFile->queryMeta()->queryPropTree(planeXPath);
        assertex(remotePlane);

        // I think only planes that are backed by PVC's will need path translation
        // Ones backed by URL's or hostGroups will be access directly.

        // Path translation is necessary, because the local plane will not necessarily have the same
        // prefix. In particular, both a local and remote plane may want to use the same prefix/mount.
        // So, the local plane will be defined with a unique prefix locally.
        if (remotePlane->hasProp("@pvc"))
        {
            // A external plane within another environment backed by a PVC, will need a pre-existing
            // corresponding plane and PVC in the local environment.
            // The local plane will be associated with the remote environment, via it's service host/IP.

            const char *remoteService = dfsFile->queryService();
            Owned<IStoragePlane> localPlane = getRemoteStoragePlaneByHost(remoteService, false);
            if (!localPlane)
                throw makeStringExceptionV(0, "Local environment does not have a corresponding plane for remote environment '%s'", remoteService);

            StringBuffer remotePlanePrefix;
            remotePlane->getProp("@prefix", remotePlanePrefix);
            if (remotePlane->hasProp("@subPath"))
                remotePlanePrefix.append('/').append(remotePlane->queryProp("@subPath"));
            // the plane prefix should match the base of file's base directory
            // Q: what if the plane has been redefined since the files were created?

            VStringBuffer clusterXPath("Cluster[@name=\"%s\"]", remotePlaneName);
            IPropertyTree *cluster = file->queryPropTree("Cluster[@name=\"%s\"]");
            assertex(cluster);
            const char *clusterDir = cluster->queryProp("@defaultBaseDir");
            assertex(startsWith(clusterDir, remotePlanePrefix));
            clusterDir += remotePlanePrefix.length();

            VStringBuffer newPath("%s/%s", localPlane->queryPrefix(), clusterDir); // add remaining tail of path
            cluster->setProp("@defaultBaseDir", newPath.str());

            const char *dir = file->queryProp("@directory");
            assertex(startsWith(dir, remotePlanePrefix));
            dir += remotePlanePrefix.length();
            newPath.clear().appendf("%s/%s", localPlane->queryPrefix(), dir); // add remaining tail of path
            file->setProp("@dir", newPath.str());
        }

        fileDesc.setown(deserializeFileDescriptorTree(file));
        if (fileDesc)
            fileDesc->setTraceName(logicalName);

        legacyDFSFile.setown(queryDistributedFileDirectory().createNew(fileDesc, logicalName));
    }    
};

class CServiceSuperDistributedFile : public CServiceDistributedFileBase<IDistributedSuperFile>
{
    typedef CServiceDistributedFileBase<IDistributedSuperFile> PARENT;
    Owned<IDistributedSuperFile> legacyDFSSuperFile;

public:
    CServiceSuperDistributedFile(IDFSFile *_dfsFile) : PARENT(_dfsFile)
    {
        IArrayOf<IDistributedFile> subFiles;
        unsigned subs = dfsFile->numSubFiles();
        for (unsigned s=0; s<subs; s++)
        {
            Owned<IDFSFile> subFile = dfsFile->getSubFile(s);
            Owned<IDistributedFile> legacyDFSFile = createLegacyDFSFile(subFile);
            subFiles.append(*legacyDFSFile.getClear());
        }
        legacyDFSSuperFile.setown(queryDistributedFileDirectory().createNewSuperFile(dfsFile->queryMeta()->queryPropTree("FileMeta/SuperFile"), logicalName, &subFiles));
        legacyDFSFile.set(legacyDFSSuperFile);
        fileDesc.setown(legacyDFSSuperFile->getFileDescriptor());
    }
// IDistributedFile overrides
    virtual IDistributedSuperFile *querySuperFile() override
    {
        return this;
    }

// IDistributedSuperFile overrides
    virtual IDistributedFile &querySubFile(unsigned idx,bool sub) override
    {
        return legacyDFSSuperFile->querySubFile(idx, sub);
    }
    virtual IDistributedFile *querySubFileNamed(const char *name, bool sub) override
    {
        return legacyDFSSuperFile->querySubFileNamed(name, sub);
    }
    virtual IDistributedFile *getSubFile(unsigned idx,bool sub) override
    {
        return legacyDFSSuperFile->getSubFile(idx, sub);
    }
    virtual unsigned numSubFiles(bool sub) override
    {
        return legacyDFSSuperFile->numSubFiles(sub);
    }
    virtual bool isInterleaved() override
    {
        return legacyDFSSuperFile->isInterleaved();
    }
    virtual IDistributedFile *querySubPart(unsigned partidx,unsigned &subfileidx) override
    {
        return legacyDFSSuperFile->querySubPart(partidx, subfileidx);
    }
    virtual unsigned getPositionPart(offset_t pos, offset_t &base) override
    {
        return legacyDFSSuperFile->getPositionPart(pos, base);
    }
    virtual IDistributedFileIterator *getSubFileIterator(bool supersub=false) override
    {
        return legacyDFSSuperFile->getSubFileIterator(supersub);
    }
    virtual void validate() override
    {
        if (!legacyDFSSuperFile->existsPhysicalPartFiles(0))
        {
            const char * logicalName = queryLogicalName();
            throw MakeStringException(-1, "Some physical parts do not exists, for logical file : %s",(isEmptyString(logicalName) ? "[unattached]" : logicalName));
        }
    }

// IDistributedSuperFile
    virtual void addSubFile(const char *subfile, bool before=false, const char *other=NULL, bool addcontents=false, IDistributedFileTransaction *transaction=NULL) override
    {
        UNIMPLEMENTED_X("CServiceSuperDistributedFile::addSubFile");        
    }
    virtual bool removeSubFile(const char *subfile, bool remsub, bool remcontents=false, IDistributedFileTransaction *transaction=NULL) override
    {
        UNIMPLEMENTED_X("CServiceSuperDistributedFile::removeSubFile");        
    }
    virtual bool removeOwnedSubFiles(bool remsub, IDistributedFileTransaction *transaction=NULL) override
    {
        UNIMPLEMENTED_X("CServiceSuperDistributedFile::removeOwnedSubFiles");        
    }
    virtual bool swapSuperFile( IDistributedSuperFile *_file, IDistributedFileTransaction *transaction) override
    {
        UNIMPLEMENTED_X("CServiceSuperDistributedFile::swapSuperFile");        
    }
};

static IDFSFile *createDFSFile(IPropertyTree *meta, const char *service, unsigned timeoutSecs, IUserDescriptor *userDesc);
class CDFSFile : public CSimpleInterfaceOf<IDFSFile>
{
    Linked<IPropertyTree> meta;
    unsigned __int64 lockId;
    std::vector<Owned<IDFSFile>> subFiles;
    StringAttr service;
    unsigned timeoutSecs;
    Linked<IUserDescriptor> userDesc;

public:
    CDFSFile(IPropertyTree *_meta, const char *_service, unsigned _timeoutSecs, IUserDescriptor *_userDesc)
        : meta(_meta), service(_service), timeoutSecs(_timeoutSecs), userDesc(_userDesc)
    {
        IPropertyTree *fileMeta = meta->queryPropTree("FileMeta");
        lockId = fileMeta->getPropInt64("@lockId");
        if (fileMeta->getPropBool("@isSuper"))
        {
            Owned<IPropertyTreeIterator> iter = fileMeta->getElements("FileMeta");
            ForEach(*iter)
            {
                IPropertyTree &subMeta = iter->query();
                subFiles.push_back(createDFSFile(&subMeta, service, timeoutSecs, userDesc));
            }
        }
    }
    virtual IPropertyTree *queryMeta() const override
    {
        return meta;
    }
    virtual unsigned __int64 getLockId() const override
    {
        return lockId;
    }
    virtual unsigned numSubFiles() const override
    {
        return (unsigned)subFiles.size();
    }
    virtual IDFSFile *getSubFile(unsigned idx) const override
    {
        return LINK(subFiles[idx]);
    }
    virtual const char *queryService() const override
    {
        return service;
    }
    virtual IUserDescriptor *queryUserDescriptor() const override
    {
        return userDesc.getLink();
    }
    virtual unsigned queryTimeoutSecs() const override
    {
        return timeoutSecs;
    }
};

static IDFSFile *createDFSFile(IPropertyTree *meta, const char *service, unsigned timeoutSecs, IUserDescriptor *userDesc)
{
    return new CDFSFile(meta, service, timeoutSecs, userDesc);
}

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

IClientWsDfs *getDfsClient(const char *serviceEndpoint, IUserDescriptor *userDesc)
{
    // JCSMORE - can I reuse these, are they thread safe (AFishbeck?)

    VStringBuffer serviceUrl("http://%s:8010/WsDfs", serviceEndpoint);
    PROGLOG("Using serviceUrl = %s", serviceUrl.str());
    Owned<IClientWsDfs> dfsClient = createWsDfsClient();
    dfsClient->addServiceUrl(serviceUrl);
    StringBuffer user, token;
    userDesc->getUserName(user),
    userDesc->getPassword(token);
    dfsClient->setUsernameToken(user, token, "");
    return dfsClient.getClear();
}

static CriticalSection serviceLeaseMapCS;
static std::unordered_map<std::string, unsigned __int64> serviceLeaseMap;
unsigned __int64 ensureClientLease(const char *service, IUserDescriptor *userDesc)
{
    CriticalBlock block(serviceLeaseMapCS);
    auto r = serviceLeaseMap.find(service);
    if (r != serviceLeaseMap.end())
        return r->second;

    Owned<IClientWsDfs> dfsClient = getDfsClient(service, userDesc);

    Owned<IClientLeaseResponse> leaseResp;

    unsigned timeoutSecs = 60;
    CTimeMon tm(timeoutSecs*1000);
    while (true)
    {
        try
        {
            Owned<IClientLeaseRequest> leaseReq = dfsClient->createGetLeaseRequest();
            leaseReq->setKeepAliveExpiryFrequency(keepAliveExpiryFrequency);
            leaseResp.setown(dfsClient->GetLease(leaseReq));

            unsigned __int64 leaseId = leaseResp->getLeaseId();
            serviceLeaseMap[service] = leaseId;
            return leaseId;
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
            throw makeStringExceptionV(0, "GetLease timed out: timeoutSecs=%u", timeoutSecs);
        Sleep(5000); // sanity sleep
    }
}


IDFSFile *lookupDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, IUserDescriptor *userDesc)
{
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

    Owned<IClientWsDfs> dfsClient = getDfsClient(service, userDesc);

    Owned<IClientDFSFileLookupResponse> dfsResp;

    CTimeMon tm(timeoutSecs*1000); // NB: this timeout loop is to cater for *a* esp disappearing.
    while (true)
    {
        try
        {
            Owned<IClientDFSFileLookupRequest> dfsReq = dfsClient->createDFSFileLookupRequest();

            dfsReq->setName(logicalName);
            unsigned remaining;
            if (tm.timedout(&remaining))
                break;
            dfsReq->setRequestTimeout(remaining/1000);
            unsigned __int64 clientLeaseId = ensureClientLease(service, userDesc);
            dfsReq->setLeaseId(clientLeaseId);

            dfsResp.setown(dfsClient->DFSFileLookup(dfsReq));

            const IMultiException *excep = &dfsResp->getExceptions(); // NB: warning despite getXX name, this does not Link
            if (excep->ordinality() > 0)
                throw LINK((IMultiException *)excep); // NB - const IException.. not caught in general..

            const char *base64Resp = dfsResp->getMeta();
            MemoryBuffer compressedRespMb;
            JBASE64_Decode(base64Resp, compressedRespMb);
            MemoryBuffer decompressedRespMb;
            fastLZDecompressToBuffer(decompressedRespMb, compressedRespMb);
            Owned<IPropertyTree> meta = createPTree(decompressedRespMb);
            return createDFSFile(meta, service, timeoutSecs, userDesc);
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
}

IDistributedFile *createLegacyDFSFile(IDFSFile *dfsFile)
{
    if (dfsFile->queryMeta()->getPropBool("FileMeta/@isSuper"))
        return new CServiceSuperDistributedFile(dfsFile);
    else
        return new CServiceDistributedFile(dfsFile);
}

IDistributedFile *lookupLegacyDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, IUserDescriptor *userDesc)
{
    Owned<IDFSFile> dfsFile = lookupDFSFile(logicalName, timeoutSecs, keepAliveExpiryFrequency, userDesc);
    return createLegacyDFSFile(dfsFile);
}


} // namespace wsdfs

