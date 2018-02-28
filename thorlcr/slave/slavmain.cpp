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

#include <platform.h>

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "jprop.hpp"
#include "jiter.ipp"
#include "jflz.hpp"
#include "jlzw.hpp"

#include "jhtree.hpp"
#include "mpcomm.hpp"

#include "portlist.h"
#include "rmtfile.hpp"
#include "daclient.hpp"
#include "dafdesc.hpp"

#include "slwatchdog.hpp"
#include "thbuf.hpp"
#include "thmem.hpp"
#include "thexception.hpp"

#include "backup.hpp"
#include "slave.hpp"
#include "thormisc.hpp"
#include "thorport.hpp"
#include "thgraphslave.hpp"
#include "slave.ipp"
#include "thcompressutil.hpp"
#include "dalienv.hpp"

//---------------------------------------------------------------------------

//---------------------------------------------------------------------------

#define ISDALICLIENT // JCSMORE plugins *can* access dali - though I think we should probably prohibit somehow.
void enableThorSlaveAsDaliClient()
{
#ifdef ISDALICLIENT
    PROGLOG("Slave activated as a Dali client");
    const char *daliServers = globals->queryProp("@DALISERVERS");
    if (!daliServers)
        throw MakeStringException(0, "No Dali server list specified");
    Owned<IGroup> serverGroup = createIGroup(daliServers, DALI_SERVER_PORT);
    unsigned retry = 0;
    for (;;)
    {
        try
        {
            LOG(MCdebugProgress, thorJob, "calling initClientProcess");
            initClientProcess(serverGroup,DCR_ThorSlave, getFixedPort(TPORT_mp));
            break;
        }
        catch (IJSOCK_Exception *e)
        {
            if ((e->errorCode()!=JSOCKERR_port_in_use))
                throw;
            FLLOG(MCexception(e), thorJob, e,"InitClientProcess");
            if (retry++>10)
                throw;
            e->Release();
            LOG(MCdebugProgress, thorJob, "Retrying");
            Sleep(retry*2000);
        }
    }
    setPasswordsFromSDS();
#endif
}

void disableThorSlaveAsDaliClient()
{
#ifdef ISDALICLIENT
    closeEnvironment();
    closedownClientProcess();   // dali client closedown
    PROGLOG("Slave deactivated as a Dali client");
#endif
}

class CJobListener : public CSimpleInterface
{
    bool &stopped;
    CriticalSection crit;
    OwningStringSuperHashTableOf<CJobSlave> jobs;
    CFifoFileCache querySoCache; // used to mirror master cache
    IArrayOf<IMPServer> mpServers;
    unsigned channelsPerSlave;

    class CThreadExceptionCatcher : implements IExceptionHandler
    {
        CJobListener &jobListener;
    public:
        CThreadExceptionCatcher(CJobListener &_jobListener) : jobListener(_jobListener)
        {
            addThreadExceptionHandler(this);
        }
        ~CThreadExceptionCatcher()
        {
            removeThreadExceptionHandler(this);
        }
        virtual bool fireException(IException *e)
        {
            mptag_t mptag;
            {
                CriticalBlock b(jobListener.crit);
                if (0 == jobListener.jobs.count())
                {
                    EXCLOG(e, "No job active exception: ");
                    return true;
                }
                IThorException *te = QUERYINTERFACE(e, IThorException);
                CJobSlave *job = NULL;
                if (te && te->queryJobId())
                    job = jobListener.jobs.find(te->queryJobId());
                if (!job)
                {
                    // JCSMORE - exception fallen through to thread exception handler, from unknown job, fire up to 1st job for now.
                    job = (CJobSlave *)jobListener.jobs.next(NULL);
                }
                mptag = job->querySlaveMpTag();
            }
            CMessageBuffer msg;
            msg.append(smt_errorMsg);
            msg.append(0); // unknown really
            serializeThorException(e, msg);

            try
            {
                if (!queryNodeComm().sendRecv(msg, 0, mptag, LONGTIMEOUT))
                    EXCLOG(e, "Failed to send exception to master");
            }
            catch (IException *e2)
            {
                StringBuffer str("Error whilst sending exception '");
                e->errorMessage(str);
                str.append("' to master");
                EXCLOG(e2, str.str());
                e2->Release();
            }
            return true;
        }
    } excptHandler;

public:
    CJobListener(bool &_stopped) : stopped(_stopped), excptHandler(*this)
    {
        stopped = true;
        channelsPerSlave = globals->getPropInt("@channelsPerSlave", 1);
        unsigned localThorPortInc = globals->getPropInt("@localThorPortInc", 200);
        mpServers.append(* getMPServer());
        bool reconnect = globals->getPropBool("@MPChannelReconnect");
        for (unsigned sc=1; sc<channelsPerSlave; sc++)
        {
            unsigned port = getMachinePortBase() + (sc * localThorPortInc);
            IMPServer *mpServer = startNewMPServer(port);
            if (reconnect)
                mpServer->setOpt(mpsopt_channelreopen, "true");
            mpServers.append(*mpServer);
        }
    }
    ~CJobListener()
    {
        for (unsigned sc=1; sc<channelsPerSlave; sc++)
            mpServers.item(sc).stop();
        mpServers.kill();
        stop();
    }
    CJobSlave *findJob(const char *key)
    {
        if (nullptr == key)
            return jobs.next(nullptr);
        return jobs.find(key);
    }
    void stop()
    {
        queryNodeComm().cancel(0, masterSlaveMpTag);
    }
    virtual void slaveMain()
    {
        rank_t slaveProc = queryNodeGroup().rank()-1;
        unsigned totSlaveProcs = queryNodeClusterWidth();
        StringBuffer slaveStr;
        for (unsigned c=0; c<channelsPerSlave; c++)
        {
            unsigned o = slaveProc + (c * totSlaveProcs);
            if (c)
                slaveStr.append(",");
            slaveStr.append(o+1);
        }
        StringBuffer virtStr;
        if (channelsPerSlave>1)
            virtStr.append("virtual slaves:");
        else
            virtStr.append("slave:");
        PROGLOG("Slave log %u contains %s %s", slaveProc+1, virtStr.str(), slaveStr.str());

        if (channelsPerSlave>1)
        {
            class CVerifyThread : public CInterface, implements IThreaded
            {
                CThreaded threaded;
                CJobListener &jobListener;
                unsigned channel;
            public:
                CVerifyThread(CJobListener &_jobListener, unsigned _channel)
                    : jobListener(_jobListener), channel(_channel), threaded("CVerifyThread", this)
                {
                    start();
                }
                ~CVerifyThread() { join(); }
                void start() { threaded.start(); }
                void join() { threaded.join(); }
                virtual void threadmain() override
                {
                    Owned<ICommunicator> comm = jobListener.mpServers.item(channel).createCommunicator(&queryClusterGroup());
                    PROGLOG("verifying mp connection to rest of slaves (from channel=%d)", channel);
                    if (!comm->verifyAll())
                        ERRLOG("Failed to connect to rest of slaves");
                    else
                        PROGLOG("verified mp connection to rest of slaves");
                }
            };
            CIArrayOf<CInterface> verifyThreads;
            for (unsigned c=0; c<channelsPerSlave; c++)
                verifyThreads.append(*new CVerifyThread(*this, c));
        }

        StringBuffer soPath;
        globals->getProp("@query_so_dir", soPath);
        StringBuffer soPattern("*.");
#ifdef _WIN32
        soPattern.append("dll");
#else
        soPattern.append("so");
#endif
        if (globals->getPropBool("Debug/@dllsToSlaves",true))
            querySoCache.init(soPath.str(), DEFAULT_QUERYSO_LIMIT, soPattern);
        Owned<ISlaveWatchdog> watchdog;
        if (globals->getPropBool("@watchdogEnabled"))
            watchdog.setown(createProgressHandler(globals->getPropBool("@useUDPWatchdog")));

        CMessageBuffer msg;
        stopped = false;
        bool doReply;
        while (!stopped && queryNodeComm().recv(msg, 0, masterSlaveMpTag))
        {
            doReply = true;
            msgids cmd;
            try
            {
                msg.read((unsigned &)cmd);
                switch (cmd)
                {
                    case QueryInit:
                    {
                        MemoryBuffer mb;
                        decompressToBuffer(mb, msg);
                        msg.swapWith(mb);
                        mptag_t mptag, slaveMsgTag;
                        deserializeMPtag(msg, mptag);
                        queryNodeComm().flush(mptag);
                        deserializeMPtag(msg, slaveMsgTag);
                        queryNodeComm().flush(slaveMsgTag);
                        StringBuffer soPath, soPathTail;
                        StringAttr wuid, graphName, remoteSoPath;
                        msg.read(wuid);
                        msg.read(graphName);
                        msg.read(remoteSoPath);
                        bool sendSo;
                        msg.read(sendSo);

                        RemoteFilename rfn;
                        SocketEndpoint masterEp = queryMyNode()->endpoint();
                        masterEp.port = 0;
                        rfn.setPath(masterEp, remoteSoPath);
                        rfn.getTail(soPathTail);
                        if (sendSo)
                        {
                            size32_t size;
                            msg.read(size);
                            globals->getProp("@query_so_dir", soPath);
                            if (soPath.length())
                                addPathSepChar(soPath);
                            soPath.append(soPathTail);
                            const byte *queryPtr = msg.readDirect(size);
                            Owned<IFile> iFile = createIFile(soPath.str());
                            try
                            {
                                iFile->setCreateFlags(S_IRWXU);
                                Owned<IFileIO> iFileIO = iFile->open(IFOwrite);
                                iFileIO->write(0, size, queryPtr);
                            }
                            catch (IException *e)
                            {
                                IException *e2 = ThorWrapException(e, "Failed to save dll: %s", soPath.str());
                                e->Release();
                                throw e2;
                            }
                            assertex(globals->getPropBool("Debug/@dllsToSlaves", true));
                            querySoCache.add(soPath.str());
                        }
                        else
                        {
                            if (!rfn.isLocal())
                            {
                                StringBuffer _remoteSoPath;
                                rfn.getRemotePath(_remoteSoPath);
                                remoteSoPath.set(_remoteSoPath);
                            }
                            if (globals->getPropBool("Debug/@dllsToSlaves", true))
                            {
                                globals->getProp("@query_so_dir", soPath);
                                if (soPath.length())
                                    addPathSepChar(soPath);
                                soPath.append(soPathTail);
                                OwnedIFile iFile = createIFile(soPath.str());
                                if (!iFile->exists())
                                {
                                    WARNLOG("Slave cached query dll missing: %s, will attempt to fetch from master", soPath.str());
                                    copyFile(soPath.str(), remoteSoPath);
                                }
                                querySoCache.add(soPath.str());
                            }
                            else
                                soPath.append(remoteSoPath);
                        }
#ifdef __linux__
                    // only relevant if dllsToSlaves=false and query_so_dir was fully qualified remote path (e.g. //<ip>/path/file
                        rfn.setRemotePath(soPath.str());
                        StringBuffer tempSo;
                        if (!rfn.isLocal())
                        {
                            WARNLOG("Cannot load shared object directly from remote path, creating temporary local copy: %s", soPath.str());
                            GetTempName(tempSo,"so",true);
                            copyFile(tempSo.str(), soPath.str());
                            soPath.clear().append(tempSo.str());
                        }
#endif
                        Owned<ILoadedDllEntry> querySo = createDllEntry(soPath.str(), false, NULL, false);

                        Owned<IPropertyTree> workUnitInfo = createPTree(msg);
                        StringBuffer user;
                        workUnitInfo->getProp("user", user);

                        PROGLOG("Started wuid=%s, user=%s, graph=%s\n", wuid.get(), user.str(), graphName.get());
                        PROGLOG("Using query: %s", soPath.str());

                        if (!globals->getPropBool("Debug/@slaveDaliClient") && workUnitInfo->getPropBool("Debug/slavedaliclient", false))
                        {
                            PROGLOG("Workunit option 'slaveDaliClient' enabled");
                            enableThorSlaveAsDaliClient();
                        }

                        Owned<IPropertyTree> deps = createPTree(msg);

                        Owned<CJobSlave> job = new CJobSlave(watchdog, workUnitInfo, graphName, querySo, mptag, slaveMsgTag);
                        job->setXGMML(deps);
                        for (unsigned sc=0; sc<channelsPerSlave; sc++)
                            job->addChannel(&mpServers.item(sc));
                        jobs.replace(*job.getLink());
                        job->startJob();

                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case QueryDone:
                    {
                        StringAttr key;
                        msg.read(key);
                        CJobSlave *job = jobs.find(key.get());
                        StringAttr wuid = job->queryWuid();
                        StringAttr graphName = job->queryGraphName();

                        PROGLOG("Finished wuid=%s, graph=%s", wuid.get(), graphName.get());

                        if (!globals->getPropBool("Debug/@slaveDaliClient") && job->getWorkUnitValueBool("slaveDaliClient", false))
                            disableThorSlaveAsDaliClient();

                        PROGLOG("QueryDone, removing %s from jobs", key.get());
                        jobs.removeExact(job);
                        PROGLOG("QueryDone, removed %s from jobs", key.get());

                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case GraphInit:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (!job)
                            throw MakeStringException(0, "Job not found: %s", jobKey.get());

                        mptag_t executeReplyTag = job->deserializeMPTag(msg);
                        size32_t len;
                        msg.read(len);
                        MemoryBuffer createInitData;
                        createInitData.append(len, msg.readDirect(len));

                        graph_id subGraphId;
                        msg.read(subGraphId);
                        unsigned graphInitDataPos = msg.getPos();

                        VStringBuffer xpath("node[@id='%" GIDPF "u']", subGraphId);
                        Owned<IPropertyTree> graphNode = job->queryGraphXGMML()->getPropTree(xpath.str());
                        job->addSubGraph(*graphNode);

                        /* JCSMORE - should improve, create 1st graph with create context/init data and clone
                         * Should perhaps do this initialization in parallel..
                         */
                        for (unsigned c=0; c<job->queryJobChannels(); c++)
                        {
                            PROGLOG("GraphInit: %s, graphId=%" GIDPF "d, slaveChannel=%d", jobKey.get(), subGraphId, c);
                            CJobChannel &jobChannel = job->queryJobChannel(c);
                            Owned<CSlaveGraph> subGraph = (CSlaveGraph *)jobChannel.getGraph(subGraphId);
                            subGraph->setExecuteReplyTag(executeReplyTag);

                            createInitData.reset(0);
                            subGraph->deserializeCreateContexts(createInitData);

                            msg.reset(graphInitDataPos);
                            subGraph->init(msg);

                            jobChannel.addDependencies(job->queryXGMML(), false);
                        }

                        for (unsigned c=0; c<job->queryJobChannels(); c++)
                        {
                            CJobChannel &jobChannel = job->queryJobChannel(c);
                            Owned<CSlaveGraph> subGraph = (CSlaveGraph *)jobChannel.getGraph(subGraphId);

                            jobChannel.startGraph(*subGraph, true, 0, NULL);
                        }
                        msg.clear();
                        msg.append(false);

                        break;
                    }
                    case GraphEnd:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            graph_id gid;
                            msg.read(gid);
                            msg.clear();
                            msg.append(false);
                            for (unsigned c=0; c<job->queryJobChannels(); c++)
                            {
                                CJobChannel &jobChannel = job->queryJobChannel(c);
                                Owned<CSlaveGraph> graph = (CSlaveGraph *)jobChannel.getGraph(gid);
                                if (graph)
                                {
                                    msg.append(jobChannel.queryMyRank()-1);
                                    graph->getDone(msg);
                                }
                                else
                                {
                                    msg.append((rank_t)0); // JCSMORE - not sure why this would ever happen
                                }
                            }
                            job->reportGraphEnd(gid);
                        }
                        else
                        {
                            msg.clear();
                            msg.append(false);
                        }
                        break;
                    }
                    case GraphAbort:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        PROGLOG("GraphAbort: %s", jobKey.get());
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            graph_id gid;
                            msg.read(gid);
                            for (unsigned c=0; c<job->queryJobChannels(); c++)
                            {
                                CJobChannel &jobChannel = job->queryJobChannel(c);
                                Owned<CGraphBase> graph = jobChannel.getGraph(gid);
                                if (graph)
                                {
                                    Owned<IThorException> e = MakeGraphException(graph, 0, "GraphAbort");
                                    graph->abort(e);
                                }
                            }
                        }
                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case Shutdown:
                    {
                        stopped = true;
                        PROGLOG("Shutdown received");
                        if (watchdog)
                            watchdog->stop();
                        mptag_t sdreplyTag;
                        deserializeMPtag(msg, sdreplyTag);
                        msg.setReplyTag(sdreplyTag);
                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case GraphGetResult:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        PROGLOG("GraphGetResult: %s", jobKey.get());
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            graph_id gid;
                            msg.read(gid);
                            activity_id ownerId;
                            msg.read(ownerId);
                            unsigned resultId;
                            msg.read(resultId);
                            mptag_t replyTag = job->deserializeMPTag(msg);
                            msg.setReplyTag(replyTag);
                            msg.clear();
                            doReply = false;
                            for (unsigned c=0; c<job->queryJobChannels(); c++)
                            {
                                CJobChannel &jobChannel = job->queryJobChannel(c);
                                Owned<IThorResult> result = jobChannel.getOwnedResult(gid, ownerId, resultId);
                                Owned<IRowStream> resultStream = result->getRowStream();
                                sendInChunks(jobChannel.queryJobComm(), 0, replyTag, resultStream, result->queryRowInterfaces());
                            }
                        }
                        break;
                    }
                    case DebugRequest:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            mptag_t replyTag = job->deserializeMPTag(msg);
                            msg.setReplyTag(replyTag);
                            StringAttr rawText;
                            msg.read(rawText);
                            PROGLOG("DebugRequest: %s %s", jobKey.get(), rawText.get());
                            msg.clear();
                            job->debugRequest(msg, rawText);
                        }
                        else
                            PROGLOG("DebugRequest: %s - Job not found", jobKey.get());

                        break;
                    }
                    default:
                        throwUnexpected();
                }
            }
            catch (IException *e)
            {
                EXCLOG(e, NULL);
                if (doReply && TAG_NULL != msg.getReplyTag())
                {
                    doReply = false;
                    msg.clear();
                    msg.append(true);
                    serializeThorException(e, msg);
                    queryNodeComm().reply(msg);
                }
                e->Release();
            }
            if (doReply && msg.getReplyTag()!=TAG_NULL)
                queryNodeComm().reply(msg);
        }
    }

friend class CThreadExceptionCatcher;
};

//////////////////////////


class CStringAttr : public StringAttr, public CSimpleInterface
{
public:
    CStringAttr(const char *str) : StringAttr(str) { }
    const char *queryFindString() const { return get(); }
};
class CFileInProgressHandler : public CSimpleInterface, implements IFileInProgressHandler
{
    CriticalSection crit;
    StringSuperHashTableOf<CStringAttr> lookup;
    QueueOf<CStringAttr, false> fipList;
    OwnedIFileIO iFileIO;
    static const char *formatV;

    void write()
    {
        if (0 == fipList.ordinality())
            iFileIO->setSize(0);
        else
        {
            Owned<IFileIOStream> stream = createBufferedIOStream(iFileIO);
            stream->write(3, formatV); // 3 byte format definition, incase of change later
            ForEachItemIn(i, fipList)
            {
                writeStringToStream(*stream, fipList.item(i)->get());
                writeCharToStream(*stream, '\n');
            }
            offset_t pos = stream->tell();
            stream.clear();
            iFileIO->setSize(pos);
        }
    }
    void doDelete(const char *fip)
    {
        OwnedIFile iFile = createIFile(fip);
        try
        {
            iFile->remove();
        }
        catch (IException *e)
        {
            StringBuffer errStr("FileInProgressHandler, failed to remove: ");
            EXCLOG(e, errStr.append(fip).str());
            e->Release();
        }
    }

    void backup(const char *dir, IFile *iFile)
    {
        StringBuffer origName(iFile->queryFilename());
        StringBuffer bakName("fiplist_");
        CDateTime dt;
        dt.setNow();
        bakName.append((unsigned)dt.getSimple()).append("_").append((unsigned)GetCurrentProcessId()).append(".bak");
        iFileIO.clear(); // close old for rename
        iFile->rename(bakName.str());
        WARNLOG("Renamed to %s", bakName.str());
        OwnedIFile newIFile = createIFile(origName);
        iFileIO.setown(newIFile->open(IFOreadwrite)); // reopen
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFileInProgressHandler()
    {
        init();
    }
    ~CFileInProgressHandler()
    {
        deinit();
    }
    void deinit()
    {
        for (;;)
        {
            CStringAttr *item = fipList.dequeue();
            if (!item) break;
            doDelete(item->get());
            item->Release();
        }
        lookup.kill();
    }
    void init()
    {
        StringBuffer dir;
        globals->getProp("@thorPath", dir);
        StringBuffer path(dir);
        addPathSepChar(path);
        path.append("fiplist_");
        globals->getProp("@name", path);
        path.append("_");
        path.append(queryNodeGroup().rank(queryMyNode()));
        path.append(".lst");
        ensureDirectoryForFile(path.str());
        Owned<IFile> iFile = createIFile(path.str());
        iFileIO.setown(iFile->open(IFOreadwrite));
        if (!iFileIO)
        {
            PROGLOG("Failed to open/create backup file: %s", path.str());
            return;
        }
        MemoryBuffer mb;
        size32_t sz = read(iFileIO, 0, (size32_t)iFileIO->size(), mb);
        const char *mem = mb.toByteArray();
        if (mem)
        {
            if (sz<=3)
            {
                WARNLOG("Corrupt files-in-progress file detected: %s", path.str());
                backup(dir, iFile);
            }
            else
            {
                const char *endMem = mem+mb.length();
                mem += 3; // formatV header
                do
                {
                    const char *eol = strchr(mem, '\n');
                    if (!eol)
                    {
                        WARNLOG("Corrupt files-in-progress file detected: %s", path.str());
                        backup(dir, iFile);
                        break;
                    }
                    StringAttr fip(mem, eol-mem);
                    doDelete(fip);
                    mem = eol+1;
                }
                while (mem != endMem);
            }
        }
        write();
    }
    
// IFileInProgressHandler
    virtual void add(const char *fip)
    {
        CriticalBlock b(crit);
        CStringAttr *item = lookup.find(fip);
        assertex(!item);
        item = new CStringAttr(fip);
        fipList.enqueue(item);
        lookup.add(* item);
        write();
    }
    virtual void remove(const char *fip)
    {
        CriticalBlock b(crit);
        CStringAttr *item = lookup.find(fip);
        if (item)
        {
            lookup.removeExact(item);
            unsigned pos = fipList.find(item);
            fipList.dequeue(item);
            item->Release();
            write();
        }
    }
};
const char *CFileInProgressHandler::formatV = "01\n";



class CKeyedRemoteLookupReceiver : public CSimpleInterface, implements IThreaded
{
    class CJoinGroup;
    struct KeyLookupHeader
    {
        CJoinGroup *jg;
    };

    enum GroupFlags:unsigned { gf_null=0x0, gf_limitatmost=0x01, gf_limitabort=0x02, gf_eog=0x04, gf_head=0x08 };

    unsigned uniqueId = 0;
    CJobListener &jobs;
    CThreadedPersistent threaded;
    mptag_t keyLookupMpTag = TAG_NULL;
    bool aborted = false;
    CriticalSection availableProcessorCrit;
    bool getAvailableBlocked = false;
    Semaphore getAvailableSem;

    class CLookupKey
    {
    protected:
        unsigned hashv;

        void calcHash()
        {
            unsigned h = hashvalue(id, 0);
            hashv = hashc((const unsigned char *)&id, sizeof(unsigned), h);
        }
    public:
        activity_id id;
        StringAttr fname;

        CLookupKey(activity_id _id, const char *_fname) : id(_id), fname(_fname)
        {
            calcHash();
        }
        CLookupKey(MemoryBuffer &mb)
        {
            mb.read(id);
            mb.read(fname);
            calcHash();
        }
        CLookupKey(const CLookupKey &other)
        {
            id = other.id;
            fname.set(other.fname);
            hashv = other.hashv;
        }
        unsigned queryHash() const { return hashv; }
        bool operator==(CLookupKey const &other) const
        {
            return (id == other.id) && strsame(fname, other.fname);
        }
    };
    class CLookupContext : public CInterface
    {
        CKeyedRemoteLookupReceiver &owner;
        CLookupKey key;
        Owned<IHThorKeyedJoinArg> helper;
        Owned<IOutputMetaData> lookupRowOutputMetaData;
        Owned<IOutputRowDeserializer> lookupDeserializer;
        Owned<IOutputRowSerializer> fetchSerializer;
        Owned<IEngineRowAllocator> lookupAllocator, fetchAllocator;
        Owned<IKeyIndex> keyIndex;
        ICodeContext *codeCtx;
    public:
        CLookupContext(CKeyedRemoteLookupReceiver &_owner, activity_id _id,  const char *_fname, IHThorKeyedJoinArg *_helper, ICodeContext *_codeCtx)
            : owner(_owner), key(_id, _fname), helper(_helper), codeCtx(_codeCtx)
        {
            lookupRowOutputMetaData.setown(new CPrefixedOutputMeta(sizeof(KeyLookupHeader), helper->queryIndexReadInputRecordSize()));
            lookupDeserializer.setown(lookupRowOutputMetaData->createDiskDeserializer(codeCtx, key.id));
            lookupAllocator.setown(codeCtx->getRowAllocatorEx(lookupRowOutputMetaData, key.id, (roxiemem::RoxieHeapFlags)roxiemem::RHFpacked|roxiemem::RHFunique));
            fetchAllocator.setown(codeCtx->getRowAllocatorEx(helper->queryJoinFieldsRecordSize(), key.id, roxiemem::RHFnone));
            fetchSerializer.setown(helper->queryJoinFieldsRecordSize()->createDiskSerializer(codeCtx, key.id));

            unsigned crc = 0; // JCSMORE
            keyIndex.setown(createKeyIndex(key.fname, crc, false, false));
        }
        unsigned queryHash() const { return key.queryHash(); }
        const CLookupKey &queryKey() const { return key; }

        inline const char *queryFileName() const { return key.fname; }
        IOutputMetaData *queryOutputMetaData() const { return lookupRowOutputMetaData; }
        IEngineRowAllocator *queryLookupAllocator() const { return lookupAllocator; }
        IOutputRowDeserializer *queryLookupDeserializer() const { return lookupDeserializer; }
        IEngineRowAllocator *queryFetchAllocator() const { return fetchAllocator; }
        IOutputRowSerializer *queryFetchSerializer() const { return fetchSerializer; }
        IKeyManager *createKeyManager()
        {
            return createLocalKeyManager(helper->queryIndexRecordSize()->queryRecordAccessor(true), keyIndex, nullptr);
        }
        inline IHThorKeyedJoinArg *queryHelper() const { return helper; }
    };
    class CKMContainer : public CInterface
    {
        CKeyedRemoteLookupReceiver &owner;
        Linked<CLookupContext> ctx;
        Owned<IKeyManager> keyManager;
        unsigned handle = 0;
        unsigned abortLimit = 0;
        unsigned atMost = 0;
        bool fetchRequired = false;
    public:
        CKMContainer(CKeyedRemoteLookupReceiver &_owner, CLookupContext *_ctx)
            : owner(_owner), ctx(_ctx)
        {
            keyManager.setown(ctx->createKeyManager());
            handle = owner.getUniqId();
        }
        ~CKMContainer()
        {
            owner.freeLookupContext(ctx.getClear());
        }
        CLookupContext &queryCtx() const { return *ctx; }
        IKeyManager *queryKeyManager() const { return keyManager; }
        unsigned queryHandle() const { return handle; }
        const void *queryFindParam() const { return &handle; } // for SimpleHashTableOf
    };
    class CKMKeyEntry : public CInterface
    {
        CLookupKey key;
        CIArrayOf<CKMContainer> kmcs;
    public:
        CKMKeyEntry(const CLookupKey &_key, CKMContainer *_kmc) : key(_key)
        {
            push(_kmc);
        }
        inline CKMContainer *pop()
        {
            return &kmcs.popGet();
        }
        inline void push(CKMContainer *kmc)
        {
            kmcs.append(*kmc);
        }
        inline unsigned count() { return kmcs.ordinality(); }
        bool remove(CKMContainer *kmc)
        {
            return kmcs.zap(*kmc);
        }
        unsigned queryHash() const { return key.queryHash(); }
        const CLookupKey &queryKey() const { return key; }
    };
    class CLookupRequest : public CSimpleInterface
    {
        Linked<CLookupContext> ctx;
        Linked<CKMContainer> kmc;
        std::vector<const void *> rows;
        rank_t sender;
        mptag_t replyTag;
        bool replyAttempt = false;
    public:
        CLookupRequest(CLookupContext *_ctx, CKMContainer *_kmc, rank_t _sender, mptag_t _replyTag)
            : ctx(_ctx), kmc(_kmc), sender(_sender), replyTag(_replyTag)
        {
        }
        ~CLookupRequest()
        {
            for (auto &r : rows)
                ReleaseThorRow(r);
        }
        inline void addRow(const void *row)
        {
            rows.push_back(row);
        }
        inline const void *getRowClear(unsigned r)
        {
            const void *row = rows[r];
            rows[r] = nullptr;
            return row;
        }
        inline unsigned getRowCount() const { return rows.size(); }
        inline CLookupContext &queryCtx() const { return *ctx; }
        inline CKMContainer *getKeyManagerClear() { return kmc.getClear(); }
        void deserialize(size32_t sz, const void *_requestData)
        {
            MemoryBuffer requestData;
            fastLZDecompressToBuffer(requestData, _requestData);
            IEngineRowAllocator *allocator = ctx->queryLookupAllocator();
            IOutputRowDeserializer *deserializer = ctx->queryLookupDeserializer();
            unsigned count;
            requestData.read(count);
            size32_t rowDataSz = requestData.remaining();
            CThorStreamDeserializerSource d(rowDataSz, requestData.readDirect(rowDataSz));
            for (unsigned r=0; r<count; r++)
            {
                assertex(!d.eos());
                RtlDynamicRowBuilder rowBuilder(allocator);
                size32_t sz = deserializer->deserialize(rowBuilder, d);
                addRow(rowBuilder.finalizeRowClear(sz));
            }
        }
        void reply(MemoryBuffer &mb)
        {
            byte errorCode = 0;
            CMessageBuffer msg;
            msg.append(errorCode);
            fastLZCompressToBuffer(msg, mb.length(), mb.toByteArray());
            replyAttempt = true;
            if (!queryNodeComm().send(msg, sender, replyTag, LONGTIMEOUT))
                throw MakeStringException(0, "Failed to reply to lookup request");
        }
        void replyError(IException *e)
        {
            EXCLOG(e, "CLookupRequest");
            if (replyAttempt)
                return;
            byte errorCode = 1;
            CMessageBuffer msg;
            msg.append(errorCode);
            serializeException(e, msg);
            if (!queryNodeComm().send(msg, sender, replyTag, LONGTIMEOUT))
                throw MakeStringException(0, "Failed to reply to lookup request");
        }
    };
    class CLookupResult
    {
        CLookupContext &ctx;
        std::vector<const void *> rows;
        std::vector<unsigned __int64> fposs;
        GroupFlags groupFlag = gf_null;
        void clearRows()
        {
            for (auto &r : rows)
                ReleaseThorRow(r);
            rows.clear();
        }
    public:
        CLookupResult(CLookupContext &_ctx) : ctx(_ctx)
        {
        }
        ~CLookupResult()
        {
            clearRows();
        }
        void addRow(const void *row, offset_t fpos)
        {
            if (row)
                rows.push_back(row);
            fposs.push_back(fpos);
        }
        void clear()
        {
            groupFlag = gf_null;
            clearRows();
            fposs.clear();
        }
        inline unsigned getCount() const { return fposs.size(); }
        inline GroupFlags queryFlag() const { return groupFlag; }
        void setFlag(GroupFlags gf)
        {
            clear();
            groupFlag = gf;
        }
        void serialize(MemoryBuffer &mb) const
        {
            mb.append(groupFlag);
            if (gf_null != groupFlag)
                return;
            unsigned candidates = fposs.size();
            mb.append(candidates);
            if (candidates)
            {
                DelayedSizeMarker sizeMark(mb);
                IOutputRowSerializer *fetchSerializer = ctx.queryFetchSerializer();
                if (rows.size())
                {
                    CMemoryRowSerializer s(mb);
                    for (auto &row : rows)
                        fetchSerializer->serialize(s, (const byte *)row);
                    sizeMark.write();
                }
                mb.append(candidates * sizeof(unsigned __int64), &fposs[0]);
            }
        }
    };
    template <class ET>
    class CLookupHT : public SuperHashTableOf<ET, CLookupKey>
    {
        typedef SuperHashTableOf<ET, CLookupKey> PARENT;
    public:
        CLookupHT() : PARENT() { }
        ~CLookupHT() { PARENT::_releaseAll(); }

        inline ET *find(const CLookupKey &fp) const                                   \
        {
            return PARENT::find(&fp);
        }
        virtual void onAdd(void * et __attribute__((unused))) override { }
        virtual void onRemove(void * et __attribute__((unused))) override { }
        virtual unsigned getHashFromElement(const void *_et) const
        {
            const ET *et = static_cast<const ET *>(_et);
            return et->queryHash();
        }
        virtual unsigned getHashFromFindParam(const void *_fp) const override
        {
            const CLookupKey *fp = static_cast<const CLookupKey *>(_fp);
            return fp->queryHash();
        }
        virtual const void *getFindParam(const void *_et) const override
        {
            const ET *et = static_cast<const ET *>(_et);
            return &et->queryKey();
        }
        virtual bool matchesFindParam(const void *_et, const void *_fp, unsigned fphash __attribute__((unused))) const override
        {
            const ET *et = static_cast<const ET *>(_et);
            const CLookupKey *fp = static_cast<const CLookupKey *>(_fp);
            return et->queryKey() == *fp;
        }
    };
    template <class ET>
    class CLookupHTOwned : public CLookupHT<ET>
    {
        typedef CLookupHT<ET> PARENT;
    public:
        CLookupHTOwned() : PARENT() { }
        ~CLookupHTOwned() { PARENT::_releaseAll(); }
        virtual void onRemove(void *_et) override
        {
            ET *et = static_cast<ET *>(_et);
            et->Release();
        }
    };
    typedef CLookupHT<CLookupContext> CLookupContextHT;
    typedef CLookupHTOwned<CKMKeyEntry> CKMContextHT;

    class CKeyedRemoteLookupProcessor : public CSimpleInterfaceOf<IInterface>, implements IThreaded
    {
        CKeyedRemoteLookupReceiver &owner;
        CThreaded threaded;
        Semaphore sem;
        Owned<CLookupRequest> lookupRequest;
        IHThorKeyedJoinArg *helper = nullptr;
        IEngineRowAllocator *joinFieldsAllocator = nullptr;

        rowcount_t abortLimit = 0;
        rowcount_t atMost = 0;
        bool fetchRequired = false;

        bool abortSoon = false;

        const unsigned DEFAULT_KEYLOOKUP_MAXREPLYSZ = 0x100000;

        template <class HeaderStruct>
        void getHeaderFromRow(const void *row, HeaderStruct &header)
        {
            memcpy(&header, row, sizeof(HeaderStruct));
        }
        void processRow(const void *row, IKeyManager *keyManager, CLookupResult &reply)
        {
            KeyLookupHeader lookupKeyHeader;
            getHeaderFromRow(row, lookupKeyHeader);
            const void *keyedFieldsRow = (byte *)row + sizeof(KeyLookupHeader);

            helper->createSegmentMonitors(keyManager, keyedFieldsRow);
            keyManager->finishSegmentMonitors();
            keyManager->reset();

            unsigned candidates = 0;
            // NB: keepLimit is not on hard matches and can only be applied later, since other filtering (e.g. in transform) may keep below keepLimit
            while (keyManager->lookup(true))
            {
                ++candidates;
                if (candidates > abortLimit)
                {
                    reply.setFlag(gf_limitabort);
                    break;
                }
                else if (candidates > atMost) // atMost - filter out group if > max hard matches
                {
                    reply.setFlag(gf_limitatmost);
                    break;
                }
                KLBlobProviderAdapter adapter(keyManager);
                byte const * keyRow = keyManager->queryKeyBuffer();
                size_t fposOffset = keyManager->queryRowSize() - sizeof(offset_t);
                offset_t fpos = rtlReadBigUInt8(keyRow + fposOffset);
                if (helper->indexReadMatch(keyedFieldsRow, keyRow,  &adapter))
                {
                    if (fetchRequired)
                        reply.addRow(nullptr, fpos);
                    else
                    {
                        RtlDynamicRowBuilder joinFieldsRowBuilder(joinFieldsAllocator);
                        size32_t sz = helper->extractJoinFields(joinFieldsRowBuilder, keyRow, &adapter);
                        /* NB: Each row lookup could in theory == lots of keyed results. If needed to break into smaller replies
                         * Would have to create/keep a keyManager per sender, in those circumstances.
                         * As it stands, each lookup will be processed and all rows (below limits) will be returned, but I think that's okay.
                         * There are other reasons why might want a keyManager per sender, e.g. for concurrency.
                         */
                        reply.addRow(joinFieldsRowBuilder.finalizeRowClear(sz), fpos);
                    }
                }
            }
            keyManager->releaseSegmentMonitors();
        }
    public:
        CKeyedRemoteLookupProcessor(CKeyedRemoteLookupReceiver &_owner) : threaded("CKeyedRemoteLookupProcessor", this),
            owner(_owner)
        {
        }
        ~CKeyedRemoteLookupProcessor()
        {
            stop();
        }
        void start()
        {
            abortSoon = false;
            threaded.start();
        }
        void join()
        {
            threaded.join();
        }
        void stop()
        {
            abortSoon = true;
            // NB: if handler is middle of handling request will stop next time around
            sem.signal();
            join();
        }
        void handleRequest(CLookupRequest *_lookupRequest)
        {
            // NB: only here, because this processor is idle
            assertex(!lookupRequest);
            lookupRequest.set(_lookupRequest);
            sem.signal();
        }
    // IThreaded
        virtual void threadmain() override
        {
            while (true)
            {
                Owned<IException> exception;
                sem.wait();
                if (!lookupRequest) // stop
                    break;
                Owned<CLookupRequest> request = lookupRequest.getClear();
                try
                {
                    Owned<CKMContainer> kmc = request->getKeyManagerClear();
                    CLookupContext &lookupCtx = request->queryCtx();
                    helper = lookupCtx.queryHelper();
                    joinFieldsAllocator = lookupCtx.queryFetchAllocator();

                    atMost = helper->getJoinLimit();
                    if (atMost == 0)
                        atMost = (unsigned)-1;
                    abortLimit = helper->getMatchAbortLimit();
                    if (abortLimit == 0)
                        abortLimit = (unsigned)-1;
                    if (abortLimit < atMost)
                        atMost = abortLimit;
                    fetchRequired = helper->diskAccessRequired();

                    CLookupResult lookupResult(lookupCtx); // reply for 1 request row

                    MemoryBuffer replyMb;
                    replyMb.append(kmc->queryHandle());
                    DelayedMarker<unsigned> countMarker(replyMb);
                    unsigned rowCount = request->getRowCount();
                    unsigned rowNum = 0;
                    while (!abortSoon)
                    {
                        OwnedConstThorRow row = request->getRowClear(rowNum++);
                        processRow(row, kmc->queryKeyManager(), lookupResult);
                        lookupResult.serialize(replyMb);
                        bool last = rowNum == rowCount;
                        if (last || (replyMb.length() >= DEFAULT_KEYLOOKUP_MAXREPLYSZ))
                        {
                            countMarker.write(rowNum);
                            request->reply(replyMb);
                            if (last)
                                break;
                            replyMb.clear();
                        }
                        lookupResult.clear();
                    }
                    helper = nullptr;
                    owner.addToKeyManagerCache(&lookupCtx, kmc.getClear());
                    owner.addAvailableProcessor(*this);
                }
                catch (IException *e)
                {
                    exception.setown(e);
                }
                if (exception)
                    request->replyError(exception);
            }
        }
    };
    CLookupContextHT lookupContextsHT; // non-owning HT of CLookupContexts
    CKMContextHT cachedKMs; // NB: HT by {actId, fname} links IKeyManager's within containers
    OwningSimpleHashTableOf<CKMContainer, unsigned> cachedKMsByHandle; // HT by { handle }, links IKeyManager's within containers.
    CriticalSection kMCrit, lCCrit;

    IArrayOf<CKeyedRemoteLookupProcessor> processors;
    std::vector<CKeyedRemoteLookupProcessor *> availableProcessors;

    CLookupContext *ensureLookupContext(CJobBase &job, const CLookupKey &key)
    {
        CriticalBlock b(lCCrit);
        CLookupContext *lookupContext = lookupContextsHT.find(key);
        if (lookupContext)
            return LINK(lookupContext);
        lookupContext = createLookupContext(job, key);
        lookupContextsHT.replace(*lookupContext); // NB: does not link/take ownership
        return lookupContext;
    }
    void freeLookupContext(CLookupContext *_lookupContext)
    {
        Owned<CLookupContext> lookupContext = _lookupContext;
        if (!lookupContext->IsShared())
        {
            CriticalBlock b(lCCrit);
            lookupContextsHT.removeExact(lookupContext);
        }
    }
    CLookupContext *createLookupContext(CJobBase &job, const CLookupKey &key)
    {
        VStringBuffer helperName("fAc%u", (unsigned)key.id);
        EclHelperFactory helperFactory = (EclHelperFactory) job.queryDllEntry().getEntry(helperName.str());
        if (!helperFactory)
            throw makeOsExceptionV(GetLastError(), "Failed to load helper factory method: %s (dll handle = %p)", helperName.str(), job.queryDllEntry().getInstance());

        ICodeContext &codeCtx = job.queryJobChannel(0).querySharedMemCodeContext();
        Owned<IHThorKeyedJoinArg> helper = static_cast<IHThorKeyedJoinArg *>(helperFactory());
        return new CLookupContext(*this, key.id, key.fname, helper.getClear(), &codeCtx);
    }
    CKMContainer *getKeyManager(unsigned handle)
    {
        CriticalBlock b(kMCrit);
        Linked<CKMContainer> kmc = cachedKMsByHandle.find(handle);
        if (kmc)
        {
            verifyex(cachedKMsByHandle.removeExact(kmc));
            CKMKeyEntry *kme = cachedKMs.find(kmc->queryCtx().queryKey());
            assertex(kme);
            verifyex(kme->remove(kmc));
            if (0 == kme->count())
                verifyex(cachedKMs.removeExact(kme));
        }
        return kmc.getClear();
    }
    bool removeKeyManager(unsigned handle)
    {
        Owned<CKMContainer> kmc = getKeyManager(handle);
        return nullptr != kmc;
    }
    unsigned getUniqId()
    {
        ++uniqueId;
        return uniqueId;
    }
public:
    CKeyedRemoteLookupReceiver(CJobListener &_jobs, mptag_t _mpTag)
        : threaded("CKeyedRemoteLookupReceiver", this), jobs(_jobs), keyLookupMpTag(_mpTag)
    {
        unsigned keyLookupMaxProcessThreads = 10;
        for (unsigned i=0; i<keyLookupMaxProcessThreads; i++)
        {
            CKeyedRemoteLookupProcessor *processor = new CKeyedRemoteLookupProcessor(*this);
            processors.append(*processor);
            availableProcessors.push_back(processor);
        }
    }
    ~CKeyedRemoteLookupReceiver()
    {
        stop();
    }
    CKMContainer *getCachedKeyManager(const CLookupKey &key)
    {
        CriticalBlock b(kMCrit);
        CKMKeyEntry *kme = cachedKMs.find(key);
        if (!kme)
            return nullptr;
        CKMContainer *kmc = kme->pop();
        if (0 == kme->count())
            verifyex(cachedKMs.removeExact(kme));
        return kmc;
    }
    CKMContainer *ensureKeyManager(CLookupContext *lookupContext)
    {
        CKMContainer *kmc = getCachedKeyManager(lookupContext->queryKey());
        if (!kmc)
        {
            // NB: container links lookupContext, and will remove it from lookupContextsHT when last reference.
            // The container creates new IKeyManager and unique handle
            kmc = new CKMContainer(*this, lookupContext);
        }
        return kmc;
    }
    void addToKeyManagerCache(CLookupContext *lookupCtx, CKMContainer *kmc)
    {
        CriticalBlock b(kMCrit);
        CKMKeyEntry *kme = cachedKMs.find(lookupCtx->queryKey());
        if (kme)
            kme->push(kmc); // JCSMORE cap. to some max #
        else
        {
            kme = new CKMKeyEntry(lookupCtx->queryKey(), kmc);
            cachedKMs.replace(*kme);
        }
        cachedKMsByHandle.replace(*LINK(kmc));
    }
    void addAvailableProcessor(CKeyedRemoteLookupProcessor &processor)
    {
        CriticalBlock b(availableProcessorCrit);
        availableProcessors.push_back(&processor);
        if (getAvailableBlocked)
        {
            getAvailableBlocked = false;
            getAvailableSem.signal();
        }
    }
    CKeyedRemoteLookupProcessor *getAvailableProcessor()
    {
        while (!aborted)
        {
            {
                CriticalBlock b(availableProcessorCrit);
                if (0 == availableProcessors.size())
                {
                    if (aborted)
                        return nullptr;
                    getAvailableBlocked = true;
                }
                else
                {
                    CKeyedRemoteLookupProcessor *processor = availableProcessors.back();
                    availableProcessors.pop_back();
                    return processor;
                }
            }
            getAvailableSem.wait();
        }
        return nullptr;
    }
    void start()
    {
        aborted = false;
        ForEachItemIn(p, processors)
            processors.item(p).start(); // NB: waits for requests to handle
        threaded.start();
    }
    void stop()
    {
        if (aborted)
            return;
        queryNodeComm().cancel(RANK_ALL, keyLookupMpTag);
        while (!threaded.join(60000))
            PROGLOG("Receiver waiting on remote handlers to signal completion");
        if (aborted)
            return;
        aborted = true;
        ForEachItemIn(p, processors)
            processors.item(p).stop();
    }
    void abort()
    {
        if (aborted)
            return;
        aborted = true;
        queryNodeComm().cancel(RANK_ALL, keyLookupMpTag);
        ForEachItemIn(p, processors)
            processors.item(p).stop();

        {
            CriticalBlock b(availableProcessorCrit);
            if (getAvailableBlocked)
            {
                getAvailableBlocked = false;
                getAvailableSem.signal();
            }
        }
        threaded.join();
    }
    void processRequest(CMessageBuffer &msg, CKMContainer *kmc, rank_t sender, mptag_t replyTag)
    {
        CLookupContext *lookupContext = &kmc->queryCtx();
        Owned<CLookupRequest> lookupRequest = new CLookupRequest(lookupContext, kmc, sender, replyTag);

        size32_t requestSz;
        msg.read(requestSz);
        lookupRequest->deserialize(requestSz, msg.readDirect(requestSz));

        CKeyedRemoteLookupProcessor *processor = getAvailableProcessor(); // will block if all busy
        if (!processor) // only if aborting
            return;
        msg.clear();
        // NB: kmc is added to cache at end of request handling
        processor->handleRequest(lookupRequest); // links lookupRequest
    }
    virtual void threadmain() override
    {
        while (!aborted)
        {
            rank_t sender = RANK_NULL;
            CMessageBuffer msg;
            mptag_t replyTag = TAG_NULL;
            byte errorCode = 0;
            bool replyAttempt = false;
            try
            {
                if (!queryNodeComm().recv(msg, RANK_ALL, keyLookupMpTag, &sender))
                    break;
                if (!msg.length())
                    break;
                KJServiceCmds cmd;
                msg.read((byte &)cmd);
                msg.read((unsigned &)replyTag);
                switch (cmd)
                {
                    case kjs_open:
                    {
                        CJobSlave *job = jobs.findJob(nullptr);
                        if (!job)
                            throw MakeStringException(0, "No active job");

                        CLookupKey key(msg);

                        Owned<CLookupContext> lookupContext = ensureLookupContext(*job, key); // ensure entry in lookupContextsHT, will be removed by last CKMContainer
                        Owned<CKMContainer> kmc = ensureKeyManager(lookupContext);
                        processRequest(msg, kmc, sender, replyTag);
                        break;
                    }
                    case kjs_read:
                    {
                        CJobSlave *job = jobs.findJob(nullptr);
                        if (!job)
                            throw MakeStringException(0, "No active job");

                        CLookupKey key(msg);
                        unsigned handle;
                        msg.read(handle);
                        dbgassertex(handle);

                        Owned<CKMContainer> kmc = getKeyManager(handle);
                        if (!kmc)
                        {
                            Owned<CLookupContext> lookupContext = ensureLookupContext(*job, key);
                            kmc.setown(ensureKeyManager(lookupContext));
                        }
                        processRequest(msg, kmc, sender, replyTag);
                        break;
                    }
                    case kjs_close:
                    {
                        unsigned handle;
                        msg.read(handle);
                        bool res = removeKeyManager(handle);
                        msg.clear();
                        msg.append(errorCode);
                        msg.append(res);
                        replyAttempt = true;
                        if (!queryNodeComm().send(msg, sender, replyTag, LONGTIMEOUT))
                            throw MakeStringException(0, "kjs_close: Failed to reply to lookup request");
                        break;
                    }
                    case kjs_clear:
                    {
                        lookupContextsHT.kill();
                        break;
                    }
                }
            }
            catch (IException *e)
            {
                if (replyAttempt)
                    EXCLOG(e, "CKeyedRemoteLookupReceiver: failed to send reply");
                else if (TAG_NULL == replyTag)
                {
                    StringBuffer msg("CKeyedRemoteLookupReceiver: Exception without reply tag. Received from slave: ");
                    if (RANK_NULL==sender)
                        msg.append("<unknown>");
                    else
                        msg.append(sender-1);
                    EXCLOG(e, msg.str());
                    msg.clear();
                }
                else
                {
                    msg.clear();
                    errorCode = 1;
                    serializeException(e, msg);
                    msg.append(errorCode);
                }
                e->Release();
            }
            if (msg.length())
            {
                if (!queryNodeComm().send(msg, sender, replyTag, LONGTIMEOUT))
                {
                    ERRLOG("CKeyedRemoteLookupReceiver: Failed to send error response");
                    break;
                }
            }
        }
    }
};


class CThorResourceSlave : public CThorResourceBase
{
    Owned<IThorFileCache> fileCache;
    Owned<IBackup> backupHandler;
    Owned<IFileInProgressHandler> fipHandler;
    Owned<CKeyedRemoteLookupReceiver> lookupReceiver;

public:
    CThorResourceSlave(CJobListener &jobs)
    {
        backupHandler.setown(createBackupHandler());
        fileCache.setown(createFileCache(globals->getPropInt("@fileCacheLimit", 1800)));
        fipHandler.setown(new CFileInProgressHandler());
        lookupReceiver.setown(new CKeyedRemoteLookupReceiver(jobs, globalTags[1]));
        lookupReceiver->start();
    }
    ~CThorResourceSlave()
    {
        fileCache.clear();
        backupHandler.clear();
        fipHandler.clear();
        lookupReceiver->stop();
    }

// IThorResource
    virtual IThorFileCache &queryFileCache() { return *fileCache.get(); }
    virtual IBackup &queryBackup() { return *backupHandler.get(); }
    virtual IFileInProgressHandler &queryFileInProgressHandler() { return *fipHandler.get(); }
};

void slaveMain(bool &jobListenerStopped)
{
    unsigned masterMemMB = globals->getPropInt("@masterTotalMem");
    HardwareInfo hdwInfo;
    getHardwareInfo(hdwInfo);
    if (hdwInfo.totalMemory < masterMemMB)
        WARNLOG("Slave has less memory than master node");
    unsigned gmemSize = globals->getPropInt("@globalMemorySize");
    bool gmemAllowHugePages = globals->getPropBool("@heapUseHugePages", false);
    bool gmemAllowTransparentHugePages = globals->getPropBool("@heapUseTransparentHugePages", true);
    bool gmemRetainMemory = globals->getPropBool("@heapRetainMemory", false);

    if (gmemSize >= hdwInfo.totalMemory)
    {
        // should prob. error here
    }
    roxiemem::setTotalMemoryLimit(gmemAllowHugePages, gmemAllowTransparentHugePages, gmemRetainMemory, ((memsize_t)gmemSize) * 0x100000, 0, thorAllocSizes, NULL);

    CJobListener jobListener(jobListenerStopped);
    CThorResourceSlave slaveResource(jobListener);
    setIThorResource(slaveResource);

#ifdef __linux__
    bool useMirrorMount = globals->getPropBool("Debug/@useMirrorMount", false);

    if (useMirrorMount && queryNodeGroup().ordinality() > 2)
    {
        unsigned slaves = queryNodeGroup().ordinality()-1;
        rank_t next = queryNodeGroup().rank()%slaves;  // note 0 = master
        const IpAddress &ip = queryNodeGroup().queryNode(next+1).endpoint();
        StringBuffer ipStr;
        ip.getIpText(ipStr);
        PROGLOG("Redirecting local mount to %s", ipStr.str());
        const char * overrideReplicateDirectory = globals->queryProp("@thorReplicateDirectory");
        StringBuffer repdir;
        if (getConfigurationDirectory(globals->queryPropTree("Directories"),"mirror","thor",globals->queryProp("@name"),repdir))
            overrideReplicateDirectory = repdir.str();
        else
            overrideReplicateDirectory = "/d$";
        setLocalMountRedirect(ip, overrideReplicateDirectory, "/mnt/mirror");
    }

#endif

    jobListener.slaveMain();
}

void abortSlave()
{
    if (clusterInitialized())
        queryNodeComm().cancel(0, masterSlaveMpTag);
}

