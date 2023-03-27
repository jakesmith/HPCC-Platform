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

#include <string>
#include "jfile.hpp"
#include "daclient.hpp"
#include "wujobq.hpp"
#include "jmisc.hpp"
#include "jlog.hpp"
#include "jfile.hpp"
#include "jutil.hpp"
#include "daqueue.hpp"
#include "workunit.hpp"
#include "environment.hpp"
#include "dafdesc.hpp"

class CEclAgentExecutionServer : public CInterfaceOf<IThreadFactory>, implements IAbortHandler
{
public:
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IThreadFactory>);

    CEclAgentExecutionServer(IPropertyTree *config);
    ~CEclAgentExecutionServer();

    int run();
    virtual IPooledThread *createNew() override;
    virtual bool onAbort() override;
private:
    bool executeWorkunit(const char * wuid);

    const char *agentName;
    const char *daliServers;
    const char *apptype;
    Owned<IJobQueue> queue;
    Linked<IPropertyTree> config;
    Owned<IThreadPool> pool; // for containerized only
    std::atomic<bool> running = { false };
};

//---------------------------------------------------------------------------------

CEclAgentExecutionServer::CEclAgentExecutionServer(IPropertyTree *_config) : config(_config)
{
    //Build logfile from component properties settings
#ifndef _CONTAINERIZED
    Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(config, "eclagent");
    lf->setCreateAliasFile(false);
    lf->beginLogging();
    PROGLOG("Logging to %s",lf->queryLogFileSpec());
#else
    setupContainerizedLogMsgHandler();
#endif

    agentName = config->queryProp("@name");
    assertex(agentName);

    daliServers = config->queryProp("@daliServers");
    assertex(daliServers);

    apptype = config->queryProp("@type");
    if (!apptype)
        apptype = "hthor";
    StatisticCreatorType ctype = SCThthor;
    if (strieq(apptype, "roxie"))
        ctype = SCTroxie;
    else if (strieq(apptype, "thor"))
        ctype = SCTthor;
    setStatisticsComponentName(ctype, agentName, true);

    if (isContainerized()) // JCS - the pool approach would also work in bare-metal if there was a configuration.
    {
        unsigned poolSize = config->getPropInt("@maxActive", 100);
        pool.setown(createThreadPool("agentPool", this, NULL, poolSize, INFINITE));
    }
}


CEclAgentExecutionServer::~CEclAgentExecutionServer()
{
    if (pool)
        pool->joinAll(false, INFINITE);

    if (queue)
        queue->cancelAcceptConversation();
}


//---------------------------------------------------------------------------------

int CEclAgentExecutionServer::run()
{
#ifdef _CONTAINERIZED
    StringBuffer queueNames;
#else
    SCMStringBuffer queueNames;
#endif
    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);
    try
    {
        Owned<IGroup> serverGroup = createIGroupRetry(daliServers, DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_AgentExec);
#ifdef _CONTAINERIZED
        if (streq("thor", apptype))
            getClusterThorQueueName(queueNames, agentName);
        else
            getClusterEclAgentQueueName(queueNames, agentName);
#else
        getAgentQueueNames(queueNames, agentName);
#endif
        queue.setown(createJobQueue(queueNames.str()));
        queue->connect(false);
    }
    catch (IException *e) 
    {
        EXCLOG(e, "Server queue create/connect: ");
        e->Release();
        return -1;
    }
    catch(...)
    {
        IERRLOG("Terminating unexpectedly");
    }

    CSDSServerStatus serverStatus("HThorServer");
    serverStatus.queryProperties()->setProp("@queue",queueNames.str());
    serverStatus.queryProperties()->setProp("@cluster", agentName);
    serverStatus.commitProperties();
    writeSentinelFile(sentinelFile);

    try 
    {
        running = true;
        LocalIAbortHandler abortHandler(*this);
        while (running)
        {
            if (pool)
            {
                if (!pool->waitAvailable(10000))
                {
                    if (config->getPropInt("@traceLevel", 0) > 2)
                        DBGLOG("Blocked for 10 seconds waiting for an available agent slot");
                    continue;
                }
            }

            PROGLOG("AgentExec: Waiting on queue(s) '%s'", queueNames.str());
            Owned<IJobQueueItem> item = queue->dequeue();
            if (item.get())
            {
                StringAttr wuid;
                wuid.set(item->queryWUID());
                PROGLOG("AgentExec: Dequeued workunit request '%s'", wuid.get());
                try
                {
                    executeWorkunit(wuid);
                }
                catch(IException *e)
                {
                    EXCLOG(e, "CEclAgentExecutionServer::run: ");
                    e->Release();
                }
                catch(...)
                {
                    IERRLOG("Unexpected exception in CEclAgentExecutionServer::run caught");
                }
            }
            else
            {
                removeSentinelFile(sentinelFile); // no reason to restart
                break;
            }
        }
        DBGLOG("Closing down");
    }

    catch (IException *e) 
    {
        EXCLOG(e, "Server Exception: ");
        e->Release();
        PROGLOG("Exiting");
    }

    try 
    {
        queue->disconnect();
    }
    catch (IException *e) 
    {
        EXCLOG(e, "Server queue disconnect: ");
        e->Release();
    }
    PROGLOG("Exiting agentexec");
    return 1;
}

//---------------------------------------------------------------------------------

// NB: WaitThread only used by Containerized
class WaitThread : public CInterfaceOf<IPooledThread>
{
public:
    WaitThread(const char *_dali, const char *_apptype, const char *_queue) : dali(_dali), apptype(_apptype), queue(_queue)
    {
    }
    virtual void init(void *param) override
    {
        auto context = *static_cast<std::tuple<unsigned, const char *, const char *, const char *> *>(param);
        std::tie(wfid, wuid, graphName, scopeName) = context;
    }
    virtual bool stop() override
    {
        return false;
    }
    virtual bool canReuse() const override
    {
        return false;
    }
    virtual void threadmain() override
    {
        Owned<IException> exception;
        try
        {
            StringAttr jobSpecName(apptype);
            StringAttr processName(apptype);

            bool isThorJob = streq("thor", apptype);
            if (isThorJob)
            {
                // JCSMORE - ideally apptype, image and executable name would all be same.
                jobSpecName.set("thormanager");
                processName.set("thormaster_lcr");
            }
            Owned<const IPropertyTree> compConfig = getComponentConfig();
            bool useChildProcesses = compConfig->getPropBool("@useChildProcesses");
            if (isContainerized() && !useChildProcesses)
            {
                std::list<std::pair<std::string, std::string>> params = { };
                if (compConfig->getPropBool("@useThorQueue", true))
                    params.push_back({ "queue", queue.get() });
                StringBuffer jobName(wuid);
                if (isThorJob)
                {
                    params.push_back({ "graphName", graphName.get() });
                    jobName.append('-').append(graphName);
                    params.push_back({ "scopeName", scopeName.str() });
                }

                {
                    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                    Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);

                    StatisticScopeType scopeType = getScopeType(scopeName);
                    unsigned __int64 timeStamp = getTimeStampNowValue();
                    workunit->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scopeName, StWhenK8sLaunched, nullptr, timeStamp, 1, 0, StatsMergeReplace);
                }

                runK8sJob(jobSpecName, wuid, jobName, params);
            }
            else
            {
                bool useValgrind = compConfig->getPropBool("@valgrind", false);
                VStringBuffer exec("%s%s --workunit=%s --daliServers=%s", useValgrind ? "valgrind " : "", processName.get(), wuid.str(), dali.str());
                if (compConfig->hasProp("@config"))
                {
                    exec.append(" --config=");
                    compConfig->getProp("@config", exec);
                }
                if (compConfig->getPropBool("@useThorQueue", true))
                    exec.append(" --queue=").append(queue);
                if (isThorJob)
                    exec.appendf(" --graphName=%s", graphName.get());
                Owned<IPipeProcess> pipe = createPipeProcess();
                pipe->setenv("SENTINEL", nullptr);
                if (!pipe->run(apptype.str(), exec.str(), ".", false, true, false, 0, false))
                    throw makeStringExceptionV(0, "Failed to run \"%s\"", exec.str());
                unsigned retCode = pipe->wait();
                if (retCode)
                    throw makeStringExceptionV(0, "Failed to run \"%s\": process exited with error: %u", exec.str(), retCode);
            }
        }
        catch (IException *e)
        {
            exception.setown(e);
        }
        if (exception)
        {
            EXCLOG(exception);
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
            if (workunit)
            {
                workunit->setState(WUStateFailed);
                StringBuffer eStr;
                addExceptionToWorkunit(workunit, SeverityError, "agentexec", exception->errorCode(), exception->errorMessage(eStr).str(), nullptr, 0, 0, 0);
                workunit->commit();
            }
        }
    }
private:
    unsigned wfid = 0;
    StringAttr wuid;
    StringAttr graphName;
    StringAttr scopeName;
    StringAttr dali;
    StringAttr apptype;
    StringAttr queue;
};

IPooledThread *CEclAgentExecutionServer::createNew()
{
    if (nullptr == pool)
        throwUnexpected();
    return new WaitThread(daliServers, apptype, agentName);
}

bool CEclAgentExecutionServer::onAbort()
{
    DBGLOG("Close down requested");
    running = false;
    if (queue)
        queue->cancelAcceptConversation();
    return false;
}

bool CEclAgentExecutionServer::executeWorkunit(const char * wuid)
{
    Owned<const IPropertyTree> compConfig = getComponentConfig();
    unsigned wfid = 0;
    StringArray sArray;
    StringBuffer scopeName; // if left blank then = SSTglobal
    std::tuple<unsigned, const char *, const char *, const char *> threadCtx;
    if (pool)
    {
        bool isThorJob = streq("thor", apptype);
        const char *graphName = nullptr;
        if (isThorJob)
        {
            // NB: In the case of handling apptype='thor', the queued items is of the form <wfid>/<wuid>/<graphName>
            // wfid and graphName not needed in other contexts
            sArray.appendList(wuid, "/");
            assertex(3 == sArray.ordinality());
            wfid = atoi(sArray.item(0));
            wuid = sArray.item(1);
            graphName = sArray.item(2);
            scopeName.setf("w%u:%s", wfid, graphName);
        }
        threadCtx = std::make_tuple(wfid, wuid, graphName, scopeName.str());
    }

    StatisticScopeType scopeType = getScopeType(scopeName);
    unsigned __int64 timeStamp = getTimeStampNowValue();

    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
        workunit->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scopeName, StWhenDequeued, nullptr, timeStamp, 1, 0, StatsMergeReplace);
    }

    if (pool)
    {
        pool->start((void *) &threadCtx);
        return true;
    }

    //build eclagent command line
    StringBuffer command;

#ifdef _WIN32
    command.append(".\\hthor.exe");
#else
    command.append("start_eclagent");
#endif

    StringBuffer cmdLine(command);
    cmdLine.append(" --workunit=").append(wuid).append(" --daliServers=").append(daliServers);

    DWORD runcode;
    PROGLOG("AgentExec: Executing '%s'", cmdLine.str());
#ifdef _WIN32
    bool success = invoke_program(cmdLine.str(), runcode, false, NULL, NULL);
#else
    //specify "wait" to eliminate linux zombies. Waits for the startup script to 
    //complete (not eclagent), because script starts eclagent in the background
    bool success = invoke_program(cmdLine.str(), runcode, true, NULL, NULL);
#endif
    if (success)
    { 
        if (runcode != 0)
            PROGLOG("Process failed during execution: %s error(%" I64F "i)", cmdLine.str(), (unsigned __int64) runcode);
        else
            PROGLOG("Execution started");
    }
    else
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
        if (workunit)
        {
            workunit->setState(WUStateFailed);
            workunit->commit();
        }
        PROGLOG("Process failed to start: %s", cmdLine.str());
    }

    return success && runcode == 0;
}

//---------------------------------------------------------------------------------

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
eclagent:
    name: myeclagent
    type: hthor
)!!";

int main(int argc, const char *argv[]) 
{ 
    if (!checkCreateDaemon(argc, argv))
        return EXIT_FAILURE;

    InitModuleObjects();

    Owned<IPropertyTree> config;
    try
    {
        config.setown(loadConfiguration(defaultYaml, argv, "eclagent", "AGENTEXEC", "agentexec.xml", nullptr));
    }
    catch (IException *e) 
    {
        EXCLOG(e, "Error processing config file\n");
        return 1;
    }

    int retcode = 0;
    try
    {
        CEclAgentExecutionServer server(config);
        server.run();
    } 
    catch (...)
    {
        printf("Unexpected error running agentexec server\r\n");
        retcode = 1;
    }

    closedownClientProcess();
    releaseAtoms();
    ExitModuleObjects();

    return retcode;
}
