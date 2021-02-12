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
#include "thirdparty.h"
#include <stdio.h>
#include <limits.h>
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"
#include "jstring.hpp"
#include "jmisc.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "environment.hpp"
#include "jio.hpp"
#include "daft.hpp"
#include "daftcfg.hpp"
#include "fterror.hpp"
#include "rmtfile.hpp"
#include "rmtspawn.hpp"
#include "daftprogress.hpp"
#include "dfurun.hpp"
#include "dfuwu.hpp"
#include "dfurepl.hpp"
#include "mplog.hpp"

#include "jprop.hpp"

#include "dfuerror.hpp"

static Owned<IPropertyTree> globals;
ILogMsgHandler * fileMsgHandler;
Owned<IDFUengine> engine;

#define DEFAULT_PERF_REPORT_DELAY 60

//============================================================================

void usage()
{
    printf("Usage:\n");
    printf("  dfuserver daliservers=<ip> queue=<q-name>          -- starts DFU Server\n\n");
    printf("  dfuserver daliservers=<ip> stop=1 queue=<q-name>   -- stops DFU Server\n\n");
}



static bool exitDFUserver()
{
    engine->abortListeners();
    return false;
}

#ifndef _CONTAINERIZED
inline void XF(IPropertyTree &pt,const char *p,IProperties &from,const char *fp)
{
    const char * v = from.queryProp(fp);
    if (v&&*v)
        pt.setProp(p,v);
}

IPropertyTree *readOldIni()
{
    IPropertyTree *ret = createPTree("DFUSERVER", ipt_caseInsensitive);
    ret->setProp("@name","mydfuserver");
    ret->addPropTree("SSH",createPTree("SSH", ipt_caseInsensitive));
    Owned<IProperties> props = createProperties("dfuserver.ini", true);
    if (props) {
        XF(*ret,"@name",*props,"name");
        XF(*ret,"@daliservers",*props,"daliservers");
        XF(*ret,"@enableSNMP",*props,"enableSNMP");
        XF(*ret,"@enableSysLog",*props,"enableSysLog");
        XF(*ret,"@queue",*props,"queue");
        XF(*ret,"@monitorqueue",*props,"monitorqueue");
        XF(*ret,"@monitorinterval",*props,"monitorinterval");
        XF(*ret,"@transferBufferSize",*props,"transferBufferSize");
        XF(*ret,"@replicatequeue",*props,"replicatequeue");
        XF(*ret,"@log_dir",*props,"log_dir");
    }
    return ret;
}
#endif

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
dfuserver:
  daliServers: dali
  enableSNMP: false
  enableSysLog: true
  name: dfuserver
  queue: dfuserver_queue
  monitorqueue: dfuserver_monitor_queue
  monitorinterval: 900
  transferBufferSize: 65536
)!!";

int main(int argc, const char *argv[])
{
    for (unsigned i=0;i<(unsigned)argc;i++) {
        if (streq(argv[i],"--daemon") || streq(argv[i],"-d")) {
            if (daemon(1,0) || write_pidfile(argv[++i])) {
                perror("Failed to daemonize");
                return EXIT_FAILURE;
            }
            break;
        }
    }
    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    NoQuickEditSection xxx;

    try
    {
        globals.setown(loadConfiguration(defaultYaml, argv, "dfuserver", "DFUSERVER", "dfuserver.xml", nullptr));
    }
    catch (IException * e)
    {
        UERRLOG(e);
        e->Release();
        return 1;
    }
    catch(...)
    {
        OERRLOG("Failed to load configuration");
        return 1;
    }

    StringBuffer daliServer;
    StringBuffer queue;
    if (!globals->getProp("@daliServers", daliServer))
        globals->getProp("@DALISERVERS", daliServer);
    if (0==daliServer.length())
    {
        PROGLOG("daliservers not specified in dfuserver.xml");
        return 1;
    }
    if (!globals->getProp("@QUEUE", queue))
        globals->getProp("@queue", queue);
    if (0==queue.length())
    {
        PROGLOG("queue not specified in dfuserver.xml");
        return 1;
    }

    Owned<IFile> sentinelFile;
    bool stop = globals->getPropInt("@stop", globals->getPropInt("@STOP",0))!=0;
    if (!stop) {
        sentinelFile.setown(createSentinelTarget());
        removeSentinelFile(sentinelFile);
    }
    const char* name = globals->queryProp("@name");
#ifdef _CONTAINERIZED
    setupContainerizedLogMsgHandler();
#else
    if (!stop)
    {
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(globals, "dfuserver");
        lf->setMaxDetail(1000);
        fileMsgHandler = lf->beginLogging();
    }
    StringBuffer ftslogdir;
    if (getConfigurationDirectory(globals->queryPropTree("Directories"),"log","ftslave",name,ftslogdir)) // NB instance deliberately dfuserver's
        setFtSlaveLogDir(ftslogdir.str());
    setRemoteSpawnSSH(
        globals->queryProp("SSH/@SSHidentityfile"),
        globals->queryProp("SSH/@SSHusername"),
        globals->queryProp("SSH/@SSHpassword"),
        globals->getPropInt("SSH/@SSHtimeout",0),
        globals->getPropInt("SSH/@SSHretries",3),
        "run_");
#endif
    bool enableSNMP = globals->getPropInt("@enableSNMP")!=0;
    CSDSServerStatus *serverstatus=NULL;
    Owned<IReplicateServer> replserver;
    try {
        Owned<IGroup> serverGroup = createIGroupRetry(daliServer.str(),DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_DfuServer, 0, NULL, NULL, stop?(1000*30):MP_WAIT_FOREVER);

        if(!stop)
        {
            if (globals->getPropBool("@enableSysLog",true))
                UseSysLogForOperatorMessages();

            serverstatus = new CSDSServerStatus("DFUserver");
            setDaliServixSocketCaching(true); // speeds up lixux operations

            startLogMsgParentReceiver();    // for auditing
            connectLogMsgManagerToDali();

            engine.setown(createDFUengine());
            engine->setDFUServerName(name);
            addAbortHandler(exitDFUserver);

            IPropertyTree * config = nullptr;
            installDefaultFileHooks(config);
        }
        const char *q = queue.str();
        for (;;) {
            StringBuffer subq;
            const char *comma = strchr(q,',');
            if (comma)
                subq.append(comma-q,q);
            else
                subq.append(q);
            if (stop) {
                stopDFUserver(subq.str());
            }
            else {
                StringBuffer mask;
                mask.appendf("Queue[@name=\"%s\"][1]",subq.str());
                IPropertyTree *t=serverstatus->queryProperties()->queryPropTree(mask.str());
                if (t)
                    t->setPropInt("@num",t->getPropInt("@num",0)+1);
                else {
                    t = createPTree();
                    t->setProp("@name",subq.str());
                    t->setPropInt("@num",1);
                    serverstatus->queryProperties()->addPropTree("Queue",t);
                }
                serverstatus->commitProperties();
                engine->setDefaultTransferBufferSize((size32_t)globals->getPropInt("@transferBufferSize"));
                engine->startListener(subq.str(),serverstatus);
            }
            if (!comma)
                break;
            q = comma+1;
            if (!*q)
                break;
        }
        if (globals->hasProp("@monitorqueue"))
            q = globals->queryProp("@monitorqueue");
        else
            q = globals->queryProp("@MONITORQUEUE");
        if (q&&*q) {
            if (stop) {
                stopDFUserver(q);
            }
            else {
                IPropertyTree *t=serverstatus->queryProperties()->addPropTree("MonitorQueue",createPTree());
                t->setProp("@name",q);
                int monitorInterval = globals->getPropInt("@monitorinterval", globals->getPropInt("@MONITORINTERVAL", 60));
                engine->startMonitor(q,serverstatus,monitorInterval*1000);
            }
        }
        if (globals->hasProp("@replicatequeue"))
            q = globals->queryProp("@replicatequeue");
        else
            q = globals->queryProp("@REPLICATEQUEUE");
        if (q&&*q) {
            if (stop) {
                // TBD?
            }
            else {
                replserver.setown(createReplicateServer(q));
                replserver->runServer();
            }
        }
        if (!stop) {
            serverstatus->commitProperties();

            writeSentinelFile(sentinelFile);

            enableForceRemoteReads(); // forces file reads to be remote reads if they match environment setting 'forceRemotePattern' pattern.

            engine->joinListeners();
            if (replserver.get())
                replserver->stopServer();
            LOG(MCprogress, unknownJob, "Exiting");
        }

    }
    catch(IException *e){
        EXCLOG(e, "DFU Server Exception: ");
        e->Release();
    }
    catch (const char *s) {
        OWARNLOG("DFU: %s",s);
    }

    delete serverstatus;
    if (stop)
        Sleep(2000);    // give time to stop
    engine.clear();
    globals.clear();
    closeEnvironment();
    closedownClientProcess();
    UseSysLogForOperatorMessages(false);
    setDaliServixSocketCaching(false);
    releaseAtoms();
    return 0;
}

