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

// todo look at IRemoteFileServer stop


#include "platform.h"
#include "limits.h"

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jqueue.tpp"

#include "securesocket.hpp"
#include "portlist.h"
#include "jsocket.hpp"
#include "jencrypt.hpp"
#include "jlzw.hpp"
#include "jset.hpp"
#include "jhtree.hpp"

#include "dafscommon.hpp"
#include <atomic>
#include <string>
#include <unordered_map>

#include "rtldynfield.hpp"
#include "rtlds_imp.hpp"
#include "rtlread_imp.hpp"
#include "rtlrecord.hpp"
#include "eclhelper_dyn.hpp"

#include "rtlcommon.hpp"
#include "rtlformat.hpp"

#include "jflz.hpp"
#include "digisign.hpp"

#include "remote.hpp"

using namespace cryptohelper;


#define SOCKET_CACHE_MAX 500


#ifdef _DEBUG
//#define SIMULATE_PACKETLOSS 1
#endif

#define TREECOPYTIMEOUT   (60*60*1000)     // 1Hr (I guess could take longer for big file but at least will stagger)
#define TREECOPYPOLLTIME  (60*1000*5)      // for tracing that delayed
#define TREECOPYPRUNETIME (24*60*60*1000)  // 1 day

static const unsigned __int64 defaultFileStreamChooseNLimit = I64C(0x7fffffffffffffff); // constant should be move to common place (see eclhelper.hpp)
static const unsigned __int64 defaultFileStreamSkipN = 0;
static const unsigned defaultDaFSReplyLimitKB = 1024; // 1MB
enum OutputFormat:byte { outFmt_Binary, outFmt_Xml, outFmt_Json };


#if SIMULATE_PACKETLOSS

#define TESTING_FAILURE_RATE_LOST_SEND  10 // per 1000
#define TESTING_FAILURE_RATE_LOST_RECV  10 // per 1000
#define DUMMY_TIMEOUT_MAX (1000*10)

static bool errorSimulationOn = true;
static ISocket *timeoutreadsock = NULL; // used to trigger


struct dummyReadWrite
{
    class X
    {
        dummyReadWrite *parent;
    public:
        X(dummyReadWrite *_parent)
        {
            parent = _parent;
        }
        ~X()
        {
            delete parent;
        }
    };

    class TimeoutSocketException: public CInterface, public IJSOCK_Exception
    {
    public:
        IMPLEMENT_IINTERFACE;

        TimeoutSocketException()
        {
        }

        virtual ~TimeoutSocketException()
        {
        }

        int errorCode() const { return JSOCKERR_timeout_expired; }
        StringBuffer &  errorMessage(StringBuffer &str) const
        {
            return str.append("timeout expired");
        }
        MessageAudience errorAudience() const
        {
            return MSGAUD_user;
        }
    };


    ISocket *sock;

    dummyReadWrite(ISocket *_sock)
    {
        sock = _sock;
    }

    void readtms(void* buf, size32_t min_size, size32_t max_size, size32_t &size_read, time_t timeout)
    {
        X x(this);
        unsigned t = msTick();
        unsigned r = getRandom();
        bool timeoutread = (timeoutreadsock==sock);
        timeoutreadsock=NULL;
        if (!timeoutread)
            sock->readtms(buf, min_size, max_size, size_read, timeout);
        if (timeoutread||(errorSimulationOn&&(TESTING_FAILURE_RATE_LOST_RECV>0)&&(r%1000<TESTING_FAILURE_RATE_LOST_RECV))) {
            PrintStackReport();
            if (timeoutread)
                PROGLOG("** Simulate timeout");
            else
                PROGLOG("** Simulate Packet loss (size %d,%d)",min_size,max_size);
            if (timeout>DUMMY_TIMEOUT_MAX)
                timeout = DUMMY_TIMEOUT_MAX;
            t = msTick()-t;
            if (t<timeout)
                Sleep(timeout-t);
            IJSOCK_Exception *e = new TimeoutSocketException;
            throw e;
        }
    }
    size32_t write(void const* buf, size32_t size)
    {
        X x(this);
        timeoutreadsock=NULL;
        unsigned r = getRandom();
        if (errorSimulationOn&&(TESTING_FAILURE_RATE_LOST_SEND>0)&&(r%1000<TESTING_FAILURE_RATE_LOST_SEND)) {
            PrintStackReport();
            PROGLOG("** Simulate Packet loss (size %d)",size);
            timeoutreadsock=sock;
            return size;
        }
        return sock->write(buf,size);
    }
};



#define SOCKWRITE(sock) (new dummyReadWrite(sock))->write
#define SOCKREADTMS(sock) (new dummyReadWrite(sock))->readtms
#else
#define SOCKWRITE(sock) sock->write
#define SOCKREADTMS(sock) sock->readtms
#endif

// backward compatible modes
typedef enum { compatIFSHnone, compatIFSHread, compatIFSHwrite, compatIFSHexec, compatIFSHall} compatIFSHmode;

static const char *VERSTRING= "DS V2.5"       // dont forget FILESRV_VERSION in header
#ifdef _WIN32
"Windows ";
#else
"Linux ";
#endif

static unsigned maxConnectTime = 0;
static unsigned maxReceiveTime = 0;

//Security and default port attributes
static class _securitySettings
{
public:
    DAFSConnectCfg  connectMethod;
    unsigned short  daFileSrvPort;
    unsigned short  daFileSrvSSLPort;
    const char *    certificate;
    const char *    privateKey;
    const char *    passPhrase;

    _securitySettings()
    {
        queryDafsSecSettings(&connectMethod, &daFileSrvPort, &daFileSrvSSLPort, &certificate, &privateKey, &passPhrase);
    }
} securitySettings;


static CriticalSection              secureContextCrit;
static Owned<ISecureSocketContext>  secureContextServer;
static Owned<ISecureSocketContext>  secureContextClient;

#ifdef _USE_OPENSSL
static ISecureSocket *createSecureSocket(ISocket *sock, SecureSocketType type)
{
    {
        CriticalBlock b(secureContextCrit);
        if (type == ServerSocket)
        {
            if (!secureContextServer)
                secureContextServer.setown(createSecureSocketContextEx(securitySettings.certificate, securitySettings.privateKey, securitySettings.passPhrase, type));
        }
        else if (!secureContextClient)
            secureContextClient.setown(createSecureSocketContext(type));
    }
    int loglevel = SSLogNormal;
#ifdef _DEBUG
    loglevel = SSLogMax;
#endif
    if (type == ServerSocket)
        return secureContextServer->createSecureSocket(sock, loglevel);
    else
        return secureContextClient->createSecureSocket(sock, loglevel);
}
#else
static ISecureSocket *createSecureSocket(ISocket *sock, SecureSocketType type)
{
    throwUnexpected();
}
#endif

void clientSetRemoteFileTimeouts(unsigned maxconnecttime,unsigned maxreadtime)
{
    maxConnectTime = maxconnecttime;
    maxReceiveTime = maxreadtime;
}


struct sRFTM
{
    CTimeMon *timemon;
    sRFTM(unsigned limit) {  timemon = limit ? new CTimeMon(limit) : NULL; }
    ~sRFTM() { delete timemon; }
};


const char *remoteServerVersionString() { return VERSTRING; }

static bool AuthenticationEnabled = true;

bool enableDafsAuthentication(bool on)
{
    bool ret = AuthenticationEnabled;
    AuthenticationEnabled = on;
    return ret;
}

bool queryDafsAuthentication()
{
    return AuthenticationEnabled;
}


#define CLIENT_TIMEOUT      (1000*60*60*12)     // long timeout in case zombies
#define CLIENT_INACTIVEWARNING_TIMEOUT (1000*60*60*12) // time between logging inactive clients
#define SERVER_TIMEOUT      (1000*60*5)         // timeout when waiting for dafilesrv to reply after command
                                                // (increased when waiting for large block)
#define DAFS_CONNECT_FAIL_RETRY_TIME (1000*60*15)
#ifdef SIMULATE_PACKETLOSS
#define NORMAL_RETRIES      (1)
#define LENGTHY_RETRIES     (1)
#else
#define NORMAL_RETRIES      (3)
#define LENGTHY_RETRIES     (12)
#endif

#ifdef _DEBUG
static byte traceFlags=0x30;
#else
static byte traceFlags=0x20;
#endif

#define TF_TRACE (traceFlags&1)
#define TF_TRACE_PRE_IO (traceFlags&2)
#define TF_TRACE_FULL (traceFlags&4)
#define TF_TRACE_CLIENT_CONN (traceFlags&8)
#define TF_TRACE_TREE_COPY (traceFlags&0x10)
#define TF_TRACE_CLIENT_STATS (traceFlags&0x20)


static const unsigned RFEnoerror = 0;

// used by testsocket only
RemoteFileCommandType queryRemoteStreamCmd()
{
    return RFCStreamReadTestSocket;
}


#define RFCText(cmd) #cmd

const char *RFCStrings[] =
{
    RFCText(RFCopenIO),
    RFCText(RFCcloseIO),
    RFCText(RFCread),
    RFCText(RFCwrite),
    RFCText(RFCsize),
    RFCText(RFCexists),
    RFCText(RFCremove),
    RFCText(RFCrename),
    RFCText(RFCgetver),
    RFCText(RFCisfile),
    RFCText(RFCisdirectory),
    RFCText(RFCisreadonly),
    RFCText(RFCsetreadonly),
    RFCText(RFCgettime),
    RFCText(RFCsettime),
    RFCText(RFCcreatedir),
    RFCText(RFCgetdir),
    RFCText(RFCstop),
    RFCText(RFCexec),
    RFCText(RFCdummy1),
    RFCText(RFCredeploy),
    RFCText(RFCgetcrc),
    RFCText(RFCmove),
    RFCText(RFCsetsize),
    RFCText(RFCextractblobelements),
    RFCText(RFCcopy),
    RFCText(RFCappend),
    RFCText(RFCmonitordir),
    RFCText(RFCsettrace),
    RFCText(RFCgetinfo),
    RFCText(RFCfirewall),
    RFCText(RFCunlock),
    RFCText(RFCunlockreply),
    RFCText(RFCinvalid),
    RFCText(RFCcopysection),
    RFCText(RFCtreecopy),
    RFCText(RFCtreecopytmp),
    RFCText(RFCsetthrottle), // legacy version
    RFCText(RFCsetthrottle2),
    RFCText(RFCsetfileperms),
    RFCText(RFCreadfilteredindex),
    RFCText(RFCreadfilteredcount),
    RFCText(RFCreadfilteredblob),
    RFCText(RFCStreamRead),
    RFCText(RFCStreamReadTestSocket),
    RFCText(RFCStreamGeneral),
};

const char *getRFCText(RemoteFileCommandType cmd)
{
    if (cmd==RFCStreamReadJSON)
        return "RFCStreamReadJSON";
    else
    {
        unsigned elems = sizeof(RFCStrings) / sizeof(RFCStrings[0]);
        if (cmd >= elems)
            return "RFCunknown";
        return RFCStrings[cmd];
    }
}

const char *getRFSERRText(unsigned err)
{
    switch (err)
    {
        case RFSERR_InvalidCommand:
            return "RFSERR_InvalidCommand";
        case RFSERR_NullFileIOHandle:
            return "RFSERR_NullFileIOHandle";
        case RFSERR_InvalidFileIOHandle:
            return "RFSERR_InvalidFileIOHandle";
        case RFSERR_TimeoutFileIOHandle:
            return "RFSERR_TimeoutFileIOHandle";
        case RFSERR_OpenFailed:
            return "RFSERR_OpenFailed";
        case RFSERR_ReadFailed:
            return "RFSERR_ReadFailed";
        case RFSERR_WriteFailed:
            return "RFSERR_WriteFailed";
        case RFSERR_RenameFailed:
            return "RFSERR_RenameFailed";
        case RFSERR_ExistsFailed:
            return "RFSERR_ExistsFailed";
        case RFSERR_RemoveFailed:
            return "RFSERR_RemoveFailed";
        case RFSERR_CloseFailed:
            return "RFSERR_CloseFailed";
        case RFSERR_IsFileFailed:
            return "RFSERR_IsFileFailed";
        case RFSERR_IsDirectoryFailed:
            return "RFSERR_IsDirectoryFailed";
        case RFSERR_IsReadOnlyFailed:
            return "RFSERR_IsReadOnlyFailed";
        case RFSERR_SetReadOnlyFailed:
            return "RFSERR_SetReadOnlyFailed";
        case RFSERR_GetTimeFailed:
            return "RFSERR_GetTimeFailed";
        case RFSERR_SetTimeFailed:
            return "RFSERR_SetTimeFailed";
        case RFSERR_CreateDirFailed:
            return "RFSERR_CreateDirFailed";
        case RFSERR_GetDirFailed:
            return "RFSERR_GetDirFailed";
        case RFSERR_GetCrcFailed:
            return "RFSERR_GetCrcFailed";
        case RFSERR_MoveFailed:
            return "RFSERR_MoveFailed";
        case RFSERR_ExtractBlobElementsFailed:
            return "RFSERR_ExtractBlobElementsFailed";
        case RFSERR_CopyFailed:
            return "RFSERR_CopyFailed";
        case RFSERR_AppendFailed:
            return "RFSERR_AppendFailed";
        case RFSERR_AuthenticateFailed:
            return "RFSERR_AuthenticateFailed";
        case RFSERR_CopySectionFailed:
            return "RFSERR_CopySectionFailed";
        case RFSERR_TreeCopyFailed:
            return "RFSERR_TreeCopyFailed";
        case RAERR_InvalidUsernamePassword:
            return "RAERR_InvalidUsernamePassword";
        case RFSERR_MasterSeemsToHaveDied:
            return "RFSERR_MasterSeemsToHaveDied";
        case RFSERR_TimeoutWaitSlave:
            return "RFSERR_TimeoutWaitSlave";
        case RFSERR_TimeoutWaitConnect:
            return "RFSERR_TimeoutWaitConnect";
        case RFSERR_TimeoutWaitMaster:
            return "RFSERR_TimeoutWaitMaster";
        case RFSERR_NoConnectSlave:
            return "RFSERR_NoConnectSlave";
        case RFSERR_NoConnectSlaveXY:
            return "RFSERR_NoConnectSlaveXY";
        case RFSERR_VersionMismatch:
            return "RFSERR_VersionMismatch";
        case RFSERR_SetThrottleFailed:
            return "RFSERR_SetThrottleFailed";
        case RFSERR_MaxQueueRequests:
            return "RFSERR_MaxQueueRequests";
        case RFSERR_KeyIndexFailed:
            return "RFSERR_MaxQueueRequests";
        case RFSERR_StreamReadFailed:
            return "RFSERR_StreamReadFailed";
        case RFSERR_InternalError:
            return "Internal Error";
    }
    return "RFSERR_Unknown";
}

unsigned mapDafilesrvixCodes(unsigned err)
{
    // old Solaris dali/remote/daliservix.cpp uses
    // different values for these error codes.
    switch (err)
    {
        case 8200:
            return RFSERR_InvalidCommand;
        case 8201:
            return RFSERR_NullFileIOHandle;
        case 8202:
            return RFSERR_InvalidFileIOHandle;
        case 8203:
            return RFSERR_TimeoutFileIOHandle;
        case 8204:
            return RFSERR_OpenFailed;
        case 8205:
            return RFSERR_ReadFailed;
        case 8206:
            return RFSERR_WriteFailed;
        case 8207:
            return RFSERR_RenameFailed;
        case 8208:
            return RFSERR_SetReadOnlyFailed;
        case 8209:
            return RFSERR_GetDirFailed;
        case 8210:
            return RFSERR_MoveFailed;
    }
    return err;
}

#define ThrottleText(throttleClass) #throttleClass
const char *ThrottleStrings[] =
{
    ThrottleText(ThrottleStd),
    ThrottleText(ThrottleSlow),
};

// very high upper limits that configure can't exceed
#define THROTTLE_MAX_LIMIT 1000000
#define THROTTLE_MAX_DELAYMS 3600000
#define THROTTLE_MAX_CPUTHRESHOLD 100
#define THROTTLE_MAX_QUEUELIMIT 10000000

const char *getThrottleClassText(ThrottleClass throttleClass) { return ThrottleStrings[throttleClass]; }

typedef enum { ACScontinue, ACSdone, ACSerror} AsyncCommandStatus;



typedef byte OnceKey[16];

static void genOnce(OnceKey &key)
{
    static __int64 inc=0;
    *(unsigned *)&key[0] = getRandom();
    *(__int64 *)&key[4] = ++inc;
    *(unsigned *)&key[12] = getRandom();
}

static void mergeOnce(OnceKey &key,size32_t sz,const void *data)
{
    assertex(sz<=sizeof(OnceKey));
    const byte *p = (const byte *)data;
    while (sz)
        key[--sz] ^= *(p++);
}

//---------------------------------------------------------------------------

class DECL_EXCEPTION CDafsException: public IDAFS_Exception, public CInterface
{
    int     errcode;
    StringAttr msg;
public:
    IMPLEMENT_IINTERFACE;

    CDafsException(int code,const char *_msg)
        : errcode(code), msg(_msg)
    {
    };

    int errorCode() const
    {
        return errcode;
    }

    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        return str.append(msg);
    }
    MessageAudience errorAudience() const
    {
        return MSGAUD_user;
    }
};

IDAFS_Exception *createDafsException(int code, const char *msg)
{
    return new CDafsException(code, msg);
}

IDAFS_Exception *createDafsExceptionVA(int code, const char *format, va_list args) __attribute__((format(printf,2,0)));

IDAFS_Exception *createDafsExceptionVA(int code, const char *format, va_list args)
{
    StringBuffer eStr;
    eStr.limited_valist_appendf(1024, format, args);
    return new CDafsException(code, eStr);
}

IDAFS_Exception *createDafsExceptionV(int code, const char *format, ...) __attribute__((format(printf,2,3)));
IDAFS_Exception *createDafsExceptionV(int code, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    IDAFS_Exception *ret = createDafsExceptionVA(code, format, args);
    va_end(args);
    return ret;
}

void setDafsEndpointPort(SocketEndpoint &ep)
{
    // odd kludge (don't do this at home)
    byte ipb[4];
    if (ep.getNetAddress(sizeof(ipb),&ipb)==sizeof(ipb)) {
        if ((ipb[0]==255)&&(ipb[1]==255)) {
            ep.port = (((unsigned)ipb[2])<<8)+ipb[3];
            ep.ipset(queryLocalIP());
        }
    }
    if (ep.port==0)
    {
        if ( (securitySettings.connectMethod == SSLNone) || (securitySettings.connectMethod == UnsecureFirst) )
            ep.port = securitySettings.daFileSrvPort;
        else
            ep.port = securitySettings.daFileSrvSSLPort;
    }
}


MemoryBuffer & initSendBuffer(MemoryBuffer & buff)
{
    buff.setEndian(__BIG_ENDIAN);       // transfer as big endian...
    buff.append((unsigned)0);           // reserve space for length prefix
    return buff;
}

inline void sendBuffer(ISocket * socket, MemoryBuffer & src, bool testSocketFlag=false)
{
    unsigned length = src.length() - sizeof(unsigned);
    byte * buffer = (byte *)src.toByteArray();
    if (TF_TRACE_FULL)
        PROGLOG("sendBuffer size %d, data = %d %d %d %d",length, (int)buffer[4],(int)buffer[5],(int)buffer[6],(int)buffer[7]);
    if (testSocketFlag)
        length |= 0x80000000;
    _WINCPYREV(buffer, &length, sizeof(unsigned));
    SOCKWRITE(socket)(buffer, src.length());
}

inline size32_t receiveBufferSize(ISocket * socket, unsigned numtries=NORMAL_RETRIES,CTimeMon *timemon=NULL)
{
    unsigned timeout = SERVER_TIMEOUT;
    if (numtries==0) {
        numtries = 1;
        timeout = 10*1000;  // 10s
    }
    while (numtries--) {
        try {
            if (timemon) {
                unsigned remaining;
                if (timemon->timedout(&remaining)||(remaining<10))
                    remaining = 10;
                if (remaining<timeout)
                    timeout = remaining;
            }
            size32_t szread;
            size32_t gotLength;
            SOCKREADTMS(socket)(&gotLength, sizeof(gotLength), sizeof(gotLength), szread, timeout);
            _WINREV(gotLength);
            if (TF_TRACE_FULL)
                PROGLOG("receiveBufferSized %d",gotLength);
            return gotLength;
        }
        catch (IJSOCK_Exception *e) {
            if ((numtries==0)||(e->errorCode()!=JSOCKERR_timeout_expired)||(timemon&&timemon->timedout())) {
                throw;
            }
            StringBuffer err;
            char peername[256];
            socket->peer_name(peername,sizeof(peername)-1);
            WARNLOG("Remote connection %s: %s",peername,e->errorMessage(err).str()); // why no peername
            e->Release();
            Sleep(500+getRandom()%1000); // ~1s
        }
    }
    return 0;
}

static void flush(ISocket *socket)
{
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetver);
    sendbuf.append((unsigned)RFCgetver);
    MemoryBuffer reply;
    size32_t totread=0;
    try
    {
        sendBuffer(socket, sendbuf);
        char buf[1024];
        for (;;)
        {
            Sleep(1000);    // breathe
            size32_t szread;
            SOCKREADTMS(socket)(buf, 1, sizeof(buf), szread, 1000*60);
            totread += szread;
        }
    }
    catch (IJSOCK_Exception *e) {
        if (totread)
            PROGLOG("%d bytes discarded",totread);
        if (e->errorCode()!=JSOCKERR_timeout_expired)
            EXCLOG(e,"flush");
        e->Release();
    }
}

inline void receiveBuffer(ISocket * socket, MemoryBuffer & tgt, unsigned numtries=1, size32_t maxsz=0x7fffffff)
    // maxsz is a guess at a resonable upper max to catch where protocol error
{
    sRFTM tm(maxReceiveTime);
    size32_t gotLength = receiveBufferSize(socket, numtries,tm.timemon);
    if (gotLength) {
        size32_t origlen = tgt.length();
        try {
            if (gotLength>maxsz) {
                StringBuffer msg;
                msg.appendf("receiveBuffer maximum block size exceeded %d/%d",gotLength,maxsz);
                PrintStackReport();
                throw createDafsException(DAFSERR_protocol_failure,msg.str());
            }
            unsigned timeout = SERVER_TIMEOUT*(numtries?numtries:1);
            if (tm.timemon) {
                unsigned remaining;
                if (tm.timemon->timedout(&remaining)||(remaining<10))
                    remaining = 10;
                if (remaining<timeout)
                    timeout = remaining;
            }
            size32_t szread;
            SOCKREADTMS(socket)((gotLength<4000)?tgt.reserve(gotLength):tgt.reserveTruncate(gotLength), gotLength, gotLength, szread, timeout);
        }
        catch (IJSOCK_Exception *e) {
            if (e->errorCode()!=JSOCKERR_timeout_expired) {
                EXCLOG(e,"receiveBuffer(1)");
                PrintStackReport();
                if (!tm.timemon||!tm.timemon->timedout())
                    flush(socket);
            }
            else {
                EXCLOG(e,"receiveBuffer");
                PrintStackReport();
            }
            tgt.setLength(origlen);
            throw;
        }
        catch (IException *e) {
            EXCLOG(e,"receiveBuffer(2)");
            PrintStackReport();
            if (!tm.timemon||!tm.timemon->timedout())
                flush(socket);
            tgt.setLength(origlen);
            throw;
        }

    }
    tgt.setEndian(__BIG_ENDIAN);
}


struct CConnectionRec
{
    SocketEndpoint ep;
    unsigned tick;
    IArrayOf<ISocket> socks;            // relies on isShared
};

//---------------------------------------------------------------------------
// Local mount redirect

struct CLocalMountRec: public CInterface
{
    IpAddress ip;
    StringAttr dir;             // dir path on remote ip
    StringAttr local;           // local dir path
};

static CIArrayOf<CLocalMountRec> localMounts;
static CriticalSection           localMountCrit;

void setDafsLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir)
{
    CriticalBlock block(localMountCrit);
    ForEachItemInRev(i,localMounts) {
        CLocalMountRec &mount = localMounts.item(i);
        if (dir==NULL) { // remove all matching mount
            if (!mountdir)
                return;
            if (strcmp(mount.local,mountdir)==0)
                localMounts.remove(i);
        }
        else if (mount.ip.ipequals(ip)&&(strcmp(mount.dir,dir)==0)) {
            if (mountdir) {
                mount.local.set(mountdir);
                return;
            }
            else
                localMounts.remove(i);
        }
    }
    if (dir&&mountdir) {
        CLocalMountRec &mount = *new CLocalMountRec;
        mount.ip.ipset(ip);
        mount.dir.set(dir);
        mount.local.set(mountdir);
        localMounts.append(mount);
    }
}

IFile *createFileLocalMount(const IpAddress &ip, const char * filename)
{
    CriticalBlock block(localMountCrit);
    ForEachItemInRev(i,localMounts) {
        CLocalMountRec &mount = localMounts.item(i);
        if (mount.ip.ipequals(ip)) {
            size32_t bl = mount.dir.length();
            if (isPathSepChar(mount.dir[bl-1]))
                bl--;
            if ((memcmp((void *)filename,(void *)mount.dir.get(),bl)==0)&&(isPathSepChar(filename[bl])||!filename[bl])) { // match
                StringBuffer locpath(mount.local);
                if (filename[bl])
                    addPathSepChar(locpath).append(filename+bl+1);
                locpath.replace((PATHSEPCHAR=='\\')?'/':'\\',PATHSEPCHAR);
                return createIFile(locpath.str());
            }
        }
    }
    return NULL;
}


//---------------------------------------------------------------------------


static class CConnectionTable: public SuperHashTableOf<CConnectionRec,SocketEndpoint>
{

    void onAdd(void *) {}

    void onRemove(void *e)
    {
        CConnectionRec *r=(CConnectionRec *)e;
        delete r;
    }

    unsigned getHashFromElement(const void *e) const
    {
        const CConnectionRec &elem=*(const CConnectionRec *)e;
        return elem.ep.hash(0);
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }

    const void * getFindParam(const void *p) const
    {
        const CConnectionRec &elem=*(const CConnectionRec *)p;
        return (void *)&elem.ep;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((CConnectionRec *)et)->ep.equals(*(SocketEndpoint *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CConnectionRec,SocketEndpoint);

    unsigned numsockets;

public:
    static CriticalSection crit;

    CConnectionTable()
    {
        numsockets = 0;
    }
    ~CConnectionTable() {
        _releaseAll();
    }

    ISocket *lookup(const SocketEndpoint &ep)
    {
        // always called from crit block
        CConnectionRec *r = SuperHashTableOf<CConnectionRec,SocketEndpoint>::find(&ep);
        if (r) {
            ForEachItemIn(i,r->socks) {
                ISocket *s = &r->socks.item(i);
                if (!QUERYINTERFACE(s, CInterface)->IsShared()) {
                    r->tick = msTick();
                    s->Link();
                    return s;
                }
            }
        }
        return NULL;
    }

    void addLink(SocketEndpoint &ep,ISocket *sock)
    {
        // always called from crit block
        while (numsockets>=SOCKET_CACHE_MAX) {
            // find oldest
            CConnectionRec *c = NULL;
            unsigned oldest = 0;
            CConnectionRec *old = NULL;
            unsigned oldi;
            unsigned now = msTick();
            for (;;) {
                c = (CConnectionRec *)SuperHashTableOf<CConnectionRec,SocketEndpoint>::next(c);
                if (!c)
                    break;
                ForEachItemIn(i,c->socks) {
                    ISocket *s = &c->socks.item(i);
                    if (!QUERYINTERFACE(s, CInterface)->IsShared()) { // candidate to remove
                        unsigned t = now-c->tick;
                        if (t>oldest) {
                            oldest = t;
                            old = c;
                            oldi = i;
                        }
                    }
                }
            }
            if (!old)
                return;
            old->socks.remove(oldi);
            numsockets--;
        }
        CConnectionRec *r = SuperHashTableOf<CConnectionRec,SocketEndpoint>::find(&ep);
        if (!r) {
            r = new CConnectionRec;
            r->ep = ep;
            SuperHashTableOf<CConnectionRec,SocketEndpoint>::add(*r);
        }
        sock->Link();
        r->socks.append(*sock);
        numsockets++;
        r->tick = msTick();
    }

    void remove(SocketEndpoint &ep,ISocket *sock)
    {
        // always called from crit block
        CConnectionRec *r = SuperHashTableOf<CConnectionRec,SocketEndpoint>::find(&ep);
        if (r)
            if (r->socks.zap(*sock)&&numsockets)
                numsockets--;

    }


} *ConnectionTable = NULL;


CriticalSection CConnectionTable::crit;

void clientSetDaliServixSocketCaching(bool on)
{
    CriticalBlock block(CConnectionTable::crit);
    if (on) {
        if (!ConnectionTable)
            ConnectionTable = new CConnectionTable;
    }
    else {
        delete ConnectionTable;
        ConnectionTable = NULL;
    }
}

//---------------------------------------------------------------------------

// TreeCopy

#define TREECOPY_CACHE_SIZE 50

struct CTreeCopyItem: public CInterface
{
    StringAttr net;
    StringAttr mask;
    offset_t sz;                // original size
    CDateTime dt;               // original date
    RemoteFilenameArray loc;    // locations for file - 0 is original
    Owned<IBitSet> busy;
    unsigned lastused;

    CTreeCopyItem(RemoteFilename &orig, const char *_net, const char *_mask, offset_t _sz, CDateTime &_dt)
        : net(_net), mask(_mask)
    {
        loc.append(orig);
        dt.set(_dt);
        sz = _sz;
        busy.setown(createThreadSafeBitSet());
        lastused = msTick();
    }
    bool equals(const RemoteFilename &orig, const char *_net, const char *_mask, offset_t _sz, CDateTime &_dt)
    {
        if (!orig.equals(loc.item(0)))
            return false;
        if (strcmp(_net,net)!=0)
            return false;
        if (strcmp(_mask,mask)!=0)
            return false;
        if (sz!=_sz)
            return false;
        return (dt.equals(_dt,false));
    }
};

static CIArrayOf<CTreeCopyItem>  treeCopyArray;
static CriticalSection           treeCopyCrit;
static unsigned                  treeCopyWaiting=0;
static Semaphore                 treeCopySem;

#define DEBUGSAMEIP false

static void cleanupSocket(ISocket *sock)
{
    if (!sock)
        return;
    try
    {
        sock->shutdown();
    }
    catch (IException *e)
    {
        e->Release();
    }
    try
    {
        sock->close();
    }
    catch (IException *e)
    {
        e->Release();
    }
}

//---------------------------------------------------------------------------

void CRemoteBase::connectSocket(SocketEndpoint &ep, unsigned localConnectTime, unsigned localRetries)
{
    unsigned retries = 3;

    if (localConnectTime)
    {
        if (localRetries)
            retries = localRetries;
        if (localConnectTime > maxConnectTime)
            localConnectTime = maxConnectTime;
    }
    else
        localConnectTime = maxConnectTime;

    sRFTM tm(localConnectTime);

    // called in CConnectionTable::crit

    if (ep.equals(lastfailep)) {
        if (msTick()-lastfailtime<DAFS_CONNECT_FAIL_RETRY_TIME) {
            StringBuffer msg("Failed to connect (host marked down) to dafilesrv/daliservix on ");
            ep.getUrlStr(msg);
            throw createDafsException(DAFSERR_connection_failed,msg.str());
        }
        lastfailep.set(NULL);
        retries = 1;    // on probation
    }
    while(retries--) {
        CriticalUnblock unblock(CConnectionTable::crit); // allow others to connect
        StringBuffer eps;
        if (TF_TRACE_CLIENT_CONN) {
            ep.getUrlStr(eps);
            if (ep.port == securitySettings.daFileSrvSSLPort)
                PROGLOG("Connecting SECURE to %s", eps.str());
            else
                PROGLOG("Connecting to %s", eps.str());
            //PrintStackReport();
        }
        bool ok = true;
        try {
            if (tm.timemon) {
                unsigned remaining;
                if (tm.timemon->timedout(&remaining))
                    throwJSocketException(JSOCKERR_connection_failed);
                socket.setown(ISocket::connect_timeout(ep,remaining));
            }
            else
                socket.setown(ISocket::connect(ep));
            if (ep.port == securitySettings.daFileSrvSSLPort)
            {
#ifdef _USE_OPENSSL
                Owned<ISecureSocket> ssock;
                try
                {
                    ssock.setown(createSecureSocket(socket.getClear(), ClientSocket));
                    int status = ssock->secure_connect();
                    if (status < 0)
                        throw createDafsException(DAFSERR_connection_failed, "Failure to establish secure connection");
                    socket.setown(ssock.getLink());
                }
                catch (IException *e)
                {
                    cleanupSocket(ssock);
                    ssock.clear();
                    cleanupSocket(socket);
                    socket.clear();
                    StringBuffer eMsg;
                    e->errorMessage(eMsg);
                    e->Release();
                    throw createDafsException(DAFSERR_connection_failed, eMsg.str());
                }
#else
                throw createDafsException(DAFSERR_connection_failed,"Failure to establish secure connection: OpenSSL disabled in build");
#endif
            }
        }
        catch (IJSOCK_Exception *e) {
            ok = false;
            if (!retries||(tm.timemon&&tm.timemon->timedout())) {
                if (e->errorCode()==JSOCKERR_connection_failed) {
                    lastfailep.set(ep);
                    lastfailtime = msTick();
                    e->Release();
                    StringBuffer msg("Failed to connect (setting host down) to dafilesrv/daliservix on ");
                    ep.getUrlStr(msg);
                    throw createDafsException(DAFSERR_connection_failed,msg.str());
                }
                throw;
            }
            StringBuffer err;
            WARNLOG("Remote file connect %s",e->errorMessage(err).str());
            e->Release();
        }
        if (ok) {
            if (TF_TRACE_CLIENT_CONN) {
                PROGLOG("Connected to %s",eps.str());
            }
            if (AuthenticationEnabled) {
                try {
                    sendAuthentication(ep); // this will log error
                    break;
                }
                catch (IJSOCK_Exception *e) {
                    StringBuffer err;
                    WARNLOG("Remote file authenticate %s for %s ",e->errorMessage(err).str(),ep.getUrlStr(eps.clear()).str());
                    e->Release();
                    if (!retries)
                        break; // MCK - is this a warning or an error ? If an error, should we close and throw here ?
                }
            }
            else
                break;
        }
        bool timeExpired = false;
        unsigned sleeptime = getRandom()%3000+1000;
        if (tm.timemon)
        {
            unsigned remaining;
            if (tm.timemon->timedout(&remaining))
                timeExpired = true;
            else
            {
                if (remaining/2<sleeptime)
                    sleeptime = remaining/2;
            }
        }
        if (!timeExpired)
        {
            Sleep(sleeptime);       // prevent multiple retries beating
            if (ep.port == securitySettings.daFileSrvSSLPort)
                PROGLOG("Retrying SECURE connect");
            else
                PROGLOG("Retrying connect");
        }
    }
    if (ConnectionTable)
        ConnectionTable->addLink(ep,socket);
}

void CRemoteBase::killSocket(SocketEndpoint &tep)
{
    CriticalBlock block2(CConnectionTable::crit); // this is nested with crit
    if (socket) {
        try {
            Owned<ISocket> s = socket.getClear();
            if (ConnectionTable)
                ConnectionTable->remove(tep,s);
        }
        catch (IJSOCK_Exception *e) {
            e->Release();   // ignore errors closing
        }
        Sleep(getRandom()%1000*5+500);      // prevent multiple beating
    }
}

void CRemoteBase::sendRemoteCommand(MemoryBuffer & src, MemoryBuffer & reply, bool retry, bool lengthy, bool handleErrCode)
{
    CriticalBlock block(crit);  // serialize commands on same file
    SocketEndpoint tep(ep);
    setDafsEndpointPort(tep);
    unsigned nretries = retry?3:0;
    Owned<IJSOCK_Exception> firstexc;   // when retrying return first error if fails
    for (;;)
    {
        try
        {
            if (socket)
            {
                sendBuffer(socket, src);
                receiveBuffer(socket, reply, lengthy?LENGTHY_RETRIES:NORMAL_RETRIES);
                break;
            }
        }
        catch (IJSOCK_Exception *e)
        {
            if (!nretries--)
            {
                if (firstexc)
                {
                    e->Release();
                    e = firstexc.getClear();
                }
                killSocket(tep);
                throw e;
            }
            StringBuffer str;
            e->errorMessage(str);
            WARNLOG("Remote File: %s, retrying (%d)",str.str(),nretries);
            if (firstexc)
                e->Release();
            else
                firstexc.setown(e);
            killSocket(tep);
        }
        CriticalBlock block2(CConnectionTable::crit); // this is nested with crit
        if (ConnectionTable)
        {
            socket.setown(ConnectionTable->lookup(tep));
            if (socket)
            {
                // validate existing socket by sending an 'exists' command with short time out
                // (use exists for backward compatibility)
                bool ok = false;
                try
                {
                    MemoryBuffer sendbuf;
                    initSendBuffer(sendbuf);
                    MemoryBuffer replybuf;
                    sendbuf.append((RemoteFileCommandType)RFCexists).append(filename);
                    sendBuffer(socket, sendbuf);
                    receiveBuffer(socket, replybuf, 0, 1024);
                    ok = true;
                }
                catch (IException *e) {
                    e->Release();
                }
                if (!ok)
                    killSocket(tep);
            }
        }

        if (!socket)
        {
            bool doConnect = true;
            if (connectMethod == SSLFirst || connectMethod == UnsecureFirst)
            {
                // MCK - could maintain a list of 100 or so previous endpoints and if connection failed
                // then mark port down for a delay (like 15 min above) to avoid having to try every time ...
                try
                {
                    connectSocket(tep, 5000, 1);
                    doConnect = false;
                }
                catch (IDAFS_Exception *e)
                {
                    if (e->errorCode() == DAFSERR_connection_failed)
                    {
                        unsigned prevPort = tep.port;
                        if (prevPort == securitySettings.daFileSrvSSLPort)
                            tep.port = securitySettings.daFileSrvPort;
                        else
                            tep.port = securitySettings.daFileSrvSSLPort;
                        WARNLOG("Connect failed on port %d, retrying on port %d", prevPort, tep.port);
                        doConnect = true;
                        e->Release();
                    }
                    else
                        throw e;
                }
            }
            if (doConnect)
                connectSocket(tep);
        }
    }

    if (!handleErrCode)
        return;
    unsigned errCode;
    reply.read(errCode);
    if (errCode)
    {
        // old Solaris daliservix.cpp error code conversion
        if ( (errCode >= 8200) && (errCode <= 8210) )
            errCode = mapDafilesrvixCodes(errCode);
        StringBuffer msg;
        if (filename.get())
            msg.append(filename);
        ep.getUrlStr(msg.append('[')).append("] ");
        size32_t pos = reply.getPos();
        if (pos<reply.length())
        {
            size32_t len = reply.length()-pos;
            const byte *rest = reply.readDirect(len);
            if (errCode==RFSERR_InvalidCommand)
            {
                const char *s = (const char *)rest;
                const char *e = (const char *)rest+len;
                while (*s&&(s!=e))
                    s++;
                msg.append(s-(const char *)rest,(const char *)rest);
            }
            else if (len&&(rest[len-1]==0))
                msg.append((const char *)rest);
            else
            {
                msg.appendf("extra data[%d]",len);
                for (unsigned i=0;(i<16)&&(i<len);i++)
                    msg.appendf(" %2x",(int)rest[i]);
            }
        }
        // NB: could append getRFSERRText for all error codes
        else if (errCode == RFSERR_GetDirFailed)
            msg.append(RFSERR_GetDirFailed_Text);
        else
            msg.append("ERROR #").append(errCode);
#ifdef _DEBUG
        ERRLOG("%s",msg.str());
        PrintStackReport();
#endif
        throw createDafsException(errCode,msg.str());
    }
}

void CRemoteBase::sendRemoteCommand(MemoryBuffer & src, bool retry)
{
    MemoryBuffer reply;
    sendRemoteCommand(src, reply, retry);
}

void CRemoteBase::throwUnauthenticated(const IpAddress &ip,const char *user,unsigned err)
{
    if (err==0)
        err = RFSERR_AuthenticateFailed;
    StringBuffer msg;
    msg.appendf("Authentication for %s on ",user);
    ip.getIpText(msg);
    msg.append(" failed");
    throw createDafsException(err, msg.str());
}

void CRemoteBase::sendAuthentication(const IpAddress &serverip)
{
    // send my sig
    // first send my sig which if stream unencrypted will get returned as a bad command
    OnceKey oncekey;
    genOnce(oncekey);
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    MemoryBuffer replybuf;
    MemoryBuffer encbuf; // because aesEncrypt clears input
    sendbuf.append((RemoteFileCommandType)RFCunlock).append(sizeof(oncekey),&oncekey);
    try
    {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
    }
    catch (IException *e)
    {
        EXCLOG(e,"Remote file - sendAuthentication(1)");
        throw;
    }
    unsigned errCode;
    replybuf.read(errCode);
    if (errCode!=0)  // no authentication required
        return;
    SocketEndpoint ep;
    ep.setLocalHost(0);
    byte ipdata[16];
    size32_t ipds = ep.getNetAddress(sizeof(ipdata),&ipdata);
    mergeOnce(oncekey,ipds,&ipdata);
    StringBuffer username;
    StringBuffer password;
    IPasswordProvider * pp = queryPasswordProvider();
    if (pp)
        pp->getPassword(serverip, username, password);
    if (!username.length())
        username.append("sds_system");      // default account (note if exists should have restricted access!)
    if (!password.length())
        password.append("sds_man");
    if (replybuf.remaining()<=sizeof(size32_t))
        throwUnauthenticated(serverip,username.str());
    size32_t bs;
    replybuf.read(bs);
    if (replybuf.remaining()<bs)
        throwUnauthenticated(serverip,username.str());
    MemoryBuffer skeybuf;
    aesDecrypt(&oncekey,sizeof(oncekey),replybuf.readDirect(bs),bs,skeybuf);
    if (skeybuf.remaining()<sizeof(OnceKey))
        throwUnauthenticated(serverip,username.str());
    OnceKey sokey;
    skeybuf.read(sizeof(OnceKey),&sokey);
    // now we have the key to use to send user/password
    MemoryBuffer tosend;
    tosend.append((byte)2).append(username).append(password);
    initSendBuffer(sendbuf.clear());
    sendbuf.append((RemoteFileCommandType)RFCunlockreply);
    aesEncrypt(&sokey, sizeof(oncekey), tosend.toByteArray(), tosend.length(), encbuf);
    sendbuf.append(encbuf.length());
    sendbuf.append(encbuf);
    try
    {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf.clear(), NORMAL_RETRIES, 1024);
    }
    catch (IException *e)
    {
        EXCLOG(e,"Remote file - sendAuthentication(2)");
        throw;
    }
    replybuf.read(errCode);
    if (errCode==0)  // suceeded!
        return;
    throwUnauthenticated(serverip,username.str(),errCode);
}

CRemoteBase::CRemoteBase(const SocketEndpoint &_ep, const char * _filename) : filename(_filename)
{
    ep = _ep;
    connectMethod = securitySettings.connectMethod;
}

CRemoteBase::CRemoteBase()
{
}

void CRemoteBase::disconnect()
{
    CriticalBlock block(crit);
    CriticalBlock block2(CConnectionTable::crit); // this shouldn't ever block
    if (socket)
    {
        ISocket *s = socket.getClear();
        if (ConnectionTable)
        {
            SocketEndpoint tep(ep);
            setDafsEndpointPort(tep);
            ConnectionTable->remove(tep,s);
        }
        ::Release(s);
    }
}

const char *CRemoteBase::queryLocalName()
{
    return filename;
}

void CRemoteBase::send(MemoryBuffer &msg, MemoryBuffer &reply)
{
    sendRemoteCommand(msg, reply);
}

void CRemoteBase::close(RemoteFileIOHandle handle)
{
    if (handle)
    {
        try
        {
            MemoryBuffer sendBuffer;
            initSendBuffer(sendBuffer);
            sendBuffer.append((RemoteFileCommandType)RFCcloseIO).append(handle);
            sendRemoteCommand(sendBuffer,false);
        }
        catch (IDAFS_Exception *e)
        {
            if ((e->errorCode()!=RFSERR_InvalidFileIOHandle)&&(e->errorCode()!=RFSERR_NullFileIOHandle))
                throw;
            e->Release();
        }
        handle = 0;
    }
}

unsigned CRemoteBase::getRemoteVersion(StringBuffer &ver)
{
    unsigned ret;
    MemoryBuffer sendBuffer;
    initSendBuffer(sendBuffer);
    sendBuffer.append((RemoteFileCommandType)RFCgetver);
    sendBuffer.append((unsigned)RFCgetver);
    MemoryBuffer replyBuffer;
    try
    {
        sendRemoteCommand(sendBuffer, replyBuffer, true, false, false);
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
        return 0;
    }
    unsigned errCode;
    replyBuffer.read(errCode);
    if (errCode==RFSERR_InvalidCommand)
    {
        ver.append("DS V1.0");
        return 10;
    }
    else if (errCode==0)
        ret = 11;
    else if (errCode<0x10000)
        return 0;
    else
        ret = errCode-0x10000;

    StringAttr vers;
    replyBuffer.read(vers);
    ver.append(vers);
    return ret;
}


SocketEndpoint  CRemoteBase::lastfailep;
unsigned CRemoteBase::lastfailtime;


//---------------------------------------------------------------------------

class CRemoteDirectoryIterator : implements IRemoteDirectoryDifferenceIterator, public CInterface
{
    Owned<IFile>    cur;
    bool            curvalid;
    bool            curisdir;
    StringAttr      curname;
    CDateTime       curdt;
    __int64         cursize;
    StringAttr      dir;
    SocketEndpoint  ep;
    byte            *flags;
    unsigned        numflags;
    unsigned        curidx;
    unsigned        mask;

    MemoryBuffer buf;
public:
    static CriticalSection      crit;

    CRemoteDirectoryIterator(SocketEndpoint &_ep,const char *_dir)
        : dir(_dir)
    {
        // an extended difference iterator starts with 2 (for bwd compatibility)
        ep = _ep;
        curisdir = false;
        curvalid = false;
        cursize = 0;
        curidx = (unsigned)-1;
        mask = 0;
        numflags = 0;
        flags = NULL;
    }

    virtual bool appendBuf(MemoryBuffer &_buf) override
    {
        buf.setSwapEndian(_buf.needSwapEndian());
        byte hdr;
        _buf.read(hdr);
        if (hdr==2) {
            _buf.read(numflags);
            flags = (byte *)malloc(numflags);
            _buf.read(numflags,flags);
        }
        else {
            buf.append(hdr);
            flags = NULL;
            numflags = 0;
        }
        size32_t rest = _buf.length()-_buf.getPos();
        const byte *rb = (const byte *)_buf.readDirect(rest);
        bool ret = true;
        // At the last byte of the rb (rb[rest-1]) is the stream live flag
        //  True if the stream has more data
        //  False at the end of stream
        // The previous byte (rb[rest-2]) is the flag to signal there are more
        // valid entries in this block
        //  True if there are valid directory entry follows this flag
        //  False if there are no more valid entry in this block aka end of block
        // If there is more data in the stream, the end of block flag should be removed
        if (rest&&(rb[rest-1]!=0))
        {
            rest--; // remove stream live flag
            if(rest && (0 == rb[rest-1]))
            	rest--; //Remove end of block flag
            ret = false;  // continuation
        }
        buf.append(rest,rb);
        return ret;
    }

    ~CRemoteDirectoryIterator()
    {
        free(flags);
    }

    IMPLEMENT_IINTERFACE

    bool first()
    {
        curidx = (unsigned)-1;
        buf.reset();
        return next();
    }
    bool next()
    {
        for (;;) {
            curidx++;
            cur.clear();
            curdt.clear();
            curname.clear();
            cursize = 0;
            curisdir = false;
            if (buf.getPos()>=buf.length())
                return false;
            byte isValidEntry;
            buf.read(isValidEntry);
            curvalid = isValidEntry!=0;
            if (!curvalid)
                return false;
            buf.read(curisdir);
            buf.read(cursize);
            curdt.deserialize(buf);
            buf.read(curname);
            // kludge for bug in old linux jlibs
            if (strchr(curname,'\\')&&(getPathSepChar(dir)=='/')) {
                StringBuffer temp(curname);
                temp.replace('\\','/');
                curname.set(temp.str());
            }
            if ((mask==0)||(getFlags()&mask))
                break;
        }
        return true;
    }

    bool isValid()
    {
        return curvalid;
    }
    IFile & query()
    {
        if (!cur) {
            StringBuffer full(dir);
            addPathSepChar(full).append(curname);
            if (ep.isNull())
                cur.setown(createIFile(full.str()));
            else {
                RemoteFilename rfn;
                rfn.setPath(ep,full.str());
                cur.setown(createIFile(rfn));
            }
        }
        return *cur;
    }
    StringBuffer &getName(StringBuffer &buf)
    {
        return buf.append(curname);
    }
    bool isDir()
    {
        return curisdir;
    }

    __int64 getFileSize()
    {
        if (curisdir)
            return -1;
        return cursize;
    }


    bool getModifiedTime(CDateTime &ret)
    {
        ret = curdt;
        return true;
    }

    void setMask(unsigned _mask)
    {
        mask = _mask;
    }

    virtual unsigned getFlags()
    {
        if (flags&&(curidx<numflags))
            return flags[curidx];
        return 0;
    }

    static bool serialize(MemoryBuffer &mb,IDirectoryIterator *iter, size32_t bufsize, bool first)
    {
        bool ret = true;
        byte b=1;
        StringBuffer tmp;
        if (first ? iter->first() : iter->next()) {
            for (;;) {
                mb.append(b);
                bool isdir = iter->isDir();
                __int64 sz = isdir?0:iter->getFileSize();
                CDateTime dt;
                iter->getModifiedTime(dt);
                iter->getName(tmp.clear());
                mb.append(isdir).append(sz);
                dt.serialize(mb);
                mb.append(tmp.str());
                if (bufsize&&(mb.length()>=bufsize-1)) {
                    ret = false;
                    break;
                }
                if (!iter->next())
                    break;
            }
        }
        b = 0;
        mb.append(b);
        return ret;
    }

    static void serializeDiff(MemoryBuffer &mb,IDirectoryDifferenceIterator *iter)
    {
        // bit slow
        MemoryBuffer flags;
        ForEach(*iter)
            flags.append((byte)iter->getFlags());
        if (flags.length()) {
            byte b = 2;
            mb.append(b).append((unsigned)flags.length()).append(flags);
        }
        serialize(mb,iter,0,true);
    }

    void serialize(MemoryBuffer &mb,bool isdiff)
    {
        byte b;
        if (isdiff&&numflags&&flags) {
            b = 2;
            mb.append(b).append(numflags).append(numflags,flags);
        }
        serialize(mb,this,0,true);
    }

};

IRemoteDirectoryDifferenceIterator *createRemoteDirectorIterator(SocketEndpoint &ep, const char *dir)
{
    return new CRemoteDirectoryIterator(ep, dir);
}

bool serializeRemoteDirectory(MemoryBuffer &mb, IDirectoryIterator *iter, size32_t bufsize, bool first)
{
    return CRemoteDirectoryIterator::serialize(mb, iter, bufsize, first);
}

void serializeRemoteDirectoryDiff(MemoryBuffer &mb, IDirectoryDifferenceIterator *iter)
{
    CRemoteDirectoryIterator::serializeDiff(mb, iter);
}


class CCritTable;
class CEndpointCS : public CriticalSection, public CInterface
{
    CCritTable &table;
    const SocketEndpoint ep;
public:
    CEndpointCS(CCritTable &_table, const SocketEndpoint &_ep) : table(_table), ep(_ep) { }
    const void *queryFindParam() const { return &ep; }

    virtual void beforeDispose();
};

class CCritTable : private SimpleHashTableOf<CEndpointCS, const SocketEndpoint>
{
    typedef SimpleHashTableOf<CEndpointCS, const SocketEndpoint> PARENT;
    CriticalSection crit;
public:
    CEndpointCS *getCrit(const SocketEndpoint &ep)
    {
        CriticalBlock b(crit);
        Linked<CEndpointCS> clientCrit = find(ep);
        if (!clientCrit || !clientCrit->isAlive()) // if !isAlive(), then it is in the process of being destroyed/removed.
        {
            clientCrit.setown(new CEndpointCS(*this, ep));
            replace(*clientCrit); // NB table doesn't own
        }
        return clientCrit.getClear();
    }
    unsigned getHashFromElement(const void *e) const
    {
        const CEndpointCS &elem=*(const CEndpointCS *)e;
        return getHashFromFindParam(elem.queryFindParam());
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }

    void removeExact(CEndpointCS *clientCrit)
    {
        CriticalBlock b(crit);
        PARENT::removeExact(clientCrit); // NB may not exist, could have been replaced if detected !isAlive() in getCrit()
    }
} *dirCSTable;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    dirCSTable = new CCritTable;
    return true;
}
MODULE_EXIT()
{
    delete dirCSTable;
}

void CEndpointCS::beforeDispose()
{
    table.removeExact(this);
}

class CRemoteFilteredFileIOBase : public CRemoteBase, implements IRemoteFileIO
{
    typedef CRemoteBase PARENT;

public:
    IMPLEMENT_IINTERFACE;
    // Really a stream, but life (maybe) easier elsewhere if looks like a file
    // Sometime should refactor to be based on ISerialStream instead - or maybe IRowStream.
    CRemoteFilteredFileIOBase(SocketEndpoint &ep, const char *filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, unsigned __int64 chooseN)
        : CRemoteBase(ep, filename)
    {
        // NB: inputGrouped == outputGrouped for now, but may want output to be ungrouped

        openRequest();
        if (queryOutputCompressionDefault())
        {
            expander.setown(getExpander(queryOutputCompressionDefault()));
            if (expander)
            {
                expandMb.setEndian(__BIG_ENDIAN);
                request.appendf("\"commCompression\" : \"%s\",\n", queryOutputCompressionDefault());
            }
            else
                WARNLOG("Failed to created compression decompressor for: %s", queryOutputCompressionDefault());
        }

        request.appendf("\"format\" : \"binary\",\n"
            "\"node\" : {\n"
            " \"fileName\" : \"%s\"", filename);
        if (chooseN)
            request.appendf(",\n \"chooseN\" : \"%" I64F "u\"", chooseN);
        if (fieldFilters.numFilterFields())
        {
            request.append(",\n \"keyFilter\" : [\n  ");
            for (unsigned idx=0; idx < fieldFilters.numFilterFields(); idx++)
            {
                auto &filter = fieldFilters.queryFilter(idx);
                StringBuffer filterString;
                filter.serialize(filterString);
                if (idx)
                    request.append(",\n  ");
                request.append("\"");
                encodeJSON(request, filterString.length(), filterString.str());
                request.append("\"");
            }
            request.append("\n ]");
        }
        MemoryBuffer actualTypeInfo;
        if (!dumpTypeInfo(actualTypeInfo, actual->querySerializedDiskMeta()->queryTypeInfo()))
            throw createDafsException(DAFSERR_cmdstream_unsupported_recfmt, "Format not supported by remote read");
        request.append(",\n \"inputBin\" : \"");
        JBASE64_Encode(actualTypeInfo.toByteArray(), actualTypeInfo.length(), request, false);
        request.append("\"");
        if (actual != projected)
        {
            MemoryBuffer projectedTypeInfo;
            dumpTypeInfo(projectedTypeInfo, projected->querySerializedDiskMeta()->queryTypeInfo());
            if (actualTypeInfo.length() != projectedTypeInfo.length() ||
                memcmp(actualTypeInfo.toByteArray(), projectedTypeInfo.toByteArray(), actualTypeInfo.length()))
            {
                request.append(",\n \"outputBin\": \"");
                JBASE64_Encode(projectedTypeInfo.toByteArray(), projectedTypeInfo.length(), request, false);
                request.append("\"");
            }
        }
        bufPos = 0;
    }
    virtual size32_t read(offset_t pos, size32_t len, void * data) override
    {
        assertex(pos == bufPos);  // Must read sequentially
        if (!bufRemaining && !eof)
            refill();
        if (eof)
            return 0;
        if (len > bufRemaining)
            len = bufRemaining;
        bufPos += len;
        bufRemaining -= len;
        memcpy(data, reply.readDirect(len), len);
        return len;
    }
    virtual offset_t size() override { return -1; }
    virtual size32_t write(offset_t pos, size32_t len, const void * data) override { throwUnexpected(); }
    virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=(offset_t)-1) override { throwUnexpected(); }
    virtual void setSize(offset_t size) override { throwUnexpected(); }
    virtual void flush() override { throwUnexpected(); }
    virtual void close() override
    {
        PARENT::close(handle);
    }
    virtual unsigned __int64 getStatistic(StatisticKind kind) override
    {
        /* NB: Would need new stat. categories added for this to make sense,
         * but this class is implemented as a IFileIO for convenience for now,
         * it may be refactored into another form later.
         */
        return 0;
    }
// IRemoteFileIO
    virtual void addVirtualFieldMapping(const char *fieldName, const char *fieldValue) override
    {
        virtualFields[fieldName] = fieldValue;
    }
    virtual void ensureAvailable() override
    {
        if (firstRequest)
            handleFirstRequest();
    }
protected:
    StringBuffer &openRequest()
    {
        return request.append("{\n");
    }
    StringBuffer &closeRequest()
    {
        return request.append("\n }\n");
    }
    void addVirtualFields()
    {
        request.append(", \n \"virtualFields\" : {\n");
        bool first=true;
        for (auto &e : virtualFields)
        {
            if (!first)
                request.append(",\n");
            request.appendf("  \"%s\" : \"%s\"", e.first.c_str(), e.second.c_str());
            first = false;
        }
        request.append(" }");
    }
    void handleFirstRequest()
    {
        firstRequest = false;
        addVirtualFields();
        closeRequest();
        sendRequest(0, nullptr);
    }
    void refill()
    {
        if (firstRequest)
        {
            handleFirstRequest();
            return;
        }
        size32_t cursorLength;
        reply.read(cursorLength);
        if (!cursorLength)
        {
            eof = true;
            return;
        }
        MemoryBuffer mrequest;
        MemoryBuffer newReply;
        initSendBuffer(mrequest);
        mrequest.append((RemoteFileCommandType)RFCStreamRead);
        VStringBuffer json("{ \"handle\" : %u }", handle);
        mrequest.append(json.length(), json.str());
        sendRemoteCommand(mrequest, newReply);
        unsigned newHandle;
        newReply.read(newHandle);
        if (newHandle == handle)
        {
            reply.swapWith(newReply);
            reply.read(bufRemaining);
            eof = (bufRemaining == 0);
            if (expander)
            {
                size32_t expandedSz = expander->init(reply.bytes()+reply.getPos());
                expandMb.clear().reserve(expandedSz);
                expander->expand(expandMb.bufferBase());
                expandMb.swapWith(reply);
            }
        }
        else
        {
            assertex(newHandle == 0);
            sendRequest(cursorLength, reply.readDirect(cursorLength));
        }
    }
    void sendRequest(unsigned cursorLen, const void *cursorData)
    {
        MemoryBuffer mrequest;
        initSendBuffer(mrequest);
        mrequest.append((RemoteFileCommandType)RFCStreamRead);
        mrequest.append(request.length(), request.str());
        if (cursorLen)
        {
            StringBuffer cursorInfo;
            cursorInfo.append(",\"cursorBin\": \"");
            JBASE64_Encode(cursorData, cursorLen, cursorInfo, false);
            cursorInfo.append("\"\n");
            mrequest.append(cursorInfo.length(), cursorInfo.str());
        }
        if (TF_TRACE_FULL)
            PROGLOG("req = <%s}>", request.str());
        mrequest.append(3, " \n}");
        sendRemoteCommand(mrequest, reply);
        reply.read(handle);
        reply.read(bufRemaining);
        eof = (bufRemaining == 0);
        if (expander)
        {
            size32_t expandedSz = expander->init(reply.bytes()+reply.getPos());
            expandMb.clear().reserve(expandedSz);
            expander->expand(expandMb.bufferBase());
            expandMb.swapWith(reply);
        }
    }
    StringBuffer request;
    MemoryBuffer reply;
    RemoteFileIOHandle handle = 0;
    size32_t bufRemaining = 0;
    offset_t bufPos = 0;
    bool eof = false;

    bool firstRequest = true;
    std::unordered_map<std::string, std::string> virtualFields;
    Owned<IExpander> expander;
    MemoryBuffer expandMb;
};

class CRemoteFilteredFileIO : public CRemoteFilteredFileIOBase
{
public:
    // Really a stream, but life (maybe) easier elsewhere if looks like a file
    // Sometime should refactor to be based on ISerialStream instead - or maybe IRowStream.
    CRemoteFilteredFileIO(SocketEndpoint &ep, const char *filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, bool compressed, bool grouped, unsigned __int64 chooseN)
        : CRemoteFilteredFileIOBase(ep, filename, actual, projected, fieldFilters, chooseN)
    {
        // NB: inputGrouped == outputGrouped for now, but may want output to be ungrouped
        request.appendf(",\n \"kind\" : \"diskread\",\n"
            " \"compressed\" : \"%s\",\n"
            " \"inputGrouped\" : \"%s\",\n"
            " \"outputGrouped\" : \"%s\"", boolToStr(compressed), boolToStr(grouped), boolToStr(grouped));
    }
};

class CRemoteFilteredRowStream : public CRemoteFilteredFileIO, implements IRowStream
{
public:
    CRemoteFilteredRowStream(const RtlRecord &_recInfo, SocketEndpoint &ep, const char * filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, bool compressed, bool grouped)
        : CRemoteFilteredFileIO(ep, filename, actual, projected, fieldFilters, compressed, grouped, 0), recInfo(_recInfo)
    {
    }
    virtual const byte *queryNextRow()  // NOTE - rows returned must NOT be freed
    {
        if (!bufRemaining && !eof)
            refill();
        if (eof)
            return nullptr;
        unsigned len = recInfo.getRecordSize(reply.readDirect(0));
        bufPos += len;
        bufRemaining -= len;
        return reply.readDirect(len);
    }
    virtual void stop() override
    {
        close();
        eof = true;
    }
protected:
    const RtlRecord &recInfo;
};

static StringAttr remoteOutputCompressionDefault;
void setRemoteOutputCompressionDefault(const char *type)
{
    if (!isEmptyString(type))
        remoteOutputCompressionDefault.set(type);
}
const char *queryOutputCompressionDefault() { return remoteOutputCompressionDefault; }

extern IRemoteFileIO *createRemoteFilteredFile(SocketEndpoint &ep, const char * filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, bool compressed, bool grouped, unsigned __int64 chooseN)
{
    try
    {
        return new CRemoteFilteredFileIO(ep, filename, actual, projected, fieldFilters, compressed, grouped, chooseN);
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        e->Release();
    }
    return nullptr;
}

class CRemoteFilteredKeyIO : public CRemoteFilteredFileIOBase
{
public:
    // Really a stream, but life (maybe) easier elsewhere if looks like a file
    // Sometime should refactor to be based on ISerialStream instead - or maybe IRowStream.
    CRemoteFilteredKeyIO(SocketEndpoint &ep, const char *filename, unsigned crc, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, unsigned __int64 chooseN)
        : CRemoteFilteredFileIOBase(ep, filename, actual, projected, fieldFilters, chooseN)
    {
        request.appendf(",\n \"kind\" : \"indexread\"");
        request.appendf(",\n \"crc\" : \"%u\"", crc);
    }
};

class CRemoteFilteredKeyCountIO : public CRemoteFilteredFileIOBase
{
public:
    // Really a stream, but life (maybe) easier elsewhere if looks like a file
    // Sometime should refactor to be based on ISerialStream instead - or maybe IRowStream.
    CRemoteFilteredKeyCountIO(SocketEndpoint &ep, const char *filename, unsigned crc, IOutputMetaData *actual, const RowFilter &fieldFilters, unsigned __int64 rowLimit)
        : CRemoteFilteredFileIOBase(ep, filename, actual, actual, fieldFilters, rowLimit)
    {
        request.appendf(",\n \"kind\" : \"indexcount\"");
        request.appendf(",\n \"crc\" : \"%u\"", crc);
    }
};

class CRemoteKey : public CSimpleInterfaceOf<IIndexLookup>
{
    Owned<IRemoteFileIO> iRemoteFileIO;
    offset_t pos = 0;
    Owned<ISourceRowPrefetcher> prefetcher;
    CThorContiguousRowBuffer prefetchBuffer;
    Owned<ISerialStream> strm;
    bool pending = false;
    SocketEndpoint ep;
    StringAttr filename;
    unsigned crc;
    Linked<IOutputMetaData> actual, projected;
    RowFilter fieldFilters;

public:
    CRemoteKey(SocketEndpoint &_ep, const char *_filename, unsigned _crc, IOutputMetaData *_actual, IOutputMetaData *_projected, const RowFilter &_fieldFilters, unsigned __int64 rowLimit)
        : ep(_ep), filename(_filename), crc(_crc), actual(_actual), projected(_projected)
    {
        for (unsigned f=0; f<_fieldFilters.numFilterFields(); f++)
            fieldFilters.addFilter(OLINK(_fieldFilters.queryFilter(f)));
        iRemoteFileIO.setown(new CRemoteFilteredKeyIO(ep, filename, crc, actual, projected, fieldFilters, rowLimit));
        if (!iRemoteFileIO)
            throwStringExceptionV(DAFSERR_cmdstream_openfailure, "Unable to open remote key part: '%s'", filename.get());
        strm.setown(createFileSerialStream(iRemoteFileIO));
        prefetcher.setown(projected->createDiskPrefetcher());
        assertex(prefetcher);
        prefetchBuffer.setStream(strm);
    }
// IIndexLookup
    virtual void ensureAvailable() override
    {
        iRemoteFileIO->ensureAvailable(); // will throw an exception if fails
    }
    virtual unsigned __int64 getCount() override
    {
        return checkCount(0);
    }
    virtual unsigned __int64 checkCount(unsigned __int64 limit) override
    {
        Owned<IFileIO> iFileIO = new CRemoteFilteredKeyCountIO(ep, filename, crc, actual, fieldFilters, limit);
        unsigned __int64 result;
        iFileIO->read(0, sizeof(result), &result);
        return result;
    }
    virtual const void *nextKey() override
    {
        if (pending)
            prefetchBuffer.finishedRow();
        if (prefetchBuffer.eos())
            return nullptr;
        prefetcher->readAhead(prefetchBuffer);
        pending = true;
        return prefetchBuffer.queryRow();
    }
    virtual unsigned querySeeks() const override { return 0; } // not sure how best to handle these, perhaps should log/record somewhere on server-side
    virtual unsigned queryScans() const override { return 0; }
    virtual unsigned querySkips() const override { return 0; }
};


extern IIndexLookup *createRemoteFilteredKey(SocketEndpoint &ep, const char * filename, unsigned crc, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, unsigned __int64 chooseN)
{
    try
    {
        return new CRemoteKey(ep, filename, crc, actual, projected, fieldFilters, chooseN);
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        e->Release();
    }
    return nullptr;
}



// JCSMORE copied from rtlds.cpp (need to be moved somewhere common, there's also an implementation in thorcommon.hpp)
class CMemoryBufferSerializer : implements IRowSerializerTarget
{
public:
    CMemoryBufferSerializer(MemoryBuffer & _buffer) : buffer(_buffer)
    {
    }

    virtual void put(size32_t len, const void * ptr)
    {
        buffer.append(len, ptr);
    }

    virtual size32_t beginNested(size32_t count)
    {
        unsigned pos = buffer.length();
        buffer.append((size32_t)0);
        return pos;
    }

    virtual void endNested(size32_t sizePos)
    {
        unsigned pos = buffer.length();
        buffer.rewrite(sizePos);
        buffer.append((size32_t)(pos - (sizePos + sizeof(size32_t))));
        buffer.rewrite(pos);
    }

protected:
    MemoryBuffer & buffer;
};


class CRemoteWriteRawRowStream : public CRemoteBase
{
    typedef CRemoteBase PARENT;

protected:
    bool firstRow = true;
    const unsigned sendThresholdBytes = 0x100000; // 1MB

    MemoryBuffer requestMb;
    RemoteFileIOHandle handle = 0;
    size32_t jsonRequestStartPos = 0;
    size32_t jsonRequestEndPos = 0;
    Owned<IPropertyTree> requestTop;
    IPropertyTree *requestNode = nullptr;

    void markJsonStart()
    {
        jsonRequestStartPos = requestMb.length();
    }
    void markJsonEnd()
    {
        jsonRequestEndPos = requestMb.length();
        size32_t jsonRequestLen = jsonRequestEndPos - jsonRequestStartPos;
        requestMb.writeEndianDirect(jsonRequestStartPos-sizeof(size32_t), sizeof(size32_t), &jsonRequestLen);
    }
    void send()
    {
        if (TF_TRACE_FULL)
        {
            StringBuffer s;
            s.append(jsonRequestEndPos-jsonRequestStartPos, requestMb.toByteArray()+jsonRequestStartPos);
            PROGLOG("req = <%s>", s.str());
        }
        MemoryBuffer reply;
        sendRemoteCommand(requestMb, reply);
        unsigned newHandle;
        reply.read(newHandle);
        if (!newHandle)
            throwStringExceptionV(DAFSERR_cmdstream_generalwritefailure, "Error whilst writing data to file: '%s'", filename.get());
        else if (handle && (newHandle != handle))
            throwStringExceptionV(DAFSERR_cmdstream_unknownwritehandle, "Unknown write handle whilst remote writing to file: '%s'", filename.get());
        handle = newHandle;

        // prepare for next send

        requestMb.clear();
        initSendBuffer(requestMb);
        requestMb.append((RemoteFileCommandType)RFCStreamGeneral);
        requestMb.append((size32_t)0); // placeholder
        markJsonStart();
        VStringBuffer json("{ \"handle\" : %u }", handle);
        requestMb.append(json.length(), json.str());
        markJsonEnd();
    }
    void close()
    {
        send(); // 1st send needed if nothing written.
        PARENT::close(handle);
    }
    void prepareFirstSend()
    {
        StringBuffer jsonStr;
#if _DEBUG
        toJSON(requestTop, jsonStr, 2);
#else
        toJSON(requestTop, jsonStr, 0, 0);
#endif
        requestMb.append(jsonStr.length(), jsonStr); // NB: if there was a IOStream to MemoryBuffer impl, could use that to avoid encoding to string, and then appending.
        markJsonEnd();

        firstRow = false;
    }
public:
    CRemoteWriteRawRowStream(SocketEndpoint &ep, const char *filename)
        : CRemoteBase(ep, filename)
    {
        initSendBuffer(requestMb);
        requestMb.append((RemoteFileCommandType)RFCStreamGeneral);
        requestMb.append((size32_t)0); // placeholder
        markJsonStart();

        if (queryOutputCompressionDefault())
            requestTop->setProp("commCompression", queryOutputCompressionDefault());
        requestTop.setown(createPTree());
        requestTop->setProp("format", "binary");
        requestNode = requestTop->setPropTree("node");
        requestNode->setProp("fileName", filename);
    }
    virtual void beforeDispose()
    {
        try
        {
            close();
        }
        catch (IException *e)
        {
            EXCLOG(e, nullptr);
            e->Release();
        }
    }
};



class CRemoteWriteRowStreamBase : public CRemoteWriteRawRowStream, implements IRowWriter
{
protected:
    Owned<IOutputRowSerializer> serializer;
    CMemoryBufferSerializer targetSerializer;

public:
    IMPLEMENT_IINTERFACE;

    CRemoteWriteRowStreamBase(SocketEndpoint &ep, const char *filename, IOutputMetaData *actual)
        : CRemoteWriteRawRowStream(ep, filename), targetSerializer(requestMb)
    {
        serializer.setown(actual->createDiskSerializer(nullptr, 1)); // JCSMORE - are these params ok?

        MemoryBuffer actualTypeInfo;
        if (!dumpTypeInfo(actualTypeInfo, actual->querySerializedDiskMeta()->queryTypeInfo()))
            throw createDafsException(DAFSERR_cmdstream_unsupported_recfmt, "Format not supported by remote read");
        StringBuffer typeInfoStr;
        JBASE64_Encode(actualTypeInfo.toByteArray(), actualTypeInfo.length(), typeInfoStr, false);
        requestNode->setProp("inputBin", typeInfoStr.str());
    }
// IRowStream impl.
    virtual void putRow(const void *row) override
    {
        throwUnexpected(); // derived classes should implement
    }
    virtual void flush() override
    {
        send();
    }
};


class CRemoteFlatFileWriterRowStream : public CRemoteWriteRowStreamBase
{
    typedef CRemoteWriteRowStreamBase PARENT;

    unsigned compMethod;
    bool grouped;

public:
    CRemoteFlatFileWriterRowStream(SocketEndpoint &ep, const char *filename, IOutputMetaData *actual, unsigned _compMethod, bool _grouped)
        : CRemoteWriteRowStreamBase(ep, filename, actual), compMethod(_compMethod), grouped(_grouped)
    {
        requestNode->setProp("kind", "diskwrite");
        requestNode->setProp("compressed", translateFromCompMethod(compMethod));
        requestNode->setProp("inputGrouped", boolToStr(grouped));
    }
    virtual void putRow(const void *row) override
    {
        if (firstRow)
            prepareFirstSend(); // will not be sent until flush()

        // buffer up
        // send when buffered up threshold reached

        serializer->serialize(targetSerializer, (const byte *)row);

        if (grouped) // JCSMORE
            UNIMPLEMENTED;

        if (requestMb.length() > sendThresholdBytes)
            flush();
    }
};


IRowWriter *createRemoteFlatFileWriter(SocketEndpoint &ep, const char *filename, IOutputMetaData *actual, unsigned compMethod, bool grouped)
{
    try
    {
        return new CRemoteFlatFileWriterRowStream(ep, filename, actual, compMethod, grouped);
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        e->Release();
    }
    return nullptr;
}


class CRemoteIndexFileWriterRowStream : public CRemoteWriteRowStreamBase
{
public:
    CRemoteIndexFileWriterRowStream(SocketEndpoint &ep, const char *filename, IOutputMetaData *actual)
        : CRemoteWriteRowStreamBase(ep, filename, actual)
    {
        requestNode->setProp("kind", "indexwrite");
    }
};


IRowWriter *createRemoteIndexFileWriter(SocketEndpoint &ep, const char * filename, IOutputMetaData *actual)
{
    try
    {
        return new CRemoteIndexFileWriterRowStream(ep, filename, actual);
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        e->Release();
    }
    return nullptr;
}






class CRemoteFile : public CRemoteBase, implements IFile
{
    StringAttr remotefilename;
    unsigned flags;
    bool isShareSet;
public:
    IMPLEMENT_IINTERFACE
    CRemoteFile(const SocketEndpoint &_ep, const char * _filename)
        : CRemoteBase(_ep, _filename)
    {
        flags = ((unsigned)IFSHread)|((S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH)<<16);
        isShareSet = false;
        if (filename.length()>2 && isPathSepChar(filename[0]) && isShareChar(filename[2]))
        {
            VStringBuffer winDriveFilename("%c:%s", filename[1], filename+3);
            filename.set(winDriveFilename);
        }
    }

    bool exists()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCexists).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    bool getTime(CDateTime * createTime, CDateTime * modifiedTime, CDateTime * accessedTime)
    {
        CDateTime dummyTime;
        if (!createTime)
            createTime = &dummyTime;
        if (!modifiedTime)
            modifiedTime = &dummyTime;
        if (!accessedTime)
            accessedTime = &dummyTime;
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCgettime).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        if (ok) {
            createTime->deserialize(replyBuffer);
            modifiedTime->deserialize(replyBuffer);
            accessedTime->deserialize(replyBuffer);
        }
        return ok;
    }

    bool setTime(const CDateTime * createTime, const CDateTime * modifiedTime, const CDateTime * accessedTime)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsettime).append(filename);
        if (createTime)
        {
            sendBuffer.append((bool)true);
            createTime->serialize(sendBuffer);
        }
        else
            sendBuffer.append((bool)false);
        if (modifiedTime)
        {
            sendBuffer.append((bool)true);
            modifiedTime->serialize(sendBuffer);
        }
        else
            sendBuffer.append((bool)false);
        if (accessedTime)
        {
            sendBuffer.append((bool)true);
            accessedTime->serialize(sendBuffer);
        }
        else
            sendBuffer.append((bool)false);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    fileBool isDirectory()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCisdirectory).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        unsigned ret;
        replyBuffer.read(ret);
        return (fileBool)ret;
    }


    fileBool isFile()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCisfile).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        unsigned ret;
        replyBuffer.read(ret);
        return (fileBool)ret;
    }

    fileBool isReadOnly()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCisreadonly).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        unsigned ret;
        replyBuffer.read(ret);
        return (fileBool)ret;
    }

    IFileIO * open(IFOmode mode,IFEflags extraFlags=IFEnone);
    IFileIO * openShared(IFOmode mode,IFSHmode shmode,IFEflags extraFlags=IFEnone);
    IFileAsyncIO * openAsync(IFOmode mode) { return NULL; } // not supported

    const char * queryFilename()
    {
        if (remotefilename.isEmpty()) {
            RemoteFilename rfn;
            rfn.setPath(ep,filename);
            StringBuffer path;
            rfn.getRemotePath(path);
            remotefilename.set(path);
        }
        return remotefilename.get();
    }

    void resetLocalFilename(const char *name)
    {
        remotefilename.clear();
        filename.set(name);
    }

    bool remove()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCremove).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    void rename(const char *newname)
    {
    // currently ignores directory on newname (in future versions newname will be required to be tail only and not full path)
        StringBuffer path;
        splitDirTail(filename,path);
        StringBuffer newdir;
        path.append(splitDirTail(newname,newdir));
        if (newdir.length()&&(strcmp(newdir.str(),path.str())!=0))
            WARNLOG("CRemoteFile::rename passed full path '%s' that may not to match original directory '%s'",newname,path.str());
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCrename).append(filename).append(path);
        sendRemoteCommand(sendBuffer, replyBuffer);
        filename.set(path);
        remotefilename.clear();
    }

    void move(const char *newname)
    {
        // like rename except between directories
        // first create replote path
        if (!newname||!*newname)
            return;
        RemoteFilename destrfn;
        if (isPathSepChar(newname[0])&&isPathSepChar(newname[1])) {
            destrfn.setRemotePath(newname);
            if (!destrfn.queryEndpoint().ipequals(ep)) {
                StringBuffer msg;
                msg.appendf("IFile::move %s to %s, destination node must match source node", queryFilename(), newname);
                throw createDafsException(RFSERR_MoveFailed,msg.str());
            }
        }
        else
            destrfn.setPath(ep,newname);
        StringBuffer dest;
        newname = destrfn.getLocalPath(dest).str();
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        StringBuffer path;
        splitDirTail(filename,path);
        StringBuffer newdir;
        const char *newtail = splitDirTail(newname,newdir);
        if (strcmp(newdir.str(),path.str())==0)
        {
            path.append(newtail);
            newname = path;
            sendBuffer.append((RemoteFileCommandType)RFCrename);    // use rename if we can (supported on older dafilesrv)
        }
        else
            sendBuffer.append((RemoteFileCommandType)RFCmove);
        sendBuffer.append(filename).append(newname);
        sendRemoteCommand(sendBuffer, replyBuffer);
        filename.set(newname);
        remotefilename.clear();
    }

    void setReadOnly(bool set)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsetreadonly).append(filename).append(set);
        sendRemoteCommand(sendBuffer, replyBuffer);
    }

    void setFilePermissions(unsigned fPerms)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsetfileperms).append(filename).append(fPerms);
        try
        {
            sendRemoteCommand(sendBuffer, replyBuffer);
        }
        catch (IDAFS_Exception *e)
        {
            if (e->errorCode() == RFSERR_InvalidCommand)
            {
                WARNLOG("umask setFilePermissions (0%o) not supported on remote server", fPerms);
                e->Release();
            }
            else
                throw;
        }

    }

    offset_t size()
    {
#if 1 // faster method (consistant with IFile)
        // do this by using dir call (could be improved with new function but this not *too* bad)
        if (isSpecialPath(filename))
            return 0;   // queries deemed to always exist (though don't know size).
                        // if needed to get size I guess could use IFileIO method and cache (bit of pain though)
        StringBuffer dir;
        const char *tail = splitDirTail(filename,dir);
        if (!dir.length())
            return false;
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        bool includedirs = true;
        bool sub=false;
        {
            //Could be removed with new dafilesrv change [ (stream != 0) ], since this is not streaming.
            Owned<CEndpointCS> crit = dirCSTable->getCrit(ep); // NB dirCSTable doesn't own, last reference will remove from table
            CriticalBlock block(*crit);
            sendBuffer.append((RemoteFileCommandType)RFCgetdir).append(dir).append(tail).append(includedirs).append(sub);
            try
            {
                sendRemoteCommand(sendBuffer, replyBuffer);
            }
            catch (IDAFS_Exception * e)
            {
                if (e->errorCode() == RFSERR_GetDirFailed)
                {
                    e->Release();
                    return (offset_t)-1;
                }
                else
                    throw e;
            }
        }
        // now should be 0 or 1 files returned
        Owned<CRemoteDirectoryIterator> iter = new CRemoteDirectoryIterator(ep, dir.str());
        iter->appendBuf(replyBuffer);
        if (!iter->first())
            return (offset_t)-1;
        return (offset_t) iter->getFileSize();
#else
        IFileIO * io = open(IFOread);
        offset_t length = (offset_t)-1;
        if (io)
        {
            length = io->size();
            io->Release();
        }
        return length;
#endif
    }

    bool createDirectory()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCcreatedir).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer);

        bool ok;
        replyBuffer.read(ok);
        return ok;
    }

    virtual IDirectoryIterator *directoryFiles(const char *mask,bool sub,bool includedirs)
    {
        if (mask&&!*mask)
            return createDirectoryIterator("",""); // NULL iterator

        CRemoteDirectoryIterator *ret = new CRemoteDirectoryIterator(ep, filename);
        byte stream = (sub || !mask || containsFileWildcard(mask)) ? 1 : 0; // no point in streaming if mask without wildcards or sub, as will only be <= 1 match.

        Owned<CEndpointCS> crit = dirCSTable->getCrit(ep); // NB dirCSTable doesn't own, last reference will remove from table
        CriticalBlock block(*crit);
        for (;;)
        {
            MemoryBuffer sendBuffer;
            initSendBuffer(sendBuffer);
            MemoryBuffer replyBuffer;
            sendBuffer.append((RemoteFileCommandType)RFCgetdir).append(filename).append(mask?mask:"").append(includedirs).append(sub).append(stream);
            sendRemoteCommand(sendBuffer, replyBuffer);
            if (ret->appendBuf(replyBuffer))
                break;
            stream = 2; // NB: will never get here if streaming was off (if stream==0 above)
        }
        return ret;
    }

    IDirectoryDifferenceIterator *monitorDirectory(
                                  IDirectoryIterator *prev=NULL,    // in (NULL means use current as baseline)
                                  const char *mask=NULL,
                                  bool sub=false,
                                  bool includedirs=false,
                                  unsigned checkinterval=60*1000,
                                  unsigned timeout=(unsigned)-1,
                                  Semaphore *abortsem=NULL) // returns NULL if timed out
    {
        // abortsem not yet supported
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCmonitordir).append(filename).append(mask?mask:"").append(includedirs).append(sub);
        sendBuffer.append(checkinterval).append(timeout);
        __int64 cancelid=0; // not yet used
        sendBuffer.append(cancelid);
        byte isprev=(prev!=NULL)?1:0;
        sendBuffer.append(isprev);
        if (prev)
            CRemoteDirectoryIterator::serialize(sendBuffer,prev,0,true);
        sendRemoteCommand(sendBuffer, replyBuffer);
        byte status;
        replyBuffer.read(status);
        if (status==1)
        {
            CRemoteDirectoryIterator *iter = new CRemoteDirectoryIterator(ep, filename);
            iter->appendBuf(replyBuffer);
            return iter;
        }
        return NULL;
    }

    bool getInfo(bool &isdir,offset_t &size,CDateTime &modtime)
    {
        // do this by using dir call (could be improved with new function but this not *too* bad)
        StringBuffer dir;
        const char *tail = splitDirTail(filename,dir);
        if (!dir.length())
            return false;
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        bool includedirs = true;
        bool sub=false;
        {
            //Could be removed with new dafilesrv change [ (stream != 0) ], since this is not streaming.
            Owned<CEndpointCS> crit = dirCSTable->getCrit(ep); // NB dirCSTable doesn't own, last reference will remove from table
            CriticalBlock block(*crit);
            sendBuffer.append((RemoteFileCommandType)RFCgetdir).append(dir).append(tail).append(includedirs).append(sub);
            sendRemoteCommand(sendBuffer, replyBuffer);
        }
        // now should be 0 or 1 files returned
        Owned<CRemoteDirectoryIterator> iter = new CRemoteDirectoryIterator(ep, dir.str());
        iter->appendBuf(replyBuffer);
        if (!iter->first())
            return false;
        isdir = iter->isDir();
        size = (offset_t) iter->getFileSize();
        iter->getModifiedTime(modtime);
        return true;
    }



    bool setCompression(bool set)
    {
        assertex(!"Need to implement compress()");
        return false;
    }

    offset_t compressedSize()
    {
        assertex(!"Need to implement actualSize()");
        return (offset_t)-1;
    }

    void serialize(MemoryBuffer &tgt)
    {
        throwUnexpected();
    }

    void deserialize(MemoryBuffer &src)
    {
        throwUnexpected();
    }

    unsigned getCRC()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCgetcrc).append(filename);
        sendRemoteCommand(sendBuffer, replyBuffer, true, true);

        unsigned crc;
        replyBuffer.read(crc);
        return crc;
    }

    void setCreateFlags(unsigned short cflags)
    {
        flags &= 0xffff;
        flags |= ((unsigned)cflags<<16);
    }

    unsigned short getCreateFlags()
    {
        return (unsigned short)(flags>>16);
    }

    void setShareMode(IFSHmode shmode)
    {
        flags &= ~(IFSHfull|IFSHread);
        flags |= (unsigned)(shmode&(IFSHfull|IFSHread));
        isShareSet = true;
    }

    unsigned short getShareMode()
    {
        return (unsigned short)(flags&0xffff);
    }

    bool getIsShareSet()
    {
        return isShareSet;
    }

    void remoteExtractBlobElements(const char * prefix, ExtractedBlobArray & extracted)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        sendBuffer.append((RemoteFileCommandType)RFCextractblobelements).append(prefix).append(filename);
        MemoryBuffer replyBuffer;
        sendRemoteCommand(sendBuffer, replyBuffer, true, true); // handles error code
        unsigned n;
        replyBuffer.read(n);
        for (unsigned i=0;i<n;i++) {
            ExtractedBlobInfo *item = new ExtractedBlobInfo;
            item->deserialize(replyBuffer);
            extracted.append(*item);
        }
    }

    bool copySectionAsync(const char *uuid,const RemoteFilename &dest, offset_t toOfs, offset_t fromOfs, offset_t size, ICopyFileProgress *progress, unsigned timeout)
    {
        // now if we get here is it can be assumed the source file is local to where we send the command
        StringBuffer tos;
        dest.getRemotePath(tos);
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCcopysection).append(uuid).append(queryLocalName()).append(tos).append(toOfs).append(fromOfs).append(size).append(timeout);
        sendRemoteCommand(sendBuffer, replyBuffer);
        unsigned status;
        replyBuffer.read(status);
        if (progress)
        {
            offset_t sizeDone;
            offset_t totalSize;
            replyBuffer.read(sizeDone).read(totalSize);
            progress->onProgress(sizeDone,totalSize);
        }
        return (AsyncCommandStatus)status!=ACScontinue; // should only otherwise be done as errors raised by exception
    }

    void copySection(const RemoteFilename &dest, offset_t toOfs, offset_t fromOfs, offset_t size, ICopyFileProgress *progress, CFflags copyFlags=CFnone)
    {
        StringBuffer uuid;
        genUUID(uuid,true);
        unsigned timeout = 60*1000; // check every minute
        while(!copySectionAsync(uuid.str(),dest,toOfs,fromOfs,size,progress,timeout));
    }

    void copyTo(IFile *dest, size32_t buffersize, ICopyFileProgress *progress, bool usetmp, CFflags copyFlags=CFnone);

    virtual IMemoryMappedFile *openMemoryMapped(offset_t ofs, memsize_t len, bool write)
    {
        return NULL;
    }
};

void clientAddSocketToCache(SocketEndpoint &ep,ISocket *socket)
{
    CriticalBlock block(CConnectionTable::crit);
    if (ConnectionTable)
        ConnectionTable->addLink(ep,socket);
}


IFile * createRemoteFile(SocketEndpoint &ep, const char * filename)
{
    IFile *ret = createFileLocalMount(ep,filename);
    if (ret)
        return ret;
    return new CRemoteFile(ep, filename);
}


void clientDisconnectRemoteFile(IFile *file)
{
    CRemoteFile *cfile = QUERYINTERFACE(file,CRemoteFile);
    if (cfile)
        cfile->disconnect();
}

bool clientResetFilename(IFile *file, const char *newname) // returns false if not remote
{
    CRemoteFile *cfile = QUERYINTERFACE(file,CRemoteFile);
    if (!cfile)
        return false;
    cfile->resetLocalFilename(newname);
    return true;
}



extern bool clientAsyncCopyFileSection(const char *uuid,
                        IFile *from,                        // expected to be remote
                        RemoteFilename &to,
                        offset_t toOfs,                     // -1 created file and copies to start
                        offset_t fromOfs,
                        offset_t size,
                        ICopyFileProgress *progress,
                        unsigned timeout)       // returns true when done
{
    CRemoteFile *cfile = QUERYINTERFACE(from,CRemoteFile);
    if (!cfile) {
        // local - do sync
        from->copySection(to,toOfs,fromOfs,size,progress);
        return true;
    }
    return cfile->copySectionAsync(uuid,to,toOfs,fromOfs, size, progress, timeout);

}




//---------------------------------------------------------------------------

class CRemoteFileIO : implements IFileIO, public CInterface
{
protected:
    Linked<CRemoteFile> parent;
    RemoteFileIOHandle  handle;
    std::atomic<cycle_t> ioReadCycles;
    std::atomic<cycle_t> ioWriteCycles;
    std::atomic<__uint64> ioReadBytes;
    std::atomic<__uint64> ioWriteBytes;
    std::atomic<__uint64> ioReads;
    std::atomic<__uint64> ioWrites;
    std::atomic<unsigned> ioRetries;
    IFOmode mode;
    compatIFSHmode compatmode;
    IFEflags extraFlags;
    bool disconnectonexit;
public:
    IMPLEMENT_IINTERFACE
    CRemoteFileIO(CRemoteFile *_parent)
        : parent(_parent), ioReadCycles(0), ioWriteCycles(0), ioReadBytes(0), ioWriteBytes(0), ioReads(0), ioWrites(0), ioRetries(0)
    {
        handle = 0;
        disconnectonexit = false;
    }

    ~CRemoteFileIO()
    {
        if (handle) {
            try {
                close();
            }
            catch (IException *e) {
                StringBuffer s;
                e->errorMessage(s);
                WARNLOG("CRemoteFileIO close file: %s",s.str());
                e->Release();
            }
        }
        if (disconnectonexit)
            parent->disconnect();
    }

    void close()
    {
        if (handle)
        {
            try
            {
                MemoryBuffer sendBuffer;
                initSendBuffer(sendBuffer);
                sendBuffer.append((RemoteFileCommandType)RFCcloseIO).append(handle);
                parent->sendRemoteCommand(sendBuffer,false);
            }
            catch (IDAFS_Exception *e)
            {
                if ((e->errorCode()!=RFSERR_InvalidFileIOHandle)&&(e->errorCode()!=RFSERR_NullFileIOHandle))
                    throw;
                e->Release();
            }
            handle = 0;
        }
    }
    RemoteFileIOHandle getHandle() const { return handle; }
    bool open(IFOmode _mode,compatIFSHmode _compatmode,IFEflags _extraFlags=IFEnone)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        const char *localname = parent->queryLocalName();
        localname = skipSpecialPath(localname);
        // also send _extraFlags
        // then also send sMode, cFlags
        unsigned short sMode = parent->getShareMode();
        unsigned short cFlags = parent->getCreateFlags();
        if (!(parent->getIsShareSet()))
        {
            switch ((compatIFSHmode)_compatmode)
            {
                case compatIFSHnone:
                    sMode = IFSHnone;
                    break;
                case compatIFSHread:
                    sMode = IFSHread;
                    break;
                case compatIFSHwrite:
                    sMode = IFSHfull;
                    break;
                case compatIFSHall:
                    sMode = IFSHfull;
                    break;
            }
        }
        sendBuffer.append((RemoteFileCommandType)RFCopenIO).append(localname).append((byte)_mode).append((byte)_compatmode).append((byte)_extraFlags).append(sMode).append(cFlags);
        parent->sendRemoteCommand(sendBuffer, replyBuffer);

        replyBuffer.read(handle);
        if (!handle)
            return false;
        switch (_mode) {
        case IFOcreate:
            mode = IFOwrite;
            break;
        case IFOcreaterw:
            mode = IFOreadwrite;
            break;
        default:
            mode = _mode;
            break;
        }
        compatmode = _compatmode;
        extraFlags = _extraFlags;
        return true;
    }

    bool reopen()
    {
        StringBuffer s;
        PROGLOG("Attempting reopen of %s on %s",parent->queryLocalName(),parent->queryEp().getUrlStr(s).str());
        if (open(mode,compatmode,extraFlags))
            return true;
        return false;

    }


    offset_t size()
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsize).append(handle);
        parent->sendRemoteCommand(sendBuffer, replyBuffer, false);
        // Retry using reopen TBD

        offset_t ret;
        replyBuffer.read(ret);
        return ret;
    }

    virtual unsigned __int64 getStatistic(StatisticKind kind)
    {
        switch (kind)
        {
        case StCycleDiskReadIOCycles:
            return ioReadCycles.load(std::memory_order_relaxed);
        case StCycleDiskWriteIOCycles:
            return ioWriteCycles.load(std::memory_order_relaxed);
        case StTimeDiskReadIO:
            return cycle_to_nanosec(ioReadCycles.load(std::memory_order_relaxed));
        case StTimeDiskWriteIO:
            return cycle_to_nanosec(ioWriteCycles.load(std::memory_order_relaxed));
        case StSizeDiskRead:
            return ioReadBytes.load(std::memory_order_relaxed);
        case StSizeDiskWrite:
            return ioWriteBytes.load(std::memory_order_relaxed);
        case StNumDiskReads:
            return ioReads.load(std::memory_order_relaxed);
        case StNumDiskWrites:
            return ioWrites.load(std::memory_order_relaxed);
        case StNumDiskRetries:
            return ioRetries.load(std::memory_order_relaxed);
        }
        return 0;
    }

    size32_t read(offset_t pos, size32_t len, void * data)
    {
        size32_t got;
        MemoryBuffer replyBuffer;
        CCycleTimer timer;
        const void *b;
        try
        {
            b = doRead(pos,len,replyBuffer,got,data);
        }
        catch (...)
        {
            ioReadCycles.fetch_add(timer.elapsedCycles());
            throw;
        }
        ioReadCycles.fetch_add(timer.elapsedCycles());
        ioReadBytes.fetch_add(got);
        ++ioReads;
        if (b!=data)
            memcpy(data,b,got);
        return got;
    }

    virtual void flush()
    {
    }

    const void *doRead(offset_t pos, size32_t len, MemoryBuffer &replyBuffer, size32_t &got, void *dstbuf)
    {
        unsigned tries=0;
        for (;;)
        {
            try
            {
                MemoryBuffer sendBuffer;
                initSendBuffer(sendBuffer);
                replyBuffer.clear();
                sendBuffer.append((RemoteFileCommandType)RFCread).append(handle).append(pos).append(len);
                parent->sendRemoteCommand(sendBuffer, replyBuffer,false);
                // kludge dafilesrv versions <= 1.5e don't return error correctly 
                if (replyBuffer.length()>len+sizeof(size32_t)+sizeof(unsigned))
                {
                    size32_t save = replyBuffer.getPos();
                    replyBuffer.reset(len+sizeof(size32_t)+sizeof(unsigned));
                    unsigned errCode;
                    replyBuffer.read(errCode);
                    if (errCode)
                    {
                        StringBuffer msg;
                        parent->ep.getUrlStr(msg.append('[')).append("] ");
                        if (replyBuffer.getPos()<replyBuffer.length())
                        {
                            StringAttr s;
                            replyBuffer.read(s);
                            msg.append(s);
                        }
                        else
                            msg.append("ERROR #").append(errCode);
                        throw createDafsException(errCode, msg.str());
                    }
                    else
                        replyBuffer.reset(save);
                }
                replyBuffer.read(got);
                if ((got>replyBuffer.remaining())||(got>len))
                {
                    PROGLOG("Read beyond buffer %d,%d,%d",got,replyBuffer.remaining(),len);
                    throw createDafsException(RFSERR_ReadFailed, "Read beyond buffer");
                }
                return replyBuffer.readDirect(got);
            }
            catch (IJSOCK_Exception *e)
            {
                EXCLOG(e,"CRemoteFileIO::read");
                if (++tries > 3)
                {
                    ioRetries.fetch_add(tries);
                    throw;
                }
                WARNLOG("Retrying read of %s (%d)",parent->queryLocalName(),tries);
                Owned<IException> exc = e;
                if (!reopen())
                {
                    ioRetries.fetch_add(tries);
                    throw exc.getClear();
                }
            }
        }
        if (tries)
            ioRetries.fetch_add(tries);
        got = 0;
        return NULL;
    }


    size32_t write(offset_t pos, size32_t len, const void * data)
    {
        unsigned tries=0;
        size32_t ret = 0;
        CCycleTimer timer;
        for (;;)
        {
            try
            {
                MemoryBuffer replyBuffer;
                MemoryBuffer sendBuffer;
                initSendBuffer(sendBuffer);
                sendBuffer.append((RemoteFileCommandType)RFCwrite).append(handle).append(pos).append(len).append(len, data);
                parent->sendRemoteCommand(sendBuffer, replyBuffer, false, true);
                replyBuffer.read(ret);
                break;
            }
            catch (IJSOCK_Exception *e)
            {
                EXCLOG(e,"CRemoteFileIO::write");
                if (++tries > 3)
                {
                    ioRetries.fetch_add(tries);
                    ioWriteCycles.fetch_add(timer.elapsedCycles());
                    throw;
                }
                WARNLOG("Retrying write(%" I64F "d,%d) of %s (%d)",pos,len,parent->queryLocalName(),tries);
                Owned<IException> exc = e;
                if (!reopen())
                {
                    ioRetries.fetch_add(tries);
                    ioWriteCycles.fetch_add(timer.elapsedCycles());
                    throw exc.getClear();
                }
            }
        }

        if (tries)
            ioRetries.fetch_add(tries);

        ioWriteCycles.fetch_add(timer.elapsedCycles());
        ioWriteBytes.fetch_add(ret);
        ++ioWrites;
        if ((ret==(size32_t)-1) || (ret < len))
            throw createDafsException(DISK_FULL_EXCEPTION_CODE,"write failed, disk full?");
        return ret;
    }

    offset_t appendFile(IFile *file,offset_t pos,offset_t len)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        const char * fname = file->queryFilename();
        sendBuffer.append((RemoteFileCommandType)RFCappend).append(handle).append(fname).append(pos).append(len);
        parent->sendRemoteCommand(sendBuffer, replyBuffer, false, true); // retry not safe

        offset_t ret;
        replyBuffer.read(ret);

        if ((ret==(offset_t)-1) || ((len != ((offset_t)-1)) && (ret < len)))
            throw createDafsException(DISK_FULL_EXCEPTION_CODE,"append failed, disk full?");    // though could be file missing TBD
        return ret;
    }


    void setSize(offset_t size)
    {
        MemoryBuffer sendBuffer;
        initSendBuffer(sendBuffer);
        MemoryBuffer replyBuffer;
        sendBuffer.append((RemoteFileCommandType)RFCsetsize).append(handle).append(size);
        parent->sendRemoteCommand(sendBuffer, replyBuffer, false, true);
        // retry using reopen TBD


    }

    void setDisconnectOnExit(bool set) { disconnectonexit = set; }

    void sendRemoteCommand(MemoryBuffer & sendBuffer, MemoryBuffer & replyBuffer, bool retry=true, bool lengthy=false, bool handleErrCode=true)
    {
        parent->sendRemoteCommand(sendBuffer, replyBuffer, retry, lengthy, handleErrCode);
    }
};

void clientDisconnectRemoteIoOnExit(IFileIO *fileio,bool set)
{
    CRemoteFileIO *cfileio = QUERYINTERFACE(fileio,CRemoteFileIO);
    if (cfileio)
        cfileio->setDisconnectOnExit(set);
}



IFileIO * CRemoteFile::openShared(IFOmode mode,IFSHmode shmode,IFEflags extraFlags)
{
    // 0x0, 0x8, 0x10 and 0x20 are only share modes supported in this assert
    // currently only 0x0 (IFSHnone), 0x8 (IFSHread) and 0x10 (IFSHfull) are used so this could be 0xffffffe7
    // note: IFSHfull also includes read sharing (ie write|read)
    assertex(((unsigned)shmode&0xffffffc7)==0);
    compatIFSHmode compatmode;
    unsigned fileflags = (flags>>16) &  (S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IWOTH|S_IXOTH);
    if (fileflags&S_IXUSR)                      // this is bit hit and miss but backward compatible
        compatmode = compatIFSHexec;
    else if (fileflags&(S_IWGRP|S_IWOTH))
        compatmode = compatIFSHall;
    else if (shmode&IFSHfull)
        compatmode = compatIFSHwrite;
    else if (((shmode&(IFSHread|IFSHfull))==0) && ((fileflags&(S_IRGRP|S_IROTH))==0))
        compatmode = compatIFSHnone;
    else
        compatmode = compatIFSHread;
    Owned<CRemoteFileIO> res = new CRemoteFileIO(this);
    if (res->open(mode,compatmode,extraFlags))
        return res.getClear();
    return NULL;
}

IFileIO * CRemoteFile::open(IFOmode mode,IFEflags extraFlags)
{
    return openShared(mode,(IFSHmode)(flags&(IFSHread|IFSHfull)),extraFlags);
}

//---------------------------------------------------------------------------

void CRemoteFile::copyTo(IFile *dest, size32_t buffersize, ICopyFileProgress *progress, bool usetmp, CFflags copyFlags)
{
    CRemoteFile *dstfile = QUERYINTERFACE(dest,CRemoteFile);
    if (dstfile&&!dstfile->queryEp().isLocal()) {
        StringBuffer tmpname;
        Owned<IFile> destf;
        RemoteFilename dest;
        if (usetmp) {
            makeTempCopyName(tmpname,dstfile->queryLocalName());
            dest.setPath(dstfile->queryEp(),tmpname.str());
        }
        else
            dest.setPath(dstfile->queryEp(),dstfile->queryLocalName());
        destf.setown(createIFile(dest));
        try {
            // following may fail if new dafilesrv not deployed on src
            copySection(dest,(offset_t)-1,0,(offset_t)-1,progress,copyFlags);
            if (usetmp) {
                StringAttr tail(pathTail(dstfile->queryLocalName()));
                dstfile->remove();
                destf->rename(tail);
            }
            return;
        }
        catch (IException *e)
        {
            StringBuffer s;
            s.appendf("Remote File Copy (%d): ",e->errorCode());
            e->errorMessage(s);
            s.append(", retrying local");
            WARNLOG("%s",s.str());
            e->Release();
        }
        // delete dest
        try {
            destf->remove();
        }
        catch (IException *e)
        {
            EXCLOG(e,"Remote File Copy, Deleting temporary file");
            e->Release();
        }
    }
    // assumption if we get here that source remote, dest local (or equiv)
    class cIntercept: implements ICopyFileIntercept
    {
        MemoryAttr ma;
        MemoryBuffer mb;
        virtual offset_t copy(IFileIO *from, IFileIO *to, offset_t ofs, size32_t sz)
        {
            if (ma.length()<sz)
                ma.allocate(sz);    // may be not used
            void *buf = ma.bufferBase();
            size32_t got;
            CRemoteFileIO *srcio = QUERYINTERFACE(from,CRemoteFileIO);
            const void *dst;
            if (srcio)
                dst = srcio->doRead(ofs,sz,mb.clear(),got,buf);
            else {
                // shouldn't ever get here if source remote
                got = from->read(ofs, sz, buf);
                dst = buf;
            }
            if (got != 0)
                to->write(ofs, got, dst);
            return got;
        }
    } intercept;
    doCopyFile(dest,this,buffersize,progress,&intercept,usetmp,copyFlags);
}

/////////////////////////

ISocket *checkSocketSecure(ISocket *socket)
{
    if (securitySettings.connectMethod == SSLNone)
        return LINK(socket);

    char pname[256];
    pname[0] = 0;
    int pport = socket->peer_name(pname, sizeof(pname)-1);

    if ( (pport == securitySettings.daFileSrvSSLPort) && (!socket->isSecure()) )
    {
#ifdef _USE_OPENSSL
        Owned<ISecureSocket> ssock;
        try
        {
            ssock.setown(createSecureSocket(LINK(socket), ClientSocket));
            int status = ssock->secure_connect();
            if (status < 0)
                throw createDafsException(DAFSERR_connection_failed, "Failure to establish secure connection");
            return ssock.getClear();
        }
        catch (IException *e)
        {
            cleanupSocket(ssock);
            ssock.clear();
            cleanupSocket(socket);
            StringBuffer eMsg;
            e->errorMessage(eMsg);
            e->Release();
            throw createDafsException(DAFSERR_connection_failed, eMsg.str());
        }
#else
        throw createDafsException(DAFSERR_connection_failed,"Failure to establish secure connection: OpenSSL disabled in build");
#endif
    }

    return LINK(socket);
}

ISocket *connectDafs(SocketEndpoint &ep, unsigned timeoutms)
{
    Owned<ISocket> socket;

    if ( (securitySettings.connectMethod == SSLNone) || (securitySettings.connectMethod == SSLOnly) )
    {
        socket.setown(ISocket::connect_timeout(ep, timeoutms));
        return checkSocketSecure(socket);
    }

    // SSLFirst or UnsecureFirst ...

    unsigned newtimeout = timeoutms;
    if (newtimeout > 5000)
        newtimeout = 5000;

    int conAttempts = 2;
    while (conAttempts > 0)
    {
        conAttempts--;
        bool connected = false;
        try
        {
            socket.setown(ISocket::connect_timeout(ep, newtimeout));
            connected = true;
            newtimeout = timeoutms;
        }
        catch (IJSOCK_Exception *e)
        {
            if (e->errorCode() == JSOCKERR_connection_failed)
            {
                e->Release();
                if (ep.port == securitySettings.daFileSrvSSLPort)
                    ep.port = securitySettings.daFileSrvPort;
                else
                    ep.port = securitySettings.daFileSrvSSLPort;
                if (!conAttempts)
                    throw;
            }
            else
                throw;
        }

        if (connected)
        {
            if (ep.port == securitySettings.daFileSrvSSLPort)
            {
                try
                {
                    return checkSocketSecure(socket);
                }
                catch (IDAFS_Exception *e)
                {
                    connected = false;
                    if (e->errorCode() == DAFSERR_connection_failed)
                    {
                        // worth logging to help identify any ssl config issues ...
                        StringBuffer errmsg;
                        e->errorMessage(errmsg);
                        WARNLOG("%s", errmsg.str());
                        e->Release();
                        ep.port = securitySettings.daFileSrvPort;
                        if (!conAttempts)
                            throw;
                    }
                    else
                        throw;
                }
            }
            else
                return socket.getClear();
        }
    }

    throw createDafsException(DAFSERR_connection_failed, "Failed to establish connection with DaFileSrv");
}

unsigned getRemoteVersion(CRemoteBase &remote, StringBuffer &ver)
{
    return remote.getRemoteVersion(ver);
}

/* JCSMORE - add a SocketEndpoint->version cache
 * Idea being, that clients will want to determine version and differentiate what they send
 * But do not want the cost of asking each time!
 * So have a 'getRemoteVersion' call ask once and store version, so next time it returns cached answer.
 *
 * May want to have timeout on cache entries, but can be long. Don't expect remote side to change often within lifetime of client.
 */
unsigned getRemoteVersion(ISocket *origSock, StringBuffer &ver)
{
    // used to have a global critical section here
    if (!origSock)
        return 0;

    Owned<ISocket> socket = checkSocketSecure(origSock);

    unsigned ret;
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetver);
    sendbuf.append((unsigned)RFCgetver);
    MemoryBuffer reply;
    try
    {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, reply, 1 ,4096);
        unsigned errCode;
        reply.read(errCode);
        if (errCode==RFSERR_InvalidCommand)
        {
            ver.append("DS V1.0");
            return 10;
        }
        else if (errCode==0)
            ret = 11;
        else if (errCode<0x10000)
            return 0;
        else
            ret = errCode-0x10000;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
        return 0;
    }
    StringAttr vers;
    reply.read(vers);
    ver.append(vers);
    return ret;
}

/////////////////////////


//////////////

extern unsigned stopRemoteServer(ISocket * socket)
{
    // used to have a global critical section here
    if (!socket)
        return 0;
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCstop);
    sendbuf.append((unsigned)RFCstop);
    MemoryBuffer replybuf;
    unsigned errCode = RFSERR_InvalidCommand;
    try
    {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        replybuf.read(errCode);
    }
    catch (IJSOCK_Exception *e)
    {
        if ((e->errorCode()!=JSOCKERR_broken_pipe)&&(e->errorCode()!=JSOCKERR_graceful_close)) 
            EXCLOG(e);
        else
            errCode = 0;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
    }
    return errCode;
}

int setDafsTrace(ISocket * socket,byte flags)
{
    if (!socket)
    {
        byte ret = traceFlags;
        traceFlags = flags;
        return ret;
    }
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCsettrace).append(flags);
    MemoryBuffer replybuf;
    try
    {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        int retcode;
        replybuf.read(retcode);
        return retcode;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
    }
    return -1;
}

int setDafsThrottleLimit(ISocket * socket, ThrottleClass throttleClass, unsigned throttleLimit, unsigned throttleDelayMs, unsigned throttleCPULimit, unsigned queueLimit, StringBuffer *errMsg)
{
    assertex(socket);
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCsetthrottle2).append((unsigned)throttleClass).append(throttleLimit);
    sendbuf.append(throttleDelayMs).append(throttleCPULimit).append(queueLimit);
    MemoryBuffer replybuf;
    try
    {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, NORMAL_RETRIES, 1024);
        int retcode;
        replybuf.read(retcode);
        if (retcode && errMsg && replybuf.remaining())
            replybuf.read(*errMsg);
        return retcode;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
    }
    return -1;
}

int getDafsInfo(ISocket * socket, unsigned level, StringBuffer &retstr)
{
    if (!socket)
    {
        retstr.append(VERSTRING);
        return 0;
    }
    MemoryBuffer sendbuf;
    initSendBuffer(sendbuf);
    sendbuf.append((RemoteFileCommandType)RFCgetinfo).append(level);
    MemoryBuffer replybuf;
    try
    {
        sendBuffer(socket, sendbuf);
        receiveBuffer(socket, replybuf, 1);
        int retcode;
        replybuf.read(retcode);
        if (retcode==0)
        {
            StringAttr s;
            replybuf.read(s);
            retstr.append(s);
        }
        return retcode;
    }
    catch (IException *e)
    {
        EXCLOG(e);
        ::Release(e);
    }
    return -1;
}



void remoteExtractBlobElements(const SocketEndpoint &ep,const char * prefix, const char * filename, ExtractedBlobArray & extracted)
{
    Owned<CRemoteFile> file = new CRemoteFile (ep,filename);
    file->remoteExtractBlobElements(prefix, extracted);
}
