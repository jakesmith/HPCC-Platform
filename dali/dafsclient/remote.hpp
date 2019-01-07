/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef REMOTE_HPP
#define REMOTE_HPP

#ifdef DAFSCLIENT_EXPORTS
#define DAFSCLIENT_API DECL_EXPORT
#else
#define DAFSCLIENT_API DECL_IMPORT
#endif

#include "jfile.hpp"
#include "dafscommon.hpp"


enum
{
    RFCopenIO,                                      // 0
    RFCcloseIO,
    RFCread,
    RFCwrite,
    RFCsize,
    RFCexists,
    RFCremove,
    RFCrename,
    RFCgetver,
    RFCisfile,
    RFCisdirectory,                                 // 10
    RFCisreadonly,
    RFCsetreadonly,
    RFCgettime,
    RFCsettime,
    RFCcreatedir,
    RFCgetdir,
    RFCstop,
    RFCexec,                                        // legacy cmd removed
    RFCdummy1,                                      // legacy placeholder
    RFCredeploy,                                    // 20
    RFCgetcrc,
    RFCmove,
// 1.5 features below
    RFCsetsize,
    RFCextractblobelements,
    RFCcopy,
    RFCappend,
    RFCmonitordir,
    RFCsettrace,
    RFCgetinfo,
    RFCfirewall,    // not used currently          // 30
    RFCunlock,
    RFCunlockreply,
    RFCinvalid,
    RFCcopysection,
// 1.7e
    RFCtreecopy,
// 1.7e - 1
    RFCtreecopytmp,
// 1.8
    RFCsetthrottle, // legacy version
// 1.9
    RFCsetthrottle2,
    RFCsetfileperms,
// 2.0
    RFCreadfilteredindex,    // No longer used     // 40
    RFCreadfilteredindexcount,
    RFCreadfilteredindexblob,
// 2.2
    RFCStreamRead,                                 // 43
// 2.4
    RFCStreamReadTestSocket,                       // 44
// 2.5
    RFCStreamGeneral,                              // 45
    RFCStreamReadJSON = '{',
    RFCmaxnormal,
    RFCmax,
    RFCunknown = 255 // 0 would have been more sensible, but can't break backward compatibility
};


typedef unsigned char RemoteFileCommandType;

const char *getRFCText(RemoteFileCommandType cmd);
const char *getRFSERRText(unsigned err);


IDAFS_Exception *createDafsException(int code, const char *msg);
IDAFS_Exception *createDafsExceptionVA(int code, const char *format, va_list args) __attribute__((format(printf,2,0)));
IDAFS_Exception *createDafsExceptionVA(int code, const char *format, va_list args);
IDAFS_Exception *createDafsExceptionV(int code, const char *format, ...) __attribute__((format(printf,2,3)));
IDAFS_Exception *createDafsExceptionV(int code, const char *format, ...);

MemoryBuffer & initSendBuffer(MemoryBuffer & buff);

typedef int RemoteFileIOHandle;

class CRemoteBase : public CInterface
{
    Owned<ISocket>          socket;
    static  SocketEndpoint  lastfailep;
    static unsigned         lastfailtime;
    DAFSConnectCfg          connectMethod;

    void connectSocket(SocketEndpoint &ep, unsigned localConnectTime=0, unsigned localRetries=0);
    void killSocket(SocketEndpoint &tep);

protected: friend class CRemoteFileIO;

    StringAttr          filename;
    CriticalSection     crit;
    SocketEndpoint      ep;

    void sendRemoteCommand(MemoryBuffer & src, MemoryBuffer & reply, bool retry=true, bool lengthy=false, bool handleErrCode=true);
    void sendRemoteCommand(MemoryBuffer & src, bool retry);
    void throwUnauthenticated(const IpAddress &ip,const char *user,unsigned err=0);
    void sendAuthentication(const IpAddress &serverip);
    void close(RemoteFileIOHandle handle);

public:
    SocketEndpoint  &queryEp() { return ep; };


    CRemoteBase(const SocketEndpoint &_ep, const char * _filename);
    CRemoteBase();
    void disconnect();
    const char *queryLocalName();
    void send(MemoryBuffer &msg, MemoryBuffer &reply);
    unsigned getRemoteVersion(StringBuffer &ver);
};


#define FILESRV_VERSION 25 // don't forget VERSTRING in sockfile.cpp

interface IKeyManager;
interface IDelayedFile;

extern DAFSCLIENT_API IFile * createRemoteFile(SocketEndpoint &ep,const char * _filename);
extern DAFSCLIENT_API unsigned getRemoteVersion(ISocket * _socket, StringBuffer &ver);
extern DAFSCLIENT_API unsigned stopRemoteServer(ISocket * _socket);
extern DAFSCLIENT_API const char *remoteServerVersionString();
extern DAFSCLIENT_API int setDafsTrace(ISocket * socket,byte flags);
extern DAFSCLIENT_API int setDafsThrottleLimit(ISocket * socket, ThrottleClass throttleClass, unsigned throttleLimit, unsigned throttleDelayMs, unsigned throttleCPULimit, unsigned queueLimit, StringBuffer *errMsg=NULL);
extern DAFSCLIENT_API bool enableDafsAuthentication(bool on);
extern DAFSCLIENT_API void remoteExtractBlobElements(const SocketEndpoint &ep, const char * prefix, const char * filename, ExtractedBlobArray & extracted);
extern DAFSCLIENT_API int getDafsInfo(ISocket * socket, unsigned level, StringBuffer &retstr);
extern DAFSCLIENT_API void setDafsEndpointPort(SocketEndpoint &ep);
extern DAFSCLIENT_API void setDafsLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir);
extern DAFSCLIENT_API ISocket *connectDafs(SocketEndpoint &ep, unsigned timeoutms); // NOTE: might alter ep.port if configured for multiple ports ...
extern DAFSCLIENT_API ISocket *checkSocketSecure(ISocket *socket);


extern DAFSCLIENT_API void setRemoteOutputCompressionDefault(const char *type);
extern DAFSCLIENT_API const char *queryOutputCompressionDefault();

interface IOutputMetaData;
class RowFilter;
interface IRemoteFileIO : extends IFileIO
{
    virtual void addVirtualFieldMapping(const char *fieldName, const char *fieldValue) = 0;
    virtual void ensureAvailable() = 0;
};
extern DAFSCLIENT_API IRemoteFileIO *createRemoteFilteredFile(SocketEndpoint &ep, const char * filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, bool compressed, bool grouped, unsigned __int64 chooseNLimit);

interface IIndexLookup;
extern DAFSCLIENT_API IIndexLookup *createRemoteFilteredKey(SocketEndpoint &ep, const char * filename, unsigned crc, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, unsigned __int64 chooseNLimit);
extern DAFSCLIENT_API IRowWriter *createRemoteFlatFileWriter(SocketEndpoint &ep, const char *filename, IOutputMetaData *actual, unsigned compMethod, bool grouped);

extern DAFSCLIENT_API bool queryDafsAuthentication();
extern DAFSCLIENT_API const char *getThrottleClassText(ThrottleClass throttleClass);

extern DAFSCLIENT_API RemoteFileCommandType queryRemoteStreamCmd(); // used by testsocket only
interface IRemoteDirectoryDifferenceIterator : extends IDirectoryDifferenceIterator
{
    virtual bool appendBuf(MemoryBuffer &_buf) = 0;
};

extern DAFSCLIENT_API IRemoteDirectoryDifferenceIterator *createRemoteDirectorIterator(SocketEndpoint &ep, const char *dir);
extern DAFSCLIENT_API bool serializeRemoteDirectory(MemoryBuffer &mb, IDirectoryIterator *iter, size32_t bufsize, bool first);
extern DAFSCLIENT_API void serializeRemoteDirectoryDiff(MemoryBuffer &mb,IDirectoryDifferenceIterator *iter);

// client only (for use by this library only)
extern void clientSetDaliServixSocketCaching(bool set);
extern void clientDisconnectRemoteFile(IFile *file);
extern void clientDisconnectRemoteIoOnExit(IFileIO *fileio,bool set);

extern bool clientResetFilename(IFile *file, const char *newname); // returns false if not remote

extern bool clientAsyncCopyFileSection(const char *uuid,    // from genUUID - must be same for subsequent calls
                        IFile *from,                        // expected to be remote
                        RemoteFilename &to,
                        offset_t toofs,                     // (offset_t)-1 created file and copies to start
                        offset_t fromofs,
                        offset_t size,                      // (offset_t)-1 for all file
                        ICopyFileProgress *progress,
                        unsigned timeout                    // 0 to start, non-zero to wait
                        ); // returns true when done

extern void clientSetRemoteFileTimeouts(unsigned maxconnecttime,unsigned maxreadtime);
extern void clientAddSocketToCache(SocketEndpoint &ep,ISocket *socket);

#endif
