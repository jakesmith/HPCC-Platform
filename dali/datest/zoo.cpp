/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "platform.h"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "jtime.hpp"

#include <zookeeper/zookeeper.h>

#include "zoo.hpp"

#define ZOOLOCK_READ 1
#define ZOOLOCK_WRITE 2
#define LOCKPATH_PREFIX "/_locks_"
#define LOCKNAME_POSTFIX "guid-lock-"
#define LOCKNAME_READPOSTFIX LOCKNAME_POSTFIX "rd-"
#define LOCKNAME_WRITEPOSTFIX LOCKNAME_POSTFIX "wr-"
#define LOCKNAME_POSTFIXLEN (sizeof(LOCKNAME_READPOSTFIX))  // write/read same length for ease

namespace zoo {

class CZooException : public CSimpleInterface, implements IException
{
    int zerror;
    StringAttr msg;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface)
    CZooException(int _zerror, const char *_msg) : zerror(_zerror), msg(_msg)
    {
        PrintStackReport();
    }
// implements IException
    virtual int errorCode() const { return zerror; }
    virtual StringBuffer &errorMessage(StringBuffer &out) const
    {
        out.append(msg).append(": ");
        switch (zerror)
        {
            case ZNONODE:
                out.append("ZNONODE - the parent node does not exist.");
                break;
            case ZNODEEXISTS:
                out.append("ZNODEEXISTS the node already exists.");
                break;
            case ZNOAUTH:
                out.append("ZNOAUTH the client does not have permission.");
                break;
            case ZNOCHILDRENFOREPHEMERALS:
                out.append("ZNOCHILDRENFOREPHEMERALS cannot create children of ephemeral nodes.");
                break;
            case ZBADARGUMENTS:
                out.append("ZBADARGUMENTS - invalid input parameters.");
                break;
            case ZINVALIDSTATE:
                out.append("ZINVALIDSTATE - zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE.");
                break;
            case ZMARSHALLINGERROR:
                out.append("ZMARSHALLINGERROR - failed to marshall a request; possibly, out of memory.");
                break;
            default:
                throwUnexpected();
        }
        return out;
    }
    virtual MessageAudience errorAudience() const { return MSGAUD_user; }
};

class CWatcherContext : public CSimpleInterface
{
    Semaphore sem;
    StringAttr path;
public:
    CWatcherContext(const char *_path) : path(_path)
    {
    }
    const char *queryFindString() const { return path; }
    bool wait(unsigned timeout)
    {
        return sem.wait(timeout);
    }
    void notify(int type, int state, const char *path)
    {
        if (ZOO_DELETED_EVENT == type)
            sem.signal();
    }
};

class CMyZooKeeper : public CSimpleInterface, implements IZooClient
{
    class CLock : public CSimpleInterface, implements ILock
    {
        CMyZooKeeper &owner;
        StringAttr lockPath, uniqueLockPath;
        unsigned mode;
        long lockId;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface)

        CLock(CMyZooKeeper &_owner, const char *_lockPath, unsigned _mode) : owner(_owner), lockPath(_lockPath), mode(_mode)
        {
            StringBuffer lockProtocolPath;
            constructLockPath(lockPath, mode, lockProtocolPath);
            StringBuffer rPath;
            owner.ensurePath(lockProtocolPath.str(), ZOO_EPHEMERAL|ZOO_SEQUENCE, &rPath);
            uniqueLockPath.set(rPath);
            lockId = CMyZooKeeper::getPathId(uniqueLockPath.get(), lockProtocolPath.str());
        }
        ~CLock()
        {
            if (uniqueLockPath.length())
                owner.deleteNode(uniqueLockPath);
        }
        StringBuffer &getPath(StringBuffer &result)
        {
            return result.append(uniqueLockPath);
        }
        bool doLock(unsigned timeout, long selfLockId=-1)
        {
            /* Strategy is:
                   1) create unique lock via ZOO_SEQUENCE under path, either wr-xxx, or rd-xxx
                   2) Look at any existing locks under path
                   3) If none, gained lock
                   4) If some, find highest below my sequence (only look at writes if gaining read lock)
                   5) Call zoo_wexists on next highest, which will either return non-existent, or causes callback
                   6) Wait on callback with timeout
                   7) Loop 2)
                */
            CTimeMon tm(timeout);
            loop
            {
                String_vector children;
                int ret = zoo_get_children(owner.getHandle(), lockPath, 0, &children);
                if (ZOK != ret)
                    throw new CZooException(ret, "zoo_get_children");
                StringAttr highestChild;
                long highestOtherWriteLockId = -1;
                for (unsigned c=0; c<children.count; c++)
                {
                    const char *child = children.data[c];
                    long otherLockId = getPathId(child, lockPath);
                    if (-1 == selfLockId || selfLockId != otherLockId) // ignore selfLockId if provided for changeMode
                    {
                        if ((mode == ZOOLOCK_WRITE) || (0 == strncmp(child, LOCKNAME_WRITEPOSTFIX, strlen(LOCKNAME_WRITEPOSTFIX))))
                        {
                            if (otherLockId < lockId && otherLockId > highestOtherWriteLockId)
                            {
                                highestOtherWriteLockId = otherLockId;
                                highestChild.set(child);
                            }
                        }
                    }
                }
                if (-1 == highestOtherWriteLockId)
                    return true;
                StringBuffer highestOtherLockPath; // reconstruct path to highest write path
                CMyZooKeeper::constructLockPath(lockPath, highestChild, highestOtherLockPath);

                Owned<CWatcherContext> watcher = new CWatcherContext(highestOtherLockPath.str());
                // wait for the other lock to go away.
                struct Stat stat;
                ret = zoo_wexists(owner.getHandle(), highestOtherLockPath.str(), &sDeleteWatcher, watcher, &stat);
                if (ZOK == ret)
                {
                    PROGLOG("Waiting on existing lock: %s", highestOtherLockPath.str());
                    unsigned remaining;
                    if (tm.timedout(&remaining))
                        break;
                    if (!watcher->wait(remaining))
                        break;
                }
            }
            return false;
        }
        bool changeMode(unsigned newMode, unsigned timeout)
        {
            if (mode == newMode)
                return true; // nothing to do

            StringBuffer lockProtocolPath;
            constructLockPath(lockPath, newMode, lockProtocolPath);
            StringBuffer newPath, newUniqueLockPath;
            int ret = owner.createNode(lockProtocolPath.str(), ZOO_EPHEMERAL|ZOO_SEQUENCE, &newUniqueLockPath);
            if (ZOK != ret)
                throw new CZooException(ret, "CLock - changeMode");
            try
            {
                if (ZOOLOCK_READ == mode)
                {
                    // from read->write
                    long myLockId = CMyZooKeeper::getPathId(newUniqueLockPath.str(), lockProtocolPath.str());
                    if (!doLock(timeout, myLockId))
                        return false; // NB: leaves current lock in place
                }
                // else write->read, had exclusive lock, so just swap in read
                mode = newMode;
                uniqueLockPath.set(newUniqueLockPath);
                owner.deleteNode(uniqueLockPath);
                return true;
            }
            catch (IException *)
            {
                owner.deleteNode(lockProtocolPath.str());
                throw;
            }
        }
    };
    StringAttr serverList;
    const clientid_t *myZooId;
    zhandle_t *zkHandle;
    SpinLock watchLock;

    static void sMainWatcher(zhandle_t *_zkHandle, int type, int state, const char *path, void *watcherCtx)
    {
        PROGLOG("sMainWatcher event: type=%d, state=%d, path=%s", type, state, (path ? path: "null"));
        CMyZooKeeper *myZooKeeper = static_cast<CMyZooKeeper *>(watcherCtx);
        myZooKeeper->mainWatcher(type, state, path);
    }
    static void sDeleteWatcher(zhandle_t *_zkHandle, int type, int state, const char *path, void *watcherCtx)
    {
        PROGLOG("sDeleteWatcher watcher event: type=%d, state=%d, path=%s", type, state, (path ? path: "null"));
        CWatcherContext *watcher = static_cast<CWatcherContext *>(watcherCtx);
        watcher->notify(type, state, path);
    }
    static long getPathId(const char *path, const char *base)
    {
        size32_t pLen = strlen(path);
        size32_t bLen = strlen(base);
        if (pLen <= bLen+1+LOCKNAME_POSTFIXLEN)
            return -1; // not a ZOO_SEQUENCE path of base
        return atol(path+1+LOCKNAME_POSTFIXLEN); // ZOO_SEQUENCE id's are padded 10 digit
    }
    static StringBuffer &constructLockPath(const char *path, unsigned mode, StringBuffer &result)
    {
        switch (mode)
        {
            case ZOOLOCK_READ:
                return constructLockPath(path, LOCKNAME_READPOSTFIX, result);
            case ZOOLOCK_WRITE:
                return constructLockPath(path, LOCKNAME_WRITEPOSTFIX, result);
            default:
                throwUnexpected();
        }
    }
    static StringBuffer &constructLockPath(const char *path, const char *tail, StringBuffer &result)
    {
        return result.append(LOCKPATH_PREFIX).append('/').append(path).append('/').append(tail);
    }

    void close()
    {
        if (zkHandle)
            zookeeper_close(zkHandle);
    }
    void ensurePath(const char *path, bool uniqueTail, StringBuffer *resultPath=NULL)
    {
        if (!path)
            return;
        PROGLOG("ensurePath = %s", path);
        StringBuffer node;
        if ('/' == *path)
        {
            ++path;
            node.append('/');
        }
        const char *end = path+strlen(path);
        loop
        {
            const char *next = strchr(path, '/');
            int flags = 0;
            if (next)
                node.append(next-path, path);
            else
            {
                node.append(end-path, path);
                if (uniqueTail)
                    flags = ZOO_EPHEMERAL|ZOO_SEQUENCE;
            }
            int ret = createNode(node.str(), flags, (!next&&uniqueTail)?resultPath:NULL);
            if (ret != ZOK && ret != ZNODEEXISTS)
            {
                VStringBuffer errMsg("ensurePath: %s", node.str());
                throw new CZooException(ret, errMsg.str());
            }
            if (!next)
                break;
            node.append('/');
            path = next+1;
        }
    }
    int createNode(const char *path, int flags=0, StringBuffer *resultPath=NULL)
    {
        size32_t maxPathLen = 0;
        char *_path = NULL;
        if (resultPath)
        {
            maxPathLen = strlen(path) + 10; // in case of ZOO_SEQUENCE
            _path = resultPath->reserveTruncate(maxPathLen);
        }
        PROGLOG("zoo_create(zkHandle=%p, path=%s, NULL, -1, flags=%d, _path=%p, maxPathLen=%d", zkHandle, path, flags, _path, maxPathLen);
        return zoo_create(zkHandle, path, NULL, -1, &ZOO_OPEN_ACL_UNSAFE, flags, _path, maxPathLen);
    }
    void deleteNode(const char *path)
    {
        int ret = zoo_delete(zkHandle, path, -1);
        if (ZOK != ret)
            throw new CZooException(ret, "create");
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface)

    CMyZooKeeper(const char *_serverList) : serverList(_serverList)
    {
        zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);
        zkHandle = NULL;
        myZooId = NULL;
    }
    void init()
    {
        zkHandle = zookeeper_init(serverList, sMainWatcher, 10000, NULL, this, 0);
        if (!zkHandle)
            throw MakeErrnoException("Failed zookeeper_init");
    }
    zhandle_t *getHandle() const { return zkHandle; }
    void mainWatcher(int type, int state, const char *path)
    {
        PROGLOG("Instance: main watcher event: type=%d, state=%d, path=%s", type, state, (path ? path: "null"));

        if (type == ZOO_SESSION_EVENT)
        {
            if (state == ZOO_CONNECTED_STATE)
            {
                PROGLOG("Zoo connected, zkHandle");
                myZooId = zoo_client_id(zkHandle);
            }
            else if (state == ZOO_CONNECTING_STATE)
                PROGLOG("Zoo connecting");
            else if (state == ZOO_AUTH_FAILED_STATE)
            {
                PROGLOG("Zoo authentication failure. Shutting down..");
                close();
            }
            else if (state == ZOO_EXPIRED_SESSION_STATE)
            {
                PROGLOG("Session expired. Shutting down..");
                close();
            }
        }
        else if (type == ZOO_CREATED_EVENT)
            PROGLOG("Zoo created event");
        else if (type == ZOO_DELETED_EVENT)
            PROGLOG("Zoo deleted event");
        else if (type == ZOO_CHANGED_EVENT)
            PROGLOG("Zoo changed event");
        else if (type == ZOO_CHILD_EVENT)
            PROGLOG("Zoo child event");
        else if (type == ZOO_NOTWATCHING_EVENT)
            PROGLOG("Zoo notwatching event");
        else
            PROGLOG("Unknown watcher event type %d", type);
    }

// IZooClient implementation
    virtual ILock *createLock(const char *path, unsigned mode, unsigned timeout)
    {
        if (NULL == path || '\0' == *path)
            throwUnexpected();

        StringAttr lockPath;
        // remove any leading & trailing '/' to make consistent
        if (*path == '/')
            ++path;
        if ('/' == (*path+strlen(path)-1))
            lockPath.set(path, strlen(path)-1);
        else
            lockPath.set(path);
        path = lockPath.get();
        PROGLOG("path = %s", path);

        Owned<CLock> lock = new CLock(*this, lockPath, mode);
        if (lock->doLock(timeout))
            return lock.getClear();
        else
            throw MakeStringException(0, "Failed to get lock to: %s", lockPath.get());
        return NULL;
    }
};

IZooClient *createZooClient(const char *serverList)
{
    CMyZooKeeper *zoo = new CMyZooKeeper(serverList);
    zoo->init();
    return zoo;
}


} // namespace zoo
