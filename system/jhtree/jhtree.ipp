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

#ifndef _JHTREEI_INCL
#define _JHTREEI_INCL

#include "jmutex.hpp"
#include "jhutil.hpp"
#include "jqueue.tpp"
#include "ctfile.hpp"

#include "jhtree.hpp"

typedef OwningStringHTMapping<IKeyIndex> CKeyIndexMapping;
typedef OwningStringSuperHashTableOf<CKeyIndexMapping> CKeyIndexTable;
typedef CMRUCacheMaxCountOf<const char *, IKeyIndex, CKeyIndexMapping, CKeyIndexTable> CKeyIndexMRUCache;

typedef class MapStringToMyClass<IKeyIndex> KeyCache;

class CKeyStore
{
private:
    Mutex mutex;
    Mutex idmutex;
    CKeyIndexMRUCache keyIndexCache;
    int nextId;
    int getUniqId() { synchronized procedure(idmutex); return ++nextId; }
    IKeyIndex *doload(const char *fileName, unsigned crc, IReplicatedFile *part, IFileIO *iFileIO, IMemoryMappedFile *iMappedFile, bool isTLK, bool allowPreload);
public:
    CKeyStore();
    ~CKeyStore();
    IKeyIndex *load(const char *fileName, unsigned crc, bool isTLK, bool allowPreload);
    IKeyIndex *load(const char *fileName, unsigned crc, IFileIO *iFileIO, bool isTLK, bool allowPreload);
    IKeyIndex *load(const char *fileName, unsigned crc, IMemoryMappedFile *iMappedFile, bool isTLK, bool allowPreload);
    IKeyIndex *load(const char *fileName, unsigned crc, IReplicatedFile &part, bool isTLK, bool allowPreload);
    IKeyIndex *load(const char *name, unsigned crc, size32_t sz, const void *data, bool isTLK);
    void clearCache(bool killAll);
    void clearCacheEntry(const char *name);
    void clearCacheEntry(const IFileIO *io);
    unsigned setKeyCacheLimit(unsigned limit);
    StringBuffer &getMetrics(StringBuffer &xml);
    void resetMetrics();
};

class CNodeCache;
enum request { LTE, GTE };

// INodeLoader impl.
interface INodeLoader
{
    virtual CJHTreeNode *loadNode(offset_t offset) = 0;
    virtual CJHTreeNode *getNode(offset_t offset, IContextLogger *ctx)  = 0;
    virtual CJHTreeBlobNode *getBlobNode(offset_t nodepos) = 0;
};

class jhtree_decl CKeyIndexBase : public CInterfaceOf<IKeyIndex>, implements INodeLoader
{
    typedef CInterfaceOf<IKeyIndex> PARENT;
    friend class CKeyStore;
    friend class CKeyCursor;

private:
    CKeyIndexBase(CKeyIndexBase &);

protected:
    int iD;
    StringAttr name;
    CKeyHdr *keyHdr;
    CJHTreeNode *rootNode;
    RelaxedAtomic<unsigned> keySeeks;
    RelaxedAtomic<unsigned> keyScans;
    offset_t latestGetNodeOffset;

    CJHTreeNode *createNode(const void *nodeData, offset_t pos, bool needsCopy);

    CKeyIndexBase(int _iD, const char *_name);
    ~CKeyIndexBase();
    void init(KeyHdr &hdr, bool isTLK);

public:
// IKeyIndex impl.
    virtual IKeyCursor *getCursor(IContextLogger *ctx);

    virtual size32_t keySize();
    virtual bool hasPayload();
    virtual size32_t keyedSize();
    virtual bool isTopLevelKey();
    virtual bool isFullySorted();
    virtual unsigned getFlags() { return (unsigned char)keyHdr->getKeyType(); };

    virtual void dumpNode(FILE *out, offset_t pos, unsigned count, bool isRaw);

    virtual unsigned numParts() { return 1; }
    virtual IKeyIndex *queryPart(unsigned idx) { return idx ? NULL : this; }
    virtual unsigned queryScans() { return keyScans; }
    virtual unsigned querySeeks() { return keySeeks; }
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize);
    virtual offset_t queryBlobHead() { return keyHdr->getHdrStruct()->blobHead; }
    virtual void resetCounts() { keyScans.store(0); keySeeks.store(0); }
    virtual offset_t queryLatestGetNodeOffset() const { return latestGetNodeOffset; }
    virtual offset_t queryMetadataHead();
    virtual IPropertyTree * getMetadata();
    virtual unsigned getNodeSize() { return keyHdr->getNodeSize(); }
    virtual const char *queryFileName() { return name.get(); }
    virtual const IFileIO *queryFileIO() const override { return nullptr; }
    virtual bool hasSpecialFileposition() const;

// INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset) = 0;
    virtual CJHTreeBlobNode *getBlobNode(offset_t nodepos) override;
};

class jhtree_decl CKeyIndex : public CKeyIndexBase
{
    typedef CKeyIndexBase PARENT;

    friend class CKeyStore;
    friend class CKeyCursor;

private:
    CKeyIndex(CKeyIndex &);
    bool preload = false;

protected:
    CriticalSection blobCacheCrit;
    Owned<CJHTreeBlobNode> cachedBlobNode;
    offset_t cachedBlobNodePos;
    CNodeCache *cache;

    CKeyIndex(int _iD, const char *_name);
    ~CKeyIndex();
    void init(KeyHdr &hdr, bool isTLK, bool allowPreload);
    void cacheNodes(CNodeCache *cache, offset_t nodePos, bool isTLK);
    
public:
// INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset) = 0;
    virtual CJHTreeNode *getNode(offset_t offset, IContextLogger *ctx) override;
    virtual CJHTreeBlobNode *getBlobNode(offset_t nodepos) override;
};

class jhtree_decl CMemoryMappedKeyIndex : public CKeyIndex
{
    typedef CKeyIndex PARENT;
private:
    Linked<IMemoryMappedFile> io;
public:
    CMemoryMappedKeyIndex(int _iD, IMemoryMappedFile *_io, const char *_name, bool _isTLK);

// INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset) override;
};

class jhtree_decl CDiskKeyIndex : public CKeyIndex
{
    typedef CKeyIndex PARENT;
private:
    Linked<IFileIO> io;
    void cacheNodes(CNodeCache *cache, offset_t firstnode, bool isTLK);
    
public:
    CDiskKeyIndex(int _iD, IFileIO *_io, const char *_name, bool _isTLK, bool _allowPreload);

    virtual const IFileIO *queryFileIO() const override { return io; }
// INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset);
};

class jhtree_decl CInMemoryKeyIndex : public CKeyIndexBase
{
    typedef CKeyIndexBase PARENT;
private:
    MemoryBuffer indexData;
    PointerArrayOf<CJHTreeNode> nodes;
    CriticalSection crit;
public:
    CInMemoryKeyIndex(int _iD, size32_t indexSz, const void *_indexData, const char *_name, bool isTLK);

// INodeLoader impl.
    virtual CJHTreeNode *loadNode(offset_t offset) override;
    virtual CJHTreeNode *getNode(offset_t offset, IContextLogger *ctx) override;
};

class jhtree_decl CKeyCursor : public IKeyCursor, public CInterface
{
private:
    IContextLogger *ctx;
    CKeyIndexBase &key;
    Owned<CJHTreeNode> node;
    unsigned int nodeKey;
    ConstPointerArray activeBlobs;

    CJHTreeNode *locateFirstNode();
    CJHTreeNode *locateLastNode();

public:
    IMPLEMENT_IINTERFACE;
    CKeyCursor(CKeyIndexBase &_key, IContextLogger *ctx);
    ~CKeyCursor();

    virtual bool next(char *dst);
    virtual bool prev(char *dst);
    virtual bool first(char *dst);
    virtual bool last(char *dst);
    virtual bool gtEqual(const char *src, char *dst, bool seekForward);
    virtual bool ltEqual(const char *src, char *dst, bool seekForward);
    virtual size32_t getSize();
    virtual offset_t getFPos(); 
    virtual void serializeCursorPos(MemoryBuffer &mb);
    virtual void deserializeCursorPos(MemoryBuffer &mb, char *keyBuffer);
    virtual unsigned __int64 getSequence(); 
    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize);
    virtual void releaseBlobs();
    virtual void reset();
};


#endif
