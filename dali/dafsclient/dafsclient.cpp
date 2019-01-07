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

#include <atomic>
#include <string>
#include <unordered_map>

#include "platform.h"

#include "jfile.hpp"
#include "jflz.hpp"
#include "jlog.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"

#include "dadfs.hpp"
#include "dafdesc.hpp"

#include "rtlcommon.hpp"
#include "rtldynfield.hpp"
#include "eclhelper_dyn.hpp"

#include "remote.hpp"
#include "dafsclient.hpp"


namespace dafsclient
{

#define DAFILESRV_STREAMREAD_MINVERSION 22
#define DAFILESRV_STREAMGENERAL_MINVERSION 25


class CDFUFile : public CSimpleInterfaceOf<IDFUFileAccess>, implements IDFUFileAccessExt
{
    typedef CSimpleInterfaceOf<IDFUFileAccess> PARENT;

    StringAttr name;
    SecAccessFlags accessType = SecAccess_None;
    unsigned expirySecs = 300; // JCSMORE
    Owned<IPropertyTree> metaInfo;

    unsigned numParts = 0;
    bool grouped = false;
    DFUFileType fileType = dft_none;
    Owned<IOutputMetaData> meta;
    Owned<IFileDescriptor> fileDesc;

    mutable MemoryBuffer binLayout;
    mutable CriticalSection decodeJsonCrit;
    mutable bool gotJsonTypeInfo = false;
    mutable StringBuffer jsonLayout;
    StringAttr metaInfoBlobB64;

public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CDFUFile(const char *_name, const char *_metaInfoBlobB64) : name(_name), metaInfoBlobB64(_metaInfoBlobB64)
    {
        MemoryBuffer compressedMetaInfoMb;
        JBASE64_Decode(metaInfoBlobB64, compressedMetaInfoMb);
        MemoryBuffer decompressedMetaInfoMb;
        fastLZDecompressToBuffer(decompressedMetaInfoMb, compressedMetaInfoMb);
        Owned<IPropertyTree> metaInfoEnvelope = createPTree(decompressedMetaInfoMb);
        StringBuffer metaInfoSignature;
        if (metaInfoEnvelope->getProp("signature", metaInfoSignature))
        {
#if !defined(_USE_OPENSSL) || !defined(_WIN32)
            // implies will not be able to use this to communicate with secure dafilesrv's
#endif
            MemoryBuffer metaInfoBlob;
            metaInfoEnvelope->getPropBin("metaInfoBlob", metaInfoBlob);

            metaInfo.setown(createPTree(metaInfoBlob));
        }
        else
            metaInfo.set(metaInfoEnvelope);

        IPropertyTree *fileInfo = metaInfo->queryPropTree("FileInfo");
        unsigned metaInfoVersion = metaInfo->getPropInt("version");
        switch (metaInfoVersion)
        {
            case 0:
                // implies unsigned direct request from engines (on unsecure port)
                // fall through
            case 1:
            {
                // old metaInfo, reconstruct a IFileDescriptor for ease of compatibility with rest of code

                unsigned numParts = fileInfo->getCount("Part");

                // calculate part mask
                const char *path = fileInfo->queryProp("Part[1]/Copy[1]/@filePath");
                StringBuffer dir, fname, ext;
                splitFilename(path, &dir, &dir, &fname, &ext);
                VStringBuffer partMask("%s._$P$_of_%u", fname.str(), numParts);

                // reconstruct group
                SocketEndpointArray eps;
                bool replicated = false;
                Owned<IPropertyTreeIterator> iter = fileInfo->getElements("Part");
                ForEach(*iter)
                {
                    IPropertyTree &part = iter->query();
                    if (part.hasProp("Copy[2]"))
                        replicated = true;
                    const char *host = part.queryProp("Copy[1]/@host");
                    SocketEndpoint ep(host);
                    eps.append(ep);
                }
                StringBuffer groupText;
                eps.getText(groupText);
                Owned<IGroup> group = createIGroup(eps);
                ClusterPartDiskMapSpec mspec;
                mspec.defaultCopies = replicated?DFD_DefaultCopies:DFD_NoCopies;

                fileDesc.setown(createFileDescriptor());
                fileDesc->setDefaultDir(dir.str());
                fileDesc->setNumParts(numParts);
                fileDesc->setPartMask(partMask);
                fileDesc->addCluster(group, mspec);
                break;
            }
            case 2: // serialized compact IFileDescriptor
            {
                fileDesc.setown(deserializeFileDescriptorTree(fileInfo, nullptr, IFDSF_EXCLUDE_PARTS));
                break;
            }
        }

        // JCSMORE - could/should I use fileDesc->queryKind() here?
        if (isFileKey(fileDesc))
            fileType = dft_index;
        else
            fileType = dft_flat;

        grouped = fileDesc->isGrouped();
        numParts = fileDesc->numParts();

        if (!metaInfo->getPropBin("binLayout", binLayout))
            throwUnexpected();
        meta.setown(createTypeInfoOutputMetaData(binLayout, grouped));
        binLayout.reset(0);
    }
    void buildInitRequest(StringBuffer &request)
    {
        Owned<IPropertyTree> requestTree = createPTree();
    }
    const MemoryBuffer &queryBinTypeInfo() const
    {
        return binLayout;
    }
    const char *queryMetaInfoBlob() const
    {
        return metaInfoBlobB64;
    }
// IDFUFileAccessExt
    virtual IOutputMetaData *queryOutputMeta() const override
    {
        return meta;
    }
    virtual IFileDescriptor &queryFileDescriptor() const
    {
        return *fileDesc;
    }
    virtual IPropertyTree &queryProperties() const override
    {
        return fileDesc->queryProperties();
    }
// IDFUFileAccess impl.
    virtual const char *queryName() const override
    {
        return name;
    }
    virtual unsigned queryNumParts() const override
    {
        return numParts;
    }
    virtual SecAccessFlags queryAccessType() const
    {
        return accessType;
    }
    virtual bool queryIsGrouped() const override
    {
        return grouped;
    }
    virtual DFUFileType queryType() const override
    {
        return fileType;
    }
    virtual bool queryIsCompressed() const override
    {
        return fileDesc->isCompressed();
    }
    virtual const SocketEndpoint &queryPartEndpoint(unsigned part, unsigned copy=0) const override
    {
        return fileDesc->queryNode(part, copy)->endpoint();
    }
    virtual const char *queryJSONTypeInfo() const override
    {
        CriticalBlock b(decodeJsonCrit);
        if (!gotJsonTypeInfo)
        {
            Owned<IRtlFieldTypeDeserializer> deserializer = createRtlFieldTypeDeserializer();
            const RtlTypeInfo *typeInfo = deserializer->deserialize(binLayout.reset(0));
            if (!dumpTypeInfo(jsonLayout, typeInfo))
                throwUnexpected();
            gotJsonTypeInfo = true;
        }
        return jsonLayout;
    }
    virtual const char *queryECLTypeInfo() const override
    {
        // JCSMORE
        CriticalBlock b(decodeJsonCrit);
        if (!gotJsonTypeInfo)
        {
            Owned<IRtlFieldTypeDeserializer> deserializer = createRtlFieldTypeDeserializer();
            const RtlTypeInfo *typeInfo = deserializer->deserialize(binLayout.reset(0));
            if (!dumpTypeInfo(jsonLayout, typeInfo))
                throwUnexpected();
            gotJsonTypeInfo = true;
        }
        return jsonLayout;
    }
// NB: the intention is for a IDFUFileAccess to be used to create instances for multiple parts, but not to mix types.
    virtual IDFUFilePartReader *createFilePartReader(unsigned p, unsigned copy, IOutputMetaData *outMeta) override;
    virtual IDFUFilePartWriter *createFilePartWriter(unsigned p) override;

    virtual IDFUFileAccessExt *queryEngineInterface() { return this; }
};

IDFUFileAccess *createDFUFileAccess(const char *name, const char *metaInfoBlobB64)
{
    return new CDFUFile(name, metaInfoBlobB64);
}


class CDaFileSrvClientBase : public CRemoteBase, implements IDFUFilePartBase
{
    typedef CRemoteBase PARENT;

protected:
    Linked<CDFUFile> file;
    unsigned part = 0;
    unsigned copy = 0;
    Owned<IPropertyTree> requestTree;
    IPropertyTree *requestNode = nullptr;
    size32_t jsonRequestStartPos = 0;
    size32_t jsonRequestEndPos = 0;
    MemoryBuffer sendMb;
    RemoteFileIOHandle handle = 0;
    unsigned serverVersion = 0;
    unsigned rowStreamReplyMb = 1024; // MB

    bool checkAccess(SecAccessFlags accessWanted)
    {
        return 0 != (file->queryAccessType() & accessWanted);
    }
    void markJsonStart()
    {
        sendMb.append((size32_t)0); // placeholder
        jsonRequestStartPos = sendMb.length();
    }
    void markJsonEnd()
    {
        jsonRequestEndPos = sendMb.length();
        size32_t jsonRequestLen = jsonRequestEndPos - jsonRequestStartPos;
        sendMb.writeEndianDirect(jsonRequestStartPos-sizeof(size32_t), sizeof(size32_t), &jsonRequestLen);
    }
    void serializeJsonRequest(IPropertyTree *tree)
    {
        StringBuffer jsonStr;
#if _DEBUG
        toJSON(tree, jsonStr, 2);
#else
        toJSON(tree, jsonStr, 0, 0);
#endif
        sendMb.append(jsonStr.length(), jsonStr); // NB: if there was a IOStream to MemoryBuffer impl, could use that to avoid encoding to string, and then appending.
    }
    void addRequest(IPropertyTree *tree, RemoteFileCommandType legacyCmd=RFCunknown)
    {
        if (serverVersion < DAFILESRV_STREAMGENERAL_MINVERSION)
        {
            assertex(legacyCmd != RFCunknown);
            sendMb.append(legacyCmd);
            serializeJsonRequest(tree);
        }
        else
        {
            sendMb.append((RemoteFileCommandType)RFCStreamGeneral);
            markJsonStart();
            serializeJsonRequest(tree);
            markJsonEnd();
        }
    }
    unsigned send(MemoryBuffer &reply)
    {
        sendRemoteCommand(sendMb, reply);
        unsigned newHandle;
        reply.read(newHandle);
        return newHandle;
    }
    void establishServerVersion()
    {
        if (serverVersion)
            return;
        StringBuffer str;
        serverVersion = getRemoteVersion(str); // NB: may also connect in the process
        if (0 == serverVersion)
            throw makeStringExceptionV(0, "CDaFileSrvClientBase: Failed to connect to %s", ep.getUrlStr(str).str());

        if (serverVersion < DAFILESRV_STREAMREAD_MINVERSION)
            throw makeStringExceptionV(0, "CDaFileSrvClientBase: serversion(%u), too old connect to %s", serverVersion, ep.getUrlStr(str).str());
    }
    void start()
    {
        establishServerVersion(); // JCSMORE - ensure cache involved behind the scenes
    }
    void close()
    {
        PARENT::close(handle);
    }
public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CDaFileSrvClientBase(CDFUFile *_file, unsigned _part, unsigned _copy) : file(_file), part(_part), copy(_copy)
    {
        requestTree.setown(createPTree());
        requestTree->setProp("format", "binary");
        requestTree->setPropInt("replyLimit", rowStreamReplyMb);
        if (queryOutputCompressionDefault())
            requestTree->setProp("commCompression", queryOutputCompressionDefault());
        requestNode = requestTree->setPropTree("node");
        ep = file->queryPartEndpoint(part);

        // NB: these are 1 based
        requestNode->setPropInt("filePart", part+1);
        requestNode->setPropInt("filePartCopy", copy+1);
        const MemoryBuffer &binLayout = file->queryBinTypeInfo();
        StringBuffer typeInfoStr;
        JBASE64_Encode(binLayout.toByteArray(), binLayout.length(), typeInfoStr, false);
        requestNode->setProp("inputBin", typeInfoStr.str());
        requestNode->setProp("metaInfo", file->queryMetaInfoBlob());
    }
    virtual void beforeDispose() override
    {
        try
        {
            finalize();
        }
        catch (IException *e)
        {
            EXCLOG(e, nullptr);
            e->Release();
        }
    }
// IDFUFilePartBase impl.
    virtual void finalize() override
    {
        close();
    }
};

class CDFUPartReader : public CDaFileSrvClientBase, implements IDFUFilePartReader, implements ISerialStream
{
    typedef CDaFileSrvClientBase PARENT;

    MemoryBuffer replyMb;
    Owned<IExpander> expander;
    MemoryBuffer expandMb;
    size32_t bufRemaining = 0;
    offset_t bufPos = 0;
    bool endOfStream = false;
    std::unordered_map<std::string, std::string> virtualFields;
    Owned<IOutputRowDeserializer> rowDeserializer;
    Owned<ISourceRowPrefetcher> rowPrefetcher;
    CThorContiguousRowBuffer prefetchBuffer;
    bool grouped = false;
    bool eog = false;
    bool eoi = false;
    Linked<IOutputMetaData> outMeta;
    const RtlRecord *outRecord = nullptr;
    const byte *bufPtr = nullptr;
    offset_t currentReadPos = 0;
    bool virtualFieldsDirty = false;
    bool started = false;
    bool pendingFinishRow = false;

    void ensureAvailable()
    {
        replyMb.read(bufRemaining);
        endOfStream = (bufRemaining == 0); // NB: if true, a cursorLength of 0 will follow.
        if (expander && !endOfStream)
        {
            size32_t expandedSz = expander->init(replyMb.bytes()+replyMb.getPos());
            expandMb.clear().reserve(expandedSz);
            expander->expand(expandMb.bufferBase());
            expandMb.swapWith(replyMb);
        }
        bufPtr = replyMb.bytes()+replyMb.getPos();
        // NB: continuation cursor (with leading length) follows the row data in replyMb, 0 if no more row data
    }
    void setVirtualFields()
    {
        if (virtualFieldsDirty)
        {
            virtualFieldsDirty = false;
            IPropertyTree *virtualFieldsTree = requestNode->setPropTree("virtualFields");
            for (auto &e : virtualFields)
                virtualFieldsTree->setProp(e.first.c_str(), e.second.c_str());
        }
    }
    unsigned sendReadStart()
    {
        setVirtualFields();
        sendMb.clear();
        initSendBuffer(sendMb);
        addRequest(requestTree, RFCStreamRead);

        unsigned newHandle = send(replyMb.clear());
        ensureAvailable(); // reads from replyMb
        return newHandle;
    }
    unsigned sendReadContinuation(MemoryBuffer &newReply)
    {
        sendMb.clear();
        initSendBuffer(sendMb);
        Owned<IPropertyTree> tree = createPTree();
        tree->setPropInt("handle", handle);
        addRequest(tree, RFCStreamRead);

        sendRemoteCommand(sendMb, newReply);
        unsigned newHandle;
        newReply.read(newHandle);
        return newHandle;
    }
    void extendReplyMb(size32_t wanted)
    {
        if (0 == bufRemaining)
        {
            refill();
            return;
        }
        MemoryBuffer newReplyMb;
        size32_t oldRemaining = bufRemaining;
        size32_t leadingSpace = bufRemaining;
        size32_t hdrSize = sizeof(handle) + sizeof(bufRemaining);
        if (leadingSpace > hdrSize)
            leadingSpace -= hdrSize;
        // reserve space to insert what remains in current replyMb into new, to ensure contiguous
        newReplyMb.ensureCapacity(leadingSpace);

        // ensures gets in one go
        if (wanted>rowStreamReplyMb) // unlikely
            requestTree->setPropInt("replyLimit", wanted);

        unsigned newHandle = sendReadContinuation(newReplyMb);
        if (newHandle == handle)
            replyMb.swapWith(newReplyMb);
        else // dafilesrv did not recognize handle, send cursor
        {
            assertex(newHandle == 0);
            // resend original request with cursor
            size32_t cursorLength;
            replyMb.skip(bufRemaining);
            replyMb.read(cursorLength);
            requestTree->setPropBin("cursorBin", cursorLength, replyMb.readDirect(cursorLength));
            handle = sendReadStart(); // new handle
        }
        ensureAvailable(); // reads from replyMb
        // now have read new handle and new bufRemaining, copy it back to reserved space
        replyMb.writeDirect(0, oldRemaining, newReplyMb.readDirect(oldRemaining)); // NB: newReplyMb was swapped and refers to old here
        bufRemaining += oldRemaining;
        bufPtr = replyMb.bytes();
        replyMb.reset(0); // read pos
    }
    void refill()
    {
        if (!started)
            throw makeStringException(0, "CDFUPartReader - not started");
        size32_t cursorLength;
        if (replyMb.needSwapEndian())
            _cpyrevn(&cursorLength, bufPtr, sizeof(cursorLength));
        else
            memcpy(&cursorLength, bufPtr, sizeof(cursorLength));
        if (!cursorLength)
        {
            endOfStream = true;
            return;
        }
        MemoryBuffer newReply;
        unsigned newHandle = sendReadContinuation(newReply);
        if (newHandle == handle)
            replyMb.swapWith(newReply);
        else // dafilesrv did not recognize handle, send cursor
        {
            assertex(newHandle == 0);
            // resend original request with cursor
            requestTree->setPropBin("cursorBin", cursorLength, replyMb.readDirect(cursorLength));
            handle = sendReadStart(); // new handle
        }
        ensureAvailable(); // reads from replyMb
    }
// ISerialStream impl.
    virtual const void *peek(size32_t wanted, size32_t &got) override
    {
        if (bufRemaining >= wanted)
        {
            got = bufRemaining;
            return bufPtr;
        }
        extendReplyMb(wanted);
        got = bufRemaining;
        return bufPtr;
    }
    virtual void get(size32_t len, void * ptr) override               // exception if no data available
    {
        while (len)
        {
            if (0 == bufRemaining)
            {
                refill();
                if (0 == bufRemaining)
                    throw makeStringException(0, "CDFUPartReader::get(): end of stream");
            }
            size32_t r = len>bufRemaining ? bufRemaining : len;
            memcpy(ptr, bufPtr, r);
            len -= r;
            bufRemaining -= r;
            bufPtr += r;
            currentReadPos += r;
        }
    }
    virtual bool eos() override
    {
        if (!eoi)
        {
            if (0 == bufRemaining)
            {
                refill();
                if (0 == bufRemaining)
                    eoi = true;
            }
        }
        return eoi;
    }
    virtual void skip(size32_t len) override
    {
        // same as get() without the memcpy
        while (len)
        {
            if (0 == bufRemaining)
            {
                refill();
                if (0 == bufRemaining)
                    throw makeStringException(0, "CDFUPartReader::skip(): end of stream");
            }
            size32_t r = len>bufRemaining ? bufRemaining : len;
            len -= r;
            bufRemaining -= r;
            bufPtr += r;
            currentReadPos += r;
        }
    }
    virtual offset_t tell() const override
    {
        return currentReadPos;
    }
    virtual void reset(offset_t _offset,offset_t _flen=(offset_t)-1) override
    {
        throwUnexpected();
    }
public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CDFUPartReader(CDFUFile *file, unsigned part, unsigned copy, IOutputMetaData *_outMeta) : CDaFileSrvClientBase(file, part, copy), outMeta(_outMeta), prefetchBuffer(nullptr)
    {
        checkAccess(SecAccess_Read);
        grouped = file->queryIsGrouped();

        if (outMeta && (outMeta != file->queryOutputMeta()))
        {
            MemoryBuffer projectedTypeInfo;
            dumpTypeInfo(projectedTypeInfo, outMeta->querySerializedDiskMeta()->queryTypeInfo());
            StringBuffer typeInfoStr;
            JBASE64_Encode(projectedTypeInfo.toByteArray(), projectedTypeInfo.length(), typeInfoStr, false);
            requestNode->setProp("outputBin", typeInfoStr.str());
        }
        else
            outMeta.set(file->queryOutputMeta());

        outRecord = &outMeta->queryRecordAccessor(true);

        rowDeserializer.set(outMeta->createDiskDeserializer(nullptr, 1));
        rowPrefetcher.setown(outMeta->createDiskPrefetcher());
        assertex(rowPrefetcher);
        prefetchBuffer.setStream(this);

        if (file->queryIsCompressed())
            requestNode->setPropBool("compressed", true);
        if (grouped)
            requestNode->setProp("inputGrouped", "true");

        switch (file->queryType())
        {
            case dft_flat:
                requestNode->setProp("kind", "diskread");
                break;
            case dft_index:
            {
                requestNode->setProp("kind", "indexread");
                break;
            }
            default:
                throwUnexpected();
        }

        if (queryOutputCompressionDefault())
        {
            expander.setown(getExpander(queryOutputCompressionDefault()));
            if (expander)
            {
                expandMb.setEndian(__BIG_ENDIAN);
                requestNode->setProp("commCompression", queryOutputCompressionDefault());
            }
            else
                WARNLOG("Failed to created compression decompressor for: %s", queryOutputCompressionDefault());
        }
    }
// IDFUFilePartReader impl.
    virtual void start() override
    {
        PARENT::start();
        eog = false;
        eoi = false;
        pendingFinishRow = false;
        started = true;
        handle = sendReadStart();
    }
    virtual const void *nextRow(size32_t &sz) override
    {
        if (eog)
            eog = false;
        else if (!eoi)
        {
            if (pendingFinishRow)
            {
                pendingFinishRow = false;
                prefetchBuffer.finishedRow();
            }
            if (prefetchBuffer.eos())
            {
                eoi = true;
                return nullptr;
            }
            rowPrefetcher->readAhead(prefetchBuffer);
            const byte * row = prefetchBuffer.queryRow();
            sz = prefetchBuffer.queryRowSize();
            pendingFinishRow = true;
            if (grouped)
                prefetchBuffer.read(sizeof(eog), &eog);
            return row;
        }
        return nullptr;
    }
    virtual void finalize() override
    {
        PARENT::finalize();
        started = false;
    }
    virtual void addVirtualFieldMapping(const char *fieldName, const char *fieldValue) override
    {
        virtualFieldsDirty = true;
        virtualFields[fieldName] = fieldValue;
    }
    virtual IOutputMetaData *queryOutputMeta() override
    {
        return outMeta;
    }
};


class CDFUPartWriterBase : public CDaFileSrvClientBase, implements IDFUFilePartWriter
{
    typedef CDaFileSrvClientBase PARENT;

    MemoryBuffer replyMb;
    bool firstSend = true;
    unsigned startPos = 0;

protected:
    const unsigned sendThresholdBytes = 0x100000; // 1MB

    void prepNext()
    {
        sendMb.clear();
        // prepare for next continuation
        initSendBuffer(sendMb);
        Owned<IPropertyTree> tree = createPTree();
        tree->setPropInt("handle", handle);
        addRequest(tree);
        startPos = sendMb.length();
    }
    void _sendWriteFirst()
    {
        unsigned newHandle = send(replyMb.clear());
        if (!newHandle)
            throwStringExceptionV(DAFSERR_cmdstream_generalwritefailure, "Error whilst writing data to file: '%s'", file->queryName());
        else if (handle && (newHandle != handle))
            throwStringExceptionV(DAFSERR_cmdstream_unknownwritehandle, "Unknown write handle whilst remote writing to file: '%s'", file->queryName());
        handle = newHandle;
    }
    void sendWriteFirst()
    {
        unsigned newHandle = send(replyMb.clear());
        if (!newHandle)
            throwStringExceptionV(DAFSERR_cmdstream_generalwritefailure, "Error whilst writing data to file: '%s'", file->queryName());
        else if (handle && (newHandle != handle))
            throwStringExceptionV(DAFSERR_cmdstream_unknownwritehandle, "Unknown write handle whilst remote writing to file: '%s'", file->queryName());
        handle = newHandle;
        prepNext();
    }
    unsigned sendWriteContinuation()
    {
        MemoryBuffer newReplyMb;
        unsigned newHandle = send(newReplyMb.clear());
        if (newHandle == handle)
            replyMb.swapWith(newReplyMb);
        else // dafilesrv did not recognize handle, send cursor
        {
            assertex(newHandle == 0);
            // resend original request with cursor
            size32_t cursorLength;
            replyMb.read(cursorLength);
            requestTree->setPropBin("cursorBin", cursorLength, replyMb.readDirect(cursorLength));
            initSendBuffer(sendMb);
            addRequest(requestTree);
            _sendWriteFirst(); // new handle
        }
        prepNext();
        return newHandle;
    }
    void sendWrite()
    {
        if (firstSend) // for 1st send, want to send even if no record, so that file is at least created
        {
            firstSend = false;
            sendWriteFirst();
        }
        else if (sendMb.length() > startPos) // if anything to send
            sendWriteContinuation();
    }
public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CDFUPartWriterBase(CDFUFile *file, unsigned part) : CDaFileSrvClientBase(file, part, 0)
    {
    }
// IDFUFilePartWriter impl.
    virtual void start() override
    {
        PARENT::start();
        sendMb.clear();
        initSendBuffer(sendMb);
        addRequest(requestTree);
    }
    virtual void write(size32_t sz, const void *row) override
    {
        sendMb.append(sz, row);
        if (sendMb.length() > sendThresholdBytes)
            sendWrite();
    }
    virtual void finalize() override
    {
        sendWrite();
        PARENT::finalize();
    }
    virtual IOutputMetaData *queryOutputMeta() override
    {
        return file->queryOutputMeta();
    }
};

static const char *defaultCompressionType = "LZ4";
class CDFUPartFlatWriter : public CDFUPartWriterBase
{
    StringAttr eclRecDef;
public:
    CDFUPartFlatWriter(CDFUFile *file, unsigned part) : CDFUPartWriterBase(file, part)
    {
        checkAccess(SecAccess_Write);
        if (dft_flat != file->queryType())
            throw makeStringExceptionV(0, "CDFUPartFlatWriter: invalid file type: %u", file->queryType());

        requestNode->setProp("kind", "diskwrite");
        if (file->queryIsCompressed())
            requestNode->setProp("compressed", defaultCompressionType);
        requestNode->setProp("inputGrouped", boolToStr(file->queryIsGrouped()));
    }
};

class CDFUPartIndexWriter : public CDFUPartWriterBase
{
    StringAttr eclRecDef;
public:
    CDFUPartIndexWriter(CDFUFile *file, unsigned part) : CDFUPartWriterBase(file, part)
    {
        checkAccess(SecAccess_Write);
        if (dft_index != file->queryType())
            throw makeStringExceptionV(0, "CDFUPartIndexWriter: invalid file type: %u", file->queryType());

        requestNode->setProp("kind", "indexwrite");
    }
};



IDFUFilePartReader *CDFUFile::createFilePartReader(unsigned p, unsigned copy, IOutputMetaData *outMeta)
{
    return new CDFUPartReader(this, p, copy, outMeta);
}

IDFUFilePartWriter *CDFUFile::createFilePartWriter(unsigned p)
{
    switch (fileType)
    {
        case dft_flat:
            return new CDFUPartFlatWriter(this, p);
        case dft_index:
            return new CDFUPartIndexWriter(this, p);
        default:
            throwUnexpected();
    }
}

////////////

IRowWriter *createRowWriter(IDFUFilePartWriter *partWriter)
{
    class CRowWriter : public CSimpleInterfaceOf<IRowWriter>, protected IRowSerializerTarget
    {
        Linked<IDFUFilePartWriter> partWriter;
        IOutputMetaData *meta = nullptr;
        Owned<IOutputRowSerializer> serializer;
        unsigned nesting = 0;
        MemoryBuffer nested;

    // IRowSerializerTarget impl.
        virtual void put(size32_t len, const void * ptr) override
        {
            if (nesting)
                nested.append(len, ptr);
            else
                partWriter->write(len, ptr);
        }
        virtual size32_t beginNested(size32_t count) override
        {
            nesting++;
            unsigned pos = nested.length();
            nested.append((size32_t)0);
            return pos;
        }
        virtual void endNested(size32_t sizePos) override
        {
            size32_t sz = nested.length()-(sizePos + sizeof(size32_t));
            nested.writeDirect(sizePos,sizeof(sz),&sz);
            nesting--;
            if (!nesting)
            {
                partWriter->write(nested.length(), nested.toByteArray());
                nested.clear();
            }
        }
    public:
        CRowWriter(IDFUFilePartWriter *_partWriter) : partWriter(_partWriter)
        {
            meta = partWriter->queryOutputMeta();
            serializer.setown(meta->createDiskSerializer(nullptr, 1)); // JCSMORE - are these params ok?
        }
        virtual void putRow(const void *row) override
        {
            serializer->serialize(*this, (const byte *)row);
        }
        virtual void flush() override
        {
            // flushing internal to partWriter
        }
    };
    return new CRowWriter(partWriter);
}

IRowStream *createRowStream(IDFUFilePartReader *partReader)
{
    class CRowStream : public CSimpleInterfaceOf<IRowStream>
    {
        IDFUFilePartReader *partReader;
    public:
        CRowStream(IDFUFilePartReader *_partReader) : partReader(_partReader)
        {
        }
        virtual const void *nextRow() override
        {
            return nullptr;
        }
        virtual void stop() override
        {
        }
    };
    return new CRowStream(partReader);
}


} // namespace dafsclient
