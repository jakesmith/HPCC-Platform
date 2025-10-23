/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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
#include "jio.hpp"
#include "jmutex.hpp"
#include "jfile.hpp"
#include "jthread.hpp"
#include "jstring.hpp"
#include "jsecrets.hpp"
#include "jplane.hpp"
#include "azureblob2.hpp"
#include "azureapiutils.hpp"

#include <curl/curl.h>
#include <memory>
#include <vector>
#include <atomic>
#include <future>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <azure/core/base64.hpp>

// Macro for conditional tracing
#define AZURE2_TRACE if (traceAzureAPI) DBGLOG

static bool traceAzureAPI = false;

//---------------------------------------------------------------------------------------------------------------------
// Helper functions for Azure Shared Key authentication
//---------------------------------------------------------------------------------------------------------------------

static std::string getCurrentHttpDate()
{
    time_t now = time(nullptr);
    struct tm timeinfo;
    gmtime_r(&now, &timeinfo);
    
    char buffer[100];
    strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S GMT", &timeinfo);
    return std::string(buffer);
}

static std::string createAzureSharedKeySignature(
    const char* accountName,
    const char* accountKey,
    const char* method,
    const char* urlPath,
    const char* httpDate,
    const char* rangeValue = nullptr)
{
    // Azure Shared Key Lite signature - simplified for GET/HEAD requests
    std::string canonicalizedResource = "/";
    canonicalizedResource.append(accountName).append(urlPath);
    
    // Build string to sign
    std::string stringToSign;
    stringToSign.append(method).append("\n");  // VERB
    stringToSign.append("\n");  // Content-Encoding
    stringToSign.append("\n");  // Content-Language
    stringToSign.append("\n");  // Content-Length
    stringToSign.append("\n");  // Content-MD5
    stringToSign.append("\n");  // Content-Type
    stringToSign.append("\n");  // Date (empty when using x-ms-date)
    stringToSign.append("\n");  // If-Modified-Since
    stringToSign.append("\n");  // If-Match
    stringToSign.append("\n");  // If-None-Match
    stringToSign.append("\n");  // If-Unmodified-Since
    if (rangeValue)
        stringToSign.append(rangeValue);
    stringToSign.append("\n");  // Range
    stringToSign.append("x-ms-date:").append(httpDate).append("\n");
    stringToSign.append("x-ms-version:2020-04-08\n");
    stringToSign.append(canonicalizedResource);
    
    // Decode account key from base64
    std::vector<uint8_t> keyBytes = Azure::Core::Convert::Base64Decode(accountKey);
    
    // HMAC-SHA256 signature
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hashLen = 0;
    
    HMAC(EVP_sha256(),
         keyBytes.data(), keyBytes.size(),
         reinterpret_cast<const unsigned char*>(stringToSign.c_str()), stringToSign.length(),
         hash, &hashLen);
    
    // Base64 encode the signature
    std::vector<uint8_t> hashVec(hash, hash + hashLen);
    return Azure::Core::Convert::Base64Encode(hashVec);
}

static struct curl_slist* addAzureAuthHeaders(
    struct curl_slist* headers,
    const char* accountName,
    const char* accountKey,
    const char* method,
    const char* urlPath,
    const char* rangeValue = nullptr)
{
    std::string httpDate = getCurrentHttpDate();
    std::string signature = createAzureSharedKeySignature(accountName, accountKey, method, urlPath, httpDate.c_str(), rangeValue);
    
    // Add Azure authentication headers
    std::string authHeader = "Authorization: SharedKey ";
    authHeader.append(accountName).append(":").append(signature);
    headers = curl_slist_append(headers, authHeader.c_str());
    
    std::string dateHeader = "x-ms-date: ";
    dateHeader.append(httpDate);
    headers = curl_slist_append(headers, dateHeader.c_str());
    
    headers = curl_slist_append(headers, "x-ms-version: 2020-04-08");
    
    return headers;
}

//---------------------------------------------------------------------------------------------------------------------
// Fast parallel chunk downloader using native libcurl with HTTP/2
//---------------------------------------------------------------------------------------------------------------------

class FastAzureBlobDownloader
{
public:
    struct ChunkDownload
    {
        offset_t offset;
        size_t length;
        byte* target;
        std::string errorMsg;
        bool success = false;
        unsigned __int64 downloadCycles = 0;
    };

    static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp)
    {
        ChunkDownload* chunk = static_cast<ChunkDownload*>(userp);
        size_t totalSize = size * nmemb;
        memcpy(chunk->target, contents, totalSize);
        chunk->target += totalSize;
        return totalSize;
    }

private:

    void downloadChunk(ChunkDownload& chunk, const char* fullUrl)
    {
        CCycleTimer timer;
        CURL* curl = curl_easy_init();
        if (!curl)
        {
            chunk.errorMsg = "Failed to initialize curl";
            return;
        }

        // Build Range header
        char rangeHeader[128];
        snprintf(rangeHeader, sizeof(rangeHeader), "Range: bytes=%llu-%llu", 
                 chunk.offset, chunk.offset + chunk.length - 1);

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, rangeHeader);

        // Configure curl for optimal performance
        curl_easy_setopt(curl, CURLOPT_URL, fullUrl);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &chunk);
        
        // Enable HTTP/2 (libcurl will negotiate, fallback to HTTP/1.1 if needed)
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
        
        // Performance optimizations
        curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 512L * 1024);  // 512KB buffer per connection
        curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);          // Disable Nagle's algorithm
        curl_easy_setopt(curl, CURLOPT_SSL_SESSIONID_CACHE, 1L);  // Reuse SSL sessions
        
        // Timeout settings
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);  // 5 minute total timeout
        
        // Follow redirects
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5L);

        CURLcode res = curl_easy_perform(curl);
        
        if (res == CURLE_OK)
        {
            long responseCode;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
            if (responseCode == 206)  // 206 Partial Content expected for range requests
            {
                chunk.success = true;
                AZURE2_TRACE("FastAzureBlobDownloader: Downloaded chunk offset=%llu, length=%zu in %llu cycles", 
                           chunk.offset, chunk.length, timer.elapsedCycles());
            }
            else
            {
                chunk.errorMsg.clear();
                chunk.errorMsg.append("Expected HTTP 206, got ").append(std::to_string(responseCode));
            }
        }
        else
        {
            chunk.errorMsg = curl_easy_strerror(res);
        }

        chunk.downloadCycles = timer.elapsedCycles();
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }

public:
    void downloadParallel(
        const char* fullUrl,
        offset_t fileSize, 
        void* buffer,
        size_t chunkSize,
        unsigned maxParallel)
    {
        if (fileSize == 0)
            return;

        std::vector<ChunkDownload> chunks;
        byte* targetBuffer = static_cast<byte*>(buffer);

        // Create chunks
        for (offset_t offset = 0; offset < fileSize; offset += chunkSize)
        {
            size_t length = (size_t)std::min((offset_t)chunkSize, fileSize - offset);
            chunks.push_back({offset, length, targetBuffer + offset, "", false, 0});
        }

        AZURE2_TRACE("FastAzureBlobDownloader: Starting parallel download of %zu chunks (%llu bytes total, %zu bytes per chunk, %u parallel)",
                   chunks.size(), fileSize, chunkSize, maxParallel);

        CCycleTimer totalTimer;

        // Download in parallel batches
        for (size_t i = 0; i < chunks.size(); i += maxParallel)
        {
            std::vector<std::future<void>> futures;
            
            for (size_t j = 0; j < maxParallel && (i + j) < chunks.size(); j++)
            {
                size_t chunkIdx = i + j;
                futures.push_back(std::async(std::launch::async, 
                    [this, &chunks, chunkIdx, fullUrl]() {
                        downloadChunk(chunks[chunkIdx], fullUrl);
                    }));
            }
            
            // Wait for batch to complete
            for (auto& f : futures)
                f.get();
        }

        // Check for errors
        for (auto& chunk : chunks)
        {
            if (!chunk.success)
            {
                throw MakeStringException(1234, "Chunk download failed at offset %llu: %s", 
                                        chunk.offset, chunk.errorMsg.c_str());
            }
        }

        AZURE2_TRACE("FastAzureBlobDownloader: Parallel download completed in %llu cycles (%zu chunks, %llu total bytes)",
                   totalTimer.elapsedCycles(), chunks.size(), fileSize);
    }
};

//---------------------------------------------------------------------------------------------------------------------

class FastAzureBlob;

class FastAzureBlobReadIO : implements CInterfaceOf<IFileIO>
{
public:
    FastAzureBlobReadIO(FastAzureBlob* _file);

    virtual void close() override {}
    
    virtual offset_t size() override;
    virtual size32_t read(offset_t pos, size32_t len, void* data) override;
    
    virtual unsigned __int64 getStatistic(StatisticKind kind) override;

    virtual IFile* queryFile() const override;

    // Write methods not implemented - this is read-only
    virtual size32_t write(offset_t pos, size32_t len, const void* data) override
    {
        throwUnexpectedX("Writing to read only file");
    }
    virtual void setSize(offset_t size) override
    {
        throwUnexpectedX("Setting size of read only azure file");
    }
    virtual void flush() override {}

private:
    Linked<FastAzureBlob> file;
    FileIOStats stats;
    bool useParallelDownload;
    size_t parallelChunkSize;
    unsigned maxParallelConnections;
};

//---------------------------------------------------------------------------------------------------------------------

class FastAzureBlob : implements CInterfaceOf<IFile>
{
public:
    FastAzureBlob(const char* _azureFileName);

    virtual bool exists() override
    {
        ensureMetaData();
        return fileExists;
    }
    
    virtual bool getTime(CDateTime* createTime, CDateTime* modifiedTime, CDateTime* accessedTime) override;
    
    virtual fileBool isDirectory() override
    {
        ensureMetaData();
        if (!fileExists)
            return fileBool::notFound;
        return fileBool::foundNo;  // Fast implementation doesn't support directories
    }
    
    virtual fileBool isFile() override
    {
        ensureMetaData();
        if (!fileExists)
            return fileBool::notFound;
        return fileBool::foundYes;
    }
    
    virtual fileBool isReadOnly() override
    {
        ensureMetaData();
        if (!fileExists)
            return fileBool::notFound;
        return fileBool::foundYes;
    }
    
    virtual IFileIO* open(IFOmode mode, IFEflags extraFlags=IFEnone) override
    {
        return openShared(mode, IFSHread, extraFlags);
    }
    
    virtual IFileAsyncIO* openAsync(IFOmode mode) override
    {
        UNIMPLEMENTED;
    }
    
    virtual IFileIO* openShared(IFOmode mode, IFSHmode shmode, IFEflags extraFlags=IFEnone) override
    {
        if (mode == IFOcreate)
            throwUnexpectedX("Fast Azure implementation is read-only");
        assertex(mode == IFOread);
        return new FastAzureBlobReadIO(this);
    }
    
    virtual const char* queryFilename() override
    {
        return fullName.str();
    }
    
    virtual offset_t size() override
    {
        ensureMetaData();
        return fileSize;
    }

    // Unsupported operations for fast read-only implementation
    virtual IDirectoryIterator* directoryFiles(const char* mask, bool sub, bool includeDirs) override
    {
        UNIMPLEMENTED_X("FastAzureBlob::directoryFiles");
    }
    
    virtual bool getInfo(bool& isdir, offset_t& size, CDateTime& modtime) override
    {
        ensureMetaData();
        isdir = false;
        size = fileSize;
        modtime.set(lastModified);
        return fileExists;
    }
    
    virtual bool setTime(const CDateTime* createTime, const CDateTime* modifiedTime, const CDateTime* accessedTime) override
    {
        DBGLOG("FastAzureBlob::setTime ignored (read-only)");
        return false;
    }
    
    virtual bool remove() override
    {
        throwUnexpectedX("FastAzureBlob is read-only");
    }
    
    virtual void rename(const char* newTail) override { UNIMPLEMENTED_X("FastAzureBlob::rename"); }
    virtual void move(const char* newName) override { UNIMPLEMENTED_X("FastAzureBlob::move"); }
    virtual void setReadOnly(bool ro) override { UNIMPLEMENTED_X("FastAzureBlob::setReadOnly"); }
    virtual void setFilePermissions(unsigned fPerms) override
    {
        DBGLOG("FastAzureBlob::setFilePermissions() ignored");
    }
    virtual bool setCompression(bool set) override { UNIMPLEMENTED_X("FastAzureBlob::setCompression"); }
    virtual offset_t compressedSize() override { UNIMPLEMENTED_X("FastAzureBlob::compressedSize"); }
    virtual unsigned getCRC() override { UNIMPLEMENTED_X("FastAzureBlob::getCRC"); }
    virtual void setCreateFlags(unsigned short cflags) override { UNIMPLEMENTED_X("FastAzureBlob::setCreateFlags"); }
    virtual void setShareMode(IFSHmode shmode) override { UNIMPLEMENTED_X("FastAzureBlob::setSharedMode"); }
    virtual bool createDirectory() override
    {
        throwUnexpectedX("FastAzureBlob is read-only");
    }
    virtual IDirectoryDifferenceIterator* monitorDirectory(
        IDirectoryIterator* prev=NULL,
        const char* mask=NULL,
        bool sub=false,
        bool includedirs=false,
        unsigned checkinterval=60*1000,
        unsigned timeout=(unsigned)-1,
        Semaphore* abortsem=NULL) override { UNIMPLEMENTED_X("FastAzureBlob::monitorDirectory"); }
    virtual void copySection(const RemoteFilename& dest, offset_t toOfs=(offset_t)-1, offset_t fromOfs=0, offset_t size=(offset_t)-1, ICopyFileProgress* progress=NULL, CFflags copyFlags=CFnone) override { UNIMPLEMENTED_X("FastAzureBlob::copySection"); }
    virtual void copyTo(IFile* dest, size32_t buffersize=DEFAULT_COPY_BLKSIZE, ICopyFileProgress* progress=NULL, bool usetmp=false, CFflags copyFlags=CFnone) override { UNIMPLEMENTED_X("FastAzureBlob::copyTo"); }
    virtual IMemoryMappedFile* openMemoryMapped(offset_t ofs=0, memsize_t len=(memsize_t)-1, bool write=false) override { UNIMPLEMENTED_X("FastAzureBlob::openMemoryMapped"); }

public:
    std::string getBlobUrl() const;
    std::string getAccountKey() const;
    std::string getUrlPath() const;  // Returns /container/blob
    const char* getAccountName() const { return accountName.get(); }
    
private:
    void ensureMetaData();
    void gatherMetaData();

private:
    StringBuffer fullName;
    StringAttr accountName;
    StringAttr containerName;
    StringBuffer secretName;
    StringAttr blobName;
    offset_t fileSize = unknownFileSize;
    time_t lastModified = 0;
    bool haveMeta = false;
    bool fileExists = false;
    bool useManagedIdentity = false;
    CriticalSection cs;
};

//---------------------------------------------------------------------------------------------------------------------

FastAzureBlobReadIO::FastAzureBlobReadIO(FastAzureBlob* _file)
: file(_file)
{
    // Get configuration for parallel downloads
    useParallelDownload = getExpertOptBool("azureFastParallelDownload", true);
    parallelChunkSize = (size_t)getExpertOptInt64("azureFastChunkSize", 16 * 1024 * 1024);  // 16MB default
    maxParallelConnections = (unsigned)getExpertOptInt64("azureFastMaxParallel", 4);  // 4 parallel connections default
    
    AZURE2_TRACE("FastAzureBlobReadIO created: parallel=%s, chunkSize=%zu, maxParallel=%u, file=%s",
               useParallelDownload ? "yes" : "no", parallelChunkSize, maxParallelConnections,
               file->queryFilename());
}

IFile* FastAzureBlobReadIO::queryFile() const
{
    return file.get();
}

offset_t FastAzureBlobReadIO::size()
{
    return file->size();
}

size32_t FastAzureBlobReadIO::read(offset_t pos, size32_t len, void* data)
{
    AZURE2_TRACE("FastAzureBlobReadIO::read() called: pos=%llu, len=%u, file=%s", 
               pos, len, file->queryFilename());
    
    CCycleTimer timer;
    offset_t fileSize = file->size();
    
    if (pos >= fileSize)
        return 0;
    
    if (pos + len > fileSize)
        len = (size32_t)(fileSize - pos);
    
    if (len == 0)
        return 0;

    try
    {
        std::string blobUrl = file->getBlobUrl();
        std::string urlPath = file->getUrlPath();
        std::string accountKey = file->getAccountKey();
        
        // For large reads, use parallel download
        if (useParallelDownload && len > parallelChunkSize)
        {
            AZURE2_TRACE("FastAzureBlobReadIO::read() using parallel download: len=%u, chunkSize=%zu, maxParallel=%u",
                       len, parallelChunkSize, maxParallelConnections);
            
            // TODO: Implement parallel download with Shared Key auth
            throwUnexpectedX("Parallel download not yet implemented with Shared Key auth");
        }
        else
        {
            // For small reads, use single-threaded download
            AZURE2_TRACE("FastAzureBlobReadIO::read() using single-threaded download: len=%u", len);
            
            FastAzureBlobDownloader::ChunkDownload chunk{pos, len, static_cast<byte*>(data), "", false, 0};
            
            CURL* curl = curl_easy_init();
            if (!curl)
                throw MakeStringException(1234, "Failed to initialize curl");
            
            // Build range value for signature
            char rangeValue[64];
            snprintf(rangeValue, sizeof(rangeValue), "bytes=%llu-%llu", pos, pos + len - 1);
            
            // Create authentication headers with range
            struct curl_slist* headers = nullptr;
            headers = addAzureAuthHeaders(headers, file->getAccountName(), accountKey.c_str(), "GET", urlPath.c_str(), rangeValue);
            
            // Add Range header
            char rangeHeader[128];
            snprintf(rangeHeader, sizeof(rangeHeader), "Range: %s", rangeValue);
            headers = curl_slist_append(headers, rangeHeader);
            
            curl_easy_setopt(curl, CURLOPT_URL, blobUrl.c_str());
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, FastAzureBlobDownloader::WriteCallback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &chunk);
            curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
            curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 512L * 1024);
            curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
            curl_easy_setopt(curl, CURLOPT_SSL_SESSIONID_CACHE, 1L);
            curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);
            curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
            
            CURLcode res = curl_easy_perform(curl);
            
            if (res != CURLE_OK)
            {
                curl_slist_free_all(headers);
                curl_easy_cleanup(curl);
                throw MakeStringException(1234, "Curl request failed: %s", curl_easy_strerror(res));
            }
            
            long responseCode;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
            
            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);
            
            if (responseCode != 206 && responseCode != 200)
                throw MakeStringException(1234, "Unexpected HTTP response code: %ld", responseCode);
        }
        
        stats.ioReads.fastAdd(1);
        stats.ioReadCycles.fastAdd(timer.elapsedCycles());
        stats.ioReadBytes.fastAdd(len);
        
        AZURE2_TRACE("FastAzureBlobReadIO::read() completed: pos=%llu, len=%u, elapsed=%llu cycles",
                   pos, len, timer.elapsedCycles());
        
        return len;
    }
    catch (IException* e)
    {
        EXCLOG(e, "FastAzureBlobReadIO::read() failed");
        throw;
    }
    catch (std::exception& e)
    {
        throw MakeStringException(1234, "FastAzureBlobReadIO::read() failed: %s", e.what());
    }
}

unsigned __int64 FastAzureBlobReadIO::getStatistic(StatisticKind kind)
{
    return stats.getStatistic(kind);
}

//---------------------------------------------------------------------------------------------------------------------

FastAzureBlob::FastAzureBlob(const char* _azureFileName) : fullName(_azureFileName)
{
    // Parse azure blob URL format: azureblob:plane[/device]/path
    if (!startsWith(fullName, azureBlobPrefix))
        throw makeStringExceptionV(99, "Unexpected prefix on azure filename %s", fullName.str());

    const char* filename = fullName + strlen(azureBlobPrefix);
    const char* slash = strchr(filename, '/');
    if (!slash)
        throw makeStringException(99, "Missing / in azureblob: file reference");

    StringBuffer planeName(slash - filename, filename);
    Owned<const IPropertyTree> plane = getStoragePlaneConfig(planeName, true);
    const IPropertyTree* storageapi = plane->queryPropTree("storageapi");
    if (!storageapi)
        throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName.str());
    
    filename = slash + 1;

    const char* api = storageapi->queryProp("@type");
    if (!api)
        throw makeStringExceptionV(99, "No storage api defined for plane %s", planeName.str());

    StringBuffer azureBlobAPI(strlen(azureBlobPrefix) - 1, azureBlobPrefix);
    if (!strieq(api, azureBlobAPI.str()))
        throw makeStringExceptionV(99, "Storage api for plane %s is not azureblob", planeName.str());

    useManagedIdentity = storageapi->getPropBool("@managed", false);
    
    unsigned numDevices = plane->getPropInt("@numDevices", 1);
    unsigned device = 1;
    
    if (numDevices != 1)
    {
        if (filename[0] != 'd')
            throw makeStringExceptionV(99, "Expected a device number in the filename %s", fullName.str());

        char* endDevice = nullptr;
        device = strtod(filename + 1, &endDevice);
        if ((device == 0) || (device > numDevices))
            throw makeStringExceptionV(99, "Device %d out of range for plane %s", device, planeName.str());

        if (!endDevice || (*endDevice != '/'))
            throw makeStringExceptionV(99, "Unexpected end of device partition %s", fullName.str());

        filename = endDevice + 1;
    }

    VStringBuffer childPath("containers[%u]", device);
    const IPropertyTree* deviceInfo = storageapi->queryPropTree(childPath);
    if (!deviceInfo)
        throw makeStringExceptionV(99, "Missing container specification for device %u in plane %s", device, planeName.str());

    containerName.set(deviceInfo->queryProp("@name"));
    accountName.set(deviceInfo->queryProp("@account"));
    secretName.set(deviceInfo->queryProp("@secret"));

    if (isEmptyString(containerName))
        throw makeStringExceptionV(99, "Missing container name for plane %s", planeName.str());

    if (isEmptyString(accountName))
        throw makeStringExceptionV(99, "Missing account name for plane %s", planeName.str());

    if (!useManagedIdentity && isEmptyString(secretName))
        throw makeStringExceptionV(99, "Missing secret name for plane %s", planeName.str());

    blobName.set(filename);
    
    // Read tracing flag
    traceAzureAPI = getExpertOptBool("traceAzureAPI", false);
    
    AZURE2_TRACE("FastAzureBlob created: %s (account=%s, container=%s, blob=%s, managed=%s)",
               fullName.str(), accountName.get(), containerName.get(), blobName.get(),
               useManagedIdentity ? "yes" : "no");
}

std::string FastAzureBlob::getBlobUrl() const
{
    std::string url("https://");
    url.append(accountName.get()).append(".blob.core.windows.net/")
       .append(containerName.get()).append("/").append(blobName.get());
    return url;
}

std::string FastAzureBlob::getUrlPath() const
{
    std::string path("/");
    path.append(containerName.get()).append("/").append(blobName.get());
    return path;
}

std::string FastAzureBlob::getAccountKey() const
{
    if (useManagedIdentity)
    {
        throwUnexpectedX("Managed identity not yet implemented in FastAzureBlob - use shared key");
    }
    
    // Get the account key from secrets (same as AzureBlob uses)
    StringBuffer key;
    getSecretValue(key, "storage", secretName, "key", true);
    
    // Trim trailing whitespace/newlines (same as azureapiutils.cpp does)
    size32_t len = key.length();
    for (;;)
    {
        if (!len)
            break;
        if (isBase64Char(key.charAt(len-1)))
            break;
        len--;
    }
    key.setLength(len);
    
    return std::string(key.str());
}

void FastAzureBlob::ensureMetaData()
{
    CriticalBlock block(cs);
    if (haveMeta)
        return;
    
    gatherMetaData();
    haveMeta = true;
}

void FastAzureBlob::gatherMetaData()
{
    AZURE2_TRACE("FastAzureBlob::gatherMetaData() called for %s", queryFilename());
    
    try
    {
        std::string blobUrl = getBlobUrl();
        std::string urlPath = getUrlPath();
        std::string accountKey = getAccountKey();
        
        CURL* curl = curl_easy_init();
        if (!curl)
            throw MakeStringException(1234, "Failed to initialize curl");
        
        // Create authentication headers for HEAD request
        struct curl_slist* headers = nullptr;
        headers = addAzureAuthHeaders(headers, accountName.get(), accountKey.c_str(), "HEAD", urlPath.c_str());
        
        // Use HEAD request to get metadata
        curl_easy_setopt(curl, CURLOPT_URL, blobUrl.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);  // HEAD request
        curl_easy_setopt(curl, CURLOPT_HEADER, 1L);  // Include headers in callback
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);
        
        CURLcode res = curl_easy_perform(curl);
        
        if (res == CURLE_OK)
        {
            long responseCode;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
            
            if (responseCode == 200)
            {
                double contentLength;
                curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &contentLength);
                
                fileExists = true;
                fileSize = (offset_t)contentLength;
                lastModified = time(nullptr);  // Could parse Last-Modified header if needed
                
                AZURE2_TRACE("FastAzureBlob::gatherMetaData() success: size=%llu", fileSize);
            }
            else if (responseCode == 404)
            {
                fileExists = false;
                fileSize = unknownFileSize;
                AZURE2_TRACE("FastAzureBlob::gatherMetaData() file not found");
            }
            else
            {
                throw MakeStringException(1234, "Unexpected HTTP response code: %ld", responseCode);
            }
        }
        else
        {
            throw MakeStringException(1234, "Curl HEAD request failed: %s", curl_easy_strerror(res));
        }
        
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }
    catch (IException* e)
    {
        EXCLOG(e, "FastAzureBlob::gatherMetaData() failed");
        fileExists = false;
        fileSize = unknownFileSize;
        throw;
    }
}

bool FastAzureBlob::getTime(CDateTime* createTime, CDateTime* modifiedTime, CDateTime* accessedTime)
{
    ensureMetaData();
    if (createTime)
        createTime->clear();
    if (modifiedTime)
    {
        modifiedTime->clear();
        modifiedTime->set(lastModified);
    }
    if (accessedTime)
        accessedTime->clear();
    return fileExists;
}

//---------------------------------------------------------------------------------------------------------------------

IFile* createFastAzureBlob(const char* azureFileName)
{
    return new FastAzureBlob(azureFileName);
}
