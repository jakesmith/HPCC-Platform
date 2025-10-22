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
#include "azureapiutils.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jstring.hpp"
#include "jlog.hpp"
#include "jfile.hpp"
#include "jmutex.hpp"
#include "jplane.hpp"
#include "jsecrets.hpp"
#include <curl/curl.h>
#include <azure/core/http/raw_response.hpp>
#include <azure/core/io/body_stream.hpp>
#include <cstdlib>

using namespace std::chrono;

// Common utility functions shared by both blob and file implementations
//---------------------------------------------------------------------------------------------------------------------

namespace HPCC {

OptimizedAzureBlobTransport::OptimizedAzureBlobTransport(
    const Azure::Core::Http::CurlTransportOptions& options)
{
    // Constructor - options could be stored if needed for future enhancements
}

// Callback for writing response data - must not throw exceptions
static size_t WriteCallback(char* ptr, size_t size, size_t nmemb, void* userdata) noexcept
{
    try
    {
        auto* bodyStream = reinterpret_cast<std::vector<uint8_t>*>(userdata);
        
        // Check for overflow
        if (size > 0 && nmemb > SIZE_MAX / size)
        {
            DBGLOG("WriteCallback: Integer overflow detected (size=%zu, nmemb=%zu)", size, nmemb);
            return 0;  // Signal error to curl
        }
        
        size_t totalSize = size * nmemb;
        if (totalSize > 0)
        {
            bodyStream->reserve(bodyStream->size() + totalSize);
            bodyStream->insert(bodyStream->end(), ptr, ptr + totalSize);
        }
        return totalSize;
    }
    catch (const std::exception& e)
    {
        DBGLOG("WriteCallback: Exception caught: %s", e.what());
        return 0;  // Signal error to curl
    }
    catch (...)
    {
        DBGLOG("WriteCallback: Unknown exception caught");
        return 0;  // Signal error to curl
    }
}

// Callback for writing response headers - must not throw exceptions
static size_t HeaderCallback(char* buffer, size_t size, size_t nitems, void* userdata) noexcept
{
    try
    {
        auto* headers = reinterpret_cast<Azure::Core::CaseInsensitiveMap*>(userdata);
        
        // Check for overflow
        if (size > 0 && nitems > SIZE_MAX / size)
        {
            DBGLOG("HeaderCallback: Integer overflow detected (size=%zu, nitems=%zu)", size, nitems);
            return 0;
        }
        
        size_t totalSize = size * nitems;
        if (totalSize == 0)
            return 0;
            
        std::string headerLine(buffer, totalSize);
        
        auto colonPos = headerLine.find(':');
        if (colonPos != std::string::npos)
        {
            std::string key = headerLine.substr(0, colonPos);
            std::string value = headerLine.substr(colonPos + 1);
            
            size_t start = value.find_first_not_of(" \t\r\n");
            size_t end = value.find_last_not_of(" \t\r\n");
            if (start != std::string::npos && end != std::string::npos)
            {
                value = value.substr(start, end - start + 1);
                (*headers)[key] = value;
            }
        }
        
        return totalSize;
    }
    catch (const std::exception& e)
    {
        DBGLOG("HeaderCallback: Exception caught: %s", e.what());
        return 0;  // Signal error to curl
    }
    catch (...)
    {
        DBGLOG("HeaderCallback: Unknown exception caught");
        return 0;  // Signal error to curl
    }
}

// RAII wrapper for curl_slist
class CurlSlistGuard
{
public:
    CurlSlistGuard() : list(nullptr) {}
    ~CurlSlistGuard() { if (list) curl_slist_free_all(list); }
    
    void append(const char* str) { list = curl_slist_append(list, str); }
    struct curl_slist* get() const { return list; }
    
    CurlSlistGuard(const CurlSlistGuard&) = delete;
    CurlSlistGuard& operator=(const CurlSlistGuard&) = delete;
    
private:
    struct curl_slist* list;
};

std::unique_ptr<Azure::Core::Http::RawResponse> OptimizedAzureBlobTransport::Send(
    Azure::Core::Http::Request& request,
    Azure::Core::Context const& context)
{
    CURL* curl = curl_easy_init();
    if (!curl)
        throw std::runtime_error("Failed to initialize CURL handle");

    // RAII cleanup for curl handle
    struct CurlCleanup
    {
        CURL* handle;
        ~CurlCleanup() { if (handle) curl_easy_cleanup(handle); }
    } cleanup{curl};

    try
    {
        std::string url = request.GetUrl().GetAbsoluteUrl();
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

        // CRITICAL: Set 4MB buffer size for optimal Azure blob reads
        // This reduces recv() calls from ~250 (16KB default) to ~1 per 4MB read
        curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, 4L * 1024 * 1024);

        // Enable HTTP/2 with fallback to HTTP/1.1
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);

        // Set HTTP method
        auto method = request.GetMethod();
        if (method == Azure::Core::Http::HttpMethod::Get)
            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
        else if (method == Azure::Core::Http::HttpMethod::Head)
            curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
        else if (method == Azure::Core::Http::HttpMethod::Put)
            curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        else if (method == Azure::Core::Http::HttpMethod::Post)
            curl_easy_setopt(curl, CURLOPT_POST, 1L);
        else if (method == Azure::Core::Http::HttpMethod::Delete)
            curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");

        // Set request headers
        CurlSlistGuard headersList;
        for (const auto& header : request.GetHeaders())
        {
            std::string headerLine = header.first + ": " + header.second;
            headersList.append(headerLine.c_str());
        }
        if (headersList.get())
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headersList.get());

        // Connection and timeout options
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 10000L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
        
        // Enable connection reuse
        curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
        curl_easy_setopt(curl, CURLOPT_MAXCONNECTS, 10L);  // Connection pool size

        // SSL/TLS options - verify peer by default
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
        
        // Enable SSL session caching for performance
        // Reuses SSL sessions to avoid expensive handshake on each request
        curl_easy_setopt(curl, CURLOPT_SSL_SESSIONID_CACHE, 1L);

        // Set up response body collection
        std::vector<uint8_t> responseBody;
        responseBody.reserve(64 * 1024);  // Pre-allocate 64KB
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);

        // Set up response headers collection
        Azure::Core::CaseInsensitiveMap responseHeaders;
        curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCallback);
        curl_easy_setopt(curl, CURLOPT_HEADERDATA, &responseHeaders);

        // Perform the request
        CURLcode res = curl_easy_perform(curl);
        
        if (res != CURLE_OK)
        {
            std::string error = curl_easy_strerror(res);
            throw std::runtime_error("CURL request failed: " + error);
        }

        // Get HTTP status code
        long httpCode = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);

        // Create RawResponse
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, static_cast<Azure::Core::Http::HttpStatusCode>(httpCode), "OK");

        // Set headers
        for (const auto& header : responseHeaders)
            response->SetHeader(header.first, header.second);

        // CRITICAL: SetBody stores the vector, then ExtractBodyStream creates a stream from it
        // The Azure SDK expects GetBodyStream() to return a valid stream
        response->SetBody(std::move(responseBody));
        response->SetBodyStream(response->ExtractBodyStream());

        return response;
    }
    catch (...)
    {
        // CurlCleanup RAII will handle cleanup
        throw;
    }
}

} // namespace HPCC

//---------------------------------------------------------------------------------------------------------------------

bool areManagedIdentitiesEnabled()
{
    // Check for Azure AD Workload Identity or legacy managed identity
    static bool hasWorkloadIdentity = std::getenv("AZURE_CLIENT_ID") &&
                                     std::getenv("AZURE_TENANT_ID") &&
                                     std::getenv("AZURE_FEDERATED_TOKEN_FILE");

    static bool hasManagedIdentity = std::getenv("MSI_ENDPOINT") || std::getenv("IDENTITY_ENDPOINT");

    return hasWorkloadIdentity || hasManagedIdentity;
}

std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> getAzureSharedKeyCredential(const char * accountName, const char * secretName)
{
    DBGLOG("getAzureSharedKeyCredential() called for account=%s, secret=%s", accountName, secretName);
    // MORE: Should we create a cache of credentials?  We would need to be careful about the lifetime of the shared key credential

    StringBuffer key;
    getSecretValue(key, "storage", secretName, "key", true);
    //Trim trailing whitespace/newlines in case the secret has been entered by hand e.g. on bare metal
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

    try
    {
        DBGLOG("getAzureSharedKeyCredential() creating credential for account=%s", accountName);
        auto credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(accountName, key.str());
        DBGLOG("getAzureSharedKeyCredential() credential created successfully for account=%s", accountName);
        return credential;
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        DBGLOG("getAzureSharedKeyCredential() failed for account=%s: %s (%d)", accountName, e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        IException * error = makeStringExceptionV(-1, "Azure access: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
}

std::shared_ptr<Azure::Core::Credentials::TokenCredential> getAzureManagedIdentityCredential()
{
    DBGLOG("getAzureManagedIdentityCredential() called");
    // MORE: Should we create a cache of credentials?  We would need to be careful about the lifetime of the managed identity credential

    // Azure SDK credential objects handle token refresh automatically
    const char * federatedTokenFile = std::getenv("AZURE_FEDERATED_TOKEN_FILE");
    if (federatedTokenFile)
    {
        // Workload Identity
        const char * clientId = std::getenv("AZURE_CLIENT_ID");
        const char * tenantId = std::getenv("AZURE_TENANT_ID");
        try
        {
#ifdef AZURE_HAS_WORKLOAD_IDENTITY_CREDENTIAL
            DBGLOG("Using Azure Workload Identity authentication (clientId=%s, tenantId=%s, tokenFile=%s)",
                clientId ? clientId : "<none>", tenantId ? tenantId : "<none>", federatedTokenFile);
            return std::make_shared<Azure::Identity::WorkloadIdentityCredential>();
#else
            // SDK doesn't have WorkloadIdentityCredential - check if workload identity environment is configured
            DBGLOG("Using DefaultAzureCredential for Workload Identity (SDK < 1.6.0) (clientId=%s, tenantId=%s, tokenFile=%s)",
                clientId ? clientId : "<none>", tenantId ? tenantId : "<none>", federatedTokenFile);
            return std::make_shared<Azure::Identity::DefaultAzureCredential>();
#endif
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            throw makeStringExceptionV(-1, "Azure authentication failed: %s (%d)",
                e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        }
    }
    // else fall through

    // Use ManagedIdentityCredential for legacy managed identity
    // Only pass clientId if MSI/IDENTITY endpoints are set (true managed identity scenario)
    const char * msiEndpoint = std::getenv("MSI_ENDPOINT");
    const char * identityEndpoint = std::getenv("IDENTITY_ENDPOINT");
    const char * clientId = (msiEndpoint || identityEndpoint) ? std::getenv("AZURE_CLIENT_ID") : nullptr;
    DBGLOG("Using Azure Managed Identity authentication (clientId=%s, MSI_ENDPOINT=%s, IDENTITY_ENDPOINT=%s)",
           clientId ? clientId : "<none>",
           msiEndpoint ? msiEndpoint : "<none>",
           identityEndpoint ? identityEndpoint : "<none>");
    try
    {
        if (clientId)
            return std::make_shared<Azure::Identity::ManagedIdentityCredential>(clientId);
        else
            return std::make_shared<Azure::Identity::ManagedIdentityCredential>();
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        throw makeStringExceptionV(-1, "Azure Managed Identity authentication failed: %s (%d)",
            e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
    }
}

//---------------------------------------------------------------------------------------------------------------------

// Global transport instance for connection reuse across all Azure blob operations
// This allows HTTP connection pooling to work effectively across different blobs/containers/accounts
static std::shared_ptr<Azure::Core::Http::HttpTransport> globalAzureTransport;
static CriticalSection globalTransportCS;

std::shared_ptr<Azure::Core::Http::HttpTransport> getHttpTransport()
{
    CriticalBlock block(globalTransportCS);
    if (!globalAzureTransport)
    {
        DBGLOG("getHttpTransport() creating new global Azure transport with 10s timeout");
        Azure::Core::Http::CurlTransportOptions transportOptions;
        transportOptions.ConnectionTimeout = std::chrono::milliseconds(10000);
        transportOptions.NoSignal = true;
        globalAzureTransport = std::make_shared<HPCC::OptimizedAzureBlobTransport>(transportOptions);
        DBGLOG("getHttpTransport() global Azure transport created successfully");
    }
    else
    {
        DBGLOG("getHttpTransport() returning existing global Azure transport");
    }
    return globalAzureTransport;
}

//---------------------------------------------------------------------------------------------------------------------

bool isBase64Char(char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '+') || (c == '/') || (c == '=');
}

void handleRequestBackoff(const char * message, unsigned attempt, unsigned maxRetries)
{
    OWARNLOG("%s", message);

    if (attempt >= maxRetries)
        throw makeStringException(1234, message);

    // Exponential backoff with jitter
    unsigned backoffMs = (1U << attempt) * 100 + (rand() % 100);
    Sleep(backoffMs);
}

void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s (%d)",
                      op, attempt, maxRetries, filename, pos, len, e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));

    handleRequestBackoff(msg, attempt, maxRetries);
}

void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s",
                      op, attempt, maxRetries, filename, pos, len, e.what());

    handleRequestBackoff(msg, attempt, maxRetries);
}

void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s: %s (%d)",
                      op, attempt, maxRetries, filename, e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));

    handleRequestBackoff(msg, attempt, maxRetries);
}

void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s: %s",
                      op, attempt, maxRetries, filename, e.what());

    handleRequestBackoff(msg, attempt, maxRetries);
}
