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
#include <cstdlib>
#include <azure/identity.hpp>

using namespace std::chrono;

// Common utility functions shared by both blob and file implementations
//---------------------------------------------------------------------------------------------------------------------

//Singleton class to manage Azure AD Workload Identity token acquisition and renewal
//Class definition is in azureapiutils.hpp

AzureWorkloadIdentityTokenManager::AzureWorkloadIdentityTokenManager()
{
    //Check for Azure AD Workload Identity environment variables
    hasWorkloadIdentity = std::getenv("AZURE_CLIENT_ID") &&
                         std::getenv("AZURE_TENANT_ID") &&
                         std::getenv("AZURE_FEDERATED_TOKEN_FILE");

    //Check for legacy managed identity endpoints
    hasManagedIdentity = std::getenv("MSI_ENDPOINT") || std::getenv("IDENTITY_ENDPOINT");

    if (hasWorkloadIdentity)
        DBGLOG("Azure AD Workload Identity detected");
    else if (hasManagedIdentity)
        DBGLOG("Legacy Azure Managed Identity detected");
}

bool AzureWorkloadIdentityTokenManager::fetchWorkloadIdentityToken()
{
    const char * clientId = std::getenv("AZURE_CLIENT_ID");
    const char * tenantId = std::getenv("AZURE_TENANT_ID");
    const char * tokenFile = std::getenv("AZURE_FEDERATED_TOKEN_FILE");

    if (!clientId || !tenantId || !tokenFile)
        return false;

    //Use Azure SDK's DefaultAzureCredential which supports Workload Identity
    try
    {
        Azure::Identity::DefaultAzureCredential credential;
        Azure::Core::Credentials::TokenRequestContext context;
        context.Scopes.push_back("https://storage.azure.com/.default");

        auto tokenResult = credential.GetToken(context, Azure::Core::Context());

        accessToken.set(tokenResult.Token.c_str());

        //Calculate expiration time (refresh 5 minutes before actual expiry)
        auto expiresOn = std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(tokenResult.ExpiresOn));
        tokenExpiresAt = expiresOn - 300; // 5 minutes buffer

        DBGLOG("Azure AD Workload Identity token acquired, expires at %s", ctime(&expiresOn));
        return true;
    }
    catch (const Azure::Core::Credentials::AuthenticationException& e)
    {
        IERRLOG("Azure AD Workload Identity authentication failed: %s", e.what());
        return false;
    }
    catch (const std::exception& e)
    {
        IERRLOG("Failed to acquire Azure AD Workload Identity token: %s", e.what());
        return false;
    }
}

bool AzureWorkloadIdentityTokenManager::isTokenValid() const
{
    return (accessToken.length() > 0) && (time(nullptr) < tokenExpiresAt);
}

AzureWorkloadIdentityTokenManager & AzureWorkloadIdentityTokenManager::instance()
{
    static AzureWorkloadIdentityTokenManager theInstance;
    return theInstance;
}

bool AzureWorkloadIdentityTokenManager::isEnabled() const
{
    return hasWorkloadIdentity || hasManagedIdentity;
}

bool AzureWorkloadIdentityTokenManager::requiresExplicitToken() const
{
    //Workload Identity requires explicit token management
    //Legacy managed identity is handled automatically by Azure SDK
    return hasWorkloadIdentity;
}

const char * AzureWorkloadIdentityTokenManager::getAccessToken()
{
    CriticalBlock block(cs);

    if (!hasWorkloadIdentity)
        return nullptr; // Let SDK handle legacy managed identity

    if (isTokenValid())
        return accessToken.str();

    //Token expired or not yet fetched, get a fresh one
    if (fetchWorkloadIdentityToken())
        return accessToken.str();

    IERRLOG("Failed to acquire Azure access token");
    return nullptr;
}

void AzureWorkloadIdentityTokenManager::invalidateToken()
{
    CriticalBlock block(cs);
    tokenExpiresAt = 0;
}

AzureWorkloadIdentityTokenManager & getAzureTokenManager()
{
    return AzureWorkloadIdentityTokenManager::instance();
}

bool areManagedIdentitiesEnabled()
{
    //Use a local static to avoid re-evaluation. Performance is not critical - so once overhead is acceptable.
    return AzureWorkloadIdentityTokenManager::instance().isEnabled();
}

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
    // Check for authentication failures and invalidate token if needed
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Unauthorized ||
        e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
    {
        DBGLOG("Authentication failure detected, invalidating Azure token");
        AzureWorkloadIdentityTokenManager::instance().invalidateToken();
    }

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
    // Check for authentication failures and invalidate token if needed
    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::Unauthorized ||
        e.StatusCode == Azure::Core::Http::HttpStatusCode::Forbidden)
    {
        DBGLOG("Authentication failure detected, invalidating Azure token");
        AzureWorkloadIdentityTokenManager::instance().invalidateToken();
    }

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
