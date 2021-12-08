/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#ifndef _WSDFS_HPP
#define _WSDFS_HPP

#ifndef WSDFS_API

#ifdef WSDFS_EXPORTS
#define WSDFS_API DECL_EXPORT
#else
#define WSDFS_API DECL_IMPORT
#endif

#endif

namespace wsdfs
{

interface IDFSFile : extends IInterface
{
    virtual IPropertyTree *getFileMeta() const = 0;
    virtual unsigned __int64 getLockId() const = 0;
};

WSDFS_API IDFSFile *lookupDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, const char *user, const char *token);
WSDFS_API IDFSFile *lookupDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, IUserDescriptor *userDesc);
WSDFS_API IDistributedFile *createLegacyDFSFile(IDFSFile *dfsFile);
WSDFS_API IDistributedFile *lookupLegacyDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, const char *user, const char *token);
WSDFS_API IDistributedFile *lookupLegacyDFSFile(const char *logicalName, unsigned timeoutSecs, unsigned keepAliveExpiryFrequency, IUserDescriptor *userDesc);

} // end of namespace wsdfs

#endif // _WSDFS_HPP
