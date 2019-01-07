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

#ifndef DAFSCLIENT_HPP
#define DAFSCLIENT_HPP

#ifdef DAFSCLIENT_EXPORTS
#define DAFSCLIENT_API DECL_EXPORT
#else
#define DAFSCLIENT_API DECL_IMPORT
#endif

#include "jlzw.hpp"
#include "seclib.hpp"

interface IOutputMetaData;
namespace dafsclient
{

enum DFUFileType { dft_none, dft_flat, dft_index };


interface DAFSCLIENT_API IDFUFilePartBase : extends IInterface
{
    virtual void start() = 0;
    virtual void finalize() = 0;
    virtual IOutputMetaData *queryOutputMeta() = 0; // JCSMORE -
};

interface DAFSCLIENT_API IDFUFilePartReader : extends IDFUFilePartBase
{
    virtual const void *nextRow(size32_t &sz) = 0;
    virtual void addVirtualFieldMapping(const char *fieldName, const char *fieldValue) = 0;
};

interface DAFSCLIENT_API IDFUFilePartWriter : extends IDFUFilePartBase
{
    virtual void write(size32_t sz, const void *row) = 0;
};


interface DAFSCLIENT_API IDFUFileAccessExt : extends IInterface
{
    virtual IOutputMetaData *queryOutputMeta() const = 0;
    virtual IFileDescriptor &queryFileDescriptor() const = 0;
    virtual IPropertyTree &queryProperties() const = 0; // JCSMORE - could get from IFileDescriptor
};

interface DAFSCLIENT_API IDFUFileAccess : extends IInterface
{
    virtual const char *queryName() const = 0;
    virtual unsigned queryNumParts() const = 0;
    virtual SecAccessFlags queryAccessType() const = 0;
    virtual bool queryIsGrouped() const = 0;
    virtual DFUFileType queryType() const = 0;
    virtual bool queryIsCompressed() const = 0;
    virtual const SocketEndpoint &queryPartEndpoint(unsigned part, unsigned copy=0) const = 0;
    virtual const char *queryJSONTypeInfo() const = 0;
    virtual const char *queryECLTypeInfo() const = 0;

// NB: the intention is for a IDFUFileAccess to be used to create instances for multiple parts, but not to mix types.
    virtual IDFUFilePartReader *createFilePartReader(unsigned p, unsigned copy=0, IOutputMetaData *outMeta=nullptr) = 0;
    virtual IDFUFilePartWriter *createFilePartWriter(unsigned p) = 0;

    virtual IDFUFileAccessExt *queryEngineInterface() = 0;
};


DAFSCLIENT_API IDFUFileAccess *createDFUFileAccess(const char *name, const char *metaInfoBlobB64);
DAFSCLIENT_API IRowWriter *createRowWriter(IDFUFilePartWriter *partWriter);

} // end of namespace dafsclient

#endif
