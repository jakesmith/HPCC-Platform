/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#pragma warning (disable : 4786)

#include "jflz.hpp"
#include "jstring.hpp"

#include "daaudit.hpp"
#include "dautils.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "esp.hpp"
#include "exception_util.hpp"
#include "package.h"

#include "ws_dfsclient.hpp"
#include "ws_dfsservice.hpp"

#include <algorithm>
#include <unordered_map>
#include <vector>

using namespace wsdfs;

// all fake for now
// this will be implemened in Dali.
static std::atomic<unsigned __int64> nextLockID{0};
static unsigned __int64 getLockId(unsigned __int64 leaseId)
{
    // associated new locks with lease
    return ++nextLockID;
}


enum LfnMetaOpts : byte
{
    LfnMOptNone   = 0x00,
    LfnMOptRemap  = 0x01,
    LfnMOptTls    = 0x02,
};
BITMASK_ENUM(LfnMetaOpts);
static void populateLFNMeta(IUserDescriptor *userDesc, const char *logicalName, unsigned __int64 leaseId, LfnMetaOpts opts, IPropertyTree *metaRoot, IPropertyTree *meta)
{
    CDfsLogicalFileName lfn;
    lfn.set(logicalName);
    if (lfn.isForeign())
        ThrowStringException(-1, "foreign file %s. Not supported", logicalName);

    assertex(!lfn.isMulti()); // not supported, don't think needs to be/will be.

    Owned<IPropertyTree> tree = queryDistributedFileDirectory().getFileTree(logicalName, userDesc);
    if (!tree)
        return;
    if (hasMask(opts, LfnMOptRemap))
    {
        bool secure = hasMask(opts, LfnMOptTls);
        // If !secure - called from insecure DFS service, remapGroupsToDafilesrv needs to direct to an insecure dafilesrv

        remapGroupsToDafilesrv(tree, false, secure);
    }

    bool isSuper = streq(tree->queryName(), queryDfsXmlBranchName(DXB_SuperFile));

    IPropertyTree *fileMeta = meta->addPropTree("FileMeta");
    fileMeta->setProp("@name", logicalName);

    // 1) establish lock 1st (from Dali)
    // TBD
    unsigned __int64 lockId = getLockId(leaseId);
    fileMeta->setPropInt64("@lockId", lockId);

    if (isSuper)
    {
        fileMeta->setPropBool("@isSuper", true);
        unsigned n = tree->getPropInt("@numsubfiles");
        if (n)
        {
            Owned<IPropertyTreeIterator> subit = tree->getElements("SubFile");
            // Adding a sub 'before' another get the list out of order (but still valid)
            OwnedMalloc<IPropertyTree *> orderedSubFiles(n, true);
            ForEach (*subit)
            {
                IPropertyTree &sub = subit->query();
                unsigned sn = sub.getPropInt("@num",0);
                if (sn == 0)
                    ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: bad subfile part number %d of %d", logicalName, sn, n);
                if (sn > n)
                    ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: out-of-range subfile part number %d of %d", logicalName, sn, n);
                if (orderedSubFiles[sn-1])
                    ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: duplicated subfile part number %d of %d", logicalName, sn, n);
                orderedSubFiles[sn-1] = &sub;
            }
            for (unsigned i=0; i<n; i++)
            {
                if (!orderedSubFiles[i])
                    ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: missing subfile part number %d of %d", logicalName, i+1, n);
            }
            StringBuffer subname;
            for (unsigned f=0; f<n; f++)
            {
                IPropertyTree &sub = *(orderedSubFiles[f]);
                sub.getProp("@name", subname.clear());
                populateLFNMeta(userDesc, subname, leaseId, opts, metaRoot, fileMeta);
            }
        }
    }
    fileMeta->setPropTree(tree->queryName(), tree.getLink());
    Owned<IPropertyTreeIterator> clusterIter = tree->getElements("Cluster");
    ForEach(*clusterIter)
    {
        IPropertyTree &cluster = clusterIter->query();
        const char *planeName = cluster.queryProp("@name");
        VStringBuffer planeXPath("planes[@name='%s']", planeName);
        if (!metaRoot->hasProp(planeXPath))
        {
            VStringBuffer storagePlaneXPath("storage/%s", planeXPath.str());
            Owned<IPropertyTree> dataPlane = getGlobalConfigSP()->getPropTree(storagePlaneXPath);

            const char *hostGroupName = dataPlane->queryProp("@hostGroup");
            if (!isEmptyString(hostGroupName))
            {
                Owned<IPropertyTree> hostGroup = getHostGroup(hostGroupName, false);
                if (hostGroup)
                {
                    // This is only likely to be used if this service is in BM
                    // Cloud based storage planes are unlikely to be backed by hosts/hostGroups
                    dataPlane.setown(createPTreeFromIPT(dataPlane));
                    unsigned daFsSrvPort = getPreferredDaFsServerPort();
                    Owned<IPropertyTreeIterator> iter = hostGroup->getElements("hosts");
                    ForEach(*iter)
                    {
                        VStringBuffer endpoint("%s:%u", iter->query().queryProp(nullptr), daFsSrvPort);
                        dataPlane->addProp("hosts", endpoint);
                    }
                    dataPlane->removeProp("@hostGroup");
                }
            }
            metaRoot->addPropTree("planes", dataPlane.getClear());
        }
    }
}


void CWsDfsEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    DBGLOG("Initializing %s service [process = %s]", service, process);
    VStringBuffer xpath("Software/EspProcess/EspBinding[@service=\"%s\"]/@protocol", service);
    isHttps = strsame("https", cfg->queryProp(xpath));
}

bool CWsDfsEx::onGetLease(IEspContext &context, IEspLeaseRequest &req, IEspLeaseResponse &resp)
{
    unsigned timeoutSecs = req.getKeepAliveExpiryFrequency();

    // TBD will get from Dali.
    resp.setLeaseId(1);
    return true;
}

bool CWsDfsEx::onKeepAlive(IEspContext &context, IEspKeepAliveRequest &req, IEspKeepAliveResponse &resp)
{
    return true;
}

bool CWsDfsEx::onDFSFileLookup(IEspContext &context, IEspDFSFileLookupRequest &req, IEspDFSFileLookupResponse &resp)
{
    try
    {
        const char *logicalName = req.getName();

        StringBuffer userID;
        context.getUserID(userID);
        Owned<IUserDescriptor> userDesc;
        if (!userID.isEmpty())
        {
            userDesc.setown(createUserDescriptor());
            userDesc->set(userID.str(), context.queryPassword(), context.querySignature());
        }

        // LDAP scope check
        checkLogicalName(logicalName, userDesc, true, false, false, nullptr); // check for read permissions

        unsigned timeoutSecs = req.getRequestTimeout();
        unsigned __int64 leaseId = req.getLeaseId();

        // populate file meta data and lock id's
        LfnMetaOpts opts = LfnMOptNone;
        if (req.getAccessViaDafilesrv())
            opts |= LfnMOptRemap;

        if (isContainerized())
        {
            // NB: if we ever have some services with tls, and some without in bare-metal, this may need revisiting.
            if (getComponentConfigSP()->getPropBool("@tls"))
                opts |= LfnMOptTls;
        }
        else if (isHttps)
            opts |= LfnMOptTls;

        Owned<IPropertyTree> responseTree = createPTree();
        populateLFNMeta(userDesc, logicalName, leaseId, opts, responseTree, responseTree);

        // serialize response
        MemoryBuffer respMb, compressedRespMb;
        responseTree->serialize(respMb);
        fastLZCompressToBuffer(compressedRespMb, respMb.length(), respMb.bytes());
        StringBuffer respStr;
        JBASE64_Encode(compressedRespMb.bytes(), compressedRespMb.length(), respStr, false);
        resp.setMeta(respStr.str());

        if (responseTree->hasProp("FileMeta")) // otherwise = not found
        {
            // update file access.
            //    Really this should be done at end (or at end as well), but this is same as existing DFS lookup.
            CDateTime dt;
            dt.setNow();
            queryDistributedFileDirectory().setFileAccessed(userDesc, logicalName, dt);

            LOG(MCauditInfo,",FileAccess,EspProcess,READ,%s,%u,%s", logicalName, timeoutSecs, userID.str());
        }
    }
    catch (IException *e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

// Helper functions for DFSListFiltered

// Case-insensitive string hash and comparison for field aliases
struct CaseInsensitiveHash
{
    size_t operator()(const std::string &s) const
    {
        std::string lower = s;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
        return std::hash<std::string>()(lower);
    }
};

struct CaseInsensitiveEqual
{
    bool operator()(const std::string &a, const std::string &b) const
    {
        return std::equal(a.begin(), a.end(), b.begin(), b.end(),
            [](char ca, char cb) { return tolower(ca) == tolower(cb); });
    }
};

static bool validateFileField(const char *requestedField, StringBuffer &attrName, DFUQResultField &fieldEnum, DFUQResultFieldType &fieldType)
{
    // Map of ECL field name aliases to canonical internal field names
    // This handles user-friendly names and their mappings
    static const std::unordered_map<std::string, const char*, CaseInsensitiveHash, CaseInsensitiveEqual> fieldAliases =
    {
        {"superfile", "numsubfiles"},  // Derived from numsubfiles
        {"rowcount", "recordcount"},   // ECL field name is "rowcount" but internal attribute is "recordCount"
        {"cluster", "group"}           // ECL field name is "cluster" but internal attribute is "group"
    };

    // Trim whitespace
    StringBuffer fieldName(requestedField);
    fieldName.trim();
    if (isEmptyString(fieldName))
        return false;

    auto it = fieldAliases.find(fieldName.str());
    if (it != fieldAliases.end())
        fieldName.set(it->second);

    return getFileAttributePath(fieldName, attrName, fieldEnum, fieldType);
}

static void parseUserFilterSyntax(const char *userFilter, StringBuffer &internalFilter)
{
    if (isEmptyString(userFilter))
        return;

    StringArray terms;
    terms.appendList(userFilter, ",");

    ForEachItemIn(i, terms)
    {
        const char *term = terms.item(i);
        if (isEmptyString(term))
            continue;

        // Save original term for error messages
        const char *originalTerm = term;

        // Check for negation prefix
        bool negate = (term[0] == '!');
        if (negate)
        {
            term++;
            if (isEmptyString(term))
                throw makeStringException(-1, "Invalid filter syntax: '!' must be followed by a filter term");
        }

        // Parse has:property
        if (strncmp(term, "has:", 4) == 0)
        {
            const char *prop = term + 4;
            if (isEmptyString(prop))
                throw makeStringException(-1, "Invalid filter syntax: 'has:' requires a property name (e.g., 'has:description')");

            // Validate and convert field name
            StringBuffer attrName;
            DFUQResultField field;
            DFUQResultFieldType fieldType;
            if (!validateFileField(prop, attrName, field, fieldType))
                throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - unknown field name '%s'", originalTerm, prop);

            internalFilter.appendf("%u%c%s%c%s%c",
                DFUQFThasProp, DFUQFilterSeparator,
                attrName.str(), DFUQFilterSeparator,
                negate ? "false" : "true", DFUQFilterSeparator);
        }
        // Parse is:filetype
        else if (strncmp(term, "is:", 3) == 0)
        {
            if (negate)
                throw makeStringExceptionV(-1, "Invalid filter syntax: negating 'is:' is not supported");

            const char *fileType = term + 3;
            if (isEmptyString(fileType))
                throw makeStringException(-1, "Invalid filter syntax: 'is:' requires a file type (superfile, normal, or any)");

            DFUQFileTypeFilter fileTypeFilter = DFUQFFTall;
            if (strieq(fileType, "any"))
                fileTypeFilter = DFUQFFTall;
            else if (strieq(fileType, "superfile"))
                fileTypeFilter = DFUQFFTsuperfileonly;
            else if (strieq(fileType, "normal"))
                fileTypeFilter = DFUQFFTnonsuperfileonly;
            else
                throw makeStringExceptionV(-1, "Invalid filter syntax: 'is:%s' - must be superfile, normal, or any", fileType);

            internalFilter.appendf("%u%c%u%c%u%c",
                DFUQFTspecial, DFUQFilterSeparator, (char)DFUQSFFileType,
                DFUQFilterSeparator, (char)fileTypeFilter, DFUQFilterSeparator);
        }
        // Parse field:value (wildcard match)
        else if (const char *colon = strchr(term, ':'))
        {
            if (negate)
                throw makeStringExceptionV(-1, "Invalid filter syntax: negating field:value filters is not supported");
            StringBuffer fieldName;
            fieldName.append(colon - term, term).trim();
            StringBuffer valueStr(colon + 1);
            valueStr.trim();
            const char *value = valueStr.str();

            if (fieldName.length() == 0)
                throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - field name required before ':'", originalTerm);
            if (isEmptyString(value))
                throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - value required after ':'", originalTerm);

            // Validate and convert field name
            StringBuffer attrName;
            DFUQResultField field;
            DFUQResultFieldType fieldType;
            if (!validateFileField(fieldName.str(), attrName, field, fieldType))
                throw makeStringException(-1, VStringBuffer("Invalid filter syntax: '%s' - unknown field name '%s'", originalTerm, fieldName.str()).str());

            internalFilter.appendf("%u%c%s%c%s%c",
                DFUQFTwildcardMatch, DFUQFilterSeparator,
                attrName.str(), DFUQFilterSeparator,
                value, DFUQFilterSeparator);
        }
        // Parse field>value, field<value, field>=value, field<=value
        else if (const char *op = strpbrk(term, "><"))
        {
            if (negate)
                throw makeStringExceptionV(-1, "Invalid filter syntax: negating comparison filters is not supported");
            StringBuffer fieldName;
            fieldName.append(op - term, term).trim();

            if (fieldName.length() == 0)
                throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - field name required before comparison operator", originalTerm);

            // Validate and convert field name
            StringBuffer attrName;
            DFUQResultField field;
            DFUQResultFieldType fieldType;
            if (!validateFileField(fieldName.str(), attrName, field, fieldType))
                throw makeStringException(-1, VStringBuffer("Invalid filter syntax: '%s' - unknown field name '%s'", originalTerm, fieldName.str()).str());

            // Determine operator
            bool hasEquals = (op[1] == '=');
            StringBuffer valueStr(hasEquals ? (op + 2) : (op + 1));
            valueStr.trim();
            const char *value = valueStr.str();

            if (isEmptyString(value))
                throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - value required after comparison operator", originalTerm);

            // Check if field is numeric/float type
            bool isNumeric = (fieldType == DFUQResultFieldType::numericType);
            bool isFloat = (fieldType == DFUQResultFieldType::floatType);

            if (isNumeric || isFloat)
            {
                // Parse numeric range
                if (op[0] == '>')
                {
                    // field > value or field >= value
                    char *endptr;
                    __int64 minVal = (__int64) strtoll(value, &endptr, 10);
                    if (!isEmptyString(endptr))
                        throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - value '%s' must be an integer", originalTerm, value);

                    if (!hasEquals)
                    {
                        if (minVal == I64C(0x7FFFFFFFFFFFFFFF))
                            throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - value too large for > comparison (would overflow)", originalTerm);
                        minVal++;
                    }
                    internalFilter.appendf("%u%c%s%c%lld%c%lld%c",
                        DFUQFTinteger64Range, DFUQFilterSeparator,
                        attrName.str(), DFUQFilterSeparator,
                        minVal, DFUQFilterSeparator, I64C(0x7FFFFFFFFFFFFFFF), DFUQFilterSeparator);
                }
                else // op[0] == '<'
                {
                    // field < value or field <= value
                    char *endptr;
                    __int64 maxVal = (__int64) strtoll(value, &endptr, 10);
                    if (!isEmptyString(endptr))
                        throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - value '%s' must be an integer", originalTerm, value);
                    if (!hasEquals)
                    {
                        if (maxVal == (-I64C(0x7FFFFFFFFFFFFFFF) - 1))
                            throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - value too small for < comparison (would underflow)", originalTerm);
                        maxVal--;
                    }
                    internalFilter.appendf("%u%c%s%c0%c%lld%c",
                        DFUQFTinteger64Range, DFUQFilterSeparator,
                        attrName.str(), DFUQFilterSeparator,
                        DFUQFilterSeparator, maxVal, DFUQFilterSeparator);
                }
            }
            else
            {
                // Parse string range (for dates, text, etc.)
                // String range filter only supports inclusive bounds (>=, <=)
                // since the filter uses standard string comparison
                if (op[0] == '>')
                {
                    if (!hasEquals)
                        throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - exclusive comparison (>) is not supported for string fields; use >= instead", originalTerm);
                    internalFilter.appendf("%u%c%s%c%s%c~~~~~~~~~~%c",
                        DFUQFTstringRange, DFUQFilterSeparator,
                        attrName.str(), DFUQFilterSeparator,
                        value, DFUQFilterSeparator, DFUQFilterSeparator);
                }
                else // op[0] == '<'
                {
                    if (!hasEquals)
                        throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - exclusive comparison (<) is not supported for string fields; use <= instead", originalTerm);
                    internalFilter.appendf("%u%c%s%c%c%s%c",
                        DFUQFTstringRange, DFUQFilterSeparator,
                        attrName.str(), DFUQFilterSeparator,
                        DFUQFilterSeparator, value, DFUQFilterSeparator);
                }
            }
        }
        else
        {
            // Unknown filter format
            throw makeStringExceptionV(-1, "Invalid filter syntax: '%s' - unrecognized format", originalTerm);
        }
    }
}

bool CWsDfsEx::onDFSListFiltered(IEspContext &context, IEspDFSListFilteredRequest &req, IEspDFSListFilteredResponse &resp)
{
    try
    {
        const char *mask = req.getMask();
        const char *filters = req.getFilters();
        const char *requestedFields = req.getRequestedFields();
        bool unknownszero = req.getUnknownSizeZero();
        __int64 maxFileLimit = req.getMaxFileLimit();

        StringBuffer userID;
        context.getUserID(userID);
        Owned<IUserDescriptor> userDesc;
        if (!userID.isEmpty())
        {
            userDesc.setown(createUserDescriptor());
            userDesc->set(userID.str(), context.queryPassword(), context.querySignature());
        }

        // Validate server-side max limit (10 million)
        constexpr __int64 SERVER_MAX_LIMIT = 10000000;
        if (maxFileLimit > SERVER_MAX_LIMIT)
        {
            throw makeStringExceptionV(-1, "WsDfs.DFSListFiltered: maxFileLimit (%lld) exceeds server maximum of %lld", maxFileLimit, SERVER_MAX_LIMIT);
        }

        if (isEmptyString(mask))
            mask = "*";
        else if (*mask == '~')
            mask++; // Strip leading ~ if present, as internal APIs expect it without
        StringBuffer masklower(mask);
        masklower.toLowerCase();

        // Build filter string - translate user-friendly syntax to internal format
        StringBuffer filterBuf;

        // Parse user-provided filters (using friendly syntax like "owner:jsmith size>1000")
        parseUserFilterSyntax(filters, filterBuf);

        // Append system filters: name pattern and max files limit
        filterBuf.appendf("%u%c%u%c%s%c",
            DFUQFTspecial, DFUQFilterSeparator,
            DFUQSFFileNameWithPrefix, DFUQFilterSeparator,
            masklower.str(), DFUQFilterSeparator);

        // Add max files limit if specified (and not -1 which means use server default)
        if (maxFileLimit > 0)
        {
            filterBuf.appendf("%u%c%u%c%lld%c",
                DFUQFTspecial, DFUQFilterSeparator,
                DFUQSFMaxFiles, DFUQFilterSeparator,
                maxFileLimit, DFUQFilterSeparator);
        }

        // Parse and validate requested fields
        std::vector<DFUQResultField> fields;
        StringArray requestedFieldNames;

        if (isEmptyString(requestedFields))
        {
            requestedFieldNames.append("name");
            requestedFieldNames.append("superfile");
            requestedFieldNames.append("size");
            requestedFieldNames.append("rowcount");
            requestedFieldNames.append("modified");
            requestedFieldNames.append("owner");
            requestedFieldNames.append("cluster");
        }
        else
        {
            // Parse comma-separated field list
            requestedFieldNames.appendList(requestedFields, ",");

            // Ensure "name" is always included (required field)
            if (!requestedFieldNames.contains("name", true))
                requestedFieldNames.append("name");
        }

        // Validate field names and build field list for getDFAttributesFilteredIterator
        ForEachItemIn(idx, requestedFieldNames)
        {
            // Trim whitespace
            const char *fieldName = requestedFieldNames.item(idx);
            if (isEmptyString(fieldName))
                continue;

            // Validate field name using existing validation function
            StringBuffer attrPath;
            DFUQResultField field;
            DFUQResultFieldType fieldType;
            if (!validateFileField(fieldName, attrPath, field, fieldType))
                throw makeStringExceptionV(-1, "WsDfs.DFSListFiltered: Invalid field name '%s'", fieldName);

            // Add field to list if not already present
            if (std::find(fields.begin(), fields.end(), field) == fields.end())
                fields.push_back(field);
        }

        // Always include numsubfiles for superfile detection
        if (std::find(fields.begin(), fields.end(), DFUQResultField::numsubfiles) == fields.end())
            fields.push_back(DFUQResultField::numsubfiles);

        // Add terminator
        fields.push_back(DFUQResultField::term);

        bool allMatchingFilesReceived = false;
        unsigned count = 0;
        Owned<IPropertyTreeIterator> iter = queryDistributedFileDirectory().getDFAttributesFilteredIterator(
            filterBuf.str(),
            nullptr,                    // no local filters
            fields.data(),              // requested fields
            userDesc,
            true,                       // recursive
            allMatchingFilesReceived,
            &count
        );

        // Build result tree
        Owned<IPropertyTree> resultTree = createPTree();
        resultTree->setPropInt("@count", count);
        resultTree->setPropBool("@allMatchingFilesReceived", allMatchingFilesReceived);

        // Add files to result
        IPropertyTree *filesTree = resultTree->addPropTree("Files");
        ForEach(*iter)
        {
            IPropertyTree &file = iter->query();
            IPropertyTree *fileTree = filesTree->addPropTree("File", &file);
            
            // Handle unknownszero flag for size field
            if (unknownszero && !fileTree->hasProp("@size"))
                fileTree->setPropInt64("@size", 0);
        }

        // Serialize response
        MemoryBuffer respMb, compressedRespMb;
        resultTree->serialize(respMb);
        fastLZCompressToBuffer(compressedRespMb, respMb.length(), respMb.bytes());
        StringBuffer respStr;
        JBASE64_Encode(compressedRespMb.bytes(), compressedRespMb.length(), respStr, false);
        resp.setResult(respStr.str());

        LOG(MCauditInfo,",FileList,EspProcess,READ,%s,%s,%u,%s", mask, filters?filters:"", count, userID.str());
    }
    catch (IException *e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


