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

#pragma warning (disable : 4786)
// TpWrapper.cpp: implementation of the CTpWrapper class.
//
//////////////////////////////////////////////////////////////////////

#include "TpWrapper.hpp"
#include <stdio.h>
#include "workunit.hpp"
#include "exception_util.hpp"
#include "portlist.h"
#include "daqueue.hpp"
#include "dautils.hpp"
#include "dameta.hpp"

void CTpWrapper::appendTpMachine(double clientVersion, IConstEnvironment* constEnv, IConstInstanceInfo& instanceInfo, IArrayOf<IConstTpMachine>& machines)
{
    SCMStringBuffer name, networkAddress, description, directory;
    Owned<IEspTpMachine> machine = createTpMachine();
    Owned<IConstMachineInfo> machineInfo = instanceInfo.getMachine();
    machine->setName(machineInfo->getName(name).str());
    machine->setOS(machineInfo->getOS());
    machine->setNetaddress(machineInfo->getNetAddress(networkAddress).str());
    machine->setDirectory(instanceInfo.getDirectory(directory).str());
    machine->setPort(instanceInfo.getPort());
    machine->setType(eqSparkThorProcess); //for now, the appendTpMachine is only used for SparkThor.
    machines.append(*machine.getLink());
}

extern TPWRAPPER_API ISashaCommand* archiveOrRestoreWorkunits(StringArray& wuids, IProperties* params, bool archive, bool dfu)
{
    StringBuffer sashaAddress;
    if (params && params->hasProp("sashaServerIP"))
    {
        sashaAddress.set(params->queryProp("sashaServerIP"));
        sashaAddress.append(':').append(params->getPropInt("sashaServerPort", DEFAULT_SASHA_PORT));
    }
    else
        getSashaService(sashaAddress, "sasha-wu-archiver", true);

    SocketEndpoint ep(sashaAddress);
    Owned<INode> node = createINode(ep);
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(archive ? SCA_ARCHIVE : SCA_RESTORE);
    if (dfu)
        cmd->setDFU(true);

    ForEachItemIn(i, wuids)
        cmd->addId(wuids.item(i));

    if (!cmd->send(node, 1*60*1000))
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,
            "Sasha (%s) took too long to respond for Archive/restore workunit.",
            sashaAddress.str());
    return cmd.getClear();
}

extern TPWRAPPER_API IStringIterator* getContainerTargetClusters(const char* processType, const char* processName)
{
    Owned<CStringArrayIterator> ret = new CStringArrayIterator;
    Owned<IPropertyTreeIterator> queues = getComponentConfigSP()->getElements("queues");
    ForEach(*queues)
    {
        IPropertyTree& queue = queues->query();
        if (!isEmptyString(processType))
        {
            const char* type = queue.queryProp("@type");
            if (isEmptyString(type) || !strieq(type, processType))
                continue;
        }
        const char* qName = queue.queryProp("@name");
        if (isEmptyString(qName))
            continue;

        if (!isEmptyString(processName) && !strieq(qName, processName))
            continue;

        ret->append_unique(qName);
    }
    if (!isEmptyString(processType) && !strieq("roxie", processType))
        return ret.getClear();

    Owned<IPropertyTreeIterator> services = getGlobalConfigSP()->getElements("services[@type='roxie']");
    ForEach(*services)
    {
        IPropertyTree& service = services->query();
        const char* targetName = service.queryProp("@target");
        if (isEmptyString(targetName))
            continue;

        if (!isEmptyString(processName) && !strieq(targetName, processName))
            continue;

        ret->append_unique(targetName);
    }
    return ret.getClear();
}

extern TPWRAPPER_API bool matchNetAddressRequest(const char* netAddressReg, bool ipReq, IConstTpMachine& tpMachine)
{
    if (ipReq)
        return streq(netAddressReg, tpMachine.getNetaddress());
    return streq(netAddressReg, tpMachine.getConfigNetaddress());
}

static SecAccessFlags getDropZoneScopePermissions(IEspContext& context, const IPropertyTree* dropZone, const char* dropZonePath)
{
    //If the dropZonePath is an absolute path, change it to a relative path.
    StringBuffer s;
    const char* prefix = dropZone->queryProp("@prefix");
    const char* name = dropZone->queryProp("@name");
    if (hasPrefix(dropZonePath, prefix, true))
    {
        const char* p = dropZonePath + strlen(prefix);
        if (!*p || !isPathSepChar(p[0]))
            addPathSepChar(s);
        s.append(p);
        dropZonePath = s.str();
    }

    Owned<IUserDescriptor> userDesc = createUserDescriptor();
    userDesc->set(context.queryUserId(), context.queryPassword(), context.querySignature());
    return queryDistributedFileDirectory().getDropZoneScopePermissions(name, dropZonePath, userDesc);
}

static std::tuple<std::string, std::string> validateDropZone(const char* dropZoneName, const char* _dropZonePath, const char* dropZoneHost)
{
    if (isEmptyString(_dropZonePath))
        throw makeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "findValidDropZoneScope(): DropZone path must be specified.");

    StringBuffer dropZonePath;
    getStandardPosixPath(dropZonePath, _dropZonePath);
    if (containsRelPaths(dropZonePath)) //Detect a path like: a/../../..
        throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Invalid dropzone path %s", dropZonePath.str());

    Owned<IPropertyTree> dropZone;
    if (isEmptyString(dropZoneName))
        dropZone.setown(findDropZonePlane(dropZonePath, dropZoneHost, true, true));
    else
    {
        dropZone.setown(getDropZonePlane(dropZoneName));
        if (!dropZone)
            throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "findValidDropZoneScope(): DropZone %s not found.", dropZoneName);
        if (!isEmptyString(dropZoneHost))
        {
            if (!isHostInPlane(dropZone, dropZoneHost, true))
                throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Host %s is not valid DropZone plane %s", dropZoneHost, dropZoneName);
        }
    }

    if (isAbsolutePath(dropZonePath))
    {
        //If the dropZonePath is an absolute path, change it to a relative path.
        const char* prefix = dropZone->queryProp("@prefix");
        if (hasPrefix(dropZonePath, prefix, true))
        {
            dropZonePath = dropZonePath + strlen(prefix);
            if (isPathSepChar(*dropZonePath))
                dropZonePath++;
        }
    }
    return { dropZone->queryProp("@name"), dropZonePath };
}

static SecAccessFlags checkDZPathPermissions(IEspContext& context, const char* dropZoneName, const char* dropZonePath,
    const char* dropZoneHost, SecAccessFlags permissionReq, bool errorOnNoPerms, CDfsLogicalFileName* dlfn, StringBuffer *resolvedDropZoneName)
{
    auto [name, relativePath] = validateDropZone(dropZoneName, dropZonePath, dropZoneHost);

    Owned<IUserDescriptor> userDesc = createUserDescriptor();
    userDesc->set(context.queryUserId(), context.queryPassword(), context.querySignature());
    SecAccessFlags permissions = queryDistributedFileDirectory().getDropZoneScopePermissions(name.c_str(), relativePath.c_str(), userDesc);
    if (dlfn)
        dlfn->setPlaneExternal(name.c_str(), relativePath.c_str());
    if (resolvedDropZoneName)
        resolvedDropZoneName->append(name.c_str());
    if (permissionReq)
    {
        if (permissions < permissionReq)
        {
            if (errorOnNoPerms)
            {
                throw makeStringExceptionV(ECLWATCH_INVALID_INPUT, "Access DropZone Scope %s %s not allowed for user %s (permission:%s). %s Access Required.",
                    name.c_str(), relativePath.c_str(), context.queryUserId(), getSecAccessFlagName(permissions), getSecAccessFlagName(permissionReq));
            }
        }
    }
    return permissions;
}

extern TPWRAPPER_API SecAccessFlags checkEspDZPathPermissions(IEspContext& context, const char* dropZoneName, const char* dropZonePath,
    const char* dropZoneHost, SecAccessFlags permissionReq, bool errorOnNoPerms, CDfsLogicalFileName* dlfn, StringBuffer *resolvedDropZoneName)
{
    return checkDZPathPermissions(context, dropZoneName, dropZonePath, dropZoneHost, permissionReq, errorOnNoPerms, dlfn, resolvedDropZoneName);
}

extern TPWRAPPER_API SecAccessFlags checkEspDZFilePermissions(IEspContext& context, const char* dropZoneName, const char* dropZoneFilePath,
    const char* dropZoneHost, SecAccessFlags permissionReq, bool errorOnNoPerms, CDfsLogicalFileName* dlfn, StringBuffer *resolvedDropZoneName)
{
    // NB: scope permissions only ever checked at the scope (directory) level, not per file 
    StringBuffer dropZonePath;
    splitFilename(dropZoneFilePath, &dropZonePath, &dropZonePath, nullptr, nullptr);

    return checkDZPathPermissions(context, dropZoneName, dropZonePath, dropZoneHost, permissionReq, errorOnNoPerms, dlfn, resolvedDropZoneName);
}

extern TPWRAPPER_API void validateDZPathAccess(IEspContext& context, const char* targetDZNameOrHost, const char* dropZoneHost, SecAccessFlags permissionReq,
    const char* dropZonePath, CDfsLogicalFileName& dlfn)
{
    // will fire an error if dropzone/path invalid or permissions denied
    checkDZPathPermissions(context, targetDZNameOrHost, dropZonePath, dropZoneHost, permissionReq, nullptr, true);
}

extern TPWRAPPER_API void validateDZFileAccess(IEspContext& context, const char* targetDZNameOrHost, const char* dropZoneHost, SecAccessFlags permissionReq,
    const char* dropZoneFilePath, CDfsLogicalFileName* dlfn)
{
    // NB: scope permissions only ever checked at the scope (directory) level, not per file 
    StringBuffer dropZonePath;
    splitFilename(dropZoneFilePath, &dropZonePath, &dropZonePath, nullptr, nullptr);

    checkDZPathPermissions(context, targetDZNameOrHost, dropZonePath, dropZoneHost, permissionReq, dlfn, true);
}
