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

#include <vector>
#include <algorithm>
#include "dasess.hpp"
#include "dadfs.hpp"
#include "thexception.hpp"

#include "../hashdistrib/thhashdistrib.ipp"
#include "thkeyedjoin.ipp"
#include "jhtree.hpp"

class CKeyedJoinMaster : public CMasterActivity
{
    IHThorKeyedJoinArg *helper = nullptr;
    Owned<IFileDescriptor> dataFileDesc, indexFileDesc;
    Owned<CSlavePartMapping> dataFileMapping;
    MemoryBuffer initMb;
    unsigned numPartsOffset = 0;
    bool remoteDataFiles = false;
    unsigned numTags = 0;
    std::vector<mptag_t> tags;
    ProgressInfoArray progressInfoArr;
    UnsignedArray progressKinds;
    Owned<CSlavePartMapping> mapping; // for local keys only
    bool remoteKeyedLookups = false;
    unsigned totalIndexParts = 0;
    std::vector<std::vector<unsigned>> slavePartMap;

public:
    CKeyedJoinMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        helper = (IHThorKeyedJoinArg *) queryHelper();
        //GH->JCS a bit wasteful creating this array each time.
        progressKinds.append(StNumIndexSeeks);
        progressKinds.append(StNumIndexScans);
        progressKinds.append(StNumIndexAccepted);
        progressKinds.append(StNumPostFiltered);
        progressKinds.append(StNumPreFiltered);

        if (helper->diskAccessRequired())
        {
            progressKinds.append(StNumDiskSeeks);
            progressKinds.append(StNumDiskAccepted);
            progressKinds.append(StNumDiskRejected);
        }
        ForEachItemIn(l, progressKinds)
            progressInfoArr.append(*new ProgressInfo(queryJob()));
        reInit = 0 != (helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) || (helper->getJoinFlags() & JFvarindexfilename);
        remoteDataFiles = false;
        remoteKeyedLookups = getOptBool(THOROPT_REMOTE_KEYED_LOOKUPS, getOptBool(THOROPT_FORCE_REMOTE_KEYED_LOOKUPS));

        /* For now KJ's in CQ's use local handlers, because there is the KJ receiver is not necessarily running on all slaves (e.g. won't be if CQ not executed on a slave)
         * In future remote handlers will talk directly to KJ service (e.g. in dafilesrv)
         */
        if (container.queryOwner().queryOwner() && (!container.queryOwner().isGlobal())) // I am in a child query
            remoteKeyedLookups = false;

        if (remoteKeyedLookups)
            ++numTags;
        if (helper->diskAccessRequired())
            numTags += 2;
        for (unsigned t=0; t<numTags; t++)
        {
            mptag_t tag = container.queryJob().allocateMPTag();
            tags.push_back(tag);
        }
    }
    ~CKeyedJoinMaster()
    {
        for (const mptag_t &tag : tags)
            container.queryJob().freeMPTag(tag);
    }
    virtual void init()
    {
        CMasterActivity::init();
        OwnedRoxieString indexFileName(helper->getIndexFileName());

        initMb.clear();
        initMb.append(indexFileName.get());
        bool keyHasTlk = false;
        totalIndexParts = 0;

        Owned<IDistributedFile> dataFile;
        Owned<IDistributedFile> indexFile = queryThorFileManager().lookup(container.queryJob(), indexFileName, false, 0 != (helper->getJoinFlags() & JFindexoptional), true);
        if (indexFile)
        {
            if (!isFileKey(indexFile))
                throw MakeActivityException(this, 0, "Attempting to read flat file as an index: %s", indexFileName.get());
            IDistributedSuperFile *superIndex = indexFile->querySuperFile();
            if (helper->diskAccessRequired())
            {
                OwnedRoxieString fetchFilename(helper->getFileName());
                if (fetchFilename)
                {
                    dataFile.setown(queryThorFileManager().lookup(container.queryJob(), fetchFilename, false, 0 != (helper->getFetchFlags() & FFdatafileoptional), true));
                    if (dataFile)
                    {
                        if (isFileKey(dataFile))
                            throw MakeActivityException(this, 0, "Attempting to read index as a flat file: %s", fetchFilename.get());
                        if (superIndex)
                            throw MakeActivityException(this, 0, "Superkeys and full keyed joins are not supported");

                        dataFileDesc.setown(getConfiguredFileDescriptor(*dataFile));
                        void *ekey;
                        size32_t ekeylen;
                        helper->getFileEncryptKey(ekeylen,ekey);
                        bool encrypted = dataFileDesc->queryProperties().getPropBool("@encrypted");
                        if (0 != ekeylen)
                        {
                            memset(ekey,0,ekeylen);
                            free(ekey);
                            if (!encrypted)
                            {
                                Owned<IException> e = MakeActivityWarning(&container, TE_EncryptionMismatch, "Ignoring encryption key provided as file '%s' was not published as encrypted", dataFile->queryLogicalName());
                                queryJobChannel().fireException(e);
                            }
                        }
                        else if (encrypted)
                            throw MakeActivityException(this, 0, "File '%s' was published as encrypted but no encryption key provided", dataFile->queryLogicalName());

                        /* If fetch file is local to cluster, fetches are sent to be processed to local node, each node has info about it's
                         * local parts only.
                         * If fetch file is off cluster, fetches are performed by requesting node directly on fetch part, therefore each nodes
                         * needs all part descriptors.
                         */
                        remoteDataFiles = false;
                        RemoteFilename rfn;
                        dataFileDesc->queryPart(0)->getFilename(0, rfn);
                        if (!rfn.queryIP().ipequals(container.queryJob().querySlaveGroup().queryNode(0).endpoint()))
                            remoteDataFiles = true;
                        if (!remoteDataFiles) // local to cluster
                        {
                            unsigned dataReadWidth = (unsigned)container.queryJob().getWorkUnitValueInt("KJDRR", 0);
                            if (!dataReadWidth || dataReadWidth>container.queryJob().querySlaves())
                                dataReadWidth = container.queryJob().querySlaves();
                            Owned<IGroup> grp = container.queryJob().querySlaveGroup().subset((unsigned)0, dataReadWidth);
                            dataFileMapping.setown(getFileSlaveMaps(dataFile->queryLogicalName(), *dataFileDesc, container.queryJob().queryUserDescriptor(), *grp, false, false, NULL));
                        }
                    }
                }
            }
            if (!helper->diskAccessRequired() || dataFile)
            {
                bool localKey = indexFile->queryAttributes().getPropBool("@local");

                checkFormatCrc(this, indexFile, helper->getIndexFormatCrc(), true);
                indexFileDesc.setown(indexFile->getFileDescriptor());

                unsigned superIndexWidth = 0;
                unsigned numSuperIndexSubs = 0;
                if (superIndex)
                {
                    numSuperIndexSubs = superIndex->numSubFiles(true);
                    bool first=true;
                    // consistency check
                    Owned<IDistributedFileIterator> iter = superIndex->getSubFileIterator(true);
                    ForEach(*iter)
                    {
                        IDistributedFile &f = iter->query();
                        unsigned np = f.numParts()-1;
                        IDistributedFilePart &part = f.queryPart(np);
                        const char *kind = part.queryAttributes().queryProp("@kind");
                        bool hasTlk = NULL != kind && 0 == stricmp("topLevelKey", kind); // if last part not tlk, then deemed local (might be singlePartKey)
                        if (first)
                        {
                            first = false;
                            keyHasTlk = hasTlk;
                            superIndexWidth = f.numParts();
                            if (keyHasTlk)
                                --superIndexWidth;
                        }
                        else
                        {
                            if (hasTlk != keyHasTlk)
                                throw MakeActivityException(this, 0, "Local/Single part keys cannot be mixed with distributed(tlk) keys in keyedjoin");
                            if (keyHasTlk && superIndexWidth != f.numParts()-1)
                                throw MakeActivityException(this, 0, "Super sub keys of different width cannot be mixed with distributed(tlk) keys in keyedjoin");
                            if (localKey && superIndexWidth != queryClusterWidth())
                                throw MakeActivityException(this, 0, "Super keys of local index must be same width as target cluster");
                        }
                    }
                    if (keyHasTlk)
                        totalIndexParts = superIndexWidth * numSuperIndexSubs;
                    else
                        totalIndexParts = superIndex->numParts();
                }
                else
                {
                    totalIndexParts = indexFile->numParts();
                    if (totalIndexParts)
                    {
                        const char *kind = indexFile->queryPart(indexFile->numParts()-1).queryAttributes().queryProp("@kind");
                        keyHasTlk = NULL != kind && 0 == stricmp("topLevelKey", kind);
                        if (keyHasTlk)
                            --totalIndexParts;
                    }
                }
                if (localKey)
                    keyHasTlk = false; // JCSMORE, not used at least for now
                initMb.append(totalIndexParts);
                if (totalIndexParts)
                {
                    initMb.append(numTags);
                    for (auto &tag: tags)
                        initMb.append(tag);
                    numPartsOffset = initMb.length();
                    initMb.append(totalIndexParts); // # placeholder, will be changed by serializeSlaveData unless all !remoteKeyedLookups || container.queryLocalData

                    ISuperFileDescriptor *superFileDesc = indexFileDesc->querySuperFileDescriptor();

                    std::vector<unsigned> partToSlave(totalIndexParts);
                    IGroup &dfsGroup = queryDfsGroup();
                    unsigned groupSize = dfsGroup.ordinality();
                    slavePartMap.clear();
                    slavePartMap.resize(dfsGroup.ordinality());
                    std::vector<unsigned> partsByPartIdx;
                    Owned<IBitSet> partsOnSlaves = createBitSet();
                    unsigned numParts = indexFileDesc->numParts();
                    for (unsigned p=0; p<numParts; p++)
                    {
                        IPartDescriptor *part = indexFileDesc->queryPart(p);
                        const char *kind = part->queryProperties().queryProp("@kind");
                        if (!kind || !strsame("topLevelKey", kind))
                        {
                            unsigned copies = part->numCopies();
                            unsigned firstMapped = NotFound;
                            for (unsigned c=0; c<copies; c++)
                            {
                                INode *node = part->queryNode(c);
                                for (unsigned gn=0; gn<groupSize; gn++)
                                {
                                    INode &groupNode = dfsGroup.queryNode(gn);
                                    if (node->equals(&groupNode))
                                    {
                                        std::vector<unsigned> &slaveParts = slavePartMap[gn];
                                        if (!partsOnSlaves->test(groupSize*gn+p))
                                        {
                                            slaveParts.push_back(p);
                                            partsOnSlaves->set(p, true);
                                            if (NotFound == firstMapped)
                                                firstMapped = gn;
                                        }
                                    }
                                }
                                if (NotFound == firstMapped)
                                {
                                    // part not within the cluster, add it to all slave maps, meaning these part meta will be serialized to all slaves so they handle the lookups directly.
                                    for (auto &slaveParts : slavePartMap)
                                        slaveParts.push_back(p);
                                }
                            }
                            // NB: in case of queryLocalData(), doesn't really need whole map
                            unsigned partIdx = part->queryPartIndex();
                            if (superFileDesc)
                            {
                                unsigned subfile, subpartnum;
                                superFileDesc->mapSubPart(partIdx, subfile, subpartnum);
                                partIdx = superIndexWidth*subfile+subpartnum;
                            }
                            partsByPartIdx.push_back(partIdx);
                            assertex(partIdx < totalIndexParts);
                            partToSlave[partIdx] = firstMapped;
                        }
                    }
                    initMb.append(totalIndexParts * sizeof(unsigned), &partToSlave[0]);
                    initMb.append(superIndexWidth); // 0 if not superIndex
                    initMb.append(keyHasTlk);
                    if (keyHasTlk)
                    {
                        if (numSuperIndexSubs)
                            initMb.append(numSuperIndexSubs);
                        else
                            initMb.append((unsigned)1);

                        Owned<IDistributedFileIterator> iter;
                        IDistributedFile *f;
                        if (superIndex)
                        {
                            iter.setown(superIndex->getSubFileIterator(true));
                            f = &iter->query();
                        }
                        else
                            f = indexFile;
                        for (;;)
                        {
                            unsigned location;
                            OwnedIFile iFile;
                            StringBuffer filePath;
                            Owned<IFileDescriptor> fileDesc = f->getFileDescriptor();
                            Owned<IPartDescriptor> tlkDesc = fileDesc->getPart(fileDesc->numParts()-1);
                            if (!getBestFilePart(this, *tlkDesc, iFile, location, filePath))
                                throw MakeThorException(TE_FileNotFound, "Top level key part does not exist, for key: %s", f->queryLogicalName());
                            OwnedIFileIO iFileIO = iFile->open(IFOread);
                            assertex(iFileIO);

                            size32_t tlkSz = (size32_t)iFileIO->size();
                            initMb.append(tlkSz);
                            ::read(iFileIO, 0, tlkSz, initMb);

                            if (!iter || !iter->next())
                                break;
                            f = &iter->query();
                        }
                    }
                    if (remoteKeyedLookups || container.queryLocalData())
                    {
                        // ensure sorted by partIdx, so that consistent order for partHandlers/lookup
                        for (auto &slaveParts : slavePartMap)
                            std::sort(slaveParts.begin(), slaveParts.end(), [partsByPartIdx](unsigned a, unsigned b) { return partsByPartIdx[a] < partsByPartIdx[b]; });
                    }
                    else
                    {
                        std::vector<unsigned> parts;
                        if (!superIndex || superIndex->isInterleaved()) // serialize first numParts parts, TLK are at end and are serialized separately.
                        {
                            for (unsigned p=0; p<totalIndexParts; p++)
                                parts.push_back(p);
                        }
                        else // non-interleaved superindex
                        {
                            unsigned p=0;
                            for (unsigned i=0; i<numSuperIndexSubs; i++)
                            {
                                for (unsigned kp=0; kp<superIndexWidth; kp++)
                                    parts.push_back(p++);
                                if (keyHasTlk)
                                    p++; // TLK's serialized separately.
                            }
                        }
                        // ensure sorted by partIdx, so that consistent order for partHandlers/lookup
                        std::sort(parts.begin(), parts.end(), [partsByPartIdx](unsigned a, unsigned b) { return partsByPartIdx[a] < partsByPartIdx[b]; });

                        indexFileDesc->serializeParts(initMb, &parts[0], parts.size());
                    }
                }
                else
                    indexFile.clear();
            }
        }
        else
            initMb.append(totalIndexParts); // 0
        if (indexFile)
        {
            addReadFile(indexFile);
            if (dataFile)
                addReadFile(dataFile);
        }
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (totalIndexParts && (remoteKeyedLookups || container.queryLocalData()))
        {
            std::vector<unsigned> &parts = slavePartMap[slave];
            unsigned numParts = slavePartMap[slave].size();
            initMb.writeDirect(numPartsOffset, sizeof(unsigned), &numParts);
            dst.append(initMb);
            if (numParts)
                indexFileDesc->serializeParts(dst, &parts[0], parts.size());
        }

        else
            dst.append(initMb);

        if (totalIndexParts && helper->diskAccessRequired())
        {
            IDistributedFile *dataFile = queryReadFile(1);
            if (dataFile)
            {
                dst.append(remoteDataFiles);
                if (remoteDataFiles)
                {
                    UnsignedArray parts;
                    parts.append((unsigned)-1); // weird convention meaning all
                    dst.append(dataFileDesc->numParts());
                    dataFileDesc->serializeParts(dst, parts);
                }
                else
                {
                    dataFileMapping->serializeMap(slave, dst);
                    dataFileMapping->serializeFileOffsetMap(dst);
                }
            }
            else
            {
                CSlavePartMapping::serializeNullMap(dst);
                CSlavePartMapping::serializeNullOffsetMap(dst);
            }
        }
    }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        CMasterActivity::deserializeStats(node, mb);
        ForEachItemIn(p, progressKinds)
        {
            unsigned __int64 st;
            mb.read(st);
            progressInfoArr.item(p).set(node, st);
        }
    }
    virtual void getEdgeStats(IStatisticGatherer & stats, unsigned idx)
    {
        //This should be an activity stats
        CMasterActivity::getEdgeStats(stats, idx);
        assertex(0 == idx);
        ForEachItemIn(p, progressInfoArr)
        {
            ProgressInfo &progress = progressInfoArr.item(p);
            progress.processInfo();
            stats.addStatistic((StatisticKind)progressKinds.item(p), progress.queryTotal());
        }
    }
};


CActivityBase *createKeyedJoinActivityMaster(CMasterGraphElement *info)
{
    return new CKeyedJoinMaster(info);
}
