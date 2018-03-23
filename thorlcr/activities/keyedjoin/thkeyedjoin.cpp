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

#include <array>
#include <vector>
#include <algorithm>
#include "dasess.hpp"
#include "dadfs.hpp"
#include "thexception.hpp"

#include "../fetch/thfetchcommon.hpp"
#include "../hashdistrib/thhashdistrib.ipp"
#include "thkeyedjoin.ipp"
#include "jhtree.hpp"

static const std::array<StatisticKind, 8> progressKinds = {{ StNumIndexSeeks, StNumIndexScans, StNumIndexAccepted, StNumPostFiltered, StNumPreFiltered, StNumDiskSeeks, StNumDiskAccepted, StNumDiskRejected }};

class CKeyedJoinMaster : public CMasterActivity
{
    IHThorKeyedJoinArg *helper = nullptr;
    Owned<IFileDescriptor> dataFileDesc, indexFileDesc;
    MemoryBuffer initMb;
    unsigned numPartsOffset = 0;
    unsigned numTags = 0;
    std::vector<mptag_t> tags;
    ProgressInfoArray progressInfoArr;

    bool remoteKeyedLookup = false;
    bool remoteKeyedFetch = false;
    unsigned totalIndexParts = 0;

    class CMap
    {
    public:
        std::vector<unsigned> allParts;
        std::vector<std::vector<unsigned>> slavePartMap; // vector of slave parts (IPartDescriptor's slavePartMap[<slave>] serialized to each slave)
        std::vector<unsigned> partToSlave; // vector mapping part index to slave (sent to all slaves)

        CMap()
        {
        }
        void setup(unsigned slaves, unsigned parts)
        {
            clear();
            slavePartMap.resize(slaves);
            partToSlave.resize(parts);
        }
        void clear()
        {
            allParts.clear();
            slavePartMap.clear();
            partToSlave.clear();
        }
        unsigned count() const { return partToSlave.size(); }
        void serializePartMap(MemoryBuffer &mb)
        {
            mb.append(partToSlave.size() * sizeof(unsigned), &partToSlave[0]);
        }
        unsigned querySlave(unsigned part) const { return partToSlave[part]; }
        std::vector<unsigned> &querySlaveParts(unsigned slave) { return slavePartMap[slave]; }
        std::vector<unsigned> &queryAllParts() { return allParts; }
    };

    CMap indexMap, dataMap;
    // Fills map
    void mapParts(CMap &map, IDistributedFile *file, bool isIndexWithTlk)
    {
        Owned<IFileDescriptor> fileDesc = file->getFileDescriptor();
        assertex(fileDesc);
        IDistributedSuperFile *super = file->querySuperFile();
        ISuperFileDescriptor *superFileDesc = fileDesc->querySuperFileDescriptor();
        unsigned totalParts = file->numParts();
        if (isIndexWithTlk)
            totalParts -= super ? super->numSubFiles(true) : 1;

        IGroup &dfsGroup = queryDfsGroup();
        map.setup(dfsGroup.ordinality(), totalParts);


        unsigned numSuperIndexSubs = 0;
        unsigned superWidth = 0;
        if (super)
        {
            if (super->numSubFiles(true))
            {
                if (!super->isInterleaved())
                    numSuperIndexSubs = super->numSubFiles(true);

                IDistributedFile &sub = super->querySubFile(0, true);
                superWidth = sub.numParts();
                if (isIndexWithTlk)
                    --superWidth;
            }
        }

        unsigned groupSize = dfsGroup.ordinality();
        std::vector<unsigned> partsByPartIdx;
        Owned<IBitSet> partsOnSlaves = createBitSet();
        unsigned numParts = fileDesc->numParts();
        unsigned nextGroupStartPos = 0;
        for (unsigned p=0; p<numParts; p++)
        {
            IPartDescriptor *part = fileDesc->queryPart(p);
            const char *kind = isIndexWithTlk ? part->queryProperties().queryProp("@kind") : nullptr;
            if (!kind || !strsame("topLevelKey", kind))
            {
                unsigned copies = part->numCopies();
                unsigned mappedPos = NotFound;
                for (unsigned c=0; c<copies; c++)
                {
                    INode *node = part->queryNode(c);
                    unsigned start=nextGroupStartPos;
                    unsigned gn=start;
                    do
                    {
                        INode &groupNode = dfsGroup.queryNode(gn);
                        if (node->equals(&groupNode))
                        {
                            std::vector<unsigned> &slaveParts = map.querySlaveParts(gn);
                            if (!partsOnSlaves->testSet(groupSize*p+gn))
                            {
                                slaveParts.push_back(p);
                                if (NotFound == mappedPos)
                                {
                                    mappedPos = gn;
                                    nextGroupStartPos = gn+1;
                                    if (nextGroupStartPos == groupSize)
                                        nextGroupStartPos = 0;
                                }
                            }
                        }
                        gn++;
                        if (gn == groupSize)
                            gn = 0;
                    }
                    while (gn != start);
                    if (NotFound == mappedPos)
                    {
                        // part not within the cluster, add it to all slave maps, meaning these part meta will be serialized to all slaves so they handle the lookups directly.
                        for (auto &slaveParts : map.slavePartMap)
                            slaveParts.push_back(p);
                    }
                }
                // NB: in case of queryLocalData(), doesn't really need whole map
                unsigned partIdx = part->queryPartIndex();
                if (superFileDesc)
                {
                    unsigned subfile, subpartnum;
                    superFileDesc->mapSubPart(partIdx, subfile, subpartnum);
                    partIdx = superWidth*subfile+subpartnum;
                }
                partsByPartIdx.push_back(partIdx);
                assertex(partIdx < totalParts);
                map.partToSlave[partIdx] = mappedPos;
            }
        }
        if (0 == numSuperIndexSubs)
        {
            for (unsigned p=0; p<totalParts; p++)
                map.allParts.push_back(p);
        }
        else // non-interleaved superindex
        {
            unsigned p=0;
            for (unsigned i=0; i<numSuperIndexSubs; i++)
            {
                for (unsigned kp=0; kp<superWidth; kp++)
                    map.allParts.push_back(p++);
                if (isIndexWithTlk)
                    p++; // TLK's serialized separately.
            }
        }
        // ensure sorted by partIdx, so that consistent order for partHandlers/lookup
        std::sort(map.allParts.begin(), map.allParts.end(), [partsByPartIdx](unsigned a, unsigned b) { return partsByPartIdx[a] < partsByPartIdx[b]; });
        // ensure sorted by partIdx, so that consistent order for partHandlers/lookup
        for (auto &slaveParts : map.slavePartMap)
            std::sort(slaveParts.begin(), slaveParts.end(), [partsByPartIdx](unsigned a, unsigned b) { return partsByPartIdx[a] < partsByPartIdx[b]; });
    }

public:
    CKeyedJoinMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        helper = (IHThorKeyedJoinArg *) queryHelper();
        unsigned numStats = helper->diskAccessRequired() ? 8 : 5; // see progressKinds array
        for (unsigned s=0; s<numStats; s++)
            progressInfoArr.append(*new ProgressInfo(queryJob()));
        reInit = 0 != (helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename)) || (helper->getJoinFlags() & JFvarindexfilename);

        /* NB: force options are there to force all parts to be remote, even if local to slave (handled on slave)
         * They have no effect if base options (remoteKeyedLookup, remoteKeyedFetch) are off.
         */
        remoteKeyedLookup = getOptBool(THOROPT_REMOTE_KEYED_LOOKUP);
        if (getOptBool(THOROPT_FORCE_REMOTE_KEYED_LOOKUP))
            remoteKeyedLookup = true;
        remoteKeyedFetch = getOptBool(THOROPT_REMOTE_KEYED_FETCH);
        if (getOptBool(THOROPT_FORCE_REMOTE_KEYED_FETCH))
            remoteKeyedFetch = true;

        if (container.queryLocalData())
        {
            remoteKeyedLookup = false;
            remoteKeyedFetch = false;
        }

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

                        /* If fetch file is local to cluster, fetches are sent to the slave the parts are local to.
                         * If fetch file is off cluster, fetches are performed by requesting node directly on fetch part, therefore each nodes
                         * needs all part descriptors.
                         */
                        if (remoteKeyedFetch)
                        {
                            RemoteFilename rfn;
                            dataFileDesc->queryPart(0)->getFilename(0, rfn);
                            if (!rfn.queryIP().ipequals(container.queryJob().querySlaveGroup().queryNode(0).endpoint()))
                                remoteKeyedFetch = false;
                        }
                        mapParts(dataMap, dataFile, false);
                    }
                }
            }
            if (!helper->diskAccessRequired() || dataFileDesc)
            {
                bool localKey = indexFile->queryAttributes().getPropBool("@local");
                checkFormatCrc(this, indexFile, helper->getIndexFormatCrc(), helper->queryProjectedIndexRecordSize(), helper->queryIndexRecordSize(), true);
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

                // serialize common (to all slaves) info
                initMb.append(totalIndexParts);
                if (totalIndexParts)
                {
                    initMb.append(numTags);
                    for (auto &tag: tags)
                        initMb.append(tag);
                    initMb.append(remoteKeyedLookup);
                    initMb.append(remoteKeyedFetch);
                    mapParts(indexMap, indexFile, keyHasTlk);
                    initMb.append(superIndexWidth); // 0 if not superIndex
                    if (localKey)
                        keyHasTlk = false; // JCSMORE, not used at least for now
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
                }
                else
                {
                    indexFile.clear();
                    indexFileDesc.clear();
                    dataFile.clear();
                    dataFileDesc.clear();
                }
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
        dst.append(initMb);
        if (totalIndexParts)
        {
            if (container.queryLocalData())
            {
                std::vector<unsigned> &parts = indexMap.querySlaveParts(slave);
                unsigned numParts = parts.size();
                dst.append(numParts);
                indexFileDesc->serializeParts(dst, &parts[0], numParts);
            }
            else
            {
                std::vector<unsigned> &parts = indexMap.queryAllParts();
                unsigned numParts = parts.size();
                dst.append(numParts);
                indexFileDesc->serializeParts(dst, &parts[0], numParts);
                if (remoteKeyedLookup)
                {
                    std::vector<unsigned> &parts = indexMap.querySlaveParts(slave);
                    dst.append(sizeof(unsigned)*parts.size(), &parts[0]);
                    indexMap.serializePartMap(dst);
                }
            }
            unsigned totalDataParts = dataMap.count();
            dst.append(totalDataParts);
            if (totalDataParts)
            {
                std::vector<unsigned> &parts = dataMap.queryAllParts();
                unsigned numParts = parts.size();
                dst.append(numParts);
                dataFileDesc->serializeParts(dst, &parts[0], numParts);
                if (remoteKeyedFetch)
                {
                    std::vector<unsigned> &parts = dataMap.querySlaveParts(slave);
                    dst.append(sizeof(unsigned)*parts.size(), &parts[0]);
                    dataMap.serializePartMap(dst);
                }
            }
        }
    }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        CMasterActivity::deserializeStats(node, mb);
        ForEachItemIn(p, progressInfoArr)
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
            stats.addStatistic(progressKinds[p], progress.queryTotal());
        }
    }
};


CActivityBase *createKeyedJoinActivityMaster(CMasterGraphElement *info)
{
    if (info->getOptBool("legacykj"))
        return LegacyKJ::createKeyedJoinActivityMaster(info);
    return new CKeyedJoinMaster(info);
}
