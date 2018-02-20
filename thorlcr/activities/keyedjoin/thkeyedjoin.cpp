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
        Owned<IDistributedFile> dataFile;
        Owned<IDistributedFile> indexFile = queryThorFileManager().lookup(container.queryJob(), indexFileName, false, 0 != (helper->getJoinFlags() & JFindexoptional), true);

        unsigned keyReadWidth = (unsigned)container.queryJob().getWorkUnitValueInt("KJKRR", 0);
        if (!keyReadWidth || keyReadWidth>container.queryJob().querySlaves())
            keyReadWidth = container.queryJob().querySlaves();
        
        initMb.clear();
        initMb.append(indexFileName.get());
        initMb.append(numTags);
        for (auto &tag: tags)
            initMb.append(tag);
        bool keyHasTlk = false;
        if (indexFile)
        {
            if (!isFileKey(indexFile))
                throw MakeActivityException(this, 0, "Attempting to read flat file as an index: %s", indexFileName.get());
            unsigned numParts = 0;
            bool localKey = indexFile->queryAttributes().getPropBool("@local");

            checkFormatCrc(this, indexFile, helper->getIndexFormatCrc(), true);
            indexFileDesc.setown(indexFile->getFileDescriptor());
            IDistributedSuperFile *superIndex = indexFile->querySuperFile();
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
                    numParts = superIndexWidth * numSuperIndexSubs;
                else
                    numParts = superIndex->numParts();
            }
            else
            {
                numParts = indexFile->numParts();
                if (numParts)
                {
                    const char *kind = indexFile->queryPart(indexFile->numParts()-1).queryAttributes().queryProp("@kind");
                    keyHasTlk = NULL != kind && 0 == stricmp("topLevelKey", kind);
                    if (keyHasTlk)
                        --numParts;
                }
            }
            numPartsOffset = initMb.length();
            if (localKey)
                keyHasTlk = false; // JCSMORE, not used at least for now
            if (numParts)
            {
                initMb.append(numParts); // placeholder
                initMb.append(superIndexWidth); // 0 if not superIndex
                bool interleaved = superIndex && superIndex->isInterleaved();
                initMb.append(numSuperIndexSubs);
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
                        else
                            indexFile.clear();
                    }
                }
                if (container.queryLocalData() || remoteKeyedLookups)
                {
                    mapping.setown(getFileSlaveMaps(indexFile->queryLogicalName(), *indexFileDesc, container.queryJob().queryUserDescriptor(), container.queryJob().querySlaveGroup(), false, true, NULL, indexFile->querySuperFile()));
                    // leave seralizeMetaData to serialize the key parts
                }
                else
                {
                    UnsignedArray parts;
                    if (!superIndex || interleaved) // serialize first numParts parts, TLK are at end and are serialized separately.
                    {
                        for (unsigned p=0; p<numParts; p++)
                            parts.append(p);
                    }
                    else // non-interleaved superindex
                    {
                        unsigned p=0;
                        for (unsigned i=0; i<numSuperIndexSubs; i++)
                        {
                            for (unsigned kp=0; kp<superIndexWidth; kp++)
                                parts.append(p++);
                            if (keyHasTlk)
                                p++; // TLK's serialized separately.
                        }
                    }
                    indexFileDesc->serializeParts(initMb, parts);
                }
            }
            else
                indexFile.clear();
        }
        if (indexFile)
        {
            addReadFile(indexFile);
            if (dataFile)
                addReadFile(dataFile);
        }
        else
            initMb.append((unsigned)0); // no key
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        unsigned numParts = 0;
        if (mapping)
        {
            IArrayOf<IPartDescriptor> parts;
            mapping->getParts(slave, parts);
            numParts = parts.ordinality();
            if (0 == numParts)
            {
                initMb.setLength(numPartsOffset);
                initMb.append(numParts);
                dst.append(initMb);
            }
            else
            {
                initMb.writeDirect(numPartsOffset, sizeof(numParts), &numParts);
                dst.append(initMb);

                if (remoteKeyedLookups)
                {
                    mapping->serializeMap(slave, dst, false);
                    dst.append(mapping->queryParts());
                    for (unsigned p=0; p<mapping->queryParts(); p++)
                        dst.append(mapping->queryNode(p));
                }
                else
                {
                    UnsignedArray partNumbers;
                    ForEachItemIn(p2, parts)
                        partNumbers.append(parts.item(p2).queryPartIndex());
                    indexFileDesc->serializeParts(dst, partNumbers);
                }
            }
        }
        else
            dst.append(initMb);

        IDistributedFile *indexFile = queryReadFile(0); // 0 == indexFile, 1 == dataFile
        if (indexFile && helper->diskAccessRequired() && (!mapping || numParts))
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
