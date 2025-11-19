/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

#include <stdio.h>
#include <string.h>
#include "jlib.hpp"
#include "jfile.hpp"
#include "jio.hpp"
#include "jexcept.hpp"

// Compressed file trailer structure (from jlzw.cpp)
#pragma pack(push, 1)
struct CompressedFileTrailer
{
    unsigned        datacrc;
    offset_t        expandedSize;
    offset_t        indexPos;       // end of blocks - start of index
    size32_t        blockSize;
    size32_t        recordSize;     // 0 is lzw or fastlz or lz4
    __int64         compressedType;
    unsigned        crc;                // must be last
    
    unsigned numBlocks() const { return (unsigned)((indexPos+blockSize-1)/blockSize); }
};

// Backward compatibility structure
struct WinCompressedFileTrailer
{
    unsigned        datacrc;
    unsigned        filler1;
    offset_t        expandedSize;
    offset_t        indexPos;       // end of blocks
    size32_t        blockSize;
    size32_t        recordSize;     // 0 is lzw or fastlz or lz4
    __int64         compressedType;
    unsigned        crc;            // must be last
    unsigned        filler2;

    void translate(CompressedFileTrailer &out)
    {
        out.datacrc = datacrc;
        out.expandedSize = expandedSize;
        out.indexPos = indexPos;
        out.blockSize = blockSize;
        out.recordSize = recordSize;
        out.compressedType = compressedType;
        out.crc = crc;
    }
};
#pragma pack(pop)

void usage(bool isHelp)
{
    printf("usage:\n"
           "  compfilecmp file1 file2\n"
           "\n"
           "  Compares two compressed file parts by reading their block index offsets.\n"
           "  Reports how much of the files appear to be the same based on how far\n"
           "  the comparison of index offsets has reached.\n"
           "\n"
           "usage:\n"
           "  compfilecmp [-h | -? | --help ]\n\n");
    exit(isHelp ? 0 : 2);
}

bool readCompressedFileTrailer(IFileIO *fileio, CompressedFileTrailer &trailer)
{
    offset_t fsize = fileio->size();
    
    if (fsize < sizeof(WinCompressedFileTrailer))
        return false;
    
    WinCompressedFileTrailer wintrailer;
    if (fileio->read(fsize-sizeof(WinCompressedFileTrailer), sizeof(WinCompressedFileTrailer), &wintrailer) != sizeof(WinCompressedFileTrailer))
        return false;
    
    wintrailer.translate(trailer);
    return true;
}

bool compareIndexOffsets(IFileIO *file1, IFileIO *file2, CompressedFileTrailer &trailer1, CompressedFileTrailer &trailer2)
{
    unsigned numBlocks1 = trailer1.numBlocks();
    unsigned numBlocks2 = trailer2.numBlocks();
    
    printf("File 1: %u blocks, expanded size: %" I64F "u, index position: %" I64F "u\n", 
           numBlocks1, trailer1.expandedSize, trailer1.indexPos);
    printf("File 2: %u blocks, expanded size: %" I64F "u, index position: %" I64F "u\n", 
           numBlocks2, trailer2.expandedSize, trailer2.indexPos);
    
    if (numBlocks1 == 0 && numBlocks2 == 0)
    {
        printf("Both files are empty compressed files.\n");
        return true;
    }
    
    unsigned minBlocks = (numBlocks1 < numBlocks2) ? numBlocks1 : numBlocks2;
    
    // Read index data - each index entry is an offset_t (8 bytes)
    size32_t indexSize1 = sizeof(offset_t) * numBlocks1;
    size32_t indexSize2 = sizeof(offset_t) * numBlocks2;
    
    MemoryAttr indexBuf1, indexBuf2;
    offset_t *index1 = (offset_t *)indexBuf1.allocate(indexSize1);
    offset_t *index2 = (offset_t *)indexBuf2.allocate(indexSize2);
    
    if (file1->read(trailer1.indexPos, indexSize1, index1) != indexSize1)
    {
        fprintf(stderr, "Error: Failed to read index from file 1\n");
        return false;
    }
    
    if (file2->read(trailer2.indexPos, indexSize2, index2) != indexSize2)
    {
        fprintf(stderr, "Error: Failed to read index from file 2\n");
        return false;
    }
    
    // Compare the index offsets
    unsigned matchingBlocks = 0;
    for (unsigned i = 0; i < minBlocks; i++)
    {
        if (index1[i] != index2[i])
        {
            printf("First difference found at block %u:\n", i);
            printf("  File 1 offset: %" I64F "u\n", index1[i]);
            printf("  File 2 offset: %" I64F "u\n", index2[i]);
            break;
        }
        matchingBlocks = i + 1;
    }
    
    if (matchingBlocks == minBlocks)
    {
        if (numBlocks1 == numBlocks2)
            printf("All %u block offsets match - files appear identical.\n", matchingBlocks);
        else
            printf("First %u block offsets match, but files have different lengths (%u vs %u blocks).\n", 
                   matchingBlocks, numBlocks1, numBlocks2);
    }
    else
    {
        printf("Files match up to block %u out of %u blocks.\n", matchingBlocks, minBlocks);
    }
    
    // Calculate percentage and expanded size that matches
    if (matchingBlocks > 0)
    {
        offset_t matchingExpandedSize = (matchingBlocks > 0) ? index1[matchingBlocks - 1] : 0;
        double percentageFile1 = (trailer1.expandedSize > 0) ? (100.0 * matchingExpandedSize / trailer1.expandedSize) : 0.0;
        double percentageFile2 = (trailer2.expandedSize > 0) ? (100.0 * matchingExpandedSize / trailer2.expandedSize) : 0.0;
        
        printf("\nMatching expanded size: %" I64F "u bytes\n", matchingExpandedSize);
        printf("  Percentage of file 1: %.2f%%\n", percentageFile1);
        printf("  Percentage of file 2: %.2f%%\n", percentageFile2);
    }
    
    return matchingBlocks == minBlocks;
}

int main(int argc, char * const * argv)
{
    InitModuleObjects();
    
    try
    {
        if (argc < 2 || strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "-?") == 0 || strcmp(argv[1], "--help") == 0)
        {
            usage(argc >= 2);
        }
        
        if (argc != 3)
        {
            fprintf(stderr, "Error: Expected exactly two file arguments\n");
            usage(false);
        }
        
        const char *filename1 = argv[1];
        const char *filename2 = argv[2];
        
        printf("Comparing compressed files:\n");
        printf("  File 1: %s\n", filename1);
        printf("  File 2: %s\n", filename2);
        printf("\n");
        
        // Open the first file
        Owned<IFile> file1 = createIFile(filename1);
        if (!file1->exists())
        {
            fprintf(stderr, "Error: File 1 does not exist: %s\n", filename1);
            return 1;
        }
        
        Owned<IFileIO> fileio1 = file1->open(IFOread);
        if (!fileio1)
        {
            fprintf(stderr, "Error: Failed to open file 1: %s\n", filename1);
            return 1;
        }
        
        // Open the second file
        Owned<IFile> file2 = createIFile(filename2);
        if (!file2->exists())
        {
            fprintf(stderr, "Error: File 2 does not exist: %s\n", filename2);
            return 1;
        }
        
        Owned<IFileIO> fileio2 = file2->open(IFOread);
        if (!fileio2)
        {
            fprintf(stderr, "Error: Failed to open file 2: %s\n", filename2);
            return 1;
        }
        
        // Read trailers
        CompressedFileTrailer trailer1, trailer2;
        
        if (!readCompressedFileTrailer(fileio1, trailer1))
        {
            fprintf(stderr, "Error: File 1 does not appear to be a valid compressed file\n");
            return 1;
        }
        
        if (!readCompressedFileTrailer(fileio2, trailer2))
        {
            fprintf(stderr, "Error: File 2 does not appear to be a valid compressed file\n");
            return 1;
        }
        
        // Compare the files
        bool result = compareIndexOffsets(fileio1, fileio2, trailer1, trailer2);
        
        fileio1->close();
        fileio2->close();
        
        return result ? 0 : 1;
    }
    catch (IException *e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        fprintf(stderr, "Error: %s\n", msg.str());
        e->Release();
        return 1;
    }
    catch (...)
    {
        fprintf(stderr, "Error: Unexpected exception\n");
        return 1;
    }
    
    releaseAtoms();
    return 0;
}
