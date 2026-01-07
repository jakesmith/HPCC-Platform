# Conceptual Test Plan for compfilecmp

## Test Scenarios

### 1. Identical Files
**Setup**: Two compressed files with identical content and structure
**Expected**: Tool reports 100% match, all blocks identical

### 2. Different Files  
**Setup**: Two compressed files with different content
**Expected**: Tool reports first difference location and matching percentage

### 3. Partial Match
**Setup**: Two files where first N blocks match, then diverge
**Expected**: Tool reports blocks 1-N match, stops at N+1

### 4. Different Sizes
**Setup**: One file with 100 blocks, another with 50 blocks, first 50 match
**Expected**: Tool reports first 50 blocks match, notes different total sizes

### 5. Invalid Files
**Setup**: Non-compressed files or corrupt files
**Expected**: Tool reports error that files are not valid compressed files

## How to Test Once Built

```bash
# After building HPCC Platform with this tool:

# Test with real compressed files from HPCC
compfilecmp /path/to/file1._1_of_2 /path/to/file1._2_of_2

# Create test files using copyexp or other HPCC tools
# Then compare them
compfilecmp testfile1 testfile2
```

## Manual Code Review Checklist

- [x] Structures match jlzw.cpp definitions
- [x] Proper error handling for file I/O
- [x] Correct calculation of index size (numBlocks * sizeof(offset_t))
- [x] Reads trailer from end of file (size - trailer_size)
- [x] Reads index from trailer.indexPos position
- [x] Compares index entries sequentially
- [x] Reports first difference accurately
- [x] Calculates percentages correctly
- [x] Handles edge cases (empty files, single block, etc.)
- [x] Proper memory management with MemoryAttr
- [x] Uses Owned<> for automatic cleanup
- [x] Follows HPCC coding style and patterns

## Implementation Verification

The implementation:
1. ✓ Opens both files using IFile/IFileIO interfaces
2. ✓ Reads WinCompressedFileTrailer (backward compatible)
3. ✓ Translates to CompressedFileTrailer
4. ✓ Calculates numBlocks from trailer
5. ✓ Reads index arrays from both files
6. ✓ Compares offset_t values sequentially
7. ✓ Reports match/mismatch with detailed information
8. ✓ Calculates matching expanded size and percentages
9. ✓ Returns appropriate exit codes
