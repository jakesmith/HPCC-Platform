# compfilecmp - Compressed File Part Comparison Tool

## Purpose

This tool compares two compressed file parts by reading and comparing their block index offsets. It is designed to work with the compressed file format used in HPCC Systems, which stores an index of expanded block sizes at the end of each file.

## Compressed File Format

The compressed file format (as defined in `system/jlib/jlzw.cpp`) consists of:

1. **Compressed Data Blocks**: Fixed-size blocks of compressed data
2. **Block Index**: An array of `offset_t` values (64-bit integers) located at `indexPos`, where each entry represents the cumulative expanded size up to that block
3. **Trailer**: A `CompressedFileTrailer` structure at the end of the file containing metadata including:
   - `datacrc`: CRC of the data
   - `expandedSize`: Total size when expanded
   - `indexPos`: Position where the index starts (end of compressed blocks)
   - `blockSize`: Size of each compressed block
   - `recordSize`: Record size (0 for LZW/FastLZ/LZ4)
   - `compressedType`: Type of compression used
   - `crc`: Overall CRC

## How It Works

The tool:

1. Opens both compressed file parts
2. Reads the `CompressedFileTrailer` from the end of each file
3. Extracts the block index array from each file (starting at `indexPos`)
4. Compares the index offsets entry by entry
5. Reports:
   - Where the first difference occurs (if any)
   - How many blocks match
   - The expanded size that matches
   - Percentage of each file that matches

## Usage

```bash
compfilecmp file1 file2
```

### Example Output

```
Comparing compressed files:
  File 1: /path/to/file1._1_of_2
  File 2: /path/to/file2._1_of_2

File 1: 100 blocks, expanded size: 1048576, index position: 524288
File 2: 100 blocks, expanded size: 1048576, index position: 524288
All 100 block offsets match - files appear identical.

Matching expanded size: 1048576 bytes
  Percentage of file 1: 100.00%
  Percentage of file 2: 100.00%
```

Or when files differ:

```
First difference found at block 50:
  File 1 offset: 524288
  File 2 offset: 524300
Files match up to block 50 out of 100 blocks.

Matching expanded size: 524288 bytes
  Percentage of file 1: 50.00%
  Percentage of file 2: 50.00%
```

## Return Codes

- `0`: Files match completely
- `1`: Files differ or an error occurred

## Building

This tool is built as part of the HPCC Platform build process. It will be installed to the `bin` directory.

## Implementation Notes

- The tool supports both the current `CompressedFileTrailer` format and the legacy `WinCompressedFileTrailer` format for backward compatibility
- Each block index entry is an `offset_t` (8 bytes on 64-bit systems)
- The comparison stops at the first difference and reports the position
- The tool calculates both the absolute matching size and the percentage for each file
