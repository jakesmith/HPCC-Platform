#!/usr/bin/env python3
"""
File Size Analysis Script for LFNS CSV Data

Analyzes the @size field from a CSV file where some rows have 4 fields (missing Attr/@expireDays)
and others have 5 fields. The @size field is always the last field.

Usage: python3 analyze_file_sizes.py <csv_file>
"""

import csv
import sys
import statistics
import numpy as np
from collections import defaultdict
import re

def is_timestamp_format(value):
    """Check if a value looks like a timestamp (e.g., 2015-06-15T04:15:05)."""
    timestamp_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
    return bool(re.match(timestamp_pattern, value.strip()))

def parse_csv_file(filename, error_file=None, fix_accessed=False, fix_size=False):
    """Parse CSV file and extract size values."""
    sizes = []
    line_count = 0
    error_count = 0
    size_field_index = None
    accessed_field_index = None
    error_log = []
    
    print(f"Reading file: {filename}")
    
    with open(filename, 'r', encoding='utf-8') as file:
        # Parse header to find field positions
        header_line = file.readline().strip()
        header_fields = header_line.split(',')
        
        print(f"Header: {header_line}")
        print(f"Header fields: {header_fields}")
        
        # Find the index of the @size field (Attr/@size) and @accessed field
        for i, field in enumerate(header_fields):
            if 'size' in field.lower() or field.strip() == 'Attr/@size':
                size_field_index = i
            elif 'accessed' in field.lower() or field.strip() == 'Attr/@accessed':
                accessed_field_index = i
        
        if size_field_index is None:
            print("Error: Could not find size field in header!")
            return []
        
        print(f"Size field found at index: {size_field_index} ('{header_fields[size_field_index]}')")
        if fix_accessed and accessed_field_index is not None:
            print(f"Accessed field found at index: {accessed_field_index} ('{header_fields[accessed_field_index]}')")
        is_size_last_field = (size_field_index == len(header_fields) - 1)
        print(f"Size field is last field in header: {is_size_last_field}")
        print(f"Expected header field count: {len(header_fields)}")
        if fix_accessed:
            print("Missing Attr/@accessed detection: ENABLED")
        if fix_size:
            print("Missing field handling for last @size position: ENABLED")
        
        for line_num, line in enumerate(file, start=2):
            line_count += 1
            if line_count % 100000 == 0:
                print(f"Processed {line_count:,} lines...")
            
            # Split by comma
            fields = line.strip().split(',')
            
            try:
                # Determine the correct size field index based on row length and missing field detection
                current_size_index = size_field_index
                accessed_missing = False
                
                if len(fields) == len(header_fields):
                    # Row has same number of fields as header - use original index
                    current_size_index = size_field_index
                elif len(fields) >= 3 and fix_accessed and accessed_field_index is not None:
                    # Handle cases where accessed field might be missing (including 3-field rows)
                    # Check if @accessed field is present by looking at the expected position
                    accessed_present = False
                    
                    if len(fields) > accessed_field_index:
                        # Check if field at expected @accessed position is a timestamp
                        accessed_present = is_timestamp_format(fields[accessed_field_index])
                    
                    if not accessed_present:
                        # @accessed field is missing - calculate new size position
                        if len(fields) == 3:
                            # 3 fields: assume OrigName, @modified, @size (both @accessed and other field missing)
                            current_size_index = 2
                        elif len(fields) == 4:
                            # 4 fields: @accessed is missing, size shifts left by 1
                            if accessed_field_index < size_field_index:
                                current_size_index = size_field_index - 1
                            else:
                                current_size_index = size_field_index
                        else:
                            # Other cases - assume @accessed is missing, adjust accordingly
                            if accessed_field_index < size_field_index:
                                current_size_index = size_field_index - 1
                            else:
                                current_size_index = size_field_index
                    else:
                        # @accessed field is present, use original logic for other missing fields
                        if len(fields) == len(header_fields) - 1:
                            if size_field_index == len(header_fields) - 1:
                                current_size_index = size_field_index - 1
                            else:
                                current_size_index = size_field_index
                        else:
                            current_size_index = size_field_index
                elif len(fields) == len(header_fields) - 1:
                    # Row has one less field than header
                    if fix_size and size_field_index == len(header_fields) - 1:
                        # Apply size fix when @size is last field
                        current_size_index = size_field_index - 1
                    else:
                        # No fix applied - use original index
                        current_size_index = size_field_index
                else:
                    # Row has unexpected number of fields
                    error_count += 1
                    error_log.append({
                        'line_num': line_num,
                        'error': f'Unexpected field count: {len(fields)} (expected {len(header_fields)} or {len(header_fields)-1})',
                        'line': line.strip()
                    })
                    continue
                
                # Extract size from the calculated index
                if len(fields) > current_size_index >= 0:
                    size_str = fields[current_size_index].strip()
                    if size_str.isdigit():
                        sizes.append(int(size_str))
                    else:
                        error_count += 1
                        error_log.append({
                            'line_num': line_num,
                            'error': f'Invalid size value: "{size_str}" at index {current_size_index}',
                            'line': line.strip()
                        })
                else:
                    error_count += 1
                    error_log.append({
                        'line_num': line_num,
                        'error': f'Size index {current_size_index} out of bounds for {len(fields)} fields',
                        'line': line.strip()
                    })
            except (ValueError, IndexError) as e:
                error_count += 1
                error_log.append({
                    'line_num': line_num,
                    'error': f'Exception: {str(e)}',
                    'line': line.strip()
                })
                continue
    
    print(f"Total lines processed: {line_count:,}")
    print(f"Valid size entries: {len(sizes):,}")
    print(f"Invalid entries: {error_count:,}")
    
    # Write error log to file if requested
    if error_file and error_log:
        try:
            with open(error_file, 'w', encoding='utf-8') as ef:
                ef.write(f"Error Log for: {filename}\n")
                ef.write(f"Generated on: {__import__('datetime').datetime.now()}\n")
                ef.write(f"Total errors: {len(error_log)}\n")
                ef.write("="*80 + "\n\n")
                
                for i, error in enumerate(error_log, 1):
                    ef.write(f"Error #{i}:\n")
                    ef.write(f"  Line: {error['line_num']}\n")
                    ef.write(f"  Issue: {error['error']}\n")
                    ef.write(f"  Content: {error['line']}\n")
                    ef.write("-" * 40 + "\n\n")
            
            print(f"Error log written to: {error_file}")
        except Exception as e:
            print(f"Warning: Could not write error log to {error_file}: {e}")
    elif error_log:
        print("Note: Use --error-file option to save error details to a file")
    
    return sizes

def calculate_percentiles(sizes, percentiles):
    """Calculate percentiles for the given data."""
    return np.percentile(sizes, percentiles)

def format_bytes(bytes_value):
    """Format bytes into human-readable format."""
    if bytes_value == 0:
        return "0 bytes"
    
    units = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB']
    size = bytes_value
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.2f} {units[unit_index]}"

def create_size_buckets(sizes):
    """Create size distribution buckets with size totals."""
    buckets = defaultdict(int)
    bucket_sizes = defaultdict(int)
    
    for size in sizes:
        if size == 0:
            buckets["0 bytes"] += 1
            bucket_sizes["0 bytes"] += size
        elif size < 1024:
            buckets["< 1 KB"] += 1
            bucket_sizes["< 1 KB"] += size
        elif size < 1024 * 1024:
            buckets["1 KB - 1 MB"] += 1
            bucket_sizes["1 KB - 1 MB"] += size
        elif size < 10 * 1024 * 1024:
            buckets["1 MB - 10 MB"] += 1
            bucket_sizes["1 MB - 10 MB"] += size
        elif size < 100 * 1024 * 1024:
            buckets["10 MB - 100 MB"] += 1
            bucket_sizes["10 MB - 100 MB"] += size
        elif size < 1024 * 1024 * 1024:
            buckets["100 MB - 1 GB"] += 1
            bucket_sizes["100 MB - 1 GB"] += size
        elif size < 10 * 1024 * 1024 * 1024:
            buckets["1 GB - 10 GB"] += 1
            bucket_sizes["1 GB - 10 GB"] += size
        elif size < 100 * 1024 * 1024 * 1024:
            buckets["10 GB - 100 GB"] += 1
            bucket_sizes["10 GB - 100 GB"] += size
        elif size < 1024 * 1024 * 1024 * 1024:
            buckets["100 GB - 1 TB"] += 1
            bucket_sizes["100 GB - 1 TB"] += size
        else:
            buckets["> 1 TB"] += 1
            bucket_sizes["> 1 TB"] += size
    
    return buckets, bucket_sizes

def print_statistics(sizes):
    """Print comprehensive statistics about file sizes."""
    if not sizes:
        print("No valid size data found!")
        return
    
    # Sort sizes for percentile calculations
    sizes_sorted = sorted(sizes)
    n = len(sizes)
    
    # Basic statistics
    total_size = sum(sizes)
    mean_size = statistics.mean(sizes)
    std_dev = statistics.stdev(sizes) if n > 1 else 0
    median = statistics.median(sizes)
    
    print("\n" + "="*60)
    print("FILE SIZE STATISTICS")
    print("="*60)
    
    print(f"Total files: {n:,}")
    print(f"Total size: {format_bytes(total_size)} ({total_size:,} bytes)")
    print(f"Mean size: {format_bytes(mean_size)} ({mean_size:,.2f} bytes)")
    print(f"Standard deviation: {format_bytes(std_dev)} ({std_dev:,.2f} bytes)")
    print(f"Median size: {format_bytes(median)} ({median:,} bytes)")
    print(f"Minimum size: {format_bytes(min(sizes))} ({min(sizes):,} bytes)")
    print(f"Maximum size: {format_bytes(max(sizes))} ({max(sizes):,} bytes)")
    
    # Percentiles
    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    percentile_values = calculate_percentiles(sizes_sorted, percentiles)
    
    print("\n" + "="*60)
    print("PERCENTILES")
    print("="*60)
    
    for p, value in zip(percentiles, percentile_values):
        print(f"{p:2d}th percentile: {format_bytes(value)} ({value:,.0f} bytes)")
    
    # Size distribution
    buckets, bucket_sizes = create_size_buckets(sizes)
    bucket_order = [
        "0 bytes", "< 1 KB", "1 KB - 1 MB", "1 MB - 10 MB", 
        "10 MB - 100 MB", "100 MB - 1 GB", "1 GB - 10 GB",
        "10 GB - 100 GB", "100 GB - 1 TB", "> 1 TB"
    ]
    
    print("\n" + "="*60)
    print("SIZE DISTRIBUTION")
    print("="*60)
    print(f"{'Size Range':<20} {'Count':>12} {'% Files':>10} {'% Data':>10}")
    print("-" * 54)
    
    for bucket_name in bucket_order:
        count = buckets.get(bucket_name, 0)
        size_total = bucket_sizes.get(bucket_name, 0)
        percent_files = (count / n) * 100
        percent_data = (size_total / total_size) * 100 if total_size > 0 else 0
        print(f"{bucket_name:<20} {count:>12,} {percent_files:>9.2f}% {percent_data:>9.2f}%")
    
    print("-" * 54)
    print(f"{'Total':<20} {n:>12,} {100.0:>9.2f}% {100.0:>9.2f}%")
    
    # Top 10 largest files
    print("\n" + "="*60)
    print("TOP 10 LARGEST FILES")
    print("="*60)
    print(f"{'Rank':<6} {'Size (bytes)':<20} {'Size (formatted)':<15}")
    print("-" * 50)
    
    largest_files = sorted(sizes, reverse=True)[:10]
    for i, size in enumerate(largest_files, 1):
        print(f"{i:<6} {size:<20,} {format_bytes(size):<15}")

def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze file sizes from CSV data')
    parser.add_argument('csv_file', help='Path to the CSV file to analyze')
    parser.add_argument('--error-file', '-e', help='Path to write error log (optional)')
    parser.add_argument('--fix-accessed', action='store_true', 
                        help='Handle missing Attr/@accessed field by detecting timestamp format')
    parser.add_argument('--fix-size', action='store_true',
                        help='Handle missing fields when @size is the last field (legacy behavior)')
    
    args = parser.parse_args()
    
    try:
        sizes = parse_csv_file(args.csv_file, args.error_file, args.fix_accessed, args.fix_size)
        print_statistics(sizes)
    except FileNotFoundError:
        print(f"Error: File '{args.csv_file}' not found!")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()