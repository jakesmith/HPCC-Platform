#!/usr/bin/env python3
"""
Analyze HPCC file access logs from get_file_access_logs.py output.

Aggregates file access operations by time buckets, components, actions, and logical filenames.
Provides statistics on operation counts and data sizes.

Input:
    CSV output from get_file_access_logs.py (via stdin or file)
    Format: timestamp,FileAccess,Component,Action,Plane,User,LogicalFilename,...

Usage:
    get_file_access_logs.py ... | python3 analyze_file_access.py [options]
    python3 analyze_file_access.py input.csv [options]

Examples:
    # Analyze all READ operations in 1-hour buckets
    cat file_access.csv | python3 analyze_file_access.py --filter-action READ --granularity 1

    # Analyze Thor and FileServices CREATE/DELETE operations for specific files
    python3 analyze_file_access.py data.csv --filter-component Thor,FileServices \\
        --filter-action CREATED,DELETED --filter-lfn "thor_data400::key::*" --granularity 1

    # Generate text report
    cat file_access.csv | python3 analyze_file_access.py --format text --granularity 1
"""

import sys
import argparse
import csv
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Optional, Set, Tuple
import fnmatch


def load_pricing_data(pricing_csv: str) -> Tuple[float, float]:
    """Load pricing data from get_storage_pricing.py output.
    
    Returns: (read_price_per_10k_ops, write_price_per_10k_ops)
    """
    read_price = None
    write_price = None
    
    with open(pricing_csv, 'r') as f:
        # Skip comment lines
        lines = [line for line in f if not line.strip().startswith('#')]
    
    # Parse CSV from non-comment lines
    reader = csv.DictReader(lines)
    for row in reader:
        category = row.get('PricingCategory', '')
        price_str = row.get('PriceUSD', '')
        unit = row.get('Unit', '')
        
        try:
            price = float(price_str)
        except (ValueError, TypeError):
            continue
        
        if 'Read Operations' in category and '10000 ops' in unit:
            read_price = price
        elif 'Write Operations' in category and '10000 ops' in unit:
            write_price = price
    
    if read_price is None or write_price is None:
        raise ValueError(f"Could not find Read and Write operation pricing in {pricing_csv}")
    
    return read_price, write_price


def calculate_operation_cost(size_bytes: int, op_size_kb: int, price_per_10k_ops: float) -> float:
    """Calculate cost for operations based on data size and operation size.
    
    Args:
        size_bytes: Total data size in bytes
        op_size_kb: Operation size in KB (e.g., 4096 for 4MB)
        price_per_10k_ops: Price per 10,000 operations
    
    Returns:
        Cost in USD
    """
    if size_bytes == 0 or price_per_10k_ops == 0:
        return 0.0
    
    op_size_bytes = op_size_kb * 1024
    num_operations = max(1, (size_bytes + op_size_bytes - 1) // op_size_bytes)  # Ceiling division
    cost = (num_operations / 10000) * price_per_10k_ops
    return cost


def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string in multiple formats."""
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Invalid datetime format: {dt_str}")


def align_to_hour_bucket(dt: datetime, granularity_hours: int) -> datetime:
    """Align datetime to hour bucket boundary."""
    # Floor to the hour
    dt_hour = dt.replace(minute=0, second=0, microsecond=0)
    # Floor to granularity bucket
    hours_since_epoch = int(dt_hour.timestamp() / 3600)
    bucket_start_hours = (hours_since_epoch // granularity_hours) * granularity_hours
    return datetime.fromtimestamp(bucket_start_hours * 3600)


def matches_any_pattern(text: str, patterns: List[str]) -> bool:
    """Check if text matches any of the wildcard patterns."""
    if not patterns:
        return True
    return any(fnmatch.fnmatch(text, pattern) for pattern in patterns)


def parse_file_access_record(line: str) -> Optional[Dict]:
    """Parse a FileAccess CSV record.
    
    Returns dict with: timestamp, component, action, plane, user, logical_filename, size
    """
    # Skip comment lines
    if line.startswith('#'):
        return None
    
    fields = line.strip().split(',')
    
    if len(fields) < 7:
        return None
    
    try:
        timestamp_str = fields[0]
        # Skip if not starting with FileAccess marker
        if fields[1] != 'FileAccess':
            return None
        
        component = fields[2]
        action = fields[3]
        plane = fields[4]
        user = fields[5]
        logical_filename = fields[6]
        
        # Try to extract size - for Thor, it's typically after workunit/graph fields
        # Format: ...,LogicalFilename,WorkunitId,GraphName,Size[,...]
        size = 0
        if len(fields) > 9 and component == 'Thor':
            # Try field 9 as size (after workunit and graph)
            try:
                size = int(fields[9])
            except (ValueError, IndexError):
                pass
        
        timestamp = parse_datetime(timestamp_str)
        
        return {
            'timestamp': timestamp,
            'component': component,
            'action': action,
            'plane': plane,
            'user': user,
            'logical_filename': logical_filename,
            'size': size
        }
    except (ValueError, IndexError):
        return None


def read_records(input_source: Optional[str], start_time: Optional[datetime], 
                end_time: Optional[datetime]) -> List[Dict]:
    """Read and filter file access records."""
    records = []
    
    if input_source:
        # Read from file
        with open(input_source, 'r') as f:
            for line in f:
                record = parse_file_access_record(line)
                if record:
                    # Apply time filters
                    if start_time and record['timestamp'] < start_time:
                        continue
                    if end_time and record['timestamp'] >= end_time:
                        continue
                    records.append(record)
    else:
        # Read from stdin
        for line in sys.stdin:
            record = parse_file_access_record(line)
            if record:
                # Apply time filters
                if start_time and record['timestamp'] < start_time:
                    continue
                if end_time and record['timestamp'] >= end_time:
                    continue
                records.append(record)
    
    return records


def aggregate_records(records: List[Dict], granularity_hours: int,
                     component_filters: List[str], action_filters: List[str],
                     lfn_filters: List[str]) -> tuple:
    """Aggregate records by time bucket and filter dimensions.
    
    Returns tuple: (aggregates, file_action_counts, file_action_sizes)
    - aggregates: {time_bucket: {(component, action): {'count': N, 'size': B, 'files': set(), 'reread_size': B}}}
    - file_action_counts: {(filename, action): count}
    - file_action_sizes: {(bucket, filename, action): size}
    """
    aggregates = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'size': 0, 'files': set(), 'reread_size': 0}))
    file_action_counts = defaultdict(int)
    file_action_sizes = defaultdict(int)
    file_action_first_size = {}  # Track first seen size for each file/action in bucket
    
    for record in records:
        # Apply filters
        if component_filters and not matches_any_pattern(record['component'], component_filters):
            continue
        if action_filters and not matches_any_pattern(record['action'], action_filters):
            continue
        if lfn_filters and not matches_any_pattern(record['logical_filename'], lfn_filters):
            continue
        
        # Determine time bucket
        bucket = align_to_hour_bucket(record['timestamp'], granularity_hours)
        
        # Aggregate by (component, action)
        key = (record['component'], record['action'])
        aggregates[bucket][key]['count'] += 1
        aggregates[bucket][key]['size'] += record['size']
        aggregates[bucket][key]['files'].add(record['logical_filename'])
        
        # Track re-read size for READ operations
        if record['action'] == 'READ' and record['size'] > 0:
            file_bucket_key = (bucket, record['logical_filename'], record['action'])
            
            # If this file was already read in this bucket, add to reread size
            if file_bucket_key in file_action_first_size:
                aggregates[bucket][key]['reread_size'] += record['size']
            else:
                # First read of this file in this bucket
                file_action_first_size[file_bucket_key] = record['size']
        
        # Track per-file action counts
        file_key = (record['logical_filename'], record['action'])
        file_action_counts[file_key] += 1
        
        # Track sizes for each file/action in bucket
        bucket_file_key = (bucket, record['logical_filename'], record['action'])
        file_action_sizes[bucket_file_key] += record['size']
    
    return aggregates, file_action_counts, file_action_sizes


def format_size(size_bytes: int) -> str:
    """Format size in human-readable format."""
    if size_bytes == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    unit_index = 0
    size = float(size_bytes)
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.2f} {units[unit_index]}"


def output_csv(aggregates: Dict, granularity_hours: int, file_action_counts: Dict, 
               pricing_data: Optional[Tuple[float, float]], op_size_kb: int, args) -> None:
    """Output aggregated data in CSV format."""
    # Output metadata
    print("# Generated by: analyze_file_access.py")
    print(f"# Date generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Command: analyze_file_access.py", end='')
    if args.input:
        print(f" {args.input}", end='')
    if args.start_time:
        print(f" --start-time \"{args.start_time}\"", end='')
    if args.end_time:
        print(f" --end-time \"{args.end_time}\"", end='')
    if args.filter_component:
        print(f" --filter-component {args.filter_component}", end='')
    if args.filter_action:
        print(f" --filter-action {args.filter_action}", end='')
    if args.filter_lfn:
        print(f" --filter-lfn {args.filter_lfn}", end='')
    print(f" --granularity {granularity_hours}", end='')
    if args.costs:
        print(f" --costs {args.costs}", end='')
        print(f" --opsize {args.opsize}", end='')
    print()
    print(f"# Granularity: {granularity_hours} hour(s)")
    if pricing_data:
        read_price, write_price = pricing_data
        print(f"# Operation size: {op_size_kb} KB ({op_size_kb / 1024:.1f} MB)")
        print(f"# Read cost: ${read_price} per 10,000 operations")
        print(f"# Write cost: ${write_price} per 10,000 operations")
    print("#")
    
    # CSV header
    if pricing_data:
        print("TimeBucket,Component,Action,OperationCount,TotalSizeBytes,UniqueFiles,RereadSizeBytes,CostUSD")
    else:
        print("TimeBucket,Component,Action,OperationCount,TotalSizeBytes,UniqueFiles,RereadSizeBytes")
    
    # Sort by time bucket
    for bucket in sorted(aggregates.keys()):
        for (component, action), stats in sorted(aggregates[bucket].items()):
            bucket_str = bucket.strftime('%Y-%m-%d %H:%M:%S')
            reread_size = stats.get('reread_size', 0)
            
            # Calculate cost if pricing data available
            cost = 0.0
            if pricing_data:
                read_price, write_price = pricing_data
                if action == 'READ':
                    cost = calculate_operation_cost(stats['size'], op_size_kb, read_price)
                elif action in ['CREATED', 'DELETED', 'WRITE']:
                    cost = calculate_operation_cost(stats['size'], op_size_kb, write_price)
            
            if pricing_data:
                print(f"{bucket_str},{component},{action},{stats['count']},{stats['size']},{len(stats['files'])},{reread_size},{cost:.4f}")
            else:
                print(f"{bucket_str},{component},{action},{stats['count']},{stats['size']},{len(stats['files'])},{reread_size}")


def output_text(aggregates: Dict, granularity_hours: int, records: List[Dict], 
               file_action_counts: Dict, pricing_data: Optional[Tuple[float, float]], 
               op_size_kb: int, args) -> None:
    """Output aggregated data in human-readable text format."""
    print("=" * 80)
    print("FILE ACCESS ANALYSIS REPORT")
    print("=" * 80)
    print()
    
    # Summary statistics
    print("SUMMARY")
    print("-" * 80)
    print(f"Total Records:        {len(records):,}")
    print(f"Time Granularity:     {granularity_hours} hour(s)")
    print(f"Time Buckets:         {len(aggregates):,}")
    
    if args.start_time:
        print(f"Start Time Filter:    {args.start_time}")
    if args.end_time:
        print(f"End Time Filter:      {args.end_time}")
    if args.filter_component:
        print(f"Component Filter:     {args.filter_component}")
    if args.filter_action:
        print(f"Action Filter:        {args.filter_action}")
    if args.filter_lfn:
        print(f"LFN Filter:           {args.filter_lfn}")
    
    if pricing_data:
        read_price, write_price = pricing_data
        print()
        print("COST PARAMETERS")
        print("-" * 80)
        print(f"Operation Size:       {op_size_kb} KB ({op_size_kb / 1024:.1f} MB)")
        print(f"Read Cost:            ${read_price} per 10,000 operations")
        print(f"Write Cost:           ${write_price} per 10,000 operations")
    
    print()
    
    # Overall statistics
    total_operations = sum(stats['count'] for bucket_data in aggregates.values() 
                          for stats in bucket_data.values())
    
    # Separate read and write sizes
    total_read_size = sum(stats['size'] for bucket_data in aggregates.values() 
                         for (component, action), stats in bucket_data.items() 
                         if action == 'READ')
    total_write_size = sum(stats['size'] for bucket_data in aggregates.values() 
                          for (component, action), stats in bucket_data.items() 
                          if action in ['CREATED', 'DELETED', 'WRITE'])
    total_other_size = sum(stats['size'] for bucket_data in aggregates.values() 
                          for (component, action), stats in bucket_data.items() 
                          if action not in ['READ', 'CREATED', 'DELETED', 'WRITE'])
    total_size = total_read_size + total_write_size + total_other_size
    
    total_reread_size = sum(stats.get('reread_size', 0) for bucket_data in aggregates.values() 
                           for (component, action), stats in bucket_data.items() if action == 'READ')
    all_files = set()
    for bucket_data in aggregates.values():
        for stats in bucket_data.values():
            all_files.update(stats['files'])
    
    print(f"Total Operations:     {total_operations:,}")
    print(f"Total Data Read:      {format_size(total_read_size)} ({total_read_size:,} bytes)")
    print(f"Total Data Written:   {format_size(total_write_size)} ({total_write_size:,} bytes)")
    if total_other_size > 0:
        print(f"Total Other Data:     {format_size(total_other_size)} ({total_other_size:,} bytes)")
    print(f"Unique Files:         {len(all_files):,}")
    if total_reread_size > 0:
        print(f"Re-read Data Size:    {format_size(total_reread_size)} ({total_reread_size:,} bytes)")
        if total_read_size > 0:
            reread_pct = (total_reread_size / total_read_size) * 100
            print(f"Re-read Percentage:   {reread_pct:.1f}%")
    
    # Calculate total costs if pricing data available
    if pricing_data:
        read_price, write_price = pricing_data
        total_read_cost = 0.0
        total_write_cost = 0.0
        total_reread_cost = 0.0
        
        for bucket_data in aggregates.values():
            for (component, action), stats in bucket_data.items():
                if action == 'READ':
                    total_read_cost += calculate_operation_cost(stats['size'], op_size_kb, read_price)
                    if stats.get('reread_size', 0) > 0:
                        total_reread_cost += calculate_operation_cost(stats['reread_size'], op_size_kb, read_price)
                elif action in ['CREATED', 'DELETED', 'WRITE']:
                    total_write_cost += calculate_operation_cost(stats['size'], op_size_kb, write_price)
        
        print()
        print("ESTIMATED COSTS")
        print("-" * 80)
        print(f"Read Cost:            ${total_read_cost:.4f}")
        print(f"Write Cost:           ${total_write_cost:.4f}")
        print(f"Total Cost:           ${(total_read_cost + total_write_cost):.4f}")
        if total_reread_cost > 0:
            print(f"Re-read Cost:         ${total_reread_cost:.4f} (potential waste)")
    
    print()
    
    # Breakdown by component
    print("BREAKDOWN BY COMPONENT")
    print("-" * 80)
    component_stats = defaultdict(lambda: {'count': 0, 'size': 0})
    for bucket_data in aggregates.values():
        for (component, action), stats in bucket_data.items():
            component_stats[component]['count'] += stats['count']
            component_stats[component]['size'] += stats['size']
    
    for component in sorted(component_stats.keys()):
        stats = component_stats[component]
        print(f"{component:20s}  {stats['count']:10,} ops  {format_size(stats['size']):>15s}")
    print()
    
    # Breakdown by action
    print("BREAKDOWN BY ACTION")
    print("-" * 80)
    action_stats = defaultdict(lambda: {'count': 0, 'size': 0, 'unique_files': set()})
    for bucket_data in aggregates.values():
        for (component, action), stats in bucket_data.items():
            action_stats[action]['count'] += stats['count']
            action_stats[action]['size'] += stats['size']
            action_stats[action]['unique_files'].update(stats['files'])
    
    print(f"{'Action':<20s}  {'Operations':>10s}  {'Total Size':>15s}  {'Unique Files':>12s}")
    print("-" * 80)
    for action in sorted(action_stats.keys()):
        stats = action_stats[action]
        print(f"{action:20s}  {stats['count']:10,}  {format_size(stats['size']):>15s}  {len(stats['unique_files']):>12,}")
    print()
    
    # Top files by READ count
    print("TOP 10 MOST READ FILES")
    print("-" * 80)
    read_counts = {filename: count for (filename, action), count in file_action_counts.items() 
                   if action == 'READ'}
    
    if read_counts:
        for filename, count in sorted(read_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"{count:10,}  {filename}")
    else:
        print("  No READ operations found")
    print()
    
    # Per-file action statistics
    print("PER-FILE ACTION STATISTICS")
    print("-" * 80)
    
    # Count files by action and occurrence count
    action_file_stats = defaultdict(lambda: defaultdict(int))
    for (filename, action), count in file_action_counts.items():
        action_file_stats[action][count] += 1
    
    for action in sorted(action_file_stats.keys()):
        print(f"\n{action}:")
        occurrence_counts = action_file_stats[action]
        total_files = sum(occurrence_counts.values())
        print(f"  Total unique files: {total_files:,}")
        
        # Show distribution of operation counts
        print(f"  Operation count distribution:")
        for op_count in sorted(occurrence_counts.keys(), reverse=True)[:10]:
            file_count = occurrence_counts[op_count]
            print(f"    {op_count:4,} operations: {file_count:5,} files")
        
        if len(occurrence_counts) > 10:
            remaining = sum(occurrence_counts[k] for k in sorted(occurrence_counts.keys())[:-10])
            if remaining > 0:
                print(f"    ... {remaining:,} more files with fewer operations")
    print()
    
    # Time bucket details
    print("TIME BUCKET BREAKDOWN")
    print("-" * 80)
    print(f"{'Time Bucket':<20s} {'Component':<15s} {'Action':<10s} {'Ops':>10s} {'Size':>15s} {'Files':>8s}")
    print("-" * 80)
    
    for bucket in sorted(aggregates.keys()):
        bucket_str = bucket.strftime('%Y-%m-%d %H:%M')
        for (component, action), stats in sorted(aggregates[bucket].items()):
            print(f"{bucket_str:<20s} {component:<15s} {action:<10s} "
                  f"{stats['count']:>10,} {format_size(stats['size']):>15s} {len(stats['files']):>8,}")
    
    print()
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze HPCC file access logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Analyze READ operations in 1-hour buckets
  cat file_access.csv | %(prog)s --filter-action READ --granularity 1
  
  # Analyze Thor CREATE/DELETE for specific files
  %(prog)s data.csv --filter-component Thor --filter-action CREATED,DELETED \\
    --filter-lfn "thor_data400::key::*" --granularity 1
  
  # Generate text report
  cat file_access.csv | %(prog)s --format text --granularity 1
  
  # Time-filtered analysis
  %(prog)s data.csv --start-time "2025-11-20 12:00" --end-time "2025-11-20 18:00" \\
    --granularity 1 --format csv
        '''
    )
    
    parser.add_argument(
        'input',
        nargs='?',
        help='Input CSV file from get_file_access_logs.py (or read from stdin)'
    )
    
    parser.add_argument(
        '--start-time',
        help='Start time filter (YYYY-MM-DD or YYYY-MM-DD HH:MM)'
    )
    
    parser.add_argument(
        '--end-time',
        help='End time filter (YYYY-MM-DD or YYYY-MM-DD HH:MM)'
    )
    
    parser.add_argument(
        '--filter-lfn',
        help='Filter by logical filename patterns (comma-separated, wildcards supported)'
    )
    
    parser.add_argument(
        '--filter-component',
        help='Filter by component (comma-separated, e.g., Thor,FileServices)'
    )
    
    parser.add_argument(
        '--filter-action',
        help='Filter by action (comma-separated, e.g., READ,CREATED,DELETED)'
    )
    
    parser.add_argument(
        '--granularity',
        type=int,
        default=1,
        help='Time bucket granularity in hours (default: 1)'
    )
    
    parser.add_argument(
        '--format',
        choices=['csv', 'text'],
        default='csv',
        help='Output format (default: csv)'
    )
    
    parser.add_argument(
        '--costs',
        help='CSV file from get_storage_pricing.py for cost calculations'
    )
    
    parser.add_argument(
        '--opsize',
        type=int,
        default=4096,
        help='Operation size in KB for cost calculations (default: 4096 = 4MB)'
    )
    
    args = parser.parse_args()
    
    # Parse filters
    component_filters = args.filter_component.split(',') if args.filter_component else []
    action_filters = args.filter_action.split(',') if args.filter_action else []
    lfn_filters = args.filter_lfn.split(',') if args.filter_lfn else []
    
    # Parse time filters
    start_time = parse_datetime(args.start_time) if args.start_time else None
    end_time = parse_datetime(args.end_time) if args.end_time else None
    
    # Load pricing data if provided
    pricing_data = None
    if args.costs:
        try:
            pricing_data = load_pricing_data(args.costs)
            print(f"Loaded pricing data: Read=${pricing_data[0]}/10k ops, Write=${pricing_data[1]}/10k ops", 
                  file=sys.stderr)
        except Exception as e:
            print(f"Error loading pricing data: {e}", file=sys.stderr)
            return 1
    
    # Read records
    records = read_records(args.input, start_time, end_time)
    
    if not records:
        print("No records found matching filters.", file=sys.stderr)
        return 1
    
    # Aggregate
    aggregates, file_action_counts, file_action_sizes = aggregate_records(
        records,
        args.granularity,
        component_filters,
        action_filters,
        lfn_filters
    )
    
    # Output
    if args.format == 'csv':
        output_csv(aggregates, args.granularity, file_action_counts, pricing_data, args.opsize, args)
    else:
        output_text(aggregates, args.granularity, records, file_action_counts, pricing_data, args.opsize, args)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
