#!/usr/bin/env python3
"""
Analyze Azure storage usage CSV data and display formatted breakdown.

This script reads storage usage CSV data (from get_storage_usage.py or compatible source)
and displays formatted usage breakdowns with various focus options.

Usage:
    python3 analyze_storage_usage.py <usage.csv> [options]

Examples:
    # Analyze overall usage breakdown
    python3 analyze_storage_usage.py usage.csv

    # Show daily breakdown instead of aggregated
    python3 analyze_storage_usage.py usage.csv --daily

    # CSV output for further processing
    python3 analyze_storage_usage.py usage.csv --format csv

    # Pipe from collector
    ./get_storage_usage.py --subscription <sub> --start-time "2025-11-01" --end-time "2025-11-07" | python3 analyze_storage_usage.py -

Input CSV Format:
    Timestamp,StorageAccount,ResourceGroup,UsedCapacityGB,BlobCapacityGB,FileCapacityGB,TableCapacityGB,QueueCapacityGB
    2025-11-01T00:00:00Z,myaccount,my-rg,1234.56,1200.00,34.56,0.00,0.00
"""

import sys
import csv
import argparse
from collections import defaultdict
from datetime import datetime


def read_usage_csv(filename):
    """Read storage usage data from CSV file or stdin.

    Args:
        filename: Path to CSV file or '-' for stdin

    Returns:
        List of dicts with parsed usage data
    """
    usage_data = []

    file_handle = sys.stdin if filename == '-' else open(filename, 'r')

    try:
        # Filter out comment lines starting with #
        filtered_lines = (line for line in file_handle if not line.strip().startswith('#'))
        reader = csv.DictReader(filtered_lines)
        for row in reader:
            timestamp = row['Timestamp']
            account = row['StorageAccount']
            rg = row['ResourceGroup']

            # Parse capacity values (may be empty strings)
            used = float(row['UsedCapacityGB']) if row['UsedCapacityGB'] else 0.0
            blob = float(row['BlobCapacityGB']) if row['BlobCapacityGB'] else 0.0
            file = float(row['FileCapacityGB']) if row['FileCapacityGB'] else 0.0
            table = float(row['TableCapacityGB']) if row['TableCapacityGB'] else 0.0
            queue = float(row['QueueCapacityGB']) if row['QueueCapacityGB'] else 0.0

            usage_data.append({
                'timestamp': timestamp,
                'account': account,
                'resource_group': rg,
                'used': used,
                'blob': blob,
                'file': file,
                'table': table,
                'queue': queue
            })
    finally:
        if filename != '-':
            file_handle.close()

    return usage_data


def group_by_date(usage_data):
    """Group usage data by date (stripping time).

    Returns:
        dict: {date_str: {account: {metric: value}}}
    """
    by_date = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for entry in usage_data:
        # Extract date from timestamp (YYYY-MM-DD)
        timestamp = entry['timestamp']
        if 'T' in timestamp:
            date_str = timestamp.split('T')[0]
        else:
            date_str = timestamp[:10]

        account = entry['account']

        # Collect all values for averaging
        by_date[date_str][account]['used'].append(entry['used'])
        by_date[date_str][account]['blob'].append(entry['blob'])
        by_date[date_str][account]['file'].append(entry['file'])
        by_date[date_str][account]['table'].append(entry['table'])
        by_date[date_str][account]['queue'].append(entry['queue'])

    # Average the values for each day
    daily_avg = {}
    for date_str, accounts in by_date.items():
        daily_avg[date_str] = {}
        for account, metrics in accounts.items():
            daily_avg[date_str][account] = {
                'used': sum(metrics['used']) / len(metrics['used']) if metrics['used'] else 0,
                'blob': sum(metrics['blob']) / len(metrics['blob']) if metrics['blob'] else 0,
                'file': sum(metrics['file']) / len(metrics['file']) if metrics['file'] else 0,
                'table': sum(metrics['table']) / len(metrics['table']) if metrics['table'] else 0,
                'queue': sum(metrics['queue']) / len(metrics['queue']) if metrics['queue'] else 0,
            }

    return daily_avg


def print_summary(by_date):
    """Print overall summary across all dates."""
    # Aggregate across all dates
    account_totals = defaultdict(lambda: {'used': 0, 'blob': 0, 'file': 0, 'table': 0, 'queue': 0, 'count': 0})

    for date_str, accounts in by_date.items():
        for account, metrics in accounts.items():
            account_totals[account]['used'] += metrics['used']
            account_totals[account]['blob'] += metrics['blob']
            account_totals[account]['file'] += metrics['file']
            account_totals[account]['table'] += metrics['table']
            account_totals[account]['queue'] += metrics['queue']
            account_totals[account]['count'] += 1

    # Calculate averages
    print("=== STORAGE USAGE SUMMARY ===\n")
    print(f"{'Storage Account':<40} {'Avg Used (GB)':<15} {'Avg Blob (GB)':<15} {'Avg File (GB)':<15} {'Avg Table (GB)':<15} {'Avg Queue (GB)':<15}")
    print("=" * 115)

    grand_total_used = 0
    grand_total_blob = 0
    grand_total_file = 0
    grand_total_table = 0
    grand_total_queue = 0

    for account in sorted(account_totals.keys()):
        totals = account_totals[account]
        count = totals['count']

        avg_used = totals['used'] / count
        avg_blob = totals['blob'] / count
        avg_file = totals['file'] / count
        avg_table = totals['table'] / count
        avg_queue = totals['queue'] / count

        grand_total_used += avg_used
        grand_total_blob += avg_blob
        grand_total_file += avg_file
        grand_total_table += avg_table
        grand_total_queue += avg_queue

        print(f"{account:<40} {avg_used:>14.2f} {avg_blob:>14.2f} {avg_file:>14.2f} {avg_table:>14.2f} {avg_queue:>14.2f}")

    print("=" * 115)
    print(f"{'TOTAL':<40} {grand_total_used:>14.2f} {grand_total_blob:>14.2f} {grand_total_file:>14.2f} {grand_total_table:>14.2f} {grand_total_queue:>14.2f}")

    # Show breakdown by type
    print("\n=== USAGE BY STORAGE TYPE ===\n")
    total = grand_total_used
    if total > 0:
        blob_pct = (grand_total_blob / total * 100) if grand_total_blob > 0 else 0
        file_pct = (grand_total_file / total * 100) if grand_total_file > 0 else 0
        table_pct = (grand_total_table / total * 100) if grand_total_table > 0 else 0
        queue_pct = (grand_total_queue / total * 100) if grand_total_queue > 0 else 0

        print(f"Blob Storage:   {grand_total_blob:>10.2f} GB ({blob_pct:>5.1f}%)")
        print(f"File Storage:   {grand_total_file:>10.2f} GB ({file_pct:>5.1f}%)")
        print(f"Table Storage:  {grand_total_table:>10.2f} GB ({table_pct:>5.1f}%)")
        print(f"Queue Storage:  {grand_total_queue:>10.2f} GB ({queue_pct:>5.1f}%)")
        print(f"{'Total:':<16} {total:>10.2f} GB")


def print_daily_breakdown(by_date):
    """Print daily breakdown for each date."""
    for date in sorted(by_date.keys()):
        print(f"\n=== {date} ===\n")
        accounts = by_date[date]

        print(f"{'Storage Account':<40} {'Used (GB)':<12} {'Blob (GB)':<12} {'File (GB)':<12} {'Table (GB)':<12} {'Queue (GB)':<12}")
        print("=" * 100)

        total_used = 0
        total_blob = 0
        total_file = 0
        total_table = 0
        total_queue = 0

        for account in sorted(accounts.keys()):
            metrics = accounts[account]
            print(f"{account:<40} {metrics['used']:>11.2f} {metrics['blob']:>11.2f} {metrics['file']:>11.2f} {metrics['table']:>11.2f} {metrics['queue']:>11.2f}")

            total_used += metrics['used']
            total_blob += metrics['blob']
            total_file += metrics['file']
            total_table += metrics['table']
            total_queue += metrics['queue']

        print("=" * 100)
        print(f"{'Total':<40} {total_used:>11.2f} {total_blob:>11.2f} {total_file:>11.2f} {total_table:>11.2f} {total_queue:>11.2f}")


def output_csv_summary(by_date, by_account=False, metadata=None):
    """Output daily aggregated summary in CSV format.

    Args:
        by_date: Grouped usage data
        by_account: If True, show per-account breakdown, otherwise totals only
        metadata: Optional dict with command metadata for comment headers
    """
    # Output metadata comments if provided
    if metadata:
        print("# Generated by: analyze_storage_usage.py")
        print(f"# Date generated: {metadata.get('date', '')}")
        print(f"# Command: {metadata.get('command', '')}")
        if metadata.get('input_file'):
            print(f"# Input file: {metadata.get('input_file')}")
        if metadata.get('date_range'):
            print(f"# Date range: {metadata.get('date_range')}")
        if by_account:
            print("# Format: Per-account daily breakdown")
        else:
            print("# Format: Daily totals")
        print("#")
    
    if by_account:
        # Print header with account breakdown
        print("Date,StorageAccount,UsedGB,BlobGB,FileGB,TableGB,QueueGB")

        for date in sorted(by_date.keys()):
            accounts = by_date[date]

            for account in sorted(accounts.keys()):
                metrics = accounts[account]
                print(f"{date},{account},{metrics['used']:.2f},{metrics['blob']:.2f},{metrics['file']:.2f},{metrics['table']:.2f},{metrics['queue']:.2f}")
    else:
        # Print header for totals
        print("Date,TotalUsedGB,BlobGB,FileGB,TableGB,QueueGB")

        for date in sorted(by_date.keys()):
            accounts = by_date[date]

            total_used = 0
            total_blob = 0
            total_file = 0
            total_table = 0
            total_queue = 0

            for account, metrics in accounts.items():
                total_used += metrics['used']
                total_blob += metrics['blob']
                total_file += metrics['file']
                total_table += metrics['table']
                total_queue += metrics['queue']

            print(f"{date},{total_used:.2f},{total_blob:.2f},{total_file:.2f},{total_table:.2f},{total_queue:.2f}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Azure storage usage data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Overall summary
  %(prog)s usage.csv

  # Daily breakdown
  %(prog)s usage.csv --daily

  # CSV output for further processing (daily totals)
  %(prog)s usage.csv --format csv

  # CSV output with per-account breakdown
  %(prog)s usage.csv --format csv --by-account

  # Pipe from collector
  ./get_storage_usage.py --subscription <sub> --start-time "2025-11-01" --end-time "2025-11-07" | %(prog)s -
        """
    )

    parser.add_argument('input_file', help='Input CSV file (or - for stdin)')
    parser.add_argument('--daily', action='store_true',
                       help='Show each day separately instead of aggregated')
    parser.add_argument('--by-account', action='store_true',
                       help='Break down by storage account (CSV format only)')
    parser.add_argument('--format', choices=['text', 'csv'],
                       default='text',
                       help='Output format: text (human-readable, default) or csv (machine-readable)')
    parser.add_argument('--start-time',
                       help='Start time filter (YYYY-MM-DD or YYYY-MM-DD HH:MM)')
    parser.add_argument('--end-time',
                       help='End time filter (YYYY-MM-DD or YYYY-MM-DD HH:MM)')

    args = parser.parse_args()

    # Read usage data
    try:
        usage_data = read_usage_csv(args.input_file)
    except BrokenPipeError:
        # Handle pipe to head/grep gracefully
        sys.exit(0)
    except KeyError as e:
        print(f"Error: Missing expected column {e} in CSV. Expected format:", file=sys.stderr)
        print("Timestamp,StorageAccount,ResourceGroup,UsedCapacityGB,BlobCapacityGB,FileCapacityGB,TableCapacityGB,QueueCapacityGB", file=sys.stderr)
        sys.exit(1)

    if not usage_data:
        print("No usage data found", file=sys.stderr)
        sys.exit(1)

    # Filter by time range if specified
    if args.start_time or args.end_time:
        # Parse filter times (support both date and datetime)
        start_filter = None
        end_filter = None

        if args.start_time:
            try:
                start_filter = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M')
            except ValueError:
                # Try date only
                start_filter = datetime.strptime(args.start_time + ' 00:00', '%Y-%m-%d %H:%M')

        if args.end_time:
            try:
                end_filter = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M')
            except ValueError:
                # Try date only - use end of day
                end_filter = datetime.strptime(args.end_time + ' 23:59', '%Y-%m-%d %H:%M')

        filtered_data = []
        for entry in usage_data:
            timestamp_str = entry['timestamp']
            # Parse timestamp from CSV (ISO format with Z or no timezone)
            timestamp_str = timestamp_str.replace('Z', '')
            try:
                entry_time = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                # Try without seconds
                try:
                    entry_time = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M')
                except ValueError:
                    # Skip entries with unparseable timestamps
                    continue

            if start_filter and entry_time < start_filter:
                continue
            if end_filter and entry_time > end_filter:
                continue

            filtered_data.append(entry)

        usage_data = filtered_data

    if not usage_data:
        print("No usage data in specified date range", file=sys.stderr)
        sys.exit(1)

    # Group by date
    by_date = group_by_date(usage_data)

    # CSV format output
    if args.format == 'csv':
        # Build metadata for comment header
        from datetime import datetime as dt
        metadata = {
            'date': dt.now().strftime('%Y-%m-%d %H:%M:%S'),
            'command': 'analyze_storage_usage.py ' + ' '.join(sys.argv[1:]),
            'input_file': args.input_file if args.input_file != '-' else 'stdin'
        }
        
        # Determine date range from data
        if by_date:
            dates = sorted(by_date.keys())
            if len(dates) > 0:
                metadata['date_range'] = f"{dates[0]} to {dates[-1]}"
        
        output_csv_summary(by_date, by_account=args.by_account, metadata=metadata)
        return

    # Text format output (default)
    if args.daily:
        # Show each day separately
        print_daily_breakdown(by_date)
    else:
        # Aggregated summary
        print_summary(by_date)


if __name__ == '__main__':
    main()
