#!/usr/bin/env python3
"""
Get storage usage metrics from Azure Monitor over time.

This script queries Azure storage accounts to get usage data over a time period,
which can be compared against storage costs. Supports hourly granularity.

Usage:
    get_storage_usage.py --subscription <name_or_id> --start-time <datetime> [options]

Examples:
    # Current snapshot (last 24 hours)
    get_storage_usage.py --subscription my-subscription

    # Specific time range with hourly data
    get_storage_usage.py --subscription my-subscription --start-time "2025-11-01 00:00" --end-time "2025-11-08 00:00"

    # Specific resource group
    get_storage_usage.py --subscription my-subscription --resource-group my-rg --start-time "2025-11-01 00:00"

Output:
    CSV format: Timestamp,StorageAccount,ResourceGroup,UsedCapacityGB,BlobCapacityGB,FileCapacityGB,TableCapacityGB,QueueCapacityGB
"""

import sys
import json
import subprocess
import argparse
from datetime import datetime, timedelta


def resolve_subscription_id(name_or_id):
    """Convert subscription name to ID, or validate ID."""
    try:
        result = subprocess.run(
            ['az', 'account', 'list', '--query', f"[?name=='{name_or_id}'].id", '-o', 'tsv'],
            capture_output=True, text=True, check=True
        )
        sub_id = result.stdout.strip()

        if sub_id:
            return sub_id

        # Try as direct ID
        result = subprocess.run(
            ['az', 'account', 'list', '--query', f"[?id=='{name_or_id}'].id", '-o', 'tsv'],
            capture_output=True, text=True, check=True
        )
        sub_id = result.stdout.strip()

        if sub_id:
            return sub_id

        # List available subscriptions
        print(f"Error: Subscription '{name_or_id}' not found", file=sys.stderr)
        print("\nAvailable subscriptions:", file=sys.stderr)
        result = subprocess.run(
            ['az', 'account', 'list', '--query', '[].{Name:name, ID:id}', '-o', 'table'],
            capture_output=True, text=True, check=True
        )
        print(result.stdout, file=sys.stderr)
        sys.exit(1)

    except subprocess.CalledProcessError as e:
        print(f"Error resolving subscription: {e}", file=sys.stderr)
        sys.exit(1)


def get_storage_accounts(subscription_id, resource_group=None):
    """Get list of storage accounts in subscription or resource group."""
    try:
        if resource_group:
            cmd = [
                'az', 'storage', 'account', 'list',
                '--subscription', subscription_id,
                '--resource-group', resource_group,
                '-o', 'json'
            ]
        else:
            cmd = [
                'az', 'storage', 'account', 'list',
                '--subscription', subscription_id,
                '-o', 'json'
            ]

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        accounts = json.loads(result.stdout)
        return accounts

    except subprocess.CalledProcessError as e:
        print(f"Error listing storage accounts: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def get_storage_metrics(subscription_id, resource_id, metric_name, start_time, end_time):
    """Get storage metrics from Azure Monitor.

    Args:
        subscription_id: Azure subscription ID
        resource_id: Full resource ID of storage account
        metric_name: Metric to query (e.g., 'UsedCapacity', 'BlobCapacacity')
        start_time: Start datetime
        end_time: End datetime

    Returns:
        List of (timestamp, value_in_bytes) tuples, or empty list if not available
    """
    try:
        cmd = [
            'az', 'monitor', 'metrics', 'list',
            '--resource', resource_id,
            '--subscription', subscription_id,
            '--metric', metric_name,
            '--start-time', start_time.isoformat(),
            '--end-time', end_time.isoformat(),
            '--interval', 'PT1H',
            '--aggregation', 'Average',
            '-o', 'json'
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)

        # Extract time series data
        metrics = []
        if data and 'value' in data and len(data['value']) > 0:
            timeseries = data['value'][0].get('timeseries', [])
            if timeseries and len(timeseries) > 0:
                datapoints = timeseries[0].get('data', [])
                for point in datapoints:
                    if point.get('average') is not None:
                        timestamp = point.get('timeStamp')
                        value = point['average']
                        metrics.append((timestamp, value))

        return metrics

    except subprocess.CalledProcessError:
        # Metric not available for this account type
        return []


def bytes_to_gb(bytes_val):
    """Convert bytes to GB."""
    if bytes_val is None:
        return None
    return bytes_val / (1024 ** 3)


def print_table(data):
    """Print data in formatted table."""
    if not data:
        return

    # Print header
    print(f"{'Storage Account':<30} {'Resource Group':<25} {'Used (GB)':>12} {'Blob (GB)':>12} {'File (GB)':>12} {'Table (GB)':>12} {'Queue (GB)':>12}")
    print("=" * 130)

    # Print rows
    for row in data:
        print(f"{row['account']:<30} {row['rg']:<25} {row['used']:>12} {row['blob']:>12} {row['file']:>12} {row['table']:>12} {row['queue']:>12}")


def print_csv(data):
    """Print data in CSV format."""
    print("StorageAccount,ResourceGroup,UsedCapacityGB,BlobCapacityGB,FileCapacityGB,TableCapacityGB,QueueCapacityGB")
    for row in data:
        print(f"{row['account']},{row['rg']},{row['used']},{row['blob']},{row['file']},{row['table']},{row['queue']}")


def main():
    parser = argparse.ArgumentParser(
        description='Get Azure storage usage metrics over time',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Current snapshot (last 24 hours)
  %(prog)s --subscription my-subscription

  # Specific date range (assumes 00:00 to 23:59)
  %(prog)s --subscription my-subscription --start-time "2025-11-01" --end-time "2025-11-08"

  # Specific time range with hourly data
  %(prog)s --subscription my-subscription --start-time "2025-11-01 00:00" --end-time "2025-11-08 00:00"

  # Specific resource group
  %(prog)s --subscription my-subscription --resource-group my-rg --start-time "2025-11-01"

  # Single storage account (faster)
  %(prog)s --subscription my-subscription --filter mystorageaccount --start-time "2025-11-01"
        """
    )

    parser.add_argument('--subscription', required=True,
                       help='Subscription name or ID')
    parser.add_argument('--start-time',
                       help='Start time (YYYY-MM-DD or YYYY-MM-DD HH:MM), defaults to 24 hours ago')
    parser.add_argument('--end-time',
                       help='End time (YYYY-MM-DD or YYYY-MM-DD HH:MM), defaults to now')
    parser.add_argument('--resource-group',
                       help='Filter to specific resource group (optional)')
    parser.add_argument('--account',
                       help='Filter to specific storage account name (optional)')
    parser.add_argument('--filter',
                       dest='account',
                       help='Filter to specific storage account name (alias for --account)')

    args = parser.parse_args()

    # Parse time range - support both date and date+time formats
    if args.end_time:
        # Try date+time format first, then date-only
        try:
            end_time = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M')
        except ValueError:
            # Date only - assume end of day (23:59)
            end_time = datetime.strptime(args.end_time + ' 23:59', '%Y-%m-%d %H:%M')
    else:
        end_time = datetime.utcnow()

    if args.start_time:
        try:
            start_time = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M')
        except ValueError:
            # Date only - assume start of day (00:00)
            start_time = datetime.strptime(args.start_time + ' 00:00', '%Y-%m-%d %H:%M')
    else:
        start_time = end_time - timedelta(hours=24)

    # Resolve subscription
    subscription_id = resolve_subscription_id(args.subscription)

    # Get storage accounts
    accounts = get_storage_accounts(subscription_id, args.resource_group)

    # Filter to specific account if requested
    if args.account:
        accounts = [acc for acc in accounts if acc['name'] == args.account]
        if not accounts:
            print(f"Error: Storage account '{args.account}' not found", file=sys.stderr)
            sys.exit(1)

    # Print metadata as comments
    from datetime import datetime as dt
    print("# Generated by: get_storage_usage.py")
    print(f"# Date generated: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Command: {' '.join(sys.argv)}")
    print(f"# Subscription: {subscription_id}")
    print(f"# Time range: {start_time.isoformat()} to {end_time.isoformat()}")
    if args.resource_group:
        print(f"# Resource group: {args.resource_group}")
    if args.account:
        print(f"# Storage account: {args.account}")
    print("#")
    
    # Print CSV header
    print("Timestamp,StorageAccount,ResourceGroup,UsedCapacityGB,BlobCapacityGB,FileCapacityGB,TableCapacityGB,QueueCapacityGB")

    # Query metrics for each account
    for account in accounts:
        name = account['name']
        rg = account['resourceGroup']
        resource_id = account['id']

        # Get various capacity metrics (returns list of (timestamp, value) tuples)
        used_metrics = get_storage_metrics(subscription_id, resource_id, 'UsedCapacity', start_time, end_time)
        blob_metrics = get_storage_metrics(subscription_id, resource_id, 'BlobCapacity', start_time, end_time)
        file_metrics = get_storage_metrics(subscription_id, resource_id, 'FileCapacity', start_time, end_time)
        table_metrics = get_storage_metrics(subscription_id, resource_id, 'TableCapacity', start_time, end_time)
        queue_metrics = get_storage_metrics(subscription_id, resource_id, 'QueueCapacity', start_time, end_time)

        # Create a dict indexed by timestamp for each metric type
        used_dict = {ts: val for ts, val in used_metrics}
        blob_dict = {ts: val for ts, val in blob_metrics}
        file_dict = {ts: val for ts, val in file_metrics}
        table_dict = {ts: val for ts, val in table_metrics}
        queue_dict = {ts: val for ts, val in queue_metrics}

        # Get all unique timestamps
        all_timestamps = set(used_dict.keys()) | set(blob_dict.keys()) | set(file_dict.keys()) | set(table_dict.keys()) | set(queue_dict.keys())

        # Output one row per timestamp
        for timestamp in sorted(all_timestamps):
            used_gb = bytes_to_gb(used_dict.get(timestamp))
            blob_gb = bytes_to_gb(blob_dict.get(timestamp))
            file_gb = bytes_to_gb(file_dict.get(timestamp))
            table_gb = bytes_to_gb(table_dict.get(timestamp))
            queue_gb = bytes_to_gb(queue_dict.get(timestamp))

            # Format values
            used_str = f"{used_gb:.2f}" if used_gb is not None else ""
            blob_str = f"{blob_gb:.2f}" if blob_gb is not None else ""
            file_str = f"{file_gb:.2f}" if file_gb is not None else ""
            table_str = f"{table_gb:.2f}" if table_gb is not None else ""
            queue_str = f"{queue_gb:.2f}" if queue_gb is not None else ""

            print(f"{timestamp},{name},{rg},{used_str},{blob_str},{file_str},{table_str},{queue_str}")


if __name__ == '__main__':
    main()
