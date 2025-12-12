#!/usr/bin/env python3
"""
Fetch Azure costs from Cost Management API and output as CSV.

This script queries the Azure Cost Management API and outputs cost data
in CSV format for further analysis.

Usage:
    python3 get_costs.py <start_date> [end_date] [options]
    
Examples:
    # Get costs for a single day
    python3 get_costs.py 2025-10-28
    
    # Get costs for a date range
    python3 get_costs.py 2025-10-28 2025-11-04
    
    # Get detailed storage costs by meter
    python3 get_costs.py 2025-10-28 2025-11-04 --group-by meter --filter storage
    
    # Get compute costs by VM SKU
    python3 get_costs.py 2025-10-28 --group-by meter --filter vm
    
Date format: YYYY-MM-DD

Output Format (CSV):
    Cost,Date,Category,Currency
    123.45,20251028,Virtual Machines,USD
    67.89,20251028,Storage,USD
"""

import sys
import json
import subprocess
import argparse
import time
from datetime import datetime, timedelta


def resolve_subscription_id(subscription_name_or_id):
    """Resolve subscription name to ID, or validate if already an ID.
    
    Args:
        subscription_name_or_id: Subscription name or GUID
        
    Returns:
        Subscription ID (GUID)
        
    Raises:
        SystemExit if subscription cannot be resolved
    """
    # Check if it's already a GUID (rough check: contains hyphens and right length)
    if '-' in subscription_name_or_id and len(subscription_name_or_id) == 36:
        # Likely already a GUID, return as-is
        return subscription_name_or_id
    
    # Try to resolve as subscription name
    try:
        cmd = [
            'az', 'account', 'list',
            '--query', f"[?name=='{subscription_name_or_id}'].id",
            '-o', 'tsv'
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        subscription_id = result.stdout.strip()
        
        if not subscription_id:
            print(f"Error: Subscription '{subscription_name_or_id}' not found", file=sys.stderr)
            print("Available subscriptions:", file=sys.stderr)
            # List available subscriptions
            list_cmd = ['az', 'account', 'list', '--query', '[].name', '-o', 'tsv']
            list_result = subprocess.run(list_cmd, capture_output=True, text=True, check=False)
            if list_result.returncode == 0:
                for sub in list_result.stdout.strip().split('\n'):
                    print(f"  - {sub}", file=sys.stderr)
            sys.exit(1)
        
        return subscription_id
        
    except subprocess.CalledProcessError as e:
        print(f"Error resolving subscription: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def get_costs(subscription_id, start_date, end_date, grouping_dimension='MeterCategory', retry_delay=2):
    """Query Azure Cost Management API for costs in the date range.
    
    Args:
        subscription_id: Azure subscription ID
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        grouping_dimension: Azure dimension to group by (MeterCategory, Meter, MeterSubcategory, etc.)
        retry_delay: Initial delay for retry on rate limiting
        
    Returns:
        List of rows: [[cost, date, category, currency], ...]
    """
    
    # Ensure dates are in ISO format with time
    if 'T' not in start_date:
        start_date = f"{start_date}T00:00:00Z"
    if 'T' not in end_date:
        end_date = f"{end_date}T00:00:00Z"
    
    request_body = {
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {
            "from": start_date,
            "to": end_date
        },
        "dataset": {
            "granularity": "Daily",
            "aggregation": {
                "totalCost": {
                    "name": "Cost",
                    "function": "Sum"
                }
            },
            "grouping": [
                {
                    "type": "Dimension",
                    "name": grouping_dimension
                }
            ]
        }
    }
    
    # Call Azure Cost Management API
    uri = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-03-01"
    
    cmd = [
        'az', 'rest',
        '--method', 'POST',
        '--uri', uri,
        '--body', json.dumps(request_body),
        '--query', 'properties.rows',
        '--output', 'json'
    ]
    
    # Retry logic for rate limiting (429 errors)
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            # Always output the actual error
            print(f"Error on attempt {attempt + 1}/{max_retries}: {e.stderr.strip()}", file=sys.stderr)
            
            # Check if it's a 429 rate limit error
            if '429' in e.stderr or 'Too many requests' in e.stderr or 'Too Many Requests' in e.stderr:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    print(f"Rate limit hit. Waiting {wait_time} seconds before retry {attempt + 2}/{max_retries}...", file=sys.stderr)
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"Error calling Azure API: Maximum retries ({max_retries}) reached due to rate limiting", file=sys.stderr)
                    sys.exit(1)
            else:
                # Non-rate-limit error, fail immediately
                print(f"Error calling Azure API (non-rate-limit error)", file=sys.stderr)
                sys.exit(1)
    
    # Should never reach here, but just in case
    print("Error: Unexpected exit from retry loop", file=sys.stderr)
    sys.exit(1)


def filter_costs(data, filter_type):
    """Filter cost data by type.
    
    Args:
        data: Cost data rows [[cost, date, category, currency], ...]
        filter_type: Filter type ('storage', 'vm', 'compute', or None)
        
    Returns:
        Filtered cost data
    """
    if not filter_type:
        return data
    
    filtered = []
    for row in data:
        cost, date, category, currency = row
        category_lower = category.lower()
        
        if filter_type == 'storage':
            # Storage-related keywords
            if any(keyword in category_lower for keyword in 
                   ['storage', 'disk', 'blob', 'file', 'data', 'hot', 'cool', 
                    'archive', 'zrs', 'lrs', 'grs', 'premium']):
                filtered.append(row)
                
        elif filter_type in ['vm', 'compute']:
            # VM/Compute-related keywords
            if any(keyword in category_lower for keyword in 
                   ['d48', 'd64', 'd16', 'd32', 'l32', 'l64', 'compute', 
                    'virtual machine', 'vm']):
                filtered.append(row)
    
    return filtered


def output_csv(data, simplify_resource_id=False, metadata=None):
    """Output cost data as CSV to stdout.
    
    Args:
        data: Cost data rows
        simplify_resource_id: If True, extract just the resource name from full ResourceId paths
        metadata: Optional dict with generation metadata (command, subscription, dates, etc.)
    """
    # Output metadata as comments if provided
    if metadata:
        print("# Generated by: get_costs.py")
        print(f"# Date generated: {metadata.get('date_generated', '')}")
        print(f"# Command: {metadata.get('command', '')}")
        print(f"# Subscription: {metadata.get('subscription', '')}")
        print(f"# Date range: {metadata.get('start_date', '')} to {metadata.get('end_date', '')}")
        print(f"# Grouping: {metadata.get('grouping', '')}")
        if metadata.get('filter'):
            print(f"# Filter: {metadata.get('filter', '')}")
        if metadata.get('resource_name'):
            print(f"# Resource name: {metadata.get('resource_name', '')}")
        print("#")
    
    print("Cost,Date,Category,Currency")
    for row in data:
        cost, date, category, currency = row
        
        # If simplify_resource_id is True and category looks like a ResourceId path, extract just the name
        if simplify_resource_id and category.startswith('/'):
            # Extract the last part of the path (resource name)
            category = category.split('/')[-1]
        
        # Escape category if it contains commas
        if ',' in category:
            category = f'"{category}"'
        print(f"{cost},{date},{category},{currency}")


def main():
    parser = argparse.ArgumentParser(
        description='Fetch Azure costs and output as CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get all costs by category
  %(prog)s --subscription <sub-id> --start-date 2025-10-28
  
  # Get costs for date range
  %(prog)s --subscription <sub-id> --start-date 2025-10-28 --end-date 2025-11-04
  
  # Get detailed storage costs
  %(prog)s --subscription <sub-id> --start-date 2025-10-28 --grouping meter --filter storage
  
  # Get compute costs by VM SKU
  %(prog)s --subscription <sub-id> --start-date 2025-10-28 --grouping meter --filter vm
  
  # Get costs for a specific storage account
  %(prog)s --subscription <sub-id> --start-date 2025-10-28 --grouping resource --resource-name mystorageaccount
  
  # Save to file
  %(prog)s --subscription <sub-id> --start-date 2025-10-28 --end-date 2025-11-04 > costs.csv
  
See also: ./analyze_costs.py for analyzing the output
        """
    )
    
    parser.add_argument('--subscription', required=True, help='Azure subscription ID or name')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD), defaults to tomorrow')
    parser.add_argument('--grouping', choices=['category', 'meter', 'subcategory', 'resource'], 
                       default='category',
                       help='Grouping dimension (default: category)')
    parser.add_argument('--filter', choices=['storage', 'vm', 'compute'],
                       help='Filter to specific cost type')
    parser.add_argument('--resource-name',
                       help='Filter to specific resource name (e.g., storage account name). Requires --grouping resource')
    parser.add_argument('--retry-delay', type=int, default=2, metavar='SECONDS',
                       help='Initial retry delay for rate limiting (default: 2)')
    
    args = parser.parse_args()
    
    start_date = args.start_date
    
    # If end_date not provided, use tomorrow (to include today's partial costs)
    if args.end_date:
        end_date = args.end_date
    else:
        tomorrow = datetime.now() + timedelta(days=1)
        end_date = tomorrow.strftime('%Y-%m-%d')
    
    # Resolve subscription name to ID
    subscription_id = resolve_subscription_id(args.subscription)
    
    # Validate resource-name requires grouping by resource
    if args.resource_name and args.grouping != 'resource':
        print("Error: --resource-name requires --grouping resource", file=sys.stderr)
        sys.exit(1)
    
    # Map grouping option to Azure dimension
    dimension_map = {
        'category': 'MeterCategory',
        'meter': 'Meter',
        'subcategory': 'MeterSubcategory',
        'resource': 'ResourceId'
    }
    dimension = dimension_map[args.grouping]
    
    print(f"Fetching costs from {start_date} to {end_date}", file=sys.stderr)
    print(f"Subscription: {subscription_id}", file=sys.stderr)
    print(f"Grouping by: {dimension}", file=sys.stderr)
    if args.filter:
        print(f"Filter: {args.filter}", file=sys.stderr)
    print(file=sys.stderr)
    
    # Fetch costs from Azure
    data = get_costs(subscription_id, start_date, end_date, dimension, args.retry_delay)
    
    # Apply filter first if specified (before resource name filtering)
    if args.filter:
        original_count = len(data)
        data = filter_costs(data, args.filter)
        print(f"Filtered by type '{args.filter}': {len(data)} rows (from {original_count})", file=sys.stderr)
    
    # Apply resource name filter if specified (for --grouping resource)
    if args.resource_name:
        original_count = len(data)
        # Extract resource name from ResourceId path and do exact match
        # ResourceId format: /subscriptions/.../resourcegroups/.../providers/microsoft.storage/storageaccounts/NAME
        filtered_data = []
        for row in data:
            resource_id = row[2].lower()
            # Extract the last part of the path (resource name)
            resource_name = resource_id.split('/')[-1]
            if resource_name == args.resource_name.lower():
                filtered_data.append(row)
        data = filtered_data
        print(f"Filtered by resource name '{args.resource_name}': {len(data)} rows (from {original_count})", file=sys.stderr)
    
    # Prepare metadata for CSV comments
    from datetime import datetime as dt
    metadata = {
        'date_generated': dt.now().strftime('%Y-%m-%d %H:%M:%S'),
        'command': ' '.join(sys.argv),
        'subscription': subscription_id,
        'start_date': start_date,
        'end_date': end_date,
        'grouping': dimension,
        'filter': args.filter,
        'resource_name': args.resource_name
    }
    
    # Output as CSV (simplify ResourceId if filtering by resource name)
    output_csv(data, simplify_resource_id=bool(args.resource_name), metadata=metadata)


if __name__ == '__main__':
    main()
