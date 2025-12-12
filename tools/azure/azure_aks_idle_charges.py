#!/usr/bin/env python3
"""
Azure AKS Idle Charges Script

This script queries Azure Cost Management API at the AKS cluster resource level
to get namespace-level cost breakdown including idle charges.

Usage:
    python3 azure_aks_idle_charges.py [--cluster-name <name>] [--date <YYYY-MM-DD>]
    
Examples:
    python3 azure_aks_idle_charges.py
    python3 azure_aks_idle_charges.py --cluster-name us-prod400thor-prod-aks-000
    python3 azure_aks_idle_charges.py --date 2025-11-04
"""

import sys
import json
import subprocess
import argparse
from datetime import datetime, timedelta
from collections import defaultdict


def get_subscription_id():
    """Get current Azure subscription ID."""
    try:
        result = subprocess.run(
            ['az', 'account', 'show', '--query', 'id', '-o', 'tsv'],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error getting subscription ID: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def get_aks_cluster_resource_id(cluster_name=None):
    """Get the full resource ID of the AKS cluster."""
    try:
        if cluster_name:
            # Query for specific cluster
            cmd = [
                'az', 'aks', 'list',
                '--query', f"[?name=='{cluster_name}'].id | [0]",
                '-o', 'tsv'
            ]
        else:
            # Get first AKS cluster if no name specified
            cmd = [
                'az', 'aks', 'list',
                '--query', '[0].id',
                '-o', 'tsv'
            ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        resource_id = result.stdout.strip()
        
        if not resource_id:
            print(f"Error: AKS cluster '{cluster_name}' not found" if cluster_name else "Error: No AKS clusters found", file=sys.stderr)
            sys.exit(1)
            
        return resource_id
    except subprocess.CalledProcessError as e:
        print(f"Error getting AKS cluster resource ID: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def query_aks_costs(subscription_id, resource_id, start_date, end_date):
    """Query AKS costs at subscription scope filtered by cluster resource ID."""
    
    # Ensure dates are in ISO format
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
            "granularity": "None",
            "aggregation": {
                "totalCost": {
                    "name": "Cost",
                    "function": "Sum"
                }
            },
            "grouping": [
                {
                    "type": "Dimension",
                    "name": "MeterSubcategory"
                }
def query_aks_costs(subscription_id, resource_id, start_date, end_date):
    """Query AKS costs at subscription scope filtered by cluster resource ID."""
    
    # Ensure dates are in ISO format
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
            "granularity": "None",
            "aggregation": {
                "totalCost": {
                    "name": "Cost",
                    "function": "Sum"
                }
            },
            "grouping": [
                {
                    "type": "Dimension",
                    "name": "MeterSubcategory"
                }
            ],
            "filter": {
                "dimensions": {
                    "name": "ResourceId",
                    "operator": "In",
                    "values": [resource_id]
                }
            }
        }
    }
    
    # Call Azure Cost Management API at subscription scope
    uri = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-03-01"
    
    cmd = [
        'az', 'rest',
        '--method', 'POST',
        '--uri', uri,
        '--body', json.dumps(request_body),
        '--output', 'json'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        response = json.loads(result.stdout)
        return response.get('properties', {}).get('rows', [])
    except subprocess.CalledProcessError as e:
        print(f"Error calling Azure Cost Management API: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def print_cost_breakdown(cost_data, cluster_name):
    """Print cost breakdown by meter subcategory."""
    
    print(f"\nAKS Cluster: {cluster_name}")
    print("=" * 80)
    print(f"{'Meter Subcategory':<50} {'Cost (USD)':>15} {'Percentage':>12}")
    print("-" * 80)
    
    # Process data
    category_costs = {}
    total_cost = 0
    
    for row in cost_data:
        cost = row[0]
        category = row[1] if len(row) > 1 else 'Unknown'
        
        if category in category_costs:
            category_costs[category] += cost
        else:
            category_costs[category] = cost
        total_cost += cost
    
    # Sort by cost descending
    sorted_categories = sorted(category_costs.items(), key=lambda x: x[1], reverse=True)
    
    # Print each category
    for category, cost in sorted_categories:
        percentage = (cost / total_cost * 100) if total_cost > 0 else 0
        print(f"{category:<50} ${cost:>14,.2f} {percentage:>11.1f}%")
    
    print("-" * 80)
    print(f"{'TOTAL':<50} ${total_cost:>14,.2f} {100.0:>11.1f}%")
    print()


def print_namespace_breakdown(cost_data, cluster_name):
    """Print cost breakdown by namespace including idle charges."""
    
    print(f"\nAKS Cluster: {cluster_name}")
    print("=" * 80)
    print(f"{'Namespace':<30} {'Cost (USD)':>15} {'Percentage':>12}")
    print("-" * 80)
    
    # Process data
    namespace_costs = {}
    total_cost = 0
    
    for row in cost_data:
        cost = row[0]
        namespace = row[1] if len(row) > 1 else 'untagged'
        
        # Handle special namespace names from Azure
        if not namespace or namespace == '':
            namespace = 'unallocated'
        
        namespace_costs[namespace] = cost
        total_cost += cost
    
    # Sort by cost descending
    sorted_namespaces = sorted(namespace_costs.items(), key=lambda x: x[1], reverse=True)
    
    # Print each namespace
    for namespace, cost in sorted_namespaces:
        percentage = (cost / total_cost * 100) if total_cost > 0 else 0
        print(f"{namespace:<30} ${cost:>14,.2f} {percentage:>11.1f}%")
    
    print("-" * 80)
    print(f"{'TOTAL':<30} ${total_cost:>14,.2f} {100.0:>11.1f}%")
    print()
    
    # Highlight idle charges if present
    if 'idle charges' in namespace_costs:
        idle_cost = namespace_costs['idle charges']
        idle_pct = (idle_cost / total_cost * 100) if total_cost > 0 else 0
        print(f"⚠️  Idle Charges: ${idle_cost:,.2f} ({idle_pct:.1f}% of total)")
        print(f"    This represents allocated but unused resources")


def main():
    parser = argparse.ArgumentParser(
        description='Query AKS cluster costs with namespace breakdown including idle charges',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--cluster-name', help='AKS cluster name (uses first cluster if not specified)')
    parser.add_argument('--date', help='Query date (YYYY-MM-DD), defaults to yesterday')
    parser.add_argument('--days', type=int, default=1, help='Number of days to query (default: 1)')
    
    args = parser.parse_args()
    
    # Determine date range
    if args.date:
        end_date_obj = datetime.strptime(args.date, '%Y-%m-%d') + timedelta(days=1)
        start_date_obj = end_date_obj - timedelta(days=args.days)
    else:
        # Default to yesterday
        end_date_obj = datetime.now()
        start_date_obj = end_date_obj - timedelta(days=args.days)
    
    start_date = start_date_obj.strftime('%Y-%m-%d')
    end_date = end_date_obj.strftime('%Y-%m-%d')
    
    print(f"Querying AKS costs from {start_date} to {end_date}")
    
    # Get subscription and cluster info
    subscription_id = get_subscription_id()
    print(f"Subscription: {subscription_id}")
    
    # Get cluster resource ID to extract cluster name
    cluster_resource_id = get_aks_cluster_resource_id(args.cluster_name)
    cluster_name = cluster_resource_id.split('/')[-1]
    print(f"Cluster: {cluster_name}")
    print()
    
    # Query costs
    cost_data = query_aks_costs(subscription_id, cluster_resource_id, start_date, end_date)
    
    if not cost_data:
        print("No cost data found for the specified period")
        sys.exit(0)
    
    # Display results
    print_cost_breakdown(cost_data, cluster_name)


if __name__ == '__main__':
    main()
