#!/usr/bin/env python3
"""
Azure Cost Breakdown Script

This script queries Azure Cost Management API and displays a breakdown of costs
by category (VMs, Storage, Other) for a given date range.

Usage:
    python3 azure_cost_breakdown.py <start_date> [end_date] [--focus=storage|compute]
    
Examples:
    python3 azure_cost_breakdown.py 2025-10-28
    python3 azure_cost_breakdown.py 2025-10-28 2025-11-04
    python3 azure_cost_breakdown.py 2025-10-28 --focus=storage
    python3 azure_cost_breakdown.py 2025-10-28 2025-11-04 --focus=compute
    
Date format: YYYY-MM-DD
"""

import sys
import json
import subprocess
import argparse
from collections import defaultdict
from datetime import datetime, timedelta


def get_costs(subscription_id, start_date, end_date, grouping_dimension='MeterCategory'):
    """Query Azure Cost Management API for costs in the date range."""
    
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
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error calling Azure API: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def process_costs(data):
    """Process cost data and group by date and category."""
    by_date = defaultdict(lambda: defaultdict(float))
    
    for row in data:
        cost, date, category, currency = row
        by_date[date][category] += cost
    
    return by_date


def print_breakdown(by_date):
    """Print formatted cost breakdown."""
    print(f"{'Date':<12} {'VMs (Nodes)':<15} {'Storage':<15} {'Other':<15} {'Total':<15}")
    print("=" * 75)
    
    total_vms = 0
    total_storage = 0
    total_other = 0
    total_all = 0
    
    for date in sorted(by_date.keys()):
        categories = by_date[date]
        vms = categories.get('Virtual Machines', 0)
        storage = categories.get('Storage', 0)
        total = sum(categories.values())
        other = total - vms - storage
        
        print(f"{date:<12} ${vms:>13,.2f} ${storage:>13,.2f} ${other:>13,.2f} ${total:>13,.2f}")
        
        total_vms += vms
        total_storage += storage
        total_other += other
        total_all += total
    
    print("=" * 75)
    print(f"{'TOTAL':<12} ${total_vms:>13,.2f} ${total_storage:>13,.2f} ${total_other:>13,.2f} ${total_all:>13,.2f}")
    print()
    
    # Print percentages
    if total_all > 0:
        print("Cost Breakdown:")
        print(f"  VMs (Nodes): {(total_vms/total_all)*100:.1f}%")
        print(f"  Storage:     {(total_storage/total_all)*100:.1f}%")
        print(f"  Other:       {(total_other/total_all)*100:.1f}%")


def print_storage_breakdown(by_date):
    """Print detailed storage costs by subcategory."""
    print("Date        ", end=' ')
    
    # Collect all unique subcategories
    all_subcategories = set()
    for date_costs in by_date.values():
        all_subcategories.update(date_costs.keys())
    
    # Get top subcategories by total cost
    subcategory_totals = defaultdict(float)
    for date_costs in by_date.values():
        for subcategory, cost in date_costs.items():
            subcategory_totals[subcategory] += cost
    
    top_subcategories = sorted(subcategory_totals.items(), key=lambda x: x[1], reverse=True)[:5]
    top_names = [name for name, _ in top_subcategories]
    
    # Print headers (abbreviated)
    for name in top_names:
        abbrev = name[:14]
        print(f"{abbrev:>15}", end=' ')
    print(f"{'Other':>15} {'Total':>15}")
    
    print("=" * 150)
    
    # Print each date's breakdown
    grand_total = 0.0
    column_totals = defaultdict(float)
    
    for date in sorted(by_date.keys()):
        date_costs = by_date[date]
        print(f"{date}", end='     ')
        
        total = sum(date_costs.values())
        grand_total += total
        
        other_cost = 0.0
        for name in top_names:
            cost = date_costs.get(name, 0.0)
            column_totals[name] += cost
            print(f"${cost:>14,.2f}", end=' ')
        
        # Calculate "Other" category
        for subcategory, cost in date_costs.items():
            if subcategory not in top_names:
                other_cost += cost
        column_totals['Other'] += other_cost
        
        print(f"${other_cost:>14,.2f} ${total:>14,.2f}")
    
    # Print totals
    print("=" * 150)
    print(f"{'TOTAL':<12}", end=' ')
    for name in top_names:
        print(f"${column_totals[name]:>14,.2f}", end=' ')
    print(f"${column_totals['Other']:>14,.2f} ${grand_total:>14,.2f}")
    
    # Print percentage breakdown
    print("\nStorage Cost Breakdown:")
    all_totals = sorted(column_totals.items(), key=lambda x: x[1], reverse=True)
    for name, cost in all_totals:
        if cost > 0:
            pct = (cost / grand_total) * 100
            print(f"  {name}: {pct:.1f}%")

def print_storage_breakdown_extended(by_date, start_date, end_date, subscription_id):
    """Print extended storage breakdown showing individual meter details."""
    print("=== EXTENDED STORAGE BREAKDOWN (By Meter) ===")
    print()
    
    # Get detailed data using Meter grouping
    data = get_costs(subscription_id, start_date, end_date, 'Meter')
    
    # Filter to storage-related costs
    storage_meters = defaultdict(lambda: defaultdict(float))
    
    for row in data:
        cost, date, meter_name, currency = row
        meter_lower = meter_name.lower()
        
        # Only process storage-related meters
        if not any(keyword in meter_lower for keyword in ['storage', 'disk', 'blob', 'file', 'data', 'lake', 'zrs', 'lrs', 'grs']):
            continue
        
        storage_meters[date][meter_name] += cost
    
    # Get top meters across all dates
    meter_totals = defaultdict(float)
    for date_meters in storage_meters.values():
        for meter, cost in date_meters.items():
            meter_totals[meter] += cost
    
    # Sort and get top 10 meters
    top_meters = sorted(meter_totals.items(), key=lambda x: x[1], reverse=True)[:10]
    top_meter_names = [name for name, _ in top_meters]
    
    # Print header
    print("Date        ", end=' ')
    for meter_name in top_meter_names:
        abbrev = meter_name[:14]
        print(f"{abbrev:>15}", end=' ')
    print(f"{'Other':>15} {'Total':>15}")
    print("=" * 200)
    
    grand_total = 0.0
    column_totals = defaultdict(float)
    
    for date in sorted(storage_meters.keys()):
        date_meters = storage_meters[date]
        date_total = sum(date_meters.values())
        grand_total += date_total
        
        print(f"{date}", end='     ')
        
        other_cost = 0.0
        for meter_name in top_meter_names:
            cost = date_meters.get(meter_name, 0.0)
            column_totals[meter_name] += cost
            print(f"${cost:>14,.2f}", end=' ')
        
        # Calculate "Other" category
        for meter, cost in date_meters.items():
            if meter not in top_meter_names:
                other_cost += cost
        column_totals['Other'] += other_cost
        
        print(f"${other_cost:>14,.2f} ${date_total:>14,.2f}")
    
    print("=" * 200)
    print(f"{'TOTAL':<12}", end=' ')
    for meter_name in top_meter_names:
        print(f"${column_totals[meter_name]:>14,.2f}", end=' ')
    print(f"${column_totals['Other']:>14,.2f} ${grand_total:>14,.2f}")
    
    # Print percentage breakdown
    print("\nMeter Breakdown:")
    all_totals = sorted(column_totals.items(), key=lambda x: x[1], reverse=True)
    for meter, cost in all_totals:
        if cost > 0:
            pct = (cost / grand_total) * 100
            print(f"  {meter}: {pct:.1f}% (${cost:,.2f})")

def print_compute_breakdown(by_date):
    """Print detailed compute cost breakdown by meter name (VM SKU)."""
    # Collect all unique VM SKUs
    all_skus = set()
    for categories in by_date.values():
        all_skus.update(categories.keys())
    
    # Print header
    header = f"{'Date':<12}"
    sorted_skus = sorted(all_skus, key=lambda x: sum(by_date[d].get(x, 0) for d in by_date.keys()), reverse=True)[:6]
    for sku in sorted_skus:
        # Shorten SKU names for display
        display_name = sku.replace('Standard_', '').replace(' Low Priority', '-LP')[:13]
        header += f" {display_name:<14}"
    header += f" {'Other':<14} {'Total':<14}"
    print(header)
    print("=" * (12 + 15 * (min(len(sorted_skus), 6) + 2)))
    
    # Track totals
    totals = defaultdict(float)
    total_all = 0
    
    for date in sorted(by_date.keys()):
        categories = by_date[date]
        total = sum(categories.values())
        
        # Print row
        row = f"{date:<12}"
        for sku in sorted_skus:
            cost = categories.get(sku, 0)
            row += f" ${cost:>12,.2f}"
            totals[sku] += cost
        
        # Calculate "other" costs
        other = sum(cost for name, cost in categories.items() if name not in sorted_skus)
        totals['Other'] += other
        total_all += total
        row += f" ${other:>12,.2f} ${total:>12,.2f}"
        print(row)
    
    # Print totals
    print("=" * (12 + 15 * (min(len(sorted_skus), 6) + 2)))
    total_row = f"{'TOTAL':<12}"
    for sku in sorted_skus:
        total_row += f" ${totals[sku]:>12,.2f}"
    total_row += f" ${totals['Other']:>12,.2f} ${total_all:>12,.2f}"
    print(total_row)
    print()
    
    # Print percentage breakdown
    if total_all > 0:
        print("Compute Cost Breakdown:")
        for sku, cost in sorted(totals.items(), key=lambda x: x[1], reverse=True):
            if cost > 0:
                print(f"  {sku}: {(cost/total_all)*100:.1f}%")


def main():
    parser = argparse.ArgumentParser(
        description='Query Azure cost breakdown by date range',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 2025-10-28
  %(prog)s 2025-10-28 2025-11-04
  %(prog)s 2025-10-28 --focus=storage
  %(prog)s 2025-10-28 2025-11-04 --focus=compute
        """
    )
    
    parser.add_argument('start_date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('end_date', nargs='?', help='End date (YYYY-MM-DD), defaults to tomorrow')
    parser.add_argument('--focus', choices=['storage', 'compute'], help='Focus on detailed storage or compute breakdown')
    parser.add_argument('--ext', action='store_true', help='Show extended information (storage: at-rest vs access costs)')
    
    args = parser.parse_args()
    
    start_date = args.start_date
    
    # If end_date not provided, use tomorrow (to include today's partial costs)
    if args.end_date:
        end_date = args.end_date
    else:
        tomorrow = datetime.now() + timedelta(days=1)
        end_date = tomorrow.strftime('%Y-%m-%d')
    
    # Get subscription ID from current context
    try:
        result = subprocess.run(
            ['az', 'account', 'show', '--query', 'id', '-o', 'tsv'],
            capture_output=True,
            text=True,
            check=True
        )
        subscription_id = result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error getting subscription ID: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Querying costs from {start_date} to {end_date}")
    print(f"Subscription: {subscription_id}")
    print()
    
    if args.focus == 'storage':
        # Get detailed storage breakdown by MeterSubcategory
        print("=== STORAGE COST BREAKDOWN ===")
        print()
        data = get_costs(subscription_id, start_date, end_date, 'MeterSubcategory')
        
        # Filter to only storage-related costs
        storage_data = []
        for row in data:
            cost, date, subcategory, currency = row
            # Keep only storage-related subcategories
            if any(keyword in subcategory.lower() for keyword in ['storage', 'disk', 'blob', 'file', 'data']):
                storage_data.append(row)
        
        by_date = process_costs(storage_data)
        print_storage_breakdown(by_date)
        
        # Show extended breakdown if requested
        if args.ext:
            print()
            print()
            print_storage_breakdown_extended(by_date, start_date, end_date, subscription_id)
        
    elif args.focus == 'compute':
        # Get detailed compute breakdown by Meter (includes VM SKU info)
        print("=== COMPUTE COST BREAKDOWN ===")
        print()
        data = get_costs(subscription_id, start_date, end_date, 'Meter')
        
        # Filter to only VM-related costs
        vm_data = []
        for row in data:
            cost, date, meter_name, currency = row
            # Keep only VM-related meter names
            if any(keyword in meter_name.lower() for keyword in ['d48', 'd64', 'd16', 'd32', 'l32', 'l64', 'compute', 'virtual machine']):
                vm_data.append(row)
        
        by_date = process_costs(vm_data)
        print_compute_breakdown(by_date)
        
    else:
        # Default summary view
        data = get_costs(subscription_id, start_date, end_date, 'MeterCategory')
        by_date = process_costs(data)
        print_breakdown(by_date)


if __name__ == '__main__':
    main()
