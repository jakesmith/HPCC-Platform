#!/usr/bin/env python3
"""
Analyze Azure cost CSV data and display formatted breakdown.

This script reads cost CSV data (from get_costs.py or compatible source)
and displays formatted cost breakdowns with various focus options.

Usage:
    python3 analyze_costs.py <costs.csv> [options]

Examples:
    # Analyze overall cost breakdown
    python3 analyze_costs.py costs.csv

    # Focus on storage subcategories
    python3 analyze_costs.py costs.csv --focus storage

    # Focus on compute/VM details
    python3 analyze_costs.py costs.csv --focus compute

    # Show daily breakdown instead of aggregated period
    python3 analyze_costs.py costs.csv --daily

    # Pipe from collector
    ./get_costs.py 2025-10-28 2025-11-04 | python3 analyze_costs.py -

Input CSV Format:
    Cost,Date,Category,Currency
    123.45,20251028,Virtual Machines,USD
    67.89,20251028,Storage,USD
"""

import sys
import csv
import argparse
from collections import defaultdict
from datetime import datetime


def read_costs_csv(filename):
    """Read cost data from CSV file or stdin.

    Args:
        filename: Path to CSV file or '-' for stdin

    Returns:
        List of tuples: [(cost, date_str, category, currency), ...]
    """
    costs = []

    file_handle = sys.stdin if filename == '-' else open(filename, 'r')

    try:
        # Filter out comment lines starting with #
        filtered_lines = (line for line in file_handle if not line.strip().startswith('#'))
        reader = csv.DictReader(filtered_lines)
        for row in reader:
            cost = float(row['Cost'])
            date = row['Date']  # Keep as string (YYYYMMDD format)
            category = row['Category']
            currency = row['Currency']
            costs.append((cost, date, category, currency))
    finally:
        if filename != '-':
            file_handle.close()

    return costs


def group_by_date_and_category(costs):
    """Group costs by date and category.

    Returns:
        dict: {date_str: {category: total_cost}}
    """
    by_date = defaultdict(lambda: defaultdict(float))

    for cost, date, category, currency in costs:
        by_date[date][category] += cost

    return by_date


def format_date(date_str):
    """Format date from YYYYMMDD to YYYY-MM-DD."""
    if len(date_str) == 8:
        return f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return date_str


def print_breakdown(by_date):
    """Print formatted cost breakdown by VM, Storage, Other."""
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

        formatted_date = format_date(date)
        print(f"{formatted_date:<12} ${vms:>13,.2f} ${storage:>13,.2f} ${other:>13,.2f} ${total:>13,.2f}")

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
    # Get top subcategories by total cost
    subcategory_totals = defaultdict(float)
    for date_costs in by_date.values():
        for subcategory, cost in date_costs.items():
            subcategory_totals[subcategory] += cost

    top_subcategories = sorted(subcategory_totals.items(), key=lambda x: x[1], reverse=True)[:5]
    top_names = [name for name, _ in top_subcategories]

    # Print headers (abbreviated)
    print("Date        ", end=' ')
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
        formatted_date = format_date(date)
        print(f"{formatted_date}", end='     ')

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


def print_storage_breakdown_extended(by_date):
    """Print extended storage breakdown showing top 10 meters with abbreviated names."""
    print("\n=== EXTENDED STORAGE BREAKDOWN (By Meter) ===\n")

    # Get top meters across all dates
    meter_totals = defaultdict(float)
    for date_meters in by_date.values():
        for meter, cost in date_meters.items():
            meter_totals[meter] += cost

    # Sort and get top 10 meters
    top_meters = sorted(meter_totals.items(), key=lambda x: x[1], reverse=True)[:10]
    top_meter_names = [name for name, _ in top_meters]

    # Print header with abbreviated meter names
    print("Date      ", end='')
    for meter_name in top_meter_names:
        # Abbreviate long meter names aggressively
        abbrev = meter_name.replace('Hot ZRS ', 'HZ-') \
                           .replace('Premium ', 'P') \
                           .replace('Standard ', 'S-') \
                           .replace('Operations', 'Op') \
                           .replace('Data Stored', 'Stor') \
                           .replace('Data Processed', 'Proc') \
                           .replace('Analytics Logs', 'A-Log') \
                           .replace('Provisioned', 'Prv') \
                           .replace('Capacity', 'Cap') \
                           .replace('Ingress', 'In') \
                           .replace('Iterative', 'Iter') \
                           .replace('Write', 'Wr') \
                           .replace('Read', 'Rd')
        abbrev = abbrev[:10]  # Limit to 10 chars
        print(f" {abbrev:>10}", end='')
    print(f" {'Other':>10} {'Total':>10}")
    print("=" * 145)

    grand_total = 0.0
    column_totals = defaultdict(float)

    for date in sorted(by_date.keys()):
        date_meters = by_date[date]
        date_total = sum(date_meters.values())
        grand_total += date_total

        formatted_date = format_date(date)
        print(f"{formatted_date}", end='  ')

        other_cost = 0.0
        for meter_name in top_meter_names:
            cost = date_meters.get(meter_name, 0.0)
            column_totals[meter_name] += cost
            print(f" ${cost:>9,.2f}", end='')

        # Calculate "Other" category
        for meter, cost in date_meters.items():
            if meter not in top_meter_names:
                other_cost += cost
        column_totals['Other'] += other_cost

        print(f" ${other_cost:>9,.2f} ${date_total:>9,.2f}")

    print("=" * 145)
    print(f"{'TOTAL':<10}", end='')
    for meter_name in top_meter_names:
        print(f" ${column_totals[meter_name]:>9,.2f}", end='')
    print(f" ${column_totals['Other']:>9,.2f} ${grand_total:>9,.2f}")

    # Print percentage breakdown
    print("\nMeter Breakdown:")
    all_totals = sorted(column_totals.items(), key=lambda x: x[1], reverse=True)
    for meter, cost in all_totals:
        if cost > 0:
            pct = (cost / grand_total) * 100
            print(f"  {meter}: {pct:.1f}% (${cost:,.2f})")


def print_compute_breakdown(by_date):
    """Print detailed compute cost breakdown by VM SKU."""
    # Get top VM SKUs by total cost
    sku_totals = defaultdict(float)
    for date_costs in by_date.values():
        for sku, cost in date_costs.items():
            sku_totals[sku] += cost

    sorted_skus = sorted(sku_totals.items(), key=lambda x: x[1], reverse=True)[:6]
    top_skus = [name for name, _ in sorted_skus]

    # Print header
    header = f"{'Date':<12}"
    for sku in top_skus:
        # Shorten SKU names for display
        display_name = sku.replace('Standard_', '').replace(' Low Priority', '-LP')[:13]
        header += f" {display_name:<14}"
    header += f" {'Other':<14} {'Total':<14}"
    print(header)
    print("=" * (12 + 15 * (len(top_skus) + 2)))

    # Track totals
    totals = defaultdict(float)
    total_all = 0

    for date in sorted(by_date.keys()):
        categories = by_date[date]
        total = sum(categories.values())

        # Print row
        formatted_date = format_date(date)
        row = f"{formatted_date:<12}"
        for sku in top_skus:
            cost = categories.get(sku, 0)
            row += f" ${cost:>12,.2f}"
            totals[sku] += cost

        # Calculate "other" costs
        other = sum(cost for name, cost in categories.items() if name not in top_skus)
        totals['Other'] += other
        total_all += total
        row += f" ${other:>12,.2f} ${total:>12,.2f}"
        print(row)

    # Print totals
    print("=" * (12 + 15 * (len(top_skus) + 2)))
    total_row = f"{'TOTAL':<12}"
    for sku in top_skus:
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


def print_single_day_breakdown(by_date, focus=None):
    """Print simplified breakdown for a single day."""
    date = list(by_date.keys())[0]
    categories = by_date[date]
    total = sum(categories.values())

    formatted_date = format_date(date)
    print(f"Date: {formatted_date}")
    print(f"Total: ${total:,.2f}\n")

    # Sort by cost
    sorted_items = sorted(categories.items(), key=lambda x: x[1], reverse=True)

    for name, cost in sorted_items:
        if cost > 0:
            pct = (cost / total) * 100
            # Shorten name for compute
            if focus == 'compute':
                display_name = name.replace('Standard_', '').replace(' Low Priority', '-LP')
            else:
                display_name = name
            print(f"  {display_name:<40} ${cost:>10,.2f}  ({pct:>5.1f}%)")


def output_csv_summary(by_date, focus=None, metadata=None):
    """Output aggregated CSV summary by date."""
    # Output metadata comments if provided
    if metadata:
        print("# Generated by: analyze_costs.py")
        print(f"# Date generated: {metadata.get('date', '')}")
        print(f"# Command: {metadata.get('command', '')}")
        if metadata.get('input_file'):
            print(f"# Input file: {metadata.get('input_file')}")
        if metadata.get('date_range'):
            print(f"# Date range: {metadata.get('date_range')}")
        if focus:
            print(f"# Focus: {focus}")
        print("# Format: Daily aggregated costs by meter category")
        print("#")
    
    # Get all unique categories across all dates
    all_categories = set()
    for date_costs in by_date.values():
        all_categories.update(date_costs.keys())

    # Sort categories by total cost to determine column order
    category_totals = defaultdict(float)
    for date_costs in by_date.values():
        for category, cost in date_costs.items():
            category_totals[category] += cost

    sorted_categories = sorted(category_totals.items(), key=lambda x: x[1], reverse=True)
    category_names = [name for name, _ in sorted_categories]

    # Print header
    header = "Date," + ",".join(category_names) + ",Total"
    print(header)

    # Print each date
    for date in sorted(by_date.keys()):
        date_costs = by_date[date]
        formatted_date = format_date(date)
        total = sum(date_costs.values())

        row = [formatted_date]
        for category in category_names:
            cost = date_costs.get(category, 0.0)
            row.append(f"{cost:.2f}")
        row.append(f"{total:.2f}")

        print(",".join(row))


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Azure cost CSV data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze overall costs
  %(prog)s costs.csv

  # Focus on storage breakdown
  %(prog)s costs.csv --focus storage

  # Extended storage breakdown (requires meter-level data)
  %(prog)s costs.csv --focus storage --ext

  # Focus on compute breakdown
  %(prog)s costs.csv --focus compute

  # Show daily breakdown
  %(prog)s costs.csv --daily

  # CSV output for graphing/processing
  %(prog)s costs.csv --format csv > summary.csv
  %(prog)s costs.csv --focus storage --format csv > storage_daily.csv

  # Pipe from collector with meter-level data
  ./get_costs.py --subscription <sub> --grouping meter 2025-10-28 | %(prog)s - --focus storage --ext
        """
    )

    parser.add_argument('input_file', help='Input CSV file (or - for stdin)')
    parser.add_argument('--focus', choices=['storage', 'compute'],
                       help='Focus on detailed breakdown')
    parser.add_argument('--ext', action='store_true',
                       help='Show extended meter-level breakdown (requires meter-level data)')
    parser.add_argument('--daily', action='store_true',
                       help='Show each day separately instead of aggregated')
    parser.add_argument('--format', choices=['text', 'csv'],
                       default='text',
                       help='Output format: text (human-readable, default) or csv (machine-readable)')
    parser.add_argument('--start-date',
                       help='Start date filter (YYYY-MM-DD)')
    parser.add_argument('--end-date',
                       help='End date filter (YYYY-MM-DD)')

    args = parser.parse_args()

    # Read cost data
    costs = read_costs_csv(args.input_file)

    # Filter by date range if specified
    if args.start_date or args.end_date:
        start_filter = args.start_date.replace('-', '') if args.start_date else '00000000'
        end_filter = args.end_date.replace('-', '') if args.end_date else '99999999'
        costs = [(c, d, cat, cur) for c, d, cat, cur in costs if start_filter <= d <= end_filter]

    if not costs:
        print("No cost data found", file=sys.stderr)
        sys.exit(1)

    # Group by date and category
    by_date = group_by_date_and_category(costs)

    # CSV format output
    if args.format == 'csv':
        # Build metadata for comment header
        from datetime import datetime as dt
        metadata = {
            'date': dt.now().strftime('%Y-%m-%d %H:%M:%S'),
            'command': 'analyze_costs.py ' + ' '.join(sys.argv[1:]),
            'input_file': args.input_file if args.input_file != '-' else 'stdin'
        }
        
        # Determine date range from data
        if by_date:
            dates = sorted(by_date.keys())
            if len(dates) > 0:
                start_date = format_date(dates[0])
                end_date = format_date(dates[-1])
                metadata['date_range'] = f"{start_date} to {end_date}"
        
        output_csv_summary(by_date, args.focus, metadata=metadata)
        return

    # Text format output (default)
    if args.daily:
        # Show each day separately
        for date in sorted(by_date.keys()):
            single_day = {date: by_date[date]}
            print_single_day_breakdown(single_day, args.focus)
            print()
    else:
        # Aggregated view
        if args.focus == 'storage':
            print("=== STORAGE COST BREAKDOWN ===\n")
            print_storage_breakdown(by_date)
            if args.ext:
                print_storage_breakdown_extended(by_date)
        elif args.focus == 'compute':
            print("=== COMPUTE COST BREAKDOWN ===\n")
            print_compute_breakdown(by_date)
        else:
            print_breakdown(by_date)


if __name__ == '__main__':
    try:
        main()
    except BrokenPipeError:
        # Handle broken pipe (e.g., when piping to head)
        import os
        import sys
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(1)
