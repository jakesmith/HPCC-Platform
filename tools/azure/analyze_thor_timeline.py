#!/usr/bin/env python3
"""
Analyze Thor workunit timeline data from CSV to calculate cluster utilization.

This script reads CSV output from get_thor_timeline.py and provides:
- Cluster utilization percentage over the time period
- Concurrent workunit statistics
- Optional text-based Gantt chart visualization

Usage:
    analyze_thor_timeline.py <timeline.csv> [options]
"""

import sys
import csv
import argparse
import statistics
from datetime import datetime, timedelta
from collections import defaultdict

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

def parse_timestamp(ts_str):
    """Parse timestamp string to datetime object."""
    if not ts_str or ts_str == 'N/A':
        return None
    
    # Handle various timestamp formats
    formats = [
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S.%f',
    ]
    
    for fmt in formats:
        try:
            # Remove timezone info if present
            clean_ts = ts_str.split('Z')[0].split('+')[0]
            return datetime.strptime(clean_ts, fmt)
        except ValueError:
            continue
    
    return None

def parse_duration(duration_str):
    """Parse duration string to timedelta.
    
    Supports both formats:
    - Human-readable: '1h 23m 45s', '2d 5h 30m'
    - HH:MM:SS: '01:23:45', '125:30:45'
    """
    if not duration_str or duration_str == 'N/A' or duration_str == 'UNKNOWN':
        return None
    
    # Try HH:MM:SS format first
    if ':' in duration_str:
        try:
            parts = duration_str.split(':')
            if len(parts) == 3:
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2])
                total_seconds = hours * 3600 + minutes * 60 + seconds
                return timedelta(seconds=total_seconds)
        except (ValueError, IndexError):
            pass
    
    # Fall back to human-readable format (1h 23m 45s)
    total_seconds = 0
    parts = duration_str.split()
    
    for part in parts:
        if 'd' in part:
            total_seconds += int(part.replace('d', '')) * 86400
        elif 'h' in part:
            total_seconds += int(part.replace('h', '')) * 3600
        elif 'm' in part:
            total_seconds += int(part.replace('m', '')) * 60
        elif 's' in part:
            total_seconds += float(part.replace('s', ''))
    
    return timedelta(seconds=total_seconds)

def read_timeline_csv(filename):
    """Read timeline CSV file and return list of workunit phases.
    
    Supports both old and new CSV formats:
    - Old: WUID,Cluster,Phase,WhenStarted,WhenFinished,Duration
    - New: WUID,Cluster,Phase,SubgraphID,WhenStarted,WhenFinished,Duration
    
    Filters out subgraph entries (non-empty SubgraphID) to focus on graph-level data.
    """
    phases = []
    
    with open(filename, 'r') as f:
        # Skip any non-CSV lines at the beginning (status messages)
        lines = f.readlines()
        csv_start = 0
        for i, line in enumerate(lines):
            if line.startswith('WUID,'):
                csv_start = i
                break
        
        # Parse CSV starting from the header line
        reader = csv.DictReader(lines[csv_start:])
        for row in reader:
            # Skip subgraph entries (new format with non-empty SubgraphID)
            subgraph_id = row.get('SubgraphID', '').strip()
            if subgraph_id:
                continue
            
            start_time = parse_timestamp(row['WhenStarted'])
            finish_time = parse_timestamp(row['WhenFinished'])
            
            if start_time and finish_time:
                phases.append({
                    'wuid': row['WUID'],
                    'cluster': row['Cluster'],
                    'phase': row['Phase'],
                    'graph': row.get('Graph', row['Phase']),  # Use Phase as fallback for Graph
                    'start': start_time,
                    'finish': finish_time,
                    'duration': finish_time - start_time
                })
    
    return phases

def calculate_utilization(phases, start_date=None, end_date=None):
    """Calculate cluster utilization statistics."""
    if not phases:
        return None
    
    # Determine overall time range
    if start_date is None:
        start_date = min(p['start'] for p in phases)
    if end_date is None:
        end_date = max(p['finish'] for p in phases)
    
    total_period = (end_date - start_date).total_seconds()
    
    # Calculate total execution time (sum of all phase durations)
    total_execution_time = sum(p['duration'].total_seconds() for p in phases)
    
    # Calculate actual utilized time (accounting for overlaps)
    # Sort events by time
    events = []
    for phase in phases:
        events.append((phase['start'], 'start', phase))
        events.append((phase['finish'], 'end', phase))
    events.sort(key=lambda x: x[0])
    
    # Calculate time with at least one workunit running
    utilized_time = 0
    active_count = 0
    last_time = None
    max_concurrent = 0
    concurrent_histogram = defaultdict(int)
    
    for event_time, event_type, phase in events:
        if last_time is not None and active_count > 0:
            duration = (event_time - last_time).total_seconds()
            utilized_time += duration
            concurrent_histogram[active_count] += duration
        
        if event_type == 'start':
            active_count += 1
            max_concurrent = max(max_concurrent, active_count)
        else:
            active_count -= 1
        
        last_time = event_time
    
    utilization_pct = (utilized_time / total_period * 100) if total_period > 0 else 0
    avg_concurrent = sum(count * time for count, time in concurrent_histogram.items()) / utilized_time if utilized_time > 0 else 0
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'total_period': timedelta(seconds=total_period),
        'utilized_time': timedelta(seconds=utilized_time),
        'idle_time': timedelta(seconds=total_period - utilized_time),
        'utilization_pct': utilization_pct,
        'total_workunits': len(set(p['wuid'] for p in phases)),
        'total_phases': len(phases),
        'max_concurrent': max_concurrent,
        'avg_concurrent': avg_concurrent,
        'concurrent_histogram': dict(concurrent_histogram),
        'total_execution_time': timedelta(seconds=total_execution_time)
    }

def format_timedelta(td):
    """Format timedelta for display."""
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def format_summary_csv(stats, costs, csv_file, args, simulated_stats=None, simulated_costs=None):
    """Format summary metrics as CSV.
    
    Args:
        stats: Utilization statistics
        costs: Cost breakdown dict
        csv_file: Input CSV filename
        args: Command line arguments
        simulated_stats: Optional simulated statistics
        simulated_costs: Optional simulated cost breakdown
    """
    import datetime
    
    # Calculate uptime
    total_uptime_hours = costs['total_instance_hours'] / stats['max_concurrent'] if stats['max_concurrent'] > 0 else 0
    total_period_hours = stats['total_period'].total_seconds() / 3600
    uptime_ratio = (total_uptime_hours / total_period_hours * 100) if total_period_hours > 0 else 0
    
    # Build command string
    cmd_parts = [f"analyze_thor_timeline.py {csv_file}"]
    if args.vms_per_instance != 25:
        cmd_parts.append(f"--vms-per-instance {args.vms_per_instance}")
    if args.cost_per_vm != 0.3936:
        cmd_parts.append(f"--cost-per-vm {args.cost_per_vm}")
    if args.spin_down_time != 10:
        cmd_parts.append(f"--spin-down-time {args.spin_down_time}")
    if args.spin_up_time != 1:
        cmd_parts.append(f"--spin-up-time {args.spin_up_time}")
    if args.spin_up_cost != 1.00:
        cmd_parts.append(f"--spin-up-cost {args.spin_up_cost}")
    if args.simulate_max_concurrent:
        cmd_parts.append(f"--simulate-max-concurrent {args.simulate_max_concurrent}")
    cmd_parts.append("--format csv")
    command_str = ' '.join(cmd_parts)
    
    # Print metadata comments
    print("# Generated by: analyze_thor_timeline.py")
    print(f"# Date generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Command: {command_str}")
    print("#")
    
    if simulated_stats:
        # Comparison mode - show original and simulated
        sim_uptime_hours = simulated_costs['total_instance_hours'] / args.simulate_max_concurrent if args.simulate_max_concurrent > 0 else 0
        sim_uptime_ratio = (sim_uptime_hours / total_period_hours * 100) if total_period_hours > 0 else 0
        
        print("Scenario,StartDate,EndDate,PeriodHours,TotalWorkunits,TotalPhases,MaxConcurrent,AvgConcurrent,UtilizationPct,UtilizedHours,IdleHours,WorkInstanceHrs,IdlInstanceHrs,NumSpinups,WorkCost,IdleCost,SpinupCost,TotalCost,CostPerGraph,UptimeHours,UptimeRatio")
        
        # Original
        print(f"Original,{stats['start_date']},{stats['end_date']},{total_period_hours:.2f},"
              f"{stats['total_workunits']},{stats['total_phases']},{stats['max_concurrent']},{stats['avg_concurrent']:.2f},"
              f"{stats['utilization_pct']:.2f},{stats['utilized_time'].total_seconds()/3600:.2f},{stats['idle_time'].total_seconds()/3600:.2f},"
              f"{costs['work_instance_hours']:.2f},{costs['idle_instance_hours']:.2f},{costs['num_spinups']},"
              f"{costs['work_cost']:.2f},{costs['idle_cost']:.2f},{costs['spinup_cost']:.2f},{costs['total_cost']:.2f},"
              f"{costs['total_cost']/stats['total_phases']:.4f},{total_uptime_hours:.2f},{uptime_ratio:.2f}")
        
        # Simulated
        print(f"Simulated,{simulated_stats['start_date']},{simulated_stats['end_date']},{total_period_hours:.2f},"
              f"{simulated_stats['total_workunits']},{simulated_stats['total_phases']},{args.simulate_max_concurrent},{simulated_stats['avg_concurrent']:.2f},"
              f"{simulated_stats['utilization_pct']:.2f},{simulated_stats['utilized_time'].total_seconds()/3600:.2f},{simulated_stats['idle_time'].total_seconds()/3600:.2f},"
              f"{simulated_costs['work_instance_hours']:.2f},{simulated_costs['idle_instance_hours']:.2f},{simulated_costs['num_spinups']},"
              f"{simulated_costs['work_cost']:.2f},{simulated_costs['idle_cost']:.2f},{simulated_costs['spinup_cost']:.2f},{simulated_costs['total_cost']:.2f},"
              f"{simulated_costs['total_cost']/simulated_stats['total_phases']:.4f},{sim_uptime_hours:.2f},{sim_uptime_ratio:.2f}")
    else:
        # Single scenario
        print("StartDate,EndDate,PeriodHours,TotalWorkunits,TotalPhases,MaxConcurrent,AvgConcurrent,UtilizationPct,UtilizedHours,IdleHours,WorkInstanceHrs,IdleInstanceHrs,NumSpinups,WorkCost,IdleCost,SpinupCost,TotalCost,CostPerGraph,UptimeHours,UptimeRatio")
        
        print(f"{stats['start_date']},{stats['end_date']},{total_period_hours:.2f},"
              f"{stats['total_workunits']},{stats['total_phases']},{stats['max_concurrent']},{stats['avg_concurrent']:.2f},"
              f"{stats['utilization_pct']:.2f},{stats['utilized_time'].total_seconds()/3600:.2f},{stats['idle_time'].total_seconds()/3600:.2f},"
              f"{costs['work_instance_hours']:.2f},{costs['idle_instance_hours']:.2f},{costs['num_spinups']},"
              f"{costs['work_cost']:.2f},{costs['idle_cost']:.2f},{costs['spinup_cost']:.2f},{costs['total_cost']:.2f},"
              f"{costs['total_cost']/stats['total_phases']:.4f},{total_uptime_hours:.2f},{uptime_ratio:.2f}")


def print_statistics(stats):
    """Print utilization statistics."""
    print("\n" + "="*70)
    print("THOR CLUSTER UTILIZATION ANALYSIS")
    print("="*70)
    
    print(f"\nTime Period:")
    print(f"  Start:              {stats['start_date']}")
    print(f"  End:                {stats['end_date']}")
    print(f"  Total Duration:     {format_timedelta(stats['total_period'])}")
    
    print(f"\nWorkunit Statistics:")
    print(f"  Total Workunits:    {stats['total_workunits']}")
    print(f"  Total Phases:       {stats['total_phases']}")
    print(f"  Total Exec Time:    {format_timedelta(stats['total_execution_time'])}")
    
    print(f"\nCluster Utilization:")
    print(f"  Time Utilized:      {format_timedelta(stats['utilized_time'])}")
    print(f"  Time Idle:          {format_timedelta(stats['idle_time'])}")
    print(f"  Utilization:        {stats['utilization_pct']:.2f}%")
    
    print(f"\nConcurrency:")
    print(f"  Max Concurrent:     {stats['max_concurrent']}")
    print(f"  Avg Concurrent:     {stats['avg_concurrent']:.2f}")
    
    print(f"\nConcurrency Distribution:")
    sorted_histogram = sorted(stats['concurrent_histogram'].items())
    for count, time_seconds in sorted_histogram:
        pct = (time_seconds / stats['utilized_time'].total_seconds() * 100) if stats['utilized_time'].total_seconds() > 0 else 0
        print(f"  {count:2d} concurrent:      {format_timedelta(timedelta(seconds=time_seconds)):>15s} ({pct:5.2f}%)")

def generate_gantt_chart(phases, resolution_minutes=5, max_width=100):
    """Generate text-based Gantt chart showing workunit execution timeline."""
    if not phases:
        return
    
    print("\n" + "="*70)
    print("GANTT CHART - Workunit Execution Timeline")
    print("="*70)
    
    # Determine time range
    start_time = min(p['start'] for p in phases)
    end_time = max(p['finish'] for p in phases)
    total_duration = (end_time - start_time).total_seconds()
    
    # Create time buckets
    resolution_seconds = resolution_minutes * 60
    num_buckets = int(total_duration / resolution_seconds) + 1
    
    # If too many buckets, adjust resolution
    if num_buckets > max_width:
        resolution_seconds = total_duration / max_width
        num_buckets = max_width
    
    # Count concurrent workunits in each bucket
    buckets = [0] * num_buckets
    
    for phase in phases:
        start_bucket = int((phase['start'] - start_time).total_seconds() / resolution_seconds)
        end_bucket = int((phase['finish'] - start_time).total_seconds() / resolution_seconds)
        
        for i in range(start_bucket, min(end_bucket + 1, num_buckets)):
            buckets[i] += 1
    
    max_concurrent = max(buckets) if buckets else 0
    
    # Print chart header
    print(f"\nTime Range: {start_time} to {end_time}")
    print(f"Resolution: {format_timedelta(timedelta(seconds=resolution_seconds))} per character")
    print(f"Max Concurrent: {max_concurrent}")
    print()
    
    # Print chart
    # Scale: height represents number of concurrent workunits
    chart_height = min(max_concurrent, 20)  # Limit height to 20 rows
    
    for level in range(chart_height, 0, -1):
        threshold = (level * max_concurrent) / chart_height
        line = f"{int(threshold):3d} |"
        
        for count in buckets:
            if count >= threshold:
                line += "█"
            else:
                line += " "
        
        print(line)
    
    # Print axis
    print("  0 +" + "-" * len(buckets))
    
    # Print time labels
    num_labels = min(10, len(buckets))
    label_positions = [int(i * len(buckets) / num_labels) for i in range(num_labels + 1)]
    
    time_label = "    "
    for i, pos in enumerate(label_positions[:-1]):
        bucket_time = start_time + timedelta(seconds=pos * resolution_seconds)
        label = bucket_time.strftime("%H:%M")
        
        # Calculate spacing
        next_pos = label_positions[i + 1] if i + 1 < len(label_positions) else len(buckets)
        spacing = next_pos - pos - len(label)
        
        time_label += label + " " * max(0, spacing)
    
    print(time_label)
    
    # Print legend
    print("\nLegend:")
    print("  Y-axis: Number of concurrent workunits")
    print("  X-axis: Time progression")
    print("  █: Active workunit execution")

def calculate_percentile(sorted_values, percentile):
    """Calculate the given percentile from a sorted list of values."""
    if not sorted_values:
        return None
    
    if percentile == 100:
        return sorted_values[-1]
    if percentile == 0:
        return sorted_values[0]
    
    # Use linear interpolation method
    n = len(sorted_values)
    k = (n - 1) * percentile / 100.0
    f = int(k)
    c = k - f
    
    if f + 1 < n:
        return sorted_values[f] + c * (sorted_values[f + 1] - sorted_values[f])
    else:
        return sorted_values[f]

def calculate_percentile_distribution(phases, max_concurrent=None, vms_per_instance=None, 
                                    cost_per_vm=None, spin_down_minutes=None, spin_up_cost=None):
    """Calculate percentile distribution across all graph executions.
    
    Args:
        phases: List of phase dicts
        max_concurrent: Optional max concurrent instances for cost calculation
        vms_per_instance: Optional VMs per instance for cost calculation
        cost_per_vm: Optional cost per VM per hour for cost calculation
        spin_down_minutes: Optional spin-down time for cost calculation
        spin_up_cost: Optional spin-up cost for cost calculation
    
    Returns:
        tuple: (percentiles, buckets, bucket_stats)
            - percentiles: dict with percentile thresholds
            - buckets: dict mapping bucket names to lists of phases
            - bucket_stats: dict with cost and time stats per bucket (if cost params provided)
    """
    # Get all durations
    all_durations = [phase['duration'].total_seconds() for phase in phases]
    sorted_durations = sorted(all_durations)
    
    # Calculate percentile thresholds
    percentiles = {
        'p50': calculate_percentile(sorted_durations, 50),
        'p75': calculate_percentile(sorted_durations, 75),
        'p90': calculate_percentile(sorted_durations, 90),
        'p95': calculate_percentile(sorted_durations, 95),
        'p99': calculate_percentile(sorted_durations, 99),
    }
    
    # Categorize each phase into a bucket
    buckets = {
        '0-50%': [],
        '50-75%': [],
        '75-90%': [],
        '90-95%': [],
        '95-99%': [],
        '99-100%': []
    }
    
    for phase in phases:
        duration = phase['duration'].total_seconds()
        if duration <= percentiles['p50']:
            buckets['0-50%'].append(phase)
        elif duration <= percentiles['p75']:
            buckets['50-75%'].append(phase)
        elif duration <= percentiles['p90']:
            buckets['75-90%'].append(phase)
        elif duration <= percentiles['p95']:
            buckets['90-95%'].append(phase)
        elif duration <= percentiles['p99']:
            buckets['95-99%'].append(phase)
        else:
            buckets['99-100%'].append(phase)
    
    # Calculate stats for each bucket if cost parameters provided
    bucket_stats = None
    if all(x is not None for x in [max_concurrent, vms_per_instance, cost_per_vm, spin_down_minutes, spin_up_cost]):
        # First calculate total cost for ALL phases to get proportional attribution
        total_costs = calculate_instance_costs(
            phases,
            max_concurrent,
            vms_per_instance,
            cost_per_vm,
            spin_down_minutes,
            spin_up_cost
        )
        
        # Calculate total work time across all phases
        total_work_time = sum(p['duration'].total_seconds() for p in phases)
        
        bucket_stats = {}
        for bucket_name, bucket_phases in buckets.items():
            if bucket_phases:
                # Calculate total execution time for phases in this bucket
                bucket_time = sum(p['duration'].total_seconds() for p in bucket_phases)
                bucket_time_hours = bucket_time / 3600
                
                # Attribute cost proportionally based on execution time
                # This represents the work cost portion only
                time_ratio = bucket_time / total_work_time if total_work_time > 0 else 0
                attributed_work_cost = total_costs['work_cost'] * time_ratio
                attributed_work_hours = total_costs['work_instance_hours'] * time_ratio
                
                # For idle and spinup costs, also attribute proportionally
                # (assumes uniform distribution, which is reasonable approximation)
                attributed_idle_cost = total_costs['idle_cost'] * time_ratio
                attributed_idle_hours = total_costs['idle_instance_hours'] * time_ratio
                attributed_spinup_cost = total_costs['spinup_cost'] * time_ratio
                attributed_spinups = int(total_costs['num_spinups'] * time_ratio)
                
                total_attributed_cost = attributed_work_cost + attributed_idle_cost + attributed_spinup_cost
                
                bucket_stats[bucket_name] = {
                    'count': len(bucket_phases),
                    'total_time_hours': bucket_time_hours,
                    'total_cost': total_attributed_cost,
                    'cost_per_graph': total_attributed_cost / len(bucket_phases),
                    'work_instance_hours': attributed_work_hours,
                    'idle_instance_hours': attributed_idle_hours,
                    'num_spinups': attributed_spinups,
                    'time_pct': time_ratio * 100
                }
            else:
                bucket_stats[bucket_name] = {
                    'count': 0,
                    'total_time_hours': 0,
                    'total_cost': 0,
                    'cost_per_graph': 0,
                    'work_instance_hours': 0,
                    'idle_instance_hours': 0,
                    'num_spinups': 0,
                    'time_pct': 0
                }
    
    return percentiles, buckets, bucket_stats

def format_duration_seconds(seconds):
    """Format duration in seconds to human-readable string."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.2f}s"
    elif seconds < 86400:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.0f}s"
    else:
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{days}d {hours}h {minutes}m"

def print_percentile_distribution(percentiles, buckets, total_count, bucket_stats=None):
    """Print percentile distribution showing how many graphs fall into each bucket.
    
    Args:
        percentiles: Dict with percentile thresholds
        buckets: Dict mapping bucket names to lists of phases
        total_count: Total number of phases
        bucket_stats: Optional dict with cost and time stats per bucket
    """
    print("\n" + "="*80)
    print("GRAPH EXECUTION TIME PERCENTILE DISTRIBUTION")
    print("="*80)
    print(f"\nTotal graph executions: {total_count}")
    print(f"\nPercentile Thresholds:")
    print(f"  P50 (Median):  {format_duration_seconds(percentiles['p50'])}")
    print(f"  P75:           {format_duration_seconds(percentiles['p75'])}")
    print(f"  P90:           {format_duration_seconds(percentiles['p90'])}")
    print(f"  P95:           {format_duration_seconds(percentiles['p95'])}")
    print(f"  P99:           {format_duration_seconds(percentiles['p99'])}")
    
    if bucket_stats:
        # Show distribution with cost and time details
        print(f"\nDistribution with Cost Analysis:")
        print(f"{'Range':<15} {'Count':>8} {'Pct':>6} {'Total Hrs':>11} {'Total Cost':>12} {'$/Graph':>10} {'Work Hrs':>10} {'Idle Hrs':>10} {'Spinups':>8}")
        print("-"*120)
        
        total_cost_all = 0
        for bucket_name in ['0-50%', '50-75%', '75-90%', '90-95%', '95-99%', '99-100%']:
            stats = bucket_stats[bucket_name]
            count = stats['count']
            pct = (count / total_count * 100) if total_count > 0 else 0
            
            print(f"{bucket_name:<15} {count:>8} {pct:>5.1f}% "
                  f"{stats['total_time_hours']:>10.2f}h "
                  f"${stats['total_cost']:>10,.2f} "
                  f"${stats['cost_per_graph']:>9.4f} "
                  f"{stats['work_instance_hours']:>9.1f}h "
                  f"{stats['idle_instance_hours']:>9.1f}h "
                  f"{stats['num_spinups']:>8}")
            total_cost_all += stats['total_cost']
        
        print("-"*120)
        print(f"{'TOTAL':<15} {total_count:>8} {'100.0%':>6} "
              f"{sum(bucket_stats[b]['total_time_hours'] for b in bucket_stats):>10.2f}h "
              f"${total_cost_all:>10,.2f}")
        
        # Show cost breakdown by percentile
        print(f"\nCost Breakdown by Percentile:")
        for bucket_name in ['0-50%', '50-75%', '75-90%', '90-95%', '95-99%', '99-100%']:
            stats = bucket_stats[bucket_name]
            if stats['count'] > 0:
                cost_pct = (stats['total_cost'] / total_cost_all * 100) if total_cost_all > 0 else 0
                print(f"  {bucket_name:<15} ${stats['total_cost']:>10,.2f} ({cost_pct:>5.1f}% of total cost)")
    else:
        # Show simple distribution without costs
        print(f"\nDistribution:")
        print(f"{'Percentile Range':<20} {'Count':>8} {'Percentage':>12}")
        print("-"*80)
        
        for bucket_name in ['0-50%', '50-75%', '75-90%', '90-95%', '95-99%', '99-100%']:
            count = len(buckets[bucket_name])
            pct = (count / total_count * 100) if total_count > 0 else 0
            print(f"{bucket_name:<20} {count:>8} {pct:>11.2f}%")
    
    print("="*80)

def list_graphs_in_percentile(buckets, percentile_range):
    """List all graphs in a specified percentile range."""
    # Normalize range format
    bucket_key = percentile_range if percentile_range.endswith('%') else f"{percentile_range}%"
    
    if bucket_key not in buckets:
        print(f"\nError: Invalid percentile range '{percentile_range}'")
        print("Valid ranges: 0-50, 50-75, 75-90, 90-95, 95-99, 99-100")
        return
    
    phases = buckets[bucket_key]
    
    print(f"\n" + "="*120)
    print(f"GRAPHS IN PERCENTILE RANGE: {bucket_key}")
    print("="*120)
    print(f"{'WUID':<25} {'Graph':<30} {'Duration':>15} {'Start Time':>20}")
    print("-"*120)
    
    # Sort by duration (descending)
    sorted_phases = sorted(phases, key=lambda p: p['duration'].total_seconds(), reverse=True)
    
    for phase in sorted_phases:
        wuid = phase.get('wuid', 'unknown')
        graph = phase.get('graph', 'unknown')
        duration = format_duration_seconds(phase['duration'].total_seconds())
        start = phase['start'].strftime('%Y-%m-%d %H:%M:%S')
        print(f"{wuid:<25} {graph:<30} {duration:>15} {start:>20}")
    
    print("="*120)
    print(f"Total: {len(phases)} graphs in {bucket_key} range")

def generate_graphical_gantt(phases, output_file='gantt.png'):
    """Generate graphical Gantt chart using matplotlib."""
    if not MATPLOTLIB_AVAILABLE:
        print("\nError: matplotlib is not installed. Install it with:", file=sys.stderr)
        print("  pip install matplotlib", file=sys.stderr)
        return False
    
    if not phases:
        return False
    
    # Sort phases by start time and WUID for better visualization
    phases_sorted = sorted(phases, key=lambda p: (p['wuid'], p['start']))
    
    # Create figure and axis
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]})
    
    # Top chart: Individual workunits as horizontal bars
    wuids = []
    wuid_to_idx = {}
    current_idx = 0
    
    for phase in phases_sorted:
        wuid = phase['wuid']
        if wuid not in wuid_to_idx:
            wuid_to_idx[wuid] = current_idx
            wuids.append(wuid)
            current_idx += 1
    
    # Plot each phase as a horizontal bar
    colors = plt.cm.Set3(range(len(wuids)))
    
    for phase in phases_sorted:
        wuid = phase['wuid']
        idx = wuid_to_idx[wuid]
        
        start = phase['start']
        duration = (phase['finish'] - phase['start']).total_seconds() / 3600  # hours
        
        ax1.barh(idx, duration, left=mdates.date2num(start), 
                height=0.8, color=colors[idx], 
                edgecolor='black', linewidth=0.5,
                label=wuid if phases_sorted.index(phase) == 0 or wuid != phases_sorted[phases_sorted.index(phase)-1]['wuid'] else '')
    
    # Format top chart
    ax1.set_yticks(range(len(wuids)))
    ax1.set_yticklabels(wuids)
    ax1.set_xlabel('Time', fontsize=12)
    ax1.set_ylabel('Workunit ID', fontsize=12)
    ax1.set_title('Thor Workunit Execution Timeline', fontsize=14, fontweight='bold')
    ax1.xaxis_date()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax1.grid(True, alpha=0.3)
    
    # Bottom chart: Concurrency over time
    start_time = min(p['start'] for p in phases)
    end_time = max(p['finish'] for p in phases)
    
    # Create timeline with 1-minute resolution
    resolution_seconds = 60
    total_duration = (end_time - start_time).total_seconds()
    num_points = int(total_duration / resolution_seconds) + 1
    
    timeline = []
    concurrency = []
    
    for i in range(num_points):
        current_time = start_time + timedelta(seconds=i * resolution_seconds)
        timeline.append(current_time)
        
        # Count how many phases are active at this time
        active = sum(1 for p in phases if p['start'] <= current_time <= p['finish'])
        concurrency.append(active)
    
    ax2.plot(timeline, concurrency, color='steelblue', linewidth=2)
    ax2.fill_between(timeline, concurrency, alpha=0.3, color='steelblue')
    ax2.set_xlabel('Time', fontsize=12)
    ax2.set_ylabel('Concurrent Phases', fontsize=12)
    ax2.set_title('Concurrency Over Time', fontsize=12, fontweight='bold')
    ax2.xaxis_date()
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax2.grid(True, alpha=0.3)
    ax2.set_ylim(bottom=0)
    
    # Add statistics annotation
    stats = calculate_utilization(phases)
    stats_text = f"Utilization: {stats['utilization_pct']:.1f}%\n"
    stats_text += f"Max Concurrent: {stats['max_concurrent']}\n"
    stats_text += f"Avg Concurrent: {stats['avg_concurrent']:.2f}"
    
    ax2.text(0.02, 0.98, stats_text, transform=ax2.transAxes,
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"\nGraphical Gantt chart saved to: {output_file}")
    
    return True

def simulate_concurrency_limit(phases, max_concurrent, debug=False):
    """Simulate timeline with a maximum concurrency limit.
    
    This simulates what would happen if the cluster could only run max_concurrent
    graphs simultaneously. Graphs that would exceed the limit are queued and delayed
    until a slot becomes available.
    
    Uses event-driven simulation to properly handle phase scheduling.
    
    Args:
        phases: List of phase dicts with 'start', 'finish', 'duration' keys
        max_concurrent: Maximum number of concurrent graphs allowed
        debug: Print debug information
    
    Returns:
        tuple: (simulated_phases, queue_stats)
            - simulated_phases: New list of phases with adjusted start/finish times
            - queue_stats: Dict with queuing statistics
    """
    if not phases or max_concurrent <= 0:
        return phases, {}
    
    # Sort phases by original start time (this is when they "arrive" and want to start)
    sorted_phases = sorted(phases, key=lambda p: (p['start'], p['finish']))
    
    # Track active running phases: list of (finish_time, phase_data) tuples
    active = []
    
    # Track phases waiting in queue: list of (arrival_time, phase_index, phase_data) tuples
    queue = []
    
    # Track simulation results (keyed by original index to maintain order)
    simulated = {}
    
    queue_stats = {
        'total_queued': 0,
        'total_queue_time': timedelta(0),
        'max_queue_length': 0,
        'queue_times': [],  # Individual queue times for statistics
    }
    
    # Create events: each phase generates an "arrival" event at its original start time
    events = []
    for i, phase in enumerate(sorted_phases):
        events.append((phase['start'], 'arrive', i, phase))
    
    # Sort events by time
    events.sort(key=lambda e: e[0])
    
    # Process events in chronological order
    event_idx = 0
    current_time = events[0][0] if events else None
    
    while event_idx < len(events) or active or queue:
        # Get next event time (either next arrival or next finish)
        next_event_time = events[event_idx][0] if event_idx < len(events) else None
        next_finish_time = min((finish for finish, _ in active), default=None)
        
        # Determine what happens next
        if next_finish_time and (not next_event_time or next_finish_time <= next_event_time):
            # A phase finishes
            current_time = next_finish_time
            
            # Remove finished phase(s)
            active = [(finish, data) for finish, data in active if finish > current_time]
            
            if debug:
                print(f"Time {current_time.strftime('%H:%M')}: Phase finished, {len(active)} active, {len(queue)} queued")
            
            # Try to start queued phases
            while queue and len(active) < max_concurrent:
                arrival_time, idx, phase = queue.pop(0)
                
                # Start this phase now
                actual_start = current_time
                actual_finish = actual_start + phase['duration']
                
                if debug:
                    print(f"Time {current_time.strftime('%H:%M')}: Starting queued {phase['wuid']} (queued from {arrival_time.strftime('%H:%M')})")
                
                # Record simulated phase
                simulated[idx] = {
                    **phase,
                    'start': actual_start,
                    'finish': actual_finish,
                    'original_start': phase['start'],
                    'queue_time': actual_start - arrival_time
                }
                
                # Track statistics
                queue_time_delta = actual_start - arrival_time
                queue_stats['total_queued'] += 1
                queue_stats['total_queue_time'] += queue_time_delta
                queue_stats['queue_times'].append(queue_time_delta.total_seconds())
                
                # Add to active
                active.append((actual_finish, simulated[idx]))
                active.sort(key=lambda x: x[0])
        
        elif next_event_time:
            # A new phase arrives
            current_time, event_type, idx, phase = events[event_idx]
            event_idx += 1
            
            if len(active) < max_concurrent:
                # Start immediately
                actual_start = current_time
                actual_finish = actual_start + phase['duration']
                
                if debug:
                    print(f"Time {current_time.strftime('%H:%M')}: {phase['wuid']} starts immediately")
                
                # Record simulated phase
                simulated[idx] = {
                    **phase,
                    'start': actual_start,
                    'finish': actual_finish,
                    'original_start': phase['start'],
                    'queue_time': timedelta(0)
                }
                
                # Add to active
                active.append((actual_finish, simulated[idx]))
                active.sort(key=lambda x: x[0])
            else:
                # Add to queue
                queue.append((current_time, idx, phase))
                queue_stats['max_queue_length'] = max(queue_stats['max_queue_length'], len(queue))
                
                if debug:
                    print(f"Time {current_time.strftime('%H:%M')}: {phase['wuid']} queued ({len(queue)} in queue)")
        else:
            break
    
    # Convert simulated dict back to list in original order
    simulated_list = [simulated[i] for i in range(len(sorted_phases))]
    
    return simulated_list, queue_stats

def calculate_instance_costs(simulated_phases, max_concurrent, vms_per_instance, cost_per_vm, 
                            spin_down_minutes, spin_up_cost):
    """Calculate costs considering instance lifecycle (spin-up, work, idle, spin-down).
    
    Args:
        simulated_phases: List of simulated phase dicts
        max_concurrent: Maximum concurrent instances
        vms_per_instance: Number of VMs per Thor instance
        cost_per_vm: Cost per VM per hour
        spin_down_minutes: Minutes instance stays up when idle
        spin_up_cost: Fixed cost in dollars per instance spin-up
    
    Returns:
        dict with cost breakdown
    """
    cost_per_instance_hour = vms_per_instance * cost_per_vm
    spin_down_time = timedelta(minutes=spin_down_minutes)
    
    # Build timeline of work activity
    events = []
    for phase in simulated_phases:
        events.append((phase['start'], 'start'))
        events.append((phase['finish'], 'end'))
    events.sort()
    
    # Track activity bursts (periods with at least 1 graph running)
    current_concurrent = 0
    activity_bursts = []  # (start, end) tuples
    burst_start = None
    
    for event_time, event_type in events:
        if event_type == 'start':
            if current_concurrent == 0:
                burst_start = event_time
            current_concurrent += 1
        else:
            current_concurrent -= 1
            if current_concurrent == 0:
                activity_bursts.append((burst_start, event_time))
    
    # Calculate billable segments (activity + spin-down timeout)
    billable_segments = []
    for start, end in activity_bursts:
        billable_end = end + spin_down_time
        billable_segments.append((start, billable_end))
    
    # Merge overlapping segments
    if billable_segments:
        merged = [billable_segments[0]]
        for current_start, current_end in billable_segments[1:]:
            last_start, last_end = merged[-1]
            if current_start <= last_end:
                merged[-1] = (last_start, max(last_end, current_end))
            else:
                merged.append((current_start, current_end))
        billable_segments = merged
    
    # Calculate instance-hours during billable segments
    total_instance_hours = 0
    work_instance_hours = 0
    idle_instance_hours = 0
    num_spinups = 0
    
    for seg_start, seg_end in billable_segments:
        # Count spin-ups at segment start
        # Assume we need to spin up instances at the beginning of each segment
        num_spinups += max_concurrent
        
        # Calculate work time within this segment
        segment_events = []
        for phase in simulated_phases:
            if phase['start'] < seg_end and phase['finish'] > seg_start:
                work_start = max(phase['start'], seg_start)
                work_end = min(phase['finish'], seg_end)
                segment_events.append((work_start, 'start'))
                segment_events.append((work_end, 'end'))
        
        segment_events.sort()
        
        # Calculate concurrent work time
        concurrent = 0
        last_time = seg_start
        
        for event_time, event_type in segment_events:
            if concurrent > 0:
                duration_hours = (event_time - last_time).total_seconds() / 3600
                work_instance_hours += concurrent * duration_hours
            
            if event_type == 'start':
                concurrent += 1
            else:
                concurrent -= 1
            
            last_time = event_time
        
        # Find actual work end time in this segment
        work_end_time = seg_start
        for phase in simulated_phases:
            if phase['start'] < seg_end and phase['finish'] > seg_start:
                work_end_time = max(work_end_time, min(phase['finish'], seg_end))
        
        # Idle time is from work end to segment end
        idle_duration = seg_end - work_end_time
        if idle_duration.total_seconds() > 0:
            # Keep instances running during idle timeout
            # Conservative: assume all max_concurrent instances stay up
            idle_hours = idle_duration.total_seconds() / 3600
            idle_instance_hours += max_concurrent * idle_hours
    
    total_instance_hours = work_instance_hours + idle_instance_hours
    spinup_total_cost = num_spinups * spin_up_cost
    
    return {
        'cost_per_instance_hour': cost_per_instance_hour,
        'total_instance_hours': total_instance_hours,
        'work_instance_hours': work_instance_hours,
        'idle_instance_hours': idle_instance_hours,
        'num_spinups': num_spinups,
        'spinup_cost_per_instance': spin_up_cost,
        'work_cost': work_instance_hours * cost_per_instance_hour,
        'idle_cost': idle_instance_hours * cost_per_instance_hour,
        'spinup_cost': spinup_total_cost,
        'total_cost': (total_instance_hours * cost_per_instance_hour) + spinup_total_cost,
        'num_segments': len(billable_segments)
    }

def print_simulation_comparison(original_stats, simulated_stats, max_concurrent, queue_stats,
                                simulated_phases=None, vms_per_instance=None, cost_per_vm=None,
                                spin_down_minutes=None, spin_up_time=None, spin_up_cost=None):
    """Print comparison between original and simulated timelines."""
    print("\n" + "="*70)
    print(f"CONCURRENCY SIMULATION: max_concurrent={max_concurrent}")
    print("="*70)
    
    print("\nQueueing Statistics:")
    print(f"  Graphs Queued:      {queue_stats['total_queued']} of {original_stats['total_phases']} ({queue_stats['total_queued']/original_stats['total_phases']*100:.1f}%)")
    if queue_stats['total_queued'] > 0:
        import statistics
        queue_times_seconds = queue_stats['queue_times']
        min_queue_time = timedelta(seconds=min(queue_times_seconds))
        avg_queue_time = timedelta(seconds=sum(queue_times_seconds) / len(queue_times_seconds))
        max_queue_time = timedelta(seconds=max(queue_times_seconds))
        stddev_queue_time = timedelta(seconds=statistics.stdev(queue_times_seconds)) if len(queue_times_seconds) > 1 else timedelta(0)
        
        print(f"  Queue Time Stats:")
        print(f"    Min:              {format_timedelta(min_queue_time)}")
        print(f"    Avg:              {format_timedelta(avg_queue_time)}")
        print(f"    Max:              {format_timedelta(max_queue_time)}")
        print(f"    StdDev:           {format_timedelta(stddev_queue_time)}")
    print(f"  Max Queue Length:   {queue_stats['max_queue_length']}")
    
    print("\nTime Period Comparison:")
    print(f"  Original Duration:  {format_timedelta(original_stats['total_period'])}")
    print(f"  Simulated Duration: {format_timedelta(simulated_stats['total_period'])}")
    
    period_increase = simulated_stats['total_period'] - original_stats['total_period']
    period_increase_pct = (period_increase.total_seconds() / original_stats['total_period'].total_seconds() * 100) if original_stats['total_period'].total_seconds() > 0 else 0
    print(f"  Increase:           {format_timedelta(period_increase)} (+{period_increase_pct:.1f}%)")
    
    print("\nUtilization Comparison:")
    print(f"  Original Util:      {original_stats['utilization_pct']:.2f}% ({format_timedelta(original_stats['utilized_time'])} / {format_timedelta(original_stats['total_period'])})")
    print(f"  Simulated Util:     {simulated_stats['utilization_pct']:.2f}% ({format_timedelta(simulated_stats['utilized_time'])} / {format_timedelta(simulated_stats['total_period'])})")
    
    util_change = simulated_stats['utilization_pct'] - original_stats['utilization_pct']
    print(f"  Change:             {util_change:+.2f} percentage points")
    
    print("\nConcurrency Comparison:")
    print(f"  Original Max:       {original_stats['max_concurrent']}")
    print(f"  Simulated Max:      {simulated_stats['max_concurrent']} (limited to {max_concurrent})")
    print(f"  Original Avg:       {original_stats['avg_concurrent']:.2f}")
    print(f"  Simulated Avg:      {simulated_stats['avg_concurrent']:.2f}")
    
    print("\nSimulated Concurrency Distribution:")
    sorted_histogram = sorted(simulated_stats['concurrent_histogram'].items())
    for count, time_seconds in sorted_histogram:
        pct = (time_seconds / simulated_stats['utilized_time'].total_seconds() * 100) if simulated_stats['utilized_time'].total_seconds() > 0 else 0
        print(f"  {count:2d} concurrent:      {format_timedelta(timedelta(seconds=time_seconds)):>15s} ({pct:5.2f}%)")
    
    # Add cost analysis if parameters provided
    if simulated_phases and vms_per_instance and cost_per_vm and spin_down_minutes is not None and spin_up_time is not None and spin_up_cost is not None:
        # Calculate costs for simulated scenario
        sim_costs = calculate_instance_costs(simulated_phases, max_concurrent, vms_per_instance, 
                                        cost_per_vm, spin_down_minutes, spin_up_cost)
        
        print("\n" + "="*70)
        print("COST ANALYSIS")
        print("="*70)
        print(f"\nConfiguration:")
        print(f"  VMs per instance:   {vms_per_instance}")
        print(f"  Cost per VM/hour:   ${cost_per_vm:.4f}")
        print(f"  Cost per inst/hour: ${sim_costs['cost_per_instance_hour']:.2f}")
        print(f"  Spin-up time:       {spin_up_time} minutes")
        print(f"  Spin-up cost:       ${spin_up_cost:.2f} per instance")
        print(f"  Spin-down timeout:  {spin_down_minutes} minutes")
        
        # Calculate simulated uptime
        sim_uptime_hours = sim_costs['total_instance_hours'] / max_concurrent if max_concurrent > 0 else 0
        
        print(f"\nSimulated (max_concurrent={max_concurrent}):")
        print(f"  Instance Uptime:    {sim_uptime_hours:>10.1f} hours (work + idle, instances running)")
        print(f"  Work time:          {sim_costs['work_instance_hours']:>10.1f} hours  ${sim_costs['work_cost']:>10,.2f}")
        print(f"  Idle time:          {sim_costs['idle_instance_hours']:>10.1f} hours  ${sim_costs['idle_cost']:>10,.2f}")
        print(f"  Spin-ups:           {sim_costs['num_spinups']:>10d} times  ${sim_costs['spinup_cost']:>10,.2f}")
        print(f"  {'─'*50}")
        print(f"  Total:              {sim_costs['total_instance_hours']:>10.1f} hours  ${sim_costs['total_cost']:>10,.2f}")
        print(f"  Activity Segments:  {sim_costs['num_segments']}")
        print(f"  Cost per Graph:     ${sim_costs['total_cost']/original_stats['total_phases']:.4f}")
        
        # Calculate original costs (using original max_concurrent from stats)
        orig_phases_for_cost = []
        for phase_list in simulated_phases:
            # Use original timings
            orig_phases_for_cost.append({
                'start': phase_list.get('original_start', phase_list['start']),
                'finish': phase_list.get('original_start', phase_list['start']) + phase_list['duration'],
                'duration': phase_list['duration']
            })
        
        orig_costs = calculate_instance_costs(orig_phases_for_cost, original_stats['max_concurrent'], 
                                             vms_per_instance, cost_per_vm, spin_down_minutes, spin_up_cost)
        
        # Calculate original uptime
        orig_uptime_hours = orig_costs['total_instance_hours'] / original_stats['max_concurrent'] if original_stats['max_concurrent'] > 0 else 0
        
        print(f"\nOriginal (max_concurrent={original_stats['max_concurrent']}):")
        print(f"  Instance Uptime:    {orig_uptime_hours:>10.1f} hours (work + idle, instances running)")
        print(f"  Work time:          {orig_costs['work_instance_hours']:>10.1f} hours  ${orig_costs['work_cost']:>10,.2f}")
        print(f"  Idle time:          {orig_costs['idle_instance_hours']:>10.1f} hours  ${orig_costs['idle_cost']:>10,.2f}")
        print(f"  Spin-ups:           {orig_costs['num_spinups']:>10d} times  ${orig_costs['spinup_cost']:>10,.2f}")
        print(f"  {'─'*50}")
        print(f"  Total:              {orig_costs['total_instance_hours']:>10.1f} hours  ${orig_costs['total_cost']:>10,.2f}")
        print(f"  Activity Segments:  {orig_costs['num_segments']}")
        print(f"  Cost per Graph:     ${orig_costs['total_cost']/original_stats['total_phases']:.4f}")
        
        # Show cost difference
        cost_diff = sim_costs['total_cost'] - orig_costs['total_cost']
        cost_diff_pct = (cost_diff / orig_costs['total_cost'] * 100) if orig_costs['total_cost'] > 0 else 0
        
        print(f"\nCost Comparison:")
        if cost_diff < 0:
            print(f"  Savings:            ${abs(cost_diff):>10,.2f} ({abs(cost_diff_pct):.1f}% reduction)")
        elif cost_diff > 0:
            print(f"  Additional Cost:    ${cost_diff:>10,.2f} ({cost_diff_pct:.1f}% increase)")
        else:
            print(f"  No cost difference")

def run_simulation_tests():
    """Run unit tests for concurrency simulation with known test cases."""
    print("\n" + "="*70)
    print("RUNNING CONCURRENCY SIMULATION TESTS")
    print("="*70)
    
    # Test Case 1: Simple sequential execution becomes parallel
    # 3 graphs, each 10 minutes, starting at same time
    # With unlimited concurrency: all run in parallel, finish at T+10
    # With max_concurrent=2: first 2 run 0-10, third runs 10-20
    base_time = datetime(2025, 1, 1, 0, 0, 0)
    test1_phases = [
        {'wuid': 'W1', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time, 'finish': base_time + timedelta(minutes=10),
         'duration': timedelta(minutes=10)},
        {'wuid': 'W2', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time, 'finish': base_time + timedelta(minutes=10),
         'duration': timedelta(minutes=10)},
        {'wuid': 'W3', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time, 'finish': base_time + timedelta(minutes=10),
         'duration': timedelta(minutes=10)},
    ]
    
    print("\nTest 1: Three concurrent 10-minute graphs limited to max_concurrent=2")
    print("Expected: First two run 0-10min, third queued and runs 10-20min")
    print("Expected total period: 20 minutes (vs original 10 minutes)")
    
    # Test Case 2: Cascading delays
    # Graph A: 0-20min, Graph B: 0-15min, Graph C: 0-10min, Graph D: 5-25min
    # With max_concurrent=2, D must wait for C to finish at 10min
    test2_phases = [
        {'wuid': 'W1', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time, 'finish': base_time + timedelta(minutes=20),
         'duration': timedelta(minutes=20)},
        {'wuid': 'W2', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time, 'finish': base_time + timedelta(minutes=15),
         'duration': timedelta(minutes=15)},
        {'wuid': 'W3', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time, 'finish': base_time + timedelta(minutes=10),
         'duration': timedelta(minutes=10)},
        {'wuid': 'W4', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=5), 
         'finish': base_time + timedelta(minutes=25),
         'duration': timedelta(minutes=20)},
    ]
    
    print("\nTest 2: Four overlapping graphs with max_concurrent=2")
    print("Original: A(0-20), B(0-15), C(0-10), D(5-25) - max 3 concurrent at T=5")
    print("Expected with limit=2:")
    print("  - A and B start at 0 (first 2)")
    print("  - D queued at 5, starts when B finishes at 15, runs 15-35")
    print("  - C queued at 0, starts when A finishes at 20, runs 20-30")
    print("Expected total period: 35 minutes (vs original 25 minutes)")
    
    # Test Case 3: Complex overlapping with varying durations
    # 10 graphs with staggered starts and different durations, limit to 3
    test3_phases = [
        {'wuid': 'W1', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time, 'finish': base_time + timedelta(minutes=30),
         'duration': timedelta(minutes=30)},
        {'wuid': 'W2', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=5), 
         'finish': base_time + timedelta(minutes=20),
         'duration': timedelta(minutes=15)},
        {'wuid': 'W3', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=10), 
         'finish': base_time + timedelta(minutes=40),
         'duration': timedelta(minutes=30)},
        {'wuid': 'W4', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=15), 
         'finish': base_time + timedelta(minutes=25),
         'duration': timedelta(minutes=10)},
        {'wuid': 'W5', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=20), 
         'finish': base_time + timedelta(minutes=35),
         'duration': timedelta(minutes=15)},
        {'wuid': 'W6', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=25), 
         'finish': base_time + timedelta(minutes=50),
         'duration': timedelta(minutes=25)},
        {'wuid': 'W7', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=30), 
         'finish': base_time + timedelta(minutes=45),
         'duration': timedelta(minutes=15)},
        {'wuid': 'W8', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=35), 
         'finish': base_time + timedelta(minutes=55),
         'duration': timedelta(minutes=20)},
        {'wuid': 'W9', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=40), 
         'finish': base_time + timedelta(minutes=50),
         'duration': timedelta(minutes=10)},
        {'wuid': 'W10', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=45), 
         'finish': base_time + timedelta(minutes=60),
         'duration': timedelta(minutes=15)},
    ]
    
    print("\nTest 3: Complex overlapping - 10 graphs with max_concurrent=3")
    print("Original: Multiple overlaps, max 5 concurrent at T=35-40")
    print("Expected with limit=3:")
    print("  - W1(0-30), W2(5-20), W3(10-40) start immediately")
    print("  - W4 queued at 15, waits for W2@20, runs 20-30")
    print("  - W5 queued at 20, waits for W4@30, runs 30-45")
    print("  - W6 queued at 25, waits for W1@30, runs 30-55")
    print("  - W7 queued at 30, waits for W3@40, runs 40-55")
    print("  - W8 queued at 35, waits for W5@45, runs 45-65")
    print("  - W9 queued at 40, waits for W7@55, runs 55-65")
    print("  - W10 queued at 45, waits for W6@55, runs 55-70")
    print("Expected: ~7 graphs queued, duration extends to ~70 minutes")
    
    # Test Case 4: Already under limit - no change
    test4_phases = [
        {'wuid': 'W1', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time, 'finish': base_time + timedelta(minutes=10),
         'duration': timedelta(minutes=10)},
        {'wuid': 'W2', 'cluster': 'test', 'phase': 'graph1', 'graph': 'graph1',
         'start': base_time + timedelta(minutes=15), 
         'finish': base_time + timedelta(minutes=25),
         'duration': timedelta(minutes=10)},
    ]
    
    print("\nTest 4: Two sequential graphs with max_concurrent=3")
    print("Expected: No change (never exceeds limit)")
    print("Expected total period: 25 minutes (same as original)")
    
    # Store test cases for validation after implementation
    test_cases = {
        'test1': {
            'phases': test1_phases,
            'max_concurrent': 2,
            'expected_duration_minutes': 20,
            'expected_queued_count': 1,
            'description': 'Three concurrent graphs'
        },
        'test2': {
            'phases': test2_phases,
            'max_concurrent': 2,
            'expected_duration_minutes': 35,
            'expected_queued_count': 2,
            'description': 'Four overlapping graphs with cascading delays'
        },
        'test3': {
            'phases': test3_phases,
            'max_concurrent': 3,
            'expected_duration_minutes': 70,
            'expected_queued_count': 7,
            'expected_max_concurrent': 3,
            'description': 'Complex overlapping with 10 graphs'
        },
        'test4': {
            'phases': test4_phases,
            'max_concurrent': 3,
            'expected_duration_minutes': 25,
            'expected_queued_count': 0,
            'expected_max_concurrent': 1,
            'description': 'Already under limit'
        }
    }
    
    return test_cases

def main():
    parser = argparse.ArgumentParser(
        description='Analyze Thor workunit timeline data',
        usage='%(prog)s <timeline.csv> [options]',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('csv_file',
                       metavar='timeline.csv',
                       help='CSV file from get_thor_timeline.py')
    parser.add_argument('--gantt',
                       action='store_true',
                       help='Generate text-based Gantt chart visualization')
    parser.add_argument('--graph',
                       metavar='<output.png>',
                       help='Generate graphical Gantt chart and save to file (requires matplotlib)')
    parser.add_argument('--resolution',
                       type=int,
                       default=5,
                       metavar='<minutes>',
                       help='Gantt chart time resolution in minutes (default: 5)')
    parser.add_argument('--width',
                       type=int,
                       default=100,
                       metavar='<chars>',
                       help='Maximum Gantt chart width in characters (default: 100)')
    parser.add_argument('--percentile-analysis',
                       action='store_true',
                       help='Show percentile distribution of graph execution times')
    parser.add_argument('--list-percentile',
                       type=str,
                       metavar='<range>',
                       help='List graphs in specified percentile range (e.g., "99-100", "0-50", "95-99")')
    parser.add_argument('--simulate-max-concurrent',
                       type=int,
                       metavar='<N>',
                       help='Simulate timeline with maximum N concurrent graphs and compare results')
    parser.add_argument('--vms-per-instance',
                       type=int,
                       default=25,
                       metavar='<N>',
                       help='Number of VMs per Thor instance (default: 25)')
    parser.add_argument('--cost-per-vm',
                       type=float,
                       default=0.3936,
                       metavar='<$>',
                       help='Cost per VM per hour in dollars (default: 0.3936)')
    parser.add_argument('--spin-down-time',
                       type=int,
                       default=10,
                       metavar='<minutes>',
                       help='Minutes an idle instance stays up before spinning down (default: 10)')
    parser.add_argument('--format',
                       choices=['text', 'csv'],
                       default='text',
                       help='Output format: text (human-readable, default) or csv (summary metrics)')
    parser.add_argument('--spin-up-time',
                       type=int,
                       default=1,
                       metavar='<minutes>',
                       help='Minutes to provision a new instance (default: 1)')
    parser.add_argument('--spin-up-cost',
                       type=float,
                       default=1.0,
                       metavar='<dollars>',
                       help='Cost in dollars per instance spin-up (default: 1.0)')
    parser.add_argument('--test-simulation',
                       action='store_true',
                       help='Run simulation unit tests with known test cases')
    
    args = parser.parse_args()
    
    # Run simulation tests if requested
    if args.test_simulation:
        test_cases = run_simulation_tests()
        
        print("\n" + "="*70)
        print("VALIDATING SIMULATION IMPLEMENTATION")
        print("="*70)
        
        all_passed = True
        for name, test in test_cases.items():
            print(f"\nRunning {name}: {test['description']}")
            
            # Run simulation
            simulated, queue_stats = simulate_concurrency_limit(
                test['phases'], 
                test['max_concurrent'],
                debug=False
            )
            
            # Calculate statistics
            orig_stats = calculate_utilization(test['phases'])
            sim_stats = calculate_utilization(simulated)
            
            # Validate expectations
            actual_duration_minutes = int(sim_stats['total_period'].total_seconds() / 60)
            expected_duration_minutes = test['expected_duration_minutes']
            
            duration_passed = actual_duration_minutes == expected_duration_minutes
            queued_passed = queue_stats['total_queued'] == test['expected_queued_count']
            
            # Check max concurrent if specified
            max_concurrent_passed = True
            if 'expected_max_concurrent' in test:
                expected_max = test['expected_max_concurrent']
                actual_max = sim_stats['max_concurrent']
                max_concurrent_passed = actual_max == expected_max
                print(f"  Max Concurrent: {actual_max} (expected {expected_max}) {'✓ PASS' if max_concurrent_passed else '✗ FAIL'}")
            
            print(f"  Duration: {actual_duration_minutes} min (expected {expected_duration_minutes} min) {'✓ PASS' if duration_passed else '✗ FAIL'}")
            print(f"  Queued:   {queue_stats['total_queued']} graphs (expected {test['expected_queued_count']} graphs) {'✓ PASS' if queued_passed else '✗ FAIL'}")
            
            if not (duration_passed and queued_passed and max_concurrent_passed):
                all_passed = False
                print(f"  Details: Original period={orig_stats['total_period']}, Simulated period={sim_stats['total_period']}, Max concurrent={sim_stats['max_concurrent']}")
        
        print("\n" + "="*70)
        if all_passed:
            print("ALL TESTS PASSED ✓")
        else:
            print("SOME TESTS FAILED ✗")
        print("="*70)
        
        return 0 if all_passed else 1
    
    # Read timeline data
    try:
        phases = read_timeline_csv(args.csv_file)
    except FileNotFoundError:
        print(f"Error: File '{args.csv_file}' not found", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error reading CSV file: {e}", file=sys.stderr)
        return 1
    
    if not phases:
        print("No valid timeline data found in CSV file", file=sys.stderr)
        return 1
    
    # Calculate statistics
    stats = calculate_utilization(phases)
    
    # Calculate original costs
    original_costs = calculate_instance_costs(
        phases, 
        stats['max_concurrent'],
        args.vms_per_instance,
        args.cost_per_vm,
        args.spin_down_time,
        args.spin_up_cost
    )
    
    # Handle CSV output format
    if args.format == 'csv':
        if args.simulate_max_concurrent:
            # Run simulation first
            simulated_phases, queue_stats = simulate_concurrency_limit(
                phases, 
                args.simulate_max_concurrent,
                debug=False
            )
            sim_stats = calculate_utilization(simulated_phases)
            sim_costs = calculate_instance_costs(
                simulated_phases,
                args.simulate_max_concurrent,
                args.vms_per_instance,
                args.cost_per_vm,
                args.spin_down_time,
                args.spin_up_cost
            )
            format_summary_csv(stats, original_costs, args.csv_file, args, sim_stats, sim_costs)
        else:
            format_summary_csv(stats, original_costs, args.csv_file, args)
        return 0
    
    # Text format output (original behavior)
    # Print statistics
    print_statistics(stats)
    
    print("\n" + "="*70)
    print("COST ANALYSIS (ORIGINAL CONFIGURATION)")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  Max Concurrent:     {stats['max_concurrent']}")
    print(f"  VMs per Instance:   {args.vms_per_instance}")
    print(f"  Cost per VM:        ${args.cost_per_vm:.4f}/hour")
    print(f"  Spin-down Time:     {args.spin_down_time} minutes")
    print(f"  Spin-up Time:       {args.spin_up_time} minutes")
    print(f"  Spin-up Cost:       ${args.spin_up_cost:.2f} per instance")
    
    # Calculate total uptime (time instances were running, excluding spin-down gaps)
    total_uptime_hours = original_costs['total_instance_hours'] / stats['max_concurrent'] if stats['max_concurrent'] > 0 else 0
    
    print(f"\nInstance Uptime:")
    print(f"  Total Uptime:       {total_uptime_hours:>10.1f} hours (instances running, work + idle)")
    print(f"  Total Period:       {stats['total_period'].total_seconds() / 3600:>10.1f} hours (elapsed wall-clock time)")
    print(f"  Uptime Ratio:       {(total_uptime_hours / (stats['total_period'].total_seconds() / 3600) * 100):>10.1f}% (uptime / total period)")
    
    print(f"\nCost Breakdown:")
    print(f"  Work Instance Hrs:  {original_costs['work_instance_hours']:>10.1f} hours  ${original_costs['work_cost']:>10,.2f}")
    print(f"  Idle Instance Hrs:  {original_costs['idle_instance_hours']:>10.1f} hours  ${original_costs['idle_cost']:>10,.2f}")
    print(f"  Spin-ups:           {original_costs['num_spinups']:>10d} times  ${original_costs['spinup_cost']:>10,.2f}")
    print(f"  {'─'*50}")
    print(f"  Total:              {original_costs['total_instance_hours']:>10.1f} hours  ${original_costs['total_cost']:>10,.2f}")
    print(f"  Activity Segments:  {original_costs['num_segments']}")
    print(f"  Cost per Graph:     ${original_costs['total_cost']/stats['total_phases']:.4f}")
    print("="*70)
    
    # Print percentile analysis if requested
    if args.percentile_analysis:
        percentiles, buckets, bucket_stats = calculate_percentile_distribution(
            phases,
            max_concurrent=stats['max_concurrent'],
            vms_per_instance=args.vms_per_instance,
            cost_per_vm=args.cost_per_vm,
            spin_down_minutes=args.spin_down_time,
            spin_up_cost=args.spin_up_cost
        )
        print_percentile_distribution(percentiles, buckets, len(phases), bucket_stats)
    
    # List graphs in specific percentile if requested
    if args.list_percentile:
        if not args.percentile_analysis:
            # Need to calculate first
            percentiles, buckets, bucket_stats = calculate_percentile_distribution(
                phases,
                max_concurrent=stats['max_concurrent'],
                vms_per_instance=args.vms_per_instance,
                cost_per_vm=args.cost_per_vm,
                spin_down_minutes=args.spin_down_time,
                spin_up_cost=args.spin_up_cost
            )
        list_graphs_in_percentile(buckets, args.list_percentile)
    
    # Generate Gantt chart if requested
    if args.gantt:
        generate_gantt_chart(phases, resolution_minutes=args.resolution, max_width=args.width)
    
    # Generate graphical Gantt chart if requested
    if args.graph:
        generate_graphical_gantt(phases, output_file=args.graph)
    
    # Run concurrency simulation if requested
    if args.simulate_max_concurrent:
        if args.simulate_max_concurrent <= 0:
            print(f"\nError: --simulate-max-concurrent must be positive", file=sys.stderr)
            return 1
        
        print(f"\nSimulating with max_concurrent={args.simulate_max_concurrent}...", file=sys.stderr)
        simulated_phases, queue_stats = simulate_concurrency_limit(
            phases, 
            args.simulate_max_concurrent,
            debug=False
        )
        
        # Calculate simulated statistics
        sim_stats = calculate_utilization(simulated_phases)
        
        # Print comparison with cost analysis
        print_simulation_comparison(stats, sim_stats, args.simulate_max_concurrent, queue_stats,
                                   simulated_phases=simulated_phases,
                                   vms_per_instance=args.vms_per_instance,
                                   cost_per_vm=args.cost_per_vm,
                                   spin_down_minutes=args.spin_down_time,
                                   spin_up_time=args.spin_up_time,
                                   spin_up_cost=args.spin_up_cost)
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
