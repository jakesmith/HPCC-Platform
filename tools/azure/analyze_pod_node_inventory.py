#!/usr/bin/env python3
"""
Analyze Kubernetes pod and node inventory from get_pod_node_inventory.py output.

Identifies HPCC components from pod names, cross-references pods to nodes, and
calculates resource consumption per component over time.

Input:
    CSV output from get_pod_node_inventory.py (via stdin or file)
    Format: TimeGenerated,RecordType,Name,Namespace,PodStatus,Computer,...

Usage:
    get_pod_node_inventory.py ... | python3 analyze_pod_node_inventory.py [options]
    python3 analyze_pod_node_inventory.py inventory.csv [options]

Examples:
    # Basic analysis
    cat inventory.csv | python3 analyze_pod_node_inventory.py

    # Text report
    python3 analyze_pod_node_inventory.py inventory.csv --format text

    # Filter by time range
    python3 analyze_pod_node_inventory.py inventory.csv --start-time "2025-11-04 12:00" \
        --end-time "2025-11-04 18:00"

    # Component-level analysis
    python3 analyze_pod_node_inventory.py inventory.csv --format text --by-component
"""

import sys
import argparse
import csv
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Optional


def parse_datetime(dt_str: str) -> datetime:
    """Parse datetime string in multiple formats."""
    formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%SZ',
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


def extract_thor_cluster_name(parts: List[str]) -> str:
    """Extract Thor cluster name from pod name parts.
    
    Args:
        parts: Pod name split by '-'
    
    Returns:
        Cluster name or empty string if not found
    """
    try:
        thor_idx = parts.index('thor')
        # Check if next part is cluster name (not thormanager/thorworker/manager/worker)
        if thor_idx + 1 < len(parts):
            next_part = parts[thor_idx + 1]
            if next_part not in ('thormanager', 'thorworker', 'manager', 'worker'):
                return next_part
    except (ValueError, IndexError):
        pass
    return ''


def identify_component(pod_name: str) -> str:
    """Identify HPCC component from pod name.
    
    HPCC pod naming conventions:
    - <namespace>-dali-<instance>
    - <namespace>-esp-<instance>
    - <namespace>-eclccserver-<instance>
    - <namespace>-roxie-<cluster>-<instance>
    - <namespace>-thor-<cluster>-thormanager-<instance>
    - <namespace>-thor-<cluster>-thorworker-<instance>
    - <namespace>-sasha-<instance>
    - etc.
    
    Returns component name or "Other" if not recognized
    """
    parts = pod_name.split('-')
    
    # Handle common HPCC components
    if len(parts) >= 2:
        # Check for standard components (dali, esp, sasha, etc.)
        if 'dali' in parts:
            return 'dali'
        elif 'esp' in parts:
            return 'esp'
        elif 'eclccserver' in parts:
            return 'eclccserver'
        elif 'sasha' in parts:
            return 'sasha'
        elif 'dfuserver' in parts:
            return 'dfuserver'
        elif 'eclagent' in parts:
            return 'eclagent'
        
        # Thor has more complex naming
        elif 'thor' in parts:
            cluster_name = extract_thor_cluster_name(parts)
            if 'thormanager' in parts or 'manager' in parts:
                return f'thor-{cluster_name}' if cluster_name else 'thor'
            elif 'thorworker' in parts or 'worker' in parts:
                return f'thor-{cluster_name}-worker' if cluster_name else 'thor-worker'
            else:
                return 'thor'
        
        # Roxie
        elif 'roxie' in parts:
            # Extract cluster name if available
            roxie_idx = parts.index('roxie')
            if roxie_idx + 1 < len(parts):
                cluster_name = parts[roxie_idx + 1]
                return f'roxie-{cluster_name}'
            return 'roxie'
    
    # If not recognized, return "Other"
    return 'Other'


def read_inventory(input_source: Optional[str], start_time: Optional[datetime], 
                   end_time: Optional[datetime]) -> tuple:
    """Read and filter pod and node inventory records.
    
    Returns: (pods, nodes) where:
        pods: List of pod records (dicts)
        nodes: List of node records (dicts)
    """
    pods = []
    nodes = []
    
    # Open input source with proper error handling
    try:
        if input_source:
            with open(input_source, 'r') as f:
                lines = [line for line in f if not line.strip().startswith('#')]
        else:
            lines = [line for line in sys.stdin if not line.strip().startswith('#')]
    except IOError as e:
        print(f"Error reading input: {e}", file=sys.stderr)
        return ([], [])
    
    # Parse CSV
    reader = csv.DictReader(lines)
    for row in reader:
        # Parse timestamp
        try:
            timestamp = parse_datetime(row.get('TimeGenerated', ''))
        except ValueError:
            continue  # Skip rows with invalid timestamps
        
        # Apply time filters
        if start_time and timestamp < start_time:
            continue
        if end_time and timestamp >= end_time:
            continue
        
        # Add timestamp to record
        row['_timestamp'] = timestamp
        
        # Separate pods and nodes
        record_type = row.get('RecordType', '')
        if record_type == 'Pod':
            pods.append(row)
        elif record_type == 'Node':
            nodes.append(row)
    
    return pods, nodes


def analyze_component_usage(pods: List[Dict], nodes: List[Dict]) -> Dict:
    """Analyze component resource usage.
    
    Returns dict with:
        - component_pods: {component: [pod_records]}
        - component_nodes: {component: set(node_names)}
        - pod_to_node: {pod_name: node_name}
        - node_info: {node_name: node_record}
    """
    # Build pod to node mapping
    pod_to_node = {}
    for pod in pods:
        pod_name = pod.get('Name', '')
        node_name = pod.get('Computer', '')
        if pod_name and node_name:
            pod_to_node[pod_name] = node_name
    
    # Build node info mapping
    node_info = {}
    for node in nodes:
        node_name = node.get('Name', '')
        if node_name:
            node_info[node_name] = node
    
    # Group pods by component
    component_pods = defaultdict(list)
    for pod in pods:
        pod_name = pod.get('Name', '')
        component = identify_component(pod_name)
        component_pods[component].append(pod)
    
    # Build component to nodes mapping
    component_nodes = defaultdict(set)
    for component, pod_list in component_pods.items():
        for pod in pod_list:
            pod_name = pod.get('Name', '')
            node_name = pod_to_node.get(pod_name)
            if node_name:
                component_nodes[component].add(node_name)
    
    return {
        'component_pods': dict(component_pods),
        'component_nodes': dict(component_nodes),
        'pod_to_node': pod_to_node,
        'node_info': node_info
    }


def estimate_durations_from_snapshots(pods: List[Dict], start_time: datetime, end_time: datetime) -> Dict:
    """Estimate how long each component consumed resources based on snapshot data.
    
    IMPORTANT: This function assumes all pods were running for the entire time window.
    This is a simplification based on snapshot data from KubePodInventory. For more
    accurate duration tracking, time-series data would be needed to track pod lifecycle
    events (start/stop times).
    
    Returns dict with:
        - component_duration: {component: duration_hours}
        - component_pod_hours: {component: pod_hours}
    """
    component_duration = defaultdict(float)
    component_pod_hours = defaultdict(float)
    
    # Total time window
    total_hours = (end_time - start_time).total_seconds() / 3600.0
    
    # Calculate pod hours for each component
    component_pod_count = defaultdict(int)
    for pod in pods:
        pod_name = pod.get('Name', '')
        component = identify_component(pod_name)
        component_pod_count[component] += 1
    
    # Assume all pods were running for the entire time window
    for component, count in component_pod_count.items():
        component_pod_hours[component] = count * total_hours
        component_duration[component] = total_hours
    
    return {
        'component_duration': dict(component_duration),
        'component_pod_hours': dict(component_pod_hours)
    }


def output_csv(analysis: Dict, pods: List[Dict], nodes: List[Dict], 
               start_time: datetime, end_time: datetime, args) -> None:
    """Output analysis results in CSV format."""
    # Metadata header
    print("# Generated by: analyze_pod_node_inventory.py")
    print(f"# Date generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Time range: {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Total pods: {len(pods)}")
    print(f"# Total nodes: {len(nodes)}")
    print("#")
    
    component_pods = analysis['component_pods']
    component_nodes = analysis['component_nodes']
    
    # Calculate estimated durations from snapshot data
    duration_info = estimate_durations_from_snapshots(pods, start_time, end_time)
    component_duration = duration_info['component_duration']
    component_pod_hours = duration_info['component_pod_hours']
    
    # Output CSV
    print("Component,PodCount,NodeCount,DurationHours,PodHours")
    
    for component in sorted(component_pods.keys()):
        pod_count = len(component_pods[component])
        node_count = len(component_nodes.get(component, set()))
        duration = component_duration.get(component, 0.0)
        pod_hours = component_pod_hours.get(component, 0.0)
        
        print(f"{component},{pod_count},{node_count},{duration:.2f},{pod_hours:.2f}")


def output_text(analysis: Dict, pods: List[Dict], nodes: List[Dict], 
                start_time: datetime, end_time: datetime, args) -> None:
    """Output analysis results in human-readable text format."""
    print("=" * 80)
    print("POD AND NODE INVENTORY ANALYSIS")
    print("=" * 80)
    print()
    
    # Summary
    print("SUMMARY")
    print("-" * 80)
    print(f"Time Range:       {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    total_hours = (end_time - start_time).total_seconds() / 3600.0
    print(f"Duration:         {total_hours:.2f} hours")
    print(f"Total Pods:       {len(pods)}")
    print(f"Total Nodes:      {len(nodes)}")
    print()
    
    component_pods = analysis['component_pods']
    component_nodes = analysis['component_nodes']
    pod_to_node = analysis['pod_to_node']
    
    # Calculate estimated durations from snapshot data
    duration_info = estimate_durations_from_snapshots(pods, start_time, end_time)
    component_duration = duration_info['component_duration']
    component_pod_hours = duration_info['component_pod_hours']
    
    # Component breakdown
    print("COMPONENT BREAKDOWN")
    print("-" * 80)
    print(f"{'Component':<30s}  {'Pods':>8s}  {'Nodes':>8s}  {'Duration':>12s}  {'Pod-Hours':>12s}")
    print("-" * 80)
    
    for component in sorted(component_pods.keys()):
        pod_count = len(component_pods[component])
        node_count = len(component_nodes.get(component, set()))
        duration = component_duration.get(component, 0.0)
        pod_hours = component_pod_hours.get(component, 0.0)
        
        print(f"{component:<30s}  {pod_count:>8d}  {node_count:>8d}  {duration:>10.2f}h  {pod_hours:>10.2f}h")
    
    print()
    
    # By-component detailed view
    if args.by_component:
        print("DETAILED COMPONENT ANALYSIS")
        print("-" * 80)
        
        for component in sorted(component_pods.keys()):
            print(f"\n{component}:")
            print(f"  Pods: {len(component_pods[component])}")
            print(f"  Nodes: {len(component_nodes.get(component, set()))}")
            
            # List pods
            if len(component_pods[component]) <= 20:
                print("  Pod List:")
                for pod in sorted(component_pods[component], key=lambda p: p.get('Name', '')):
                    pod_name = pod.get('Name', '')
                    node_name = pod_to_node.get(pod_name, 'Unknown')
                    status = pod.get('PodStatus', '')
                    print(f"    - {pod_name:<50s} -> {node_name:<30s} [{status}]")
            else:
                print(f"  Pod List: (showing first 20 of {len(component_pods[component])})")
                for pod in sorted(component_pods[component], key=lambda p: p.get('Name', ''))[:20]:
                    pod_name = pod.get('Name', '')
                    node_name = pod_to_node.get(pod_name, 'Unknown')
                    status = pod.get('PodStatus', '')
                    print(f"    - {pod_name:<50s} -> {node_name:<30s} [{status}]")
            
            # List nodes
            nodes_for_component = component_nodes.get(component, set())
            if nodes_for_component:
                print(f"  Nodes used: {', '.join(sorted(nodes_for_component))}")
        
        print()
    
    # Node utilization
    print("NODE UTILIZATION")
    print("-" * 80)
    
    # Count pods per node
    node_pod_count = defaultdict(int)
    for pod_name, node_name in pod_to_node.items():
        node_pod_count[node_name] += 1
    
    print(f"{'Node Name':<50s}  {'Pod Count':>10s}")
    print("-" * 80)
    
    for node_name in sorted(node_pod_count.keys()):
        count = node_pod_count[node_name]
        print(f"{node_name:<50s}  {count:>10d}")
    
    print()
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze Kubernetes pod and node inventory',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Basic CSV analysis
  cat inventory.csv | %(prog)s
  
  # Text report
  %(prog)s inventory.csv --format text
  
  # Detailed component analysis
  %(prog)s inventory.csv --format text --by-component
  
  # Time-filtered analysis
  %(prog)s inventory.csv --start-time "2025-11-04 12:00" --end-time "2025-11-04 18:00"
        '''
    )
    
    parser.add_argument(
        'input',
        nargs='?',
        help='Input CSV file from get_pod_node_inventory.py (or read from stdin)'
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
        '--format',
        choices=['csv', 'text'],
        default='csv',
        help='Output format (default: csv)'
    )
    
    parser.add_argument(
        '--by-component',
        action='store_true',
        help='Show detailed breakdown by component (text format only)'
    )
    
    args = parser.parse_args()
    
    # Parse time filters
    start_time = parse_datetime(args.start_time) if args.start_time else None
    end_time = parse_datetime(args.end_time) if args.end_time else None
    
    # Read inventory
    pods, nodes = read_inventory(args.input, start_time, end_time)
    
    if not pods and not nodes:
        print("No records found matching filters.", file=sys.stderr)
        return 1
    
    # Determine time range from data if not specified
    if pods:
        pod_times = [p['_timestamp'] for p in pods if '_timestamp' in p]
        if pod_times and not start_time:
            start_time = min(pod_times)
        if pod_times and not end_time:
            end_time = max(pod_times)
    
    if not start_time or not end_time:
        print("Error: Could not determine time range. Please specify --start-time and --end-time", file=sys.stderr)
        return 1
    
    # Analyze
    analysis = analyze_component_usage(pods, nodes)
    
    # Output
    if args.format == 'csv':
        output_csv(analysis, pods, nodes, start_time, end_time, args)
        return 0
    else:
        output_text(analysis, pods, nodes, start_time, end_time, args)
        return 0


if __name__ == '__main__':
    sys.exit(main())
