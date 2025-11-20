#!/usr/bin/env python3
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

"""
Azure Log Analytics Analyze Tool

This tool analyzes the CSV output from azure_log_analytics_fetch.py and categorizes
pods into HPCC components based on their naming patterns. It creates a time-series
analysis showing which components were running on which nodes over time, enabling
visualization as Gantt charts and cost analysis.

HPCC Component Categories (derived from helm/hpcc chart):
- dali: Distributed Array Logical Index
- thor: Thor cluster (manager, workers)
- roxie: Roxie cluster (server, agent)
- esp: Enterprise Services Platform (eclwatch, eclservices, etc.)
- eclagent: ECL Agent
- eclccserver: ECL CC Server
- eclscheduler: ECL Scheduler
- dfuserver: Distributed File Utility Server
- sasha: Storage Archive Service
- dafilesrv: Dali File Server
"""

import argparse
import sys
import csv
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional
from collections import defaultdict
import re


# HPCC component patterns based on helm chart naming conventions
HPCC_COMPONENT_PATTERNS = {
    'dali': [r'.*-dali(-.*)?$', r'^dali(-.*)?$'],
    'thor': [r'.*-thor(-.*)?$', r'^thor(-.*)?$', r'.*-thoragent(-.*)?$', r'.*-thormanager(-.*)?$', r'.*-thorworker(-.*)?$'],
    'roxie': [r'.*-roxie(-.*)?$', r'^roxie(-.*)?$', r'.*-roxieagent(-.*)?$', r'.*-roxieserver(-.*)?$', r'.*-toposerver(-.*)?$'],
    'esp': [r'.*-esp(-.*)?$', r'^esp(-.*)?$', r'.*-eclwatch(-.*)?$', r'.*-eclservices(-.*)?$', r'.*-eclqueries(-.*)?$', r'.*-esdl-sandbox(-.*)?$'],
    'eclagent': [r'.*-eclagent(-.*)?$', r'^eclagent(-.*)?$'],
    'eclccserver': [r'.*-eclccserver(-.*)?$', r'^eclccserver(-.*)?$'],
    'eclscheduler': [r'.*-eclscheduler(-.*)?$', r'^eclscheduler(-.*)?$'],
    'dfuserver': [r'.*-dfuserver(-.*)?$', r'^dfuserver(-.*)?$'],
    'sasha': [r'.*-sasha(-.*)?$', r'^sasha(-.*)?$'],
    'dafilesrv': [r'.*-dafilesrv(-.*)?$', r'^dafilesrv(-.*)?$'],
}

# Common non-HPCC system pods
SYSTEM_COMPONENT_PATTERNS = {
    'azure-system': [r'^aks-.*', r'.*-azuremonitor(-.*)?$', r'^omsagent(-.*)?$', r'.*-omsagent(-.*)?$', r'.*-azuredisk(-.*)?$', r'.*-azurefile(-.*)?$'],
    'monitoring': [r'^prometheus(-.*)?$', r'.*-prometheus(-.*)?$', r'^grafana(-.*)?$', r'.*-grafana(-.*)?$', r'^alertmanager(-.*)?$', r'.*-alertmanager(-.*)?$'],
    'logging': [r'^fluentd(-.*)?$', r'.*-fluentd(-.*)?$', r'^fluent-bit(-.*)?$', r'.*-fluent-bit(-.*)?$', r'^elasticsearch(-.*)?$', r'.*-elasticsearch(-.*)?$', r'^kibana(-.*)?$', r'.*-kibana(-.*)?$', r'^logstash(-.*)?$', r'.*-logstash(-.*)?$'],
    'ingress': [r'.*-ingress(-.*)?$', r'.*-nginx(-.*)?$', r'.*-traefik(-.*)?$'],
    'storage': [r'.*-csi(-.*)?$', r'.*-nfs(-.*)?$'],
    'networking': [r'.*-calico(-.*)?$', r'.*-flannel(-.*)?$', r'.*-weave(-.*)?$', r'.*-cilium(-.*)?$'],
    'security': [r'.*-vault(-.*)?$', r'.*-cert-manager(-.*)?$'],
    'kubernetes-system': [r'^kube-.*', r'.*-kube-.*', r'^coredns(-.*)?$'],
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Analyze Azure Log Analytics CSV output and categorize HPCC components',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic analysis
  %(prog)s --input results.csv --output analysis.csv

  # Detailed analysis with verbose output
  %(prog)s --input results.csv --output analysis.csv --verbose

  # Generate summary report
  %(prog)s --input results.csv --output analysis.csv --summary summary.txt
        """
    )
    
    parser.add_argument('--input', '-i', required=True, 
                       help='Input CSV file from azure_log_analytics_fetch.py')
    parser.add_argument('--output', '-o', required=True,
                       help='Output CSV file with categorized component analysis')
    parser.add_argument('--summary', '-s',
                       help='Optional summary report file (text format)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose output')
    
    return parser.parse_args()


def categorize_pod(pod_name: str, namespace: str, controller_name: str) -> tuple:
    """
    Categorize a pod into HPCC component or system category.
    
    Args:
        pod_name: Name of the pod
        namespace: Kubernetes namespace
        controller_name: Controller name (deployment, statefulset, etc.)
    
    Returns:
        Tuple of (category, component_type, is_hpcc)
    """
    # Normalize names for pattern matching
    search_name = pod_name.lower()
    search_controller = (controller_name or "").lower()
    
    # Check HPCC components first
    for component, patterns in HPCC_COMPONENT_PATTERNS.items():
        for pattern in patterns:
            if re.match(pattern, search_name) or re.match(pattern, search_controller):
                return ('hpcc', component, True)
    
    # Check system components
    for category, patterns in SYSTEM_COMPONENT_PATTERNS.items():
        for pattern in patterns:
            if re.match(pattern, search_name):
                return (category, 'system', False)
    
    # Check namespace-based categorization for system pods
    if namespace in ['kube-system', 'kube-public', 'kube-node-lease']:
        return ('kubernetes-system', 'system', False)
    
    # Uncategorized
    return ('unknown', 'unknown', False)


def parse_csv_input(input_path: str, verbose: bool = False) -> tuple:
    """
    Parse the input CSV file and extract metadata and records.
    
    Args:
        input_path: Path to input CSV file
        verbose: Enable verbose output
    
    Returns:
        Tuple of (metadata dict, list of pod records, list of node records)
    """
    metadata = {}
    pod_records = []
    node_records = []
    
    with open(input_path, 'r', encoding='utf-8') as csvfile:
        # Read all lines first
        lines = csvfile.readlines()
    
    # Parse metadata from comment lines
    data_start_idx = 0
    for i, line in enumerate(lines):
        if line.startswith('#'):
            # Parse metadata
            if ':' in line:
                key_value = line[1:].strip().split(':', 1)
                if len(key_value) == 2:
                    key = key_value[0].strip().lower().replace(' ', '_')
                    value = key_value[1].strip()
                    metadata[key] = value
        else:
            data_start_idx = i
            break
    
    # Parse CSV data
    if data_start_idx < len(lines):
        csv_lines = lines[data_start_idx:]
        reader = csv.DictReader(csv_lines)
        for row in reader:
            record_type = row.get('RecordType', '')
            if record_type == 'Pod':
                pod_records.append(row)
            elif record_type == 'Node':
                node_records.append(row)
    
    if verbose:
        print(f"Parsed metadata: {len(metadata)} items")
        print(f"Found {len(pod_records)} pod records")
        print(f"Found {len(node_records)} node records")
    
    return metadata, pod_records, node_records


def analyze_pods(pod_records: List[Dict], verbose: bool = False) -> List[Dict]:
    """
    Analyze pod records and categorize them.
    
    Args:
        pod_records: List of pod record dictionaries
        verbose: Enable verbose output
    
    Returns:
        List of analyzed pod records with categorization
    """
    analyzed_records = []
    category_counts = defaultdict(int)
    component_counts = defaultdict(int)
    
    for record in pod_records:
        pod_name = record.get('Name', '')
        namespace = record.get('Namespace', '')
        controller_name = record.get('ControllerName', '')
        
        category, component, is_hpcc = categorize_pod(pod_name, namespace, controller_name)
        
        analyzed_record = record.copy()
        analyzed_record['Category'] = category
        analyzed_record['Component'] = component
        analyzed_record['IsHPCC'] = 'Yes' if is_hpcc else 'No'
        
        analyzed_records.append(analyzed_record)
        
        category_counts[category] += 1
        if is_hpcc:
            component_counts[component] += 1
    
    if verbose:
        print("\nCategorization Summary:")
        print(f"  Total pods analyzed: {len(pod_records)}")
        print(f"  HPCC components found: {sum(1 for r in analyzed_records if r['IsHPCC'] == 'Yes')}")
        print("\nCategory breakdown:")
        for category, count in sorted(category_counts.items()):
            print(f"  {category}: {count}")
        print("\nHPCC component breakdown:")
        for component, count in sorted(component_counts.items()):
            print(f"  {component}: {count}")
    
    return analyzed_records


def create_time_series(analyzed_pods: List[Dict], node_records: List[Dict], 
                       verbose: bool = False) -> List[Dict]:
    """
    Create time-series data showing components and their node usage over time.
    
    Args:
        analyzed_pods: List of analyzed pod records
        node_records: List of node records
        verbose: Enable verbose output
    
    Returns:
        List of time-series records
    """
    time_series = []
    
    # Create a mapping of computer/node to its details
    node_map = {}
    for node in node_records:
        computer = node.get('Computer', '')
        if computer:
            node_map[computer] = node
    
    # Process each pod record as a time-series entry
    for pod in analyzed_pods:
        time_generated = pod.get('TimeGenerated', '')
        component = pod.get('Component', '')
        category = pod.get('Category', '')
        is_hpcc = pod.get('IsHPCC', 'No')
        pod_name = pod.get('Name', '')
        namespace = pod.get('Namespace', '')
        computer = pod.get('Computer', '')
        pod_status = pod.get('PodStatus', '')
        controller_name = pod.get('ControllerName', '')
        controller_kind = pod.get('ControllerKind', '')
        
        # Get node information if available
        node_info = node_map.get(computer, {})
        node_status = node_info.get('Status', 'Unknown')
        node_zone = node_info.get('KubernetesZone', 'Unknown')
        
        time_series_entry = {
            'TimeGenerated': time_generated,
            'Component': component,
            'Category': category,
            'IsHPCC': is_hpcc,
            'PodName': pod_name,
            'Namespace': namespace,
            'NodeName': computer,
            'NodeStatus': node_status,
            'NodeZone': node_zone,
            'PodStatus': pod_status,
            'ControllerName': controller_name,
            'ControllerKind': controller_kind,
        }
        
        time_series.append(time_series_entry)
    
    # Sort by time
    time_series.sort(key=lambda x: x['TimeGenerated'])
    
    if verbose:
        print(f"\nCreated time-series with {len(time_series)} entries")
    
    return time_series


def write_analysis_output(time_series: List[Dict], output_path: str, 
                         metadata: Dict, verbose: bool = False):
    """
    Write analyzed time-series data to CSV file.
    
    Args:
        time_series: List of time-series records
        output_path: Output file path
        metadata: Original metadata from input
        verbose: Enable verbose output
    """
    if not time_series:
        print("No data to write", file=sys.stderr)
        return
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        # Write metadata header
        csvfile.write("# Azure Log Analytics Component Analysis\n")
        csvfile.write(f"# Generated: {datetime.now(timezone.utc).isoformat()}\n")
        csvfile.write(f"# Source AKS Cluster: {metadata.get('aks_cluster', 'N/A')}\n")
        csvfile.write(f"# Source Time Range: {metadata.get('time_range', 'N/A')}\n")
        csvfile.write(f"# Total Records: {len(time_series)}\n")
        csvfile.write("#\n")
        csvfile.write("# This file contains a time-series analysis of Kubernetes components\n")
        csvfile.write("# suitable for visualization (e.g., Gantt chart) and cost analysis.\n")
        csvfile.write("#\n")
        
        # Get columns
        columns = list(time_series[0].keys()) if time_series else []
        
        # Write CSV
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        writer.writerows(time_series)
    
    if verbose:
        print(f"Analysis written to {output_path}")


def generate_summary_report(analyzed_pods: List[Dict], time_series: List[Dict], 
                           summary_path: str, metadata: Dict, verbose: bool = False):
    """
    Generate a human-readable summary report.
    
    Args:
        analyzed_pods: List of analyzed pod records
        time_series: List of time-series records
        summary_path: Output file path for summary
        metadata: Original metadata from input
        verbose: Enable verbose output
    """
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("Azure Log Analytics Component Analysis Summary\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Source AKS Cluster: {metadata.get('aks_cluster', 'N/A')}\n")
        f.write(f"Source Time Range: {metadata.get('time_range', 'N/A')}\n\n")
        
        # Overall statistics
        f.write("-" * 80 + "\n")
        f.write("Overall Statistics\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total pod records: {len(analyzed_pods)}\n")
        hpcc_pods = [p for p in analyzed_pods if p['IsHPCC'] == 'Yes']
        f.write(f"HPCC pods: {len(hpcc_pods)}\n")
        f.write(f"Non-HPCC pods: {len(analyzed_pods) - len(hpcc_pods)}\n\n")
        
        # Category breakdown
        f.write("-" * 80 + "\n")
        f.write("Category Breakdown\n")
        f.write("-" * 80 + "\n")
        category_counts = defaultdict(int)
        for pod in analyzed_pods:
            category_counts[pod['Category']] += 1
        
        for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(analyzed_pods)) * 100 if analyzed_pods else 0
            f.write(f"  {category:30s}: {count:5d} ({percentage:5.1f}%)\n")
        f.write("\n")
        
        # HPCC component breakdown
        if hpcc_pods:
            f.write("-" * 80 + "\n")
            f.write("HPCC Component Breakdown\n")
            f.write("-" * 80 + "\n")
            component_counts = defaultdict(int)
            for pod in hpcc_pods:
                component_counts[pod['Component']] += 1
            
            for component, count in sorted(component_counts.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / len(hpcc_pods)) * 100
                f.write(f"  {component:30s}: {count:5d} ({percentage:5.1f}%)\n")
            f.write("\n")
        
        # Node usage
        f.write("-" * 80 + "\n")
        f.write("Node Usage\n")
        f.write("-" * 80 + "\n")
        node_counts = defaultdict(int)
        for entry in time_series:
            node_counts[entry['NodeName']] += 1
        
        f.write(f"Unique nodes: {len(node_counts)}\n")
        f.write("\nTop 10 nodes by pod count:\n")
        for node, count in sorted(node_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            f.write(f"  {node:50s}: {count:5d} pod records\n")
        f.write("\n")
        
        # Namespace breakdown
        f.write("-" * 80 + "\n")
        f.write("Namespace Breakdown\n")
        f.write("-" * 80 + "\n")
        namespace_counts = defaultdict(int)
        for pod in analyzed_pods:
            namespace = pod.get('Namespace', 'unknown')
            namespace_counts[namespace] += 1
        
        for namespace, count in sorted(namespace_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(analyzed_pods)) * 100 if analyzed_pods else 0
            f.write(f"  {namespace:30s}: {count:5d} ({percentage:5.1f}%)\n")
        f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("End of Report\n")
        f.write("=" * 80 + "\n")
    
    if verbose:
        print(f"Summary report written to {summary_path}")


def main():
    """Main entry point."""
    args = parse_args()
    
    try:
        # Parse input CSV
        if args.verbose:
            print(f"Reading input from {args.input}")
        
        metadata, pod_records, node_records = parse_csv_input(args.input, args.verbose)
        
        if not pod_records:
            print("Warning: No pod records found in input file", file=sys.stderr)
        
        # Analyze pods
        if args.verbose:
            print("\nAnalyzing pods...")
        
        analyzed_pods = analyze_pods(pod_records, args.verbose)
        
        # Create time-series
        if args.verbose:
            print("\nCreating time-series...")
        
        time_series = create_time_series(analyzed_pods, node_records, args.verbose)
        
        # Write output
        if args.verbose:
            print(f"\nWriting analysis to {args.output}")
        
        write_analysis_output(time_series, args.output, metadata, args.verbose)
        
        # Generate summary if requested
        if args.summary:
            if args.verbose:
                print(f"\nGenerating summary report to {args.summary}")
            
            generate_summary_report(analyzed_pods, time_series, args.summary, 
                                  metadata, args.verbose)
        
        print(f"\nAnalysis complete!")
        print(f"  Analyzed {len(pod_records)} pod records")
        print(f"  Generated {len(time_series)} time-series entries")
        print(f"  Output written to {args.output}")
        if args.summary:
            print(f"  Summary report written to {args.summary}")
        
    except FileNotFoundError as e:
        print(f"Error: Input file not found: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
