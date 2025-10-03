#!/usr/bin/env python3

'''
/*#############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################ */
'''

"""
analyze_wuids.py - Analyze workunits from ESP WsWorkunits service

This script fetches detailed information about specified workunits from an
ESP server's WsWorkunits service.

Usage:
    analyze_wuids.py <espserver:port> <wuid1> <wuid2> ...
    analyze_wuids.py <espserver:port> -f <wuids-file>
    analyze_wuids.py <espserver:port> -f <wuids-file> -e <error-patterns-file>
    cat <wuids-file> | analyze_wuids.py <espserver:port>

Arguments:
    espserver:port  ESP server address (e.g., localhost:8010)
    wuid1 wuid2     One or more workunit IDs
    -f file         Read WUIDs from file (one per line)
    -e file         File containing error patterns to match (one per line)
                    When specified, only workunits with matching errors are displayed
    -v              Show verbose information including timings
    -s              Show summary table only
    stdin           Read WUIDs from standard input (one per line)

Examples:
    # Analyze specific WUIDs
    analyze_wuids.py localhost:8010 W20240101-120000 W20240101-120001
    
    # Read WUIDs from a file
    analyze_wuids.py localhost:8010 -f wuids.txt
    
    # Filter to specific error patterns
    analyze_wuids.py localhost:8010 -f wuids.txt -e error_patterns.txt
    
    # Pipe WUIDs from another command and filter by errors
    ./getwuids.py localhost:8010 2024-01-01 2024-01-01 | tail -n +3 | awk '{print $1}' | analyze_wuids.py localhost:8010 -e errors.txt

The script connects to the ESP server and queries the WUInfo service endpoint
for each workunit, displaying detailed information including state, owner,
jobname, cluster, timing information, and the first ERROR exception if present.

When using the -e flag, only workunits whose first ERROR exception matches
one of the specified patterns (case-insensitive substring match) will be
displayed. This is useful for filtering large numbers of workunits to find
specific error types.
"""

import sys
import argparse
import requests
import json
import re
from urllib.parse import urljoin

def parse_error_info(error_message):
    """Parse graph name and worker number from error message.
    
    Returns dict with 'graph_name' and 'worker_number' or None if not found.
    Examples:
        "Graph graph30[2]" -> graph_name="graph30", subgraph="2"
        "WORKER #118" -> worker_number="118"
    """
    info = {'graph_name': None, 'subgraph_id': None, 'worker_number': None}
    
    # Parse graph name: "Graph <graphName>[<subgraphID>]"
    graph_match = re.search(r'Graph\s+(\w+)\[(\d+)\]', error_message, re.IGNORECASE)
    if graph_match:
        info['graph_name'] = graph_match.group(1)
        info['subgraph_id'] = graph_match.group(2)
    
    # Parse worker number: "WORKER #<number>"
    worker_match = re.search(r'WORKER\s+#(\d+)', error_message, re.IGNORECASE)
    if worker_match:
        info['worker_number'] = worker_match.group(1)
    
    return info

def get_process_info(esp_url, wuid):
    """Fetch process information for a workunit."""
    if not esp_url.startswith(('http://', 'https://')):
        esp_url = f"http://{esp_url}"
    
    url = f"{esp_url}/WsWorkunits/WUInfo.json"
    params = {
        'Wuid': wuid,
        'IncludeProcesses': 1,
        'IncludeExceptions': 0,
        'IncludeGraphs': 0,
        'IncludeSourceFiles': 0,
        'IncludeResults': 0,
        'IncludeVariables': 0,
        'IncludeTimers': 0,
        'IncludeDebugValues': 0,
        'IncludeApplicationValues': 0,
        'IncludeWorkflows': 0,
        'IncludeXmlSchemas': 0,
        'IncludeResourceURLs': 0,
        'IncludeECL': 0,
        'IncludeHelpers': 0,
        'IncludeAllowedClusters': 0,
        'SuppressResultSchemas': 1,
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        workunit = data.get('WUInfoResponse', {}).get('Workunit', {})
        process_list = workunit.get('ECLWUProcessList', {}).get('ECLWUProcess', [])
        
        # Handle single process returned as dict
        if isinstance(process_list, dict):
            process_list = [process_list]
        
        return process_list
    except requests.exceptions.RequestException as e:
        return []

def find_worker_pod_info(processes, graph_name, worker_number):
    """Find Thor worker pod/container info based on graph name and worker number.
    
    Returns dict with pod_name, container_name, or None if not found.
    """
    # Step 1: Find Thor process with the graph name to get instance number
    thor_instance_num = None
    for proc in processes:
        if proc.get('Type') == 'Thor':
            graphs = proc.get('Graphs', {})
            graph_items = graphs.get('Item', []) if isinstance(graphs, dict) else graphs or []
            # Handle both list and single string
            if isinstance(graph_items, str):
                graph_items = [graph_items]
            
            if graph_name in graph_items:
                thor_instance_num = proc.get('InstanceNumber')
                break
    
    if thor_instance_num is None:
        return None
    
    # Step 2: Find ThorWorker with matching instance number and worker number (sequence)
    for proc in processes:
        if proc.get('Type') == 'ThorWorker':
            if proc.get('InstanceNumber') == thor_instance_num:
                # Check if sequence matches worker number
                sequence = proc.get('Sequence', '')
                if sequence == worker_number:
                    return {
                        'pod_name': proc.get('PodName'),
                        'container_name': proc.get('ContainerName'),
                        'instance_number': thor_instance_num,
                        'sequence': sequence
                    }
    
    # If exact match not found by sequence, try pod name pattern
    for proc in processes:
        if proc.get('Type') == 'ThorWorker':
            if proc.get('InstanceNumber') == thor_instance_num:
                pod_name = proc.get('PodName', '')
                
                # Try to find worker number in pod name
                pod_worker_match = re.search(r'-(\d+)-', pod_name)
                if pod_worker_match and pod_worker_match.group(1) == worker_number:
                    return {
                        'pod_name': proc.get('PodName'),
                        'container_name': proc.get('ContainerName'),
                        'instance_number': thor_instance_num,
                        'note': 'Matched by pod name pattern'
                    }
    
    # If exact match not found, return first worker for that instance
    for proc in processes:
        if proc.get('Type') == 'ThorWorker' and proc.get('InstanceNumber') == thor_instance_num:
            return {
                'pod_name': proc.get('PodName'),
                'container_name': proc.get('ContainerName'),
                'instance_number': thor_instance_num,
                'note': 'Approximate match - exact worker not identified'
            }
    
    return None

def get_workunit_info(esp_url, wuid):
    """Fetch detailed workunit information."""
    # Ensure URL has protocol prefix
    if not esp_url.startswith(('http://', 'https://')):
        esp_url = f"http://{esp_url}"
    
    url = f"{esp_url}/WsWorkunits/WUInfo.json"
    params = {
        'Wuid': wuid,
        'IncludeExceptions': 1,
        'IncludeGraphs': 0,
        'IncludeSourceFiles': 0,
        'IncludeResults': 0,
        'IncludeVariables': 0,
        'IncludeTimers': 0,
        'IncludeDebugValues': 0,
        'IncludeApplicationValues': 0,
        'IncludeWorkflows': 0,
        'IncludeXmlSchemas': 0,
        'IncludeResourceURLs': 0,
        'IncludeECL': 0,
        'IncludeHelpers': 0,
        'IncludeAllowedClusters': 0,
        'SuppressResultSchemas': 1,
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        workunit = data.get('WUInfoResponse', {}).get('Workunit', {})
        
        exceptions = []
        exception_list = workunit.get('Exceptions', {}).get('ECLException', [])
        if isinstance(exception_list, dict):
            exception_list = [exception_list]
        
        for exc in exception_list:
            exceptions.append({
                'severity': exc.get('Severity', ''),
                'code': exc.get('Code', ''),
                'message': exc.get('Message', ''),
                'source': exc.get('Source', ''),
                'filename': exc.get('FileName', ''),
                'lineno': exc.get('LineNo', ''),
                'column': exc.get('Column', '')
            })
        
        # Extract first ERROR exception
        first_error = None
        for exc in exceptions:
            if exc.get('severity', '').lower() == 'error':
                first_error = exc
                break
        
        # Parse error message for graph/worker info and fetch process details
        error_details = None
        worker_pod_info = None
        if first_error:
            error_msg = first_error.get('message', '')
            error_details = parse_error_info(error_msg)
            
            # If we found graph and worker info, fetch process information
            if error_details.get('graph_name') and error_details.get('worker_number'):
                processes = get_process_info(esp_url, wuid)
                worker_pod_info = find_worker_pod_info(
                    processes, 
                    error_details['graph_name'], 
                    error_details['worker_number']
                )
        
        return {
            'wuid': wuid,
            'state': workunit.get('State', ''),
            'owner': workunit.get('Owner', ''),
            'cluster': workunit.get('Cluster', ''),
            'jobname': workunit.get('Jobname', ''),
            'protected': workunit.get('Protected', False),
            'created': workunit.get('DateTimeScheduled', ''),
            'total_time': workunit.get('TotalClusterTime', ''),
            'compile_time': workunit.get('CompileTime', ''),
            'execute_time': workunit.get('ExecuteTime', ''),
            'exceptions': exceptions,
            'first_error': first_error,
            'error_details': error_details,
            'worker_pod_info': worker_pod_info
        }
        
    except requests.exceptions.RequestException as e:
        return {
            'wuid': wuid,
            'error': f"Failed to fetch workunit info: {e}"
        }

def read_wuids_from_file(filepath):
    """Read WUIDs from a file, one per line."""
    wuids = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                wuid = line.strip()
                if wuid and not wuid.startswith('#'):  # Skip empty lines and comments
                    wuids.append(wuid)
    except IOError as e:
        print(f"Error reading file '{filepath}': {e}", file=sys.stderr)
        sys.exit(1)
    return wuids

def read_wuids_from_stdin():
    """Read WUIDs from standard input, one per line."""
    wuids = []
    for line in sys.stdin:
        wuid = line.strip()
        if wuid and not wuid.startswith('#'):  # Skip empty lines and comments
            wuids.append(wuid)
    return wuids

def read_error_patterns_from_file(filepath):
    """Read error patterns from a file, one per line."""
    patterns = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                pattern = line.strip()
                if pattern and not pattern.startswith('#'):  # Skip empty lines and comments
                    patterns.append(pattern)
    except IOError as e:
        print(f"Error reading error patterns file '{filepath}': {e}", file=sys.stderr)
        sys.exit(1)
    return patterns

def match_error_pattern(error_message, patterns):
    """Check if error message matches any of the patterns."""
    if not patterns:
        return True  # No patterns means match all
    
    error_message_lower = error_message.lower()
    for pattern in patterns:
        pattern_lower = pattern.lower()
        if pattern_lower in error_message_lower:
            return True
    return False

def format_time(time_str):
    """Format time string, handling None."""
    if time_str is None:
        return 'N/A'
    return time_str

def print_workunit_info(info, verbose=False, matched=None):
    """Print workunit information in a readable format."""
    if info.get('error'):
        print(f"WUID: {info['wuid']}")
        print(f"  ERROR: {info['error']}")
        print()
        return
    
    # Add matched indicator if pattern matching was used
    wuid_display = info['wuid']
    if matched is not None:
        wuid_display += f" {'[MATCHED]' if matched else '[NO MATCH]'}"
    
    print(f"WUID: {wuid_display}")
    print(f"  State:        {info['state']}")
    print(f"  Owner:        {info['owner']}")
    print(f"  Jobname:      {info['jobname']}")
    print(f"  Cluster:      {info['cluster']}")
    print(f"  Protected:    {info['protected']}")
    if verbose:
        print(f"  Created:      {info['created']}")
        print(f"  Total Time:   {format_time(info['total_time'])}")
        print(f"  Compile Time: {format_time(info['compile_time'])}")
        print(f"  Execute Time: {format_time(info['execute_time'])}")
    
    # Display first ERROR exception only
    first_error = info.get('first_error')
    if first_error:
        severity = first_error.get('severity', 'Unknown')
        source = first_error.get('source', '')
        code = first_error.get('code', '')
        message = first_error.get('message', '')
        filename = first_error.get('filename', '')
        line = first_error.get('line', '')
        column = first_error.get('column', '')
        
        print(f"  Error:        {severity.upper()}", end='')
        if code:
            print(f" (Code {code})", end='')
        if source:
            print(f" - {source}", end='')
        print()
        
        if message:
            # Indent and wrap long messages
            for msg_line in message.split('\n'):
                print(f"                {msg_line}")
        
        if filename:
            location = f"                File: {filename}"
            if line:
                location += f", Line: {line}"
            if column:
                location += f", Column: {column}"
            print(location)
        
        # Display parsed error details and worker pod info
        error_details = info.get('error_details')
        if error_details:
            if error_details.get('graph_name'):
                print(f"  Graph:        {error_details['graph_name']}", end='')
                if error_details.get('subgraph_id'):
                    print(f"[{error_details['subgraph_id']}]", end='')
                print()
            if error_details.get('worker_number'):
                print(f"  Worker:       #{error_details['worker_number']}")
        
        worker_pod_info = info.get('worker_pod_info')
        if worker_pod_info:
            print(f"  Pod:          {worker_pod_info.get('pod_name', 'N/A')}")
            print(f"  Container:    {worker_pod_info.get('container_name', 'N/A')}")
            if worker_pod_info.get('note'):
                print(f"  Note:         {worker_pod_info['note']}")
    
    print()

def print_summary_table(infos, show_matched=False):
    """Print a summary table of all workunits."""
    headers = f"{'WUID':<20} {'State':<12} {'Owner':<15} {'Cluster':<15}"
    if show_matched:
        headers += f" {'Match':<8}"
    headers += f" {'Error'}"
    print(headers)
    print("-" * (110 if show_matched else 100))
    
    for info in infos:
        if info.get('error'):
            line = f"{info['wuid']:<20} {'ERROR':<12} {'':<15} {'':<15}"
            if show_matched:
                line += f" {'':<8}"
            line += f" {info['error']}"
            print(line)
        else:
            wuid = info['wuid'][:20]
            state = info['state'][:12]
            owner = info['owner'][:15]
            cluster = info['cluster'][:15]
            
            first_error = info.get('first_error')
            error_text = ""
            if first_error:
                error_text = first_error.get('message', '')[:60]
            
            line = f"{wuid:<20} {state:<12} {owner:<15} {cluster:<15}"
            if show_matched:
                matched = info.get('matched', False)
                match_str = "YES" if matched else "NO"
                line += f" {match_str:<8}"
            line += f" {error_text}"
            print(line)

def main():
    parser = argparse.ArgumentParser(
        description='Analyze workunit details from ESP WsWorkunits service',
        usage='%(prog)s <espserver:port> [<wuid1> <wuid2> ...] [-f <file>] [-e <file>] [-v] [-s]',
        epilog='''
Examples:
  %(prog)s localhost:8010 W20240101-120000 W20240101-120001
      Analyze specific workunits
  
  %(prog)s localhost:8010 -f wuids.txt
      Read WUIDs from file (one per line)
  
  cat wuids.txt | %(prog)s localhost:8010
      Read WUIDs from stdin

  ./getwuids.py localhost:8010 2024-01-01 2024-01-01 | tail -n +3 | awk '{print $1}' | %(prog)s localhost:8010
      Pipe WUIDs from getwuids.py
  
  %(prog)s localhost:8010 -f wuids.txt -e error_patterns.txt
      Filter to workunits matching specific error patterns
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('espserver', 
                       metavar='espserver:port',
                       help='ESP server address in format host:port (e.g., localhost:8010)')
    parser.add_argument('wuids',
                       nargs='*',
                       help='Workunit IDs to analyze')
    parser.add_argument('-f', '--file',
                       metavar='<file>',
                       help='Read WUIDs from file (one per line)')
    parser.add_argument('-v', '--verbose',
                       action='store_true',
                       help='Show detailed information including timings')
    parser.add_argument('-s', '--summary',
                       action='store_true',
                       help='Show summary table only')
    parser.add_argument('-e', '--errors',
                       metavar='<file>',
                       help='File containing error patterns to match (one per line)')
    
    args = parser.parse_args()
    
    # Read error patterns if provided
    error_patterns = []
    if args.errors:
        error_patterns = read_error_patterns_from_file(args.errors)
        if not error_patterns:
            print(f"Warning: No error patterns found in {args.errors}")
    
    # Collect WUIDs from various sources
    wuids = []
    
    if args.file:
        # Read from file
        wuids = read_wuids_from_file(args.file)
    elif args.wuids:
        # Use command line arguments
        wuids = args.wuids
    elif not sys.stdin.isatty():
        # Read from stdin if available
        wuids = read_wuids_from_stdin()
    else:
        print("Error: No WUIDs provided. Use command line arguments, -f option, or pipe from stdin.", file=sys.stderr)
        parser.print_help(sys.stderr)
        return 1
    
    if not wuids:
        print("Error: No valid WUIDs found.", file=sys.stderr)
        return 1
    
    print(f"Analyzing {len(wuids)} workunit(s)...\n")
    
    # Fetch information for each workunit
    infos = []
    for wuid in wuids:
        info = get_workunit_info(args.espserver, wuid)
        
        # If error patterns specified, filter workunits and mark matched ones
        if error_patterns:
            first_error = info.get('first_error')
            if first_error:
                error_msg = first_error.get('message', '')
                matched = match_error_pattern(error_msg, error_patterns)
                info['matched'] = matched
                # Only include workunits that match error patterns
                if matched:
                    infos.append(info)
            # Skip workunits without errors when filtering by patterns
        else:
            infos.append(info)
    
    if error_patterns and len(infos) == 0:
        print("No workunits matched the specified error patterns.")
        return 0
    
    # Display results
    if args.summary:
        print_summary_table(infos, show_matched=bool(error_patterns))
    else:
        for info in infos:
            matched = info.get('matched', False) if error_patterns else None
            print_workunit_info(info, verbose=args.verbose, matched=matched)
        
        if len(infos) > 1 and not args.verbose:
            print("\n" + "=" * 100)
            print("SUMMARY")
            print("=" * 100 + "\n")
            print_summary_table(infos, show_matched=bool(error_patterns))
    
    # Print statistics
    print(f"\nTotal workunits analyzed: {len(infos)}")
    
    # Count by state and exceptions
    states = {}
    errors = 0
    total_first_errors = 0
    wus_with_errors = 0
    matched_wus = 0
    
    for info in infos:
        if info.get('error'):
            errors += 1
        else:
            state = info.get('state', 'unknown')
            states[state] = states.get(state, 0) + 1
            
            # Count first ERROR exception
            if info.get('first_error'):
                wus_with_errors += 1
                total_first_errors += 1
                
            # Count matched patterns
            if info.get('matched'):
                matched_wus += 1
    
    if states:
        print("\nBy state:")
        for state, count in sorted(states.items()):
            print(f"  {state}: {count}")
    
    if wus_with_errors > 0:
        print(f"\nWorkunits with errors: {wus_with_errors}")
        if error_patterns:
            print(f"Workunits matching patterns: {matched_wus}")
    
    if errors > 0:
        print(f"\nQuery errors: {errors}")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
