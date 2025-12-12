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
Azure Log Analytics Fetch Tool

This tool queries Azure Log Analytics using KQL (Kusto Query Language) to fetch
information about Kubernetes nodes and pods from a specified AKS cluster for a
given date/time range.

It queries KubeNodeInventory and KubePodInventory tables and outputs the results
in CSV format with metadata about the query in a comment header.

Requirements:
    Azure CLI must be installed and authenticated (az login)
"""

import argparse
import sys
import csv
import json
import subprocess
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple


def run_az_command(command: List[str], debug: bool = False) -> Tuple[Optional[str], Optional[str]]:
    """Run Azure CLI command and return output.
    
    Args:
        command: List of command arguments
        debug: Enable debug output
    
    Returns:
        Tuple of (stdout, stderr) or (None, error_message)
    """
    if debug:
        print(f"DEBUG: Running command: {' '.join(command)}", file=sys.stderr)
    
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout, None
    except subprocess.CalledProcessError as e:
        error_msg = f"Command failed: {' '.join(command)}\nError: {e.stderr}"
        return None, error_msg
    except FileNotFoundError:
        return None, "Azure CLI (az) not found. Please install it."


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Fetch Kubernetes node and pod inventory from Azure Log Analytics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch data for the last 24 hours
  %(prog)s --subscription-id <sub-id> --resource-group <rg> --aks-name <aks> \\
           --workspace-id <workspace-id> --start "2024-01-01T00:00:00Z" \\
           --end "2024-01-01T23:59:59Z" --output results.csv

  # Fetch data for specific namespaces only
  %(prog)s --subscription-id <sub-id> --resource-group <rg> --aks-name <aks> \\
           --workspace-id <workspace-id> --start "2024-01-01T00:00:00Z" \\
           --end "2024-01-01T23:59:59Z" --namespaces default,kube-system \\
           --output results.csv

Note: Uses 'az login' for authentication. Service principal auth is not supported
      in this version (use Azure CLI az login with service principal instead).
        """
    )
    
    # Required arguments
    required = parser.add_argument_group('required arguments')
    required.add_argument('--subscription-id', required=True, help='Azure subscription ID')
    required.add_argument('--workspace-id', required=True, help='Log Analytics workspace ID')
    required.add_argument('--aks-name', required=True, help='AKS cluster name')
    required.add_argument('--start', required=True, help='Start datetime (ISO 8601 format, e.g., 2024-01-01T00:00:00Z)')
    required.add_argument('--end', required=True, help='End datetime (ISO 8601 format, e.g., 2024-01-01T23:59:59Z)')
    
    # Optional arguments
    parser.add_argument('--resource-group', help='Azure resource group name (optional, used in metadata)')
    parser.add_argument('--namespaces', help='Comma-separated list of namespaces to filter (default: all)')
    parser.add_argument('--output', '-o', default='azure_log_analytics_output.csv', 
                       help='Output CSV file path (default: azure_log_analytics_output.csv)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    
    return parser.parse_args()


def build_kql_query(aks_name: str, start_time: str, end_time: str, namespaces: Optional[List[str]] = None) -> str:
    """
    Build the KQL query to fetch node and pod inventory data.
    
    Args:
        aks_name: Name of the AKS cluster
        start_time: Start time in ISO 8601 format
        end_time: End time in ISO 8601 format
        namespaces: Optional list of namespaces to filter
    
    Returns:
        KQL query string
    """
    namespace_filter = ""
    if namespaces:
        namespace_list = "', '".join(namespaces)
        namespace_filter = f"| where Namespace in ('{namespace_list}')"
    
    query = f"""
let startTime = datetime({start_time});
let endTime = datetime({end_time});
let clusterName = "{aks_name}";
// Get node inventory
let nodes = KubeNodeInventory
| where TimeGenerated between (startTime .. endTime)
| where ClusterName == clusterName
| project TimeGenerated, ClusterName, Computer, Status, 
          KubeletVersion, KubeProxyVersion, CreationTimeStamp,
          Labels, KubernetesProviderID, KubernetesZone
| extend RecordType = "Node";
// Get pod inventory
let pods = KubePodInventory
| where TimeGenerated between (startTime .. endTime)
| where ClusterName == clusterName
{namespace_filter}
| project TimeGenerated, ClusterName, Namespace, Name, PodLabel, 
          PodStatus, PodCreationTimeStamp, Computer, ControllerName, 
          ControllerKind, ContainerName, ContainerStatus, PodIp,
          ServiceName, PodUid
| extend RecordType = "Pod";
// Union results
union nodes, pods
| order by TimeGenerated asc
"""
    return query.strip()


def query_log_analytics(workspace_id: str, query: str, verbose: bool = False) -> List[Dict]:
    """
    Execute KQL query against Log Analytics workspace using az rest.
    
    Args:
        workspace_id: Log Analytics workspace ID
        query: KQL query string
        verbose: Enable verbose output
    
    Returns:
        List of result rows as dictionaries
    """
    if verbose:
        print(f"Executing query against workspace {workspace_id}", file=sys.stderr)
        print(f"Query:\n{query}\n", file=sys.stderr)
    
    # Build REST API request
    uri = f"https://api.loganalytics.io/v1/workspaces/{workspace_id}/query"
    request_body = {"query": query}
    
    # Execute query using az rest
    stdout, error = run_az_command([
        'az', 'rest',
        '--method', 'POST',
        '--uri', uri,
        '--body', json.dumps(request_body),
        '--headers', 'Content-Type=application/json',
        '--output', 'json'
    ], verbose)
    
    if error:
        print(f"Error executing query: {error}", file=sys.stderr)
        return []
    
    if not stdout:
        return []
    
    try:
        data = json.loads(stdout)
        
        # Extract tables from response
        tables = data.get('tables', [])
        if not tables:
            if verbose:
                print("DEBUG: No tables in response", file=sys.stderr)
            return []
        
        # Get first table (query results)
        table = tables[0]
        columns = [col['name'] for col in table.get('columns', [])]
        rows = table.get('rows', [])
        
        if verbose:
            print(f"Retrieved {len(rows)} records", file=sys.stderr)
        
        # Convert rows to dict format
        results = []
        for row in rows:
            row_dict = {}
            for i, col_name in enumerate(columns):
                row_dict[col_name] = row[i] if i < len(row) else None
            results.append(row_dict)
        
        return results
        
    except json.JSONDecodeError as e:
        print(f"Error parsing query results: {e}", file=sys.stderr)
        return []


def write_csv_output(results: List[Dict], output_path: str, metadata: Dict, verbose: bool = False):
    """
    Write query results to CSV file with metadata header.
    
    Args:
        results: List of result rows as dictionaries
        output_path: Output CSV file path
        metadata: Metadata dictionary for header comments
        verbose: Enable verbose output
    """
    if not results:
        print("No results to write", file=sys.stderr)
        return
    
    # Get all unique column names from results
    all_columns = set()
    for row in results:
        all_columns.update(row.keys())
    
    # Sort columns for consistent output
    columns = sorted(all_columns)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        # Write metadata as comments
        csvfile.write("# Azure Log Analytics Query Results\n")
        csvfile.write(f"# Generated: {datetime.now(timezone.utc).isoformat()}\n")
        csvfile.write(f"# Subscription ID: {metadata.get('subscription_id', 'N/A')}\n")
        if metadata.get('resource_group'):
            csvfile.write(f"# Resource Group: {metadata['resource_group']}\n")
        csvfile.write(f"# AKS Cluster: {metadata.get('aks_name', 'N/A')}\n")
        csvfile.write(f"# Workspace ID: {metadata.get('workspace_id', 'N/A')}\n")
        csvfile.write(f"# Time Range: {metadata.get('start_time', 'N/A')} to {metadata.get('end_time', 'N/A')}\n")
        if metadata.get('namespaces'):
            csvfile.write(f"# Namespaces: {metadata['namespaces']}\n")
        else:
            csvfile.write("# Namespaces: All\n")
        csvfile.write(f"# Total Records: {len(results)}\n")
        csvfile.write("#\n")
        
        # Write CSV data
        writer = csv.DictWriter(csvfile, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    
    if verbose:
        print(f"Results written to {output_path}", file=sys.stderr)


def main():
    """Main entry point."""
    args = parse_args()
    
    # Validate datetime formats
    try:
        datetime.fromisoformat(args.start.replace('Z', '+00:00'))
        datetime.fromisoformat(args.end.replace('Z', '+00:00'))
    except ValueError as e:
        print(f"Error: Invalid datetime format: {e}", file=sys.stderr)
        print("Please use ISO 8601 format (e.g., 2024-01-01T00:00:00Z)", file=sys.stderr)
        sys.exit(1)
    
    # Parse namespaces if provided
    namespaces = None
    if args.namespaces:
        namespaces = [ns.strip() for ns in args.namespaces.split(',')]
    
    try:
        # Build query
        query = build_kql_query(args.aks_name, args.start, args.end, namespaces)
        
        # Execute query
        results = query_log_analytics(args.workspace_id, query, args.verbose)
        
        if not results:
            print("No results found for the specified query", file=sys.stderr)
            sys.exit(1)
        
        # Prepare metadata
        metadata = {
            'subscription_id': args.subscription_id,
            'resource_group': args.resource_group,
            'aks_name': args.aks_name,
            'workspace_id': args.workspace_id,
            'start_time': args.start,
            'end_time': args.end,
            'namespaces': args.namespaces
        }
        
        # Write output
        write_csv_output(results, args.output, metadata, args.verbose)
        
        print(f"Successfully fetched {len(results)} records and saved to {args.output}")
        
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
