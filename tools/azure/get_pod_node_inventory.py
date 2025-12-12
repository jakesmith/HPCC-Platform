#!/usr/bin/env python3
"""
Get Azure Kubernetes Pod and Node Inventory from Log Analytics

This script queries the KubePodInventory and KubeNodeInventory tables in Azure 
Log Analytics to retrieve information about pods and nodes running in a specific 
namespace during a given time range.

Usage:
    ./get_pod_node_inventory.py --start-time "2025-11-04 12:00" [options]

Examples:
    # Query using workspace ID directly
    ./get_pod_node_inventory.py --workspace-id <workspace-id> \
      --start-time "2025-11-04 12:00" -n hpcc

    # Auto-discover from AKS cluster
    ./get_pod_node_inventory.py --cluster my-aks-cluster \
      --resource-group my-rg --start-time "2025-11-04 12:00" -n hpcc

    # Query specific namespace with time range
    ./get_pod_node_inventory.py --workspace-id <workspace-id> \
      --start-time "2025-11-04 09:00" --end-time "2025-11-04 17:00" -n hpcc

    # Query all namespaces
    ./get_pod_node_inventory.py --workspace-id <workspace-id> \
      --start-time "2025-11-04 12:00" --all-namespaces

    # Export to CSV file
    ./get_pod_node_inventory.py --workspace-id <workspace-id> \
      --start-time "2025-11-04 12:00" -n hpcc > inventory.csv

DateTime format: YYYY-MM-DD HH:MM (defaults to UTC)
"""

import sys
import json
import subprocess
import argparse
from datetime import datetime, timedelta


def query_log_analytics(workspace_id, query, verbose=False):
    """Execute a KQL query against Log Analytics workspace using REST API."""

    # Use the Log Analytics Query API
    uri = f"https://api.loganalytics.io/v1/workspaces/{workspace_id}/query"

    request_body = {
        "query": query
    }

    cmd = [
        'az', 'rest',
        '--method', 'POST',
        '--uri', uri,
        '--body', json.dumps(request_body),
        '--headers', 'Content-Type=application/json',
        '--output', 'json'
    ]

    if verbose:
        print("=== API Request ===", file=sys.stderr)
        print(f"URI: {uri}", file=sys.stderr)
        print(f"Command: {' '.join(cmd)}", file=sys.stderr)
        print("===================\n", file=sys.stderr)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        if verbose:
            print("=== Raw API Response ===", file=sys.stderr)
            print(result.stdout[:2000], file=sys.stderr)  # First 2000 chars
            if len(result.stdout) > 2000:
                print(f"... (truncated, total length: {len(result.stdout)} chars)", file=sys.stderr)
            print("========================\n", file=sys.stderr)

        data = json.loads(result.stdout)

        if verbose:
            print("=== Parsed Response ===", file=sys.stderr)
            print(f"Response keys: {list(data.keys())}", file=sys.stderr)
            print(f"Number of tables: {len(data.get('tables', []))}", file=sys.stderr)

        # Extract tables from response
        tables = data.get('tables', [])
        if not tables:
            if verbose:
                print("WARNING: No tables in response!", file=sys.stderr)
                print(f"Full response: {json.dumps(data, indent=2)}", file=sys.stderr)
                print("=======================\n", file=sys.stderr)
            return []

        # Get first table (query results)
        table = tables[0]
        columns = [col['name'] for col in table.get('columns', [])]
        rows = table.get('rows', [])

        if verbose:
            print(f"Columns: {columns}", file=sys.stderr)
            print(f"Number of rows: {len(rows)}", file=sys.stderr)
            print("=======================\n", file=sys.stderr)

        # Convert rows to dict format
        results = []
        for row in rows:
            row_dict = {}
            for i, col_name in enumerate(columns):
                row_dict[col_name] = row[i] if i < len(row) else None
            results.append(row_dict)

        return results

    except subprocess.CalledProcessError as e:
        print(f"Error querying Log Analytics: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}", file=sys.stderr)
        sys.exit(1)


def get_workspace_info(subscription_id, resource_group, cluster_name, verbose=False):
    """Get Log Analytics workspace information from AKS cluster.

    Args:
        subscription_id: Azure subscription ID or name (None to use current az account)
        resource_group: Azure resource group containing the cluster
        cluster_name: AKS cluster name
        verbose: Enable debug output

    Returns:
        tuple: (workspace_id, resource_group, subscription_id)
    """

    if verbose:
        if subscription_id:
            print(f"Querying AKS cluster '{cluster_name}' in subscription '{subscription_id}', RG '{resource_group}'...", file=sys.stderr)
        else:
            print(f"Querying AKS cluster '{cluster_name}' in RG '{resource_group}' (using current az account)...", file=sys.stderr)

    # Build command
    cmd = ['az', 'aks', 'show']
    if subscription_id:
        cmd.extend(['--subscription', subscription_id])
    cmd.extend([
        '-n', cluster_name,
        '-g', resource_group,
        '--query', '{id:id, workspace:addonProfiles.omsagent.config.logAnalyticsWorkspaceResourceID}',
        '-o', 'json'
    ])

    # Query the specific cluster for its workspace
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Error: Could not find AKS cluster '{cluster_name}' in resource group '{resource_group}'", file=sys.stderr)
            print(f"Subscription: {subscription_id}", file=sys.stderr)
            print(f"Details: {result.stderr}", file=sys.stderr)
            sys.exit(1)

        cluster_info = json.loads(result.stdout)
        workspace_resource_id = cluster_info.get('workspace', '').strip()

        if not workspace_resource_id:
            print(f"Error: Cluster '{cluster_name}' does not have Container Insights (omsagent addon) configured.", file=sys.stderr)
            print("To fix this:", file=sys.stderr)
            print(f"  Enable Container Insights on the cluster via Azure Portal or CLI", file=sys.stderr)
            sys.exit(1)

        if verbose:
            print(f"Workspace resource ID: {workspace_resource_id}", file=sys.stderr)

        # Extract workspace details from resource ID
        # Format: /subscriptions/.../resourceGroups/.../providers/Microsoft.OperationalInsights/workspaces/NAME
        parts = workspace_resource_id.split('/')
        workspace_name = parts[-1]
        workspace_rg = parts[4]
        workspace_sub = parts[2]

        if verbose:
            print(f"Workspace name: {workspace_name}, RG: {workspace_rg}", file=sys.stderr)

        # Get workspace customer ID (needed for Log Analytics API)
        result = subprocess.run(
            ['az', 'monitor', 'log-analytics', 'workspace', 'show',
             '--subscription', workspace_sub,
             '-g', workspace_rg,
             '-n', workspace_name,
             '--query', 'customerId',
             '-o', 'tsv'],
            capture_output=True,
            text=True,
            check=True
        )

        workspace_id = result.stdout.strip()

        if verbose:
            print(f"Auto-discovered workspace for cluster '{cluster_name}': {workspace_id}", file=sys.stderr)

        return workspace_id, resource_group, subscription_id

    except subprocess.CalledProcessError as e:
        print(f"Error querying AKS cluster: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except (json.JSONDecodeError, IndexError, KeyError) as e:
        print(f"Error parsing cluster information: {e}", file=sys.stderr)
        sys.exit(1)


def build_query(namespace, start_time, end_time):
    """Build the KQL query for pod and node inventory.
    
    Args:
        namespace: Kubernetes namespace to filter (None for all namespaces)
        start_time: Start datetime object
        end_time: End datetime object
    """
    # Validate inputs
    if not isinstance(start_time, datetime):
        raise ValueError("start_time must be a datetime object")
    if not isinstance(end_time, datetime):
        raise ValueError("end_time must be a datetime object")
    
    # Format times for KQL
    start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    query = f"""
let startTime = datetime({start_str});
let endTime = datetime({end_str});
// Get pod inventory
let pods = KubePodInventory
| where TimeGenerated >= startTime and TimeGenerated < endTime
"""

    # Add namespace filter only if specified
    if namespace:
        # Validate namespace to prevent KQL injection
        # Kubernetes namespaces (RFC 1123): lowercase letters, numbers, hyphens, and dots
        # Must start and end with alphanumeric character
        if not namespace:
            raise ValueError("Namespace cannot be empty")
        if not (namespace[0].isalnum() and namespace[-1].isalnum()):
            raise ValueError(f"Invalid namespace: {namespace}. Must start and end with alphanumeric character.")
        if not all(c.islower() or c.isdigit() or c in '.-' for c in namespace):
            raise ValueError(f"Invalid namespace: {namespace}. Only lowercase letters, numbers, hyphens, and dots are allowed.")
        query += f"| where Namespace == '{namespace}'\n"

    query += """| summarize arg_max(TimeGenerated, *) by Name, Computer
| project TimeGenerated, RecordType="Pod", Name, Namespace, PodStatus, Computer, 
          ContainerStatus, PodCreationTimeStamp, ControllerName, ControllerKind, PodUid,
          ServiceName="", NodeStatus="", KubeletVersion="";
// Get node inventory
let nodes = KubeNodeInventory
| where TimeGenerated >= startTime and TimeGenerated < endTime
| summarize arg_max(TimeGenerated, *) by Computer
| project TimeGenerated, RecordType="Node", Name=Computer, Namespace="", PodStatus="", 
          Computer, ContainerStatus="", PodCreationTimeStamp="", ControllerName="", 
          ControllerKind="", PodUid="", ServiceName="", NodeStatus=Status, 
          KubeletVersion;
// Union both results
union pods, nodes
| sort by RecordType asc, Name asc
"""

    return query


def format_csv_output(results, metadata=None):
    """Format the query results as CSV."""

    lines = []
    
    # Add metadata as comments if provided
    if metadata:
        lines.append("# Generated by: get_pod_node_inventory.py")
        lines.append(f"# Date generated: {metadata.get('date_generated', '')}")
        lines.append(f"# Command: {metadata.get('command', '')}")
        lines.append(f"# Workspace ID: {metadata.get('workspace_id', '')}")
        lines.append(f"# Time range: {metadata.get('start_time', '')} to {metadata.get('end_time', '')}")
        if metadata.get('namespace'):
            lines.append(f"# Namespace: {metadata.get('namespace', '')}")
        else:
            lines.append("# Namespace: All")
        lines.append("#")
    
    if not results:
        return '\n'.join(lines) if lines else ""

    # CSV header - fixed column order
    headers = [
        'TimeGenerated', 'RecordType', 'Name', 'Namespace', 'PodStatus', 
        'Computer', 'ContainerStatus', 'PodCreationTimeStamp', 'ControllerName',
        'ControllerKind', 'PodUid', 'ServiceName', 'NodeStatus', 'KubeletVersion'
    ]
    lines.append(','.join(headers))

    # CSV rows
    for row in results:
        values = [str(row.get(h, '')) for h in headers]
        lines.append(','.join(values))

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='Query Kubernetes pod and node inventory from Azure Log Analytics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using cluster discovery
  %(prog)s --cluster my-aks-cluster --resource-group my-resource-group -n hpcc --start-time "2025-11-04 12:00"
  %(prog)s --cluster <cluster> --resource-group <rg> --subscription <sub> -n hpcc --start-time "2025-11-04 09:00" --end-time "2025-11-04 17:00"

  # Using workspace ID directly
  %(prog)s --workspace-id xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx -n hpcc --start-time "2025-11-04 12:00"
  %(prog)s --workspace-id <workspace-id> --all-namespaces --start-time "2025-11-04 12:00"
  %(prog)s --workspace-id <workspace-id> -n hpcc --start-time "2025-11-04 12:00" --duration 120
  %(prog)s --workspace-id <workspace-id> -n hpcc --start-time "2025-11-04 12:00" > inventory.csv
        """
    )

    # Workspace identification (mutually exclusive)
    parser.add_argument('--workspace-id', help='Log Analytics workspace ID (alternative to cluster discovery)')

    parser.add_argument('--start-time', required=True, metavar='DATETIME', help='Start time (YYYY-MM-DD or YYYY-MM-DD HH:MM)')
    parser.add_argument('--end-time', metavar='DATETIME', help='End time (YYYY-MM-DD or YYYY-MM-DD HH:MM). If specified, --duration is ignored.')

    # Namespace (mutually exclusive)
    namespace_group = parser.add_mutually_exclusive_group(required=True)
    namespace_group.add_argument('-n', '--namespace', help='Kubernetes namespace to query')
    namespace_group.add_argument('--all-namespaces', action='store_true', help='Query all namespaces')

    parser.add_argument('--duration', type=int, default=60, metavar='MINUTES',
                        help='Time window duration in minutes from start time (default: 60). Ignored if --end-time is specified.')
    parser.add_argument('--subscription', help='Azure subscription ID or name (defaults to current az account)')
    parser.add_argument('--resource-group', help='Azure resource group containing AKS cluster (required with --cluster)')
    parser.add_argument('--cluster', help='AKS cluster name (alternative to --workspace-id)')
    parser.add_argument('--verbose', action='store_true', help='Print the KQL query for debugging')

    args = parser.parse_args()

    # Validate workspace arguments - must provide either workspace-id OR cluster+resource-group
    if not args.workspace_id and not args.cluster:
        print("Error: Must provide either --workspace-id OR --cluster with --resource-group", file=sys.stderr)
        sys.exit(1)

    if args.workspace_id and args.cluster:
        print("Error: Cannot specify both --workspace-id and --cluster (use one or the other)", file=sys.stderr)
        sys.exit(1)

    # Validate cluster arguments
    if args.cluster and not args.resource_group:
        print("Error: --resource-group is required when using --cluster", file=sys.stderr)
        sys.exit(1)

    # Get namespace from args
    if args.all_namespaces:
        namespace = None  # Query all namespaces
    else:
        namespace = args.namespace

    # Parse datetime - support both date and date+time formats
    try:
        dt = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M')
    except ValueError:
        try:
            # Date only - assume start of day (00:00)
            dt = datetime.strptime(args.start_time + ' 00:00', '%Y-%m-%d %H:%M')
        except ValueError:
            print(f"Error: Invalid start-time format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM", file=sys.stderr)
            sys.exit(1)

    start_time = dt

    # Parse end time if provided
    if args.end_time:
        try:
            end_dt = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M')
        except ValueError:
            try:
                # Date only - assume end of day (23:59)
                end_dt = datetime.strptime(args.end_time + ' 23:59', '%Y-%m-%d %H:%M')
            except ValueError:
                print(f"Error: Invalid end-time format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM", file=sys.stderr)
                sys.exit(1)

        end_time = end_dt
    else:
        # Use duration
        end_time = dt + timedelta(minutes=args.duration)

    # Get workspace information
    if args.workspace_id:
        workspace_id = args.workspace_id
        resource_group = None
        subscription_id = None
    else:
        workspace_id, resource_group, subscription_id = get_workspace_info(
            subscription_id=args.subscription,
            resource_group=args.resource_group,
            cluster_name=args.cluster,
            verbose=args.verbose
        )

    if namespace:
        print(f"Querying pod and node inventory for namespace '{namespace}'", file=sys.stderr)
    else:
        print(f"Querying pod and node inventory for ALL namespaces", file=sys.stderr)
    print(f"Time window: {start_time.strftime('%Y-%m-%d %H:%M')} UTC to {end_time.strftime('%Y-%m-%d %H:%M')} UTC", file=sys.stderr)
    if subscription_id:
        print(f"Subscription: {subscription_id}", file=sys.stderr)
    print(f"Workspace: {workspace_id}", file=sys.stderr)
    print(file=sys.stderr)

    # Build and execute query
    query = build_query(namespace, start_time, end_time)

    if args.verbose:
        print("=== KQL Query ===", file=sys.stderr)
        print(query, file=sys.stderr)
        print("=================\n", file=sys.stderr)

    results = query_log_analytics(workspace_id, query, args.verbose)

    # Prepare metadata for CSV comments
    metadata = {
        'date_generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'command': ' '.join(sys.argv),
        'workspace_id': workspace_id,
        'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
        'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
        'namespace': args.namespace
    }

    # Format output as CSV
    output = format_csv_output(results, metadata=metadata)

    # Display summary
    pod_count = sum(1 for r in results if r.get('RecordType') == 'Pod')
    node_count = sum(1 for r in results if r.get('RecordType') == 'Node')
    print(f"Found {pod_count} pods and {node_count} nodes", file=sys.stderr)
    print(file=sys.stderr)

    # Print output
    print(output)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
