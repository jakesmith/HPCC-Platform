#!/usr/bin/env python3
"""
Query Azure Log Analytics for HPCC file access audit logs from ContainerLogV2.

This script retrieves and parses file access audit logs from HPCC components
(Thor, Roxie, etc.) that are logged to Azure Log Analytics. The logs contain
information about file operations (CREATE, READ, DELETED) including logical
filenames, users, plane names, and operation metadata.

Log Format:
    The audit logs contain comma-separated fields after ",FileAccess,":
    Component,Action,Plane,User,LogicalFilename,WorkunitId,GraphName,FileSize,...

Example Log Lines:
    "FileAccess,Thor,CREATED,thor40-bussvcqa,scoringqadm_svc,hpccinternal::scoringqadm_svc::temporary::jobtemp::auto7__w20251120-114829,W20251120-114829,graph1,7018028,data"
    "FileAccess,Thor,READ,thor40-bussvcqa,scoringqadm_svc,hpccinternal::scoringqadm_svc::temporary::jobtemp::auto7__w20251120-114829,W20251120-114829,graph1,87961399,1,data"
    "FileAccess,Thor,DELETED,thor400-dev,slucero_prod,thor_data400::persist::vehreg_postdid,W20251120-132321,graph1,12222471782808,data"

Requirements:
    Azure CLI must be installed and authenticated (az login)
    User must have permissions to query the Log Analytics workspace
"""

import argparse
import json
import subprocess
import sys
import csv
from datetime import datetime, timedelta
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


def get_subscription_id(subscription: str, debug: bool = False) -> Optional[str]:
    """Get subscription ID from name or validate ID.
    
    Args:
        subscription: Subscription ID or name
        debug: Enable debug output
    
    Returns:
        Subscription ID or None on error
    """
    # Try to use it as-is first (might already be an ID)
    stdout, error = run_az_command([
        'az', 'account', 'show',
        '--subscription', subscription,
        '--query', 'id',
        '--output', 'tsv'
    ], debug)
    
    if stdout:
        return stdout.strip()
    
    return None


def find_cluster_resource_group(subscription: str, aks_cluster: str, debug: bool = False) -> Optional[str]:
    """Find the resource group containing the AKS cluster.
    
    Args:
        subscription: Azure subscription ID or name
        aks_cluster: AKS cluster name
        debug: Enable debug output
    
    Returns:
        Resource group name or None on error
    """
    if debug:
        print(f"DEBUG: Searching for AKS cluster '{aks_cluster}'...", file=sys.stderr)
    
    cmd = ['az', 'aks', 'list', '--subscription', subscription,
           '--query', f"[?name=='{aks_cluster}'].resourceGroup", '-o', 'tsv']
    
    stdout, error = run_az_command(cmd, debug)
    if not stdout:
        return None
    
    resource_groups = [rg.strip() for rg in stdout.strip().split('\n') if rg.strip()]
    
    if not resource_groups:
        return None
    
    if len(resource_groups) > 1 and debug:
        print(f"DEBUG: Found multiple clusters named '{aks_cluster}', using first: {resource_groups[0]}", file=sys.stderr)
    
    if debug:
        print(f"DEBUG: Found cluster in resource group '{resource_groups[0]}'", file=sys.stderr)
    
    return resource_groups[0]


def get_log_analytics_workspace(resource_group: str, aks_cluster: str, subscription: str, debug: bool = False) -> Optional[str]:
    """Get Log Analytics workspace customer ID for the AKS cluster.
    
    Args:
        resource_group: Resource group name (None to auto-discover)
        aks_cluster: AKS cluster name
        subscription: Subscription ID
        debug: Enable debug output
    
    Returns:
        Workspace customer ID or None on error
    """
    # Auto-discover resource group if not provided
    if not resource_group:
        if debug:
            print("DEBUG: Resource group not specified, searching for cluster...", file=sys.stderr)
        resource_group = find_cluster_resource_group(subscription, aks_cluster, debug)
        if not resource_group:
            print(f"Error: Could not find AKS cluster '{aks_cluster}'", file=sys.stderr)
            return None
    
    # Get the AKS cluster's Log Analytics workspace resource ID
    cmd = ['az', 'aks', 'show',
           '--subscription', subscription,
           '-n', aks_cluster,
           '-g', resource_group,
           '--query', 'addonProfiles.omsagent.config.logAnalyticsWorkspaceResourceID',
           '--output', 'tsv']
    
    stdout, error = run_az_command(cmd, debug)
    
    if not stdout or not stdout.strip():
        print(f"Error: Cluster '{aks_cluster}' does not have Container Insights enabled", file=sys.stderr)
        return None
    
    workspace_resource_id = stdout.strip()
    if debug:
        print(f"DEBUG: Workspace resource ID: {workspace_resource_id}", file=sys.stderr)
    
    # Extract workspace details from resource ID
    # Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.OperationalInsights/workspaces/{name}
    parts = workspace_resource_id.split('/')
    if len(parts) < 9:
        print(f"Error: Invalid workspace resource ID format", file=sys.stderr)
        return None
    
    workspace_name = parts[-1]
    workspace_rg = parts[4]
    workspace_sub = parts[2]
    
    if debug:
        print(f"DEBUG: Workspace: {workspace_name}, RG: {workspace_rg}, Sub: {workspace_sub}", file=sys.stderr)
    
    # Get the workspace customer ID (used for API queries)
    cmd = ['az', 'monitor', 'log-analytics', 'workspace', 'show',
           '--subscription', workspace_sub,
           '-n', workspace_name,
           '-g', workspace_rg,
           '--query', 'customerId',
           '-o', 'tsv']
    
    stdout, error = run_az_command(cmd, debug)
    
    if not stdout:
        return None
    
    workspace_id = stdout.strip()
    if debug:
        print(f"DEBUG: Workspace customer ID: {workspace_id}", file=sys.stderr)
    
    return workspace_id


def build_kql_query(start_time: str, end_time: str, pod_filter: Optional[str] = None, limit: Optional[int] = None) -> str:
    """Build KQL query for file access logs.
    
    Args:
        start_time: Start time in ISO format
        end_time: End time in ISO format
        pod_filter: Optional pod name prefix filter
        limit: Optional limit on number of results (default: no limit)
    
    Returns:
        KQL query string
    """
    query = f"""
ContainerLogV2
| where TimeGenerated between (datetime({start_time}) .. datetime({end_time}))
| where LogMessage contains ",FileAccess,"
"""
    
    if pod_filter:
        query += f"| where PodName startswith '{pod_filter}'\n"
    
    query += """| project TimeGenerated, PodName, LogMessage
| order by TimeGenerated asc
"""
    
    if limit:
        query += f"| limit {limit}\n"
    
    return query.strip()


def parse_datetime(dt_str: str) -> str:
    """Parse datetime string and return ISO format.
    
    Args:
        dt_str: Date/time string in format YYYY-MM-DD or YYYY-MM-DD HH:MM
    
    Returns:
        ISO format datetime string
    """
    formats = [
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d'
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(dt_str, fmt)
            return dt.isoformat() + 'Z'
        except ValueError:
            continue
    
    raise ValueError(f"Invalid datetime format: {dt_str}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM")


def parse_log_message(log_message: str, timestamp: str, pod_name: str) -> Optional[Dict]:
    """Extract FileAccess CSV data from log message.
    
    Args:
        log_message: Raw log message
        timestamp: Log timestamp from query
        pod_name: Pod name
    
    Returns:
        Dict with timestamp and raw FileAccess CSV data (with internal timestamp removed), or None if parsing fails
    """
    try:
        # Find the FileAccess section
        if ',FileAccess,' not in log_message:
            return None
        
        # Extract the quoted section containing the CSV data
        # Look for the pattern: "...,FileAccess,..."
        start = log_message.find(',FileAccess,')
        if start == -1:
            return None
        
        # Find the opening quote before FileAccess
        quote_start = log_message.rfind('"', 0, start)
        if quote_start == -1:
            return None
        
        # Find the closing quote after FileAccess
        quote_end = log_message.find('"', start)
        if quote_end == -1:
            return None
        
        # Extract the entire CSV data between quotes
        csv_data = log_message[quote_start+1:quote_end]
        
        # Remove leading comma if present (empty first field in HPCC logs)
        if csv_data.startswith(','):
            csv_data = csv_data[1:]
        
        # Use the query timestamp (convert to simpler format)
        # Convert 2025-11-20T12:04:02.4019518Z to 2025-11-20 12:04:02
        log_timestamp = timestamp.replace('T', ' ').split('.')[0].replace('Z', '')
        
        return {
            'timestamp': log_timestamp,
            'file_access_data': csv_data
        }
    
    except Exception as e:
        return None


def query_logs_single(workspace_id: str, kql_query: str, debug: bool = False) -> List[Dict]:
    """Execute a single KQL query against Log Analytics workspace using REST API.
    
    Args:
        workspace_id: Log Analytics workspace customer ID
        kql_query: KQL query string
        debug: Enable debug output
    
    Returns:
        List of log entries
    """
    # Build REST API request
    uri = f"https://api.loganalytics.io/v1/workspaces/{workspace_id}/query"
    request_body = {"query": kql_query}
    
    # Execute query using az rest
    stdout, error = run_az_command([
        'az', 'rest',
        '--method', 'POST',
        '--uri', uri,
        '--body', json.dumps(request_body),
        '--headers', 'Content-Type=application/json',
        '--output', 'json'
    ], debug)
    
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
            return []
        
        # Get first table (query results)
        table = tables[0]
        columns = [col['name'] for col in table.get('columns', [])]
        rows = table.get('rows', [])
        
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


def query_logs_streaming(workspace_id: str, start_time: str, end_time: str, pod_filter: Optional[str], debug: bool = False, chunk_days: int = 3):
    """Execute KQL query against Log Analytics workspace with time-based chunking.
    Generator that yields log entries as they're retrieved to enable streaming output.
    
    Args:
        workspace_id: Log Analytics workspace customer ID
        start_time: Start time in ISO format
        end_time: End time in ISO format
        pod_filter: Optional pod name prefix filter
        debug: Enable debug output
        chunk_days: Number of days per query chunk (default 3)
    
    Yields:
        Individual log entry dicts
    """
    # Parse time strings
    start_dt = datetime.fromisoformat(start_time.replace('Z', ''))
    end_dt = datetime.fromisoformat(end_time.replace('Z', ''))
    
    # Calculate total duration
    total_duration = end_dt - start_dt
    
    if debug:
        print(f"DEBUG: Querying from {start_time} to {end_time} ({total_duration.days} days)", file=sys.stderr)
        print(f"DEBUG: Splitting into {chunk_days}-day chunks", file=sys.stderr)
    
    total_count = 0
    current_start = start_dt
    chunk_num = 1
    
    while current_start < end_dt:
        # Calculate chunk end (min of chunk_days from start or final end_dt)
        current_end = min(current_start + timedelta(days=chunk_days), end_dt)
        
        # Format times for KQL
        chunk_start_str = current_start.isoformat() + 'Z'
        chunk_end_str = current_end.isoformat() + 'Z'
        
        if debug:
            print(f"DEBUG: Chunk {chunk_num}: {chunk_start_str} to {chunk_end_str}", file=sys.stderr)
        
        # Build query for this time chunk
        kql_query = build_kql_query(chunk_start_str, chunk_end_str, pod_filter)
        
        if debug and chunk_num == 1:
            print(f"DEBUG: Sample KQL Query:\n{kql_query}\n", file=sys.stderr)
        
        # Execute query
        chunk_results = query_logs_single(workspace_id, kql_query, debug)
        
        chunk_count = len(chunk_results)
        total_count += chunk_count
        
        if debug:
            print(f"DEBUG: Chunk {chunk_num}: Retrieved {chunk_count} rows (total so far: {total_count})", file=sys.stderr)
        
        # Yield each result immediately (streaming)
        for result in chunk_results:
            yield result
        
        # Move to next chunk
        current_start = current_end
        chunk_num += 1
    
    if debug:
        print(f"DEBUG: Total rows retrieved across all chunks: {total_count}", file=sys.stderr)


def format_output(entries: List[Dict]) -> None:
    """Output log entries as CSV with timestamp and raw FileAccess data.
    
    Args:
        entries: List of parsed log entries
    """
    if not entries:
        print("No file access logs found.", file=sys.stderr)
        return
    
    # Output raw CSV format: timestamp,<entire FileAccess CSV section>
    for entry in entries:
        print(f"{entry['timestamp']},{entry['file_access_data']}")


def main():
    parser = argparse.ArgumentParser(
        description='Query Azure Log Analytics for HPCC file access audit logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Query logs for last hour
  %(prog)s --subscription my-sub --aks-cluster my-aks \\
    --start-time "2025-11-20 11:00" --end-time "2025-11-20 12:00"
  
  # Query logs with pod filter
  %(prog)s --subscription my-sub --aks-cluster my-aks \\
    --start-time "2025-11-20" --end-time "2025-11-21" --filter thor400
  
  # Save to file
  %(prog)s --subscription my-sub --aks-cluster my-aks \\
    --start-time "2025-11-20" --end-time "2025-11-21" > file_access.csv
  
  # Debug mode
  %(prog)s --subscription my-sub --aks-cluster my-aks \\
    --start-time "2025-11-20" --end-time "2025-11-21" --debug
        '''
    )
    
    parser.add_argument(
        '--subscription',
        required=True,
        help='Azure subscription ID or name'
    )
    
    parser.add_argument(
        '--resource-group',
        help='Azure resource group containing AKS cluster (optional, will auto-discover)'
    )
    
    parser.add_argument(
        '--aks-cluster',
        required=True,
        help='AKS cluster name'
    )
    
    parser.add_argument(
        '--start-time',
        required=True,
        help='Start date/time (YYYY-MM-DD or YYYY-MM-DD HH:MM)'
    )
    
    parser.add_argument(
        '--end-time',
        required=True,
        help='End date/time (YYYY-MM-DD or YYYY-MM-DD HH:MM)'
    )
    
    parser.add_argument(
        '--filter',
        dest='pod_filter',
        help='Pod name prefix filter (e.g., thor400)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug output to stderr (logs KQL commands)'
    )
    
    args = parser.parse_args()
    
    # Parse datetime strings
    try:
        start_time = parse_datetime(args.start_time)
        end_time = parse_datetime(args.end_time)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    
    # Get subscription ID
    subscription_id = get_subscription_id(args.subscription, args.debug)
    if not subscription_id:
        print(f"Error: Could not find subscription '{args.subscription}'", file=sys.stderr)
        return 1
    
    if args.debug:
        print(f"DEBUG: Using subscription: {subscription_id}", file=sys.stderr)
    
    # Get Log Analytics workspace
    workspace_id = get_log_analytics_workspace(
        args.resource_group,
        args.aks_cluster,
        subscription_id,
        args.debug
    )
    
    if not workspace_id:
        print(f"Error: Could not find Log Analytics workspace for AKS cluster '{args.aks_cluster}'", 
              file=sys.stderr)
        return 1
    
    # Output metadata header (with placeholder for record count)
    print("# Generated by: get_file_access_logs.py")
    print(f"# Date generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Command: get_file_access_logs.py --subscription {args.subscription} --aks-cluster {args.aks_cluster}", end='')
    if args.resource_group:
        print(f" --resource-group {args.resource_group}", end='')
    print(f" --start-time \"{args.start_time}\" --end-time \"{args.end_time}\"", end='')
    if args.pod_filter:
        print(f" --filter {args.pod_filter}", end='')
    print()
    print(f"# Workspace ID: {workspace_id}")
    print(f"# Time range: {start_time} to {end_time}")
    if args.pod_filter:
        print(f"# Pod filter: {args.pod_filter}")
    print("#")
    
    # Stream results: parse and output each log entry as it's retrieved
    parsed_count = 0
    raw_count = 0
    
    for log in query_logs_streaming(workspace_id, start_time, end_time, args.pod_filter, args.debug):
        raw_count += 1
        timestamp = log.get('TimeGenerated', '')
        pod_name = log.get('PodName', '')
        log_message = log.get('LogMessage', '')
        
        entry = parse_log_message(log_message, timestamp, pod_name)
        if entry:
            # Output immediately (streaming)
            print(f"{entry['timestamp']},{entry['file_access_data']}")
            parsed_count += 1
    
    if args.debug:
        print(f"DEBUG: Retrieved {raw_count} raw log entries", file=sys.stderr)
        print(f"DEBUG: Parsed {parsed_count} file access log entries", file=sys.stderr)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
