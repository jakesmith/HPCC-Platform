#!/usr/bin/env python3
"""
Extract Thor workunit timeline from Azure Log Analytics logs

This script queries ContainerLogV2 in Azure Log Analytics to retrieve Thor audit events
and constructs a timeline of graph executions. It produces the same CSV format as
get_thor_timeline.py for compatibility with analyze_thor_timeline.py.

Usage:
    ./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" [options]

Examples:
    # Using cluster discovery (resource group auto-discovered)
    ./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
        --subscription my-subscription --aks-cluster my-aks-cluster -c thor400

    # Using cluster discovery with explicit resource group
    ./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
        --subscription my-subscription --aks-cluster my-aks-cluster \
        --resource-group my-resource-group -c thor400

    # Using workspace ID directly
    ./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
        --workspace-id xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx -c thor400

    # Output to CSV file with debug info
    ./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
        --subscription my-subscription --aks-cluster my-aks-cluster \
        -c thor400 --format csv --debug > timeline.csv

DateTime format: YYYY-MM-DD HH:MM (UTC)

Audit Log Format:
  System events: timestamp,Progress,Thor,<Initializing|Startup|Terminate>,<cluster>,<podname>,<container>
  Job events:    timestamp,Progress,Thor,<Start|Stop>,<cluster>,<wuid>,<graphName>,<user>,<podname>,<container>
  Timing events: timestamp,Timing,ThorGraph,<cluster>,<wuid>,<graphNum>,<subgraphNum>,1,<durationMs>,<SUCCESS|FAILED>,<nodeGroup>,<queue>
"""

import sys
import json
import subprocess
import argparse
from datetime import datetime, timedelta
from collections import defaultdict


def debug_print(msg, debug=False):
    """Print debug message to stderr if debug mode is enabled."""
    if debug:
        print(f"DEBUG: {msg}", file=sys.stderr)


def query_log_analytics(workspace_id, resource_group, subscription, query, debug=False):
    """Execute a KQL query against Log Analytics workspace using REST API."""

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

    if debug:
        debug_print(f"API URI: {uri}", debug)
        debug_print(f"Executing: {' '.join(cmd)}", debug)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        if debug:
            debug_print(f"API response length: {len(result.stdout)} chars", debug)

        data = json.loads(result.stdout)

        # Extract tables from response
        tables = data.get('tables', [])
        if not tables:
            debug_print("WARNING: No tables in response!", debug)
            return []

        # Get first table (query results)
        table = tables[0]
        columns = [col['name'] for col in table.get('columns', [])]
        rows = table.get('rows', [])

        if debug:
            debug_print(f"Columns: {columns}", debug)
            debug_print(f"Number of rows: {len(rows)}", debug)

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
        print(f"Raw stdout: {result.stdout}", file=sys.stderr)
        sys.exit(1)


def find_cluster_resource_group(subscription_id, cluster_name, debug=False):
    """Find the resource group containing the AKS cluster.

    Args:
        subscription_id: Azure subscription ID or name (None to use current az account)
        cluster_name: AKS cluster name
        debug: Enable debug output

    Returns:
        str: Resource group name, or None if not found
    """
    debug_print(f"Searching for AKS cluster '{cluster_name}'...", debug)

    # Build command to list all AKS clusters
    cmd = ['az', 'aks', 'list']
    if subscription_id:
        cmd.extend(['--subscription', subscription_id])
    cmd.extend(['--query', f"[?name=='{cluster_name}'].resourceGroup", '-o', 'tsv'])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        resource_groups = [rg.strip() for rg in result.stdout.strip().split('\n') if rg.strip()]
        
        if not resource_groups:
            return None
        
        if len(resource_groups) > 1:
            debug_print(f"WARNING: Found multiple clusters named '{cluster_name}' in different resource groups: {resource_groups}", debug)
            debug_print(f"Using first match: {resource_groups[0]}", debug)
        
        debug_print(f"Found cluster '{cluster_name}' in resource group '{resource_groups[0]}'", debug)
        return resource_groups[0]

    except subprocess.CalledProcessError as e:
        debug_print(f"Error searching for cluster: {e.stderr}", debug)
        return None


def get_workspace_info(subscription_id, resource_group, cluster_name, debug=False):
    """Get Log Analytics workspace information from AKS cluster.

    Args:
        subscription_id: Azure subscription ID or name (None to use current az account)
        resource_group: Azure resource group containing the cluster (None to auto-discover)
        cluster_name: AKS cluster name
        debug: Enable debug output

    Returns:
        tuple: (workspace_id, resource_group, subscription_id)
    """

    # Auto-discover resource group if not provided
    if not resource_group:
        debug_print("Resource group not specified, searching for cluster...", debug)
        resource_group = find_cluster_resource_group(subscription_id, cluster_name, debug)
        if not resource_group:
            print(f"Error: Could not find AKS cluster '{cluster_name}'", file=sys.stderr)
            if subscription_id:
                print(f"Subscription: {subscription_id}", file=sys.stderr)
            print("Please specify the resource group with --resource-group", file=sys.stderr)
            sys.exit(1)

    if debug:
        if subscription_id:
            debug_print(f"Querying AKS cluster '{cluster_name}' in subscription '{subscription_id}', RG '{resource_group}'...", debug)
        else:
            debug_print(f"Querying AKS cluster '{cluster_name}' in RG '{resource_group}' (using current az account)...", debug)

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
            if subscription_id:
                print(f"Subscription: {subscription_id}", file=sys.stderr)
            print(f"Details: {result.stderr}", file=sys.stderr)
            sys.exit(1)

        cluster_info = json.loads(result.stdout)
        workspace_resource_id = cluster_info.get('workspace', '').strip()

        if not workspace_resource_id:
            print(f"Error: Cluster '{cluster_name}' does not have Container Insights (omsagent addon) configured.", file=sys.stderr)
            print("To fix this:", file=sys.stderr)
            print(f"  1. Go to Azure Portal -> AKS cluster '{cluster_name}'", file=sys.stderr)
            print(f"  2. Navigate to 'Insights' or 'Monitoring'", file=sys.stderr)
            print(f"  3. Enable Container Insights", file=sys.stderr)
            sys.exit(1)

        debug_print(f"Workspace resource ID: {workspace_resource_id}", debug)

        # Extract workspace customer ID from workspace resource ID
        # Format: /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.OperationalInsights/workspaces/{name}
        parts = workspace_resource_id.split('/')
        if len(parts) < 9:
            print(f"Error: Invalid workspace resource ID format: {workspace_resource_id}", file=sys.stderr)
            sys.exit(1)

        workspace_name = parts[-1]
        workspace_rg = parts[4]
        workspace_sub = parts[2]

        # Get subscription ID from workspace resource ID if not provided
        if not subscription_id:
            subscription_id = workspace_sub

        debug_print(f"Extracted workspace info - Name: {workspace_name}, RG: {workspace_rg}, Subscription: {workspace_sub}", debug)

        # Get the workspace customer ID (this is what we use for API queries)
        # Use the subscription from the workspace resource ID, not the one passed in
        cmd_workspace = ['az', 'monitor', 'log-analytics', 'workspace', 'show',
                        '--subscription', workspace_sub,
                        '-n', workspace_name, '-g', workspace_rg,
                        '--query', 'customerId', '-o', 'tsv']
        
        debug_print(f"Running: {' '.join(cmd_workspace)}", debug)
        
        result = subprocess.run(
            cmd_workspace,
            capture_output=True,
            text=True,
            check=True
        )
        workspace_id = result.stdout.strip()

        debug_print(f"Found workspace {workspace_id} (name: {workspace_name}, resource group: {workspace_rg}) for cluster '{cluster_name}'", debug)

        return workspace_id, resource_group, subscription_id

    except (subprocess.CalledProcessError, json.JSONDecodeError, IndexError) as e:
        print(f"Error getting workspace from cluster '{cluster_name}': {e}", file=sys.stderr)
        sys.exit(1)


def build_audit_query(start_time, end_time, cluster_filter=None, include_subgraphs=False):
    """Build KQL query to extract Thor audit events from ContainerLogV2."""

    # Format times for KQL
    start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Build log message filter based on whether subgraphs are needed
    if include_subgraphs:
        log_filter = 'LogMessage contains ",Progress,Thor," or LogMessage contains ",Timing,ThorGraph,"'
    else:
        log_filter = 'LogMessage contains ",Progress,Thor,"'

    query = f"""
ContainerLogV2
| where TimeGenerated >= datetime({start_str}) and TimeGenerated < datetime({end_str})
| where PodName startswith "thormanager"
| where {log_filter}
"""

    if cluster_filter:
        query += f"| where LogMessage contains ',{cluster_filter},'\n"

    query += """| project TimeGenerated, PodName, ContainerName, LogMessage
| sort by TimeGenerated asc
"""

    return query


def parse_audit_event(log_message, pod_name, container_name, timestamp):
    """Parse a Thor audit log message into structured event data.

    Returns dict with keys: event_type, cluster, wuid, graph_name, user, pod_name, container_name, timestamp
    or None if the log message doesn't match expected format.
    """

    # Split CSV fields
    parts = log_message.split(',')

    if len(parts) < 3:
        return None

    # Check for Timing events: timestamp,Timing,ThorGraph,<cluster>,<wuid>,<graphNum>,<subgraphNum>,1,<durationMs>,<SUCCESS|FAILED>,<nodeGroup>,<queue>
    if parts[1] == 'Timing' and parts[2] == 'ThorGraph':
        if len(parts) < 9:
            return None

        cluster = parts[3] if len(parts) > 3 else None
        wuid = parts[4] if len(parts) > 4 else None
        graph_num = parts[5] if len(parts) > 5 else None
        subgraph_num = parts[6] if len(parts) > 6 else None
        # parts[7] is always "1" (legacy field)
        duration_ms = int(parts[8]) if len(parts) > 8 and parts[8].isdigit() else 0
        status = parts[9] if len(parts) > 9 else 'UNKNOWN'

        return {
            'event_type': 'Timing',
            'cluster': cluster,
            'wuid': wuid,
            'graph_name': f"graph{graph_num}" if graph_num else None,
            'subgraph_id': f"sg{subgraph_num}" if subgraph_num else None,
            'duration_ms': duration_ms,
            'status': status,
            'pod_name': pod_name,
            'container_name': container_name,
            'timestamp': timestamp
        }

    # Check for Progress events
    if parts[1] != 'Progress' or parts[2] != 'Thor':
        return None

    if len(parts) < 5:
        return None

    event_type = parts[3]
    cluster = parts[4] if len(parts) > 4 else None

    # System events: Initializing, Startup, Terminate
    # Format: timestamp,Progress,Thor,<event>,<cluster>,<podname>,<container>
    if event_type in ['Initializing', 'Startup', 'Terminate']:
        return {
            'event_type': event_type,
            'cluster': cluster,
            'wuid': None,
            'graph_name': None,
            'user': None,
            'pod_name': pod_name,
            'container_name': container_name,
            'timestamp': timestamp
        }

    # Job events: Start, Stop
    # Format: timestamp,Progress,Thor,<event>,<cluster>,<wuid>,<graphName>,<user>,<podname>,<container>
    elif event_type in ['Start', 'Stop']:
        wuid = parts[5] if len(parts) > 5 else None
        graph_name = parts[6] if len(parts) > 6 else None
        user = parts[7] if len(parts) > 7 else None

        return {
            'event_type': event_type,
            'cluster': cluster,
            'wuid': wuid,
            'graph_name': graph_name,
            'user': user,
            'pod_name': pod_name,
            'container_name': container_name,
            'timestamp': timestamp
        }

    return None


def match_events(events, include_subgraphs=False, sort_by_wuid=False, debug=False):
    """Match Start/Stop events to build timeline entries and collect Timing events for subgraphs.

    Args:
        events: List of parsed audit events
        include_subgraphs: If True, include subgraph-level timing entries in output
        sort_by_wuid: If True, sort by WUID; if False, keep natural chronological order
        debug: Enable debug output

    Returns list of timeline entries (dicts with wuid, cluster, graph_name, subgraph_id, start, stop, duration).
    Timeline includes both graph-level entries (from Start/Stop) and subgraph-level entries (from Timing).
    Also tracks Thor instance lifecycle (Startup to Terminate) for future idle time analysis.
    """

    timeline = []

    # Track pending Start events by (wuid, graph_name, pod_name)
    pending_starts = {}

    # Track Thor instance lifecycles by pod_name
    thor_lifecycles = []
    pending_startups = {}

    # Track unmatched events for error reporting
    unmatched_stops = []

    # Collect Timing events for subgraphs
    subgraph_timings = []

    for event in events:
        event_type = event['event_type']

        # Handle Thor lifecycle events
        if event_type == 'Startup':
            pod_name = event['pod_name']
            pending_startups[pod_name] = event
            debug_print(f"Thor Startup: pod={pod_name} at {event['timestamp']}", debug)

        elif event_type == 'Terminate':
            pod_name = event['pod_name']
            if pod_name in pending_startups:
                startup = pending_startups.pop(pod_name)
                lifecycle = {
                    'pod_name': pod_name,
                    'cluster': event['cluster'],
                    'startup': startup['timestamp'],
                    'terminate': event['timestamp']
                }
                thor_lifecycles.append(lifecycle)
                debug_print(f"Thor Lifecycle: pod={pod_name} from {startup['timestamp']} to {event['timestamp']}", debug)
            else:
                debug_print(f"WARNING: Terminate without Startup for pod={pod_name}", debug)

        # Handle Timing events (subgraph completion)
        elif event_type == 'Timing':
            subgraph_timings.append(event)
            debug_print(f"Timing: wuid={event['wuid']} graph={event['graph_name']} subgraph={event['subgraph_id']} duration={event['duration_ms']}ms", debug)

        # Handle job events
        elif event_type == 'Start':
            key = (event['wuid'], event['graph_name'], event['pod_name'])
            pending_starts[key] = event
            debug_print(f"Start: wuid={event['wuid']} graph={event['graph_name']} pod={event['pod_name']}", debug)

        elif event_type == 'Stop':
            key = (event['wuid'], event['graph_name'], event['pod_name'])

            if key in pending_starts:
                start_event = pending_starts.pop(key)

                # Calculate duration
                start_dt = datetime.fromisoformat(start_event['timestamp'].replace('Z', '+00:00'))
                stop_dt = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
                duration = (stop_dt - start_dt).total_seconds()

                timeline.append({
                    'wuid': event['wuid'],
                    'cluster': event['cluster'],
                    'graph_name': event['graph_name'],
                    'subgraph_id': None,  # Graph-level entry has no subgraph
                    'start': start_event['timestamp'],
                    'stop': event['timestamp'],
                    'duration': duration,
                    'pod_name': event['pod_name']
                })

                debug_print(f"Matched: wuid={event['wuid']} graph={event['graph_name']} duration={duration:.1f}s", debug)
            else:
                unmatched_stops.append(event)
                debug_print(f"ERROR: Stop without Start for wuid={event['wuid']} graph={event['graph_name']} pod={event['pod_name']}", debug)

    # Handle unmatched Start events (Thor crashed/terminated before Stop)
    # Collect these first, then sort by timestamp before adding to timeline
    terminated_entries = []
    for key, start_event in pending_starts.items():
        wuid, graph_name, pod_name = key

        # Look for a Terminate event for this pod after the Start
        terminate_event = None
        for lifecycle in thor_lifecycles:
            if lifecycle['pod_name'] == pod_name:
                start_dt = datetime.fromisoformat(start_event['timestamp'].replace('Z', '+00:00'))
                terminate_dt = datetime.fromisoformat(lifecycle['terminate'].replace('Z', '+00:00'))
                if terminate_dt > start_dt:
                    terminate_event = lifecycle
                    break

        if terminate_event:
            # Use Terminate as the stop time
            duration = (terminate_dt - start_dt).total_seconds()
            terminated_entries.append({
                'wuid': wuid,
                'cluster': start_event['cluster'],
                'graph_name': graph_name,
                'subgraph_id': None,
                'start': start_event['timestamp'],
                'stop': terminate_event['terminate'],
                'duration': duration,
                'pod_name': pod_name,
                'note': 'Terminated'
            })
            debug_print(f"Matched Start with Terminate: wuid={wuid} graph={graph_name} duration={duration:.1f}s", debug)
        else:
            # No Terminate found - Thor manager died unexpectedly
            terminated_entries.append({
                'wuid': wuid,
                'cluster': start_event['cluster'],
                'graph_name': graph_name,
                'subgraph_id': None,
                'start': start_event['timestamp'],
                'stop': 'N/A',
                'duration': -1,
                'pod_name': pod_name,
                'note': 'ThorDied'
            })
            debug_print(f"WARNING: Start without Stop or Terminate for wuid={wuid} graph={graph_name} pod={pod_name}", debug)

    # Add terminated entries to timeline
    # Since terminated entries are out-of-order, add them and we'll sort the entire timeline later if needed
    timeline.extend(terminated_entries)

    # Report unmatched Stop events as errors
    for stop_event in unmatched_stops:
        print(f"ERROR: Stop without matching Start - WUID: {stop_event['wuid']}, Graph: {stop_event['graph_name']}, Pod: {stop_event['pod_name']}, Time: {stop_event['timestamp']}", file=sys.stderr)

    # Process subgraph timing events and add to timeline (if enabled)
    if include_subgraphs:
        # Deduplicate based on the actual timing data (wuid, cluster, graph, subgraph, duration, status)
        # Some HPCC Platform versions have a bug that logs the same Timing event twice
        debug_print(f"Processing {len(subgraph_timings)} subgraph timing events", debug)

        # Deduplicate subgraph timings based on unique timing signature
        seen_subgraphs = set()
        for timing_event in subgraph_timings:
            # Create key from the Timing event data (excludes timestamp which may vary slightly)
            # This matches the unique parts of the log message: wuid, cluster, graph, subgraph, duration, status
            key = (timing_event['wuid'], timing_event['cluster'], timing_event['graph_name'],
                   timing_event['subgraph_id'], timing_event['duration_ms'], timing_event['status'])

            if key in seen_subgraphs:
                debug_print(f"Skipping duplicate timing event: wuid={timing_event['wuid']} graph={timing_event['graph_name']} subgraph={timing_event['subgraph_id']} duration={timing_event['duration_ms']}ms", debug)
                continue

            seen_subgraphs.add(key)

            timeline.append({
                'wuid': timing_event['wuid'],
                'cluster': timing_event['cluster'],
                'graph_name': timing_event['graph_name'],
                'subgraph_id': timing_event['subgraph_id'],
                'start': 'N/A',  # Timing events don't have start/stop timestamps
                'stop': 'N/A',
                'duration': timing_event['duration_ms'] / 1000.0,  # Convert ms to seconds
                'pod_name': timing_event['pod_name']
            })

        debug_print(f"Added {len(seen_subgraphs)} unique subgraph entries (from {len(subgraph_timings)} timing events)", debug)
    else:
        debug_print(f"Skipping {len(subgraph_timings)} subgraph timing events (--subgraphs not enabled)", debug)

    # Store lifecycle data for future use (idle time calculation)
    # For now, just report summary
    if debug and thor_lifecycles:
        debug_print(f"Tracked {len(thor_lifecycles)} Thor instance lifecycles", debug)
        for lc in thor_lifecycles:
            startup_dt = datetime.fromisoformat(lc['startup'].replace('Z', '+00:00'))
            terminate_dt = datetime.fromisoformat(lc['terminate'].replace('Z', '+00:00'))
            uptime = (terminate_dt - startup_dt).total_seconds()
            debug_print(f"  Pod: {lc['pod_name']}, Uptime: {uptime:.1f}s", debug)

    # Sort by WUID if requested, otherwise sort chronologically by start time
    # Note: We must sort chronologically because terminated entries are added out of order
    if sort_by_wuid:
        return sorted(timeline, key=lambda x: (x['wuid'], x['graph_name'], x['subgraph_id'] or ''))
    else:
        # Sort chronologically by start time (handles 'N/A' for subgraph entries)
        return sorted(timeline, key=lambda x: (x['start'] if x['start'] != 'N/A' else 'z'))  # 'z' sorts after dates


def format_timestamp(ts_str):
    """Convert ISO timestamp to format matching get_thor_timeline.py output."""
    # Input: 2025-11-03T12:34:56.789Z
    # Output: 2025-11-03T12:34:56
    dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


def format_duration(seconds):
    """Format duration in seconds to HH:MM:SS format."""
    if seconds < 0:
        return "N/A"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def output_timeline(timeline, output_format='table', metadata=None):
    """Output the timeline in the specified format."""

    if output_format == 'csv':
        # Output metadata as comments if provided
        if metadata:
            from datetime import datetime as dt
            print("# Generated by: get_ala_thor_timeline.py")
            print(f"# Date generated: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"# Command: {metadata.get('command', '')}")
            print(f"# Workspace ID: {metadata.get('workspace_id', '')}")
            print(f"# Cluster: {metadata.get('cluster', '')}")
            print(f"# Time range: {metadata.get('start_time', '')} to {metadata.get('end_time', '')}")
            print("#")
        
        # CSV header with SubgraphID column
        print("WUID,Cluster,Phase,SubgraphID,WhenStarted,WhenFinished,Duration")

        for entry in timeline:
            wuid = entry['wuid']
            cluster = entry['cluster']
            phase = entry['graph_name']
            subgraph_id = entry.get('subgraph_id', '') or ''  # Empty string for graph-level entries

            if entry['start'] == 'N/A':
                when_started = 'N/A'
            else:
                when_started = format_timestamp(entry['start'])

            if entry['stop'] == 'N/A':
                when_finished = 'N/A'
            else:
                when_finished = format_timestamp(entry['stop'])

            # Duration is always formatted (even for subgraph entries where start/stop are N/A)
            duration = format_duration(entry['duration'])

            print(f"{wuid},{cluster},{phase},{subgraph_id},{when_started},{when_finished},{duration}")

    else:  # table format
        if not timeline:
            print("No timeline entries found.")
            return

        # Calculate column widths
        col_widths = {
            'wuid': max(len('WUID'), max(len(e['wuid']) for e in timeline)),
            'cluster': max(len('Cluster'), max(len(e['cluster']) for e in timeline)),
            'phase': max(len('Phase'), max(len(e['graph_name']) for e in timeline)),
            'subgraph': max(len('SubgraphID'), max(len(e.get('subgraph_id', '') or '') for e in timeline)),
            'start': len('WhenStarted'),
            'stop': len('WhenFinished'),
            'duration': len('Duration')
        }

        # Limit widths for readability
        col_widths['wuid'] = min(col_widths['wuid'], 20)
        col_widths['cluster'] = min(col_widths['cluster'], 15)
        col_widths['phase'] = min(col_widths['phase'], 15)
        col_widths['subgraph'] = min(col_widths['subgraph'], 12)

        # Header
        header = f"{'WUID':<{col_widths['wuid']}} | {'Cluster':<{col_widths['cluster']}} | {'Phase':<{col_widths['phase']}} | {'SubgraphID':<{col_widths['subgraph']}} | {'WhenStarted':<{col_widths['start']}} | {'WhenFinished':<{col_widths['stop']}} | {'Duration':<{col_widths['duration']}}"
        print(header)
        print('-' * len(header))

        # Rows
        for entry in timeline:
            wuid = entry['wuid'][:col_widths['wuid']]
            cluster = entry['cluster'][:col_widths['cluster']]
            phase = entry['graph_name'][:col_widths['phase']]
            subgraph = (entry.get('subgraph_id', '') or '')[:col_widths['subgraph']]

            if entry['start'] == 'N/A':
                when_started = 'N/A'
            else:
                when_started = format_timestamp(entry['start'])

            if entry['stop'] == 'N/A':
                when_finished = 'N/A'
            else:
                when_finished = format_timestamp(entry['stop'])

            # Duration is always formatted (even for subgraph entries where start/stop are N/A)
            duration = format_duration(entry['duration'])
            note = f" [{entry['note']}]" if 'note' in entry else ""

            row = f"{wuid:<{col_widths['wuid']}} | {cluster:<{col_widths['cluster']}} | {phase:<{col_widths['phase']}} | {subgraph:<{col_widths['subgraph']}} | {when_started:<{col_widths['start']}} | {when_finished:<{col_widths['stop']}} | {duration:<{col_widths['duration']}}{note}"
            print(row)


def main():
    parser = argparse.ArgumentParser(
        description='Extract Thor workunit timeline from Azure Log Analytics audit logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --subscription my-sub --aks-cluster my-cluster -c thor400 \\
      --start-time "2025-11-03" --end-time "2025-11-08"                                                     # Auto-discover resource group
  %(prog)s --subscription my-sub --aks-cluster my-cluster --resource-group my-rg -c thor400 \\
      --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00"                                         # With explicit resource group
  %(prog)s --subscription my-sub --aks-cluster my-cluster -c thor400 \\
      --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" --format csv                            # CSV format
  %(prog)s --workspace-id <workspace-id> -c thor400 \\
      --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00"                                         # Using workspace ID directly
        """
    )

    parser.add_argument('--start-time', required=True, help='Start date/time (YYYY-MM-DD or YYYY-MM-DD HH:MM)')
    parser.add_argument('--end-time', required=True, help='End date/time (YYYY-MM-DD or YYYY-MM-DD HH:MM)')
    parser.add_argument('-c', '--cluster', help='Thor cluster name to filter (optional)')
    parser.add_argument('--format', choices=['table', 'csv'], default='table',
                        help='Output format (default: table)')
    parser.add_argument('--subgraphs', action='store_true',
                        help='Include subgraph-level timing data in output (default: off)')
    parser.add_argument('--sort-by-wuid', action='store_true',
                        help='Sort output by WUID instead of chronological order (default: off)')

    # Workspace identification (mutually exclusive)
    parser.add_argument('--workspace-id', help='Log Analytics workspace ID (alternative to cluster discovery)')
    parser.add_argument('--resource-group', help='Azure resource group containing AKS cluster (optional, will auto-discover if not specified)')
    parser.add_argument('--aks-cluster', help='AKS cluster name (alternative to --workspace-id)')
    parser.add_argument('--subscription', help='Azure subscription ID or name (defaults to current az account)')

    parser.add_argument('--debug', action='store_true', help='Enable debug output to stderr')

    args = parser.parse_args()

    # Validate workspace arguments
    if not args.workspace_id and not args.aks_cluster:
        print("Error: Must provide either --workspace-id OR --aks-cluster", file=sys.stderr)
        sys.exit(1)

    if args.workspace_id and args.aks_cluster:
        print("Error: Cannot specify both --workspace-id and --aks-cluster (use one or the other)", file=sys.stderr)
        sys.exit(1)

    # Parse start time - support both date and date+time formats
    try:
        start_dt = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M')
    except ValueError:
        try:
            # Date only - assume start of day (00:00)
            start_dt = datetime.strptime(args.start_time + ' 00:00', '%Y-%m-%d %H:%M')
        except ValueError:
            print(f"Error: Invalid start time format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM", file=sys.stderr)
            sys.exit(1)

    # Parse end time
    try:
        end_dt = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M')
    except ValueError:
        try:
            # Date only - assume end of day (23:59)
            end_dt = datetime.strptime(args.end_time + ' 23:59', '%Y-%m-%d %H:%M')
        except ValueError:
            print(f"Error: Invalid end time format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM", file=sys.stderr)
            sys.exit(1)

    # Get workspace information
    if args.workspace_id:
        workspace_id = args.workspace_id
        resource_group = None
        subscription_id = None
    else:
        workspace_id, resource_group, subscription_id = get_workspace_info(
            subscription_id=args.subscription,
            resource_group=args.resource_group,
            cluster_name=args.aks_cluster,
            debug=args.debug
        )

    # Print configuration
    print(f"Querying Thor audit logs", file=sys.stderr)
    print(f"Time range: {start_dt.strftime('%Y-%m-%d %H:%M')} UTC to {end_dt.strftime('%Y-%m-%d %H:%M')} UTC", file=sys.stderr)
    if args.cluster:
        print(f"Cluster filter: {args.cluster}", file=sys.stderr)
    print(f"Workspace: {workspace_id}", file=sys.stderr)
    print(file=sys.stderr)

    # Build and execute query
    query = build_audit_query(start_dt, end_dt, args.cluster, include_subgraphs=args.subgraphs)

    if args.debug:
        debug_print("=== KQL Query ===", args.debug)
        debug_print(query, args.debug)
        debug_print("=================\n", args.debug)

    results = query_log_analytics(workspace_id, resource_group, subscription_id, query, args.debug)

    debug_print(f"Retrieved {len(results)} audit log entries", args.debug)

    # Parse audit events
    events = []
    for row in results:
        log_message = row.get('LogMessage', '')
        pod_name = row.get('PodName', '')
        container_name = row.get('ContainerName', '')
        timestamp = row.get('TimeGenerated', '')

        event = parse_audit_event(log_message, pod_name, container_name, timestamp)
        if event:
            events.append(event)
        elif args.debug:
            debug_print(f"Skipped non-audit log: {log_message[:100]}", args.debug)

    debug_print(f"Parsed {len(events)} audit events", args.debug)

    # Match Start/Stop events to build timeline
    timeline = match_events(events, include_subgraphs=args.subgraphs, sort_by_wuid=args.sort_by_wuid, debug=args.debug)

    print(f"Generated {len(timeline)} timeline entries", file=sys.stderr)
    print(file=sys.stderr)

    # Prepare metadata for CSV output
    metadata = {
        'command': ' '.join(sys.argv),
        'workspace_id': workspace_id,
        'cluster': args.cluster,
        'start_time': start_dt.strftime('%Y-%m-%d %H:%M:%S UTC'),
        'end_time': end_dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    }

    # Output results
    output_timeline(timeline, args.format, metadata=metadata)


if __name__ == '__main__':
    main()
