# Azure Data Collection and Analysis Tools

Tools for collecting and analyzing Azure cost, storage, and workload data. All collectors query Azure services and output standardized CSV format for downstream analysis.

## Directory Overview

This directory contains scripts that interact with Azure services to collect operational data:

- **Data Collectors** (`get_*.py`) - Fetch data from Azure APIs and Log Analytics
- **Data Analyzers** (`analyze_*.py`) - Process and format collected data
- **Documentation** - Usage guides and examples

## Table of Contents

- [Data Collectors](#data-collectors)
  - [get_costs.py](#get_costspy) - Azure Cost Management API
  - [get_storage_usage.py](#get_storage_usagepy) - Storage metrics from Azure Monitor
  - [get_pod_inventory.py](#get_pod_inventorypy) - Kubernetes pod inventory from Log Analytics (legacy)
  - [get_pod_node_inventory.py](#get_pod_node_inventorypy) - Kubernetes pod and node inventory from Log Analytics
  - [get_ala_thor_timeline.py](#get_ala_thor_timelinepy) - Thor workunit timeline from Log Analytics
  - [get_vm_pricing.py](#get_vm_pricingpy) - Azure VM pricing from Retail Prices API
- [Data Analyzers](#data-analyzers)
  - [analyze_costs.py](#analyze_costspy) - Cost breakdown and visualization
  - [analyze_storage_usage.py](#analyze_storage_usagepy) - Storage usage analysis
  - [analyze_pod_node_inventory.py](#analyze_pod_node_inventorypy) - Pod and node inventory analysis with HPCC component identification
  - [analyze_thor_timeline.py](#analyze_thor_timelinepy) - Thor timeline utilization and cost modeling
- [Quick Start](#quick-start)
- [Authentication](#authentication)
- [Common Workflows](#common-workflows)

---

## Data Collectors

### get_costs.py

Fetch Azure costs from the Cost Management API and output as CSV.

#### Purpose

Query the Azure Cost Management API to retrieve cost data for a specified date range, with optional filtering and grouping dimensions. This is the primary tool for cost data collection.

#### When to Use

- Fetching daily cost data for analysis
- Extracting storage-specific costs by subcategory
- Retrieving VM costs by SKU
- Generating cost datasets for trending and correlation analysis
- Pipeline cost data to downstream analyzers

#### Features

- Queries Azure Cost Management API via Azure CLI
- Supports multiple grouping dimensions (category, meter, subcategory)
- Optional filtering by service type (storage, vm, compute)
- Automatic retry with exponential backoff for rate limiting (429 errors)
- Outputs standardized CSV format to stdout
- No formatting logic - pure data collection
- Subscription name or ID resolution

#### Usage

```bash
# Basic usage - fetch all costs for a date range
./get_costs.py --subscription <sub-id> --start-date 2025-10-28 --end-date 2025-11-04 > costs.csv

# Single day
./get_costs.py --subscription <sub-id> --start-date 2025-11-04 > daily_costs.csv

# Detailed storage costs by subcategory
./get_costs.py --subscription <sub-id> --start-date 2025-10-28 --end-date 2025-11-04 \
  --grouping subcategory --filter storage > storage_costs.csv

# VM costs by specific meter (SKU)
./get_costs.py --subscription <sub-id> --start-date 2025-10-28 --end-date 2025-11-04 \
  --grouping meter --filter vm > vm_costs.csv

# Compute costs (broader than just VMs)
./get_costs.py --subscription <sub-id> --start-date 2025-10-28 --end-date 2025-11-04 \
  --grouping meter --filter compute > compute_costs.csv

# Pipeline directly to analyzer
./get_costs.py --subscription <sub-id> --start-date 2025-10-28 --end-date 2025-11-04 | \
  ./analyze_costs.py -
```

#### Command-Line Options

**Required Arguments:**
- `--subscription ID` - Azure subscription ID or name
- `--start-date DATE` - Start date in YYYY-MM-DD format

**Optional Arguments:**
- `--end-date DATE` - End date in YYYY-MM-DD format (defaults to tomorrow)
- `--grouping {category,meter,subcategory}` - Grouping dimension (default: category)
  - `category` - High-level service categories (e.g., "Virtual Machines", "Storage")
  - `meter` - Specific meters/SKUs (e.g., "D48s v3", "Premium SSD Managed Disks")
  - `subcategory` - Service subcategories (e.g., "Files", "Blobs", "Disks")
- `--filter {storage,vm,compute}` - Filter by service type (optional)
- `--retry-delay SECONDS` - Retry delay for rate limiting in seconds (default: 2)

#### Output Format

CSV to stdout with the following columns:

```csv
Cost,Date,Category,Currency
123.45,20251104,Premium SSD Managed Disks,USD
67.89,20251104,Files,USD
```

#### Dependencies

- Python 3.7+
- Azure CLI (`az`) installed and authenticated
- Standard library modules only

#### Required Permissions

- Cost Management Reader role on the subscription, OR
- Billing Reader role on the subscription

---

### get_storage_usage.py

Get storage usage metrics from Azure Monitor over a time period.

#### Purpose

Query Azure Monitor for storage account capacity metrics to correlate storage usage with costs. Supports hourly granularity for detailed tracking.

#### When to Use

- Comparing storage usage trends with storage costs
- Tracking storage growth over time
- Identifying storage accounts with high capacity usage
- Generating usage datasets for capacity planning
- Correlating storage usage with application activity

#### Features

- Queries Azure Monitor Metrics API for storage capacity
- Supports hourly granularity
- Retrieves multiple capacity types (Used, Blob, File, Table, Queue)
- Resource group filtering
- Single account filtering
- CSV output for time-series analysis

#### Usage

```bash
# Current snapshot (last 24 hours)
./get_storage_usage.py --subscription <sub-id>

# Specific time range with hourly data
./get_storage_usage.py --subscription <sub-id> --start-time "2025-11-01 00:00" --end-time "2025-11-08 00:00"

# Specific resource group
./get_storage_usage.py --subscription <sub-id> --resource-group my-rg --start-time "2025-11-01 00:00"

# Single storage account
./get_storage_usage.py --subscription <sub-id> --account mystorageaccount --start-time "2025-11-01"

# Pipeline to analyzer
./get_storage_usage.py --subscription <sub-id> --start-time "2025-11-01" --end-time "2025-11-07" | \
  ./analyze_storage_usage.py -
```

#### Command-Line Options

**Required Arguments:**
- `--subscription ID` - Azure subscription ID or name

**Optional Arguments:**
- `--start-time DATETIME` - Start time (YYYY-MM-DD or YYYY-MM-DD HH:MM), defaults to 24 hours ago
- `--end-time DATETIME` - End time (YYYY-MM-DD or YYYY-MM-DD HH:MM), defaults to now
- `--resource-group NAME` - Filter to specific resource group
- `--account NAME` - Filter to specific storage account name

#### Output Format

CSV to stdout with the following columns:

```csv
Timestamp,StorageAccount,ResourceGroup,UsedCapacityGB,BlobCapacityGB,FileCapacityGB,TableCapacityGB,QueueCapacityGB
2025-11-01T00:00:00Z,myaccount,my-rg,1234.56,1200.00,34.56,0.00,0.00
```

#### Dependencies

- Python 3.7+
- Azure CLI (`az`) installed and authenticated
- Standard library modules only

#### Required Permissions

- Monitoring Reader role on storage accounts, OR
- Reader role on storage accounts

---

### get_pod_inventory.py

Get Kubernetes pod inventory from Azure Log Analytics.

#### Purpose

Query the `KubePodInventory` table in Azure Log Analytics to retrieve information about pods running in specific namespaces at given times. This is an **Azure-specific** tool that uses Azure Log Analytics, not direct Kubernetes API calls.

#### When to Use

- Inventorying pods running during a specific time window
- Correlating pod counts with cost data
- Identifying which pods were running during cost spikes
- Exporting pod metadata for offline analysis
- Debugging cluster state at specific points in time

#### Features

- Queries `KubePodInventory` table via Azure Log Analytics REST API
- Supports workspace ID or AKS cluster discovery for workspace lookup
- Namespace filtering or all-namespaces mode
- Flexible time window specification (start/end or start+duration)
- Multiple output formats (table, CSV, JSON)
- Pod status filtering (Running, Pending, Failed, etc.)
- Result limiting for large clusters
- Time zone support (EST/EDT or UTC)

#### Usage

```bash
# Using workspace ID directly
./get_pod_inventory.py --workspace-id xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  -n hpcc --start-time "2025-11-04 12:00"

# Using cluster discovery (with current az subscription)
./get_pod_inventory.py --cluster my-aks-cluster \
  --resource-group my-resource-group -n hpcc --start-time "2025-11-04 12:00"

# Using cluster discovery with explicit subscription
./get_pod_inventory.py --cluster my-aks-cluster \
  --resource-group my-resource-group --subscription my-subscription \
  -n hpcc --start-time "2025-11-04 09:00" --end-time "2025-11-04 17:00"

# Query all namespaces
./get_pod_inventory.py --workspace-id <workspace-id> \
  --all-namespaces --start-time "2025-11-04 12:00"

# Filter running pods only
./get_pod_inventory.py --workspace-id <workspace-id> \
  -n hpcc --start-time "2025-11-04 12:00" --status Running --limit 50

# Export to JSON
./get_pod_inventory.py --workspace-id <workspace-id> \
  -n hpcc --start-time "2025-11-04 12:00" --format json > pods.json

# Use UTC time (default is EST/EDT)
./get_pod_inventory.py --workspace-id <workspace-id> \
  -n hpcc --start-time "2025-11-04 17:00" --utc
```

#### Command-Line Options

**Required Arguments:**
- `--start-time DATETIME` - Start time in YYYY-MM-DD HH:MM format
- Either `-n, --namespace NAME` or `--all-namespaces` - Namespace to query
- Either `--workspace-id ID` or `--cluster NAME` with `--resource-group RG`

**Workspace Identification (choose one):**
- `--workspace-id ID` - Log Analytics workspace ID (customer ID) directly
- `--cluster NAME` - AKS cluster name (discovers workspace from cluster)
  - Requires: `--resource-group RG`
  - Optional: `--subscription ID`

**Optional Arguments:**
- `--end-time DATETIME` - End time (YYYY-MM-DD HH:MM)
- `--utc` - Treat times as UTC (default is EST/EDT)
- `--duration MINUTES` - Time window in minutes from start (default: 60)
- `--status {Running,Pending,Succeeded,Failed,Terminating}` - Filter by pod status
- `--limit N` - Limit number of results
- `--format {table,json,csv}` - Output format (default: table)
- `--verbose` - Print KQL query for debugging

#### Output Format

**Table (default):**
```
Name                                           | Status  | Node                    | Container | Created
-----------------------------------------------------------------------------------------------------------------
hpcc-dali-0                                    | Running | aks-agentpool-12345     | ready     | 2025-11-01T08:30:00Z
```

**CSV:**
```csv
Name,PodStatus,Computer,ContainerStatus,PodCreationTimeStamp
hpcc-dali-0,Running,aks-agentpool-12345,ready,2025-11-01T08:30:00Z
```

**JSON:**
```json
[{"Name": "hpcc-dali-0", "PodStatus": "Running", ...}]
```

#### Dependencies

- Python 3.7+
- Azure CLI (`az`) installed and authenticated
- Container Insights enabled on AKS cluster (if using cluster discovery)
- Standard library modules only

#### Required Permissions

- Reader role on the AKS cluster resource (if using cluster discovery)
- Log Analytics Reader role on the workspace

---

### get_pod_node_inventory.py

Get Kubernetes pod and node inventory from Azure Log Analytics.

#### Purpose

Query both `KubePodInventory` and `KubeNodeInventory` tables in Azure Log Analytics to retrieve comprehensive information about pods and nodes in a namespace during a given time range. This tool provides unified pod and node data for component-level resource analysis.

#### When to Use

- Analyzing HPCC component resource consumption
- Cross-referencing pods to nodes for capacity planning
- Understanding which components are using which nodes
- Generating data for component-level cost attribution
- Investigating resource allocation patterns

#### Features

- Queries both `KubePodInventory` and `KubeNodeInventory` tables via Azure Log Analytics REST API
- Supports workspace ID or AKS cluster discovery for workspace lookup
- Namespace filtering or all-namespaces mode
- Flexible time window specification (start/end or start+duration)
- CSV output format with metadata headers
- Comprehensive validation (RFC 1123 namespace names, datetime validation)
- KQL injection protection

#### Usage

```bash
# Using workspace ID directly
./get_pod_node_inventory.py --workspace-id xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx \
  -n hpcc --start-time "2025-11-04 12:00"

# Using cluster discovery
./get_pod_node_inventory.py --cluster my-aks-cluster \
  --resource-group my-resource-group -n hpcc --start-time "2025-11-04 12:00"

# With explicit time range
./get_pod_node_inventory.py --workspace-id <workspace-id> \
  -n hpcc --start-time "2025-11-04 09:00" --end-time "2025-11-04 17:00"

# Query all namespaces
./get_pod_node_inventory.py --workspace-id <workspace-id> \
  --all-namespaces --start-time "2025-11-04 12:00"

# Save to CSV file
./get_pod_node_inventory.py --workspace-id <workspace-id> \
  -n hpcc --start-time "2025-11-04 12:00" > inventory.csv
```

#### Command-Line Options

**Required Arguments:**
- `--start-time DATETIME` - Start time (YYYY-MM-DD or YYYY-MM-DD HH:MM)
- Either `-n, --namespace NAME` or `--all-namespaces` - Namespace to query
- Either `--workspace-id ID` or `--cluster NAME` with `--resource-group RG`

**Workspace Identification (choose one):**
- `--workspace-id ID` - Log Analytics workspace ID (customer ID) directly
- `--cluster NAME` - AKS cluster name (discovers workspace from cluster)
  - Requires: `--resource-group RG`
  - Optional: `--subscription ID`

**Optional Arguments:**
- `--end-time DATETIME` - End time (YYYY-MM-DD or YYYY-MM-DD HH:MM)
- `--duration MINUTES` - Time window in minutes from start (default: 60)
- `--verbose` - Print KQL query for debugging

#### Output Format

CSV format with metadata header:
```csv
# Generated by: get_pod_node_inventory.py
# Date generated: 2025-12-12 09:00:00
# Workspace ID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
# Time range: 2025-11-04 12:00:00 UTC to 2025-11-04 14:00:00 UTC
# Namespace: hpcc
#
TimeGenerated,RecordType,Name,Namespace,PodStatus,Computer,ContainerStatus,...
2025-11-04T12:00:00Z,Pod,hpcc-dali-0,hpcc,Running,aks-node-123,ready,...
2025-11-04T12:00:00Z,Node,aks-node-123,,,aks-node-123,,,,...
```

#### Dependencies

- Python 3.7+
- Azure CLI (`az`) installed and authenticated
- Container Insights enabled on AKS cluster (if using cluster discovery)
- Standard library modules only

#### Required Permissions

- Reader role on the AKS cluster resource (if using cluster discovery)
- Log Analytics Reader role on the workspace

---

### get_ala_thor_timeline.py

Extract Thor workunit timeline from Azure Log Analytics audit logs.

#### Purpose

Query `ContainerLogV2` in Azure Log Analytics to retrieve Thor audit events and construct a timeline of graph executions.

#### When to Use

- Building timeline of Thor workunit executions for cost attribution
- Correlating Thor job duration with compute costs
- Identifying long-running graphs for optimization
- Tracking Thor cluster utilization over time
- Debugging workunit execution issues via audit trail
- Analyzing subgraph performance within graphs

#### Features

- Queries `ContainerLogV2` table via Azure Log Analytics REST API
- Parses Thor audit events (Progress and Timing events)
- Matches Start/Stop events to calculate graph durations
- Extracts Timing events for subgraph-level durations
- Handles Thor manager lifecycle (Startup/Terminate)
- Supports workspace ID or AKS cluster discovery
- Cluster filtering for multi-cluster environments
- Multiple output formats (table, CSV)
- Debug mode for troubleshooting event matching
- Deduplicates duplicate Timing events (HPCC Platform bug workaround)

#### Usage

```bash
# Using workspace ID directly
./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
  --workspace-id xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx -c thor400

# Using cluster discovery (with current az subscription)
./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
  --aks-cluster my-aks-cluster --resource-group my-resource-group -c thor400

# Using cluster discovery with explicit subscription
./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
  --aks-cluster my-aks-cluster --resource-group my-resource-group \
  --subscription my-subscription -c thor400

# Output to CSV file with debug info
./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
  --workspace-id <workspace-id> -c thor400 --format csv --debug > timeline.csv

# Query all Thor clusters (no filter)
./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
  --workspace-id <workspace-id> --format csv
```

#### Command-Line Options

**Required Arguments:**
- `--start-time DATETIME` - Start date/time in YYYY-MM-DD HH:MM format (UTC)
- `--end-time DATETIME` - End date/time in YYYY-MM-DD HH:MM format (UTC)
- Either `--workspace-id ID` or `--aks-cluster NAME` with `--resource-group RG`

**Workspace Identification (choose one):**
- `--workspace-id ID` - Log Analytics workspace ID (customer ID) directly
- `--aks-cluster NAME` - AKS cluster name (discovers workspace from cluster)
  - Requires: `--resource-group RG`
  - Optional: `--subscription ID`

**Optional Arguments:**
- `-c, --cluster NAME` - Thor cluster name to filter (optional, queries all clusters if omitted)
- `--format {table,csv}` - Output format (default: table)
- `--debug` - Enable debug output to stderr

#### Audit Log Format

The script parses these Thor audit event types:

**System events:**
```
timestamp,Progress,Thor,<Initializing|Startup|Terminate>,<cluster>,<podname>,<container>
```

**Job events:**
```
timestamp,Progress,Thor,<Start|Stop>,<cluster>,<wuid>,<graphName>,<user>,<podname>,<container>
```

**Timing events:**
```
timestamp,Timing,ThorGraph,<cluster>,<wuid>,<graphNum>,<subgraphNum>,1,<durationMs>,<SUCCESS|FAILED>,<nodeGroup>,<queue>
```

#### Output Format

**CSV:**
```csv
WUID,Cluster,Phase,SubgraphID,WhenStarted,WhenFinished,Duration
W20251103-120000,thor400,graph1,,2025-11-03T12:00:15,2025-11-03T12:05:30,00:05:15
W20251103-120000,thor400,graph1,sg0,N/A,N/A,00:00:45
W20251103-120000,thor400,graph1,sg1,N/A,N/A,00:04:30
```

**Table:**
```
WUID                 | Cluster  | Phase    | SubgraphID | WhenStarted         | WhenFinished        | Duration
-----------------------------------------------------------------------------------------------------------------------
W20251103-120000     | thor400  | graph1   |            | 2025-11-03T12:00:15 | 2025-11-03T12:05:30 | 00:05:15
W20251103-120000     | thor400  | graph1   | sg0        | N/A                 | N/A                 | 00:00:45
```

**Notes:**
- Graph-level entries (from Start/Stop events) have timestamps and empty SubgraphID
- Subgraph entries (from Timing events) have SubgraphID and N/A timestamps (duration only)
- Duration is always in HH:MM:SS format

#### Event Matching Logic

1. **Start/Stop Matching**: Pairs Start and Stop events by (WUID, graph, pod) to calculate graph duration
2. **Unmatched Starts**: If Thor terminated before Stop, uses Terminate event as end time
3. **Thor Died**: If no Stop or Terminate found, marks duration as -1 with note "ThorDied"
4. **Timing Deduplication**: Deduplicates duplicate Timing events (some HPCC versions log duplicates)
5. **Lifecycle Tracking**: Tracks Thor manager Startup→Terminate for future idle time analysis

#### Dependencies

- Python 3.7+
- Azure CLI (`az`) installed and authenticated
- Container Insights enabled on AKS cluster (if using cluster discovery)
- Standard library modules only

#### Required Permissions

- Reader role on the AKS cluster resource (if using cluster discovery)
- Log Analytics Reader role on the workspace

---

### get_vm_pricing.py

Query Azure VM pricing from the Retail Prices API.

#### Purpose

Retrieve current retail pricing for Azure Virtual Machines by SKU name and region. No authentication required - uses public Azure Retail Prices API.

#### When to Use

- Getting current VM pricing for cost modeling
- Comparing Linux vs Windows pricing
- Validating cost parameters in analysis scripts
- Checking pricing across different Azure regions
- Finding Dev/Test or Reserved Instance pricing

#### Features

- Queries public Azure Retail Prices API (no auth required)
- Filters by SKU name, region, and OS type
- Supports multiple pricing types (Consumption, DevTestConsumption, Reservation)
- Multiple output formats (text, CSV, JSON)
- Optional VM specification display
- Lists common Azure regions in help

#### Usage

```bash
# Get Linux pricing for Standard_D48ds_v4 in East US
./get_vm_pricing.py --sku Standard_D48ds_v4 --region eastus

# Get Windows pricing
./get_vm_pricing.py --sku Standard_D48ds_v4 --region eastus --os windows

# CSV format
./get_vm_pricing.py --sku Standard_D48ds_v4 --region eastus --format csv

# JSON format with all details
./get_vm_pricing.py --sku Standard_D48ds_v4 --region eastus --format json

# Different region
./get_vm_pricing.py --sku Standard_E16s_v3 --region westeurope

# Dev/Test pricing
./get_vm_pricing.py --sku Standard_D48ds_v4 --region eastus --pricing-type DevTestConsumption
```

#### Command-Line Options

**Required Arguments:**
- `--sku NAME` - VM SKU name (e.g., Standard_D48ds_v4, Standard_E16s_v3)
- `--region NAME` - Azure region name (e.g., eastus, westus, northeurope)

**Optional Arguments:**
- `--os {linux,windows}` - Operating system type (default: linux)
- `--pricing-type {Consumption,DevTestConsumption,Reservation}` - Pricing model (default: Consumption)
- `--format {text,json,csv}` - Output format (default: text)
- `--show-spec` - Show VM specifications (cores, RAM, disk)

**Common Azure Regions:**
- `eastus`, `westus`, `centralus`
- `eastus2`, `westus2`
- `northeurope`, `westeurope`
- `uksouth`, `ukwest`
- `southeastasia`, `eastasia`
- `japaneast`, `japanwest`
- `australiaeast`, `australiasoutheast`

#### Output Format

**Text (default):**
```
Virtual Machines Ddsv4 Series
  SKU:      Standard_D48ds_v4
  Region:   eastus
  OS:       Linux
  Type:     Consumption
  Price:    USD $0.5420/1 Hour
```

**CSV:**
```csv
SKU,Region,OS,Type,Price,Unit,ProductName
Standard_D48ds_v4,eastus,Linux,Consumption,0.5420,1 Hour,Virtual Machines Ddsv4 Series
```

**JSON:**
```json
[
  {
    "armSkuName": "Standard_D48ds_v4",
    "armRegionName": "eastus",
    "productName": "Virtual Machines Ddsv4 Series",
    "retailPrice": 0.542,
    "unitOfMeasure": "1 Hour",
    "type": "Consumption",
    "currencyCode": "USD"
  }
]
```

#### Notes

- Returns multiple pricing entries if multiple meter types exist for the SKU (e.g., different commitment tiers)
- No Azure CLI required - uses public API endpoint
- Pricing is in USD by default
- Linux pricing is typically lower than Windows for the same SKU
- Dev/Test pricing available for eligible subscriptions

#### Dependencies

- Python 3.7+
- Standard library modules only (urllib, json)
- No Azure CLI or authentication required

---

## Data Analyzers

### analyze_costs.py

Analyze Azure cost CSV data and display formatted breakdown.

#### Purpose

Read cost CSV data (from `get_costs.py` or compatible source) and display formatted cost breakdowns with various focus options, aggregations, and presentation styles.

#### When to Use

- Analyzing daily cost trends across VM, Storage, and Other categories
- Identifying top storage subcategories by cost
- Analyzing VM costs by SKU
- Generating cost reports for specific date ranges
- Comparing costs across different time periods
- Investigating cost anomalies or spikes

#### Features

- Reads cost CSV from file or stdin (pipeline-friendly)
- Multiple output formats: overall breakdown, storage focus, compute focus
- Extended meter-level breakdown for detailed analysis
- Daily or aggregated views
- Percentage breakdowns
- Top N subcategory analysis
- No external dependencies (works offline on saved CSV files)

#### Usage

```bash
# Basic usage - overall cost breakdown
./analyze_costs.py costs.csv

# Focus on storage subcategories (top 5)
./analyze_costs.py storage_costs.csv --focus storage

# Extended storage breakdown (requires meter-level data)
./analyze_costs.py storage_costs.csv --focus storage --ext

# Focus on VM/compute details (top 6 SKUs)
./analyze_costs.py vm_costs.csv --focus compute

# Show daily breakdown instead of aggregated
./analyze_costs.py costs.csv --daily

# Read from stdin (pipeline)
./get_costs.py --subscription <sub> --start-date 2025-10-28 --end-date 2025-11-04 | \
  ./analyze_costs.py -

# Combine collection and analysis in one command
./get_costs.py --subscription <sub> --start-date 2025-11-01 --end-date 2025-11-08 \
  --grouping subcategory --filter storage | \
  ./analyze_costs.py - --focus storage
```

#### Command-Line Options

**Positional Arguments:**
- `input_file` - CSV file path, or `-` for stdin (required)

**Optional Arguments:**
- `--focus {storage,compute}` - Detail level for output
- `--ext` - Show extended meter-level breakdown (requires meter-level data)
- `--daily` - Show each day separately (default: aggregate across date range)
- `--format {text,csv}` - Output format (default: text)
- `--start-date DATE` - Start date filter (YYYY-MM-DD)
- `--end-date DATE` - End date filter (YYYY-MM-DD)

#### Output Formats

**Overall Breakdown (default):**
```
Date         VMs (Nodes)     Storage         Other           Total
===========================================================================
2025-11-04    $    1,234.56  $      234.78  $       89.12  $    1,558.46
2025-11-05    $    1,245.67  $      238.90  $       91.23  $    1,575.80
===========================================================================
TOTAL         $    2,480.23  $      473.68  $      180.35  $    3,134.26

Cost Breakdown:
  VMs (Nodes): 79.1%
  Storage:     15.1%
  Other:        5.8%
```

**Storage Focus (--focus storage):**
```
Date         Premium SSD   Premium Files  Block Blob HN  Standard SSD  Standard HDD  Other           Total
===========================================================================================================
2025-11-04   $      123.45 $       67.89 $       43.44  $       15.23 $        8.77  $         0.00  $      234.78
```

**Compute Focus (--focus compute):**
```
Date         D48s_v3        D64s_v3        L32s_v2        E16s_v3        D16s_v3        D8s_v3         Other           Total
===========================================================================================================================
2025-11-04   $      789.12 $      345.67 $       99.77  $       87.54  $       54.32  $       43.21  $         0.00  $    1,234.56
```

#### Dependencies

- Python 3.7+
- Standard library modules only (csv, sys, argparse)
- No external dependencies
- No Azure CLI required

---

### analyze_storage_usage.py

Analyze Azure storage usage CSV data and display formatted breakdown.

#### Purpose

Read storage usage CSV data (from `get_storage_usage.py` or compatible source) and display formatted usage breakdowns with various presentation options.

#### When to Use

- Analyzing storage usage trends by account
- Identifying storage accounts with high capacity growth
- Breaking down usage by storage type (Blob, File, Table, Queue)
- Generating usage reports for capacity planning
- Comparing usage across different time periods

#### Features

- Reads usage CSV from file or stdin (pipeline-friendly)
- Overall summary or daily breakdown
- Usage by storage type (Blob, File, Table, Queue)
- Multiple output formats (text, CSV)
- No external dependencies (works offline)

#### Usage

```bash
# Overall summary
./analyze_storage_usage.py usage.csv

# Daily breakdown
./analyze_storage_usage.py usage.csv --daily

# CSV output for further processing (daily totals)
./analyze_storage_usage.py usage.csv --format csv

# CSV output with per-account breakdown
./analyze_storage_usage.py usage.csv --format csv --by-account

# Pipe from collector
./get_storage_usage.py --subscription <sub> --start-time "2025-11-01" --end-time "2025-11-07" | \
  ./analyze_storage_usage.py -
```

#### Command-Line Options

**Positional Arguments:**
- `input_file` - CSV file path, or `-` for stdin (required)

**Optional Arguments:**
- `--daily` - Show each day separately instead of aggregated
- `--by-account` - Break down by storage account (CSV format only)
- `--format {text,csv}` - Output format (default: text)
- `--start-time DATETIME` - Start time filter
- `--end-time DATETIME` - End time filter

#### Dependencies

- Python 3.7+
- Standard library modules only (csv, sys, argparse, datetime)
- No external dependencies
- No Azure CLI required

---

### HPCC Global Metrics Analysis

**Note:** HPCC global metrics analysis has been moved to the `../hpcc-globalmetrics/` directory as it does not depend on Azure APIs. See [hpcc-globalmetrics/README.md](../hpcc-globalmetrics/README.md) for documentation on analyzing `globalmetrics.xml` files.

---

### analyze_pod_node_inventory.py

Analyze pod and node inventory data with HPCC component identification.

#### Purpose

Analyze the CSV output from `get_pod_node_inventory.py` to identify HPCC components, cross-reference pods to nodes, and calculate resource consumption per component over time. This tool provides insights into which HPCC components are consuming which resources.

#### When to Use

- Understanding HPCC component resource consumption patterns
- Identifying which nodes are running which components
- Calculating pod-hours and duration for cost attribution
- Analyzing component deployment patterns
- Generating component-level resource usage reports

#### Features

- Identifies HPCC components from pod naming conventions
  - dali, esp, eclccserver, sasha, dfuserver, eclagent
  - Thor clusters (manager and worker pods)
  - Roxie clusters
- Cross-references pods to nodes (computers)
- Calculates pod count and node count per component
- Estimates resource consumption duration (pod-hours)
- Multiple output formats (CSV, text)
- Detailed component breakdown option (--by-component)
- Time range filtering

#### Component Identification

The tool identifies HPCC components based on pod naming conventions:
- `hpcc-dali-*` → dali
- `hpcc-esp-*` → esp
- `hpcc-thor-<cluster>-thormanager-*` → thor-<cluster>
- `hpcc-thor-<cluster>-thorworker-*` → thor-<cluster>-worker
- `hpcc-roxie-<cluster>-*` → roxie-<cluster>
- Other standard HPCC components

#### Usage

```bash
# Basic CSV analysis
cat inventory.csv | ./analyze_pod_node_inventory.py

# From file with CSV output
./analyze_pod_node_inventory.py inventory.csv

# Human-readable text report
./analyze_pod_node_inventory.py inventory.csv --format text

# Detailed component breakdown
./analyze_pod_node_inventory.py inventory.csv --format text --by-component

# Time-filtered analysis
./analyze_pod_node_inventory.py inventory.csv \
  --start-time "2025-11-04 12:00" --end-time "2025-11-04 18:00"

# Pipeline from collector
./get_pod_node_inventory.py --workspace-id <workspace-id> -n hpcc \
  --start-time "2025-11-04 12:00" | ./analyze_pod_node_inventory.py
```

#### Command-Line Options

**Positional Arguments:**
- `input` - Input CSV file from get_pod_node_inventory.py (or read from stdin if omitted)

**Optional Arguments:**
- `--start-time DATETIME` - Start time filter (YYYY-MM-DD or YYYY-MM-DD HH:MM)
- `--end-time DATETIME` - End time filter (YYYY-MM-DD or YYYY-MM-DD HH:MM)
- `--format {csv,text}` - Output format (default: csv)
- `--by-component` - Show detailed breakdown by component (text format only)

#### Output Formats

**CSV (default):**
```csv
# Generated by: analyze_pod_node_inventory.py
# Date generated: 2025-12-12 09:00:00
# Time range: 2025-11-04 12:00:00 to 2025-11-04 14:00:00
# Total pods: 42
# Total nodes: 10
#
Component,PodCount,NodeCount,DurationHours,PodHours
dali,1,1,2.00,2.00
esp,3,3,2.00,6.00
thor-mycluster,1,1,2.00,2.00
thor-mycluster-worker,8,8,2.00,16.00
roxie-cluster1,4,4,2.00,8.00
```

**Text:**
```
================================================================================
POD AND NODE INVENTORY ANALYSIS
================================================================================

SUMMARY
--------------------------------------------------------------------------------
Time Range:       2025-11-04 12:00:00 to 2025-11-04 14:00:00
Duration:         2.00 hours
Total Pods:       42
Total Nodes:      10

COMPONENT BREAKDOWN
--------------------------------------------------------------------------------
Component                           Pods     Nodes      Duration     Pod-Hours
--------------------------------------------------------------------------------
dali                                   1         1        2.00h        2.00h
esp                                    3         3        2.00h        6.00h
thor-mycluster                         1         1        2.00h        2.00h
thor-mycluster-worker                  8         8        2.00h       16.00h
roxie-cluster1                         4         4        2.00h        8.00h

NODE UTILIZATION
--------------------------------------------------------------------------------
Node Name                                            Pod Count
--------------------------------------------------------------------------------
aks-nodepool1-12345                                          5
aks-nodepool1-12346                                          4
...
```

**Text with --by-component:**
Includes detailed pod-to-node mapping for each component.

#### Important Notes

- **Duration Calculation:** The tool estimates durations based on snapshot data from KubePodInventory. It assumes all pods in the snapshot were running for the entire time window. For more accurate pod lifecycle tracking, time-series data would be needed.
- **Component Identification:** Based on standard HPCC pod naming conventions. Non-HPCC pods are categorized as "Other".

#### Dependencies

- Python 3.7+
- Standard library modules only (csv, datetime, collections)
- No Azure CLI required (works offline on saved CSV files)

---

### analyze_thor_timeline.py

Thor cluster utilization analysis and cost modeling from timeline data.

#### Purpose

Analyze Thor timeline CSV (from `get_ala_thor_timeline.py`) to calculate utilization, model costs, simulate concurrency limits, and analyze graph execution time distributions.

#### When to Use

- Calculating Thor cluster utilization and concurrency patterns
- Modeling costs based on instance lifecycle (spin-up, work, idle, spin-down)
- Simulating cost impacts of reducing max_concurrent instances
- Analyzing graph execution time percentiles to identify optimization candidates
- Comparing actual costs against simulated scenarios

#### Features

- Timeline parsing with utilization calculation
- Concurrency histogram and statistics
- Cost modeling with parameterized VM pricing
- Instance lifecycle costing (spin-up fixed cost, idle timeout)
- Concurrency simulation with queue tracking
- Graph execution percentile analysis (P50/P75/P90/P95/P99)
- Gantt chart generation (text and graphical)
- Cost comparison (simulated vs original)
- Instance uptime tracking

#### Usage

```bash
# Basic utilization analysis with original costs
./analyze_thor_timeline.py thor_timeline.csv

# Simulate reduced concurrency
./analyze_thor_timeline.py thor_timeline.csv --simulate-max-concurrent 3

# Percentile analysis of graph durations
./analyze_thor_timeline.py thor_timeline.csv --percentile-analysis

# List slowest graphs (99th percentile)
./analyze_thor_timeline.py thor_timeline.csv --percentile-analysis --list-percentile "99-100"

# Custom cost parameters
./analyze_thor_timeline.py thor_timeline.csv --vms-per-instance 25 --cost-per-vm 0.542 \
  --spin-down-time 10 --spin-up-cost 1.00

# Generate Gantt chart
./analyze_thor_timeline.py thor_timeline.csv --graph gantt.png
```

#### Command-Line Options

**Required:**
- `csv_file` - Timeline CSV from get_ala_thor_timeline.py

**Cost Parameters:**
- `--vms-per-instance N` - VMs per Thor instance (default: 25)
- `--cost-per-vm DOLLARS` - Cost per VM per hour (default: 0.3936)
- `--spin-down-time MINUTES` - Idle timeout before spin-down (default: 10)
- `--spin-up-time MINUTES` - Spin-up time in minutes (default: 1)
- `--spin-up-cost DOLLARS` - Fixed cost per instance spin-up (default: 1.00)

**Analysis:**
- `--simulate-max-concurrent N` - Simulate concurrency limit with queue
- `--percentile-analysis` - Show graph duration distribution
- `--list-percentile RANGE` - List graphs in percentile (e.g., "99-100", "0-50")

**Visualization:**
- `--gantt` - Generate text Gantt chart
- `--graph FILE` - Generate graphical Gantt (PNG)
- `--resolution MINUTES` - Gantt resolution (default: 5)
- `--width CHARS` - Gantt width (default: 100)

#### Output

**Utilization Analysis:**
```
THOR CLUSTER UTILIZATION ANALYSIS
Time Period: 2025-11-03 to 2025-11-07 (119h 40m)
Total Phases: 16,495
Utilization: 71.88%
Max Concurrent: 5
Avg Concurrent: 1.94
```

**Cost Analysis (Original Configuration):**
```
COST ANALYSIS (ORIGINAL CONFIGURATION)
Configuration:
  Max Concurrent: 5
  VMs per Instance: 25
  Cost per VM: $0.3936/hour

Instance Uptime:
  Total Uptime: 44.0 hours (work + idle, instances running)
  Total Period: 119.7 hours (elapsed wall-clock time)
  Uptime Ratio: 36.8%

Cost Breakdown:
  Work Instance Hrs: 166.9 hours  $1,642.08
  Idle Instance Hrs: 53.3 hours   $524.75
  Spin-ups: 320 times             $320.00
  Total: 220.2 hours              $2,486.83
  Cost per Graph: $0.1508
```

**Simulation with Cost Comparison:**
```
Simulated (max_concurrent=3):
  Instance Uptime: 66.3 hours
  Total: 198.9 hours  $2,148.93
  Cost per Graph: $0.1303

Original (max_concurrent=5):
  Instance Uptime: 44.0 hours
  Total: 220.2 hours  $2,486.83
  Cost per Graph: $0.1508

Cost Comparison:
  Savings: $337.90 (13.6% reduction)
```

**Percentile Distribution:**
```
GRAPH EXECUTION TIME PERCENTILE DISTRIBUTION
Total executions: 16,495
P50: 4.00s  P75: 12.00s  P90: 35.00s  P95: 1m 21s  P99: 9m 19s

Distribution:
0-50%:     8,348 (50.61%)
50-75%:    4,124 (25.00%)
75-90%:    2,474 (15.00%)
90-95%:      825 (5.00%)
95-99%:      659 (3.99%)
99-100%:     165 (1.00%)
```

#### Dependencies

- Python 3.7+
- Standard library (csv, datetime, statistics)
- Optional: matplotlib (for graphical Gantt)
- No Azure CLI required

---

## Quick Start

### 1. Collect Cost Data

```bash
# Fetch costs for the last week
./get_costs.py --subscription my-subscription --start-date 2025-11-01 --end-date 2025-11-08 > costs.csv
```

### 2. Analyze Costs

```bash
# Overall breakdown
./analyze_costs.py costs.csv

# Storage details
./get_costs.py --subscription my-subscription --start-date 2025-11-01 --end-date 2025-11-08 \
  --grouping subcategory --filter storage | ./analyze_costs.py - --focus storage
```

### 3. Collect Pod Inventory

```bash
# Query pods during a specific time window
./get_pod_inventory.py --workspace-id <workspace-id> -n hpcc --start-time "2025-11-04 12:00"
```

### 4. Build Thor Timeline

```bash
# Get Thor workunit timeline
./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
  --workspace-id <workspace-id> -c thor400 --format csv > timeline.csv
```

---

## Authentication

All Azure collectors require Azure CLI authentication:

```bash
# Login to Azure
az login

# Verify you have access to subscriptions
az account list --query "[].{Name:name, ID:id}" -o table
```

**Note**: You do NOT need to set the active subscription with `az account set`. The `--subscription` parameter explicitly specifies which subscription to query.

### Workspace Discovery

`get_pod_inventory.py` and `get_ala_thor_timeline.py` support two methods for identifying the Log Analytics workspace:

1. **Direct workspace ID** (simplest):
   - Use `--workspace-id <customer-id>` if you know the workspace ID
   - No additional Azure queries needed

2. **From AKS cluster**:
   - Use `--cluster` (or `--aks-cluster`) with `--resource-group`
   - Optionally specify `--subscription` (defaults to current az account)
   - Queries the cluster for its Container Insights workspace
   - Cluster must have Container Insights (omsagent addon) enabled

---

## Common Workflows

### Cost Analysis Workflow

```bash
# 1. Collect overall costs
./get_costs.py --subscription my-sub --start-date 2025-11-01 --end-date 2025-11-08 > costs.csv

# 2. Analyze overall breakdown
./analyze_costs.py costs.csv

# 3. Get detailed storage analysis
./get_costs.py --subscription my-sub --start-date 2025-11-01 --end-date 2025-11-08 \
  --grouping meter --filter storage | ./analyze_costs.py - --focus storage --ext

# 4. Get VM cost details
./get_costs.py --subscription my-sub --start-date 2025-11-01 --end-date 2025-11-08 \
  --grouping meter --filter vm | ./analyze_costs.py - --focus compute
```

### Storage Usage Workflow

```bash
# 1. Collect storage usage data
./get_storage_usage.py --subscription my-sub --start-time "2025-11-01" --end-time "2025-11-07" > usage.csv

# 2. Analyze overall usage
./analyze_storage_usage.py usage.csv

# 3. Daily breakdown
./analyze_storage_usage.py usage.csv --daily
```

### Thor Analysis Workflow

```bash
# 1. Build Thor workunit timeline
./get_ala_thor_timeline.py --start-time "2025-11-03 00:00" --end-time "2025-11-08 00:00" \
  --workspace-id <workspace-id> -c thor400-d48-a --format csv > thor_timeline.csv

# 2. Analyze utilization and costs
./analyze_thor_timeline.py thor_timeline.csv

# 3. Simulate reduced concurrency
./analyze_thor_timeline.py thor_timeline.csv --simulate-max-concurrent 3

# 4. Analyze graph duration distribution
./analyze_thor_timeline.py thor_timeline.csv --percentile-analysis

# 5. Get actual costs from global metrics (if available)
# Note: analyze_global_metrics.py moved to ../hpcc-globalmetrics/
../hpcc-globalmetrics/analyze_global_metrics.py globalmetrics.xml --instance thor400-d48-a \
  --start-time "2025-11-03 00:00" --end-time "2025-11-07 23:59" \
  --combined-view --combine-users --format table
```

### Pod Inventory Workflow

```bash
# 1. Investigate pods during a cost spike
./get_pod_inventory.py --workspace-id <workspace-id> -n hpcc \
  --start-time "2025-11-04 14:00" --duration 120 --format csv > spike_pods.csv

# 2. Compare pod counts across time windows
./get_pod_inventory.py --workspace-id <workspace-id> -n hpcc \
  --start-time "2025-11-04 08:00" --duration 60 | grep -c "Running"

./get_pod_inventory.py --workspace-id <workspace-id> -n hpcc \
  --start-time "2025-11-04 14:00" --duration 60 | grep -c "Running"
```

---

## Tips for Copilot

When users ask about:

1. **Azure costs** → Use `get_costs.py` to collect data, `analyze_costs.py` to analyze
2. **Storage usage** → Use `get_storage_usage.py` to collect metrics, `analyze_storage_usage.py` to analyze
3. **Pod inventory** or **Kubernetes pods in Azure** → Use `get_pod_inventory.py` (Azure Log Analytics)
4. **Thor workunits** or **HPCC Thor timeline** → Use `get_ala_thor_timeline.py` (Azure Log Analytics)
5. **VM pricing** or **Azure VM costs** → Use `get_vm_pricing.py` (public API, no auth)
6. **HPCC global metrics or component costs** → Use `../hpcc-globalmetrics/analyze_global_metrics.py` with globalmetrics.xml
7. **Thor utilization or cost modeling** → Use `analyze_thor_timeline.py` with timeline CSV
8. **Concurrency simulation** → Use `analyze_thor_timeline.py --simulate-max-concurrent N`
9. **Graph performance analysis** → Use `analyze_thor_timeline.py --percentile-analysis`
10. **Time ranges** → All tools support date/datetime formats (YYYY-MM-DD or YYYY-MM-DD HH:MM)
11. **Workspace discovery** → Prefer `--workspace-id` for simplicity, or `--cluster` + `--resource-group` for auto-discovery
12. **Piping data** → All collectors output to stdout, analyzers that support stdin use `-`
13. **Grouping levels** → Use `--grouping meter` for most detailed cost analysis
14. **Extended analysis** → Use `--ext` flag with `analyze_costs.py` when you have meter-level data

### Key Distinctions

- **`get_pod_inventory.py`** queries Azure Log Analytics `KubePodInventory` table (Azure-specific, not generic Kubernetes)
- **`get_ala_thor_timeline.py`** queries Azure Log Analytics `ContainerLogV2` table to parse Thor audit logs (Azure-specific)
- **`get_vm_pricing.py`** queries public Azure Retail Prices API (no authentication required)
- **`../hpcc-globalmetrics/analyze_global_metrics.py`** parses HPCC globalmetrics.xml locally (no Azure or cloud provider dependencies, moved to hpcc-globalmetrics directory)
- **`analyze_thor_timeline.py`** analyzes timeline CSV locally with cost modeling (no Azure connection required)
- Pod inventory and timeline collectors support workspace auto-discovery from AKS clusters
- Collectors output to stdout, most analyzers support stdin piping
- Cost tools use subscription ID/name, Log Analytics tools use workspace ID
- Timeline analyzer shows original costs by default (simulation optional)

---

## See Also

- Azure Documentation: https://learn.microsoft.com/azure/
