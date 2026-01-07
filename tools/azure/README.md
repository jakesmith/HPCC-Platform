# Azure Log Analytics Tools

This directory contains Python tools for querying and analyzing Azure Log Analytics data for HPCC Systems deployments on Azure Kubernetes Service (AKS).

## Overview

These tools help analyze Kubernetes pod and node inventory data from Azure Log Analytics to understand resource usage, identify which HPCC components were running, and support cost analysis and optimization.

### Tools

1. **azure_log_analytics_fetch.py** - Queries Azure Log Analytics using KQL
2. **azure_log_analytics_analyze.py** - Analyzes the fetched data and categorizes components

## Prerequisites

- Python 3.7 or higher
- Azure subscription with Log Analytics workspace
- Appropriate Azure credentials (see Authentication section below)

## Installation

Install required Python packages:

```bash
pip install -r requirements.txt
```

Or install packages individually:

```bash
pip install azure-identity azure-monitor-query
```

## Authentication

The tools support two authentication methods:

### 1. Default Azure Credentials (Recommended)

Uses Azure CLI, managed identity, or environment variables. Set up Azure CLI:

```bash
az login
az account set --subscription <subscription-id>
```

### 2. Service Principal

Use client credentials by providing tenant ID, client ID, and client secret:

```bash
# Set environment variables
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

## Usage

### Step 1: Fetch Data from Azure Log Analytics

Query Azure Log Analytics to fetch Kubernetes node and pod inventory data:

```bash
./azure_log_analytics_fetch.py \
  --subscription-id <subscription-id> \
  --workspace-id <log-analytics-workspace-id> \
  --aks-name <aks-cluster-name> \
  --start "2024-01-01T00:00:00Z" \
  --end "2024-01-01T23:59:59Z" \
  --output results.csv \
  --verbose
```

#### Optional Parameters

**Filter by specific namespaces:**
```bash
--namespaces "default,hpcc,monitoring"
```

**Include resource group in metadata:**
```bash
--resource-group <resource-group-name>
```

**Use service principal authentication:**
```bash
--tenant-id <tenant-id> \
--client-id <client-id> \
--client-secret <client-secret>
```

#### Output Format

The output CSV file contains:
- Metadata header with query parameters (as comments)
- Combined node and pod inventory data
- RecordType field to distinguish between Node and Pod records

### Step 2: Analyze the Data

Analyze the fetched CSV data to categorize pods into HPCC components:

```bash
./azure_log_analytics_analyze.py \
  --input results.csv \
  --output analysis.csv \
  --summary summary.txt \
  --verbose
```

#### Output Files

1. **analysis.csv** - Time-series data with categorized components
   - Component categorization (HPCC vs system pods)
   - Node usage information
   - Suitable for visualization (e.g., Gantt charts)

2. **summary.txt** (optional) - Human-readable summary report
   - Statistics on pod distribution
   - Category and component breakdowns
   - Node usage analysis
   - Namespace distribution

## HPCC Component Categories

The analysis tool recognizes the following HPCC components based on the helm/hpcc chart:

- **dali** - Distributed Array Logical Index
- **thor** - Thor cluster (manager, workers, agents)
- **roxie** - Roxie cluster (server, agent, toposerver)
- **esp** - Enterprise Services Platform (eclwatch, eclservices, eclqueries, esdl-sandbox)
- **eclagent** - ECL Agent
- **eclccserver** - ECL CC Server
- **eclscheduler** - ECL Scheduler
- **dfuserver** - Distributed File Utility Server
- **sasha** - Storage Archive Service
- **dafilesrv** - Dali File Server

Non-HPCC system components are also categorized:
- kubernetes-system (kube-*)
- monitoring (prometheus, grafana)
- logging (fluentd, elasticsearch)
- ingress (nginx, traefik)
- azure-system (aks-*, omsagent, azure-disk, azure-file)
- And others...

## Example Workflow

Complete example for analyzing a 24-hour period:

```bash
# Step 1: Fetch data
./azure_log_analytics_fetch.py \
  --subscription-id "12345678-1234-1234-1234-123456789012" \
  --workspace-id "abcd1234-5678-90ab-cdef-1234567890ab" \
  --aks-name "my-hpcc-cluster" \
  --start "2024-11-01T00:00:00Z" \
  --end "2024-11-02T00:00:00Z" \
  --output hpcc_inventory_nov1.csv \
  --verbose

# Step 2: Analyze data
./azure_log_analytics_analyze.py \
  --input hpcc_inventory_nov1.csv \
  --output hpcc_analysis_nov1.csv \
  --summary hpcc_summary_nov1.txt \
  --verbose

# Step 3: Review the results
cat hpcc_summary_nov1.txt
```

## Output Analysis and Visualization

The analysis output is designed to support:

### 1. Cost Analysis
- Identify which components were running on which nodes
- Correlate with Azure VM costs to determine component-level expenses
- Understand which components kept VMs active

### 2. Time-Series Visualization
The `analysis.csv` output contains time-series data with:
- TimeGenerated: Timestamp of the record
- Component: HPCC component name or category
- NodeName: Node where the pod was running
- PodStatus: Status of the pod (Running, Pending, etc.)

This data can be imported into visualization tools to create:
- **Gantt charts** showing component lifecycles
- **Resource usage timelines** by component
- **Node utilization charts** showing which nodes were active

### 3. Visualization Examples

#### Using Python (Pandas + Matplotlib)
```python
import pandas as pd
import matplotlib.pyplot as plt

# Load the analysis
df = pd.read_csv('hpcc_analysis_nov1.csv', comment='#')
df['TimeGenerated'] = pd.to_datetime(df['TimeGenerated'])

# Filter for HPCC components
hpcc_df = df[df['IsHPCC'] == 'Yes']

# Group by component and time
component_timeline = hpcc_df.groupby(['Component', 'TimeGenerated']).size()

# Create visualization
# ... (customize based on your needs)
```

#### Using Excel/Google Sheets
1. Import the CSV file
2. Filter by IsHPCC='Yes' for HPCC components
3. Create pivot tables for component analysis
4. Generate charts for timeline visualization

## Troubleshooting

### Authentication Issues

If you encounter authentication errors:

```bash
# Verify Azure CLI login
az account show

# Re-login if needed
az login

# Verify workspace access
az monitor log-analytics workspace show \
  --workspace-name <workspace-name> \
  --resource-group <resource-group>
```

### No Data Returned

If the query returns no results:

1. Verify the date range is correct (use ISO 8601 format with 'Z' for UTC)
2. Check that the AKS cluster name matches exactly
3. Verify the cluster is sending data to the Log Analytics workspace
4. Ensure KubeNodeInventory and KubePodInventory tables exist in the workspace

### Large Result Sets

For large time ranges or busy clusters:

1. Query smaller time windows (e.g., 1 day at a time)
2. Use namespace filtering to reduce data volume
3. Consider using Azure's query limits and pagination

## Integration with Cost Analysis

To integrate with Azure cost analysis:

1. Export Azure cost data for the same time period
2. Match VM resources to node names from the analysis
3. Correlate component usage with VM costs
4. Calculate per-component cost allocation

Example workflow:
```bash
# Get VM costs from Azure
az consumption usage list \
  --start-date "2024-11-01" \
  --end-date "2024-11-02" \
  --query "[?contains(instanceName, 'aks')]" > vm_costs.json

# Correlate with component analysis
# ... (custom script to join data)
```

## Contributing

When adding new HPCC components or improving categorization:

1. Update the component patterns in `azure_log_analytics_analyze.py`
2. Refer to `helm/hpcc/templates/` for component naming conventions
3. Test with sample data to ensure correct categorization
4. Update this README with any new features

## License

Copyright (C) 2024 HPCC Systems®

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
