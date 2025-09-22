# Global Metrics Python Client

This directory contains a Python script to interact with the HPCC Platform GetGlobalMetrics service in ws_machines.

## Files

- `get_global_metrics.py` - Main Python script for querying global metrics
- `../testing/unittests/test_get_global_metrics.py` - Unit tests for the script

## Features

The `get_global_metrics.py` script provides:

1. **Time Range Filtering**: Query metrics for a specific date/time range
2. **Category Filtering**: Filter by specific metric categories
3. **Dimension Filtering**: Filter by dimension name/value pairs (e.g., user, cluster)
4. **Statistics Selection**: Extract only specific statistics from the results
5. **Multiple Output Formats**: JSON, table, and CSV output formats
6. **Authentication Support**: Basic HTTP authentication
7. **HTTPS Support**: Secure connections to HPCC Platform

## Usage

### Basic Usage

```bash
# Get all metrics
python3 get_global_metrics.py --host localhost --port 8010

# Get help
python3 get_global_metrics.py --help
```

### Time Range Filtering

```bash
# Query metrics for a specific time range
python3 get_global_metrics.py --host localhost --port 8010 \
    --start "2023-01-01T00:00:00" \
    --end "2023-12-31T23:59:59"
```

### Category and Dimension Filtering

```bash
# Filter by category
python3 get_global_metrics.py --host localhost --port 8010 \
    --category "categoryOne"

# Filter by dimensions
python3 get_global_metrics.py --host localhost --port 8010 \
    --dimension user:alice \
    --dimension cluster:thor1

# Combine filters
python3 get_global_metrics.py --host localhost --port 8010 \
    --category "categoryOne" \
    --dimension user:bob \
    --start "2023-01-01T00:00:00" \
    --end "2023-12-31T23:59:59"
```

### Statistics Selection and Output Formatting

```bash
# Select specific statistics and output as JSON
python3 get_global_metrics.py --host localhost --port 8010 \
    --stats TimeLocalExecute CostExecute \
    --format json

# Output as CSV for further processing
python3 get_global_metrics.py --host localhost --port 8010 \
    --format csv > metrics.csv
```

### Authentication

```bash
# With authentication
python3 get_global_metrics.py --host remote-host --port 8010 \
    --username myuser --password mypass \
    --https
```

## Output Formats

### Table Format (Default)
Human-readable tabular output showing all metric details.

### JSON Format
Structured JSON output suitable for programmatic processing:

```json
[
  {
    "category": "categoryOne",
    "dimensions": {
      "user": "alice",
      "cluster": "thor1"
    },
    "datetime_range": {
      "start": "1999070112",
      "end": "1999070112"
    },
    "stats": {
      "TimeLocalExecute": 222,
      "CostExecute": 5
    }
  }
]
```

### CSV Format
Comma-separated values for spreadsheet processing:

```csv
category,dimensions,start_time,end_time,stat_name,stat_value
categoryOne,user=alice;cluster=thor1,1999070112,1999070112,TimeLocalExecute,222
categoryOne,user=alice;cluster=thor1,1999070112,1999070112,CostExecute,5
```

## Service Interface

The script communicates with the HPCC Platform GetGlobalMetrics service:

- **URL**: `http(s)://host:port/WsMachine/GetGlobalMetrics`
- **Method**: HTTP POST
- **Content-Type**: application/xml
- **Authentication**: Basic HTTP authentication (optional)

### Request Format

```xml
<GetGlobalMetricsRequest>
    <Category>categoryOne</Category>
    <Dimensions>
        <Dimension>
            <Name>user</Name>
            <Value>alice</Value>
        </Dimension>
    </Dimensions>
    <DateTimeRange>
        <Start>1999-01-01T00:00:00</Start>
        <End>2099-01-01T00:00:00</End>
    </DateTimeRange>
</GetGlobalMetricsRequest>
```

### Response Format

```xml
<GetGlobalMetricsResponse>
    <GlobalMetrics>
        <GlobalMetric>
            <Category>categoryOne</Category>
            <Dimensions>
                <Dimension>
                    <Name>user</Name>
                    <Value>alice</Value>
                </Dimension>
            </Dimensions>
            <DateTimeRange>
                <Start>1999070112</Start>
                <End>1999070112</End>
            </DateTimeRange>
            <Stats>
                <Stat>
                    <Name>TimeLocalExecute</Name>
                    <Value>222</Value>
                </Stat>
                <Stat>
                    <Name>CostExecute</Name>
                    <Value>5</Value>
                </Stat>
            </Stats>
        </GlobalMetric>
    </GlobalMetrics>
</GetGlobalMetricsResponse>
```

## Future Extensions

The script is designed to be easily extended with additional functionality:

1. **Aggregation Functions**: Add support for aggregating statistics (sum, average, min, max)
2. **Filtering**: Advanced filtering capabilities (value ranges, regex patterns)
3. **Caching**: Optional response caching for repeated queries
4. **Batch Processing**: Process multiple queries from configuration files
5. **Visualization**: Integration with plotting libraries for chart generation

## Testing

Run the unit tests:

```bash
cd /path/to/HPCC-Platform
python3 testing/unittests/test_get_global_metrics.py
```

The tests verify:
- XML request generation
- XML response parsing
- Statistics filtering
- Output formatting
- Utility functions

## Dependencies

- Python 3.6+
- Standard library modules only (no external dependencies)

## Error Handling

The script provides informative error messages for common issues:
- Connection failures
- Authentication errors
- Invalid XML responses
- Malformed time formats
- Missing required parameters

Use the `--verbose` flag for additional debugging information.