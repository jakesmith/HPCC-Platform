#!/usr/bin/env python3
"""
Python script to interact with HPCC Platform GetGlobalMetrics service.

This script provides functionality to query global metrics from the HPCC Platform
ws_machine service, with support for filtering by time range, category, and dimensions.
It can be extended to perform aggregates of certain filtered statistics.

Usage:
    python get_global_metrics.py --host <hpcc_host> --port <port> [options]

Example:
    python get_global_metrics.py --host localhost --port 8010 \
        --start "2023-01-01T00:00:00" --end "2023-12-31T23:59:59" \
        --category "categoryOne" --dimension user:alice
"""

import argparse
import json
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Tuple


class HPCCGlobalMetricsClient:
    """Client for HPCC Platform GetGlobalMetrics service."""
    
    def __init__(self, host: str, port: int, username: Optional[str] = None, 
                 password: Optional[str] = None, use_https: bool = False):
        """
        Initialize the client.
        
        Args:
            host: HPCC Platform hostname
            port: ESP service port
            username: Optional username for authentication
            password: Optional password for authentication  
            use_https: Whether to use HTTPS instead of HTTP
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_https = use_https
        self.base_url = f"{'https' if use_https else 'http'}://{host}:{port}"
    
    def create_request_xml(self, category: Optional[str] = None, 
                          dimensions: Optional[Dict[str, str]] = None,
                          start_time: Optional[str] = None,
                          end_time: Optional[str] = None) -> str:
        """
        Create XML request for GetGlobalMetrics service.
        
        Args:
            category: Optional category filter (use 'All' or None for all categories)
            dimensions: Optional dictionary of dimension name/value pairs
            start_time: Start time in format 'yyyy-mm-ddThh:mm:ss'
            end_time: End time in format 'yyyy-mm-ddThh:mm:ss'
            
        Returns:
            XML request string
        """
        root = ET.Element("GetGlobalMetricsRequest")
        
        # Add category if specified
        if category and category != "All":
            category_elem = ET.SubElement(root, "Category")
            category_elem.text = category
        
        # Add dimensions if specified
        if dimensions:
            dimensions_elem = ET.SubElement(root, "Dimensions")
            for name, value in dimensions.items():
                dim_elem = ET.SubElement(dimensions_elem, "Dimension")
                name_elem = ET.SubElement(dim_elem, "Name")
                name_elem.text = name
                value_elem = ET.SubElement(dim_elem, "Value")
                value_elem.text = value
        
        # Add date/time range if specified
        if start_time or end_time:
            datetime_range = ET.SubElement(root, "DateTimeRange")
            if start_time:
                start_elem = ET.SubElement(datetime_range, "Start")
                start_elem.text = start_time
            if end_time:
                end_elem = ET.SubElement(datetime_range, "End")
                end_elem.text = end_time
        
        return ET.tostring(root, encoding='unicode')
    
    def send_request(self, xml_request: str) -> str:
        """
        Send HTTP request to GetGlobalMetrics service.
        
        Args:
            xml_request: XML request payload
            
        Returns:
            XML response string
            
        Raises:
            Exception: If HTTP request fails
        """
        url = f"{self.base_url}/WsMachine/GetGlobalMetrics"
        
        # Prepare request
        data = xml_request.encode('utf-8')
        req = urllib.request.Request(url, data=data)
        req.add_header('Content-Type', 'application/xml')
        req.add_header('SOAPAction', '""')
        
        # Add authentication if provided
        if self.username and self.password:
            import base64
            credentials = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            req.add_header('Authorization', f'Basic {credentials}')
        
        try:
            with urllib.request.urlopen(req) as response:
                return response.read().decode('utf-8')
        except urllib.error.HTTPError as e:
            error_msg = f"HTTP Error {e.code}: {e.reason}"
            if hasattr(e, 'read'):
                error_msg += f"\nResponse: {e.read().decode('utf-8')}"
            raise Exception(error_msg)
        except Exception as e:
            raise Exception(f"Request failed: {str(e)}")
    
    def parse_response(self, xml_response: str) -> List[Dict]:
        """
        Parse XML response into structured data.
        
        Args:
            xml_response: XML response from service
            
        Returns:
            List of metric dictionaries
        """
        try:
            root = ET.fromstring(xml_response)
            metrics = []
            
            # Find all GlobalMetric elements
            for metric_elem in root.findall('.//GlobalMetric'):
                metric = {}
                
                # Extract category
                category_elem = metric_elem.find('Category')
                if category_elem is not None:
                    metric['category'] = category_elem.text
                
                # Extract dimensions
                dimensions = {}
                dimensions_elem = metric_elem.find('Dimensions')
                if dimensions_elem is not None:
                    for dim_elem in dimensions_elem.findall('Dimension'):
                        name_elem = dim_elem.find('Name')
                        value_elem = dim_elem.find('Value')
                        if name_elem is not None and value_elem is not None:
                            dimensions[name_elem.text] = value_elem.text
                metric['dimensions'] = dimensions
                
                # Extract date/time range
                datetime_range_elem = metric_elem.find('DateTimeRange')
                if datetime_range_elem is not None:
                    start_elem = datetime_range_elem.find('Start')
                    end_elem = datetime_range_elem.find('End')
                    metric['datetime_range'] = {
                        'start': start_elem.text if start_elem is not None else None,
                        'end': end_elem.text if end_elem is not None else None
                    }
                
                # Extract stats
                stats = {}
                stats_elem = metric_elem.find('Stats')
                if stats_elem is not None:
                    for stat_elem in stats_elem.findall('Stat'):
                        name_elem = stat_elem.find('Name')
                        value_elem = stat_elem.find('Value')
                        if name_elem is not None and value_elem is not None:
                            try:
                                # Try to convert to numeric value
                                stats[name_elem.text] = int(value_elem.text)
                            except ValueError:
                                # Keep as string if not numeric
                                stats[name_elem.text] = value_elem.text
                metric['stats'] = stats
                
                metrics.append(metric)
            
            return metrics
        except ET.ParseError as e:
            raise Exception(f"Failed to parse XML response: {str(e)}")
    
    def get_global_metrics(self, category: Optional[str] = None,
                          dimensions: Optional[Dict[str, str]] = None,
                          start_time: Optional[str] = None,
                          end_time: Optional[str] = None) -> List[Dict]:
        """
        Get global metrics from HPCC Platform.
        
        Args:
            category: Optional category filter
            dimensions: Optional dimension filters
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            List of metric dictionaries
        """
        xml_request = self.create_request_xml(category, dimensions, start_time, end_time)
        xml_response = self.send_request(xml_request)
        return self.parse_response(xml_response)
    
    def filter_stats(self, metrics: List[Dict], stat_names: List[str]) -> List[Dict]:
        """
        Filter metrics to include only specified statistics.
        
        Args:
            metrics: List of metric dictionaries
            stat_names: List of statistic names to include
            
        Returns:
            Filtered list of metric dictionaries
        """
        filtered_metrics = []
        for metric in metrics:
            filtered_stats = {name: value for name, value in metric.get('stats', {}).items() 
                            if name in stat_names}
            if filtered_stats:  # Only include metrics that have at least one of the requested stats
                filtered_metric = metric.copy()
                filtered_metric['stats'] = filtered_stats
                filtered_metrics.append(filtered_metric)
        return filtered_metrics


def parse_dimensions(dimension_args: List[str]) -> Dict[str, str]:
    """
    Parse dimension arguments in format 'name:value'.
    
    Args:
        dimension_args: List of dimension strings
        
    Returns:
        Dictionary of dimension name/value pairs
    """
    dimensions = {}
    for dim_arg in dimension_args:
        if ':' not in dim_arg:
            raise ValueError(f"Invalid dimension format: {dim_arg}. Expected 'name:value'")
        name, value = dim_arg.split(':', 1)
        dimensions[name.strip()] = value.strip()
    return dimensions


def format_output(metrics: List[Dict], output_format: str) -> str:
    """
    Format metrics for output.
    
    Args:
        metrics: List of metric dictionaries
        output_format: Output format ('json', 'table', 'csv')
        
    Returns:
        Formatted output string
    """
    if output_format == 'json':
        return json.dumps(metrics, indent=2)
    
    elif output_format == 'table':
        if not metrics:
            return "No metrics found."
        
        output = []
        output.append("Global Metrics:")
        output.append("=" * 50)
        
        for i, metric in enumerate(metrics):
            output.append(f"\nMetric {i + 1}:")
            output.append(f"  Category: {metric.get('category', 'N/A')}")
            
            dimensions = metric.get('dimensions', {})
            if dimensions:
                output.append("  Dimensions:")
                for name, value in dimensions.items():
                    output.append(f"    {name}: {value}")
            
            datetime_range = metric.get('datetime_range', {})
            if datetime_range.get('start') or datetime_range.get('end'):
                output.append("  Time Range:")
                if datetime_range.get('start'):
                    output.append(f"    Start: {datetime_range['start']}")
                if datetime_range.get('end'):
                    output.append(f"    End: {datetime_range['end']}")
            
            stats = metric.get('stats', {})
            if stats:
                output.append("  Statistics:")
                for name, value in stats.items():
                    output.append(f"    {name}: {value}")
        
        return '\n'.join(output)
    
    elif output_format == 'csv':
        if not metrics:
            return "category,dimensions,start_time,end_time,stat_name,stat_value"
        
        lines = ["category,dimensions,start_time,end_time,stat_name,stat_value"]
        
        for metric in metrics:
            category = metric.get('category', '')
            dimensions_str = ';'.join([f"{k}={v}" for k, v in metric.get('dimensions', {}).items()])
            datetime_range = metric.get('datetime_range', {})
            start_time = datetime_range.get('start', '')
            end_time = datetime_range.get('end', '')
            
            for stat_name, stat_value in metric.get('stats', {}).items():
                lines.append(f"{category},{dimensions_str},{start_time},{end_time},{stat_name},{stat_value}")
        
        return '\n'.join(lines)
    
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Query HPCC Platform global metrics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get all metrics for all categories
  %(prog)s --host localhost --port 8010

  # Get metrics for specific category and time range
  %(prog)s --host localhost --port 8010 \\
    --category "categoryOne" \\
    --start "2023-01-01T00:00:00" \\
    --end "2023-12-31T23:59:59"

  # Get metrics with dimension filter
  %(prog)s --host localhost --port 8010 \\
    --dimension user:alice \\
    --dimension cluster:thor1

  # Filter specific statistics and output as JSON
  %(prog)s --host localhost --port 8010 \\
    --stats TimeLocalExecute CostExecute \\
    --format json
        """)
    
    parser.add_argument('--host', required=True,
                        help='HPCC Platform hostname')
    parser.add_argument('--port', type=int, required=True,
                        help='ESP service port')
    parser.add_argument('--username',
                        help='Username for authentication')
    parser.add_argument('--password',
                        help='Password for authentication')
    parser.add_argument('--https', action='store_true',
                        help='Use HTTPS instead of HTTP')
    
    parser.add_argument('--category',
                        help='Category filter (use "All" or omit for all categories)')
    parser.add_argument('--dimension', action='append', dest='dimensions',
                        help='Dimension filter in format "name:value" (can be specified multiple times)')
    parser.add_argument('--start',
                        help='Start time in format "yyyy-mm-ddThh:mm:ss"')
    parser.add_argument('--end',
                        help='End time in format "yyyy-mm-ddThh:mm:ss"')
    
    parser.add_argument('--stats', nargs='*',
                        help='Filter to include only specified statistics')
    parser.add_argument('--format', choices=['json', 'table', 'csv'], default='table',
                        help='Output format (default: table)')
    
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    
    args = parser.parse_args()
    
    try:
        # Parse dimensions
        dimensions = None
        if args.dimensions:
            dimensions = parse_dimensions(args.dimensions)
        
        # Create client
        client = HPCCGlobalMetricsClient(
            host=args.host,
            port=args.port,
            username=args.username,
            password=args.password,
            use_https=args.https
        )
        
        if args.verbose:
            print(f"Connecting to {client.base_url}", file=sys.stderr)
            if args.category:
                print(f"Category filter: {args.category}", file=sys.stderr)
            if dimensions:
                print(f"Dimension filters: {dimensions}", file=sys.stderr)
            if args.start or args.end:
                print(f"Time range: {args.start} to {args.end}", file=sys.stderr)
        
        # Get metrics
        metrics = client.get_global_metrics(
            category=args.category,
            dimensions=dimensions,
            start_time=args.start,
            end_time=args.end
        )
        
        # Filter statistics if requested
        if args.stats:
            metrics = client.filter_stats(metrics, args.stats)
            if args.verbose:
                print(f"Filtered to statistics: {args.stats}", file=sys.stderr)
        
        # Output results
        output = format_output(metrics, args.format)
        print(output)
        
        if args.verbose:
            print(f"Found {len(metrics)} metric(s)", file=sys.stderr)
    
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()