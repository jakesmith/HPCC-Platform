#!/usr/bin/env python3
"""
Example script showing how to use get_global_metrics.py for aggregation tasks.

This demonstrates how the script can be extended to perform various aggregations
of metrics data, which could be built into the main script in future iterations.
"""

import json
import subprocess
import sys
from collections import defaultdict
from typing import Dict, List, Any


def run_metrics_query(host: str, port: int, **kwargs) -> List[Dict]:
    """
    Run get_global_metrics.py and return parsed results.
    
    Args:
        host: HPCC hostname
        port: ESP port
        **kwargs: Additional arguments for the script
        
    Returns:
        List of metric dictionaries
    """
    cmd = ['python3', 'get_global_metrics.py', '--host', host, '--port', str(port), '--format', 'json']
    
    # Add optional arguments
    for key, value in kwargs.items():
        if key == 'category' and value:
            cmd.extend(['--category', value])
        elif key == 'dimensions' and value:
            for dim_name, dim_value in value.items():
                cmd.extend(['--dimension', f'{dim_name}:{dim_value}'])
        elif key == 'start_time' and value:
            cmd.extend(['--start', value])
        elif key == 'end_time' and value:
            cmd.extend(['--end', value])
        elif key == 'stats' and value:
            cmd.extend(['--stats'] + value)
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running metrics query: {e.stderr}", file=sys.stderr)
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}", file=sys.stderr)
        return []


def aggregate_by_category(metrics: List[Dict]) -> Dict[str, Dict[str, float]]:
    """
    Aggregate statistics by category.
    
    Args:
        metrics: List of metric dictionaries
        
    Returns:
        Dictionary with categories as keys and aggregated stats as values
    """
    category_stats = defaultdict(lambda: defaultdict(list))
    
    for metric in metrics:
        category = metric.get('category', 'Unknown')
        stats = metric.get('stats', {})
        
        for stat_name, stat_value in stats.items():
            if isinstance(stat_value, (int, float)):
                category_stats[category][stat_name].append(stat_value)
    
    # Calculate aggregations
    result = {}
    for category, stats in category_stats.items():
        result[category] = {}
        for stat_name, values in stats.items():
            if values:
                result[category][stat_name] = {
                    'sum': sum(values),
                    'avg': sum(values) / len(values),
                    'min': min(values),
                    'max': max(values),
                    'count': len(values)
                }
    
    return result


def aggregate_by_dimension(metrics: List[Dict], dimension_name: str) -> Dict[str, Dict[str, float]]:
    """
    Aggregate statistics by a specific dimension.
    
    Args:
        metrics: List of metric dictionaries
        dimension_name: Name of dimension to aggregate by
        
    Returns:
        Dictionary with dimension values as keys and aggregated stats as values
    """
    dimension_stats = defaultdict(lambda: defaultdict(list))
    
    for metric in metrics:
        dimensions = metric.get('dimensions', {})
        dimension_value = dimensions.get(dimension_name, 'Unknown')
        stats = metric.get('stats', {})
        
        for stat_name, stat_value in stats.items():
            if isinstance(stat_value, (int, float)):
                dimension_stats[dimension_value][stat_name].append(stat_value)
    
    # Calculate aggregations
    result = {}
    for dim_value, stats in dimension_stats.items():
        result[dim_value] = {}
        for stat_name, values in stats.items():
            if values:
                result[dim_value][stat_name] = {
                    'sum': sum(values),
                    'avg': sum(values) / len(values),
                    'min': min(values),
                    'max': max(values),
                    'count': len(values)
                }
    
    return result


def find_top_performers(metrics: List[Dict], stat_name: str, top_n: int = 5) -> List[Dict]:
    """
    Find top performing metrics by a specific statistic.
    
    Args:
        metrics: List of metric dictionaries
        stat_name: Name of statistic to rank by
        top_n: Number of top performers to return
        
    Returns:
        List of top performing metrics
    """
    # Filter metrics that have the requested statistic
    valid_metrics = []
    for metric in metrics:
        stats = metric.get('stats', {})
        if stat_name in stats and isinstance(stats[stat_name], (int, float)):
            valid_metrics.append((stats[stat_name], metric))
    
    # Sort by statistic value (descending)
    valid_metrics.sort(key=lambda x: x[0], reverse=True)
    
    # Return top N metrics
    return [metric for _, metric in valid_metrics[:top_n]]


def time_series_analysis(metrics: List[Dict], stat_name: str) -> Dict[str, Any]:
    """
    Analyze time series data for a specific statistic.
    
    Args:
        metrics: List of metric dictionaries
        stat_name: Name of statistic to analyze
        
    Returns:
        Dictionary with time series analysis results
    """
    time_points = []
    values = []
    
    for metric in metrics:
        stats = metric.get('stats', {})
        datetime_range = metric.get('datetime_range', {})
        
        if stat_name in stats and isinstance(stats[stat_name], (int, float)):
            start_time = datetime_range.get('start')
            if start_time:
                time_points.append(start_time)
                values.append(stats[stat_name])
    
    if not values:
        return {'error': f'No data found for statistic: {stat_name}'}
    
    # Simple time series analysis
    return {
        'stat_name': stat_name,
        'total_points': len(values),
        'time_range': {
            'start': min(time_points) if time_points else None,
            'end': max(time_points) if time_points else None
        },
        'value_stats': {
            'sum': sum(values),
            'avg': sum(values) / len(values),
            'min': min(values),
            'max': max(values),
            'variance': sum((x - sum(values) / len(values)) ** 2 for x in values) / len(values)
        }
    }


def main():
    """Example usage of aggregation functions."""
    if len(sys.argv) < 3:
        print("Usage: python3 example_aggregation.py <host> <port>")
        sys.exit(1)
    
    host = sys.argv[1]
    port = int(sys.argv[2])
    
    print("=== Example: Basic Metrics Query ===")
    # Get all metrics for demonstration
    all_metrics = run_metrics_query(
        host, port,
        start_time="1999-01-01T00:00:00",
        end_time="2099-01-01T00:00:00"
    )
    
    if not all_metrics:
        print("No metrics found or query failed.")
        return
    
    print(f"Found {len(all_metrics)} total metrics")
    
    print("\n=== Example: Aggregation by Category ===")
    category_agg = aggregate_by_category(all_metrics)
    for category, stats in category_agg.items():
        print(f"\nCategory: {category}")
        for stat_name, stat_data in stats.items():
            print(f"  {stat_name}: avg={stat_data['avg']:.2f}, sum={stat_data['sum']}, count={stat_data['count']}")
    
    print("\n=== Example: Aggregation by User Dimension ===")
    user_agg = aggregate_by_dimension(all_metrics, 'user')
    for user, stats in user_agg.items():
        print(f"\nUser: {user}")
        for stat_name, stat_data in stats.items():
            print(f"  {stat_name}: avg={stat_data['avg']:.2f}, sum={stat_data['sum']}, count={stat_data['count']}")
    
    print("\n=== Example: Top Performers by TimeLocalExecute ===")
    top_performers = find_top_performers(all_metrics, 'TimeLocalExecute', top_n=3)
    for i, metric in enumerate(top_performers, 1):
        print(f"{i}. Category: {metric.get('category')}, "
              f"Dimensions: {metric.get('dimensions')}, "
              f"TimeLocalExecute: {metric.get('stats', {}).get('TimeLocalExecute')}")
    
    print("\n=== Example: Time Series Analysis for TimeLocalExecute ===")
    time_analysis = time_series_analysis(all_metrics, 'TimeLocalExecute')
    if 'error' not in time_analysis:
        print(f"Analyzed {time_analysis['total_points']} data points")
        print(f"Value range: {time_analysis['value_stats']['min']} - {time_analysis['value_stats']['max']}")
        print(f"Average: {time_analysis['value_stats']['avg']:.2f}")
        print(f"Variance: {time_analysis['value_stats']['variance']:.2f}")
    else:
        print(time_analysis['error'])
    
    print("\n=== Example: Filtered Query with Specific Statistics ===")
    filtered_metrics = run_metrics_query(
        host, port,
        category="categoryOne",
        dimensions={'user': 'alice'},
        stats=['TimeLocalExecute', 'CostExecute'],
        start_time="1999-01-01T00:00:00",
        end_time="2099-01-01T00:00:00"
    )
    
    print(f"Filtered query returned {len(filtered_metrics)} metrics")
    for metric in filtered_metrics:
        print(f"  Category: {metric.get('category')}, Stats: {metric.get('stats')}")


if __name__ == '__main__':
    main()