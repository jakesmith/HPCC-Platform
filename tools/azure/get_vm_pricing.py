#!/usr/bin/env python3
"""
Query Azure Retail Prices API for VM pricing information.

This script queries the Azure Retail Prices API to retrieve current pricing
for Azure Virtual Machines by SKU name and region.
"""

import argparse
import json
import sys
import urllib.request
import urllib.parse
from typing import List, Dict, Optional


def build_filter(sku_name: str, region: str, os_type: str = 'linux') -> str:
    """Build OData filter string for Azure Retail Prices API.
    
    Args:
        sku_name: VM SKU name (e.g., 'Standard_D48ds_v4')
        region: Azure region name (e.g., 'eastus', 'westus')
        os_type: Operating system type ('linux' or 'windows')
    
    Returns:
        OData filter string
    """
    filters = [
        "serviceName eq 'Virtual Machines'",
        f"armRegionName eq '{region}'",
        f"armSkuName eq '{sku_name}'"
    ]
    
    # For Linux pricing, we want items without Windows in the product name
    # For Windows pricing, we want items with Windows in the product name
    if os_type.lower() == 'linux':
        # Linux pricing will have lower costs and no Windows in the name
        # We'll filter in post-processing
        pass
    
    return ' and '.join(filters)


def query_pricing(sku_name: str, region: str, os_type: str = 'linux') -> List[Dict]:
    """Query Azure Retail Prices API for VM pricing.
    
    Args:
        sku_name: VM SKU name (e.g., 'Standard_D48ds_v4')
        region: Azure region name (e.g., 'eastus')
        os_type: Operating system type ('linux' or 'windows')
    
    Returns:
        List of pricing items
    """
    base_url = 'https://prices.azure.com/api/retail/prices'
    filter_str = build_filter(sku_name, region, os_type)
    
    # URL encode the filter
    params = {
        '$filter': filter_str
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"
    
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode('utf-8'))
            return data.get('Items', [])
    except urllib.error.URLError as e:
        print(f"Error querying Azure Pricing API: {e}", file=sys.stderr)
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}", file=sys.stderr)
        return []


def filter_items(items: List[Dict], os_type: str, pricing_type: str = 'Consumption') -> List[Dict]:
    """Filter pricing items by OS type and pricing model.
    
    Args:
        items: List of pricing items from API
        os_type: 'linux' or 'windows'
        pricing_type: 'Consumption' (pay-as-you-go), 'DevTestConsumption', or 'Reservation'
    
    Returns:
        Filtered list of items
    """
    filtered = []
    
    for item in items:
        # Check pricing type
        if item.get('type') != pricing_type:
            continue
        
        product_name = item.get('productName', '').lower()
        
        # Filter by OS
        if os_type.lower() == 'linux':
            # Linux pricing: no 'windows' in product name, and typically lower cost
            if 'windows' not in product_name:
                filtered.append(item)
        else:
            # Windows pricing: 'windows' in product name
            if 'windows' in product_name:
                filtered.append(item)
    
    return filtered


def format_output(items: List[Dict], format_type: str, show_spec: bool = False) -> None:
    """Format and print pricing information.
    
    Args:
        items: List of pricing items
        format_type: 'text', 'json', or 'csv'
        show_spec: Whether to show VM specifications
    """
    if not items:
        print("No pricing information found.", file=sys.stderr)
        return
    
    if format_type == 'json':
        print(json.dumps(items, indent=2))
        return
    
    if format_type == 'csv':
        # CSV header
        if show_spec:
            print("SKU,Region,OS,Type,Price,Unit,Cores,RAM_GB,Disk,ProductName")
        else:
            print("SKU,Region,OS,Type,Price,Unit,ProductName")
        
        for item in items:
            sku = item.get('armSkuName', 'N/A')
            region = item.get('armRegionName', 'N/A')
            product = item.get('productName', 'N/A')
            price = item.get('retailPrice', 0)
            unit = item.get('unitOfMeasure', 'N/A')
            pricing_type = item.get('type', 'N/A')
            
            # Determine OS from product name
            os_type = 'Windows' if 'windows' in product.lower() else 'Linux'
            
            if show_spec:
                # Extract specs if available (this would require additional parsing)
                cores = 'N/A'
                ram = 'N/A'
                disk = 'N/A'
                print(f"{sku},{region},{os_type},{pricing_type},{price:.4f},{unit},{cores},{ram},{disk},{product}")
            else:
                print(f"{sku},{region},{os_type},{pricing_type},{price:.4f},{unit},{product}")
        return
    
    # Text format (default)
    for item in items:
        sku = item.get('armSkuName', 'N/A')
        region = item.get('armRegionName', 'N/A')
        product = item.get('productName', 'N/A')
        price = item.get('retailPrice', 0)
        unit = item.get('unitOfMeasure', 'N/A')
        pricing_type = item.get('type', 'N/A')
        currency = item.get('currencyCode', 'USD')
        
        # Determine OS from product name
        os_type = 'Windows' if 'windows' in product.lower() else 'Linux'
        
        print(f"{product}")
        print(f"  SKU:      {sku}")
        print(f"  Region:   {region}")
        print(f"  OS:       {os_type}")
        print(f"  Type:     {pricing_type}")
        print(f"  Price:    {currency} ${price:.4f}/{unit}")
        
        if show_spec:
            # Show additional details if available
            meter_name = item.get('meterName')
            if meter_name:
                print(f"  Meter:    {meter_name}")
        
        print()


def main():
    parser = argparse.ArgumentParser(
        description='Query Azure VM pricing from Retail Prices API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Get Linux pricing for Standard_D48ds_v4 in East US
  %(prog)s --sku Standard_D48ds_v4 --region eastus
  
  # Get Windows pricing
  %(prog)s --sku Standard_D48ds_v4 --region eastus --os windows
  
  # CSV format
  %(prog)s --sku Standard_D48ds_v4 --region eastus --format csv
  
  # JSON format with all details
  %(prog)s --sku Standard_D48ds_v4 --region eastus --format json
  
  # Show specifications
  %(prog)s --sku Standard_D48ds_v4 --region eastus --show-spec
  
  # Dev/Test pricing
  %(prog)s --sku Standard_D48ds_v4 --region eastus --pricing-type DevTestConsumption

Common Regions:
  eastus, westus, centralus, northeurope, westeurope, southeastasia,
  eastus2, westus2, uksouth, japaneast, australiaeast
        '''
    )
    
    parser.add_argument(
        '--sku',
        required=True,
        help='VM SKU name (e.g., Standard_D48ds_v4, Standard_E16s_v3)'
    )
    
    parser.add_argument(
        '--region',
        required=True,
        help='Azure region name (e.g., eastus, westus, northeurope)'
    )
    
    parser.add_argument(
        '--os',
        choices=['linux', 'windows'],
        default='linux',
        help='Operating system type (default: linux)'
    )
    
    parser.add_argument(
        '--pricing-type',
        choices=['Consumption', 'DevTestConsumption', 'Reservation'],
        default='Consumption',
        help='Pricing type (default: Consumption for pay-as-you-go)'
    )
    
    parser.add_argument(
        '--format',
        choices=['text', 'json', 'csv'],
        default='text',
        help='Output format (default: text)'
    )
    
    parser.add_argument(
        '--show-spec',
        action='store_true',
        help='Show VM specifications (cores, RAM, disk)'
    )
    
    args = parser.parse_args()
    
    # Query the API
    items = query_pricing(args.sku, args.region, args.os)
    
    if not items:
        print(f"No pricing data found for {args.sku} in {args.region}", file=sys.stderr)
        return 1
    
    # Filter items
    filtered_items = filter_items(items, args.os, args.pricing_type)
    
    if not filtered_items:
        print(f"No {args.os} {args.pricing_type} pricing found for {args.sku} in {args.region}", 
              file=sys.stderr)
        # Show what was found
        if items:
            print(f"\nFound {len(items)} pricing items of other types:", file=sys.stderr)
            for item in items[:3]:
                print(f"  - {item.get('productName')} ({item.get('type')})", file=sys.stderr)
        return 1
    
    # Add metadata comment for CSV/JSON output
    if args.format in ['csv', 'json']:
        import datetime
        print("# Generated by: get_vm_pricing.py", file=sys.stderr if args.format == 'json' else sys.stdout)
        print(f"# Date generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", file=sys.stderr if args.format == 'json' else sys.stdout)
        print(f"# Command: get_vm_pricing.py --sku {args.sku} --region {args.region} --os {args.os} --pricing-type {args.pricing_type} --format {args.format}", file=sys.stderr if args.format == 'json' else sys.stdout)
        print("#", file=sys.stderr if args.format == 'json' else sys.stdout)
    
    # Format and print output
    format_output(filtered_items, args.format, args.show_spec)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
