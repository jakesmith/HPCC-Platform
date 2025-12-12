#!/usr/bin/env python3
"""
Get Azure storage account pricing information based on storage type and configuration.

This script queries Azure to determine a storage account's configuration (SKU, tier, location)
and outputs the relevant pricing information for that storage type.

Usage:
    python3 get_storage_pricing.py --subscription <subscription> --storage-account <name>

Examples:
    # Get pricing for a storage account
    python3 get_storage_pricing.py --subscription us-dataland-prod --storage-account hpccluwildata1
    
    # Save to CSV file
    python3 get_storage_pricing.py --subscription us-dataland-prod --storage-account hpccluwildata1 > pricing.csv

Output Format (CSV):
    StorageAccount,Location,SKU,Kind,AccessTier,PricingCategory,Unit,PriceUSD,Notes
    hpccluwildata1,eastus,Standard_ZRS,StorageV2,Hot,Storage,GB/month,0.0246,Hot Tier ZRS
    hpccluwildata1,eastus,Standard_ZRS,StorageV2,Hot,Write Operations,10000 ops,0.065,
    ...

Note: Pricing is based on Azure's published rates and may vary by region and specific configuration.
      Always verify current pricing at https://azure.microsoft.com/en-us/pricing/details/storage/
"""

import sys
import json
import subprocess
import argparse
from datetime import datetime


# Azure Storage pricing table (as of 2025, approximate rates)
# Format: {(kind, sku_base, tier, location_type): {category: (price, unit, notes)}}
STORAGE_PRICING = {
    # General Purpose v2 - Standard LRS
    ('StorageV2', 'Standard_LRS', 'Hot', 'US'): {
        'Storage': (0.0184, 'GB/month', 'First 50 TB'),
        'Storage (50-450 TB)': (0.0177, 'GB/month', '50-450 TB'),
        'Storage (>450 TB)': (0.0170, 'GB/month', 'Over 450 TB'),
        'Write Operations': (0.05, '10000 ops', 'Put, Copy, Post, List'),
        'Read Operations': (0.004, '10000 ops', 'Get, All others'),
        'Data Retrieval': (0.0, 'GB', 'No retrieval charge for Hot'),
    },
    # General Purpose v2 - Standard ZRS
    ('StorageV2', 'Standard_ZRS', 'Hot', 'US'): {
        'Storage': (0.0246, 'GB/month', 'First 50 TB'),
        'Storage (50-450 TB)': (0.0236, 'GB/month', '50-450 TB'),
        'Storage (>450 TB)': (0.0227, 'GB/month', 'Over 450 TB'),
        'Write Operations': (0.065, '10000 ops', 'Put, Copy, Post, List'),
        'Read Operations': (0.0044, '10000 ops', 'Get, All others'),
        'Data Retrieval': (0.0, 'GB', 'No retrieval charge for Hot'),
    },
    # General Purpose v2 - Standard LRS Cool
    ('StorageV2', 'Standard_LRS', 'Cool', 'US'): {
        'Storage': (0.0115, 'GB/month', 'First 50 TB'),
        'Storage (50-450 TB)': (0.0110, 'GB/month', '50-450 TB'),
        'Storage (>450 TB)': (0.0106, 'GB/month', 'Over 450 TB'),
        'Write Operations': (0.10, '10000 ops', 'Put, Copy, Post, List'),
        'Read Operations': (0.01, '10000 ops', 'Get, All others'),
        'Data Retrieval': (0.01, 'GB', 'Cool tier retrieval'),
    },
    # General Purpose v2 - Standard ZRS Cool
    ('StorageV2', 'Standard_ZRS', 'Cool', 'US'): {
        'Storage': (0.0154, 'GB/month', 'First 50 TB'),
        'Storage (50-450 TB)': (0.0147, 'GB/month', '50-450 TB'),
        'Storage (>450 TB)': (0.0141, 'GB/month', 'Over 450 TB'),
        'Write Operations': (0.13, '10000 ops', 'Put, Copy, Post, List'),
        'Read Operations': (0.01, '10000 ops', 'Get, All others'),
        'Data Retrieval': (0.01, 'GB', 'Cool tier retrieval'),
    },
    # General Purpose v2 - Premium Block Blob
    ('BlockBlobStorage', 'Premium_LRS', None, 'US'): {
        'Storage': (0.15, 'GB/month', 'Premium block blob'),
        'Write Operations': (0.05, '10000 ops', 'Put Block'),
        'Read Operations': (0.004, '10000 ops', 'Get operations'),
        'Other Operations': (0.65, '10000 ops', 'Put Blob/Block List'),
    },
    # File Storage - Premium
    ('FileStorage', 'Premium_LRS', None, 'US'): {
        'Storage': (0.15, 'GB/month', 'Premium file storage'),
        'Write Operations': (0.04, '10000 ops', 'All operations'),
        'Read Operations': (0.004, '10000 ops', 'All operations'),
    },
}

# Data transfer pricing (egress from Azure)
DATA_TRANSFER_PRICING = {
    'First 100 GB': (0.0, 'GB', 'Free tier'),
    '100 GB - 10 TB': (0.087, 'GB', 'North America egress'),
    '10 TB - 50 TB': (0.083, 'GB', 'North America egress'),
    '50 TB - 150 TB': (0.07, 'GB', 'North America egress'),
    'Over 150 TB': (0.05, 'GB', 'North America egress'),
}


def get_storage_account_info(subscription, storage_account):
    """Query Azure for storage account configuration.
    
    Returns:
        dict with keys: name, sku, kind, accessTier, location, enableHierarchicalNamespace
    """
    cmd = [
        'az', 'storage', 'account', 'show',
        '--name', storage_account,
        '--subscription', subscription,
        '--query', '{name:name, sku:sku.name, kind:kind, accessTier:accessTier, '
                   'enableHierarchicalNamespace:enableHierarchicalNamespace, location:location}',
        '-o', 'json'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error querying storage account: {e.stderr}", file=sys.stderr)
        sys.exit(1)


def map_location_to_region_type(location):
    """Map Azure location to pricing region type."""
    # US locations
    us_locations = ['eastus', 'eastus2', 'westus', 'westus2', 'westus3', 'centralus', 
                    'northcentralus', 'southcentralus', 'westcentralus']
    
    if location.lower() in us_locations:
        return 'US'
    
    # Default to US pricing for now (can be extended)
    return 'US'


def get_pricing_info(account_info):
    """Get pricing information based on storage account configuration.
    
    Returns:
        list of pricing entries: [(category, price, unit, notes), ...]
    """
    kind = account_info['kind']
    sku = account_info['sku']
    access_tier = account_info.get('accessTier')
    location = account_info['location']
    
    # Extract base SKU (Standard/Premium + redundancy)
    sku_base = sku  # e.g., Standard_ZRS, Premium_LRS
    
    # Map location to region type
    region_type = map_location_to_region_type(location)
    
    # Look up pricing
    pricing_key = (kind, sku_base, access_tier, region_type)
    
    if pricing_key in STORAGE_PRICING:
        pricing_data = STORAGE_PRICING[pricing_key]
    else:
        # Try without access tier for premium storage
        pricing_key = (kind, sku_base, None, region_type)
        if pricing_key in STORAGE_PRICING:
            pricing_data = STORAGE_PRICING[pricing_key]
        else:
            print(f"Warning: No pricing data for {kind}, {sku_base}, {access_tier}, {region_type}", 
                  file=sys.stderr)
            print(f"Available pricing keys:", file=sys.stderr)
            for key in STORAGE_PRICING.keys():
                print(f"  {key}", file=sys.stderr)
            return []
    
    # Convert to list format
    pricing_list = []
    for category, (price, unit, notes) in pricing_data.items():
        pricing_list.append((category, price, unit, notes))
    
    # Add data transfer pricing
    for category, (price, unit, notes) in DATA_TRANSFER_PRICING.items():
        pricing_list.append((f'Data Transfer Out - {category}', price, unit, notes))
    
    return pricing_list


def output_csv(account_info, pricing_info, args):
    """Output pricing information as CSV."""
    # Metadata header
    print("# Generated by: get_storage_pricing.py")
    print(f"# Date generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# Command: {' '.join(sys.argv)}")
    print(f"# Storage Account: {account_info['name']}")
    print(f"# Subscription: {args.subscription}")
    print(f"# Location: {account_info['location']}")
    print(f"# SKU: {account_info['sku']}")
    print(f"# Kind: {account_info['kind']}")
    if account_info.get('accessTier'):
        print(f"# Access Tier: {account_info['accessTier']}")
    print("#")
    print("# Note: Pricing is approximate and based on Azure's published rates.")
    print("#       Always verify current pricing at https://azure.microsoft.com/pricing/")
    print("#")
    
    # CSV header
    print("StorageAccount,Location,SKU,Kind,AccessTier,PricingCategory,PriceUSD,Unit,Notes")
    
    # Output pricing rows
    for category, price, unit, notes in pricing_info:
        access_tier = account_info.get('accessTier', '')
        # Escape fields with commas
        category_escaped = f'"{category}"' if ',' in category else category
        notes_escaped = f'"{notes}"' if ',' in notes else notes
        
        print(f"{account_info['name']},{account_info['location']},{account_info['sku']},"
              f"{account_info['kind']},{access_tier},{category_escaped},{price},{unit},{notes_escaped}")


def calculate_example_costs(pricing_info):
    """Calculate example costs for common scenarios."""
    print("\n# EXAMPLE COST CALCULATIONS", file=sys.stderr)
    print("# " + "=" * 60, file=sys.stderr)
    print("#", file=sys.stderr)
    
    # Find storage and operation prices
    storage_price_per_gb = None
    write_ops_price = None
    read_ops_price = None
    
    for category, price, unit, notes in pricing_info:
        if category == 'Storage' and 'month' in unit:
            storage_price_per_gb = price
        elif category == 'Write Operations':
            write_ops_price = price
        elif category == 'Read Operations':
            read_ops_price = price
    
    if storage_price_per_gb:
        storage_per_tb_month = storage_price_per_gb * 1024
        storage_per_tb_day = storage_per_tb_month / 30
        print(f"# Store 1 TB for 24 hours: ${storage_per_tb_day:.2f}", file=sys.stderr)
        print(f"# Store 1 TB for 30 days:  ${storage_per_tb_month:.2f}", file=sys.stderr)
    
    if read_ops_price or write_ops_price:
        print("#", file=sys.stderr)
        print("# Note: Azure charges by operation count, not by data volume.", file=sys.stderr)
        print("#       The cost to read/write 1 TB depends on:", file=sys.stderr)
        print("#       - Number of operations (depends on blob/file size)", file=sys.stderr)
        print("#       - Example: 1 TB split into 1 GB files = 1,024 operations", file=sys.stderr)
        
        if read_ops_price:
            # Example: 1 TB as 1 GB files = 1,024 ops = 0.1024 * 10,000 ops
            cost_1gb_files = (1024 / 10000) * read_ops_price
            print(f"#           Read 1,024 x 1GB files (1TB): ${cost_1gb_files:.4f}", file=sys.stderr)
        
        if write_ops_price:
            cost_1gb_files = (1024 / 10000) * write_ops_price
            print(f"#           Write 1,024 x 1GB files (1TB): ${cost_1gb_files:.4f}", file=sys.stderr)
    
    print("#", file=sys.stderr)
    print("# " + "=" * 60, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description='Get Azure storage account pricing information',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get pricing for a storage account
  %(prog)s --subscription us-dataland-prod --storage-account hpccluwildata1
  
  # Save to CSV
  %(prog)s --subscription us-dataland-prod --storage-account hpccluwildata1 > pricing.csv
  
Output includes:
  - Storage costs (per GB/month)
  - Operation costs (read, write, per 10,000 operations)
  - Data transfer/egress costs
  - Example cost calculations
  
Note: Pricing is approximate. Always verify at https://azure.microsoft.com/pricing/
        """
    )
    
    parser.add_argument(
        '--subscription',
        required=True,
        help='Azure subscription name or ID'
    )
    
    parser.add_argument(
        '--storage-account',
        required=True,
        help='Storage account name'
    )
    
    args = parser.parse_args()
    
    # Get storage account information
    print(f"Querying storage account '{args.storage_account}'...", file=sys.stderr)
    account_info = get_storage_account_info(args.subscription, args.storage_account)
    
    print(f"Account type: {account_info['kind']}, SKU: {account_info['sku']}, "
          f"Tier: {account_info.get('accessTier', 'N/A')}", file=sys.stderr)
    print(f"Location: {account_info['location']}", file=sys.stderr)
    print(file=sys.stderr)
    
    # Get pricing information
    pricing_info = get_pricing_info(account_info)
    
    if not pricing_info:
        print("Error: Could not determine pricing for this storage configuration", file=sys.stderr)
        sys.exit(1)
    
    # Output CSV
    output_csv(account_info, pricing_info, args)
    
    # Calculate example costs (to stderr)
    calculate_example_costs(pricing_info)


if __name__ == '__main__':
    main()
