#!/usr/bin/env python3

'''
/*#############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################ */
'''

import sys
import argparse
import requests
import xml.etree.ElementTree as ET
from urllib.parse import urljoin

def parse_wuquery_response(xml_content):
    """Parse WUQuery XML response and extract workunits with their states."""
    try:
        root = ET.fromstring(xml_content)
        
        # Find all workunit elements in the response
        # The namespace might be present, so we need to handle both cases
        workunits = []
        
        # Try with namespace-aware search
        ns = {'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
              'ws': 'urn:hpccsystems:ws:wsworkunits'}
        
        # Look for workunits in the response
        for wu_elem in root.findall('.//ws:ECLWorkunit', ns):
            wuid = wu_elem.find('ws:Wuid', ns)
            state = wu_elem.find('ws:State', ns)
            if wuid is not None:
                workunits.append({
                    'wuid': wuid.text if wuid.text else '',
                    'state': state.text if state is not None and state.text else ''
                })
        
        # If no workunits found with namespace, try without
        if not workunits:
            for wu_elem in root.findall('.//ECLWorkunit'):
                wuid = wu_elem.find('Wuid')
                state = wu_elem.find('State')
                if wuid is not None:
                    workunits.append({
                        'wuid': wuid.text if wuid.text else '',
                        'state': state.text if state is not None and state.text else ''
                    })
        
        return workunits
    except ET.ParseError as e:
        print(f"Error parsing XML response: {e}", file=sys.stderr)
        return []

def query_workunits(esp_server, start_date, end_date):
    """Query workunits from ESP server between start and end dates."""
    
    # Construct the URL for the WUQuery service
    base_url = f"http://{esp_server}"
    service_url = urljoin(base_url, "/WsWorkunits/WUQuery.json")
    
    # Build query parameters
    params = {
        'StartDate': start_date,
        'EndDate': end_date,
        'PageSize': 100  # Fetch up to 100 workunits per request
    }
    
    try:
        # Make the request
        response = requests.get(service_url, params=params, timeout=30)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Extract workunits from JSON response
        workunits = []
        wu_response = data.get('WUQueryResponse', {})
        wu_list = wu_response.get('Workunits', {}).get('ECLWorkunit', [])
        
        # Handle case where single workunit is returned as dict instead of list
        if isinstance(wu_list, dict):
            wu_list = [wu_list]
        
        for wu in wu_list:
            workunits.append({
                'wuid': wu.get('Wuid', ''),
                'state': wu.get('State', '')
            })
        
        return workunits
        
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to ESP server: {e}", file=sys.stderr)
        return []
    except (KeyError, ValueError) as e:
        print(f"Error parsing response: {e}", file=sys.stderr)
        return []

def main():
    parser = argparse.ArgumentParser(
        description='Fetch workunit IDs and states from ESP WsWorkunits service',
        usage='%(prog)s <espserver:port> <start-datestamp> <end-datestamp>'
    )
    parser.add_argument('espserver', help='ESP server address in format host:port (e.g., localhost:8010)')
    parser.add_argument('start_date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('end_date', help='End date in YYYY-MM-DD format')
    
    args = parser.parse_args()
    
    # Query the workunits
    workunits = query_workunits(args.espserver, args.start_date, args.end_date)
    
    if not workunits:
        print("No workunits found or error occurred", file=sys.stderr)
        return 1
    
    # Print results
    print(f"{'WUID':<20} {'State'}")
    print("-" * 50)
    for wu in workunits:
        print(f"{wu['wuid']:<20} {wu['state']}")
    
    print(f"\nTotal workunits: {len(workunits)}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
