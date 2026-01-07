#!/usr/bin/env python3
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

"""
Test script for categorization logic in azure_log_analytics_analyze.py
"""

import sys
import re

# Import the patterns from the analyze script
sys.path.insert(0, '.')
from azure_log_analytics_analyze import categorize_pod, HPCC_COMPONENT_PATTERNS, SYSTEM_COMPONENT_PATTERNS


def test_hpcc_components():
    """Test HPCC component categorization."""
    print("Testing HPCC Component Categorization")
    print("=" * 80)
    
    test_cases = [
        # (pod_name, namespace, controller_name, expected_category, expected_component)
        ('myhpcc-dali-0', 'default', 'myhpcc-dali', 'hpcc', 'dali'),
        ('cluster-dali-0', 'default', 'cluster-dali', 'hpcc', 'dali'),
        ('myhpcc-thor-thormanager-0', 'default', 'myhpcc-thor-thormanager', 'hpcc', 'thor'),
        ('myhpcc-thor-thorworker-0', 'default', 'myhpcc-thor-thorworker', 'hpcc', 'thor'),
        ('my-thor-0', 'default', 'my-thor', 'hpcc', 'thor'),
        ('myhpcc-roxie-server-0', 'default', 'myhpcc-roxie-server', 'hpcc', 'roxie'),
        ('prod-roxie-agent-1', 'default', 'prod-roxie-agent', 'hpcc', 'roxie'),
        ('myhpcc-toposerver-0', 'default', 'myhpcc-toposerver', 'hpcc', 'roxie'),
        ('myhpcc-eclservices-0', 'default', 'myhpcc-eclservices', 'hpcc', 'esp'),
        ('myhpcc-eclwatch-0', 'default', 'myhpcc-eclwatch', 'hpcc', 'esp'),
        ('test-esp-0', 'default', 'test-esp', 'hpcc', 'esp'),
        ('myhpcc-eclagent-0', 'default', 'myhpcc-eclagent', 'hpcc', 'eclagent'),
        ('myhpcc-eclccserver-0', 'default', 'myhpcc-eclccserver', 'hpcc', 'eclccserver'),
        ('myhpcc-eclscheduler-0', 'default', 'myhpcc-eclscheduler', 'hpcc', 'eclscheduler'),
        ('myhpcc-dfuserver-0', 'default', 'myhpcc-dfuserver', 'hpcc', 'dfuserver'),
        ('myhpcc-sasha-0', 'default', 'myhpcc-sasha', 'hpcc', 'sasha'),
        ('myhpcc-dafilesrv-0', 'default', 'myhpcc-dafilesrv', 'hpcc', 'dafilesrv'),
    ]
    
    passed = 0
    failed = 0
    
    for pod_name, namespace, controller_name, expected_category, expected_component in test_cases:
        category, component, is_hpcc = categorize_pod(pod_name, namespace, controller_name)
        
        if category == expected_category and component == expected_component and is_hpcc:
            print(f"✓ PASS: {pod_name:40s} -> {component}")
            passed += 1
        else:
            print(f"✗ FAIL: {pod_name:40s} -> Expected: {expected_component}, Got: {component}")
            failed += 1
    
    print(f"\nHPCC Components: {passed} passed, {failed} failed")
    print()
    return failed == 0


def test_system_components():
    """Test system component categorization."""
    print("Testing System Component Categorization")
    print("=" * 80)
    
    test_cases = [
        # (pod_name, namespace, controller_name, expected_category)
        ('kube-proxy-abc123', 'kube-system', 'kube-proxy', 'kubernetes-system'),
        ('kube-dns-xyz', 'kube-system', 'kube-dns', 'kubernetes-system'),
        ('coredns-12345', 'kube-system', 'coredns', 'kubernetes-system'),
        ('omsagent-xyz789', 'kube-system', 'omsagent', 'azure-system'),
        ('aks-link-abc', 'default', 'aks-link', 'azure-system'),
        ('nginx-ingress-controller-abc', 'ingress-nginx', 'nginx-ingress', 'ingress'),
        ('prometheus-server-0', 'monitoring', 'prometheus-server', 'monitoring'),
        ('grafana-0', 'monitoring', 'grafana', 'monitoring'),
        ('fluentd-abc', 'logging', 'fluentd', 'logging'),
        ('elasticsearch-master-0', 'logging', 'elasticsearch-master', 'logging'),
    ]
    
    passed = 0
    failed = 0
    
    for pod_name, namespace, controller_name, expected_category in test_cases:
        category, component, is_hpcc = categorize_pod(pod_name, namespace, controller_name)
        
        if category == expected_category and not is_hpcc:
            print(f"✓ PASS: {pod_name:40s} -> {category}")
            passed += 1
        else:
            print(f"✗ FAIL: {pod_name:40s} -> Expected: {expected_category}, Got: {category}")
            failed += 1
    
    print(f"\nSystem Components: {passed} passed, {failed} failed")
    print()
    return failed == 0


def test_unknown_components():
    """Test unknown component categorization."""
    print("Testing Unknown Component Categorization")
    print("=" * 80)
    
    test_cases = [
        # (pod_name, namespace, controller_name)
        ('my-custom-app-0', 'default', 'my-custom-app'),
        ('random-service-123', 'myapp', 'random-service'),
    ]
    
    passed = 0
    failed = 0
    
    for pod_name, namespace, controller_name in test_cases:
        category, component, is_hpcc = categorize_pod(pod_name, namespace, controller_name)
        
        if category == 'unknown' and not is_hpcc:
            print(f"✓ PASS: {pod_name:40s} -> unknown")
            passed += 1
        else:
            print(f"✗ FAIL: {pod_name:40s} -> Expected: unknown, Got: {category}")
            failed += 1
    
    print(f"\nUnknown Components: {passed} passed, {failed} failed")
    print()
    return failed == 0


def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("Azure Log Analytics Analyze - Categorization Tests")
    print("=" * 80 + "\n")
    
    all_passed = True
    
    all_passed = test_hpcc_components() and all_passed
    all_passed = test_system_components() and all_passed
    all_passed = test_unknown_components() and all_passed
    
    print("=" * 80)
    if all_passed:
        print("✓ ALL TESTS PASSED")
        print("=" * 80)
        return 0
    else:
        print("✗ SOME TESTS FAILED")
        print("=" * 80)
        return 1


if __name__ == '__main__':
    sys.exit(main())
