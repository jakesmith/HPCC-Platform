#!/usr/bin/env python3
"""
Unit tests for get_global_metrics.py script.

This test module verifies the XML request generation and response parsing
functionality of the HPCCGlobalMetricsClient without requiring a live HPCC server.
"""

import unittest
import sys
import os

# Add the tools directory to the path to import the script
tools_path = os.path.join(os.path.dirname(__file__), '..', '..', 'tools')
sys.path.insert(0, tools_path)

from get_global_metrics import HPCCGlobalMetricsClient, parse_dimensions, format_output


class TestHPCCGlobalMetricsClient(unittest.TestCase):
    """Test cases for HPCCGlobalMetricsClient."""
    
    def setUp(self):
        """Set up test client."""
        self.client = HPCCGlobalMetricsClient("localhost", 8010)
    
    def test_create_request_xml_minimal(self):
        """Test creating minimal XML request."""
        xml = self.client.create_request_xml()
        expected = "<GetGlobalMetricsRequest />"
        self.assertEqual(xml.replace('>', ' />'), expected.replace('>', ' />'))
    
    def test_create_request_xml_with_category(self):
        """Test creating XML request with category."""
        xml = self.client.create_request_xml(category="categoryOne")
        self.assertIn("<Category>categoryOne</Category>", xml)
        self.assertIn("<GetGlobalMetricsRequest>", xml)
    
    def test_create_request_xml_with_all_category(self):
        """Test creating XML request with 'All' category (should be omitted)."""
        xml = self.client.create_request_xml(category="All")
        self.assertNotIn("<Category>", xml)
        self.assertIn("<GetGlobalMetricsRequest", xml)
    
    def test_create_request_xml_with_dimensions(self):
        """Test creating XML request with dimensions."""
        dimensions = {"user": "alice", "cluster": "thor1"}
        xml = self.client.create_request_xml(dimensions=dimensions)
        
        self.assertIn("<Dimensions>", xml)
        self.assertIn("<Dimension>", xml)
        self.assertIn("<Name>user</Name>", xml)
        self.assertIn("<Value>alice</Value>", xml)
        self.assertIn("<Name>cluster</Name>", xml)
        self.assertIn("<Value>thor1</Value>", xml)
    
    def test_create_request_xml_with_datetime_range(self):
        """Test creating XML request with date/time range."""
        xml = self.client.create_request_xml(
            start_time="1999-01-01T00:00:00",
            end_time="2099-01-01T00:00:00"
        )
        
        self.assertIn("<DateTimeRange>", xml)
        self.assertIn("<Start>1999-01-01T00:00:00</Start>", xml)
        self.assertIn("<End>2099-01-01T00:00:00</End>", xml)
    
    def test_create_request_xml_complete(self):
        """Test creating complete XML request with all parameters."""
        dimensions = {"user": "bob", "cluster": "thor1"}
        xml = self.client.create_request_xml(
            category="categoryOne",
            dimensions=dimensions,
            start_time="1999-01-01T00:00:00",
            end_time="2099-01-01T00:00:00"
        )
        
        # Verify all elements are present
        self.assertIn("<GetGlobalMetricsRequest>", xml)
        self.assertIn("<Category>categoryOne</Category>", xml)
        self.assertIn("<Dimensions>", xml)
        self.assertIn("<Name>user</Name>", xml)
        self.assertIn("<Value>bob</Value>", xml)
        self.assertIn("<DateTimeRange>", xml)
        self.assertIn("<Start>1999-01-01T00:00:00</Start>", xml)
        self.assertIn("<End>2099-01-01T00:00:00</End>", xml)
    
    def test_parse_response_based_on_cppunit_test(self):
        """Test parsing response based on the expected format from cppunit tests."""
        # This is the expected XML from the cppunit test case
        xml_response = """<GetGlobalMetricsResponse><GlobalMetrics>
            <GlobalMetric>
                <Category>categoryTwo</Category>
                <DateTimeRange><Start>1999070112</Start><End>1999070112</End></DateTimeRange>
                <Stats><Stat><Name>TimeLocalExecute</Name><Value>444</Value></Stat></Stats>
            </GlobalMetric>
            <GlobalMetric>
                <Category>categoryOne</Category>
                <Dimensions>
                    <Dimension><Name>user</Name><Value>alice</Value></Dimension>
                </Dimensions>
                <DateTimeRange><Start>1999070112</Start><End>1999070112</End></DateTimeRange>
                <Stats>
                    <Stat><Name>TimeLocalExecute</Name><Value>222</Value></Stat>
                    <Stat><Name>CostExecute</Name><Value>5</Value></Stat>
                </Stats>
            </GlobalMetric>
        </GlobalMetrics></GetGlobalMetricsResponse>"""
        
        metrics = self.client.parse_response(xml_response)
        
        # Verify we got 2 metrics
        self.assertEqual(len(metrics), 2)
        
        # Verify first metric
        metric1 = metrics[0]
        self.assertEqual(metric1['category'], 'categoryTwo')
        self.assertEqual(metric1['dimensions'], {})
        self.assertEqual(metric1['datetime_range']['start'], '1999070112')
        self.assertEqual(metric1['datetime_range']['end'], '1999070112')
        self.assertEqual(metric1['stats']['TimeLocalExecute'], 444)
        
        # Verify second metric
        metric2 = metrics[1]
        self.assertEqual(metric2['category'], 'categoryOne')
        self.assertEqual(metric2['dimensions'], {'user': 'alice'})
        self.assertEqual(metric2['datetime_range']['start'], '1999070112')
        self.assertEqual(metric2['datetime_range']['end'], '1999070112')
        self.assertEqual(metric2['stats']['TimeLocalExecute'], 222)
        self.assertEqual(metric2['stats']['CostExecute'], 5)
    
    def test_filter_stats(self):
        """Test filtering statistics."""
        metrics = [
            {
                'category': 'test',
                'dimensions': {},
                'stats': {
                    'TimeLocalExecute': 100,
                    'CostExecute': 5,
                    'OtherStat': 200
                }
            },
            {
                'category': 'test2',
                'dimensions': {},
                'stats': {
                    'TimeLocalExecute': 150,
                    'UnwantedStat': 999
                }
            }
        ]
        
        filtered = self.client.filter_stats(metrics, ['TimeLocalExecute', 'CostExecute'])
        
        # Verify filtering
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]['stats'], {'TimeLocalExecute': 100, 'CostExecute': 5})
        self.assertEqual(filtered[1]['stats'], {'TimeLocalExecute': 150})


class TestUtilityFunctions(unittest.TestCase):
    """Test utility functions."""
    
    def test_parse_dimensions(self):
        """Test parsing dimension arguments."""
        dimensions = parse_dimensions(['user:alice', 'cluster:thor1', 'key:value with spaces'])
        expected = {
            'user': 'alice',
            'cluster': 'thor1',
            'key': 'value with spaces'
        }
        self.assertEqual(dimensions, expected)
    
    def test_parse_dimensions_invalid_format(self):
        """Test parsing dimensions with invalid format."""
        with self.assertRaises(ValueError):
            parse_dimensions(['invalid_format'])
    
    def test_format_output_json(self):
        """Test JSON output formatting."""
        metrics = [{'category': 'test', 'stats': {'count': 42}}]
        output = format_output(metrics, 'json')
        self.assertIn('"category": "test"', output)
        self.assertIn('"count": 42', output)
    
    def test_format_output_table(self):
        """Test table output formatting."""
        metrics = [
            {
                'category': 'test',
                'dimensions': {'user': 'alice'},
                'datetime_range': {'start': '2023-01-01', 'end': '2023-12-31'},
                'stats': {'count': 42}
            }
        ]
        output = format_output(metrics, 'table')
        self.assertIn('Global Metrics:', output)
        self.assertIn('Category: test', output)
        self.assertIn('user: alice', output)
        self.assertIn('count: 42', output)
    
    def test_format_output_csv(self):
        """Test CSV output formatting."""
        metrics = [
            {
                'category': 'test',
                'dimensions': {'user': 'alice'},
                'datetime_range': {'start': '2023-01-01', 'end': '2023-12-31'},
                'stats': {'count': 42, 'total': 100}
            }
        ]
        output = format_output(metrics, 'csv')
        lines = output.strip().split('\n')
        
        # Check header
        self.assertEqual(lines[0], 'category,dimensions,start_time,end_time,stat_name,stat_value')
        
        # Check data lines
        self.assertEqual(len(lines), 3)  # Header + 2 stat lines
        self.assertIn('test,user=alice,2023-01-01,2023-12-31,count,42', lines)
        self.assertIn('test,user=alice,2023-01-01,2023-12-31,total,100', lines)
    
    def test_format_output_empty_metrics(self):
        """Test output formatting with empty metrics."""
        self.assertEqual(format_output([], 'table'), "No metrics found.")
        self.assertEqual(format_output([], 'json'), "[]")
        self.assertIn('category,dimensions', format_output([], 'csv'))


if __name__ == '__main__':
    unittest.main()