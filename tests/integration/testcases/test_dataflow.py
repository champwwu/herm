#!/usr/bin/env python3
"""
Data flow test suite.

Tests end-to-end data flow from mock exchanges through aggregator to publishers.
"""

from testplan.testing.multitest import testcase, testsuite
from .base_test_suite import BaseTestSuite


@testsuite
class DataFlowTests(BaseTestSuite):
    """Test suite for end-to-end data flow"""
    
    @testcase(tags={'dataflow', 'e2e', 'smoke'})
    def test_data_flow_from_mock_to_publishers(self, env, result):
        """Test that data flows from mock exchange through aggregator to publishers"""
        import requests
        import time
        
        PUBLISHER_BBO_REST_PORT = 8085
        PUBLISHER_PRICE_BANDS_REST_PORT = 8082
        PUBLISHER_VOLUME_BANDS_REST_PORT = 8083
        
        # Wait for data to stabilize
        self.wait_for_propagation(5)
        
        # Check all publishers have data
        publishers = [
            ("BBO", PUBLISHER_BBO_REST_PORT),
            ("PriceBands", PUBLISHER_PRICE_BANDS_REST_PORT),
            ("VolumeBands", PUBLISHER_VOLUME_BANDS_REST_PORT),
        ]
        
        for name, port in publishers:
            url = f"http://localhost:{port}/latestOrderBook"
            try:
                response = requests.get(url, timeout=5)
                result.equal(response.status_code, 200, 
                            f"{name} publisher should return 200")
                data = response.json()
                result.contain("timestamp_us", data, 
                             f"{name} publisher should have timestamp")
                result.greater(data.get("timestamp_us", 0), 0,
                             f"{name} publisher timestamp should be positive")
            except Exception as e:
                result.fail(f"Failed to get data from {name} publisher: {e}")
