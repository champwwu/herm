#!/usr/bin/env python3
"""
Publisher REST API test suite.

Tests the REST endpoints of BBO, PriceBands, and VolumeBands publishers.
"""

from testplan.testing.multitest import testcase, testsuite
from .base_test_suite import BaseTestSuite


@testsuite
class PublisherRESTTests(BaseTestSuite):
    """Test suite for publisher REST endpoints"""
    
    @testcase(tags={'bbo', 'publisher', 'smoke'})
    def test_bbo_publisher_endpoint(self, env, result):
        """Test BBO publisher /latestOrderBook endpoint"""
        import requests
        import time
        
        PUBLISHER_BBO_REST_PORT = 8085
        
        # Wait for data to be available
        max_retries = 10
        for i in range(max_retries):
            try:
                url = f"http://localhost:{PUBLISHER_BBO_REST_PORT}/latestOrderBook"
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    result.contain("symbol", data, "Order book should contain symbol")
                    result.contain("timestamp_us", data, "Order book should contain timestamp_us")
                    result.contain("bids", data, "Order book should contain bids array")
                    result.contain("asks", data, "Order book should contain asks array")
                    
                    # Verify bids and asks are arrays
                    result.true(isinstance(data["bids"], list), "Bids should be a list")
                    result.true(isinstance(data["asks"], list), "Asks should be a list")
                    
                    # Verify structure matches proto
                    if len(data["bids"]) > 0:
                        bid = data["bids"][0]
                        result.contain("price", bid, "Bid should contain price")
                        result.contain("quantity", bid, "Bid should contain quantity")
                        result.greater(bid["price"], 0, "Bid price should be positive")
                    
                    if len(data["asks"]) > 0:
                        ask = data["asks"][0]
                        result.contain("price", ask, "Ask should contain price")
                        result.contain("quantity", ask, "Ask should contain quantity")
                        result.greater(ask["price"], 0, "Ask price should be positive")
                    
                    return
            except Exception as e:
                if i < max_retries - 1:
                    time.sleep(2)
                    continue
                raise
    
    @testcase(tags={'price_bands', 'publisher'})
    def test_price_bands_publisher_endpoint(self, env, result):
        """Test PriceBands publisher /latestOrderBook endpoint"""
        import requests
        import time
        
        PUBLISHER_PRICE_BANDS_REST_PORT = 8082
        
        max_retries = 10
        for i in range(max_retries):
            try:
                url = f"http://localhost:{PUBLISHER_PRICE_BANDS_REST_PORT}/latestOrderBook"
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    result.contain("symbol", data, "Order book should contain symbol")
                    result.contain("bids", data, "Order book should contain bids array")
                    result.contain("asks", data, "Order book should contain asks array")
                    return
            except Exception as e:
                if i < max_retries - 1:
                    time.sleep(2)
                    continue
                raise
    
    @testcase(tags={'volume_bands', 'publisher'})
    def test_volume_bands_publisher_endpoint(self, env, result):
        """Test VolumeBands publisher /latestOrderBook endpoint"""
        import requests
        import time
        
        PUBLISHER_VOLUME_BANDS_REST_PORT = 8083
        
        max_retries = 10
        for i in range(max_retries):
            try:
                url = f"http://localhost:{PUBLISHER_VOLUME_BANDS_REST_PORT}/latestOrderBook"
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    result.contain("symbol", data, "Order book should contain symbol")
                    result.contain("bids", data, "Order book should contain bids array")
                    result.contain("asks", data, "Order book should contain asks array")
                    return
            except Exception as e:
                if i < max_retries - 1:
                    time.sleep(2)
                    continue
                raise
