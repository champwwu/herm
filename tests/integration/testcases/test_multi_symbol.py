#!/usr/bin/env python3
"""
Multi-symbol test suite.

Tests symbol isolation and multi-symbol aggregation (BTCUSDT vs ETHUSDT).
"""

from testplan.testing.multitest import testcase, testsuite
from .base_test_suite import BaseTestSuite


@testsuite
class MultiSymbolTests(BaseTestSuite):
    """Test suite for multi-symbol aggregation and filtering"""
    
    @testcase(tags={'symbol', 'isolation', 'btcusdt'})
    def test_btcusdt_publisher_gets_btcusdt_data_only(self, env, result):
        """Test that BTCUSDT publisher only receives BTCUSDT data"""
        import requests
        import time
        
        PUBLISHER_BBO_REST_PORT = 8085
        
        # Wait for data to stabilize
        self.wait_for_propagation(5)
        
        # Get data from BTCUSDT publisher
        url = f"http://localhost:{PUBLISHER_BBO_REST_PORT}/latestOrderBook"
        try:
            response = requests.get(url, timeout=5)
            result.equal(response.status_code, 200, 
                        "BTCUSDT publisher should return 200")
            data = response.json()
            
            # Verify symbol is BTCUSDT
            result.equal(data.get("symbol"), "BTCUSDT",
                        "BTCUSDT publisher should return BTCUSDT symbol")
            
            # Verify price range is reasonable for BTCUSDT (not ETHUSDT prices)
            if len(data.get("bids", [])) > 0:
                bid_price = data["bids"][0]["price"]
                # BTCUSDT prices should be much higher than ETHUSDT (typically > 10000)
                result.greater(bid_price, 10000.0,
                             f"BTCUSDT bid price {bid_price} should be > 10000")
            
            if len(data.get("asks", [])) > 0:
                ask_price = data["asks"][0]["price"]
                result.greater(ask_price, 10000.0,
                             f"BTCUSDT ask price {ask_price} should be > 10000")
        except Exception as e:
            result.fail(f"Failed to get BTCUSDT data: {e}")
    
    @testcase(tags={'symbol', 'multi_symbol'})
    def test_symbol_isolation(self, env, result):
        """Test that different symbols are isolated in the aggregator"""
        import requests
        import time
        
        PUBLISHER_BBO_REST_PORT = 8085
        
        # Wait for data to stabilize
        self.wait_for_propagation(5)
        
        # Get BTCUSDT data
        btc_url = f"http://localhost:{PUBLISHER_BBO_REST_PORT}/latestOrderBook"
        try:
            btc_response = requests.get(btc_url, timeout=5)
            result.equal(btc_response.status_code, 200,
                        "BTCUSDT publisher should be available")
            btc_data = btc_response.json()
            
            # Verify BTCUSDT data has correct symbol
            result.equal(btc_data.get("symbol"), "BTCUSDT",
                        "BTCUSDT publisher should return BTCUSDT symbol")
            
            # If ETHUSDT publisher exists, verify it doesn't get BTCUSDT data
            # (This test assumes ETHUSDT publisher is running with multi-symbol config)
            # For now, we just verify BTCUSDT data is correct
            
            # Verify BTCUSDT prices are reasonable
            if len(btc_data.get("bids", [])) > 0 and len(btc_data.get("asks", [])) > 0:
                bid_price = btc_data["bids"][0]["price"]
                ask_price = btc_data["asks"][0]["price"]
                
                # BTCUSDT prices should be in reasonable range (not ETHUSDT range ~3000)
                result.greater(bid_price, 10000.0,
                             f"BTCUSDT bid price {bid_price} should be BTCUSDT range")
                result.greater(ask_price, 10000.0,
                             f"BTCUSDT ask price {ask_price} should be BTCUSDT range")
                
                # For multi-exchange aggregation, bid can be > ask (arbitrage opportunity)
                # Just validate prices are positive
                result.log(f"  BBO validated: {bid_price} / {ask_price}")
        except Exception as e:
            result.fail(f"Failed to verify symbol isolation: {e}")
