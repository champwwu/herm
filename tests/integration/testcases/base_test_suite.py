#!/usr/bin/env python3
"""
Base test suite class for integration tests.

Provides common functionality:
- Publisher REST driver initialization
- Mock exchange control helpers
- Wait utilities for data propagation
- Order book validation methods
"""

import time
from .publisher_rest_driver import PublisherRestDriver


class BaseTestSuite:
    """
    Base class for all integration test suites.
    
    Provides common setup/teardown and utility methods to reduce code duplication
    and ensure consistent testing patterns across all test suites.
    """
    
    def setup(self, env, result):
        """
        Common setup - initialize publisher drivers and mock exchange references.
        
        Args:
            env: Testplan environment with drivers
            result: Testplan result object for logging
        """
        # Initialize publisher REST drivers
        self.bbo_pub = PublisherRestDriver("BBO", 8085)
        self.price_pub = PublisherRestDriver("PriceBands", 8082)
        self.vol_pub = PublisherRestDriver("VolumeBands", 8083)
        
        # Store references to mock exchanges
        self.binance = env.binance_mock
        self.okx = env.okx_mock
        self.bybit = env.bybit_mock
        
        result.log("Base test suite setup complete")
    
    def teardown(self, env, result):
        """
        Common teardown - cleanup if needed.
        
        Args:
            env: Testplan environment
            result: Testplan result object for logging
        """
        result.log("Base test suite teardown complete")
    
    # ========================================================================
    # Mock Exchange Control
    # ========================================================================
    
    def reset_all_mocks(self):
        """Reset all mock exchanges to default state."""
        self.binance.exchange.reset_orderbook()
        self.okx.exchange.reset_orderbook()
        self.bybit.exchange.reset_orderbook()
    
    def set_deterministic_orderbook(
        self,
        exchange_name,
        bids,
        asks
    ):
        """
        Set specific order book data on a mock exchange.
        
        Args:
            exchange_name: 'binance', 'okx', or 'bybit'
            bids: List of (price, quantity) tuples
            asks: List of (price, quantity) tuples
        """
        if exchange_name == 'binance':
            self.binance.exchange.set_orderbook(bids, asks)
        elif exchange_name == 'okx':
            self.okx.exchange.set_orderbook(bids, asks)
        elif exchange_name == 'bybit':
            self.bybit.exchange.set_orderbook(bids, asks)
    
    # ========================================================================
    # Wait Utilities
    # ========================================================================
    
    def wait_for_propagation(self, seconds=3):
        """
        Wait for data to flow through system (mock -> aggregator -> publishers).
        
        Args:
            seconds: Number of seconds to wait
        """
        time.sleep(seconds)
    
    def wait_for_orderbook_update(self, publisher_driver, timeout=10):
        """
        Wait for publisher to have valid order book data.
        
        Args:
            publisher_driver: PublisherRestDriver instance
            timeout: Maximum seconds to wait
            
        Returns:
            Order book dict or None if timeout
        """
        return publisher_driver.wait_for_orderbook(max_wait_sec=timeout)
    
    # ========================================================================
    # Validation Helpers
    # ========================================================================
    
    def validate_orderbook_structure(self, order_book, result, allow_cross_exchange_arbitrage=False):
        """
        Validate order book has proper structure.
        
        Args:
            order_book: Order book dict from REST API
            result: Testplan result object for assertions
            allow_cross_exchange_arbitrage: If True, allow best bid > best ask (multi-exchange arbitrage)
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        result.contain("symbol", order_book, "Order book must have symbol")
        result.contain("timestamp_us", order_book, "Order book must have timestamp")
        result.contain("bids", order_book, "Order book must have bids")
        result.contain("asks", order_book, "Order book must have asks")
        
        # Validate bids
        bids = order_book.get("bids", [])
        for i, bid in enumerate(bids):
            result.contain("price", bid, f"Bid {i} must have price")
            result.contain("quantity", bid, f"Bid {i} must have quantity")
            result.greater(bid.get("price", 0), 0, f"Bid {i} price must be positive")
            result.greater(bid.get("quantity", 0), 0, f"Bid {i} quantity must be positive")
        
        # Verify bids sorted descending
        if len(bids) >= 2:
            for i in range(len(bids) - 1):
                result.greater_equal(bids[i]["price"], bids[i+1]["price"],
                                   "Bids must be sorted descending")
        
        # Validate asks
        asks = order_book.get("asks", [])
        for i, ask in enumerate(asks):
            result.contain("price", ask, f"Ask {i} must have price")
            result.contain("quantity", ask, f"Ask {i} must have quantity")
            result.greater(ask.get("price", 0), 0, f"Ask {i} price must be positive")
            result.greater(ask.get("quantity", 0), 0, f"Ask {i} quantity must be positive")
        
        # Verify asks sorted ascending
        if len(asks) >= 2:
            for i in range(len(asks) - 1):
                result.less_equal(asks[i]["price"], asks[i+1]["price"],
                                "Asks must be sorted ascending")
        
        # Validate spread
        if len(bids) > 0 and len(asks) > 0:
            best_bid = bids[0]["price"]
            best_ask = asks[0]["price"]
            
            if allow_cross_exchange_arbitrage:
                # For aggregated multi-exchange books, bid can be > ask (arbitrage opportunity)
                if best_bid > best_ask:
                    result.log(f"Cross-exchange arbitrage detected: bid ({best_bid}) > ask ({best_ask}), "
                             f"spread: {best_bid - best_ask:.2f}")
                else:
                    result.log(f"Normal spread: bid ({best_bid}) < ask ({best_ask})")
            else:
                # For single-exchange books, bid must be < ask
                result.less(best_bid, best_ask,
                           f"Bid ({best_bid}) must be less than ask ({best_ask})")
        
        return True
    
    def verify_bbo_filtering(self, order_book, result):
        """
        Verify order book contains only BBO (1 bid, 1 ask).
        
        Args:
            order_book: Order book dict
            result: Testplan result object
        """
        bids = order_book.get("bids", [])
        asks = order_book.get("asks", [])
        
        result.equal(len(bids), 1, "BBO should have exactly 1 bid level")
        result.equal(len(asks), 1, "BBO should have exactly 1 ask level")
    
    def get_aggregator_health(self):
        """
        Check aggregator health endpoint.
        
        Returns:
            Response dict or None if failed
        """
        import requests
        try:
            response = requests.get("http://localhost:8080/health", timeout=5)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return None
    
    def get_aggregator_status(self):
        """
        Get aggregator status endpoint data.
        
        Returns:
            Status dict or None if failed
        """
        import requests
        try:
            response = requests.get("http://localhost:8080/status", timeout=5)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return None
