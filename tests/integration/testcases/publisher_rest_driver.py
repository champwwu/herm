#!/usr/bin/env python3
"""
Publisher REST API Driver

Helper class to encapsulate REST interactions with publisher services.
Provides methods for querying order books and managing publisher state for testing.
"""

import requests
import time
from typing import Dict, Optional, List


class PublisherRestDriver:
    """
    REST API driver for publisher services.
    
    Provides convenient methods for:
    - Getting latest order books
    - Checking publisher health
    - Waiting for expected data
    """
    
    def __init__(self, name: str, port: int, host: str = "localhost", timeout: int = 5):
        """
        Initialize publisher REST driver.
        
        Args:
            name: Publisher name (for logging)
            port: REST API port
            host: Host address (default: localhost)
            timeout: Default request timeout in seconds
        """
        self.name = name
        self.port = port
        self.host = host
        self.timeout = timeout
        self.base_url = f"http://{host}:{port}"
    
    def get_latest_orderbook(self) -> Optional[Dict]:
        """
        Get latest order book from publisher.
        
        Returns:
            Order book data as dictionary, or None if request fails
        """
        try:
            url = f"{self.base_url}/latestOrderBook"
            response = requests.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception:
            return None
    
    def wait_for_orderbook(self, max_wait_sec: int = 10, min_levels: int = 1) -> Optional[Dict]:
        """
        Wait for publisher to have valid order book data.
        
        Args:
            max_wait_sec: Maximum time to wait in seconds
            min_levels: Minimum number of bid/ask levels required
        
        Returns:
            Order book data when available, or None if timeout
        """
        start_time = time.time()
        while time.time() - start_time < max_wait_sec:
            order_book = self.get_latest_orderbook()
            if order_book:
                bids = order_book.get("bids", [])
                asks = order_book.get("asks", [])
                if len(bids) >= min_levels and len(asks) >= min_levels:
                    return order_book
            time.sleep(0.5)
        return None
    
    def wait_for_price(
        self,
        expected_bid: Optional[float] = None,
        expected_ask: Optional[float] = None,
        tolerance: float = 0.01,
        max_wait_sec: int = 10
    ) -> Optional[Dict]:
        """
        Wait for specific price levels to appear in order book.
        
        Args:
            expected_bid: Expected best bid price (None to skip check)
            expected_ask: Expected best ask price (None to skip check)
            tolerance: Price tolerance (as fraction, e.g., 0.01 = 1%)
            max_wait_sec: Maximum time to wait in seconds
        
        Returns:
            Order book data when expected prices found, or None if timeout
        """
        start_time = time.time()
        while time.time() - start_time < max_wait_sec:
            order_book = self.get_latest_orderbook()
            if order_book:
                bids = order_book.get("bids", [])
                asks = order_book.get("asks", [])
                
                if len(bids) == 0 or len(asks) == 0:
                    time.sleep(0.5)
                    continue
                
                best_bid = bids[0].get("price", 0)
                best_ask = asks[0].get("price", 0)
                
                bid_match = True
                if expected_bid is not None:
                    bid_diff = abs(best_bid - expected_bid) / expected_bid
                    bid_match = bid_diff <= tolerance
                
                ask_match = True
                if expected_ask is not None:
                    ask_diff = abs(best_ask - expected_ask) / expected_ask
                    ask_match = ask_diff <= tolerance
                
                if bid_match and ask_match:
                    return order_book
            
            time.sleep(0.5)
        return None
    
    def wait_for_timestamp_update(
        self,
        initial_timestamp: int,
        max_wait_sec: int = 10
    ) -> Optional[Dict]:
        """
        Wait for order book timestamp to be newer than given timestamp.
        
        Args:
            initial_timestamp: Previous timestamp in microseconds
            max_wait_sec: Maximum time to wait in seconds
        
        Returns:
            Order book with newer timestamp, or None if timeout
        """
        start_time = time.time()
        while time.time() - start_time < max_wait_sec:
            order_book = self.get_latest_orderbook()
            if order_book:
                current_timestamp = order_book.get("timestamp_us", 0)
                if current_timestamp > initial_timestamp:
                    return order_book
            time.sleep(0.5)
        return None
    
    def reset_cache(self) -> bool:
        """
        Reset/clear the publisher's order book cache (for testing).
        
        This is useful to ensure tests start with a clean state and don't
        see stale data from previous tests.
        
        Returns:
            True if reset was successful, False otherwise
        """
        try:
            url = f"{self.base_url}/reset"
            response = requests.post(url, timeout=self.timeout)
            if response.status_code == 200:
                return True
            return False
        except Exception:
            return False
    
    def validate_orderbook_structure(self, order_book: Dict) -> List[str]:
        """
        Validate order book has proper structure.
        
        Args:
            order_book: Order book data to validate
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Check required fields
        if "symbol" not in order_book:
            errors.append("Missing 'symbol' field")
        if "timestamp_us" not in order_book:
            errors.append("Missing 'timestamp_us' field")
        if "bids" not in order_book:
            errors.append("Missing 'bids' field")
        if "asks" not in order_book:
            errors.append("Missing 'asks' field")
        
        # Validate bids
        bids = order_book.get("bids", [])
        if not isinstance(bids, list):
            errors.append("'bids' is not a list")
        else:
            for i, bid in enumerate(bids):
                if not isinstance(bid, dict):
                    errors.append(f"Bid {i} is not a dict")
                    continue
                if "price" not in bid:
                    errors.append(f"Bid {i} missing 'price'")
                if "quantity" not in bid:
                    errors.append(f"Bid {i} missing 'quantity'")
                if bid.get("price", 0) <= 0:
                    errors.append(f"Bid {i} has non-positive price")
                if bid.get("quantity", 0) <= 0:
                    errors.append(f"Bid {i} has non-positive quantity")
        
        # Validate asks
        asks = order_book.get("asks", [])
        if not isinstance(asks, list):
            errors.append("'asks' is not a list")
        else:
            for i, ask in enumerate(asks):
                if not isinstance(ask, dict):
                    errors.append(f"Ask {i} is not a dict")
                    continue
                if "price" not in ask:
                    errors.append(f"Ask {i} missing 'price'")
                if "quantity" not in ask:
                    errors.append(f"Ask {i} missing 'quantity'")
                if ask.get("price", 0) <= 0:
                    errors.append(f"Ask {i} has non-positive price")
                if ask.get("quantity", 0) <= 0:
                    errors.append(f"Ask {i} has non-positive quantity")
        
        # Validate bid-ask spread (allow arbitrage for multi-exchange)
        if len(bids) > 0 and len(asks) > 0:
            best_bid = bids[0].get("price", 0)
            best_ask = asks[0].get("price", 0)
            # For multi-exchange aggregation, bid can be > ask (arbitrage opportunity)
            # This is a VALID scenario
            if best_bid > best_ask:
                errors.append(f"INFO: Cross-exchange arbitrage detected: bid ({best_bid}) > ask ({best_ask}), profit={best_bid - best_ask:.2f}")
        
        return errors
    
    def __repr__(self):
        return f"PublisherRestDriver(name='{self.name}', port={self.port})"
