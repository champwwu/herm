#!/usr/bin/env python3
"""
Helper utilities for integration tests.

Provides standalone functions for common test operations:
- Order book validation
- Aggregation math verification
- Health checks
- Data setup utilities
"""

import requests
from typing import Dict, List, Tuple, Optional


def verify_orderbook_structure(order_book: Dict) -> List[str]:
    """
    Validate order book structure and return list of errors.
    
    Args:
        order_book: Order book dictionary
        
    Returns:
        List of error messages (empty if valid)
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
    
    # Validate spread
    if len(bids) > 0 and len(asks) > 0:
        best_bid = bids[0].get("price", 0)
        best_ask = asks[0].get("price", 0)
        # For multi-exchange aggregation, bid can be > ask (arbitrage opportunity)
        # This is a VALID scenario, so only log it as info
        if best_bid > best_ask:
            errors.append(f"INFO: Cross-exchange arbitrage detected: bid ({best_bid}) > ask ({best_ask}), profit={best_bid - best_ask:.2f}")
    
    return errors


def verify_aggregation_math(
    venue_books: Dict[str, Dict],
    aggregated_book: Dict,
    price_level: float,
    is_bid: bool
) -> Tuple[bool, str]:
    """
    Verify that aggregated quantity equals sum of venue quantities at a price level.
    
    Args:
        venue_books: Dict mapping venue name to order book
        aggregated_book: Aggregated order book
        price_level: Price to check
        is_bid: True for bid side, False for ask side
        
    Returns:
        (is_valid, error_message)
    """
    side = "bids" if is_bid else "asks"
    
    # Calculate expected quantity (sum from all venues)
    expected_qty = 0.0
    for venue_name, book in venue_books.items():
        levels = book.get(side, [])
        for level in levels:
            if abs(level["price"] - price_level) < 0.001:  # Floating point tolerance
                expected_qty += level["quantity"]
                break
    
    # Get actual quantity from aggregated book
    actual_qty = 0.0
    agg_levels = aggregated_book.get(side, [])
    for level in agg_levels:
        if abs(level["price"] - price_level) < 0.001:
            actual_qty = level["quantity"]
            break
    
    # Verify
    if abs(actual_qty - expected_qty) < 0.001:
        return True, ""
    else:
        return False, f"Quantity mismatch at {price_level}: expected {expected_qty}, got {actual_qty}"


def set_deterministic_orderbooks(
    exchanges: Dict[str, any],
    orderbook_config: Dict[str, Tuple[List, List]]
):
    """
    Set specific order book data on multiple mock exchanges.
    
    Args:
        exchanges: Dict mapping exchange name to mock exchange driver
        orderbook_config: Dict mapping exchange name to (bids, asks) tuples
                         where bids/asks are lists of (price, quantity) tuples
    """
    for exchange_name, (bids, asks) in orderbook_config.items():
        if exchange_name in exchanges:
            exchanges[exchange_name].exchange.set_orderbook(bids, asks)


def check_health_endpoints(services: Dict[str, str]) -> Dict[str, bool]:
    """
    Check health endpoints for multiple services.
    
    Args:
        services: Dict mapping service name to health endpoint URL
        
    Returns:
        Dict mapping service name to health status (True/False)
    """
    results = {}
    for service_name, url in services.items():
        try:
            response = requests.get(url, timeout=5)
            results[service_name] = (response.status_code == 200)
        except Exception:
            results[service_name] = False
    
    return results


def get_aggregator_status() -> Optional[Dict]:
    """
    Get aggregator status.
    
    Returns:
        Status dict or None if failed
    """
    try:
        response = requests.get("http://localhost:8080/status", timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None


def verify_price_band_boundaries(
    order_book: Dict,
    bbo_price: float,
    basis_points: int,
    is_bid: bool
) -> Tuple[bool, str]:
    """
    Verify all levels in order book are within specified basis points of BBO.
    
    Args:
        order_book: Order book dict
        bbo_price: Best bid or ask price
        basis_points: Maximum deviation in basis points (1 bp = 0.01%)
        is_bid: True for bid side (check levels are >= lower bound)
                False for ask side (check levels are <= upper bound)
        
    Returns:
        (is_valid, error_message)
    """
    side = "bids" if is_bid else "asks"
    levels = order_book.get(side, [])
    
    # Calculate boundary
    deviation = basis_points / 10000.0  # Convert basis points to fraction
    
    if is_bid:
        # Bid levels should be >= bbo_price * (1 - deviation)
        lower_bound = bbo_price * (1 - deviation)
        for i, level in enumerate(levels):
            if level["price"] < lower_bound:
                return False, f"Bid level {i} at {level['price']} is below {lower_bound} ({basis_points} bps)"
    else:
        # Ask levels should be <= bbo_price * (1 + deviation)
        upper_bound = bbo_price * (1 + deviation)
        for i, level in enumerate(levels):
            if level["price"] > upper_bound:
                return False, f"Ask level {i} at {level['price']} is above {upper_bound} ({basis_points} bps)"
    
    return True, ""


def calculate_vwap(levels: List[Dict], target_notional: float) -> Optional[float]:
    """
    Calculate VWAP for given target notional volume.
    
    Args:
        levels: List of price levels (each with 'price' and 'quantity')
        target_notional: Target notional volume
        
    Returns:
        VWAP price or None if insufficient liquidity
    """
    total_notional = 0.0
    total_quantity = 0.0
    
    for level in levels:
        price = level["price"]
        quantity = level["quantity"]
        level_notional = price * quantity
        
        if total_notional + level_notional >= target_notional:
            # This level completes the target
            remaining_notional = target_notional - total_notional
            remaining_qty = remaining_notional / price
            total_quantity += remaining_qty
            total_notional += remaining_notional
            break
        else:
            # Consume entire level
            total_quantity += quantity
            total_notional += level_notional
    
    if total_notional < target_notional:
        # Insufficient liquidity
        return None
    
    return total_notional / total_quantity if total_quantity > 0 else None


def verify_timestamp_freshness(timestamp_us: int, max_age_sec: float = 10.0) -> Tuple[bool, float]:
    """
    Verify timestamp is recent.
    
    Args:
        timestamp_us: Timestamp in microseconds
        max_age_sec: Maximum age in seconds
        
    Returns:
        (is_fresh, age_in_seconds)
    """
    import time
    current_time_us = int(time.time() * 1_000_000)
    age_us = current_time_us - timestamp_us
    age_sec = age_us / 1_000_000
    
    is_fresh = age_sec <= max_age_sec
    return is_fresh, age_sec
