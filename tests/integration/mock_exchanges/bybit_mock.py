#!/usr/bin/env python3
"""
Bybit mock exchange WebSocket server.

Implements the Bybit WebSocket protocol with orderbook messages.
Message format matches Bybit WebSocket API v5.

Example message:
{
  "topic": "orderbook.200.BTCUSDT",
  "type": "snapshot",
  "ts": 1234567890123,
  "data": {
    "s": "BTCUSDT",
    "b": [["50000.00", "10.5"], ["49999.00", "5.2"]],
    "a": [["50001.00", "8.3"], ["50002.00", "12.1"]],
    "u": 160,
    "seq": 123456
  }
}
"""

import json
import time
from .base_mock_exchange import BaseMockExchange


class BybitMockExchange(BaseMockExchange):
    """
    Mock Bybit exchange with realistic WebSocket protocol.
    
    Simulates Bybit orderbook updates.
    """
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8093,
        symbol: str = "BTCUSDT",
        base_price: float = 50000.0,
        update_interval_ms: int = 100,
        use_ssl: bool = True,
        depth: int = 200  # Bybit supports depth 1, 50, 200, 500
    ):
        super().__init__(
            name="Bybit",
            host=host,
            port=port,
            symbol=symbol,
            base_price=base_price,
            update_interval_ms=update_interval_ms,
            use_ssl=use_ssl
        )
        
        # Bybit-specific state
        self.depth = depth
        self.update_id = 100
        self.seq = 1000
        self.is_snapshot = True  # First message is snapshot, rest are delta
    
    def format_message(self) -> str:
        """Format order book data into Bybit orderbook format."""
        # Get order book levels
        bids = self.order_book.get_bids(num_levels=10)
        asks = self.order_book.get_asks(num_levels=10)
        
        # Format price levels as [price, quantity] strings
        formatted_bids = [[f"{price:.2f}", f"{qty:.4f}"] for price, qty in bids]
        formatted_asks = [[f"{price:.2f}", f"{qty:.4f}"] for price, qty in asks]
        
        # Increment update ID and sequence number
        self.update_id += 1
        self.seq += 1
        
        # Build Bybit message
        msg_type = "snapshot" if self.is_snapshot else "delta"
        self.is_snapshot = False  # After first message, send deltas
        
        message = {
            "topic": f"orderbook.{self.depth}.{self.symbol}",
            "type": msg_type,
            "ts": int(time.time() * 1000),
            "data": {
                "s": self.symbol,
                "b": formatted_bids,
                "a": formatted_asks,
                "u": self.update_id,
                "seq": self.seq
            }
        }
        
        # For snapshot, include update time
        if msg_type == "snapshot":
            message["cts"] = int(time.time() * 1000)
        
        return json.dumps(message)


# For backward compatibility and testing
if __name__ == "__main__":
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run Bybit mock
    exchange = BybitMockExchange(
        host="0.0.0.0",
        port=8093,
        symbol="BTCUSDT",
        base_price=50000.0,
        use_ssl=True
    )
    
    try:
        exchange.start()
        print(f"Bybit mock exchange running on wss://localhost:8093")
        print("Press Ctrl+C to stop...")
        
        # Keep running
        import signal
        signal.pause()
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        exchange.stop()
