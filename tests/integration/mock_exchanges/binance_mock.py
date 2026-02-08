#!/usr/bin/env python3
"""
Binance mock exchange WebSocket server.

Implements the Binance combined stream WebSocket protocol with depthUpdate messages.
Message format matches Binance Spot/Futures WebSocket API.

Example message:
{
  "stream": "btcusdt@depth@100ms",
  "data": {
    "e": "depthUpdate",
    "E": 1234567890,
    "s": "BTCUSDT",
    "U": 157,
    "u": 160,
    "b": [["50000.00", "10.5"], ["49999.00", "5.2"]],
    "a": [["50001.00", "8.3"], ["50002.00", "12.1"]]
  }
}
"""

import json
import time
from .base_mock_exchange import BaseMockExchange


class BinanceMockExchange(BaseMockExchange):
    """
    Mock Binance exchange with realistic WebSocket protocol.
    
    Simulates Binance combined stream depth updates.
    """
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8091,
        symbol: str = "BTCUSDT",
        base_price: float = 50000.0,
        update_interval_ms: int = 100,
        use_ssl: bool = True
    ):
        super().__init__(
            name="Binance",
            host=host,
            port=port,
            symbol=symbol,
            base_price=base_price,
            update_interval_ms=update_interval_ms,
            use_ssl=use_ssl
        )
        
        # Binance-specific state
        self.first_update_id = 100
        self.last_update_id = self.first_update_id
    
    def format_message(self) -> str:
        """Format order book data into Binance depthUpdate format."""
        # Get order book levels
        bids = self.order_book.get_bids(num_levels=10)
        asks = self.order_book.get_asks(num_levels=10)
        
        # Format price levels as [price, quantity] strings
        formatted_bids = [[f"{price:.2f}", f"{qty:.4f}"] for price, qty in bids]
        formatted_asks = [[f"{price:.2f}", f"{qty:.4f}"] for price, qty in asks]
        
        # Update IDs for this update
        first_update_id = self.last_update_id + 1
        last_update_id = first_update_id + len(bids) + len(asks)
        self.last_update_id = last_update_id
        
        # Build Binance combined stream message
        stream_name = f"{self.symbol.lower()}@depth@100ms"
        
        message = {
            "stream": stream_name,
            "data": {
                "e": "depthUpdate",
                "E": int(time.time() * 1000),  # Event time
                "s": self.symbol,
                "U": first_update_id,  # First update ID in event
                "u": last_update_id,   # Final update ID in event
                "b": formatted_bids,
                "a": formatted_asks
            }
        }
        
        return json.dumps(message)


# For backward compatibility and testing
if __name__ == "__main__":
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run Binance mock
    exchange = BinanceMockExchange(
        host="0.0.0.0",
        port=8091,
        symbol="BTCUSDT",
        base_price=50000.0,
        use_ssl=True
    )
    
    try:
        exchange.start()
        print(f"Binance mock exchange running on wss://localhost:8091")
        print("Press Ctrl+C to stop...")
        
        # Keep running
        import signal
        signal.pause()
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        exchange.stop()
