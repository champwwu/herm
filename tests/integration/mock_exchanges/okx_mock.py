#!/usr/bin/env python3
"""
OKX mock exchange WebSocket server.

Implements the OKX WebSocket protocol with books channel messages.
Message format matches OKX WebSocket API v5.

Example message:
{
  "arg": {
    "channel": "books",
    "instId": "BTC-USDT"
  },
  "action": "update",
  "data": [{
    "asks": [["50001", "8.3", "0", "1"]],
    "bids": [["50000", "10.5", "0", "1"]],
    "ts": "1234567890123",
    "checksum": 123456
  }]
}
"""

import json
import time
from .base_mock_exchange import BaseMockExchange


class OKXMockExchange(BaseMockExchange):
    """
    Mock OKX exchange with realistic WebSocket protocol.
    
    Simulates OKX books channel updates.
    """
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8092,
        symbol: str = "BTCUSDT",
        base_price: float = 50000.0,
        update_interval_ms: int = 100,
        use_ssl: bool = True
    ):
        super().__init__(
            name="OKX",
            host=host,
            port=port,
            symbol=symbol,
            base_price=base_price,
            update_interval_ms=update_interval_ms,
            use_ssl=use_ssl
        )
        
        # Convert symbol to OKX format (e.g., BTCUSDT -> BTC-USDT)
        self.okx_symbol = self._convert_symbol(symbol)
        self.is_snapshot = True  # First message is snapshot, rest are updates
    
    def _convert_symbol(self, symbol: str) -> str:
        """Convert standard symbol to OKX format (e.g., BTCUSDT -> BTC-USDT)."""
        # Simple heuristic: if symbol ends with USDT, split before USDT
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT"
        elif symbol.endswith("USDC"):
            base = symbol[:-4]
            return f"{base}-USDC"
        elif symbol.endswith("USD"):
            base = symbol[:-3]
            return f"{base}-USD"
        else:
            # Default: assume 3-char base currency
            return f"{symbol[:3]}-{symbol[3:]}"
    
    def _calculate_checksum(self, bids, asks) -> int:
        """Calculate a simple checksum for the order book (simplified version)."""
        # OKX uses CRC32 checksum, but for mock we'll use a simple hash
        checksum_str = ""
        for bid in bids[:25]:
            checksum_str += f"{bid[0]}:{bid[1]}"
        for ask in asks[:25]:
            checksum_str += f"{ask[0]}:{ask[1]}"
        return abs(hash(checksum_str)) % 1000000000
    
    def format_message(self) -> str:
        """Format order book data into OKX books channel format."""
        # Get order book levels
        bids = self.order_book.get_bids(num_levels=10)
        asks = self.order_book.get_asks(num_levels=10)
        
        # Format price levels as [price, quantity, deprecated, num_orders] strings
        # OKX format: [price, size, liquidation_orders, num_orders]
        formatted_bids = [
            [f"{price:.2f}", f"{qty:.4f}", "0", "1"]
            for price, qty in bids
        ]
        formatted_asks = [
            [f"{price:.2f}", f"{qty:.4f}", "0", "1"]
            for price, qty in asks
        ]
        
        # Calculate checksum
        checksum = self._calculate_checksum(formatted_bids, formatted_asks)
        
        # Build OKX message
        action = "snapshot" if self.is_snapshot else "update"
        self.is_snapshot = False  # After first message, send updates
        
        message = {
            "arg": {
                "channel": "books",
                "instId": self.okx_symbol
            },
            "action": action,
            "data": [{
                "asks": formatted_asks,
                "bids": formatted_bids,
                "ts": str(int(time.time() * 1000)),
                "checksum": checksum
            }]
        }
        
        return json.dumps(message)


# For backward compatibility and testing
if __name__ == "__main__":
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run OKX mock
    exchange = OKXMockExchange(
        host="0.0.0.0",
        port=8092,
        symbol="BTCUSDT",
        base_price=50000.0,
        use_ssl=True
    )
    
    try:
        exchange.start()
        print(f"OKX mock exchange running on wss://localhost:8092")
        print("Press Ctrl+C to stop...")
        
        # Keep running
        import signal
        signal.pause()
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        exchange.stop()
