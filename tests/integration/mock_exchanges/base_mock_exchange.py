#!/usr/bin/env python3
"""
Base class for mock cryptocurrency exchange WebSocket servers.

Provides common functionality for SSL WebSocket servers, order book generation,
and integration with Testplan framework.

Test Control Methods:
- set_orderbook() - Set specific order book data
- reset_orderbook() - Reset to default order book
- get_orderbook() - Get current order book data
"""

import asyncio
import json
import logging
import ssl
import threading
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import websockets
    from websockets.server import WebSocketServerProtocol
except ImportError:
    raise ImportError("websockets library not found. Install with: pip install websockets")


class OrderBook:
    """Order book model with support for setting specific data."""
    
    def __init__(self, symbol: str, base_price: float = 50000.0):
        self.symbol = symbol
        self.base_price = base_price
        self.update_id = 0
        self.timestamp_ms = int(time.time() * 1000)
        
        # Specific order book data (set via REST API)
        self._bids: List[Tuple[float, float]] = []
        self._asks: List[Tuple[float, float]] = []
        self._use_specific_data = False
        
        # Generate default data
        self._generate_default_orderbook()
    
    def _generate_default_orderbook(self):
        """Generate default order book around base price."""
        # Default bids (descending)
        self._bids = [
            (self.base_price * 0.9999, 10.0),
            (self.base_price * 0.9998, 15.0),
            (self.base_price * 0.9997, 20.0),
            (self.base_price * 0.9996, 25.0),
            (self.base_price * 0.9995, 30.0),
        ]
        
        # Default asks (ascending)
        self._asks = [
            (self.base_price * 1.0001, 8.0),
            (self.base_price * 1.0002, 12.0),
            (self.base_price * 1.0003, 16.0),
            (self.base_price * 1.0004, 20.0),
            (self.base_price * 1.0005, 24.0),
        ]
    
    def set_orderbook(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]):
        """Set specific order book data."""
        self._bids = bids
        self._asks = asks
        self._use_specific_data = True
        self.update_id += 1
        self.timestamp_ms = int(time.time() * 1000)
    
    def reset(self):
        """Reset to default order book."""
        self._generate_default_orderbook()
        self._use_specific_data = False
        self.update_id = 0
        self.timestamp_ms = int(time.time() * 1000)
    
    def update_timestamp(self):
        """Update timestamp (for periodic updates)."""
        self.update_id += 1
        self.timestamp_ms = int(time.time() * 1000)
    
    def get_bids(self, num_levels: int = 10) -> List[Tuple[float, float]]:
        """Get bid levels."""
        return self._bids[:num_levels] if self._bids else []
    
    def get_asks(self, num_levels: int = 10) -> List[Tuple[float, float]]:
        """Get ask levels."""
        return self._asks[:num_levels] if self._asks else []
    
    def get_data(self) -> Dict:
        """Get order book as dictionary."""
        return {
            "symbol": self.symbol,
            "bids": [[price, qty] for price, qty in self._bids],
            "asks": [[price, qty] for price, qty in self._asks],
            "timestamp_ms": self.timestamp_ms,
            "update_id": self.update_id
        }


class BaseMockExchange(ABC):
    """
    Base class for mock exchange WebSocket servers.
    
    Provides:
    - SSL WebSocket server for market data
    - REST API for test control (set order book, reset)
    - Order book management
    - Periodic market data updates
    - Testplan driver compatibility
    """
    
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        symbol: str,
        base_price: float,
        update_interval_ms: int = 100,
        use_ssl: bool = True
    ):
        self.name = name
        self.host = host
        self.port = port
        self.symbol = symbol
        self.base_price = base_price
        self.update_interval_ms = update_interval_ms
        self.use_ssl = use_ssl
        
        # Order book
        self.order_book = OrderBook(symbol, base_price)
        
        # Server state
        self.server = None
        self.loop = None
        self.thread = None
        self.running = False
        self.clients = set()
        
        # Logging
        self.logger = logging.getLogger(f"MockExchange.{name}")
        self.logger.setLevel(logging.INFO)
        
        # SSL context
        self.ssl_context = None
        if use_ssl:
            self._setup_ssl_context()
    
    def _setup_ssl_context(self):
        """Set up SSL context with self-signed certificates."""
        cert_dir = Path(__file__).parent.parent / "certs"
        cert_file = cert_dir / "server.crt"
        key_file = cert_dir / "server.key"
        
        if not cert_file.exists() or not key_file.exists():
            raise FileNotFoundError(
                f"SSL certificates not found in {cert_dir}. "
                f"Run generate_certs.py first."
            )
        
        self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.ssl_context.load_cert_chain(str(cert_file), str(key_file))
        self.logger.info(f"Loaded SSL certificates from {cert_dir}")
    
    @abstractmethod
    def format_message(self) -> str:
        """
        Format order book data into exchange-specific message format.
        Must be implemented by subclasses.
        """
        pass
    
    async def handle_client(self, websocket: WebSocketServerProtocol):
        """Handle a WebSocket client connection."""
        client_addr = websocket.remote_address
        self.logger.info(f"Client connected from {client_addr}")
        self.clients.add(websocket)
        
        try:
            # Send initial snapshot
            message = self.format_message()
            await websocket.send(message)
            self.logger.debug(f"Sent initial snapshot to {client_addr}")
            
            # Keep connection alive and handle incoming messages
            async for msg in websocket:
                # Handle subscription or other messages if needed
                self.logger.debug(f"Received message from {client_addr}: {msg[:100]}")
                
        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"Client disconnected: {client_addr}")
        except Exception as e:
            self.logger.error(f"Error handling client {client_addr}: {e}")
        finally:
            self.clients.discard(websocket)
    
    async def broadcast_updates(self):
        """Periodically broadcast order book updates to all connected clients."""
        while self.running:
            try:
                await asyncio.sleep(self.update_interval_ms / 1000.0)
                
                if not self.clients:
                    continue
                
                # Update timestamp
                self.order_book.update_timestamp()
                
                # Format and broadcast message
                message = self.format_message()
                
                # Send to all connected clients
                disconnected = set()
                for client in self.clients:
                    try:
                        await client.send(message)
                    except websockets.exceptions.ConnectionClosed:
                        disconnected.add(client)
                    except Exception as e:
                        self.logger.error(f"Error sending to client: {e}")
                        disconnected.add(client)
                
                # Remove disconnected clients
                self.clients -= disconnected
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in broadcast loop: {e}")
    
    async def _run_server(self):
        """Run the WebSocket server."""
        protocol = "wss" if self.use_ssl else "ws"
        self.logger.info(f"Starting {self.name} mock exchange on {protocol}://{self.host}:{self.port}")
        
        self.running = True
        
        # Start WebSocket server
        self.server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ssl=self.ssl_context
        )
        
        self.logger.info(f"{self.name} mock exchange server started on {protocol}://{self.host}:{self.port}")
        
        # Start broadcast task
        broadcast_task = asyncio.create_task(self.broadcast_updates())
        
        try:
            # Keep server running
            await asyncio.Future()  # Run forever
        except asyncio.CancelledError:
            self.logger.info(f"Server cancelled")
        finally:
            # Clean up broadcast task
            if not broadcast_task.done():
                broadcast_task.cancel()
                try:
                    await broadcast_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    self.logger.error(f"Error stopping broadcast task: {e}")
    
    def _run_event_loop(self):
        """Run the asyncio event loop in a separate thread."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            self.loop.run_until_complete(self._run_server())
        except Exception as e:
            self.logger.error(f"Event loop error: {e}")
        finally:
            # Cancel all remaining tasks
            try:
                pending = asyncio.all_tasks(self.loop)
                for task in pending:
                    task.cancel()
                # Wait for all tasks to complete cancellation
                if pending:
                    self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            finally:
                self.loop.close()
    
    # ========================================================================
    # Test Control Methods
    # ========================================================================
    
    def set_orderbook(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]):
        """
        Set specific order book data (for test control).
        
        Args:
            bids: List of (price, quantity) tuples for bid side
            asks: List of (price, quantity) tuples for ask side
        """
        self.order_book.set_orderbook(bids, asks)
        self.logger.info(f"{self.name}: Order book set - {len(bids)} bids, {len(asks)} asks")
    
    def reset_orderbook(self):
        """Reset order book to default state (for test control)."""
        self.order_book.reset()
        self.logger.info(f"{self.name}: Order book reset to default")
    
    def get_orderbook(self) -> Dict:
        """Get current order book data (for test control)."""
        return self.order_book.get_data()
    
    # ========================================================================
    # Server Lifecycle
    # ========================================================================
    
    def start(self):
        """Start the mock exchange server (Testplan driver compatible)."""
        if self.running:
            self.logger.warning(f"{self.name} is already running")
            return
        
        self.logger.info(f"Starting {self.name} mock exchange...")
        
        # Start WebSocket server
        self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.thread.start()
        
        # Wait for server to start
        time.sleep(1)
        self.logger.info(f"{self.name} mock exchange started on port {self.port}")
    
    def stop(self):
        """Stop the mock exchange server (Testplan driver compatible)."""
        if not self.running:
            self.logger.warning(f"{self.name} is not running")
            return
        
        self.logger.info(f"Stopping {self.name} mock exchange...")
        self.running = False
        
        # Stop WebSocket server
        if self.loop and not self.loop.is_closed():
            # Schedule loop stop from the loop's thread
            try:
                self.loop.call_soon_threadsafe(self.loop.stop)
            except Exception as e:
                self.logger.error(f"Error stopping event loop: {e}")
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        self.logger.info(f"{self.name} mock exchange stopped")
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
