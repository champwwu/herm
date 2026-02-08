# Market Data Aggregator

A high-performance C++ system for aggregating cryptocurrency market data from multiple exchanges with real-time streaming and flexible filtering capabilities.

## Overview

This system aggregates order books from multiple cryptocurrency exchanges (Binance, Bybit, OKX), consolidates them per symbol, and provides filtered views through gRPC streaming and REST APIs. It supports various filtering strategies including Best Bid/Offer (BBO), price band liquidity analysis, and volume-weighted average price (VWAP) calculations.

### Key Features

- **Multi-Exchange Support**: Connects to Binance (Spot/Futures), Bybit (Spot/Perpetual), and OKX (Spot)
- **Real-Time Aggregation**: Consolidates order books across exchanges with venue contribution tracking
- **Flexible Filtering**: BBO, price band analysis, and volume band VWAP calculations
- **High Performance**: Lock-free data structures, CPU pinning, object pooling
- **gRPC Streaming**: Low-latency streaming API with per-subscriber filtering
- **REST API**: Query endpoints for order books and system health
- **Comprehensive Testing**: Unit tests with Google Test, integration tests with Python Testplan

### Supported Exchanges

| Exchange | Markets | WebSocket Protocol |
|----------|---------|-------------------|
| Binance | Spot, Futures | Native depth stream |
| Bybit | Spot, Perpetual | V5 public API |
| OKX | Spot, Perpetual | V5 public API |

## Project Structure

```
cinque/
├── apps/                          # Application executables
│   ├── aggregator/                # Main aggregation service
│   ├── publisher/                 # Publisher client service
│   └── mock_exchange_server/      # Mock exchange for testing
├── engine/                        # Core engine components
│   ├── common/                    # Shared infrastructure
│   │   ├── application_kernel.*   # Base application framework
│   │   ├── rcu_ptr.hpp           # Lock-free RCU pattern
│   │   ├── event_thread.*        # Event-driven task processing
│   │   ├── config_manager.*      # JSON configuration
│   │   ├── rest_server.*         # HTTP REST server
│   │   └── instrument_registry.* # Symbol metadata
│   ├── exchange/                  # Exchange-specific handlers
│   │   ├── market_data_handler.* # Base handler interface
│   │   ├── binance_*_handler.*   # Binance implementations
│   │   ├── bybit_handler.*       # Bybit implementation
│   │   ├── okx_handler.*         # OKX implementation
│   │   └── rate_limiter.*        # Rate limiting for REST calls
│   ├── market_data/               # Order book data structures
│   │   ├── order_book.*          # Core order book
│   │   └── order_book_aggregator.* # Multi-exchange aggregation
│   ├── aggregation/               # Aggregation service logic
│   │   ├── aggregator_service.*  # gRPC service implementation
│   │   ├── market_data_manager.* # Exchange connection manager
│   │   ├── order_book_processor.* # Processing pipeline
│   │   └── order_book_resampler.* # Filtering algorithms
│   └── publishing/                # Publisher client service
│       ├── publisher_service.*   # Client service implementation
│       └── order_book_json_converter.* # Protobuf to JSON
├── proto/                         # Protobuf definitions
│   └── market_data.proto         # Market data messages
├── config/                        # Configuration files
│   ├── aggregator.json           # Aggregator configuration
│   └── publisher_*.json          # Publisher configurations
├── tests/                         # Test suites
│   ├── unit/                     # C++ unit tests (Google Test)
│   └── integration/              # Python integration tests (Testplan)
├── docker/                        # Dockerfiles for services
├── scripts/                       # Build and test scripts
│   ├── build.sh                  # Local build script
│   ├── test.sh                   # Run unit tests
│   ├── docker-build.sh           # Build Docker images
│   └── run-integration-tests.sh  # Run integration tests
└── static/                        # Static data
    └── instruments.csv           # Instrument registry
```

## Build Instructions

### Prerequisites

- **C++20** compatible compiler (GCC 10+, Clang 12+)
- **CMake** 3.20 or higher
- **Python** 3.8+ with pip
- **Docker** and Docker Compose (for containerized deployment)
- **Conan** 2.0+ (C++ package manager)

### Local Build

1. **Setup Python environment and install Conan**:
   ```bash
   ./scripts/setup_venv.sh
   source .venv/bin/activate
   ```

2. **Build the project** (Debug or Release):
   ```bash
   # Debug build (default)
   ./scripts/build.sh debug
   
   # Release build
   ./scripts/build.sh release
   ```

   The build script will:
   - Install C++ dependencies via Conan (gRPC, Boost, spdlog, etc.)
   - Generate protobuf/gRPC code
   - Compile all binaries to `build/Debug/bin/` or `build/Release/bin/`

3. **Verify build**:
   ```bash
   ls build/Debug/bin/
   # Expected: aggregator, publisher, mock_exchange_server, and test executables
   ```

### Build Configuration

- **Debug**: Includes debug symbols, logging at TRACE level, optimizations disabled
- **Release**: Optimizations enabled, minimal logging overhead

Dependencies are managed via [`conanfile.py`](conanfile.py):
- gRPC 1.65.0 (with Protobuf)
- Boost 1.84.0 (system, Beast)
- nlohmann_json 3.11.3
- spdlog 1.17.0
- Google Test 1.14.0

## Testing

### Unit Tests

Run all unit tests with Google Test:

```bash
# Run tests (ensure you've built first)
./scripts/test.sh debug

# Or for release build
./scripts/test.sh release
```

**Test Coverage** (12 test suites):
- `order_book_test`: Order book data structure operations
- `order_book_aggregator_test`: Multi-exchange aggregation logic
- `order_book_resampler_test`: BBO, price band, and volume band filtering
- `order_book_processor_test`: Processing pipeline
- `config_manager_test`: JSON configuration parsing
- `instrument_registry_test`: Symbol metadata management
- `event_thread_test`: Event-driven task scheduling
- `rcu_ptr_test`: Lock-free RCU pointer operations
- `rate_limiter_test`: Rate limiting logic
- `order_book_json_converter_test`: Protobuf to JSON conversion
- `market_data_handler_factory_test`: Exchange handler creation
- `mock_exchange_test`: Mock exchange functionality

### Integration Tests

Run end-to-end integration tests with Python Testplan:

```bash
# Full integration test suite (first run or after C++ changes)
./scripts/run-integration-tests.sh
```

This will:
1. Build debug Docker images with all dependencies
2. Compile C++ binaries in debug mode
3. Start mock exchange servers (Python WebSocket servers)
4. Launch aggregator and publisher services
5. Run comprehensive test suites
6. Generate HTML and PDF test reports in `test-results/`

**Integration Test Suites**:
- `AggregatorRESTTests`: Aggregator health and REST endpoints
- `PublisherRESTTests`: Publisher health and metrics
- `DataFlowTests`: End-to-end data flow validation
- `MultiSymbolTests`: Multiple symbol handling
- `MultiExchangeTests`: Multi-exchange aggregation
- `DeterministicTests`: Calculation correctness with deterministic data

Test results are saved to:
- `test-results/testplan.html` - Interactive HTML report
- `test-results/testplan.pdf` - PDF report
- `logs/` - Service logs for debugging

See [`tests/integration/README.md`](tests/integration/README.md) for more details.

## Running the System

### Docker Compose (Recommended)

1. **Build Docker images**:
   ```bash
   ./scripts/docker-build.sh
   ```
   
   This builds images in the correct order:
   - `herm-base`: Base image with all dependencies
   - `herm-aggregator`: Aggregator service
   - `herm-publisher`: Publisher service

2. **Start all services**:
   ```bash
   # Start all service
   docker compose up

   # Or Start detached (background)
   docker compose up -d
   ```

   Services started:
   - **aggregator**: gRPC on port 50051, REST on port 8080
   - **publisher_bbo**: BBO publisher, REST on port 8085
   - **publisher_price_bands**: Price bands publisher, REST on port 8082
   - **publisher_volume_bands**: Volume bands publisher, REST on port 8083

3. **View logs**:
   ```bash
   # All services
   docker compose logs -f
   
   # Specific service
   docker logs -f aggregator
   docker logs -f publisher_bbo
   docker logs -f publisher_price_bands
   docker logs -f publisher_volume_bands
   ```

4. **Stop services**:
   ```bash
   docker compose down
   ```

### Local Execution

Run services locally after building:

```bash
# Terminal 1: Start aggregator
./build/Debug/bin/aggregator --config_file=config/aggregator.json

# Terminal 2: Start publisher (BBO example)
./build/Debug/bin/publisher --config_file=config/publisher_bbo.json
```

### Service Endpoints

**Aggregator**:
- gRPC: `localhost:50051` - `StreamMarketData(request)` for streaming
- REST: `http://localhost:8080/health` - Health check
- REST: `http://localhost:8080/orderbook?instrument_id=BTCUSDT.SPOT.BNC` - Query order book

**Publishers**:
- BBO: `http://localhost:8085/latestOrderBook` - Latest BBO data
- Price Bands: `http://localhost:8082/latestOrderBook` - Price band analysis
- Volume Bands: `http://localhost:8083/latestOrderBook` - Volume band VWAP

## Configuration

### Aggregator Configuration

Example [`config/aggregator.json`](config/aggregator.json):

```json
{
  "app": {
    "log": {
      "file": "logs/aggregator.log",
      "level": "info"
    },
    "rest_port": 8080,
    "instrument_registry": "static/instruments.csv",
    "core_pinning": {
      "enable": true,
      "cores": [1, 2, 3, 4, 5, 6, 7, 8]
    }
  },
  "strategy": {
    "rpc_port": 50051,
    "symbol": {
      "BTCUSDT": ["BTCUSDT.SPOT.BNC", "BTCUSDT.PERP.BNC"],
      "ETHUSDT": ["ETHUSDT.SPOT.BNC", "ETHUSDT.PERP.BNC"]
    }
  },
  "market_data": {
    "binance": {
      "spot": {
        "websocket_url_template": "wss://stream.binance.com:9443/ws/{symbol}@depth@100ms",
        "rate_limit": { "requests_per_second": 15 }
      }
    }
  }
}
```

### Publisher Configuration

Example [`config/publisher_bbo.json`](config/publisher_bbo.json):

```json
{
  "app": {
    "log": { "file": "logs/publisher_bbo.log", "level": "info" },
    "rest_port": 8085
  },
  "strategy": {
    "aggregator_server": "localhost:50051",
    "subscribe": [
      { "request": "BBO", "symbol_root": "BTCUSDT" }
    ]
  }
}
```

### Instrument Registry

Instrument metadata is loaded from [`static/instruments.csv`](static/instruments.csv):

```csv
instrument_id,base_ccy,quote_ccy,instrument_type,exchange,lotsize,ticksize,min_order_size,max_order_size
BTCUSDT.SPOT.BNC,BTC,USDT,SPOT,BNC,0.00001,0.01,0.00001,9000
BTCUSDT.PERP.BNC,BTC,USDT,PERP,BNC,0.001,0.1,0.001,1000
```

## Design Decisions

This section highlights key architectural and implementation decisions made to achieve high performance and maintainability.

### 1. Lock-Free RCU Pattern for Concurrent Reads

**Decision**: Use Read-Copy-Update (RCU) pattern with C++20 `atomic<shared_ptr<T>>` for subscriber management and order book snapshots.

**Rationale**:
- Order book aggregation is read-heavy: many concurrent readers (gRPC streams), infrequent writers (market data updates)
- Lock-free reads eliminate contention and enable wait-free access for readers
- Atomic shared_ptr ensures memory safety through reference counting
- Automatic memory reclamation when no readers hold references

**Implementation**: [`engine/common/rcu_ptr.hpp`](engine/common/rcu_ptr.hpp)

**Trade-offs**:
- Memory overhead: Temporary copies during updates
- Write amplification: Must copy entire structure on update
- Best suited for read-heavy workloads (which matches our use case)

### 2. Per-Subscriber Writer Threads with Queues

**Decision**: Each gRPC subscriber gets dedicated writer thread with message queue.

**Rationale**:
- Decouples order book processing from gRPC streaming
- Isolates slow/disconnected clients from affecting others
- Enables backpressure handling per subscriber
- Simplifies concurrency model (no shared state between streams)

**Implementation**: [`engine/aggregation/aggregator_service.hpp`](engine/aggregation/aggregator_service.hpp)

**Benefits**:
- Prevents one slow subscriber from blocking others
- Natural load isolation and failure containment
- Simplified debugging and monitoring per connection

### 3. Object Pooling for Zero-Allocation Message Passing

**Decision**: Use thread-safe object pools for OrderBook protobuf messages.

**Rationale**:
- Protobuf messages are expensive to allocate/deallocate
- High-frequency updates (100ms or faster from exchanges)
- Memory allocation causes cache misses and heap contention
- Pool pre-allocates messages and reuses them

**Implementation**: [`engine/common/thread_safe_object_pool.hpp`](engine/common/thread_safe_object_pool.hpp)

**Benefits**:
- Reduced allocation overhead in hot path
- More predictable latency (no GC pauses)
- Better cache locality with reused objects

### 4. Event-Driven Architecture with EventThread

**Decision**: Centralized event loop (EventThread) for async tasks and periodic operations.

**Rationale**:
- Simplifies async programming model (no callback hell)
- Single-threaded event processing eliminates data races
- Natural fit for I/O-bound operations (WebSocket reconnections, heartbeats)
- Supports delayed and periodic task scheduling

**Implementation**: [`engine/common/event_thread.hpp`](engine/common/event_thread.hpp)

**Use Cases**:
- WebSocket reconnection logic
- Periodic health checks
- Deferred cleanup operations
- Rate limiter token replenishment

### 5. CPU Pinning for Latency Optimization

**Decision**: Support CPU core affinity pinning for latency-sensitive threads.

**Rationale**:
- Reduces context switching overhead
- Improves cache locality by keeping thread on same core
- Predictable performance for latency-sensitive operations
- Configurable per deployment (enabled in production, disabled in dev)

**Implementation**: [`engine/common/cpu_pinning.hpp`](engine/common/cpu_pinning.hpp)

**Configuration**:
```json
{
  "core_pinning": {
    "enable": true,
    "cores": [1, 2, 3, 4, 5, 6, 7, 8]
  }
}
```

### 6. ApplicationKernel Framework

**Decision**: Create reusable application framework (ApplicationKernel) for common infrastructure.

**Rationale**:
- Standardizes lifecycle management across services
- Reduces code duplication between aggregator and publisher
- Provides consistent configuration, logging, and REST endpoints
- Simplifies signal handling and graceful shutdown

**Implementation**: [`engine/common/application_kernel.hpp`](engine/common/application_kernel.hpp)

**Benefits**:
- New services inherit all infrastructure for free
- Consistent operational behavior across all services
- Easier to maintain and extend

### 7. Factory Pattern for Exchange Handlers

**Decision**: Use factory pattern for creating exchange-specific market data handlers.

**Rationale**:
- Encapsulates exchange-specific protocol details
- Simplifies adding new exchanges (implement interface, register in factory)
- Enables runtime selection based on configuration
- Promotes loose coupling between aggregation logic and exchange protocols

**Implementation**: [`engine/exchange/market_data_handler_factory.hpp`](engine/exchange/market_data_handler_factory.hpp)

**Extensibility**: Adding a new exchange requires:
1. Implement `MarketDataHandler` interface
2. Register in `MarketDataHandlerFactory`
3. Add configuration in JSON

## Documentation

Detailed technical documentation is available in the [`docs/`](docs/) directory:

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)**: System architecture, components, data flow, and filtering algorithms
- **[APPLICATION_KERNEL.md](docs/APPLICATION_KERNEL.md)**: Core framework functionality and usage
- **[THREADING.md](docs/THREADING.md)**: Multi-threading design and lock-free patterns
- **[TESTING.md](docs/TESTING.md)**: Unit test and integration test coverage
- **[DOCKER.md](docs/DOCKER.md)**: Dockerization and deployment guide

## Future Work

### Database Persistence

**Motivation**: Enable historical analysis, backtesting, and long-term storage of market data.

**Considerations**:
- Time-series database (InfluxDB, TimescaleDB) for efficient storage
- Configurable retention policies (e.g., keep tick data for 30 days, 1-minute bars for 1 year)
- Separate writer service to avoid impacting real-time aggregation performance
- Query API for historical data retrieval

### Additional Exchange Integrations

Potential exchanges to add:
- Coinbase, Kraken, Bitfinex, HTX (Huobi)
- DEX aggregators (Uniswap, Curve via The Graph)

### Horizontal Scaling

Current architecture supports vertical scaling (more CPU/memory on single instance). For horizontal scaling:

**Symbol Partitioning**:
- Multiple aggregator instances, each handling subset of symbols
- Load balancer routes publisher subscriptions based on symbol
- Stateless design simplifies horizontal scaling

**Shared State Considerations**:
- No shared state between aggregators (each has own exchange connections)
- Publisher clients can connect to any aggregator instance
- Database for historical data acts as shared storage layer

**Implementation Approach**:
1. Add symbol routing logic to load balancer
2. Configure each aggregator with symbol partition in config
3. Update publishers to discover aggregator instances (service discovery)
4. Monitor and rebalance partitions based on load

