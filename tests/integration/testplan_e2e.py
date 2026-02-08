#!/usr/bin/env python3
"""
End-to-end integration test using Testplan framework.

This test uses Testplan's MultiTest with proper drivers and assertions
to test the market data aggregator system with multiple Python mock exchanges.
"""

import sys
import os
from pathlib import Path
import logging
import threading
import time
import re

# Add testplan to path if needed
from testplan import test_plan
from testplan.testing.multitest import MultiTest
from testplan.testing.multitest.driver.base import Driver
from testplan.common.utils.context import context
from testplan.testing.multitest.driver.app import App

# Import test suites from testcases module
from testcases import (
    AggregatorRESTTests,
    PublisherRESTTests,
    DataFlowTests,
    MultiSymbolTests,
    MultiExchangeTests,
    DeterministicTests,
)

# Import mock exchanges
from mock_exchanges import (
    BinanceMockExchange,
    OKXMockExchange,
    BybitMockExchange,
)

# Test configuration
TEST_DIR = Path(__file__).parent
REPO_ROOT = TEST_DIR.parent.parent
BUILD_DIR = REPO_ROOT / "build" / "Debug" / "bin"
CONFIG_DIR = REPO_ROOT / "config"
TEST_CONFIG_DIR = TEST_DIR / "config"
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"

# Port Configuration (Consolidated)
PORTS = {
    # Mock Exchanges (8090-8099)
    'BINANCE_MOCK': 8091,
    'OKX_MOCK': 8092,
    'BYBIT_MOCK': 8093,
    
    # Aggregator
    'AGGREGATOR_GRPC': 50051,
    'AGGREGATOR_REST': 8080,
    
    # Publishers (8082-8089)
    'PUBLISHER_BBO': 8085,
    'PUBLISHER_PRICE_BANDS': 8082,
    'PUBLISHER_VOLUME_BANDS': 8083,
}


class MockExchangeDriver(Driver):
    """
    Testplan driver wrapper for Python mock exchange servers.
    
    Integrates mock exchanges with Testplan's driver lifecycle.
    """
    
    def __init__(self, name, exchange_class, **kwargs):
        super(MockExchangeDriver, self).__init__(name=name)
        self.exchange_class = exchange_class
        self.exchange_kwargs = kwargs
        self.exchange = None
    
    def starting(self):
        """Start the mock exchange server."""
        super(MockExchangeDriver, self).starting()
        self.logger.info(f"Starting {self.name} mock exchange...")
        
        try:
            self.exchange = self.exchange_class(**self.exchange_kwargs)
            self.exchange.start()
            
            # Wait for server to be ready
            time.sleep(2)
            
            self.logger.info(f"{self.name} mock exchange started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start {self.name} mock exchange: {e}")
            raise
    
    def stopping(self):
        """Stop the mock exchange server."""
        super(MockExchangeDriver, self).stopping()
        self.logger.info(f"Stopping {self.name} mock exchange...")
        
        if self.exchange:
            try:
                self.exchange.stop()
                self.logger.info(f"{self.name} mock exchange stopped")
            except Exception as e:
                self.logger.error(f"Error stopping {self.name} mock exchange: {e}")
    
    def aborting(self):
        """Handle driver abort."""
        self.stopping()


@test_plan(name='MarketDataAggregatorE2E')
def main(plan):
    """Main test plan with C++ services and Python mock exchanges"""
    
    # Set up environment
    env = os.environ.copy()
    env["herm_CONFIG_DIR"] = str(TEST_CONFIG_DIR)
    
    # Check if binaries exist
    aggregator_bin = BUILD_DIR / "aggregator"
    publisher_bin = BUILD_DIR / "publisher"
    
    if not aggregator_bin.exists():
        plan.logger.error(f"{aggregator_bin} not found. Build the project first.")
        return
    
    if not publisher_bin.exists():
        plan.logger.error(f"{publisher_bin} not found. Build the project first.")
        return
    
    # ========================================================================
    # Python Mock Exchange Drivers
    # ========================================================================
    
    binance_mock = MockExchangeDriver(
        name='binance_mock',
        exchange_class=BinanceMockExchange,
        host="0.0.0.0",
        port=PORTS['BINANCE_MOCK'],
        symbol="BTCUSDT",
        base_price=50000.0,
        update_interval_ms=100,
        use_ssl=True
    )
    
    okx_mock = MockExchangeDriver(
        name='okx_mock',
        exchange_class=OKXMockExchange,
        host="0.0.0.0",
        port=PORTS['OKX_MOCK'],
        symbol="BTCUSDT",
        base_price=50000.0,
        update_interval_ms=100,
        use_ssl=True
    )
    
    bybit_mock = MockExchangeDriver(
        name='bybit_mock',
        exchange_class=BybitMockExchange,
        host="0.0.0.0",
        port=PORTS['BYBIT_MOCK'],
        symbol="BTCUSDT",
        base_price=50000.0,
        update_interval_ms=100,
        use_ssl=True
    )
    
    # ========================================================================
    # C++ Service Drivers
    # ========================================================================
    
    # Aggregator
    aggregator = App(
        name='aggregator',
        binary=str(aggregator_bin),
        args=[f'--config_file={TEST_CONFIG_DIR}/aggregator_multi_symbol.json'],
        working_dir=str(REPO_ROOT),
        env=env,
        stdout_regexps=[re.compile(r".*Application aggregator started.*")]
    )
    
    # Publishers - update to use TEST_CONFIG_DIR
    publisher_bbo = App(
        name='publisher_bbo',
        binary=str(publisher_bin),
        args=[f'--config_file={TEST_CONFIG_DIR}/publisher_bbo.json'],
        working_dir=str(REPO_ROOT),
        env=env
    )
    
    publisher_price_bands = App(
        name='publisher_price_bands',
        binary=str(publisher_bin),
        args=[f'--config_file={TEST_CONFIG_DIR}/publisher_price_bands.json'],
        working_dir=str(REPO_ROOT),
        env=env
    )
    
    publisher_volume_bands = App(
        name='publisher_volume_bands',
        binary=str(publisher_bin),
        args=[f'--config_file={TEST_CONFIG_DIR}/publisher_volume_bands.json'],
        working_dir=str(REPO_ROOT),
        env=env
    )
    
    # ========================================================================
    # Create MultiTest with all suites and drivers
    # ========================================================================
    
    test = MultiTest(
        name='MarketDataAggregatorTest',
        suites=[
            AggregatorRESTTests(),
            PublisherRESTTests(),
            DataFlowTests(),
            MultiSymbolTests(),
            MultiExchangeTests(),
            DeterministicTests(),
        ],
        environment=[
            # Python mock exchanges (start first)
            binance_mock,
            okx_mock,
            bybit_mock,
            # Aggregator and publishers
            aggregator,
            publisher_bbo,
            publisher_price_bands,
            publisher_volume_bands,
        ],
        # Make mock exchanges accessible to tests via environment
        # Tests can access them as: env.binance_mock, env.okx_mock, env.bybit_mock
    )
    
    plan.add(test)


if __name__ == '__main__':
    sys.exit(not main())
