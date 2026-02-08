"""
Test cases for market data aggregator integration testing.

This package contains organized test suites for different aspects of the system:
- Aggregator REST API tests
- Publisher REST API tests
- Data flow validation tests
- Multi-symbol handling tests
- Multi-exchange aggregation tests
- Deterministic tests with controlled data
"""

from .base_test_suite import BaseTestSuite
from . import test_helpers
from .test_aggregator import AggregatorRESTTests
from .test_publisher import PublisherRESTTests
from .test_dataflow import DataFlowTests
from .test_multi_symbol import MultiSymbolTests
from .test_multi_exchange import MultiExchangeTests
from .test_deterministic import DeterministicTests

__all__ = [
    'BaseTestSuite',
    'test_helpers',
    'AggregatorRESTTests',
    'PublisherRESTTests',
    'DataFlowTests',
    'MultiSymbolTests',
    'MultiExchangeTests',
    'DeterministicTests',
]
