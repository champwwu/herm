#!/usr/bin/env python3
"""
Aggregator REST API test suite.

Tests the health, status, and metrics endpoints of the aggregator service.
"""

from testplan.testing.multitest import testcase, testsuite
from .base_test_suite import BaseTestSuite


@testsuite
class AggregatorRESTTests(BaseTestSuite):
    """Test suite for aggregator REST endpoints"""
    
    @testcase(tags={'health', 'aggregator', 'smoke'})
    def test_health_endpoint(self, env, result):
        """Test /health endpoint"""
        import requests
        
        # Get port from PORTS constant
        AGGREGATOR_REST_PORT = 8080
        
        url = f"http://localhost:{AGGREGATOR_REST_PORT}/health"
        response = requests.get(url, timeout=5)
        result.equal(response.status_code, 200, "Health endpoint should return 200")
        data = response.json()
        result.equal(data.get("status"), "ok", "Health status should be ok")
    
    @testcase(tags={'status', 'aggregator'})
    def test_status_endpoint(self, env, result):
        """Test /status endpoint"""
        import requests
        
        AGGREGATOR_REST_PORT = 8080
        
        url = f"http://localhost:{AGGREGATOR_REST_PORT}/status"
        response = requests.get(url, timeout=5)
        result.equal(response.status_code, 200, "Status endpoint should return 200")
        data = response.json()
        result.contain("app_name", data, "Status should contain app_name")
        result.contain("uptime_seconds", data, "Status should contain uptime_seconds")
        result.contain("state", data, "Status should contain state")
        result.equal(data.get("state"), "running", "Aggregator should be running")
    
    @testcase(tags={'metrics', 'aggregator'})
    def test_metrics_endpoint(self, env, result):
        """Test /metrics endpoint"""
        import requests
        
        AGGREGATOR_REST_PORT = 8080
        
        url = f"http://localhost:{AGGREGATOR_REST_PORT}/metrics"
        response = requests.get(url, timeout=5)
        result.equal(response.status_code, 200, "Metrics endpoint should return 200")
        data = response.json()
        result.contain("uptime_seconds", data, "Metrics should contain uptime_seconds")
