#!/bin/bash
# Integration Test Runner Script
# Builds and runs the integration test container which manages all services internally

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}herm Integration Test Runner${NC}"
echo -e "${GREEN}========================================${NC}"
echo

# Step 1: Build debug base image (with Debug Conan dependencies)
echo -e "${YELLOW}[1/4] Building debug base image (Conan Debug dependencies)...${NC}"
docker build -f docker/debug-base/Dockerfile -t herm-debug-base:latest .
echo -e "${GREEN}✓ Debug base image built${NC}"
echo

# Step 2: Build debug apps image (compile all binaries)
echo -e "${YELLOW}[2/4] Building debug apps image (compile all C++ binaries)...${NC}"
docker build -f docker/debug-apps/Dockerfile -t herm-debug-apps:latest .
echo -e "${GREEN}✓ Debug apps image built${NC}"
echo

# Step 3: Build test container (Python env + binaries)
echo -e "${YELLOW}[3/4] Building integration test image (Python env + test setup)...${NC}"
docker build -f docker/test/Dockerfile -t herm-test:latest .
echo -e "${GREEN}✓ Integration test image built${NC}"
echo

# Step 4: Clean up old test results and logs
echo -e "${YELLOW}[4/4] Preparing test environment...${NC}"
rm -rf "${PROJECT_ROOT}/test-results"
rm -rf "${PROJECT_ROOT}/logs"
mkdir -p "${PROJECT_ROOT}/test-results"
mkdir -p "${PROJECT_ROOT}/logs"
echo -e "${GREEN}✓ Test environment prepared${NC}"
echo

# Run integration tests
echo -e "${YELLOW}Starting integration test suite...${NC}"
echo -e "${YELLOW}The test container will:${NC}"
echo -e "  - Start 3 mock exchange servers (Python)"
echo -e "  - Start the aggregator service (C++ binary)"
echo -e "  - Start 3 publisher services (C++ binaries)"
echo -e "  - Run the full integration test suite (Testplan)"
echo

# Run the test container (it manages everything internally via testplan)
if docker compose -f docker-compose.test.yml run --rm integration-test \
    python3 tests/integration/testplan_e2e.py --pdf /app/test-results/testplan.pdf; then
    echo
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo -e "${GREEN}========================================${NC}"
    TEST_EXIT_CODE=0
else
    echo
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}✗ Tests failed!${NC}"
    echo -e "${RED}========================================${NC}"
    TEST_EXIT_CODE=1
fi

echo
echo -e "${YELLOW}Test results saved to: ${PROJECT_ROOT}/test-results${NC}"
echo -e "${YELLOW}Service logs saved to: ${PROJECT_ROOT}/logs${NC}"
echo

# Show test results if available
if [ -f "${PROJECT_ROOT}/test-results/testplan.html" ]; then
    echo -e "${GREEN}View HTML report at: test-results/testplan.html${NC}"
fi

if [ -f "${PROJECT_ROOT}/test-results/testplan.pdf" ]; then
    echo -e "${GREEN}View PDF report at: test-results/testplan.pdf${NC}"
fi

echo

# Cleanup any leftover containers
docker compose -f docker-compose.test.yml down 2>/dev/null || true

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Integration test run completed successfully!${NC}"
else
    echo -e "${RED}Integration test run failed. Check logs in ./logs/ directory.${NC}"
fi

exit $TEST_EXIT_CODE
