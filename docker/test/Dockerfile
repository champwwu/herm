# Integration Test Container - Lightweight runtime with Python test environment
# Uses pre-compiled binaries from herm-debug-apps
FROM ubuntu:24.04

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    ca-certificates \
    libstdc++6 \
    curl \
    openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Set up Python virtual environment
RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy compiled binaries from debug-apps image (NOT rebuilding!)
COPY --from=herm-debug-apps:latest /app/build-debug/bin/ /app/build/Debug/bin/

# Copy test files and configurations
COPY tests/integration/ /app/tests/integration/
COPY static/ /app/static/
COPY config/ /app/config/

# Create directories for test results and logs
RUN mkdir -p /app/test-results /app/logs

WORKDIR /app

# Expose ports for external access (optional, for debugging)
EXPOSE 8080 50051 8082 8083 8085 8091 8092 8093

# Default: run integration tests
CMD ["python3", "tests/integration/testplan_e2e.py", "--pdf", "/app/test-results/testplan.pdf"]
