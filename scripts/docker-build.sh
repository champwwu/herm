#!/bin/bash
# Build Docker images efficiently with shared base

set -e

echo "==> Building base image with all dependencies (pip, conan)..."
docker build -f docker/base/Dockerfile -t herm-base:latest .

echo ""
echo "==> Building aggregator (using base image)..."
docker build -f docker/aggregator/Dockerfile -t herm-aggregator:latest .

echo ""
echo "==> Building publisher (using base image)..."
docker build -f docker/publisher/Dockerfile -t herm-publisher:latest .

echo ""
echo "✅ All images built successfully!"
echo ""
echo "Images:"
docker images | grep herm

echo ""
echo "To run: docker compose up"
