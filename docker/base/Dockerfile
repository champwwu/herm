# Base builder image with all dependencies
# Using Ubuntu 24.04 for GCC 13
FROM ubuntu:24.04 AS base

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    python3 \
    python3-venv \
    python3-pip \
    git \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python venv and Conan (these are expensive and shared)
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Conan install dependencies (most expensive step, shared across all services)
COPY conanfile.py ./
RUN conan profile detect --force
RUN conan install . --output-folder=build --build=missing \
    -s compiler.cppstd=20 -s build_type=Release

# Copy static files (instruments registry)
COPY static/ static/

# This base image is now ready for any service to build from
