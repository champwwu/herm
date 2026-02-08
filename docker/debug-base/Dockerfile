# Debug Base Image - Conan dependencies for Debug builds
# This is separate from herm-base (Release) to cache Debug dependencies
FROM ubuntu:24.04

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

# Python venv and Conan
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Conan install dependencies for DEBUG build (different from Release base)
COPY conanfile.py ./
RUN conan profile detect --force
RUN conan install . --output-folder=build-debug --build=missing \
    -s compiler.cppstd=20 -s build_type=Debug

# Copy static files (instruments registry)
COPY static/ static/

# This base image is now ready for Debug builds
