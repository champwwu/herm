# Build from common base
FROM herm-base:latest AS builder

WORKDIR /app

# Copy source and build aggregator only
COPY proto/ proto/
COPY apps/ apps/
COPY engine/ engine/
COPY config/ config/
COPY CMakeLists.txt .
RUN cd build && cmake .. -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
RUN cd build && cmake --build . --target aggregator

# Runtime stage: minimal image with aggregator binary
FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/build/bin/aggregator /app/aggregator
COPY --from=builder /app/config /app/config
COPY --from=builder /app/static /app/static

EXPOSE 50051 8080

# Default: run aggregator
CMD ["./aggregator", "--config_file=aggregator.json"]
