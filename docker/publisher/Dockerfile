# Build from common base
FROM herm-base:latest AS builder

WORKDIR /app

# Copy source and build publisher only
COPY proto/ proto/
COPY apps/ apps/
COPY engine/ engine/
COPY config/ config/
COPY CMakeLists.txt .
RUN cd build && cmake .. -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
RUN cd build && cmake --build . --target publisher

# Runtime stage: minimal image with publisher binary
FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/build/bin/publisher /app/publisher
COPY --from=builder /app/config /app/config

EXPOSE 8082 8083 8085

# Default: run publisher (config file should be passed as argument)
CMD ["./publisher", "--config_file=publisher_bbo.json"]
