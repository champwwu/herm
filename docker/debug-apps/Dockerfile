# Debug Apps Image - All compiled binaries (aggregator + publisher) in Debug mode
FROM herm-debug-base:latest

WORKDIR /app

# Copy source and build ALL binaries
COPY proto/ proto/
COPY apps/ apps/
COPY engine/ engine/
COPY CMakeLists.txt .

# Build all targets in Debug mode
RUN cd build-debug && cmake .. -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=OFF
RUN cd build-debug && cmake --build . --parallel $(nproc)

# This image now contains:
# - /app/build-debug/bin/aggregator
# - /app/build-debug/bin/publisher
# - All Debug symbols and libraries
