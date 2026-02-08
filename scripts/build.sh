#!/usr/bin/env bash
# Install Conan deps and build with CMake. Run from repo root with venv activated.
# Usage: ./scripts/build.sh [debug|release]  (default: release)
set -e
cd "$(dirname "$0")/.."

BUILD_TYPE="${1:-debug}"
case "$(echo "$BUILD_TYPE" | tr '[:upper:]' '[:lower:]')" in
  debug)   CMAKE_TYPE=Debug; CONAN_TYPE=Debug ;;
  release) CMAKE_TYPE=Release; CONAN_TYPE=Release ;;
  *)       echo "Usage: $0 [debug|release]" >&2; exit 1 ;;
esac

BUILD_DIR="build/$CMAKE_TYPE"
conan install . --output-folder="$BUILD_DIR" --build=missing \
  -s compiler.cppstd=20 -s build_type=$CONAN_TYPE
cmake -B "$BUILD_DIR" -DCMAKE_TOOLCHAIN_FILE="$BUILD_DIR/conan_toolchain.cmake" \
  -DCMAKE_BUILD_TYPE=$CMAKE_TYPE
cmake --build "$BUILD_DIR" --target all
echo "Binaries built in: $BUILD_DIR/bin"
