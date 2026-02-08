#!/usr/bin/env bash
# Run all tests. Run from repo root with venv activated.
set -e
cd "$(dirname "$0")/.."

BUILD_TYPE="${1:-debug}"
case "$(echo "$BUILD_TYPE" | tr '[:upper:]' '[:lower:]')" in
  debug)   CMAKE_TYPE=Debug ;;
  release) CMAKE_TYPE=Release ;;
  *)       echo "Usage: $0 [debug|release]" >&2; exit 1 ;;
esac

BUILD_DIR="build/$CMAKE_TYPE"
if [ ! -d "$BUILD_DIR" ]; then
    echo "Build directory $BUILD_DIR does not exist. Run ./scripts/build.sh first." >&2
    exit 1
fi

cd "$BUILD_DIR"
ctest --output-on-failure
