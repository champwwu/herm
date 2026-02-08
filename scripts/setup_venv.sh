#!/usr/bin/env bash
# Create Python venv and install Conan for C++ dependency management.
set -e
cd "$(dirname "$0")/.."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo "Activate with: source .venv/bin/activate"
