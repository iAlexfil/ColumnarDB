#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BUILD_TYPE="${BUILD_TYPE:-Release}"
OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build}"
CMAKE_BUILD_DIR="${OUTPUT_FOLDER}/build/${BUILD_TYPE}"

mkdir -p "${CMAKE_BUILD_DIR}"

cmake -S "${ROOT_DIR}" -B "${CMAKE_BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"

cmake --build "${CMAKE_BUILD_DIR}" -j "$(nproc)"
