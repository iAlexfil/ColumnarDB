#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -lt 2 ]]; then
  echo "Usage: script/convert.sh <input_csv> <output_columnar> [input_schema]" >&2
  exit 2
fi

INPUT_CSV="$1"
COLUMNAR="$2"
INPUT_SCHEMA="${3:-${ROOT_DIR}/hits.schema}"

BUILD_TYPE="${BUILD_TYPE:-Release}"
OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build}"
BIN="${OUTPUT_FOLDER}/build/${BUILD_TYPE}/csv_to_columnar"

if [[ ! -x "${BIN}" ]]; then
  echo "ERROR: csv_to_columnar not found at ${BIN}" >&2
  echo "Run script/build.sh first" >&2
  exit 1
fi

mkdir -p "$(dirname "${COLUMNAR}")"

"${BIN}" --input "${INPUT_CSV}" --schema "${INPUT_SCHEMA}" --output "${COLUMNAR}"
