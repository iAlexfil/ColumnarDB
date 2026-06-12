#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -lt 4 ]]; then
  echo "Usage: script/run_query.sh <query_num> <columnar> <output_csv> <log_file>" >&2
  exit 2
fi

QUERY_NUM="$1"
COLUMNAR="$2"
OUTPUT_CSV="$3"
LOG_FILE="$4"
QUERY_NUM_PADDED="$(printf "%02d" "${QUERY_NUM}")"

BUILD_TYPE="${BUILD_TYPE:-Release}"
OUTPUT_FOLDER="${OUTPUT_FOLDER:-${ROOT_DIR}/build}"
SCHEMA="${SCHEMA:-${ROOT_DIR}/hits.schema}"
BIN="${OUTPUT_FOLDER}/build/${BUILD_TYPE}/clickbench_run"

if [[ ! -x "${BIN}" ]]; then
  echo "ERROR: clickbench_run not found at ${BIN}" >&2
  echo "Run script/build.sh first" >&2
  exit 1
fi

mkdir -p "$(dirname "${OUTPUT_CSV}")"
mkdir -p "$(dirname "${LOG_FILE}")"

TEMP_OUTPUT_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TEMP_OUTPUT_DIR}"
}
trap cleanup EXIT

"${BIN}" \
  --input "${COLUMNAR}" \
  --schema "${SCHEMA}" \
  --output_dir "${TEMP_OUTPUT_DIR}" \
  --queries="${QUERY_NUM}" \
  2>&1 | tee "${LOG_FILE}"

EXPECTED_CSV="${TEMP_OUTPUT_DIR}/q${QUERY_NUM_PADDED}.csv"
if [[ -f "${EXPECTED_CSV}" ]]; then
  mv "${EXPECTED_CSV}" "${OUTPUT_CSV}"
  exit 0
fi

echo "ERROR: query output CSV not found for Q${QUERY_NUM} in ${TEMP_OUTPUT_DIR}" >&2
exit 3
