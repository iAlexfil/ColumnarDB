#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <path/to/hits.csv>" >&2
  exit 2
fi

HITS_CSV="$(realpath "$1")"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "${HITS_CSV}" ]]; then
  echo "ERROR: hits.csv not found: ${HITS_CSV}" >&2
  exit 2
fi

WORK="/tmp/bench_work"
MY_RESULTS="${WORK}/my_results"
DUCK_RESULTS="${WORK}/duck_results"
BENCHMARK_CSV="${WORK}/benchmark.csv"
COLUMNAR="${WORK}/hits.columnar"
DUCK_DB="${WORK}/hits.duckdb"
QUERIES_SQL="${WORK}/queries.sql"

rm -rf "${WORK}"
mkdir -p "${MY_RESULTS}" "${DUCK_RESULTS}"

log() { printf '\n==> %s\n' "$*"; }

DUCKDB_BIN="${WORK}/duckdb"

log "Installing DuckDB"
if command -v duckdb >/dev/null 2>&1; then
  DUCKDB_BIN="$(command -v duckdb)"
elif [[ ! -x "${DUCKDB_BIN}" ]]; then
  wget -q https://github.com/duckdb/duckdb/releases/download/v1.2.2/duckdb_cli-linux-amd64.zip \
    -O /tmp/duckdb.zip
  unzip -o /tmp/duckdb.zip -d "${WORK}/" >/dev/null
  chmod +x "${DUCKDB_BIN}"
  rm /tmp/duckdb.zip
fi
echo "DuckDB at ${DUCKDB_BIN}"

log "Building project"
cd "${ROOT_DIR}"
bash script/build.sh >/dev/null 2>&1
echo "Build OK"

log "Converting CSV to columnar format"
START=$(date +%s)
bash script/convert.sh "${HITS_CSV}" "${COLUMNAR}"
END=$(date +%s)
echo "Convert took $((END - START))s"

log "Downloading DuckDB schema"
wget -q https://raw.githubusercontent.com/ClickHouse/ClickBench/main/duckdb/create.sql \
  -O "${WORK}/create.sql"

log "Loading hits into DuckDB"
START=$(date +%s)
"${DUCKDB_BIN}" "${DUCK_DB}" -c ".read ${WORK}/create.sql"
"${DUCKDB_BIN}" "${DUCK_DB}" -c "
COPY hits FROM '${HITS_CSV}' (
  FORMAT CSV, HEADER FALSE, DELIMITER ',', QUOTE '\"',
  DATEFORMAT '%Y-%m-%d', TIMESTAMPFORMAT '%Y-%m-%d %H:%M:%S',
  IGNORE_ERRORS TRUE
);
SELECT COUNT(*) AS rows FROM hits;
"
END=$(date +%s)
echo "DuckDB load took $((END - START))s"

log "Downloading ClickBench queries for DuckDB"
wget -q https://raw.githubusercontent.com/ClickHouse/ClickBench/main/duckdb/queries.sql \
  -O "${QUERIES_SQL}"
echo "$(wc -l < "${QUERIES_SQL}") queries downloaded"

log "Running benchmark: all 43 queries"
echo "query,my_ms,duck_ms,my_rows,duck_rows,match" > "${BENCHMARK_CSV}"

EXACT_COUNT=0
FLOAT_COUNT=0
DIFF_COUNT=0
FAIL_COUNT=0

for q in $(seq 0 42); do
  QP=$(printf "%02d" "$q")
  MY_CSV="${MY_RESULTS}/q${QP}.csv"
  MY_LOG="${MY_RESULTS}/q${QP}.log"
  DUCK_CSV="${DUCK_RESULTS}/q${QP}.csv"

  MY_MS="NA"
  DUCK_MS="NA"
  MY_ROWS=0
  DUCK_ROWS=0
  MATCH="FAIL"

  MY_START=$(date +%s%N)
  if bash script/run_query.sh "$q" "${COLUMNAR}" "${MY_CSV}" "${MY_LOG}" >/dev/null 2>&1; then
    MY_END=$(date +%s%N)
    MY_MS=$(( (MY_END - MY_START) / 1000000 ))
  else
    MY_END=$(date +%s%N)
    MY_MS="FAIL"
  fi

  QUERY=$(sed -n "$((q+1))p" "${QUERIES_SQL}")
  DUCK_START=$(date +%s%N)
  if "${DUCKDB_BIN}" "${DUCK_DB}" -csv -noheader -c "${QUERY}" > "${DUCK_CSV}" 2>/dev/null; then
    DUCK_END=$(date +%s%N)
    DUCK_MS=$(( (DUCK_END - DUCK_START) / 1000000 ))
  else
    DUCK_END=$(date +%s%N)
    DUCK_MS="FAIL"
  fi

  if [[ -f "${MY_CSV}" && -f "${DUCK_CSV}" ]]; then
    tail -n +2 "${MY_CSV}" > "${MY_CSV}.body"

    MY_ROWS=$(wc -l < "${MY_CSV}.body")
    DUCK_ROWS=$(wc -l < "${DUCK_CSV}")

    sort "${MY_CSV}.body"  > "${MY_CSV}.sorted"
    sort "${DUCK_CSV}"     > "${DUCK_CSV}.sorted"

    if diff -q "${MY_CSV}.sorted" "${DUCK_CSV}.sorted" >/dev/null 2>&1; then
      MATCH="EXACT"
      EXACT_COUNT=$((EXACT_COUNT + 1))
    else
      awk -F',' '{for(i=1;i<=NF;i++){if($i+0==$i){$i=sprintf("%.2f",$i)}}print}' OFS=',' \
        "${MY_CSV}.sorted" > "${MY_CSV}.rounded"
      awk -F',' '{for(i=1;i<=NF;i++){if($i+0==$i){$i=sprintf("%.2f",$i)}}print}' OFS=',' \
        "${DUCK_CSV}.sorted" > "${DUCK_CSV}.rounded"

      if diff -q "${MY_CSV}.rounded" "${DUCK_CSV}.rounded" >/dev/null 2>&1; then
        MATCH="FLOAT_OK"
        FLOAT_COUNT=$((FLOAT_COUNT + 1))
      else
        MATCH="DIFF"
        DIFF_COUNT=$((DIFF_COUNT + 1))
        diff "${MY_CSV}.sorted" "${DUCK_CSV}.sorted" \
          > "${WORK}/diff_q${QP}.txt" 2>&1 || true
      fi
    fi
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi

  printf "Q%-2s  my=%-8s  duck=%-8s  rows=%s/%s  %s\n" \
    "$q" "${MY_MS}ms" "${DUCK_MS}ms" "$MY_ROWS" "$DUCK_ROWS" "$MATCH"
  echo "${q},${MY_MS},${DUCK_MS},${MY_ROWS},${DUCK_ROWS},${MATCH}" >> "${BENCHMARK_CSV}"
done

log "SUMMARY"
echo "EXACT:    ${EXACT_COUNT}/43"
echo "FLOAT_OK: ${FLOAT_COUNT}/43"
echo "DIFF:     ${DIFF_COUNT}/43"
echo "FAIL:     ${FAIL_COUNT}/43"

if [[ ${DIFF_COUNT} -gt 0 ]]; then
  echo ""
  echo "Diffs saved to:"
  ls "${WORK}"/diff_q*.txt 2>/dev/null
fi

echo ""
echo "Full results: ${BENCHMARK_CSV}"
echo ""
column -t -s',' "${BENCHMARK_CSV}"
