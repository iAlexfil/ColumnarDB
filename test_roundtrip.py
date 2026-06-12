# Файл сгенерированный с помощью LLM для тестирования и замеров скорости конвертации
# пример запуска:  python3 test_roundtrip.py --workdir ./bench_multi multi --tests 5 --min-mb 64 --max-gb 1

#!/usr/bin/env python3
# roundtrip_min.py
#
# Usage:
#   python3 roundtrip_min.py /path/to/ColumnarDB 10
#
# Generates random CSV files (64 MiB..1 GiB), runs:
#   csv2col schema.csv data.csv data.columnar
#   col2csv data.columnar schema_back.csv data_back.csv
# Verifies byte-equality of schema and data, measures time.

from __future__ import annotations

import os
import random
import subprocess
import sys
import time


MiB = 1024 * 1024
GiB = 1024 * 1024 * 1024


def die(msg: str, code: int = 2) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def run(cmd: list[str]) -> float:
    t0 = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.perf_counter() - t0
    if p.returncode != 0:
        die(
            "Command failed:\n"
            f"  cmd: {' '.join(cmd)}\n"
            f"  code: {p.returncode}\n"
            f"  stdout:\n{p.stdout}\n"
            f"  stderr:\n{p.stderr}\n"
        )
    return dt


def same_file(a: str, b: str) -> None:
    sa = os.path.getsize(a)
    sb = os.path.getsize(b)
    if sa != sb:
        die(f"Size mismatch: {a} ({sa}) != {b} ({sb})")
    with open(a, "rb") as fa, open(b, "rb") as fb:
        while True:
            ca = fa.read(4 * MiB)
            cb = fb.read(4 * MiB)
            if ca != cb:
                die(f"Content mismatch: {a} != {b}")
            if not ca:
                break


def write_schema(path: str, cols: int = 10, int_cols: int = 5) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        for i in range(cols):
            f.write(f"c{i},{'int64' if i < int_cols else 'string'}\n")


def write_csv(path: str, target_bytes: int, cols: int = 10, int_cols: int = 5, seed: int = 1) -> int:
    rnd = random.Random(seed)
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    pool = ["".join(rnd.choice(alphabet) for _ in range(rnd.randint(6, 18))) for _ in range(5000)]

    written = 0
    with open(path, "wb") as f:
        while True:
            fields = []
            for c in range(cols):
                if c < int_cols:
                    fields.append(str(rnd.randint(-10**12, 10**12)))
                else:
                    fields.append(pool[rnd.randrange(len(pool))])
            line = (",".join(fields) + "\n").encode("utf-8")
            if written + len(line) > target_bytes:
                break
            f.write(line)
            written += len(line)
    return written


def main() -> int:
    if len(sys.argv) != 3:
        die("Usage: python3 roundtrip_min.py /path/to/ColumnarDB <n>")

    bin_path = sys.argv[1]
    try:
        n = int(sys.argv[2])
    except ValueError:
        die("n must be integer")
    if n <= 0:
        die("n must be > 0")

    if not (os.path.isfile(bin_path) or any(os.access(os.path.join(p, bin_path), os.X_OK) for p in os.getenv("PATH", "").split(os.pathsep))):
        # allow PATH name OR explicit file; this is a cheap check
        # if it's a PATH name, subprocess will still work even if we can't stat it reliably here
        pass

    rnd = random.Random(12345)

    total1 = 0.0
    total2 = 0.0

    for i in range(1, n + 1):
        d = f"bench_{i:03d}"
        os.makedirs(d, exist_ok=True)

        schema = os.path.join(d, "schema.csv")
        data = os.path.join(d, "data.csv")
        col = os.path.join(d, "data.columnar")
        schema2 = os.path.join(d, "schema_back.csv")
        data2 = os.path.join(d, "data_back.csv")

        target = rnd.randint(64 * MiB, 1 * GiB)

        write_schema(schema)
        actual = write_csv(data, target, seed=1_000_003 * i)

        t1 = run([bin_path, "to-columnar", schema, data, col])
        t2 = run([bin_path, "to-csv", col, schema2, data2])

        same_file(schema, schema2)
        same_file(data, data2)

        total1 += t1
        total2 += t2

        in_sz = actual
        col_sz = os.path.getsize(col)
        mbs1 = (in_sz / MiB) / t1 if t1 > 0 else float("inf")
        mbs2 = (col_sz / MiB) / t2 if t2 > 0 else float("inf")

        print(
            f"[{i}/{n}] CSV={in_sz/MiB:.1f} MiB  COL={col_sz/MiB:.1f} MiB  "
            f"csv2col={t1:.3f}s ({mbs1:.2f} MiB/s)  col2csv={t2:.3f}s ({mbs2:.2f} MiB/s)"
        )

    print(f"\nTOTAL: csv2col={total1:.3f}s  col2csv={total2:.3f}s  (n={n})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
