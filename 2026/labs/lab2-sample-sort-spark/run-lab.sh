#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
#
# Lab 2 -- run sample sort in the course container and write the receipt.
# Usage:  bash run-lab.sh            (defaults N=2000000 P=8)
#         bash run-lab.sh 5000000 8  (bigger N if your laptop allows)
set -eu
N="${1:-2000000}"
P="${2:-8}"
IMAGE="vakeworks/scadamale-lab:0.1.0"
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$HERE/receipts"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RECEIPT="$HERE/receipts/receipt-lab2-$STAMP.txt"
LOG="$HERE/receipts/run-lab2-$STAMP.log"

docker run --rm -m 3g \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp -e HADOOP_USER_NAME=student -e USER=student \
  -v "$HERE:/lab" -w /lab "$IMAGE" \
  python3 sample_sort.py "$N" "$P" > "$LOG" 2>&1 || { tail -20 "$LOG"; exit 1; }

{
  echo "ScaDaMaLe Lab 2 receipt -- sample sort on Spark"
  echo "generated_utc: $STAMP"
  echo "image: $IMAGE"
  docker --version
  echo "---- measured ----"
  grep "^MEASURED" "$LOG"
  echo "---- log tail ----"
  tail -15 "$LOG"
} > "$RECEIPT"
hash_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1; else shasum -a 256 | cut -d' ' -f1; fi; }
HASH="$(hash_stdin < "$RECEIPT")"
echo "sha256: $HASH" >> "$RECEIPT"

# append to your local sigma-log chain (labs/README.md explains why)
CHAIN="$HERE/../sigma-log.jsonl"
if [ ! -f "$HERE/../README.md" ]; then CHAIN="$HERE/sigma-log.jsonl"; fi
if [ -s "$CHAIN" ]; then PREV="$(tail -1 "$CHAIN" | hash_stdin)"; else PREV="genesis"; fi
echo "{\"ts\":\"$STAMP\",\"lab\":\"lab2\",\"receipt\":\"$(basename "$RECEIPT")\",\"sha256\":\"$HASH\",\"prev\":\"$PREV\"}" >> "$CHAIN"

echo "receipt written: $RECEIPT"
echo "sigma-log appended: $CHAIN"
grep "^MEASURED verified" "$LOG"
