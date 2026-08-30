#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
#
# Lab 1 -- measure alpha+beta between two containers; write the receipt.
# Usage: bash run-lab.sh
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
mkdir -p receipts
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RECEIPT="receipts/receipt-lab1-$STAMP.txt"
LOG="receipts/run-lab1-$STAMP.log"
export LAB_UID="$(id -u)"
export LAB_GID="$(id -g)"

docker compose up --abort-on-container-exit --exit-code-from client > "$LOG" 2>&1 \
  || { tail -20 "$LOG"; docker compose down -v >/dev/null 2>&1; exit 1; }
docker compose down -v >/dev/null 2>&1

{
  echo "ScaDaMaLe Lab 1 receipt -- alpha-beta network measurement"
  echo "generated_utc: $STAMP"
  echo "image: vakeworks/scadamale-lab:0.1.0 (two containers, one bridge network)"
  docker --version
  echo "---- measured ----"
  grep "MEASURED" "$LOG" | sed 's/^.*| //'
  echo "---- log tail ----"
  tail -10 "$LOG"
} > "$RECEIPT"
hash_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1; else shasum -a 256 | cut -d' ' -f1; fi; }
HASH="$(hash_stdin < "$RECEIPT")"
echo "sha256: $HASH" >> "$RECEIPT"

# append to your local sigma-log chain (labs/README.md explains why)
CHAIN="$HERE/../sigma-log.jsonl"
if [ ! -f "$HERE/../README.md" ]; then CHAIN="$HERE/sigma-log.jsonl"; fi
if [ -s "$CHAIN" ]; then PREV="$(tail -1 "$CHAIN" | hash_stdin)"; else PREV="genesis"; fi
echo "{\"ts\":\"$STAMP\",\"lab\":\"lab1\",\"receipt\":\"$(basename "$RECEIPT")\",\"sha256\":\"$HASH\",\"prev\":\"$PREV\"}" >> "$CHAIN"

echo "receipt written: $RECEIPT"
echo "sigma-log appended: $CHAIN"
grep "alpha_us\|beta_ns" "$RECEIPT" || true
