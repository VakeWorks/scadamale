#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
#
# Lab 4 -- run the blocked-matmul lab (Scala) in the course container and write the receipt.
# Usage:  bash run-lab.sh
#
# The Scala program goes INTO the container as a tar stream on stdin, so no folder of yours is
# mounted: the container writes nothing into your lab folder, and it works the same whatever your
# user id is (macOS, Linux, WSL2). spark-shell runs it; the log and the receipt are written by this
# script, on your side.
set -eu
IMAGE="vakeworks/scadamale-lab:0.1.2"
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$HERE/receipts"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RECEIPT="$HERE/receipts/receipt-lab4-$STAMP.txt"
LOG="$HERE/receipts/run-lab4-$STAMP.log"

export COPYFILE_DISABLE=1   # macOS: keep Apple metadata files out of the tar stream
tar -C "$HERE" -cf - matmul_blocks.scala \
  | docker run --rm -i -m 3g \
      -e HOME=/tmp -e HADOOP_USER_NAME=student -e USER=student \
      "$IMAGE" sh -c "mkdir -p /tmp/lab && cd /tmp/lab \
        && tar --no-same-owner --warning=no-unknown-keyword -xf - \
        && spark-shell --master 'local[*]' --driver-memory 2g --conf spark.jars.ivy=/tmp/.ivy2 \
             --conf spark.ui.showConsoleProgress=false -i matmul_blocks.scala" \
      > "$LOG" 2>&1 \
  || { tail -20 "$LOG"; exit 1; }

{
  echo "ScaDaMaLe Lab 4 receipt -- blocked matrix multiplication"
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
echo "{\"ts\":\"$STAMP\",\"lab\":\"lab4\",\"receipt\":\"$(basename "$RECEIPT")\",\"sha256\":\"$HASH\",\"prev\":\"$PREV\"}" >> "$CHAIN"

echo "receipt written: $RECEIPT   (upload this .txt, not the .log)"
echo "sigma-log appended: $CHAIN"
grep "^MEASURED verified" "$LOG"
