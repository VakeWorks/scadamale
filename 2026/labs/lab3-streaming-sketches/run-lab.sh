#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
#
# Lab 3 -- run the streaming-sketches lab in the course container and write the receipt.
# Usage:  bash run-lab.sh             (defaults N=1000000 P=8)
#         bash run-lab.sh 3000000 8   (a bigger stream; see the README about memory)
#
# The lab's two programs go INTO the container as a tar stream on stdin, so no folder of yours is
# mounted: the container writes nothing into your lab folder, and it works the same whatever your
# user id is (macOS, Linux, WSL2). Inside, the Python part runs first and writes the stream to
# Parquet; then the Scala part (spark-shell) reads that same stream. The log and the receipt are
# written by this script, on your side.
set -eu
N="${1:-1000000}"
P="${2:-8}"
case "$N$P" in
  *[!0-9]*) echo "usage: bash run-lab.sh [N] [P]   (whole numbers, e.g. bash run-lab.sh 1000000 8)"; exit 2 ;;
esac
IMAGE="vakeworks/scadamale-lab:0.1.2"
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$HERE/receipts"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RECEIPT="$HERE/receipts/receipt-lab3-$STAMP.txt"
LOG="$HERE/receipts/run-lab3-$STAMP.log"

export COPYFILE_DISABLE=1   # macOS: keep Apple metadata files out of the tar stream
tar -C "$HERE" -cf - streaming_sketches.py countmin_library.scala \
  | docker run --rm -i -m 3g \
      -e HOME=/tmp -e HADOOP_USER_NAME=student -e USER=student \
      "$IMAGE" sh -c "mkdir -p /tmp/lab && cd /tmp/lab \
        && tar --no-same-owner --warning=no-unknown-keyword -xf - \
        && python3 streaming_sketches.py $N $P \
        && spark-shell --master 'local[*]' --driver-memory 2g --conf spark.jars.ivy=/tmp/.ivy2 \
             --conf spark.ui.showConsoleProgress=false -i countmin_library.scala" \
      > "$LOG" 2>&1 \
  || { tail -20 "$LOG"; exit 1; }

{
  echo "ScaDaMaLe Lab 3 receipt -- streaming sketches"
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
echo "{\"ts\":\"$STAMP\",\"lab\":\"lab3\",\"receipt\":\"$(basename "$RECEIPT")\",\"sha256\":\"$HASH\",\"prev\":\"$PREV\"}" >> "$CHAIN"

echo "receipt written: $RECEIPT   (upload this .txt, not the .log)"
echo "sigma-log appended: $CHAIN"
grep -e "^MEASURED verified" -e "^MEASURED scala_verified" "$LOG"
