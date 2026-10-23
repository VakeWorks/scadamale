#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
#
# Lab 6 -- run the PageRank-two-ways lab in the course container and write the receipt.
# Usage:  bash run-lab.sh
#
# The lab's two programs go INTO the container as a tar stream on stdin, so no folder of yours is
# mounted: the container writes nothing into your lab folder, and it works the same whatever your
# user id is (macOS, Linux, WSL2). Inside, the Python part runs first and writes the graph to
# Parquet; then the Scala part (spark-shell, GraphX) reads that same graph. The log and the
# receipt are written by this script, on your side.
set -eu
IMAGE="vakeworks/scadamale-lab:0.1.2"
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$HERE/receipts"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RECEIPT="$HERE/receipts/receipt-lab6-$STAMP.txt"
LOG="$HERE/receipts/run-lab6-$STAMP.log"

export COPYFILE_DISABLE=1   # macOS: keep Apple metadata files out of the tar stream
tar -C "$HERE" -cf - pagerank_rdd.py pagerank_graphx.scala \
  | docker run --rm -i -m 3g \
      -e HOME=/tmp -e HADOOP_USER_NAME=student -e USER=student \
      "$IMAGE" sh -c "mkdir -p /tmp/lab && cd /tmp/lab \
        && tar --no-same-owner --warning=no-unknown-keyword -xf - \
        && python3 -u pagerank_rdd.py \
        && spark-shell --master 'local[*]' --driver-memory 2g --conf spark.jars.ivy=/tmp/.ivy2 \
             --conf spark.ui.showConsoleProgress=false -i pagerank_graphx.scala" \
      > "$LOG" 2>&1 \
  || { tail -20 "$LOG"; exit 1; }
# spark-shell -i exits 0 even when a line of the Scala part fails, so require both pass lines.
for PASS in "^MEASURED verified=True" "^MEASURED graphx_verified=true"; do
  grep -q "$PASS" "$LOG" || { tail -30 "$LOG"; echo "Lab 6 did not finish: no '${PASS#^}' line (full log: $LOG)"; exit 1; }
done

{
  echo "ScaDaMaLe Lab 6 receipt -- PageRank two ways"
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
echo "{\"ts\":\"$STAMP\",\"lab\":\"lab6\",\"receipt\":\"$(basename "$RECEIPT")\",\"sha256\":\"$HASH\",\"prev\":\"$PREV\"}" >> "$CHAIN"

echo "receipt written: $RECEIPT   (upload this .txt, not the .log)"
echo "sigma-log appended: $CHAIN"
grep -e "^MEASURED verified" -e "^MEASURED graphx_verified" "$LOG"
