#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
#
# Lab 0 -- verify the toolchain (git + ssh + docker) + run the proof-checker
# container on the smallest possible theorem; write the receipt.
# Usage: bash run-lab.sh
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
mkdir -p receipts
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RECEIPT="receipts/receipt-lab0-$STAMP.txt"
LOG="receipts/run-lab0-$STAMP.log"
LAB_IMAGE="vakeworks/scadamale-lab:0.1.0"
PROOF_IMAGE="vakeworks/iasprovedog:lean-v4.32.0"

{
  echo "== toolchain versions =="
  git --version
  ssh -V 2>&1 || true
  docker --version
  docker compose version
  echo "== ssh key (public fingerprint only; generate with ssh-keygen -t ed25519 if empty) =="
  ls ~/.ssh/*.pub >/dev/null 2>&1 && ssh-keygen -lf "$(ls ~/.ssh/*.pub | head -1)" || echo "no ssh public key found yet"
  echo "== pulling course images (cached after the first time) =="
  docker pull "$LAB_IMAGE"   || docker image inspect "$LAB_IMAGE"   --format "local image present: {{.RepoTags}}"
  docker pull "$PROOF_IMAGE" || docker image inspect "$PROOF_IMAGE" --format "local image present: {{.RepoTags}}"
  echo "== lab image smoke: pyspark present =="
  docker run --rm --user "$(id -u):$(id -g)" "$LAB_IMAGE" python3 -c "import pyspark; print('MEASURED pyspark_version=' + pyspark.__version__)"
  echo "== proof-checker kernel: the smallest theorem =="
  # Gate the MEASURED line on lean's REAL exit: a brace-group returns its LAST
  # command's status, so an unconditional trailing echo would mask a kernel
  # rejection. The trailing `false` on the reject
  # branch forces the group nonzero so the `|| { ... exit 1; }` guard fires and
  # NO passing receipt is written.
  if docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp -v "$HERE:/lab" -w /lab "$PROOF_IMAGE" lean Check.lean; then
    echo "MEASURED kernel_accepted=one_plus_one"
  else
    echo "MEASURED kernel_REJECTED=one_plus_one"
    false
  fi
} > "$LOG" 2>&1 || { tail -25 "$LOG"; exit 1; }

{
  echo "ScaDaMaLe Lab 0 receipt -- toolchain + proof-checker"
  echo "generated_utc: $STAMP"
  echo "---- run log ----"
  cat "$LOG"
} > "$RECEIPT"
hash_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1; else shasum -a 256 | cut -d' ' -f1; fi; }
HASH="$(hash_stdin < "$RECEIPT")"
echo "sha256: $HASH" >> "$RECEIPT"

# start (or extend) your local sigma-log chain (labs/README.md explains why)
CHAIN="$HERE/../sigma-log.jsonl"
if [ ! -f "$HERE/../README.md" ]; then CHAIN="$HERE/sigma-log.jsonl"; fi
if [ -s "$CHAIN" ]; then PREV="$(tail -1 "$CHAIN" | hash_stdin)"; else PREV="genesis"; fi
echo "{\"ts\":\"$STAMP\",\"lab\":\"lab0\",\"receipt\":\"$(basename "$RECEIPT")\",\"sha256\":\"$HASH\",\"prev\":\"$PREV\"}" >> "$CHAIN"

echo "receipt written: $RECEIPT"
echo "sigma-log appended: $CHAIN"
echo "Lab 0 complete. Try breaking Check.lean (change 2 to 3) and rerun to watch the kernel say no."
