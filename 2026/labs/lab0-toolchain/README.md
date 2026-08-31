# Lab 0 — toolchain + proof-checker (ideally due Mon 14 Sep; hard deadline Mon 6 Oct)

**Do this one before anything else** — every later lab builds on it, and it
must be complete before the Module-1 meeting. It has no algorithmic
content: the deliverable is a *working toolchain*, receipted.

## Steps

1. **Install Docker** — official links + a terminal on-ramp are in
   `../README.md` (one level up). Verify: `docker run --rm hello-world`.
2. **git**: `git --version` (macOS prompts an install if missing; Linux:
   your package manager; Windows: https://git-scm.com/downloads).
3. **ssh key** (used later for sovereign git, chapter F.E1b): if
   `ls ~/.ssh/*.pub` shows nothing, run
   `ssh-keygen -t ed25519` and press Enter through the prompts.
4. **Run the lab**:

       bash run-lab.sh

   It pulls both course images, smoke-tests the lab image, and asks the
   Lean proof-checker to accept the one-line theorem in `Check.lean`
   (`1 + 1 = 2`). It writes `receipts/receipt-lab0-<timestamp>.txt` (the
   file you upload) plus a `receipts/run-lab0-<timestamp>.log` (the raw run
   output, kept for debugging — you do *not* upload the log).

   **Receipts are bound to a git commit.** The script refuses to write a
   receipt while you have uncommitted changes, so that the receipt you
   submit corresponds to an exact, reproducible commit of your work. A
   fresh clone is already committed, so the first run just works. Whenever
   you change a file, commit it before re-running (see the next section).

## The one idea worth taking away

A checker that cannot say *no* certifies nothing (book unit U0P). See it
for yourself — committing before each run, since receipts are bound to a
commit:

1. Open `Check.lean`, change the `2` to a `3`, and commit the change:

       git add Check.lean
       git commit -m "break: 1+1=3"

2. Rerun `bash run-lab.sh` and watch the proof-checker **REJECT** it. No
   receipt is written for a broken proof — that rejection is the point.
3. Change it back to `2`, commit, and rerun to get your passing receipt:

       git add Check.lean
       git commit -m "restore: 1+1=2"
       bash run-lab.sh

## Submit

Upload the passing receipt file (`receipts/receipt-lab0-<timestamp>.txt`)
on your course page, and answer this week's quiz. The full procedure — the
same for every lab — is **[How to submit](../README.md#how-to-submit)**.

## Troubleshooting

- `permission denied ... docker.sock` on Linux → do the post-install step
  (link in `../README.md`), log out and in again.
- Corporate laptop blocks Docker Desktop → ask in the course discussion
  NOW, not on 13 September; there is a fallback path.
- The images are large — the lab image is ~1.2 GB and the proof-checker
  image (`vakeworks/lean-checker`) is bigger; the first pull can take a
  while on slow networks. Both are cached after the first run.
