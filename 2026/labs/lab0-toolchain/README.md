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
   (`1 + 1 = 2`). It writes `receipts/receipt-lab0-<timestamp>.txt` —
   upload that file.

## The one idea worth taking away

Open `Check.lean`, change the `2` to a `3`, rerun `bash run-lab.sh`, and
watch it FAIL. A checker that cannot say *no* certifies nothing (book
unit U0P). Change it back, rerun, upload the passing receipt.

## Submit

Upload the passing receipt file (`receipts/receipt-lab0-<timestamp>.txt`)
on your course page, and answer this week's quiz. The full procedure — the
same for every lab — is **[How to submit](../README.md#how-to-submit)**.

## Troubleshooting

- `permission denied ... docker.sock` on Linux → do the post-install step
  (link in `../README.md`), log out and in again.
- Corporate laptop blocks Docker Desktop → ask in the course discussion
  NOW, not on 13 September; there is a fallback path.
- The proof-checker image is a few GB — first pull takes a while on slow
  networks; it is cached afterwards.
