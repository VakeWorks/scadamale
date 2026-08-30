# Lab 1 — ping-pong + tree vs ring allreduce (ideally due Mon 21 Sep; hard deadline Mon 6 Oct)

The book's whole communication-cost story (chapter F.E4) rests on a
two-parameter model: sending an n-byte message costs

    T(n) = alpha + beta * n

alpha = per-MESSAGE cost (latency), beta = per-BYTE cost (1/bandwidth).
In this lab you *measure* both, on your own machine, between two Docker
containers — and check how well the straight-line model actually fits
(that's the R² in your receipt).

## Prerequisite (one-time)

Docker installed and running — see **[Install Docker](../README.md#install-docker-one-time-official-instructions--pick-your-laptops-path)** in `labs/README.md` (macOS / Linux / Windows-WSL2). Everything else runs inside the container.

## Run it

Open a terminal in this folder (`cd lab1-alpha-beta/`), then:

    docker pull vakeworks/scadamale-lab:0.1.0
    bash run-lab.sh

Two containers come up on one Docker network: a server and a client. The
client times round trips for message sizes from 1 byte to 16 MB (medians
over many repetitions), then solves the least-squares fit (chapter F.M2's
normal equations, 5 lines of code — read them in `measure.py`).

## What to look at before uploading

1. **alpha_us vs beta_ns_per_byte**: on a container bridge you'll see a
   very small alpha (microseconds) and a large effective bandwidth — this
   is the *loopback baseline*. Real cluster networks have alpha thousands
   of times larger; the RATIO alpha/beta is what decides tree-vs-flat
   aggregation (F.E4 section 3), and your receipt gives you your machine's.
   Do not be alarmed if your fitted alpha comes out slightly NEGATIVE: a
   physical latency cannot be negative, but a least-squares *intercept* can
   be, when the loopback alpha is tiny and the large-message points steer
   the line (that is measurement noise plus extrapolation, and exactly why
   the receipt carries R² — a model is a fit, not a truth; say so in your
   one-sentence receipt comment and you have understood the lab).
2. **r_squared**: if it is not close to 1, the linear model missed
   something on your machine (a cache effect, a power-management kink) —
   that observation is worth a sentence in the course discussion.
3. **The allreduce analysis (the second half of the lab's name — no new
   code).** Plug YOUR measured alpha and beta into the two classic
   allreduce cost models for p workers and an n-byte vector:
   tree: T ≈ 2·log₂(p)·(alpha + beta·n); ring: T ≈ 2(p−1)·alpha +
   2·((p−1)/p)·beta·n. For your numbers, which wins at n = 1 KB and which
   at n = 100 MB — and at what n do they cross? Put the crossover in your
   receipt comment. This is F.E4's tree-vs-flat trade-off made personal:
   small messages are latency-bound (tree wins), big ones are
   bandwidth-bound (ring wins).
4. The quiz-style takeaway: p-1 one-byte messages cost ~(p-1)·alpha; one
   n-byte stream costs alpha + beta·n. Which dominates for your alpha,
   beta at n = 1 MB, p = 16?

## Submit

`run-lab.sh` prints a receipt path (`receipts/receipt-lab1-<timestamp>.txt`)
and appends a line to `../sigma-log.jsonl`. **Upload that receipt file** on
your course page, and add your one-sentence comments (the alpha/beta model
note from step 1, the tree-vs-ring crossover from step 3) in the submission
text. Also answer this week's quiz on the course page. The full procedure —
the same for every lab — is **[How to submit](../README.md#how-to-submit)**.

*Looking further*: actually implementing and racing tree vs ring allreduce
across real containers is an optional deep-dive lab later in the course
(it arrives with the container-orchestration track), not part of this
receipt.

Optional (Linux, 16 GB, curious): add artificial latency to feel a real
network — `docker compose run --cap-add NET_ADMIN client sh -c "tc qdisc
add dev eth0 root netem delay 1ms && python3 measure.py --client server"`
— alpha should jump by ~2 ms while beta barely moves.
