# Lab 2 — sample sort on Spark (ideally due Mon 28 Sep; hard deadline Mon 6 Oct)

You implement **sample sort** (book unit U2) on **Spark** (unit U5), verify
it, and race it against Spark's built-in `sortBy`. Spark runs in `local[*]`
mode inside one container — every CPU core is a worker; this is safe on an
8 GB laptop.

## Prerequisite (one-time)

Docker installed and running — see **[Install Docker](../README.md#install-docker-one-time-official-instructions--pick-your-laptops-path)** in `labs/README.md`. Everything else runs inside the container.

## Run it

Open a terminal in this folder (`cd lab2-sample-sort-spark/`), then:

    docker pull vakeworks/scadamale-lab:0.1.0
    bash run-lab.sh

That's the whole lab mechanically: it writes
`receipts/receipt-lab2-<timestamp>.txt`; upload that file. But the points
of understanding, before you upload, are:

1. **Read `sample_sort.py`** (~100 lines). Find the ONE shuffle
   (`partitionBy`). U2's claim: sampling `32p` keys is enough to make all
   `p` buckets balanced with high probability — your receipt's
   `balance` number is that claim, measured on your machine.
2. **The verification** never trusts the algorithm: it checks global order
   across bucket boundaries AND that the multiset of records survived
   (count + checksum). Grading is on the receipt's `verified=True`.
3. **The race**: is your sample sort faster or slower than `sortBy`? Within
   noise they should be close — `sortBy` IS a range-partition sort. Both
   move all n records exactly once.

Optional, if your laptop has 16 GB: rerun with a bigger input,
`bash run-lab.sh 10000000 8`, and watch whether the balance factor and the
speed ratio hold at scale.

## Submit

Upload the receipt file (`receipts/receipt-lab2-<timestamp>.txt`) on your
course page, and answer this week's quiz. The full procedure — the same for
every lab — is **[How to submit](../README.md#how-to-submit)**.
