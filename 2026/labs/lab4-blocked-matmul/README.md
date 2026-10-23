
# Lab 4 — blocked matrix multiplication (deadline Mon 30 Nov 2026)

Unit U6 says that on a cluster, matrix multiplication is a **communication**
problem: the arithmetic is fixed (2n³ operations), but how many bytes must
move between machines depends on how the matrices are cut into blocks and
where the blocks live. In this lab you multiply matrices with Spark's
`BlockMatrix`, count every byte Spark moves, and compare two ways of
multiplying sparse matrices. This lab is written in **Scala**, Spark's own
language; Lab 3 was in Python.

## Prerequisite (one-time)

Docker installed and running — see **[Install Docker](../README.md#install-docker-one-time-official-instructions--pick-your-laptops-path)** in `labs/README.md`. In Docker Desktop, give Docker at least **4 GB of memory** (Settings → Resources).

## Run it

Open a terminal in this folder (`cd lab4-blocked-matmul/`), then:

    docker pull vakeworks/scadamale-lab:0.1.2
    bash run-lab.sh

It takes a few minutes. It writes `receipts/receipt-lab4-<timestamp>.txt`
(upload that `.txt`, not the `.log` next to it). The bytes in the receipt are
the same on every machine; the `seconds` vary from run to run and from laptop
to laptop.

## What to look at before uploading

**Part 1 — one product, four block sizes (U6 §4.1, §4.4).** Two random dense
matrices of size n = 2048 (`matrix_MB=33.6` each, as 8-byte numbers) are
multiplied with `BlockMatrix.multiply`, cut into blocks of 128, 256, 512 and
1024 rows and columns. Spark moves blocks in a shuffle to where they are
multiplied, multiplies each pair of blocks locally, and sums the partial
blocks in a second shuffle (U6 §4.4). For each block size the
`dense_multiply` line shows:

- `partitions` — how many partitions Spark spreads the product's blocks over.
  The program creates each input matrix with min(number of blocks, 64)
  partitions (`nb * nb min 64` in the code); Spark turns that into a square
  grid of partitions for C, with at most one partition per block of C;
- `shuffle_written_MB` — the bytes Spark wrote to shuffle, from its own task
  metrics, and `matrix_copies_moved` — the same bytes divided by the size of
  one matrix;
- `stages` and `seconds`. The 6 stages are two small planning jobs inside
  `multiply`, three shuffle stages (the blocks of A, the blocks of B, the
  partial blocks of C) and the final result stage. The `seconds` vary from run
  to run (an untimed warm-up product runs first, but a laptop is a noisy clock);
- `freivalds_rel_error` — every product is checked with **Freivalds' test**:
  for a random vector x, C·x must equal A·(B·x). That costs a few
  matrix–vector products (O(n²) work) instead of recomputing C (O(n³)), and a
  wrong C fails it with high probability. A randomized check of a
  deterministic answer.

Look at `matrix_copies_moved` next to `partitions`, and at how the bytes do
**not** follow the block size one-for-one: question 1 asks why.

**Part 2 — sparse matrices, two ways (U6 §4.4, U5).** Two random sparse
1024 × 1024 matrices with density ρ = 0.001, 0.01, 0.05 and 0.1 (a fraction ρ
of the entries is non-zero), multiplied:

- **as blocks** — `CoordinateMatrix(...).toBlockMatrix(256, 256)` then
  `multiply`; the input blocks are stored sparse, each pair of blocks is
  multiplied locally, and the product blocks come out dense (Spark's shuffle
  compression shrinks them while they are mostly zeros);
- **as a join of entries** — A's entries (i, k, a) keyed by k, B's entries
  (k, j, b) keyed by k, joined on k, multiplied, and summed per output
  position (i, j) with `reduceByKey`: the join pattern of U5.

Both give the same product: for a random vector x, the two results give the
same C·x (`same_Cx_rel_gap`, Freivalds' idea again). The `sparse_multiply` line shows the
non-zeros of A, B and C, `partial_products` — how many pairs (A_ik, B_kj)
with both non-zero there are — and the shuffle bytes of each method. Watch how
`join_shuffle_MB` grows with ρ compared with `nnz_A`: question 2 asks why.

`MEASURED verified=true` is the pass line: every dense product passes
Freivalds' test, and the two sparse methods give the same C·x.

## Reading the Scala

`matmul_blocks.scala` is about 140 lines. If you know Python, five things
make it readable:

- `val x = ...` is a name that does not change (`var` can change);
  `def f(a: Int): Double = ...` is a function with typed arguments;
- `for (bs <- Seq(128, 256, 512, 1024)) { ... }` is a for loop over a list;
- `rdd.map { case (k, (a, b)) => ... }` takes each element apart by its shape,
  like Python's tuple unpacking;
- `f"... $x%.2f ..."` is a formatted string, like Python's f-strings;
- the last expression of a `{ ... }` block is its value (no `return`);
- `measure { ... }` hands the block to `measure` *unevaluated*, like passing
  `lambda: ...` in Python: `measure` starts its clock and its byte meter, then
  runs the block;
- `_ + _` is a short lambda, `lambda a, b: a + b`; `a min b` is `min(a, b)`.

## What happens under the hood

**BlockMatrix.** An n × n matrix is an RDD of `((blockRow, blockCol), block)`
pairs. To multiply, Spark lays the blocks of C on a grid of partitions and
copies to each partition, through the shuffle, the blocks of A and B it needs
(which ones, and how many copies that makes, is question 1). Each pair of
blocks is multiplied locally, and the partial blocks for the same C block are
summed (`reduceByKey`) — a last, smaller shuffle.

**Counting bytes.** Spark records, for every finished task, how many bytes it
wrote to and read from the shuffle. The program registers a *listener* (a
callback Spark calls after each task) and adds those numbers up for each
product. These are the numbers the Spark UI shows.

**Compression.** Spark compresses shuffle data (LZ4) before writing it. Random
numbers barely compress, so the dense part of the lab moves about what the
matrices weigh; blocks that are mostly zeros compress very well.

**Scala and PySpark.** `BlockMatrix` exists in PySpark too (it calls this same
Scala code), but a listener that counts task metrics, as here, is written in
Scala or Java.

## Submit

Upload the receipt file (`receipts/receipt-lab4-<timestamp>.txt`) on your
course page, and answer two questions in the **comment box** of the upload page
(do not edit the receipt: its hash would no longer match), one or two
sentences each:

1. **Block sizes 128 and 256 move the same number of matrix copies (17.0, to
   within 0.1%), while 512 and 1024 move fewer. Why? Derive the number of
   copies from `partitions`.** (Hint: the 2D layout of U6 §4.1.)
2. **Why does the join's shuffle grow so much faster with the density than
   the number of non-zeros does, while the blocks' shuffle does not?** (Hint:
   compare `join_shuffle_MB` with `partial_products` and with `nnz_A`.)

The full procedure — the same for every lab — is
**[How to submit](../README.md#how-to-submit)**.
