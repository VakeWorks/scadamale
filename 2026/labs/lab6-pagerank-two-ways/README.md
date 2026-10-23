
# Lab 6 — PageRank two ways (deadline Mon 30 Nov 2026)

Unit U12 computes PageRank by power iteration: every iteration, each vertex
sends its rank to its neighbours, and on a cluster that means data moves. U12
§4.2 proves that partitioning the graph once, the right way, stops the largest
table from moving at all. In this lab you run PageRank on a graph of about a
million edges three ways — naive and co-partitioned on Spark RDDs (Python), and
with GraphX, Spark's graph library (Scala) — and measure what each one moves.

## Prerequisite (one-time)

Docker installed and running — see **[Install Docker](../README.md#install-docker-one-time-official-instructions--pick-your-laptops-path)** in `labs/README.md`. In Docker Desktop, give Docker at least **4 GB of memory** (Settings → Resources).

## Run it

Open a terminal in this folder (`cd lab6-pagerank-two-ways/`), then:

    docker pull vakeworks/scadamale-lab:0.1.2
    bash run-lab.sh

While it runs, `tail -f receipts/run-lab6-*.log` shows its progress.

It takes a few minutes. It writes `receipts/receipt-lab6-<timestamp>.txt`
(upload that `.txt`, not the `.log` next to it). The bytes and ranks are the
same on every machine; only the `seconds` differ.

## What to look at before uploading

**The graph** (the `graph` line). 100,000 vertex ids and about 944,000 directed
edges with power-law degrees: a few hubs with very many links, most vertices
with few. `vertices_with_edges` counts the ids that have at least one edge.
PageRank runs for 10 iterations; a link is followed with probability 0.85, a
random jump taken with probability 0.15 (U12 §2.1's α).

1. **Naive and co-partitioned, on RDDs — U12 §4.1, §4.2.** Both run the loop
   of U12 §4.1: a `join` of the link table with the ranks, a `flatMap` that
   turns each vertex's rank into one contribution per out-link, and a
   `reduceByKey` that sums the contributions per destination. They give the
   same ranks (`same_ranks`). They differ in one thing: the naive version
   builds its tables with `map`, the co-partitioned one with `mapValues`
   (see "Under the hood"). Each iteration runs as two Spark jobs, so the
   receipt shows the shuffle of the join and of the contributions separately:
   compare `join_shuffle_MB_per_iteration` and
   `contrib_shuffle_MB_per_iteration` across the two versions. (The first
   iteration or two moves a little less: every starting rank is 1/n, and Spark
   compresses what it shuffles, so identical values cost fewer bytes. Compare
   iterations 2–10.)
2. **These are the ranks of the classic Spark loop, not quite U12 §2.1's.** A
   vertex with no in-links receives no contribution, so it drops out of the
   rank table after the first iteration; and the rank of a vertex with no
   out-links goes nowhere. That is why `ranked_vertices` is below
   `vertices_with_edges`, and why `rank_sum` is about 0.91, not 1. (U12 §2.1
   gives every vertex the teleport term and spreads the dangling rank over all
   vertices.)
3. **GraphX, in Scala — U12 §5.3, §6.4.** `pagerank_graphx.scala` reads the
   same graph (from Parquet) and calls GraphX's `staticPageRank` for 10
   iterations, on 8 edge partitions. GraphX keeps every vertex that has an edge
   and scales the ranks to sum to their number, so the lab compares which
   vertices come out on top (`same_top10_as_rdd`, `same_top100_as_rdd`). Its
   `shuffle_MB_per_iteration` leaves out the one-time setup (`setup_MB`).
   Compare its bytes and its `seconds` with the two RDD versions.

`MEASURED verified=True` and `MEASURED graphx_verified=true` are the pass
lines: the two RDD versions give the same ranks, the co-partitioned join moves
nothing, and GraphX finds the same top vertices.

## What happens under the hood

**Partitioners and narrow joins.** An RDD of key–value pairs can carry a
*partitioner*: a promise that key k lives in partition h(k) mod P. When two
RDDs carry the same partitioner, `join` can match partition i with partition
i, without moving anything: a narrow dependency. `mapValues` keeps the promise
(keys do not change); `map` breaks it, because Spark cannot know that your
function left the keys alone. In PySpark, two partitioners are the same only if
they have the same number of partitions and the same hash function, which is
why the lab passes `P` to every `join` and `reduceByKey`.

**Python and the JVM.** Spark itself runs in the Java virtual machine (JVM).
When an RDD job calls a Python function (each `lambda` here), every record is
serialised (pickled) in the JVM, sent to a Python worker process, handled
there, and serialised back. GraphX is written in Scala and runs in the JVM.

**GraphX.** GraphX stores a graph as an edge table, split by `EdgePartition2D`
(a vertex-cut: each edge lives in one partition, a vertex may appear in
several), and a vertex table. Each iteration ships the current ranks to the
edge partitions that need them, computes the contributions there, and sums
them back per vertex (U12 §5.3, §6.4). GraphFrames, a separate package, offers
the same on DataFrames with a Python API; the lab uses GraphX because it ships
with Spark.

**Parquet** — as in Lab 3: the Python part writes the graph, column by column,
and the Scala part reads it back.

## Submit

Upload the receipt file (`receipts/receipt-lab6-<timestamp>.txt`) on your
course page, and answer two questions in the **comment box** of the upload page
(do not edit the receipt: its hash would no longer match), one or two
sentences each:

1. **In the co-partitioned version the join moves nothing, yet in both
   versions the contributions still move about 4.1 MB every iteration. Why can
   co-partitioning remove the join's shuffle, but not this one?** (Hint: U12
   §4.2's proof and its "honest scope", and the edge-cut of U12 §5.3.)
2. **GraphX moves about as many bytes per iteration as the RDD versions (about
   5.2 MB, against 4.1 MB co-partitioned), yet its 10 iterations take a few
   seconds instead of half a minute. If the bytes do not explain the
   difference, what does?** (Hint: "Under the hood"; count the records each
   iteration handles.)

The full procedure — the same for every lab — is
**[How to submit](../README.md#how-to-submit)**.
