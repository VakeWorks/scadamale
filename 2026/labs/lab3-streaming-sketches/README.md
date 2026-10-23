
# Lab 3 — streaming sketches (deadline Mon 30 Nov 2026)

Unit U4 says that **memory is a resource too**: with one pass over a stream and
a small, fixed memory we can still sample it uniformly, estimate how often each
item occurs, and estimate how many distinct items it holds — approximately, but
with guarantees. In this lab you watch each of those guarantees hold on a
stream of a million items, on your own laptop, first in Python (PySpark) and
then in Scala, Spark's own language.

## Prerequisite (one-time)

Docker installed and running — see **[Install Docker](../README.md#install-docker-one-time-official-instructions--pick-your-laptops-path)** in `labs/README.md`. In Docker Desktop, give Docker at least **4 GB of memory** (Settings → Resources); the lab itself uses up to 3 GB. Everything else runs inside the container.

## Run it

Open a terminal in this folder (`cd lab3-streaming-sketches/`), then:

    docker pull vakeworks/scadamale-lab:0.1.2
    bash run-lab.sh

While it runs, `tail -f receipts/run-lab3-*.log` shows its progress.

It takes a few minutes. It writes `receipts/receipt-lab3-<timestamp>.txt`
(upload that `.txt`, not the `.log` next to it). Before you upload, read the
numbers below in your receipt: each one is a claim of the book, measured on
your machine.

`0.1.2` has the same software as the image Labs 0–2 used (Python, PySpark
3.5.4 with its Scala runtime, Java 17), built natively for both Intel/AMD and
Apple-Silicon machines, so on a Mac it does not run under emulation.

## What to look at before uploading

**The stream.** One million items drawn from a skewed (Zipf) distribution — a
few items are very frequent, most are rare — split into 8 *shards* of
**unequal** length (the shard lengths are in the first `MEASURED` line of your
receipt), the way a real stream is split by time or by source. The reservoirs
and the Count-Min tables are built on each shard separately and then merged:
the pattern of U4 §7.1.

1. **A uniform sample — U4 §3 and §6.1.** Each shard keeps a *reservoir* of
   k = 1000 items with Vitter's algorithm; read `reservoir` in
   `streaming_sketches.py` next to the pseudocode of U4 §3.2. The eight
   reservoirs are then merged in two ways, and each merged sample is checked
   for being evenly spread over the ten tenths of the stream (a chi-square
   test; `per_tenth_of_stream` should be about 100 each):
   - the **exact merge** of U4 §6.1 (the hypergeometric split): its `chi2` is
     small;
   - the **naive merge** (pool every reservoir and pick k): its `chi2` is
     large, and the first tenths of the stream get far more than 100. Same
     shards, same reservoirs; only the merge differs. Question 1 below asks why.
   - the **sort-based sample** `orderBy(rand()).limit(k)` (U4 §7.3): give every
     item a random key and keep the k smallest. It is evenly spread too (small
     `chi2`). Now look at the `shuffle_bytes` line: the bytes Spark moved
     between partitions for this sample when it is collected to the driver,
     when it is written out, and for a **full** sort of the stream. Question 2
     below asks why they differ so much. (The `_seconds` lines differ less than
     the bytes: on one laptop every one of these jobs still reads all n rows
     once; the shuffle is what costs on a real network.)
2. **Frequencies in a small table — U4 §4.** The Count-Min sketch with
   ε = 0.001 and δ = 0.01 has w = ⌈e/ε⌉ = 2719 columns and d = ⌈ln(1/δ)⌉ = 5
   rows (U4 §4.2), so `cells=13595` — against `distinct_items`, the size of
   the exact table. Check the two claims of U4 §4.3:
   - `never_undercounts=True` — a certainty, proved in two lines;
   - each item is overcounted by more than εn with probability at most δ: the
     check is `fraction_of_items_over_eps_n` against δ = 0.01. The maximum
     overcount over all items (`max_overcount_over_f1`) is printed as an
     observation; no theorem bounds the maximum.

   Then U4 §4.4: every true heavy hitter is reported (`all_true_reported=True`),
   at the price of a few reported items that are not heavy
   (`share_of_reported_not_heavy`). The lab asks the sketch about every item
   that occurs, taken from the exact table, so that it can check the answers;
   a streaming system without that table keeps a small heap of candidates
   instead (U4 §4.4). The `top100` line compares the 100 most frequent items
   with their estimates. The eight shard tables are merged by **adding them**
   (U4 §6.2).
3. **Distinct items — U4 §5.** Spark's `approx_count_distinct` is HyperLogLog.
   First on the stream itself (`hll_on_the_stream`, against the exact
   `distinct_items`). Then on synthetic streams of n items with exactly
   D = 10³, 10⁴, 10⁵ and 10⁶ distinct values (x = i mod D, so the truth is
   known), at four requested accuracies `rsd` (relative standard deviation: the
   error you ask for, as a fraction of the true count; end of U4 §5.3). Each
   line shows the number of registers m that Spark's rule gives for that `rsd`,
   the book's standard error 1.04/√m (U4 §5.3) and the measured
   `relative_error`. A standard error is a typical size, not a bound: expect
   some errors above it and most below. With m = 16384 registers (a few
   kilobytes) the standard error is 0.8%; in this run the million distinct
   values are counted to within 0.4%.
4. **The same sketch from Spark's library, in Scala — U4 §7.3.** After the
   Python part, `run-lab.sh` runs `countmin_library.scala` with `spark-shell`
   on the **same** stream. It calls Spark's built-in
   `df.stat.countMinSketch(...)` with the same ε and δ. Compare the
   `scala_countmin` lines with Part 2: Spark sizes its sketch 2000 × 7, the book
   2719 × 5 — two analyses of the same guarantee (Spark uses w = ⌈2/ε⌉ and
   d = ⌈log₂(1/δ)⌉: a failure chance of ½ per row instead of 1/e). Both never
   undercount, and both find every heavy hitter.

`MEASURED verified=True` and `MEASURED scala_verified=true` are the pass lines.
They check the claims that must hold: the merged sample has k distinct stream
positions and passes the test, the naive merge fails it, the sort-based sample
passes it, Count-Min never undercounts (in Python and in Scala), every heavy
hitter is found, and every HyperLogLog estimate is within five standard errors.

The numbers are the same on every machine for the default run (the stream and
every random choice are seeded); only the timings differ.

Optional, a bigger stream: `bash run-lab.sh 3000000 8`. The reservoirs, the
Count-Min table and the HyperLogLog registers keep their size while the stream
triples. It stays inside the same 3 GB container limit. Upload the
default-size receipt.

## What happens under the hood

**PySpark and Scala.** Spark is written in Scala and runs on the Java virtual
machine (JVM). PySpark is a Python front end: your Python program talks to the
same JVM engine through a bridge (Py4J). A DataFrame query — `orderBy`,
`groupBy`, `approx_count_distinct` — is planned and executed inside the JVM, so
it runs the same whether you wrote it in Python or in Scala, once its rows are
in the JVM. (In this lab the stream DataFrame is built from a Python RDD, so
every query on it first converts the rows from Python; that, and the warm-up of
the first job, is why the `_seconds` lines do not follow the shuffle bytes.)
Python functions
you hand to Spark (the `lambda`s and `mapPartitions` functions of Parts 1 and 2)
run in separate Python worker processes, and every row they see is serialised
from the JVM to Python and back; that is the price of writing the sketch
yourself in Python. A few APIs exist only in Scala and Java — the queryable
Count-Min sketch of Part 4 is one (U4 §7.3).

**Parquet.** The Python part writes the stream to `/tmp/lab/stream.parquet`
inside the container, and the Scala part reads it back. Parquet is a
*columnar* file format: the values of each column are stored together,
compressed, with the schema (column names and types) and per-chunk statistics
(minimum, maximum, count) in the file. A reader that needs one column reads
only that column, and can skip chunks whose statistics rule them out. Spark
writes a *directory*, not a file: one part-file per partition, written in
parallel — the same partition-by-partition pattern as the sketches.

**Plans, stages and shuffles.** Spark turns a query into a *physical plan* of
operators; `df.explain()` prints it. Wherever rows must move between
partitions — to bring equal keys together (`groupBy`, `reduceByKey`) or to put
rows in a global order (`orderBy`) — the plan has an *Exchange*: a shuffle.
Each partition writes its outgoing rows to local disk, and the next stage reads
them over the network; the stage boundary is the shuffle. The `shuffle_bytes`
line reads those bytes from Spark's own task metrics (the numbers the Spark UI
shows), and is the ground truth: one operator, `TakeOrderedAndProject`, does
its small single-partition shuffle internally when its result is written out,
with no Exchange in the plan. A per-partition summary merged by `treeReduce`,
like the Count-Min tables of Part 2, moves summaries, not rows.

**The runner.** `run-lab.sh` sends the two programs into the container as a
`tar` stream on standard input, unpacks them in `/tmp/lab`, runs the Python
part and then the Scala part, and writes the log, the receipt and its SHA-256
on your side. No folder of yours is mounted, so the container writes nothing
into your lab folder and your user id does not matter.

## Submit

Upload the receipt file (`receipts/receipt-lab3-<timestamp>.txt`) on your
course page, and answer two questions in the **comment box** of the upload
page (do not edit the receipt: its hash would no longer match), one or two
sentences each:

1. **Predict, from the shard lengths in the first `MEASURED` line and U4
   §3.3, about how many of the naive merge's k = 1000 items should fall in the
   first tenth of the stream. Compare with your receipt, and say in one
   sentence why that tenth is over-represented.**
2. **Why does `orderBy(rand()).limit(k)` move only about a hundred kilobytes
   (or nothing, when collected), while a full `orderBy(rand())` shuffles the
   whole stream?** (Hint: where can the k smallest keys of the whole stream
   be?)

The full procedure — the same for every lab — is
**[How to submit](../README.md#how-to-submit)**.
