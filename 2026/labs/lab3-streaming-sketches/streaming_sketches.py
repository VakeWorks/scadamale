# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
"""Lab 3 -- streaming sketches (PySpark, local[*]).

One stream, three kinds of summary, each straight out of unit U4:

  the stream   n items from a skewed (Zipf) distribution, split into P shards of
               UNEQUAL length (shard p has length proportional to p+1), the way a
               real stream is split by time or by source;
  Part 1       a uniform sample of k items: Vitter's reservoir (U4 §3) on every
               shard, then the shards' reservoirs merged with the EXACT
               hypergeometric split (U4 §6.1) -- and, for contrast, with the naive
               union that §6.1 calls wrong; then the sort-based sample
               orderBy(rand()).limit(k) (U4 §7.3), with the shuffle bytes Spark
               moves for it and for a full sort;
  Part 2       item frequencies in d*w counters: a Count-Min sketch (U4 §4) built
               by hand on every shard, merged by adding the tables (U4 §6.2);
               heavy hitters and the top-100 items;
  Part 3       the number of distinct items: Spark's HyperLogLog
               (approx_count_distinct, U4 §5), on the stream and then on
               synthetic streams with a known number D of distinct values;
  Part 4       (countmin_library.scala, run after this script by run-lab.sh) Spark's
               own Count-Min from Scala, on the same stream, read back from Parquet.

The reservoirs and Count-Min tables are built per shard and then merged: the
per-partition-summary + one-aggregate pattern of U4 §7.1. Prints MEASURED lines
the receipt captures.

Run inside the course container (run-lab.sh does this for you):
    python3 streaming_sketches.py [N] [P]
Defaults: N=1_000_000 items, P=8 shards.
"""

import json
import math
import random
import sys
import time
import urllib.error
import urllib.request

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType

SEED = 20261022
K = 1000                  # reservoir size (Part 1)
EPS, DELTA = 0.001, 0.01  # Count-Min accuracy and failure probability (Part 2)
ZIPF_A = 1.2              # stream skew: a few items are very frequent
PRIME = 2**31 - 1         # Mersenne prime for the hash family ((a*x + b) mod p) mod w
CHI2_CRIT_DF9 = 27.877    # chi-square, 9 degrees of freedom, upper 0.1% point
STREAM_PARQUET = "/tmp/lab/stream.parquet"   # the stream, for Part 4 (countmin_library.scala)


def shard_lengths(n, p):
    """Shard i gets a length proportional to i+1: unequal on purpose (U4 §6.1)."""
    weights = [i + 1 for i in range(p)]
    total = sum(weights)
    lengths = [n * w // total for w in weights]
    lengths[-1] += n - sum(lengths)
    return lengths


def generate_shard(idx, lengths):
    """Shard idx of the stream: (global position, item) pairs, reproducible."""
    start = sum(lengths[:idx])
    rng = np.random.default_rng(SEED + idx)
    items = rng.zipf(ZIPF_A, size=lengths[idx]).tolist()
    return [(start + i, x) for i, x in enumerate(items)]


# ---------------- Part 1: reservoir sampling (U4 §3) + merges (U4 §6.1) ----------------

def reservoir(shard, k, seed):
    """Vitter's Algorithm R (U4 §3.2): one pass, exactly k items of memory."""
    rnd = random.Random(seed)
    res = []
    for i, item in enumerate(shard, start=1):
        if i <= k:
            res.append(item)
        else:
            j = rnd.randint(1, i)       # uniform in [1, i]
            if j <= k:
                res[j - 1] = item       # replace a uniformly chosen slot
    return res


def merge_exact(a, b, k, rng):
    """U4 §6.1: K1 ~ Hypergeometric(n1, n2, k), then K1 items of R1 and k-K1 of R2."""
    (n1, r1), (n2, r2) = a, b
    k1 = int(rng.hypergeometric(n1, n2, min(k, n1 + n2)))
    take1 = [r1[i] for i in rng.choice(len(r1), size=k1, replace=False)]
    take2 = [r2[i] for i in rng.choice(len(r2), size=min(k, n1 + n2) - k1, replace=False)]
    return (n1 + n2, take1 + take2)


def merge_naive(reservoirs, k, rng):
    """The WRONG merge of U4 §6.1: pool every reservoir, subsample k uniformly."""
    pool = [item for _, r in reservoirs for item in r]
    return [pool[i] for i in rng.choice(len(pool), size=k, replace=False)]


def chi2_positions(sample, n, buckets=10):
    """Chi-square of the sample's stream positions over equal-width buckets.
    A uniform sample of the STREAM spreads evenly over positions, whatever the
    item values are."""
    counts = [0] * buckets
    for pos, _ in sample:
        counts[pos * buckets // n] += 1
    expected = len(sample) / buckets
    return sum((c - expected) ** 2 / expected for c in counts), counts


# ---------------- Part 2: Count-Min sketch (U4 §4) ----------------

def cm_hashes(d):
    """d hash functions h_r(x) = ((a_r * x + b_r) mod p) mod w, fixed by the seed."""
    rng = np.random.default_rng(SEED)
    a = rng.integers(1, PRIME, size=d, dtype=np.int64)
    b = rng.integers(0, PRIME, size=d, dtype=np.int64)
    return a, b


def cm_columns(xs, a, b, w):
    """Column of every item in every row: shape (d, len(xs)). x mod p first, so
    a*x < 2^62 stays exact in int64."""
    x = np.asarray(xs, dtype=np.int64) % PRIME
    return ((a[:, None] * x[None, :] + b[:, None]) % PRIME) % w


def cm_build(items, a, b, w):
    """U4 §4.2 update: C[r][h_r(x)] += 1 for every row r."""
    cols = cm_columns(items, a, b, w)
    return np.stack([np.bincount(row, minlength=w) for row in cols]).astype(np.int64)


def cm_query(table, xs, a, b, w):
    """U4 §4.2 query: min over the rows of C[r][h_r(x)]."""
    cols = cm_columns(xs, a, b, w)
    return np.min(table[np.arange(table.shape[0])[:, None], cols], axis=0)


def timed(label, f):
    t0 = time.time()
    out = f()
    print("MEASURED %s_seconds=%.3f" % (label, time.time() - t0))
    return out


def shuffle_bytes_written(sc, group):
    """Bytes written to shuffle by every job of one job group: Spark's own task metrics, the
    numbers the Spark UI shows, read from the driver's monitoring REST API (/api/v1)."""
    base = "%s/api/v1/applications/%s/stages/" % (sc.uiWebUrl, sc.applicationId)
    tracker = sc.statusTracker()
    stages = []
    for j in tracker.getJobIdsForGroup(group):
        info = tracker.getJobInfo(j)
        stages += list(info.stageIds) if info else []
    total = 0
    for s in stages:
        attempts = []
        for _ in range(50):                      # the metrics arrive a moment after the job ends
            try:
                attempts = json.load(urllib.request.urlopen(base + str(s)))
            except urllib.error.HTTPError:       # a stage Spark skipped (its input was cached)
                attempts = []
                break
            if all(a["status"] in ("COMPLETE", "SKIPPED", "FAILED") for a in attempts):
                break
            time.sleep(0.1)
        total += sum(a.get("shuffleWriteBytes", 0) for a in attempts)
    return total


def measured(sc, label, f):
    """Run f as its own Spark job group; print its wall time; return (result, shuffle bytes)."""
    sc.setJobGroup(label, label)
    out = timed(label, f)
    return out, shuffle_bytes_written(sc, label)


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    p = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    lengths = shard_lengths(n, p)

    spark = (
        SparkSession.builder.appName("scadamale-lab3-streaming-sketches")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    stream = sc.parallelize(range(p), p).mapPartitionsWithIndex(
        lambda idx, _: generate_shard(idx, lengths)).cache()
    stream.count()
    print("MEASURED n=%d shards=%d shard_lengths=%s" % (n, p, lengths))

    # ---- Part 1: one reservoir per shard, then merge ----
    k = K
    local = timed("reservoir_per_shard", lambda: stream.mapPartitionsWithIndex(
        lambda idx, it: [(lengths[idx], reservoir(it, k, SEED + 100 + idx))]).collect())
    rng = np.random.default_rng(SEED + 1)
    level = local
    while len(level) > 1:                       # pairwise merge tree: O(log P) rounds
        nxt = [merge_exact(level[i], level[i + 1], k, rng) for i in range(0, len(level) - 1, 2)]
        if len(level) % 2:
            nxt.append(level[-1])
        level = nxt
    merged = level[0][1]
    naive = merge_naive(local, k, rng)
    chi2_exact, counts_exact = chi2_positions(merged, n)
    chi2_naive, counts_naive = chi2_positions(naive, n)
    sample_ok = len(merged) == k and len({pos for pos, _ in merged}) == k
    print("MEASURED reservoir k=%d memory_items_per_shard=%d" % (k, k))
    print("MEASURED reservoir_exact_merge chi2=%.1f (pass if < %.1f) per_tenth_of_stream=%s"
          % (chi2_exact, CHI2_CRIT_DF9, counts_exact))
    print("MEASURED reservoir_naive_merge chi2=%.1f per_tenth_of_stream=%s"
          % (chi2_naive, counts_naive))

    # ---- Part 1b (U4 §7.3, §9 experiment 3): the sort-based sample, orderBy(rand()).limit(k) ----
    # Measured three ways: collected to the driver, written out, and against a full sort of the
    # stream. Question 2 of the submission asks about these shuffle bytes.
    sdf = spark.createDataFrame(stream, StructType([StructField("pos", LongType(), False),
                                                    StructField("x", LongType(), False)]))
    sdf.write.mode("overwrite").parquet(STREAM_PARQUET)   # Part 4 (Scala) reads the same stream
    q_limit = sdf.orderBy(F.rand(SEED)).limit(k)
    by_sort, b_collected = measured(sc, "sort_based_sample", lambda: [(r.pos, r.x) for r in q_limit.collect()])
    _, b_written = measured(sc, "sort_based_sample_written",
                            lambda: q_limit.write.format("noop").mode("overwrite").save())
    _, b_full = measured(sc, "full_sort_of_stream",
                         lambda: sdf.orderBy(F.rand(SEED)).write.format("noop").mode("overwrite").save())
    plan_limit = q_limit._jdf.queryExecution().executedPlan().toString()
    chi2_sort, counts_sort = chi2_positions(by_sort, n)
    print("MEASURED sort_based_sample chi2=%.1f per_tenth_of_stream=%s plan=%s"
          % (chi2_sort, counts_sort,
             "TakeOrderedAndProject" if "TakeOrderedAndProject" in plan_limit else "other"))
    print("MEASURED shuffle_bytes sample_collected=%d sample_written=%d full_sort=%d "
          "(stream: %d rows in %d partitions; k=%d)"
          % (b_collected, b_written, b_full, n, sdf.rdd.getNumPartitions(), k))

    # ---- Part 2: one Count-Min sketch per shard, merged by adding the tables ----
    w = math.ceil(math.e / EPS)
    d = math.ceil(math.log(1 / DELTA))
    a, b = cm_hashes(d)
    table = timed("countmin_per_shard_and_merge", lambda: stream.mapPartitions(
        lambda it: [cm_build([x for _, x in it], a, b, w)]).treeReduce(lambda s, t: s + t))
    exact = timed("exact_counts_with_shuffle", lambda: stream.map(lambda px: (px[1], 1))
                  .reduceByKey(lambda u, v: u + v).collect())
    keys = [x for x, _ in exact]
    true_f = np.array([c for _, c in exact], dtype=np.int64)
    est_f = cm_query(table, keys, a, b, w)
    over = est_f - true_f
    never_under = bool(np.all(over >= 0))
    frac_above = float(np.mean(over > EPS * n))
    phi = 2 * EPS
    heavy_true = {x for x, c in exact if c >= phi * n}
    heavy_reported = {x for x, e in zip(keys, est_f) if e >= phi * n}
    below_half = {x for x, c in exact if c < (phi / 2) * n}
    print("MEASURED countmin w=%d d=%d cells=%d distinct_items=%d (exact table size)"
          % (w, d, w * d, len(keys)))
    # U4 §4.3: each item exceeds its count by more than eps*n with probability at most delta --
    # the theorem is about each item, so its check is the FRACTION of items over eps*n; the
    # maximum over all items is an observation, not a guarantee.
    print("MEASURED countmin never_undercounts=%s fraction_of_items_over_eps_n=%.4f "
          "(theorem: each item at most delta=%.2f) max_overcount=%d eps_times_n=%.0f "
          "max_overcount_over_f1=%.5f (an observation; no theorem bounds the max)"
          % (never_under, frac_above, DELTA, int(over.max()), EPS * n, over.max() / n))
    # Candidates are every item that occurs (taken from the exact table, to check the sketch);
    # a streaming system without the exact table keeps a candidate heap instead (U4 §4.4).
    not_heavy = len(heavy_reported - heavy_true) / max(1, len(heavy_reported))
    print("MEASURED heavy_hitters phi=%.3f true=%d reported=%d all_true_reported=%s "
          "share_of_reported_not_heavy=%.3f reported_below_phi_half=%d"
          % (phi, len(heavy_true), len(heavy_reported), heavy_true <= heavy_reported,
             not_heavy, len(heavy_reported & below_half)))
    order = np.argsort(-true_f)[:100]          # U4 §9: the top-100 items, estimate vs exact
    print("MEASURED countmin top100 max_overcount=%d mean_overcount=%.1f"
          % (int(over[order].max()), float(over[order].mean())))
    for i in order[:3]:
        print("MEASURED top_item item=%d true=%d countmin=%d" % (keys[i], true_f[i], est_f[i]))

    # ---- Part 3 (U4 §9, experiment 1): HyperLogLog error vs m, approx_count_distinct ----
    # First on the Zipf stream itself (the exact distinct count is the size of the exact table).
    est_stream = sdf.agg(F.approx_count_distinct("x", rsd=0.01)).first()[0]
    rel_stream = abs(est_stream - len(keys)) / len(keys)
    print("MEASURED hll_on_the_stream rsd=0.01 estimate=%d exact_distinct=%d relative_error=%.4f"
          % (est_stream, len(keys), rel_stream))
    # Then on synthetic streams of n items with exactly D distinct values (x = i mod D), so the
    # truth is known for every D. For each D, one Spark job estimates at every requested rsd.
    rsds = (0.20, 0.10, 0.05, 0.01)
    hll_ok = rel_stream < 5 * 1.04 / math.sqrt(16384)
    for D in (10**3, 10**4, 10**5, 10**6):
        if D > n:
            continue
        ddf = spark.range(n).select((F.col("id") % D).alias("x"))
        row = ddf.agg(*[F.approx_count_distinct("x", rsd=r) for r in rsds]).collect()[0]
        for i, rsd in enumerate(rsds):
            # Spark's rule (end of U4 §5.3, checked against Spark's own code): m is the smallest
            # power of two with 1.106/sqrt(m) <= rsd; the book's standard error is 1.04/sqrt(m)
            m = 2 ** math.ceil(2 * math.log2(1.106 / rsd))
            se = 1.04 / math.sqrt(m)
            rel = abs(row[i] - D) / D
            hll_ok = hll_ok and rel < 5 * se
            print("MEASURED hll D=%d rsd=%.2f registers_m=%d book_std_error=%.4f estimate=%d "
                  "relative_error=%.4f" % (D, rsd, m, se, row[i], rel))

    # ---- the checks that MUST hold (each is a theorem of U4, or the stated HLL error) ----
    #  * the exact merge gives exactly k distinct stream positions, and its positions pass the
    #    chi-square test at the 0.1% level (fixed seed): U4 §6.1 says the merge is EXACTLY uniform;
    #  * the naive merge FAILS that test (U4 §6.1 calls it the naive wrong approach; question 1
    #    asks why), and by a wide margin, so this is not a near miss;
    #  * Count-Min never undercounts (U4 §4.3, a certainty, not a probability), and every true
    #    heavy hitter is reported (U4 §4.4, a consequence of the same certainty);
    #  * the sort-based sample (orderBy(rand()).limit(k)) is uniform too: random keys, k smallest;
    #  * every HLL estimate is within 5 book standard errors 1.04/sqrt(m) (generous on purpose).
    sort_uniform = chi2_sort < CHI2_CRIT_DF9 and len(by_sort) == k
    verified = (sample_ok and chi2_exact < CHI2_CRIT_DF9 and chi2_naive > CHI2_CRIT_DF9
                and sort_uniform and never_under and heavy_true <= heavy_reported and hll_ok)
    print("MEASURED verified=%s (exact_sample=%s exact_uniform=%s naive_biased=%s "
          "sort_sample_uniform=%s countmin_never_under=%s heavy_all_found=%s hll_within_5se=%s)"
          % (verified, sample_ok, chi2_exact < CHI2_CRIT_DF9, chi2_naive > CHI2_CRIT_DF9,
             sort_uniform, never_under, heavy_true <= heavy_reported, hll_ok))
    print("LOOK-AT-THIS: %d counters (Count-Min) summarised %d items with %d distinct values,"
          " and 16384 small registers counted a million distinct values (HLL at rsd 0.01)"
          " -- U4 §1: memory is a resource too." % (w * d, n, len(keys)))
    print("LOOK-AT-THIS: compare the reservoir chi2 values: same shards, same reservoirs,"
          " only the merge differs (question 1); and the three shuffle_bytes (question 2).")

    spark.stop()
    if not verified:
        sys.exit(1)


if __name__ == "__main__":
    main()
