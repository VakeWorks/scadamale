# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
"""Lab 6, Parts 1-2 -- PageRank on Spark RDDs, two ways (PySpark, local[*]).

  the graph   n vertices, m directed edges with power-law degrees (a few hubs, many small
              vertices), generated reproducibly; written to Parquet for Part 3 (GraphX, Scala);
  Part 1      the NAIVE version of U12 §4.1's loop: the link table's partitioning is lost (map),
              so Spark cannot know that links and ranks are already lined up;
  Part 2      the CO-PARTITIONED version of U12 §4.2: links hash-partitioned once and persisted,
              ranks updated with mapValues, so both keep the same partitioner.
Both run the same iterations from the same start and must give the same ranks. Every iteration
runs as two Spark jobs -- the join, then the contributions -- and the lab reads the shuffle bytes
of each from Spark's own task metrics.

Run inside the course container (run-lab.sh does this):  python3 pagerank_rdd.py
"""

import json
import sys
import time
import urllib.error
import urllib.request
from operator import add

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructField, StructType

SEED = 20261022
N, M, P = 100_000, 1_000_000, 8      # vertices, edges drawn (before removing duplicates), partitions
ITERS = 10
FOLLOW = 0.85                         # follow a link w.p. 0.85, teleport w.p. 0.15 (U12 §2.1's alpha)
OUT = "/tmp/lab"


def shuffle_bytes_written(sc, group):
    """Bytes written to shuffle by the jobs of one job group (Spark's task metrics, via /api/v1)."""
    base = "%s/api/v1/applications/%s/stages/" % (sc.uiWebUrl, sc.applicationId)
    tracker = sc.statusTracker()
    stages = []
    for j in tracker.getJobIdsForGroup(group):
        info = tracker.getJobInfo(j)
        stages += list(info.stageIds) if info else []
    total = 0
    for s in stages:
        attempts = []
        for _ in range(50):
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


def edges_of(p):
    """Partition p's edges: sources and targets drawn with power-law weights (Chung-Lu style)."""
    w = 1.0 / np.arange(1, N + 1) ** 0.8
    w /= w.sum()
    r = np.random.default_rng(SEED + p)
    src = r.choice(N, size=M // P, p=w)
    dst = r.choice(N, size=M // P, p=w)
    keep = src != dst                                 # no self-loops
    return list(zip(src[keep].tolist(), dst[keep].tolist()))


def contributions(kv):
    _, (nbrs, rank) = kv
    share = rank / len(nbrs)
    return [(v, share) for v in nbrs]


def run(sc, edges, co_partitioned):
    label = "copartitioned" if co_partitioned else "naive"
    if co_partitioned:
        # U12 §4.2: hash-partition the link table ONCE and keep it; mapValues keeps the partitioner.
        links = edges.groupByKey(P).mapValues(list).persist()
        ranks = links.mapValues(lambda _: 1.0 / N)
    else:
        # The same table, but built with map(): Spark can no longer know that its keys are still
        # where groupByKey put them, so it forgets the partitioner.
        links = edges.groupByKey(P).map(lambda kv: (kv[0], list(kv[1]))).persist()
        ranks = links.map(lambda kv: (kv[0], 1.0 / N))
    sc.setJobGroup(label + "_setup", label)
    links.count()
    ranks = ranks.persist()
    ranks.count()
    join_bytes, contrib_bytes, secs = [], [], 0.0
    for i in range(ITERS):
        group = "%s_%d" % (label, i)
        # job 1: the join (links with ranks, into P partitions)
        sc.setJobGroup(group + "_join", group)
        t0 = time.time()
        joined = links.join(ranks, P).persist()
        joined.count()
        secs += time.time() - t0
        join_bytes.append(shuffle_bytes_written(sc, group + "_join"))
        # job 2: the contributions, summed per destination vertex
        sc.setJobGroup(group + "_contrib", group)
        t0 = time.time()
        summed = joined.flatMap(contributions).reduceByKey(add, P)
        if co_partitioned:
            new = summed.mapValues(lambda s: (1 - FOLLOW) / N + FOLLOW * s)
        else:
            new = summed.map(lambda kv: (kv[0], (1 - FOLLOW) / N + FOLLOW * kv[1]))
        new = new.persist()
        new.count()
        secs += time.time() - t0
        contrib_bytes.append(shuffle_bytes_written(sc, group + "_contrib"))
        joined.unpersist()
        ranks.unpersist()
        ranks = new
    final = dict(ranks.collect())
    links.unpersist()
    return join_bytes, contrib_bytes, secs, final


def mb(xs):
    return " ".join("%.2f" % (b / 1e6) for b in xs)


def main():
    spark = (SparkSession.builder.appName("scadamale-lab6-pagerank").master("local[*]")
             .config("spark.driver.memory", "2g").config("spark.jars.ivy", "/tmp/.ivy2")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    edges = sc.parallelize(range(P), P).flatMap(edges_of).distinct(P).persist()
    m = edges.count()
    with_out = edges.keys().distinct().count()
    with_any = edges.flatMap(lambda e: e).distinct().count()
    spark.createDataFrame(edges, StructType([StructField("src", LongType(), False),
                                             StructField("dst", LongType(), False)])) \
        .write.mode("overwrite").parquet(OUT + "/edges.parquet")
    print("MEASURED graph vertices=%d edges=%d vertices_with_edges=%d vertices_with_out_links=%d "
          "iterations=%d partitions=%d cpus=%d" % (N, m, with_any, with_out, ITERS, P, sc.defaultParallelism))

    results = {}
    for co in (False, True):
        jb, cb, secs, ranks = run(sc, edges, co_partitioned=co)
        name = "copartitioned" if co else "naive"
        results[name] = (jb, cb, ranks)
        print("MEASURED %s join_shuffle_MB_per_iteration=%s" % (name, mb(jb)))
        print("MEASURED %s contrib_shuffle_MB_per_iteration=%s" % (name, mb(cb)))
        print("MEASURED %s total_MB=%.1f seconds=%.1f" % (name, (sum(jb) + sum(cb)) / 1e6, secs))

    r_naive, r_copart = results["naive"][2], results["copartitioned"][2]
    keys = set(r_naive) | set(r_copart)
    diff = max(abs(r_naive.get(k, 0.0) - r_copart.get(k, 0.0)) for k in keys)
    ranked = sorted(r_copart.items(), key=lambda kv: -kv[1])
    with open(OUT + "/top100_rdd.txt", "w") as f:
        f.write(" ".join(str(k) for k, _ in ranked[:100]))
    print("MEASURED same_ranks max_abs_difference=%.1e ranked_vertices=%d rank_sum=%.3f"
          % (diff, len(r_copart), sum(r_copart.values())))
    print("MEASURED top10_vertices=%s top10_ranks=%s"
          % (" ".join(str(k) for k, _ in ranked[:10]), " ".join("%.5f" % v for _, v in ranked[:10])))

    same = diff < 1e-12
    narrow = sum(results["copartitioned"][0]) == 0 and sum(results["naive"][0]) > 0
    verified = same and narrow
    print("MEASURED verified=%s (same_ranks=%s copartitioned_join_moves_nothing=%s)" % (verified, same, narrow))
    spark.stop()
    if not verified:
        sys.exit(1)


if __name__ == "__main__":
    main()
