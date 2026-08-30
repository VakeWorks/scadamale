# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
"""Lab 2 -- sample sort on Spark (local[*] standalone).

Implements sample sort exactly as unit U2 describes it:
  1. SAMPLE  a small set of keys from the input;
  2. pick p-1 SPLITTERS from the sorted sample (equally spaced quantiles);
  3. RANGE-PARTITION every record to the bucket its key falls in (the only
     all-to-all shuffle);
  4. SORT each bucket locally;
  5. concatenating buckets 0..p-1 is then globally sorted.

Then verifies correctness two ways (global order + content checksum) and
compares against Spark's built-in sortBy. Prints MEASURED lines the receipt
captures.

Run inside the course container (run-lab.sh does this for you):
    python3 sample_sort.py [N] [P]
Defaults: N=2_000_000 64-bit keys, P=8 buckets.
"""

import bisect
import random
import sys
import time

from pyspark.sql import SparkSession


def timed(label, f):
    t0 = time.time()
    out = f()
    dt = time.time() - t0
    print("MEASURED %s_seconds=%.3f" % (label, dt))
    return out, dt


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2_000_000
    p = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    oversample = 32  # sample p*oversample keys; U2 explains why oversampling

    spark = (
        SparkSession.builder.appName("scadamale-lab2-sample-sort")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        # absolute ivy dir: the container runs as YOUR uid (no passwd entry),
        # so the JVM's user.home is unusable -- pin it to /tmp explicitly
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("MEASURED n=%d p=%d sample_size=%d" % (n, p, p * oversample))

    # input: n pseudorandom 64-bit keys, reproducible seed
    seed = 20260828
    rdd = sc.parallelize(range(n), p).map(
        lambda i: random.Random(seed + i).getrandbits(63)
    ).cache()
    checksum_in = rdd.map(lambda x: x % 1_000_003).sum()
    count_in = rdd.count()  # materialise before timing the sorts

    # ---- sample sort ----
    def sample_sort():
        sample = rdd.takeSample(False, p * oversample, seed)
        sample.sort()
        splitters = [sample[(i + 1) * len(sample) // p] for i in range(p - 1)]

        def bucket_of(key):
            return bisect.bisect_right(splitters, key)

        return (
            rdd.map(lambda x: (bucket_of(x), x))
            .partitionBy(p, lambda b: b)          # THE one shuffle
            .mapPartitions(lambda it: iter(sorted(v for _, v in it)))
        ).cache()

    sorted_rdd, t_sample_sort = timed("sample_sort", lambda: (lambda r: (r.count(), r)[1])(sample_sort()))

    # ---- verify: global order across bucket boundaries + content preserved ----
    def part_minmax(idx, it):
        lo = hi = None
        cnt = 0
        prev = None
        ok = True
        for v in it:
            if prev is not None and v < prev:
                ok = False
            prev = v
            if lo is None:
                lo = v
            hi = v
            cnt += 1
        yield (idx, lo, hi, cnt, ok)

    stats = sorted(sorted_rdd.mapPartitionsWithIndex(part_minmax).collect())
    locally_sorted = all(s[4] for s in stats)
    boundaries_ok = all(
        stats[i][2] is None or stats[i + 1][1] is None or stats[i][2] <= stats[i + 1][1]
        for i in range(len(stats) - 1)
    )
    count_out = sum(s[3] for s in stats)
    checksum_out = sorted_rdd.map(lambda x: x % 1_000_003).sum()
    verified = locally_sorted and boundaries_ok and count_out == count_in and checksum_out == checksum_in
    print("MEASURED verified=%s (order=%s boundaries=%s count=%s checksum=%s)"
          % (verified, locally_sorted, boundaries_ok, count_out == count_in,
             checksum_out == checksum_in))
    bucket_sizes = [s[3] for s in stats]
    print("MEASURED bucket_sizes min=%d max=%d (perfect=%d) balance=%.2f"
          % (min(bucket_sizes), max(bucket_sizes), n // p,
             max(bucket_sizes) / (n / p)))

    # ---- baseline: Spark's own sortBy ----
    _, t_sortby = timed("spark_sortby", lambda: rdd.sortBy(lambda x: x, numPartitions=p).count())

    print("MEASURED speed_ratio_sortby_over_samplesort=%.2f" % (t_sortby / t_sample_sort))
    print("LOOK-AT-THIS: one shuffle moved all %d records; the splitters came"
          " from a sample of only %d keys -- U2 sec 4 explains why the buckets"
          " still balance (your balance factor above)." % (n, p * oversample))

    spark.stop()
    if not verified:
        sys.exit(1)


if __name__ == "__main__":
    main()
