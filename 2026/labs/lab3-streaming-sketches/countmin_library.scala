// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 VakeWorks AB
//
// Lab 3, Part 4 -- Spark's OWN Count-Min sketch, from Scala, on the SAME stream that Part 2 built a
// sketch for by hand in Python. Spark is written in Scala: this is its native API, and the method
// df.stat.countMinSketch, which returns a sketch you can query, exists only in Scala and Java.
// run-lab.sh runs this file with spark-shell inside the course container, after the Python part.
import org.apache.spark.sql.functions._

val eps = 0.001                 // the same accuracy as Part 2
val confidence = 0.99           // = 1 - delta, with delta = 0.01 as in Part 2
val phi = 2 * eps               // heavy-hitter threshold, U4 §4.4

// The stream Part 1 wrote: a Parquet directory, one file per partition, column "x" = the item.
val df = spark.read.parquet("/tmp/lab/stream.parquet")
val n = df.count()

// One pass per partition, merged into one sketch: Spark's org.apache.spark.util.sketch.CountMinSketch.
val cms = df.stat.countMinSketch("x", eps, confidence, 20261022)

// The exact table (a shuffle: groupBy brings equal items together) to check the sketch against.
val exact = df.groupBy("x").count().collect().map(r => (r.getLong(0), r.getLong(1)))
val over = exact.map { case (x, c) => cms.estimateCount(x) - c }
val heavyTrue = exact.filter(_._2 >= phi * n).map(_._1).toSet
val heavyReported = exact.filter { case (x, _) => cms.estimateCount(x) >= phi * n }.map(_._1).toSet

// Sizes: Spark's choice of shape against the book's (U4 §4.2), for the same eps and delta.
val bookW = math.ceil(math.E / eps).toInt
val bookD = math.ceil(math.log(1 / (1 - confidence))).toInt
val neverUnder = over.forall(_ >= 0)
val fracOver = over.count(_ > eps * n).toDouble / over.length
val allFound = heavyTrue.subsetOf(heavyReported)

println(f"MEASURED scala_countmin spark_width=${cms.width} spark_depth=${cms.depth} spark_cells=${cms.width * cms.depth} book_width=$bookW book_depth=$bookD book_cells=${bookW * bookD}")
println(f"MEASURED scala_countmin never_undercounts=$neverUnder fraction_of_items_over_eps_n=$fracOver%.4f max_overcount_over_f1=${over.max.toDouble / n}%.5f heavy_true=${heavyTrue.size} heavy_reported=${heavyReported.size} all_true_reported=$allFound")
// The checks that must hold (certainties, as on the Python side): never undercounts, every heavy
// hitter found. The fraction over eps*n is a probability statement, printed above, not a pass condition.
val scalaOk = neverUnder && allFound
println(s"MEASURED scala_verified=$scalaOk")
System.exit(if (scalaOk) 0 else 1)
