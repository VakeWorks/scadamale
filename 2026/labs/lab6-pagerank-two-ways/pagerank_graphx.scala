// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 VakeWorks AB
//
// Lab 6, Part 3 -- the same PageRank with GraphX, Spark's graph library, in Scala (GraphX has no
// Python API). Unit U12 §6.4: GraphX stores a graph as an edge table plus a vertex table and runs
// "think like a vertex" programs as joins and aggregations over them (a vertex-cut layout).
// run-lab.sh runs this with spark-shell after pagerank_rdd.py, on the graph that file wrote.
import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.graphx.{Graph, PartitionStrategy}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

val iterations = 10
val resetProb = 0.15                         // teleport probability, as in Parts 1-2
val edgePartitions = 8                       // = P of Parts 1-2, the same on every machine

object Meter { val written = new AtomicLong(0) }
sc.addSparkListener(new SparkListener {
  override def onTaskEnd(e: SparkListenerTaskEnd): Unit =
    if (e.taskMetrics != null) Meter.written.addAndGet(e.taskMetrics.shuffleWriteMetrics.bytesWritten)
})
def settle(): Unit = { var last = -1L; while (last != Meter.written.get) { last = Meter.written.get; Thread.sleep(300) } }

// The graph Part 1 wrote: one row per directed edge (src, dst).
val edges = spark.read.parquet("/tmp/lab/edges.parquet").rdd.map(r => (r.getLong(0), r.getLong(1)))
val graph = Graph.fromEdgeTuples(edges, 1).partitionBy(PartitionStrategy.EdgePartition2D, edgePartitions).cache()
println(s"MEASURED graphx vertices=${graph.vertices.count()} edges=${graph.edges.count()} edge_partitions=${graph.edges.getNumPartitions}")

// staticPageRank first computes every vertex's out-degree and ships it to the edges (one-time
// setup), then iterates. Measure a 1-iteration run and a 10-iteration run: the difference is
// 9 iterations' worth of bytes, without the setup.
settle(); Meter.written.set(0)
graph.staticPageRank(1, resetProb).vertices.count()
settle()
val oneIter = Meter.written.get
Meter.written.set(0)
val t0 = System.nanoTime
val ranks = graph.staticPageRank(iterations, resetProb).vertices.cache()
ranks.count()
val secs = (System.nanoTime - t0) / 1e9
settle()
val allIters = Meter.written.get
val perIterMB = (allIters - oneIter) / (iterations - 1) / 1e6
val setupMB = oneIter / 1e6 - perIterMB

// Compare with Parts 1-2. GraphX keeps every vertex that has an edge (each gets the teleport term)
// and rescales the ranks to sum to the number of vertices, so compare which vertices come out on top.
val top = ranks.top(100)(Ordering.by(_._2)).map(_._1)
val rddTop = scala.io.Source.fromFile("/tmp/lab/top100_rdd.txt").mkString.trim.split(" ").map(_.toLong)
val overlap10 = top.take(10).toSet.intersect(rddTop.take(10).toSet).size
val overlap100 = top.toSet.intersect(rddTop.toSet).size
println(f"MEASURED graphx pagerank iterations=$iterations seconds=$secs%.1f shuffle_MB_per_iteration=$perIterMB%.2f setup_MB=$setupMB%.2f")
println(s"MEASURED graphx top10_vertices=${top.take(10).mkString(" ")} same_top10_as_rdd=$overlap10/10 same_top100_as_rdd=$overlap100/100")
val ok = overlap10 >= 9 && overlap100 >= 95
println(s"MEASURED graphx_verified=$ok")
System.exit(if (ok) 0 else 1)
