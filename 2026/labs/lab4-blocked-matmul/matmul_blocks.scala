// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 VakeWorks AB
//
// Lab 4 -- blocked matrix multiplication on Spark, in Scala (Spark's own language).
// Unit U6 §4.4: Spark's BlockMatrix.multiply moves BLOCKS between partitions (a shuffle), multiplies
// them locally, and sums the partial blocks (a second shuffle). This lab measures those bytes.
//
//   Part 1  dense n x n matrices, the same product with four block sizes: shuffle bytes, stages, time;
//           every product checked with Freivalds' randomized test (C x = A (B x) for a random x).
//   Part 2  sparse matrices, the same product two ways: as blocks (BlockMatrix) and as a join of
//           (row, column, value) entries (the sparse pattern of U6 §4.4 and U5): shuffle bytes vs density.
//
// run-lab.sh runs this file with spark-shell inside the course container; it prints MEASURED lines.
import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrix}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}

val SEED = 20261022L

// ---- a meter: Spark reports every finished task's shuffle bytes to listeners; we add them up ----
object Meter {
  val written = new AtomicLong(0)
  val read = new AtomicLong(0)
  val stages = new AtomicLong(0)
  def reset(): Unit = { written.set(0); read.set(0); stages.set(0) }
}
sc.addSparkListener(new SparkListener {
  override def onTaskEnd(e: SparkListenerTaskEnd): Unit = if (e.taskMetrics != null) {
    Meter.written.addAndGet(e.taskMetrics.shuffleWriteMetrics.bytesWritten)
    Meter.read.addAndGet(e.taskMetrics.shuffleReadMetrics.totalBytesRead)
  }
  override def onStageCompleted(e: SparkListenerStageCompleted): Unit = Meter.stages.incrementAndGet()
})
// The listener hears about a task a moment after it ends: wait until the counters stop moving.
def settle(): Unit = {
  var last = -1L
  while (last != Meter.stages.get + Meter.written.get) {
    last = Meter.stages.get + Meter.written.get
    Thread.sleep(300)
  }
}
// Run one action and return (result, seconds, shuffle bytes written, shuffle bytes read, stages).
def measure[T](body: => T): (T, Double, Long, Long, Long) = {
  settle(); Meter.reset()
  val t0 = System.nanoTime
  val out = body
  val secs = (System.nanoTime - t0) / 1e9
  settle()
  (out, secs, Meter.written.get, Meter.read.get, Meter.stages.get)
}

// ---- random matrices, reproducible ----
def denseBlocks(n: Int, bs: Int, seed: Long): BlockMatrix = {
  val nb = n / bs
  val ids = for (i <- 0 until nb; j <- 0 until nb) yield (i, j)
  val blocks = sc.parallelize(ids, nb * nb min 64).map { case (i, j) =>
    ((i, j), DenseMatrix.rand(bs, bs, new java.util.Random(seed + 1000L * i + j)): Matrix)
  }
  new BlockMatrix(blocks, bs, bs, n.toLong, n.toLong).cache()
}
def sparseEntries(n: Int, rho: Double, seed: Long) =
  sc.parallelize(0 until n, 16).flatMap { i =>
    val r = new java.util.Random(seed + i)
    (0 until n).flatMap(j => if (r.nextDouble() < rho) Some(MatrixEntry(i, j, r.nextDouble())) else None)
  }.cache()
def vector(n: Int, bs: Int, seed: Long): BlockMatrix = {
  val r = new java.util.Random(seed)
  val x = Array.fill(n)(r.nextDouble())
  val blocks = sc.parallelize(0 until n / bs, 8).map(i =>
    ((i, 0), new DenseMatrix(bs, 1, x.slice(i * bs, (i + 1) * bs)): Matrix))
  new BlockMatrix(blocks, bs, 1, n.toLong, 1L)
}
def column(m: BlockMatrix): Array[Double] =
  m.toLocalMatrix().toArray

// Warm-up, not timed: the first Spark job of a session also pays for starting the executors,
// loading classes and compiling hot code; one small product first makes the timed ones comparable.
{ val w = denseBlocks(512, 256, SEED + 3); w.blocks.count(); w.multiply(w).blocks.count(); w.blocks.unpersist() }

// ================= Part 1: dense, block-size sweep (U6 §4.1, §4.4) =================
val n = 2048
val matrixBytes = 8.0 * n * n                    // one dense n x n matrix of doubles
println(f"MEASURED dense n=$n matrix_MB=${matrixBytes / 1e6}%.1f flops_per_product=${2.0 * n * n * n / 1e9}%.1fe9")
var freivaldsOk = true
for (bs <- Seq(128, 256, 512, 1024)) {
  val A = denseBlocks(n, bs, SEED)
  val B = denseBlocks(n, bs, SEED + 7)
  A.blocks.count(); B.blocks.count()             // build (and cache) the inputs before measuring
  val (c, secs, wr, rd, st) = measure { val C = A.multiply(B).cache(); C.blocks.count(); C }
  // Freivalds: for a random x, C x must equal A (B x); an error in C shows up with high probability.
  val x = vector(n, bs, SEED + 99)
  val lhs = column(c.multiply(x))
  val rhs = column(A.multiply(B.multiply(x)))
  val err = lhs.zip(rhs).map { case (u, v) => math.abs(u - v) / (math.abs(v) + 1.0) }.max
  freivaldsOk = freivaldsOk && err < 1e-9
  val parts = c.blocks.getNumPartitions          // the partitions Spark spreads C's blocks over
  println(f"MEASURED dense_multiply block=$bs blocks_per_side=${n / bs} partitions=$parts seconds=$secs%.2f shuffle_written_MB=${wr / 1e6}%.1f shuffle_read_MB=${rd / 1e6}%.1f matrix_copies_moved=${wr / matrixBytes}%.2f stages=$st freivalds_rel_error=$err%.1e")
  A.blocks.unpersist(); B.blocks.unpersist(); c.blocks.unpersist()
}

// ================= Part 2: sparse, blocks vs a join of entries (U6 §4.4, U5) =================
val ns = 1024
var sameAnswer = true
println(f"MEASURED sparse n=$ns dense_matrix_MB=${8.0 * ns * ns / 1e6}%.1f")
for (rho <- Seq(0.001, 0.01, 0.05, 0.1)) {
  val ea = sparseEntries(ns, rho, SEED + 11)
  val eb = sparseEntries(ns, rho, SEED + 13)
  val nnzA = ea.count(); val nnzB = eb.count()
  val bA = new CoordinateMatrix(ea, ns, ns).toBlockMatrix(256, 256).cache()
  val bB = new CoordinateMatrix(eb, ns, ns).toBlockMatrix(256, 256).cache()
  bA.blocks.count(); bB.blocks.count()
  // (a) as blocks: BlockMatrix.multiply
  val (cBlocks, sB, wB, _, _) = measure { val C = bA.multiply(bB).cache(); C.blocks.count(); C }
  // (b) as a join of entries: A's entries keyed by column k, B's by row k, joined on k,
  //     multiplied, then summed per output position (i, j)
  val (p, sJ, wJ, _, _) = measure {
    val p = ea.map(e => (e.j, (e.i, e.value)))
      .join(eb.map(e => (e.i, (e.j, e.value))))
      .map { case (_, ((i, a), (j, b))) => ((i, j), a * b) }
      .reduceByKey(_ + _).cache()
    p.count()
    p
  }
  val nnzC = p.count()
  // Same product? Compare C x for a random vector x (Freivalds' idea again): blocks result vs join result.
  val xs = { val r = new java.util.Random(SEED + 77); Array.fill(ns)(r.nextDouble()) }
  val bx = sc.broadcast(xs)
  val cxBlocks = cBlocks.toCoordinateMatrix().entries.map(e => (e.i.toInt, e.value * bx.value(e.j.toInt))).reduceByKey(_ + _).collectAsMap()
  val cxJoin = p.map { case ((i, j), v) => (i.toInt, v * bx.value(j.toInt)) }.reduceByKey(_ + _).collectAsMap()
  val rows = cxBlocks.keySet ++ cxJoin.keySet
  val gap = rows.map(i => math.abs(cxBlocks.getOrElse(i, 0.0) - cxJoin.getOrElse(i, 0.0)) / (math.abs(cxJoin.getOrElse(i, 0.0)) + 1.0)).max
  sameAnswer = sameAnswer && gap < 1e-9
  // How many (i, k, j) pairs the join creates: for each k, (non-zeros in column k of A) x (in row k of B).
  val colA = ea.map(e => (e.j, 1L)).reduceByKey(_ + _)
  val rowB = eb.map(e => (e.i, 1L)).reduceByKey(_ + _)
  val partial = colA.join(rowB).map { case (_, (a, b)) => a * b }.sum().toLong
  println(f"MEASURED sparse_multiply density=$rho%.3f nnz_A=$nnzA nnz_B=$nnzB partial_products=$partial nnz_C=$nnzC blocks_shuffle_MB=${wB / 1e6}%.2f join_shuffle_MB=${wJ / 1e6}%.2f blocks_seconds=$sB%.2f join_seconds=$sJ%.2f same_Cx_rel_gap=$gap%.1e")
  ea.unpersist(); eb.unpersist(); bA.blocks.unpersist(); bB.blocks.unpersist(); cBlocks.blocks.unpersist(); p.unpersist()
}

println(s"MEASURED verified=${freivaldsOk && sameAnswer} (every dense product passes Freivalds' test=$freivaldsOk, blocks and join give the same C x for a random x=$sameAnswer)")
System.exit(if (freivaldsOk && sameAnswer) 0 else 1)
