# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
"""Lab 5 -- distributed gradient descent and parameter averaging (PySpark, local[*]).

One convex problem, solved three ways, each straight out of unit U9:

  the problem  logistic regression with an L2 penalty on n examples with d features,
               split over P partitions ("workers"); f(w) = mean log(1+exp(-y x.w)) + lam/2 |w|^2,
               so f is L-smooth and mu-strongly convex with mu = lam, L = lam + lambda_max(X'X/n)/4;
               f* is found exactly by a few Newton steps (d is small);
  Part 1       full-batch gradient descent with step 1/L: every step sums P partial gradients
               (treeAggregate: one round of communication per step); the gap f(w_k) - f* against
               the bound (1 - mu/L)^k of Theorem 9.5;
  Part 2       parameter averaging ("local SGD"): each worker runs H steps of minibatch SGD on its
               own data, then the P models are averaged -- one round of communication per H local
               steps. For a fixed budget of rounds, H = 1, 10, 100;
  Part 3       the bytes per round, and the ring all-reduce budget of U9 §6.1 for a large model.

Spark could be replaced by Ray (actors) or by MPI/NCCL all-reduce for the same algorithms; the
communication pattern, not the framework, is the point.

Run inside the course container (run-lab.sh does this for you):  python3 distributed_sgd.py
"""

import math
import sys
import time

import numpy as np
from pyspark.sql import SparkSession

SEED = 20261022
N, P, D = 200_000, 8, 50      # examples, workers (partitions), features
LAM = 1e-3                    # the L2 penalty
ROUNDS = 10                   # communication budget in Part 2
BATCH = 64                    # minibatch size per local SGD step


def make_shard(p, scales, w_true):
    """Worker p's examples: Gaussian features with unequal scales (an ill-conditioned problem)."""
    r = np.random.default_rng(SEED + 1 + p)
    X = r.normal(size=(N // P, D)) * scales
    prob = 1.0 / (1.0 + np.exp(-X @ w_true))
    y = np.where(r.random(N // P) < prob, 1.0, -1.0)
    return X, y


def main():
    spark = (SparkSession.builder.appName("scadamale-lab5-distributed-sgd").master("local[*]")
             .config("spark.driver.memory", "2g").config("spark.jars.ivy", "/tmp/.ivy2")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    scales = np.linspace(1.0, 4.0, D)
    w_true = np.random.default_rng(SEED).normal(0, 3.0 / math.sqrt(D), D)
    data = sc.parallelize(range(P), P).map(lambda p: make_shard(p, scales, w_true)).cache()
    data.count()
    add = lambda a, b: a + b

    def loss_grad(w):
        """f(w) and its gradient: each worker sums over its examples; treeAggregate adds the P parts."""
        def part(Xy):
            X, y = Xy
            m = y * (X @ w)
            return np.concatenate([[np.logaddexp(0, -m).sum()], X.T @ (-y / (1 + np.exp(m)))])
        tot = data.map(part).treeAggregate(np.zeros(D + 1), add, add)
        return tot[0] / N + LAM / 2 * w @ w, tot[1:] / N + LAM * w

    def hessian(w):
        def part(Xy):
            X, _ = Xy
            s = 1 / (1 + np.exp(-(X @ w)))
            return (X * (s * (1 - s))[:, None]).T @ X
        return data.map(part).treeAggregate(np.zeros((D, D)), add, add) / N + LAM * np.eye(D)

    # ---- the constants of U9: L (smoothness), mu (strong convexity), and f* ----
    gram = data.map(lambda Xy: Xy[0].T @ Xy[0]).treeAggregate(np.zeros((D, D)), add, add) / N
    L = LAM + np.linalg.eigvalsh(gram).max() / 4       # the logistic loss has curvature <= 1/4
    mu = LAM
    w = np.zeros(D)
    for _ in range(12):                                # Newton's method: exact f* for small d
        _, g = loss_grad(w)
        w = w - np.linalg.solve(hessian(w), g)
    fstar, gstar = loss_grad(w)
    mu_opt = np.linalg.eigvalsh(hessian(w)).min()      # smallest curvature of f at w*
    mu_start = np.linalg.eigvalsh(hessian(np.zeros(D))).min()   # ... and at the start, w = 0
    print("MEASURED problem n=%d d=%d workers=%d lam=%g L=%.4f mu=%g kappa=%.0f "
          "mu_at_optimum=%.4f kappa_at_optimum=%.0f fstar=%.6f grad_norm_at_fstar=%.1e"
          % (N, D, P, LAM, L, mu, L / mu, mu_opt, L / mu_opt, fstar, np.linalg.norm(gstar)))

    # ---- Part 1: full-batch GD with step 1/L (Theorem 9.5) ----
    K = 40
    w = np.zeros(D)
    gaps = []
    t0 = time.time()
    for k in range(K + 1):
        f, g = loss_grad(w)
        gaps.append(f - fstar)
        if k == K:
            break
        w = w - g / L
    secs = time.time() - t0
    mu_step40 = np.linalg.eigvalsh(hessian(w)).min()   # smallest curvature where GD is at step 40
    bound_ok = all(gaps[k] <= (1 - mu / L) ** k * gaps[0] * (1 + 1e-9) for k in range(K + 1))
    shrink = lambda a, b: (gaps[b] / gaps[a]) ** (1 / (b - a))   # measured factor per step
    print("MEASURED gd steps=%d seconds=%.1f gap_at_step_0_10_20_30_40=%s"
          % (K, secs, " ".join("%.3e" % gaps[k] for k in range(0, K + 1, 10))))
    print("MEASURED gd shrink_per_step_steps_10_20=%.4f shrink_per_step_steps_30_40=%.4f "
          "bound_1_minus_mu_over_L=%.5f bound_holds_every_step=%s"
          % (shrink(10, 20), shrink(30, 40), 1 - mu / L, bound_ok))
    print("MEASURED curvature smallest_eigenvalue_of_hessian at_start=%.4f at_step_40=%.4f "
          "at_optimum=%.4f (lam=%g)" % (mu_start, mu_step40, mu_opt, LAM))

    # ---- Part 2: parameter averaging (local SGD) with a budget of ROUNDS rounds ----
    def local_sgd(H):
        w = np.zeros(D)
        out = []
        for r in range(ROUNDS):
            bw = sc.broadcast(w)

            def run(idx, it, r=r, bw=bw):
                X, y = next(it)
                rng = np.random.default_rng(SEED + 1000 * r + idx)
                v = bw.value.copy()
                for _ in range(H):                    # H minibatch SGD steps on this worker's data
                    ii = rng.integers(0, len(y), BATCH)
                    Xb, yb = X[ii], y[ii]
                    m = yb * (Xb @ v)
                    v -= (Xb.T @ (-yb / (1 + np.exp(m))) / BATCH + LAM * v) / L
                return [v]
            w = data.mapPartitionsWithIndex(run).treeAggregate(np.zeros(D), add, add) / P   # average
            out.append(loss_grad(w)[0] - fstar)
        return out

    finals = {}
    for H in (1, 10, 100):
        t0 = time.time()
        g = local_sgd(H)
        finals[H] = g[-1]
        print("MEASURED averaging H=%d rounds=%d local_steps_per_round=%d seconds=%.1f gap_after_round_1_to_%d=%s"
              % (H, ROUNDS, H, time.time() - t0, ROUNDS, " ".join("%.2e" % x for x in g)))
    print("MEASURED gd_same_rounds gap_after_%d_steps=%.2e (one gradient step per round)"
          % (ROUNDS, gaps[ROUNDS]))

    # ---- Part 3: bytes ----
    per_round = P * D * 8                              # each worker sends one d-vector of doubles
    d7b = 7e9
    ring = 2 * (P - 1) / P * d7b * 4                   # U9 §6.1, float32, per worker per step
    print("MEASURED bytes_per_round workers=%d d=%d bytes=%d (each worker sends one vector of %d doubles)"
          % (P, D, per_round, D))
    print("MEASURED ring_allreduce_7B_model_fp32 bytes_per_worker_per_step=%.1fe9 seconds_at_100Gbit=%.1f"
          % (ring / 1e9, ring / (100e9 / 8)))

    # ---- the checks that must hold ----
    #  * Theorem 9.5: the GD gap is below (1 - mu/L)^k times the first gap at every step;
    #  * Newton found the optimum (gradient ~ 0);
    #  * with the same number of rounds, more local work per round gets closer: H=100 < H=10 < H=1.
    newton_ok = np.linalg.norm(gstar) < 1e-8
    order_ok = finals[100] < finals[10] < finals[1]
    verified = bound_ok and newton_ok and order_ok
    print("MEASURED verified=%s (gd_bound_holds=%s newton_optimum=%s more_local_work_helps=%s)"
          % (verified, bound_ok, newton_ok, order_ok))
    spark.stop()
    if not verified:
        sys.exit(1)


if __name__ == "__main__":
    main()
