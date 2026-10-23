
# Lab 5 — distributed SGD and parameter averaging (deadline Mon 30 Nov 2026)

Unit U9 proves how fast gradient descent converges, and explains what
distributing it costs: every step, the workers must combine their gradients —
a round of communication. In this lab you solve one convex problem on 8
workers, check the convergence theorem against what you measure, and then
trade communication for local work with **parameter averaging**. This lab is
in Python (PySpark), like Lab 3; Lab 4 was in Scala.

## Prerequisite (one-time)

Docker installed and running — see **[Install Docker](../README.md#install-docker-one-time-official-instructions--pick-your-laptops-path)** in `labs/README.md`. In Docker Desktop, give Docker at least **4 GB of memory** (Settings → Resources).

## Run it

Open a terminal in this folder (`cd lab5-distributed-sgd/`), then:

    docker pull vakeworks/scadamale-lab:0.1.2
    bash run-lab.sh

While it runs, `tail -f receipts/run-lab5-*.log` shows its progress.

It takes about a minute or two. It writes `receipts/receipt-lab5-<timestamp>.txt`
(upload that `.txt`, not the `.log` next to it). The numbers are the same on
every machine (everything is seeded); only the `seconds` differ.

## What to look at before uploading

**The problem.** Logistic regression with an L2 penalty (λ = 0.001) on
n = 200,000 examples with d = 50 features, split over 8 workers (Spark
partitions). The objective f is L-smooth and μ-strongly convex (U9 §2): the
`problem` line shows L, μ = λ, the condition number κ = L/μ, and the exact
optimum f* (found with a few Newton steps, which is cheap because d is small).
It also shows `mu_at_optimum`, the smallest curvature of f at the optimum (the
smallest eigenvalue of its Hessian there).

1. **Gradient descent against its theorem — U9 Theorem 9.5.** Full-batch GD
   with step 1/L, 40 steps from w = 0. Every step each worker computes the
   gradient on its own examples and Spark adds the 8 partial gradients
   (`treeAggregate`): one round of communication per step. Theorem 9.5
   promises f(w_k) − f* ≤ (1 − μ/L)^k (f(w_0) − f*). Check
   `bound_holds_every_step`, then compare the bound's factor per step
   (`bound_1_minus_mu_over_L`) with the factors you measure over steps 10–20
   and 30–40 (`shrink_per_step_steps_...`), and the `curvature` line: the
   smallest curvature of f at the start, at step 40 and at the optimum.
   Question 1 asks about the difference.
2. **Parameter averaging — U9 §6.4.** Each worker starts from the same model,
   runs H steps of minibatch SGD (64 examples per step, constant step size
   1/L) on its own data, and
   then the 8 models are averaged: one round of communication per H local
   steps. The budget is 10 rounds. Compare the gaps f(w) − f* after each
   round for H = 1, 10 and 100, and with plain GD after 10 steps
   (`gd_same_rounds`). Look at how H = 100 behaves in its last rounds.
   Question 2 asks why.
3. **What a round costs — U9 §6.1.** `bytes_per_round`: the upload half of a
   round, one vector of d numbers per worker (the new model then goes back
   down). For a real model the same arithmetic
   is large: the `ring_allreduce_7B_model_fp32` line applies U9 §6.1's
   ring all-reduce budget to a 7-billion-parameter model.

`MEASURED verified=True` is the pass line: the GD gap respects Theorem 9.5 at
every step, Newton found the optimum, and with 10 rounds more local work per
round ends closer to the optimum.

## What happens under the hood

**treeAggregate.** Summing the workers' gradients is a reduction (U1). Inside
each partition NumPy sums the examples; then Spark adds the 8 partition sums in
a tree, combining some on the executors (a small shuffle) and the rest on the
driver, which receives 2 vectors instead of 8. One `treeAggregate` is one
combination of the workers' vectors: in this lab, one round of communication.

**broadcast.** In Part 2, before each round of averaging, the driver sends the
current model to every worker with `sc.broadcast`, the other half of a round.
(In Part 1 the model simply travels inside each task.)

**NumPy inside Spark.** Each partition holds its examples as one NumPy array,
and the gradient of a partition is a few matrix–vector products: Spark
distributes the work, NumPy does the arithmetic. (Spark's MLlib has
ready-made logistic regression; writing the loop yourself is what lets you
count the rounds.)

**Other frameworks.** The same algorithms run on Ray (actors and an object
store), or with MPI or NCCL all-reduce on GPU clusters. The communication
pattern — one vector per worker per round — is the point, not the framework.

## Submit

Upload the receipt file (`receipts/receipt-lab5-<timestamp>.txt`) on your
course page, and answer two questions in the **comment box** of the upload page
(do not edit the receipt: its hash would no longer match), one or two
sentences each:

1. **Theorem 9.5's bound promises a factor of about 0.9998 per step, but over
   steps 30–40 GD shrinks the gap by about 3% per step — and faster still over
   steps 10–20. Why is the bound so pessimistic, and why is GD slowing down?**
   (Hint: compare λ with the three numbers on the `curvature` line.)
2. **With 10 rounds of communication, H = 10 needs all 10 rounds to reach the
   gap that H = 100 reaches after one (8.1e-3). Why — and why does H = 100 then
   stop improving?** (Hint: what a round costs, and U9 §4.3 for a constant
   step size.)

The full procedure — the same for every lab — is
**[How to submit](../README.md#how-to-submit)**.
