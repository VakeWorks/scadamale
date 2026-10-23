
# ScaDaMaLe labs

Every lab runs in Docker on your own laptop (8 GB RAM is enough), produces a
machine-generated **receipt file** (measured numbers + run log + hash), and
you upload that receipt on the course page. Nothing is graded by style —
the receipt is the submission.

## If the terminal is new to you

You will type commands into a *terminal* (macOS: the Terminal app;
Windows: PowerShell; Linux: any terminal). A command is one line of text;
press Enter to run it; the output appears below. That is all the labs
assume — and chapter **F.E1 (Linux and the shell)** in the course book is a
gentle 30-minute on-ramp. Two conventions in these instructions:

- Lines you type are shown as code; type the line, press Enter.
- `cd some-folder` means "move into that folder"; `ls` lists what is there.

## Install Docker (one-time, official instructions) — pick your laptop's path

**macOS** (Intel and Apple Silicon both work):
1. Docker Desktop — https://docs.docker.com/desktop/setup/install/mac-install/
2. Your terminal is the built-in Terminal app. Done.

**Linux**:
1. Docker Engine — https://docs.docker.com/engine/install/ (pick your
   distribution), then the post-install step so `docker` works without
   `sudo`: https://docs.docker.com/engine/install/linux-postinstall/
2. Any terminal. Done.

**Windows** (two pieces — WSL2 first, then Docker Desktop):
1. Install WSL2 with Ubuntu (one command in an *administrator* PowerShell:
   `wsl --install`; reboot) — official guide:
   https://learn.microsoft.com/windows/wsl/install
2. Docker Desktop for Windows with the **WSL 2 backend** (the default) —
   https://docs.docker.com/desktop/setup/install/windows-install/ and
   enable WSL integration for Ubuntu under Settings → Resources → WSL
   integration: https://docs.docker.com/desktop/features/wsl/
3. **Do all course work inside the Ubuntu (WSL2) terminal** — open
   "Ubuntu" from the Start menu. `git`, `bash`, and `docker` all work
   there, and every lab command in this course is then identical on all
   three operating systems.

Verify your install (any machine, in your terminal):

    docker --version
    docker run --rm hello-world

The course images are published multi-architecture: the same `docker pull`
works on Intel/AMD (`amd64`) and on Apple-Silicon/ARM (`arm64`) machines —
you never pick an architecture by hand. This holds for the proof-checker
image and, from Lab 3 on, for the Spark lab image
`vakeworks/scadamale-lab:0.1.2` (Python and Scala). Labs 0–2 used the
earlier `0.1.0`, which was published for Intel/AMD only, so Apple-Silicon
Macs ran it under emulation.

## The labs (Period 1)

Labs 0–2 have an *ideally-due* paced date — the recommended rhythm, so you
arrive at Module 1 (29–30 Sep) with a working toolchain — and ONE shared
**hard deadline: Tuesday 6 October, 23:59 Stockholm** (the week after
Module 1, where the lab-lectures include hands-on setup help; a setup
struggle never costs you a late submission). Labs 3–7 keep their Monday
dates as *recommended* dates and share ONE hard deadline, **Monday 30
November**\*; each lab appears in this folder (and in the table below) when
it is published. The authoritative schedule is the course page's Learning
Management Table.

\* Only 5 of the 8 labs (Labs 0–7) need to be submitted to pass the course (due to API
errors in 2026).

| Lab | Folder | Ideally due | Hard deadline |
|---|---|---|---|
| Lab 0 — toolchain + proof-checker | `lab0-toolchain/` | Mon 14 Sep | **Tue 6 Oct** |
| Lab 1 — ping-pong + tree vs ring allreduce | `lab1-alpha-beta/` | Mon 21 Sep | **Tue 6 Oct** |
| Lab 2 — sample sort on Spark | `lab2-sample-sort-spark/` | Mon 28 Sep | **Tue 6 Oct** |
| Lab 3 — streaming sketches | `lab3-streaming-sketches/` | Mon 12 Oct | **Mon 30 Nov**\* |
| Lab 4 — blocked matrix multiplication (Scala) | `lab4-blocked-matmul/` | Mon 19 Oct | **Mon 30 Nov**\* |
| Lab 5 — distributed SGD and parameter averaging | `lab5-distributed-sgd/` | Mon 26 Oct | **Mon 30 Nov**\* |
| Lab 6 — PageRank two ways (Python + Scala) | `lab6-pagerank-two-ways/` | Mon 2 Nov | **Mon 30 Nov**\* |
| Lab 7 — serve a small model | *(published after Module 2)* | Mon 9 Nov | **Mon 30 Nov**\* |

Each folder has its own `README.md` (the steps) and a `run-lab.sh` (runs
everything and writes `receipts/receipt-<lab>-<timestamp>.txt`). Read the
README first — the point of a lab is what you *observe*, and each README
tells you which numbers to look at and which book section explains them.

<a id="how-to-submit"></a>
## How to submit — labs and quizzes (read once; the SAME for every lab)

Every lab in this course submits the same way, so this is explained here
once and each lab's README links back to it.

**1. Run the lab → it writes a receipt.** `run-lab.sh` writes a plain-text
receipt containing the tool versions, the measured numbers, the tail of the
run log, and a SHA-256 hash of everything above it — and it prints the
receipt's path (`receipts/receipt-<lab>-<timestamp>.txt`). It also appends
one line to `labs/sigma-log.jsonl`, your local **sigma-log**: each line
records {when, which lab, receipt hash, hash of the previous line}. That
hash-chain is the course's own provenance mechanism (chapter F.E1b + the
sigma-log lab) applied to *your own work*: the chain proves the order and
integrity of your runs without any server.

**2. Submit a lab = upload that receipt file** (the `.txt`; the `.log` next to
it is the full run log, for you) on your course page. Nothing
is graded by style — the receipt (measured numbers + hash) is the
submission. Some labs also ask for one or two sentences of comment; the
README for that lab says so, and you type them in the **comment box** of the
upload page (Canvas shows one under the file). Never edit the receipt itself:
its SHA-256 line would no longer match.

**3. Submit a quiz = answer that week's quiz** on your course page. Each
week has a short quiz (a couple of questions) tied to the reading; it makes
sure you have glossed the week's material. It is separate from the lab
receipt.

**4. Keep `sigma-log.jsonl`.** At Module 1 you will read your own chain in
the sigma-log lab, and if a submission is ever disputed, your chain is your
evidence. Do not delete or hand-edit it.

**Where "your course page" is.** WASP-cohort students submit on **KTH
Canvas** (a graded assignment per lab; the receipt is the upload). If you
are taking the open self-study version, keep your receipts + `sigma-log.jsonl`
locally for now; a VakeWorks-certified completion path (upload receipts at
enable.vake.works for an optional micro-credential) is coming later. Either
way the labs, receipts, and hash-chain are identical — only *where you
upload the receipt* differs.

## Troubleshooting

- Docker Desktop must be *running* (whale icon) before any `docker` command.
- On an 8 GB laptop close heavy applications first; every lab is sized to
  run inside 2–3 GB of container memory. In Docker Desktop, give Docker at
  least 4 GB (Settings → Resources). On Windows with WSL 2 there is no such
  slider: WSL's memory limit (`memory=` in `%UserProfile%\.wslconfig`)
  applies, and its default, half your RAM, is usually enough.
- On Windows, clone and run the labs **inside the Ubuntu (WSL2) terminal**,
  not in a Windows folder: a checkout made by Windows git can turn the
  scripts' line endings into `\r\n`, and bash then fails with
  `$'\r': command not found`.
- If a pull is slow, that is normal the first time (the lab image is about
  1.2 GB; the proof-checker image is larger) — it is cached afterwards.
- Stuck? Ask in the course discussion early; include the last ~20 lines of
  your terminal output.
