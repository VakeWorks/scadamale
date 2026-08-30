# ScaDaMaLe -- Scalable Data Science and Distributed Machine Learning

Open course materials by VakeWorks. Each cohort/year is a directory: **`2026/`** (this run),
then `2027/`, and a rolling `latest/`.

- **The book (PDF):** https://vake.works/courses/ScaDaMaLe/ScaDaMaLe-Book-2026.pdf
- **The 2026 labs:** [`2026/labs/`](2026/labs/)

Labs are Apache-2.0; the book text is CC-BY-SA-4.0. See `LICENSE`.

## Getting started (new to Git? start here)

You need three tools: **git**, **ssh**, and **docker**. The book's Foundations chapters
*"The Linux shell"* and *"Git, SSH and the sovereign centre"* teach these from scratch --
read them first if any of this is new
([book PDF](https://vake.works/courses/ScaDaMaLe/ScaDaMaLe-Book-2026.pdf), Foundations part).

**1. Install git.**

- **Ubuntu/Debian:** `sudo apt update && sudo apt install -y git`
- **macOS:** `xcode-select --install` (or `brew install git`)
- **Windows:** install [Git for Windows](https://git-scm.com/download/win), then use *Git Bash*
- Verify: `git --version`

**2. Clone this repository.**

```bash
git clone https://git.vake.works/vakeworks/scadamale.git
cd scadamale/2026/labs
```

(Mirrors, if the primary is busy: `codeberg.org`, `github.com`, `gitlab.com` -- all `vakeworks/scadamale`.)

**3. Run a lab.** Each lab is self-contained; `cd` into it and follow its `README.md`:

```bash
cd lab0-toolchain
bash run-lab.sh
```

Lab 0 checks your toolchain (git + ssh + docker) and runs the proof-checker on a one-line
theorem -- do it first. Then `lab1-alpha-beta` and `lab2-sample-sort-spark`. Every lab writes
a timestamped receipt you keep as your submission evidence (see [`2026/labs/README.md`](2026/labs/README.md)).
