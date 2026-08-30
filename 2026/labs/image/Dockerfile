# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 VakeWorks AB
#
# The ScaDaMaLe course lab image: Python + JRE + PySpark, lean on purpose.
# Sized for an 8 GB laptop: Spark runs in local[*] mode inside this one
# container; driver memory is capped by the lab scripts (2g).
#
# Build (from labs/image/):  docker compose build
# Or pull:                   docker pull vakeworks/scadamale-lab:0.1.0

# bookworm pinned: Spark 3.5 supports Java 17; trixie ships only openjdk-21
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      iproute2 iputils-ping procps git \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.4 numpy==1.26.4

# a real user at uid 1000 (the default on Linux and WSL2): the JVM's Unix
# login needs a passwd entry when labs run with --user "$(id -u):$(id -g)"
RUN useradd --create-home --uid 1000 student

# arch-agnostic JAVA_HOME: the JRE installs to java-17-openjdk-<arch> (amd64 on
# Intel, arm64 on Apple Silicon). Symlink a stable path so the SAME multi-arch
# image works on both -- a hardcoded -amd64 breaks the arm64 manifest silently.
RUN ln -s "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/java-17

ENV JAVA_HOME=/usr/lib/jvm/java-17 \
    PYSPARK_PYTHON=python3

WORKDIR /lab
CMD ["python3", "--version"]
