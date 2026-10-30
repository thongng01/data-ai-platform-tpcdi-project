# Spark cluster image for the coursework platform.
#
# Two things are added to the stock Apache Spark image:
#
#   1. hadoop-aws plus an AWS SDK, so Spark can read and write the SeaweedFS
#      bucket through the S3A filesystem. Spark ships no S3 client of its own.
#   2. PySpark and a couple of job dependencies, so a job is a Python module
#      mounted into the cluster rather than a file that has to be copied into
#      the image each time it changes.
#
# Multi-stage build. The builder stage downloads jars into /opt/jars and is
# thrown away; the runtime stage copies the finished jars across. The
# alternative, a single stage, leaves curl, the apt cache and the download
# scratch space inside the shipped image. docs/IaC.md records the measured
# size of both builds.

FROM debian:bookworm-slim AS builder

ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ENV MAVEN_REPO=https://repo1.maven.org/maven2

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl zip; \
    rm -rf /var/lib/apt/lists/*

# Only two jars are fetched. hadoop-aws is the S3A filesystem implementation and
# aws-java-sdk-bundle is the client it calls.
#
# Two usual companions are deliberately absent:
#   jmespath-java     only needed for JMESPath filters on object listings
#   wildfly-sasl      only needed for SASL authentication
# We use static credentials and plain listings, so carrying ~2 MB of
# unreachable paths buys nothing. docs/IaC.md records the effect on image size.
RUN set -eux; \
    mkdir -p /opt/jars; \
    curl -fsSL -o "/opt/jars/hadoop-aws-${HADOOP_VERSION}.jar" \
      "${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSL -o "/opt/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "${MAVEN_REPO}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    # Strip signatures: the shaded AWS bundle carries signatures from jars that
    # are no longer in the file, and an unsigned-jar error stops the class load.
    for jar in /opt/jars/*.jar; do \
      zip -d "$jar" 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*.DSA' >/dev/null 2>&1 || true; \
    done; \
    ls -la /opt/jars


FROM apache/spark:3.5.3 AS runtime

USER root

RUN set -eux; \
    mkdir -p /opt/spark/jars; \
    ln -sf /usr/bin/python3 /usr/bin/python || true; \
    python3 -V

COPY --from=builder /opt/jars/ /opt/spark/jars/

# PySpark and the job dependencies live in one virtualenv so the image stays
# inspectable: `pip list` inside the container matches infra/docker/spark/requirements.txt.
COPY requirements.txt /opt/spark/requirements.txt
RUN set -eux; \
    pip3 install --no-cache-dir --no-compile -r /opt/spark/requirements.txt; \
    find /usr /opt -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true

ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# PYSPARK_SUBMIT_ARGS is deliberately NOT set. spark-submit appends it to every
# submission, so it injects `pyspark-shell` into a batch job's argument list and
# the job's resource request comes out wrong. It belongs to the `pyspark` REPL
# launcher only.

USER 185

WORKDIR /opt/spark/work-dir
