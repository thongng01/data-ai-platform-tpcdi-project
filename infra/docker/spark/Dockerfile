# Build the Spark runtime with Hadoop S3A libraries and Python job clients.
# The Apache Spark base supplies Spark and PySpark; this build adds S3 access.
#
# The builder downloads Java archives. Only those archives enter the runtime,
# so download tools and their system packages stay out of the final image.
# Compose mounts job code separately, so code edits do not need an image build.

FROM debian:bookworm-slim AS builder

ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ENV MAVEN_REPO=https://repo1.maven.org/maven2

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl zip; \
    rm -rf /var/lib/apt/lists/*

# hadoop-aws implements S3A; the AWS bundle supplies its Java client. Their
# versions must match the Hadoop libraries shipped with the Spark base image.
# This build does not resolve transitive dependencies through Maven.
RUN set -eux; \
    mkdir -p /opt/jars; \
    curl -fsSL -o "/opt/jars/hadoop-aws-${HADOOP_VERSION}.jar" \
      "${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSL -o "/opt/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "${MAVEN_REPO}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    # Remove signature metadata before loading the archives. This also removes
    # signature verification; HTTPS download alone is not a checksum check.
    # zip exits nonzero if no matching entries exist, so that case is ignored.
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

# Install job clients into the base image Python, not a virtual environment.
# PySpark remains the version supplied by Apache Spark. pip list also includes
# base-image packages and transitive dependencies beyond requirements.txt.
COPY requirements.txt /opt/spark/requirements.txt
RUN set -eux; \
    pip3 install --no-cache-dir --no-compile -r /opt/spark/requirements.txt; \
    find /usr /opt -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true

ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Leave PYSPARK_SUBMIT_ARGS unset. PySpark launchers read that value, and an
# unintended value can replace application submission arguments. It is not a
# daemon resource setting.

USER 185

WORKDIR /opt/spark/work-dir
