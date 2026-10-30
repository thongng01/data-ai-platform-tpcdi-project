# One-shot image that prepares shared state before any service starts.
#
# It creates the S3 buckets and the Postgres databases. It is idempotent, so
# running it twice is the same as running it once.
#
# Multi-stage on purpose: the builder resolves and installs the dependencies,
# the runtime stage keeps only the installed site-packages and drops the
# compiler toolchain and the pip cache. See docs/IaC.md for the size comparison.

FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Build context is infra/, so every COPY path is relative to that directory.
COPY docker/bootstrap/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# curl is used by compose healthchecks only in services that have it; bootstrap
# polls with python instead, so nothing extra is installed here.
COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY docker/bootstrap/bootstrap/ /app/bootstrap/

ENTRYPOINT []
CMD ["python", "-m", "bootstrap.run"]
