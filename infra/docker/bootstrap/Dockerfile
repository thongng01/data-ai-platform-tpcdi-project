# Build the one-shot runner that creates buckets and application databases.
# PostgreSQL and SeaweedFS must be ready before the runner connects to them.
#
# Both stages use the same Python base so the copied virtual environment can
# find the same interpreter and system libraries. The runtime copies installed
# dependencies, not the builder temporary files.

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

# The runner polls service APIs through Python clients and does not need curl.
COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY docker/bootstrap/bootstrap/ /app/bootstrap/

ENTRYPOINT []
CMD ["python", "-m", "bootstrap.run"]
