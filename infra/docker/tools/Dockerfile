# Build the one-shot runner for smoke checks, queries, and storage admin tasks.
# Run from infra/ against services that are already ready:
#   docker compose --profile tools run --rm tools python -m tools.smoke
#
# The builder installs clients into a virtual environment. Copying that directory
# to the same Python base keeps client versions separate from system packages.

FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Compose uses infra/ as the build context, so COPY paths start there.
COPY docker/tools/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY docker/tools/tools/ /app/tools/

CMD ["python", "-m", "tools.smoke"]
