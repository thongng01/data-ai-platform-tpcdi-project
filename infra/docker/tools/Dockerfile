# One-shot tooling image: smoke tests, spike scripts and admin commands.
#
# Deliberately not a service. It runs scripts that a human or an Airflow task
# invokes, against whatever is already up:
#
#   docker compose --profile tools run --rm tools python -m tools.smoke
#
# Keeps pyarrow, the S3 client, the Trino client and the Postgres driver in one
# place so spike scripts do not each carry their own requirements file.

FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

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