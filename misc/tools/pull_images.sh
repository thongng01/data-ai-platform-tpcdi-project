#!/usr/bin/env bash
# Pull the base images the coursework stack needs.
# Run in the background so the working session does not block on downloads.
set -u

cd "$(dirname "$0")/../.." || exit 1
set -a
# shellcheck disable=SC1091
. infra/.env
set +a

CORE=(
  "${POSTGRES_IMAGE}"
  "${SEAWEEDFS_IMAGE}"
  "${TRINO_IMAGE}"
  "${HIVE_METASTORE_IMAGE}"
  "${AIRFLOW_BASE_IMAGE}"
  "apache/spark:3.5.3"
  "apache/flink:1.20.1-scala_2.12-java17"
  "${KAFKA_IMAGE}"
  "python:3.11-slim"
)

GOVERNANCE=(
  "opensearchproject/opensearch:2.19.3"
  "acryldata/datahub-upgrade:v1.5.0.6"
  "acryldata/datahub-gms:v1.5.0.6"
  "acryldata/datahub-frontend-react:v1.5.0.6"
  "acryldata/datahub-actions:v0.0.15"
)

pull_group() {
  local label="$1"
  shift
  for image in "$@"; do
    echo "=== $label :: pulling $image"
    docker pull "$image" || echo "=== $label :: FAILED $image"
  done
  echo "=== $label :: GROUP_DONE"
}

case "${1:-core}" in
  core) pull_group CORE "${CORE[@]}" ;;
  governance) pull_group GOVERNANCE "${GOVERNANCE[@]}" ;;
  all) pull_group CORE "${CORE[@]}"; pull_group GOVERNANCE "${GOVERNANCE[@]}" ;;
  *) echo "usage: $0 [core|governance|all]"; exit 2 ;;
esac