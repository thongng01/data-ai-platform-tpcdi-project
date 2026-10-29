#!/usr/bin/env bash
# Report the restart count and last exit reason for each coursework container.
# Used to tell a slow start apart from a crash loop.
set -u

cd "$(dirname "$0")/../../infra" || exit 1
docker compose ps -a --format '{{.Name}}' | while read -r name; do
  [ -z "$name" ] && continue
  docker inspect -f '{{.Name}} restarts={{.RestartCount}} running={{.State.Running}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} err={{.State.Error}}' "$name"
done