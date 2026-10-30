#!/usr/bin/env bash
# Report anything left in the lakehouse that a completed run should not have.
#
# Cleanup after a Spark job is not just stopping containers. A killed job
# leaves `.spark-staging-*` trees behind, and those are invisible in a table
# listing because they sit above the partition directories. They are also what
# makes a later MSCK REPAIR register partitions that belong to a run that never
# committed.
#
# Usage: lakehouse_litter.sh
set -u

cd "$(dirname "$0")/../.." || exit 1

echo "=== staging trees left under the lakehouse"
found=0
for prefix in bronze silver gold warehouse; do
  out=$(docker compose --project-directory infra --profile tools run --rm tools \
        python -m tools.ls lakehouse "$prefix/" 2>/dev/null \
        | grep -oE "${prefix}/[a-z_]+/\.spark-staging-[a-z0-9-]+" | sort -u)
  if [ -n "$out" ]; then
    echo "$out"
    found=1
  fi
done
[ "$found" -eq 0 ] && echo "  none"

echo
echo "=== probe and smoke objects left behind"
docker compose --project-directory infra --profile tools run --rm tools \
  python -m tools.ls lakehouse probe/ 2>/dev/null | tail -3
docker compose --project-directory infra --profile tools run --rm tools \
  python -m tools.ls landing probe/ 2>/dev/null | tail -3

echo
echo "=== stray objects at a silver table root (outside dt= partitions)"
for table in stg_trade stg_cash_transaction stg_daily_market stg_watch_history \
             stg_holding_history stg_trade_history; do
  out=$(docker compose --project-directory infra --profile tools run --rm tools \
        python -m tools.ls lakehouse "silver/${table}/part-" 2>/dev/null | tail -n +2)
  if [ -n "$out" ]; then
    echo "  $table:"
    echo "$out" | sed 's/^/    /'
  fi
done
echo "  (empty means none)"
