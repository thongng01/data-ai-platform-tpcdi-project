#!/usr/bin/env bash
# Print every host port the whole stack publishes, grouped by port.
#
# Reads the fully resolved compose configuration for every profile, so the
# numbers are the ones Docker would actually bind, not the ones written in the
# yaml. A duplicate here is a port collision that will stop the stack starting.
#
# Usage: list_ports.sh
set -u

cd "$(dirname "$0")/../../infra" || exit 1

out=$(mktemp)
for profile in "" compute stream airflow governance; do
  docker compose --profile "$profile" config 2>/dev/null
done > "$out"

echo "=== published ports, with the service that binds each"
python3 - "$out" <<'PY'
import re
import sys
from collections import defaultdict

text = open(sys.argv[1], encoding="utf-8").read()
# Each service block starts at two-space indentation with "name:".
service = None
ports = defaultdict(list)
for line in text.splitlines():
    m = re.match(r"^  ([a-z][a-z0-9-]*):\s*$", line)
    if m:
        service = m.group(1)
        continue
    m = re.search(r"published:\s*\"?(\d+)\"?", line)
    if m and service:
        ports[int(m.group(1))].append(service)

for port in sorted(ports):
    owners = sorted(set(ports[port]))
    flag = "  <-- COLLISION" if len(owners) > 1 else ""
    print(f"  {port:>6}  {', '.join(owners)}{flag}")
PY

rm -f "$out"