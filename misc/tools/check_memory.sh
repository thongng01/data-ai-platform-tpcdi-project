#!/usr/bin/env bash
# Print the memory limit of every service and a total per profile set.
#
# docs/IaC.md section 6 quotes a limit for each service and a total for the core
# and compute sets. `docker compose config` renders those limits in bytes, so
# this converts them back and prints the sums the document claims.
#
# Usage: check_memory.sh
set -u

cd "$(dirname "$0")/../../infra" || exit 1

python3 - <<'PY'
import re
import subprocess


def rendered(*profiles):
    cmd = ["docker", "compose"]
    for profile in profiles:
        cmd += ["--profile", profile]
    cmd += ["config"]
    return subprocess.run(cmd, capture_output=True, text=True).stdout


def limits(text):
    """Map service name to memory limit, read from the rendered config."""
    result = {}
    service = None
    pending = None
    for line in text.splitlines():
        m = re.match(r"^  ([a-z][a-z0-9-]*):\s*$", line)
        if m:
            if service and pending:
                result[service] = pending
            service, pending = m.group(1), None
            continue
        m = re.match(r'^\s+memory:\s*"?(\d+)"?\s*$', line)
        if m and service:
            pending = int(m.group(1))
    if service and pending:
        result[service] = pending
    return result


core = limits(rendered())
compute = limits(rendered("compute"))

print("=== memory limit per service")
for name in sorted(set(core) | set(compute)):
    value = core.get(name) or compute.get(name)
    print(f"  {name:<22} {value / 2**30:.2f} GB" if value else f"  {name:<22} (none)")


def total(mapping, names):
    return sum(mapping.get(n, 0) for n in names)


core_names = set(core)
compute_only = set(compute) - core_names

print()
print("=== totals")
print(f"  core only       {total(core, core_names) / 2**30:.1f} GB")
print(f"  core + compute  {(total(core, core_names) + total(compute, compute_only)) / 2**30:.1f} GB")
print()
print("  machine has 15 GB, so core + governance does not fit alongside compute")
PY
