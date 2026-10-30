#!/usr/bin/env bash
# Show the Spark standalone master's view of workers and applications.
# Usage: spark_master_state.sh
set -u

port="${SPARK_MASTER_UI_PORT:-8082}"
base="http://localhost:${port}"

curl -fsS "${base}/json/" 2>/dev/null | python3 /home/thongng/workspace-wsl/fsds-edai/coursework/misc/tools/spark_state.py
