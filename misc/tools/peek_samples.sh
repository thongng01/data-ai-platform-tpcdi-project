#!/usr/bin/env bash
# Show the first two rows and the row count of each generated TPC-DI file.
# Used to read the generator's output shape without opening twenty files by hand.
#
# Usage: peek_samples.sh [batch]   (batch defaults to Batch1)
set -u

root="$(dirname "$0")/../../misc/sample_run"
batch="${1:-Batch1}"
dir="${root}/${batch}"

[ -d "$dir" ] || { echo "no such directory: $dir"; exit 1; }

for file in "$dir"/*; do
  [ -f "$file" ] || continue
  name=$(basename "$file")
  count=$(wc -l < "$file")
  printf '\n== %s  (%s lines)\n' "$name" "$count"
  head -2 "$file" | cut -c1-400
done
