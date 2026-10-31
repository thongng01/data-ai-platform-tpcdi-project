#!/usr/bin/env bash
# Check every relative Markdown link in the repository and report broken ones.
#
# A documentation review is mostly link checking: a link to a file that was
# renamed or moved is the most common form of stale information, and it is the
# one a reader hits first.
#
# Usage: check_links.sh [root]   (root defaults to the repository root)
set -u

root="${1:-$(dirname "$0")/../..}"
cd "$root" || exit 1

broken=0
checked=0

while IFS= read -r file; do
  dir=$(dirname "$file")
  # Only inline Markdown links and images, not fenced code blocks.
  while IFS= read -r target; do
    [ -z "$target" ] && continue
    case "$target" in
      http:*|https:*|mailto:*|\\*) continue ;;
      "#"*) continue ;;
    esac
    # Strip an anchor and a query string.
    clean="${target%%#*}"
    clean="${clean%%\?*}"
    [ -z "$clean" ] && continue

    checked=$((checked + 1))
    if [ ! -e "$dir/$clean" ]; then
      broken=$((broken + 1))
      printf 'BROKEN  %s  ->  %s\n' "$file" "$target"
    fi
  done < <(grep -oE '\]\(([^)]+)\)' "$file" 2>/dev/null | sed 's/^](//; s/)$//')
done < <(
  find . -name '*.md' -type f -not -path './.git/*' -not -path './.venv/*' -not -path './misc/*'
  find misc/tools -name '*.md' -type f -not -path 'misc/tools/diagrams/drawio/*' -not -path '*/node_modules/*'
  printf '%s\n' misc/README.md
)

echo
echo "checked $checked links, $broken broken"
exit $(( broken > 0 ? 1 : 0 ))
