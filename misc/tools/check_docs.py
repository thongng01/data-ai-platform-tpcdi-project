"""Check links, anchors, whitespace, dashes and banned words in all docs."""
import re
from pathlib import Path

BANNED = re.compile(r"\b(honestly|genuinely|truly|frankly|quietly|silently|delve|leverage|ultimately)\b", re.I)


def slug(h):
    s = h.strip().lower()
    s = re.sub(r"[^\w\- ]", "", s)
    return s.replace(" ", "-")


def anchors(path):
    seen, out, in_code = {}, set(), False
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if line.startswith("```"):
            in_code = not in_code
        if in_code:
            continue
        m = re.match(r"^(#+)\s+(.*)", line)
        if m:
            base = slug(m.group(2))
            n = seen.get(base, 0)
            out.add(base if n == 0 else f"{base}-{n}")
            seen[base] = n + 1
    return out


files = sorted(str(p) for p in Path("docs").rglob("*.md") if "course" not in p.parts)
files += ["README.md", "AGENTS.md", "misc/README.md"]
files += sorted(str(p) for p in Path("misc/tools").rglob("*.md") if "drawio" not in p.parts)
bad = 0
for f in files:
    p = Path(f)
    raw = p.read_bytes()
    text = raw.decode("utf-8-sig")
    issues = []
    if b"\r\n" in raw:
        issues.append("CRLF")
    if raw.startswith(b"\xef\xbb\xbf"):
        issues.append("BOM")
    for i, line in enumerate(text.splitlines(), 1):
        if line != line.rstrip():
            issues.append(f"trailing ws {i}")
        if re.search("[\u2013\u2014]", line):
            issues.append(f"dash {i}")
        if "\u00e2\u20ac" in line:
            issues.append(f"mojibake {i}")
        m = BANNED.search(line)
        if m:
            issues.append(f"banned '{m.group(0)}' {i}")
    for m in re.finditer(r"\]\(([^)\s]+)\)", text):
        target = m.group(1)
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        path_part, _, frag = target.partition("#")
        dest = (p.parent / path_part).resolve() if path_part else p.resolve()
        if not dest.exists():
            issues.append(f"missing {target}")
            continue
        if frag and dest.suffix == ".md" and frag not in anchors(dest):
            issues.append(f"bad anchor {target}")
    print(f, "OK" if not issues else issues)
    bad += len(issues)
print("issues", bad)
raise SystemExit(1 if bad else 0)
