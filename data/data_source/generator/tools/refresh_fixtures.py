"""Regenerate the TPC-DI format fixture from the upstream clone.

The contract tests must run without a 369MB upstream checkout present, so a
small slice of real TPC-DI bytes is committed under
``tests/fixtures/tpcdi_sf3/``. This script regenerates that slice from
``local/tpcdi`` whenever the upstream reference is available.

Run it after re-cloning or updating the reference, and commit the result, so a
change in the upstream format shows up as a diff in the fixture rather than as
a test failure nobody can explain.

    python data/data_source/generator/tools/refresh_fixtures.py  # default: local/tpcdi
    python tools/refresh_fixtures.py /path/to/tpcdi
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

GENERATOR_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = GENERATOR_ROOT / "tests" / "fixtures" / "tpcdi_sf3"
MANIFEST_PATH = GENERATOR_ROOT / "tests" / "fixtures" / "manifest.json"

DEFAULT_REFERENCE = GENERATOR_ROOT.parents[2] / "local" / "tpcdi"

# Which files to slice, and how many lines of each to keep. Enough to pin the
# field count, the delimiter, the line ending, and the encoded values, without
# committing a corpus.
SLICES: dict[str, int] = {
    "Batch1/Trade.txt": 3,
    "Batch1/TradeHistory.txt": 2,
    "Batch1/CashTransaction.txt": 2,
    "Batch1/HoldingHistory.txt": 2,
    "Batch1/DailyMarket.txt": 2,
    "Batch1/WatchHistory.txt": 2,
    "Batch1/TradeType.txt": 10,   # the whole table; it is 5 rows
    "Batch1/StatusType.txt": 10,  # the whole table; it is 6 rows
    "Batch1/Industry.txt": 3,
    "Batch1/TaxRate.txt": 3,
    "Batch1/BatchDate.txt": 1,
    "Batch2/Trade.txt": 3,          # the 16-field delta shape
    "Batch2/CashTransaction.txt": 2,
}

DELIMITERS = {
    ".csv": ",",
    ".txt": "|",
    ".xml": None,
}


def source_root(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).resolve()
    if DEFAULT_REFERENCE.exists():
        return DEFAULT_REFERENCE
    raise SystemExit(
        f"Upstream TPC-DI reference not found at {DEFAULT_REFERENCE}.\n"
        "Restore it with:\n"
        "    git clone https://github.com/AlaaAlmutawa/TPC-DI.git local/tpcdi\n"
        "or pass the path explicitly:\n"
        "    python tools/refresh_fixtures.py /path/to/TPC-DI"
    )


def slice_lines(path: Path, limit: int) -> bytes:
    """First ``limit`` CRLF-terminated lines, byte-for-byte."""
    raw = path.read_bytes()
    out = bytearray()
    for line in raw.split(b"\r\n"):
        if not line:
            continue
        out += line + b"\r\n"
        if len(out.split(b"\r\n")) - 1 >= limit:
            break
    return bytes(out)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    root = source_root(argv[0] if argv else None)
    data_root = root / "sf_3" / "output_data"

    manifest_files: dict[str, dict[str, object]] = {}

    for relative, limit in SLICES.items():
        source = data_root / relative
        if not source.exists():
            print(f"  MISSING upstream file, skipped: {relative}")
            continue
        target = FIXTURE_ROOT / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = slice_lines(source, limit)
        target.write_bytes(payload)

        first = payload.split(b"\r\n")[0].decode("utf-8")
        delimiter = DELIMITERS.get(source.suffix, "|")
        fields = len(first.split(delimiter)) if delimiter else 1
        manifest_files[relative] = {
            "fields": fields,
            "delimiter": delimiter or "",
            "lines": payload.count(b"\r\n"),
            "sha256": hashlib.sha256(payload).hexdigest()[:16],
        }
        print(f"  {relative:<34} {fields:>3} fields, {manifest_files[relative]['lines']} lines")

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(
        json.dumps(
            {
                "source": "https://github.com/AlaaAlmutawa/TPC-DI.git",
                "path": "sf_3/output_data",
                "note": "Regenerate with tools/refresh_fixtures.py. Byte-exact slices.",
                "files": manifest_files,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"\nWrote {FIXTURE_ROOT.relative_to(GENERATOR_ROOT)} and {MANIFEST_PATH.relative_to(GENERATOR_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
