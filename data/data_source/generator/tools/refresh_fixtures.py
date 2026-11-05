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
    """Resolve and require an explicit or default reference checkout directory."""
    root = Path(explicit).resolve() if explicit else DEFAULT_REFERENCE
    if root.is_dir():
        return root
    raise SystemExit(
        f"Upstream TPC-DI reference not found at {root}.\n"
        "Restore it with:\n"
        "    git clone https://github.com/AlaaAlmutawa/TPC-DI.git local/tpcdi\n"
        "or pass the path explicitly:\n"
        "    python tools/refresh_fixtures.py /path/to/TPC-DI"
    )


def check_required_sources(data_root: Path) -> None:
    """Require every source file before the refresh can replace any fixture byte."""
    missing = [relative for relative in SLICES if not (data_root / relative).is_file()]
    if missing:
        details = "\n".join(f"    {relative}" for relative in missing)
        raise SystemExit(f"Required upstream source files are missing or not regular files:\n{details}")


def slice_lines(path: Path, limit: int) -> bytes:
    """Read only the requested source-line prefix, retaining nonempty CRLF records."""
    out = bytearray()
    # Upstream source records use CRLF. The physical line limit also applies to
    # blank input, so an unusable prefix cannot trigger a scan of the whole file.
    with path.open("rb") as stream:
        for _, raw_line in zip(range(limit), stream):
            line = raw_line.removesuffix(b"\r\n")
            if line:
                out += line + b"\r\n"
    return bytes(out)


def read_samples(data_root: Path) -> dict[str, bytes]:
    """Read and validate all required sample payloads before any output can change."""
    check_required_sources(data_root)
    samples = {}
    invalid = []
    for relative, limit in SLICES.items():
        payload = slice_lines(data_root / relative, limit)
        records = payload.split(b"\r\n")[:-1]
        if not records or any(not record.strip() for record in records):
            invalid.append(relative)
        samples[relative] = payload
    if invalid:
        details = "\n".join(f"    {relative}" for relative in invalid)
        raise SystemExit(f"Required upstream samples contain no records or blank records:\n{details}")
    return samples


def main(argv: list[str] | None = None) -> int:
    """Save byte-exact reference slices and rebuild their format and hash manifest."""
    argv = sys.argv[1:] if argv is None else argv
    root = source_root(argv[0] if argv else None)
    data_root = root / "sf_3" / "output_data"
    # Coverage is all-or-nothing: a missing or blank late sample must not leave
    # earlier fixtures overwritten or replace the manifest with unusable entries.
    samples = read_samples(data_root)

    manifest_files: dict[str, dict[str, object]] = {}

    for relative, payload in samples.items():
        source = data_root / relative
        target = FIXTURE_ROOT / relative
        target.parent.mkdir(parents=True, exist_ok=True)
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
