"""Measure the characteristics of a generated dataset.

The rubric asks us to document, after generating: which column is skewed and at
what ratio, which column has high cardinality (defined in the rubric as
unique-values / total-values), what the schema looks like before and after
evolution, the duplicate ratio, and the data volume. All five are questions
about a file on disk, so they get measured here rather than asserted at the
call site.

Run it over a generated run::

    python -m datagen.inspect ../sample_run
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ColumnProfile:
    name: str
    rows: int
    distinct: int
    top_value: str
    top_count: int
    empty: int

    @property
    def cardinality_ratio(self) -> float:
        """Rubric definition: unique values / total values."""
        return self.distinct / self.rows if self.rows else 0.0

    @property
    def top_share(self) -> float:
        return self.top_count / self.rows if self.rows else 0.0

    def as_dict(self) -> dict[str, object]:
        return {
            "column": self.name,
            "rows": self.rows,
            "distinct": self.distinct,
            "cardinality_ratio": round(self.cardinality_ratio, 6),
            "top_value": self.top_value,
            "top_count": self.top_count,
            "top_share": round(self.top_share, 6),
            "empty": self.empty,
        }


@dataclass
class FileProfile:
    path: str
    bytes: int
    lines: int
    fields: int
    duplicates: int
    duplicate_rate: float
    columns: list[ColumnProfile] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "bytes": self.bytes,
            "lines": self.lines,
            "fields": self.fields,
            "duplicates": self.duplicates,
            "duplicate_rate": round(self.duplicate_rate, 6),
            "columns": [column.as_dict() for column in self.columns],
        }


def _read_lines(path: Path) -> list[str]:
    return [line for line in path.read_bytes().decode("utf-8", errors="replace").split("\r\n") if line]


def profile_file(path: Path, delimiter: str = "|", column_indexes: dict[str, int] | None = None) -> FileProfile:
    """Profile one delimited file: volume, field count, duplicates, column stats."""
    lines = _read_lines(path)
    lines = list(dict.fromkeys(lines)) if not lines else lines
    total = len(lines)
    unique = len(set(lines))
    duplicates = total - unique
    fields = len(lines[0].split(delimiter)) if lines else 0

    columns: list[ColumnProfile] = []
    if lines and delimiter:
        rows = [line.split(delimiter) for line in lines]
        width = min(len(row) for row in rows)
        for index in range(width):
            values = [row[index] for row in rows]
            counter = Counter(values)
            top_value, top_count = counter.most_common(1)[0] if counter else ("", 0)
            label = (column_indexes or {}).get(index, f"col{index + 1}")
            columns.append(
                ColumnProfile(
                    name=label,
                    rows=total,
                    distinct=len(counter),
                    top_value=top_value,
                    top_count=top_count,
                    empty=counter.get("", 0),
                )
            )

    return FileProfile(
        path=str(path),
        bytes=path.stat().st_size,
        lines=total,
        fields=fields,
        duplicates=duplicates,
        duplicate_rate=duplicates / total if total else 0.0,
        columns=columns,
    )


# Column names per file, so the report reads in domain terms rather than col3.
TRADE_COLUMNS_B1 = {
    0: "T_ID", 1: "T_DTS", 2: "T_ST_ID", 3: "T_TT_ID", 4: "T_IS_CASH",
    5: "T_S_SYMB", 6: "T_QTY", 7: "T_BID_PRICE", 8: "T_CA_ID",
    9: "T_EXEC_NAME", 10: "T_TRADE_PRICE", 11: "T_CHRG", 12: "T_COMM", 13: "T_TAX",
}
TRADE_COLUMNS_BN = {
    0: "op", 1: "row_id", 2: "TH_TRADE_ID", 3: "T_DTS", 4: "T_ST_ID",
    5: "T_TT_ID", 6: "T_IS_CASH", 7: "T_S_SYMB", 8: "T_QTY",
    9: "T_BID_PRICE", 10: "T_CA_ID", 11: "T_EXEC_NAME", 12: "T_TRADE_PRICE",
    13: "T_CHRG", 14: "T_COMM", 15: "T_TAX",
}
CASH_COLUMNS = {0: "CT_CA_ID", 1: "CT_DTS", 2: "CT_AMT", 3: "CT_NAME"}
CASH_COLUMNS_BN = {0: "op", 1: "row_id", 2: "CT_CA_ID", 3: "CT_DTS", 4: "CT_AMT", 5: "CT_NAME"}

# The literal delimiter character, not the emit.py delimiter *name*.
PIPE = "|"

KNOWN = {
    "Trade.txt": (PIPE, TRADE_COLUMNS_B1),
    "CashTransaction.txt": (PIPE, CASH_COLUMNS),
    "DailyMarket.txt": (PIPE, {0: "DM_DATE", 1: "DM_S_SYMB", 2: "DM_CLOSE", 3: "DM_HIGH", 4: "DM_LOW", 5: "DM_VOL"}),
    "WatchHistory.txt": (PIPE, {0: "W_C_ID", 1: "W_S_SYMB", 2: "W_DTS", 3: "W_ACTION"}),
    "HoldingHistory.txt": (PIPE, {0: "HH_H_T_ID", 1: "HH_T_ID", 2: "HH_BEFORE_QTY", 3: "HH_AFTER_QTY"}),
    "TradeHistory.txt": (PIPE, {0: "TH_TRADE_ID", 1: "TRADE_T", 2: "TH_ST_ID"}),
}


def columns_for(filename: str, batch: str) -> dict[str, int] | None:
    """Column index -> domain name for a file in a given batch.

    The delta batches carry the op-code + row-id prefix, which shifts every
    index, so the map depends on which batch the file came from.
    """
    columns = KNOWN.get(filename, (PIPE, None))[1]
    if batch != "Batch1":
        if filename == "Trade.txt":
            return TRADE_COLUMNS_BN
        if filename == "CashTransaction.txt":
            return CASH_COLUMNS_BN
    return columns


def profile_generated_file(root: Path, filename: str, batch: str = "Batch1") -> FileProfile:
    """Profile one file from a generated run, applying its domain column names."""
    path = Path(root) / batch / filename
    if not path.exists():
        raise FileNotFoundError(path)
    return profile_file(path, PIPE, columns_for(filename, batch))


def profile_run(root: Path) -> dict[str, object]:
    """Profile every delimited file across every batch under ``root``."""
    batches: dict[str, object] = {}
    total_bytes = 0
    for batch_dir in sorted(root.glob("Batch*")):
        files: list[dict[str, object]] = []
        batch_bytes = 0
        for path in sorted(batch_dir.iterdir()):
            if path.suffix != ".txt" or path.name == "BatchDate.txt":
                continue
            delimiter, columns = KNOWN.get(path.name, (PIPE, None))
            if batch_dir.name != "Batch1":
                # The delta batches carry the op-code + row-id prefix, which
                # shifts every column index by two.
                if path.name == "Trade.txt":
                    columns = TRADE_COLUMNS_BN
                elif path.name == "CashTransaction.txt":
                    columns = CASH_COLUMNS_BN
            profile = profile_file(path, delimiter, columns)
            files.append(profile.as_dict())
            batch_bytes += profile.bytes
        batches[batch_dir.name] = {"bytes": batch_bytes, "files": files}
        total_bytes += batch_bytes

    return {
        "root": str(root),
        "total_bytes": total_bytes,
        "batches": batches,
    }


def render(report: dict[str, object]) -> str:
    """Human-readable table, the form that goes in docs/."""
    lines: list[str] = []
    total = int(report["total_bytes"])
    lines.append(f"root: {report['root']}")
    lines.append(f"total size: {total:,} bytes ({total / 1_048_576:.2f} MiB)")
    lines.append("")
    for batch_name, batch in report["batches"].items():
        assert isinstance(batch, dict)
        lines.append(f"== {batch_name}  ({int(batch['bytes']):,} bytes) ==")
        for entry in batch["files"]:
            assert isinstance(entry, dict)
            lines.append(
                f"  {os.path.basename(str(entry['path'])):<24} "
                f"rows={int(entry['lines']):>9,}  fields={entry['fields']:>2}  "
                f"dup={entry['duplicate_rate']:.4f}"
            )
            for column in entry["columns"]:
                assert isinstance(column, dict)
                share = float(column["top_share"])
                ratio = float(column["cardinality_ratio"])
                flag = ""
                if share >= 0.5:
                    flag = "  <-- skewed"
                elif ratio >= 0.5:
                    flag = "  <-- high cardinality"
                lines.append(
                    f"      {str(column['column']):<16} distinct={int(column['distinct']):>9,}  "
                    f"card={ratio:.4f}  top={str(column['top_value'])[:24]:<24} "
                    f"share={share:.4f}{flag}"
                )
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="datagen.inspect", description="Profile a generated dataset")
    parser.add_argument("root", help="run root, e.g. ../sample_run")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = parser.parse_args(argv)

    report = profile_run(Path(args.root))
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(render(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
