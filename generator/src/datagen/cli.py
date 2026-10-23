"""Command-line entry point.

::

    datagen --scale 0.01 --out data/            # small demo run
    datagen --scale 1.0  --out data/ --clean    # full run
    datagen --scale 0.01 --no-duplicates        # clean data, for ingest tests
"""

from __future__ import annotations

import argparse
import dataclasses
import shutil
import sys

from .batches import generate
from .config import GenConfig, ProblemRates


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="datagen", description="Synthetic TPC-DI data generator")
    parser.add_argument("--scale", type=float, default=1.0, help="scale factor (default 1.0)")
    parser.add_argument("--out", default="data", help="output root (default ./data)")
    parser.add_argument("--seed", type=int, default=20261018, help="run seed for reproducibility")
    parser.add_argument("--batches", type=int, default=3, help="number of batches (default 3)")
    parser.add_argument("--incremental-ratio", type=float, default=0.02, help="delta size vs Batch1")
    parser.add_argument("--clean", action="store_true", help="remove the output root first")
    parser.add_argument("--no-duplicates", action="store_true", help="disable duplicate injection")
    parser.add_argument("--no-skew", action="store_true", help="disable skew injection")
    parser.add_argument("--no-late", action="store_true", help="disable late-arrival injection")
    parser.add_argument("--no-schema-evolution", action="store_true", help="disable schema evolution")
    parser.add_argument("--late-arrival-rate", type=float, default=None)
    parser.add_argument("--duplicate-rate-offline", type=float, default=None)
    parser.add_argument("--duplicate-rate-streaming", type=float, default=None)
    return parser


def config_from_args(args: argparse.Namespace) -> GenConfig:
    problems = ProblemRates()
    overrides = {}
    if args.no_duplicates:
        overrides["duplicate_rate_offline"] = 0.0
        overrides["duplicate_rate_streaming"] = 0.0
    if args.no_skew:
        overrides["trade_status_skew"] = 0.0
        overrides["trade_type_skew"] = 0.0
        overrides["join_skew"] = 0.0
    if args.no_late:
        overrides["late_arrival_rate"] = 0.0
    if args.no_schema_evolution:
        overrides["schema_evolution_batch"] = 0
    if args.late_arrival_rate is not None:
        overrides["late_arrival_rate"] = args.late_arrival_rate
    if args.duplicate_rate_offline is not None:
        overrides["duplicate_rate_offline"] = args.duplicate_rate_offline
    if args.duplicate_rate_streaming is not None:
        overrides["duplicate_rate_streaming"] = args.duplicate_rate_streaming
    if overrides:
        problems = dataclasses.replace(problems, **overrides)

    return GenConfig(
        scale_factor=args.scale,
        seed=args.seed,
        batch_count=args.batches,
        incremental_ratio=args.incremental_ratio,
        problems=problems,
        output_root=args.out,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.clean:
        shutil.rmtree(args.out, ignore_errors=True)

    config = config_from_args(args)
    summary = generate(config)

    total_rows = sum(int(f["rows"]) for f in summary.files if int(f["rows"]) > 0)
    print(f"Wrote {len(summary.files)} files across {len(summary.batches)} batches to {args.out}/")
    for batch, batch_date in summary.batch_dates.items():
        print(f"  {batch}: {batch_date}")
    print(f"  approx rows: {total_rows:,}  scale: {config.scale_factor}  seed: {config.seed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
