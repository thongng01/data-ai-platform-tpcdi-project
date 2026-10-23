"""Generation configuration and the scale model.

Two concerns live here:

- :class:`GenConfig` is the single knob-set the caller controls. It carries the
  scale factor and every data-problem rate, so the whole surface of what the
  generator can emit is one object.
- :func:`row_counts` turns a scale factor into per-table row counts. Row counts
  scale linearly off a small table of base cardinalities, the same shape
  TPC-DI uses for its own scale factors.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Literal

# Base cardinalities at scale factor 1.0. These are the reference points the
# scale factor multiplies; they are not sf_3 measurements, since sf_3 is a
# format reference rather than our target dataset.
BASE_ROW_COUNTS: dict[str, int] = {
    # Reference data (Batch1 only). Fixed in the spec, but scaled so a large
    # run can still stress the reference joins.
    "tax_rate": 320,
    "industry": 102,
    "status_type": 6,
    "trade_type": 5,
    "date": 25_933,
    "time": 86_400,
    "finwire_cmp": 5_000,
    "finwire_sec": 5_000,
    "finwire_fin": 5_000,
    # Entities.
    "hr": 15_000,
    "prospect": 14_982,
    "customer": 4_728,
    "account": 12_860,
    "trade": 390_978,
    "trade_history": 982_180,
    "cash_transaction": 361_856,
    "holding_history": 362_342,
    "daily_market": 1_282_768,
    "watch_history": 900_042,
}

# Multipliers for the incremental batches, relative to their Batch1 parent.
# Batch2/Batch3 are deltas in TPC-DI; at sf_3 they are tiny (hundreds of rows),
# so these are configurable rather than fixed to the sample ratio.
INCREMENTAL_RATIO = 0.02


@dataclass(frozen=True)
class ProblemRates:
    """Knobs for every data problem the generator can inject.

    All rates are fractions in ``[0.0, 1.0]``. A rate of ``0.0`` disables the
    problem entirely. Defaults are deliberately non-zero for the problems the
    rubric asks us to demonstrate, so a default run already produces something
    worth measuring.
    """

    # Skew: force this share of rows onto a single key, producing one hot key.
    trade_status_skew: float = 0.92
    trade_type_skew: float = 0.30
    join_skew: float = 0.0
    join_skew_key: str = "CT_CA_ID"

    # Duplicates. Offline duplicates are whole-row repeats in Batch1 files;
    # streaming duplicates are repeats in the Batch2/3 replay stream.
    duplicate_rate_offline: float = 0.02
    duplicate_rate_streaming: float = 0.015

    # Schema evolution: inject a new column starting at this batch. 0 disables.
    schema_evolution_batch: int = 2
    schema_evolution_column: str = "T_STOP_PRICE"

    # Streaming time problems, expressed in seconds.
    late_arrival_rate: float = 0.08
    late_arrival_max_delay_s: int = 600
    out_of_order_rate: float = 0.05

    def validate(self) -> None:
        for name, value in vars(self).items():
            if name.endswith("_rate") or name.endswith("_skew"):
                if not 0.0 <= value <= 1.0:
                    raise ValueError(f"{name} must be in [0.0, 1.0], got {value}")
        if self.late_arrival_max_delay_s < 0:
            raise ValueError("late_arrival_max_delay_s must be >= 0")


@dataclass(frozen=True)
class GenConfig:
    """Everything the generator needs to produce a run."""

    scale_factor: float = 1.0
    seed: int = 20261018
    batch_count: int = 3
    problems: ProblemRates = field(default_factory=ProblemRates)
    incremental_ratio: float = INCREMENTAL_RATIO
    #: Tables to emit. Empty means "everything we have a spec for".
    only_tables: tuple[str, ...] = ()
    output_root: str = "data"

    def __post_init__(self) -> None:
        if self.scale_factor <= 0:
            raise ValueError("scale_factor must be > 0")
        if self.batch_count < 1:
            raise ValueError("batch_count must be >= 1")
        self.problems.validate()

    def for_batch(self, batch_index: int) -> "GenConfig":
        """Return a config scoped to one batch.

        Schema evolution is expressed as a batch number, so a per-batch view of
        the config lets the emit layer ask "is this column live yet?" without
        re-deriving batch identity.
        """
        return replace(
            self,
            problems=replace(
                self.problems,
                schema_evolution_batch=self.problems.schema_evolution_batch
                if batch_index >= self.problems.schema_evolution_batch
                else 0,
            ),
        )

    def batch_date(self, batch_index: int) -> str:
        """ISO batch date. Batch 1 is 2017-07-07, matching TPC-DI."""
        from datetime import date, timedelta

        return (date(2017, 7, 7) + timedelta(days=batch_index - 1)).isoformat()


def row_counts(config: GenConfig) -> dict[str, int]:
    """Per-table row counts for Batch1 at this config's scale factor."""
    return {
        table: max(1, int(round(base * config.scale_factor)))
        for table, base in BASE_ROW_COUNTS.items()
    }


def incremental_counts(config: GenConfig) -> dict[str, int]:
    """Row counts for each incremental batch.

    Only the tables TPC-DI ships as deltas appear here. The rest are Batch1-only.
    """
    base = row_counts(config)
    delta_tables = (
        "customer",
        "account",
        "trade",
        "cash_transaction",
        "holding_history",
        "daily_market",
        "watch_history",
    )
    return {
        table: max(1, int(round(base[table] * config.incremental_ratio)))
        for table in delta_tables
    }


TablesToEmit = Literal["reference", "entities", "events", "all"]
