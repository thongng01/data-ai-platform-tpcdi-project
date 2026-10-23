"""Batch orchestration.

A run emits ``batch_count`` batches into ``<output_root>/Batch<N>/``:

- **Batch1** is the full historical load: every entity, every reference table,
  the CustomerMgmt action stream, and the FINWIRE quarters. It carries the
  offline data problems.
- **Batch2..N** are incremental deltas: only the streamed entities, each row
  prefixed with an ``I``/``U`` operation code and a surrogate row id, and each
  carrying the streaming data problems (duplicates, and the schema change the
  prefix itself introduces).

Every batch writes a ``BatchDate.txt``, because the loader reads it.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from .config import GenConfig, incremental_counts, row_counts
from .specs_events import (
    write_cash_transaction,
    write_customer_mgmt,
    write_daily_market,
    write_holding_history,
    write_trade,
    write_trade_history,
    write_watch_history,
)
from .specs_reference import write_finwire, write_reference_tables
from .emit import SingleColumnWriter


@dataclass
class RunSummary:
    batches: list[str]
    files: list[dict[str, object]]
    counts: dict[str, int]
    batch_dates: dict[str, str]

    def as_dict(self) -> dict[str, object]:
        return {
            "batches": self.batches,
            "files": self.files,
            "counts": self.counts,
            "batch_dates": self.batch_dates,
        }


def generate(config: GenConfig) -> RunSummary:
    """Emit the full run described by ``config``. Returns a summary."""
    root = config.output_root
    counts = row_counts(config)
    deltas = incremental_counts(config)
    # Customer and account counts drive the FK ranges every fact draws from.
    max_customer = max(1, counts["customer"] - 1)
    max_account = max(1, counts["account"] - 1)

    summary = RunSummary(batches=[], files=[], counts=counts, batch_dates={})
    symbol_seed = config.seed ^ 0x5EED
    symbol_count = min(1_805, max(50, counts["daily_market"] // 700))
    from .specs_events import _symbol_pool

    symbols = _symbol_pool(symbol_seed, symbol_count)

    for batch in range(1, config.batch_count + 1):
        batch_root = os.path.join(root, f"Batch{batch}")
        os.makedirs(batch_root, exist_ok=True)
        batch_config = config.for_batch(batch)
        batch_date = config.batch_date(batch)
        summary.batches.append(batch_root)
        summary.batch_dates[f"Batch{batch}"] = batch_date

        # BatchDate.txt: a bare ISO date, no delimiter, no trailing field.
        with SingleColumnWriter(os.path.join(batch_root, "BatchDate.txt")) as w:
            w.write_row([batch_date])
        summary.files.append({"path": os.path.join(batch_root, "BatchDate.txt"), "rows": 1, "kind": "date"})

        if batch == 1:
            for path in write_reference_tables(batch_root, counts, config.seed):
                summary.files.append({"path": path, "rows": -1, "kind": "reference"})
            for path in write_finwire(batch_root, counts, config.seed):
                summary.files.append({"path": path, "rows": -1, "kind": "finwire"})
            path = write_customer_mgmt(
                batch_root,
                count=counts["customer"],
                accounts_per_customer=3,
                problems=batch_config.problems,
                seed=config.seed,
            )
            summary.files.append({"path": path, "rows": -1, "kind": "xml"})
            # TradeHistory is Batch1-only in TPC-DI; the delta batches carry the
            # trade status change inline in Trade.txt via the TH_TRADE_ID column.
            path = write_trade_history(
                batch_root,
                count=counts["trade_history"],
                max_trade=max(1, counts["trade"] - 1),
                problems=batch_config.problems,
                seed=config.seed,
            )
            summary.files.append({"path": path, "rows": counts["trade_history"], "kind": "event"})

        size = counts if batch == 1 else deltas
        # Trade
        path = write_trade(
            batch_root,
            count=size["trade"],
            symbols=symbols,
            max_account=max_account,
            problems=batch_config.problems,
            seed=config.seed,
            batch=batch,
        )
        summary.files.append({"path": path, "rows": size["trade"], "kind": "event"})
        # CashTransaction
        path = write_cash_transaction(
            batch_root,
            count=size["cash_transaction"],
            max_account=max_account,
            problems=batch_config.problems,
            seed=config.seed,
            batch=batch,
        )
        summary.files.append({"path": path, "rows": size["cash_transaction"], "kind": "event"})
        # DailyMarket
        path = write_daily_market(
            batch_root,
            count=size["daily_market"],
            symbols=symbols,
            problems=batch_config.problems,
            seed=config.seed,
            batch=batch,
        )
        summary.files.append({"path": path, "rows": size["daily_market"], "kind": "event"})
        # WatchHistory
        path = write_watch_history(
            batch_root,
            count=size["watch_history"],
            symbols=symbols,
            max_customer=max_customer,
            problems=batch_config.problems,
            seed=config.seed,
            batch=batch,
        )
        summary.files.append({"path": path, "rows": size["watch_history"], "kind": "event"})
        # HoldingHistory
        path = write_holding_history(
            batch_root,
            count=size["holding_history"],
            problems=batch_config.problems,
            seed=config.seed,
            batch=batch,
        )
        summary.files.append({"path": path, "rows": size["holding_history"], "kind": "event"})

    # A run manifest, mirroring benchbox's generation-run record: what was
    # produced, at what config, so a dataset on disk is traceable to its inputs.
    manifest = os.path.join(root, "manifest.json")
    with open(manifest, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "scale_factor": config.scale_factor,
                "seed": config.seed,
                "batch_count": config.batch_count,
                "incremental_ratio": config.incremental_ratio,
                "problems": vars(config.problems),
                "summary": summary.as_dict(),
            },
            handle,
            indent=2,
        )
    summary.files.append({"path": manifest, "rows": -1, "kind": "manifest"})
    return summary
