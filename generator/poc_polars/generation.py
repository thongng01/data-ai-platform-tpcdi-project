from pathlib import Path

import polars as pl

from .config import Config
from .formats import load_sources
from .sorting import SortedOutput


def padded(expression):
    return expression.cast(pl.String).str.pad_start(15, "0")


def order_key(*expressions):
    return pl.concat_str(expressions, separator="/").alias("_key")


class Generation:
    def __init__(self, config: Config, root: Path):
        self.config = config
        self.sources = load_sources()
        self.output = SortedOutput(root, config.merge_fan_in)
        self.specifications = {}
        for batch in range(1, config.incremental_days + 2):
            names = (list(self.sources) if batch == 1 else
                     [name for name, source in self.sources.items() if source.cdc] + ["Prospect"])
            for name in names:
                if batch == 1 and name in ("Account", "Customer"):
                    continue
                source = self.sources[name]
                self.specifications[f"Batch{batch}/{source.filename}"] = (source.cdc and batch > 1, False)
            self.specifications[f"Batch{batch}/BatchDate.txt"] = (False, False)
        self.specifications["Batch1/CustomerMgmt.xml"] = (False, True)

    def emit(self, name: str, batch: int, frame: pl.DataFrame):
        source = self.sources[name]
        cdc = source.cdc and batch > 1
        if cdc and "_flag" not in frame.columns:
            frame = frame.with_columns(pl.lit("I").alias("_flag"))
        self.output.add(f"Batch{batch}/{source.filename}", frame, source, cdc)

    def text(self, relative: str, rows: list[tuple[str, str]]):
        if not rows:
            return
        self.specifications.setdefault(relative, (False, False))
        self.output.add(relative, pl.DataFrame(rows, schema=["_key", "_payload"], orient="row"))

    def finish(self):
        return self.output.finish(self.specifications)


def account_group(config: Config, first: int, last: int, day: int) -> pl.DataFrame:
    customers = pl.DataFrame({"customer": pl.int_range(first, last, eager=True)})
    customers = customers.with_columns(
        pl.when(pl.col("customer") <= config.customers).then(0).otherwise(
            config.historical_days + (pl.col("customer") - config.customers - 1) // max(1, config.new_customers_per_day)
        ).alias("created"),
        pl.when((pl.col("customer") <= config.customers) & (pl.col("customer") % 11 == 0))
        .then(config.historical_days - 1).otherwise(config.total_days + 10).alias("closed"),
    )
    accounts = customers.join(pl.DataFrame({"slot": [1, 2, 3]}), how="cross")
    return accounts.with_columns(
        ((pl.col("customer") - 1) * 3 + pl.col("slot")).alias("account"),
        pl.when(pl.col("slot") == 3).then(2).otherwise(pl.col("created")).alias("created"),
        pl.when((pl.col("slot") == 2) & (pl.col("customer") % 7 == 0) & (pl.col("customer") <= config.customers))
        .then(config.historical_days - 2).otherwise(pl.col("closed")).alias("closed"),
    ).filter(
        ((pl.col("slot") != 3) | ((pl.col("customer") % 5 == 0) & (pl.col("customer") <= config.customers)))
        & (pl.col("created") <= day) & (pl.col("closed") > day)
    )
