"""Keep family-specific preparation and publication rules in one policy per family."""

from datetime import date

import polars as pl

from .formats import trade_channel_day
from .output_formats import cash_name, native_contract


CASH_NAME_PATTERN = r"^(?:Deposit|Withdrawal|Trade settlement [1-9][0-9]{0,14}|Dividend [A-Za-z0-9._-]{1,15})$"


class SourcePolicy:
    """Prepare ordinary typed rows and encode their checked source and CDC columns."""

    order_defaults = {}
    dated_start = None

    def __init__(self, source):
        """Retain the source contract and whether publication uses daily CDC batches."""
        self.source = source
        self.cdc = source.cdc if source is not None else False

    def complete_order(self, order):
        """Keep the producer's complete ordering facts unchanged."""
        return order

    def prepare(self, frame):
        """Keep ordinary recorded columns unchanged before part storage."""
        return frame

    def contract(self, write_kind):
        """Use the same business fields for initial and daily records."""
        return self.source

    def encode(self, batch, write_kind, writer):
        """Format business fields and add per-file CDC flags and sequence numbers."""
        source = self.contract(write_kind)
        frame = native_contract(source).format(batch).with_columns(batch["_relative"])
        if source.cdc and write_kind:
            # Only the first source file can continue the previous batch's open
            # file. Later files start at one. This window covers one batch only.
            carried = (pl.when(pl.col("_relative") == writer.relative).then(writer.rows).otherwise(0)
                       if writer is not None else pl.lit(0))
            cdc = batch.select(pl.col("_flag").alias("CDC_FLAG"),
                (pl.col("_relative").cum_count().over("_relative").cast(pl.Int64) + carried).alias("CDC_DSN"))
            if cdc["CDC_DSN"].max() >= 10**12:
                raise ValueError("CDC_DSN exceeds NUM(12).")
            frame = frame.with_columns(cdc["CDC_FLAG"], cdc["CDC_DSN"]).select(
                "CDC_FLAG", "CDC_DSN", *source.columns, "_relative")
        return frame, source.separator


class CashPolicy(SourcePolicy):
    """Check cash facts, assign global cash IDs, and encode daily cash messages."""

    order_defaults = {"_reference": 0}

    def __init__(self, config, source):
        """Start one publication sequence and retain the legal recording-day range."""
        super().__init__(source)
        self.cdc = True
        self.sequence = 0
        self.first_epoch_day = (config.start - date(1970, 1, 1)).days
        self.total_days = config.total_days

    def complete_order(self, order):
        """Allow a cash row without a numeric name reference."""
        return (*order, 0) if len(order) == 1 else order

    def prepare(self, frame):
        """Expand cash names and reject timestamps and IDs outside the source contract."""
        frame = frame.with_columns(cash_name().alias("CT_NAME"))
        invalid = ((pl.col("CT_CA_ID") <= 0) | (pl.col("CT_CA_ID") >= 10**11) |
                   (pl.col("_ordinal") < 0) | (pl.col("_ordinal") >= 10**15) |
                   (pl.col("_record_day") < 0) | (pl.col("_record_day") >= self.total_days) |
                   (pl.col("CT_DTS") // 86400 != self.first_epoch_day + pl.col("_record_day")))
        if frame.select(invalid.fill_null(True).any()).item():
            raise ValueError("A cash timestamp or identity exceeds the recording contract.")
        return frame

    def encode(self, batch, write_kind, writer):
        """Continue CT_ID across cutover and use exact JSON only for daily cash."""
        if batch.select((~pl.col("CT_NAME").str.contains(CASH_NAME_PATTERN)).fill_null(True).any()).item():
            raise ValueError("Invalid CT_NAME.")
        batch = batch.with_columns(pl.int_range(
            self.sequence + 1, self.sequence + batch.height + 1).alias("CT_ID"))
        self.sequence += batch.height
        frame = native_contract(self.source).format(batch).with_columns(batch["_relative"])
        if write_kind:
            fields = [pl.col("CT_ID").cast(pl.Int64).alias("ct_id"),
                      pl.col("CT_CA_ID").cast(pl.Int64).alias("ct_ca_id"),
                      (pl.col("CT_DTS").str.replace(" ", "T", literal=True) + "Z").alias("ct_dts"),
                      pl.col("CT_AMT").alias("ct_amt"), pl.col("CT_NAME").alias("ct_name"),
                      pl.lit(1).alias("schema_version")]
            # CPython ensure_ascii escapes ASCII DEL, while serde leaves it
            # literal. Keep this rule even though legal cash names exclude DEL.
            frame = frame.select(pl.struct(fields).struct.json_encode().str.replace_all(
                "\x7f", "\\u007f", literal=True).alias("_payload"), "_relative")
        return frame, self.source.separator


class TradePolicy(SourcePolicy):
    """Keep initial trades unchanged and select the dated daily channel contract."""

    def __init__(self, config, source):
        """Resolve the first channel delivery once for this producer or publisher."""
        super().__init__(source)
        self.dated_start = (trade_channel_day(config) - config.start).days

    def prepare(self, frame):
        """Keep a nullable channel column even when no recorded row needs it."""
        if "T_CHANNEL" not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.String).alias("T_CHANNEL"))
        return frame

    def contract(self, write_kind):
        """Add the channel only to the dated daily contract."""
        return self.source.daily_version if write_kind == 2 else self.source


FAMILY_POLICIES = {"CashTransaction": CashPolicy, "Trade": TradePolicy}


def family_policies(config, sources):
    """Create private family state so publication sequences cannot leak between runs."""
    return {family: (FAMILY_POLICIES[family](config, sources[family])
                     if family in FAMILY_POLICIES else SourcePolicy(sources.get(family)))
            for family in (*sources, "CustomerMgmt", "FINWIRE", "__audit")}
