"""The schema contract between the generator and the lakehouse.

Everything the pipeline knows about a source file's shape lives here, in one
place, because the schema changes between batches on purpose and the readers
must not guess.

The shape problem is specific. Batch1 `Trade.txt` has 14 pipe-delimited
fields. Batch2 has 16, because an operation code and a business history id
appear at the front. A reader that infers the schema once from Batch1 reads
Batch2 shifted by two columns and produces plausible numbers, which is worse
than failing. So the schema is declared here, per batch, and the reader
selects the variant by counting fields.

Column naming: source columns keep their TPC-DI meaning and get an
`ingest_` prefix only when the name would collide with a provenance column.
Bronze adds four provenance columns; silver drops them except for the batch
id, which is needed to decide which version of a row is current.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Provenance columns added to every bronze table. They are the only thing that
# separates bronze from "the file, but in Parquet".
PROVENANCE_COLUMNS = (
    "ingest_batch_id",
    "ingest_batch_seq",
    "ingest_ts",
    "ingest_source_file",
)


@dataclass(frozen=True)
class TableSchema:
    """One source file: its name, its delimiter, and its per-batch columns.

    Attributes:
        name: Source file stem, e.g. ``Trade``.
        table: Silver table name, e.g. ``stg_trade``.
        bronze_table: Bronze table name, e.g. ``raw_trade``.
        delimiter: The delimiter this file family uses.
        columns: A mapping from batch number to that batch's column list.
        business_key: Columns that identify the real-world thing, not the row.
        event_time: The column that says when the event happened, not when the
            row was delivered. Partitioning uses this, not arrival order.
        skip_header: True for files whose first line is column names.
    """

    name: str
    table: str
    bronze_table: str
    delimiter: str
    columns: dict[int, tuple[str, ...]]
    business_key: tuple[str, ...] = ()
    historical_business_key: tuple[str, ...] = ()
    event_time: str | None = None
    skip_header: bool = False

    @property
    def widest_batch(self) -> int:
        """The highest batch number this schema declares."""
        return max(self.columns)

    def key_for(self, batch_seq: int) -> tuple[str, ...]:
        """Return the business key this batch uses.

        By default the key is every declared column for that batch, which is a
        whole-record identity. That matches the glossary: a duplicate is the
        same real-world record delivered twice, a whole record repeated. It is
        also the only key that is safe by default, because a partial key quietly
        collapses genuinely different rows. Two partial keys did exactly that
        during the build: keying watch history on customer, ticker and action
        removed 62 percent of the table, and keying trade history on business id
        and status removed 58 percent, against an injected duplicate rate of 2
        percent. A key narrower than the record only answers "do these rows
        agree on some columns", not "is this the same record twice".

        Trade is the one exception, and it needs one because its two shapes do
        not agree on what identifies it. In a delta batch the identity is the
        history id plus the status, because one trade is delivered twice, once
        submitted and once completed. In the historical load the history id does
        not exist and there is only ever one row per trade, so the identity is
        the row id.

        Args:
            batch_seq: The batch number.

        Returns:
            The key columns for that batch.
        """
        if batch_seq == 1 and self.historical_business_key:
            return self.historical_business_key
        if self.business_key:
            return self.business_key
        return self.columns_for(batch_seq)

    def columns_for(self, batch: int) -> tuple[str, ...]:
        """Return the column list for one batch.

        A batch with no declared variant falls back to the closest declared
        batch at or below it, which is the historical-load case: every future
        batch carries the Batch2+ shape once the prefix pair has appeared.

        Args:
            batch: The batch number.

        Returns:
            The column names in file order.
        """
        if batch in self.columns:
            return self.columns[batch]
        candidates = [b for b in self.columns if b <= batch]
        if not candidates:
            return self.columns[min(self.columns)]
        return self.columns[max(candidates)]

    def column_count(self, batch: int) -> int:
        """Return how many fields this batch's rows have.

        Args:
            batch: The batch number.

        Returns:
            The field count.
        """
        return len(self.columns_for(batch))


TRADE_PAYLOAD = (
    "trade_ts",
    "status",
    "trade_type",
    "is_cash",
    "ticker",
    "share_count",
    "bid_price",
    "account_id",
    "exec_name",
    "trade_price",
    "charge_amount",
    "commission",
    "tax_amount",
)

# The delta batches prefix every row with an operation code and a surrogate row
# id, then the business history id. That prefix pair is the schema evolution.
DELTA_PREFIX = ("operation", "row_id", "business_id")

TRADE = TableSchema(
    name="Trade",
    table="stg_trade",
    bronze_table="raw_trade",
    delimiter="|",
    columns={
        1: ("row_id", *TRADE_PAYLOAD),
        2: (*DELTA_PREFIX, *TRADE_PAYLOAD),
    },
    # A trade's business identity is the history id, not the row id. In a delta
    # batch the same trade appears twice: once submitted, once completed. Both
    # rows share the business id, so keying on row_id models two trades.
    business_key=("business_id", "status"),
    # In the historical load there is no history id and exactly one row per
    # trade, so the row id is the identity. Keying the historical load on a
    # column that does not exist yet collapses every trade into one row per
    # status, which is the failure this line exists to prevent.
    historical_business_key=("row_id", "status"),
    event_time="trade_ts",
)

CASH_TRANSACTION = TableSchema(
    name="CashTransaction",
    table="stg_cash_transaction",
    bronze_table="raw_cash_transaction",
    delimiter="|",
    columns={
        1: ("account_id", "posted_ts", "amount", "ct_name"),
        2: ("operation", "row_id", "account_id", "posted_ts", "amount", "ct_name"),
    },
    # No key declared, so it is every column. CT_AMT is signed to 2dp and
    # CT_NAME is long random text with spaces, so two genuine rows agreeing on
    # every column is not a coincidence.
    event_time="posted_ts",
)

DAILY_MARKET = TableSchema(
    name="DailyMarket",
    table="stg_daily_market",
    bronze_table="raw_daily_market",
    delimiter="|",
    columns={
        1: ("market_date", "ticker", "close", "high", "low", "volume"),
        2: (
            "operation",
            "row_id",
            "market_date",
            "ticker",
            "close",
            "high",
            "low",
            "volume",
        ),
    },
    # date and ticker look like a key, and in this generator they happen to be
    # one, but close, high, low and volume would then be dropped without being
    # compared. Keying on the whole record makes the check stricter for free.
    event_time="market_date",
)

WATCH_HISTORY = TableSchema(
    name="WatchHistory",
    table="stg_watch_history",
    bronze_table="raw_watch_history",
    delimiter="|",
    columns={
        1: ("customer_id", "ticker", "watched_ts", "action"),
        2: ("operation", "row_id", "customer_id", "ticker", "watched_ts", "action"),
    },
    # customer, ticker and action was a key here and it was wrong. A customer
    # watching the same ticker with the same action on different minutes is two
    # real events, and that key collapsed 62 percent of the table.
    event_time="watched_ts",
)

HOLDING_HISTORY = TableSchema(
    name="HoldingHistory",
    table="stg_holding_history",
    bronze_table="raw_holding_history",
    delimiter="|",
    columns={
        1: ("holding_trade_id", "business_id", "quantity_before", "quantity_after"),
        2: (
            "operation",
            "row_id",
            "holding_trade_id",
            "business_id",
            "quantity_before",
            "quantity_after",
        ),
    },
    # Whole record. The quantities change as a holding moves, so a key that
    # stopped at holding_trade_id would call two different holdings the same.
    event_time=None,
)

TRADE_HISTORY = TableSchema(
    name="TradeHistory",
    table="stg_trade_history",
    bronze_table="raw_trade_history",
    delimiter="|",
    columns={1: ("business_id", "trade_t", "status")},
    # Whole record. Keying on business id and status alone removed 58 percent of
    # the table, because a trade legitimately has many history rows.
    event_time="trade_t",
)

STATUS_TYPE = TableSchema(
    name="StatusType",
    table="stg_status_type",
    bronze_table="raw_status_type",
    delimiter="|",
    columns={1: ("status_id", "status_name")},
)

TRADE_TYPE = TableSchema(
    name="TradeType",
    table="stg_trade_type",
    bronze_table="raw_trade_type",
    delimiter="|",
    columns={1: ("trade_type_id", "trade_type_name", "is_sell", "is_market")},
)

INDUSTRY = TableSchema(
    name="Industry",
    table="stg_industry",
    bronze_table="raw_industry",
    delimiter="|",
    columns={1: ("industry_id", "industry_name", "sector_id")},
)

TAX_RATE = TableSchema(
    name="TaxRate",
    table="stg_tax_rate",
    bronze_table="raw_tax_rate",
    delimiter="|",
    columns={1: ("tax_id", "tax_name", "tax_rate")},
)

# Column types applied when bronze becomes silver. Anything not named here
# stays a string. Declaring them centrally means a type change is one edit and
# one test, not a hunt through job code.
COLUMN_TYPES: dict[str, str] = {
    "row_id": "bigint",
    "account_id": "bigint",
    "business_id": "bigint",
    "holding_trade_id": "bigint",
    "customer_id": "bigint",
    "share_count": "bigint",
    "volume": "bigint",
    "quantity_before": "bigint",
    "quantity_after": "bigint",
    "is_cash": "boolean",
    "is_sell": "boolean",
    "is_market": "boolean",
    "trade_price": "decimal(18,4)",
    "bid_price": "decimal(18,4)",
    "charge_amount": "decimal(18,4)",
    "commission": "decimal(18,4)",
    "tax_amount": "decimal(18,4)",
    "amount": "decimal(18,4)",
    "tax_rate": "decimal(18,6)",
    "close": "decimal(18,4)",
    "high": "decimal(18,4)",
    "low": "decimal(18,4)",
    "trade_ts": "timestamp",
    "posted_ts": "timestamp",
    "watched_ts": "timestamp",
    "trade_t": "timestamp",
    "market_date": "date",
}

# Tables that only exist in the historical load. A delta batch never carries
# them, so the reader skips them rather than failing on a missing file.
HISTORICAL_ONLY = {schema.name for schema in (TRADE_HISTORY, STATUS_TYPE, TRADE_TYPE, INDUSTRY, TAX_RATE)}

DELTA_TABLES = (TRADE, CASH_TRANSACTION, DAILY_MARKET, WATCH_HISTORY, HOLDING_HISTORY)

REFERENCE_TABLES = (STATUS_TYPE, TRADE_TYPE, INDUSTRY, TAX_RATE)

ALL_TABLES: tuple[TableSchema, ...] = (*DELTA_TABLES, TRADE_HISTORY, *REFERENCE_TABLES)

SCHEMAS_BY_NAME: dict[str, TableSchema] = {schema.name: schema for schema in ALL_TABLES}


# The CustomerMgmt XML stream carries customer and account changes. It is the
# source of the slowly changing dimensions, so it is handled separately from
# the delimited files: it is XML, it has actions rather than rows, and an update
# is a partial payload that carries only the fields that changed.
CUSTOMER_MGMT = TableSchema(
    name="CustomerMgmt",
    table="stg_customer_change",
    bronze_table="raw_customer_change",
    delimiter=",",
    columns={1: ("operation", "customer_id", "payload")},
    business_key=("customer_id",),
)


@dataclass(frozen=True)
class ZoneLayout:
    """Where each zone's tables live and how they are partitioned.

    Attributes:
        landing_prefix: Landing path inside the landing bucket.
        bronze_prefix: Bronze path inside the lakehouse bucket.
        silver_prefix: Silver path inside the lakehouse bucket.
        partition_column: The column every fact-style table partitions on.
    """

    landing_prefix: str = "tpcdi"
    bronze_prefix: str = "bronze"
    silver_prefix: str = "silver"
    partition_column: str = "dt"

    def landing_dir(self, batch_id: str) -> str:
        """Return the landing directory for one batch."""
        return f"{self.landing_prefix}/{batch_id}"

    def bronze_dir(self, table: str) -> str:
        """Return the bronze directory for one table."""
        return f"{self.bronze_prefix}/{table}"

    def silver_dir(self, table: str) -> str:
        """Return the silver directory for one table."""
        return f"{self.silver_prefix}/{table}"


LAYOUT = ZoneLayout()

# Column order for the CustomerMgmt XML action stream. The generator emits these
# attributes; the order here is the order they appear in the file.
CUSTOMER_ATTRS = (
    "customer_id",
    "salutation",
    "first_name",
    "last_name",
    "birth_date",
    "email",
    "address_line_1",
    "address_line_2",
    "city",
    "state",
    "zip",
    "country",
    "phone",
    "status",
    "segment",
    "income",
    "credit_score",
    "net_worth",
    "customer_since",
    "marketing_ok",
    "preferred_channel",
)

ACCOUNT_ATTRS = (
    "account_id",
    "customer_id",
    "account_type",
    "status",
    "taxable",
    "market",
    "opened_date",
    "closed_date",
    "broker",
    "employee_code",
)

# An account attribute list keyed by the number of attributes present. TPC-DI
# uses a bare element, so the count is the only signal of which kind it is.
_ACCOUNT_FIELD_COUNTS = {
    9: (1, 2, 3, 4, 5, 6, 7, 8, 9),
    10: (1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
}


@dataclass
class BatchManifest:
    """What the generator recorded about a run, read from its manifest file.

    Attributes:
        batch_id: The batch identifier, e.g. ``Batch1``.
        batch_seq: The batch number as an integer, 1 for the historical load.
        batch_date: The business date the batch carries.
        tables: Mapping of table name to the row counts the generator emitted.
        problems: The injection rates the run was configured with.
    """

    batch_id: str
    batch_seq: int
    batch_date: str
    tables: dict[str, int] = field(default_factory=dict)
    problems: dict[str, float] = field(default_factory=dict)