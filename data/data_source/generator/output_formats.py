"""Prepare typed source schemas, native formatting, and lexical contract checks."""

from functools import cache

import polars as pl

from .formats import TYPES, Source, _numeric_type


# These source fields carry one-based security IDs until final native formatting.
SYMBOL_FIELDS = frozenset(("T_S_SYMB", "W_S_SYMB", "DM_S_SYMB"))
RAW_FAMILIES = frozenset(("CustomerMgmt", "FINWIRE", "__audit"))
ORDER_COLUMNS = {
    "Trade": ("_ordinal",),
    "TradeHistory": ("_ordinal",),
    "CashTransaction": ("_ordinal", "_reference"),
    "HoldingHistory": ("_timestamp", "_account", "_ordinal"),
    "Customer": ("_timestamp", "_ordinal", "_action"),
    "Account": ("_timestamp", "_ordinal"),
    "CustomerMgmt": ("_timestamp", "_customer", "_ordinal", "_action"),
    "FINWIRE": ("_posting", "_rank", "_identity"),
    "Prospect": ("_identity",),
    "__audit": ("_audit_key",),
}
STRING_METADATA = frozenset(("_action", "_posting", "_audit_key"))


def input_fields(family: str, sources: dict[str, Source]) -> tuple[str, ...]:
    """Return the exact tuple and batch column names accepted from a producer."""
    if family in RAW_FAMILIES:
        return ("_payload",)
    source = sources[family]
    return tuple(field.name for field in source.fields if field.name != "CT_ID")


def field_dtype(field):
    """Keep money and dates integral, and retain explicitly lexical fixed rates."""
    datatype = TYPES.get(field.type, field.type)
    if field.name in SYMBOL_FIELDS or datatype in ("DATE", "DATETIME", "BOOLEAN"):
        return pl.Int64
    if datatype.startswith("CHAR(") or field.name == "TX_RATE":
        return pl.String
    _numeric_type(datatype)
    return pl.Int64


def input_schema(family: str, sources: dict[str, Source]) -> dict[str, pl.DataType]:
    """Describe correct source values and shared metadata without type inference."""
    if family in RAW_FAMILIES:
        schema = {"_payload": pl.String}
    else:
        schema = {field.name: field_dtype(field) for field in sources[family].fields if field.name != "CT_ID"}
    schema.update({name: pl.String if name in STRING_METADATA else pl.Int64
                   for name in ORDER_COLUMNS.get(family, ())})
    schema.update({"_record_day": pl.Int64, "_flag": pl.String, "_profile": pl.Int64,
                   "_delivery_rank": pl.Int64, "_relative": pl.String})
    return schema


def padded(value, width: int) -> pl.Expr:
    """Preserve minimum-width decimal padding, including wider integer values."""
    value = pl.col(value) if isinstance(value, str) else value
    return value.cast(pl.String).str.pad_start(width, "0")


def timestamp_text(value) -> pl.Expr:
    """Format timezone-free epoch seconds exactly like the existing source clock."""
    value = pl.col(value) if isinstance(value, str) else value
    return pl.from_epoch(value, time_unit="s").dt.strftime("%Y-%m-%d %H:%M:%S")


def symbol_text(value) -> pl.Expr:
    """Create the fixed source symbol from its one-based security ordinal."""
    return pl.concat_str([pl.lit("S"), padded(value, 10)])


def cents_text(value) -> pl.Expr:
    """Format signed integer cents with no binary floating-point conversion."""
    value = pl.col(value) if isinstance(value, str) else value
    magnitude = value.abs()
    return pl.concat_str([
        pl.when(value < 0).then(pl.lit("-")).otherwise(pl.lit("")),
        (magnitude // 100).cast(pl.String), pl.lit("."),
        (magnitude % 100).cast(pl.String).str.pad_start(2, "0"),
    ])


def cash_name() -> pl.Expr:
    """Expand typed cash references while preserving already complete legal names."""
    return (pl.when((pl.col("CT_NAME") == "Trade settlement") & (pl.col("_reference") > 0))
            .then(pl.concat_str([pl.lit("Trade settlement "), pl.col("_reference").cast(pl.String)]))
            .when((pl.col("CT_NAME") == "Dividend") & (pl.col("_reference") > 0))
            .then(pl.concat_str([pl.lit("Dividend "), symbol_text("_reference")]))
            .otherwise(pl.col("CT_NAME")))


def order_key(family: str) -> pl.Expr:
    """Build the complete legacy ASCII key natively from typed event facts."""
    if family == "Trade":
        initial = padded("T_ID", 15)
        daily = pl.concat_str([timestamp_text("T_DTS"), padded("T_ID", 15), padded("_ordinal", 8)], separator="/")
        return pl.when(pl.col("_delivery_rank") == 0).then(initial).otherwise(daily)
    if family == "TradeHistory":
        values = [padded("TH_T_ID", 15), timestamp_text("TH_DTS"), padded("_ordinal", 8)]
    elif family == "CashTransaction":
        rank = (pl.when(pl.col("CT_NAME") == "Deposit").then(pl.lit("0"))
                .when(pl.col("CT_NAME").str.starts_with("Trade settlement ")).then(pl.lit("1"))
                .when(pl.col("CT_NAME").str.starts_with("Dividend ")).then(pl.lit("2"))
                .otherwise(pl.lit("3")))
        values = [timestamp_text("CT_DTS"), padded("CT_CA_ID", 11), padded("_ordinal", 15), rank]
    elif family == "HoldingHistory":
        values = [timestamp_text("_timestamp"), padded("_account", 11), padded("_ordinal", 12)]
    elif family == "WatchHistory":
        # The old key uses a zero-based security ordinal; the source symbol is one-based.
        values = [timestamp_text("W_DTS"), padded("W_C_ID", 11), padded(pl.col("W_S_SYMB") - 1, 10), pl.col("W_ACTION")]
    elif family in ("Customer", "CustomerMgmt"):
        customer = "C_ID" if family == "Customer" else "_customer"
        values = [timestamp_text("_timestamp"), padded(customer, 11), padded("_ordinal", 8), pl.col("_action")]
    elif family == "Account":
        values = [timestamp_text("_timestamp"), padded("CA_ID", 11), padded("_ordinal", 8)]
    elif family == "FINWIRE":
        values = [pl.col("_posting"), pl.col("_rank").cast(pl.String), padded("_identity", 15)]
    elif family == "DailyMarket":
        values = [pl.from_epoch(pl.col("DM_DATE"), time_unit="d").dt.strftime("%Y-%m-%d"), padded("DM_S_SYMB", 15)]
    elif family == "Prospect":
        return padded("_identity", 15)
    elif family == "__audit":
        return pl.col("_audit_key")
    else:
        name = {"HR": "EmployeeID", "Date": "SK_DateID", "Time": "SK_TimeID"}.get(family)
        if name:
            return padded(name, 15)
        return pl.col({"StatusType": "ST_ID", "TradeType": "TT_ID", "Industry": "IN_ID", "TaxRate": "TX_ID"}[family])
    return pl.concat_str(values, separator="/")


def lexical_invalid(field, separator: str) -> pl.Expr:
    """Express the independent scalar field contract on formatted String columns."""
    value = pl.col(field.name)
    datatype = TYPES.get(field.type, field.type)
    common = (value.str.contains(r"[^\x00-\x7F]|[\r\n\t]") |
              value.str.contains(separator, literal=True))
    if datatype == "DATE":
        parsed = value.str.to_date("%Y-%m-%d", strict=False, exact=True)
        typed = parsed.is_null() | (parsed.dt.strftime("%Y-%m-%d") != value)
    elif datatype == "DATETIME":
        parsed = value.str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False, exact=True)
        typed = parsed.is_null() | (value.str.len_bytes() != 19) | (parsed.dt.strftime("%Y-%m-%d %H:%M:%S") != value)
    elif datatype == "BOOLEAN":
        typed = ~value.is_in(("0", "1"))
    elif datatype.startswith("CHAR("):
        typed = value.str.len_bytes() > int(datatype[5:-1])
    else:
        signed, precision, scale = _numeric_type(datatype)
        pattern = r"^[+-]?[0-9]+(?:\.[0-9]+)?$" if signed else r"^[0-9]+(?:\.[0-9]+)?$"
        digits = value.str.strip_chars_start("+-").str.replace_all(".", "", literal=True).str.len_bytes()
        fraction = value.str.extract(r"\.(.*)$", 1).str.len_bytes().fill_null(0)
        typed = ~value.str.contains(pattern) | (digits > precision) | (fraction > scale)
    nonempty = value != ""
    return ((nonempty & (common | typed)) | (~nonempty & pl.lit(not field.nullable))).fill_null(True)


class NativeContract:
    """Hide native expression preparation behind checked source-column formatting."""

    def __init__(self, source: Source):
        """Compile formatting, integral bounds, and lexical checks once per schema."""
        self.source = source
        self.bounds = []
        self.formatters = []
        self.invalid = [lexical_invalid(field, source.separator) for field in source.fields]
        for field in source.fields:
            value = pl.col(field.name)
            datatype = TYPES.get(field.type, field.type)
            if field.name in SYMBOL_FIELDS:
                self.bounds.append(((value < 1) | (value >= 10**10)).fill_null(False))
                formatted = symbol_text(value)
            elif datatype == "DATE":
                formatted = pl.from_epoch(value, time_unit="d").dt.strftime("%Y-%m-%d")
            elif datatype == "DATETIME":
                formatted = timestamp_text(value)
            elif datatype.startswith("CHAR(") or field.name == "TX_RATE":
                formatted = value
            elif datatype == "BOOLEAN":
                formatted = value.cast(pl.String)
            else:
                signed, precision, scale = _numeric_type(datatype)
                maximum = 10**precision - 1
                minimum = -maximum if signed else 0
                self.bounds.append(((value < minimum) | (value > maximum)).fill_null(False))
                formatted = cents_text(value) if scale == 2 else value.cast(pl.String)
            self.formatters.append(formatted.cast(pl.String).fill_null("").alias(field.name))

    def format(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Reject dangerous integral values before formatting and validate output text."""
        if self.bounds and frame.select(pl.any_horizontal(self.bounds).any()).item():
            raise ValueError(f"A typed value exceeds its source bounds in {self.source.name}.")
        formatted = frame.select(self.formatters)
        self.check(formatted)
        return formatted

    def check(self, formatted: pl.DataFrame) -> None:
        """Reject lexical errors in native columns without Python per-cell loops."""
        if formatted.columns != self.source.columns:
            raise ValueError(f"Incorrect field count or order in {self.source.name}.")
        failures = formatted.select([expression.any().alias(field.name)
                                     for expression, field in zip(self.invalid, self.source.fields)])
        if failures.height and failures.select(pl.any_horizontal(pl.all())).item():
            field = next(name for name in failures.columns if failures[name][0])
            raise ValueError(f"Invalid source value in {self.source.name}.{field}.")


@cache
def native_contract(source: Source) -> NativeContract:
    """Reuse the native plan for each immutable initial or dated source contract."""
    return NativeContract(source)
