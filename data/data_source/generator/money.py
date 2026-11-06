"""Format integer cents as scalar text or native column expressions without floats."""

import polars as pl


def cents_text(cents: int) -> str:
    """Render signed integer cents with exactly two decimal places."""
    cents = int(cents)
    return f"{'-' if cents < 0 else ''}{abs(cents) // 100}.{abs(cents) % 100:02d}"


def cents_expression(value) -> pl.Expr:
    """Render a column name or expression in the same integer-cent text form."""
    value = pl.col(value) if isinstance(value, str) else value
    magnitude = value.abs()
    return pl.concat_str([
        pl.when(value < 0).then(pl.lit("-")).otherwise(pl.lit("")),
        (magnitude // 100).cast(pl.String), pl.lit("."),
        (magnitude % 100).cast(pl.String).str.pad_start(2, "0"),
    ])
