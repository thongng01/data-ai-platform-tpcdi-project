"""Describe source fields and select the dated daily Trade contract."""

import re
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from functools import cache, cached_property
from pathlib import Path

from .config import read_yaml


TYPES = {
    "IDENT_T": "NUM(11)", "TRADE_T": "NUM(15)", "S_QTY_T": "NUM(6)",
    "S_COUNT_T": "NUM(12)", "VALUE_T": "SNUM(10,2)", "S_PRICE_T": "SNUM(8,2)",
}


@cache
def _numeric_type(datatype):
    """Parse the signed flag, precision, and scale from a numeric field type."""
    match = re.fullmatch(r"(S?NUM)\((\d+)(?:,(\d+))?\)", datatype)
    if not match:
        raise ValueError(f"Unsupported source type: {datatype}")
    signed, precision, scale = match.groups()
    return signed == "SNUM", int(precision), int(scale or 0)


@dataclass(frozen=True)
class Field:
    """Describe one source field and its structural restrictions."""
    name: str
    type: str
    nullable: bool = False

    @cached_property
    def checker(self):
        """Prepare immutable type metadata once for repeated scalar validation."""
        datatype = TYPES.get(self.type, self.type)
        if datatype == "DATE":
            def typed(value):
                """Require a valid date and its canonical source spelling."""
                if date.fromisoformat(value).isoformat() != value:
                    raise ValueError(f"Invalid date in {self.name}.")
        elif datatype == "DATETIME":
            def typed(value):
                """Require a canonical whole-second timestamp without a zone."""
                if datetime.fromisoformat(value).isoformat(sep=" ") != value or len(value) != 19:
                    raise ValueError(f"Invalid timestamp in {self.name}.")
        elif datatype == "BOOLEAN":
            def typed(value):
                """Accept only the two source boolean spellings."""
                if value not in ("0", "1"):
                    raise ValueError(f"Invalid boolean in {self.name}.")
        elif datatype.startswith("CHAR("):
            width = int(datatype[5:-1])
            def typed(value):
                """Enforce the declared character width without trimming text."""
                if len(value) > width:
                    raise ValueError(f"{self.name} exceeds {datatype}.")
        else:
            signed, precision, scale = _numeric_type(datatype)
            match = re.compile(r"[+-]?\d+(?:\.\d+)?" if signed else r"\d+(?:\.\d+)?").fullmatch
            def typed(value):
                """Count all source digits without rounding or dropping leading zeros."""
                if not match(value):
                    raise ValueError(f"Invalid number in {self.name}: {value}")
                integer, _, fractional = value.lstrip("+-").partition(".")
                if len(integer) + len(fractional) > precision or len(fractional) > scale:
                    raise ValueError(f"{self.name} exceeds {datatype}: {value}")

        def check(value):
            """Keep empty, ASCII, and control checks before the prepared type check."""
            if not value:
                if self.nullable:
                    return
                raise ValueError(f"{self.name} cannot be empty.")
            if not value.isascii() or "\r" in value or "\n" in value or "\t" in value:
                raise ValueError(f"{self.name} must contain single-byte text without control separators.")
            typed(value)
        return check

    def check(self, value: str):
        """Check one lexical value with this field's prepared contract."""
        self.checker(value)


@dataclass(frozen=True)
class Source:
    """Describe the ordered business fields and separator of one source family."""
    name: str
    filename: str
    separator: str
    fields: tuple[Field, ...]
    cdc: bool = False
    daily_version_fields: tuple[Field, ...] = ()

    @property
    def columns(self):
        """Return business column names in their on-disk order."""
        return [field.name for field in self.fields]

    @cached_property
    def checks(self):
        """Retain prepared field predicates without rebuilding metadata per row."""
        return tuple(field.checker for field in self.fields)

    def check(self, values):
        """Check ordered lexical values against the independent scalar contract."""
        if len(values) != len(self.fields):
            raise ValueError(f"Incorrect field count in {self.name}.")
        for field, check, value in zip(self.fields, self.checks, values):
            if self.separator in value:
                raise ValueError(f"Delimiter in {self.name}.{field.name}.")
            check(value)

    @cached_property
    def daily_version(self):
        """Build the upgraded contract once without hashing all fields per event."""
        return replace(self, fields=(*self.fields, *self.daily_version_fields))


def trade_channel_day(config) -> date:
    """Return the first daily date that carries the Trade channel field."""
    return max(config.cutover + timedelta(days=1), date.fromisoformat(config.trade_channel_start))


def source_for_day(source: Source, day: date, config) -> Source:
    """Add the channel only to daily Trade rows on or after the configured date."""
    if source.name == "Trade" and day >= trade_channel_day(config):
        return source.daily_version
    return source


@cache
def load_sources() -> dict[str, Source]:
    """Load immutable field contracts from the committed source definitions."""
    definitions = read_yaml(Path(__file__).parent / "definitions" / "sources.yaml")
    sources = {}
    for name, definition in definitions.items():
        fields = []
        for column, field in definition["fields"].items():
            field = {"type": field} if isinstance(field, str) else field
            fields.append(Field(column, field["type"], field.get("nullable", False)))
        additions = tuple(Field(column, datatype) for column, datatype in definition.get("daily_version_fields", {}).items())
        sources[name] = Source(name, definition["filename"], definition["separator"], tuple(fields), definition.get("cdc", False), additions)
    return sources
