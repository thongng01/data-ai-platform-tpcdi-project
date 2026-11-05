import re
from dataclasses import dataclass
from datetime import date, datetime
from functools import cache
from pathlib import Path

from .config import read_yaml


TYPES = {
    "IDENT_T": "NUM(11)", "TRADE_T": "NUM(15)", "S_QTY_T": "NUM(6)",
    "S_COUNT_T": "NUM(12)", "VALUE_T": "SNUM(10,2)", "S_PRICE_T": "SNUM(8,2)",
}


@cache
def _numeric_type(datatype):
    match = re.fullmatch(r"(S?NUM)\((\d+)(?:,(\d+))?\)", datatype)
    if not match:
        raise ValueError(f"Unsupported source type: {datatype}")
    signed, precision, scale = match.groups()
    return signed == "SNUM", int(precision), int(scale or 0)


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    nullable: bool = False

    def check(self, value: str):
        if not value:
            if self.nullable:
                return
            raise ValueError(f"{self.name} cannot be empty.")
        if not value.isascii() or any(character in value for character in "\r\n\t"):
            raise ValueError(f"{self.name} must contain single-byte text without control separators.")
        datatype = TYPES.get(self.type, self.type)
        if datatype == "DATE":
            if date.fromisoformat(value).isoformat() != value:
                raise ValueError(f"Invalid date in {self.name}.")
        elif datatype == "DATETIME":
            if datetime.fromisoformat(value).isoformat(sep=" ") != value or len(value) != 19:
                raise ValueError(f"Invalid timestamp in {self.name}.")
        elif datatype == "BOOLEAN":
            if value not in ("0", "1"):
                raise ValueError(f"Invalid boolean in {self.name}.")
        elif datatype.startswith("CHAR("):
            if len(value) > int(datatype[5:-1]):
                raise ValueError(f"{self.name} exceeds {datatype}.")
        else:
            signed, precision, scale = _numeric_type(datatype)
            if not re.fullmatch(r"[+-]?\d+(?:\.\d+)?" if signed else r"\d+(?:\.\d+)?", value):
                raise ValueError(f"Invalid number in {self.name}: {value}")
            integer, _, fractional = value.lstrip("+-").partition(".")
            if len(integer) + len(fractional) > precision or len(fractional) > scale:
                raise ValueError(f"{self.name} exceeds {datatype}: {value}")


@dataclass(frozen=True)
class Source:
    name: str
    filename: str
    separator: str
    fields: tuple[Field, ...]
    cdc: bool = False

    @property
    def columns(self):
        return [field.name for field in self.fields]

    def check(self, values):
        if len(values) != len(self.fields):
            raise ValueError(f"Incorrect field count in {self.name}.")
        for field, value in zip(self.fields, values):
            if self.separator in value:
                raise ValueError(f"Delimiter in {self.name}.{field.name}.")
            field.check(value)

    def check_payload(self, payload):
        self.check(payload.split(self.separator))


def record_fields(source: Source, day, row):
    """Stage 3 attaches here; M1 preserves the business fields and format."""
    return row


@cache
def load_sources() -> dict[str, Source]:
    definitions = read_yaml(Path(__file__).parent / "definitions" / "sources.yaml")
    sources = {}
    for name, definition in definitions.items():
        fields = []
        for column, field in definition["fields"].items():
            field = {"type": field} if isinstance(field, str) else field
            fields.append(Field(column, field["type"], field.get("nullable", False)))
        sources[name] = Source(name, definition["filename"], definition["separator"], tuple(fields), definition.get("cdc", False))
    return sources
