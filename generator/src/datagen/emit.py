"""The format contract: how a generated row becomes bytes on disk.

Everything here follows TPC-DI source conventions, verified byte-for-byte in
``.scratch/coursework/research/R02-tpcdi-source-formats.md``:

- **CRLF line endings**, every file.
- **No header row**, ever. The only headered files in TPC-DI are ``*_audit.csv``,
  which we do not emit.
- **Pipe delimiter** for ``.txt`` OLTP and reference files, **comma** for the two
  ``.csv`` files, **no delimiter** for FINWIRE's fixed-width records.
- **Empty string means NULL**, so writers never quote, escape, or elide a
  field. A field with a value that looks empty is emitted as ``||``.
- **Text fields contain embedded spaces** and must never be whitespace-tokenised.
  The delimiter is the only protection.
- Booleans are ``0``/``1`` in trade files but ``true``/``false`` in
  ``Date.txt`` and ``Time.txt``.

This module is the only place that knows about bytes. Table specs build lists of
field values and hand them to a writer; nothing else touches encoding.
"""

from __future__ import annotations

import io
import os
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TextIO

CRLF = "\r\n"
DELIMITERS = {"pipe": "|", "csv": ",", "none": ""}


class Writer:
    """Base writer: opens a file and writes CRLF-terminated rows.

    Line endings and the no-header policy are set here so no subclass has to
    remember them.
    """

    def __init__(self, path: str, *, append: bool = False) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._handle: TextIO = open(path, "a" if append else "w", encoding="utf-8", newline="")
        self.rows_written = 0

    def __enter__(self) -> "Writer":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def write_row(self, fields: Sequence[object]) -> None:
        raise NotImplementedError

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()

    @staticmethod
    def _cell(value: object) -> str:
        """Render one field. ``None`` becomes an empty string, which means NULL."""
        if value is None:
            return ""
        return str(value)


class DelimitedWriter(Writer):
    """Pipe- or comma-delimited rows, no header. The common case."""

    def __init__(self, path: str, delimiter: str = "pipe", *, append: bool = False) -> None:
        super().__init__(path, append=append)
        self.delimiter = DELIMITERS[delimiter]

    def write_row(self, fields: Sequence[object]) -> None:
        self._handle.write(self.delimiter.join(self._cell(f) for f in fields))
        self._handle.write(CRLF)
        self.rows_written += 1

    def write_rows(self, rows: Iterable[Sequence[object]]) -> None:
        for row in rows:
            self.write_row(row)


class SingleColumnWriter(Writer):
    """One field per line, no delimiter. ``BatchDate.txt`` and FINWIRE use this."""

    def write_row(self, fields: Sequence[object]) -> None:
        self._handle.write(self._cell(fields[0]))
        self._handle.write(CRLF)
        self.rows_written += 1


class FixedWidthWriter(Writer):
    """Space-padded fixed-width records, the FINWIRE layout.

    Records are padded to a declared width per record type. The tail field of a
    CMP record is either a company name or a numeric CIK, and the two are told
    apart downstream by ``char_length >= 10``, so padding must be preserved.
    """

    def __init__(
        self,
        path: str,
        layout: Sequence[tuple[str, int]],
        *,
        record_type_column: str = "record_type",
    ) -> None:
        super().__init__(path)
        self.layout = layout
        self.record_type_column = record_type_column
        self._widths = {name: width for name, width in layout}

    def write_row(self, fields: dict[str, object]) -> None:
        parts: list[str] = []
        for name, width in self.layout:
            raw = "" if fields.get(name) is None else str(fields[name])
            parts.append(raw[:width].ljust(width))
        self._handle.write("".join(parts))
        self._handle.write(CRLF)
        self.rows_written += 1


class XmlActionWriter:
    """Writes ``CustomerMgmt.xml``, the one structured source file.

    TPC-DI's XML is tab-indented, namespaced ``TPCDI:``, carries one
    ``<TPCDI:Action>`` per record, and **has no trailing newline** (its last
    bytes are the closing tag). Payloads are partial: a non-``NEW`` action
    carries only the ids it changes, which is precisely why downstream SCD2
    needs a carry-forward. We reproduce that partiality rather than emitting
    full rows, because the whole point is to exercise the carry-forward.
    """

    HEADER = (
        '<?xml version="1.0" encoding="UTF-8"?>' + CRLF
        + '<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">' + CRLF
    )
    FOOTER = "</TPCDI:Actions>"

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._buf = io.StringIO()
        self._buf.write(self.HEADER)
        self.actions_written = 0

    def write_action(
        self,
        *,
        action_type: str,
        action_ts: str,
        customer_id: int,
        payload: dict[str, object] | None = None,
    ) -> None:
        """Emit one action. ``payload`` is ``None`` for partial (non-NEW) actions."""
        tab = "\t"
        buf = self._buf
        buf.write(f"{tab}<TPCDI:Action ActionType=\"{action_type}\" ActionTS=\"{action_ts}\">" + CRLF)

        attributes = f' C_ID="{customer_id}"'
        if payload:
            for key in ("C_TAX_ID", "C_GNDR", "C_TIER", "C_DOB"):
                value = payload.get(key)
                if value is not None:
                    attributes += f' {key}="{value}"'
        buf.write(f"{tab * 2}<Customer{attributes}>" + CRLF)

        if payload:
            for element, children in _customer_children(payload):
                buf.write(f"{tab * 3}<{element}>" + CRLF)
                for child_name, child_value in children:
                    buf.write(f"{tab * 4}<{child_name}>")
                    buf.write("" if child_value is None else str(child_value))
                    # Empty optional elements are self-closing in the source,
                    # not absent. Reproduce that so parsers see the same shape.
                    buf.write(f"</{child_name}>" if child_value else "/" + CRLF)
                buf.write(f"{tab * 3}</{element}>" + CRLF)

        buf.write(f"{tab * 2}</Customer>" + CRLF)
        buf.write(f"{tab}</TPCDI:Action>" + CRLF)
        self.actions_written += 1

    def close(self) -> None:
        if not self._buf.closed:
            self._buf.write(self.FOOTER)
            with open(self.path, "w", encoding="utf-8", newline="") as handle:
                handle.write(self._buf.getvalue())
            self._buf.close()


def _customer_children(payload: dict[str, object]) -> Iterator[tuple[str, Sequence[tuple[str, object]]]]:
    """Yield the ``<Customer>`` child elements, in the order TPC-DI uses.

    Order matters and is not alphabetical: Name, Address, ContactInfo, TaxInfo,
    Account. Within ``TaxInfo`` the local tax id precedes the national one.
    """
    name = payload.get("C_L_NAME"), payload.get("C_F_NAME"), payload.get("C_M_NAME")
    if any(v is not None for v in name):
        yield "Name", [
            ("C_L_NAME", payload.get("C_L_NAME")),
            ("C_F_NAME", payload.get("C_F_NAME")),
            ("C_M_NAME", payload.get("C_M_NAME")),
        ]

    address = (
        payload.get("C_ADLINE1"),
        payload.get("C_ADLINE2"),
        payload.get("C_ZIPCODE"),
        payload.get("C_CITY"),
        payload.get("C_STATE_PROV"),
        payload.get("C_CTRY"),
    )
    if any(v is not None for v in address):
        yield "Address", [
            ("C_ADLINE1", payload.get("C_ADLINE1")),
            ("C_ADLINE2", payload.get("C_ADLINE2")),
            ("C_ZIPCODE", payload.get("C_ZIPCODE")),
            ("C_CITY", payload.get("C_CITY")),
            ("C_STATE_PROV", payload.get("C_STATE_PROV")),
            ("C_CTRY", payload.get("C_CTRY")),
        ]

    phones = [payload.get(f"C_PHONE_{i}") for i in (1, 2, 3)]
    if payload.get("C_PRIM_EMAIL") or payload.get("C_ALT_EMAIL") or any(phones):
        children: list[tuple[str, object]] = [
            ("C_PRIM_EMAIL", payload.get("C_PRIM_EMAIL")),
            ("C_ALT_EMAIL", payload.get("C_ALT_EMAIL")),
        ]
        for index, phone in enumerate(phones, start=1):
            if phone is None:
                continue
            children.append((f"C_PHONE_{index}", phone))
        yield "ContactInfo", children

    if payload.get("C_LCL_TX_ID") or payload.get("C_NAT_TX_ID"):
        yield "TaxInfo", [
            ("C_LCL_TX_ID", payload.get("C_LCL_TX_ID")),
            ("C_NAT_TX_ID", payload.get("C_NAT_TX_ID")),
        ]

    if payload.get("CA_ID") is not None:
        yield "Account", [
            ("CA_ID", payload.get("CA_ID")),
            ("CA_B_ID", payload.get("CA_B_ID")),
            ("CA_NAME", payload.get("CA_NAME")),
        ]


def timestamp(value: datetime) -> str:
    """TPC-DI datetime encoding: ``YYYY-MM-DD HH:MM:SS``, no timezone."""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def action_timestamp(value: datetime) -> str:
    """CustomerMgmt action encoding: ISO local, ``T`` separator, no timezone."""
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def iso_date(value: datetime) -> str:
    return value.strftime("%Y-%m-%d")


@dataclass(frozen=True)
class EmittedFile:
    """What a batch wrote, for the run manifest."""

    path: str
    rows: int
    kind: str
