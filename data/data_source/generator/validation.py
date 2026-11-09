"""Parse saved source records, then replay account and customer partitions."""

import calendar
import hashlib
import heapq
import json
import math
import re
import tempfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from itertools import groupby
from multiprocessing import get_context
from pathlib import Path
from typing import NamedTuple
from xml.etree import ElementTree as ET

import polars as pl

from .config import Config, read_yaml
from .formats import TYPES, load_sources, source_for_day
from .random_values import uniform
from .validation_faults import FaultAudit


_VALIDATION_PARTITION_BYTES = 16 * 1024**2
# Default account IDs advance by 24. This prime ceiling avoids routing those IDs
# into only a fraction of the partitions, as a power-of-two ceiling would.
_VALIDATION_MAX_PARTITIONS = 1021
_PARQUET_BUFFER_BYTES = 64 * 1024**2


class ValidationError(ValueError):
    """Report a source-contract or business-consistency failure."""
    pass


def _require(condition, message):
    """Raise a validation failure when a stated invariant is false."""
    if not condition:
        raise ValidationError(message)


def _cents(value):
    """Parse exact decimal cents and reject fractional cents or nonfinite values."""
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value)) * 100
        _require(result.is_finite() and result == result.to_integral_value(), "Money must have exact cent precision.")
        return int(result)
    except (InvalidOperation, ValueError) as error:
        raise ValidationError(f"Invalid money: {value}") from error


def _lines(path):
    """Read bounded ASCII source lines and enforce LF-only record endings."""
    with path.open("rb") as stream:
        number = 0
        while raw := stream.readline(1024 * 1024):
            number += 1
            _require(len(raw) < 1024 * 1024, f"{path}, row {number}: source record exceeds the parser bound.")
            _require(raw.endswith(b"\n") and b"\r" not in raw, f"{path}, row {number}: expected LF endings.")
            _require(b"\x00" not in raw, f"{path}, row {number}: NUL is not allowed.")
            try:
                yield raw[:-1].decode("ascii")
            except UnicodeDecodeError as error:
                raise ValidationError(f"{path}, row {number}: expected ASCII source text.") from error


def _rows(path, source, batch):
    """Batch 1 has no CDC columns. Later batches have an exact 1..N sequence."""
    previous = 0
    for number, line in enumerate(_lines(path), 1):
        try:
            values = line.split(source.separator)
            metadata = {}
            if source.cdc and batch > 1:
                _require(len(values) >= 2, "Missing CDC fields.")
                flag, sequence, *values = values
                _require(re.fullmatch(r"[0-9]{1,12}", sequence) is not None and int(sequence) == previous + 1,
                         "CDC_DSN must be the exact sequence 1..N.")
                previous += 1
                _require(flag in (("I", "U") if source.name in ("Customer", "Account", "Trade") else ("I",)),
                         "Invalid CDC operation.")
                metadata = {"_flag": flag, "_order": previous}
            _require(len(values) == len(source.fields), "Unexpected field count.")
            source.check(values)
            yield {**dict(zip(source.columns, values)), **metadata}
        except (ValueError, TypeError) as error:
            raise ValidationError(f"{path}, row {number}: {error}") from error


def _prior_year(day):
    """Find the required prior-year price boundary, including leap dates."""
    return day.replace(year=day.year - 1, day=min(day.day, calendar.monthrange(day.year - 1, day.month)[1]))


def _quarter(day):
    """Map a date to a consecutive reporting-quarter ordinal."""
    return day.year * 4 + (day.month - 1) // 3


def _release(quarter):
    """Derive the final date of the month after a reporting quarter."""
    year, number = divmod(quarter + 1, 4)
    return date(year, number * 3 + 2, 1) - timedelta(days=1)


def _expected_files(config):
    """Derive cadence here, independently of the exporter and its manifest."""
    initial = ("CustomerMgmt.xml", "Trade.txt", "TradeHistory.txt", "CashTransaction.txt", "HoldingHistory.txt",
               "WatchHistory.txt", "DailyMarket.txt", "HR.csv", "Prospect.csv", "Date.txt", "Time.txt", "Industry.txt",
               "StatusType.txt", "TaxRate.txt", "TradeType.txt")
    result = {"offline/initial/" + name for name in initial}
    first_quarter = _quarter(config.start) - 5
    first_year, first_number = divmod(first_quarter, 4)
    result.add(f"offline/initial/FINWIRE{first_year}Q{first_number + 1}")
    for quarter in range(_quarter(config.start) - 5, _quarter(config.end) + 1):
        released = _release(quarter)
        if released > config.end:
            continue
        name = f"FINWIRE{released.year}Q{(released.month - 1) // 3 + 1}"
        folder = "offline/initial" if released <= config.cutover else f"offline/daily/{released:%Y%m%d}"
        result.add(f"{folder}/{name}")
    current = config.daily_start_date
    while current <= config.end:
        stamp = current.strftime("%Y%m%d")
        for source in ("Customer", "Account", "Trade", "HoldingHistory", "WatchHistory", "DailyMarket"):
            result.add(f"offline/daily/{stamp}/{source}_{stamp}.txt")
        if current.day == 1:
            result.add(f"offline/daily/{stamp}/Prospect_{stamp}.csv")
        result.add(f"stream/cash_transactions/{stamp}.jsonl")
        current += timedelta(days=1)
    return result


def _field_type(field):
    """Choose the typed replay representation, storing money as integer cents."""
    datatype = TYPES.get(field.type, field.type)
    if datatype.startswith(("NUM(", "SNUM(")) or datatype == "BOOLEAN":
        return pl.Int64 if "," not in datatype or datatype.endswith(",2)") else pl.String
    return pl.String


def _typed(source, row):
    """Convert parsed source text into typed business values for replay."""
    result = {}
    for field in source.fields:
        value = row.get(field.name)
        datatype = _field_type(field)
        result[field.name] = (None if value in (None, "") else
                              _cents(value) if ",2)" in TYPES.get(field.type, field.type) else int(value)) if datatype == pl.Int64 else value
    return result


PARQUET_BUFFER_ROWS = 262144


def _partition_count(total_bytes):
    """Use the source-byte target until the replay partition ceiling is reached."""
    return max(1, min(_VALIDATION_MAX_PARTITIONS, math.ceil(total_bytes / _VALIDATION_PARTITION_BYTES)))


def _staged_row_bytes(schema):
    """Estimate a maximum Arrow row from source widths and private staging fields."""
    widths = {"_flag": 1, "_day": 10, "_partition": 10, "_timestamp": 19, "_kind": 9,
              "date": 10}
    for source in load_sources().values():
        for field in (*source.fields, *source.daily_version_fields):
            datatype = TYPES.get(field.type, field.type)
            if datatype.startswith("CHAR("):
                width = int(datatype[5:-1])
            elif datatype in {"DATE", "DATETIME"}:
                width = 10 if datatype == "DATE" else 19
            else:
                width = 32
            widths[field.name] = max(widths.get(field.name, 0), width)
    widths["identity"] = widths["AgencyID"]
    size = 0
    for name, datatype in schema.items():
        # Include offsets and validity space. XML presence lists contain field
        # names, not source values, and cannot contain more names than the schema.
        if datatype == pl.String:
            size += 16 + widths[name]
        elif isinstance(datatype, pl.List):
            size += 16 + sum(16 + len(column) for column in schema)
        else:
            size += 16
    # Trade staging adds a day key and two joined price columns after parsing.
    return size + 64


class _Stage:
    """Write bounded typed blocks and bound row-group buffers across partitions."""

    def __init__(self, root, partitions, date_partitions=None):
        """Initialize aggregate parse buffers and schemas for bounded disk staging."""
        self.root = root
        self.partitions = partitions
        self.date_partitions = date_partitions or partitions
        self.buffers = defaultdict(list)
        self.schemas = {}
        self.counter = defaultdict(int)
        self.rows = 0
        self.peak_rows = 0
        self._source_plans = {}

    def add(self, table, row, schema):
        """Buffer a typed row and flush the largest table when the shared row bound is reached."""
        self.schemas[table] = schema
        self.buffers[table].append(row)
        self.rows += 1
        self.peak_rows = max(self.peak_rows, self.rows)
        if self.rows >= 8192:
            self.flush(max(self.buffers, key=lambda name: len(self.buffers[name])))

    def flush(self, table):
        """Write one typed staging block and release its share of the memory buffer."""
        rows = self.buffers[table]
        if not rows:
            return
        directory = self.root / "blocks" / table
        directory.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(rows, schema=self.schemas[table]).write_parquet(directory / f"{self.counter[table]:08d}.parquet")
        self.counter[table] += 1
        self.rows -= len(rows)
        rows.clear()

    @staticmethod
    def _row_group_rows(partitions, schema=None):
        """Bound aggregate row counts and estimated Arrow bytes across writer partitions."""
        _require(0 < partitions <= PARQUET_BUFFER_ROWS, "Partition count exceeds the row-group buffer budget.")
        byte_rows = _PARQUET_BUFFER_BYTES // _staged_row_bytes(schema) if schema else PARQUET_BUFFER_ROWS
        return max(1, min(8192, min(PARQUET_BUFFER_ROWS, byte_rows) // partitions))

    def finish(self):
        """Join daily prices once and partition typed records for independent replay."""
        for table in list(self.buffers):
            self.flush(table)
        # Project only price fields into the join so market metadata cannot
        # replace a trade's account partition, delivery day, or source order.
        tables = sorted(self.buffers, key=lambda name: (name != "DailyMarket", name))
        for table in tables:
            if not self.counter[table]:
                continue
            directory = self.root / "blocks" / table
            scan = pl.scan_parquet(directory / "*.parquet")
            if table == "Trade":
                if self.counter.get("DailyMarket", 0):
                    prices = pl.scan_parquet(self.root / "partitioned" / "DailyMarket" / "*" / "*.parquet").select(
                        pl.col("DM_DATE").alias("_market_day"), pl.col("DM_S_SYMB").alias("T_S_SYMB"),
                        pl.col("DM_LOW").alias("_market_low"), pl.col("DM_HIGH").alias("_market_high"))
                else:
                    prices = pl.LazyFrame(schema={"_market_day": pl.String, "T_S_SYMB": pl.String,
                                                  "_market_low": pl.Int64, "_market_high": pl.Int64})
                # Market settings fix the number of price rows across customer
                # scales. Build the hash state from that side and stream trades;
                # the optimizer must not switch to a scale-sized trade hash map.
                scan = scan.with_columns(pl.col("T_DTS").str.slice(0, 10).alias("_market_day")).join(
                    prices, on=["_market_day", "T_S_SYMB"], how="left", build_side="force_right")
            partition_count = self.date_partitions if table == "DailyMarket" else self.partitions
            scan.sink_parquet(pl.PartitionBy(self.root / "partitioned" / table, key="_partition", max_rows_per_file=65536),
                              mkdir=True, engine="streaming", row_group_size=self._row_group_rows(partition_count, self.schemas[table]))
            for path in directory.glob("*.parquet"):
                path.unlink()
            directory.rmdir()

    def frame(self, table, partition):
        """Materialize one replay partition, preserving its schema when empty."""
        paths = list((self.root / "partitioned" / table / f"_partition={partition}").glob("*.parquet"))
        if not paths:
            return pl.DataFrame(schema=self.schemas.get(table, {}))
        return pl.scan_parquet(paths).collect(engine="streaming")

    def records(self, table, partition):
        """Iterate named records from one typed replay partition."""
        return self.frame(table, partition).iter_rows(named=True)


def _source_schema(source):
    """Describe typed business columns and private staging metadata."""
    columns = {field.name: _field_type(field) for field in source.fields}
    if source.name == "Trade":
        columns["T_CHANNEL"] = pl.String
    return {**columns, "_flag": pl.String,
            "_order": pl.Int64, "_day": pl.String, "_partition": pl.String}


def _compile_plan(source):
    """Resolve field converters and the staging schema once per source identity."""
    converters = []
    for field in source.fields:
        converter = None
        if _field_type(field) == pl.Int64:
            converter = _cents if ",2)" in TYPES.get(field.type, field.type) else int
        converters.append((field.name, converter))
    return tuple(converters), _source_schema(source)


def _stage_source(stage, source, row, day, key):
    """Reuse this stage's source plan to convert and route one replay row."""
    identity = id(source)
    entry = stage._source_plans.get(identity)
    if entry is None:
        converters, schema = _compile_plan(source)
        # Retaining the Source prevents identity reuse while this stage exists.
        entry = stage._source_plans[identity] = (source, converters, schema)
    _, converters, schema = entry
    result = {}
    for name, converter in converters:
        value = row.get(name)
        result[name] = value if converter is None else None if value in (None, "") else converter(value)
    result.update(_flag=row.get("_flag", "I"), _order=row.get("_order", 0), _day=day,
                  _partition=str(key) if source.name == "DailyMarket" else str(int(key) % stage.partitions))
    stage.add(source.name, result, schema)


_XML_GROUPS = {
    "Name": {"C_L_NAME": "C_L_NAME", "C_F_NAME": "C_F_NAME", "C_M_NAME": "C_M_NAME"},
    "Address": {name: name for name in ("C_ADLINE1", "C_ADLINE2", "C_ZIPCODE", "C_CITY", "C_STATE_PROV", "C_CTRY")},
    "TaxInfo": {"C_LCL_TX_ID": "C_LCL_TX_ID", "C_NAT_TX_ID": "C_NAT_TX_ID"},
    "ContactInfo": {"C_PRIM_EMAIL": "C_EMAIL_1", "C_ALT_EMAIL": "C_EMAIL_2"},
}


def _xml_patch(customer, source):
    """Absent fields do not change. Present empty fields explicitly clear values."""
    patch = {name: value for name, value in customer.attrib.items()}
    _require(set(patch) <= {"C_ID", "C_TAX_ID", "C_GNDR", "C_TIER", "C_DOB"}, "Unexpected customer XML attribute.")
    seen = set()
    for group in customer:
        _require(group.tag in (*_XML_GROUPS, "Account"), f"Unexpected customer XML element: {group.tag}")
        if group.tag == "Account":
            continue
        _require(group.tag not in seen and not group.attrib, "Duplicate or attributed customer XML group.")
        seen.add(group.tag)
        members = set()
        for element in group:
            _require(element.tag not in members and not element.attrib, "Duplicate or attributed customer XML field.")
            members.add(element.tag)
            if element.tag in _XML_GROUPS[group.tag]:
                _require(not len(element), "A customer field cannot contain elements.")
                patch[_XML_GROUPS[group.tag][element.tag]] = element.text or ""
            else:
                _require(group.tag == "ContactInfo" and element.tag in ("C_PHONE_1", "C_PHONE_2", "C_PHONE_3"),
                         "Unexpected customer XML field.")
                number = element.tag[-1]
                parts = {"C_CTRY_CODE": "CTRY", "C_AREA_CODE": "AREA", "C_LOCAL": "LOCAL", "C_EXT": "EXT"}
                phone_seen = set()
                for part in element:
                    _require(part.tag in parts and part.tag not in phone_seen and not part.attrib and not len(part),
                             "Invalid XML phone field.")
                    phone_seen.add(part.tag)
                    patch[f"C_{parts[part.tag]}_{number}"] = part.text or ""
    fields = {field.name: field for field in source.fields}
    for name, value in patch.items():
        _require(name in fields, f"Unknown customer XML field: {name}")
        fields[name].check(value)
    return patch


def _stage_xml_customer(stage, source, patch, kind, timestamp, order, schema):
    """Build a customer version while retaining explicit XML field presence."""
    if kind in {"NEW", "UPDCUST", "INACT"}:
        patch["C_ST_ID"] = "INAC" if kind == "INACT" else "ACTV"
        if kind == "NEW":
            for field in source.fields:
                _require(field.nullable or field.name in patch, f"A new customer requires {field.name}.")
        record = _typed(source, patch)
        record.update(_present=list(patch), _kind=kind, _timestamp=timestamp, _day=timestamp[:10],
                      _flag="I" if kind == "NEW" else "U", _order=order, _partition=str(int(patch["C_ID"]) % stage.partitions))
        stage.add("CustomerXML", record, schema)


def _stage_xml_accounts(stage, source, accounts, identifier, kind, timestamp, order, schema):
    """Check account XML fields and stage explicit creation, update, or closure versions."""
    for account in accounts:
        _require(set(account.attrib) <= {"CA_ID", "CA_TAX_ST"} and "CA_ID" in account.attrib, "Invalid account XML attributes.")
        values = {**account.attrib, "CA_C_ID": str(identifier), "CA_ST_ID": "INAC" if kind in {"CLOSEACCT", "INACT"} else "ACTV"}
        names = set()
        for element in account:
            _require(element.tag in {"CA_B_ID", "CA_NAME"} and element.tag not in names and not element.attrib and not len(element),
                     "Invalid account XML field.")
            names.add(element.tag)
            values[element.tag] = element.text or ""
        for field in source.fields:
            if field.name in values:
                field.check(values[field.name])
            elif kind in {"NEW", "ADDACCT"}:
                _require(field.nullable, f"A new account requires {field.name}.")
        account_id = int(values["CA_ID"])
        record = _typed(source, values)
        record.update(_present=list(values), _kind=kind, _timestamp=timestamp, _day=timestamp[:10],
                      _flag="I" if kind in {"NEW", "ADDACCT"} else "U", _order=order,
                      _partition=str(account_id % stage.partitions))
        stage.add("AccountXML", record, schema)


def _xml(stage, path, sources, config, audit=None):
    """Parse initial CMS actions and retain explicit partial-update fields."""
    schema = {**_source_schema(sources["Customer"]), "_present": pl.List(pl.String), "_timestamp": pl.String, "_kind": pl.String}
    account_schema = {**_source_schema(sources["Account"]), "_present": pl.List(pl.String), "_timestamp": pl.String, "_kind": pl.String}
    context = ET.iterparse(path, events=("start", "end"))
    _, root = next(context)
    _require(root.tag == "{http://www.tpc.org/tpc-di}Actions" and not root.attrib, "Unexpected XML root.")
    previous = ("", -1)
    counts = defaultdict(int)
    depth = 1
    for event, action in context:
        if event == "start":
            depth += 1
            _require(depth != 2 or action.tag == "{http://www.tpc.org/tpc-di}Action", "Unexpected XML root child.")
            continue
        depth -= 1
        if action.tag != "{http://www.tpc.org/tpc-di}Action":
            continue
        _require(depth == 1, "An XML action must be a direct child of Actions.")
        _require(set(action.attrib) == {"ActionTS", "ActionType"}, "Unexpected XML action attributes.")
        timestamp = action.attrib["ActionTS"]
        _require(len(timestamp) == 19 and datetime.fromisoformat(timestamp).isoformat() == timestamp, "Invalid XML action timestamp.")
        _require(str(config.start) <= timestamp[:10] <= str(config.cutover), "Customer XML timestamp lies outside initial period.")
        kind = action.attrib["ActionType"]
        _require(kind in {"NEW", "UPDCUST", "ADDACCT", "UPDACCT", "CLOSEACCT", "INACT"}, "Unknown XML action.")
        _require(len(action) == 1 and action[0].tag == "Customer", "An action must contain exactly one customer.")
        customer = action[0]
        patch = _xml_patch(customer, sources["Customer"])
        _require("C_ID" in patch, "Missing customer XML ID.")
        identifier = int(patch["C_ID"])
        accounts = customer.findall("Account")
        if kind in {"ADDACCT", "UPDACCT", "CLOSEACCT"}:
            _require(len(accounts) == 1 and "CA_ID" in accounts[0].attrib,
                     "An account-specific CMS action requires exactly one account ID.")
        account_id = int(accounts[0].attrib["CA_ID"]) if len(accounts) == 1 else None
        if audit is not None:
            patch = audit.restore("CustomerMgmt", timestamp[:10], patch, kind=kind, timestamp=timestamp, account_id=account_id)
        key = timestamp, identifier
        _require(key >= previous, "Customer actions must be ordered by timestamp and customer ID.")
        previous = key
        counts[kind] += 1
        timestamp = timestamp.replace("T", " ")
        _stage_xml_customer(stage, sources["Customer"], patch, kind, timestamp, counts[kind], schema)
        _require(bool(accounts) if kind in {"NEW", "ADDACCT", "UPDACCT", "CLOSEACCT"} else kind == "INACT" or not accounts,
                 "Unexpected number of accounts in XML action.")
        _stage_xml_accounts(stage, sources["Account"], accounts, identifier, kind, timestamp, counts[kind], account_schema)
        root.clear()
    return dict(counts)


def _finwire_values(kind, values):
    """Check required FINWIRE values, date text, and numeric field syntax."""
    optional = {"FoundingDate", "AddrLine2", "Country"} if kind == "CMP" else set()
    for name, value in values.items():
        _require(value != "" or name in optional, f"Missing required FINWIRE {kind}.{name}.")
    dates = {"CMP": ("FoundingDate",), "SEC": ("FirstTradeDate", "FirstTradeExchg"),
             "FIN": ("QtrStartDate", "PostingDate")}
    for name in dates[kind]:
        if values[name]:
            try:
                parsed = datetime.strptime(values[name], "%Y%m%d").date()
                _require(parsed.strftime("%Y%m%d") == values[name], f"Invalid FINWIRE {kind}.{name} date.")
            except ValueError as error:
                raise ValidationError(f"Invalid FINWIRE {kind}.{name} date.") from error
    integers = {"CMP": ("CIK",), "SEC": ("ShOut", "CoNameOrCIK"),
                "FIN": ("Year", "Quarter", "ShOut", "DilutedShOut", "CoNameOrCIK")}
    for name in integers[kind]:
        _require(re.fullmatch(r"[0-9]+", values[name]) is not None, f"Invalid FINWIRE {kind}.{name} integer.")
    numbers = {"CMP": (), "SEC": ("Dividend",),
               "FIN": ("Revenue", "Earnings", "EPS", "DilutedEPS", "Margin", "Inventory", "Assets", "Liabilities")}
    for name in numbers[kind]:
        _require(re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", values[name]) is not None,
                 f"Invalid FINWIRE {kind}.{name} number.")


def _expected_financial_reports(config):
    """Derive every company-period identity from configuration and release dates."""
    return {(company, ordinal // 4, ordinal % 4 + 1)
            for ordinal in range(_quarter(config.start) - 5, _quarter(config.end) + 1)
            if _release(ordinal) <= config.end
            for company in range(1, config.companies + 1)}


def _expected_finwire_postings(config):
    """Derive initial identities and every released-period posting without market helpers."""
    ordinal = _quarter(config.start) - 5
    first = date(ordinal // 4, ordinal % 4 * 3 + 1, 1)
    return {first.strftime("%Y%m%d-000000")} | {
        _release(period).strftime("%Y%m%d-000000")
        for period in range(ordinal, _quarter(config.end) + 1) if _release(period) <= config.end}


def _decode_finwire(line, path, layouts, config):
    """Decode fixed widths and check the posting timestamp and delivery envelope."""
    kind = line[15:18]
    _require(kind in layouts, "Invalid FINWIRE record type.")
    layout = layouts[kind]
    _require(len(line) == sum(field[0] for field in layout.values()), f"Invalid {kind} record width.")
    values = {}
    offset = 0
    for name, (width, _) in layout.items():
        values[name] = line[offset:offset + width].strip()
        offset += width
    _finwire_values(kind, values)
    timestamp = values["PTS"]
    posted = datetime.strptime(timestamp, "%Y%m%d-%H%M%S")
    _require(posted.strftime("%Y%m%d-%H%M%S") == timestamp, "Invalid FINWIRE PTS.")
    _require(path.name == f"FINWIRE{posted.year}Q{(posted.month - 1) // 3 + 1}", "FINWIRE is in the wrong posting-quarter file.")
    _require(posted.date() <= config.cutover if path.parent.name == "initial" else posted.strftime("%Y%m%d") == path.parent.name,
             "FINWIRE release lies outside its delivery.")
    return kind, values, posted


def _finwire(root, paths, config, industries, audit, saved):
    """Validate released fixed-width company, security, and financial versions."""
    layouts = read_yaml(Path(__file__).parent / "definitions" / "finwire.yaml")
    companies = {}
    securities = defaultdict(list)
    reports = set()
    company_postings, security_postings = set(), set()
    counts = defaultdict(int)
    results = {}
    slices = audit.slices({path.relative_to(root).as_posix() for path in paths})
    for path in sorted(paths, key=lambda item: (0 if item.parent.name == "initial" else 1, str(item))):
        reader = audit.reader(slices.get(path.relative_to(root).as_posix(), {}))
        result = _ParsedFile(path.relative_to(root).as_posix())
        results[result.relative] = result
        before = sum(counts.values())
        try:
            previous = None
            for line in _lines(path):
                kind, values, posted = _decode_finwire(line, path, layouts, config)
                identity = values["CIK"] if kind == "CMP" else values["Symbol"] if kind == "SEC" else values["CoNameOrCIK"]
                key = values["PTS"], {"CMP": 0, "SEC": 1, "FIN": 2}[kind], identity
                _require(previous is None or key > previous, "FINWIRE rows must have unique ordered posting keys.")
                previous = key
                counts[kind] += 1
                if kind == "CMP":
                    values = reader.restore("FINWIRE:CMP", str(posted.date()), values)
                    _require(values["Status"] in {"ACTV", "INAC"} and values["CIK"].isdigit(), "Invalid company identity or status.")
                    _require(values["IndustryID"] in industries, "Company industry is missing.")
                    _require(values["SPrating"] in {"AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B+", "B", "B-", "CCC+", "CCC", "CCC-", "CC", "C", "D"}, "Invalid company credit rating.")
                    company_postings.add((int(values["CIK"]), values["PTS"]))
                    companies[int(values["CIK"])] = posted.strftime("%Y-%m-%d %H:%M:%S")
                elif kind == "SEC":
                    _require(values["CoNameOrCIK"].isdigit() and int(values["CoNameOrCIK"]) in companies, "Security company is missing.")
                    dividend = _cents(values["Dividend"])
                    _require(dividend is not None and dividend >= 0 and values["Status"] in {"ACTV", "INAC"}, "Invalid security dividend or status.")
                    security_postings.add((values["Symbol"], values["PTS"]))
                    securities[values["Symbol"]].append((posted.strftime("%Y-%m-%d %H:%M:%S"), dividend, values["Status"]))
                else:
                    quarter, year = int(values["Quarter"]), int(values["Year"])
                    _require(1 <= quarter <= 4, "Invalid financial quarter.")
                    company = int(values["CoNameOrCIK"])
                    _require(company in companies, "Financial company is missing.")
                    report = company, year, quarter
                    _require(report not in reports, "Duplicate financial report.")
                    reports.add(report)
                    _require(posted.date() == _release(year * 4 + quarter - 1), "Financial report has the wrong release date.")
                    _require(values["QtrStartDate"] == f"{year:04d}{quarter * 3 - 2:02d}01",
                             "Financial quarter start differs from its year and quarter.")
                    _require(values["PostingDate"] == posted.strftime("%Y%m%d"),
                             "Financial posting date differs from PTS.")
                    for name in ("Revenue", "Earnings", "EPS", "DilutedEPS", "Margin", "Inventory", "Assets", "Liabilities", "ShOut", "DilutedShOut"):
                        _require(Decimal(values[name]).is_finite(), "Invalid financial number.")
            result.count = sum(counts.values()) - before
            _check_saved_file(root, saved, result)
        except Exception as error:
            result.error = error
        result.audit_used, result.audit_measurements = reader.used, reader.measurements
        if result.error is not None:
            break
    else:
        try:
            _require(all(counts[kind] for kind in ("CMP", "SEC", "FIN")), "Every FINWIRE record type must be present.")
            _require(reports == _expected_financial_reports(config), "Financial company-period coverage is incomplete or unexpected.")
            # Market._emit_finwire posts every configured company and security at
            # the initial quarter start and at each financial release, at midnight.
            postings = _expected_finwire_postings(config)
            _require(company_postings == {(company, stamp) for company in range(1, config.companies + 1) for stamp in postings},
                     "Company posting coverage is incomplete or unexpected.")
            _require(security_postings == {(f"S{security:010d}", stamp) for security in range(1, config.securities + 1) for stamp in postings},
                     "Security posting coverage is incomplete or unexpected.")
        except Exception as error:
            result.error = error
    for versions in securities.values():
        versions.sort()
    return securities, dict(counts), results


def _security(securities, symbol, timestamp):
    """Resolve an active security version at the activity timestamp."""
    _require(symbol in securities, f"Missing security: {symbol}")
    version = None
    for candidate in securities[symbol]:
        if candidate[0] > timestamp:
            break
        version = candidate
    _require(version is not None and version[2] == "ACTV", f"Security is not active at {timestamp}: {symbol}")
    return version


def _dividend_dates(symbol, config, sessions):
    """Derive each security's quarterly payment dates from the trading calendar."""
    _require(re.fullmatch(r"S[0-9]{10}", symbol), "Dividend security does not use a generated symbol.")
    day_number = (int(symbol[1:]) - 1) % 25 + 1
    for year in range(config.start.year, config.end.year + 1):
        for month in (1, 4, 7, 10):
            due = date(year, month, day_number)
            while due <= config.end and due.isoformat() not in sessions:
                due += timedelta(days=1)
            if config.start <= due <= config.end:
                yield due.isoformat()


def _check_dividends(trade_events, cash_transactions, config, sessions, securities):
    """Reconstruct start-of-day share counts and require every expected payment."""
    changes = defaultdict(list)
    for row in trade_events:
        if row["T_ST_ID"] == "CMPT":
            changes[row["T_CA_ID"], row["T_S_SYMB"]].append(row)
    expected = {}
    for (account, symbol), trades in changes.items():
        ordered = sorted(trades, key=lambda row: (row["T_DTS"], row["T_ID"]))
        offset, quantity = 0, 0
        for due in _dividend_dates(symbol, config, sessions):
            while offset < len(ordered) and ordered[offset]["T_DTS"][:10] < due:
                row = ordered[offset]
                quantity += (-1 if row["T_TT_ID"] in {"TMS", "TLS", "TSL"} else 1) * row["T_QTY"]
                offset += 1
            annual = _security(securities, symbol, due + " 06:00:00")[1]
            amount = int((Decimal(quantity * annual) / 4).quantize(Decimal(1), rounding=ROUND_HALF_UP))
            if amount > 0:
                expected[account, symbol, due] = amount
    actual = {}
    for row in cash_transactions:
        if row["CT_NAME"].startswith("Dividend "):
            key = row["CT_CA_ID"], row["CT_NAME"][9:], row["CT_DTS"][:10]
            _require(key not in actual, "Duplicate dividend for an account and security on one date.")
            actual[key] = row["CT_AMT"]
    _require(actual == expected, "Missing, extra, mistimed, or incorrect start-of-day dividend.")


def _sessions(config, first=None):
    """Derive NYSE session dates and early closes independently of saved metadata."""
    import exchange_calendars as xcals

    first = _prior_year(config.start) if first is None else first
    calendar = xcals.get_calendar("XNYS", start=first.isoformat(), end=config.end.isoformat())
    early = {stamp.date().isoformat() for stamp in calendar.early_closes}
    return {stamp.date().isoformat(): "13:00:00" if stamp.date().isoformat() in early else "16:00:00"
            for stamp in calendar.sessions if first <= stamp.date() <= config.end}


def _stream(path, source):
    """Parse ordered cash transaction JSON fields with strict types and exact cents."""
    expected = ["ct_id", "ct_ca_id", "ct_dts", "ct_amt", "ct_name", "schema_version"]
    for number, line in enumerate(_lines(path), 1):
        def pairs(items):
            """Reject unexpected JSON field order, missing fields, and duplicate keys."""
            _require([name for name, _ in items] == expected, "Unexpected stream fields, field order, or duplicate JSON key.")
            return dict(items)
        try:
            payload = json.loads(line, object_pairs_hook=pairs)
            _require(isinstance(payload, dict), "A stream row must be a JSON object.")
            _require(type(payload["ct_id"]) is int and type(payload["ct_ca_id"]) is int and
                     type(payload["schema_version"]) is int and payload["schema_version"] == 1,
                     "Stream IDs and schema_version must be integers, not booleans.")
            _require(isinstance(payload["ct_dts"], str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", payload["ct_dts"]),
                     "Invalid UTC stream timestamp.")
            _require(isinstance(payload["ct_amt"], str) and re.fullmatch(r"-?\d+\.\d{2}", payload["ct_amt"]),
                     "Stream amount must be decimal text with exactly two decimal places.")
            _require(isinstance(payload["ct_name"], str), "Stream name must be text.")
            row = {"CT_ID": str(payload["ct_id"]), "CT_CA_ID": str(payload["ct_ca_id"]),
                   "CT_DTS": payload["ct_dts"][:-1].replace("T", " "), "CT_AMT": payload["ct_amt"], "CT_NAME": payload["ct_name"]}
            for field in source.fields:
                field.check(row[field.name])
            yield row
        except (ValueError, TypeError, KeyError) as error:
            raise ValidationError(f"{path}, row {number}: {error}") from error


def _customer_replay(stage, partition, securities, tax_rates):
    """Apply partial customer versions and prove watch lifetimes and cancellations."""
    events = list(stage.records("CustomerXML", partition))
    for row in stage.records("Customer", partition):
        row.update(_present=[name for name in row if name.startswith("C_")],
                   _timestamp=row["_day"] + (" 23:59:59" if row["C_ST_ID"] == "INAC" else " 00:00:00"), _kind="CDC")
        events.append(row)
    events.sort(key=lambda row: (row["_timestamp"], row["C_ID"], row["_order"]))
    current = {}
    lifetimes = {}
    for row in events:
        identifier = row["C_ID"]
        existing = current.get(identifier)
        _require((existing is None) == (row["_flag"] == "I"), "Customer insertion reused an ID or update has no insertion.")
        _require(existing is None or existing["C_ST_ID"] == "ACTV", "Customer update follows inactivation.")
        values = {} if existing is None else dict(existing)
        for name in row["_present"]:
            values[name] = row[name]
        _require(values["C_TIER"] in (1, 2, 3) and values["C_ST_ID"] in {"ACTV", "INAC"}, "Invalid customer tier or status.")
        _require(values["C_LCL_TX_ID"] in tax_rates and values["C_NAT_TX_ID"] in tax_rates, "Customer tax rate is missing.")
        born = date.fromisoformat(values["C_DOB"])
        current_date = date.fromisoformat(row["_day"])
        age = current_date.year - born.year
        _require(born <= current_date and age <= 120, "Customer date of birth is outside the valid range.")
        if existing is None:
            lifetimes[identifier] = [row["_timestamp"], None]
        if values["C_ST_ID"] == "INAC":
            lifetimes[identifier][1] = row["_timestamp"]
        current[identifier] = values
    active = set()
    for row in sorted(stage.records("WatchHistory", partition), key=lambda item: (item["W_DTS"], item["_order"])):
        identity = row["W_C_ID"], row["W_S_SYMB"]
        lifetime = lifetimes.get(identity[0])
        _require(lifetime is not None and lifetime[0] <= row["W_DTS"] and
                 (lifetime[1] is None or row["W_DTS"] <= lifetime[1]), "Watch lies outside the customer lifetime.")
        _security(securities, identity[1], row["W_DTS"])
        _require(row["W_ACTION"] in {"ACTV", "CNCL"}, "Invalid watch action.")
        if row["W_ACTION"] == "ACTV":
            _require(identity not in active and (lifetime[1] is None or row["W_DTS"] < lifetime[1]), "Duplicate or inactive watch placement.")
            active.add(identity)
        else:
            _require(identity in active, "Watch cancellation has no active placement.")
            active.remove(identity)
    _require(all(lifetimes[customer][1] is None for customer, _ in active), "Customer inactivation did not cancel all watches.")
    schema = {"customer_id": pl.Int64, "created": pl.String, "closed": pl.String}
    rows = [{"customer_id": identifier, "created": lifetime[0], "closed": lifetime[1]} for identifier, lifetime in lifetimes.items()]
    directory = stage.root / "customers"
    directory.mkdir(exist_ok=True)
    pl.DataFrame(rows, schema=schema).write_parquet(directory / f"{partition:08d}.parquet")
    return len(current)


def _causal_events(events):
    """Keep source-proven dependencies when several events share one second."""
    events.sort(key=lambda event: (event.timestamp, event.causal_order, event.rank,
                                   event.row["_order"] if event.kind == "trade" else 0))
    for _, same_second in groupby(events, key=lambda item: item.timestamp):
        group = list(same_second)
        edges = [set() for _ in group]
        indegree = [0] * len(group)
        transitions = defaultdict(list)
        insertions = defaultdict(list)
        cash_nodes = []

        def precedes(left, right):
            """Add one causal dependency without counting the same edge twice."""
            if right not in edges[left]:
                edges[left].add(right)
                indegree[right] += 1

        for index, event in enumerate(group):
            kind, row = event.kind, event.row
            if kind == "trade":
                transitions[row["T_ID"]].append(index)
                if row["_flag"] == "I":
                    insertions[row["T_CA_ID"]].append(index)
            elif kind == "cash":
                cash_nodes.append(index)
        # Source order fixes transitions of the same trade. Immediate market
        # completion must also precede the next insertion for that account.
        for indices in transitions.values():
            indices.sort(key=lambda index: group[index].row["_order"])
            for left, right in zip(indices, indices[1:]):
                precedes(left, right)
        for indices in insertions.values():
            indices.sort(key=lambda index: (group[index].row["T_ID"], group[index].row["_order"]))
            for left, right in zip(indices, indices[1:]):
                precedes(left, right)
                if group[left].row["T_TT_ID"] in {"TMB", "TMS"}:
                    terminal = next((node for node in transitions[group[left].row["T_ID"]]
                                     if group[node].row["T_ST_ID"] == "CMPT"), None)
                    if terminal is not None:
                        precedes(terminal, right)
        # Settlement links completion to the source cash transaction sequence.
        cash_nodes.sort(key=lambda index: group[index].row["CT_ID"])
        previous_cash = None
        for index in cash_nodes:
            row = group[index].row
            if previous_cash is not None:
                precedes(previous_cash, index)
            if row["CT_NAME"].startswith("Trade settlement "):
                trade_id = int(row["CT_NAME"][17:])
                indices = transitions.get(trade_id, [])
                terminal = next((node for node in indices if group[node].row["T_ST_ID"] == "CMPT"), None)
                if terminal is not None:
                    precedes(terminal, index)
                    if previous_cash is not None:
                        precedes(previous_cash, indices[0])
            previous_cash = index
        def priority(index):
            """Prefer source-proven cash order and new pending reservations in ties."""
            kind, row = group[index].kind, group[index].row
            if kind == "cash":
                return 0, index
            if kind == "trade" and row["_flag"] == "I" and row["T_ST_ID"] == "PNDG":
                # No source ordinal places this new pending order against an older
                # outcome. Use the executor's new-submission-before-due precedence.
                return 1, index
            return 2, index

        available = [priority(index) for index, count in enumerate(indegree) if count == 0]
        heapq.heapify(available)
        visited = 0
        while available:
            _, index = heapq.heappop(available)
            visited += 1
            yield group[index]
            for following in edges[index]:
                indegree[following] -= 1
                if indegree[following] == 0:
                    heapq.heappush(available, priority(following))
        _require(visited == len(group), "Same-second trade insertions conflict with the source cash transaction order.")


def _check_trade_channel(row, account, config):
    """Check the dated format and the channel implied by hidden account profile."""
    channel = row.get("T_CHANNEL")
    if row["_day"] == "initial" or row["_day"] < config.trade_channel_start:
        _require(channel is None, "Trade channel appears before the daily schema change.")
        return
    shares = ((.45, .25), (.50, .35), (.20, .10), (.55, .40), (.45, .53))
    profile = account["profile"]
    _require(0 <= profile < len(shares), "Trade channel has an unknown hidden profile.")
    web, mobile = shares[profile]
    draw = uniform(row["T_ID"], config.seed, "source/trade-channel")
    expected = "WEB" if draw < web else "MOBILE" if draw < web + mobile else "BROKER"
    _require(channel == expected, "Trade channel differs from its profile-weighted source recording.")


def _replay_accounts(stage, partition, brokers):
    """Resolve account versions against saved customer lifetimes and hidden traits."""
    versions = list(stage.records("AccountXML", partition))
    for row in stage.records("Account", partition):
        row.update(_present=[name for name in row if name.startswith("CA_")],
                   _timestamp=row["_day"] + (" 23:59:59" if row["CA_ST_ID"] == "INAC" else " 00:00:00"), _kind="CDC")
        versions.append(row)
    hidden_frame = stage.frame("Hidden", partition)
    hidden = {row["account_id"]: row for row in hidden_frame.iter_rows(named=True)}
    _require(len(hidden) == hidden_frame.height, "Duplicate hidden account ID.")
    owners = list({row["CA_C_ID"] for row in versions})
    customers = {row["customer_id"]: row for row in pl.scan_parquet(stage.root / "customers" / "*.parquet")
                 .filter(pl.col("customer_id").is_in(owners)).collect(engine="streaming").iter_rows(named=True)}
    accounts = {}
    for row in sorted(versions, key=lambda item: (item["_timestamp"], item["CA_ID"], item["_order"])):
        identifier = row["CA_ID"]
        existing = accounts.get(identifier)
        _require((existing is None) == (row["_flag"] == "I"), "Account insertion reused an ID or update has no insertion.")
        _require(existing is None or existing["closed"] is None, "Account update follows closure.")
        owner = row["CA_C_ID"]
        _require(owner in customers, "Account customer is missing.")
        customer = customers[owner]
        _require(row["_timestamp"] >= customer["created"] and (customer["closed"] is None or row["_timestamp"] <= customer["closed"]),
                 "Account version lies outside the customer lifetime.")
        if existing is None:
            _require(identifier in hidden, "Hidden account attributes are missing.")
            attributes = hidden[identifier]
            _require(attributes["customer_id"] == owner, "Hidden account owner differs from source owner.")
            _require(str(attributes["open_date"]) == row["_day"], "Hidden account open date differs from source creation.")
            existing = {"created": row["_timestamp"], "closed": None, "owner": owner, "values": {}, **attributes}
            accounts[identifier] = existing
        _require(existing["owner"] == owner, "Account update changed ownership.")
        for name in row["_present"]:
            existing["values"][name] = row[name]
        values = existing["values"]
        _require(values["CA_B_ID"] in brokers and values["CA_TAX_ST"] in (0, 1, 2) and values["CA_ST_ID"] in {"ACTV", "INAC"},
                 "Invalid account broker, tax status, or status.")
        if values["CA_ST_ID"] == "INAC":
            existing["closed"] = row["_timestamp"]
    _require(set(accounts) == set(hidden), "Hidden account and source account identities differ.")
    return accounts, customers


def _account_closures(accounts, customers, sessions):
    """Check actual closure dates and create explicit replay boundary events."""
    account_events = []
    for identifier, account in accounts.items():
        customer = customers[account["owner"]]
        if customer["closed"] is not None:
            # Both initial CMS INACT actions and daily Account deliveries carry
            # explicit closures. A customer row cannot replace that evidence.
            _require(account["closed"] is not None and account["closed"] <= customer["closed"],
                     "Customer inactivation requires explicit account closure evidence.")
        hidden_close = account["close_date"]
        _require((hidden_close is None and account["closed"] is None) or
                 (hidden_close is not None and account["closed"] is not None and str(hidden_close) == account["closed"][:10]),
                 "Hidden account closure differs from source closure.")
        if account["closed"]:
            # Activity.run requires an open market for liquidation in both periods.
            _require(account["closed"][:10] in sessions, "Account closure must fall on an NYSE session date.")
            account_events.append(ReplayEvent(account["closed"], 10**20, identifier, "close", identifier))

    return account_events


def _replay_trade_versions(stage, partition):
    """Expand initial snapshots through their histories, then append daily versions."""
    snapshots = {}
    daily = []
    for row in stage.records("Trade", partition):
        identifier = row["T_ID"]
        _require(identifier // 1000000 == row["T_CA_ID"], "Trade composite ID does not match its account.")
        if row["_day"] == "initial":
            _require(identifier not in snapshots, "Initial trade reused an ID.")
            snapshots[identifier] = row
        else:
            daily.append(row)
    histories = defaultdict(list)
    for row in stage.records("TradeHistory", partition):
        _require(row["TH_T_ID"] in snapshots, "Initial trade history has no initial trade.")
        histories[row["TH_T_ID"]].append(row)
    trade_events = []
    for identifier, snapshot in snapshots.items():
        amounts = [snapshot[name] for name in ("T_TRADE_PRICE", "T_CHRG", "T_COMM", "T_TAX")]
        _require(all(value is not None for value in amounts) if snapshot["T_ST_ID"] == "CMPT" else all(value is None for value in amounts),
                 "Only completed initial snapshots may have execution amounts.")
        history = sorted(histories.get(identifier, []), key=lambda row: (row["TH_DTS"], row["_order"]))
        _require(bool(history), "Initial trade has no history.")
        _require((history[-1]["TH_ST_ID"], history[-1]["TH_DTS"]) == (snapshot["T_ST_ID"], snapshot["T_DTS"]),
                 "Initial trade snapshot does not match its latest history.")
        for number, row in enumerate(history):
            event = {**snapshot, "T_ST_ID": row["TH_ST_ID"], "T_DTS": row["TH_DTS"], "_order": row["_order"],
                     "_flag": "I" if number == 0 else "U"}
            if event["T_ST_ID"] != "CMPT":
                for name in ("T_TRADE_PRICE", "T_CHRG", "T_COMM", "T_TAX"):
                    event[name] = None
            trade_events.append(event)
    trade_events.extend(daily)
    return trade_events


def _replay_inputs(stage, partition, config, sessions, securities):
    """Index holding changes and settlements and verify all dividend payments."""
    trade_events = _replay_trade_versions(stage, partition)
    holding_changes = {}
    for row in stage.records("HoldingHistory", partition):
        _require(row["HH_T_ID"] not in holding_changes, "A trade has more than one holding change.")
        holding_changes[row["HH_T_ID"]] = row
    cash_transactions = list(stage.records("CashTransaction", partition))
    _check_dividends(trade_events, cash_transactions, config, sessions, securities)
    settlements = {}
    for row in cash_transactions:
        if row["CT_NAME"].startswith("Trade settlement "):
            trade_id = int(row["CT_NAME"][17:])
            _require(trade_id not in settlements, "A trade has more than one settlement.")
            settlements[trade_id] = row
    return trade_events, holding_changes, cash_transactions, settlements


def _replay_events(account_events, trade_events, cash_transactions, settlements):
    """Order account boundaries, trades, and cash transactions by source evidence."""
    events = list(account_events)
    for row in trade_events:
        settlement = settlements.get(row["T_ID"])
        # CT_ID retains the source's same-second cash order. A trade ID does not:
        # an older pending order can complete after a newer market order.
        causal_order = (settlement["CT_ID"] if settlement is not None and settlement["CT_DTS"] == row["T_DTS"]
                        else 10**19 + row["T_ID"])
        events.append(ReplayEvent(row["T_DTS"], causal_order, 0, "trade", row))
    events.extend(ReplayEvent(row["CT_DTS"], row["CT_ID"], 1, "cash", row) for row in cash_transactions)
    return _causal_events(events)


class ReplayEvent(NamedTuple):
    """Name the timestamp, source ordering evidence, event kind, and payload."""

    timestamp: str
    causal_order: int
    rank: int
    kind: str
    row: dict | int


class _AccountReplay:
    """Replay one account partition with correct fees, balances, and reservations."""

    def __init__(self, accounts, holding_changes, settlements, config: Config, securities, sessions):
        """Start empty money and holding state for the partition's saved accounts."""
        self.accounts, self.holding_changes, self.settlements = accounts, holding_changes, settlements
        self.config, self.securities, self.sessions = config, securities, sessions
        self.balances = defaultdict(int)
        self.holding_state = {}
        self.trades = {}
        self.reservations = {}
        self.reserved_cash = defaultdict(int)
        self.reserved_shares = defaultdict(int)
        self.completed = set()

    def account_at(self, account_id, timestamp):
        """Require an existing account whose lifetime contains this activity."""
        _require(account_id in self.accounts, "Activity account is missing.")
        account = self.accounts[account_id]
        _require(timestamp >= account["created"] and (account["closed"] is None or timestamp <= account["closed"]),
                 "Activity lies outside the account lifetime.")
        return account

    def check_spending(self, account_id, account, message):
        """Protect reserved cash and the account's allowed margin borrowing."""
        limit = 0 if account["is_cash"] else account["margin_limit_cents"]
        _require(self.balances[account_id] - self.reserved_cash[account_id] >= -limit, message)

    def close_account(self, identifier):
        """Prove that closure removes no remaining money, holdings, or open order."""
        _require(self.balances[identifier] == 0 and not any(account == identifier for account, _ in self.holding_state) and
                 not any(trade["T_CA_ID"] == identifier and trade["T_ST_ID"] not in {"CMPT", "CNCL"} for trade in self.trades.values()),
                 "Account closed before liquidation, debt repayment, withdrawal, or order cancellation.")

    def cash_transaction(self, row):
        """Check ordinary money movements; completed trades apply settlements once."""
        account_id, timestamp = row["CT_CA_ID"], row["CT_DTS"]
        account = self.account_at(account_id, timestamp)
        amount, name = row["CT_AMT"], row["CT_NAME"]
        if name == "Deposit":
            _require(amount > 0, "Deposit must be positive.")
        elif name == "Withdrawal":
            _require(amount < 0, "Withdrawal must be negative.")
        elif name.startswith("Dividend "):
            symbol = name[9:]
            annual = _security(self.securities, symbol, timestamp)[1]
            holding = self.holding_state.get((account_id, symbol))
            _require(timestamp[11:] == "06:00:00" and timestamp[:10] in self.sessions and holding is not None,
                     "Dividend requires start-of-day holdings at 06:00 on a session date.")
            expected = int((Decimal(holding[1] * annual) / 4).quantize(Decimal(1), rounding=ROUND_HALF_UP))
            _require(amount == expected and expected > 0, "Dividend does not equal start-of-day shares times annual SEC dividend / 4.")
        else:
            _require(name.startswith("Trade settlement "), "Invalid cash transaction name.")
            identifier = int(name[17:])
            trade = self.trades.get(identifier)
            _require(trade is not None and trade["T_ST_ID"] == "CMPT" and identifier in self.completed, "Settlement has no completed trade.")
            return
        self.balances[account_id] += amount
        self.check_spending(account_id, account, "Cash balance or reserved spending exceeds the borrowing limit.")

    def check_transition(self, row):
        """Enforce lifecycle transitions and retain immutable order attributes."""
        identifier, status, timestamp = row["T_ID"], row["T_ST_ID"], row["T_DTS"]
        previous = self.trades.get(identifier)
        if row["_flag"] == "I":
            expected = "SBMT" if row["T_TT_ID"] in {"TMB", "TMS"} else "PNDG"
            _require(previous is None and status == expected, "Trade insertion reused an ID or has an invalid initial status.")
        else:
            _require(previous is not None and (previous["T_ST_ID"], status) in
                     {("PNDG", "SBMT"), ("PNDG", "CMPT"), ("PNDG", "CNCL"), ("SBMT", "CMPT"), ("SBMT", "CNCL")},
                     f"Invalid trade lifecycle transition: trade={identifier}, prior={previous['T_ST_ID'] if previous else None}, "
                     f"next={status}, flag={row['_flag']}, timestamp={timestamp}, source_order={row['_order']}.")
            _require(timestamp >= previous["T_DTS"], "Trade status timestamps must not decrease.")
            if previous["T_TT_ID"] in {"TMB", "TMS"}:
                _require(status == "CMPT" and timestamp == previous["T_DTS"],
                         "Market orders must complete at their creation timestamp.")
            _require(all(row[name] == previous[name] for name in ("T_CA_ID", "T_S_SYMB", "T_QTY", "T_TT_ID", "T_IS_CASH", "T_BID_PRICE", "T_EXEC_NAME")),
                     "Trade update changed immutable attributes.")
        amounts = [row[name] for name in ("T_TRADE_PRICE", "T_CHRG", "T_COMM", "T_TAX")]
        _require(all(value is not None for value in amounts) if status == "CMPT" else all(value is None for value in amounts),
                 "Only completed trades may have execution amounts.")

    def change_reservation(self, row, account):
        """Release prior reservations, then reserve only still-open order amounts."""
        identifier, account_id = row["T_ID"], row["T_CA_ID"]
        key = account_id, row["T_S_SYMB"]
        sell = row["T_TT_ID"] in {"TMS", "TLS", "TSL"}
        if identifier in self.reservations:
            reserve = self.reservations.pop(identifier)
            if sell:
                self.reserved_shares[key] -= reserve
            else:
                self.reserved_cash[account_id] -= reserve
        if row["T_ST_ID"] in {"CMPT", "CNCL"}:
            return
        if sell:
            available = self.holding_state.get(key, (0, 0))[1] - self.reserved_shares[key]
            _require(available >= row["T_QTY"], f"Open sell order reserves unavailable shares: trade={identifier}, account={account_id}, timestamp={row['T_DTS']}, available={available}, requested={row['T_QTY']}, position={self.holding_state.get(key)}, reserved={self.reserved_shares[key]}.")
            self.reserved_shares[key] += row["T_QTY"]
            self.reservations[identifier] = row["T_QTY"]
        else:
            value = row["T_QTY"] * row["T_BID_PRICE"]
            reserve = value + self.config.charge_cents + (value * self.config.commission_basis_points + 5000) // 10000
            self.reserved_cash[account_id] += reserve
            self.check_spending(account_id, account, "Open buy orders reserve unavailable funds.")
            self.reservations[identifier] = reserve

    def change_holding(self, row):
        """Match each completed trade to the saved origin and before/after quantity."""
        identifier, timestamp = row["T_ID"], row["T_DTS"]
        key = row["T_CA_ID"], row["T_S_SYMB"]
        holding = self.holding_changes.get(identifier)
        _require(holding is not None, "Completed trade has no holding change.")
        origin, before = self.holding_state.get(key, (identifier, 0))
        sell = row["T_TT_ID"] in {"TMS", "TLS", "TSL"}
        after = before + (-1 if sell else 1) * row["T_QTY"]
        _require(after >= self.reserved_shares[key], "Completed sell exceeds unreserved holdings.")
        _require((holding["HH_H_T_ID"], holding["HH_BEFORE_QTY"], holding["HH_AFTER_QTY"]) == (origin, before, after),
                 "Holding origin or before/after quantity differs from position replay.")
        _require(holding["_day"] == "initial" if timestamp[:10] <= str(self.config.cutover) else holding["_day"] == timestamp[:10],
                 "Holding change lies in the wrong delivery.")
        if after:
            self.holding_state[key] = origin, after
        else:
            self.holding_state.pop(key, None)

    def complete_trade(self, row, account):
        """Check correct fees and prices, then apply the exact holding and cash effects."""
        identifier, account_id, timestamp = row["T_ID"], row["T_CA_ID"], row["T_DTS"]
        price, charge, commission, tax = [row[name] for name in ("T_TRADE_PRICE", "T_CHRG", "T_COMM", "T_TAX")]
        expected_commission = (row["T_QTY"] * price * self.config.commission_basis_points + 5000) // 10000
        _require(price > 0 and charge == self.config.charge_cents and commission == expected_commission and tax == 0,
                 "Stage 1 execution fees or tax differ from the configured rule.")
        low, high = row.get("_market_low"), row.get("_market_high")
        _require(low is not None and low <= price <= high, "Execution price lies outside the daily low and high.")
        if row["T_TT_ID"] == "TLB":
            _require(price <= row["T_BID_PRICE"], "Limit buy exceeds its bid.")
        elif row["T_TT_ID"] == "TLS":
            _require(price >= row["T_BID_PRICE"], "Limit sell is below its bid.")
        self.change_holding(row)
        settlement = self.settlements.get(identifier)
        sell = row["T_TT_ID"] in {"TMS", "TLS", "TSL"}
        expected = (1 if sell else -1) * row["T_QTY"] * price - charge - commission - tax
        # Activity.complete rejects zero and negative proceeds, not only losses.
        _require(not sell or expected > 0, "Sell proceeds must be positive after fees.")
        _require(settlement is not None and (settlement["CT_CA_ID"], settlement["CT_DTS"], settlement["CT_AMT"]) ==
                 (account_id, timestamp, expected), "Settlement does not match trade account, timestamp, value, or fees.")
        self.balances[account_id] += expected
        self.check_spending(account_id, account, "Settlement spends reserved funds or exceeds the cash or margin limit.")
        self.completed.add(identifier)

    def trade_event(self, row):
        """Follow one source trade transition through format, state, and ledger checks."""
        identifier, account_id, symbol, timestamp = row["T_ID"], row["T_CA_ID"], row["T_S_SYMB"], row["T_DTS"]
        account = self.account_at(account_id, timestamp)
        _check_trade_channel(row, account, self.config)
        _security(self.securities, symbol, timestamp)
        _require(timestamp[:10] in self.sessions and "09:30:00" <= timestamp[11:] <= self.sessions[timestamp[:10]],
                 "Trade timestamp lies outside its NYSE session, including early closes.")
        _require(row["T_TT_ID"] in {"TMB", "TMS", "TLB", "TLS", "TSL"} and row["T_IS_CASH"] == int(account["is_cash"]),
                 "Trade type or account cash type is invalid.")
        _require(row["T_QTY"] > 0 and row["T_BID_PRICE"] > 0, "Trade quantity and bid price must be positive.")
        self.check_transition(row)
        self.change_reservation(row, account)
        if row["T_ST_ID"] == "CMPT":
            self.complete_trade(row, account)
        self.trades[identifier] = row


def _account_replay(stage, partition, config, securities, sessions, brokers):
    """Prepare source-proven inputs and replay each named account event in order."""
    accounts, customers = _replay_accounts(stage, partition, brokers)
    closures = _account_closures(accounts, customers, sessions)
    trades, holding_changes, cash_transactions, settlements = _replay_inputs(stage, partition, config, sessions, securities)
    replay = _AccountReplay(accounts, holding_changes, settlements, config, securities, sessions)
    for event in _replay_events(closures, trades, cash_transactions, settlements):
        if event.kind == "close":
            replay.close_account(event.row)
        elif event.kind == "cash":
            replay.cash_transaction(event.row)
        else:
            replay.trade_event(event.row)
    _require(all(row["T_TT_ID"] not in {"TMB", "TMS"} or row["T_ST_ID"] == "CMPT" for row in replay.trades.values()),
             "Market orders must complete at their creation timestamp.")
    _require(set(holding_changes) == replay.completed and set(settlements) == replay.completed,
             "Holding changes or settlements include a non-completed trade.")
    return len(accounts), len(replay.trades), len(replay.completed)


def _account_partition(stage, partition, config, securities, sessions, brokers):
    """Check snapshot identities before replaying one independent account partition."""
    prospects = stage.frame("ProspectIdentity", partition)
    _require(prospects.is_empty() or not prospects.select("identity", "date").is_duplicated().any(),
             "Duplicate Prospect identity in one snapshot.")
    return _account_replay(stage, partition, config, securities, sessions, brokers)


def _replay_wave(executor, function, stage, *arguments):
    """Return partition results in serial order, including the first failing partition."""
    if executor is None:
        return [function(stage, partition, *arguments) for partition in range(stage.partitions)]
    futures = [executor.submit(function, stage, partition, *arguments) for partition in range(stage.partitions)]
    # Reading futures in partition order makes the failure independent of scheduling.
    return [future.result() for future in futures]


def _replay_partitions(stage, config, securities, sessions, brokers, tax_rates):
    """Finish every customer lifetime before starting the dependent account wave."""
    workers = min(config.workers, stage.partitions)
    if workers == 1:
        customers = _replay_wave(None, _customer_replay, stage, securities, tax_rates)
        accounts = _replay_wave(None, _account_partition, stage, config, securities, sessions, brokers)
    else:
        # Workers need only the saved blocks and schemas. Source conversion plans
        # belong to parsing and contain lexical-check closures that cannot be pickled.
        reader = _Stage(stage.root, stage.partitions, stage.date_partitions)
        reader.schemas = stage.schemas
        # Spawn avoids inheriting Polars thread pools.
        with ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn")) as executor:
            customers = _replay_wave(executor, _customer_replay, reader, securities, tax_rates)
            accounts = _replay_wave(executor, _account_partition, reader, config, securities, sessions, brokers)
    return sum(customers), [sum(values) for values in zip(*accounts)]


def _trade_schema_counts():
    """Start counts for the initial and dated daily Trade contracts."""
    return {"initial_v1": 0, "daily_v1": 0, "daily_v2": 0,
            "channels": {"WEB": 0, "MOBILE": 0, "BROKER": 0}}


class _ParsedFile:
    """Carry file counts, boundary keys, and staged block metadata, never source rows."""

    def __init__(self, relative):
        """Start an empty file summary that also survives a later parse failure."""
        self.relative = relative
        self.count = 0
        self.first_cash = self.last_cash = None
        self.first_market = self.last_market = None
        self.brokers = set()
        self.trade_schemas = _trade_schema_counts()
        self.schemas = {}
        self.peak_rows = 0
        self.error = None
        self.action_counts = {}
        self.audit_used = set()
        self.audit_measurements = {}


def _parse_finwire_files(inputs):
    """Keep FINWIRE dependencies ordered in one parser with read-only fault evidence."""
    root, config = inputs["root"], inputs["config"]
    audit = FaultAudit(config, _require, inputs["audit_entries"])
    return _finwire(root, [root / name for name in inputs["files"]], config,
                    inputs["industries"], audit, inputs["saved"])


# Only spawned parser workers use this process-local, read-only context.
_PARSE_INPUTS = None


def _initialize_parser(inputs):
    """Load process-local lexical contracts without pickling prepared check closures."""
    global _PARSE_INPUTS
    _PARSE_INPUTS = {**inputs, "sources": load_sources()}


def _parse_independent_file(job):
    """Parse into an isolated stage and return fault keys and counts, not source rows."""
    index, relative, entries = job
    inputs = _PARSE_INPUTS
    stage = _Stage(inputs["temporary"] / "files" / str(index), inputs["partitions"], inputs["date_partitions"])
    result = _ParsedFile(relative)
    audit = FaultAudit(inputs["config"], _require, entries)
    try:
        _parse_file(inputs["root"], relative, stage, inputs["config"], inputs["sources"], inputs["sessions"],
                    inputs["securities"], audit, inputs["saved"], result)
        for table in list(stage.buffers):
            stage.flush(table)
        result.schemas = stage.schemas
        result.peak_rows = stage.peak_rows
    except Exception as error:
        # Keep the valid prefix so an earlier cross-file failure still wins.
        result.error = error
    result.audit_used, result.audit_measurements = audit.used, audit.measurements
    return result


def _check_file_boundary(result, cash_sequence, previous_cash, previous_market):
    """Verify global keys in file order before accepting a worker's later error."""
    if result.first_cash is not None:
        identifier, order = result.first_cash
        _require(identifier == cash_sequence + 1, "CT_ID must be one global sequence 1..N across cutover.")
        _require(order > previous_cash, "Cash transactions must be in timestamp and ID order.")
    if result.first_market is not None:
        _require(previous_market is None or result.first_market > previous_market, "Market keys must be unique and ordered.")
    if result.error is not None:
        raise result.error
    if result.last_cash is not None:
        cash_sequence, previous_cash = result.last_cash
    if result.last_market is not None:
        previous_market = result.last_market
    return cash_sequence, previous_cash, previous_market


def _adopt_file_blocks(stage, index, result):
    """Move completed file blocks into the shared stage without reading their rows."""
    for table, schema in result.schemas.items():
        stage.schemas[table] = schema
        stage.buffers.setdefault(table, [])
        directory = stage.root / "blocks" / table
        directory.mkdir(parents=True, exist_ok=True)
        for path in (stage.root / "files" / str(index) / "blocks" / table).glob("*.parquet"):
            path.rename(directory / f"file-{index}-{path.name}")
            stage.counter[table] += 1
    stage.peak_rows = max(stage.peak_rows, result.peak_rows)


def _reference_start(config):
    """Find the first required date from the price history and initial FINWIRE posting."""
    ordinal = _quarter(config.start) - 5
    return min(_prior_year(config.start), date(ordinal // 4, ordinal % 4 * 3 + 1, 1))


def _check_time_reference(row, second):
    """Check clock labels, components, and business-hour flags against the time ID."""
    hour, remainder = divmod(second, 3600)
    minute, seconds = divmod(remainder, 60)
    full = f"{hour:02d}:{minute:02d}:{seconds:02d}"
    expected = {"TimeValue": full, "HourID": hour, "HourDesc": f"{hour:02d}",
                "MinuteID": minute, "MinuteDesc": full[:5], "SecondID": seconds, "SecondDesc": full,
                "MarketHoursFlag": int(34200 <= second < 57600), "OfficeHoursFlag": int(28800 <= second < 61200)}
    _require(all((int(row[name]) if isinstance(value, int) else row[name]) == value for name, value in expected.items()),
             "Time reference columns differ from SK_TimeID.")


def _check_date_reference(row, day, sessions):
    """Check the complete ordered calendar and its calendar, fiscal, and holiday labels."""
    quarter = (day.month - 1) // 3 + 1
    iso = day.isocalendar()
    expected = {"SK_DateID": int(day.strftime("%Y%m%d")), "DateValue": day.isoformat(),
                "DateDesc": f"{day:%B} {day.day}, {day.year}", "CalendarYearID": day.year,
                "CalendarYearDesc": str(day.year), "CalendarQtrID": day.year * 10 + quarter,
                "CalendarQtrDesc": f"{day.year} Q{quarter}", "CalendarMonthID": day.year * 100 + day.month,
                "CalendarMonthDesc": f"{day:%Y %B}", "CalendarWeekID": iso.year * 100 + iso.week,
                "CalendarWeekDesc": f"{iso.year}-W{iso.week:02d}", "DayOfWeekNum": day.isoweekday(),
                "DayOfWeekDesc": day.strftime("%A"), "FiscalYearID": day.year, "FiscalYearDesc": str(day.year),
                "FiscalQtrID": day.year * 10 + quarter, "FiscalQtrDesc": f"{day.year} Q{quarter}",
                "HolidayFlag": int(day.weekday() < 5 and day.isoformat() not in sessions)}
    _require(all((int(row[name]) if isinstance(value, int) else row[name]) == value for name, value in expected.items()),
             "Date reference columns or coverage differ from the generated calendar.")


def _file_contract(relative, sources, config):
    """Resolve the source family, delivery date, and dated contract once per file."""
    path = Path(relative)
    initial = relative.startswith("offline/initial/")
    day = "initial" if initial else (path.stem if relative.startswith("stream/") else path.parent.name)
    if not initial:
        day = datetime.strptime(day, "%Y%m%d").date().isoformat()
    name = "CashTransaction" if relative.startswith("stream/") else path.stem.split("_")[0]
    source = sources[name] if initial else source_for_day(sources[name], date.fromisoformat(day), config)
    return source, day, initial


def _parse_reference_rows(rows, source, day, stage, config, result):
    """Check physical lookup rows and stage Prospect snapshot identities."""
    name = source.name
    count, previous_employee = 0, -1
    seen, managers = set(), set()
    first = _reference_start(config) if name == "Date" else None
    sessions = _sessions(config, first) if name == "Date" else None
    for number, row in enumerate(rows, 1):
        count = number
        if name != "Prospect":
            identity = row[source.columns[0]]
            _require(identity not in seen, f"Duplicate {name} identity.")
            seen.add(identity)
        if name == "HR":
            employee = int(row["EmployeeID"])
            _require(employee > previous_employee, "HR employee IDs must be increasing.")
            previous_employee = employee
            managers.add(int(row["ManagerID"]))
            if row["EmployeeJobCode"] == "314":
                result.brokers.add(employee)
        elif name == "Time":
            _require(int(row["SK_TimeID"]) == number - 1, "Time reference must cover ordered seconds 0..86399.")
            _check_time_reference(row, number - 1)
        elif name == "Date":
            _check_date_reference(row, first + timedelta(days=number - 1), sessions)
        elif name == "Prospect":
            # Personal and Prospect column builders emit these source-contract codes.
            _require(row["Gender"] in {"M", "F", "U"} and row["MaritalStatus"] in {"S", "M", "D", "W", "U"}
                     and row["OwnOrRentFlag"] in {"O", "R", "U"}, "Invalid Prospect enum value.")
            identity = row["AgencyID"]
            bucket = int.from_bytes(hashlib.blake2b(identity.encode("ascii"), digest_size=8).digest(), "big") % stage.partitions
            schema = {"identity": pl.String, "date": pl.String, "_partition": pl.String}
            stage.add("ProspectIdentity", {"identity": identity, "date": day, "_partition": str(bucket)}, schema)
    if name != "Prospect":
        _require(count > 0, f"{name} reference must not be empty.")
    if name == "HR":
        # Reference.employee_columns assigns manager 0 and includes employee 0.
        _require(managers <= {int(identifier) for identifier in seen}, "HR manager is missing from employee roster.")
    elif name == "StatusType":
        _require({"ACTV", "INAC", "PNDG", "SBMT", "CMPT", "CNCL"} <= seen,
                 "StatusType reference is missing a replay status.")
    elif name == "TradeType":
        _require({"TMB", "TMS", "TLB", "TLS", "TSL"} <= seen,
                 "TradeType reference is missing a replay trade type.")
    elif name == "Date":
        _require(count == (config.end - first).days + 1, "Date reference must cover the complete generated range.")
    elif name == "Time":
        _require(count == 86400, "Time reference must contain 86400 seconds.")
    return count


def _check_trade_row(row, version, initial, previous, result):
    """Count the dated Trade schema and check physical initial snapshot order."""
    result.trade_schemas[version] += 1
    if version == "daily_v2":
        _require(row["T_CHANNEL"] in result.trade_schemas["channels"], "Invalid trade channel.")
        result.trade_schemas["channels"][row["T_CHANNEL"]] += 1
    if initial:
        identifier = int(row["T_ID"])
        _require(identifier > previous, "Initial trades must have unique increasing IDs.")
        return identifier
    return previous


def _check_cash_transaction_row(row, result):
    """Retain boundary keys before checks that can fail later in the same row."""
    identifier = int(row["CT_ID"])
    order = row["CT_DTS"], identifier
    if result.first_cash is None:
        result.first_cash = identifier, order
    previous = result.last_cash
    expected = previous[0] + 1 if previous is not None else identifier
    _require(identifier == expected, "CT_ID must be one global sequence 1..N across cutover.")
    _require(order > (previous[1] if previous is not None else ("", 0)),
             "Cash transactions must be in timestamp and ID order.")
    result.last_cash = identifier, order
    _require(re.fullmatch(r"Deposit|Withdrawal|Trade settlement [1-9][0-9]*|Dividend [A-Za-z0-9._-]+", row["CT_NAME"]),
             "Invalid cash transaction name.")


def _check_market_row(row, result, sessions, securities):
    """Keep price boundaries before checking sessions, price bounds, and security life."""
    day = row["DM_DATE"]
    order = day, row["DM_S_SYMB"]
    if result.first_market is None:
        result.first_market = order
    _require(result.last_market is None or order > result.last_market, "Market keys must be unique and ordered.")
    result.last_market = order
    _require(day in sessions and 0 < _cents(row["DM_LOW"]) <= _cents(row["DM_CLOSE"]) <= _cents(row["DM_HIGH"]),
             "Invalid market date or price bounds.")
    _require(int(row["DM_VOL"]) >= 0, "Market volume cannot be negative.")
    _security(securities, row["DM_S_SYMB"], day + " 23:59:59")


def _parse_activity_rows(rows, source, day, stage, config, sessions, securities, audit, result):
    """Check source order and delivery, restore faults, then stage typed activity."""
    name, initial = source.name, day == "initial"
    timestamp_field = {"Trade": "T_DTS", "TradeHistory": "TH_DTS", "CashTransaction": "CT_DTS",
                       "WatchHistory": "W_DTS", "DailyMarket": "DM_DATE"}.get(name)
    key_field = {"Trade": "T_CA_ID", "TradeHistory": "TH_T_ID", "HoldingHistory": "HH_T_ID",
                 "CashTransaction": "CT_CA_ID", "Customer": "C_ID", "Account": "CA_ID",
                 "WatchHistory": "W_C_ID", "DailyMarket": "DM_DATE"}[name]
    lower = str(_prior_year(config.start) if name == "DailyMarket" else config.start)
    upper = str(config.cutover)
    version = "initial_v1" if initial else "daily_v2" if day >= config.trade_channel_start else "daily_v1"
    count, previous_trade, previous_history, previous_watch = 0, -1, None, ""
    for number, row in enumerate(rows, 1):
        count = number
        row.setdefault("_order", number)
        if timestamp_field:
            date_value = row[timestamp_field][:10]
            _require(lower <= date_value <= upper if initial else date_value == day,
                     f"{name} timestamp lies outside its delivery.")
        key = row[key_field] if name == "DailyMarket" else int(row[key_field])
        if name in {"TradeHistory", "HoldingHistory"}:
            key //= 1000000
        if name == "Trade":
            previous_trade = _check_trade_row(row, version, initial, previous_trade, result)
        elif name == "TradeHistory":
            order = int(row["TH_T_ID"]), row["TH_DTS"], row["_order"]
            _require(previous_history is None or order > previous_history, "Initial trade history keys must be ordered.")
            previous_history = order
        elif name == "CashTransaction":
            _check_cash_transaction_row(row, result)
        elif name == "WatchHistory" and initial:
            _require(row["W_DTS"] >= previous_watch, "Initial watch history must be ordered by timestamp.")
            previous_watch = row["W_DTS"]
        elif name == "DailyMarket":
            _check_market_row(row, result, sessions, securities)
        if name in {"Customer", "Trade"}:
            row = audit.restore(name, row["T_DTS"][:10] if name == "Trade" else day, row)
        _stage_source(stage, source, row, day, key)
    return count


def _parse_file(root, relative, stage, config, sources, sessions, securities, audit, saved, result):
    """Choose one parser and retain its summary before checking saved inventory."""
    path = root / relative
    if path.suffix == ".xml":
        result.action_counts = _xml(stage, path, sources, config, audit)
        for _ in _lines(path):
            pass
        result.count = sum(result.action_counts.values())
    else:
        source, day, initial = _file_contract(relative, sources, config)
        rows = _stream(path, source) if relative.startswith("stream/") else _rows(path, source, 1 if initial else 2)
        if source.name in {"HR", "Time", "Date", "StatusType", "TradeType", "Industry", "TaxRate", "Prospect"}:
            result.count = _parse_reference_rows(rows, source, day, stage, config, result)
        else:
            result.count = _parse_activity_rows(rows, source, day, stage, config, sessions, securities, audit, result)
    _check_saved_file(root, saved, result)

def _check_saved_file(root, saved, result):
    """Check the parsed count and actual saved bytes against optional inventory evidence."""
    if saved is not None:
        path = root / result.relative
        evidence = saved[result.relative]
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        _require((result.count, path.stat().st_size, digest) == (evidence["rows"], evidence["bytes"], evidence["sha256"]),
                 f"File rows, bytes, or SHA-256 differ from saved evidence: {result.relative}")


def _parse_files(root, expected, stage, config, sources, sessions, securities, audit, saved, finwire_results):
    """Parse independent files in workers and consume all evidence in source order."""
    files = sorted(expected, key=lambda name: (0 if name.startswith("offline/initial/") else 1 if name.startswith("offline/") else 2, name))
    slices = audit.slices(expected)
    jobs = ((index, relative, slices.get(relative, {})) for index, relative in enumerate(files)
            if not Path(relative).name.startswith("FINWIRE"))
    inputs = {"root": root, "temporary": stage.root, "partitions": stage.partitions,
              "date_partitions": stage.date_partitions, "config": config, "sessions": sessions,
              "securities": securities, "saved": saved}
    workers = min(config.workers, sum(not Path(relative).name.startswith("FINWIRE") for relative in files))
    pool = (ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn"),
                                initializer=_initialize_parser, initargs=(inputs,)) if workers > 1 else nullcontext())
    counts, brokers, action_counts = {}, set(), {}
    cash_sequence, previous_cash, previous_market = 0, ("", 0), None
    trade_schemas = _trade_schema_counts()
    with pool as executor:
        # Bound queued summaries and tasks independently of the source file count.
        results = (executor.map(_parse_independent_file, jobs, buffersize=workers * 128)
                   if executor is not None else None)
        for index, relative in enumerate(files):
            if Path(relative).name.startswith("FINWIRE"):
                result = finwire_results[relative]
            elif executor is not None:
                result = next(results)
            else:
                result = _ParsedFile(relative)
                reader = audit.reader(slices.get(relative, {}))
                try:
                    _parse_file(root, relative, stage, config, sources, sessions, securities, reader, saved, result)
                except Exception as error:
                    result.error = error
                result.audit_used, result.audit_measurements = reader.used, reader.measurements
            cash_sequence, previous_cash, previous_market = _check_file_boundary(result, cash_sequence, previous_cash, previous_market)
            audit.accept(result.audit_used, result.audit_measurements)
            action_counts.update(result.action_counts)
            if result.schemas:
                _adopt_file_blocks(stage, index, result)
            counts[relative] = result.count
            brokers.update(result.brokers)
            for version in ("initial_v1", "daily_v1", "daily_v2"):
                trade_schemas[version] += result.trade_schemas[version]
            for channel in trade_schemas["channels"]:
                trade_schemas["channels"][channel] += result.trade_schemas["channels"][channel]
    return counts, brokers, cash_sequence, trade_schemas, action_counts


def _validation_inventory(root, config, inventory):
    """Prove file cadence and optional manifest coverage before parsing source bytes."""
    expected = _expected_files(config)
    actual = {path.relative_to(root).as_posix() for folder in (root / "offline", root / "stream") for path in folder.rglob("*") if path.is_file()}
    _require(actual == expected, f"Source file coverage differs: missing={sorted(expected - actual)[:8]}, unexpected={sorted(actual - expected)[:8]}")
    _require(not any((root / relative).is_symlink() for relative in actual), "Source files must not be symbolic links.")
    saved = inventory
    if saved is None and (root / "manifest.json").exists():
        saved = json.loads((root / "manifest.json").read_text())["files"]
    saved = {item["path"]: item for item in saved} if saved is not None else None
    if saved is not None:
        _require(set(saved) == expected, "Manifest file coverage differs from independently derived cadence.")
    total_bytes = sum((root / relative).stat().st_size for relative in actual)
    return expected, saved, total_bytes


def _stage_hidden_accounts(stage, root):
    """Read every saved account-trait file in bounded blocks and route account IDs."""
    hidden_paths = sorted((root / "summary" / "hidden").glob("accounts-*.parquet"))
    _require(bool(hidden_paths), "Hidden account attributes are required to prove cash and margin constraints.")
    hidden_schema = {"account_id": pl.Int64, "customer_id": pl.Int64, "is_cash": pl.Boolean,
                     "margin_limit_cents": pl.Int64, "wealth_cents": pl.Int64, "profile": pl.Int64,
                     "open_date": pl.Date, "close_date": pl.Date, "_partition": pl.String}
    for path in hidden_paths:
        for frame in pl.scan_parquet(path).select([name for name in hidden_schema if name != "_partition"]).collect_batches(chunk_size=8192):
            for row in frame.iter_rows(named=True):
                _require(row["account_id"] > 0 and row["customer_id"] > 0 and row["margin_limit_cents"] >= 0,
                         "Invalid hidden account identity or margin limit.")
                row["_partition"] = str(row["account_id"] % stage.partitions)
                stage.add("Hidden", row, hidden_schema)


def _check_market_coverage(stage, config, sessions, securities):
    """Require every active security on each promised market-history session."""
    for market_date in sessions:
        if not str(_prior_year(config.start)) <= market_date <= str(config.end):
            continue
        required_symbols = {symbol for symbol, versions in securities.items()
                            if any(version[0][:10] <= market_date for version in versions) and
                            _security(securities, symbol, market_date + " 23:59:59")[2] == "ACTV"}
        observed = stage.frame("DailyMarket", market_date)
        observed_symbols = set(observed["DM_S_SYMB"].to_list()) if not observed.is_empty() else set()
        _require(observed_symbols == required_symbols, f"Market coverage is incomplete for {market_date}.")


def _finwire_wave(root, expected, config, industries, saved, audit):
    """Read dependent FINWIRE files in one worker with only company fault evidence."""
    finwire_inputs = {"root": root, "files": [name for name in expected if Path(name).name.startswith("FINWIRE")],
                      "config": config, "industries": industries, "saved": saved,
                      "audit_entries": {key: evidence for key, evidence in audit.entries.items()
                                        if json.loads(key[0])[0] == "FINWIRE:CMP"}}
    # FINWIRE checks posting dependencies across files. Its ordered parser
    # supplies the security versions that ordinary file parsers need. Keep
    # this dependency wave separate; never share writable audit state.
    if config.workers > 1:
        with ProcessPoolExecutor(max_workers=1, mp_context=get_context("spawn")) as executor:
            securities, finwire_counts, finwire_results = executor.submit(_parse_finwire_files, finwire_inputs).result()
    else:
        securities, finwire_counts, finwire_results = _parse_finwire_files(finwire_inputs)
    # Later market checks need these security versions. Report the failed
    # dependency before a missing-version symptom or any ordinary-file failure.
    for result in finwire_results.values():
        if result.error is not None:
            raise result.error
    return securities, finwire_counts, finwire_results


def validate(root: Path, config: Config, inventory=None):
    """Parse saved sources, verify fault evidence, and replay partitioned business state."""
    root = Path(root)
    sources = load_sources()
    expected, saved, total_bytes = _validation_inventory(root, config, inventory)
    partitions = _partition_count(total_bytes)
    sessions = _sessions(config)
    industries = {row["IN_ID"] for row in _rows(root / "offline" / "initial" / "Industry.txt", sources["Industry"], 1)}
    tax_rates = {row["TX_ID"] for row in _rows(root / "offline" / "initial" / "TaxRate.txt", sources["TaxRate"], 1)}
    audit = FaultAudit.load(root, config, _require)
    with tempfile.TemporaryDirectory(prefix="validation-", dir=root) as temporary:
        securities, finwire_counts, finwire_results = _finwire_wave(root, expected, config, industries, saved, audit)
        stage = _Stage(Path(temporary), partitions, date_partitions=len(sessions))
        _stage_hidden_accounts(stage, root)
        counts, brokers, cash_sequence, trade_schemas, action_counts = _parse_files(
            root, expected, stage, config, sources, sessions, securities, audit, saved, finwire_results)
        # Parsing owns audit keys. Preserve the final unused-evidence failure's
        # place after replay while dropping every catalog and consumed-key owner.
        fault_count, error_measurements, audit_complete = audit.release()
        finwire_results.clear()
        del audit
        stage.finish()
        _check_market_coverage(stage, config, sessions, securities)
        customers, totals = _replay_partitions(stage, config, securities, sessions, brokers, tax_rates)
        _require(audit_complete, "Unused fault audit evidence has no configured source fault.")
        peak_rows = stage.peak_rows
    return {"status": "passed", "files": len(expected), "rows": sum(counts.values()), "bytes": total_bytes,
            "customers": customers, "accounts": totals[0], "trades": totals[1], "completed_trades": totals[2],
            "cash_transactions": cash_sequence, "customer_actions": action_counts, "finwire": finwire_counts,
            "partitions": partitions, "maximum_parse_buffer_rows": peak_rows, "verified_source_errors": fault_count,
            "source_recording": {"errors": error_measurements, "trade_schemas": trade_schemas},
            "checks": ["independent file cadence", "formats and LF endings", "saved hashes and counts" if saved is not None else "parsed counts",
                       "customer and account versions", "trade lifecycles and cutover snapshots", "cash and margin replay",
                       "source-visible order reservations", "settlements", "position holding history", "NYSE session and price bounds",
                       "start-of-day dividend amounts", "watch lifecycles and inactivation", "source error audit and correct-value replay", "dated trade channels",
                       "explicit account closures", "FINWIRE values and financial coverage", "reference meanings and coverage",
                       "market order completion", "HR and initial watch order", "Prospect enums and HR managers",
                       "closure sessions and positive sell proceeds", "company and security posting coverage"]}
