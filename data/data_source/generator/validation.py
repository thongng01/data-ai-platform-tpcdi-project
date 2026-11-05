"""Parse saved source records, then replay bounded account and customer partitions."""

import calendar
import hashlib
import heapq
import json
import math
import re
import tempfile
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from itertools import groupby
from pathlib import Path
from xml.etree import ElementTree as ET

import polars as pl

from .config import Config, read_yaml
from .formats import TYPES, load_sources


_VALIDATION_PARTITION_BYTES = 16 * 1024**2


class ValidationError(ValueError):
    pass


def _require(condition, message):
    if not condition:
        raise ValidationError(message)


def _cents(value):
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value)) * 100
        _require(result.is_finite() and result == result.to_integral_value(), "Money must have exact cent precision.")
        return int(result)
    except (InvalidOperation, ValueError) as error:
        raise ValidationError(f"Invalid money: {value}") from error


def _lines(path):
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
    return day.replace(year=day.year - 1, day=min(day.day, calendar.monthrange(day.year - 1, day.month)[1]))


def _quarter(day):
    return day.year * 4 + (day.month - 1) // 3


def _release(quarter):
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
    datatype = TYPES.get(field.type, field.type)
    if datatype.startswith(("NUM(", "SNUM(")) or datatype == "BOOLEAN":
        return pl.Int64 if "," not in datatype or datatype.endswith(",2)") else pl.String
    return pl.String


def _typed(source, row):
    result = {}
    for field in source.fields:
        value = row.get(field.name)
        datatype = _field_type(field)
        result[field.name] = (None if value in (None, "") else
                              _cents(value) if ",2)" in TYPES.get(field.type, field.type) else int(value)) if datatype == pl.Int64 else value
    return result


class _Stage:
    """Write bounded typed blocks, then let the streaming engine partition them."""

    def __init__(self, root, partitions):
        self.root = root
        self.partitions = partitions
        self.buffers = defaultdict(list)
        self.schemas = {}
        self.counter = defaultdict(int)
        self.rows = 0
        self.peak_rows = 0

    def add(self, table, row, schema):
        self.schemas[table] = schema
        self.buffers[table].append(row)
        self.rows += 1
        self.peak_rows = max(self.peak_rows, self.rows)
        if self.rows >= 8192:
            self.flush(max(self.buffers, key=lambda name: len(self.buffers[name])))

    def flush(self, table):
        rows = self.buffers[table]
        if not rows:
            return
        directory = self.root / "blocks" / table
        directory.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(rows, schema=self.schemas[table]).write_parquet(directory / f"{self.counter[table]:08d}.parquet")
        self.counter[table] += 1
        self.rows -= len(rows)
        rows.clear()

    def finish(self):
        for table in list(self.buffers):
            self.flush(table)
        # Join prices by day before account partitioning. This reads the market once,
        # not once per account partition, and never builds a whole-market hash map.
        tables = sorted(self.buffers, key=lambda name: (name != "DailyMarket", name))
        for table in tables:
            if not self.counter[table]:
                continue
            directory = self.root / "blocks" / table
            scan = pl.scan_parquet(directory / "*.parquet")
            if table == "Trade":
                day_root = self.root / "trade-days"
                scan.with_columns(pl.col("T_DTS").str.slice(0, 10).alias("_market_day")).sink_parquet(
                    pl.PartitionBy(day_root, key="_market_day", max_rows_per_file=65536), mkdir=True, engine="streaming")
                enriched = self.root / "trade-prices"
                enriched.mkdir()
                for day_directory in sorted(day_root.iterdir()):
                    day = day_directory.name.split("=", 1)[1]
                    market = self.frame("DailyMarket", day)
                    if market.is_empty():
                        prices = pl.DataFrame(schema={"T_S_SYMB": pl.String, "_market_low": pl.Int64, "_market_high": pl.Int64})
                    else:
                        prices = market.select(pl.col("DM_S_SYMB").alias("T_S_SYMB"),
                                               pl.col("DM_LOW").alias("_market_low"), pl.col("DM_HIGH").alias("_market_high"))
                    pl.scan_parquet(day_directory / "*.parquet").join(prices.lazy(), on="T_S_SYMB", how="left").sink_parquet(
                        enriched / f"{day}.parquet", engine="streaming")
                    for path in day_directory.glob("*.parquet"):
                        path.unlink()
                    day_directory.rmdir()
                day_root.rmdir()
                scan = pl.scan_parquet(enriched / "*.parquet")
            scan.sink_parquet(pl.PartitionBy(self.root / "partitioned" / table, key="_partition", max_rows_per_file=65536),
                              mkdir=True, engine="streaming")
            for path in directory.glob("*.parquet"):
                path.unlink()
            directory.rmdir()
            if table == "Trade":
                for path in enriched.glob("*.parquet"):
                    path.unlink()
                enriched.rmdir()

    def frame(self, table, partition):
        paths = list((self.root / "partitioned" / table / f"_partition={partition}").glob("*.parquet"))
        if not paths:
            return pl.DataFrame(schema=self.schemas.get(table, {}))
        return pl.scan_parquet(paths).collect(engine="streaming")

    def records(self, table, partition):
        return self.frame(table, partition).iter_rows(named=True)


def _source_schema(source):
    return {**{field.name: _field_type(field) for field in source.fields}, "_flag": pl.String,
            "_order": pl.Int64, "_day": pl.String, "_partition": pl.String}


def _stage_source(stage, source, row, day, key):
    result = _typed(source, row)
    result.update(_flag=row.get("_flag", "I"), _order=row.get("_order", 0), _day=day,
                  _partition=str(key) if source.name == "DailyMarket" else str(int(key) % stage.partitions))
    stage.add(source.name, result, _source_schema(source))


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


def _xml(stage, path, sources, config):
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
        key = timestamp, identifier
        _require(key >= previous, "Customer actions must be ordered by timestamp and customer ID.")
        previous = key
        counts[kind] += 1
        timestamp = timestamp.replace("T", " ")
        if kind in {"NEW", "UPDCUST", "INACT"}:
            patch["C_ST_ID"] = "INAC" if kind == "INACT" else "ACTV"
            if kind == "NEW":
                for field in sources["Customer"].fields:
                    _require(field.nullable or field.name in patch, f"A new customer requires {field.name}.")
            record = _typed(sources["Customer"], patch)
            record.update(_present=list(patch), _kind=kind, _timestamp=timestamp, _day=timestamp[:10],
                          _flag="I" if kind == "NEW" else "U", _order=counts[kind], _partition=str(identifier % stage.partitions))
            stage.add("CustomerXML", record, schema)
        accounts = customer.findall("Account")
        _require(bool(accounts) if kind in {"NEW", "ADDACCT", "UPDACCT", "CLOSEACCT"} else kind == "INACT" or not accounts,
                 "Unexpected number of accounts in XML action.")
        for account in accounts:
            _require(set(account.attrib) <= {"CA_ID", "CA_TAX_ST"} and "CA_ID" in account.attrib, "Invalid account XML attributes.")
            values = {**account.attrib, "CA_C_ID": str(identifier), "CA_ST_ID": "INAC" if kind in {"CLOSEACCT", "INACT"} else "ACTV"}
            names = set()
            for element in account:
                _require(element.tag in {"CA_B_ID", "CA_NAME"} and element.tag not in names and not element.attrib and not len(element),
                         "Invalid account XML field.")
                names.add(element.tag)
                values[element.tag] = element.text or ""
            for field in sources["Account"].fields:
                if field.name in values:
                    field.check(values[field.name])
                elif kind in {"NEW", "ADDACCT"}:
                    _require(field.nullable, f"A new account requires {field.name}.")
            account_id = int(values["CA_ID"])
            record = _typed(sources["Account"], values)
            record.update(_present=list(values), _kind=kind, _timestamp=timestamp, _day=timestamp[:10],
                          _flag="I" if kind in {"NEW", "ADDACCT"} else "U", _order=counts[kind],
                          _partition=str(account_id % stage.partitions))
            stage.add("AccountXML", record, account_schema)
        root.clear()
    return dict(counts)


def _finwire(paths, config, industries):
    layouts = read_yaml(Path(__file__).parent / "definitions" / "finwire.yaml")
    companies = {}
    securities = defaultdict(list)
    reports = set()
    counts = defaultdict(int)
    for path in sorted(paths, key=lambda item: (0 if item.parent.name == "initial" else 1, str(item))):
        previous = None
        for line in _lines(path):
            kind = line[15:18]
            _require(kind in layouts, "Invalid FINWIRE record type.")
            layout = layouts[kind]
            _require(len(line) == sum(field[0] for field in layout.values()), f"Invalid {kind} record width.")
            values = {}
            offset = 0
            for name, (width, _) in layout.items():
                values[name] = line[offset:offset + width].strip()
                offset += width
            timestamp = values["PTS"]
            posted = datetime.strptime(timestamp, "%Y%m%d-%H%M%S")
            _require(posted.strftime("%Y%m%d-%H%M%S") == timestamp, "Invalid FINWIRE PTS.")
            _require(path.name == f"FINWIRE{posted.year}Q{(posted.month - 1) // 3 + 1}", "FINWIRE is in the wrong posting-quarter file.")
            _require(posted.date() <= config.cutover if path.parent.name == "initial" else posted.strftime("%Y%m%d") == path.parent.name,
                     "FINWIRE release lies outside its delivery.")
            identity = values["CIK"] if kind == "CMP" else values["Symbol"] if kind == "SEC" else values["CoNameOrCIK"]
            key = timestamp, {"CMP": 0, "SEC": 1, "FIN": 2}[kind], identity
            _require(previous is None or key > previous, "FINWIRE rows must have unique ordered posting keys.")
            previous = key
            counts[kind] += 1
            if kind == "CMP":
                _require(values["Status"] in {"ACTV", "INAC"} and values["CIK"].isdigit(), "Invalid company identity or status.")
                _require(values["IndustryID"] in industries, "Company industry is missing.")
                _require(values["SPrating"] in {"AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-", "B+", "B", "B-", "CCC+", "CCC", "CCC-", "CC", "C", "D"}, "Invalid company credit rating.")
                companies[int(values["CIK"])] = posted.strftime("%Y-%m-%d %H:%M:%S")
            elif kind == "SEC":
                _require(values["CoNameOrCIK"].isdigit() and int(values["CoNameOrCIK"]) in companies, "Security company is missing.")
                dividend = _cents(values["Dividend"])
                _require(dividend is not None and dividend >= 0 and values["Status"] in {"ACTV", "INAC"}, "Invalid security dividend or status.")
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
                for name in ("Revenue", "Earnings", "EPS", "DilutedEPS", "Margin", "Inventory", "Assets", "Liabilities", "ShOut", "DilutedShOut"):
                    _require(Decimal(values[name]).is_finite(), "Invalid financial number.")
    _require(all(counts[kind] for kind in ("CMP", "SEC", "FIN")), "Every FINWIRE record type must be present.")
    for versions in securities.values():
        versions.sort()
    return securities, dict(counts)


def _security(securities, symbol, timestamp):
    _require(symbol in securities, f"Missing security: {symbol}")
    version = None
    for candidate in securities[symbol]:
        if candidate[0] > timestamp:
            break
        version = candidate
    _require(version is not None and version[2] == "ACTV", f"Security is not active at {timestamp}: {symbol}")
    return version


def _dividend_dates(symbol, config, sessions):
    _require(re.fullmatch(r"S[0-9]{10}", symbol), "Dividend security does not use a generated symbol.")
    day_number = (int(symbol[1:]) - 1) % 25 + 1
    for year in range(config.start.year, config.end.year + 1):
        for month in (1, 4, 7, 10):
            due = date(year, month, day_number)
            while due <= config.end and due.isoformat() not in sessions:
                due += timedelta(days=1)
            if config.start <= due <= config.end:
                yield due.isoformat()


def _check_dividends(trade_events, cash, config, sessions, securities):
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
    for row in cash:
        if row["CT_NAME"].startswith("Dividend "):
            key = row["CT_CA_ID"], row["CT_NAME"][9:], row["CT_DTS"][:10]
            _require(key not in actual, "Duplicate dividend for an account and security on one date.")
            actual[key] = row["CT_AMT"]
    _require(actual == expected, "Missing, extra, mistimed, or incorrect start-of-day dividend.")


def _sessions(config):
    import exchange_calendars as xcals

    first = _prior_year(config.start)
    calendar = xcals.get_calendar("XNYS", start=first.isoformat(), end=config.end.isoformat())
    early = {stamp.date().isoformat() for stamp in calendar.early_closes}
    return {stamp.date().isoformat(): "13:00:00" if stamp.date().isoformat() in early else "16:00:00"
            for stamp in calendar.sessions if first <= stamp.date() <= config.end}


def _stream(path, source):
    expected = ["ct_id", "ct_ca_id", "ct_dts", "ct_amt", "ct_name", "schema_version"]
    for number, line in enumerate(_lines(path), 1):
        def pairs(items):
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


def _customer_replay(stage, partition, config, securities, tax_rates):
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
    events.sort(key=lambda item: (*item[:3], item[4]["_order"] if item[3] == "trade" else 0))
    for _, same_second in groupby(events, key=lambda item: item[0]):
        group = list(same_second)
        edges = [set() for _ in group]
        indegree = [0] * len(group)
        transitions = defaultdict(list)
        insertions = defaultdict(list)
        cash_nodes = []

        def precedes(left, right):
            if right not in edges[left]:
                edges[left].add(right)
                indegree[right] += 1

        for index, (_, _, _, kind, row) in enumerate(group):
            if kind == "trade":
                transitions[row["T_ID"]].append(index)
                if row["_flag"] == "I":
                    insertions[row["T_CA_ID"]].append(index)
            elif kind == "cash":
                cash_nodes.append(index)
        for indices in transitions.values():
            indices.sort(key=lambda index: group[index][4]["_order"])
            for left, right in zip(indices, indices[1:]):
                precedes(left, right)
        for indices in insertions.values():
            indices.sort(key=lambda index: (group[index][4]["T_ID"], group[index][4]["_order"]))
            for left, right in zip(indices, indices[1:]):
                precedes(left, right)
                if group[left][4]["T_TT_ID"] in {"TMB", "TMS"}:
                    terminal = next((node for node in transitions[group[left][4]["T_ID"]]
                                     if group[node][4]["T_ST_ID"] == "CMPT"), None)
                    if terminal is not None:
                        precedes(terminal, right)
        cash_nodes.sort(key=lambda index: group[index][4]["CT_ID"])
        previous_cash = None
        for index in cash_nodes:
            row = group[index][4]
            if previous_cash is not None:
                precedes(previous_cash, index)
            if row["CT_NAME"].startswith("Trade settlement "):
                trade_id = int(row["CT_NAME"][17:])
                indices = transitions.get(trade_id, [])
                terminal = next((node for node in indices if group[node][4]["T_ST_ID"] == "CMPT"), None)
                if terminal is not None:
                    precedes(terminal, index)
                    if previous_cash is not None:
                        precedes(previous_cash, indices[0])
            previous_cash = index
        def priority(index):
            kind, row = group[index][3:]
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


def _account_replay(stage, partition, config, securities, sessions, brokers):
    versions = list(stage.records("AccountXML", partition))
    for row in stage.records("Account", partition):
        row.update(_present=[name for name in row if name.startswith("CA_")],
                   _timestamp=row["_day"] + (" 23:59:59" if row["CA_ST_ID"] == "INAC" else " 00:00:00"), _kind="CDC")
        versions.append(row)
    hidden = {row["account_id"]: row for row in stage.records("Hidden", partition)}
    _require(len(hidden) == stage.frame("Hidden", partition).height, "Duplicate hidden account ID.")
    owners = list({row["CA_C_ID"] for row in versions})
    customers = {row["customer_id"]: row for row in pl.scan_parquet(stage.root / "customers" / "*.parquet")
                 .filter(pl.col("customer_id").is_in(owners)).collect(engine="streaming").iter_rows(named=True)}
    accounts = {}
    account_events = []
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
    for identifier, account in accounts.items():
        customer = customers[account["owner"]]
        if customer["closed"] is not None:
            account["closed"] = min(value for value in (account["closed"], customer["closed"]) if value is not None)
        hidden_close = account["close_date"]
        _require((hidden_close is None and account["closed"] is None) or
                 (hidden_close is not None and account["closed"] is not None and str(hidden_close) == account["closed"][:10]),
                 "Hidden account closure differs from source closure.")
        if account["closed"]:
            account_events.append((account["closed"], 4, identifier, "close", identifier))

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
    holdings = {}
    for row in stage.records("HoldingHistory", partition):
        _require(row["HH_T_ID"] not in holdings, "A trade has more than one holding change.")
        holdings[row["HH_T_ID"]] = row
    cash = list(stage.records("CashTransaction", partition))
    _check_dividends(trade_events, cash, config, sessions, securities)
    settlements = {}
    for row in cash:
        if row["CT_NAME"].startswith("Trade settlement "):
            trade_id = int(row["CT_NAME"][17:])
            _require(trade_id not in settlements, "A trade has more than one settlement.")
            settlements[trade_id] = row
    events = [(timestamp, 10**20, identifier, kind, identifier) for timestamp, _, identifier, kind, _ in account_events]
    for row in trade_events:
        settlement = settlements.get(row["T_ID"])
        # CT_ID retains the source's same-second cash order. A trade ID does not:
        # an older pending order can complete after a newer market order.
        causal_order = (settlement["CT_ID"] if settlement is not None and settlement["CT_DTS"] == row["T_DTS"]
                        else 10**19 + row["T_ID"])
        events.append((row["T_DTS"], causal_order, 0, "trade", row))
    events.extend((row["CT_DTS"], row["CT_ID"], 1, "cash", row) for row in cash)
    balances = defaultdict(int)
    positions = {}
    trades = {}
    reservations = {}
    reserved_cash = defaultdict(int)
    reserved_shares = defaultdict(int)
    completed = set()

    def lifetime(account_id, timestamp):
        _require(account_id in accounts, "Activity account is missing.")
        account = accounts[account_id]
        _require(timestamp >= account["created"] and (account["closed"] is None or timestamp <= account["closed"]),
                 "Activity lies outside the account lifetime.")
        return account

    for timestamp, _, _, kind, row in _causal_events(events):
        if kind == "close":
            identifier = row
            _require(balances[identifier] == 0 and not any(account == identifier for account, _ in positions) and
                     not any(trade["T_CA_ID"] == identifier and trade["T_ST_ID"] not in {"CMPT", "CNCL"} for trade in trades.values()),
                     "Account closed before liquidation, debt repayment, withdrawal, or order cancellation.")
            continue
        if kind == "cash":
            account_id = row["CT_CA_ID"]
            account = lifetime(account_id, timestamp)
            amount, name = row["CT_AMT"], row["CT_NAME"]
            if name == "Deposit":
                _require(amount > 0, "Deposit must be positive.")
            elif name == "Withdrawal":
                _require(amount < 0, "Withdrawal must be negative.")
            elif name.startswith("Dividend "):
                symbol = name[9:]
                annual = _security(securities, symbol, timestamp)[1]
                position = positions.get((account_id, symbol))
                _require(timestamp[11:] == "06:00:00" and timestamp[:10] in sessions and position is not None,
                         "Dividend requires start-of-day holdings at 06:00 on a session date.")
                expected = int((Decimal(position[1] * annual) / 4).quantize(Decimal(1), rounding=ROUND_HALF_UP))
                _require(amount == expected and expected > 0, "Dividend does not equal start-of-day shares times annual SEC dividend / 4.")
            else:
                _require(name.startswith("Trade settlement "), "Invalid cash transaction name.")
                identifier = int(name[17:])
                trade = trades.get(identifier)
                _require(trade is not None and trade["T_ST_ID"] == "CMPT" and identifier in completed, "Settlement has no completed trade.")
                continue
            balances[account_id] += amount
            limit = 0 if account["is_cash"] else account["margin_limit_cents"]
            _require(balances[account_id] - reserved_cash[account_id] >= -limit, "Cash balance or reserved spending exceeds the borrowing limit.")
            continue
        identifier, account_id, symbol = row["T_ID"], row["T_CA_ID"], row["T_S_SYMB"]
        account = lifetime(account_id, timestamp)
        _security(securities, symbol, timestamp)
        _require(timestamp[:10] in sessions and "09:30:00" <= timestamp[11:] <= sessions[timestamp[:10]],
                 "Trade timestamp lies outside its NYSE session, including early closes.")
        _require(row["T_TT_ID"] in {"TMB", "TMS", "TLB", "TLS", "TSL"} and row["T_IS_CASH"] == int(account["is_cash"]),
                 "Trade type or account cash type is invalid.")
        _require(row["T_QTY"] > 0 and row["T_BID_PRICE"] > 0, "Trade quantity and bid price must be positive.")
        status, previous = row["T_ST_ID"], trades.get(identifier)
        if row["_flag"] == "I":
            _require(previous is None and status in {"PNDG", "SBMT"}, "Trade insertion reused an ID or has an invalid initial status.")
        else:
            _require(previous is not None and (previous["T_ST_ID"], status) in
                     {("PNDG", "SBMT"), ("PNDG", "CMPT"), ("PNDG", "CNCL"), ("SBMT", "CMPT"), ("SBMT", "CNCL")},
                     f"Invalid trade lifecycle transition: trade={identifier}, prior={previous['T_ST_ID'] if previous else None}, "
                     f"next={status}, flag={row['_flag']}, timestamp={timestamp}, source_order={row['_order']}.")
            _require(timestamp >= previous["T_DTS"], "Trade status timestamps must not decrease.")
            _require(all(row[name] == previous[name] for name in ("T_CA_ID", "T_S_SYMB", "T_QTY", "T_TT_ID", "T_IS_CASH", "T_BID_PRICE", "T_EXEC_NAME")),
                     "Trade update changed immutable attributes.")
        values = [row[name] for name in ("T_TRADE_PRICE", "T_CHRG", "T_COMM", "T_TAX")]
        _require(all(value is not None for value in values) if status == "CMPT" else all(value is None for value in values),
                 "Only completed trades may have execution amounts.")
        sell = row["T_TT_ID"] in {"TMS", "TLS", "TSL"}
        position_key = account_id, symbol
        if identifier in reservations:
            reserve = reservations.pop(identifier)
            if sell:
                reserved_shares[position_key] -= reserve
            else:
                reserved_cash[account_id] -= reserve
        if status not in {"CMPT", "CNCL"}:
            if sell:
                available = positions.get(position_key, (0, 0))[1] - reserved_shares[position_key]
                _require(available >= row["T_QTY"], "Open sell order reserves unavailable shares.")
                reserved_shares[position_key] += row["T_QTY"]
                reservations[identifier] = row["T_QTY"]
            else:
                value = row["T_QTY"] * row["T_BID_PRICE"]
                reserve = value + config.charge_cents + (value * config.commission_basis_points + 5000) // 10000
                limit = 0 if account["is_cash"] else account["margin_limit_cents"]
                _require(balances[account_id] - reserved_cash[account_id] - reserve >= -limit, "Open buy orders reserve unavailable funds.")
                reserved_cash[account_id] += reserve
                reservations[identifier] = reserve
        if status == "CMPT":
            price, charge, commission, tax = values
            expected_commission = (row["T_QTY"] * price * config.commission_basis_points + 5000) // 10000
            _require(price > 0 and charge == config.charge_cents and commission == expected_commission and tax == 0,
                     "Stage 1 execution fees or tax differ from the configured rule.")
            low, high = row.get("_market_low"), row.get("_market_high")
            _require(low is not None and low <= price <= high, "Execution price lies outside the daily low and high.")
            if row["T_TT_ID"] == "TLB":
                _require(price <= row["T_BID_PRICE"], "Limit buy exceeds its bid.")
            elif row["T_TT_ID"] == "TLS":
                _require(price >= row["T_BID_PRICE"], "Limit sell is below its bid.")
            holding = holdings.get(identifier)
            _require(holding is not None, "Completed trade has no holding change.")
            origin, before = positions.get(position_key, (identifier, 0))
            after = before + (-1 if sell else 1) * row["T_QTY"]
            _require(after >= reserved_shares[position_key], "Completed sell exceeds unreserved holdings.")
            _require((holding["HH_H_T_ID"], holding["HH_BEFORE_QTY"], holding["HH_AFTER_QTY"]) == (origin, before, after),
                     "Holding origin or before/after quantity differs from position replay.")
            _require(holding["_day"] == "initial" if timestamp[:10] <= str(config.cutover) else holding["_day"] == timestamp[:10],
                     "Holding change lies in the wrong delivery.")
            if after:
                positions[position_key] = origin, after
            else:
                positions.pop(position_key, None)
            settlement = settlements.get(identifier)
            expected = (1 if sell else -1) * row["T_QTY"] * price - charge - commission - tax
            _require(settlement is not None and (settlement["CT_CA_ID"], settlement["CT_DTS"], settlement["CT_AMT"]) ==
                     (account_id, timestamp, expected), "Settlement does not match trade account, timestamp, value, or fees.")
            balances[account_id] += expected
            limit = 0 if account["is_cash"] else account["margin_limit_cents"]
            _require(balances[account_id] - reserved_cash[account_id] >= -limit,
                     "Settlement spends reserved funds or exceeds the cash or margin limit.")
            completed.add(identifier)
        trades[identifier] = row
    _require(set(holdings) == completed and set(settlements) == completed, "Holding changes or settlements include a non-completed trade.")
    return len(accounts), len(trades), len(completed)


def validate(root: Path, config: Config, inventory=None):
    root = Path(root)
    sources = load_sources()
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
    partitions = max(1, math.ceil(total_bytes / _VALIDATION_PARTITION_BYTES))
    sessions = _sessions(config)
    industries = {row["IN_ID"] for row in _rows(root / "offline" / "initial" / "Industry.txt", sources["Industry"], 1)}
    tax_rates = {row["TX_ID"] for row in _rows(root / "offline" / "initial" / "TaxRate.txt", sources["TaxRate"], 1)}
    securities, finwire_counts = _finwire([root / name for name in expected if Path(name).name.startswith("FINWIRE")], config, industries)
    brokers = set()
    counts = {}
    cash_sequence = 0
    previous_cash = ("", 0)
    previous_market = None
    previous_initial_trade = -1
    previous_history = None
    with tempfile.TemporaryDirectory(prefix="validation-", dir=root) as temporary:
        stage = _Stage(Path(temporary), partitions)
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
                    row["_partition"] = str(row["account_id"] % partitions)
                    stage.add("Hidden", row, hidden_schema)
        action_counts = _xml(stage, root / "offline" / "initial" / "CustomerMgmt.xml", sources, config)
        for relative in sorted(expected, key=lambda name: (0 if name.startswith("offline/initial/") else 1 if name.startswith("offline/") else 2, name)):
            path = root / relative
            if path.name.startswith("FINWIRE"):
                count = sum(1 for _ in _lines(path))
            elif path.suffix == ".xml":
                for _ in _lines(path):
                    pass
                count = sum(action_counts.values())
            else:
                initial = relative.startswith("offline/initial/")
                day = "initial" if initial else (path.stem if relative.startswith("stream/") else path.parent.name)
                if day != "initial":
                    day = datetime.strptime(day, "%Y%m%d").date().isoformat()
                name = "CashTransaction" if relative.startswith("stream/") else path.stem.split("_")[0]
                source = sources[name]
                rows = _stream(path, source) if relative.startswith("stream/") else _rows(path, source, 1 if initial else 2)
                count = 0
                seen = set() if name in {"HR", "Date", "Time", "StatusType", "TradeType", "Industry", "TaxRate"} else None
                for number, row in enumerate(rows, 1):
                    count = number
                    row.setdefault("_order", number)
                    if seen is not None:
                        identity = row[source.columns[0]]
                        _require(identity not in seen, f"Duplicate {name} identity.")
                        seen.add(identity)
                    if name == "HR":
                        if row["EmployeeJobCode"] == "314":
                            brokers.add(int(row["EmployeeID"]))
                    elif name == "Time":
                        _require(int(row["SK_TimeID"]) == number - 1, "Time reference must cover ordered seconds 0..86399.")
                    elif name == "Prospect":
                        identity = row["AgencyID"]
                        bucket = int.from_bytes(hashlib.blake2b(identity.encode("ascii"), digest_size=8).digest(), "big") % partitions
                        schema = {"identity": pl.String, "date": pl.String, "_partition": pl.String}
                        stage.add("ProspectIdentity", {"identity": identity, "date": day, "_partition": str(bucket)}, schema)
                    elif name in {"StatusType", "TradeType", "Industry", "TaxRate", "Date"}:
                        pass
                    else:
                        timestamp_field = {"Trade": "T_DTS", "TradeHistory": "TH_DTS", "CashTransaction": "CT_DTS", "WatchHistory": "W_DTS", "DailyMarket": "DM_DATE"}.get(name)
                        if timestamp_field:
                            date_value = row[timestamp_field][:10]
                            lower = _prior_year(config.start) if name == "DailyMarket" else config.start
                            _require(str(lower) <= date_value <= str(config.cutover) if initial else date_value == day,
                                     f"{name} timestamp lies outside its delivery.")
                        if name == "Trade":
                            if initial:
                                identifier = int(row["T_ID"])
                                _require(identifier > previous_initial_trade, "Initial trades must have unique increasing IDs.")
                                previous_initial_trade = identifier
                            key = int(row["T_CA_ID"])
                        elif name == "TradeHistory":
                            key = int(row["TH_T_ID"]) // 1000000
                            order = int(row["TH_T_ID"]), row["TH_DTS"], row["_order"]
                            _require(previous_history is None or order > previous_history, "Initial trade history keys must be ordered.")
                            previous_history = order
                        elif name == "HoldingHistory":
                            key = int(row["HH_T_ID"]) // 1000000
                        elif name == "CashTransaction":
                            key = int(row["CT_CA_ID"])
                            cash_sequence += 1
                            _require(int(row["CT_ID"]) == cash_sequence, "CT_ID must be one global sequence 1..N across cutover.")
                            order = row["CT_DTS"], int(row["CT_ID"])
                            _require(order > previous_cash, "Cash transactions must be in timestamp and ID order.")
                            previous_cash = order
                            _require(re.fullmatch(r"Deposit|Withdrawal|Trade settlement [1-9][0-9]*|Dividend [A-Za-z0-9._-]+", row["CT_NAME"]), "Invalid cash transaction name.")
                        elif name == "Customer":
                            key = int(row["C_ID"])
                        elif name == "Account":
                            key = int(row["CA_ID"])
                        elif name == "WatchHistory":
                            key = int(row["W_C_ID"])
                        elif name == "DailyMarket":
                            key = row["DM_DATE"]
                            order = row["DM_DATE"], row["DM_S_SYMB"]
                            _require(previous_market is None or order > previous_market, "Market keys must be unique and ordered.")
                            previous_market = order
                            _require(key in sessions and 0 < _cents(row["DM_LOW"]) <= _cents(row["DM_CLOSE"]) <= _cents(row["DM_HIGH"]), "Invalid market date or price bounds.")
                            _require(int(row["DM_VOL"]) >= 0, "Market volume cannot be negative.")
                            _security(securities, row["DM_S_SYMB"], key + " 23:59:59")
                        _stage_source(stage, source, row, day, key)
                if name == "Time":
                    _require(count == 86400, "Time reference must contain 86400 seconds.")
            counts[relative] = count
            if saved is not None:
                evidence = saved[relative]
                with path.open("rb") as stream:
                    digest = hashlib.file_digest(stream, "sha256").hexdigest()
                _require((count, path.stat().st_size, digest) == (evidence["rows"], evidence["bytes"], evidence["sha256"]),
                         f"File rows, bytes, or SHA-256 differ from saved evidence: {relative}")
        stage.finish()
        for market_date in sessions:
            if not str(_prior_year(config.start)) <= market_date <= str(config.end):
                continue
            required_symbols = {symbol for symbol, versions in securities.items()
                                if any(version[0][:10] <= market_date for version in versions) and
                                _security(securities, symbol, market_date + " 23:59:59")[2] == "ACTV"}
            observed = stage.frame("DailyMarket", market_date)
            observed_symbols = set(observed["DM_S_SYMB"].to_list()) if not observed.is_empty() else set()
            _require(observed_symbols == required_symbols, f"Market coverage is incomplete for {market_date}.")
        customers = sum(_customer_replay(stage, partition, config, securities, tax_rates) for partition in range(partitions))
        totals = [0, 0, 0]
        for partition in range(partitions):
            prospects = stage.frame("ProspectIdentity", partition)
            _require(prospects.is_empty() or not prospects.select("identity", "date").is_duplicated().any(),
                     "Duplicate Prospect identity in one snapshot.")
            for index, value in enumerate(_account_replay(stage, partition, config, securities, sessions, brokers)):
                totals[index] += value
        peak_rows = stage.peak_rows
    return {"status": "passed", "files": len(expected), "rows": sum(counts.values()), "bytes": total_bytes,
            "customers": customers, "accounts": totals[0], "trades": totals[1], "completed_trades": totals[2],
            "cash_transactions": cash_sequence, "customer_actions": action_counts, "finwire": finwire_counts,
            "partitions": partitions, "maximum_parse_buffer_rows": peak_rows,
            "checks": ["independent file cadence", "formats and LF endings", "saved hashes and counts" if saved is not None else "parsed counts",
                       "customer and account versions", "trade lifecycles and cutover snapshots", "cash and margin replay",
                       "source-visible order reservations", "settlements", "position holding history", "NYSE session and price bounds",
                       "start-of-day dividend amounts", "watch lifecycles and inactivation"]}
