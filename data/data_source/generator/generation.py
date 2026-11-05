"""Source routing and bounded recording for generation stage 1."""

import calendar
import json
import re
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from .config import Config
from .formats import load_sources, record_fields
from .sorting import SortedOutput


INITIAL_SOURCES = ("Trade", "TradeHistory", "CashTransaction", "HoldingHistory", "WatchHistory",
                   "DailyMarket", "HR", "Prospect", "StatusType", "TradeType", "Industry", "TaxRate", "Date", "Time")
DAILY_SOURCES = ("Customer", "Account", "Trade", "HoldingHistory", "WatchHistory", "DailyMarket")
CASH_KIND_RANK = {"Deposit": 0, "Trade settlement": 1, "Dividend": 2, "Withdrawal": 3}
FORMAT_VERSION = "brokerage-m1-1"


def padded(expression):
    return expression.cast(pl.String).str.pad_start(15, "0")


def order_key(*expressions):
    return pl.concat_str(expressions, separator="/").alias("_key")


def business_date(config, day):
    if type(day) is int:
        return config.start + timedelta(days=day)
    if isinstance(day, str):
        return date.fromisoformat(day)
    if isinstance(day, date):
        return day
    raise ValueError("A source date must be a date or an integer day offset.")


def source_path(config, name, day):
    day = business_date(config, day)
    if name == "CashTransaction" and day > config.cutover:
        return f"stream/cash_transactions/{day:%Y%m%d}.jsonl"
    if name == "CustomerMgmt":
        return "offline/initial/CustomerMgmt.xml"
    if name.startswith("FINWIRE"):
        filename = name
    else:
        filename = load_sources()[name].filename
    if day <= config.cutover:
        return f"offline/initial/{filename}"
    if name.startswith("FINWIRE"):
        return f"offline/daily/{day:%Y%m%d}/{filename}"
    file = Path(filename)
    return f"offline/daily/{day:%Y%m%d}/{file.stem}_{day:%Y%m%d}{file.suffix}"


def finwire_release_dates(config):
    first_quarter = config.start.year * 4 + (config.start.month - 1) // 3 - 5
    quarter = first_quarter
    while True:
        year, zero_quarter = divmod(quarter, 4)
        release_month = zero_quarter * 3 + 4
        if release_month == 13:
            year += 1
            release_month = 1
        release = date(year, release_month, calendar.monthrange(year, release_month)[1])
        if release > config.end:
            break
        yield release
        quarter += 1


def expected_file_list(config):
    """Derive delivery coverage without inspecting generated records or runs."""
    paths = {source_path(config, name, config.cutover) for name in INITIAL_SOURCES}
    paths.add("offline/initial/CustomerMgmt.xml")
    day = config.daily_start_date
    while day <= config.end:
        paths.update(source_path(config, name, day) for name in DAILY_SOURCES)
        paths.add(source_path(config, "CashTransaction", day))
        if day.day == 1:
            paths.add(source_path(config, "Prospect", day))
        day += timedelta(days=1)
    support_quarter = config.start.year * 4 + (config.start.month - 1) // 3 - 5
    year, quarter = divmod(support_quarter, 4)
    paths.add(f"offline/initial/FINWIRE{year}Q{quarter + 1}")
    for release in finwire_release_dates(config):
        name = f"FINWIRE{release.year}Q{(release.month - 1) // 3 + 1}"
        paths.add(source_path(config, name, release))
    return sorted(paths)


def cash_kind(name):
    if name in ("Deposit", "Withdrawal"):
        return name
    if re.fullmatch(r"Trade settlement [1-9][0-9]{0,14}", name):
        return "Trade settlement"
    if re.fullmatch(r"Dividend [A-Za-z0-9._-]{1,15}", name):
        return "Dividend"
    raise ValueError(f"Invalid CT_NAME: {name}")


def cents_text(cents):
    if type(cents) is not int:
        raise ValueError("Cash amounts must use integer cents.")
    sign = "-" if cents < 0 else ""
    return f"{sign}{abs(cents) // 100}.{abs(cents) % 100:02d}"


class Generation:
    def __init__(self, config: Config, root: Path):
        self.config = config
        self.root = Path(root)
        self.sources = load_sources()
        self.output = SortedOutput(self.root, config.merge_fan_in, config.export_rows)
        self._cash_sequence = 0

    def path(self, source, day):
        return source_path(self.config, source, day)

    def emit(self, name: str, day, rows):
        if name == "CashTransaction":
            raise ValueError("Use Generation.cash so CT_ID is assigned during the global merge.")
        day = business_date(self.config, day)
        source = self.sources[name]
        if isinstance(rows, pl.DataFrame):
            rows = rows.iter_rows(named=True)
        recorded = (record_fields(source, day, row) for row in rows)
        self.output.add(self.path(name, day), recorded, source, source.cdc and day > self.config.cutover)

    def text(self, relative: str, rows):
        relative = str(relative)
        if not relative.startswith(("offline/", "stream/")) or ".." in Path(relative).parts:
            raise ValueError("Text output paths must stay inside offline or stream.")
        self.output.add(relative, ({"_key": key, "_payload": payload} for key, payload in rows))

    def cash(self, day, timestamp_str, account, ordinal, amount_cents, name):
        day = business_date(self.config, day)
        if not self.config.start <= day <= self.config.end or timestamp_str[:10] != day.isoformat():
            raise ValueError("A cash timestamp must match a date within the run.")
        account, ordinal = int(account), int(ordinal)
        if not 0 < account < 10**11 or not 0 <= ordinal < 10**15:
            raise ValueError("Cash identity exceeds its sort-key bounds.")
        kind = cash_kind(name)
        amount = cents_text(amount_cents)
        values = ["1", str(account), timestamp_str, amount, name]
        self.sources["CashTransaction"].check(values)
        key = f"{timestamp_str}/{account:011d}/{ordinal:015d}/{CASH_KIND_RANK[kind]}"
        self.output.add(self.path("CashTransaction", day), [{"_key": key, "_payload": "|".join(values[1:])}])

    def flush(self):
        self.output.flush()

    def close(self):
        self.output.close()

    def adopt(self, group_root):
        self.output.adopt(group_root)

    def _cash_record(self, stream):
        def transform(key, payload):
            self._cash_sequence += 1
            account, timestamp, amount, name = payload.split("|", 3)
            self.sources["CashTransaction"].check([str(self._cash_sequence), account, timestamp, amount, name])
            cash_kind(name)
            if stream:
                return json.dumps({"ct_id": self._cash_sequence, "ct_ca_id": int(account),
                                   "ct_dts": timestamp.replace(" ", "T") + "Z", "ct_amt": amount,
                                   "ct_name": name, "schema_version": 1}, separators=(",", ":"), ensure_ascii=True)
            return f"{self._cash_sequence}|{payload}"
        return transform

    @staticmethod
    def _check_finwire(payload):
        lengths = {"CMP": (543,), "SEC": (170, 220), "FIN": (196, 246)}
        if payload[15:18] not in lengths or len(payload) not in lengths[payload[15:18]]:
            raise ValueError("Invalid FINWIRE record type or width.")
        if not re.fullmatch(r"[0-9]{8}-[0-9]{6}", payload[:15]):
            raise ValueError("Invalid FINWIRE timestamp.")

    def finish(self):
        self.flush()
        expected = expected_file_list(self.config)
        expected_set = set(expected)
        for (relative,) in self.output.catalog.execute("SELECT DISTINCT relative FROM runs"):
            if relative not in expected_set:
                raise ValueError(f"Unexpected source output: {relative}")
        cash_initial = "offline/initial/CashTransaction.txt"
        cash_paths = [cash_initial] + [p for p in expected if p.startswith("stream/")]
        inventory = []
        for relative in cash_paths:
            inventory.append(self.output.write(relative, transform=self._cash_record(relative.startswith("stream/"))))
        cash_path_set = set(cash_paths)
        for relative in expected:
            if relative in cash_path_set:
                continue
            filename = Path(relative).name
            xml = filename == "CustomerMgmt.xml"
            name = Path(filename).stem.split("_", 1)[0]
            source = self.sources.get(name)
            check = self._check_finwire if filename.startswith("FINWIRE") else source.check_payload if source else None
            cdc = bool(source and source.cdc and relative.startswith("offline/daily/"))
            inventory.append(self.output.write(relative, cdc=cdc, xml=xml, check=check))
        self.output.close()
        return sorted(inventory, key=lambda item: item["path"])
