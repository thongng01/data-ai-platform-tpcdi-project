"""Route typed business values through source recording and bounded native export."""

import calendar
import json
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

from .config import Config
from .formats import load_sources
from .output import TypedOutput
from .recording import AUDIT_PATH, SourceRecorder


INITIAL_SOURCES = ("Trade", "TradeHistory", "CashTransaction", "HoldingHistory", "WatchHistory",
                   "DailyMarket", "HR", "Prospect", "StatusType", "TradeType", "Industry", "TaxRate", "Date", "Time")
DAILY_SOURCES = ("Customer", "Account", "Trade", "HoldingHistory", "WatchHistory", "DailyMarket")
FORMAT_VERSION = "brokerage-m3-1"


def business_date(config, day):
    """Normalize a source date without changing the configured UTC calendar."""
    if type(day) is int:
        return config.start + timedelta(days=day)
    if isinstance(day, str):
        return date.fromisoformat(day)
    if isinstance(day, date):
        return day
    raise ValueError("A source date must be a date or an integer day offset.")


def source_path(config, name, day):
    """Choose the initial, daily, or cash-stream path from the business date."""
    day = business_date(config, day)
    if name == "CashTransaction" and day > config.cutover:
        return f"stream/cash_transactions/{day:%Y%m%d}.jsonl"
    if name == "CustomerMgmt":
        return "offline/initial/CustomerMgmt.xml"
    filename = name if name.startswith("FINWIRE") else load_sources()[name].filename
    if day <= config.cutover:
        return f"offline/initial/{filename}"
    if name.startswith("FINWIRE"):
        return f"offline/daily/{day:%Y%m%d}/{filename}"
    file = Path(filename)
    return f"offline/daily/{day:%Y%m%d}/{file.stem}_{day:%Y%m%d}{file.suffix}"


def finwire_release_dates(config):
    """Yield quarterly release dates, including the required support history."""
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
    """Derive delivery coverage without inspecting generated records or source parts."""
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


class Generation:
    """Hide source recording, ordering, and exact writers behind typed handoffs."""

    def __init__(self, config: Config, root: Path, *, group_id: int | None = None):
        """Open one private part under the shared dataset root for this producer."""
        self.config, self.root = config, Path(root)
        self.sources = load_sources()
        # Cache only this live part's recent routes. A global method cache would
        # retain finished groups and their recording state throughout a large run.
        self._path = lru_cache(maxsize=2048)(self._source_path)
        self.recorder = SourceRecorder(self)
        self.output = TypedOutput(self, group_id)

    def _source_path(self, family, day):
        """Resolve raw family variants internally without exposing arbitrary paths."""
        if family == "__audit":
            return AUDIT_PATH
        if family == "FINWIRE":
            family = f"FINWIRE{day.year}Q{(day.month - 1) // 3 + 1}"
        return source_path(self.config, family, day)

    def record(self, family: str, day: int, values: tuple, *, order: tuple = (),
               flag: str = "I", profile: int | None = None) -> None:
        """Queue one correct typed tuple with only the extra facts ordering needs."""
        self.output.record(family, day, values, order=order, flag=flag, profile=profile)

    def record_batch(self, family: str, day: int, columns, *, order=None, flag="I") -> None:
        """Consume bounded typed source columns without producing row dictionaries."""
        self.output.record_batch(family, day, columns, order=order, flag=flag)

    def _record_fault(self, entry):
        """Spool sparse exact error evidence separately from all ingestion records."""
        payload = json.dumps(entry, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        key = json.dumps([entry["source"], entry["day"], entry["identity"], entry["field"]],
                         sort_keys=True, separators=(",", ":"))
        day = (date.fromisoformat(entry["day"]) - self.config.start).days
        self.record("__audit", day, (payload,), order=(key,))

    def finish(self):
        """Seal a group or publish all expected sources after every group completes."""
        receipt = self.output.seal()
        if self.output.group_id is not None:
            return receipt
        return self.output.publish(expected_file_list(self.config), AUDIT_PATH)
