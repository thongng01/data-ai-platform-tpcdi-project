"""Shared, read-only market history and released FINWIRE records."""

import calendar
from datetime import date, timedelta
import hashlib
from importlib.metadata import version
import json
from pathlib import Path

import numpy as np
from numpy.lib.format import open_memmap

from .config import Config, calendar_for, read_yaml
from .random_values import values


def prior_year(day: date) -> date:
    return day.replace(year=day.year - 1, day=min(day.day, calendar.monthrange(day.year - 1, day.month)[1]))


def quarter_start(day: date) -> date:
    return date(day.year, (day.month - 1) // 3 * 3 + 1, 1)


def shift_quarter(day: date, count: int) -> date:
    ordinal = day.year * 4 + (day.month - 1) // 3 + count
    year, quarter = divmod(ordinal, 4)
    return date(year, quarter * 3 + 1, 1)


def report_release(quarter: date) -> date:
    following = shift_quarter(quarter, 1)
    return following.replace(day=calendar.monthrange(following.year, following.month)[1])


def finwire_postings(config: Config):
    """Yield posting dates and report periods, with None for initial identities."""
    quarter = shift_quarter(quarter_start(config.start), -5)
    yield quarter, None
    while report_release(quarter) <= config.end:
        yield report_release(quarter), quarter
        quarter = shift_quarter(quarter, 1)


def money(cents: int) -> str:
    cents = int(cents)
    return f"{'-' if cents < 0 else ''}{abs(cents) // 100}.{abs(cents) % 100:02d}"


def _uniform(ids, config, purpose):
    return (values(ids, config.seed, purpose) >> np.uint64(11)).astype(np.float64) / 2**53


def _fixed(layout, fields):
    result = []
    for name, (width, alignment) in layout.items():
        value = str(fields.get(name, ""))
        if len(value) > width or not value.isascii() or any(character in value for character in "\r\n\t"):
            raise ValueError(f"Invalid FINWIRE field {name}: {value}")
        result.append(value.zfill(width) if alignment == "zero" else value.rjust(width) if alignment == "right" else value.ljust(width))
    return "".join(result)


class Market:
    def __init__(self, root: Path, config: Config):
        self.config = config
        self.directory = Path(root) / "_work" / "market"
        self.calendar_metadata = json.loads((self.directory / "calendar.json").read_text(encoding="utf-8"))
        self.fingerprint = self.calendar_metadata["fingerprint"]
        self.sessions_per_year = {int(year): count for year, count in self.calendar_metadata["sessions_per_year"].items()}
        self.first = date.fromisoformat(self.calendar_metadata["first_price_date"])
        self._arrays = {name: np.load(self.directory / f"{name}.npy", mmap_mode="r", allow_pickle=False)
                        for name in ("close", "high", "low", "dividends", "annual_dividends", "trading", "close_second", "shock")}
        self._release_dates = np.load(self.directory / "release_dates.npy", mmap_mode="r", allow_pickle=False)

    def day(self, day: date) -> dict:
        index = (day - self.first).days
        if index < 0 or index >= len(self._arrays["trading"]):
            raise ValueError(f"Market date is outside this run: {day}")
        return {name: (bool(array[index]) if name in ("trading", "shock") else
                       int(array[index]) if name == "close_second" else array[index])
                for name, array in self._arrays.items() if name != "annual_dividends"}

    def annual_dividends(self, day: date):
        index = int(np.searchsorted(self._release_dates, day.toordinal(), side="right")) - 1
        if index < 0:
            raise ValueError("No security posting exists at this date.")
        return self._arrays["annual_dividends"][index]

    @classmethod
    def build(cls, config: Config, root: Path, generation):
        directory = Path(root) / "_work" / "market"
        directory.mkdir(parents=True, exist_ok=True)
        first = prior_year(config.start)
        days = (config.end - first).days + 1
        calendar_start = date(first.year, 1, 1)
        calendar_end = date(config.end.year, 12, 31)
        exchange = calendar_for(calendar_start, calendar_end)
        sessions = {stamp.date() for stamp in exchange.sessions}
        early = {stamp.date() for stamp in exchange.early_closes}
        fingerprint_rows = [f"{day.isoformat()}:{13 if day in early else 16}" for day in sorted(sessions)]
        fingerprint = hashlib.sha256((version("exchange-calendars") + "\n" + "\n".join(fingerprint_rows)).encode("ascii")).hexdigest()
        counts = {year: sum(day.year == year for day in sessions) for year in range(first.year, config.end.year + 1)}
        metadata = {"calendar": "XNYS", "version": version("exchange-calendars"), "fingerprint": fingerprint,
                    "first_price_date": first.isoformat(), "last_price_date": config.daily_end,
                    "sessions_per_year": counts, "initial_sessions": sum(config.start <= day <= config.cutover for day in sessions),
                    "daily_sessions": sum(config.daily_start_date <= day <= config.end for day in sessions),
                    "clock": "UTC fixed NYSE wall-clock hours"}
        arrays = {name: open_memmap(directory / f"{name}.npy", mode="w+", dtype=np.int64, shape=(days, config.securities))
                  for name in ("close", "high", "low", "dividends")}
        arrays.update({name: open_memmap(directory / f"{name}.npy", mode="w+", dtype=dtype, shape=(days,))
                       for name, dtype in (("trading", np.bool_), ("close_second", np.int32), ("shock", np.bool_))})
        ids = np.arange(1, config.securities + 1, dtype=np.uint64)
        base = np.floor(config.price_min_cents + _uniform(ids, config, "market/base") * (config.price_max_cents - config.price_min_cents + 1)).astype(np.int64)
        close = base.copy()
        postings = list(finwire_postings(config))
        release_dates = open_memmap(directory / "release_dates.npy", mode="w+", dtype=np.int32, shape=(len(postings),))
        annual = open_memmap(directory / "annual_dividends.npy", mode="w+", dtype=np.int64, shape=(len(postings), config.securities))
        paying = _uniform(ids, config, "market/dividend-paying") < config.dividend_share
        yields = config.dividend_yield_min + _uniform(ids, config, "market/dividend-yield") * (config.dividend_yield_max - config.dividend_yield_min)
        for index, (posting, _) in enumerate(postings):
            release_dates[index] = posting.toordinal()
            annual[index] = np.where(paying, np.maximum(4, np.floor(base * yields / 4 + .5).astype(np.int64) * 4), 0)
        shocks = set()
        for year in counts:
            candidates = sorted(day for day in sessions if day.year == year)
            ranked = sorted(candidates, key=lambda day: int(values([day.toordinal()], config.seed, "market/shock-day")[0]))
            shocks.update(ranked[:config.shock_days_per_year])
        metadata["shock_dates"] = [day.isoformat() for day in sorted(shocks) if first <= day <= config.end]
        (directory / "calendar.json").write_text(json.dumps(metadata, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        payment_dates = {}
        for year in counts:
            for month in (1, 4, 7, 10):
                for fixed_day in range(1, 26):
                    payment = date(year, month, fixed_day)
                    while payment not in sessions:
                        payment += timedelta(days=1)
                    payment_dates.setdefault(payment, []).append(fixed_day)
        fixed_days = ((ids - 1) % 25 + 1).astype(np.int64)
        release_index = 0
        for offset in range(days):
            day = first + timedelta(days=offset)
            trading = day in sessions
            shock = day in shocks
            while release_index + 1 < len(postings) and postings[release_index + 1][0] <= day:
                release_index += 1
                if postings[release_index][0] >= first:
                    annual[release_index] = np.where(paying, np.maximum(4, np.floor(close * yields / 4 + .5).astype(np.int64) * 4), 0)
            if trading:
                first_draw = np.maximum(_uniform(ids, config, f"market/return-a/{day}"), 1e-12)
                second_draw = _uniform(ids, config, f"market/return-b/{day}")
                noise = np.sqrt(-2 * np.log(first_draw)) * np.cos(2 * np.pi * second_draw)
                common = (float(_uniform([day.toordinal()], config, "market/common")[0]) - .5) * .015
                if shock:
                    common += -.06 if int(values([day.toordinal()], config.seed, "market/shock-sign")[0]) % 2 else .06
                previous = close
                close = np.clip(np.floor(previous * np.exp(noise * config.market_volatility + common + .00015) + .5), 1, 90000000).astype(np.int64)
                spread = np.maximum(1, np.floor(close * (.002 + _uniform(ids, config, f"market/spread/{day}") * .015)).astype(np.int64))
                high = np.minimum(99999999, np.maximum(previous, close) + spread)
                low = np.maximum(1, np.minimum(previous, close) - spread)
            else:
                high, low = close, close
            arrays["close"][offset], arrays["high"][offset], arrays["low"][offset] = close, high, low
            arrays["trading"][offset] = trading
            arrays["close_second"][offset] = (13 if day in early else 16) * 3600
            arrays["shock"][offset] = shock
            arrays["dividends"][offset] = np.where(np.isin(fixed_days, payment_dates.get(day, [])), annual[release_index] // 4, 0)
            if trading:
                volumes = 100000 + values(ids, config.seed, f"market/volume/{day}") % 10000000
                for first_security in range(0, config.securities, config.export_rows):
                    rows = [{"DM_DATE": str(day), "DM_S_SYMB": f"S{security + 1:010d}",
                             "DM_CLOSE": money(close[security]), "DM_HIGH": money(high[security]), "DM_LOW": money(low[security]),
                             "DM_VOL": int(volumes[security]),
                             "_key": f"{day}/{security + 1:015d}"}
                            for security in range(first_security, min(config.securities, first_security + config.export_rows))]
                    generation.emit("DailyMarket", day, rows)
        for array in [*arrays.values(), annual, release_dates]:
            array.flush()
        market = cls(root, config)
        market._emit_finwire(generation, postings, base)
        return market

    def _emit_finwire(self, generation, postings, base):
        config = self.config
        layouts = read_yaml(Path(__file__).parent / "definitions" / "finwire.yaml")
        company_ids = np.arange(1, config.companies + 1, dtype=np.uint64)
        pe = 5 + _uniform(company_ids, config, "market/pe") * 35
        losses = _uniform(company_ids, config, "market/loss") < .03
        initial_posting = postings[0][0]
        for index, (posting, quarter) in enumerate(postings):
            stamp = posting.strftime("%Y%m%d-000000")
            filename = f"FINWIRE{posting.year}Q{(posting.month - 1) // 3 + 1}"
            relative = f"offline/initial/{filename}" if posting <= config.cutover else f"offline/daily/{posting:%Y%m%d}/{filename}"
            rows = []

            def emit(kind, identifier, fields):
                rows.append((f"{stamp}/{('CMP', 'SEC', 'FIN').index(kind)}/{identifier:015d}",
                             _fixed(layouts[kind], {**fields, "PTS": stamp, "RecType": kind})))
                if len(rows) >= config.export_rows:
                    generation.text(relative, rows.copy())
                    rows.clear()

            for identifier in range(1, config.companies + 1):
                emit("CMP", identifier, {"CompanyName": f"Company {identifier}", "CIK": identifier,
                    "Status": "ACTV", "IndustryID": "AA", "SPrating": "AAA", "FoundingDate": f"{min(1950, initial_posting.year):04d}0101",
                    "AddrLine1": "1 Main Street", "PostalCode": "10001", "City": "New York", "StateProvince": "NY",
                    "Country": "USA", "CEOname": f"Executive {identifier}", "Description": "Publicly traded company."})
            for identifier in range(1, config.securities + 1):
                emit("SEC", identifier, {"Symbol": f"S{identifier:010d}", "IssueType": "COMMON", "Status": "ACTV",
                    "Name": f"Security {identifier}", "ExID": "NYSE", "ShOut": "1000000",
                    "FirstTradeDate": initial_posting.strftime("%Y%m%d"), "FirstTradeExchg": initial_posting.strftime("%Y%m%d"),
                    "Dividend": money(self._arrays["annual_dividends"][index, identifier - 1]),
                    "CoNameOrCIK": (identifier - 1) % config.companies + 1})
            if quarter is not None:
                for identifier in range(1, config.companies + 1):
                    representative = identifier - 1
                    price = int(base[representative]) if posting < self.first else int(self.day(posting)["close"][representative])
                    quarter_eps = max(1, round(price / pe[representative] / 4))
                    if losses[representative]:
                        quarter_eps = -quarter_eps
                    earnings = quarter_eps * 1000000
                    revenue = max(abs(earnings) * 8, 100000000)
                    emit("FIN", identifier, {"Year": quarter.year, "Quarter": (quarter.month - 1) // 3 + 1,
                        "QtrStartDate": quarter.strftime("%Y%m%d"), "PostingDate": posting.strftime("%Y%m%d"),
                        "Revenue": money(revenue), "Earnings": money(earnings), "EPS": money(quarter_eps),
                        "DilutedEPS": money(quarter_eps), "Margin": f"{earnings / revenue:.2f}",
                        "Inventory": money(revenue // 10), "Assets": money(revenue * 4), "Liabilities": money(revenue * 2),
                        "ShOut": "1000000", "DilutedShOut": "1000000", "CoNameOrCIK": identifier})
            if rows:
                generation.text(relative, rows)
