"""Initial-only source references and the firm's fixed HR roster."""

from datetime import timedelta

import numpy as np

from .config import calendar_for
from .market import finwire_postings, prior_year
from .personal_data import personal_columns


def employee_columns(config) -> dict[str, np.ndarray]:
    """Build the fixed HR roster with independent employee names and branch contacts."""
    identifiers = np.arange(config.brokers * 4, dtype=np.int64)
    people = personal_columns(identifiers, config.seed, "employee")
    return {
        "EmployeeID": identifiers, "ManagerID": np.zeros(len(identifiers), dtype=np.int64),
        "EmployeeFirstName": people["first_name"], "EmployeeLastName": people["last_name"],
        "EmployeeMI": people["middle_initial"],
        "EmployeeJobCode": np.where((identifiers >= 1) & (identifiers <= config.brokers), 314, 100),
        "EmployeeBranch": people["city"],
        "EmployeeOffice": np.strings.add("Office", (identifiers % 8 + 1).astype(str)),
        "EmployeePhone": people["phone"],
    }


def _time_columns(seconds: np.ndarray) -> dict[str, np.ndarray]:
    """Build one bounded clock block with exact two-digit time labels."""
    hours, minutes = seconds // 3600, seconds // 60 % 60
    hour_text = np.strings.zfill(hours.astype(str), 2)
    minute_text = np.strings.zfill(minutes.astype(str), 2)
    second_text = np.strings.zfill((seconds % 60).astype(str), 2)
    hour_minute = np.strings.add(np.strings.add(hour_text, ":"), minute_text)
    full_time = np.strings.add(np.strings.add(hour_minute, ":"), second_text)
    return {
        "SK_TimeID": seconds, "TimeValue": full_time, "HourID": hours, "HourDesc": hour_text,
        "MinuteID": minutes, "MinuteDesc": hour_minute,
        "SecondID": seconds % 60, "SecondDesc": full_time,
        "MarketHoursFlag": ((seconds >= 34200) & (seconds < 57600)).astype(np.int64),
        "OfficeHoursFlag": ((seconds >= 28800) & (seconds < 61200)).astype(np.int64),
    }


def generate_reference(generation) -> None:
    """Emit initial-only lookup data, a fixed HR roster, and calendar dimensions."""
    config = generation.config

    def emit(source_name, records):
        """Collect small references and calendar labels into bounded typed columns."""
        names = generation.sources[source_name].columns
        columns = {name: [] for name in names}
        size = 0
        for values in records:
            for name, value in zip(names, values, strict=True):
                columns[name].append(value)
            size += 1
            if size == config.export_rows:
                generation.record_batch(source_name, 0, columns)
                columns = {name: [] for name in names}
                size = 0
        if size:
            generation.record_batch(source_name, 0, columns)

    emit("StatusType", [("ACTV", "Active"), ("INAC", "Inactive"), ("PNDG", "Pending"),
                        ("SBMT", "Submitted"), ("CMPT", "Completed"), ("CNCL", "Canceled")])
    emit("TradeType", [("TMB", "Market Buy", 0, 1), ("TMS", "Market Sell", 1, 1),
                       ("TLB", "Limit Buy", 0, 0), ("TLS", "Limit Sell", 1, 0), ("TSL", "Stop Loss", 1, 0)])
    emit("Industry", [("AA", "Misc. Capital Goods", "FN")])
    emit("TaxRate", [("US1", "Local income tax", "0.10000"), ("US2", "National income tax", "0.20000")])
    employees = employee_columns(config)
    for first in range(0, config.brokers * 4, config.export_rows):
        generation.record_batch("HR", 0, {name: column[first:first + config.export_rows]
                                         for name, column in employees.items()})
    # Clock labels need no Python work per second. Numeric columns stay numeric
    # until the shared writer formats them in source-field order.
    for first in range(0, 86400, config.export_rows):
        seconds = np.arange(first, min(first + config.export_rows, 86400), dtype=np.int64)
        generation.record_batch("Time", 0, _time_columns(seconds))
    first = min(prior_year(config.start), next(finwire_postings(config))[0])
    exchange = calendar_for(first, config.end)
    sessions = {stamp.date() for stamp in exchange.sessions}

    def dates():
        """Yield calendar attributes and the exchange holiday flag for each date."""
        current = first
        while current <= config.end:
            quarter = (current.month - 1) // 3 + 1
            iso = current.isocalendar()
            yield (int(current.strftime("%Y%m%d")), str(current), f"{current.strftime('%B')} {current.day}, {current.year}",
                   current.year, str(current.year), current.year * 10 + quarter, f"{current.year} Q{quarter}",
                   current.year * 100 + current.month, current.strftime("%Y %B"), iso.year * 100 + iso.week,
                   f"{iso.year}-W{iso.week:02d}", current.isoweekday(), current.strftime("%A"), current.year,
                   str(current.year), current.year * 10 + quarter, f"{current.year} Q{quarter}",
                   int(current.weekday() < 5 and current not in sessions))
            current += timedelta(days=1)

    emit("Date", dates())
