"""Initial-only source references and the firm's fixed HR roster."""

from datetime import timedelta

from .config import calendar_for
from .market import finwire_postings, prior_year


def generate_reference(generation):
    config = generation.config

    def emit(name, records):
        columns = generation.sources[name].columns
        rows = []
        for values in records:
            row = dict(zip(columns, values, strict=True))
            identifier = values[0]
            row["_key"] = f"{identifier:015d}" if isinstance(identifier, int) else str(identifier)
            rows.append(row)
            if len(rows) == config.export_rows:
                generation.emit(name, config.start, rows)
                rows = []
        if rows:
            generation.emit(name, config.start, rows)

    emit("StatusType", [("ACTV", "Active"), ("INAC", "Inactive"), ("PNDG", "Pending"),
                        ("SBMT", "Submitted"), ("CMPT", "Completed"), ("CNCL", "Canceled")])
    emit("TradeType", [("TMB", "Market Buy", 0, 1), ("TMS", "Market Sell", 1, 1),
                       ("TLB", "Limit Buy", 0, 0), ("TLS", "Limit Sell", 1, 0), ("TSL", "Stop Loss", 1, 0)])
    emit("Industry", [("AA", "Misc. Capital Goods", "FN")])
    emit("TaxRate", [("US1", "Local income tax", "0.10000"), ("US2", "National income tax", "0.20000")])
    emit("HR", ((identifier, 0, "Alex", f"Employee{identifier}", "", 314 if 1 <= identifier <= config.brokers else 100,
                 "New York", "Office1", "2125550100") for identifier in range(config.brokers * 4)))
    emit("Time", ((second, f"{second // 3600:02d}:{second // 60 % 60:02d}:{second % 60:02d}", second // 3600,
                   f"{second // 3600:02d}", second // 60 % 60, f"{second // 3600:02d}:{second // 60 % 60:02d}",
                   second % 60, f"{second // 3600:02d}:{second // 60 % 60:02d}:{second % 60:02d}",
                   int(34200 <= second < 57600), int(28800 <= second < 61200)) for second in range(86400)))
    first = min(prior_year(config.start), next(finwire_postings(config))[0])
    exchange = calendar_for(first, config.end)
    sessions = {stamp.date() for stamp in exchange.sessions}

    def dates():
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
