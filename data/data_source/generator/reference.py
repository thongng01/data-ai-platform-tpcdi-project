from datetime import date, timedelta
from pathlib import Path

import polars as pl

from .config import read_yaml
from .generation import Generation, order_key, padded
from .random_values import money


def _small_table(generation, name, records):
    columns = generation.sources[name].columns
    frame = pl.DataFrame(records, schema=columns, orient="row").with_row_index("_index")
    generation.emit(name, 1, frame.with_columns(order_key(padded(pl.col("_index")))))


def generate_reference(generation: Generation):
    config = generation.config
    _small_table(generation, "StatusType", [("ACTV", "Active"), ("INAC", "Inactive"), ("PNDG", "Pending"), ("SBMT", "Submitted"), ("CMPT", "Completed"), ("CNCL", "Canceled")])
    _small_table(generation, "TradeType", [("TMB", "Market Buy", 0, 1), ("TMS", "Market Sell", 1, 1), ("TLB", "Limit Buy", 0, 0), ("TLS", "Limit Sell", 1, 0), ("TSL", "Stop Loss", 1, 1)])
    _small_table(generation, "Industry", [("AA", "Misc. Capital Goods", "FN")])
    _small_table(generation, "TaxRate", [("US1", "POC local tax", "0.10000"), ("US2", "POC national tax", "0.20000")])
    for first in range(0, config.brokers + 1, 4096):
        frame = pl.DataFrame({"EmployeeID": range(first, min(first + 4096, config.brokers + 1))}).with_columns(
            pl.lit(0).alias("ManagerID"), pl.lit("Alex").alias("EmployeeFirstName"),
            pl.concat_str(pl.lit("Broker"), pl.col("EmployeeID").cast(pl.String)).alias("EmployeeLastName"),
            pl.lit("").alias("EmployeeMI"), pl.when(pl.col("EmployeeID") == 0).then(100).otherwise(314).alias("EmployeeJobCode"),
            pl.lit("New York").alias("EmployeeBranch"), pl.lit("Office1").alias("EmployeeOffice"), pl.lit("2125550100").alias("EmployeePhone"),
            order_key(padded(pl.col("EmployeeID"))),
        )
        generation.emit("HR", 1, frame)
    for batch in range(1, config.incremental_days + 2):
        generation.text(f"Batch{batch}/BatchDate.txt", [("0", str(config.day(config.historical_days + batch - 1)))])
    for first in range(0, 86400, 4096):
        frame = pl.DataFrame({"SK_TimeID": range(first, min(first + 4096, 86400))}).with_columns(
            (pl.col("SK_TimeID") // 3600).alias("HourID"), (pl.col("SK_TimeID") // 60 % 60).alias("MinuteID"),
            (pl.col("SK_TimeID") % 60).alias("SecondID"),
        ).with_columns(
            pl.concat_str([pl.col(column).cast(pl.String).str.pad_start(2, "0") for column in ("HourID", "MinuteID", "SecondID")], separator=":").alias("TimeValue"),
            pl.col("HourID").cast(pl.String).str.pad_start(2, "0").alias("HourDesc"),
            ((pl.col("SK_TimeID") >= 34200) & (pl.col("SK_TimeID") < 57600)).cast(pl.Int8).alias("MarketHoursFlag"),
            ((pl.col("SK_TimeID") >= 28800) & (pl.col("SK_TimeID") < 61200)).cast(pl.Int8).alias("OfficeHoursFlag"),
            order_key(padded(pl.col("SK_TimeID"))),
        ).with_columns(pl.col("TimeValue").str.slice(0, 5).alias("MinuteDesc"), pl.col("TimeValue").alias("SecondDesc"))
        generation.emit("Time", 1, frame)
    current = _quarters(config.day(0))[0]
    rows = []
    while current <= config.day(config.total_days):
        quarter = (current.month - 1) // 3 + 1
        iso = current.isocalendar()
        rows.append([int(current.strftime("%Y%m%d")), str(current), f"{current.strftime('%B')} {current.day}, {current.year}",
                     current.year, str(current.year), current.year * 10 + quarter, f"{current.year} Q{quarter}",
                     current.year * 100 + current.month, current.strftime("%Y %B"), iso.year * 100 + iso.week,
                     f"{iso.year}-W{iso.week:02d}", current.isoweekday(), current.strftime("%A"), current.year,
                     str(current.year), current.year * 10 + quarter, f"{current.year} Q{quarter}", 0])
        current += timedelta(days=1)
        if len(rows) == 1024 or current > config.day(config.total_days):
            frame = pl.DataFrame(rows, schema=generation.sources["Date"].columns, orient="row")
            generation.emit("Date", 1, frame.with_columns(order_key(padded(pl.col("SK_DateID")))))
            rows = []


def _quarters(start):
    current = date(start.year, ((start.month - 1) // 3) * 3 + 1, 1)
    result = []
    for _ in range(4):
        current = date(current.year - 1, 10, 1) if current.month == 1 else date(current.year, current.month - 3, 1)
        result.append(current)
    return list(reversed(result))


def _fixed(layout, values):
    result = []
    for name, (width, alignment) in layout.items():
        value = str(values.get(name, ""))
        if len(value) > width or not value.isascii() or any(character in value for character in "\r\n\t"):
            raise ValueError(f"Invalid FINWIRE field {name}: {value}")
        result.append(value.zfill(width) if alignment == "zero" else value.rjust(width) if alignment == "right" else value.ljust(width))
    return "".join(result)


def generate_finwire(generation: Generation):
    config = generation.config
    layouts = read_yaml(Path(__file__).parent / "definitions" / "finwire.yaml")
    quarters = _quarters(config.day(0))

    def emit(records):
        grouped = {}
        for kind, identifier, timestamp, values in records:
            posting = date.fromisoformat(f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}")
            path = f"Batch1/FINWIRE{posting.year}Q{(posting.month - 1) // 3 + 1}"
            payload = _fixed(layouts[kind], {**values, "PTS": timestamp, "RecType": kind})
            grouped.setdefault(path, []).append((f"{timestamp}/{kind}/{identifier:015d}", payload))
        for path, rows in grouped.items():
            generation.text(path, rows)

    for first in range(1, config.companies + 1, 1024):
        records = []
        for identifier in range(first, min(first + 1024, config.companies + 1)):
            records.append(("CMP", identifier, quarters[0].strftime("%Y%m%d-000000"), {
                "CompanyName": f"Company {identifier}", "CIK": identifier, "Status": "ACTV", "IndustryID": "AA", "SPrating": "AAA",
                "FoundingDate": "19500101", "AddrLine1": "1 Main Street", "PostalCode": "10001", "City": "New York",
                "StateProvince": "NY", "Country": "USA", "CEOname": "Alex Executive", "Description": "Synthetic POC company.",
            }))
            for quarter in quarters:
                posting = date(quarter.year + 1, 1, 1) if quarter.month == 10 else date(quarter.year, quarter.month + 3, 1)
                records.append(("FIN", identifier, posting.strftime("%Y%m%d-000002"), {
                    "Year": quarter.year, "Quarter": (quarter.month - 1) // 3 + 1, "QtrStartDate": quarter.strftime("%Y%m%d"),
                    "PostingDate": posting.strftime("%Y%m%d"), "Revenue": "1000000.00", "Earnings": "100000.00",
                    "EPS": "1.00", "DilutedEPS": "1.00", "Margin": "0.10", "Inventory": "10000.00", "Assets": "5000000.00",
                    "Liabilities": "2000000.00", "ShOut": "100000", "DilutedShOut": "100000", "CoNameOrCIK": identifier,
                }))
        emit(records)
    for first in range(1, config.securities + 1, 1024):
        emit([("SEC", identifier, quarters[0].strftime("%Y%m%d-000001"), {
            "Symbol": f"S{identifier:010d}", "IssueType": "COMMON", "Status": "ACTV", "Name": f"Security {identifier}", "ExID": "NYSE",
            "ShOut": "100000", "FirstTradeDate": quarters[0].strftime("%Y%m%d"), "FirstTradeExchg": quarters[0].strftime("%Y%m%d"),
            "Dividend": "0.10", "CoNameOrCIK": (identifier - 1) % config.companies + 1,
        }) for identifier in range(first, min(first + 1024, config.securities + 1))])


def generate_market(generation: Generation):
    config = generation.config
    for day in range(config.total_days):
        for first in range(1, config.securities + 1, 4096):
            frame = pl.DataFrame({"identifier": range(first, min(first + 4096, config.securities + 1))}).with_columns(
                pl.lit(str(config.day(day))).alias("DM_DATE"),
                pl.concat_str(pl.lit("S"), pl.col("identifier").cast(pl.String).str.pad_start(10, "0")).alias("DM_S_SYMB"),
                (1000 + pl.col("identifier") % 90 * 100 + day % 21).alias("close"),
                (1200 + pl.col("identifier") % 90 * 100).alias("high"),
                (800 + pl.col("identifier") % 90 * 100).alias("low"),
                pl.lit(max(1000000, config.total_customers * 3 * config.cycles_per_account_day * 200)).alias("DM_VOL"),
            ).with_columns(money("close").alias("DM_CLOSE"), money("high").alias("DM_HIGH"), money("low").alias("DM_LOW"),
                           order_key(pl.col("DM_DATE"), padded(pl.col("identifier"))))
            generation.emit("DailyMarket", config.batch(day), frame)
