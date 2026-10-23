"""Reference tables (Batch1 only).

These are the small, fixed code lists: tax rates, industries, status types,
trade types, plus the calendar (``Date.txt``, ``Time.txt``) and the FINWIRE
reference records. They are small because they are small in TPC-DI too; the
point of generating them is that they are the join targets every other table
depends on, so their keys have to be stable and complete.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import date, datetime, timedelta

from .emit import DelimitedWriter, FixedWidthWriter, SingleColumnWriter
from .rng import bool_word, pick, zero_padded

# StatusType codes. Trade.T_ST_ID, TradeHistory.TH_ST_ID, WatchHistory.W_ACTION,
# and FINWIRE CMP.Status all live in this domain.
STATUS_TYPES = [
    ("ACTV", "Active"),
    ("CMPT", "Completed"),
    ("CNCL", "Canceled"),
    ("PNDG", "Pending"),
    ("SBMT", "Submitted"),
    ("INAC", "Inactive"),
]
STATUS_CODES = [code for code, _ in STATUS_TYPES]

# Trade status is a *subset* of the StatusType domain. A trade is never
# ACTV or INAC; those belong to WatchHistory.W_ACTION and Account.CA_ST_ID.
# Emitting them here would still satisfy the FK to StatusType but would be
# semantically wrong, and the whole point of a skewed status column is that the
# dominant value means something (CMPT = most trades have completed).
TRADE_STATUS_CODES = ["CMPT", "SBMT", "CNCL", "PNDG"]

TRADE_TYPES = [
    # TT_ID, TT_NAME, TT_IS_SELL, TT_IS_MRKT
    ("TMB", "Market Buy", 0, 1),
    ("TMS", "Market Sell", 1, 1),
    ("TSL", "Stop Loss", 1, 1),
    ("TLS", "Limit Sell", 1, 0),
    ("TLB", "Limit Buy", 0, 0),
]
TRADE_TYPE_CODES = [code for code, _, _, _ in TRADE_TYPES]

_INDUSTRIES = [
    ("AA", "Misc. Capital Goods", "FN"),
    ("AC", "Retail (Drugs)", "TC"),
    ("AT", "Retail (Apparel)", "CC"),
    ("AU", "Retail (General)", "CC"),
    ("BH", "Health Care", "HC"),
    ("CC", "Communications", "CC"),
    ("CH", "Banking", "FN"),
    ("CN", "Consumer Non-Durables", "CN"),
    ("DD", "Durable Consumer Goods", "CN"),
    ("DE", "Drugs", "HC"),
    ("EV", "Energy", "EN"),
    ("FS", "Finance", "FN"),
    ("GI", "Government", "GV"),
    ("IN", "Insurance", "FN"),
    ("IT", "Technology", "TC"),
    ("MA", "Manufacturing", "MA"),
    ("ME", "Metals", "MA"),
    ("MN", "Mining", "ME"),
    ("PA", "Paper/Alloys", "ME"),
    ("PB", "Petroleum/BioFuel", "EN"),
    ("RE", "Real Estate", "RE"),
    ("RT", "Retail", "CC"),
    ("SC", "Services", "SV"),
    ("SR", "Soap/Perfumery", "CH"),
    ("TR", "Transportation", "TR"),
    ("UT", "Utilities", "UT"),
    ("WM", "Waste Management", "WM"),
    ("WP", "Wholesale", "WH"),
]

_TAX_COUNTRIES = ("US", "CA", "GB", "DE", "FR", "AU", "JP", "MX", "BR", "IN")
_TAX_LABELS = (
    "Income Tax Bracket",
    "Capital Gains Tax",
    "Sales Tax",
    "Property Levy",
    "Withholding Tax",
    "Corporate Surtax",
    "Municipal Tax",
    "Transit Levy",
    "Environmental Levy",
    "Wealth Tax",
)


def _tax_rate_rows(count: int) -> Iterator[list[object]]:
    rng = random.Random(11)
    seen: set[str] = set()
    index = 0
    while len(seen) < count:
        country = _TAX_COUNTRIES[index % len(_TAX_COUNTRIES)]
        code = f"{country}{rng.randint(1, 9)}"
        index += 1
        if code in seen:
            continue
        seen.add(code)
        # Rate is always 5 dp. The name contains '&' but never a pipe, which is
        # what keeps the pipe delimiter safe for this file.
        yield [code, f"{country} {pick(rng, _TAX_LABELS)} & Duties", f"{rng.uniform(0.0, 0.5):.5f}"]


def write_reference_tables(root: str, counts: dict[str, int], seed: int) -> list[str]:
    """Emit every Batch1 reference file under ``root``. Returns the paths written."""
    written: list[str] = []
    import os

    def path_for(name: str) -> str:
        return os.path.join(root, name)

    # TaxRate: TX_ID | TX_NAME | TX_RATE. TX_NAME contains '&' but never '|'.
    with DelimitedWriter(path_for("TaxRate.txt"), "pipe") as w:
        for row in _tax_rate_rows(counts.get("tax_rate", 320)):
            w.write_row(row)
    written.append(path_for("TaxRate.txt"))

    # Industry: IN_ID | IN_NAME | IN_SC_ID. IN_NAME may repeat across IN_ID, so
    # it is not a key; IN_ID is.
    with DelimitedWriter(path_for("Industry.txt"), "pipe") as w:
        for industry in _INDUSTRIES:
            w.write_row(list(industry))
    written.append(path_for("Industry.txt"))

    # StatusType: ST_ID | ST_NAME.
    with DelimitedWriter(path_for("StatusType.txt"), "pipe") as w:
        for row in STATUS_TYPES:
            w.write_row(list(row))
    written.append(path_for("StatusType.txt"))

    # TradeType: TT_ID | TT_NAME | TT_IS_SELL | TT_IS_MRKT. The flags are 0/1
    # ints, not true/false.
    with DelimitedWriter(path_for("TradeType.txt"), "pipe") as w:
        for row in TRADE_TYPES:
            w.write_row(list(row))
    written.append(path_for("TradeType.txt"))

    _write_calendar(root, counts, written)
    _write_clock(root, written)
    return written


def _write_calendar(root: str, counts: dict[str, int], written: list[str]) -> None:
    """Date.txt: 18 columns, two date encodings coexisting.

    ``SK_DateID`` is ``YYYYMMDD`` as an int while ``DateValue`` is ISO. The
    fiscal year is offset from the calendar year. ``HolidayFlag`` is literal
    true/false here, unlike the 0/1 encoding used in trade files.
    """
    path = f"{root}/Date.txt"
    start = date(1950, 1, 1)
    days = counts.get("date", 25_933)
    with DelimitedWriter(path, "pipe") as w:
        current = start
        for _ in range(days):
            sk = int(current.strftime("%Y%m%d"))
            cal_q = (current.month - 1) // 3 + 1
            # Fiscal year starts in November, so it lags the calendar year.
            fiscal_year = current.year if current.month >= 11 else current.year - 1
            fiscal_month = (current.month - 10) % 12 + 1
            fiscal_q = (fiscal_month - 1) // 3 + 1
            week = current.isocalendar()
            w.write_row(
                [
                    sk,
                    current.isoformat(),
                    f"{current.strftime('%B')} {current.day}, {current.year}",
                    current.year,
                    current.year,
                    f"{current.year}{cal_q}",
                    f"{current.year} Q{cal_q}",
                    f"{current.year}{current.month:02d}",
                    f"{current.year} {current.strftime('%B')}",
                    f"{current.year}{week.week:02d}",
                    f"{current.year}-W{week.week}",
                    week.weekday,
                    current.strftime("%A"),
                    fiscal_year,
                    fiscal_year,
                    f"{fiscal_year}{fiscal_q}",
                    f"{fiscal_year} Q{fiscal_q}",
                    "true" if (current.month, current.day) in {(1, 1), (7, 4), (12, 25)} else "false",
                ]
            )
            current += timedelta(days=1)


def _write_clock(root: str, written: list[str]) -> None:
    """Time.txt: one row per second of day, every numeric field zero-padded.

    ``SK_TimeID`` is a 6-digit zero-padded ``HHMMSS``, not a sequence number, so
    an INT64 load coerces it. ``MarketHoursFlag``/``OfficeHoursFlag`` are
    literal true/false.
    """
    path = f"{root}/Time.txt"
    with DelimitedWriter(path, "pipe") as w:
        for second in range(86_400):
            hour, rest = divmod(second, 3600)
            minute, sec = divmod(rest, 60)
            market = 9 <= hour < 16
            w.write_row(
                [
                    f"{hour:02d}{minute:02d}{sec:02d}",
                    f"{hour:02d}:{minute:02d}:{sec:02d}",
                    f"{hour:02d}",
                    f"{hour:02d}",
                    f"{minute:02d}",
                    f"{minute:02d}",
                    f"{sec:02d}",
                    f"{sec:02d}",
                    "true" if market else "false",
                    "true" if 8 <= hour < 17 else "false",
                ]
            )


def write_finwire(root: str, counts: dict[str, int], seed: int) -> list[str]:
    """Emit FINWIRE quarterly files with fixed-width CMP/SEC/FIN records.

    Record type is 3 characters at positions 16-18, after a shared 15-char
    publication timestamp. Lines are variable width (170/196/220/246/543)
    because records are space-padded and the file is right-trimmed; the CMP
    tail field is either a company name or a numeric CIK, disambiguated
    downstream by ``char_length >= 10``.
    """
    import os

    rng = random.Random(seed)
    paths: list[str] = []
    quarter = date(2015, 1, 1)
    cmp_count = counts.get("finwire_cmp", 5_000)
    sec_count = counts.get("finwire_sec", 5_000)
    fin_count = counts.get("finwire_fin", 5_000)

    layout = [("pts", 15), ("record_type", 3), ("body", 640)]
    for _ in range(4):  # a handful of quarters is enough to exercise the parser
        year, q = quarter.year, (quarter.month - 1) // 3 + 1
        path = f"{root}/FINWIRE{year}Q{q}"
        with FixedWidthWriter(path, layout) as w:
            for _ in range(cmp_count):
                pts = f"{rng.randint(0, 23):02d}{rng.randint(0, 59):02d}{rng.randint(0, 59):02d}"
                tail_is_cik = rng.random() < 0.5
                tail = f"{rng.randint(100000, 999999)}" if tail_is_cik else f"{pick(rng, _WORDS_FALLBACK)} Holdings"
                w.write_row(
                    {
                        "pts": pts,
                        "record_type": "CMP",
                        "body": (
                            f"{rng.randint(100000, 999999):06d}"
                            f"{pick(rng, _WORDS_FALLBACK):<12}"
                            f"{tail[:58]}"
                        ),
                    }
                )
            for _ in range(sec_count):
                pts = f"{rng.randint(0, 23):02d}{rng.randint(0, 59):02d}{rng.randint(0, 59):02d}"
                w.write_row(
                    {
                        "pts": pts,
                        "record_type": "SEC",
                        "body": f"{rng.randint(0, 999999):06d}{rng.randint(0, 999999):08d}",
                    }
                )
            for _ in range(fin_count):
                pts = f"{rng.randint(0, 23):02d}{rng.randint(0, 59):02d}{rng.randint(0, 59):02d}"
                w.write_row(
                    {
                        "pts": pts,
                        "record_type": "FIN",
                        "body": f"{rng.randint(1960, 2017)}Q{rng.randint(1, 4)}{rng.uniform(-1e9, 1e9):.0f}",
                    }
                )
        paths.append(path)
        quarter = quarter + timedelta(days=91)
    return paths


_WORDS_FALLBACK = (
    "Auric Petroleum Stellar Vertex Harbor Nordic Cascade Pinnacle "
    "Ironwood Granite Vertex"
).split()
