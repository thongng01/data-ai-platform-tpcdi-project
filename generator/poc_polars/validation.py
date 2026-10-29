import csv
import hashlib
import sqlite3
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from xml.etree import ElementTree as ET

from .config import Config
from .formats import load_sources


class ValidationError(ValueError):
    pass


def _require(condition, message):
    if not condition:
        raise ValidationError(message)


def _cents(value):
    if not value:
        return None
    result = Decimal(value) * 100
    _require(result == result.to_integral_value(), "Money must have exact cent precision.")
    return int(result)


def _rows(path, source, batch):
    previous = 0
    with path.open(encoding="utf-8", newline="") as stream:
        for number, values in enumerate(csv.reader(stream, delimiter=source.separator), 1):
            try:
                metadata = {}
                if source.cdc and batch > 1:
                    flag, sequence, *values = values
                    _require(sequence.isdigit() and len(sequence) <= 12 and int(sequence) > previous, "CDC_DSN must increase numerically.")
                    previous = int(sequence)
                    _require(flag in (("I", "U") if source.name in ("Customer", "Account", "Trade") else ("I",)), "Invalid CDC operation.")
                    metadata = {"_flag": flag, "_dsn": previous}
                _require(len(values) == len(source.fields), "Unexpected field count.")
                for field, value in zip(source.fields, values):
                    field.check(value)
                yield {**dict(zip(source.columns, values)), **metadata}
            except (ValueError, TypeError) as error:
                raise ValidationError(f"{path.name}, row {number}: {error}") from error


def _insert(connection, table, records, size=2048):
    pending = []
    for record in records:
        pending.append(record)
        if len(pending) == size:
            connection.executemany(f"INSERT INTO {table} VALUES ({','.join('?' for _ in record)})", pending)
            pending.clear()
    if pending:
        connection.executemany(f"INSERT INTO {table} VALUES ({','.join('?' for _ in pending[0])})", pending)


def _xml(connection, path):
    context = ET.iterparse(path, events=("start", "end"))
    _, root = next(context)
    _require(root.tag == "{http://www.tpc.org/tpc-di}Actions", "Unexpected XML root.")
    previous = ""
    actions = {}
    for event, action in context:
        if event != "end" or action.tag != "{http://www.tpc.org/tpc-di}Action":
            continue
        timestamp = action.attrib["ActionTS"]
        datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
        _require(timestamp >= previous, "Customer actions must be in timestamp order.")
        previous = timestamp
        timestamp = timestamp.replace("T", " ")
        kind = action.attrib["ActionType"]
        _require(kind in {"NEW", "UPDCUST", "ADDACCT", "UPDACCT", "CLOSEACCT", "INACT"}, "Unknown XML action.")
        actions[kind] = actions.get(kind, 0) + 1
        customer = action.find("Customer")
        _require(customer is not None, "An action must contain a customer.")
        identifier = int(customer.attrib["C_ID"])
        existing = connection.execute("SELECT closed FROM customers WHERE id=?", (identifier,)).fetchone()
        if kind == "NEW":
            _require(existing is None, "A NEW action reused a customer ID.")
            _require(customer.find("Name/C_L_NAME") is not None and customer.find("Address/C_ADLINE1") is not None,
                     "A new customer requires name and address fields.")
            _require(customer.findall("Account"), "A new customer must have an account.")
            _require(int(customer.attrib["C_TIER"]) in (1, 2, 3), "Invalid customer tier.")
            connection.execute("INSERT INTO customers VALUES (?, ?, NULL, ?)", (identifier, timestamp, int(customer.attrib["C_TIER"])))
        else:
            _require(existing is not None and existing[0] is None, "An action requires an active existing customer.")
        if kind == "UPDCUST" and "C_TIER" in customer.attrib:
            _require(int(customer.attrib["C_TIER"]) in (1, 2, 3), "Invalid customer tier.")
            connection.execute("UPDATE customers SET tier=? WHERE id=?", (int(customer.attrib["C_TIER"]), identifier))
        if kind == "INACT":
            connection.execute("UPDATE customers SET closed=? WHERE id=?", (timestamp, identifier))
            connection.execute("UPDATE accounts SET closed=COALESCE(closed, ?) WHERE customer=?", (timestamp, identifier))
        for account in customer.findall("Account"):
            account_id = int(account.attrib["CA_ID"])
            existing_account = connection.execute("SELECT customer, closed FROM accounts WHERE id=?", (account_id,)).fetchone()
            if kind in ("NEW", "ADDACCT"):
                _require(existing_account is None, "An account ID was reused.")
                broker = int(account.findtext("CA_B_ID"))
                connection.execute("INSERT INTO accounts VALUES (?, ?, ?, ?, NULL)", (account_id, identifier, broker, timestamp))
            else:
                _require(existing_account == (identifier, None), "An account update requires the correct owner and an open account.")
                if kind == "CLOSEACCT":
                    connection.execute("UPDATE accounts SET closed=? WHERE id=?", (timestamp, account_id))
        root.clear()
    return actions


def _finwire(connection, directory):
    layouts = {
        "CMP": [15, 3, 60, 10, 4, 2, 4, 8, 80, 80, 12, 25, 20, 24, 46, 150],
        "SEC": [15, 3, 15, 6, 4, 70, 6, 13, 8, 8, 12, 10],
        "FIN": [15, 3, 4, 1, 8, 8, 17, 17, 12, 12, 12, 17, 17, 17, 13, 13, 10],
    }
    counts = {kind: 0 for kind in layouts}
    for path in sorted(directory.glob("FINWIRE*")):
        previous = ""
        with path.open(encoding="ascii") as stream:
            for line in stream:
                line = line.rstrip("\n")
                kind = line[15:18]
                _require(kind in layouts, "Invalid FINWIRE record type.")
                widths = layouts[kind]
                _require(len(line) == sum(widths), f"Invalid {kind} record width.")
                values = []
                offset = 0
                for width in widths:
                    values.append(line[offset:offset + width].strip())
                    offset += width
                timestamp = values[0]
                posted = datetime.strptime(timestamp, "%Y%m%d-%H%M%S")
                _require(timestamp >= previous, "FINWIRE PTS must be ordered.")
                _require(path.name == f"FINWIRE{posted.year}Q{(posted.month - 1) // 3 + 1}", "FINWIRE record is in the wrong quarter file.")
                previous = timestamp
                counts[kind] += 1
                if kind == "CMP":
                    connection.execute("INSERT INTO companies VALUES (?)", (int(values[3]),))
                elif kind == "SEC":
                    connection.execute("INSERT INTO securities VALUES (?, ?, ?)", (values[2], int(values[-1]), posted.strftime("%Y-%m-%d %H:%M:%S")))
                else:
                    _require(1 <= int(values[3]) <= 4, "Invalid financial quarter.")
                    _require(Decimal(values[7]) / Decimal(values[6]) == Decimal(values[10]), "Financial margin does not match earnings/revenue.")
                    _require(connection.execute("SELECT 1 FROM companies WHERE id=?", (int(values[-1]),)).fetchone(), "Financial company is missing.")
    _require(all(counts.values()), "Every FINWIRE record type must be present.")
    return counts


def _trade(connection, row, batch, cutoff, lower):
    identifier = int(row["T_ID"])
    status = row["T_ST_ID"]
    _require(status in {"PNDG", "SBMT", "CMPT", "CNCL"}, "Invalid trade status.")
    _require(row["T_TT_ID"] in {"TLB", "TLS", "TMB", "TMS", "TSL"}, "Invalid trade type.")
    _require(lower <= row["T_DTS"] < cutoff, "Trade timestamp lies outside its batch.")
    amounts = [_cents(row[name]) for name in ("T_TRADE_PRICE", "T_CHRG", "T_COMM", "T_TAX")]
    _require(all(value is not None for value in amounts) if status == "CMPT" else all(value is None for value in amounts),
             "Only completed trades may have execution amounts.")
    if status == "CMPT":
        _require(amounts[0] > 0 and all(value >= 0 for value in amounts[1:]), "Invalid execution amounts.")
    _require(int(row["T_QTY"]) > 0 and _cents(row["T_BID_PRICE"]) > 0, "Trade quantity and bid price must be positive.")
    current = connection.execute("SELECT status, account, symbol, quantity, kind, cash FROM trades WHERE id=?", (identifier,)).fetchone()
    if batch == 1 or row["_flag"] == "I":
        _require(current is None, "A trade insertion reused an ID.")
    else:
        _require(current is not None, "A trade update has no prior insertion.")
        _require((current[0], status) in {("PNDG", "SBMT"), ("SBMT", "CMPT"), ("SBMT", "CNCL")}, "Invalid trade status transition.")
        _require(current[1:] == (int(row["T_CA_ID"]), row["T_S_SYMB"], int(row["T_QTY"]), row["T_TT_ID"], int(row["T_IS_CASH"])),
                 "A lifecycle update changed immutable trade attributes.")
    connection.execute("INSERT OR REPLACE INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (identifier, int(row["T_CA_ID"]), row["T_S_SYMB"], int(row["T_QTY"]), row["T_TT_ID"], int(row["T_IS_CASH"]),
                        status, row["T_DTS"], *amounts))
    if batch > 1:
        connection.execute("INSERT INTO history VALUES (?, ?, ?)", (identifier, row["T_DTS"], status))


def validate(root: Path, config: Config, inventory=None):
    sources = load_sources()
    directory = root / "sources"
    checks = []
    with tempfile.TemporaryDirectory(prefix="validation-", dir=root) as temporary:
        connection = sqlite3.connect(str(Path(temporary) / "checks.sqlite"))
        try:
            connection.executescript("""
                PRAGMA journal_mode=OFF;
                PRAGMA synchronous=OFF;
                PRAGMA cache_size=-8192;
                PRAGMA temp_store=FILE;
                CREATE TABLE customers(id INTEGER PRIMARY KEY, created TEXT, closed TEXT, tier INTEGER);
                CREATE TABLE accounts(id INTEGER PRIMARY KEY, customer INTEGER, broker INTEGER, created TEXT, closed TEXT);
                CREATE TABLE brokers(id INTEGER PRIMARY KEY);
                CREATE TABLE companies(id INTEGER PRIMARY KEY);
                CREATE TABLE securities(symbol TEXT PRIMARY KEY, company INTEGER, created TEXT);
                CREATE TABLE trades(id INTEGER PRIMARY KEY, account INTEGER, symbol TEXT, quantity INTEGER, kind TEXT, cash INTEGER,
                                    status TEXT, timestamp TEXT, price INTEGER, charge INTEGER, commission INTEGER, tax INTEGER);
                CREATE TABLE history(trade_id INTEGER, timestamp TEXT, status TEXT, PRIMARY KEY(trade_id, timestamp));
                CREATE TABLE holdings(origin INTEGER, trade_id INTEGER PRIMARY KEY, before_quantity INTEGER, after_quantity INTEGER);
                CREATE TABLE cash(trade_id INTEGER PRIMARY KEY, account INTEGER, timestamp TEXT, amount INTEGER);
                CREATE TABLE watches(customer INTEGER, symbol TEXT, active INTEGER, PRIMARY KEY(customer, symbol));
                CREATE TABLE market(date TEXT, symbol TEXT, low INTEGER, high INTEGER, PRIMARY KEY(date, symbol));
            """)
            first = directory / "Batch1"
            for name in ("StatusType", "TradeType", "Industry", "TaxRate", "Date", "Time", "HR"):
                previous = -1
                row_count = 0
                for row in _rows(first / sources[name].filename, sources[name], 1):
                    row_count += 1
                    identifier_name = {"HR": "EmployeeID", "Date": "SK_DateID", "Time": "SK_TimeID"}.get(name)
                    if identifier_name:
                        identifier = int(row[identifier_name])
                        _require(identifier > previous, f"{name} must be ordered by numeric ID.")
                        previous = identifier
                    if name == "HR" and row["EmployeeJobCode"] == "314":
                        connection.execute("INSERT INTO brokers VALUES (?)", (int(row["EmployeeID"]),))
                if name == "Time":
                    _require(row_count == 86400 and previous == 86399, "The Time reference must cover every second of the day.")
            finwire_counts = _finwire(connection, first)
            action_counts = _xml(connection, first / "CustomerMgmt.xml")

            def zero(query, name):
                count = connection.execute(query).fetchone()[0]
                _require(count == 0, f"{name}: {count} invalid records.")
                checks.append(name)

            for batch in range(1, config.incremental_days + 2):
                folder = directory / f"Batch{batch}"
                batch_date = str(config.day(config.historical_days + batch - 1))
                _require((folder / "BatchDate.txt").read_text().strip() == batch_date, "Incorrect batch date.")
                cutoff = batch_date + " 00:00:00"
                lower = str(config.day(0 if batch == 1 else config.historical_days + batch - 2)) + " 00:00:00"
                if batch > 1:
                    for row in _rows(folder / "Customer.txt", sources["Customer"], batch):
                        identifier = int(row["C_ID"])
                        _require(int(row["C_TIER"]) in (1, 2, 3), "Invalid customer tier.")
                        existing = connection.execute("SELECT id FROM customers WHERE id=?", (identifier,)).fetchone()
                        if row["_flag"] == "I":
                            _require(existing is None, "Customer insertion reused an ID.")
                            connection.execute("INSERT INTO customers VALUES (?, ?, NULL, ?)", (identifier, lower, int(row["C_TIER"])))
                        else:
                            _require(existing is not None, "Customer update has no prior insertion.")
                            connection.execute("UPDATE customers SET tier=? WHERE id=?", (int(row["C_TIER"]), identifier))
                    for row in _rows(folder / "Account.txt", sources["Account"], batch):
                        identifier = int(row["CA_ID"])
                        existing = connection.execute("SELECT customer FROM accounts WHERE id=?", (identifier,)).fetchone()
                        if row["_flag"] == "I":
                            _require(existing is None, "Account insertion reused an ID.")
                            connection.execute("INSERT INTO accounts VALUES (?, ?, ?, ?, NULL)", (identifier, int(row["CA_C_ID"]), int(row["CA_B_ID"]), lower))
                        else:
                            _require(existing == (int(row["CA_C_ID"]),), "Account update changed ownership or has no prior insertion.")
                for _ in _rows(folder / "Prospect.csv", sources["Prospect"], batch):
                    pass
                previous_trade = -1
                for row in _rows(folder / "Trade.txt", sources["Trade"], batch):
                    if batch == 1:
                        _require(int(row["T_ID"]) > previous_trade, "Historical trades must be ordered by ID.")
                        previous_trade = int(row["T_ID"])
                    _trade(connection, row, batch, cutoff, lower)
                if batch == 1:
                    previous_trade = -1
                    def histories():
                        nonlocal previous_trade
                        for row in _rows(folder / "TradeHistory.txt", sources["TradeHistory"], 1):
                            identifier = int(row["TH_T_ID"])
                            _require(identifier >= previous_trade, "TradeHistory must be ordered by trade ID.")
                            _require(lower <= row["TH_DTS"] < cutoff, "Historical status timestamp lies outside the batch.")
                            previous_trade = identifier
                            yield identifier, row["TH_DTS"], row["TH_ST_ID"]
                    _insert(connection, "history", histories())
                _insert(connection, "holdings", ((int(row["HH_H_T_ID"]), int(row["HH_T_ID"]), int(row["HH_BEFORE_QTY"]), int(row["HH_AFTER_QTY"]))
                                                 for row in _rows(folder / "HoldingHistory.txt", sources["HoldingHistory"], batch)))
                def cash_rows():
                    for row in _rows(folder / "CashTransaction.txt", sources["CashTransaction"], batch):
                        _require(row["CT_NAME"].startswith("Trade "), "The POC cash lineage marker is missing.")
                        _require(lower <= row["CT_DTS"] < cutoff, "Cash timestamp lies outside the batch.")
                        yield int(row["CT_NAME"][6:]), int(row["CT_CA_ID"]), row["CT_DTS"], _cents(row["CT_AMT"])
                _insert(connection, "cash", cash_rows())
                previous_time = ""
                for row in _rows(folder / "WatchHistory.txt", sources["WatchHistory"], batch):
                    _require(lower <= row["W_DTS"] < cutoff, "Watch timestamp lies outside the batch.")
                    _require(row["W_DTS"] >= previous_time, "Watch events must be chronological.")
                    previous_time = row["W_DTS"]
                    identity = (int(row["W_C_ID"]), row["W_S_SYMB"])
                    lifetime = connection.execute("SELECT created, closed FROM customers WHERE id=?", (identity[0],)).fetchone()
                    _require(lifetime is not None and row["W_DTS"] >= lifetime[0] and (lifetime[1] is None or row["W_DTS"] < lifetime[1]),
                             "Watch event lies outside the customer's lifetime.")
                    state = connection.execute("SELECT active FROM watches WHERE customer=? AND symbol=?", identity).fetchone()
                    _require(row["W_ACTION"] in ("ACTV", "CNCL"), "Invalid watch action.")
                    expected = 1 if row["W_ACTION"] == "CNCL" else 0
                    _require((state[0] if state else 0) == expected, "Invalid watch transition.")
                    connection.execute("INSERT OR REPLACE INTO watches VALUES (?, ?, ?)", (*identity, 1 - expected))
                def market_rows():
                    for row in _rows(folder / "DailyMarket.txt", sources["DailyMarket"], batch):
                        _require(lower[:10] <= row["DM_DATE"] < batch_date, "Market date lies outside the batch.")
                        low, close, high = [_cents(row[name]) for name in ("DM_LOW", "DM_CLOSE", "DM_HIGH")]
                        _require(0 < low <= close <= high, "Invalid market price range.")
                        yield row["DM_DATE"], row["DM_S_SYMB"], low, high
                _insert(connection, "market", market_rows())
                zero("SELECT count(*) FROM trades AS trade LEFT JOIN history AS event ON event.trade_id=trade.id AND event.timestamp=trade.timestamp AND event.status=trade.status WHERE event.trade_id IS NULL", f"Batch{batch} trade snapshots match status history")
                zero("SELECT count(*) FROM trades AS trade WHERE trade.timestamp!=(SELECT max(timestamp) FROM history WHERE trade_id=trade.id)", f"Batch{batch} snapshots contain the latest known state")
                zero("SELECT count(*) FROM trades AS trade LEFT JOIN cash ON cash.trade_id=trade.id WHERE trade.status='CMPT' AND trade.cash=1 AND cash.trade_id IS NULL", f"Batch{batch} completed cash trades have movements")
                zero("SELECT count(*) FROM trades AS trade LEFT JOIN holdings ON holdings.trade_id=trade.id WHERE trade.status='CMPT' AND holdings.trade_id IS NULL", f"Batch{batch} completed trades have holding changes")
                connection.commit()
            zero("SELECT count(*) FROM accounts AS account LEFT JOIN customers AS customer ON account.customer=customer.id LEFT JOIN brokers AS broker ON account.broker=broker.id WHERE customer.id IS NULL OR broker.id IS NULL", "Account owners and brokers exist")
            zero("SELECT count(*) FROM securities AS security LEFT JOIN companies AS company ON security.company=company.id WHERE company.id IS NULL", "Security companies exist")
            zero("SELECT count(*) FROM trades AS trade LEFT JOIN accounts AS account ON trade.account=account.id LEFT JOIN securities AS security ON trade.symbol=security.symbol WHERE account.id IS NULL OR security.symbol IS NULL OR trade.timestamp<account.created OR trade.timestamp>=account.closed OR trade.timestamp<security.created", "Trades use valid accounts and securities")
            zero("SELECT count(*) FROM history AS event LEFT JOIN trades AS trade ON event.trade_id=trade.id LEFT JOIN accounts AS account ON trade.account=account.id WHERE trade.id IS NULL OR event.timestamp<account.created OR event.timestamp>=account.closed", "Trade events respect account lifetime")
            zero("SELECT count(*) FROM (SELECT event.*, trade.kind, lag(event.status) OVER (PARTITION BY trade_id ORDER BY event.timestamp) AS prior_status FROM history AS event JOIN trades AS trade ON trade.id=event.trade_id) WHERE (prior_status IS NULL AND status!=CASE WHEN kind IN ('TLB','TLS') THEN 'PNDG' ELSE 'SBMT' END) OR (prior_status IS NOT NULL AND NOT ((prior_status='PNDG' AND status='SBMT') OR (prior_status='SBMT' AND status IN ('CMPT','CNCL'))))", "All trade histories follow legal transitions")
            zero("SELECT count(*) FROM cash LEFT JOIN trades AS trade ON cash.trade_id=trade.id WHERE trade.id IS NULL OR trade.status!='CMPT' OR trade.cash!=1 OR cash.account!=trade.account OR cash.timestamp!=trade.timestamp OR cash.amount!=(CASE WHEN trade.kind IN ('TLB','TMB') THEN -1 ELSE 1 END)*trade.quantity*trade.price-trade.charge-trade.commission-trade.tax", "Cash reconciles with completed trades in integer cents")
            zero("SELECT count(*) FROM holdings AS holding LEFT JOIN trades AS trade ON holding.trade_id=trade.id LEFT JOIN trades AS origin ON holding.origin=origin.id WHERE trade.id IS NULL OR origin.id IS NULL OR trade.status!='CMPT' OR origin.status!='CMPT' OR origin.kind NOT IN ('TLB','TMB') OR trade.account!=origin.account OR trade.symbol!=origin.symbol OR trade.timestamp<origin.timestamp OR (CASE WHEN trade.kind IN ('TLB','TMB') THEN trade.quantity ELSE -trade.quantity END)!=holding.after_quantity-holding.before_quantity", "Holding changes have valid origins and quantities")
            zero("SELECT count(*) FROM (SELECT holding.*, lag(after_quantity,1,0) OVER (PARTITION BY origin ORDER BY trade.timestamp, trade.id) AS prior_quantity FROM holdings AS holding JOIN trades AS trade ON holding.trade_id=trade.id) WHERE before_quantity!=prior_quantity", "Holding quantities remain continuous across batches")
            zero("SELECT count(*) FROM watches AS watch LEFT JOIN customers AS customer ON watch.customer=customer.id LEFT JOIN securities AS security ON watch.symbol=security.symbol WHERE customer.id IS NULL OR security.symbol IS NULL", "Watch customers and securities exist")
            zero("SELECT count(*) FROM market LEFT JOIN securities AS security ON market.symbol=security.symbol WHERE security.symbol IS NULL", "Market securities exist")
            zero("SELECT count(*) FROM trades AS trade LEFT JOIN market ON market.symbol=trade.symbol AND market.date=substr(trade.timestamp,1,10) WHERE trade.status='CMPT' AND (market.symbol IS NULL OR trade.price<market.low OR trade.price>market.high)", "Execution prices lie within the daily range")
            totals = {name: connection.execute(f"SELECT count(*) FROM {name}").fetchone()[0] for name in ("customers", "accounts", "trades", "history", "holdings", "cash")}
            _require(totals["customers"] == config.total_customers, "The customer count differs from configuration.")
            _require(totals["accounts"] == config.total_customers * 2 + config.customers // 5, "The account count differs from the configured POC schedule.")
            _require(finwire_counts == {"CMP": config.companies, "SEC": config.securities, "FIN": config.companies * 4}, "FINWIRE population counts differ from configuration.")
            checks.append("Configured entity counts match generated records")
            totals["net_cash_cents"] = connection.execute("SELECT COALESCE(sum(amount),0) FROM cash").fetchone()[0]
        except (ValueError, sqlite3.Error, ET.ParseError) as error:
            raise ValidationError(str(error)) from error
        finally:
            connection.close()
    if inventory is not None:
        actual_files = {str(path.relative_to(directory)) for path in directory.rglob("*") if path.is_file()}
        _require(actual_files == {item["path"] for item in inventory}, "Source file coverage differs from the manifest.")
        for item in inventory:
            path = directory / item["path"]
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            _require(digest == item["sha256"] and path.stat().st_size == item["bytes"], f"Manifest mismatch: {item['path']}")
        checks.append("All source bytes match the manifest")
    return {"checks": checks, "totals": totals, "xml_actions": action_counts, "finwire": finwire_counts}
