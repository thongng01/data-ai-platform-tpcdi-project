"""Independent saved-file checks must fail even without checksum evidence."""

import json
import shutil
from datetime import date
from types import SimpleNamespace

import polars as pl
from decimal import Decimal
from xml.etree import ElementTree as ET

import pytest

from data.data_source.generator import validation
from data.data_source.generator.config import Config
from data.data_source.generator.formats import load_sources
from data.data_source.generator.generation import Generation, expected_file_list
from data.data_source.generator.validation import ValidationError, _cents, _rows, _stream, _xml_patch, validate


@pytest.mark.parametrize("value,expected", [("0.00", 0), ("-12.34", -1234), ("999.99", 99999), ("", None)])
def test_exact_cent_parser(value, expected):
    """Parse decimal amounts without changing their exact cent values."""
    assert _cents(value) == expected


@pytest.mark.parametrize("value", ["0.001", "NaN", "Infinity", "not money"])
def test_cent_parser_rejects_non_cent_values(value):
    """Reject fractional cents and nonfinite money values."""
    with pytest.raises(ValidationError):
        _cents(value)


@pytest.mark.parametrize("payload", ["I|2|1|1|0|1\n", "U|1|1|1|0|1\n", "I|1|1|1|0|1\r\n", "I|1|1|1|0|1"])
def test_daily_parser_rejects_sequences_operations_and_line_endings(tmp_path, payload):
    """Enforce consecutive CDC sequences, accepted flags, and LF record endings."""
    path = tmp_path / "HoldingHistory.txt"
    path.write_bytes(payload.encode())
    with pytest.raises(ValidationError):
        list(_rows(path, load_sources()["HoldingHistory"], 2))


def test_xml_patch_keeps_absent_and_empty_distinct():
    """Keep omitted CMS fields separate from explicit empty field updates."""
    customer = ET.fromstring('<Customer C_ID="1"><Name><C_M_NAME /></Name><ContactInfo><C_ALT_EMAIL /></ContactInfo></Customer>')
    patch = _xml_patch(customer, load_sources()["Customer"])
    assert patch == {"C_ID": "1", "C_M_NAME": "", "C_EMAIL_2": ""}
    assert "C_EMAIL_1" not in patch
    assert "C_L_NAME" not in patch


@pytest.mark.parametrize("changes", [{"ct_id": True}, {"ct_amt": 1.0}, {"ct_amt": "1.001"},
                                    {"ct_dts": "2019-01-01 08:00:00"}, {"schema_version": True}, {"extra": 1}])
def test_stream_contract_rejects_wrong_types_and_fields(tmp_path, changes):
    """Reject stream records that violate strict JSON types or field coverage."""
    payload = {"ct_id": 1, "ct_ca_id": 1, "ct_dts": "2019-01-01T08:00:00Z", "ct_amt": "1.00",
               "ct_name": "Deposit", "schema_version": 1, **changes}
    path = tmp_path / "20190101.jsonl"
    path.write_text(json.dumps(payload) + "\n")
    with pytest.raises(ValidationError):
        list(_stream(path, load_sources()["CashTransaction"]))


def test_independent_cadence_and_calendar_boundaries():
    """Derive source coverage and NYSE dates independently of exporter state."""
    config = Config(initial_start="2018-12-20", initial_end="2018-12-31", daily_start="2019-01-01", daily_end="2019-02-03",
                    scale_factor=.01, securities=8, companies=4)
    expected = validation._expected_files(config)
    assert expected == set(expected_file_list(config))
    assert "offline/daily/20190131/FINWIRE2019Q1" in expected
    assert "offline/daily/20190201/Prospect_20190201.csv" in expected
    assert not any("CashTransaction" in path for path in expected if "/daily/" in path)
    assert "stream/cash_transactions/20190203.jsonl" in expected
    sessions = validation._sessions(config)
    assert "2019-01-01" not in sessions
    assert sessions["2018-12-24"] == "13:00:00"
    assert "2019-02-03" not in sessions
    assert validation._release(2018 * 4 + 3) == date(2019, 1, 31)
    assert validation._prior_year(date(2020, 2, 29)) == date(2019, 2, 28)


@pytest.fixture(scope="module")
def exported(tmp_path_factory):
    """Create a small exported fixture without manifest hash evidence."""
    from data.data_source.generator.activity import generate_activity
    from data.data_source.generator.customers import generate_prospects
    from data.data_source.generator.market import Market
    from data.data_source.generator.population import build_population
    from data.data_source.generator.reference import generate_reference

    root = tmp_path_factory.mktemp("validator-m1")
    config = Config(initial_start="2018-11-15", initial_end="2018-12-31", daily_start="2019-01-01", daily_end="2019-02-03",
                    scale_factor=.01, securities=8, companies=4, dividend_share=1,
                    annual_trade_rates=(180, 180, 180, 180, 180), annual_closure=.5, annual_inactivation=.2,
                    customer_group_size=7, export_rows=256, threads=1)
    generation = Generation(config, root)
    generate_reference(generation)
    market = Market.build(config, root, generation)
    population = build_population(config, root)
    generate_activity(generation, market, population)
    generate_prospects(generation, population)
    inventory = generation.finish()
    # No manifest is created. Semantic corruption tests cannot fail on a saved hash.
    assert not (root / "manifest.json").exists()
    return root, config, inventory


def _mutate_row(root, source, field, replacement, predicate=lambda row: True, *, daily=False):
    """Change the first matching source field while retaining its row structure."""
    columns = load_sources()[source].columns
    paths = sorted((root / "offline" / "daily").glob(f"*/{source}_*.txt")) if daily else [root / "offline" / "initial" / f"{source}.txt"]
    for path in paths:
        lines = path.read_text().splitlines()
        for index, line in enumerate(lines):
            values = line.split("|")
            prefix = values[:2] if daily else []
            row = dict(zip(columns, values[2:] if daily else values))
            if predicate(row):
                row[field] = replacement(row[field]) if callable(replacement) else replacement
                lines[index] = "|".join(prefix + [row[name] for name in columns])
                path.write_text("\n".join(lines) + "\n")
                return path
    raise AssertionError(f"Fixture has no matching {source} row.")


def test_exported_replay_and_partition_replay_agree(exported, monkeypatch):
    """Require equivalent business totals with one partition and multiple partitions."""
    root, config, inventory = exported
    result = validate(root, config, inventory)
    assert result["status"] == "passed"
    assert result["completed_trades"] > 0
    assert result["cash_transactions"] > result["completed_trades"]
    assert result["customer_actions"].get("CLOSEACCT", 0) + result["customer_actions"].get("INACT", 0) > 0
    assert not list(root.glob("validation-*"))
    monkeypatch.setattr(validation, "_VALIDATION_PARTITION_BYTES", max(1, result["bytes"] // 3))
    partitioned = validate(root, config, inventory)
    assert partitioned["partitions"] >= 3
    for name in ("customers", "accounts", "trades", "completed_trades", "cash_transactions", "rows", "bytes"):
        assert partitioned[name] == result[name]


@pytest.mark.parametrize("case,match", [
    ("settlement", "Settlement"), ("holding_origin", "Holding origin"), ("holding_quantity", "Holding origin"),
    ("execution_price", "fees|price|Limit"), ("null_execution", "Only completed"),
    ("account_type", "cash type"), ("cash_overdraft", "funds|spending|balance"),
    ("cash_sequence", "CT_ID"), ("watch", "Watch cancellation"),
    ("missing_holding", "no holding"), ("missing_dividend", "dividend"),
    ("lifecycle", "lifecycle|initial status"), ("cdc_sequence", "CDC_DSN"),
    ("missing_file", "coverage"), ("unexpected_file", "coverage"), ("early_close", "NYSE session|snapshot"),
    ("prospect_identity", "Duplicate Prospect"), ("customer_tax", "tax rate"), ("xml_structure", "XML root child"),
])
def test_corrupted_source_fails_without_hash_evidence(exported, tmp_path, case, match):
    """Reject each business or structural corruption through independent replay checks."""
    original, config, _ = exported
    root = tmp_path / "corrupt"
    shutil.copytree(original, root, ignore=shutil.ignore_patterns("_work"))
    if case == "settlement":
        _mutate_row(root, "CashTransaction", "CT_AMT", lambda value: str(Decimal(value) + Decimal(".01")),
                    lambda row: row["CT_NAME"].startswith("Trade settlement "))
    elif case == "holding_origin":
        _mutate_row(root, "HoldingHistory", "HH_H_T_ID", lambda value: str(int(value) + 1))
    elif case == "holding_quantity":
        _mutate_row(root, "HoldingHistory", "HH_AFTER_QTY", lambda value: str(int(value) + 1))
    elif case == "execution_price":
        _mutate_row(root, "Trade", "T_TRADE_PRICE", "999999.99", lambda row: row["T_ST_ID"] == "CMPT")
    elif case == "null_execution":
        _mutate_row(root, "Trade", "T_CHRG", "1.00", lambda row: row["T_ST_ID"] != "CMPT")
    elif case == "account_type":
        _mutate_row(root, "Trade", "T_IS_CASH", lambda value: str(1 - int(value)))
    elif case == "cash_overdraft":
        cash_accounts = {row.split("|")[8] for row in (root / "offline" / "initial" / "Trade.txt").read_text().splitlines()
                         if row.split("|")[4] == "1" and row.split("|")[3] in {"TMB", "TLB"}}
        _mutate_row(root, "CashTransaction", "CT_AMT", "0.01", lambda row: row["CT_NAME"] == "Deposit" and row["CT_CA_ID"] in cash_accounts)
    elif case == "cash_sequence":
        _mutate_row(root, "CashTransaction", "CT_ID", "2")
    elif case == "watch":
        _mutate_row(root, "WatchHistory", "W_ACTION", "CNCL")
    elif case == "missing_holding":
        path = root / "offline" / "initial" / "HoldingHistory.txt"
        lines = path.read_text().splitlines()
        assert lines
        path.write_text("\n".join(lines[1:]) + "\n" if len(lines) > 1 else "")
    elif case == "missing_dividend":
        found = False
        for path in sorted((root / "stream" / "cash_transactions").glob("*.jsonl")):
            payloads = [json.loads(line) for line in path.read_text().splitlines()]
            for payload in payloads:
                if payload["ct_name"].startswith("Dividend "):
                    payload["ct_name"] = "Deposit"
                    found = True
                    break
            if found:
                path.write_text("".join(json.dumps(payload) + "\n" for payload in payloads))
                break
        assert found
    elif case == "lifecycle":
        _mutate_row(root, "TradeHistory", "TH_ST_ID", "CNCL")
    elif case == "cdc_sequence":
        path = next(path for path in sorted((root / "offline" / "daily").glob("*/Trade_*.txt")) if path.stat().st_size)
        lines = path.read_text().splitlines()
        values = lines[0].split("|")
        values[1] = "2"
        lines[0] = "|".join(values)
        path.write_text("\n".join(lines) + "\n")
    elif case == "missing_file":
        (root / "stream" / "cash_transactions" / "20190203.jsonl").unlink()
    elif case == "unexpected_file":
        (root / "offline" / "daily" / "20190203" / "CashTransaction_20190203.txt").touch()
    elif case == "prospect_identity":
        path = root / "offline" / "initial" / "Prospect.csv"
        with path.open("a") as stream:
            stream.write(path.read_text().splitlines()[0] + "\n")
    elif case == "customer_tax":
        path = root / "offline" / "initial" / "CustomerMgmt.xml"
        original_text = path.read_text()
        changed = original_text.replace("<C_LCL_TX_ID>US1</C_LCL_TX_ID>", "<C_LCL_TX_ID>NOPE</C_LCL_TX_ID>", 1)
        assert changed != original_text
        path.write_text(changed)
    elif case == "xml_structure":
        path = root / "offline" / "initial" / "CustomerMgmt.xml"
        path.write_text(path.read_text().replace("</TPCDI:Actions>", "<Ignored />\n</TPCDI:Actions>"))
    elif case == "early_close":
        history = [line.split("|") for line in (root / "offline" / "initial" / "TradeHistory.txt").read_text().splitlines()]
        latest = max((row for row in history if row[1].startswith("2018-12-24") and row[2] == "CMPT"), key=lambda row: row[1])
        path = _mutate_row(root, "TradeHistory", "TH_DTS", "2018-12-24 15:00:00",
                           lambda row: row["TH_T_ID"] == latest[0] and row["TH_ST_ID"] == "CMPT")
        # Keep the snapshot and history consistent to isolate the session check.
        lines = sorted(path.read_text().splitlines(), key=lambda line: (int(line.split("|")[0]), line.split("|")[1]))
        path.write_text("\n".join(lines) + "\n")
        identifier = next(line.split("|")[0] for line in lines if line.split("|")[1] == "2018-12-24 15:00:00")
        _mutate_row(root, "Trade", "T_DTS", "2018-12-24 15:00:00", lambda row: row["T_ID"] == identifier)
        cash_path = _mutate_row(root, "CashTransaction", "CT_DTS", "2018-12-24 15:00:00",
                                lambda row: row["CT_NAME"] == f"Trade settlement {identifier}")
        cash_rows = sorted((line.split("|") for line in cash_path.read_text().splitlines()), key=lambda row: (row[2], int(row[0])))
        for number, row in enumerate(cash_rows, 1):
            row[0] = str(number)
        cash_path.write_text("".join("|".join(row) + "\n" for row in cash_rows))
    assert not (root / "manifest.json").exists()
    with pytest.raises(ValidationError, match=match):
        validate(root, config)
    assert not list(root.glob("validation-*"))


@pytest.mark.parametrize("source_name,partitions", [("CashTransaction", 128), ("DailyMarket", 1024)])
def test_partition_sink_bounds_aggregate_row_group_buffers(tmp_path, monkeypatch, source_name, partitions):
    """Keep aggregate Parquet buffers bounded when many partitions exist."""
    from data.data_source.generator.validation import _Stage, _stage_source
    from data.data_source.generator.formats import load_sources
    import polars as pl

    stage = _Stage(tmp_path, 128)
    stage.date_partitions = 1024
    source = load_sources()[source_name]
    row = {"CT_ID": "1", "CT_CA_ID": "1", "CT_DTS": "2019-01-02 08:00:00", "CT_AMT": "10.00", "CT_NAME": "Deposit"} if source_name == "CashTransaction" else {
        "DM_DATE": "2019-01-02", "DM_S_SYMB": "S0000000001", "DM_CLOSE": "10.00", "DM_HIGH": "11.00", "DM_LOW": "9.00", "DM_VOL": "1"}
    _stage_source(stage, source, row, "initial", 1 if source_name == "CashTransaction" else "2019-01-02")
    original = pl.LazyFrame.sink_parquet
    calls = []
    def bounded_sink(frame, path, **kwargs):
        """Record each partitioned sink and enforce its aggregate row-group bound."""
        if isinstance(path, pl.PartitionBy):
            group_rows = kwargs.get("row_group_size")
            assert group_rows is not None
            assert group_rows * partitions <= 262144
            calls.append(group_rows)
        return original(frame, path, **kwargs)
    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", bounded_sink)
    stage.finish()
    assert calls


def test_saved_hash_evidence_detects_unrelated_text_change(exported, tmp_path):
    """Detect a valid-format text mutation using the saved source hash."""
    original, config, inventory = exported
    root = tmp_path / "hash"
    shutil.copytree(original, root, ignore=shutil.ignore_patterns("_work"))
    path = root / "offline" / "initial" / "Prospect.csv"
    lines = path.read_text().splitlines()
    fields = lines[0].split(",")
    fields[1] = "Different" if fields[1] != "Different" else "Changed"
    lines[0] = ",".join(fields)
    path.write_text("\n".join(lines) + "\n")
    with pytest.raises(ValidationError, match="SHA-256"):
        validate(root, config, inventory)


def test_duplicate_json_keys_fail(tmp_path):
    """Reject duplicate stream JSON keys instead of silently overwriting them."""
    path = tmp_path / "20190101.jsonl"
    path.write_text('{"ct_id":1,"ct_id":1,"ct_ca_id":1,"ct_dts":"2019-01-01T08:00:00Z",'
                    '"ct_amt":"1.00","ct_name":"Deposit","schema_version":1}\n')
    with pytest.raises(ValidationError, match="duplicate JSON key"):
        list(_stream(path, load_sources()["CashTransaction"]))


def _causal_replay(tmp_path, *, pending_first, submission_conflict=False, negative_first=False):
    """Build same-second source scenarios that require causal cash and trade ordering."""
    config = Config(initial_start="2018-11-15", initial_end="2018-12-31", daily_start="2019-01-01", daily_end="2019-01-01",
                    scale_factor=.01, companies=1, securities=1, commission_basis_points=0, charge_cents=0 if submission_conflict else 100)
    customer_directory = tmp_path / "customers"
    customer_directory.mkdir()
    pl.DataFrame({"customer_id": [1], "created": ["2018-11-15 00:00:00"], "closed": [None]},
                 schema={"customer_id": pl.Int64, "created": pl.String, "closed": pl.String}).write_parquet(customer_directory / "one.parquet")
    account = {"account_id": 1, "customer_id": 1, "is_cash": True, "margin_limit_cents": 0,
               "wealth_cents": 100000, "profile": 0, "open_date": config.start, "close_date": None}
    fields = {"CA_ID": 1, "CA_B_ID": 1, "CA_C_ID": 1, "CA_NAME": "One", "CA_TAX_ST": 1, "CA_ST_ID": "ACTV"}
    version = {**fields, "_present": list(fields), "_timestamp": "2018-11-15 00:00:00", "_day": "2018-11-15", "_flag": "I", "_order": 1}
    tables = {"Hidden": [account], "AccountXML": [version], "Trade": [], "TradeHistory": [], "HoldingHistory": [], "CashTransaction": []}

    def snapshot(identifier, kind, timestamp):
        """Add a completed initial Trade snapshot with exact replay money fields."""
        tables["Trade"].append({"T_ID": identifier, "T_CA_ID": 1, "T_S_SYMB": "S0000000001", "T_ST_ID": "CMPT",
                                "T_DTS": timestamp, "T_TT_ID": kind, "T_IS_CASH": 1, "T_QTY": 1, "T_BID_PRICE": 1000,
                                "T_EXEC_NAME": "Alex Broker", "T_TRADE_PRICE": 1000, "T_CHRG": 100, "T_COMM": 0, "T_TAX": 0,
                                "_day": "initial", "_flag": "I", "_order": len(tables["Trade"]) + 1,
                                "_market_low": 1000, "_market_high": 1000})

    def history(identifier, status, timestamp):
        """Add one source-visible status transition to the initial trade history."""
        tables["TradeHistory"].append({"TH_T_ID": identifier, "TH_DTS": timestamp, "TH_ST_ID": status,
                                       "_day": "initial", "_order": len(tables["TradeHistory"]) + 1})

    def cash(amount, name, timestamp):
        """Append a cash transaction with its exact source sequence and amount."""
        tables["CashTransaction"].append({"CT_ID": len(tables["CashTransaction"]) + 1, "CT_CA_ID": 1,
                                          "CT_DTS": timestamp, "CT_AMT": amount, "CT_NAME": name, "_day": "initial"})

    def holding(identifier, origin, before, after):
        """Append the saved origin and before/after quantities for a completed trade."""
        tables["HoldingHistory"].append({"HH_T_ID": identifier, "HH_H_T_ID": origin, "HH_BEFORE_QTY": before,
                                         "HH_AFTER_QTY": after, "_day": "initial"})

    timestamp = "2018-11-16 09:35:00"
    if negative_first:
        cash(-100, "Withdrawal", "2018-11-16 08:00:00")
        cash(100, "Deposit", "2018-11-16 08:00:00")
        expected_trades = 0
    elif submission_conflict:
        cash(100, "Deposit", "2018-11-16 08:00:00")
        snapshot(1000001, "TLB", timestamp)
        tables["Trade"][-1].update(T_ST_ID="PNDG", T_BID_PRICE=70, T_TRADE_PRICE=None, T_CHRG=None, T_COMM=None, T_TAX=None)
        history(1000001, "PNDG", timestamp)
        snapshot(1000002, "TMB", timestamp)
        tables["Trade"][-1].update(T_BID_PRICE=80, T_TRADE_PRICE=20, T_CHRG=0, _market_low=20, _market_high=80)
        history(1000002, "SBMT", timestamp)
        history(1000002, "CMPT", timestamp)
        cash(-20, "Trade settlement 1000002", timestamp)
        holding(1000002, 1000002, 0, 1)
        expected_trades = 2
    elif pending_first:
        # An older order reserves $11. The newer market order creates the position
        # before that older order completes in the same source second.
        cash(2200, "Deposit", "2018-11-15 08:00:00")
        snapshot(1000001, "TLB", timestamp)
        history(1000001, "PNDG", "2018-11-15 09:30:00")
        history(1000001, "CMPT", timestamp)
        snapshot(1000002, "TMB", timestamp)
        history(1000002, "SBMT", timestamp)
        history(1000002, "CMPT", timestamp)
        cash(-1100, "Trade settlement 1000002", timestamp)
        cash(-1100, "Trade settlement 1000001", timestamp)
        holding(1000002, 1000002, 0, 1)
        holding(1000001, 1000002, 1, 2)
        expected_trades = 2
    else:
        # The first buy leaves $2 and one share. The same-second sale adds $9,
        # which funds the next $11 buy. Trade-only ordering loses that funding.
        cash(1300, "Deposit", "2018-11-16 08:00:00")
        opening = "2018-11-16 09:30:00"
        snapshot(1000001, "TMB", opening)
        history(1000001, "SBMT", opening)
        history(1000001, "CMPT", opening)
        cash(-1100, "Trade settlement 1000001", opening)
        holding(1000001, 1000001, 0, 1)
        for identifier, kind in ((1000002, "TMS"), (1000003, "TMB")):
            snapshot(identifier, kind, timestamp)
            history(identifier, "SBMT", timestamp)
            history(identifier, "CMPT", timestamp)
        cash(900, "Trade settlement 1000002", timestamp)
        cash(-1100, "Trade settlement 1000003", timestamp)
        holding(1000002, 1000001, 1, 0)
        holding(1000003, 1000003, 0, 1)
        expected_trades = 3
    stage = SimpleNamespace(root=tmp_path, records=lambda table, partition: iter(tables.get(table, [])),
                            frame=lambda table, partition: pl.DataFrame(tables.get(table, [])))
    securities = {"S0000000001": [("2017-07-01 00:00:00", 0, "ACTV")]}
    result = validation._account_replay(stage, 0, config, securities,
                                        {"2018-11-15": "16:00:00", "2018-11-16": "16:00:00"}, {1})
    assert result == (1, expected_trades, expected_trades)


@pytest.mark.parametrize("pending_first", [False, True])
def test_same_second_settlement_order_preserves_cash_and_position_origin(tmp_path, pending_first):
    """Use source cash order to preserve spending and holding origins in timestamp ties."""
    _causal_replay(tmp_path, pending_first=pending_first)


def test_same_second_new_pending_order_reserves_before_higher_trade_id(tmp_path):
    """Reject a market buy that would spend funds already reserved by a pending order."""
    with pytest.raises(ValidationError, match="Open buy orders reserve unavailable funds"):
        _causal_replay(tmp_path, pending_first=False, submission_conflict=True)


def test_same_second_later_deposit_cannot_fund_earlier_cash_transaction(tmp_path):
    """Reject a withdrawal that would need a later same-second deposit."""
    with pytest.raises(ValidationError, match="Cash balance"):
        _causal_replay(tmp_path, pending_first=False, negative_first=True)
