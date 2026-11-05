"""Independent saved-file checks must fail even without checksum evidence."""

import json
import shutil
from datetime import date
from decimal import Decimal
from pathlib import Path
from xml.etree import ElementTree as ET

import polars as pl
import pytest

from data.data_source.generator import validation
from data.data_source.generator.config import Config
from data.data_source.generator.formats import load_sources
from data.data_source.generator.generation import Generation, expected_file_list
from data.data_source.generator.validation import ValidationError, _cents, _rows, _stream, _xml_patch, validate


@pytest.mark.parametrize("value,expected", [("0.00", 0), ("-12.34", -1234), ("999.99", 99999), ("", None)])
def test_exact_cent_parser(value, expected):
    assert _cents(value) == expected


@pytest.mark.parametrize("value", ["0.001", "NaN", "Infinity", "not money"])
def test_cent_parser_rejects_non_cent_values(value):
    with pytest.raises(ValidationError):
        _cents(value)


@pytest.mark.parametrize("payload", ["I|2|1|1|0|1\n", "U|1|1|1|0|1\n", "I|1|1|1|0|1\r\n", "I|1|1|1|0|1"])
def test_daily_parser_rejects_sequences_operations_and_line_endings(tmp_path, payload):
    path = tmp_path / "HoldingHistory.txt"
    path.write_bytes(payload.encode())
    with pytest.raises(ValidationError):
        list(_rows(path, load_sources()["HoldingHistory"], 2))


def test_xml_patch_keeps_absent_and_empty_distinct():
    customer = ET.fromstring('<Customer C_ID="1"><Name><C_M_NAME /></Name><ContactInfo><C_ALT_EMAIL /></ContactInfo></Customer>')
    patch = _xml_patch(customer, load_sources()["Customer"])
    assert patch == {"C_ID": "1", "C_M_NAME": "", "C_EMAIL_2": ""}
    assert "C_EMAIL_1" not in patch
    assert "C_L_NAME" not in patch


@pytest.mark.parametrize("changes", [{"ct_id": True}, {"ct_amt": 1.0}, {"ct_amt": "1.001"},
                                    {"ct_dts": "2019-01-01 08:00:00"}, {"schema_version": True}, {"extra": 1}])
def test_stream_contract_rejects_wrong_types_and_fields(tmp_path, changes):
    payload = {"ct_id": 1, "ct_ca_id": 1, "ct_dts": "2019-01-01T08:00:00Z", "ct_amt": "1.00",
               "ct_name": "Deposit", "schema_version": 1, **changes}
    path = tmp_path / "20190101.jsonl"
    path.write_text(json.dumps(payload) + "\n")
    with pytest.raises(ValidationError):
        list(_stream(path, load_sources()["CashTransaction"]))


def test_independent_cadence_and_calendar_boundaries():
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
    root, config, inventory = exported
    result = validate(root, config, inventory)
    assert result["status"] == "passed"
    assert result["completed_trades"] > 0
    assert result["cash_transactions"] > result["completed_trades"]
    assert result["customer_actions"].get("CLOSEACCT", 0) + result["customer_actions"].get("INACT", 0) > 0
    assert not list(root.glob("validation-*"))
    original_init = validation._Stage.__init__

    def smaller_partitions(self, directory, count):
        original_init(self, directory, count * 3)

    # Split the typed blocks to inspect partition I/O directly. The production
    # validator still visits every configured partition, so replay all three here.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(validation._Stage, "__init__", smaller_partitions)
        stage = validation._Stage(root / "partition-unit", 1)
        schema = {"account": pl.Int64, "_partition": pl.String}
        for account in range(9):
            stage.add("Example", {"account": account, "_partition": str(account % 3)}, schema)
        stage.finish()
        for partition in range(3):
            assert sorted(stage.frame("Example", partition)["account"].to_list()) == list(range(partition, 9, 3))
    shutil.rmtree(root / "partition-unit")


@pytest.mark.parametrize("case,match", [
    ("settlement", "Settlement"), ("holding_origin", "Holding origin"), ("holding_quantity", "Holding origin"),
    ("execution_price", "fees|price|Limit"), ("null_execution", "Only completed"),
    ("account_type", "cash type"), ("cash_overdraft", "funds|spending|balance"),
    ("cash_sequence", "CT_ID"), ("watch", "Watch cancellation"),
    ("missing_holding", "no holding"), ("missing_dividend", "dividend"),
    ("lifecycle", "lifecycle|initial status"), ("cdc_sequence", "CDC_DSN"),
    ("missing_file", "coverage"), ("unexpected_file", "coverage"), ("early_close", "NYSE session|snapshot"),
])
def test_corrupted_source_fails_without_hash_evidence(exported, tmp_path, case, match):
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


def test_saved_hash_evidence_detects_unrelated_text_change(exported, tmp_path):
    original, config, inventory = exported
    root = tmp_path / "hash"
    shutil.copytree(original, root, ignore=shutil.ignore_patterns("_work"))
    path = root / "offline" / "initial" / "Prospect.csv"
    path.write_text(path.read_text().replace("Alex", "Alec", 1))
    with pytest.raises(ValidationError, match="SHA-256"):
        validate(root, config, inventory)


def test_duplicate_json_keys_fail(tmp_path):
    path = tmp_path / "20190101.jsonl"
    path.write_text('{"ct_id":1,"ct_id":1,"ct_ca_id":1,"ct_dts":"2019-01-01T08:00:00Z",'
                    '"ct_amt":"1.00","ct_name":"Deposit","schema_version":1}\n')
    with pytest.raises(ValidationError, match="duplicate JSON key"):
        list(_stream(path, load_sources()["CashTransaction"]))
