"""Prove source-only faults, dated channels, and independent fault verification."""

from collections import Counter
from datetime import date
from decimal import Decimal
import json
import shutil
from types import SimpleNamespace

import pytest

from data.data_source.generator.config import Config
from data.data_source.generator.formats import load_sources, source_for_day
from data.data_source.generator.recording import SourceRecorder
from data.data_source.generator.runner import run
from data.data_source.generator.validation import ValidationError, validate


def _config(**changes):
    """Keep the recording fixture small while crossing the daily schema date."""
    return Config(**({"initial_start": "2019-06-01", "initial_end": "2019-06-27",
                     "daily_start": "2019-06-28", "daily_end": "2019-07-12", "scale_factor": .01,
                     "securities": 3, "companies": 2, "annual_closure": 0, "annual_inactivation": 0,
                     "annual_trade_rates": (200, 200, 200, 200, 200), "annual_customer_update": 1,
                     "threads": 1, "export_rows": 113} | changes))


def _trade(identifier=1000001):
    """Describe one completed buy in correct source units."""
    return {"T_ID": identifier, "T_DTS": "2019-07-01 10:00:00", "T_ST_ID": "CMPT", "T_TT_ID": "TMB",
            "T_IS_CASH": 1, "T_S_SYMB": "S0000000001", "T_QTY": 10, "T_BID_PRICE": "20.00",
            "T_CA_ID": 1, "T_EXEC_NAME": "Sam Brown", "T_TRADE_PRICE": "20.00", "T_CHRG": "1.00",
            "T_COMM": ".20", "T_TAX": "0.00"}


@pytest.mark.parametrize("charge,commission", [(1, 0), (0, 1), (1, 1)])
def test_fee_faults_are_separate_and_do_not_change_correct_fields(charge, commission):
    """Each fee draw changes only its selected source field, not the input."""
    emitted, audit = [], []
    config = _config(trade_charge_error_rate=charge, trade_commission_error_rate=commission)
    generation = SimpleNamespace(config=config, emit=lambda *args: emitted.append(args), record_fault=audit.append)
    fields = _trade()
    recorder = SourceRecorder(generation)
    recorder.record("Trade", date(2019, 7, 1), fields, "key", context={"profile": 2})
    recorded = emitted[0][2][0]
    assert fields["T_CHRG"] == "1.00" and fields["T_COMM"] == ".20"
    assert recorded["T_CHRG"] == ("200.01" if charge else "1.00")
    assert recorded["T_COMM"] == ("200.01" if commission else ".20")
    assert len(audit) == charge + commission
    assert "profile" not in recorded


def test_channel_contract_changes_only_daily_rows_and_is_cached():
    """The initial load stays at 14 fields even with an earlier change date."""
    config = _config(trade_channel_start="2019-06-01")
    source = load_sources()["Trade"]
    assert len(source_for_day(source, config.cutover, config).fields) == 14
    first = source_for_day(source, config.daily_start_date, config)
    assert len(first.fields) == 15
    assert first is source_for_day(source, config.end, config)


def test_channels_follow_profile_weights_without_hidden_output():
    """Retirees use brokers more often than active and day traders."""
    config = _config(trade_charge_error_rate=0, trade_commission_error_rate=0)
    broker_counts = []
    for profile in (2, 4):
        channels = []
        generation = SimpleNamespace(config=config, emit=lambda source, day, rows: channels.append(rows[0]["T_CHANNEL"]))
        recorder = SourceRecorder(generation)
        for identifier in range(1, 1001):
            recorder.record("Trade", "2019-07-01", _trade(identifier), str(identifier), context={"profile": profile})
        broker_counts.append(channels.count("BROKER"))
    assert broker_counts[0] > 600 and broker_counts[1] < 50


@pytest.fixture(scope="module")
def recorded_run(tmp_path_factory):
    """Export every fault type at rate one so small tests need no lucky draw."""
    root = tmp_path_factory.mktemp("recording-m3") / "run"
    config = _config(customer_tier_error_rate=1, customer_dob_error_rate=1, company_rating_error_rate=1,
                     trade_charge_error_rate=1, trade_commission_error_rate=1)
    run(config, root)
    return root, config


def _audit(root):
    """Read test-sized fault evidence without involving source ingestion."""
    return [json.loads(line) for line in (root / "summary/data_errors.jsonl").read_text().splitlines()]


def test_all_faults_validate_with_correct_settlements(recorded_run):
    """XML, FINWIRE, daily customers, and Trade retain faults beside correct replay."""
    root, config = recorded_run
    audit = _audit(root)
    assert {entry["source"] for entry in audit} == {"CustomerMgmt", "Customer", "FINWIRE:CMP", "Trade"}
    assert {entry["field"] for entry in audit} == {"C_TIER", "C_DOB", "SPrating", "T_CHRG", "T_COMM"}
    validation = validate(root, config)
    assert validation["verified_source_errors"] == len(audit)
    for measured in validation["source_recording"]["errors"].values():
        assert measured["errors"] == measured["eligible"] > 0
        assert measured["rate"] == measured["configured_rate"] == 1
    assert validation["source_recording"]["trade_schemas"]["daily_v1"] > 0
    assert validation["source_recording"]["trade_schemas"]["daily_v2"] > 0
    initial = root / "offline/initial/Trade.txt"
    completed = next(line.split("|") for line in initial.read_text().splitlines() if line.split("|")[2] == "CMPT")
    assert len(completed) == 14
    assert Decimal(completed[11]) > Decimal(completed[10]) * int(completed[6])
    for path in (root / "offline/daily").glob("*/Trade_*.txt"):
        expected = 17 if path.parent.name >= "20190701" else 16
        assert all(len(line.split("|")) == expected for line in path.read_text().splitlines())
    manifest = json.loads((root / "manifest.json").read_text())
    assert not any(item["path"].startswith("summary/") for item in manifest["files"])
    assert "duckdb" not in manifest["runtime"] and "pyarrow" not in manifest["runtime"]
    assert "definitions/given_names.csv" in manifest["implementation_files"]


@pytest.mark.parametrize("case", ["missing_audit", "extra_audit", "wrong_correct_fee", "changed_fault", "changed_settlement", "changed_channel"])
def test_audit_does_not_authorize_arbitrary_corruption(recorded_run, tmp_path, case):
    """Remove saved hashes to prove business and fault checks reject corruption."""
    original, config = recorded_run
    root = tmp_path / "corrupt"
    shutil.copytree(original, root)
    (root / "manifest.json").unlink()
    entries = _audit(root)
    if case == "missing_audit":
        entries.pop()
    elif case == "extra_audit":
        entry = dict(entries[0])
        entry["identity"] = {**entry["identity"], "extra": "not-a-source-identity"}
        entries.append(entry)
    elif case == "wrong_correct_fee":
        entry = next(entry for entry in entries if entry["field"] == "T_CHRG")
        entry["correct"] = "2.00"
    elif case == "changed_fault":
        path = root / "offline/initial/Trade.txt"
        rows = [line.split("|") for line in path.read_text().splitlines()]
        next(row for row in rows if row[2] == "CMPT")[11] = "123.45"
        path.write_text("".join("|".join(row) + "\n" for row in rows))
    elif case == "changed_settlement":
        path = root / "offline/initial/CashTransaction.txt"
        rows = [line.split("|") for line in path.read_text().splitlines()]
        row = next(row for row in rows if row[4].startswith("Trade settlement "))
        row[3] = str(Decimal(row[3]) + Decimal(".01"))
        path.write_text("".join("|".join(row) + "\n" for row in rows))
    else:
        path = next(path for path in (root / "offline/daily").glob("*/Trade_*.txt") if path.parent.name >= "20190701" and path.stat().st_size)
        rows = [line.split("|") for line in path.read_text().splitlines()]
        rows[0][-1] = "BROKER" if rows[0][-1] != "BROKER" else "WEB"
        path.write_text("".join("|".join(row) + "\n" for row in rows))
    (root / "summary/data_errors.jsonl").write_text("".join(json.dumps(entry) + "\n" for entry in entries))
    with pytest.raises(ValidationError):
        validate(root, config)


@pytest.mark.parametrize("quantity,price,valid", [(999999, "90.00", True), (999999, "999999.99", False)])
def test_fee_fault_requires_a_representable_value_above_the_trade(quantity, price, valid):
    """Keep large valid faults, and explain when no larger VALUE_T value exists."""
    emitted = []
    generation = SimpleNamespace(config=_config(trade_charge_error_rate=1, trade_commission_error_rate=0),
                                 emit=lambda *args: emitted.append(args), record_fault=lambda entry: None)
    fields = {**_trade(), "T_QTY": quantity, "T_TRADE_PRICE": price}
    recorder = SourceRecorder(generation)
    if valid:
        recorder.record("Trade", "2019-07-01", fields, "key")
        assert emitted[0][2][0]["T_CHRG"] == "89999910.01"
    else:
        with pytest.raises(ValueError, match="cannot fit VALUE_T"):
            recorder.record("Trade", "2019-07-01", fields, "key")


def test_default_customer_error_rates_are_independent():
    """Measure default customer fault draws on a fixed deterministic population."""
    counts = Counter()

    def count(entry):
        """Count audit fields without retaining the generated event history."""
        counts[entry["field"]] += 1

    generation = SimpleNamespace(config=_config(), emit=lambda *args: None, record_fault=count)
    recorder = SourceRecorder(generation)
    for identifier in range(1, 50001):
        fields = {"C_ID": identifier, "C_ST_ID": "ACTV", "C_TIER": 3, "C_DOB": "1980-01-01"}
        recorder.record("Customer", "2019-07-01", fields, str(identifier))
    assert 60 <= counts["C_TIER"] <= 140
    assert 25 <= counts["C_DOB"] <= 80
