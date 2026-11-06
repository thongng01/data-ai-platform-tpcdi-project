"""Prove source-only faults, dated channels, and independent fault verification."""

from collections import Counter
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
import json
import shutil
from types import SimpleNamespace

import polars as pl
import pytest

from data.data_source.generator.config import Config
from data.data_source.generator.formats import load_sources, source_for_day
from data.data_source.generator.recording import SourceRecorder, _selected
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
    """Describe one completed buy in integer-cent and epoch-second storage units."""
    seconds = (datetime(2019, 7, 1, 10) - datetime(1970, 1, 1)).days * 86400 + 10 * 3600
    return {"T_ID": identifier, "T_DTS": seconds, "T_ST_ID": "CMPT", "T_TT_ID": "TMB",
            "T_IS_CASH": 1, "T_S_SYMB": 1, "T_QTY": 10, "T_BID_PRICE": 2000,
            "T_CA_ID": 1, "T_EXEC_NAME": "Sam Brown", "T_TRADE_PRICE": 2000, "T_CHRG": 100,
            "T_COMM": 20, "T_TAX": 0}


def _frame(config, rows, *, profile=1):
    """Add the batch recorder metadata without changing source-field types."""
    return pl.DataFrame(rows).with_columns(
        pl.lit((date(2019, 7, 1) - config.start).days, dtype=pl.Int64).alias("_record_day"),
        pl.lit(profile, dtype=pl.Int64).alias("_profile"),
    )


@pytest.mark.parametrize("charge,commission", [(1, 0), (0, 1), (1, 1)])
def test_fee_faults_are_separate_and_do_not_change_correct_fields(charge, commission):
    """Each fee draw changes only its selected source field, not the input."""
    config = _config(trade_charge_error_rate=charge, trade_commission_error_rate=commission)
    fields = _frame(config, [_trade()], profile=2)
    recorded, audit = SourceRecorder(SimpleNamespace(config=config)).apply("Trade", fields)
    assert fields["T_CHRG"].item() == 100 and fields["T_COMM"].item() == 20
    assert recorded["T_CHRG"].item() == (20001 if charge else 100)
    assert recorded["T_COMM"].item() == (20001 if commission else 20)
    assert len(audit) == charge + commission
    assert all(entry["recorded"] == "200.01" for entry in audit)
    expected_correct = ({"1.00"} if charge else set()) | ({"0.20"} if commission else set())
    assert {entry["correct"] for entry in audit} == expected_correct
    assert "profile" not in recorded.columns


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
    config = _config(trade_charge_error_rate=0, trade_commission_error_rate=0, export_rows=1024)
    broker_counts = []
    for profile in (2, 4):
        fields = _frame(config, [_trade(identifier) for identifier in range(1, 1001)], profile=profile)
        recorded, audit = SourceRecorder(SimpleNamespace(config=config)).apply("Trade", fields)
        assert not audit
        broker_counts.append(recorded["T_CHANNEL"].to_list().count("BROKER"))
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
    parallel = validate(root, replace(config, workers=2))
    # File stages have separate buffers, so their measured peaks can differ.
    assert {key: value for key, value in parallel.items() if key != "maximum_parse_buffer_rows"} == {
        key: value for key, value in validation.items() if key != "maximum_parse_buffer_rows"}
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


@pytest.mark.parametrize("quantity,price,valid", [(999999, 9000, True), (999999, 99999999, False)])
def test_fee_fault_requires_a_representable_value_above_the_trade(quantity, price, valid):
    """Keep large valid faults, and explain when no larger VALUE_T value exists."""
    config = _config(trade_charge_error_rate=1, trade_commission_error_rate=0)
    fields = _frame(config, [{**_trade(), "T_QTY": quantity, "T_TRADE_PRICE": price}])
    recorder = SourceRecorder(SimpleNamespace(config=config))
    if valid:
        recorded, audit = recorder.apply("Trade", fields)
        assert recorded["T_CHRG"].item() == 8999991001
        assert audit[0]["recorded"] == "89999910.01"
    else:
        with pytest.raises(ValueError, match="cannot fit VALUE_T"):
            recorder.apply("Trade", fields)


def test_default_customer_error_rates_are_independent():
    """Measure default customer fault draws on a fixed deterministic population."""
    counts = Counter()

    config = _config()
    recorder = SourceRecorder(SimpleNamespace(config=config))
    birth_day = (date(1980, 1, 1) - date(1970, 1, 1)).days
    for first in range(1, 50001, config.export_rows):
        fields = pl.DataFrame({"C_ID": range(first, min(first + config.export_rows, 50001))}).with_columns(
            pl.lit("ACTV").alias("C_ST_ID"), pl.lit("U").alias("_flag"),
            pl.lit(3, dtype=pl.Int64).alias("C_TIER"),
            pl.lit(birth_day, dtype=pl.Int64).alias("C_DOB"),
            pl.lit((date(2019, 7, 1) - config.start).days, dtype=pl.Int64).alias("_record_day"))
        recorded, audit = recorder.apply("Customer", fields)
        counts.update(entry["field"] for entry in audit)
        assert fields["C_TIER"].unique().to_list() == [3]
        assert recorded.height == fields.height
    assert 60 <= counts["C_TIER"] <= 140
    assert 25 <= counts["C_DOB"] <= 80


def test_mixed_trade_batch_matches_scalar_faults_and_keeps_input_immutable():
    """Batch selection preserves dated keys, exact audit text, and nullable executions."""
    config = _config(trade_charge_error_rate=.35, trade_commission_error_rate=.45)
    offsets = [0, 26, 27, 30] * 12
    rows = []
    expected_audit = []
    scalar_recorder = SourceRecorder(SimpleNamespace(config=config, _record_fault=expected_audit.append))
    expected_values = []
    for index, offset in enumerate(offsets):
        fields = _trade(index + 1)
        stamp = datetime.combine(config.start + timedelta(days=offset), datetime.min.time()) + timedelta(hours=10)
        fields["T_DTS"] = (stamp - datetime(1970, 1, 1)).days * 86400 + 10 * 3600
        if index % 3 == 0:
            fields.update(T_ST_ID="PNDG", T_TRADE_PRICE=None, T_CHRG=None, T_COMM=None, T_TAX=None)
        rows.append(fields)
        legacy = {**fields, "T_DTS": stamp.isoformat(sep=" "),
                  "T_TRADE_PRICE": "20.00" if fields["T_ST_ID"] == "CMPT" else "",
                  "T_CHRG": "1.00" if fields["T_ST_ID"] == "CMPT" else "",
                  "T_COMM": "0.20" if fields["T_ST_ID"] == "CMPT" else ""}
        result = scalar_recorder._recorded("Trade", config.day(offset), legacy)
        expected_values.append(tuple(int(Decimal(result[field]) * 100) if result[field] else None
                                     for field in ("T_CHRG", "T_COMM")))
    correct = _frame(config, rows).with_columns(pl.Series("_record_day", offsets, dtype=pl.Int64))
    before = correct.clone()
    recorded, audit = SourceRecorder(SimpleNamespace(config=config)).apply("Trade", correct)
    assert correct.equals(before)
    assert list(recorded.select("T_CHRG", "T_COMM").iter_rows()) == expected_values
    assert sorted(map(lambda entry: json.dumps(entry, sort_keys=True), audit)) == sorted(
        map(lambda entry: json.dumps(entry, sort_keys=True), expected_audit))
    assert recorded.filter(pl.col("_record_day") <= 26)["T_CHANNEL"].null_count() == 24
    assert recorded.filter(pl.col("_record_day") == 27)["T_CHANNEL"].null_count() == 12
    assert recorded.filter(pl.col("_record_day") == 30)["T_CHANNEL"].null_count() == 0


def test_mixed_customer_batch_preserves_scalar_selection_and_date_units():
    """Customer faults use each row's date and status, including independent fields."""
    config = _config(customer_tier_error_rate=.4, customer_dob_error_rate=.5)
    birth_day = (date(1980, 1, 1) - date(1970, 1, 1)).days
    correct = pl.DataFrame({"C_ID": range(1, 81), "C_ST_ID": ["ACTV", "INAC"] * 40,
                            "C_TIER": [3] * 80, "C_DOB": [birth_day] * 80,
                            "_record_day": [27, 30] * 40, "_flag": ["I", "U"] * 40})
    recorded, audit = SourceRecorder(SimpleNamespace(config=config)).apply("Customer", correct)
    expected = set()
    for row in correct.iter_rows(named=True):
        day = config.day(row["_record_day"])
        identity = {"C_ID": str(row["C_ID"]), "C_ST_ID": row["C_ST_ID"]}
        for field in ("C_TIER", "C_DOB"):
            if _selected(config, "Customer", day, identity, field):
                expected.add((row["C_ID"], field))
    assert {(int(entry["identity"]["C_ID"]), entry["field"]) for entry in audit} == expected
    assert {entry["correct"] for entry in audit if entry["field"] == "C_DOB"} == {"1980-01-01"}
    for entry in audit:
        assert entry["recorded"] == ("9" if entry["field"] == "C_TIER" else
                                      (date.fromisoformat(entry["day"]) + timedelta(days=1)).isoformat())
    assert correct["C_DOB"].unique().to_list() == [birth_day]
    assert recorded.height == correct.height


def test_fee_fault_checks_wide_product_before_narrowing():
    """An oversized notional fails clearly instead of wrapping a signed integer."""
    config = _config(trade_charge_error_rate=1, trade_commission_error_rate=0)
    fields = _frame(config, [{**_trade(), "T_QTY": 4, "T_TRADE_PRICE": 2**62}])
    with pytest.raises(ValueError, match="cannot fit VALUE_T"):
        SourceRecorder(SimpleNamespace(config=config)).apply("Trade", fields)


@pytest.mark.parametrize("offset,raises", [(0, False), (27, False), (30, True)])
def test_channel_profile_is_checked_only_for_the_dated_channel_contract(offset, raises):
    """An unused hidden profile does not change initial or version-one source behavior."""
    config = _config(trade_charge_error_rate=0, trade_commission_error_rate=0)
    fields = _frame(config, [_trade()], profile=99).with_columns(pl.lit(offset).alias("_record_day"))
    recorder = SourceRecorder(SimpleNamespace(config=config))
    if raises:
        with pytest.raises(ValueError, match="known behavior profile"):
            recorder.apply("Trade", fields)
    else:
        recorded, audit = recorder.apply("Trade", fields)
        assert recorded["T_CHANNEL"].item() is None
        assert not audit
