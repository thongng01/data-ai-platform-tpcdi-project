"""Exercise typed export contracts, ownership, native keys, and failure cleanup."""

import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
import threading
from xml.etree import ElementTree as ET

import numpy as np
import polars as pl
import pytest

from data.data_source.generator.formats import Field, Source
from data.data_source.generator.generation import Generation
from data.data_source.generator.output import native_batches, native_environment
from data.data_source.generator.output_formats import NativeContract, field_dtype, lexical_invalid, order_key
from data.data_source.generator.tests.test_export import config


def scalar_accepts(field, value):
    """Ask the independent prepared scalar checker whether a lexical value is valid."""
    try:
        Source("Example", "example.txt", "|", (field,)).check([value])
    except ValueError:
        return False
    return True


@pytest.mark.parametrize("datatype,values", [
    ("SNUM(8,2)", ["", "0", "-0", "+0.00", "000001.20", "1.2", ".20", "1.", "1e2", "1.234", "1234567.89", "1|2", "é", "1\n"]),
    ("NUM(4)", ["0", "0000", "+1", "-1", "1.0", "12345", "１２", "1\t"]),
    ("CHAR(4)", ["", "text", "five!", "a|b", "é", "a\r", "a\x00", "a\x7f"]),
    ("BOOLEAN", ["", "0", "1", "True", "2", "01"]),
    ("DATE", ["", "2000-02-29", "1900-02-29", "0001-01-01", "9999-12-31", "20190101", "2019-1-01", "2019-01-01Z"]),
    ("DATETIME", ["", "2019-01-01 01:02:03", "2019-01-01T01:02:03", "2019-01-01 01:02:03.0", "2019-01-01 24:00:00", "0001-01-01 00:00:00"]),
])
@pytest.mark.parametrize("nullable", [False, True])
def test_native_lexical_contract_matches_scalar(datatype, values, nullable):
    """Compare representative accepted and malformed spellings without a broad campaign."""
    field = Field("value", datatype, nullable)
    actual = pl.DataFrame({"value": values}).select(lexical_invalid(field, "|")).to_series().to_list()
    assert [not failure for failure in actual] == [scalar_accepts(field, value) for value in values]


def test_native_cents_keep_sign_and_two_fractional_digits():
    """Keep the production native money format exact without float conversion."""
    source = Source("Example", "example.txt", "|", (Field("amount", "SNUM(8,2)"),))
    formatted = NativeContract(source).format(pl.DataFrame({"amount": [-1, 12345, 0]}))
    assert formatted["amount"].to_list() == ["-0.01", "123.45", "0.00"]


def test_native_contract_rejects_unsupported_numeric_scale():
    """Reject integer storage that would silently discard a declared decimal scale."""
    field = Field("amount", "SNUM(8,3)")
    source = Source("Example", "example.txt", "|", (field,))
    with pytest.raises(ValueError, match="Unsupported numeric scale 3.*amount"):
        field_dtype(field)
    with pytest.raises(ValueError, match="Unsupported numeric scale 3.*amount"):
        NativeContract(source)


def test_prepared_scalar_contract_is_reused():
    """Keep immutable metadata prepared on each source and field object."""
    field = Field("value", "SNUM(8,2)")
    source = Source("Example", "example.txt", "|", (field,))
    assert field.checker is field.checker
    assert source.checks is source.checks
    source.check(["+0001.20"])
    source.check("-1.00".split(source.separator))


@pytest.mark.parametrize("array", [np.asarray([1.5]), pl.Series([1.5])])
def test_batch_rejects_lossy_integer_conversion(tmp_path, array):
    """Do not let strict NumPy-to-Polars casts silently truncate a money value."""
    generation = Generation(config(), tmp_path)
    with pytest.raises(ValueError, match="integral"):
        generation.record_batch("DailyMarket", 0, {
            "DM_DATE": 17893, "DM_S_SYMB": [1], "DM_CLOSE": array,
            "DM_HIGH": [200], "DM_LOW": [50], "DM_VOL": [1]})


def test_borrowed_mutable_columns_are_consumed_before_return(tmp_path):
    """Keep saved values exact when the producer immediately reuses its NumPy arrays."""
    generation = Generation(config(), tmp_path)
    dates = np.asarray([(date(2018, 12, 28) - date(1970, 1, 1)).days])
    prices = np.asarray([123], dtype=np.int64)
    generation.record_batch("DailyMarket", 0, {"DM_DATE": dates, "DM_S_SYMB": [1],
        "DM_CLOSE": prices, "DM_HIGH": [200], "DM_LOW": [50], "DM_VOL": [3]})
    dates[0], prices[0] = 0, 999
    generation.finish()
    assert (tmp_path / "offline/initial/DailyMarket.txt").read_text() == "2018-12-28|S0000000001|1.23|2.00|0.50|3\n"


@pytest.mark.parametrize("schedule,marker", [([1, 1], True), ([1], False), ([2], True), (None, True)])
def test_group_discovery_requires_exact_completed_schedule(tmp_path, schedule, marker):
    """Reject duplicate, missing, unexpected, and unscheduled group namespaces."""
    coordinator = Generation(config(), tmp_path)
    group = Generation(config(), tmp_path, group_id=1)
    if marker:
        group.finish()
    if schedule is not None:
        (tmp_path / "_work/source-parts/scheduled-groups.json").write_text(json.dumps(schedule))
    with pytest.raises(ValueError, match="group|Group"):
        coordinator.finish()
    assert not (tmp_path / "manifest.json").exists()


def test_duplicate_key_across_sealed_groups_fails(tmp_path):
    """Detect a duplicate even when each independently completed part is unique."""
    coordinator = Generation(config(export_rows=1), tmp_path)
    for group_id in (1, 2):
        group = Generation(config(export_rows=1), tmp_path, group_id=group_id)
        group.record("StatusType", 0, ("SAME", "value"))
        group.finish()
    (tmp_path / "_work/source-parts/scheduled-groups.json").write_text("[1,2]")
    with pytest.raises(ValueError, match="Nonunique"):
        coordinator.finish()


def test_same_day_first_account_updates_have_deterministic_xml_order(tmp_path):
    """Order two accounts' first updates without changing their shared customer key."""
    outputs = []
    for account_ids in ((13, 12), (12, 13)):
        root = tmp_path / str(account_ids[0])
        generation = Generation(config(export_rows=1), root)
        for account_id in account_ids:
            generation.recorder.xml_action("UPDACCT", "2018-12-28T00:00:02", 7, None,
                [{"CA_ID": account_id, "CA_NAME": f"Account {account_id} v1"}], ordinal=1)
        generation.finish()
        payload = (root / "offline/initial/CustomerMgmt.xml").read_bytes()
        actions = ET.fromstring(payload)
        assert [int(action.find("Customer/Account").attrib["CA_ID"]) for action in actions] == [12, 13]
        outputs.append(payload)
    assert outputs[0] == outputs[1]


def test_duplicate_customer_account_update_still_fails(tmp_path):
    """Reject repeating one account update even when other accounts can share its time."""
    generation = Generation(config(export_rows=1), tmp_path)
    for _ in range(2):
        generation.recorder.xml_action("UPDACCT", "2018-12-28T00:00:02", 7, None,
            [{"CA_ID": 12, "CA_NAME": "Account 12 v1"}], ordinal=1)
    with pytest.raises(ValueError, match="Nonunique"):
        generation.finish()


def test_audit_order_ignores_initial_and_daily_delivery_rank(tmp_path):
    """Preserve canonical audit-key order rather than sorting error evidence by period."""
    generation = Generation(config(export_rows=1), tmp_path)
    entries = [
        {"source": "Trade", "day": "2018-12-28", "identity": {"T_ID": "2"}, "field": "T_COMM", "correct": "0.00", "recorded": "1.00"},
        {"source": "Customer", "day": "2019-01-01", "identity": {"C_ID": "1"}, "field": "C_TIER", "correct": "1", "recorded": "9"},
    ]
    for entry in entries:
        generation._record_fault(entry)
    generation.finish()
    actual = (tmp_path / "summary/data_errors.jsonl").read_text().splitlines()
    expected = [json.dumps(entry, sort_keys=True, separators=(",", ":"), ensure_ascii=True) for entry in reversed(entries)]
    assert actual == expected


def test_exact_key_padding_overflow_and_zero_based_watch_security():
    """Keep legacy ASCII spelling when ordinals exceed their minimum padding width."""
    timestamp = (date(2019, 1, 1) - date(1970, 1, 1)).days * 86400
    frame = pl.DataFrame({"W_DTS": [timestamp], "W_C_ID": [2], "W_S_SYMB": [3], "W_ACTION": ["ACTV"]})
    assert frame.select(order_key("WatchHistory")).item() == "2019-01-01 00:00:00/00000000002/0000000002/ACTV"
    frame = pl.DataFrame({"_timestamp": [timestamp], "_account": [2], "_ordinal": [10**12]})
    assert frame.select(order_key("HoldingHistory")).item().endswith("/1000000000000")


def test_mixed_deliveries_format_once_per_bounded_batch(tmp_path, monkeypatch):
    """Share native checks across files and carry only the continuing file's CDC offset."""
    generation = Generation(config(export_rows=7, daily_end="2019-01-20"), tmp_path)
    calls = []
    original = NativeContract.format

    def observe(contract, frame):
        """Count contract batches without adding production instrumentation."""
        calls.append((contract.source.name, frame.height))
        return original(contract, frame)

    monkeypatch.setattr(NativeContract, "format", observe)
    for day_offset in range(4, 24):
        epoch_day = (generation.config.start + timedelta(days=day_offset) - date(1970, 1, 1)).days
        for security in (2, 1):
            generation.record("DailyMarket", day_offset, (epoch_day, security, 123, 200, 50, 1))
    generation.finish()
    # Forty rows yield six contract batches, not twenty per-file format calls.
    assert calls == [("DailyMarket", 7)] * 5 + [("DailyMarket", 5)]
    for day in range(1, 21):
        relative = f"offline/daily/201901{day:02d}/DailyMarket_201901{day:02d}.txt"
        rows = (tmp_path / relative).read_text().splitlines()
        assert [row.split("|")[:2] for row in rows] == [["I", "1"], ["I", "2"]]
        assert [row.split("|")[3] for row in rows] == ["S0000000001", "S0000000002"]


def test_trade_contract_batches_cross_cutover_and_schema_date(tmp_path):
    """Keep initial, daily v1, and daily v2 fields exact within mixed native batches."""
    settings = config(initial_start="2019-06-27", initial_end="2019-06-28",
                      daily_start="2019-06-29", daily_end="2019-07-02", export_rows=4,
                      trade_charge_error_rate=0.0, trade_commission_error_rate=0.0)
    generation = Generation(settings, tmp_path)
    for day_offset in (0, 2, 3, 4, 5):
        timestamp = (settings.start + timedelta(days=day_offset) - date(1970, 1, 1)).days * 86400
        for ordinal in (2, 1):
            trade_id = day_offset * 10 + ordinal
            values = (trade_id, timestamp, "CMPT", "TMB", 1, 1, 2, 100, 1,
                      "Alex Broker", 100, 1, 1, 0)
            generation.record("Trade", day_offset, values, order=(1,), profile=1)
    generation.finish()
    initial = (tmp_path / "offline/initial/Trade.txt").read_text().splitlines()
    assert [row.split("|")[0] for row in initial] == ["1", "2"]
    assert all(len(row.split("|")) == 14 for row in initial)
    for stamp, width in (("20190629", 16), ("20190630", 16), ("20190701", 17), ("20190702", 17)):
        rows = (tmp_path / f"offline/daily/{stamp}/Trade_{stamp}.txt").read_text().splitlines()
        assert [row.split("|")[:2] for row in rows] == [["I", "1"], ["I", "2"]]
        assert all(len(row.split("|")) == width for row in rows)
        if width == 17:
            assert all(row.split("|")[-1] in ("WEB", "MOBILE", "BROKER") for row in rows)


def test_batch_contract_failure_precedes_delivery_writes(tmp_path):
    """Reject a later file's bad field before dispatching any file in that batch."""
    generation = Generation(config(export_rows=8), tmp_path)
    for day_offset, volume in ((4, 1), (5, 10**12)):
        epoch_day = (generation.config.start + timedelta(days=day_offset) - date(1970, 1, 1)).days
        generation.record("DailyMarket", day_offset, (epoch_day, 1, 123, 200, 50, volume))
    with pytest.raises(ValueError, match="source bounds"):
        generation.finish()
    assert not (tmp_path / "offline/daily/20190101/DailyMarket_20190101.txt").exists()
    assert not (tmp_path / "offline/daily/20190102/DailyMarket_20190102.txt").exists()


def test_native_stop_preserves_original_consumer_error():
    """Stop the synchronous native batch sink when a checked writer fails."""
    seen = []

    def fail(frame):
        """Fail in the first output batch to test query shutdown and error locality."""
        seen.append(frame.height)
        raise ValueError("original consumer failure")

    with pytest.raises(ValueError, match="original consumer failure"):
        native_batches(pl.DataFrame({"x": list(range(100))}).lazy(), 3, fail)
    assert seen == [3]


def test_native_environment_is_explicit_and_inside_outer_allowances(tmp_path):
    """Expose private engine controls only through the launcher helper."""
    settings = config(memory_limit_mb=4096, disk_limit_mb=16384, threads=2)
    environment = native_environment(settings, tmp_path)
    assert environment["POLARS_OOC_MEMORY_BUDGET_MB"] == "256"
    assert environment["POLARS_MAX_THREADS"] == "2"
    assert environment["POLARS_OOC_SPILL_DIR"].endswith("_work/polars-spill")
    assert 0 < int(environment["POLARS_OOC_DISK_BUDGET_MB"]) * 1_000_000 < settings.disk_limit_mb * 1024**2


def test_failed_seal_has_no_completion_marker(tmp_path, monkeypatch):
    """Keep fragments as failure evidence if native consolidation cannot finish."""
    generation = Generation(config(), tmp_path)
    generation.record("StatusType", 0, ("OK", "value"))
    generation.output.flush()

    def fail(*args, **kwargs):
        """Represent disk or native-sink failure before the part becomes complete."""
        raise OSError("seal failed")

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", fail)
    with pytest.raises(OSError, match="seal failed"):
        generation.finish()
    assert not (tmp_path / "_work/source-parts/coordinator/complete.json").exists()
    assert list((tmp_path / "_work/source-parts/coordinator/fragments/StatusType").glob("*.parquet"))


def run_spill_probe(root: Path) -> dict:
    """Exercise one real native backend sort under fresh-process spill settings."""
    settings = config(export_rows=4096, memory_limit_mb=4096, disk_limit_mb=2048)
    generation = Generation(settings, root)
    stopped = threading.Event()
    observed = {"spill_bytes": 0}

    def sample():
        """Observe live spill files because the native engine removes them on completion."""
        while not stopped.wait(0.002):
            size = 0
            for path in (root / "_work/polars-spill").rglob("*"):
                try:
                    if path.is_file():
                        size += path.stat().st_size
                except FileNotFoundError:
                    pass
            observed["spill_bytes"] = max(observed["spill_bytes"], size)

    sampler = threading.Thread(target=sample)
    sampler.start()
    try:
        # These are encoding/sort probe rows, not a generated business dataset.
        # Reverse input ensures the native sort cannot stream preordered rows.
        for last in range(65536, 0, -settings.export_rows):
            identifiers = np.arange(last - 1, last - settings.export_rows - 1, -1, dtype=np.int64)
            payloads = [f'<Action id="{identifier:08d}">' + "x" * 1024 + "</Action>" for identifier in identifiers]
            generation.record_batch("CustomerMgmt", 0, {"_payload": payloads},
                order={"_timestamp": 0, "_customer": 1, "_ordinal": identifiers, "_action": "NEW", "_account": 0})
        try:
            inventory = generation.finish()
            result = next(item for item in inventory if item["path"].endswith("CustomerMgmt.xml"))
            return {"ok": True, "rows": result["rows"], "spill_bytes": observed["spill_bytes"],
                    "maximum_batch_rows": generation.output.metrics["maximum_stream_batch_rows"]}
        except Exception as error:
            return {"ok": False, "error": str(error), "spill_bytes": observed["spill_bytes"],
                    "final_manifest": (root / "manifest.json").exists(),
                    "retained_part": (root / "_work/source-parts/coordinator/CustomerMgmt.parquet").is_file(),
                    "source_file": (root / "offline/initial/CustomerMgmt.xml").exists()}
    finally:
        stopped.set()
        sampler.join()


@pytest.mark.parametrize("disk_mb", [128, 1])
def test_actual_native_spill_and_disk_budget_failure(tmp_path, disk_mb):
    """Prove actual spill and fail safely when that spill exceeds its disk allowance."""
    environment = os.environ | native_environment(config(memory_limit_mb=4096), tmp_path)
    environment.update(POLARS_OOC_MEMORY_BUDGET_MB="1", POLARS_OOC_DISK_BUDGET_MB=str(disk_mb),
                       OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1")
    command = [sys.executable, "-c",
        "import json,sys; from pathlib import Path; "
        "from data.data_source.generator.tests.test_output_native import run_spill_probe; "
        "print(json.dumps(run_spill_probe(Path(sys.argv[1]))))", str(tmp_path)]
    completed = subprocess.run(command, env=environment, text=True, capture_output=True, timeout=60)
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout.splitlines()[-1])
    (tmp_path / "probe-result.json").write_text(json.dumps(result, sort_keys=True) + "\n")
    if disk_mb == 128:
        assert result["ok"], result
        assert result["rows"] == 65536
        assert result["maximum_batch_rows"] <= 4096
        assert result["spill_bytes"] > 0
    else:
        assert not result["ok"], result
        assert "POLARS_OOC_DISK_BUDGET_MB" in result["error"], result
        assert not result["final_manifest"] and not result["source_file"]
        assert result["retained_part"]
