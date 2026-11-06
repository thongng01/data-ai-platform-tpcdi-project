"""Verify bounded export, cash identity, source formats, and retained failure evidence."""

import hashlib
import json
from datetime import date
import shutil
import subprocess
import sys

import psutil
import pytest

from data.data_source.generator.config import Config
from data.data_source.generator.formats import Field, Source
from data.data_source.generator.generation import Generation, expected_file_list, source_path
from data.data_source.generator.runner import _temporary_disk_bytes, compare, implementation_hashes, inventory_summary, run, semantic_dataset_id, write_json


def config(**kwargs):
    """Build a short export configuration with deliberately small merge bounds."""
    values = dict(initial_start="2018-12-28", initial_end="2018-12-31", daily_start="2019-01-01",
                  daily_end="2019-01-03", securities=2, companies=1, scale_factor=.001,
                  export_rows=3, merge_fan_in=2)
    return Config(**(values | kwargs))


def test_expected_paths_have_cadence_and_no_benchmark_files():
    """Require the designed source cadence and exclude benchmark-only files."""
    settings = config(daily_end="2019-02-02")
    paths = expected_file_list(settings)
    assert "offline/initial/CustomerMgmt.xml" in paths
    assert "offline/initial/FINWIRE2017Q3" in paths
    assert "offline/daily/20190131/FINWIRE2019Q1" in paths
    assert "offline/daily/20190201/Prospect_20190201.csv" in paths
    assert "offline/daily/20190202/Prospect_20190202.csv" not in paths
    assert "stream/cash_transactions/20190202.jsonl" in paths
    assert not any("Batch" in path or "audit" in path for path in paths)
    assert not any("CashTransaction" in path for path in paths if "/daily/" in path)
    assert not any("TradeHistory" in path for path in paths if "/daily/" in path)
    assert source_path(settings, "Trade", 4) == "offline/daily/20190101/Trade_20190101.txt"


def test_temporary_disk_measurement_includes_validation_partitions(tmp_path):
    """Count validation partitions as temporary disk rather than final source data."""
    for relative, size in (("_work/runs/1.txt", 2), ("validation-one/blocks/trade.parquet", 3),
                           ("validation-two/partitioned/cash.parquet", 5), ("offline/Trade.txt", 7),
                           ("validation.json", 11)):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * size)
    assert _temporary_disk_bytes(tmp_path) == 10


def test_aggregate_buffer_and_sealed_family_bounds(tmp_path):
    """Bound pending rows and replace fragment catalogs with one sealed family file."""
    generation = Generation(config(export_rows=7), tmp_path)
    for i in range(200):
        generation.record("StatusType", 0, (f"{i:04d}", "value"))
        assert generation.output.buffer_rows <= 7
    inventory = generation.finish()
    metrics = generation.output.metrics
    assert metrics["maximum_buffer_rows"] == 7
    assert metrics["maximum_frame_rows"] <= 7
    assert metrics["maximum_stream_batch_rows"] <= 7
    assert not hasattr(generation.output, "catalog")
    assert len(list((tmp_path / "_work/source-parts/coordinator").glob("*.parquet"))) == 1
    result = next(item for item in inventory if item["path"].endswith("StatusType.txt"))
    assert result["rows"] == 200
    assert (tmp_path / result["path"]).read_text().splitlines() == [f"{i:04d}|value" for i in range(200)]


def test_native_order_and_cdc_sequence(tmp_path):
    """Sort typed fragments and retain one consecutive CDC sequence across batches."""
    generation = Generation(config(securities=101, export_rows=2), tmp_path)
    epoch_day = (date(2019, 1, 1) - date(1970, 1, 1)).days
    for i in reversed(range(1, 102)):
        generation.record("DailyMarket", 4, (epoch_day, i, 100, 200, 50, i))
    inventory = generation.finish()
    result = next(item for item in inventory if "DailyMarket_20190101" in item["path"])
    assert result["rows"] == 101
    assert (tmp_path / result["path"]).read_text().splitlines() == [
        f"I|{i}|2019-01-01|S{i:010d}|1.00|2.00|0.50|{i}" for i in range(1, 102)]
    assert generation.output.metrics["maximum_stream_batch_rows"] <= 2


@pytest.mark.parametrize("across_runs", [False, True])
def test_duplicate_complete_keys_fail(tmp_path, across_runs):
    """Reject duplicate complete keys inside a batch and across typed fragments."""
    generation = Generation(config(), tmp_path)
    generation.record("StatusType", 0, ("SAME", "first"))
    if across_runs:
        generation.output.flush()
    generation.record("StatusType", 0, ("SAME", "second"))
    with pytest.raises(ValueError, match="Nonunique"):
        generation.finish()


def test_final_writer_checks_widths(tmp_path):
    """Reject a source field that violates the final contract after typed recording."""
    generation = Generation(config(), tmp_path)
    generation.record("StatusType", 0, ("LONGER", "bad"))
    with pytest.raises(ValueError, match="Invalid source value"):
        generation.finish()


def test_raw_delimiter_and_control_character_fail(tmp_path):
    """Reject source delimiters and raw controls before final bytes can be published."""
    generation = Generation(config(), tmp_path / "delimiter")
    generation.record("StatusType", 0, ("ID", "a|b"))
    with pytest.raises(ValueError, match="Invalid source value"):
        generation.finish()
    generation = Generation(config(), tmp_path / "control")
    generation.record("CustomerMgmt", 0, ("a\nb",), order=(0, 1, 0, "NEW"))
    with pytest.raises(ValueError, match="Invalid encoded"):
        generation.finish()


def add_cash(generation):
    """Queue typed cash transactions out of order to test final global identity."""
    for day, account, ordinal, amount, name in (
        (date(2019, 1, 1), 2, 1, 200, "Deposit"),
        (date(2018, 12, 31), 1, 2, -300, "Withdrawal"),
        (date(2018, 12, 31), 1, 1, 500, "Deposit"),
        (date(2019, 1, 1), 1, 1, 100, "Dividend SYM"),
    ):
        timestamp = (day - date(1970, 1, 1)).days * 86400 + 8 * 3600
        generation.record("CashTransaction", (day - generation.config.start).days,
                          (account, timestamp, amount, name), order=(ordinal,))


def test_global_cash_sequence_crosses_cutover_and_uses_event_order(tmp_path):
    """Assign global CT_ID values across file and stream deliveries in event order."""
    generation = Generation(config(), tmp_path)
    add_cash(generation)
    inventory = generation.finish()
    initial = (tmp_path / "offline/initial/CashTransaction.txt").read_text().splitlines()
    assert initial == ["1|1|2018-12-31 08:00:00|5.00|Deposit", "2|1|2018-12-31 08:00:00|-3.00|Withdrawal"]
    stream_path = tmp_path / "stream/cash_transactions/20190101.jsonl"
    records = [json.loads(line) for line in stream_path.read_text().splitlines()]
    assert [row["ct_id"] for row in records] == [3, 4]
    assert [row["ct_ca_id"] for row in records] == [1, 2]
    assert records[0]["ct_dts"] == "2019-01-01T08:00:00Z"
    assert records[0]["ct_amt"] == "1.00"
    assert list(records[0]) == ["ct_id", "ct_ca_id", "ct_dts", "ct_amt", "ct_name", "schema_version"]
    assert (tmp_path / "stream/cash_transactions/20190102.jsonl").read_bytes() == b""
    assert {row["path"] for row in inventory} == set(expected_file_list(config()))
    for row in inventory:
        data = (tmp_path / row["path"]).read_bytes()
        assert row["bytes"] == len(data)
        assert row["sha256"] == hashlib.sha256(data).hexdigest()
    summary = inventory_summary(inventory)
    assert summary["deliveries"]["stream"]["rows"] == 2
    assert summary["families"]["CashTransaction"]["initial"]["rows"] == 2


@pytest.mark.parametrize("name", ["deposit", "Trade 1", "Dividend", "Trade settlement -1"])
def test_cash_name_is_fixed(tmp_path, name):
    """Reject descriptions outside the shared cash transaction naming contract."""
    generation = Generation(config(), tmp_path)
    timestamp = (date(2019, 1, 1) - date(1970, 1, 1)).days * 86400 + 8 * 3600
    generation.record("CashTransaction", 4, (1, timestamp, 100, name), order=(1,))
    with pytest.raises(ValueError, match="CT_NAME"):
        generation.finish()


def test_adopted_group_runs_repeat_bytes_across_bounds(tmp_path):
    """Preserve final bytes across sealed groups and different buffer allowances."""
    first = Generation(config(), tmp_path / "first")
    add_cash(first)
    first_files = first.finish()
    settings = config(export_rows=1, merge_fan_in=3)
    second = Generation(settings, tmp_path / "second")
    group = Generation(settings, tmp_path / "second", group_id=1)
    write_json(second.root / "_work/source-parts/scheduled-groups.json", [1])
    add_cash(group)
    group.finish()
    second_files = second.finish()
    assert first_files == second_files
    for path in expected_file_list(settings):
        assert (tmp_path / "first" / path).read_bytes() == (tmp_path / "second" / path).read_bytes()


def test_existing_output_and_partial_refuse_without_changes(tmp_path):
    """Refuse occupied destinations while keeping all existing evidence unchanged."""
    destination = tmp_path / "run"
    partial = tmp_path / "run.partial"
    partial.mkdir()
    (partial / "evidence").write_text("keep")
    with pytest.raises(FileExistsError):
        run(config(), destination)
    assert (partial / "evidence").read_text() == "keep"
    partial.rename(destination)
    with pytest.raises(FileExistsError):
        run(config(), destination)


def test_failed_worker_retains_evidence_without_final_rename(tmp_path, monkeypatch):
    """Keep worker diagnostics in the partial directory after a failed run."""
    from data.data_source.generator import runner
    real_popen = subprocess.Popen

    def failed_worker(*args, **kwargs):
        """Launch a worker that fails immediately to test retained diagnostics."""
        return real_popen([sys.executable, "-c", "raise RuntimeError('worker evidence')"], **kwargs)

    monkeypatch.setattr(runner.subprocess, "Popen", failed_worker)
    destination = tmp_path / "failed"
    with pytest.raises(RuntimeError, match="Retained files"):
        run(config(), destination)
    assert not destination.exists()
    partial = tmp_path / "failed.partial"
    assert "worker evidence" in (partial / "worker.log").read_text()
    assert "status" in json.loads((partial / "failure.json").read_text())["reason"]
    assert (partial / "config.json").is_file()


def test_supervisor_stops_nested_workers_and_retains_budget_failure(tmp_path, monkeypatch):
    """Stop an excessive process tree and save its resource-limit failure."""
    from data.data_source.generator import runner
    real_popen = subprocess.Popen
    pid_file = tmp_path / "child.pid"
    script = ("import subprocess,sys,time; from pathlib import Path; "
              "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); "
              f"Path({str(pid_file)!r}).write_text(str(child.pid)); time.sleep(60)")

    def nested_worker(*args, **kwargs):
        """Launch an extra descendant to exceed the configured process bound."""
        return real_popen([sys.executable, "-c", script], **kwargs)

    monkeypatch.setattr(runner.subprocess, "Popen", nested_worker)
    with pytest.raises(RuntimeError, match="configured worker count"):
        run(config(workers=1), tmp_path / "budget")
    failure = json.loads((tmp_path / "budget.partial/failure.json").read_text())
    assert failure["maximum_worker_processes"] == 2
    child = int(pid_file.read_text())
    assert not psutil.pid_exists(child) or psutil.Process(child).status() == psutil.STATUS_ZOMBIE
    assert not (tmp_path / "budget").exists()


def test_timeout_retains_partial_without_completed_manifest(tmp_path, monkeypatch):
    """Retain partial evidence when the worker exceeds its time budget."""
    from data.data_source.generator import runner
    real_popen = subprocess.Popen

    def slow_worker(*args, **kwargs):
        """Launch a sleeping process that exceeds the short test timeout."""
        return real_popen([sys.executable, "-c", "import time; time.sleep(60)"], **kwargs)

    monkeypatch.setattr(runner.subprocess, "Popen", slow_worker)
    with pytest.raises(RuntimeError, match="time budget"):
        run(config(timeout_seconds=1), tmp_path / "timeout")
    assert (tmp_path / "timeout.partial/failure.json").exists()
    assert not (tmp_path / "timeout").exists()


def test_compare_rehashes_actual_files(tmp_path):
    """Detect source changes even when both saved manifests claim identical bytes."""
    inventory = []
    for name in ("first", "second"):
        root = tmp_path / name
        generation = Generation(config(), root)
        add_cash(generation)
        inventory = generation.finish()
        write_json(root / "manifest.json", {"status": "validated", "dataset_id": "same", "files": inventory})
    assert compare(tmp_path / "first", tmp_path / "second")["identical_files"] == len(inventory)
    (tmp_path / "second/offline/initial/CashTransaction.txt").write_text("corrupted\n")
    with pytest.raises(ValueError, match="no longer match"):
        compare(tmp_path / "first", tmp_path / "second")


@pytest.mark.parametrize("filename", ["given_names.csv", "surnames.csv", "locations.csv"])
def test_csv_content_changes_definition_hash_and_dataset_identity(tmp_path, filename):
    """Change only an isolated list copy and require a distinct semantic dataset ID."""
    from pathlib import Path
    from data.data_source.generator.generation import FORMAT_VERSION

    package = tmp_path / "isolated-package"
    shutil.copytree(Path(__file__).parents[1] / "definitions", package / "definitions")
    before = implementation_hashes(package)
    before_id = semantic_dataset_id(config(), FORMAT_VERSION, before, "fixed-calendar")
    path = package / "definitions" / filename
    lines = path.read_text().splitlines()
    fields = lines[1].split(",")
    fields[0] = "Testvalue" if fields[0] != "Testvalue" else "Changedvalue"
    lines[1] = ",".join(fields)
    path.write_text("\n".join(lines) + "\n")
    after = implementation_hashes(package)
    after_id = semantic_dataset_id(config(), FORMAT_VERSION, after, "fixed-calendar")
    changed = {name for name in before if before[name] != after[name]}
    assert changed == {f"definitions/{filename}"}
    assert before_id != after_id
    for directory, identifier in (("before", before_id), ("after", after_id)):
        write_json(tmp_path / directory / "manifest.json", {"status": "validated", "dataset_id": identifier, "files": []})
    with pytest.raises(ValueError, match="semantic configuration"):
        compare(tmp_path / "before", tmp_path / "after")


TPCDI_FIXTURE_PATHS = frozenset({
    "Batch1/BatchDate.txt", "Batch1/CashTransaction.txt", "Batch1/DailyMarket.txt",
    "Batch1/HoldingHistory.txt", "Batch1/Industry.txt", "Batch1/StatusType.txt",
    "Batch1/TaxRate.txt", "Batch1/Trade.txt", "Batch1/TradeHistory.txt",
    "Batch1/TradeType.txt", "Batch1/WatchHistory.txt",
    "Batch2/CashTransaction.txt", "Batch2/Trade.txt",
})


def _check_tpcdi_format_fixtures(root):
    """Require the independent fixture inventory before checking pinned source rows."""
    from pathlib import Path
    from data.data_source.generator.formats import load_sources

    manifest = json.loads((root / "manifest.json").read_text())
    assert set(manifest["files"]) == TPCDI_FIXTURE_PATHS, "TPC-DI fixture inventory must contain all 13 expected paths."
    sources = load_sources()
    for relative, metadata in manifest["files"].items():
        assert type(metadata["lines"]) is int and metadata["lines"] > 0, "TPC-DI fixtures require a positive integer line count."
        raw = (root / "tpcdi_sf3" / relative).read_bytes()
        assert hashlib.sha256(raw).hexdigest().startswith(metadata["sha256"])
        lines = raw.decode("ascii").splitlines()
        assert lines and all(line.strip() for line in lines), "TPC-DI fixtures require nonempty source records."
        assert len(lines) == metadata["lines"]
        source_name = Path(relative).stem
        if source_name == "BatchDate":
            continue
        source = sources[source_name]
        for line in lines:
            fields = line.split(metadata["delimiter"])
            assert len(fields) == metadata["fields"]
            if relative.startswith("Batch2/"):
                fields = fields[2:]
            if source_name == "CashTransaction":
                fields = ["1", *fields]
            source.check(fields)


def test_tpcdi_format_fixtures_keep_the_original_business_fields():
    """Check all pinned official sample rows against the original source contracts."""
    from pathlib import Path

    _check_tpcdi_format_fixtures(Path(__file__).parent / "fixtures")


@pytest.mark.parametrize("case", ["empty", "missing_path"])
def test_tpcdi_fixture_inventory_rejects_empty_or_incomplete_manifest(tmp_path, case):
    """Reject missing manifest entries before attempting to read any fixture rows."""
    from pathlib import Path

    manifest = json.loads((Path(__file__).parent / "fixtures/manifest.json").read_text())
    if case == "empty":
        manifest["files"] = {}
    else:
        del manifest["files"]["Batch1/Trade.txt"]
    write_json(tmp_path / "manifest.json", manifest)
    # No fixture files exist here. The inventory check must fail before the row
    # loop can either skip an empty manifest or try to open a partial fixture.
    with pytest.raises(AssertionError, match="fixture inventory"):
        _check_tpcdi_format_fixtures(tmp_path)


@pytest.mark.parametrize("metadata_lines", [0, -1, True, "1", 1.0, 1])
def test_tpcdi_fixture_inventory_rejects_empty_payloads(tmp_path, metadata_lines):
    """Reject all 13 empty fixture files even when their saved hashes match."""
    files = {}
    for relative in sorted(TPCDI_FIXTURE_PATHS):
        path = tmp_path / "tpcdi_sf3" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"")
        files[relative] = {"delimiter": "|", "fields": 1, "lines": metadata_lines,
                           "sha256": hashlib.sha256(b"").hexdigest()}
    write_json(tmp_path / "manifest.json", {"files": files})
    message = "nonempty source records" if type(metadata_lines) is int and metadata_lines > 0 else "positive integer line count"
    with pytest.raises(AssertionError, match=message):
        _check_tpcdi_format_fixtures(tmp_path)
