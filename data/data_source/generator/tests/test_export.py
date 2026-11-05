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
from data.data_source.generator.sorting import SortedOutput


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


def test_aggregate_buffer_and_disk_catalog_bound_paths(tmp_path):
    """Keep the shared row buffer and retained Python path lists within their bounds."""
    output = SortedOutput(tmp_path, fan_in=3, export_rows=7)
    for i in range(200):
        output.add(f"offline/{i}.txt", [{"_key": "1", "_payload": "value"}])
        assert len(output.buffer) <= 7
    output.flush()
    assert output.maximum_buffer_rows == 7
    assert output.maximum_frame_rows == 7
    assert not hasattr(output, "runs")
    assert output.catalog.execute("SELECT COUNT(DISTINCT relative) FROM runs").fetchone()[0] == 200
    assert output.maximum_retained_run_paths <= 3
    result = output.write("offline/12.txt")
    assert result["rows"] == 1
    assert (tmp_path / result["path"]).read_bytes() == b"value\n"
    output.close()


def test_multilevel_merge_and_cdc_sequence(tmp_path):
    """Merge many runs and assign one consecutive CDC sequence at final write."""
    output = SortedOutput(tmp_path, 2, 2)
    for i in reversed(range(101)):
        output.add("offline/daily/test.txt", [{"_key": f"{i:05d}", "_payload": f"I|{i}"}])
    result = output.write("offline/daily/test.txt", cdc=True)
    assert result["rows"] == 101
    assert (tmp_path / result["path"]).read_text().splitlines() == [f"I|{i + 1}|{i}" for i in range(101)]
    assert output.maximum_merge_inputs <= 2
    assert output.maximum_retained_run_paths <= 2
    assert output.catalog.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0
    assert not list((tmp_path / "_work/runs").glob("*.txt"))
    output.close()


@pytest.mark.parametrize("across_runs", [False, True])
def test_duplicate_complete_keys_fail(tmp_path, across_runs):
    """Reject duplicate complete keys both inside a buffer and across runs."""
    output = SortedOutput(tmp_path, 2, 3)
    output.add("offline/file.txt", [{"_key": "same", "_payload": "first"}])
    if across_runs:
        output.flush()
    with pytest.raises(ValueError, match="[Dd]uplicate|Nonunique"):
        output.add("offline/file.txt", [{"_key": "same", "_payload": "second"}])
        output.write("offline/file.txt")


def test_final_writer_checks_widths_even_for_raw_text(tmp_path):
    """Reject raw text that bypasses early formatting but violates the final contract."""
    generation = Generation(config(), tmp_path)
    generation.text("offline/initial/StatusType.txt", [("1", "LONGER|bad")])
    with pytest.raises(ValueError, match="exceeds"):
        generation.finish()


def test_raw_delimiter_and_control_character_fail(tmp_path):
    """Reject delimiters and control separators before they corrupt the spool format."""
    output = SortedOutput(tmp_path, 2, 2)
    source = Source("Example", "example.txt", "|", (Field("value", "CHAR(20)"),))
    with pytest.raises(ValueError, match="Delimiter"):
        output.add("offline/example.txt", [{"_key": "1", "value": "a|b"}], source)
    with pytest.raises(ValueError, match="separator"):
        output.add("offline/example.txt", [{"_key": "1", "_payload": "a\nb"}])
    output.close()


def add_cash(generation):
    """Queue cash events in a different order from their required final identity order."""
    generation.cash(date(2019, 1, 1), "2019-01-01 08:00:00", 2, 1, 200, "Deposit")
    generation.cash(date(2018, 12, 31), "2018-12-31 08:00:00", 1, 2, -300, "Withdrawal")
    generation.cash(date(2018, 12, 31), "2018-12-31 08:00:00", 1, 1, 500, "Deposit")
    generation.cash(date(2019, 1, 1), "2019-01-01 08:00:00", 1, 1, 100, "Dividend SYM")


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
    with pytest.raises(ValueError, match="CT_NAME"):
        generation.cash(date(2019, 1, 1), "2019-01-01 08:00:00", 1, 1, 100, name)
    generation.close()


def test_adopted_group_runs_repeat_bytes_across_bounds(tmp_path):
    """Preserve final bytes when a coordinator adopts differently buffered group runs."""
    first = Generation(config(), tmp_path / "first")
    add_cash(first)
    first_files = first.finish()
    settings = config(export_rows=1, merge_fan_in=3)
    group = Generation(settings, tmp_path / "group")
    add_cash(group)
    group.close()
    second = Generation(settings, tmp_path / "second")
    second.adopt(tmp_path / "group")
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
        run(config(), tmp_path / "budget")
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


def test_tpcdi_format_fixtures_keep_the_original_business_fields():
    """Check pinned official sample rows against the original source contracts."""
    from pathlib import Path
    from data.data_source.generator.formats import load_sources

    root = Path(__file__).parent / "fixtures"
    manifest = json.loads((root / "manifest.json").read_text())
    sources = load_sources()
    for relative, metadata in manifest["files"].items():
        raw = (root / "tpcdi_sf3" / relative).read_bytes()
        assert hashlib.sha256(raw).hexdigest().startswith(metadata["sha256"])
        lines = raw.decode("ascii").splitlines()
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
