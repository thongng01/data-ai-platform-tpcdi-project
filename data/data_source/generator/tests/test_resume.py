"""Prove matching checkpoint reuse, retry safety, and explicit validation states."""

from dataclasses import replace
import json
from pathlib import Path
import sys

import polars as pl
import pytest

from data.data_source.generator import __main__, activity, runner
from data.data_source.generator.checkpoints import Checkpoints
from data.data_source.generator.config import Config
from data.data_source.generator.output import TypedOutput


@pytest.fixture(scope="module")
def config():
    """Use several customer groups and cross the daily Trade contract boundary."""
    return Config(initial_start="2019-06-24", initial_end="2019-06-30",
                  daily_start="2019-07-01", daily_end="2019-07-08",
                  scale_factor=.004, securities=8, companies=5, workers=1,
                  customer_group_size=5, export_rows=257, threads=1,
                  annual_trade_rates=(40, 120, 60, 300, 750))


@pytest.fixture(scope="module")
def uninterrupted(tmp_path_factory, config):
    """Generate the independent uninterrupted counterpart once for retry tests."""
    root = tmp_path_factory.mktemp("resume-baseline")
    runner.worker(config, root)
    return root


def assert_same_business(first, second):
    """Compare exact source bytes, audit, typed hidden records, and business reports."""
    assert runner.compare(first, second)["identical_files"] > 0
    for path in (first / "offline").rglob("*"):
        if path.is_file():
            assert path.read_bytes() == (second / path.relative_to(first)).read_bytes()
    assert (first / "summary/data_errors.jsonl").read_bytes() == (second / "summary/data_errors.jsonl").read_bytes()
    assert {path.name for path in (first / "summary/hidden").glob("*.parquet")} == {
        path.name for path in (second / "summary/hidden").glob("*.parquet")}
    for path in (first / "summary/hidden").glob("*.parquet"):
        assert pl.read_parquet(path).equals(pl.read_parquet(second / path.relative_to(first)))
    summaries = [json.loads((root / "summary.json").read_text()) for root in (first, second)]
    for key in ("activity", "prospects", "files", "data_errors", "trade_recording", "calendar", "status"):
        assert summaries[0][key] == summaries[1][key], key


def test_completed_groups_survive_failure_and_resume(tmp_path, config, uninterrupted, monkeypatch):
    """Inject a test-only failure after a group seals, then reuse that exact part."""
    partial = tmp_path / "retry.partial"
    original = activity._run_group
    calls = []

    def interrupt(*args):
        """Stop before the second group without changing production configuration."""
        if calls:
            raise RuntimeError("Injected group interruption")
        calls.append(args[2])
        return original(*args)

    monkeypatch.setattr(activity, "_run_group", interrupt)
    with pytest.raises(RuntimeError, match="Injected group"):
        runner.worker(config, partial)
    marker = partial / "_work/source-parts/group-00000000001/complete.json"
    sealed_bytes = marker.read_bytes()
    sealed_time = marker.stat().st_mtime_ns
    receipt = json.loads(sealed_bytes)
    assert receipt["context_sha256"] and receipt["result"]

    def remaining(*args):
        """Reject any attempt to simulate the already completed first group."""
        assert args[2] != 1
        return original(*args)

    monkeypatch.setattr(activity, "_run_group", remaining)
    runner.worker(config, partial)
    assert marker.read_bytes() == sealed_bytes and marker.stat().st_mtime_ns == sealed_time
    assert_same_business(uninterrupted, partial)


def test_unsealed_mutated_group_restarts_from_planned_population(tmp_path, config, uninterrupted, monkeypatch):
    """Retry after actual lifetimes flush but before the group's last marker commits."""
    root = tmp_path / "unsealed"
    original = TypedOutput.seal
    failed = False

    def interrupt(output, **kwargs):
        """Leave the first group's hidden files and mutable arrays without a seal."""
        nonlocal failed
        if output.group_id == 1 and not failed:
            failed = True
            raise RuntimeError("Injected pre-seal failure")
        return original(output, **kwargs)

    monkeypatch.setattr(TypedOutput, "seal", interrupt)
    with pytest.raises(RuntimeError, match="pre-seal"):
        runner.worker(config, root)
    runner.worker(config, root)
    assert_same_business(uninterrupted, root)


def test_partial_export_is_removed_before_retry(tmp_path, config, uninterrupted, monkeypatch):
    """Redo export as one step without recreating completed business parts."""
    root = tmp_path / "export"
    original = TypedOutput.write_family
    failed = False

    def interrupt(output, family, *args):
        """Leave one complete output family before forcing export to stop."""
        nonlocal failed
        result = original(output, family, *args)
        if family != "__audit" and not failed:
            failed = True
            raise RuntimeError("Injected export failure")
        return result

    monkeypatch.setattr(TypedOutput, "write_family", interrupt)
    with pytest.raises(RuntimeError, match="export failure"):
        runner.worker(config, root)
    assert list((root / "offline").rglob("*.txt"))
    marker = root / "_work/source-parts/coordinator/complete.json"
    sealed_time = marker.stat().st_mtime_ns
    runner.worker(config, root)
    assert marker.stat().st_mtime_ns == sealed_time
    assert_same_business(uninterrupted, root)


def test_validation_failure_resumes_without_generation(tmp_path, config, uninterrupted, monkeypatch):
    """Retain completed export and run only validation after its earlier failure."""
    root = tmp_path / "validation"
    original = runner.validate_saved

    def fail(*args):
        """Simulate an independent validator failure after export completes."""
        raise ValueError("Injected validation failure")

    monkeypatch.setattr(runner, "validate_saved", fail)
    with pytest.raises(ValueError, match="validation failure"):
        runner.worker(config, root)
    assert json.loads((root / "manifest.json").read_text())["status"] == "generated-unvalidated"

    def no_generation(*args, **kwargs):
        """Fail if validation-only resume tries to run a business group."""
        pytest.fail("Resume regenerated a completed export")

    monkeypatch.setattr(activity, "_run_group", no_generation)
    monkeypatch.setattr(runner, "validate_saved", original)
    runner.worker(config, root)
    assert_same_business(uninterrupted, root)


@pytest.mark.parametrize("change", ["code", "semantic", "group_size", "export_rows"])
def test_resume_refuses_changed_context_before_writing(tmp_path, config, monkeypatch, change):
    """Reject changed executable inputs or output settings before starting a child."""
    root = tmp_path / "refuse.partial"
    hashes = runner.implementation_hashes(Path(runner.__file__).parent)
    Checkpoints(root, config, hashes, runner.RUNTIME_KEYS)
    before = (root / "_checkpoints/context.json").read_bytes()
    if change == "code":
        monkeypatch.setattr(runner, "implementation_hashes", lambda package: {**hashes, "activity.py": "changed"})
    elif change == "semantic":
        config = replace(config, seed=config.seed + 1)
    elif change == "group_size":
        config = replace(config, customer_group_size=config.customer_group_size + 1)
    else:
        config = replace(config, export_rows=config.export_rows + 1)
    with pytest.raises(ValueError, match="Cannot resume"):
        runner.run(config, resume=root)
    assert (root / "_checkpoints/context.json").read_bytes() == before
    assert not (root / "config.json").exists()


def test_skip_exports_final_name_and_validate_promotes_saved_bytes(tmp_path, config, uninterrupted, monkeypatch):
    """An explicit skip keeps cheap export evidence and later validation changes no source."""
    root = tmp_path / "skipped"
    manifest = runner.run(config, root, skip_validation=True)
    assert root.exists() and not root.with_name("skipped.partial").exists()
    assert manifest["status"] == "generated-unvalidated" and manifest["validation"] is None
    summary = json.loads((root / "summary.json").read_text())
    assert summary["status"] == "generated-unvalidated"
    assert summary["data_errors"]["measurements"] is None
    assert not (root / "validation.json").exists() and not (root / "_work").exists()
    before = manifest["files"]
    monkeypatch.setattr(sys, "argv", ["generator", "validate", str(root)])
    __main__.main()
    assert json.loads((root / "manifest.json").read_text())["files"] == before
    assert_same_business(uninterrupted, root)


def test_resume_of_skipped_final_dataset_validates_only(tmp_path, config):
    """Resume a published unvalidated directory without requiring temporary parts."""
    root = tmp_path / "skipped-resume"
    runner.run(config, root, skip_validation=True)
    manifest = runner.run(replace(config, timeout_seconds=120, workers=2), resume=root)
    assert manifest["status"] == "validated"
    assert manifest["config"]["workers"] == 2
    assert not (root / "_work").exists()
