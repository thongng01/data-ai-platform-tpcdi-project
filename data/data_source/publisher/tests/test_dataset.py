"""Reject corrupted source bytes and cadence before any delivery can start."""

from datetime import date
import json
import os

import pytest

from data.data_source.publisher.dataset import Dataset, INITIAL_FILES, expected_paths, read_json
from data.data_source.publisher.errors import PublisherError
from .conftest import make_dataset, refresh_file

STREAM = "stream/cash_transactions/20190101.jsonl"


def test_complete_saved_source_checks_without_reading_hidden_state(saved_dataset):
    """Only public source trees need to be readable for publication."""
    (saved_dataset / "summary.json").write_text("not-json-private-hidden-data")
    result = Dataset(saved_dataset).verify()
    assert result["status"] == "verified"
    assert result["stream_bytes"] > 0
    assert result["files"] == len(json.loads((saved_dataset / "manifest.json").read_text())["files"])


def test_cadence_independently_matches_monthly_and_finwire_contract():
    """Include empty daily files on holidays, monthly prospects, and posting quarters."""
    config = {"initial_start": "2018-12-30", "initial_end": "2018-12-31",
              "daily_start": "2019-01-01", "daily_end": "2019-04-30"}
    paths, start, end = expected_paths(config)
    assert start == date(2019, 1, 1) and end == date(2019, 4, 30)
    initial = {path.removeprefix("offline/initial/") for path in paths if path.startswith("offline/initial/")}
    assert initial == set(INITIAL_FILES) | {"FINWIRE2017Q3", "FINWIRE2017Q4", "FINWIRE2018Q1",
                                           "FINWIRE2018Q2", "FINWIRE2018Q3", "FINWIRE2018Q4"}
    daily = {path for path in paths if path.startswith("offline/daily/")}
    assert len(daily) == 120 * 6 + 4 + 2
    assert "offline/daily/20190131/FINWIRE2019Q1" in daily
    assert "offline/daily/20190430/FINWIRE2019Q2" in daily
    assert "offline/daily/20190101/Prospect_20190101.csv" in daily
    assert "offline/daily/20190102/Prospect_20190102.csv" not in daily
    assert not any("CashTransaction" in path and "/daily/" in path for path in paths)


@pytest.mark.parametrize("change", ["remove", "extra", "duplicate_manifest", "traversal", "wrong_hash", "bad_status"])
def test_source_and_manifest_corruption_fails(saved_dataset, change):
    """Reject coverage and hash defects rather than silently skipping files."""
    path = saved_dataset / "offline/daily/20190102/Trade_20190102.txt"
    manifest_path = saved_dataset / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if change == "remove":
        path.unlink()
    elif change == "extra":
        path.with_name("BatchDate.txt").write_bytes(b"")
    elif change == "duplicate_manifest":
        manifest["files"].append(manifest["files"][0])
    elif change == "traversal":
        manifest["files"][0]["path"] = "offline/initial/../../manifest.json"
    elif change == "wrong_hash":
        path.write_bytes(b"changed")
    else:
        manifest["status"] = "failed"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(PublisherError):
        Dataset(saved_dataset).verify()


@pytest.mark.parametrize("kind", ["file", "parent", "tree", "manifest", "fifo"])
def test_links_and_special_source_files_fail(saved_dataset, tmp_path, kind):
    """Never follow a linked file or parent and never block while opening a FIFO."""
    file = saved_dataset / "offline/daily/20190102/Trade_20190102.txt"
    if kind in ("file", "fifo"):
        file.unlink()
        if kind == "fifo":
            os.mkfifo(file)
        else:
            target = tmp_path / "outside"
            target.write_bytes(b"")
            file.symlink_to(target)
    else:
        link = {"parent": file.parent, "tree": saved_dataset / "stream", "manifest": saved_dataset / "manifest.json"}[kind]
        target = tmp_path / "outside"
        link.rename(target)
        link.symlink_to(target, target_is_directory=target.is_dir())
    with pytest.raises(PublisherError):
        Dataset(saved_dataset).verify()


def test_replaced_parent_link_after_preflight_is_rejected(saved_dataset, tmp_path):
    """Reopen source components with no-follow flags after the initial tree check."""
    dataset = Dataset(saved_dataset)
    dataset.verify()
    directory = saved_dataset / "offline/daily/20190102"
    target = tmp_path / "outside"
    directory.rename(target)
    directory.symlink_to(target, target_is_directory=True)
    item = dataset.files["offline/daily/20190102/Trade_20190102.txt"]
    with pytest.raises(PublisherError):
        with dataset.snapshot(item, tmp_path):
            pytest.fail("A linked source parent reached delivery.")


@pytest.mark.parametrize("change", ["order", "duplicate_field", "amount_number", "amount_precision", "key_bool",
                                    "schema_bool", "event_date", "timestamp_offset", "no_lf", "blank", "gap",
                                    "duplicate_id", "time_reverse", "settlement_zero", "spaces", "rows"])
def test_saved_payload_contract_failures_with_matching_hash(saved_dataset, change):
    """A matching manifest hash does not hide an invalid saved stream contract."""
    lines = (saved_dataset / STREAM).read_bytes().splitlines(keepends=True)
    value = json.loads(lines[0])
    if change == "order":
        value = {key: value[key] for key in reversed(value)}
    elif change == "duplicate_field":
        lines[0] = lines[0].replace(b'{"ct_id":2,', b'{"ct_id":2,"ct_id":2,')
    elif change == "amount_number":
        value["ct_amt"] = -4.20
    elif change == "amount_precision":
        value["ct_amt"] = "-4.2"
    elif change == "key_bool":
        value["ct_ca_id"] = True
    elif change == "schema_bool":
        value["schema_version"] = True
    elif change == "event_date":
        value["ct_dts"] = "2019-01-02T00:00:00Z"
    elif change == "timestamp_offset":
        value["ct_dts"] = "2019-01-01T00:00:00+00:00"
    elif change == "no_lf":
        lines[-1] = lines[-1].rstrip(b"\n")
    elif change == "blank":
        lines.append(b"\n")
    elif change == "gap":
        value["ct_id"] = 10
    elif change == "duplicate_id":
        other = json.loads(lines[1])
        other["ct_id"] = 2
        lines[1] = json.dumps(other, separators=(",", ":")).encode() + b"\n"
    elif change == "time_reverse":
        value["ct_dts"] = "2019-01-01T23:59:59Z"
    elif change == "settlement_zero":
        value["ct_name"] = "Trade settlement 0"
    elif change == "spaces":
        lines[0] = json.dumps(value).encode() + b"\n"
    if change not in ("duplicate_field", "no_lf", "blank", "duplicate_id", "spaces", "rows"):
        lines[0] = json.dumps(value, separators=(",", ":")).encode() + b"\n"
    refresh_file(saved_dataset, STREAM, b"".join(lines), rows=99 if change == "rows" else None)
    with pytest.raises(PublisherError):
        Dataset(saved_dataset).verify()


def test_stream_continues_the_initial_cash_id(saved_dataset):
    """A forged initial sequence cannot produce an unseen gap at cutover."""
    initial = "offline/initial/CashTransaction.txt"
    refresh_file(saved_dataset, initial, b"2|1001|2018-12-31 08:00:00|100.00|Deposit\n")
    with pytest.raises(PublisherError, match="contiguous"):
        Dataset(saved_dataset).verify()


def test_empty_initial_and_stream_files_are_valid(tmp_path):
    """Zero-record sources count as complete files and require no remote messages."""
    root = make_dataset(tmp_path / "empty", messages=0)
    refresh_file(root, "offline/initial/CashTransaction.txt", b"", rows=0)
    assert Dataset(root).verify()["stream_bytes"] == 0


@pytest.mark.parametrize("raw", [b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}', b'not-json'])
def test_metadata_duplicate_or_nonstandard_values_fail(tmp_path, raw):
    """Reject ambiguous and nonstandard configuration JSON without printing it."""
    path = tmp_path / "metadata.json"
    path.write_bytes(raw)
    with pytest.raises(PublisherError):
        read_json(path)


def test_metadata_read_is_bounded(tmp_path):
    """Do not load an unbounded manifest or private Kafka settings file."""
    path = tmp_path / "metadata.json"
    path.write_bytes(b" " * 100)
    with pytest.raises(PublisherError, match="size limit"):
        read_json(path, limit=50)


def test_snapshot_success_and_failure_do_not_leak_handles(saved_dataset, tmp_path):
    """Repeated copies and corrupt-source failures close all source and temporary handles."""
    from pathlib import Path

    dataset = Dataset(saved_dataset)
    item = dataset.files["offline/initial/CashTransaction.txt"]
    before = len(list(Path("/proc/self/fd").iterdir()))
    for _ in range(30):
        with dataset.snapshot(item, tmp_path) as snapshot:
            assert snapshot.read() == (saved_dataset / item.path).read_bytes()
    (saved_dataset / item.path).write_bytes(b"changed")
    for _ in range(30):
        with pytest.raises(PublisherError):
            with dataset.snapshot(item, tmp_path):
                pytest.fail("Corrupt source reached delivery.")
    assert len(list(Path("/proc/self/fd").iterdir())) == before
