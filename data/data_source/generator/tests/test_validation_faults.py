"""Check independent parser fault consumption and exact coordinator measurements."""

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
import json
import os

import pytest

from data.data_source.generator import validation
from data.data_source.generator.config import Config
from data.data_source.generator.customers import customer_values
from data.data_source.generator.formats import load_sources
from data.data_source.generator.validation import ValidationError, _require
from data.data_source.generator.validation_faults import FaultAudit


def _fault_fixture(tmp_path):
    """Save one selected daily customer fault and its exact source-visible identity."""
    config = Config(initial_start="2019-06-01", initial_end="2019-06-23",
                    daily_start="2019-06-24", daily_end="2019-07-12", scale_factor=.01,
                    securities=3, companies=2, customer_tier_error_rate=1,
                    customer_dob_error_rate=0, threads=1)
    relative = "offline/daily/20190624/Customer_20190624.txt"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    row = customer_values(1)
    row["C_TIER"] = 9
    path.write_text("I|1|" + "|".join(str(row[name]) for name in load_sources()["Customer"].columns) + "\n")
    entry = {"source": "Customer", "day": "2019-06-24", "field": "C_TIER",
             "identity": {"C_ID": "1", "C_ST_ID": "ACTV", "CDC_FLAG": "I"}, "correct": "3", "recorded": "9"}
    audit_path = tmp_path / "summary/data_errors.jsonl"
    audit_path.parent.mkdir()
    audit_path.write_text(json.dumps(entry) + "\n")
    audit = FaultAudit.load(tmp_path, config, _require)
    inputs = {"root": tmp_path, "temporary": tmp_path / "stage", "partitions": 1, "date_partitions": 1,
              "config": config, "sessions": {}, "securities": {}, "saved": None, "audit_entries": audit.entries}
    return config, relative, entry, audit, inputs


def _parse_in_worker(job):
    """Return the actual parser summary and process ID for separate-worker checks."""
    return os.getpid(), validation._parse_independent_file(job)


def test_duplicate_consumption_across_parser_workers_fails(tmp_path):
    """Two readers can restore a row locally, but the coordinator must reject reuse."""
    _, relative, _, audit, inputs = _fault_fixture(tmp_path)
    original = dict(audit.entries)
    # One task per process proves that distinct spawned workers each consume the
    # same evidence. No shared connection or shared mutable used flag hides it.
    with ProcessPoolExecutor(max_workers=2, max_tasks_per_child=1, mp_context=get_context("spawn"),
                             initializer=validation._initialize_parser, initargs=(inputs,)) as executor:
        results = list(executor.map(_parse_in_worker, [(0, relative), (1, relative)]))
    assert len({pid for pid, _ in results}) == 2
    first, second = [result for _, result in results]
    assert first.error is second.error is None
    assert first.audit_used == second.audit_used and len(first.audit_used) == 1
    audit.accept(first.audit_used, first.audit_measurements)
    assert audit.measurements["C_TIER"]["daily"] == {"eligible": 1, "errors": 1}
    assert audit.measurements["C_DOB"]["daily"] == {"eligible": 1, "errors": 0}
    audit.finish()
    with pytest.raises(ValidationError, match="consumed more than once"):
        audit.accept(second.audit_used, second.audit_measurements)
    assert audit.entries == original
    assert audit.measurements["C_TIER"]["daily"] == {"eligible": 1, "errors": 1}


def test_unconsumed_entry_fails_after_worker_results(tmp_path):
    """A valid but unused identity cannot disappear when worker summaries are merged."""
    config, relative, entry, _, inputs = _fault_fixture(tmp_path)
    unused = {**entry, "identity": {**entry["identity"], "C_ID": "2"}}
    with (tmp_path / "summary/data_errors.jsonl").open("a") as stream:
        stream.write(json.dumps(unused) + "\n")
    audit = FaultAudit.load(tmp_path, config, _require)
    inputs["audit_entries"] = audit.entries
    validation._initialize_parser(inputs)
    result = validation._parse_independent_file((0, relative))
    assert result.error is None
    audit.accept(result.audit_used, result.audit_measurements)
    with pytest.raises(ValidationError, match="Unused fault audit evidence"):
        audit.finish()


def test_duplicate_consumption_inside_one_file_fails(tmp_path):
    """Retain one-time consumption within each worker as well as across workers."""
    _, relative, _, _, inputs = _fault_fixture(tmp_path)
    path = tmp_path / relative
    with path.open("a") as stream:
        stream.write(path.read_text().replace("I|1|", "I|2|", 1))
    validation._initialize_parser(inputs)
    result = validation._parse_independent_file((0, relative))
    assert isinstance(result.error, ValidationError)
    assert str(result.error) == "A fault audit entry was consumed more than once."


def test_duplicate_saved_evidence_still_fails(tmp_path):
    """Reject duplicate input evidence before any parser worker can read it."""
    config, _, entry, _, _ = _fault_fixture(tmp_path)
    with (tmp_path / "summary/data_errors.jsonl").open("a") as stream:
        stream.write(json.dumps(entry) + "\n")
    with pytest.raises(ValidationError, match="Duplicate fault audit identity and field"):
        FaultAudit.load(tmp_path, config, _require)


def test_worker_measurements_sum_exactly(tmp_path):
    """Combine separate row counts without changing any field or delivery bucket."""
    config, _, _, audit, _ = _fault_fixture(tmp_path)
    # Disjoint evidence keys can be checked in separate files. Match the combined
    # integer counts and rate calculation to one reader's direct calculation.
    first, second, direct = audit.reader(), audit.reader(), audit.reader()
    rows = [{"C_ID": "1", "C_ST_ID": "ACTV", "_flag": "I", "C_TIER": "9", "C_DOB": "1980-01-01"},
            {"C_ID": "2", "C_ST_ID": "ACTV", "_flag": "U", "C_DOB": "1970-01-01"}]
    for reader, row in zip((first, second), rows, strict=True):
        reader.restore("Customer", "2019-06-24", row)
        direct.restore("Customer", "2019-06-24", row)
        audit.accept(reader.used, reader.measurements)
    assert audit.report() == direct.report()
    assert audit.measurements["C_DOB"]["daily"] == {"eligible": 2, "errors": 0}
    assert audit.report()["C_TIER"]["configured_rate"] == config.customer_tier_error_rate
    audit.finish()
