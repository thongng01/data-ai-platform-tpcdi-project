"""Keep source-visible fault identities unique without changing selection draws."""

from datetime import date
import json

import polars as pl
import pytest

from data.data_source.generator.config import Config
from data.data_source.generator.generation import Generation
from data.data_source.generator.recording import SourceRecorder
from data.data_source.generator.tests.recording_oracle import scalar_faults, scalar_selected
from data.data_source.generator.validation import _require, _Stage, _xml
from data.data_source.generator.validation_faults import FaultAudit


def _config():
    """Select customer tier errors on the probe date and leave other fields clean."""
    return Config(initial_start="2019-06-01", initial_end="2019-06-23",
                  daily_start="2019-06-24", daily_end="2019-07-12", scale_factor=.01,
                  securities=3, companies=2, customer_tier_error_rate=1,
                  customer_dob_error_rate=0, threads=1, export_rows=1)


def test_daily_new_and_update_have_distinct_audit_keys(tmp_path):
    """Export the reachable ACTV tier-3 NEW and tier-2 UPDCUST collision."""
    config = _config()
    generation = Generation(config, tmp_path)
    recorder = SourceRecorder(generation)
    frame = pl.DataFrame({"C_ID": [1, 1], "C_ST_ID": ["ACTV", "ACTV"],
                          "C_TIER": [3, 2], "_flag": ["I", "U"],
                          "_record_day": [23, 23]})
    recorded, entries = recorder.apply("Customer", frame)
    assert recorded["C_TIER"].to_list() == [9, 9]
    assert [entry["correct"] for entry in entries] == ["3", "2"]
    assert [entry["identity"]["CDC_FLAG"] for entry in entries] == ["I", "U"]
    # The full identity enters Generation's canonical order key. Exporting both
    # entries must not hit the duplicate-key guard, even across one-row parts.
    for entry in reversed(entries):
        generation._record_fault(entry)
    generation.finish()
    audit = FaultAudit.load(tmp_path, config, _require)
    for row, expected in zip(recorded.iter_rows(named=True), ("3", "2"), strict=True):
        assert audit.restore("Customer", date(2019, 6, 24), row)["C_TIER"] == expected
    audit.finish()
    scalar_entries = []
    for row in frame.iter_rows(named=True):
        _, evidence = scalar_faults(config, "Customer", date(2019, 6, 24), row)
        scalar_entries.extend(evidence)
    assert scalar_entries == entries
    for entry in entries:
        old_identity = {name: entry["identity"][name] for name in ("C_ID", "C_ST_ID")}
        assert scalar_selected(config, "Customer", entry["day"], entry["identity"], "C_TIER") == scalar_selected(
            config, "Customer", entry["day"], old_identity, "C_TIER")


@pytest.mark.parametrize("kind", ["ADDACCT", "UPDACCT", "CLOSEACCT"])
def test_same_second_account_actions_have_distinct_audit_keys(tmp_path, kind):
    """Restore both same-customer actions from saved XML account IDs alone."""
    config = _config()
    generation = Generation(config, tmp_path)
    for account_id, tier in ((12, 2), (11, 3)):
        generation.recorder.xml_action(kind, "2019-06-02T00:00:02", 1,
                                      {"C_TIER": tier}, [{"CA_ID": account_id,
                                      "CA_B_ID": 1, "CA_NAME": "Account", "CA_TAX_ST": 1}], ordinal=1)
    generation.finish()
    entries = [json.loads(line) for line in (tmp_path / "summary/data_errors.jsonl").read_text().splitlines()]
    assert {entry["identity"]["CA_ID"] for entry in entries} == {"11", "12"}
    audit = FaultAudit.load(tmp_path, config, _require)
    counts = _xml(_Stage(tmp_path / "stage", 1), tmp_path / "offline/initial/CustomerMgmt.xml",
                  generation.sources, config, audit)
    assert counts == {kind: 2}
    audit.finish()
    for entry in entries:
        old_identity = {name: value for name, value in entry["identity"].items() if name != "CA_ID"}
        assert scalar_selected(config, "CustomerMgmt", entry["day"], entry["identity"], "C_TIER") == scalar_selected(
            config, "CustomerMgmt", entry["day"], old_identity, "C_TIER")
