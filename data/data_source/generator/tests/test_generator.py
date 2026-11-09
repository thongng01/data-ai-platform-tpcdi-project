"""Small complete runs prove source contracts and execution independence."""

from dataclasses import replace
import json
from pathlib import Path

import pytest

from data.data_source.generator.config import Config
from data.data_source.generator.runner import compare, run


@pytest.fixture(scope="module")
def short_config():
    """Build a complete small run that crosses the daily Trade schema change."""
    return Config(initial_start="2019-06-24", initial_end="2019-06-30",
                  daily_start="2019-07-01", daily_end="2019-07-08",
                  scale_factor=.01, securities=8, companies=5,
                  customer_group_size=2048, export_rows=257, threads=1,
                  annual_trade_rates=(40, 120, 60, 300, 750))


@pytest.fixture(scope="module")
def complete_run(tmp_path_factory, short_config):
    """Generate one shared validated fixture for contract and repeatability checks."""
    output = tmp_path_factory.mktemp("source-runs") / "baseline"
    run(short_config, output)
    return output


def test_complete_dataset_has_only_expected_paths(complete_run):
    """Require the source delivery layout and upgraded daily Trade row widths."""
    manifest = json.loads((complete_run / "manifest.json").read_text())
    assert manifest["status"] == "validated"
    assert manifest["export"]["backend"] == "polars"
    assert manifest["export"]["ignored_legacy_settings"] == {"merge_fan_in": manifest["config"]["merge_fan_in"]}
    assert "maximum_merge_inputs" not in manifest
    assert (complete_run / "offline/initial/CashTransaction.txt").exists()
    assert not list((complete_run / "offline/daily").rglob("CashTransaction*"))
    assert len(list((complete_run / "stream/cash_transactions").glob("*.jsonl"))) == 8
    for trade_file in (complete_run / "offline/daily").rglob("Trade_*.txt"):
        for line in trade_file.read_text().splitlines():
            assert len(line.split("|")) == 17


@pytest.mark.parametrize("group,workers,threads,fan_in", [(1, 1, 1, 2), (7, 2, 2, 4), (7, 4, 2, 4), (2048, 2, 1, 16)])
def test_source_bytes_repeat_across_execution_settings(tmp_path, short_config, complete_run, group, workers, threads, fan_in):
    """Require identical source and audit bytes across group and worker settings."""
    config = replace(short_config, customer_group_size=group, workers=workers,
                     threads=threads, merge_fan_in=fan_in, export_rows=113)
    output = tmp_path / "repeat"
    run(config, output)
    result = compare(complete_run, output)
    assert result["identical_files"] > 0
    assert (complete_run / "summary/data_errors.jsonl").read_bytes() == (output / "summary/data_errors.jsonl").read_bytes()
    for base in (complete_run / "offline", complete_run / "stream"):
        for path in base.rglob("*"):
            if path.is_file():
                assert path.read_bytes() == (output / path.relative_to(complete_run)).read_bytes()


@pytest.mark.parametrize("year", [1902, 2100])
def test_complete_run_supports_calendar_boundary_years(tmp_path, year):
    """Generate supported boundary years with valid customer and company dates."""
    from xml.etree import ElementTree as ET
    from data.data_source.generator.config import read_yaml

    config = Config(initial_start=f"{year}-01-02", initial_end=f"{year}-01-05",
                    daily_start=f"{year}-01-06", daily_end=f"{year}-01-09",
                    scale_factor=.001, securities=2, companies=1, workers=1)
    output = tmp_path / f"year-{year}"
    run(config, output)
    for _, element in ET.iterparse(output / "offline/initial/CustomerMgmt.xml"):
        if element.tag == "Customer" and "C_DOB" in element.attrib:
            assert 0 <= year - int(element.attrib["C_DOB"][:4]) <= 120
    layout = read_yaml(Path(__file__).parents[1] / "definitions/finwire.yaml")["CMP"]
    names = list(layout)
    offset = sum(layout[name][0] for name in names[:names.index("FoundingDate")])
    for path in (output / "offline/initial").glob("FINWIRE*"):
        for line in path.read_text().splitlines():
            if line[15:18] == "CMP":
                assert line[offset:offset + 8] <= line[:8]


def test_run_refuses_existing_and_partial_directories(tmp_path, short_config):
    """Refuse any occupied final or partial run destination."""
    output = tmp_path / "occupied"
    output.mkdir()
    with pytest.raises(FileExistsError):
        run(short_config, output)
    output.rmdir()
    output.with_name("occupied.partial").mkdir()
    with pytest.raises(FileExistsError):
        run(short_config, output)


def test_signal_command_uses_saved_configuration(tmp_path, short_config, monkeypatch, capsys):
    """Run the sampled signal check explicitly, using the validated run's settings."""
    import sys
    from data.data_source.generator import __main__, signal_check

    (tmp_path / "manifest.json").write_text(json.dumps({"status": "validated", "config": short_config.as_dict()}))
    received = []

    def check(root, config, sample_accounts):
        """Capture the command's arguments without fitting another test model."""
        received.append((root, config, sample_accounts))
        return {"status": "measured"}

    monkeypatch.setattr(signal_check, "check_signal", check)
    monkeypatch.setattr(sys, "argv", ["generator", "signal-check", str(tmp_path), "--sample-accounts", "37"])
    __main__.main()
    assert received == [(tmp_path, short_config, 37)]
    assert json.loads(capsys.readouterr().out) == {"status": "measured"}
