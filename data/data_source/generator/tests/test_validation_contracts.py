"""Reject source-contract gaps without relying on saved inventory hashes."""

from datetime import date
from pathlib import Path
from types import SimpleNamespace
import shutil

import polars as pl
import pytest

from data.data_source.generator import validation as v
from data.data_source.generator.config import read_yaml
from data.data_source.generator.formats import load_sources
from data.data_source.generator.tests.test_validation_m1 import exported
from data.data_source.generator.validation_faults import FaultAudit


def _parse_reference(root, config, name):
    """Parse one physical reference file with no saved inventory evidence."""
    relative = f"offline/initial/{name}"
    result = v._ParsedFile(relative)
    v._parse_file(root, relative, v._Stage(root / "stage", 1), config, load_sources(), {}, {},
                  FaultAudit(config, v._require, {}), None, result)


@pytest.mark.parametrize("name", ["Date.txt", "StatusType.txt", "TradeType.txt", "Industry.txt", "TaxRate.txt"])
def test_empty_reference_fails_without_hashes(exported, tmp_path, name):
    """An empty lookup cannot establish source meanings or calendar coverage."""
    _, config, _ = exported
    path = tmp_path / "offline/initial" / name
    path.parent.mkdir(parents=True)
    path.touch()
    with pytest.raises(v.ValidationError, match="must not be empty"):
        _parse_reference(tmp_path, config, name)


@pytest.mark.parametrize("name,field,value,match", [
    ("Time.txt", 1, "99:99:99", "Time reference columns"),
    ("Time.txt", 2, "01", "Time reference columns"),
    ("Time.txt", 8, "1", "Time reference columns"),
    ("Date.txt", 2, "Wrong date", "Date reference columns"),
    ("Date.txt", 13, "2000", "Date reference columns"),
    ("Date.txt", 17, "1", "Date reference columns"),
])
def test_reference_columns_agree_with_identity(exported, tmp_path, name, field, value, match):
    """Check physical labels and decomposed values rather than only their types."""
    original, config, _ = exported
    path = tmp_path / "offline/initial" / name
    path.parent.mkdir(parents=True)
    lines = (original / "offline/initial" / name).read_text().splitlines()
    row = lines[0].split("|")
    assert row[field] != value
    row[field] = value
    lines[0] = "|".join(row)
    path.write_text("\n".join(lines) + "\n")
    with pytest.raises(v.ValidationError, match=match):
        _parse_reference(tmp_path, config, name)


@pytest.mark.parametrize("name", ["StatusType.txt", "TradeType.txt", "Date.txt"])
def test_reference_missing_required_row(exported, tmp_path, name):
    """Keep required replay codes and the final promised calendar date."""
    original, config, _ = exported
    path = tmp_path / "offline/initial" / name
    path.parent.mkdir(parents=True)
    lines = (original / "offline/initial" / name).read_text().splitlines()
    path.write_text("\n".join(lines[:-1]) + "\n")
    with pytest.raises(v.ValidationError, match="missing a replay|complete generated range"):
        _parse_reference(tmp_path, config, name)


@pytest.mark.parametrize("name", ["HR.csv", "WatchHistory.txt"])
def test_physical_initial_order_is_checked(exported, tmp_path, name):
    """Replay sorting must not hide a reversed physical source file."""
    original, config, _ = exported
    path = tmp_path / "offline/initial" / name
    path.parent.mkdir(parents=True)
    lines = (original / "offline/initial" / name).read_text().splitlines()
    assert len(lines) > 1
    path.write_text("\n".join(reversed(lines)) + "\n")
    with pytest.raises(v.ValidationError, match="employee IDs|watch history"):
        _parse_reference(tmp_path, config, name)


def _finwire_copy(exported, tmp_path):
    """Copy only FINWIRE and fault evidence for an isolated ordered parser run."""
    original, config, _ = exported
    for path in [*(original / "offline").rglob("FINWIRE*"), original / "summary/data_errors.jsonl"]:
        target = tmp_path / path.relative_to(original)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
    return config, sorted((tmp_path / "offline").rglob("FINWIRE*"))


def _finwire_errors(root, config, paths):
    """Return deferred FINWIRE errors with no inventory or output-module helpers."""
    audit = FaultAudit.load(root, config, v._require)
    _, _, results = v._finwire(root, paths, config, {"AA"}, audit, None)
    return [result.error for result in results.values() if result.error is not None]


@pytest.mark.parametrize("kind,field,value", [
    ("CMP", "CompanyName", ""), ("CMP", "AddrLine1", ""), ("CMP", "CEOname", ""),
    ("CMP", "FoundingDate", "99999999"), ("SEC", "Name", ""), ("SEC", "IssueType", ""),
    ("SEC", "ExID", ""), ("SEC", "ShOut", "NOT-A-NUMBER"),
    ("SEC", "FirstTradeDate", "99999999"), ("SEC", "FirstTradeExchg", "99999999"),
    ("FIN", "QtrStartDate", "99999999"), ("FIN", "PostingDate", "99999999"),
    ("FIN", "QtrStartDate", "20000101"), ("FIN", "PostingDate", "20000101"),
    ("FIN", "ShOut", "1.5"), ("FIN", "Revenue", "1e2"),
])
def test_finwire_required_values_dates_and_numbers(exported, tmp_path, kind, field, value):
    """Reject a single malformed field while retaining every fixed record width."""
    config, paths = _finwire_copy(exported, tmp_path)
    layout = read_yaml(Path(v.__file__).parent / "definitions/finwire.yaml")[kind]
    for path in paths:
        lines = path.read_text().splitlines()
        index = next((index for index, line in enumerate(lines) if line[15:18] == kind), None)
        if index is None:
            continue
        offset = sum(width for name, (width, _) in layout.items() if list(layout).index(name) < list(layout).index(field))
        width = layout[field][0]
        lines[index] = lines[index][:offset] + value.ljust(width) + lines[index][offset + width:]
        path.write_text("\n".join(lines) + "\n")
        break
    else:
        pytest.fail("The fixture has no requested FINWIRE record.")
    errors = _finwire_errors(tmp_path, config, paths)
    assert errors, f"Accepted malformed {kind}.{field}."
    assert isinstance(errors[0], v.ValidationError)


def test_finwire_optional_values_and_nonliteral_names_stay_legal(exported, tmp_path):
    """The contract accepts optional blanks and names outside generator templates."""
    config, paths = _finwire_copy(exported, tmp_path)
    layout = read_yaml(Path(v.__file__).parent / "definitions/finwire.yaml")["CMP"]
    path = next(path for path in paths if "CMP" in path.read_text())
    lines = path.read_text().splitlines()
    index = next(index for index, line in enumerate(lines) if line[15:18] == "CMP")
    offset = 0
    for name, (width, _) in layout.items():
        if name in {"CompanyName", "FoundingDate", "AddrLine2", "Country"}:
            value = "Another company" if name == "CompanyName" else ""
            lines[index] = lines[index][:offset] + value.ljust(width) + lines[index][offset + width:]
        offset += width
    path.write_text("\n".join(lines) + "\n")
    assert not _finwire_errors(tmp_path, config, paths)


def test_finwire_requires_each_company_period(exported, tmp_path):
    """A surviving FIN report must not hide a missing report for another company."""
    config, paths = _finwire_copy(exported, tmp_path)
    for path in reversed(paths):
        lines = path.read_text().splitlines()
        index = next((index for index, line in enumerate(lines) if line[15:18] == "FIN"), None)
        if index is not None:
            lines.pop(index)
            path.write_text("\n".join(lines) + "\n")
            break
    errors = _finwire_errors(tmp_path, config, paths)
    assert len(errors) == 1 and "company-period coverage" in str(errors[0])


@pytest.mark.parametrize("closed", ["2018-12-03 23:59:59", "2019-01-03 23:59:59"])
def test_customer_inactivation_requires_account_evidence(closed):
    """Initial and daily customer inactivation cannot silently close an account."""
    accounts = {1: {"owner": 1, "closed": None, "close_date": date.fromisoformat(closed[:10])}}
    customers = {1: {"closed": closed}}
    with pytest.raises(v.ValidationError, match="explicit account closure evidence"):
        v._account_closures(accounts, customers)


def test_already_closed_account_needs_no_second_closure():
    """Customer inactivation keeps an earlier explicit account closure legal."""
    closed = "2018-12-03 23:59:59"
    accounts = {1: {"owner": 1, "closed": closed, "close_date": date(2018, 12, 3)}}
    assert len(v._account_closures(accounts, {1: {"closed": "2019-01-03 23:59:59"}})) == 1


@pytest.mark.parametrize("kind,status", [("TMB", "PNDG"), ("TMS", "PNDG"), ("TLB", "SBMT"), ("TLS", "SBMT"), ("TSL", "SBMT")])
def test_creation_status_follows_order_type(exported, kind, status):
    """Market orders start submitted; other orders start pending."""
    _, config, _ = exported
    replay = v._AccountReplay({}, {}, {}, config, {}, {})
    row = {"T_ID": 1, "T_ST_ID": status, "T_DTS": "2018-12-03 10:00:00", "T_TT_ID": kind,
           "_flag": "I", "T_TRADE_PRICE": None, "T_CHRG": None, "T_COMM": None, "T_TAX": None}
    with pytest.raises(v.ValidationError, match="invalid initial status"):
        replay.check_transition(row)


@pytest.mark.parametrize("kind", ["TMB", "TMS"])
def test_market_order_cannot_remain_open(exported, tmp_path, kind):
    """A full account replay rejects an unfinished market order at the end."""
    _, config, _ = exported
    with pytest.raises(v.ValidationError, match="creation timestamp"):
        _open_order_replay(tmp_path, config, kind)


@pytest.mark.parametrize("kind", ["TLB", "TLS", "TSL"])
def test_pending_limit_order_at_cutover_and_end_is_legal(exported, tmp_path, kind):
    """The immediate-completion rule leaves pending limit orders unchanged."""
    _, config, _ = exported
    assert _open_order_replay(tmp_path, config, kind) == (1, 1, 0)


def _open_order_replay(root, config, kind):
    """Build minimal saved account inputs without generation-time business state."""
    directory = root / "customers"
    directory.mkdir()
    created = str(config.start) + " 00:00:00"
    pl.DataFrame({"customer_id": [1], "created": [created], "closed": [None]},
                 schema={"customer_id": pl.Int64, "created": pl.String, "closed": pl.String}).write_parquet(directory / "one.parquet")
    fields = {"CA_ID": 1, "CA_B_ID": 1, "CA_C_ID": 1, "CA_NAME": "One", "CA_TAX_ST": 1, "CA_ST_ID": "ACTV"}
    account = {"account_id": 1, "customer_id": 1, "is_cash": True, "margin_limit_cents": 0,
               "wealth_cents": 100000, "profile": 0, "open_date": config.start, "close_date": None}
    status = "SBMT" if kind in {"TMB", "TMS"} else "PNDG"
    # Sell reservations need existing shares, which this focused fixture supplies
    # as replay input. The test isolates completion, not holding reconstruction.
    trade = {"T_ID": 1000001, "T_CA_ID": 1, "T_S_SYMB": "S0000000001", "T_ST_ID": status,
             "T_DTS": str(config.start) + " 10:00:00", "T_TT_ID": kind, "T_IS_CASH": 1, "T_QTY": 1,
             "T_BID_PRICE": 1000, "T_EXEC_NAME": "Broker", "T_TRADE_PRICE": None, "T_CHRG": None,
             "T_COMM": None, "T_TAX": None, "_day": "initial", "_flag": "I", "_order": 1}
    tables = {"Hidden": [account], "AccountXML": [{**fields, "_present": list(fields), "_timestamp": created,
               "_day": str(config.start), "_flag": "I", "_order": 1}], "Trade": [trade],
              "TradeHistory": [{"TH_T_ID": trade["T_ID"], "TH_DTS": trade["T_DTS"], "TH_ST_ID": status, "_order": 1}],
              "CashTransaction": [{"CT_ID": 1, "CT_CA_ID": 1, "CT_DTS": str(config.start) + " 08:00:00",
                                   "CT_AMT": 100000, "CT_NAME": "Deposit"}]}
    stage = SimpleNamespace(root=root, records=lambda table, partition: iter(tables.get(table, [])),
                            frame=lambda table, partition: pl.DataFrame(tables.get(table, [])))
    from unittest.mock import patch
    original = v._AccountReplay.__init__
    def initial_holding(replay, *args):
        """Supply shares solely for the open sell reservation in this fixture."""
        original(replay, *args)
        replay.positions[(1, "S0000000001")] = (999, 1)
    with patch.object(v._AccountReplay, "__init__", initial_holding):
        return v._account_replay(stage, 0, config, {"S0000000001": [(created, 0, "ACTV")]},
                                 {str(config.start): "16:00:00"}, {1})


def test_market_completion_uses_creation_timestamp(exported):
    """A later market completion violates the immediate-execution contract."""
    _, config, _ = exported
    replay = v._AccountReplay({}, {}, {}, config, {}, {})
    previous = {"T_ID": 1, "T_ST_ID": "SBMT", "T_DTS": "2018-12-03 10:00:00", "T_TT_ID": "TMB",
                **{name: 1 for name in ("T_CA_ID", "T_S_SYMB", "T_QTY", "T_IS_CASH", "T_BID_PRICE", "T_EXEC_NAME")}}
    replay.trades[1] = previous
    row = {**previous, "T_ST_ID": "CMPT", "T_DTS": "2018-12-03 10:00:01", "_flag": "U", "_order": 2,
           "T_TRADE_PRICE": 1, "T_CHRG": 1, "T_COMM": 0, "T_TAX": 0}
    with pytest.raises(v.ValidationError, match="creation timestamp"):
        replay.check_transition(row)
