import json
from dataclasses import replace
from pathlib import Path
from xml.etree import ElementTree as ET

import numpy as np
import polars as pl
import pytest

from data.data_source.generator.config import Config, ConfigError
from data.data_source.generator.formats import Field, load_sources
from data.data_source.generator.random_values import values
from data.data_source.generator.runner import compare, run
from data.data_source.generator.sorting import SortedOutput
from data.data_source.generator.validation import ValidationError, _rows, validate


@pytest.fixture(scope="module")
def small_config():
    return Config(customers=22, historical_days=6, companies=2, securities=4, new_customers_per_day=2, customer_batch_size=7, merge_fan_in=3)


@pytest.fixture(scope="module")
def generated(tmp_path_factory, small_config):
    root = tmp_path_factory.mktemp("polars-poc") / "run"
    return root, run(small_config, root)


@pytest.mark.parametrize("changes", [
    {"customers": 0}, {"customers": True}, {"historical_days": 2}, {"seed": -1},
    {"cash_basis_points": 10001}, {"customer_batch_size": 200001},
    {"merge_fan_in": 1}, {"companies": 100}, {"start_date": "2017-02-31"},
    {"incremental_days": -1}, {"cycles_per_account_day": 0}, {"threads": 0},
])
def test_invalid_configuration(changes):
    with pytest.raises(ConfigError):
        Config(**changes)


def test_yaml_rejects_unknown_and_duplicate_keys(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("customers: 20\ncustomers: 30\n")
    with pytest.raises(ConfigError, match="Duplicate"):
        Config.load(path)
    path.write_text("customerz: 20\n")
    with pytest.raises(ConfigError, match="Unknown"):
        Config.load(path)


def test_random_values_are_independent_of_batches_and_order():
    identifiers = np.arange(1, 1000, dtype=np.uint64)
    whole = values(identifiers, 22, "price")
    split = np.concatenate([values(chunk, 22, "price") for chunk in np.array_split(identifiers, 17)])
    np.testing.assert_array_equal(whole, split)
    np.testing.assert_array_equal(whole, values(identifiers[::-1], 22, "price")[::-1])
    assert not np.array_equal(whole, values(identifiers, 23, "price"))
    assert not np.array_equal(whole, values(identifiers, 22, "quantity"))


@pytest.mark.parametrize("relative", [
    "Batch1/CashTransaction.txt", "Batch1/DailyMarket.txt", "Batch1/HoldingHistory.txt",
    "Batch1/Industry.txt", "Batch1/StatusType.txt", "Batch1/TaxRate.txt", "Batch1/Trade.txt",
    "Batch1/TradeHistory.txt", "Batch1/TradeType.txt", "Batch1/WatchHistory.txt",
    "Batch2/CashTransaction.txt", "Batch2/Trade.txt",
])
def test_declarative_formats_accept_native_reference_fixtures(relative):
    fixture_root = Path(__file__).resolve().parent / "fixtures/tpcdi_sf3"
    path = fixture_root / relative
    source = load_sources()[path.stem]
    assert sum(1 for record in _rows(path, source, int(path.parent.name[5:]))) > 0


@pytest.mark.parametrize("datatype,value", [
    ("VALUE_T", "100000000.00"), ("S_QTY_T", "-1"), ("CHAR(3)", "four"),
    ("DATETIME", "2017-06-01T09:00:00"), ("DATETIME", "2017-02-31 09:00:00"),
    ("DATE", "20170601"), ("BOOLEAN", "true"), ("NUM(6,5)", "0.123456"),
])
def test_source_fields_reject_invalid_encodings(datatype, value):
    with pytest.raises(ValueError):
        Field("field", datatype).check(value)


def test_source_money_supports_exact_signed_limits():
    Field("amount", "VALUE_T").check("-99999999.99")
    Field("amount", "VALUE_T").check("99999999.99")


def test_full_connected_dataset(generated):
    root, manifest = generated
    assert manifest["status"] == "validated"
    assert manifest["validation"]["xml_actions"] == {
        "NEW": 22, "UPDCUST": 7, "UPDACCT": 5, "ADDACCT": 4, "CLOSEACCT": 3, "INACT": 2,
    }
    assert manifest["validation"]["totals"]["cash"] > 0
    assert manifest["maximum_merge_inputs"] <= 3
    assert list((root / "runs").iterdir()) == []
    assert not list(root.glob("validation-*"))
    assert (root / "sources/Batch1/CustomerMgmt.xml").exists()
    assert not (root / "sources/Batch1/Account.txt").exists()
    assert not (root / "sources/Batch2/TradeHistory.txt").exists()


def test_all_source_bytes_match_across_batch_sizes_and_threads(generated, small_config, tmp_path):
    first, _ = generated
    second = tmp_path / "different-execution"
    run(replace(small_config, customer_batch_size=1, merge_fan_in=8, threads=2), second)
    assert compare(first, second)["identical_files"] >= 35


def test_cross_batch_lifecycles_and_holdings(generated):
    root, _ = generated
    historical = {row.split("|")[0]: row.split("|")[2] for row in (root / "sources/Batch1/Trade.txt").read_text().splitlines()}
    updates = [row.split("|") for row in (root / "sources/Batch2/Trade.txt").read_text().splitlines()]
    assert any(row[0] == "U" and historical.get(row[2]) in ("PNDG", "SBMT") and row[4] == "CMPT" for row in updates)
    earlier_origins = {row.split("|")[0] for row in (root / "sources/Batch1/HoldingHistory.txt").read_text().splitlines()}
    later = [row.split("|") for row in (root / "sources/Batch2/HoldingHistory.txt").read_text().splitlines()]
    assert any(row[2] in earlier_origins and int(row[4]) > 0 and row[5] == "0" for row in later)


def test_xml_structure_has_namespaced_actions_and_unqualified_children(generated):
    root, _ = generated
    document = ET.parse(root / "sources/Batch1/CustomerMgmt.xml")
    actions = document.getroot().findall("{http://www.tpc.org/tpc-di}Action")
    first = actions[0].find("Customer")
    assert first.find("Name/C_L_NAME").text == "Family1"
    assert first.find("TaxInfo/C_LCL_TX_ID").text == "US1"
    assert len(first.findall("Account")) == 2


def test_xml_against_specification_xsd(generated):
    etree = pytest.importorskip("lxml.etree")
    root, _ = generated
    checker = etree.XMLSchema(etree.parse(str(Path(__file__).with_name("CustomerMgmt.xsd"))))
    checker.assertValid(etree.parse(str(root / "sources/Batch1/CustomerMgmt.xml")))


def test_compare_checks_actual_bytes(generated):
    root, _ = generated
    path = root / "sources/Batch1/BatchDate.txt"
    original = path.read_bytes()
    try:
        path.write_text("1999-01-01\n")
        with pytest.raises(ValueError, match="no longer match"):
            compare(root, root)
    finally:
        path.write_bytes(original)


def test_validation_detects_corrupt_cash(generated, small_config):
    root, _ = generated
    path = root / "sources/Batch1/CashTransaction.txt"
    original = path.read_bytes()
    fields = original.decode().splitlines()
    row = fields[0].split("|")
    row[2] = "1.00"
    fields[0] = "|".join(row)
    try:
        path.write_text("\n".join(fields) + "\n")
        with pytest.raises(ValidationError, match="Cash reconciles"):
            validate(root, small_config)
    finally:
        path.write_bytes(original)


def test_validation_detects_holding_discontinuity(generated, small_config):
    root, _ = generated
    path = root / "sources/Batch1/HoldingHistory.txt"
    original = path.read_bytes()
    rows = original.decode().splitlines()
    fields = rows[0].split("|")
    fields[2], fields[3] = str(int(fields[2]) + 1), str(int(fields[3]) + 1)
    rows[0] = "|".join(fields)
    try:
        path.write_text("\n".join(rows) + "\n")
        with pytest.raises(ValidationError, match="continuous"):
            validate(root, small_config)
    finally:
        path.write_bytes(original)


def test_validation_detects_future_execution_values(generated, small_config):
    root, _ = generated
    path = root / "sources/Batch1/Trade.txt"
    original = path.read_bytes()
    rows = original.decode().splitlines()
    for index, row in enumerate(rows):
        fields = row.split("|")
        if fields[2] == "SBMT":
            fields[10] = "10.00"
            rows[index] = "|".join(fields)
            break
    else:
        pytest.fail("The test dataset must contain an unfinished trade.")
    try:
        path.write_text("\n".join(rows) + "\n")
        with pytest.raises(ValidationError, match="Only completed"):
            validate(root, small_config)
    finally:
        path.write_bytes(original)


def test_existing_output_is_preserved(generated, small_config):
    root, manifest = generated
    with pytest.raises(FileExistsError):
        run(small_config, root)
    assert json.loads((root / "manifest.json").read_text())["dataset_id"] == manifest["dataset_id"]


@pytest.mark.parametrize("cash_probability", [0, 10000])
def test_empty_incremental_scope_and_cash_extremes(tmp_path, cash_probability):
    config = Config(customers=1, companies=1, securities=1, brokers=1, historical_days=6,
                    incremental_days=0, new_customers_per_day=0, cash_basis_points=cash_probability,
                    cancel_basis_points=0, customer_batch_size=1)
    manifest = run(config, tmp_path / "edge")
    assert manifest["validation"]["totals"]["customers"] == 1
    assert (manifest["validation"]["totals"]["cash"] == 0) == (cash_probability == 0)
    assert not (tmp_path / "edge/sources/Batch2").exists()


def test_resource_failure_retains_partial_output(small_config, tmp_path):
    output = tmp_path / "limited"
    with pytest.raises(RuntimeError, match="memory budget"):
        run(replace(small_config, memory_limit_mb=1), output)
    assert not output.exists()
    assert (output.with_name("limited.partial") / "failure.json").exists()


def test_external_merge_bounds_files_and_preserves_global_order(tmp_path):
    writer = SortedOutput(tmp_path, 3)
    for identifier in range(99, -1, -1):
        writer.add("Batch1/example.txt", pl.DataFrame({"_key": [f"{identifier:05d}"], "_payload": [str(identifier)]}))
    writer.finish({"Batch1/example.txt": (False, False)})
    assert (tmp_path / "sources/Batch1/example.txt").read_text().splitlines() == list(map(str, range(100)))
    assert writer.maximum_merge_inputs <= 3
    assert writer.maximum_retained_run_paths < 15


def test_external_merge_rejects_duplicate_global_keys(tmp_path):
    writer = SortedOutput(tmp_path, 3)
    for value in ("first", "second"):
        writer.add("Batch1/example.txt", pl.DataFrame({"_key": ["1"], "_payload": [value]}))
    with pytest.raises(ValueError, match="Nonunique"):
        writer.finish({"Batch1/example.txt": (False, False)})
