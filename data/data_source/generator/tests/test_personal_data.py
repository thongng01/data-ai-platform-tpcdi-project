"""Regress stable personal fields, source widths, and customer-prospect matching."""

import csv
from dataclasses import replace
from datetime import date
from pathlib import Path

import numpy as np
import pytest

from data.data_source.generator import customers, personal_data
from data.data_source.generator.config import Config
from data.data_source.generator.customers import (
    _prospect_columns, _prospect_values, account_values, customer_birth_year,
    customer_values, generate_prospects,
)
from data.data_source.generator.formats import load_sources
from data.data_source.generator.personal_data import personal_columns, personal_values
from data.data_source.generator.population import build_population
from data.data_source.generator.random_values import uniform
from data.data_source.generator.reference import employee_columns


MATCH_FIELDS = (
    ("LastName", "C_L_NAME"), ("FirstName", "C_F_NAME"), ("MiddleInitial", "C_M_NAME"),
    ("AddressLine1", "C_ADLINE1"), ("AddressLine2", "C_ADLINE2"),
    ("PostalCode", "C_ZIPCODE"), ("City", "C_CITY"), ("State", "C_STATE_PROV"), ("Country", "C_CTRY"),
)


def _definition_rows(filename):
    """Read committed source words independently from the production cache."""
    path = Path(personal_data.__file__).parent / "definitions" / filename
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _assert_source_widths(source_name, row):
    """Check every field against the unchanged source definition, including widths."""
    source = load_sources()[source_name]
    source.check([str(row[name]) for name in source.columns])


def _short_config(**changes):
    """Keep prospect tests on a small firm and two monthly snapshots."""
    settings = dict(
        initial_start="2018-12-20", initial_end="2018-12-31",
        daily_start="2019-01-01", daily_end="2019-02-03",
        scale_factor=.05, customers_per_unit=400, securities=3, companies=2,
        prospect_overlap=1.0, export_rows=7, seed=419,
    )
    return Config(**{**settings, **changes})


class ProspectCapture:
    """Collect only prospect export batches without running business activity."""

    def __init__(self, config):
        """Retain the configuration and an empty snapshot map."""
        self.config = config
        self.snapshots = {}
        self.batch_sizes = []

    def record_batch(self, name, day, columns, *, order=None, flag="I"):
        """Consume direct prospect arrays and retain their numeric ordering metadata."""
        assert name == "Prospect"
        assert tuple(columns) == tuple(load_sources()[name].columns)
        assert isinstance(day, int)
        size = len(columns["AgencyID"])
        assert all(len(column) == size for column in columns.values())
        np.testing.assert_array_equal(order["_identity"],
                                      [int(value.removeprefix("AG")) for value in columns["AgencyID"]])
        rows = [{name: column[index].item() for name, column in columns.items()} for index in range(size)]
        self.batch_sizes.append(size)
        self.snapshots.setdefault(self.config.day(day), []).extend(rows)


@pytest.mark.parametrize("seed", [0, 7, 2**63 - 1])
def test_personal_columns_agree_with_scalar_order_and_batching(seed):
    """Neither reordered identities nor a different block split changes any field."""
    identifiers = np.asarray([1, 19, 3, 100, 100000, 99999999999])
    full = personal_columns(identifiers, seed)
    reordered = personal_columns(identifiers[::-1], seed)
    chunks = [personal_columns(block, seed) for block in np.array_split(identifiers, 3)]
    for name, column in full.items():
        np.testing.assert_array_equal(column, reordered[name][::-1])
        np.testing.assert_array_equal(column, np.concatenate([chunk[name] for chunk in chunks]))
        assert column.tolist() == [personal_values(int(identifier), seed)[name] for identifier in identifiers]
    changed = personal_columns(identifiers, seed + 1)
    assert any(not np.array_equal(full[name], changed[name]) for name in full)


def test_scalar_cache_does_not_share_mutable_personal_records():
    """A recorder can change its returned dictionary without changing later views."""
    original = personal_values(23, 17)
    edited = personal_values(23, 17)
    edited["last_name"] = "Changed"
    assert personal_values(23, 17) == original
    assert personal_values(23, 17, "employee") != original


def test_names_addresses_and_contacts_are_plausible_and_coherent():
    """Common names vary while addresses and safe contact fields remain consistent."""
    identifiers = np.arange(1, 10001)
    people = personal_columns(identifiers, 97)
    names = _definition_rows("given_names.csv")
    gender_names = {gender: {row["name"] for row in names if row["gender"] == gender} for gender in ("M", "F")}
    for gender in ("M", "F"):
        selected = people["gender"] == gender
        assert .4 < selected.mean() < .6
        assert set(people["first_name"][selected]) <= gender_names[gender]
    assert len(set(people["first_name"])) > 200
    assert len(set(people["last_name"])) > 450
    surname_counts = dict(zip(*np.unique(people["last_name"], return_counts=True), strict=True))
    assert surname_counts["Smith"] > surname_counts["Orozco"] * 10
    assert .80 < np.count_nonzero(people["middle_initial"] != "") / len(identifiers) < .90
    assert all(not value or (len(value) == 1 and value.isupper()) for value in people["middle_initial"])
    locations = {(row["city"], row["state"], row["postal_code"], row["area_code"]) for row in _definition_rows("locations.csv")}
    actual_locations = set(zip(people["city"], people["state"], people["postal_code"], people["phone_area"], strict=True))
    assert actual_locations == locations
    assert "02108" in people["postal_code"]  # Leading zeros are part of a postal code.
    assert set(people["country"]) == {"USA"}
    for index in range(len(identifiers)):
        house, street, kind = people["address_line1"][index].split()
        assert 1 <= int(house) <= 9999
        assert street in personal_data.STREET_NAMES
        assert kind in personal_data.STREET_TYPES
        assert people["phone_local"][index].startswith("55501")
        assert len(people["phone_local"][index]) == 7
        assert people["phone"][index] == people["phone_area"][index] + people["phone_local"][index]
    addresses = set(zip(people["address_line1"], people["address_line2"], people["postal_code"], strict=True))
    assert len(addresses) > .99 * len(identifiers)
    assert len(set(people["email"])) == len(identifiers)
    assert all(value.endswith("@example.com") and len(value) <= 50 for value in people["email"])
    assert all(not value or (value.endswith("@example.net") and len(value) <= 50) for value in people["alternate_email"])


def test_all_personal_source_fields_keep_existing_widths_and_identifiers():
    """Long valid source IDs do not overflow personal fields or change source keys."""
    config = _short_config()
    for identifier in [1, 100, 99999999999]:
        customer = customer_values(identifier, 2, config.seed)
        prospect = _prospect_values(identifier, identifier, config.cutover, config.seed)
        account = account_values(config, {"account_id": identifier, "customer_id": identifier, "broker_id": 1, "tax_status": 1})
        assert customer["C_ID"] == account["CA_C_ID"] == identifier
        assert customer["C_TIER"] == 2
        assert prospect["AgencyID"] == f"AG{identifier}"
        assert customer["C_L_NAME"] in account["CA_NAME"]
        assert str(identifier) in account["CA_NAME"]
        assert "Brokerage" in account["CA_NAME"]
        _assert_source_widths("Customer", customer)
        _assert_source_widths("Account", account)
        _assert_source_widths("Prospect", prospect)
        retirement = account_values(config, {"account_id": identifier, "customer_id": identifier, "broker_id": 1, "tax_status": 2})
        assert "Retirement" in retirement["CA_NAME"]
        _assert_source_widths("Account", retirement)
    # The original two-argument scalar interface remains available.
    assert customer_values(1, 2) == customer_values(1, 2, 0)


def test_prospect_arrays_keep_scalar_fields_and_business_values():
    """Batch selection preserves scalar income, ages, field order, and matching data."""
    identifiers = np.asarray([1, 19, 55, 799])
    identities = np.asarray([51, 19, 30, 950])
    snapshot = date(2019, 1, 1)
    years = np.asarray([1941, 1959, 1970, 1984])
    columns = _prospect_columns(identifiers, identities, snapshot, 7, years)
    assert tuple(columns) == tuple(load_sources()["Prospect"].columns)
    for index, (identifier, identity) in enumerate(zip(identifiers, identities, strict=True)):
        row = _prospect_values(int(identifier), int(identity), snapshot, 7, int(years[index]))
        assert list(row) == list(columns)
        assert row == {name: values[index].item() for name, values in columns.items()}
        assert row["Income"] == 25000 + int(uniform(int(identifier), 7, "prospect-income") * 175000)
        assert row["Age"] == snapshot.year - years[index]
        assert row["NetWorth"] == row["Income"] * (2 + int(identifier) % 10)
        customer = customer_values(int(identity), seed=7)
        for prospect_name, customer_name in MATCH_FIELDS:
            assert row[prospect_name] == customer[customer_name]
        assert row["Gender"] == customer["C_GNDR"]
        assert row["Phone"] == customer["C_AREA_1"] + customer["C_LOCAL_1"]


def test_snapshots_match_customers_without_scalar_selection_and_repeat_across_batches(tmp_path, monkeypatch):
    """Matched prospects keep complete matching fields across monthly export splits."""
    config = _short_config()
    population = build_population(config, tmp_path)
    first = ProspectCapture(config)

    def reject_scalar(*args, **kwargs):
        """Fail if production prospect generation selects fields one person at a time."""
        raise AssertionError("Prospects must select fields in arrays.")

    monkeypatch.setattr(customers, "_prospect_values", reject_scalar)
    generate_prospects(first, population)
    second = ProspectCapture(replace(config, export_rows=101))
    generate_prospects(second, population)
    assert first.snapshots == second.snapshots
    assert max(first.batch_sizes) <= 7
    assert max(second.batch_sizes) > 7
    identities = np.load(population.path / "prospects.npy")
    assert np.any(identities["customer_id"] > 0)
    customer_keys = {
        tuple(customer_values(int(identifier), seed=config.seed)[name] for _, name in MATCH_FIELDS)
        for identifier in population.customers["customer_id"]
    }
    for snapshot, rows in first.snapshots.items():
        for row in rows:
            prospect = int(row["AgencyID"].removeprefix("AG"))
            customer_id = int(identities[prospect - 1]["customer_id"])
            identity = customer_id or len(population.customers) + prospect
            assert row["Age"] == snapshot.year - customer_birth_year(config, identity)
            key = tuple(row[name] for name, _ in MATCH_FIELDS)
            if customer_id:
                assert key in customer_keys
                customer = customer_values(customer_id, seed=config.seed)
                assert key == tuple(customer[name] for _, name in MATCH_FIELDS)
            else:
                assert key not in customer_keys
            _assert_source_widths("Prospect", row)
    initial = {row["AgencyID"]: row for row in first.snapshots[config.cutover]}
    latest = {row["AgencyID"]: row for row in first.snapshots[max(first.snapshots)]}
    for identifier in initial.keys() & latest.keys():
        assert {name: value for name, value in initial[identifier].items() if name != "Age"} == {
            name: value for name, value in latest[identifier].items() if name != "Age"
        }


def test_hr_roster_uses_real_names_and_consistent_branch_phones():
    """HR retains broker identity rules while names and branch contacts vary."""
    config = _short_config(accounts_per_broker=5)
    columns = employee_columns(config)
    assert len(columns["EmployeeID"]) == config.brokers * 4
    assert np.count_nonzero(columns["EmployeeJobCode"] == 314) == config.brokers
    locations = {(row["city"], row["area_code"]) for row in _definition_rows("locations.csv")}
    for index, identifier in enumerate(columns["EmployeeID"]):
        row = {name: column[index].item() for name, column in columns.items()}
        person = personal_values(int(identifier), config.seed, "employee")
        assert row["EmployeeFirstName"] == person["first_name"]
        assert row["EmployeeLastName"] == person["last_name"]
        assert row["EmployeeMI"] == person["middle_initial"]
        assert (row["EmployeeBranch"], row["EmployeePhone"][:3]) in locations
        _assert_source_widths("HR", row)
    repeat = employee_columns(config)
    for name in columns:
        np.testing.assert_array_equal(columns[name], repeat[name])
    assert len(set(columns["EmployeeFirstName"])) > 1


def test_empty_personal_arrays_and_bad_shapes():
    """An empty group stays empty; mismatched or nested identity arrays fail clearly."""
    assert all(column.shape == (0,) for column in personal_columns([], 7).values())
    assert all(column.shape == (0,) for column in _prospect_columns([], [], date(2019, 1, 1), 7).values())
    with pytest.raises(ValueError, match="one-dimensional"):
        personal_columns([[1, 2]], 7)
    with pytest.raises(ValueError, match="same shape"):
        _prospect_columns([1, 2], [1], date(2019, 1, 1), 7)
