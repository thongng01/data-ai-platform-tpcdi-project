from dataclasses import replace
from datetime import date
from pathlib import Path
from xml.etree import ElementTree as ET

import numpy as np
import polars as pl
import pytest
from hypothesis import given, strategies as st

from data.data_source.generator.config import Config
from data.data_source.generator.customers import _prospect_values, account_values, customer_birth_year, customer_values, generate_prospects, xml_action
from data.data_source.generator.population import ACCOUNT_DTYPE, CUSTOMER_DTYPE, _account_traits, assign_tiers, build_population


def short_config(**kwargs):
    return Config(initial_start="2018-01-01", initial_end="2018-12-31",
                  daily_start="2019-01-01", daily_end="2020-12-31",
                  customers_per_unit=500, companies=2, securities=3, **kwargs)


def parse_action(action):
    return ET.fromstring(f'<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">{action}</TPCDI:Actions>')[0]


@given(seed=st.integers(min_value=0, max_value=2**63 - 1),
       count=st.integers(min_value=1, max_value=300),
       margin_share=st.sampled_from([0, .2, .75, 1]))
def test_account_trait_bounds(seed, count, margin_share):
    config = short_config(seed=seed, margin_share=margin_share)
    accounts = _account_traits(config, np.arange(1, count + 1))
    assert np.all(accounts["wealth_cents"] >= config.wealth_min * 100)
    assert np.all(accounts["wealth_cents"] <= config.wealth_max * 100)
    assert np.all(accounts["margin_limit_cents"][accounts["is_cash"]] == 0)
    assert np.all(accounts["margin_limit_cents"] <= accounts["wealth_cents"])
    assert np.all((accounts["profile"] >= 0) & (accounts["profile"] <= 4))
    assert np.all((accounts["broker_id"] >= 1) & (accounts["broker_id"] <= config.brokers))
    if margin_share == 0:
        assert np.all(accounts["is_cash"])
    if margin_share == 1:
        assert np.all(~accounts["is_cash"])


def test_population_is_independent_of_execution_groups(tmp_path):
    config = short_config()
    first = build_population(config, tmp_path / "one")
    other = build_population(replace(config, customer_group_size=7, workers=2, threads=1, merge_fan_in=2), tmp_path / "seven")
    assert first.customers.tobytes() == other.customers.tobytes()
    assert first.accounts.tobytes() == other.accounts.tobytes()
    assert first.slot_stride == other.slot_stride
    assert np.concatenate([accounts for _, accounts in first.groups(7)]).tobytes() == first.accounts.tobytes()
    assert np.concatenate([customers for customers, _ in first.groups(1)]).tobytes() == first.customers.tobytes()
    assert isinstance(first.accounts, np.memmap)
    assert not first.accounts.flags.writeable


def test_cutover_target_and_account_lifetimes(tmp_path):
    config = short_config()
    population = build_population(config, tmp_path)
    customers, accounts = population.customers, population.accounts
    day = (config.cutover - config.start).days
    active = (customers["open_day"] <= day) & (customers["inactivation_day"] > day)
    assert np.count_nonzero(active) == config.cutover_customers
    assert np.all(accounts["open_day"] < accounts["close_day"])
    owners = customers[accounts["customer_id"] - 1]
    assert np.all(accounts["open_day"] >= owners["open_day"])
    assert np.all(accounts["close_day"] <= owners["inactivation_day"])
    assert np.unique(accounts["account_id"]).size == len(accounts)
    assert np.all(accounts["account_id"] < 10**9)
    assert np.all((accounts["account_id"] - 1) // population.slot_stride + 1 == accounts["customer_id"])


def test_customer_inactivation_closes_remaining_accounts_together(tmp_path):
    config = short_config(annual_closure=0.3, annual_inactivation=0.2)
    population = build_population(config, tmp_path)
    customers, accounts = population.customers, population.accounts
    inactive = customers[customers["inactivation_day"] < config.total_days]
    assert len(inactive)
    for customer in inactive:
        owned = accounts[accounts["customer_id"] == customer["customer_id"]]
        assert np.all(owned["open_day"] < customer["inactivation_day"])
        assert np.all(owned["close_day"] <= customer["inactivation_day"])
        assert np.any(owned["close_day"] == customer["inactivation_day"])


def test_full_calendar_rates_and_growth(tmp_path):
    config = Config(customers_per_unit=10000)
    population = build_population(config, tmp_path)
    customers, accounts = population.customers, population.accounts
    account_exposure = np.sum(accounts["close_day"] - accounts["open_day"]) / 365.25
    customer_exposure = np.sum(customers["inactivation_day"] - customers["open_day"]) / 365.25
    assert np.count_nonzero(accounts["close_day"] < config.total_days) / account_exposure == pytest.approx(.03, abs=.002)
    assert np.count_nonzero(customers["inactivation_day"] < config.total_days) / customer_exposure == pytest.approx(.02, abs=.002)
    opening_rate = np.count_nonzero(accounts["open_day"] > 0) / account_exposure
    assert opening_rate == pytest.approx(.11, abs=.005)
    first_count = np.count_nonzero(accounts["open_day"] == 0)
    last_count = np.count_nonzero(accounts["close_day"] == config.total_days)
    growth = (last_count / first_count) ** (365.25 / (config.total_days - 1)) - 1
    assert growth == pytest.approx(.08, abs=.003)
    profile_shares = np.bincount(accounts["profile"], minlength=5) / len(accounts)
    assert profile_shares == pytest.approx(config.profile_shares, abs=.01)
    assert np.mean(~accounts["is_cash"]) == pytest.approx(config.margin_share, abs=.01)


def test_closures_use_nyse_sessions(tmp_path):
    from data.data_source.generator.config import calendar_for
    config = short_config(annual_closure=.5, annual_inactivation=.2)
    population = build_population(config, tmp_path)
    sessions = {session.date() for session in calendar_for(config.start, config.end).sessions}
    closes = population.accounts["close_day"]
    for day in np.unique(closes[closes < config.total_days]):
        assert config.day(int(day)) in sessions


def test_tiers_rank_complete_cohort_and_break_ties_by_customer_id():
    customers = np.zeros(10, dtype=CUSTOMER_DTYPE)
    customers["customer_id"] = np.arange(1, 11)
    accounts = np.zeros(10, dtype=ACCOUNT_DTYPE)
    accounts["customer_id"] = np.arange(1, 11)
    accounts["wealth_cents"] = [100, 200, 300, 400, 500, 600, 700, 800, 900, 900]
    assign_tiers(customers, accounts)
    assert customers["tier"].tolist() == [3, 3, 3, 3, 3, 3, 2, 2, 1, 2]
    assert np.bincount(customers["tier"], minlength=4)[1:].tolist() == [1, 3, 6]


def test_tiers_use_monthly_cohorts_not_small_daily_groups():
    customers = np.zeros(10, dtype=CUSTOMER_DTYPE)
    customers["customer_id"] = np.arange(1, 11)
    customers["open_day"] = np.arange(1, 11)
    accounts = np.zeros(10, dtype=ACCOUNT_DTYPE)
    accounts["customer_id"] = np.arange(1, 11)
    accounts["open_day"] = np.arange(1, 11)
    accounts["wealth_cents"] = np.arange(1, 11) * 100
    assign_tiers(customers, accounts, date(2018, 1, 1))
    assert customers["tier"].tolist() == [3, 3, 3, 3, 3, 3, 2, 2, 2, 1]


def test_configured_tier_and_tax_shares(tmp_path):
    config = short_config(tier_shares=(.2, .5, .3), tax_shares=(0, 1, 0))
    population = build_population(config, tmp_path)
    initial = population.customers[population.customers["open_day"] == 0]
    counts = np.bincount(initial["tier"], minlength=4)[1:]
    assert counts / len(initial) == pytest.approx(config.tier_shares, abs=1 / len(initial))
    assert np.all(population.accounts["tax_status"] == 0)
    for shares, code in (((1, 0, 0), 1), ((0, 0, 1), 2)):
        rows = _account_traits(replace(config, tax_shares=shares), np.arange(1, 101))
        assert np.all(rows["tax_status"] == code)


def test_tier_uses_total_joining_wealth_not_replacement_wealth():
    customers = np.zeros(10, dtype=CUSTOMER_DTYPE)
    customers["customer_id"] = np.arange(1, 11)
    accounts = np.zeros(12, dtype=ACCOUNT_DTYPE)
    accounts["customer_id"] = [*range(1, 11), 1, 10]
    accounts["wealth_cents"] = [100] * 10 + [1000, 1000000]
    accounts["open_day"][-1] = 1
    assign_tiers(customers, accounts)
    assert customers["tier"][0] == 1
    assert customers["tier"][-1] == 3


def test_hidden_accounts_are_typed_and_have_dates(tmp_path):
    config = short_config()
    population = build_population(config, tmp_path)
    paths = sorted((tmp_path / "summary" / "hidden").glob("accounts-*.parquet"))
    frame = pl.concat([pl.read_parquet(path) for path in paths])
    assert frame.height == len(population.accounts)
    assert frame.schema["open_date"] == pl.Date
    assert frame.schema["close_date"] == pl.Date
    assert frame.schema["wealth_cents"] == pl.Int64
    assert frame.filter(pl.col("close_day") == config.total_days)["close_date"].null_count() > 0


def test_xml_preserves_omitted_and_explicit_empty_values():
    action = parse_action(xml_action("UPDCUST", "2018-01-01T12:00:00", 7,
                                    {"C_M_NAME": None, "C_EMAIL_2": ""}))
    customer = action.find("Customer")
    assert customer.attrib == {"C_ID": "7"}
    assert customer.find("Name/C_F_NAME") is None
    assert customer.find("Name/C_M_NAME") is not None
    assert customer.find("Name/C_M_NAME").text is None
    assert customer.find("ContactInfo/C_ALT_EMAIL") is not None
    assert customer.find("Address") is None


def test_xml_accounts_can_be_partial_and_escaped():
    action = parse_action(xml_action("UPDACCT", "2018-01-01T12:00:00", 7,
                                    account_values=[{"CA_ID": 4, "CA_NAME": "A & B"}, {"CA_ID": 5}]))
    accounts = action.findall("Customer/Account")
    assert accounts[0].attrib == {"CA_ID": "4"}
    assert accounts[0].find("CA_NAME").text == "A & B"
    assert accounts[0].find("CA_B_ID") is None
    assert accounts[1].attrib == {"CA_ID": "5"}
    assert not list(accounts[1])


def test_new_xml_matches_tpcdi_schema(tmp_path):
    etree = pytest.importorskip("lxml.etree")
    config = short_config()
    population = build_population(config, tmp_path)
    values = customer_values(1, 2)
    account = account_values(config, population.accounts[0])
    action = xml_action("NEW", "2018-01-01T12:00:00", 1, values, [account])
    root = etree.fromstring(f'<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">{action}</TPCDI:Actions>'.encode())
    schema = etree.XMLSchema(etree.parse(str(Path(__file__).with_name("CustomerMgmt.xsd"))))
    schema.assertValid(root)


@pytest.mark.parametrize("year,expected_birth_year", [(1902, 1884), (2019, 1960), (2100, 1980)])
def test_birth_year_keeps_prospect_ages_valid_at_calendar_limits(tmp_path, year, expected_birth_year):
    config = Config(initial_start=f"{year}-01-01", initial_end=f"{year}-01-31",
                    daily_start=f"{year}-02-01", daily_end=f"{year}-03-31",
                    customers_per_unit=10, companies=2, securities=3)
    birth_year = customer_birth_year(config)
    assert birth_year == expected_birth_year
    for identifier in range(1, 91):
        birth_date = date(customer_birth_year(config, identifier), 1, 1)
        assert birth_date < config.start
        fields = {**customer_values(identifier), "C_DOB": birth_date.isoformat()}
        action = parse_action(xml_action("NEW", f"{config.start}T00:00:00", identifier, fields))
        assert date.fromisoformat(action.find("Customer").get("C_DOB")) < config.start
    population = build_population(config, tmp_path)
    generation = ProspectCapture(config)
    generate_prospects(generation, population)
    for snapshot, rows in generation.snapshots.items():
        assert all(0 <= row["Age"] <= 120 for row in rows)
        for row in rows:
            identity = int(row["LastName"].removeprefix("Family"))
            assert row["Age"] == snapshot.year - customer_birth_year(config, identity)


def test_prospect_default_birth_year_matches_reference_calendar():
    default = _prospect_values(1, 1, date(2019, 1, 1), 7)
    explicit = _prospect_values(1, 1, date(2019, 1, 1), 7, 1941)
    assert default == explicit
    assert default["Age"] == 78
    assert _prospect_values(1, 1, date(2019, 1, 1), 7, 1960)["Age"] == 59
    assert customer_birth_year(Config()) == 1960


def test_reference_calendar_preserves_varied_birth_year_values_and_field_order():
    config = Config()
    years = set()
    for identity in range(1, 91):
        original_birth_year = date.fromisoformat(customer_values(identity)["C_DOB"]).year
        bounded_birth_year = customer_birth_year(config, identity)
        assert bounded_birth_year == original_birth_year
        original = _prospect_values(identity, identity, config.cutover, config.seed)
        bounded = _prospect_values(identity, identity, config.cutover, config.seed, bounded_birth_year)
        assert tuple(original.items()) == tuple(bounded.items())
        years.add(bounded_birth_year)
    assert years == set(range(1940, 1985))


class ProspectCapture:
    def __init__(self, config):
        self.config = config
        self.snapshots = {}

    def emit(self, name, day, rows):
        assert name == "Prospect"
        assert len(rows) <= 2048
        self.snapshots.setdefault(day, []).extend(rows)


def test_prospects_keep_identity_without_future_customer_status(tmp_path):
    config = short_config(annual_growth=.3, export_rows=19)
    population = build_population(config, tmp_path)
    generation = ProspectCapture(config)
    generate_prospects(generation, population)
    assert list(generation.snapshots)[0] == config.cutover
    assert all(day == config.cutover or day.day == 1 for day in generation.snapshots)
    snapshots = list(generation.snapshots.values())
    initial = {row["AgencyID"]: row for row in snapshots[0]}
    latest = {row["AgencyID"]: row for row in snapshots[-1]}
    for identifier in initial.keys() & latest.keys():
        for field in ("LastName", "FirstName", "AddressLine1", "Income", "NetWorth"):
            assert initial[identifier][field] == latest[identifier][field]
    identities = np.load(population.path / "prospects.npy")
    matches = identities["customer_id"][identities["customer_id"] > 0]
    joiners = population.customers[population.customers["open_day"] > (config.cutover - config.start).days]
    matched_joiners = np.intersect1d(matches, joiners["customer_id"])
    assert len(matched_joiners) / len(joiners) == pytest.approx(.3, abs=.1)
    existing = population.customers[population.customers["open_day"] <= (config.cutover - config.start).days]
    matched_existing = np.intersect1d(matches, existing["customer_id"])
    assert len(matched_existing) / len(existing) == pytest.approx(.3, abs=.1)
    assert all("IsCustomer" not in row and "C_TIER" not in row for row in snapshots[0])
    for row in snapshots[0]:
        identity = int(row["LastName"].removeprefix("Family"))
        assert row["Age"] == config.cutover.year - customer_birth_year(config, identity)
    day = (config.cutover - config.start).days
    active_accounts = np.count_nonzero((population.accounts["open_day"] <= day) & (population.accounts["close_day"] > day))
    assert len(snapshots[0]) == active_accounts * 2
