"""Check population plans, actual lifetime publication, and personal source fields."""

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
    """Create a small firm across cutover while preserving strict default policies."""
    return Config(initial_start="2018-01-01", initial_end="2018-12-31",
                  daily_start="2019-01-01", daily_end="2020-12-31",
                  customers_per_unit=500, companies=2, securities=3, **kwargs)


def parse_action(action):
    """Parse one CMS action inside the required TPC-DI namespace wrapper."""
    return ET.fromstring(f'<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">{action}</TPCDI:Actions>')[0]


@given(seed=st.integers(min_value=0, max_value=2**63 - 1),
       count=st.integers(min_value=1, max_value=300),
       margin_share=st.sampled_from([0, .2, .75, 1]))
def test_account_trait_bounds(seed, count, margin_share):
    """Hidden account traits respect wealth, profile, tax, broker, and margin bounds."""
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
    """Worker and group settings cannot change planned identities or account traits."""
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


@pytest.fixture(scope="module")
def dynamic_run(tmp_path_factory):
    """Run real market and activity stages before inspecting realized lifetimes."""
    from data.data_source.generator.activity import generate_activity
    from data.data_source.generator.generation import Generation
    from data.data_source.generator.market import Market
    config = Config(initial_start="2018-01-01", initial_end="2018-12-31", daily_start="2019-01-01", daily_end="2019-12-31",
                    customers_per_unit=128, companies=2, securities=3, annual_closure=.3, annual_inactivation=.2,
                    watch_daily_rate=.005, customer_group_size=31)
    root = tmp_path_factory.mktemp("dynamic-population")
    generation = Generation(config, root)
    market = Market.build(config, root, generation)
    population = build_population(config, root)
    before = population.accounts.copy()
    summary = generate_activity(generation, market, population)
    generation.close()
    return config, root, population, before, summary


def test_runtime_lifetimes_replace_plans_and_preserve_ownership(dynamic_run):
    """Actual hazards update mmap lifetimes without creating invalid references."""
    config, root, population, before, summary = dynamic_run
    customers, accounts = population.customers, population.accounts
    opened = accounts[accounts["open_day"] < config.total_days]
    assert np.any(customers["inactivation_day"] < config.total_days)
    assert np.any(accounts["open_day"] != before["open_day"])
    assert np.all(opened["open_day"] <= opened["close_day"])
    owners = customers[opened["customer_id"] - 1]
    assert np.all(opened["open_day"] >= owners["open_day"])
    assert np.all(opened["close_day"] <= owners["inactivation_day"])
    assert np.unique(opened["account_id"]).size == len(opened)
    for customer in customers[customers["inactivation_day"] < config.total_days]:
        owned = opened[opened["customer_id"] == customer["customer_id"]]
        assert np.any(owned["close_day"] == customer["inactivation_day"])
    hidden = pl.concat([pl.read_parquet(path) for path in sorted((root / "summary/hidden").glob("accounts-*.parquet"))])
    assert hidden.height == len(opened)
    assert hidden.schema["open_date"] == hidden.schema["close_date"] == pl.Date
    assert hidden["account_id"].to_list() == opened["account_id"].tolist()
    assert summary["closures"] == np.count_nonzero(opened["close_day"] < config.total_days)
    assert summary["generation_stage"] == 2
    plans = pl.concat([pl.read_parquet(path) for path in sorted((root / "summary/hidden").glob("attrition-*.parquet"))])
    assert plans.schema["classification"] == pl.String
    assert plans.schema["final_plan"] == pl.Boolean
    final_closed = plans.filter(pl.col("classification").is_in(["closed_warning", "closed_no_warning"]))
    assert final_closed.height == summary["closures"]
    assert final_closed["final_plan"].all()
    assert final_closed.filter(pl.col("classification") == "closed_warning").height == summary["warning_closures"]
    assert plans.filter(pl.col("kind") == "outflow_candidate").height == summary.get("outflow_candidates", 0)


def test_global_risk_curve_ignores_execution_groups(tmp_path):
    """Expected normalization depends on joining traits, never future losses."""
    config = Config(customers_per_unit=64, securities=3, companies=2)
    first = build_population(config, tmp_path / "first")
    second = build_population(replace(config, customer_group_size=7, workers=2), tmp_path / "second")
    np.testing.assert_array_equal(np.load(first.path / "risk_normalizer.npy"), np.load(second.path / "risk_normalizer.npy"))
    assert np.all(first.customers["inactivation_day"] == config.total_days)
    assert np.all(first.accounts["close_day"] == config.total_days)


def test_static_cutover_target_and_account_lifetimes(tmp_path):
    """The stage 1 policy retains its exact preplanned cutover count."""
    config = short_config(attrition_enabled=False)
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


def test_static_customer_inactivation_closes_remaining_accounts_together(tmp_path):
    """Static inactivation closes every remaining owned account."""
    config = short_config(attrition_enabled=False, annual_closure=0.3, annual_inactivation=0.2)
    population = build_population(config, tmp_path)
    customers, accounts = population.customers, population.accounts
    inactive = customers[customers["inactivation_day"] < config.total_days]
    assert len(inactive)
    for customer in inactive:
        owned = accounts[accounts["customer_id"] == customer["customer_id"]]
        assert np.all(owned["open_day"] < customer["inactivation_day"])
        assert np.all(owned["close_day"] <= customer["inactivation_day"])
        assert np.any(owned["close_day"] == customer["inactivation_day"])


def test_static_full_calendar_rates_and_growth(tmp_path):
    """The retained stage 1 model supplies the original rate and growth baseline."""
    config = Config(customers_per_unit=10000, attrition_enabled=False)
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


def test_closures_use_nyse_sessions(dynamic_run):
    """Every realized liquidation date must belong to a real NYSE session."""
    from data.data_source.generator.config import calendar_for
    config, _, population, _, _ = dynamic_run
    sessions = {session.date() for session in calendar_for(config.start, config.end).sessions}
    closes = population.accounts["close_day"]
    realized = np.unique(closes[closes < config.total_days])
    assert len(realized) > 0
    for day in realized:
        assert config.day(int(day)) in sessions


def test_tiers_rank_complete_cohort_and_break_ties_by_customer_id():
    """Rank joining wealth across the cohort and resolve equal wealth by customer ID."""
    customers = np.zeros(10, dtype=CUSTOMER_DTYPE)
    customers["customer_id"] = np.arange(1, 11)
    accounts = np.zeros(10, dtype=ACCOUNT_DTYPE)
    accounts["customer_id"] = np.arange(1, 11)
    accounts["wealth_cents"] = [100, 200, 300, 400, 500, 600, 700, 800, 900, 900]
    assign_tiers(customers, accounts)
    assert customers["tier"].tolist() == [3, 3, 3, 3, 3, 3, 2, 2, 1, 2]
    assert np.bincount(customers["tier"], minlength=4)[1:].tolist() == [1, 3, 6]


def test_tiers_use_monthly_cohorts_not_small_daily_groups():
    """Customers joining in one month share one wealth ranking despite different dates."""
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
    """Nondefault tier shares and source tax codes reach the generated population."""
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
    """Later replacement wealth cannot change the tier assigned at customer joining."""
    customers = np.zeros(10, dtype=CUSTOMER_DTYPE)
    customers["customer_id"] = np.arange(1, 11)
    accounts = np.zeros(12, dtype=ACCOUNT_DTYPE)
    accounts["customer_id"] = [*range(1, 11), 1, 10]
    accounts["wealth_cents"] = [100] * 10 + [1000, 1000000]
    accounts["open_day"][-1] = 1
    assign_tiers(customers, accounts)
    assert customers["tier"][0] == 1
    assert customers["tier"][-1] == 3


def test_static_hidden_accounts_are_typed_and_have_dates(tmp_path):
    """Static evidence is typed; dynamic evidence follows business execution."""
    config = short_config(attrition_enabled=False)
    population = build_population(config, tmp_path)
    paths = sorted((tmp_path / "summary" / "hidden").glob("accounts-*.parquet"))
    frame = pl.concat([pl.read_parquet(path) for path in paths])
    assert frame.height == len(population.accounts)
    assert frame.schema["open_date"] == pl.Date
    assert frame.schema["close_date"] == pl.Date
    assert frame.schema["wealth_cents"] == pl.Int64
    assert frame.filter(pl.col("close_day") == config.total_days)["close_date"].null_count() > 0


def test_xml_preserves_omitted_and_explicit_empty_values():
    """CMS updates keep omitted fields distinct from fields explicitly cleared."""
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
    """Partial account updates omit unchanged fields and escape XML-sensitive text."""
    action = parse_action(xml_action("UPDACCT", "2018-01-01T12:00:00", 7,
                                    account_values=[{"CA_ID": 4, "CA_NAME": "A & B"}, {"CA_ID": 5}]))
    accounts = action.findall("Customer/Account")
    assert accounts[0].attrib == {"CA_ID": "4"}
    assert accounts[0].find("CA_NAME").text == "A & B"
    assert accounts[0].find("CA_B_ID") is None
    assert accounts[1].attrib == {"CA_ID": "5"}
    assert not list(accounts[1])


def test_new_xml_matches_tpcdi_schema(tmp_path):
    """A generated NEW action satisfies the supplied TPC-DI XML schema."""
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
    """Supported calendar endpoints keep customer births and prospect ages in range."""
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
            prospect = int(row["AgencyID"].removeprefix("AG"))
            matched = np.load(population.path / "prospects.npy")[prospect - 1]["customer_id"]
            identity = int(matched) if matched else len(population.customers) + prospect
            assert row["Age"] == snapshot.year - customer_birth_year(config, identity)


def test_emitted_cms_matches_seeded_prospects_and_preserves_updated_names(tmp_path):
    """Emitted CMS, prospect, and account-update fields preserve one seeded identity."""
    from types import SimpleNamespace
    from data.data_source.generator.activity import BusinessSimulation
    config = Config(initial_start="2019-01-01", initial_end="2019-01-31", daily_start="2019-02-01", daily_end="2019-03-03",
                    seed=987654321, customers_per_unit=128, companies=1, securities=1, annual_growth=0,
                    annual_closure=0, annual_inactivation=0, annual_customer_update=0, annual_account_update=1,
                    annual_trade_rates=(0, 0, 0, 0, 0), annual_deposit_rates=(0, 0, 0, 0, 0),
                    annual_withdrawal_rates=(0, 0, 0, 0, 0), watch_daily_rate=0,
                    customer_tier_error_rate=0, customer_dob_error_rate=0)
    population = build_population(config, tmp_path)
    text_rows, emitted = [], []
    generation = SimpleNamespace(config=config, text=lambda path, rows: text_rows.extend(rows),
                                 emit=lambda source, day, rows: emitted.append((source, day, rows)), cash=lambda *args: None)
    prices = {"close": np.array([10000]), "low": np.array([9000]), "high": np.array([11000]),
              "dividends": np.array([0]), "trading": True, "close_second": 57600, "shock": False}
    market = SimpleNamespace(day=lambda day: prices, sessions_per_year={2019: 252})
    BusinessSimulation(generation, market, population.customers.copy(), population.accounts.copy()).run()
    actions = [parse_action(payload) for _, payload in text_rows]
    customers, base_names = {}, {}
    initial_updates = []
    for action in actions:
        customer = action.find("Customer")
        if action.attrib["ActionType"] == "NEW":
            customers[int(customer.attrib["C_ID"])] = customer
            for account in customer.findall("Account"):
                base_names[int(account.attrib["CA_ID"])] = account.findtext("CA_NAME")
        elif action.attrib["ActionType"] == "UPDACCT":
            initial_updates.extend(customer.findall("Account"))
    prospects = ProspectCapture(config)
    generate_prospects(prospects, population)
    identities = np.load(population.path / "prospects.npy")
    checked = 0
    for row in prospects.snapshots[config.cutover]:
        agency = int(row["AgencyID"].removeprefix("AG"))
        identifier = int(identities[agency - 1]["customer_id"])
        if not identifier:
            continue
        customer = customers[identifier]
        for path, column in (("Name/C_L_NAME", "LastName"), ("Name/C_F_NAME", "FirstName"),
                             ("Address/C_ADLINE1", "AddressLine1"), ("Address/C_CITY", "City"),
                             ("Address/C_STATE_PROV", "State"), ("Address/C_ZIPCODE", "PostalCode")):
            assert customer.findtext(path) == row[column]
        checked += 1
    assert checked > 0
    assert initial_updates
    for account in initial_updates:
        identifier = int(account.attrib["CA_ID"])
        recorded = account.findtext("CA_NAME")
        assert " v" in recorded and len(recorded) <= 50
        assert base_names[identifier].startswith(recorded.rsplit(" v", 1)[0])
        assert not recorded.startswith("Account ")
    daily_updates = [row for source, _, rows in emitted if source == "Account" for row in rows if row["_flag"] == "U"]
    assert daily_updates
    for row in daily_updates:
        assert " v" in row["CA_NAME"] and len(row["CA_NAME"]) <= 50
        assert base_names[row["CA_ID"]].startswith(row["CA_NAME"].rsplit(" v", 1)[0])
        assert not row["CA_NAME"].startswith("Account ")


def test_prospect_default_birth_year_matches_reference_calendar():
    """The prospect helper preserves its explicit and default reference-year behavior."""
    default = _prospect_values(1, 1, date(2019, 1, 1), 7)
    explicit = _prospect_values(1, 1, date(2019, 1, 1), 7, 1941)
    assert default == explicit
    assert default["Age"] == 78
    assert _prospect_values(1, 1, date(2019, 1, 1), 7, 1960)["Age"] == 59
    assert customer_birth_year(Config()) == 1960


def test_reference_calendar_preserves_varied_birth_year_values_and_field_order():
    """Reference dates retain varied birth years and the ordered prospect field contract."""
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
    """Collect prospect batches by snapshot without replacing field generation."""
    def __init__(self, config):
        """Collect bounded prospect batches by snapshot date for source checks."""
        self.config = config
        self.snapshots = {}

    def emit(self, name, day, rows):
        """Capture a prospect batch and enforce the batch-size bound."""
        assert name == "Prospect"
        assert len(rows) <= 2048
        self.snapshots.setdefault(day, []).extend(rows)


def test_prospects_keep_identity_without_future_customer_status(tmp_path):
    """Monthly prospects retain personal identity without exposing future customer status."""
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
        prospect = int(row["AgencyID"].removeprefix("AG"))
        matched = identities[prospect - 1]["customer_id"]
        identity = int(matched) if matched else len(population.customers) + prospect
        assert row["Age"] == config.cutover.year - customer_birth_year(config, identity)
    day = (config.cutover - config.start).days
    active_accounts = np.count_nonzero((population.accounts["open_day"] <= day) & (population.accounts["close_day"] > day))
    assert len(snapshots[0]) == active_accounts * 2
