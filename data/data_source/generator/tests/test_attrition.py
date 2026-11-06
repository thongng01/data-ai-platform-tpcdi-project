"""Check point-in-time risk, real cash effects, and leakage-free signal evidence."""

from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest

from data.data_source.generator.activity import BusinessSimulation, Stage1IntentPlanner, Order
from data.data_source.generator.attrition import AttritionPlanner, LeavingPeriod, ReturnHistory, risk_factors, three_months_before
from data.data_source.generator.business import BusinessDay, Intent, IntentKind, LIQUIDATION_SECOND
from data.data_source.generator.config import Config
from data.data_source.generator.formats import load_sources
from data.data_source.generator.population import ACCOUNT_DTYPE, CUSTOMER_DTYPE
from data.data_source.generator.random_values import uniform, uniforms
from data.data_source.generator.signal_check import AccountLifetime, CashObservation, FEATURES, evaluate, observations


def config_for_test(**settings):
    """Use a long enough calendar for real three-month history, with quiet activity."""
    defaults = dict(initial_start="2018-01-01", initial_end="2018-12-31", daily_start="2019-01-01", daily_end="2020-12-31",
                    securities=1, companies=1, customers_per_unit=1, annual_customer_update=0, annual_account_update=0,
                    annual_trade_rates=(0, 0, 0, 0, 0), annual_deposit_rates=(0, 0, 0, 0, 0),
                    annual_withdrawal_rates=(0, 0, 0, 0, 0), watch_daily_rate=0,
                    customer_tier_error_rate=0, customer_dob_error_rate=0)
    return Config(**{**defaults, **settings})


def simulation_for_test(config, planner=None, account_count=1):
    """Keep real ledger and planning code behind a small in-memory output sink."""
    customers = np.array([(1, 0, config.total_days, 3)], dtype=CUSTOMER_DTYPE)
    accounts = np.zeros(account_count, dtype=ACCOUNT_DTYPE)
    accounts["account_id"] = np.arange(1, account_count + 1)
    accounts["customer_id"] = 1
    accounts["close_day"] = config.total_days
    accounts["is_cash"] = True
    accounts["profile"] = 1
    accounts["wealth_cents"] = 100000
    accounts["tax_status"] = 1
    accounts["broker_id"] = 1
    emitted, cash = [], []
    sources = load_sources()

    def record(source, day, values, **metadata):
        """Capture typed source values while keeping source errors outside business tests."""
        if source == "CashTransaction":
            cash.append((day, *values))
        elif source in sources:
            row = dict(zip(sources[source].columns, values, strict=True))
            emitted.append((source, day, [row]))

    sink = SimpleNamespace(config=config, sources=sources, record=record, _record_fault=lambda *args: None)
    prices = {"close": np.array([10000]), "high": np.array([11000]), "low": np.array([9000]),
              "dividends": np.array([0]), "trading": True, "close_second": 57600, "shock": False}
    market = SimpleNamespace(day=lambda day: prices, sessions_per_year={year: 252 for year in range(config.start.year, config.end.year + 1)})
    return BusinessSimulation(sink, market, customers, accounts, planner=planner), emitted, cash, prices


def test_relative_risks_have_expected_values():
    """All four causes change the mathematical hazard in the accepted direction."""
    np.testing.assert_allclose(risk_factors(np.arange(5), np.full(5, 400), np.full(5, 3), np.zeros(5)), [2, 1, .5, 1.5, 1.5])
    assert risk_factors([1], [100], [3], [0])[0] == 2
    assert risk_factors([1], [400], [1], [0])[0] == .7
    assert risk_factors([1], [400], [3], [-.2])[0] == 1
    assert risk_factors([1], [400], [3], [-.20001])[0] == 1.5
    assert risk_factors([1], [400], [3], [np.nan])[0] == 1


def test_external_flows_do_not_create_investment_returns():
    """Funding changes equity, but cannot create a false gain or loss."""
    config = config_for_test()
    history = ReturnHistory(1)
    equity = 100000
    for offset in range(96):
        flow = 100000 if offset == 20 else -50000 if offset == 60 else 0
        equity += flow
        history.observe(offset, np.array([equity]), np.array([flow]))
    assert history.trailing(config, 96)[0] == pytest.approx(0)
    history.observe(96, np.array([100000]), np.array([0]))
    assert history.trailing(config, 97)[0] == pytest.approx(-1 / 3)


@pytest.mark.parametrize("last,anchor", [(date(2019, 5, 31), date(2019, 2, 28)), (date(2020, 5, 31), date(2020, 2, 29)), (date(2020, 2, 29), date(2019, 11, 29))])
def test_calendar_month_lookback(last, anchor):
    """A three-month window follows calendar months, including leap days."""
    assert three_months_before(last) == anchor
    config = config_for_test(initial_start=anchor.isoformat(), initial_end=last.isoformat(),
                             daily_start=(last + timedelta(days=1)).isoformat(), daily_end=(last + timedelta(days=2)).isoformat())
    history = ReturnHistory(1)
    days = (last - anchor).days
    for offset in range(days + 1):
        history.observe(offset, np.array([1000 if offset < days else 700]), np.array([0]))
    assert history.trailing(config, days + 1)[0] == pytest.approx(-.3)


def test_missing_history_and_capital_wipeout_are_distinct():
    """A market wipeout is loss; a full withdrawal is not investment loss."""
    config = config_for_test()
    history = ReturnHistory(3)
    assert np.all(np.isnan(history.trailing(config, 0)))
    for offset in range(100):
        equity = np.array([1000., 1000., 0.])
        flow = np.zeros(3)
        if offset >= 95:
            equity[:2] = [-100., 0.]
            if offset == 95:
                flow[1] = -1000
        history.observe(offset, equity, flow)
    result = history.trailing(config, 100)
    assert result[0] == -1
    assert np.isnan(result[1])
    assert np.isnan(result[2])


@pytest.mark.parametrize("customer_exit", [True, False])
def test_realized_past_loss_changes_both_exit_selections(customer_exit):
    """The same keyed draw can select loss risk but reject otherwise equal risk."""
    offset = 400
    purpose = "inactivation-hazard:0" if customer_exit else "closure-hazard:0"
    rate = .5 if customer_exit else .4
    lower, upper = -np.expm1(-rate / 365.2425), -np.expm1(-rate * 1.5 / 365.2425)
    key = (offset + 1) * 1_000_000_000 + 1
    seed = next(seed for seed in range(100000) if lower <= uniform(key, seed, purpose) < upper)
    outcomes = []
    for loss in (0., -.3):
        config = config_for_test(seed=seed, annual_inactivation=.5 if customer_exit else 0,
                                 annual_closure=.5 if customer_exit else .4, attrition_warning_share=0)
        planner = AttritionPlanner()
        simulation, _, _, _ = simulation_for_test(config, planner)
        planner.normalizer = np.ones(config.total_days)
        planner.returns.trailing = lambda config, day: np.array([loss])
        planner.closures(simulation, offset, ())
        outcomes.append(1 in planner.customer_plans if customer_exit else 0 in planner.leaving)
    assert outcomes == [False, True]


@pytest.mark.parametrize("customer_exit", [True, False])
def test_prior_outflow_candidate_does_not_suppress_either_hazard(customer_exit):
    """A recent outflow cannot grant its account or another owned account immunity."""
    offset = 400
    purpose = "inactivation-hazard:0" if customer_exit else "closure-hazard:0"
    rate = .5 if customer_exit else .4
    key = (offset + 1) * 1_000_000_000 + 1
    seed = next(seed for seed in range(100000) if uniform(key, seed, purpose) < -np.expm1(-rate / 365.2425))
    outcomes = []
    for prior_outflow in (False, True):
        config = config_for_test(seed=seed, annual_inactivation=.5 if customer_exit else 0,
                                 annual_closure=.5 if customer_exit else .4, attrition_warning_share=0)
        planner = AttritionPlanner()
        simulation, _, _, _ = simulation_for_test(config, planner, 2 if customer_exit else 1)
        planner.normalizer = np.ones(config.total_days)
        planner.returns.trailing = lambda config, day: np.zeros(len(simulation.accounts))
        if prior_outflow:
            planner.record_outflow_candidate(simulation, 1 if customer_exit else 0, offset - 1)
        selected = planner.closures(simulation, offset, ())
        outcomes.append(1 in planner.customer_plans if customer_exit else 0 in selected)
    assert outcomes == [True, True]


def test_summary_classifies_observed_outcomes_without_changing_lifetimes():
    """Confirm only observed nonclosures; keep observed closures and censoring separate."""
    config = config_for_test()
    planner = AttritionPlanner()
    simulation, _, _, _ = simulation_for_test(config, planner, 4)
    starts = [10, 10, config.total_days - 20, config.total_days - 20]
    closes = [40, 41, config.total_days, config.total_days - 10]
    simulation.accounts["close_day"] = closes
    for index, start in enumerate(starts):
        planner.record_outflow_candidate(simulation, index, start)
    before = simulation.accounts.copy()
    planner.finish(simulation)
    assert [row["classification"] for row in planner.records] == ["closing", "confirmed_lookalike", "censored", "closing"]
    assert simulation.metrics["outflow_candidates"] == 4
    assert simulation.metrics["confirmed_lookalikes"] == 1
    assert simulation.metrics["closing_outflow_candidates"] == 2
    assert simulation.metrics["censored_outflow_candidates"] == 1
    np.testing.assert_array_equal(simulation.accounts, before)


def test_summary_marks_superseded_plans_and_actual_final_warning():
    """An overridden proposal must not count as a second closure or warning."""
    planner = AttritionPlanner()
    simulation, _, _, _ = simulation_for_test(config_for_test(), planner)
    planner._start(simulation, 0, LeavingPeriod(10, 40, (10, 30)))
    planner._start(simulation, 0, LeavingPeriod(20, 50, (20, 40), True))
    planner.finish(simulation)
    assert [row["classification"] for row in planner.records] == ["superseded", "closed_warning"]
    assert [row["final_plan"] for row in planner.records] == [False, True]
    assert planner.records[-1]["actual_close_day"] == 50
    assert simulation.metrics["warning_closures"] == 1
    assert simulation.metrics["superseded_exit_plans"] == 1


def test_hazard_array_draws_match_scalar_selection():
    """Group size changes neither named draws nor hazard decisions."""
    ids = np.arange(1, 1001, dtype=np.uint64)
    keys = ids + np.uint64(401 * 1_000_000_000)
    draws = uniforms(keys, 1234, "inactivation-hazard:0")
    expected = [uniform(int(key), 1234, "inactivation-hazard:0") for key in keys]
    np.testing.assert_array_equal(draws, expected)
    np.testing.assert_array_equal(draws, np.concatenate([uniforms(block, 1234, "inactivation-hazard:0") for block in np.array_split(keys, 7)]))


def test_opportunity_multiplier_changes_only_outflow_opportunity_frequency():
    """The calibrated proposal draw does not change closure or ordinary activity settings."""
    base_probability = -np.expm1(-.02 / 252)
    calibrated_probability = -np.expm1(-.02 * 2.12 / 252)
    assert 0 < base_probability < calibrated_probability < 1
    key = 1_000_000_001
    seed = next(seed for seed in range(100000) if base_probability <= uniform(key, seed, "lookalike-hazard:0") < calibrated_probability)
    proposals = []
    for multiplier in (1., 2.12):
        config = config_for_test(seed=seed, lookalike_opportunity_multiplier=multiplier)
        simulation, _, _, prices = simulation_for_test(config, AttritionPlanner())
        day = BusinessDay(config.start, 0, prices, (), (), (), set(), np.array([0]))
        simulation.planner.intents(simulation, day)
        proposals.append(simulation.metrics["lookalike_proposals"])
        assert config.annual_lookalike == .02
        assert config.annual_closure == .03 and config.annual_inactivation == .02
        assert simulation.metrics["outflow_candidates"] == 0
    assert proposals == [0, 1]
    assert config_for_test().lookalike_opportunity_multiplier == 2.12


def test_position_universe_bounds_holdings_without_changing_market_paying_share():
    """Repeated buys choose from the account universe, not every market security."""
    config = config_for_test(securities=24, positions_per_account=4, order_shares=(1, 0, 0, 0))
    simulation, _, _, prices = simulation_for_test(config, Stage1IntentPlanner())
    prices = {**prices, **{name: np.full(24, value) for name, value in (("close", 10000), ("low", 9000), ("high", 11000), ("dividends", 0))}}
    simulation.ledger.balance[0] = 10000000
    for ordinal in range(100):
        simulation.submit(0, config.start, 0, 35000 + ordinal, prices, ordinal)
    assert 1 <= len(simulation.holdings.by_account[1]) <= 4
    assert config.dividend_share == .4


def test_lookalike_proposals_require_real_sale_and_withdrawal():
    """Do not count an empty account as a realized outflow look-alike."""
    config = config_for_test(annual_lookalike=1)
    simulation, _, _, prices = simulation_for_test(config, AttritionPlanner())
    offset = next(day for day in range(config.total_days - 30) if simulation.draw(1, day, "lookalike-hazard") < -np.expm1(-1 / 252))
    day = BusinessDay(config.day(offset), offset, prices, (), (), (), set(), np.array([0]))
    intents = simulation.planner.intents(simulation, day)
    assert simulation.metrics["lookalike_proposals"] == 1
    assert simulation.metrics["outflow_candidates"] == 0
    assert not intents
    simulation.holdings.change(1, 0, 100, 100, 1000000)
    day.intents = simulation.planner.intents(simulation, day)
    simulation._execute_intents(day)
    assert simulation.metrics["outflow_candidates"] == 1
    assert 50 <= simulation.holdings.row(1, 0)[3] <= 80
    assert simulation.ledger.balance[0] == 0
    assert simulation.planner.records[-1]["classification"] is None
    simulation.planner.finish(simulation)
    assert simulation.planner.records[-1]["classification"] == "confirmed_lookalike"
    assert simulation.metrics["confirmed_lookalikes"] == 1


def test_staged_cancel_precedes_new_sale_in_saved_time():
    """Cancel a reserved limit sell before a forced sale can reserve its share."""
    simulation, emitted, _, prices = simulation_for_test(config_for_test(), Stage1IntentPlanner())
    simulation.holdings.change(1, 0, 100, 1, 10000)
    simulation.holdings.row(1, 0)[5] = 1
    old = Order(1000001, 0, 0, 1, 10000, "TLS", 0, 999, False, 0, "PNDG", 34200)
    simulation._add_order(old)
    simulation.trade_ordinals[0] = 1
    day = BusinessDay(simulation.config.day(1), 1, prices, (), (), (), set(), np.array([0]),
                      [Intent(LIQUIDATION_SECOND, 1, 50, IntentKind.LIQUIDATE, 0)])
    simulation._execute_intents(day)
    history = [row for source, _, rows in emitted if source == "TradeHistory" for row in rows]
    canceled = next(row for row in history if row["TH_ST_ID"] == "CNCL")
    created = next(row for row in history if row["TH_ST_ID"] == "SBMT")
    assert canceled["TH_DTS"] < created["TH_DTS"]
    assert not simulation.holdings.index
    assert not simulation.orders


def test_customer_warning_closes_all_accounts_and_watches_together():
    """One customer plan stops buys and deposits, then closes every live account."""
    class ForcedCustomerExit(AttritionPlanner):
        """Select a known customer exit while retaining real leaving execution."""
        def closures(self, simulation, day, scheduled):
            """Start both accounts on one warning window and close on day ten."""
            if day == 1:
                period = LeavingPeriod(1, 10, (1, 5, 9), True)
                self.customer_plans[1] = period
                simulation.customers[0]["inactivation_day"] = 10
                for index in range(2):
                    self._start(simulation, index, period)
            return [index for index in range(2) if int(simulation.accounts[index]["close_day"]) == day]
    config = config_for_test(initial_end="2018-01-10", daily_start="2018-01-11", daily_end="2018-01-12", account_watch_cancel_share=1)
    planner = ForcedCustomerExit()
    simulation, emitted, cash, _ = simulation_for_test(config, planner, 2)
    simulation._add_watch(1, 0, 100)
    simulation.run()
    assert np.all(simulation.closed)
    assert np.all(simulation.accounts["close_day"] == 10)
    assert np.all(simulation.ledger.balance == 0)
    assert not simulation.watches[1]
    cancellations = [row for source, _, rows in emitted if source == "WatchHistory" for row in rows if row["W_ACTION"] == "CNCL"]
    canceled_day = cancellations[0]["W_DTS"] // 86400
    assert canceled_day == (config.day(1) - date(1970, 1, 1)).days
    assert planner.leaving[0] == planner.leaving[1]
    assert not any(args[-1] == "Deposit" and args[0] > 0 for args in cash)


def test_observation_windows_exclude_future_and_censored_labels():
    """Later cash activity cannot enter an earlier feature row."""
    start = date(2020, 1, 1)
    lifetime = AccountLifetime(7, start, start + timedelta(days=70))
    events = [CashObservation(7, start + timedelta(days=29), -100, "Withdrawal"),
              CashObservation(7, start + timedelta(days=31), 500, "Deposit")]
    frame = observations([lifetime], events, start + timedelta(days=90))
    first = frame.row(0, named=True)
    assert first["snapshot"] == start + timedelta(days=30)
    assert first["deposit_count_30d"] == 0
    assert first["external_outflow_30d"] == 100
    assert first["rule"]
    assert frame["snapshot"].max() <= start + timedelta(days=60)
    later = observations([lifetime], events + [CashObservation(7, start + timedelta(days=80), 999, "Deposit")], start + timedelta(days=90))
    assert frame.equals(later)
    assert "account_id" not in FEATURES and "label" not in FEATURES
    assert not any(any(hidden in field for hidden in ("leaving", "profile", "wealth", "candidate", "classification")) for field in FEATURES)


def test_source_only_lifetimes_include_customer_wide_inactivation(tmp_path):
    """Read visible lifetime actions even when hidden evidence is absent or invalid."""
    from data.data_source.generator.signal_check import _source_lifetimes, check_signal
    root = tmp_path
    initial = root / "offline/initial"
    initial.mkdir(parents=True)
    (initial / "CustomerMgmt.xml").write_text(
        '<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">'
        '<TPCDI:Action ActionType="NEW" ActionTS="2020-01-01T00:00:00"><Customer C_ID="1"><Account CA_ID="7"/><Account CA_ID="8"/></Customer></TPCDI:Action>'
        '<TPCDI:Action ActionType="INACT" ActionTS="2020-03-20T23:00:00"><Customer C_ID="1"><Account CA_ID="7"/><Account CA_ID="8"/></Customer></TPCDI:Action>'
        '</TPCDI:Actions>')
    (initial / "CashTransaction.txt").write_text("1|7|2020-01-02 08:00:00|1000.00|Deposit\n")
    daily = root / "offline/daily/20200401"
    daily.mkdir(parents=True)
    (daily / "Account_20200401.txt").write_text("I|1|9|1|2|Savings|1|ACTV\n")
    hidden = root / "summary/hidden"
    hidden.mkdir(parents=True)
    (hidden / "accounts-poison.parquet").write_text("This is not a valid Parquet file.")
    config = config_for_test(initial_start="2020-01-01", initial_end="2020-03-31", daily_start="2020-04-01", daily_end="2020-05-31")
    lifetimes = _source_lifetimes(root, config)
    assert lifetimes == [AccountLifetime(7, date(2020, 1, 1), date(2020, 3, 20)),
                         AccountLifetime(8, date(2020, 1, 1), date(2020, 3, 20)),
                         AccountLifetime(9, date(2020, 4, 1))]
    result = check_signal(root, config)
    frame = pl.read_parquet(root / "summary/signal_observations.parquet")
    assert frame["snapshot"].max() <= config.end - timedelta(days=30)
    assert result["sample_accounts"] == 3


def test_signal_evaluation_reports_insufficient_evidence():
    """An empty sample cannot claim that a model beats the rule."""
    result = evaluate(pl.DataFrame())
    assert result["status"] == "insufficient_data"
    assert "model_beats_rule" not in result
