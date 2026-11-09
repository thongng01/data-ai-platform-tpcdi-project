"""Exercise business state, deterministic planning, and chronological spending."""

from datetime import date
from types import SimpleNamespace

from hypothesis import given, settings, strategies as st
import numpy as np
import polars as pl
import pytest

from data.data_source.generator.activity import BusinessSimulation, HoldingBook, Ledger, Order, SourceRecorder, Stage1IntentPlanner, poisson
from data.data_source.generator.config import Config
from data.data_source.generator.formats import load_sources
from data.data_source.generator.random_values import scalar, values


@given(st.lists(st.integers(min_value=1, max_value=1000), min_size=1, max_size=100))
def test_holding_conservation_and_new_origin(quantities):
    """Buys conserve quantity, and a full sale releases the old position origin."""
    book = HoldingBook(1)
    total = 0
    for ordinal, quantity in enumerate(quantities, 1):
        origin, before, after = book.change(7, 2, ordinal, quantity, quantity * 100)
        assert origin == 1
        assert before == total
        total += quantity
        assert after == total
    assert book.change(7, 2, 999, -total, 0) == (1, total, 0)
    assert book.row(7, 2) is None
    assert book.change(7, 2, 1000, 5, 500) == (1000, 0, 5)


@given(st.integers(min_value=0, max_value=2**63 - 1), st.integers(min_value=0, max_value=2**63 - 1))
def test_scalar_and_vector_random_values_match(identifier, seed):
    """The same identity, seed, and purpose produce identical scalar and array bits."""
    assert scalar(identifier, seed, "unit-test") == int(values([identifier], seed, "unit-test")[0])


@given(st.integers(min_value=0, max_value=1_000_000), st.lists(st.integers(min_value=-100000, max_value=100000), max_size=100))
def test_ledger_spending_invariant(limit, proposals):
    """Accepted cash effects never take an account below its borrowing limit."""
    ledger = Ledger(np.array([(limit,)], dtype=[("margin_limit_cents", "i8")]))
    expected = 0
    for amount in proposals:
        if expected + amount < -limit:
            with pytest.raises(ValueError):
                ledger.apply(0, amount)
        else:
            ledger.apply(0, amount)
            expected += amount
        assert ledger.balance[0] == expected
        assert ledger.balance[0] >= -limit


def test_reservations_prevent_double_spending():
    """Reserved buy funds cannot fund another order or an earlier withdrawal."""
    ledger = Ledger(np.array([(0,)], dtype=[("margin_limit_cents", "i8")]))
    ledger.apply(0, 10000)
    ledger.reserve(0, 8000)
    assert ledger.available(0) == 2000
    with pytest.raises(ValueError):
        ledger.reserve(0, 2001)
    with pytest.raises(ValueError):
        ledger.apply(0, -2001)
    ledger.reserved[0] -= 8000
    ledger.apply(0, -8000)
    assert ledger.balance[0] == 2000


def test_sell_cannot_create_a_holding():
    """Reject a sale when the account does not own the security."""
    with pytest.raises(ValueError):
        HoldingBook().change(1, 1, 3, -1, 0)


def test_recording_cannot_mutate_business_fields():
    """A recorder may change output fields but cannot change correct business values."""
    generation = SimpleNamespace(config=Config())
    original = pl.DataFrame({"T_ID": [1], "T_ST_ID": ["PNDG"], "T_CHRG": [100], "_record_day": [0]})
    def transform(source, day, fields):
        """Reject mutation of the immutable input while returning a changed typed value."""
        assert source == "Trade" and day == generation.config.start
        with pytest.raises(TypeError):
            fields["T_CHRG"] = 20000
        return {**fields, "T_CHRG": 20000}
    recorded, audit = SourceRecorder(generation, transform).apply("Trade", original)
    assert original["T_CHRG"].to_list() == [100]
    assert recorded["T_CHRG"].to_list() == [20000]
    assert not audit


def test_intent_planner_controls_cash_trades_and_closure(tmp_path):
    """Planner restrictions apply to proposals while customer inactivation still forces closure."""
    from data.data_source.generator.config import Config
    from data.data_source.generator.population import ACCOUNT_DTYPE, CUSTOMER_DTYPE

    config = Config(initial_start="2019-01-25", initial_end="2019-01-25",
                    daily_start="2019-01-26", daily_end="2019-01-26",
                    securities=1, companies=1, scale_factor=.001,
                    annual_trade_rates=(0, 0, 0, 0, 0), annual_deposit_rates=(0, 0, 0, 0, 0),
                    annual_withdrawal_rates=(0, 0, 0, 0, 0), watch_daily_rate=0,
                    annual_customer_update=0, annual_account_update=0)
    customers = np.array([(1, 0, 2, 3)], dtype=CUSTOMER_DTYPE)
    accounts = np.array([(4, 1, 0, 0, 1, True, 100000, 0, 1, 1)], dtype=ACCOUNT_DTYPE)
    calls = []
    class Planner(Stage1IntentPlanner):
        """Refuse optional business proposals and record which policy hooks execute."""
        def closing(self, account, day):
            """Record the closure-policy call and reject its scheduled proposal."""
            calls.append(("close", day))
            return False
        def allow_cash(self, simulation, index, day, name):
            """Record the cash-policy call and reject the optional cash proposal."""
            calls.append((name, day))
            return False
        def allow_trade(self, simulation, index, day, kind):
            """Record the trade-policy call and reject the optional trade proposal."""
            calls.append((kind, day))
            return False
    records = []
    generation = SimpleNamespace(config=config, sources=load_sources(),
                                 record=lambda family, day, values, **metadata: records.append((family, day, values, metadata)),
                                 _record_fault=lambda entry: None)
    prices = {"close": np.array([100]), "low": np.array([90]), "high": np.array([110]),
              "dividends": np.array([0]), "trading": True, "close_second": 57600, "shock": False}
    market = SimpleNamespace(day=lambda day: prices, sessions_per_year={2019: 252})
    simulation = BusinessSimulation(generation, market, customers, accounts, planner=Planner())
    simulation.run()
    assert ("close", 0) in calls
    assert ("Deposit", 0) in calls
    assert accounts[0]["close_day"] == config.total_days
    assert sum(family == "CashTransaction" for family, *_ in records) == 1  # Opening funding remains a business requirement.
    simulation.submit(0, config.start, 0, 34200, prices, 1)
    assert not simulation.orders
    assert simulation.metrics["rejected_by_intent_planner"] == 1 or simulation.metrics["rejected_no_holding"] == 1

    customers[0]["inactivation_day"] = 1
    accounts[0]["close_day"] = 0
    inactive = BusinessSimulation(generation, market, customers, accounts, planner=Planner())
    inactive.run()
    assert accounts[0]["close_day"] == 1
    assert inactive.closed[0]
    assert inactive.ledger.balance[0] == 0
    assert inactive.metrics["closures"] == 1

    class RepeatedClosure(Stage1IntentPlanner):
        """Propose the same account repeatedly to exercise the duplicate-closure guard."""
        def closures(self, simulation, day, scheduled):
            """Return one fixed account index even after that account has already closed."""
            return (0,)
    accounts[0]["close_day"] = 0
    duplicate = BusinessSimulation(generation, market, customers, accounts, planner=RepeatedClosure())
    with pytest.raises(ValueError, match="existing account"):
        duplicate.run()
    assert duplicate.metrics["closures"] == 1


def test_array_poisson_matches_scalar():
    """Array planning must retain the scalar inverse-CDF event counts."""
    from data.data_source.generator.business import poisson_counts
    rates = np.linspace(0, 20, 2001)
    draws = np.linspace(0, .99999999, 2001)
    expected = [poisson(float(rate), float(draw)) for rate, draw in zip(rates, draws)]
    np.testing.assert_array_equal(poisson_counts(rates, draws), expected)


def test_typed_intents_preserve_equal_time_order():
    """Stable event-kind order makes same-second cash receipts deterministic."""
    from data.data_source.generator.business import Intent, IntentKind
    cash = Intent(28800, 3, 0, IntentKind.CASH, 0, amount=100, name="Deposit")
    trade = Intent(28800, 3, 0, IntentKind.SUBMIT, 0)
    assert sorted([trade, cash], key=lambda intent: intent.sort_key) == [cash, trade]


@settings(max_examples=50)
@given(st.floats(min_value=0, max_value=20, allow_nan=False, allow_infinity=False), st.floats(min_value=0, max_value=.99999999, allow_nan=False, allow_infinity=False))
def test_poisson_draw_is_bounded_integer(rate, draw):
    """Inverse-CDF event counts remain nonnegative and below the expansion limit."""
    result = poisson(rate, draw)
    assert isinstance(result, int)
    assert 0 <= result < 10000


def _captured_simulation():
    """Build one small account with a typed capture instead of an export backend."""
    from data.data_source.generator.population import ACCOUNT_DTYPE, CUSTOMER_DTYPE

    config = Config(initial_start="2019-06-25", initial_end="2019-06-27",
                    daily_start="2019-06-28", daily_end="2019-07-02", securities=2, companies=1,
                    scale_factor=.001, attrition_enabled=False)
    customers = np.array([(1, 0, config.total_days, 3)], dtype=CUSTOMER_DTYPE)
    accounts = np.array([(4, 1, 0, config.total_days, 4, True, 100000, 0, 1, 1)], dtype=ACCOUNT_DTYPE)
    captured = []
    generation = SimpleNamespace(config=config, sources=load_sources(),
                                 record=lambda family, day, values, **metadata: captured.append((family, day, values, metadata)),
                                 _record_fault=lambda entry: None)
    prices = {"close": np.array([100, 100]), "low": np.array([90, 90]), "high": np.array([110, 110]),
              "dividends": np.array([3, 5]), "trading": True, "close_second": 57600, "shock": False}
    market = SimpleNamespace(day=lambda day: prices, sessions_per_year={2019: 252})
    return BusinessSimulation(generation, market, customers, accounts, planner=Stage1IntentPlanner()), captured, prices


def test_typed_cutover_trade_captures_original_timestamp_and_pending_nulls():
    """The cutover copy cannot follow later mutations of the same live order."""
    from data.data_source.generator.business import BusinessDay

    simulation, captured, prices = _captured_simulation()
    order = Order(4000001, 0, 1, 10, 100, "TLB", 0, 5, False, 1000, "PNDG", 35000, transition=1)
    simulation.orders[order.trade_id] = order
    config = simulation.config
    day = BusinessDay(config.cutover, 2, prices, (), (), (), set(), np.array([0]))
    simulation._finish_day(day)
    family, offset, values, metadata = captured[0]
    assert family == "Trade" and offset == 2
    assert values[1] == (config.start - date(1970, 1, 1)).days * 86400 + 35000
    assert values[2] == "PNDG" and values[5] == 2
    assert values[10:] == (None, None, None, None)
    assert metadata == {"order": (1,), "flag": "I", "profile": 4}
    simulation.transition(order, config.daily_start_date, 36000, "SBMT", creation=True)
    order.quantity = 999
    assert values[6] == 10 and values[2] == "PNDG"
    assert captured[1][1] == 3 and captured[1][3]["flag"] == "I"
    assert captured[1][3]["order"] == (2,)


def test_typed_dividends_preserve_holding_insertion_order_and_cash_ordinals():
    """Native name formatting receives references without reordering same-account dividends."""
    from data.data_source.generator.business import BusinessDay

    simulation, captured, prices = _captured_simulation()
    simulation.holdings.change(4, 1, 11, 5, 500)
    simulation.holdings.change(4, 0, 12, 7, 700)
    day = BusinessDay(simulation.config.start, 0, prices, (), (), (), set(), np.array([0]))
    simulation._pay_dividends(day)
    assert [record[3]["order"] for record in captured] == [(1, 2), (2, 1)]
    assert [record[2][2:] for record in captured] == [(25, "Dividend"), (21, "Dividend")]
    assert all(record[0] == "CashTransaction" for record in captured)
    assert simulation.ledger.balance[0] == 46
    assert simulation.cash_type_counts == {"Dividend": 2}
