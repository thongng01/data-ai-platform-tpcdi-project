from types import SimpleNamespace

from hypothesis import given, settings, strategies as st
import numpy as np
import pytest

from data.data_source.generator.activity import BusinessSimulation, HoldingBook, Ledger, SourceRecorder, Stage1IntentPlanner, decimal_cents, poisson
from data.data_source.generator.random_values import scalar, values


@given(st.lists(st.integers(min_value=1, max_value=1000), min_size=1, max_size=100))
def test_holding_conservation_and_new_origin(quantities):
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
    assert scalar(identifier, seed, "unit-test") == int(values([identifier], seed, "unit-test")[0])


@given(st.integers(min_value=0, max_value=1_000_000), st.lists(st.integers(min_value=-100000, max_value=100000), max_size=100))
def test_ledger_spending_invariant(limit, proposals):
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
    with pytest.raises(ValueError):
        HoldingBook().change(1, 1, 3, -1, 0)


def test_recording_cannot_mutate_business_fields():
    emitted = []
    generation = SimpleNamespace(emit=lambda *args: emitted.append(args))
    original = {"T_CHRG": "1.00"}
    def transform(source, day, fields):
        with pytest.raises(TypeError):
            fields["T_CHRG"] = "200.00"
        return {**fields, "T_CHRG": "200.00"}
    SourceRecorder(generation, transform).record("Trade", 0, original, "key")
    assert original == {"T_CHRG": "1.00"}
    assert emitted[0][2][0]["T_CHRG"] == "200.00"


def test_intent_planner_controls_cash_trades_and_closure(tmp_path):
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
        def closing(self, account, day):
            calls.append(("close", day))
            return False
        def allow_cash(self, simulation, index, day, name):
            calls.append((name, day))
            return False
        def allow_trade(self, simulation, index, day, kind):
            calls.append((kind, day))
            return False
    cash = []
    generation = SimpleNamespace(config=config, cash=lambda *args: cash.append(args),
                                 emit=lambda *args: None, text=lambda *args: None)
    prices = {"close": np.array([100]), "low": np.array([90]), "high": np.array([110]),
              "dividends": np.array([0]), "trading": True, "close_second": 57600, "shock": False}
    market = SimpleNamespace(day=lambda day: prices, sessions_per_year={2019: 252})
    simulation = BusinessSimulation(generation, market, customers, accounts, planner=Planner())
    simulation.run()
    assert ("close", 0) in calls
    assert ("Deposit", 0) in calls
    assert accounts[0]["close_day"] == config.total_days
    assert len(cash) == 1  # Opening funding remains a business requirement.
    simulation.submit(0, config.start, 0, 34200, prices, 1)
    assert not simulation.orders
    assert simulation.metrics["rejected_by_intent_planner"] == 1 or simulation.metrics["rejected_no_holding"] == 1


def test_decimal_cent_encoding():
    assert decimal_cents(-1) == "-0.01"
    assert decimal_cents(12345) == "123.45"
    assert decimal_cents(0) == "0.00"


@settings(max_examples=50)
@given(st.floats(min_value=0, max_value=20, allow_nan=False, allow_infinity=False), st.floats(min_value=0, max_value=.99999999, allow_nan=False, allow_infinity=False))
def test_poisson_draw_is_bounded_integer(rate, draw):
    result = poisson(rate, draw)
    assert isinstance(result, int)
    assert 0 <= result < 10000
