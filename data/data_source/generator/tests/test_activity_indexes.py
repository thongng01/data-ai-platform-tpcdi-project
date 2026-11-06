"""Compare indexed day planning with scalar business rules on small populations."""

from concurrent.futures import Future
from dataclasses import replace
import json
from types import SimpleNamespace

import numpy as np
import pytest

from data.data_source.generator.activity import BusinessSimulation, Order, Stage1IntentPlanner, poisson
from data.data_source.generator.attrition import AttritionPlanner, LeavingPeriod
from data.data_source.generator.business import BusinessDay, DAY_KEY_STRIDE, WATCH_ADD_SECOND, WATCH_CANCEL_SECOND, CLOSE_SECOND
from data.data_source.generator.config import Config
from data.data_source.generator.formats import load_sources
from data.data_source.generator.population import ACCOUNT_DTYPE, CUSTOMER_DTYPE
from data.data_source.generator.random_values import scalar, uniform, uniforms, values


class WatchPolicy(Stage1IntentPlanner):
    """Exercise leaving cancellations and customer-specific cancellation shares."""

    def leaving_customers(self, simulation, day):
        """Start one warning independently of account closure."""
        return {1, 4} if day == 4 else set()

    def watch_cancel_share(self, simulation, customer, day, default_share):
        """Keep some watches when only an account leaves."""
        return .75 if customer % 2 else .25


def make_simulation(rate=.1, planner=None):
    """Build a dated capture with enough customers for updates and watch collisions."""
    config = Config(initial_start="2019-06-25", initial_end="2019-06-27",
                    daily_start="2019-06-28", daily_end="2019-07-20", securities=8, companies=2,
                    scale_factor=.01, attrition_enabled=False, annual_customer_update=1,
                    watch_daily_rate=rate, watch_lifetime_min_days=1, watch_lifetime_max_days=5)
    customers = np.zeros(64, dtype=CUSTOMER_DTYPE)
    customers["customer_id"] = np.arange(1, 65)
    customers["open_day"] = np.arange(64) % 5
    customers["inactivation_day"] = config.total_days
    customers["inactivation_day"][:3] = [6, 9, 14]
    customers["tier"] = np.arange(64) % 3 + 1
    accounts = np.zeros(128, dtype=ACCOUNT_DTYPE)
    accounts["account_id"] = np.arange(1, 129)
    accounts["customer_id"] = np.repeat(customers["customer_id"], 2)
    accounts["open_day"] = np.repeat(customers["open_day"], 2)
    accounts["close_day"] = config.total_days
    accounts["wealth_cents"] = 100000
    accounts["is_cash"] = True
    captured = []
    generation = SimpleNamespace(config=config, sources=load_sources(),
                                 record=lambda family, day, fields, **metadata: captured.append((family, day, fields, metadata)))
    prices = {"close": np.arange(100, 108), "low": np.arange(90, 98), "high": np.arange(110, 118),
              "dividends": np.zeros(8, dtype=np.int64), "trading": True, "close_second": 57600, "shock": False}
    market = SimpleNamespace(day=lambda day: prices, sessions_per_year={2019: 252})
    simulation = BusinessSimulation(generation, market, customers, accounts, planner=planner or WatchPolicy())
    return simulation, captured, prices


def scalar_customer_day(simulation, day):
    """Apply the simple full-scan rule independently of the new scheduling indexes."""
    closing = {int(simulation.accounts[index]["customer_id"]) for index in day.closes}
    closing |= simulation.planner.leaving_customers(simulation, day.offset)
    eligible = ((simulation.customers["open_day"] <= day.offset)
                & (simulation.customers["inactivation_day"] >= day.offset))
    for customer in simulation.customers[eligible]:
        identifier = int(customer["customer_id"])
        if identifier not in day.inactive_customers and simulation.draw(identifier, day.offset, "customer-update") < simulation.config.annual_customer_update / 365.2425:
            step = -1 if simulation.draw(identifier, day.offset, "tier-step") < .5 else 1
            simulation.tiers[identifier] = max(1, min(3, simulation.tiers[identifier] + step))
            simulation.tier_array[np.searchsorted(simulation.customers["customer_id"], identifier)] = simulation.tiers[identifier]
            simulation.customer_versions[identifier] += 1
            simulation.action("UPDCUST", day.date, 3, identifier, ordinal=simulation.customer_versions[identifier])
        for security, due in sorted(simulation.watches.get(identifier, {}).items()):
            share = simulation.planner.watch_cancel_share(simulation, identifier, day.offset, simulation.config.account_watch_cancel_share)
            cancel = identifier in closing and simulation.draw(identifier, day.offset, "close-watch", security) < share
            if due <= day.offset or identifier in day.inactive_customers or cancel:
                simulation.watch(identifier, security, day.date, WATCH_CANCEL_SECOND, "CNCL")
                del simulation.watches[identifier][security]
        if identifier in day.inactive_customers:
            indices = [index for index in day.closes if int(simulation.accounts[index]["customer_id"]) == identifier]
            simulation.action("INACT", day.date, CLOSE_SECOND, identifier, indices)
            continue
        count = poisson(simulation.config.watch_daily_rate, simulation.draw(identifier, day.offset, "watch-count"))
        for ordinal in range(count):
            security = simulation.choose_security(identifier, day.offset, 1000 + ordinal)
            watches = simulation.watches.setdefault(identifier, {})
            if security not in watches:
                simulation.watch(identifier, security, day.date, WATCH_ADD_SECOND + ordinal, "ACTV")
                watches[security] = day.offset + simulation.config.watch_lifetime_min_days + int(simulation.draw(identifier, day.offset, "watch-duration", ordinal) * (simulation.config.watch_lifetime_max_days - simulation.config.watch_lifetime_min_days + 1))
    for index in day.closes:
        customer = int(simulation.accounts[index]["customer_id"])
        if customer not in day.inactive_customers:
            simulation.action("CLOSEACCT", day.date, CLOSE_SECOND, customer, [index], int(simulation.accounts[index]["account_id"]))


@pytest.mark.parametrize("rate", [0, .1, 2, 20])
def test_watch_schedule_matches_daily_full_scan(rate):
    """Due, leaving, closure, and inactivation cancellations keep exact records and state."""
    actual, actual_records, prices = make_simulation(rate)
    expected, expected_records, _ = make_simulation(rate)
    # Seed due and future watches together, including a watch canceled early by
    # a warning. Reuse of that security must not revive its old due entry.
    for identifier in range(1, 9):
        for security, due in [(5, 4), (1, 6), (3, 12)]:
            actual._add_watch(identifier, security, due)
            expected.watches.setdefault(identifier, {})[security] = due
    for offset in range(4, 21):
        inactive = set(actual.customers["customer_id"][actual.customers["inactivation_day"] == offset].tolist())
        closes = tuple(index for index, account in enumerate(actual.accounts)
                       if int(account["customer_id"]) in inactive or (offset == 5 and index in (6, 8)))
        day = BusinessDay(actual.config.day(offset), offset, prices, (), (), closes, inactive, np.array([], dtype=np.int64))
        actual._update_customers(day)
        scalar_customer_day(expected, day)
        assert actual_records == expected_records
        assert actual.watches == expected.watches
        assert actual.customer_versions == expected.customer_versions
        assert actual.tiers == expected.tiers
        np.testing.assert_array_equal(actual.tier_array, expected.tier_array)
        assert actual.watch_count == sum(len(watches) for watches in expected.watches.values())
        scheduled = {(identifier, security, due) for due, customers in actual.watch_due_days.items()
                     for identifier, securities in customers.items() for security in securities}
        live = {(identifier, security, due) for identifier, watches in actual.watches.items()
                for security, due in watches.items()}
        assert scheduled == live
    assert actual.metrics == expected.metrics
    assert actual.metrics["INACT"] == 3
    assert actual.metrics["UPDCUST"] > 0


@pytest.mark.parametrize("seed", [0, 20261029, 2**63 - 1])
@pytest.mark.parametrize("purpose,ordinal", [("customer-update", 0), ("watch-count", 0),
                                             ("close-watch", 7), ("watch-duration", 3), ("tier-step", 0)])
def test_customer_array_draws_match_scalar_bits(seed, purpose, ordinal):
    """Array draws retain the day key and the colon ordinal used by BusinessSimulation.draw."""
    simulation, _, _ = make_simulation()
    simulation.config = replace(simulation.config, seed=seed)
    for day in [0, 303, 4382, 72683]:
        identifiers = np.array([1, 7, 2048, 2049, 999999999], dtype=np.uint64)
        keys = identifiers + np.uint64((day + 1) * DAY_KEY_STRIDE)
        field = f"{purpose}:{ordinal}"
        np.testing.assert_array_equal(values(keys, seed, field), [scalar(int(key), seed, field) for key in keys])
        np.testing.assert_array_equal(uniforms(keys, seed, field), [uniform(int(key), seed, field) for key in keys])
        np.testing.assert_array_equal(uniforms(keys, seed, field),
                                      [simulation.draw(int(identifier), day, purpose, ordinal) for identifier in identifiers])


def add_orders(simulation):
    """Insert cross-account limit orders with due dates that differ from insertion order."""
    for ordinal, (index, due) in enumerate([(3, 5), (0, 4), (2, 12), (1, 5), (0, 99), (3, 4)], 1):
        account = int(simulation.accounts[index]["account_id"])
        order = Order(account * 1000000 + ordinal, index, 1, 1, 100, "TLB", 3, due,
                      False, 100, "PNDG", 35000)
        simulation._add_order(order)


def test_due_order_index_matches_scan_across_closed_sessions_and_unfilled_orders():
    """Ready orders survive unfilled sessions; canceled entries and closing accounts do not run."""
    simulation, _, prices = make_simulation()
    add_orders(simulation)
    removed = list(simulation.orders.values())[1]
    simulation._remove_order(removed)
    for offset in range(4, 14):
        trading = offset not in (4, 6, 7)
        closes = (3,) if offset == 5 else ()
        day = BusinessDay(simulation.config.day(offset), offset, {**prices, "trading": trading},
                          (), (), closes, set(), np.array([], dtype=np.int64))
        expected = [order for order in simulation.orders.values()
                    if trading and order.due_day <= offset and order.account_index not in closes]
        simulation._plan_due_orders(day)
        assert [intent.order for intent in day.intents] == expected
        if offset == 8:
            simulation._remove_order(expected[-1])
    assert all(order.due_day <= 13 for order in simulation.due_orders.values())
    assert removed.trade_id not in simulation.due_orders


def test_leaving_order_index_matches_scan_and_warning_supersession():
    """Leaving cancels only its live account orders and retains global insertion order."""
    simulation, _, prices = make_simulation(planner=AttritionPlanner())
    planner = simulation.planner
    add_orders(simulation)
    planner._start(simulation, 0, LeavingPeriod(4, 14, (4, 10)))
    planner._start(simulation, 3, LeavingPeriod(4, 15, (4, 11)))
    planner._start(simulation, 0, LeavingPeriod(5, 16, (5, 12)))
    assert planner.leaving_customers(simulation, 4) == {2}
    assert planner.leaving_customers(simulation, 5) == {1}
    assert planner.leaving_customers(simulation, 5) == {1}
    simulation._remove_order(list(simulation.orders.values())[0])
    day = BusinessDay(simulation.config.day(5), 5, prices, (), (), (), set(), np.array([], dtype=np.int64))
    expected = [order for order in simulation.orders.values() if planner.leaving_mask[order.account_index]]
    assert [intent.order for intent in planner.intents(simulation, day)] == expected
    assert simulation.orders_for_accounts({0, 3}) == expected


@pytest.mark.parametrize("workers", [1, 2, 4, 8])
def test_activity_worker_count_accepts_supported_bounds(workers):
    """All supported process counts remain outside semantic dataset identity."""
    from data.data_source.generator.runner import semantic_dataset_id

    base = Config(workers=1)
    assert semantic_dataset_id(base, "test", {}, "calendar") == semantic_dataset_id(replace(base, workers=workers), "test", {}, "calendar")


def test_activity_pool_failure_cancels_queued_groups_and_retains_schedule(tmp_path, monkeypatch):
    """A worker error stops queued work without deleting the scheduled-group evidence."""
    from data.data_source.generator.activity import generate_activity

    submitted = []
    class FailingPool:
        """Represent a completed failure and tasks still waiting for a free process."""

        def __init__(self, max_workers, mp_context):
            """Check the selected concurrency before accepting groups."""
            assert max_workers == 4
            assert mp_context.get_start_method() == "spawn"

        def __enter__(self):
            """Provide the executor for this group schedule."""
            return self

        def submit(self, function, *arguments):
            """Fail the first group and leave later groups queued."""
            future = Future()
            if not submitted:
                future.set_exception(RuntimeError("group failed"))
            submitted.append(future)
            return future

        def __exit__(self, kind, error, traceback):
            """Require cancellation before the executor waits for running work."""
            assert kind is RuntimeError
            assert all(future.cancelled() for future in submitted[1:])

    monkeypatch.setattr("concurrent.futures.ProcessPoolExecutor", FailingPool)
    config = Config(workers=4, customer_group_size=1)
    generation = SimpleNamespace(config=config, root=tmp_path)
    population = SimpleNamespace(customers=np.array([(identifier,) for identifier in range(1, 7)],
                                                   dtype=[("customer_id", "i8")]))
    with pytest.raises(RuntimeError, match="group failed"):
        generate_activity(generation, None, population)
    assert len(submitted) == 6
    assert json.loads((tmp_path / "_work/source-parts/scheduled-groups.json").read_text()) == list(range(1, 7))


def test_array_holding_lookup_preserves_scalar_equity_and_return_history():
    """Native account lookup adds positions in their existing insertion order."""
    simulation, _, prices = make_simulation()
    # Deliberately unsort account IDs before attaching the planner. Production
    # IDs are sorted, but the lookup does not require that storage order.
    simulation.accounts["account_id"] = simulation.accounts["account_id"][::-1]
    simulation.account_indices = {int(row["account_id"]): index for index, row in enumerate(simulation.accounts)}
    planner = AttritionPlanner()
    planner.prepare(simulation)
    simulation.ledger.balance[:] = np.arange(len(simulation.accounts)) * 100
    for account, security, quantity in [(128, 7, 9), (5, 0, 11), (128, 1, 3), (12, 2, 6)]:
        simulation.holdings.change(account, security, account * 100, quantity, 10000)
    simulation.holdings.change(5, 0, 500, -11, 0)
    simulation.holdings.change(12, 0, 1200, 4, 10000)
    expected = simulation.ledger.balance.astype(np.float64)
    for slot in simulation.holdings.index.values():
        account, security, _, quantity = simulation.holdings.data[slot, :4]
        expected[simulation.account_indices[int(account)]] += int(quantity) * int(prices["close"][security])
    day = BusinessDay(simulation.config.day(5), 5, prices, (), (), (), set(), np.array([], dtype=np.int64))
    planner.after_day(simulation, day)
    np.testing.assert_array_equal(planner.returns.previous_equity, expected)
    assert list(simulation.holdings.index) == [(128, 7), (128, 1), (12, 2), (12, 0)]
