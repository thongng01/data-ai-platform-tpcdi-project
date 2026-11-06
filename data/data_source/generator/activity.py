"""Day-ordered business execution. Planning and recording do not mutate the ledger."""

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
import json
import math
import heapq

import numpy as np
import polars as pl

from .customers import account_values, customer_birth_year, customer_values
from .random_values import uniform, uniforms
from .business import (
    BusinessDay, Intent, IntentKind, MarketDayCache, plan_daily_activity, DAY_KEY_STRIDE, ATTRITION_HORIZON_DAYS,
    DIVIDEND_SECOND, FUNDING_SECOND, MARKET_OPEN_SECOND, ORDER_DUE_SECOND,
    LIQUIDATION_SECOND, REPAYMENT_SECOND, FINAL_WITHDRAWAL_SECOND,
    WATCH_CANCEL_SECOND, WATCH_ADD_SECOND, CLOSE_SECOND,
)

from .recording import SourceRecorder

TRADE_STRIDE = 1_000_000
ATTRITION_RECORD_SCHEMA = {
    "account_id": pl.Int64, "start_day": pl.Int64, "close_day": pl.Int64,
    "steps": pl.List(pl.Int64), "customer_exit": pl.Boolean, "kind": pl.String,
    "classification": pl.String, "final_plan": pl.Boolean,
    "actual_close_day": pl.Int64, "outcome_day": pl.Int64,
}


def timestamp(day: date, seconds: int) -> str:
    """Put a UTC date and second-of-day into the source timestamp form."""
    return f"{day} {seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"


def poisson(rate: float, draw: float) -> int:
    """Invert one Poisson cumulative probability for the scalar watch planner."""
    probability = math.exp(-rate)
    total = probability
    count = 0
    while draw >= total:
        count += 1
        probability *= rate / count
        total += probability
        if count >= 10000:
            raise ValueError("The daily event expansion exceeds its checked bound.")
    return count


class HoldingBook:
    """Sparse, reusable numeric buffers keep live holdings, not buy lots or history."""

    def __init__(self, capacity: int = 64) -> None:
        """Allocate reusable numeric position slots and sparse lookup indexes."""
        self.data = np.zeros((capacity, 6), dtype=np.int64)
        self.index = {}
        self.by_account = defaultdict(set)
        self.free = list(range(capacity - 1, -1, -1))
        self.high_water = 0

    def row(self, account: int, security: int) -> np.ndarray | None:
        """Find one live position, or return None when the account holds no shares."""
        slot = self.index.get((account, security))
        return None if slot is None else self.data[slot]

    def change(self, account: int, security: int, trade_id: int, signed_quantity: int, value_cents: int) -> tuple[int, int, int]:
        """Apply one completed trade and retain its position origin until a full sale."""
        key = (account, security)
        slot = self.index.get(key)
        if slot is None:
            if signed_quantity <= 0:
                raise ValueError("A sell requires an existing holding.")
            if not self.free:
                size = len(self.data)
                self.data = np.vstack((self.data, np.zeros_like(self.data)))
                self.free.extend(range(2 * size - 1, size - 1, -1))
            slot = self.free.pop()
            self.index[key] = slot
            self.by_account[account].add(security)
            self.data[slot] = (account, security, trade_id, 0, 0, 0)
        row = self.data[slot]
        before = int(row[3])
        after = before + signed_quantity
        if after < 0 or after >= 10**6:
            raise ValueError("A holding quantity is outside the source width.")
        origin = int(row[2])
        row[4] = int(row[4]) + value_cents if signed_quantity > 0 else int(row[4]) * after // before
        row[3] = after
        if not after:
            if row[5]:
                raise ValueError("A full sale cannot erase reserved shares.")
            del self.index[key]
            self.by_account[account].remove(security)
            self.free.append(slot)
            self.data[slot] = 0
        self.high_water = max(self.high_water, len(self.index))
        return origin, before, after

    def snapshot(self) -> pl.DataFrame:
        """Expose current positions as a small typed inspection frame."""
        rows = [self.data[slot] for slot in self.index.values()]
        return pl.DataFrame(rows, schema=["account", "security", "origin", "quantity", "cost_cents", "reserved"], orient="row") if rows else pl.DataFrame()


class Ledger:
    """Keep cash balances and reservations in checked integer-cent arrays."""
    def __init__(self, accounts: np.ndarray) -> None:
        """Allocate balances and copy the account-specific borrowing limits."""
        self.balance = np.zeros(len(accounts), dtype=np.int64)
        self.reserved = np.zeros(len(accounts), dtype=np.int64)
        self.limits = np.asarray(accounts["margin_limit_cents"], dtype=np.int64)

    def available(self, index: int) -> int:
        """Return spendable cash plus borrowing, excluding pending-buy reservations."""
        return int(self.balance[index]) + int(self.limits[index]) - int(self.reserved[index])

    def reserve(self, index: int, amount: int) -> None:
        """Reserve funds for a buy only when the account can afford them."""
        if amount < 0 or amount > self.available(index):
            raise ValueError("An order cannot reserve unavailable funds.")
        self.reserved[index] += amount

    def apply(self, index: int, amount: int) -> None:
        """Apply one cent-denominated cash effect while preserving spending limits."""
        after = int(self.balance[index]) + int(amount)
        if after < -int(self.limits[index]) or abs(after) >= 10**12:
            raise ValueError("A cash transaction exceeds the account's spending or source-width limit.")
        if after + int(self.limits[index]) < int(self.reserved[index]):
            raise ValueError("A cash transaction spends reserved funds.")
        self.balance[index] = after


@dataclass
class Order:
    """Keep the correct current state of one unfinished or completed trade."""
    trade_id: int
    account_index: int
    security: int
    quantity: int
    bid: int
    kind: str
    created_day: int
    due_day: int
    cancel: bool
    reservation: int
    status: str
    seconds: int
    transition: int = 0

    @property
    def sell(self) -> bool:
        """Identify the two sell order kinds without interpreting source flags elsewhere."""
        return self.kind in ("TMS", "TLS")


class Stage1IntentPlanner:
    """Supply base-rate intentions through the shared business planner interface."""

    def prepare(self, simulation):
        """Attach to a group without changing its base-rate lifetime plan."""

    def start_day(self, simulation, day):
        """Apply any state changes before today's ordinary planning."""

    def intents(self, simulation, day):
        """Return additional business proposals without changing the ledger."""
        return []

    def after_day(self, simulation, day):
        """Observe completed business effects without changing source records."""

    def finish(self, simulation) -> None:
        """Leave summary outcomes unchanged for the explicit stage 1 policy."""

    def numeric_state_bytes(self) -> int:
        """Return the extra numeric storage used by this stateless policy."""
        return 0

    def observe_cash(self, simulation, index, amount, name):
        """Observe correct cash effects without changing the ledger."""

    def closing(self, account, day):
        """Use the planned account close date for the explicit stage 1 policy."""
        return int(account["close_day"]) == day

    def closures(self, simulation, day, scheduled_indices):
        """Select due base-rate closures through the planner seam."""
        return [index for index in scheduled_indices if self.closing(simulation.accounts[index], day)]

    def allow_ordinary(self, account, day):
        """Permit activity only inside the account lifetime."""
        return int(account["open_day"]) <= day < int(account["close_day"])

    def allow_cash(self, simulation, index, day, name):
        """Permit ordinary stage 1 cash proposals."""
        return True

    def allow_trade(self, simulation, index, day, kind):
        """Permit ordinary stage 1 trade proposals."""
        return True

    def leaving_customers(self, simulation, day):
        """Return customers whose warning starts today for one watch-cancel draw."""
        return set()

    def watch_cancel_share(self, simulation, customer, day, default_share):
        """Keep the configured account-closure watch cancellation share."""
        return default_share


class BusinessSimulation:
    """Plan brokerage days and execute money and holdings in time order."""
    def __init__(self, generation, market, customers, accounts, planner=None):
        """Attach planners and recorders and allocate bounded per-group state."""
        self.generation = generation
        self.config = generation.config
        self.market = market
        self.customers = customers
        self.accounts = accounts
        if planner is None and self.config.attrition_enabled:
            from .attrition import AttritionPlanner
            planner = AttritionPlanner()
        self.planner = planner or Stage1IntentPlanner()
        self.recorder = getattr(generation, "recorder", None) or SourceRecorder(generation)
        self.start_epoch_day = (self.config.start - date(1970, 1, 1)).days
        self.start_epoch_second = self.start_epoch_day * 86400
        self.ledger = Ledger(accounts)
        self.holdings = HoldingBook()
        self.orders = {}
        self.watches = {}
        self.cash_ordinals = np.zeros(len(accounts), dtype=np.int64)
        self.trade_ordinals = np.zeros(len(accounts), dtype=np.int64)
        self.account_versions = np.zeros(len(accounts), dtype=np.int64)
        self.closed = np.zeros(len(accounts), dtype=np.bool_)
        self.customer_versions = Counter()
        self.tiers = {int(c["customer_id"]): int(c["tier"]) for c in customers}
        self.tier_array = customers["tier"].copy()
        self.account_indices = {int(a["account_id"]): i for i, a in enumerate(accounts)}
        self.customer_accounts = defaultdict(list)
        for i, account in enumerate(accounts):
            self.customer_accounts[int(account["customer_id"])].append(i)
        self.metrics = Counter()
        self.profile_counts = Counter(int(a["profile"]) for a in accounts)
        self.account_trade_counts = Counter()
        self.security_trade_counts = Counter()
        self.daily_trade_counts = Counter()
        self.cash_type_counts = Counter()
        self.maximum_open_orders = 0
        self.maximum_watches = 0
        self.total_days = (self.config.end - self.config.start).days + 1
        self.popular_count = max(1, math.ceil(self.config.securities * .01))
        self.activity_variation = self.config.activity_variation_min + (self.config.activity_variation_max - self.config.activity_variation_min) * uniforms(accounts["account_id"], self.config.seed, "activity-variation") ** self.config.activity_variation_power
        self.market_cache = MarketDayCache(market)
        self.order_thresholds = np.cumsum(self.config.order_shares)
        self.planner.prepare(self)

    def draw(self, identity: int, day: int, purpose: str, ordinal: int = 0) -> float:
        """Derive a date-specific draw from stable identity, purpose, and ordinal."""
        key = (day + 1) * DAY_KEY_STRIDE + int(identity)
        return uniform(key, self.config.seed, f"{purpose}:{ordinal}")

    def cash(self, day: date, second: int, index: int, amount: int, name: str, *, reference: int = 0) -> None:
        """Apply correct money, then capture its kind and numeric source reference."""
        if not amount:
            return
        self.ledger.apply(index, amount)
        self.planner.observe_cash(self, index, amount, name)
        self.cash_ordinals[index] += 1
        account = int(self.accounts[index]["account_id"])
        offset = (day - self.config.start).days
        self.generation.record("CashTransaction", offset,
                               (account, self.start_epoch_second + offset * 86400 + second, int(amount), name),
                               order=(int(self.cash_ordinals[index]), int(reference)))
        kind = "Settlement" if name == "Trade settlement" else name
        self.cash_type_counts[kind] += 1

    def account_fields(self, index: int, inactive: bool = False) -> dict:
        """Build the observable account version without changing hidden traits."""
        fields = account_values(self.config, self.accounts[index])
        version = int(self.account_versions[index])
        if version:
            suffix = f" v{version}"
            fields["CA_NAME"] = fields["CA_NAME"][:50 - len(suffix)] + suffix
        if inactive:
            fields["CA_ST_ID"] = "INAC"
        return fields

    def customer_fields(self, identifier: int, inactive: bool = False) -> dict:
        """Build the observable customer version at the current tier."""
        fields = customer_values(identifier, self.tiers[identifier], seed=self.config.seed)
        fields["C_DOB"] = f"{customer_birth_year(self.config, identifier):04d}-01-01"
        if inactive:
            fields["C_ST_ID"] = "INAC"
        return fields

    def action(self, kind: str, day: date, second: int, customer: int, indices=(), ordinal: int = 0) -> None:
        """Record one customer-management action in the dated source form."""
        inactive = kind in ("INACT", "CLOSEACCT")
        cv = self.customer_fields(customer, kind == "INACT")
        av = [self.account_fields(i, inactive) for i in indices]
        offset = (day - self.config.start).days
        epoch_second = self.start_epoch_second + offset * 86400 + second
        if day <= self.config.cutover:
            # XML is the small, exact scalar encoder exception. Ordinary daily
            # customer and account records enter the same typed export buffers.
            self.recorder.xml_action(kind, timestamp(day, second).replace(" ", "T"), customer,
                                     cv, av, ordinal=ordinal)
        else:
            if kind in ("NEW", "UPDCUST", "INACT"):
                values = tuple((date.fromisoformat(cv[field.name]) - date(1970, 1, 1)).days
                               if field.name == "C_DOB" else cv[field.name]
                               for field in self.generation.sources["Customer"].fields)
                self.generation.record("Customer", offset, values,
                                       order=(epoch_second, ordinal, kind), flag="I" if kind == "NEW" else "U")
            if kind != "UPDCUST":
                for fields in av:
                    values = tuple(fields[field.name] for field in self.generation.sources["Account"].fields)
                    self.generation.record("Account", offset, values, order=(epoch_second, ordinal),
                                           flag="I" if kind in ("NEW", "ADDACCT") else "U")
        self.metrics[kind] += 1

    def record_trade(self, order: Order, offset: int, seconds: int, status: str,
                     price: int | None = None, flag: str = "I", *, value_day: int | None = None) -> None:
        """Capture one typed trade version before its mutable order can change."""
        account = self.accounts[order.account_index]
        charge, commission = self.fees(order.quantity, price) if price is not None else (None, None)
        epoch_second = self.start_epoch_second + (offset if value_day is None else value_day) * 86400 + seconds
        values = (order.trade_id, epoch_second, status, order.kind, int(account["is_cash"]),
                  order.security + 1, order.quantity, order.bid, int(account["account_id"]),
                  "Alex Broker", price, charge, commission, 0 if price is not None else None)
        self.generation.record("Trade", offset, values, order=(order.transition,), flag=flag,
                               profile=int(account["profile"]))

    def transition(self, order: Order, day: date, seconds: int, status: str, price: int | None = None, creation: bool = False) -> None:
        """Record one legal status change and its stable equal-time ordinal."""
        order.status = status
        order.seconds = seconds
        order.transition += 1
        offset = (day - self.config.start).days
        if day <= self.config.cutover:
            epoch_second = self.start_epoch_second + offset * 86400 + seconds
            self.generation.record("TradeHistory", offset, (order.trade_id, epoch_second, status),
                                   order=(order.transition,))
            if status in ("CMPT", "CNCL"):
                self.record_trade(order, offset, seconds, status, price)
        else:
            self.record_trade(order, offset, seconds, status, price, "I" if creation else "U")
        if status in ("CMPT", "CNCL"):
            self.metrics[status] += 1

    def fees(self, quantity: int, price: int) -> tuple[int, int]:
        """Calculate the charge and rounded commission in integer cents."""
        return self.config.charge_cents, (quantity * price * self.config.commission_basis_points + 5000) // 10000

    def release(self, order: Order) -> None:
        """Release exactly the shares or funds reserved by one order."""
        index = order.account_index
        account = int(self.accounts[index]["account_id"])
        if order.sell:
            row = self.holdings.row(account, order.security)
            if row is None or row[5] < order.quantity:
                raise ValueError("A sell lost its holding reservation.")
            row[5] -= order.quantity
        else:
            self.ledger.reserved[index] -= order.reservation

    def cancel(self, order: Order, day: date, second: int) -> None:
        """Release an order and record its cancellation before removing it."""
        self.release(order)
        self.transition(order, day, second, "CNCL")
        self.orders.pop(order.trade_id, None)

    def complete(self, order: Order, day: date, second: int, prices: dict) -> bool:
        """Execute inside market and limit bounds, then settle and change holdings."""
        low = int(prices["low"][order.security])
        high = int(prices["high"][order.security])
        if order.kind == "TLB":
            high = min(high, order.bid)
        elif order.kind == "TLS":
            low = max(low, order.bid)
        if high < low:
            return False
        price = low + int((high - low) * self.draw(order.trade_id, (day - self.config.start).days, "execution"))
        charge, commission = self.fees(order.quantity, price)
        amount = order.quantity * price
        self.release(order)
        delta = amount - charge - commission if order.sell else -amount - charge - commission
        if order.sell and delta <= 0:
            raise ValueError("Sell proceeds must cover the configured fees.")
        self.cash(day, second, order.account_index, delta, "Trade settlement", reference=order.trade_id)
        account = int(self.accounts[order.account_index]["account_id"])
        origin, before, after = self.holdings.change(account, order.security, order.trade_id, -order.quantity if order.sell else order.quantity, amount + charge + commission)
        self.transition(order, day, second, "CMPT", price)
        offset = (day - self.config.start).days
        self.generation.record("HoldingHistory", offset, (origin, order.trade_id, before, after),
                               order=(self.start_epoch_second + offset * 86400 + second,
                                      account, int(self.cash_ordinals[order.account_index])))
        self.orders.pop(order.trade_id, None)
        return True

    def choose_security(self, account: int, offset: int, ordinal: int) -> int:
        """Select from the shared market while preserving configured security skew."""
        draw = self.draw(account, offset, "security", ordinal)
        share = self.config.security_top_share
        if self.popular_count == self.config.securities:
            return min(self.config.securities - 1, int(draw * self.config.securities))
        if draw < share:
            return min(self.popular_count - 1, int(draw / share * self.popular_count))
        return self.popular_count + min(self.config.securities - self.popular_count - 1, int((draw - share) / (1 - share) * (self.config.securities - self.popular_count)))

    def submit(self, index: int, day: date, offset: int, second: int, prices: dict, ordinal: int, force_sell: tuple[int, int] | None = None) -> None:
        """Constrain a proposal, reserve its resources, and create a checked trade."""
        account = int(self.accounts[index]["account_id"])
        draw = self.draw(account, offset, "trade-kind", ordinal)
        kind = ("TMB", "TLB", "TLS", "TMS")[min(3, int(np.searchsorted(self.order_thresholds, draw, side="right")))]
        held = self.holdings.by_account[account]
        if force_sell is not None:
            security, quantity = force_sell
            kind = "TMS"
        elif kind in ("TLS", "TMS"):
            if not held:
                self.metrics["rejected_no_holding"] += 1
                return
            ordered = sorted(held)
            security = ordered[min(len(ordered) - 1, int(self.draw(account, offset, "sell-security", ordinal) * len(ordered)))]
            row = self.holdings.row(account, security)
            available = int(row[3] - row[5])
            quantity = max(1, int(available * (.1 + .9 * self.draw(account, offset, "sell-quantity", ordinal))))
            if not available:
                self.metrics["rejected_reserved_holding"] += 1
                return
        else:
            if self.config.positions_per_account:
                slot = min(self.config.positions_per_account - 1, int(self.draw(account, offset, "investment-slot", ordinal) * self.config.positions_per_account))
                security = self.choose_security(account, 0, 20000 + slot)
            else:
                security = self.choose_security(account, offset, ordinal)
            value = int(int(self.accounts[index]["wealth_cents"]) * (self.config.trade_wealth_min + (self.config.trade_wealth_max - self.config.trade_wealth_min) * self.draw(account, offset, "trade-size", ordinal)))
            quantity = max(1, min(999999, value // int(prices["high"][security])))
        bid = int(prices["close"][security])
        if kind in ("TMB", "TMS"):
            bid = int(prices["high"][security]) if kind == "TMB" else int(prices["low"][security])
        if force_sell is None and not self.planner.allow_trade(self, index, offset, kind):
            self.metrics["rejected_by_intent_planner"] += 1
            return
        if kind in ("TMB", "TLB"):
            available = self.ledger.available(index)
            unit_cost = bid + (bid * self.config.commission_basis_points + 9999) // 10000
            affordable = max(0, (available - self.config.charge_cents) // unit_cost)
            if quantity > affordable:
                self.metrics["reduced_unaffordable"] += 1
                quantity = affordable
            if not quantity:
                self.metrics["rejected_unaffordable"] += 1
                return
            current = self.holdings.row(account, security)
            if current is not None and int(current[3]) + quantity >= 10**6:
                self.metrics["rejected_holding_width"] += 1
                return
            charge, commission = self.fees(quantity, bid)
            reservation = quantity * bid + charge + commission
            self.ledger.reserve(index, reservation)
        else:
            if quantity * int(prices["low"][security]) <= sum(self.fees(quantity, int(prices["low"][security]))):
                self.metrics["rejected_sell_fee"] += 1
                return
            row = self.holdings.row(account, security)
            row[5] += quantity
            reservation = 0
        self.trade_ordinals[index] += 1
        trade_ordinal = int(self.trade_ordinals[index])
        if trade_ordinal >= TRADE_STRIDE or account * TRADE_STRIDE + trade_ordinal >= 10**15:
            raise ValueError("The checked trade identifier capacity was exceeded.")
        trade_id = account * TRADE_STRIDE + trade_ordinal
        outcome = self.draw(account, offset, "order-outcome", ordinal)
        due = offset if kind in ("TMB", "TMS") else offset + 1 + int(self.draw(account, offset, "order-lifetime", ordinal) * self.config.order_lifetime_max_days)
        cancel = kind in ("TLB", "TLS") and outcome < self.config.limit_cancel_share
        if kind in ("TLB", "TLS") and outcome >= 1 - self.config.limit_open_share:
            due = self.total_days + 1
        order = Order(trade_id, index, security, quantity, bid, kind, offset, due, cancel, reservation, "", second)
        self.orders[trade_id] = order
        self.transition(order, day, second, "SBMT" if kind in ("TMB", "TMS") else "PNDG", creation=True)
        self.account_trade_counts[account] += 1
        self.security_trade_counts[security] += 1
        self.daily_trade_counts[str(day)] += 1
        self.metrics["trades"] += 1
        if kind in ("TMB", "TMS"):
            self.complete(order, day, second, prices)

    def close_accounts(self, day: date, offset: int, indices: tuple[int, ...], prices: dict) -> None:
        """Cancel, liquidate, repay borrowing, withdraw, and mark each account closed."""
        closing = set(indices)
        for order in list(self.orders.values()):
            if order.account_index in closing:
                self.cancel(order, day, MARKET_OPEN_SECOND)
        for index in indices:
            account = int(self.accounts[index]["account_id"])
            for ordinal, security in enumerate(sorted(self.holdings.by_account[account])):
                quantity = int(self.holdings.row(account, security)[3])
                self.submit(index, day, offset, LIQUIDATION_SECOND + ordinal % 3600, prices, 10000 + ordinal, (security, quantity))
            if self.holdings.by_account[account]:
                raise ValueError("Closure requires complete liquidation.")
            if self.ledger.balance[index] < 0:
                self.cash(day, REPAYMENT_SECOND, index, -int(self.ledger.balance[index]), "Deposit")
            if self.ledger.balance[index] > 0:
                self.cash(day, FINAL_WITHDRAWAL_SECOND, index, -int(self.ledger.balance[index]), "Withdrawal")
            self.closed[index] = True
            self.metrics["closures"] += 1

    def watch(self, customer: int, security: int, day: date, second: int, action: str) -> None:
        """Record a watch change with a stable customer and security identity."""
        offset = (day - self.config.start).days
        self.generation.record("WatchHistory", offset,
                               (customer, security + 1, self.start_epoch_second + offset * 86400 + second, action))
        self.metrics[f"watch_{action}"] += 1

    def _schedule_lifetimes(self) -> tuple:
        """Index population dates once instead of scanning all lifetimes each day."""
        joins, opens, closes, inactive = (defaultdict(list) for _ in range(4))
        for customer in self.customers:
            joins[int(customer["open_day"])].append(int(customer["customer_id"]))
            inactive[int(customer["inactivation_day"])].append(int(customer["customer_id"]))
        for index, account in enumerate(self.accounts):
            opens[int(account["open_day"])].append(index)
            closes[int(account["close_day"])].append(index)
        return joins, opens, closes, inactive

    def _begin_day(self, offset: int, schedules: tuple) -> BusinessDay:
        """Resolve closures first so closed accounts cannot propose new activity."""
        joins, opens, closes, inactive = schedules
        if self.config.attrition_enabled:
            # Runtime changes these dates; the opening plan is not authoritative.
            opens[offset] = np.flatnonzero(self.accounts["open_day"] == offset).tolist()
        scheduled = {index for index in closes[offset] if not self.closed[index]}
        forced = {index for customer in inactive[offset] for index in self.customer_accounts[customer]
                  if not self.closed[index] and int(self.accounts[index]["open_day"]) <= offset}
        selected = set(self.planner.closures(self, offset, tuple(sorted(scheduled)))) | forced
        if self.config.attrition_enabled:
            inactive[offset] = self.customers["customer_id"][self.customers["inactivation_day"] == offset].tolist()
            selected |= {i for customer in inactive[offset] for i in self.customer_accounts[customer]
                         if not self.closed[i] and int(self.accounts[i]["open_day"]) <= offset}
        for index in scheduled - selected:
            self.accounts[index]["close_day"] = self.total_days
        for index in selected:
            if not 0 <= index < len(self.accounts) or self.closed[index] or int(self.accounts[index]["open_day"]) > offset:
                raise ValueError("A closure intent requires an existing account.")
            self.accounts[index]["close_day"] = offset
        active = (self.accounts["open_day"] <= offset) & (self.accounts["close_day"] > offset) & ~self.closed
        self.metrics["account_days"] += int(np.count_nonzero(active)) + len(selected)
        if offset + ATTRITION_HORIZON_DAYS < self.total_days:
            self.metrics["fully_observed_lookalike_account_days"] += int(np.count_nonzero(active)) + len(selected)
        customer_active = (self.customers["open_day"] <= offset) & (self.customers["inactivation_day"] > offset)
        self.metrics["customer_days"] += int(np.count_nonzero(customer_active)) + len(inactive[offset])
        if offset == 0 or self.config.day(offset) == self.config.cutover or offset == self.total_days - 1:
            prefix = "initial" if offset == 0 else "cutover" if self.config.day(offset) == self.config.cutover else "final"
            self.metrics[f"{prefix}_active_accounts"] = int(np.count_nonzero(active))
            self.metrics[f"{prefix}_active_customers"] = int(np.count_nonzero(customer_active))
        self.metrics["inactivation_account_closures"] += sum(int(self.accounts[i]["customer_id"]) in inactive[offset] for i in selected)
        day = self.config.day(offset)
        return BusinessDay(day, offset, self.market_cache.day(day), tuple(joins[offset]), tuple(opens[offset]),
                           tuple(sorted(selected)), set(inactive[offset]), np.flatnonzero(active))

    def _open_accounts(self, day: BusinessDay) -> None:
        """Record new lifetimes and schedule required opening deposits."""
        new_customers = set(day.joins)
        for customer in day.joins:
            indices = [i for i in self.customer_accounts[customer] if int(self.accounts[i]["open_day"]) == day.offset]
            self.action("NEW", day.date, 0, customer, indices)
        for index in day.opens:
            account = self.accounts[index]
            account_id = int(account["account_id"])
            customer = int(account["customer_id"])
            if customer not in new_customers:
                self.action("ADDACCT", day.date, 1, customer, [index], account_id)
            fraction = self.config.opening_funding_min + (self.config.opening_funding_max - self.config.opening_funding_min) * self.draw(account_id, day.offset, "opening-funding")
            day.intents.append(Intent(FUNDING_SECOND, account_id, 0, IntentKind.CASH, index,
                                      amount=int(int(account["wealth_cents"]) * fraction), name="Deposit"))

    def _pay_dividends(self, day: BusinessDay) -> None:
        """Pay start-of-day holders before any same-day deposit or purchase."""
        if not self.holdings.index:
            return
        slots = np.fromiter(self.holdings.index.values(), dtype=np.int64)
        rows = self.holdings.data[slots]
        dividends = day.prices["dividends"][rows[:, 1]]
        for position in np.flatnonzero(dividends):
            account, security, _, quantity = rows[position, :4]
            self.cash(day.date, DIVIDEND_SECOND, self.account_indices[int(account)],
                      int(dividends[position]) * int(quantity), "Dividend", reference=int(security) + 1)

    def _plan_cash(self, day: BusinessDay, index: int, deposit: bool, withdrawal: bool) -> None:
        """Apply profile amounts and planner restrictions to ordinary cash proposals."""
        account = self.accounts[index]
        account_id = int(account["account_id"])
        multiplier = self.config.tax_season_multiplier if day.date.month in (4, 12) and int(account["tax_status"]) != 1 else 1
        for selected, name, kind, ordinal in ((deposit, "Deposit", IntentKind.CASH, 100),
                                               (withdrawal, "Withdrawal", IntentKind.WITHDRAW, 101)):
            if not selected or not self.planner.allow_cash(self, index, day.offset, name):
                continue
            purpose = "deposit" if name == "Deposit" else "withdrawal"
            fraction = self.config.cash_wealth_min + (self.config.cash_wealth_max - self.config.cash_wealth_min) * self.draw(account_id, day.offset, f"{purpose}-size")
            amount = max(100, int(int(account["wealth_cents"]) * fraction * multiplier))
            second = FUNDING_SECOND + int(self.draw(account_id, day.offset, f"{purpose}-time") * 14 * 3600)
            day.intents.append(Intent(second, account_id, ordinal, kind, index, amount=amount, name=name))

    def _plan_activity(self, day: BusinessDay) -> None:
        """Expand array-selected proposals, retaining only necessary event records."""
        activity = plan_daily_activity(self, day)
        for position, index in enumerate(activity.indices):
            index = int(index)
            account = self.accounts[index]
            account_id = int(account["account_id"])
            if day.prices["trading"] and self.planner.allow_ordinary(account, day.offset):
                for ordinal in range(int(activity.counts[position])):
                    draw = self.draw(account_id, day.offset, "trade-time", ordinal)
                    width = int(day.prices["close_second"]) - MARKET_OPEN_SECOND - 3
                    fraction = .5 * (2 * draw) ** 3 if draw < .5 else 1 - .5 * (2 * (1 - draw)) ** 3
                    second = MARKET_OPEN_SECOND + int(width * fraction)
                    day.intents.append(Intent(second, account_id, ordinal, IntentKind.SUBMIT, index))
            self._plan_cash(day, index, bool(activity.deposits[position]), bool(activity.withdrawals[position]))
            if activity.updates[position]:
                self.account_versions[index] += 1
                self.action("UPDACCT", day.date, 2, int(account["customer_id"]), [index], int(self.account_versions[index]))

    def _plan_due_orders(self, day: BusinessDay) -> None:
        """Schedule existing orders after the market opens, except final-day orders."""
        if not day.prices["trading"]:
            return
        closing = set(day.closes)
        for order in list(self.orders.values()):
            if order.due_day <= day.offset and order.account_index not in closing:
                account = int(self.accounts[order.account_index]["account_id"])
                day.intents.append(Intent(ORDER_DUE_SECOND, account, order.trade_id, IntentKind.DUE,
                                          order.account_index, order=order))

    def _liquidate(self, day: BusinessDay, intent: Intent) -> list[Intent]:
        """Cancel orders now and schedule sales later, preserving source causality."""
        for order in list(self.orders.values()):
            if order.account_index == intent.index:
                self.cancel(order, day.date, intent.second)
        sales = []
        for ordinal, security in enumerate(sorted(self.holdings.by_account[intent.account_id])):
            quantity = int(self.holdings.row(intent.account_id, security)[3])
            quantity = int(np.ceil(quantity * intent.fraction)) if intent.name != "outflow_candidate" else int(quantity * intent.fraction)
            if quantity <= 0:
                continue
            sales.append(Intent(intent.second + 1 + ordinal, intent.account_id, 30000 + intent.ordinal * 100 + ordinal,
                                IntentKind.FORCE_SELL, intent.index, amount=quantity, security=security, name=intent.name))
        return sales

    def _execute_withdrawal(self, day: BusinessDay, intent: Intent, candidate_sales: set) -> None:
        """Limit withdrawals to funds available at this exact point in the day."""
        index, second = intent.index, intent.second
        if intent.name == "outflow_candidate" and index not in candidate_sales:
            return
        # Attrition drains owned cash, not the unused borrowing allowance.
        available = self.ledger.available(index)
        if intent.kind == IntentKind.DRAIN:
            available = min(available, int(self.ledger.balance[index]))
        amount = min(intent.amount, available)
        if amount > 0:
            self.cash(day.date, second, index, -amount, "Withdrawal")
            if intent.name == "outflow_candidate":
                self.planner.record_outflow_candidate(self, index, day.offset)
        else:
            self.metrics["rejected_withdrawal"] += 1


    def _execute_intents(self, day: BusinessDay) -> None:
        """Apply operations in time order; earlier receipts fund only later spending."""
        queue = [(intent.sort_key, sequence, intent) for sequence, intent in enumerate(day.intents)]
        heapq.heapify(queue)
        sequence = len(queue)
        candidate_sales = set()
        while queue:
            _, _, intent = heapq.heappop(queue)
            index, second = intent.index, intent.second
            if intent.kind == IntentKind.CASH:
                self.cash(day.date, second, index, intent.amount, intent.name)
            elif intent.kind in (IntentKind.WITHDRAW, IntentKind.DRAIN):
                self._execute_withdrawal(day, intent, candidate_sales)
            elif intent.kind == IntentKind.SUBMIT:
                self.submit(index, day.date, day.offset, second, day.prices, intent.ordinal)
            elif intent.kind == IntentKind.LIQUIDATE:
                for sale in self._liquidate(day, intent):
                    heapq.heappush(queue, (sale.sort_key, sequence, sale))
                    sequence += 1
            elif intent.kind == IntentKind.FORCE_SELL:
                row = self.holdings.row(intent.account_id, intent.security)
                quantity = min(intent.amount, int(row[3] - row[5])) if row is not None else 0
                if quantity > 0:
                    before = int(self.ledger.balance[index])
                    self.submit(index, day.date, day.offset, second, day.prices, intent.ordinal,
                                (intent.security, quantity))
                    if intent.name == "outflow_candidate" and int(self.ledger.balance[index]) > before:
                        candidate_sales.add(index)
            elif intent.kind == IntentKind.CANCEL:
                if intent.order.trade_id in self.orders:
                    self.cancel(intent.order, day.date, second)
            elif intent.order.trade_id in self.orders:
                if intent.order.cancel:
                    self.cancel(intent.order, day.date, second)
                else:
                    self.complete(intent.order, day.date, second, day.prices)

    def _update_watches(self, day: BusinessDay, identifier: int, closing_customers: set) -> None:
        """Cancel due watches and create new watches only for active customers."""
        for security, due in sorted(list(self.watches.get(identifier, {}).items())):
            share = self.planner.watch_cancel_share(self, identifier, day.offset, self.config.account_watch_cancel_share)
            cancel_for_account = identifier in closing_customers and self.draw(identifier, day.offset, "close-watch", security) < share
            if due <= day.offset or identifier in day.inactive_customers or cancel_for_account:
                self.watch(identifier, security, day.date, WATCH_CANCEL_SECOND, "CNCL")
                del self.watches[identifier][security]
        if identifier in day.inactive_customers:
            indices = [i for i in day.closes if int(self.accounts[i]["customer_id"]) == identifier]
            self.action("INACT", day.date, CLOSE_SECOND, identifier, indices)
            return
        count = poisson(self.config.watch_daily_rate, self.draw(identifier, day.offset, "watch-count"))
        for ordinal in range(count):
            security = self.choose_security(identifier, day.offset, 1000 + ordinal)
            watches = self.watches.setdefault(identifier, {})
            if security not in watches:
                self.watch(identifier, security, day.date, WATCH_ADD_SECOND + ordinal, "ACTV")
                watches[security] = day.offset + self.config.watch_lifetime_min_days + int(self.draw(identifier, day.offset, "watch-duration", ordinal) * (self.config.watch_lifetime_max_days - self.config.watch_lifetime_min_days + 1))

    def _update_customers(self, day: BusinessDay) -> None:
        """Record customer updates, watch changes, and final lifetime actions."""
        closing_customers = {int(self.accounts[i]["customer_id"]) for i in day.closes}
        closing_customers |= self.planner.leaving_customers(self, day.offset)
        eligible = (self.customers["open_day"] <= day.offset) & (self.customers["inactivation_day"] >= day.offset)
        for customer in self.customers[eligible]:
            identifier = int(customer["customer_id"])
            if identifier not in day.inactive_customers and self.draw(identifier, day.offset, "customer-update") < self.config.annual_customer_update / 365.2425:
                step = -1 if self.draw(identifier, day.offset, "tier-step") < .5 else 1
                self.tiers[identifier] = max(1, min(3, self.tiers[identifier] + step))
                self.tier_array[int(np.searchsorted(self.customers["customer_id"], identifier))] = self.tiers[identifier]
                self.customer_versions[identifier] += 1
                self.action("UPDCUST", day.date, 3, identifier, ordinal=self.customer_versions[identifier])
            self._update_watches(day, identifier, closing_customers)
        for index in day.closes:
            customer = int(self.accounts[index]["customer_id"])
            if customer not in day.inactive_customers:
                self.action("CLOSEACCT", day.date, CLOSE_SECOND, customer, [index], int(self.accounts[index]["account_id"]))

    def _finish_day(self, day: BusinessDay) -> None:
        """Save the cutover trade snapshot and record peak live state."""
        if day.date == self.config.cutover:
            for order in self.orders.values():
                # The initial pending snapshot keeps the original creation date
                # with the order's last recorded second and status.
                self.record_trade(order, day.offset, order.seconds, order.status, value_day=order.created_day)
        self.maximum_open_orders = max(self.maximum_open_orders, len(self.orders))
        self.maximum_watches = max(self.maximum_watches, sum(len(w) for w in self.watches.values()))
        self.planner.after_day(self, day)

    def run(self) -> dict:
        """Run named day steps; only execution changes account money and holdings."""
        schedules = self._schedule_lifetimes()
        for offset in range(self.total_days):
            day = self._begin_day(offset, schedules)
            self._open_accounts(day)
            self._pay_dividends(day)
            self.planner.start_day(self, day)
            self._plan_activity(day)
            self._plan_due_orders(day)
            day.intents.extend(self.planner.intents(self, day))
            self._execute_intents(day)
            if day.closes:
                if not day.prices["trading"]:
                    raise ValueError("The population plan schedules liquidation on a closed-market date.")
                self.close_accounts(day.date, offset, day.closes, day.prices)
            self._update_customers(day)
            self._finish_day(day)
        self.planner.finish(self)
        return self._summary()

    def _summary(self) -> dict:
        """Return measured business counts, not configured volume claims."""
        return {
            **dict(self.metrics), "cash_transaction_types": dict(self.cash_type_counts),
            "account_trade_counts": dict(self.account_trade_counts), "security_trade_counts": dict(self.security_trade_counts),
            "daily_trade_counts": dict(self.daily_trade_counts), "profile_counts": dict(Counter(int(a["profile"]) for a in self.accounts if int(a["open_day"]) < self.total_days)),
            "live_holdings": len(self.holdings.index), "peak_live_holdings": self.holdings.high_water,
            "open_trades": len(self.orders), "peak_open_trades": self.maximum_open_orders,
            "active_watches": sum(len(w) for w in self.watches.values()), "peak_active_watches": self.maximum_watches,
            "numeric_state_buffer_bytes": self.planner.numeric_state_bytes() + sum(array.nbytes for array in self.market_cache.arrays.values()) + sum(array.nbytes for array in (self.holdings.data, self.ledger.balance, self.ledger.reserved, self.ledger.limits, self.accounts, self.customers, self.cash_ordinals, self.trade_ordinals, self.account_versions, self.closed, self.activity_variation, self.tier_array)),
        }


def _run_group(config_dict: dict, root_string: str, first_customer: int, last_customer: int, planner_factory=None, recorder_factory=SourceRecorder) -> dict:
    """Execute one disjoint customer group and publish its actual lifetimes."""
    from .config import Config
    from .generation import Generation
    from .market import Market

    config = Config(**config_dict)
    root = Path(root_string)
    customers = np.load(root / "_work/population/customers.npy", mmap_mode="r+")
    accounts = np.load(root / "_work/population/accounts.npy", mmap_mode="r+")
    left, right = np.searchsorted(accounts["customer_id"], [first_customer, last_customer])
    group_customers = customers[first_customer - 1:last_customer - 1].copy()
    group_accounts = accounts[left:right].copy()
    generation = Generation(config, root, group_id=first_customer)
    if recorder_factory is not SourceRecorder:
        generation.recorder = recorder_factory(generation)
    simulation = BusinessSimulation(generation, Market(root, config), group_customers, group_accounts,
                                    planner=planner_factory() if planner_factory else None)
    if config.attrition_enabled and hasattr(simulation.planner, "normalizer"):
        simulation.planner.normalizer = np.load(root / "_work/population/risk_normalizer.npy", mmap_mode="r")
    result = simulation.run()
    # Groups own disjoint contiguous customer slices. Publish actual lifetimes
    # before the coordinator derives prospect snapshot counts from population.
    customers[first_customer - 1:last_customer - 1] = group_customers
    accounts[left:right] = group_accounts
    customers.flush()
    accounts.flush()
    opened_accounts = group_accounts[group_accounts["open_day"] < config.total_days]
    frame = pl.DataFrame({name: opened_accounts[name] for name in opened_accounts.dtype.names}).with_columns(
        (pl.lit(config.start) + pl.duration(days=pl.col("open_day"))).cast(pl.Date).alias("open_date"),
        pl.when(pl.col("close_day") < config.total_days)
        .then(pl.lit(config.start) + pl.duration(days=pl.col("close_day")))
        .otherwise(None).cast(pl.Date).alias("close_date"),
    )
    hidden = root / "summary" / "hidden"
    hidden.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(hidden / f"accounts-{first_customer:011d}.parquet")
    if hasattr(simulation.planner, "records") and simulation.planner.records:
        pl.DataFrame(simulation.planner.records, schema=ATTRITION_RECORD_SCHEMA).write_parquet(hidden / f"attrition-{first_customer:011d}.parquet")
    generation.finish()
    return result


def generate_activity(generation, market, population, planner_factory=None, recorder_factory=SourceRecorder) -> dict:
    """Seal bounded customer groups and combine counts without moving their typed parts."""
    from concurrent.futures import ProcessPoolExecutor
    from multiprocessing import get_context

    config = generation.config
    root = generation.root
    for path in (root / "summary" / "hidden").glob("accounts-*.parquet"):
        path.unlink()
    maximum_customer = int(population.customers["customer_id"].max())
    groups = [(first, min(maximum_customer + 1, first + config.customer_group_size)) for first in range(1, maximum_customer + 1, config.customer_group_size)]
    parts = root / "_work" / "source-parts"
    parts.mkdir(parents=True, exist_ok=True)
    schedule = parts / "scheduled-groups.json"
    temporary_schedule = schedule.with_suffix(".json.tmp")
    temporary_schedule.write_text(json.dumps([first for first, _ in groups]) + "\n", encoding="ascii")
    temporary_schedule.replace(schedule)
    summaries = []
    if config.workers == 1:
        for first, last in groups:
            summaries.append(_run_group(config.as_dict(), str(root), first, last, planner_factory, recorder_factory))
    else:
        # The coordinator also simulates groups. Only one additional worker runs.
        with ProcessPoolExecutor(max_workers=1, mp_context=get_context("spawn")) as executor:
            for offset in range(0, len(groups), 2):
                first, last = groups[offset]
                future = executor.submit(_run_group, config.as_dict(), str(root), first, last, planner_factory, recorder_factory)
                if offset + 1 < len(groups):
                    a, b = groups[offset + 1]
                    summaries.append(_run_group(config.as_dict(), str(root), a, b, planner_factory, recorder_factory))
                summaries.append(future.result())
    counters = {name: Counter() for name in ("account_trade_counts", "security_trade_counts", "daily_trade_counts", "cash_transaction_types", "profile_counts")}
    totals = Counter()
    peaks = Counter()
    for summary in summaries:
        for key, value in summary.items():
            if key in counters:
                counters[key].update(value)
            elif key.startswith("peak_") or key == "numeric_state_buffer_bytes":
                peaks[key] = max(peaks[key], value)
            else:
                totals[key] += value
    def share(counter, fraction):
        """Measure the activity share made by the busiest configured fraction."""
        counts = sorted(counter.values(), reverse=True)
        return sum(counts[:max(1, math.ceil(len(counts) * fraction))]) / sum(counts) if counts else 0
    account_counts = counters.pop("account_trade_counts")
    security_counts = counters.pop("security_trade_counts")
    return {
        **dict(totals), **dict(peaks), **{key: dict(value) for key, value in counters.items()},
        "account_top_1_percent_trade_share": share(account_counts, .01),
        "account_top_10_percent_trade_share": share(account_counts, .10),
        "security_top_1_percent_trade_share": share(security_counts, .01),
        "customer_groups": len(groups), "generation_stage": 2 if config.attrition_enabled else 1,
        "annual_account_closure_rate": totals["closures"] * 365.2425 / max(1, totals["account_days"]),
        "annual_customer_inactivation_rate": totals["INACT"] * 365.2425 / max(1, totals["customer_days"]),
        "annual_inactivation_account_closure_rate": totals["inactivation_account_closures"] * 365.2425 / max(1, totals["account_days"]),
        "lookalike_target_annual_rate": config.annual_lookalike,
        "lookalike_opportunity_multiplier": config.lookalike_opportunity_multiplier,
        "raw_annual_lookalike_opportunity_rate": config.annual_lookalike * config.lookalike_opportunity_multiplier,
        "annual_lookalike_proposal_rate": totals["lookalike_proposals"] * 365.2425 / max(1, totals["account_days"]),
        "annual_outflow_candidate_rate": totals["outflow_candidates"] * 365.2425 / max(1, totals["account_days"]),
        "annual_confirmed_lookalike_rate": totals["confirmed_lookalikes"] * 365.2425 / max(1, totals["fully_observed_lookalike_account_days"]),
    }
