"""Day-ordered business execution. Planning and recording do not mutate the ledger."""

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from types import MappingProxyType
import math

import numpy as np
import polars as pl

from .customers import account_values, customer_values, xml_action
from .random_values import uniform, values

TRADE_STRIDE = 1_000_000
DAY_KEY_STRIDE = 1_000_000_000


def decimal_cents(cents):
    cents = int(cents)
    return f"{'-' if cents < 0 else ''}{abs(cents) // 100}.{abs(cents) % 100:02d}"


def timestamp(day, seconds):
    return f"{day} {seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"


def poisson(rate, draw):
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


class SourceRecorder:
    """The recording seam receives immutable correct business fields."""

    def __init__(self, generation, transform=None):
        self.generation = generation
        self.transform = transform or (lambda source, day, fields: fields)

    def record(self, source, day, fields, key, flag="I"):
        recorded = self.transform(source, day, MappingProxyType(dict(fields)))
        self.generation.emit(source, day, [{**recorded, "_key": key, "_flag": flag}])


class HoldingBook:
    """Sparse, reusable numeric buffers keep live holdings, not buy lots or history."""

    def __init__(self, capacity=64):
        self.data = np.zeros((capacity, 6), dtype=np.int64)
        self.index = {}
        self.by_account = defaultdict(set)
        self.free = list(range(capacity - 1, -1, -1))
        self.high_water = 0

    def row(self, account, security):
        slot = self.index.get((account, security))
        return None if slot is None else self.data[slot]

    def change(self, account, security, trade_id, signed_quantity, value_cents):
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

    def snapshot(self):
        rows = [self.data[slot] for slot in self.index.values()]
        return pl.DataFrame(rows, schema=["account", "security", "origin", "quantity", "cost_cents", "reserved"], orient="row") if rows else pl.DataFrame()


class Ledger:
    def __init__(self, accounts):
        self.balance = np.zeros(len(accounts), dtype=np.int64)
        self.reserved = np.zeros(len(accounts), dtype=np.int64)
        self.limits = np.asarray(accounts["margin_limit_cents"], dtype=np.int64)

    def available(self, index):
        return int(self.balance[index]) + int(self.limits[index]) - int(self.reserved[index])

    def reserve(self, index, amount):
        if amount < 0 or amount > self.available(index):
            raise ValueError("An order cannot reserve unavailable funds.")
        self.reserved[index] += amount

    def apply(self, index, amount):
        after = int(self.balance[index]) + int(amount)
        if after < -int(self.limits[index]) or abs(after) >= 10**12:
            raise ValueError("A cash transaction exceeds the account's spending or source-width limit.")
        if after + int(self.limits[index]) < int(self.reserved[index]):
            raise ValueError("A cash transaction spends reserved funds.")
        self.balance[index] = after


@dataclass
class Order:
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
    def sell(self):
        return self.kind in ("TMS", "TLS")


class Stage1IntentPlanner:
    """Generation stage 2 replaces closure selection and intent planning here."""

    def closing(self, account, day):
        return int(account["close_day"]) == day

    def allow_ordinary(self, account, day):
        return int(account["open_day"]) <= day < int(account["close_day"])


class BusinessSimulation:
    def __init__(self, generation, market, customers, accounts, planner=None, recorder=None):
        self.generation = generation
        self.config = generation.config
        self.market = market
        self.customers = customers
        self.accounts = accounts
        self.planner = planner or Stage1IntentPlanner()
        self.recorder = recorder or SourceRecorder(generation)
        self.ledger = Ledger(accounts)
        self.holdings = HoldingBook()
        self.orders = {}
        self.watches = {}
        self.cash_ordinals = np.zeros(len(accounts), dtype=np.int64)
        self.trade_ordinals = np.zeros(len(accounts), dtype=np.int64)
        self.account_versions = np.zeros(len(accounts), dtype=np.int64)
        self.customer_versions = Counter()
        self.tiers = {int(c["customer_id"]): int(c["tier"]) for c in customers}
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
        self.activity_variation = np.array([self.config.activity_variation_min + (self.config.activity_variation_max - self.config.activity_variation_min) * uniform(int(a["account_id"]), self.config.seed, "activity-variation") ** self.config.activity_variation_power for a in accounts])

    def draw(self, identity, day, purpose, ordinal=0):
        key = (day + 1) * DAY_KEY_STRIDE + int(identity)
        return uniform(key, self.config.seed, f"{purpose}:{ordinal}")

    def cash(self, day, second, index, amount, name):
        if not amount:
            return
        self.ledger.apply(index, amount)
        self.cash_ordinals[index] += 1
        account = int(self.accounts[index]["account_id"])
        self.generation.cash(day, timestamp(day, second), account, int(self.cash_ordinals[index]), int(amount), name)
        kind = "Settlement" if name.startswith("Trade settlement ") else "Dividend" if name.startswith("Dividend ") else name
        self.cash_type_counts[kind] += 1

    def account_fields(self, index, inactive=False):
        fields = account_values(self.config, self.accounts[index])
        version = int(self.account_versions[index])
        if version:
            fields["CA_NAME"] = f"Account {fields['CA_ID']} version {version}"
        if inactive:
            fields["CA_ST_ID"] = "INAC"
        return fields

    def customer_fields(self, identifier, inactive=False):
        fields = customer_values(identifier, self.tiers[identifier])
        if inactive:
            fields["C_ST_ID"] = "INAC"
        return fields

    def action(self, kind, day, second, customer, indices=(), ordinal=0):
        ts = timestamp(day, second)
        inactive = kind in ("INACT", "CLOSEACCT")
        cv = self.customer_fields(customer, kind == "INACT")
        av = [self.account_fields(i, inactive) for i in indices]
        key = f"{ts}/{customer:011d}/{ordinal:08d}/{kind}"
        if day <= self.config.cutover:
            payload = xml_action(kind, ts.replace(" ", "T"), customer, cv, av)
            self.generation.text("offline/initial/CustomerMgmt.xml", [(key, payload)])
        else:
            if kind in ("NEW", "UPDCUST", "INACT"):
                self.recorder.record("Customer", day, cv, key, "I" if kind == "NEW" else "U")
            if kind != "UPDCUST":
                for i, fields in zip(indices, av):
                    self.recorder.record("Account", day, fields, f"{ts}/{fields['CA_ID']:011d}/{ordinal:08d}", "I" if kind in ("NEW", "ADDACCT") else "U")
        self.metrics[kind] += 1

    def trade_fields(self, order, day, seconds, status, price=None):
        account = self.accounts[order.account_index]
        charge, commission = self.fees(order.quantity, price) if price is not None else (None, None)
        return {
            "T_ID": order.trade_id, "T_DTS": timestamp(day, seconds), "T_ST_ID": status,
            "T_TT_ID": order.kind, "T_IS_CASH": int(account["is_cash"]),
            "T_S_SYMB": f"S{order.security + 1:010d}", "T_QTY": order.quantity,
            "T_BID_PRICE": decimal_cents(order.bid), "T_CA_ID": int(account["account_id"]),
            "T_EXEC_NAME": "Alex Broker", "T_TRADE_PRICE": decimal_cents(price) if price is not None else "",
            "T_CHRG": decimal_cents(charge) if charge is not None else "",
            "T_COMM": decimal_cents(commission) if commission is not None else "",
            "T_TAX": "0.00" if price is not None else "",
        }

    def transition(self, order, day, seconds, status, price=None, creation=False):
        order.status = status
        order.seconds = seconds
        fields = self.trade_fields(order, day, seconds, status, price)
        order.transition += 1
        if day <= self.config.cutover:
            self.recorder.record("TradeHistory", day, {"TH_T_ID": order.trade_id, "TH_DTS": fields["T_DTS"], "TH_ST_ID": status}, f"{order.trade_id:015d}/{fields['T_DTS']}/{order.transition:08d}")
            if status in ("CMPT", "CNCL"):
                self.recorder.record("Trade", day, fields, f"{order.trade_id:015d}")
        else:
            self.recorder.record("Trade", day, fields, f"{fields['T_DTS']}/{order.trade_id:015d}/{order.transition:08d}", "I" if creation else "U")
        if status in ("CMPT", "CNCL"):
            self.metrics[status] += 1

    def fees(self, quantity, price):
        return self.config.charge_cents, (quantity * price * self.config.commission_basis_points + 5000) // 10000

    def release(self, order):
        index = order.account_index
        account = int(self.accounts[index]["account_id"])
        if order.sell:
            row = self.holdings.row(account, order.security)
            if row is None or row[5] < order.quantity:
                raise ValueError("A sell lost its holding reservation.")
            row[5] -= order.quantity
        else:
            self.ledger.reserved[index] -= order.reservation

    def cancel(self, order, day, second):
        self.release(order)
        self.transition(order, day, second, "CNCL")
        self.orders.pop(order.trade_id, None)

    def complete(self, order, day, second, prices):
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
        self.cash(day, second, order.account_index, delta, f"Trade settlement {order.trade_id}")
        account = int(self.accounts[order.account_index]["account_id"])
        origin, before, after = self.holdings.change(account, order.security, order.trade_id, -order.quantity if order.sell else order.quantity, amount + charge + commission)
        self.transition(order, day, second, "CMPT", price)
        self.recorder.record("HoldingHistory", day, {"HH_H_T_ID": origin, "HH_T_ID": order.trade_id, "HH_BEFORE_QTY": before, "HH_AFTER_QTY": after}, f"{timestamp(day, second)}/{account:011d}/{int(self.cash_ordinals[order.account_index]):012d}")
        self.orders.pop(order.trade_id, None)
        return True

    def choose_security(self, account, offset, ordinal):
        draw = self.draw(account, offset, "security", ordinal)
        share = self.config.security_top_share
        if self.popular_count == self.config.securities:
            return min(self.config.securities - 1, int(draw * self.config.securities))
        if draw < share:
            return min(self.popular_count - 1, int(draw / share * self.popular_count))
        return self.popular_count + min(self.config.securities - self.popular_count - 1, int((draw - share) / (1 - share) * (self.config.securities - self.popular_count)))

    def submit(self, index, day, offset, second, prices, ordinal, force_sell=None):
        account = int(self.accounts[index]["account_id"])
        draw = self.draw(account, offset, "trade-kind", ordinal)
        thresholds = np.cumsum(self.config.order_shares)
        kind = ("TMB", "TLB", "TLS", "TMS")[min(3, int(np.searchsorted(thresholds, draw, side="right")))]
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
            security = self.choose_security(account, offset, ordinal)
            value = int(int(self.accounts[index]["wealth_cents"]) * (self.config.trade_wealth_min + (self.config.trade_wealth_max - self.config.trade_wealth_min) * self.draw(account, offset, "trade-size", ordinal)))
            quantity = max(1, min(999999, value // int(prices["high"][security])))
        bid = int(prices["close"][security])
        if kind in ("TMB", "TMS"):
            bid = int(prices["high"][security]) if kind == "TMB" else int(prices["low"][security])
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

    def close_accounts(self, day, offset, indices, prices):
        closing = set(indices)
        for order in list(self.orders.values()):
            if order.account_index in closing:
                self.cancel(order, day, 9 * 3600 + 30 * 60)
        for index in indices:
            account = int(self.accounts[index]["account_id"])
            for ordinal, security in enumerate(sorted(self.holdings.by_account[account])):
                quantity = int(self.holdings.row(account, security)[3])
                self.submit(index, day, offset, 10 * 3600 + ordinal % 3600, prices, 10000 + ordinal, (security, quantity))
            if self.holdings.by_account[account]:
                raise ValueError("Closure requires complete liquidation.")
            if self.ledger.balance[index] < 0:
                self.cash(day, 14 * 3600, index, -int(self.ledger.balance[index]), "Deposit")
            if self.ledger.balance[index] > 0:
                self.cash(day, 15 * 3600, index, -int(self.ledger.balance[index]), "Withdrawal")
            self.metrics["closures"] += 1

    def watch(self, customer, security, day, second, action):
        self.recorder.record("WatchHistory", day, {"W_C_ID": customer, "W_S_SYMB": f"S{security + 1:010d}", "W_DTS": timestamp(day, second), "W_ACTION": action}, f"{timestamp(day, second)}/{customer:011d}/{security:010d}/{action}")
        self.metrics[f"watch_{action}"] += 1

    def run(self):
        config = self.config
        joins = defaultdict(list)
        opens = defaultdict(list)
        closes = defaultdict(list)
        inactivations = defaultdict(list)
        for customer in self.customers:
            joins[int(customer["open_day"])].append(int(customer["customer_id"]))
            inactivations[int(customer["inactivation_day"])].append(int(customer["customer_id"]))
        for index, account in enumerate(self.accounts):
            opens[int(account["open_day"])].append(index)
            closes[int(account["close_day"])].append(index)
        for offset in range(self.total_days):
            day = config.start + timedelta(days=offset)
            prices = self.market.day(day)
            active = (self.accounts["open_day"] <= offset) & (self.accounts["close_day"] > offset)
            self.metrics["account_days"] += int(np.count_nonzero(active)) + len(closes[offset])
            new_customers = set(joins[offset])
            for customer in joins[offset]:
                indices = [i for i in self.customer_accounts[customer] if int(self.accounts[i]["open_day"]) == offset]
                self.action("NEW", day, 0, customer, indices)
            for index in opens[offset]:
                customer = int(self.accounts[index]["customer_id"])
                if customer not in new_customers:
                    self.action("ADDACCT", day, 1, customer, [index], int(self.accounts[index]["account_id"]))
            # Dividends precede today's deposits and trades, so today's buys do not qualify.
            for (account, security), slot in list(self.holdings.index.items()):
                dividend = int(prices["dividends"][security])
                if dividend:
                    self.cash(day, 6 * 3600, self.account_indices[account], dividend * int(self.holdings.data[slot, 3]), f"Dividend S{security + 1:010d}")
            intents = []
            for index in opens[offset]:
                funding = int(int(self.accounts[index]["wealth_cents"]) * (config.opening_funding_min + (config.opening_funding_max - config.opening_funding_min) * self.draw(int(self.accounts[index]["account_id"]), offset, "opening-funding")))
                intents.append((8 * 3600, int(self.accounts[index]["account_id"]), 0, "cash", index, (funding, "Deposit")))
            indices = np.flatnonzero(active)
            if len(indices):
                ids = self.accounts["account_id"][indices].astype(np.uint64)
                keys = ids + np.uint64((offset + 1) * DAY_KEY_STRIDE)
                draws = (values(keys, config.seed, "daily-trading") >> np.uint64(11)).astype(np.float64) / 2**53
                profiles = self.accounts["profile"][indices].astype(np.int64)
                weekend_multiplier = config.weekend_cash_multiplier if day.weekday() >= 5 else 1
                deposit_hazards = (values(keys, config.seed, "deposit-hazard:0") >> np.uint64(11)).astype(np.float64) / 2**53
                withdrawal_hazards = (values(keys, config.seed, "withdrawal-hazard:0") >> np.uint64(11)).astype(np.float64) / 2**53
                update_draws = (values(keys, config.seed, "account-update:0") >> np.uint64(11)).astype(np.float64) / 2**53
                regular_dates = np.minimum(28, 25 + ids % 4) == day.day
                deposits = ((profiles == 1) & regular_dates) | (deposit_hazards < np.asarray(config.annual_deposit_rates)[profiles] / 365.2425 * weekend_multiplier)
                withdrawals = ((profiles == 2) & regular_dates) | (withdrawal_hazards < np.asarray(config.annual_withdrawal_rates)[profiles] / 365.2425 * weekend_multiplier)
                updates = update_draws < config.annual_account_update / 365.2425
                rates = np.zeros(len(indices))
                if prices["trading"]:
                    rates = np.asarray(config.annual_trade_rates)[profiles] * self.activity_variation[indices] / self.market.sessions_per_year[day.year]
                    if prices["shock"]:
                        rates *= config.shock_trade_multiplier
                candidates = np.flatnonzero((draws >= np.exp(-rates)) | deposits | withdrawals | updates)
                for position in candidates:
                    index = indices[position]
                    account = self.accounts[index]
                    account_id = int(account["account_id"])
                    profile = int(account["profile"])
                    if prices["trading"] and self.planner.allow_ordinary(account, offset):
                        sessions = self.market.sessions_per_year[day.year]
                        rate = config.annual_trade_rates[profile] * self.activity_variation[index] / sessions
                        rate *= config.shock_trade_multiplier if prices["shock"] else 1
                        count = poisson(rate, float(draws[position]))
                        for ordinal in range(count):
                            draw = self.draw(account_id, offset, "trade-time", ordinal)
                            width = int(prices["close_second"]) - (9 * 3600 + 30 * 60) - 3
                            fraction = .5 * (2 * draw) ** 3 if draw < .5 else 1 - .5 * (2 * (1 - draw)) ** 3
                            second = 9 * 3600 + 30 * 60 + int(width * fraction)
                            intents.append((second, account_id, ordinal, "submit", int(index), ordinal))
                    deposit = bool(deposits[position])
                    withdrawal = bool(withdrawals[position])
                    if deposit or withdrawal:
                        multiplier = config.tax_season_multiplier if day.month in (4, 12) and int(account["tax_status"]) != 1 else 1
                        if deposit:
                            amount = max(100, int(int(account["wealth_cents"]) * (config.cash_wealth_min + (config.cash_wealth_max - config.cash_wealth_min) * self.draw(account_id, offset, "deposit-size")) * multiplier))
                            intents.append((8 * 3600 + int(self.draw(account_id, offset, "deposit-time") * 14 * 3600), account_id, 100, "cash", int(index), (amount, "Deposit")))
                        if withdrawal:
                            amount = max(100, int(int(account["wealth_cents"]) * (config.cash_wealth_min + (config.cash_wealth_max - config.cash_wealth_min) * self.draw(account_id, offset, "withdrawal-size")) * multiplier))
                            intents.append((8 * 3600 + int(self.draw(account_id, offset, "withdrawal-time") * 14 * 3600), account_id, 101, "withdraw", int(index), amount))
                    if updates[position]:
                        self.account_versions[index] += 1
                        self.action("UPDACCT", day, 2, int(account["customer_id"]), [int(index)], int(self.account_versions[index]))
            if prices["trading"]:
                for order in list(self.orders.values()):
                    if order.due_day <= offset and order.account_index not in closes[offset]:
                        account_id = int(self.accounts[order.account_index]["account_id"])
                        intents.append((9 * 3600 + 35 * 60, account_id, order.trade_id, "due", order.account_index, order))
            for second, account_id, ordinal, kind, index, payload in sorted(intents, key=lambda item: item[:4]):
                if kind == "cash":
                    self.cash(day, second, index, *payload)
                elif kind == "withdraw":
                    amount = min(payload, self.ledger.available(index))
                    if amount > 0:
                        self.cash(day, second, index, -amount, "Withdrawal")
                    else:
                        self.metrics["rejected_withdrawal"] += 1
                elif kind == "submit":
                    self.submit(index, day, offset, second, prices, payload)
                elif payload.trade_id in self.orders:
                    if payload.cancel:
                        self.cancel(payload, day, second)
                    else:
                        self.complete(payload, day, second, prices)
            if closes[offset]:
                if not prices["trading"]:
                    raise ValueError("The population plan schedules liquidation on a closed-market date.")
                self.close_accounts(day, offset, closes[offset], prices)
            inactive_customers = set(inactivations[offset])
            closing_customers = {int(self.accounts[i]["customer_id"]) for i in closes[offset]}
            for customer in self.customers:
                identifier = int(customer["customer_id"])
                if int(customer["open_day"]) > offset or int(customer["inactivation_day"]) < offset:
                    continue
                if identifier not in inactive_customers and self.draw(identifier, offset, "customer-update") < config.annual_customer_update / 365.2425:
                    step = -1 if self.draw(identifier, offset, "tier-step") < .5 else 1
                    self.tiers[identifier] = max(1, min(3, self.tiers[identifier] + step))
                    self.customer_versions[identifier] += 1
                    self.action("UPDCUST", day, 3, identifier, ordinal=self.customer_versions[identifier])
                for security, due in sorted(list(self.watches.get(identifier, {}).items())):
                    cancel_for_account = identifier in closing_customers and self.draw(identifier, offset, "close-watch", security) < config.account_watch_cancel_share
                    if due <= offset or identifier in inactive_customers or cancel_for_account:
                        self.watch(identifier, security, day, 21 * 3600, "CNCL")
                        del self.watches[identifier][security]
                if identifier in inactive_customers:
                    self.action("INACT", day, 23 * 3600, identifier, closes[offset] and [i for i in closes[offset] if int(self.accounts[i]["customer_id"]) == identifier])
                else:
                    count = poisson(config.watch_daily_rate, self.draw(identifier, offset, "watch-count"))
                    for ordinal in range(count):
                        security = self.choose_security(identifier, offset, 1000 + ordinal)
                        watches = self.watches.setdefault(identifier, {})
                        if security not in watches:
                            self.watch(identifier, security, day, 22 * 3600 + ordinal, "ACTV")
                            watches[security] = offset + config.watch_lifetime_min_days + int(self.draw(identifier, offset, "watch-duration", ordinal) * (config.watch_lifetime_max_days - config.watch_lifetime_min_days + 1))
            for index in closes[offset]:
                customer = int(self.accounts[index]["customer_id"])
                if customer not in inactive_customers:
                    self.action("CLOSEACCT", day, 23 * 3600, customer, [index], int(self.accounts[index]["account_id"]))
            if day == config.cutover:
                for order in self.orders.values():
                    self.recorder.record("Trade", day, self.trade_fields(order, config.start + timedelta(days=order.created_day), order.seconds, order.status), f"{order.trade_id:015d}")
            self.maximum_open_orders = max(self.maximum_open_orders, len(self.orders))
            self.maximum_watches = max(self.maximum_watches, sum(len(w) for w in self.watches.values()))
        return {
            **dict(self.metrics), "cash_transaction_types": dict(self.cash_type_counts),
            "account_trade_counts": dict(self.account_trade_counts), "security_trade_counts": dict(self.security_trade_counts),
            "daily_trade_counts": dict(self.daily_trade_counts), "profile_counts": dict(self.profile_counts),
            "live_holdings": len(self.holdings.index), "peak_live_holdings": self.holdings.high_water,
            "open_trades": len(self.orders), "peak_open_trades": self.maximum_open_orders,
            "active_watches": sum(len(w) for w in self.watches.values()), "peak_active_watches": self.maximum_watches,
            "state_buffer_bytes": self.holdings.data.nbytes + self.ledger.balance.nbytes + self.ledger.reserved.nbytes,
        }


def _run_group(config_dict, root_string, first_customer, last_customer):
    from .config import Config
    from .generation import Generation
    from .market import Market

    config = Config(**config_dict)
    root = Path(root_string)
    customers = np.load(root / "_work/population/customers.npy", mmap_mode="r")
    accounts = np.load(root / "_work/population/accounts.npy", mmap_mode="r")
    group_customers = customers[(customers["customer_id"] >= first_customer) & (customers["customer_id"] < last_customer)]
    group_accounts = accounts[(accounts["customer_id"] >= first_customer) & (accounts["customer_id"] < last_customer)]
    group_root = root / "_work/groups" / f"{first_customer:011d}"
    generation = Generation(config, group_root)
    simulation = BusinessSimulation(generation, Market(root, config), group_customers, group_accounts)
    result = simulation.run()
    generation.close()
    return str(group_root), result


def generate_activity(generation, market, population):
    from concurrent.futures import ProcessPoolExecutor
    from multiprocessing import get_context

    config = generation.config
    root = generation.root
    maximum_customer = int(population.customers["customer_id"].max())
    groups = [(first, min(maximum_customer + 1, first + config.customer_group_size)) for first in range(1, maximum_customer + 1, config.customer_group_size)]
    summaries = []
    if config.workers == 1:
        for first, last in groups:
            path, summary = _run_group(config.as_dict(), str(root), first, last)
            generation.adopt(Path(path))
            summaries.append(summary)
    else:
        # The coordinator also simulates groups. Only one additional worker runs.
        with ProcessPoolExecutor(max_workers=1, mp_context=get_context("spawn")) as executor:
            for offset in range(0, len(groups), 2):
                first, last = groups[offset]
                future = executor.submit(_run_group, config.as_dict(), str(root), first, last)
                if offset + 1 < len(groups):
                    a, b = groups[offset + 1]
                    path, summary = _run_group(config.as_dict(), str(root), a, b)
                    generation.adopt(Path(path))
                    summaries.append(summary)
                path, summary = future.result()
                generation.adopt(Path(path))
                summaries.append(summary)
    counters = {name: Counter() for name in ("account_trade_counts", "security_trade_counts", "daily_trade_counts", "cash_transaction_types", "profile_counts")}
    totals = Counter()
    peaks = Counter()
    for summary in summaries:
        for key, value in summary.items():
            if key in counters:
                counters[key].update(value)
            elif key.startswith("peak_") or key == "state_buffer_bytes":
                peaks[key] = max(peaks[key], value)
            else:
                totals[key] += value
    def share(counter, fraction):
        counts = sorted(counter.values(), reverse=True)
        return sum(counts[:max(1, math.ceil(len(counts) * fraction))]) / sum(counts) if counts else 0
    account_counts = counters.pop("account_trade_counts")
    security_counts = counters.pop("security_trade_counts")
    return {
        **dict(totals), **dict(peaks), **{key: dict(value) for key, value in counters.items()},
        "account_top_1_percent_trade_share": share(account_counts, .01),
        "account_top_10_percent_trade_share": share(account_counts, .10),
        "security_top_1_percent_trade_share": share(security_counts, .01),
        "customer_groups": len(groups), "generation_stage": 1,
    }
