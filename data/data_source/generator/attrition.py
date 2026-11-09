"""Risk-based leaving behavior with past account returns and customer-wide exits."""

import calendar
from collections import Counter, defaultdict
from dataclasses import dataclass

import numpy as np

from .activity import Stage1IntentPlanner
from .business import Intent, IntentKind, LIQUIDATION_SECOND, FINAL_WITHDRAWAL_SECOND, MARKET_OPEN_SECOND, ATTRITION_HORIZON_DAYS
from .random_values import uniforms

PROFILE_RISKS = np.asarray([2., 1., .5, 1.5, 1.5])
RETURN_HISTORY_DAYS = 96
CLASSIFICATION_COUNTS = {
    "confirmed_lookalike": "confirmed_lookalikes",
    "closing": "closing_outflow_candidates",
    "censored": "censored_outflow_candidates",
    "closed_warning": "closed_warning_plans",
    "closed_no_warning": "closed_no_warning_plans",
    "superseded": "superseded_exit_plans",
    "unclosed": "unclosed_exit_plans",
    "unmatched_close": "unmatched_closed_plans",
}


def three_months_before(day):
    """Subtract three calendar months, clipping the day at the month boundary."""
    ordinal = day.year * 12 + day.month - 1 - 3
    year, month = divmod(ordinal, 12)
    month += 1
    return day.replace(year=year, month=month, day=min(day.day, calendar.monthrange(year, month)[1]))


def risk_factors(profiles, tenure_days, tiers, trailing_returns):
    """Apply accepted relative risks; missing return history has no loss factor."""
    return (PROFILE_RISKS[np.asarray(profiles)] * np.where(np.asarray(tenure_days) < 365, 2., 1.)
            * np.where(np.asarray(tiers) == 1, .7, 1.)
            * np.where(np.asarray(trailing_returns) < -.2, 1.5, 1.))


@dataclass(frozen=True)
class LeavingPeriod:
    """Describe one hidden exit plan without placing it in source records."""

    start: int
    close: int
    steps: tuple[int, ...]
    customer_exit: bool = False


class ReturnHistory:
    """Keep bounded daily time-weighted equity indexes, excluding external flows."""

    def __init__(self, count: int):
        """Allocate per-account return state and a 96-day circular history."""
        self.previous_equity = np.zeros(count, dtype=np.float64)
        self.index = np.ones(count, dtype=np.float64)
        self.history = np.full((RETURN_HISTORY_DAYS, count), np.nan)
        self.valid_since = np.full(count, 2**31 - 1, dtype=np.int64)
        self.loss_day = np.full(count, -1000, dtype=np.int64)
        self.loss_valid_since = np.full(count, 2**31 - 1, dtype=np.int64)

    def observe(self, offset: int, equity: np.ndarray, external_flow: np.ndarray) -> None:
        """Remove today's deposits and withdrawals before measuring the return."""
        # End-of-day external-flow adjustment keeps funding and withdrawals from
        # appearing as investment gains or losses. A real capital wipeout retains
        # a 100-percent-loss marker even after ordinary return ratios become
        # undefined. An external withdrawal alone does not create that marker.
        adjusted = equity - external_flow
        wiped_out = (self.previous_equity > 0) & (adjusted <= 0)
        self.loss_day[wiped_out] = offset
        self.loss_valid_since[wiped_out] = self.valid_since[wiped_out]
        valid = (self.previous_equity > 0) & (adjusted > 0) & (equity > 0)
        self.index[valid] *= adjusted[valid] / self.previous_equity[valid]
        self.index[~valid] = 1.
        self.valid_since[~valid] = offset
        self.history[offset % RETURN_HISTORY_DAYS] = np.where(equity > 0, self.index, np.nan)
        self.previous_equity[:] = equity

    def trailing(self, config, offset: int) -> np.ndarray:
        """Use only completed days, with an exact three-calendar-month lookback."""
        if offset <= 0:
            return np.full(len(self.index), np.nan)
        last = config.day(offset - 1)
        anchor = (three_months_before(last) - config.start).days
        if anchor < 0:
            return np.full(len(self.index), np.nan)
        prior = self.history[anchor % RETURN_HISTORY_DAYS]
        valid = (self.valid_since <= anchor) & np.isfinite(prior) & (prior > 0)
        result = np.full(len(self.index), np.nan)
        np.divide(self.index, prior, out=result, where=valid)
        result[valid] -= 1
        wiped_out = (self.loss_day >= anchor) & (self.loss_day < offset) & (self.loss_valid_since <= anchor)
        result[wiped_out] = -1.
        return result


class AttritionPlanner(Stage1IntentPlanner):
    """Select cause-specific hazards and translate exits into ordinary events."""

    def prepare(self, simulation) -> None:
        """Attach bounded state; hidden traits never become model features."""
        count = len(simulation.accounts)
        self.returns = ReturnHistory(count)
        self.external_flow = np.zeros(count, dtype=np.int64)
        self.leaving = {}
        self.warning_accounts = defaultdict(set)
        self.records = []
        self.final_plan_records = {}
        self.customer_plans = {}
        self.normalizer = getattr(simulation, "risk_normalizer", None)
        self.customer_indices = {int(row["customer_id"]): i for i, row in enumerate(simulation.customers)}
        self.account_indices = simulation.account_indices
        # Holdings keep insertion order. A sorted ID lookup maps their rows to
        # equity slots without a Python account lookup for every position.
        self.account_id_order = np.argsort(simulation.accounts["account_id"])
        self.sorted_account_ids = simulation.accounts["account_id"][self.account_id_order]
        self.owners = np.searchsorted(simulation.customers["customer_id"], simulation.accounts["customer_id"])
        self.leaving_mask = np.zeros(count, dtype=np.bool_)
        self.customer_leaving = np.zeros(len(simulation.customers), dtype=np.bool_)
        self.risks = np.ones(count)

    def numeric_state_bytes(self) -> int:
        """Count all planner arrays, including the circular return history."""
        arrays = [value for value in vars(self).values() if isinstance(value, np.ndarray)]
        arrays += [value for value in vars(self.returns).values() if isinstance(value, np.ndarray)]
        return sum(array.nbytes for array in arrays)

    def _next_session(self, simulation, offset: int) -> int:
        """Move a proposed final liquidation forward to an actual market session."""
        while offset < simulation.total_days and not simulation.market_cache.day(simulation.config.day(offset))["trading"]:
            offset += 1
        return offset

    def _period(self, simulation, identity: int, offset: int, customer: bool) -> LeavingPeriod:
        """Draw one warning and liquidation plan from a stable identity and date."""
        config = simulation.config
        purpose = "customer-exit" if customer else "account-exit"
        warning = simulation.draw(identity, offset, purpose + "-warning") < config.attrition_warning_share
        weeks = config.attrition_warning_min_weeks + int(simulation.draw(identity, offset, purpose + "-weeks") * (config.attrition_warning_max_weeks - config.attrition_warning_min_weeks + 1))
        close = self._next_session(simulation, offset + weeks * 7 if warning else offset)
        if warning and config.attrition_warning_max_weeks > 1 and close - offset > config.attrition_warning_max_weeks * 7 and close < simulation.total_days:
            # Keep the maximum warning length at eight weeks when its nominal
            # last date is a weekend or holiday. Calendar dates are known data.
            close = offset + config.attrition_warning_max_weeks * 7
            while close > offset + 7 and not simulation.market_cache.day(config.day(close))["trading"]:
                close -= 1
        count = config.attrition_liquidation_min_steps + int(simulation.draw(identity, offset, purpose + "-steps") * (config.attrition_liquidation_max_steps - config.attrition_liquidation_min_steps + 1))
        # A no-warning exit liquidates on its final day. Warning steps span the
        # full leaving period and move forward over holidays and weekends.
        steps = tuple(sorted({self._next_session(simulation, int(day)) for day in
                              np.linspace(offset, max(offset, close - 1), count)})) if warning else ()
        return LeavingPeriod(offset, close, steps, customer)

    def _start(self, simulation, index: int, period: LeavingPeriod) -> None:
        """Set a lifetime endpoint and keep the hidden explanation in the summary."""
        prior = self.leaving.get(index)
        if prior is not None and prior.steps:
            self.warning_accounts[prior.start].remove(index)
            if not self.warning_accounts[prior.start]:
                del self.warning_accounts[prior.start]
        self.leaving[index] = period
        self.leaving_mask[index] = True
        if period.steps:
            self.warning_accounts[period.start].add(index)
        simulation.accounts[index]["close_day"] = min(period.close, simulation.total_days)
        self.final_plan_records[index] = len(self.records)
        self.records.append({"account_id": int(simulation.accounts[index]["account_id"]),
                             "start_day": period.start, "close_day": period.close,
                             "steps": list(period.steps), "customer_exit": period.customer_exit,
                             "kind": "leaving", "classification": None, "final_plan": None,
                             "actual_close_day": None, "outcome_day": None})
        simulation.metrics["leaving_periods"] += bool(period.steps)
        simulation.metrics["exit_plans"] += 1

    def _risk_curve(self, simulation, offset: int) -> float:
        """Use a shared expected-risk curve, never a group-dependent average."""
        if self.normalizer is not None:
            return max(.01, float(self.normalizer[offset]))
        config = simulation.config
        profile = float(np.dot(config.profile_shares, PROFILE_RISKS))
        return profile * (1 - .3 * config.tier_shares[0]) * (2 if offset < 365 else 1)

    def closures(self, simulation, day: int, scheduled_indices) -> list[int]:
        """Select hazards using yesterday's account results, then return due exits."""
        accounts = simulation.accounts
        active = (accounts["open_day"] <= day) & ~simulation.closed & (accounts["open_day"] < simulation.total_days)
        indices = np.flatnonzero(active)
        tier_values = simulation.tier_array
        owners = self.owners
        trailing = self.returns.trailing(simulation.config, day)
        self.risks[:] = 0
        self.risks[indices] = risk_factors(accounts["profile"][indices], day - accounts["open_day"][indices],
                                          tier_values[owners[indices]], trailing[indices])
        normalizer = self._risk_curve(simulation, day)
        self._select_customer_exits(simulation, day, indices, owners, normalizer)
        eligible = ~self.leaving_mask[indices] & ~self.customer_leaving[owners[indices]]
        residual = simulation.config.annual_closure - simulation.config.annual_inactivation
        rates = residual * self.risks[indices] / normalizer / 365.2425
        keys = accounts["account_id"][indices].astype(np.uint64) + np.uint64((day + 1) * 1_000_000_000)
        selected = eligible & (uniforms(keys, simulation.config.seed, "closure-hazard:0") < -np.expm1(-rates))
        for index in indices[selected]:
            self._start(simulation, int(index), self._period(simulation, int(accounts[index]["account_id"]), day, False))
        return indices[accounts["close_day"][indices] == day].astype(int).tolist()

    def _select_customer_exits(self, simulation, offset: int, indices: np.ndarray, owners: np.ndarray, normalizer: float) -> None:
        """Use mean current account risk so extra accounts do not multiply risk."""
        count = len(simulation.customers)
        totals = np.bincount(owners[indices], weights=self.risks[indices], minlength=count)
        counts = np.bincount(owners[indices], minlength=count)
        means = totals / np.maximum(1, counts)
        eligible = ((counts > 0) & (simulation.customers["open_day"] <= offset)
                    & ~self.customer_leaving)
        rates = simulation.config.annual_inactivation * means / normalizer / 365.2425
        keys = simulation.customers["customer_id"].astype(np.uint64) + np.uint64((offset + 1) * 1_000_000_000)
        selected = eligible & (uniforms(keys, simulation.config.seed, "inactivation-hazard:0") < -np.expm1(-rates))
        active_set = set(indices)
        for position in np.flatnonzero(selected):
            customer = simulation.customers[position]
            identifier = int(customer["customer_id"])
            period = self._period(simulation, identifier, offset, True)
            self.customer_plans[identifier] = period
            self.customer_leaving[position] = True
            customer["inactivation_day"] = min(period.close, simulation.total_days)
            for index in simulation.customer_accounts[identifier]:
                if index in active_set:
                    self._start(simulation, index, period)

    def start_day(self, simulation, day) -> None:
        """Reset today's external cash-flow totals before cash intents execute."""
        # Cancellations stay in the event queue. Releasing reservations before
        # their recorded timestamp would let an earlier withdrawal spend them.
        self.external_flow[:] = 0

    def allow_ordinary(self, account, day) -> bool:
        """Leave liquidation to the staged plan rather than ordinary trade draws."""
        return self.account_indices[int(account["account_id"])] not in self.leaving

    def allow_cash(self, simulation, index, day, name) -> bool:
        """Stop ordinary deposits during leaving; closure debt repayment is required."""
        return name != "Deposit" or index not in self.leaving

    def allow_trade(self, simulation, index, day, kind) -> bool:
        """Stop buys during leaving while allowing sales to reduce positions."""
        return index not in self.leaving or kind in ("TLS", "TMS")

    def intents(self, simulation, day) -> list[Intent]:
        """Plan staged sales and within-day withdrawals, including look-alikes."""
        result = []
        if not day.prices["trading"]:
            return result
        leaving_accounts = simulation.orders_by_account.keys() & self.leaving.keys()
        for order in simulation.orders_for_accounts(leaving_accounts):
            account = int(simulation.accounts[order.account_index]["account_id"])
            result.append(Intent(MARKET_OPEN_SECOND, account, order.trade_id, IntentKind.CANCEL,
                                 order.account_index, order=order))
        active = day.active_indices
        for index in active[self.leaving_mask[active]]:
            index = int(index)
            account = int(simulation.accounts[index]["account_id"])
            period = self.leaving[index]
            if day.offset in period.steps:
                remaining = sum(step >= day.offset for step in period.steps)
                result.extend(self._sale_intents(account, index, day.offset, 1 / remaining))
            elif simulation.ledger.balance[index] > 0:
                result.append(Intent(FINAL_WITHDRAWAL_SECOND, account, 40000, IntentKind.DRAIN, index, amount=10**12))
        raw_rate = simulation.config.annual_lookalike * simulation.config.lookalike_opportunity_multiplier
        rate = raw_rate / simulation.market.sessions_per_year[day.date.year]
        keys = simulation.accounts["account_id"][active].astype(np.uint64) + np.uint64((day.offset + 1) * 1_000_000_000)
        proposed = ~self.leaving_mask[active] & (uniforms(keys, simulation.config.seed, "lookalike-hazard:0") < -np.expm1(-rate))
        simulation.metrics["lookalike_proposals"] += int(np.count_nonzero(proposed))
        for index in active[proposed]:
            index = int(index)
            account = int(simulation.accounts[index]["account_id"])
            fraction = simulation.config.lookalike_sale_min + simulation.draw(account, day.offset, "lookalike-size") * (simulation.config.lookalike_sale_max - simulation.config.lookalike_sale_min)
            # An empty or one-share position cannot produce a 20-to-50 percent
            # sale. Count the proposal, but do not report a realized look-alike.
            viable = any(int(simulation.holdings.row(account, security)[3] * fraction) * int(day.prices["low"][security]) > simulation.config.charge_cents + 1
                         for security in simulation.holdings.by_account[account])
            if not viable:
                continue
            result.extend(self._sale_intents(account, index, day.offset, fraction, "outflow_candidate"))
        return result

    def record_outflow_candidate(self, simulation, index: int, offset: int) -> None:
        """Record an executed sale and withdrawal without changing future hazards."""
        self.records.append({"account_id": int(simulation.accounts[index]["account_id"]), "start_day": offset,
                             "close_day": None, "steps": [offset], "customer_exit": False,
                             "kind": "outflow_candidate", "classification": None, "final_plan": None,
                             "actual_close_day": None, "outcome_day": None})
        simulation.metrics["outflow_candidates"] += 1

    def _sale_intents(self, account: int, index: int, ordinal: int, fraction: float, name: str = "leaving") -> list[Intent]:
        """Keep the sale before its withdrawal on the same UTC date."""
        return [Intent(LIQUIDATION_SECOND, account, ordinal, IntentKind.LIQUIDATE, index, fraction=fraction, name=name),
                Intent(FINAL_WITHDRAWAL_SECOND, account, ordinal, IntentKind.DRAIN, index, amount=10**12, name=name)]

    def observe_cash(self, simulation, index: int, amount: int, name: str) -> None:
        """Track only external flows; settlements and dividends remain returns."""
        if name in ("Deposit", "Withdrawal"):
            self.external_flow[index] += amount

    def after_day(self, simulation, day) -> None:
        """Value positions at today's close and open deterministic replacement slots."""
        equity = simulation.ledger.balance.astype(np.float64).copy()
        if simulation.holdings.index:
            rows = simulation.holdings.data[np.fromiter(simulation.holdings.index.values(), dtype=np.int64)]
            indices = self.account_id_order[np.searchsorted(self.sorted_account_ids, rows[:, 0])]
            np.add.at(equity, indices, rows[:, 3] * day.prices["close"][rows[:, 1]])
        self.returns.observe(day.offset, equity, self.external_flow)
        for index in day.closes:
            period = self.leaving.get(index)
            simulation.metrics["warning_closures"] += bool(period and period.steps)
            account = simulation.accounts[index]
            customer = int(account["customer_id"])
            if customer in self.customer_plans or day.offset + 1 >= simulation.total_days:
                continue
            candidates = [i for i in simulation.customer_accounts[customer]
                          if int(simulation.accounts[i]["open_day"]) == simulation.total_days]
            if not candidates:
                raise ValueError("The checked dynamic replacement-account capacity is exhausted.")
            simulation.accounts[min(candidates)]["open_day"] = day.offset + 1

    def finish(self, simulation) -> None:
        """Classify outcomes for the summary after all business events are fixed."""
        counts = Counter()
        for ordinal, record in enumerate(self.records):
            index = self.account_indices[record["account_id"]]
            close = int(simulation.accounts[index]["close_day"])
            record["actual_close_day"] = close if close < simulation.total_days else None
            if record["kind"] == "outflow_candidate":
                self._classify_outflow(record, close, simulation.total_days)
            else:
                final = self.final_plan_records[index] == ordinal
                self._classify_plan(record, final, close, simulation.total_days)
            counts[record["classification"]] += 1
        for classification, metric in CLASSIFICATION_COUNTS.items():
            simulation.metrics[metric] = counts[classification]
        simulation.metrics["warning_closures"] = counts["closed_warning"]

    def _classify_outflow(self, record: dict, close: int, total_days: int) -> None:
        """Confirm a negative outcome only after a fully observed 30-day horizon."""
        maturity = record["start_day"] + ATTRITION_HORIZON_DAYS
        if close < total_days and close <= maturity:
            record.update(classification="closing", outcome_day=close)
        elif maturity >= total_days:
            record.update(classification="censored", outcome_day=total_days - 1)
        else:
            record.update(classification="confirmed_lookalike", outcome_day=maturity)

    def _classify_plan(self, record: dict, final: bool, close: int, total_days: int) -> None:
        """Separate superseded proposals from the final plan of an actual closure."""
        record["final_plan"] = final
        if not final:
            record["classification"] = "superseded"
        elif close >= total_days:
            record["classification"] = "unclosed"
        elif close != record["close_day"]:
            record["classification"] = "unmatched_close"
        else:
            record["classification"] = "closed_warning" if record["steps"] else "closed_no_warning"
            record["outcome_day"] = close

    def leaving_customers(self, simulation, day) -> set:
        """Cancel the configured watch share once when a warning starts."""
        return {int(simulation.accounts[index]["customer_id"]) for index in self.warning_accounts.get(day, ())}

    def watch_cancel_share(self, simulation, customer, day, default_share) -> float:
        """Use the configured share for leaving watches; inactivation cancels all."""
        return default_share
