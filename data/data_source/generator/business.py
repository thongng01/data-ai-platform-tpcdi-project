"""Named day records, array planning, and bounded reads for business execution."""

from dataclasses import dataclass, field
from datetime import date
from enum import IntEnum, StrEnum
from typing import Any

import numpy as np

from .random_values import uniforms

SECONDS_PER_HOUR = 3600
DIVIDEND_SECOND = 6 * SECONDS_PER_HOUR
FUNDING_SECOND = 8 * SECONDS_PER_HOUR
MARKET_OPEN_SECOND = 9 * SECONDS_PER_HOUR + 30 * 60
ORDER_DUE_SECOND = MARKET_OPEN_SECOND + 5 * 60
LIQUIDATION_SECOND = 10 * SECONDS_PER_HOUR
REPAYMENT_SECOND = 14 * SECONDS_PER_HOUR
FINAL_WITHDRAWAL_SECOND = 15 * SECONDS_PER_HOUR
WATCH_CANCEL_SECOND = 21 * SECONDS_PER_HOUR
WATCH_ADD_SECOND = 22 * SECONDS_PER_HOUR
CLOSE_SECOND = 23 * SECONDS_PER_HOUR
DAY_KEY_STRIDE = 1_000_000_000
ATTRITION_HORIZON_DAYS = 30


class Profile(IntEnum):
    """Give the five hidden behavior profile codes readable names."""

    DORMANT = 0
    LONG_TERM = 1
    RETIREE = 2
    ACTIVE_TRADER = 3
    DAY_TRADER = 4


class IntentKind(StrEnum):
    """Identify the ledger operation an intent asks the executor to apply."""

    CASH = "cash"
    WITHDRAW = "withdraw"
    SUBMIT = "submit"
    DUE = "due"
    CANCEL = "cancel"
    LIQUIDATE = "liquidate"
    FORCE_SELL = "force_sell"
    DRAIN = "drain"


@dataclass(frozen=True)
class Intent:
    """Carry one planned operation; planning alone never changes money or shares."""

    second: int
    account_id: int
    ordinal: int
    kind: IntentKind
    index: int
    amount: int = 0
    name: str = ""
    order: Any = None
    fraction: float = 1.0
    security: int = 0

    @property
    def sort_key(self) -> tuple:
        """Preserve chronological execution and stable equal-time ordering."""
        return self.second, self.account_id, self.ordinal, self.kind


@dataclass
class BusinessDay:
    """Share the day's calendar, lifetime selections, and planned operations."""

    date: date
    offset: int
    prices: dict
    joins: tuple[int, ...]
    opens: tuple[int, ...]
    closes: tuple[int, ...]
    inactive_customers: set[int]
    active_indices: np.ndarray
    intents: list[Intent] = field(default_factory=list)


@dataclass(frozen=True)
class DailyActivity:
    """Hold parallel arrays for the accounts with business proposals today."""

    indices: np.ndarray
    counts: np.ndarray
    deposits: np.ndarray
    withdrawals: np.ndarray
    updates: np.ndarray


def poisson_counts(rates: np.ndarray, draws: np.ndarray) -> np.ndarray:
    """Invert Poisson cumulative probabilities over all accounts at once."""
    probabilities = np.exp(-rates)
    cumulative = probabilities.copy()
    counts = np.zeros(len(rates), dtype=np.int64)
    pending = draws >= cumulative
    # Most accounts need zero or one iteration. Only nonzero tails keep expanding.
    while np.any(pending):
        counts[pending] += 1
        if np.max(counts) >= 10000:
            raise ValueError("The daily event expansion exceeds its checked bound.")
        probabilities[pending] *= rates[pending] / counts[pending]
        cumulative[pending] += probabilities[pending]
        pending = draws >= cumulative
    return counts


def plan_daily_activity(simulation, day: BusinessDay) -> DailyActivity:
    """Select trade counts and cash schedules with stable whole-array draws."""
    config = simulation.config
    indices = day.active_indices
    accounts = simulation.accounts[indices]
    ids = accounts["account_id"].astype(np.uint64)
    keys = ids + np.uint64((day.offset + 1) * DAY_KEY_STRIDE)
    profiles = accounts["profile"].astype(np.int64)
    regular = np.minimum(28, 25 + ids % 4) == day.date.day
    weekend = config.weekend_cash_multiplier if day.date.weekday() >= 5 else 1
    deposits = ((profiles == Profile.LONG_TERM) & regular) | (
        uniforms(keys, config.seed, "deposit-hazard:0") < np.asarray(config.annual_deposit_rates)[profiles] / 365.2425 * weekend)
    withdrawals = ((profiles == Profile.RETIREE) & regular) | (
        uniforms(keys, config.seed, "withdrawal-hazard:0") < np.asarray(config.annual_withdrawal_rates)[profiles] / 365.2425 * weekend)
    updates = uniforms(keys, config.seed, "account-update:0") < config.annual_account_update / 365.2425
    rates = np.zeros(len(indices))
    if day.prices["trading"]:
        rates = np.asarray(config.annual_trade_rates)[profiles] * simulation.activity_variation[indices] / simulation.market.sessions_per_year[day.date.year]
        if day.prices["shock"]:
            rates *= config.shock_trade_multiplier
    counts = poisson_counts(rates, uniforms(keys, config.seed, "daily-trading"))
    selected = (counts > 0) | deposits | withdrawals | updates
    return DailyActivity(indices[selected], counts[selected], deposits[selected], withdrawals[selected], updates[selected])


class MarketDayCache:
    """Copy at most 32 market days into ordinary arrays before scalar execution."""

    def __init__(self, market, block_days: int = 32):
        """Keep the market provider and a fixed-size, per-worker read cache."""
        self.market = market
        self.block_days = block_days
        self.first = -1
        self.arrays = {}

    def day(self, day: date) -> dict:
        """Return one day from a bounded block; preserve small fake providers."""
        if not hasattr(self.market, "_arrays"):
            return {name: np.asarray(value).copy() if isinstance(value, np.ndarray) else value
                    for name, value in self.market.day(day).items()}
        offset = (day - self.market.first).days
        if offset < 0 or offset >= len(self.market._arrays["trading"]):
            raise ValueError(f"Market date is outside this run: {day}")
        first = offset // self.block_days * self.block_days
        if first != self.first:
            self.arrays = {name: np.array(array[first:first + self.block_days], copy=True)
                           for name, array in self.market._arrays.items() if name != "annual_dividends"}
            for array in self.arrays.values():
                array.flags.writeable = False
            self.first = first
        index = offset - first
        return {name: bool(array[index]) if name in ("trading", "shock") else
                int(array[index]) if name == "close_second" else array[index]
                for name, array in self.arrays.items()}
