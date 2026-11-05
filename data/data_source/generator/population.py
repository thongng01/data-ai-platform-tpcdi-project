"""Compact, repeatable customer and account lifetime planning."""

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import exchange_calendars as xcals
import numpy as np
import polars as pl

from .random_values import values


CUSTOMER_DTYPE = np.dtype([
    ("customer_id", "<i8"), ("open_day", "<i4"),
    ("inactivation_day", "<i4"), ("tier", "i1"),
])
ACCOUNT_DTYPE = np.dtype([
    ("account_id", "<i8"), ("customer_id", "<i8"),
    ("open_day", "<i4"), ("close_day", "<i4"), ("profile", "i1"),
    ("is_cash", "?"), ("wealth_cents", "<i8"),
    ("margin_limit_cents", "<i8"), ("tax_status", "i1"),
    ("broker_id", "<i4"),
])


def rounded(value):
    """Round a population count with decimal half-up semantics."""
    return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def uniform(identifiers, seed, field):
    """Use the upper 53 bits so that conversion cannot produce 1.0."""
    return (values(identifiers, seed, field) >> 11).astype(np.float64) / 2**53


@dataclass(frozen=True)
class Population:
    """Expose compact lifetime arrays and bounded contiguous customer groups."""
    customers: np.ndarray
    accounts: np.ndarray
    path: Path
    slot_stride: int

    def groups(self, size=2048):
        """Yield complete customer groups without splitting their account ownership."""
        for first in range(1, len(self.customers) + 1, size):
            last = min(first + size, len(self.customers) + 1)
            left, right = np.searchsorted(self.accounts["customer_id"], [first, last])
            yield self.customers[first - 1:last - 1], self.accounts[left:right]


def _session_days(config):
    """Return actual NYSE session offsets for static lifetime planning."""
    calendar = xcals.get_calendar("XNYS", start=config.start, end=config.end)
    return np.asarray([
        (session.date() - config.start).days for session in calendar.sessions
        if config.start <= session.date() <= config.end
    ], dtype=np.int32)


def _death_days(ids, opening, rate, seed, field, sessions, sentinel):
    """Draw continuous base-rate lifetimes and move closure to the next session."""
    if rate == 0 or not len(ids):
        return np.full(len(ids), sentinel, dtype=np.int32)
    # A continuous calendar-day hazard moves to the next session, not backwards.
    exposure = -np.log1p(-uniform(ids, seed, field)) * 365.25 / rate
    due = np.asarray(opening) + np.maximum(1, np.ceil(exposure).astype(np.int64))
    indices = np.searchsorted(sessions, due)
    padded = np.append(sessions, sentinel)
    return padded[np.minimum(indices, len(sessions))].astype(np.int32)


def _account_traits(config, ids):
    """Select repeatable hidden account traits using whole-column draws."""
    n = len(ids)
    rows = np.zeros(n, dtype=ACCOUNT_DTYPE)
    rows["account_id"] = ids
    profiles = np.searchsorted(np.cumsum(config.profile_shares), uniform(ids, config.seed, "profile"), side="right")
    rows["profile"] = profiles
    # Trading profiles have larger typical wealth, while all profiles retain variation.
    location = np.asarray([0.20, 0.40, 0.50, 0.65, 0.78])[profiles]
    draw = uniform(ids, config.seed, "wealth")
    fraction = np.clip(location + (draw - 0.5) * 0.55, 0, 1)
    wealth = config.wealth_min * np.exp(np.log(config.wealth_max / config.wealth_min) * fraction)
    rows["wealth_cents"] = np.floor(wealth * 100 + 0.5).astype(np.int64)
    # Normalize profile effects so the configured overall margin share stays meaningful.
    effects = np.asarray([0.5, 0.85, 0.6, 2.5, 3.0])
    low, high = 0.0, 2.0
    for _ in range(48):
        scale = (low + high) / 2
        share = np.dot(config.profile_shares, np.minimum(1, scale * effects))
        if share < config.margin_share:
            low = scale
        else:
            high = scale
    probabilities = np.minimum(1, high * effects)
    rows["is_cash"] = uniform(ids, config.seed, "account-type") >= probabilities[profiles]
    limits = config.margin_limit_min + uniform(ids, config.seed, "margin-limit") * (config.margin_limit_max - config.margin_limit_min)
    rows["margin_limit_cents"] = np.where(rows["is_cash"], 0, np.floor(rows["wealth_cents"] * limits + 0.5)).astype(np.int64)
    tax_codes = np.asarray([1, 0, 2], dtype=np.int8)
    rows["tax_status"] = tax_codes[np.searchsorted(np.cumsum(config.tax_shares)[:-1], uniform(ids, config.seed, "tax-status"), side="right")]
    rows["broker_id"] = values(ids, config.seed, "broker") % config.brokers + 1
    return rows


def assign_tiers(customers, accounts, start=None, shares=(.1, .3, .6)):
    """Rank monthly joining cohorts; initial-start customers form their own cohort."""
    totals = np.zeros(len(customers), dtype=np.int64)
    # Replacement accounts are not part of the wealth that a customer brings at joining.
    customer_index = accounts["customer_id"] - 1
    joining = accounts["open_day"] == customers["open_day"][customer_index]
    np.add.at(totals, customer_index[joining], accounts["wealth_cents"][joining])
    if start is None:
        cohorts = customers["open_day"]
    else:
        dates = np.datetime64(start) + customers["open_day"].astype("timedelta64[D]")
        cohorts = dates.astype("datetime64[M]").astype(np.int64)
        cohorts[customers["open_day"] == 0] = -1
    by_cohort = np.argsort(cohorts, stable=True)
    boundaries = np.flatnonzero(np.diff(cohorts[by_cohort])) + 1
    for indices in np.split(by_cohort, boundaries):
        order = np.lexsort((customers["customer_id"][indices], -totals[indices]))
        count = len(indices)
        top = rounded(Decimal(count) * Decimal(str(shares[0])))
        middle = rounded(Decimal(count) * (Decimal(str(shares[0])) + Decimal(str(shares[1]))))
        customers["tier"][indices[order]] = np.where(np.arange(count) < top, 1, np.where(np.arange(count) < middle, 2, 3))


def _dynamic_population(config, root, path, customers, sentinel):
    """Reserve checked account generations; runtime decides actual lifetimes."""
    counts = 1 + np.searchsorted(np.cumsum(config.account_shares), uniform(customers["customer_id"], config.seed, "account-count"), side="right")
    stride = 3 * config.account_generations
    if len(customers) * stride >= 10**9:
        raise ValueError("Dynamic account slots exceed the trade identifier capacity.")
    blocks = []
    for generation in range(config.account_generations):
        for slot in range(1, 4):
            indices = np.flatnonzero(counts >= slot)
            ids = indices * stride + generation * 3 + slot
            rows = _account_traits(config, ids)
            rows["customer_id"] = indices + 1
            rows["open_day"] = customers["open_day"][indices] if generation == 0 else sentinel
            rows["close_day"] = sentinel
            blocks.append(rows)
    accounts = np.concatenate(blocks)
    accounts.sort(order="account_id")
    assign_tiers(customers, accounts, config.start, config.tier_shares)
    # A global, deterministic expected-risk curve keeps normalization independent
    # of execution groups. It uses planned joining traits, not future returns.
    initial = accounts[accounts["open_day"] < sentinel]
    weights = np.asarray([2., 1., .5, 1.5, 1.5])[initial["profile"]]
    weights *= np.where(customers["tier"][initial["customer_id"] - 1] == 1, .7, 1.)
    weighted = np.zeros(sentinel + 367)
    count_curve = np.zeros_like(weighted)
    openings = initial["open_day"]
    np.add.at(weighted, openings, weights * 2)
    np.add.at(weighted, openings + 365, -weights)
    np.add.at(count_curve, openings, 1)
    normalizer = np.cumsum(weighted)[:sentinel] / np.maximum(1, np.cumsum(count_curve)[:sentinel])
    np.save(path / "risk_normalizer.npy", normalizer, allow_pickle=False)
    np.save(path / "customers.npy", customers, allow_pickle=False)
    np.save(path / "accounts.npy", accounts, allow_pickle=False)
    return Population(np.load(path / "customers.npy", mmap_mode="r"),
                      np.load(path / "accounts.npy", mmap_mode="r"), path, stride)


def build_population(config, root):
    """Plan base-rate closures. Generation stage 2 can replace this selection."""
    root = Path(root)
    path = root / "_work" / "population"
    path.mkdir(parents=True, exist_ok=True)
    sentinel = (config.end - config.start).days + 1
    cutover = (config.cutover - config.start).days
    sessions = _session_days(config)
    growth = (Decimal(1) + Decimal(str(config.annual_growth))).ln()
    targets = np.asarray([
        max(1, rounded(Decimal(config.cutover_customers) * (growth * Decimal(day - cutover) / Decimal("365.25")).exp()))
        for day in range(sentinel)
    ], dtype=np.int64)
    capacity = max(16, int(targets.max() * (1 + config.annual_inactivation * sentinel / 365.25)) + sentinel + 8)
    customers = np.empty(capacity, dtype=CUSTOMER_DTYPE)
    death_counts = np.zeros(sentinel + 1, dtype=np.int64)
    customer_count = active = 0
    for day, target in enumerate(targets):
        active -= death_counts[day]
        new = int(target - active)
        if new <= 0:
            continue
        ids = np.arange(customer_count + 1, customer_count + new + 1, dtype=np.int64)
        if customer_count + new > len(customers):
            customers.resize(max(len(customers) * 2, customer_count + new), refcheck=False)
        block = customers[customer_count:customer_count + new]
        block["customer_id"] = ids
        block["open_day"] = day
        block["inactivation_day"] = _death_days(ids, day, config.annual_inactivation, config.seed, "customer-inactivation", sessions, sentinel)
        block["tier"] = 3
        np.add.at(death_counts, block["inactivation_day"], 1)
        customer_count += new
        active += new
    customers = customers[:customer_count].copy()
    if config.attrition_enabled:
        # The first plan anchors expected joining counts. Runtime risks decide
        # actual inactivation dates, so realized cutover counts can fluctuate.
        customers["inactivation_day"] = sentinel
        return _dynamic_population(config, root, path, customers, sentinel)
    # Grow numeric blocks rather than retaining one Python object per planned account.
    account_capacity = max(16, customer_count * 4)
    accounts = np.empty(account_capacity, dtype=ACCOUNT_DTYPE)
    used = 0
    standalone_rate = max(0.0, config.annual_closure - config.annual_inactivation)
    initial_counts = 1 + np.searchsorted(np.cumsum(config.account_shares), uniform(customers["customer_id"], config.seed, "account-count"), side="right")
    next_slot = np.ones(customer_count, dtype=np.int64)

    def append(customer_indices, openings):
        """Append one static account generation without retaining Python row objects."""
        nonlocal accounts, used
        count = len(customer_indices)
        # Draw keys do not depend on the eventual published slot stride.
        ids = (customer_indices + 1) * 2**32 + next_slot[customer_indices]
        next_slot[customer_indices] += 1
        rows = _account_traits(config, ids)
        rows["customer_id"] = customer_indices + 1
        rows["open_day"] = openings
        rows["close_day"] = np.minimum(
            customers["inactivation_day"][customer_indices],
            _death_days(ids, openings, standalone_rate, config.seed, "account-closure", sessions, sentinel),
        )
        if used + count > len(accounts):
            accounts.resize(max(len(accounts) * 2, used + count), refcheck=False)
        accounts[used:used + count] = rows
        used += count
        return rows

    for slot in range(1, 4):
        indices = np.flatnonzero(initial_counts >= slot)
        pending = append(indices, customers["open_day"][indices])
        while len(pending):
            indices = pending["customer_id"] - 1
            # Do not replace an account after customer inactivation or at the run boundary.
            eligible = (pending["close_day"] + 1 < customers["inactivation_day"][indices]) & (pending["close_day"] + 1 < sentinel)
            if not np.any(eligible):
                break
            pending = append(indices[eligible], pending["close_day"][eligible] + 1)
    accounts = accounts[:used].copy()
    stride = int(next_slot.max() - 1)
    if customer_count * stride >= 10**9:
        raise ValueError("The population account-slot bound exceeds the trade identifier capacity.")
    slots = accounts["account_id"] % 2**32
    accounts["account_id"] = (accounts["customer_id"] - 1) * stride + slots
    accounts.sort(order="account_id")
    assign_tiers(customers, accounts, config.start, config.tier_shares)
    np.save(path / "customers.npy", customers, allow_pickle=False)
    np.save(path / "accounts.npy", accounts, allow_pickle=False)
    population = Population(np.load(path / "customers.npy", mmap_mode="r"), np.load(path / "accounts.npy", mmap_mode="r"), path, stride)
    hidden = root / "summary" / "hidden"
    hidden.mkdir(parents=True, exist_ok=True)
    for group, (_, block) in enumerate(population.groups()):
        frame = pl.DataFrame({name: block[name] for name in block.dtype.names})
        frame = frame.with_columns(
            (pl.lit(config.start) + pl.duration(days=pl.col("open_day"))).cast(pl.Date).alias("open_date"),
            pl.when(pl.col("close_day") < sentinel)
            .then(pl.lit(config.start) + pl.duration(days=pl.col("close_day")))
            .otherwise(None).cast(pl.Date).alias("close_date"),
        )
        frame.write_parquet(hidden / f"accounts-{group:06d}.parquet")
    return population
