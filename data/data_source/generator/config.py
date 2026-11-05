"""Strict run settings for brokerage behavior, calendar, and resource bounds."""

from dataclasses import asdict, dataclass, fields
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
import math
from pathlib import Path

import exchange_calendars as xcals
import yaml


class ConfigError(ValueError):
    """Report invalid run settings before generation writes business output."""
    pass


class UniqueKeyLoader(yaml.SafeLoader):
    """Reject duplicate YAML keys instead of silently overwriting run settings."""
    pass


def _mapping(loader, node, deep=False):
    """Construct a strict string-keyed YAML mapping with unique keys."""
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise ConfigError("YAML mapping keys must be strings.")
        if key in result:
            raise ConfigError(f"Duplicate configuration key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def read_yaml(path: Path) -> dict:
    """Read one YAML settings mapping and report clear parse errors."""
    try:
        with Path(path).open(encoding="utf-8") as stream:
            result = yaml.load(stream, Loader=UniqueKeyLoader)
    except yaml.YAMLError as error:
        raise ConfigError(f"Invalid YAML: {error}") from error
    if not isinstance(result, dict):
        raise ConfigError("The YAML document must be a mapping.")
    return result


def rounded_count(value, multiplier=1) -> int:
    """Round population counts in decimal, with half values rounded up."""
    return int((Decimal(str(value)) * Decimal(str(multiplier))).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


@lru_cache(maxsize=16)
def calendar_for(start: date, end: date):
    # These limits bound the project's historical model, including prior prices.
    """Load and cache the supported NYSE calendar range."""
    if start.year < 1900 or end.year > 2100:
        raise ConfigError("The supported market calendar is 1900 through 2100.")
    try:
        return xcals.get_calendar("XNYS", start=start.isoformat(), end=end.isoformat())
    except (ValueError, OverflowError) as error:
        raise ConfigError(f"Dates are outside the supported XNYS calendar: {error}") from error


@dataclass(frozen=True)
class Config:
    """Hold validated semantic settings and bounded execution settings."""
    initial_start: str = "2014-01-01"
    initial_end: str = "2018-12-31"
    daily_start: str = "2019-01-01"
    daily_end: str = "2025-12-31"
    scale_factor: float = 1.0
    customers_per_unit: int = 3601
    seed: int = 20261029
    account_shares: tuple[float, ...] = (.5, .33, .17)
    profile_shares: tuple[float, ...] = (.3, .45, .15, .09, .01)
    order_shares: tuple[float, ...] = (.2, .3, .3, .2)
    tier_shares: tuple[float, ...] = (.1, .3, .6)
    tax_shares: tuple[float, ...] = (.7, .1, .2)
    activity_variation_min: float = .3
    activity_variation_max: float = 3.1
    activity_variation_power: float = 3
    annual_trade_rates: tuple[float, ...] = (4, 24, 12, 150, 750)
    annual_deposit_rates: tuple[float, ...] = (.2, 1, .2, 8, 36)
    annual_withdrawal_rates: tuple[float, ...] = (.1, .8, .2, 4, 18)
    weekend_cash_multiplier: float = .4
    watch_lifetime_min_days: int = 7
    watch_lifetime_max_days: int = 187
    opening_funding_min: float = .5
    opening_funding_max: float = .8
    trade_wealth_min: float = .002
    trade_wealth_max: float = .02
    cash_wealth_min: float = .002
    cash_wealth_max: float = .01
    tax_season_multiplier: float = 1.5
    order_lifetime_max_days: int = 5
    limit_cancel_share: float = .058 / .6
    limit_open_share: float = .015 / .6
    security_top_share: float = .30
    # A four-security investment universe bounds quarterly dividend events while
    # retaining the market's paying share and yield. Zero retains the unbounded
    # stage 1 policy for controlled before-and-after evidence.
    positions_per_account: int = 4
    annual_growth: float = .08
    attrition_enabled: bool = True
    attrition_warning_share: float = .8
    attrition_warning_min_weeks: int = 1
    attrition_warning_max_weeks: int = 8
    attrition_liquidation_min_steps: int = 1
    attrition_liquidation_max_steps: int = 4
    annual_lookalike: float = .02
    # Account for candidates that cannot execute both a sale and a withdrawal.
    # This multiplier adjusts opportunity frequency, not the target annual rate,
    # and gives no protection from either closure hazard.
    lookalike_opportunity_multiplier: float = 2.12
    lookalike_sale_min: float = .2
    lookalike_sale_max: float = .5
    account_generations: int = 8
    annual_closure: float = .03
    annual_inactivation: float = .02
    annual_customer_update: float = .05
    annual_account_update: float = .05
    margin_share: float = .2
    wealth_min: int = 1000
    wealth_max: int = 2000000
    margin_limit_min: float = .1
    margin_limit_max: float = .5
    watch_daily_rate: float = .1
    account_watch_cancel_share: float = .5
    prospects_per_account: float = 2
    prospect_overlap: float = .3
    accounts_per_broker: int = 1000
    charge_cents: int = 100
    commission_basis_points: int = 10
    securities: int = 2000
    companies: int = 1250
    shock_days_per_year: int = 4
    shock_trade_multiplier: float = 3
    dividend_share: float = .4
    dividend_yield_min: float = .01
    dividend_yield_max: float = .04
    price_min_cents: int = 1000
    price_max_cents: int = 30000
    market_volatility: float = .015
    customer_group_size: int = 2048
    workers: int = 1
    threads: int = 2
    export_rows: int = 8192
    merge_fan_in: int = 16
    memory_limit_mb: int = 2048
    disk_limit_mb: int = 65536
    timeout_seconds: int = 43200
    customer_tier_error_rate: float = .002
    customer_dob_error_rate: float = .001
    company_rating_error_rate: float = .01
    trade_charge_error_rate: float = .001
    trade_commission_error_rate: float = .001
    trade_channel_start: str = "2019-07-01"

    def __post_init__(self):
        """Reject invalid types, probabilities, periods, and capacity projections."""
        dates = ("initial_start", "initial_end", "daily_start", "daily_end", "trade_channel_start")
        vectors = {"account_shares": 3, "profile_shares": 5, "order_shares": 4, "tier_shares": 3,
                   "tax_shares": 3, "annual_trade_rates": 5, "annual_deposit_rates": 5, "annual_withdrawal_rates": 5}
        integers = {
            "customers_per_unit", "seed", "wealth_min", "wealth_max", "accounts_per_broker",
            "charge_cents", "commission_basis_points", "securities", "companies", "shock_days_per_year",
            "price_min_cents", "price_max_cents", "customer_group_size", "workers", "threads",
            "export_rows", "merge_fan_in", "memory_limit_mb", "disk_limit_mb", "timeout_seconds",
            "watch_lifetime_min_days", "watch_lifetime_max_days", "order_lifetime_max_days", "positions_per_account",
            "attrition_warning_min_weeks", "attrition_warning_max_weeks", "attrition_liquidation_min_steps",
            "attrition_liquidation_max_steps", "account_generations",
        }
        for field in fields(self):
            name, value = field.name, getattr(self, field.name)
            if name == "attrition_enabled":
                if type(value) is not bool:
                    raise ConfigError("attrition_enabled must be a boolean.")
            elif name in dates:
                try:
                    if type(value) is not str or date.fromisoformat(value).isoformat() != value:
                        raise ValueError
                except (ValueError, TypeError):
                    raise ConfigError(f"{name} must be a quoted ISO date.") from None
            elif name in vectors:
                if not isinstance(value, (tuple, list)) or len(value) != vectors[name]:
                    raise ConfigError(f"{name} must contain {vectors[name]} numbers.")
                if any(type(number) not in (int, float) or not math.isfinite(number) or number < 0 for number in value):
                    raise ConfigError(f"{name} must contain finite nonnegative numbers.")
                object.__setattr__(self, name, tuple(value))
                if name.endswith("shares") and abs(sum(Decimal(str(number)) for number in value) - 1) > Decimal("1e-12"):
                    raise ConfigError(f"{name} must sum to one.")
            elif name in integers:
                if type(value) is not int:
                    raise ConfigError(f"{name} must be an integer.")
                if value < 0 or (value == 0 and name not in {"seed", "charge_cents", "commission_basis_points", "shock_days_per_year", "positions_per_account"}):
                    raise ConfigError(f"{name} must be positive.")
            elif type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ConfigError(f"{name} must be a finite nonnegative number.")
        if self.start.year < 1902 or self.end.year > 2100:
            raise ConfigError("Run dates must be within 1902 through 2100, including the required market support.")
        if not (self.start <= self.cutover and self.daily_start_date <= self.end):
            raise ConfigError("Each run period must contain at least one date.")
        if self.daily_start_date != self.cutover + timedelta(days=1):
            raise ConfigError("daily_start must be the day after initial_end.")
        if self.scale_factor > 10**9 or self.customers_per_unit >= 10**9:
            raise ConfigError("Configured customer counts exceed identifier capacity.")
        if self.scale_factor <= 0 or self.cutover_customers < 1:
            raise ConfigError("scale_factor must produce at least one cutover customer.")
        probabilities = (
            "annual_closure", "annual_inactivation", "annual_customer_update", "annual_account_update",
            "margin_share", "margin_limit_min", "margin_limit_max", "account_watch_cancel_share",
            "prospect_overlap", "dividend_share", "dividend_yield_min", "dividend_yield_max",
            "weekend_cash_multiplier", "opening_funding_min", "opening_funding_max",
            "trade_wealth_min", "trade_wealth_max", "cash_wealth_min", "cash_wealth_max",
            "limit_cancel_share", "limit_open_share", "security_top_share",
            "customer_tier_error_rate", "customer_dob_error_rate", "company_rating_error_rate",
            "trade_charge_error_rate", "trade_commission_error_rate",
            "attrition_warning_share", "annual_lookalike", "lookalike_sale_min", "lookalike_sale_max",
        )
        for name in probabilities:
            if getattr(self, name) > 1:
                raise ConfigError(f"{name} must be between zero and one.")
        if self.annual_growth > 1 or self.annual_inactivation > self.annual_closure:
            raise ConfigError("Growth must not exceed one; inactivation must be within total closures.")
        if self.wealth_min > self.wealth_max or self.margin_limit_min > self.margin_limit_max:
            raise ConfigError("Wealth and margin distribution bounds must be in order.")
        for prefix in ("opening_funding", "trade_wealth", "cash_wealth"):
            if getattr(self, f"{prefix}_min") > getattr(self, f"{prefix}_max"):
                raise ConfigError(f"{prefix} distribution bounds must be in order.")
        if self.activity_variation_min > self.activity_variation_max or self.activity_variation_max > 16 or self.activity_variation_power <= 0:
            raise ConfigError("Activity variation requires ordered bounds through 16 and a positive power.")
        if self.watch_lifetime_min_days > self.watch_lifetime_max_days:
            raise ConfigError("Watch lifetime bounds must be in order.")
        if self.limit_cancel_share + self.limit_open_share > 1:
            raise ConfigError("Limit cancellation and open shares must not exceed one together.")
        if self.price_min_cents > self.price_max_cents or self.dividend_yield_min > self.dividend_yield_max:
            raise ConfigError("Price and dividend distribution bounds must be in order.")
        if not 1 <= self.attrition_warning_min_weeks <= self.attrition_warning_max_weeks <= 8:
            raise ConfigError("Attrition warning periods must be ordered within 1 through 8 weeks.")
        if not 1 <= self.attrition_liquidation_min_steps <= self.attrition_liquidation_max_steps <= 4:
            raise ConfigError("Attrition liquidation steps must be ordered within 1 through 4.")
        if self.lookalike_opportunity_multiplier <= 0:
            raise ConfigError("lookalike_opportunity_multiplier must be positive and finite.")
        if not .2 <= self.lookalike_sale_min <= self.lookalike_sale_max <= .5:
            raise ConfigError("Look-alike sale fractions must be ordered within .2 through .5.")
        if not 2 <= self.account_generations <= 32:
            raise ConfigError("account_generations must be between 2 and 32.")
        if self.positions_per_account > 64:
            raise ConfigError("positions_per_account must be zero or between 1 and 64.")
        if self.companies > self.securities:
            raise ConfigError("There must be at least one security per company.")
        if not 1 <= self.workers <= 2 or not 2 <= self.merge_fan_in <= 64:
            raise ConfigError("workers must be 1 or 2; merge_fan_in must be between 2 and 64.")
        if not 0 <= self.seed < 2**63:
            raise ConfigError("seed must fit a nonnegative signed 64-bit integer.")
        if self.prospects_per_account <= 0 or rounded_count(self.cutover_customers * 1.67, self.prospects_per_account) < 1:
            raise ConfigError("The firm must have at least one prospect.")
        if self.price_max_cents >= 10**8 or self.wealth_max * 100 >= 10**10 or self.charge_cents >= 10**10:
            raise ConfigError("Configured money values exceed source widths.")
        if self.commission_basis_points > 10000 or self.market_volatility > .5 or self.shock_days_per_year > 100 or self.shock_trade_multiplier < 1:
            raise ConfigError("Fee, market volatility, or shock settings exceed supported bounds.")
        if self.securities >= 10**10 or self.companies >= 10**10 or self.brokers * 4 >= 10**11:
            raise ConfigError("Market or HR identifiers exceed source widths.")
        years = self.total_days / 365.2425
        exposure = (self.annual_growth + self.annual_closure) * years
        if exposure > 40:
            raise ConfigError("Population growth exceeds identifier capacity.")
        upper_accounts = math.ceil(self.cutover_customers * 3 * math.exp(exposure))
        if self.attrition_enabled:
            upper_accounts *= self.account_generations
        if upper_accounts >= 10**9:
            raise ConfigError("Composite trade identifiers exceed TRADE_T.")
        expansion = max(self.annual_trade_rates) * (years + 1) * self.shock_trade_multiplier * 16
        if expansion >= 1000000:
            raise ConfigError("Configured trade expansion exceeds the per-account identifier stride.")
        if self.customer_group_size * 3 * max(1, max(self.annual_trade_rates) / 250 * self.shock_trade_multiplier) > 10000000:
            raise ConfigError("Customer group event expansion exceeds the bounded worker plan.")
        prior = self.start.replace(year=self.start.year - 1, day=min(self.start.day, 28))
        fixed_market_bytes = ((self.end - prior).days + 1) * self.securities * 32
        if fixed_market_bytes > self.disk_limit_mb * 1024**2:
            raise ConfigError("Shared market arrays exceed the configured disk budget.")
        calendar_for(prior, self.end)

    @classmethod
    def load(cls, path: Path):
        """Read a strict configuration file and reject unknown setting names."""
        values = read_yaml(path)
        unknown = set(values) - {field.name for field in fields(cls)}
        if unknown:
            raise ConfigError(f"Unknown configuration keys: {sorted(unknown)}")
        return cls(**values)

    @property
    def start(self) -> date:
        """Return the inclusive first simulation date."""
        return date.fromisoformat(self.initial_start)

    @property
    def end(self) -> date:
        """Return the inclusive last simulation date."""
        return date.fromisoformat(self.daily_end)

    @property
    def cutover(self) -> date:
        """Return the final initial-load date."""
        return date.fromisoformat(self.initial_end)

    @property
    def daily_start_date(self) -> date:
        """Return the first daily-delivery date."""
        return date.fromisoformat(self.daily_start)

    @property
    def cutover_customers(self) -> int:
        """Convert the configured factor to its customer-count anchor."""
        return rounded_count(self.scale_factor, self.customers_per_unit)

    @property
    def brokers(self) -> int:
        """Derive broker staffing from firm size and accounts per broker."""
        return max(1, math.ceil(self.cutover_customers * 1.67 / self.accounts_per_broker))

    @property
    def total_days(self) -> int:
        """Count every inclusive simulation date, including closed-market days."""
        return (self.end - self.start).days + 1

    def day(self, offset: int) -> date:
        """Translate a simulation-day offset to its UTC calendar date."""
        return self.start + timedelta(days=offset)

    def as_dict(self) -> dict:
        """Serialize all settings without changing their semantic values."""
        return asdict(self)
