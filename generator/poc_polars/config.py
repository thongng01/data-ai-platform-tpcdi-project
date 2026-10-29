from dataclasses import asdict, dataclass, fields
from datetime import date, timedelta
from pathlib import Path

import yaml


class ConfigError(ValueError):
    pass


class UniqueKeyLoader(yaml.SafeLoader):
    pass


def _mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ConfigError(f"Duplicate configuration key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def read_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as stream:
        result = yaml.load(stream, Loader=UniqueKeyLoader)
    if not isinstance(result, dict):
        raise ConfigError("The YAML document must be a mapping.")
    return result


@dataclass(frozen=True)
class Config:
    seed: int = 20261029
    start_date: str = "2017-06-01"
    historical_days: int = 28
    incremental_days: int = 2
    customers: int = 200
    new_customers_per_day: int = 4
    securities: int = 24
    companies: int = 12
    brokers: int = 10
    cycles_per_account_day: int = 1
    customer_batch_size: int = 128
    cancel_basis_points: int = 1000
    cash_basis_points: int = 8000
    merge_fan_in: int = 16
    memory_limit_mb: int = 2048
    disk_limit_mb: int = 8192
    timeout_seconds: int = 900
    threads: int = 4

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name != "start_date" and type(value) is not int:
                raise ConfigError(f"{field.name} must be an integer.")
        if not isinstance(self.start_date, str):
            raise ConfigError("start_date must be a quoted ISO date.")
        try:
            parsed_date = date.fromisoformat(self.start_date)
            if parsed_date.isoformat() != self.start_date or not 2 <= parsed_date.year <= 9988:
                raise ValueError("The date lies outside the supported range.")
        except ValueError as error:
            raise ConfigError("start_date must be an ISO date.") from error
        for name in ("customers", "securities", "companies", "brokers", "cycles_per_account_day",
                     "customer_batch_size", "memory_limit_mb", "disk_limit_mb", "timeout_seconds", "threads"):
            if getattr(self, name) <= 0:
                raise ConfigError(f"{name} must be positive.")
        if not 6 <= self.historical_days <= 3650:
            raise ConfigError("historical_days must be between 6 and 3650.")
        if not 0 <= self.incremental_days <= 365 or self.new_customers_per_day < 0:
            raise ConfigError("Incremental counts must be nonnegative; days must not exceed 365.")
        if not 2 <= self.merge_fan_in <= 64:
            raise ConfigError("merge_fan_in must be between 2 and 64.")
        if not 0 <= self.seed < 2**63:
            raise ConfigError("seed must fit a nonnegative signed 64-bit integer.")
        if not all(0 <= value <= 10000 for value in (self.cancel_basis_points, self.cash_basis_points)):
            raise ConfigError("Probability basis points must be between 0 and 10000.")
        if self.companies > self.securities:
            raise ConfigError("There must be at least one security per company.")
        if self.total_customers * 3 >= 10**11:
            raise ConfigError("Account IDs exceed IDENT_T.")
        if self.total_days * self.total_customers * 3 * self.cycles_per_account_day * 2 >= 10**15:
            raise ConfigError("Trade IDs exceed TRADE_T.")
        if self.customer_batch_size * self.cycles_per_account_day > 200000:
            raise ConfigError("The account-group expansion exceeds the POC batch bound.")
        if self.securities >= 10**10 or self.brokers >= 10**11:
            raise ConfigError("Reference entity counts exceed the POC identifier limits.")

    @classmethod
    def load(cls, path: Path):
        values = read_yaml(path)
        unknown = set(values) - {field.name for field in fields(cls)}
        if unknown:
            raise ConfigError(f"Unknown configuration keys: {sorted(unknown)}")
        return cls(**values)

    @property
    def total_customers(self):
        return self.customers + self.incremental_days * self.new_customers_per_day

    @property
    def total_days(self):
        return self.historical_days + self.incremental_days

    def day(self, offset: int) -> date:
        return date.fromisoformat(self.start_date) + timedelta(days=offset)

    def batch(self, offset: int) -> int:
        return 1 if offset < self.historical_days else offset - self.historical_days + 2

    def as_dict(self):
        return asdict(self)
