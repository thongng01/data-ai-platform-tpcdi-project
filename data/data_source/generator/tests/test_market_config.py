"""Check strict run settings and shared market and reference contracts."""

from collections import defaultdict
from dataclasses import FrozenInstanceError, replace
from datetime import date, timedelta
import json
from pathlib import Path

from hypothesis import given, strategies as st
import numpy as np
import pytest

from data.data_source.generator.config import Config, ConfigError, calendar_for, rounded_count
from data.data_source.generator.formats import load_sources
from data.data_source.generator.market import Market, finwire_postings, prior_year, report_release
from data.data_source.generator.reference import generate_reference


class Capture:
    """Support Capture."""
    def __init__(self, config):
        """Check   init  ."""
        self.config = config
        self.sources = load_sources()
        self.rows = defaultdict(list)
        self.text_rows = defaultdict(list)
        self.max_batch = 0

    def emit(self, source, day, rows):
        """Check emit."""
        self.max_batch = max(self.max_batch, len(rows))
        self.rows[source, day].extend(rows)

    def text(self, path, rows):
        """Check text."""
        self.max_batch = max(self.max_batch, len(rows))
        self.text_rows[path].extend(rows)


def short_config(**kwargs):
    """Check short config."""
    return Config(initial_start="2018-12-20", initial_end="2018-12-31", daily_start="2019-01-01",
                  daily_end="2019-04-30", scale_factor=.01, companies=2, securities=5, **kwargs)


def test_defaults_and_frozen_config():
    """Check defaults and frozen config."""
    config = Config()
    assert (config.start, config.cutover, config.daily_start_date, config.end) == (
        date(2014, 1, 1), date(2018, 12, 31), date(2019, 1, 1), date(2025, 12, 31))
    assert config.cutover_customers == 3300
    assert config.brokers == 6
    with pytest.raises(FrozenInstanceError):
        config.seed = 2
    assert Config(**config.as_dict()) == config


@given(st.integers(min_value=1, max_value=100000))
def test_decimal_population_rounding(half):
    """Check decimal population rounding."""
    assert rounded_count(f"{half}.5") == half + 1
    assert rounded_count("0.15", 10) == 2


@pytest.mark.parametrize("settings", [
    {"scale_factor": True}, {"scale_factor": float("inf")}, {"scale_factor": float("nan")},
    {"scale_factor": .00001}, {"workers": 3}, {"workers": 0}, {"seed": -1}, {"seed": 2**63},
    {"annual_growth": True}, {"account_shares": [.5, .5, True]}, {"profile_shares": [.5] * 5},
    {"annual_trade_rates": [1, 2, 3, 4, float("nan")]}, {"annual_trade_rates": [1, 2, 3, 4, 1e9]},
    {"margin_share": 1.1}, {"margin_limit_min": .8}, {"wealth_min": 3000000},
    {"annual_inactivation": .04}, {"companies": 2001}, {"initial_start": date(2014, 1, 1)},
    {"daily_start": "2019-01-02"}, {"initial_start": "2019-01-01"}, {"daily_end": "2018-12-31"},
    {"initial_start": "0001-01-01"}, {"price_max_cents": 10**8}, {"scale_factor": 1e8},
    {"initial_start": "1899-01-01"}, {"daily_end": "2101-01-01"}, {"scale_factor": 1e308},
    {"order_shares": [.5, .5]}, {"tier_shares": [.1, .2, .3]}, {"tax_shares": [.5, .5, -1]},
    {"annual_deposit_rates": [.2, 1, .2, 8, True]}, {"watch_lifetime_min_days": 200},
    {"opening_funding_min": .9}, {"limit_cancel_share": .99}, {"activity_variation_max": .1},
    {"activity_variation_power": 0}, {"disk_limit_mb": 1},
    {"positions_per_account": -1}, {"positions_per_account": True}, {"positions_per_account": 65},
    {"attrition_enabled": 1}, {"attrition_warning_share": 1.01}, {"annual_lookalike": True},
    {"attrition_warning_min_weeks": 9}, {"attrition_liquidation_max_steps": 5},
    {"lookalike_sale_min": .1}, {"lookalike_sale_max": .6}, {"account_generations": 1},
    {"customer_tier_error_rate": 1.01}, {"trade_charge_error_rate": True},
    {"trade_channel_start": date(2019, 7, 1)}, {"trade_channel_start": "2019-02-30"},

])
def test_invalid_config(settings):
    """Check invalid config."""
    with pytest.raises(ConfigError):
        Config(**settings)


@pytest.mark.parametrize("content", ["seed: 1\nseed: 2\n", "unknown: 1\n", "[]\n", "1: 2\n", "seed: [\n"])
def test_strict_yaml(tmp_path, content):
    """Check strict yaml."""
    path = tmp_path / "config.yaml"
    path.write_text(content)
    with pytest.raises(ConfigError):
        Config.load(path)


def test_all_examples_load():
    """Check all examples load."""
    for path in (Path(__file__).parents[1] / "configs").glob("*.yaml"):
        Config.load(path)


def test_reference_sessions_and_special_closures():
    """Check reference sessions and special closures."""
    exchange = calendar_for(date(2013, 1, 1), date(2026, 1, 7))
    sessions = {stamp.date() for stamp in exchange.sessions}
    assert sum(date(2014, 1, 1) <= day <= date(2018, 12, 31) for day in sessions) == 1258
    assert sum(date(2019, 1, 1) <= day <= date(2025, 12, 31) for day in sessions) == 1760
    assert date(2018, 12, 5) not in sessions
    assert date(2025, 1, 9) not in sessions
    assert date(2022, 6, 20) not in sessions
    assert date(2021, 6, 18) in sessions
    assert date(2018, 12, 24) in {stamp.date() for stamp in exchange.early_closes}


def test_earnings_support_and_release_cadence():
    """Check earnings support and release cadence."""
    config = Config()
    postings = list(finwire_postings(config))
    assert postings[0] == (date(2012, 10, 1), None)
    assert sum(quarter is not None and posting <= config.start for posting, quarter in postings) == 4
    assert report_release(date(2018, 10, 1)) == date(2019, 1, 31)
    assert report_release(date(2019, 1, 1)) == date(2019, 4, 30)
    assert report_release(date(2019, 4, 1)) == date(2019, 7, 31)
    assert report_release(date(2019, 7, 1)) == date(2019, 10, 31)
    assert all(posting <= config.end for posting, _ in postings)
    assert prior_year(date(2020, 2, 29)) == date(2019, 2, 28)


def test_market_mmap_prices_releases_and_dividends(tmp_path):
    """Check market mmap prices releases and dividends."""
    config = short_config(dividend_share=1, export_rows=3)
    capture = Capture(config)
    market = Market.build(config, tmp_path, capture)
    assert market.first == date(2017, 12, 20)
    assert market.day(date(2018, 12, 24))["close_second"] == 13 * 3600
    assert not market.day(date(2018, 12, 25))["trading"]
    assert market.day(date(2018, 12, 26))["close_second"] == 16 * 3600
    assert isinstance(market.day(config.start)["close"], np.memmap)
    assert not market.day(config.start)["close"].flags.writeable
    assert capture.max_batch <= 3
    assert ("DailyMarket", date(2017, 12, 20)) in capture.rows
    assert ("DailyMarket", date(2018, 12, 25)) not in capture.rows
    assert "offline/daily/20190131/FINWIRE2019Q1" in capture.text_rows
    assert "offline/daily/20190430/FINWIRE2019Q2" in capture.text_rows
    for path, rows in capture.text_rows.items():
        for _, payload in rows:
            assert len(payload) == {"CMP": 543, "SEC": 170, "FIN": 196}[payload[15:18]]
            posting = date(int(payload[:4]), int(payload[4:6]), int(payload[6:8]))
            assert path.endswith(f"FINWIRE{posting.year}Q{(posting.month - 1) // 3 + 1}")
            if "/daily/" in path:
                assert path.split("/")[2] == posting.strftime("%Y%m%d")
    for day in (config.start, date(2019, 1, 2), config.end):
        data = market.day(day)
        assert np.all(data["low"] > 0)
        assert np.all(data["low"] <= data["close"])
        assert np.all(data["close"] <= data["high"])
    assert market.day(date(2019, 1, 1))["dividends"].sum() == 0
    assert market.day(date(2019, 1, 2))["dividends"][0] == market.annual_dividends(date(2019, 1, 2))[0] // 4
    from data.data_source.generator.business import MarketDayCache
    cache = MarketDayCache(market)
    for day in (config.start, config.start + timedelta(days=40), config.end):
        cached = cache.day(day)
        assert type(cached["close"]) is np.ndarray
        assert not cached["close"].flags.writeable
        np.testing.assert_array_equal(cached["close"], market.day(day)["close"])
        assert cache.arrays["close"].shape[0] <= 32
    assert market.fingerprint == Market(tmp_path, config).fingerprint
    assert market.sessions_per_year[2019] == 252
    with pytest.raises(ValueError):
        market.day(config.end + timedelta(days=1))


def test_market_ignores_firm_size_and_execution_settings(tmp_path):
    """Check market ignores firm size and execution settings."""
    first = short_config(export_rows=3)
    second = replace(first, scale_factor=5, export_rows=64, customer_group_size=2, workers=2, threads=1, merge_fan_in=2)
    captures = [Capture(first), Capture(second)]
    markets = [Market.build(config, tmp_path / str(index), capture)
               for index, (config, capture) in enumerate(zip((first, second), captures))]
    assert captures[0].rows == captures[1].rows
    for path in captures[0].text_rows:
        assert sorted(captures[0].text_rows[path]) == sorted(captures[1].text_rows[path])
    for name in markets[0]._arrays:
        np.testing.assert_array_equal(markets[0]._arrays[name], markets[1]._arrays[name])


def test_reference_has_static_hr_and_all_seconds():
    """Check reference has static hr and all seconds."""
    config = short_config(export_rows=37)
    capture = Capture(config)
    generate_reference(capture)
    assert len(capture.rows["Time", config.start]) == 86400
    employees = capture.rows["HR", config.start]
    assert len(employees) == config.brokers * 4
    assert sum(row["EmployeeJobCode"] == 314 for row in employees) == config.brokers
    assert capture.max_batch <= 37
    assert not capture.text_rows
    for (source, _), rows in capture.rows.items():
        for row in rows:
            for field in capture.sources[source].fields:
                field.check(str(row[field.name]))
