"""Tests for the characteristics inspector.

The inspector is what produces the rubric's "document data characteristics"
evidence, so its numbers have to be right, not merely present.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datagen.batches import generate  # noqa: E402
from datagen.config import GenConfig, ProblemRates  # noqa: E402
from datagen.inspect import (  # noqa: E402
    profile_file,
    profile_generated_file,
    profile_run,
)


@pytest.fixture()
def run(tmp_path):
    out = tmp_path / "run"
    generate(
        GenConfig(
            scale_factor=0.002,
            seed=42,
            batch_count=3,
            problems=ProblemRates(
                duplicate_rate_offline=0.10,
                duplicate_rate_streaming=0.05,
                trade_status_skew=0.90,
            ),
            output_root=str(out),
        )
    )
    return out


@pytest.fixture()
def big_run(tmp_path):
    """A larger run, for rate assertions.

    Duplicate *rates* need enough rows in the sample to mean anything. At scale
    0.002 the delta batches hold ~16 rows, where a 5% rate legitimately yields
    zero observed duplicates, so rate checks run at a scale where the sample can
    actually distinguish 0% from 5%.
    """
    out = tmp_path / "big"
    generate(
        GenConfig(
            scale_factor=0.05,
            seed=42,
            batch_count=3,
            incremental_ratio=0.05,
            problems=ProblemRates(
                duplicate_rate_offline=0.10,
                duplicate_rate_streaming=0.05,
                trade_status_skew=0.90,
            ),
            output_root=str(out),
        )
    )
    return out


def test_duplicate_rate_is_measured_not_asserted(big_run):
    """A configured 10% offline duplicate rate should measure near 10%."""
    profile = profile_generated_file(big_run, "Trade.txt")
    assert 0.08 <= profile.duplicate_rate <= 0.12
    assert profile.duplicates > 0


def test_streaming_duplicate_rate_is_separate_from_offline(big_run):
    offline = profile_generated_file(big_run, "Trade.txt").duplicate_rate
    streaming = profile_generated_file(big_run, "Trade.txt", "Batch2").duplicate_rate
    assert offline != streaming
    assert 0.03 <= streaming <= 0.07


def test_zero_rate_measures_zero(big_run):
    """The inverse check: off means exactly zero, not merely small."""
    from datagen.config import ProblemRates as PR
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        generate(
            GenConfig(
                scale_factor=0.05,
                seed=42,
                batch_count=1,
                problems=PR(duplicate_rate_offline=0.0, duplicate_rate_streaming=0.0),
                output_root=tmp,
            )
        )
        assert profile_generated_file(tmp, "Trade.txt").duplicates == 0


def test_skew_is_detected_on_the_status_column(run):
    """The dominant trade status should show up as a high top-share."""
    profile = profile_generated_file(run, "Trade.txt")
    by_name = {column.name: column for column in profile.columns}
    status = by_name["T_ST_ID"]
    assert status.top_share > 0.80
    assert status.distinct == 4


def test_cardinality_ratio_matches_rubric_definition(run):
    """Rubric defines cardinality as unique-values / total-values."""
    profile = profile_generated_file(run, "Trade.txt")
    by_name = {column.name: column for column in profile.columns}
    trade_id = by_name["T_ID"]
    assert trade_id.cardinality_ratio == pytest.approx(trade_id.distinct / trade_id.rows)


def test_field_counts_differ_across_batches(run):
    """Schema evolution is observable as a field-count change."""
    assert profile_generated_file(run, "Trade.txt").fields == 14
    assert profile_generated_file(run, "Trade.txt", "Batch2").fields == 16


def test_profile_run_reports_volume(run):
    report = profile_run(run)
    assert report["total_bytes"] > 0
    assert set(report["batches"]) == {"Batch1", "Batch2", "Batch3"}
    assert report["batches"]["Batch1"]["bytes"] > report["batches"]["Batch2"]["bytes"]


def test_clean_run_measures_no_duplicates(tmp_path):
    """With injection off the inspector must report zero, so the baseline is real."""
    out = tmp_path / "clean"
    generate(
        GenConfig(
            scale_factor=0.002,
            seed=42,
            batch_count=1,
            problems=ProblemRates(duplicate_rate_offline=0.0, duplicate_rate_streaming=0.0),
            output_root=str(out),
        )
    )
    assert profile_generated_file(out, "Trade.txt").duplicates == 0
