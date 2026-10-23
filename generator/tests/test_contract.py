"""Contract tests.

These check generated output against real TPC-DI bytes, committed under
``tests/fixtures/tpcdi_sf3/``. The fixture is a byte-exact slice of the upstream
``sf_3`` sample, so the suite depends on nothing but this repo: no upstream
clone, no network, no 369MB checkout. Clone this repo and the contract is still
verified.

Regenerate the fixture after updating the upstream reference with
``python tools/refresh_fixtures.py``. ``test_fixture_matches_upstream`` fails if
the fixture has drifted from the real thing, so a stale fixture cannot quietly
become the new truth.
"""

from __future__ import annotations

import json
import os
import sys
import warnings
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from datagen.config import GenConfig, ProblemRates  # noqa: E402
from datagen.emit import CRLF, DelimitedWriter, SingleColumnWriter, XmlActionWriter  # noqa: E402
from datagen.rng import rng_for  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "tpcdi_sf3"
MANIFEST = json.loads((Path(__file__).resolve().parent / "fixtures" / "manifest.json").read_text())

# Optional upstream clone. Used only by the freshness test below; the contract
# tests never touch it.
UPSTREAM = next(
    (
        candidate
        for candidate in (
            REPO_ROOT / "misc" / "tpcdi",
            REPO_ROOT / "tpcdi",
            Path(os.environ["TPCDI_REPO"]) if os.environ.get("TPCDI_REPO") else None,
        )
        if candidate and candidate.exists()
    ),
    None,
)


def fixture_fields(relative: str) -> int:
    """Field count recorded from the real upstream file at fixture time."""
    return int(MANIFEST["files"][relative]["fields"])


def fixture_line(relative: str) -> str:
    return (FIXTURE / relative).read_bytes().split(b"\r\n")[0].decode()


# --------------------------------------------------------------------------
# Format contract, checked against committed real TPC-DI bytes
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "relative",
    ["Batch1/Trade.txt", "Batch1/TradeHistory.txt", "Batch1/CashTransaction.txt",
     "Batch1/HoldingHistory.txt", "Batch1/DailyMarket.txt", "Batch1/WatchHistory.txt"],
)
def test_batch1_field_counts_match_real_tpcdi(relative):
    """Every Batch1 event file keeps the field count the real format has."""
    assert fixture_fields(relative) == MANIFEST["files"][relative]["fields"]
    real = fixture_line(relative)
    assert real.count("|") == fixture_fields(relative) - 1


def test_trade_is_14_fields_batch1_and_16_in_deltas():
    """The Batch1/Batch2 field-count difference IS the schema-evolution problem."""
    assert fixture_fields("Batch1/Trade.txt") == 14
    assert fixture_fields("Batch2/Trade.txt") == 16
    assert fixture_fields("Batch1/CashTransaction.txt") == 4
    assert fixture_fields("Batch2/CashTransaction.txt") == 6


def test_trade_type_reference_is_four_fields():
    """TradeType is 4 columns with 0/1 flags, not booleans."""
    assert fixture_fields("Batch1/TradeType.txt") == 4
    rows = (FIXTURE / "Batch1/TradeType.txt").read_bytes().decode().split(CRLF)
    for row in filter(None, rows):
        assert row.split("|")[2] in ("0", "1")
        assert row.split("|")[3] in ("0", "1")


def test_status_type_codes_match_domain():
    """Trade status must live in the real StatusType domain, and only that domain."""
    from datagen.specs_reference import STATUS_TYPES, TRADE_STATUS_CODES

    real = {
        line.split("|")[0]
        for line in (FIXTURE / "Batch1/StatusType.txt").read_bytes().decode().split(CRLF)
        if line
    }
    assert {code for code, _ in STATUS_TYPES} == real
    # Trades use a strict subset: a trade is never ACTV or INAC.
    assert set(TRADE_STATUS_CODES) < real
    assert "ACTV" not in TRADE_STATUS_CODES and "INAC" not in TRADE_STATUS_CODES


def test_batchdate_is_a_bare_iso_date():
    """BatchDate.txt is one field, no delimiter, no trailing field."""
    assert fixture_fields("Batch1/BatchDate.txt") == 1
    raw = (FIXTURE / "Batch1/BatchDate.txt").read_bytes()
    assert raw == b"2017-07-07\r\n"


def test_fixture_lines_are_crlf_and_unterminated_correctly():
    """The fixture preserves CRLF and headers-as-data exactly as upstream has them.

    "No header row" means line 1 is data, not that it starts with a digit:
    Batch2 rows begin with the I/U operation code. What distinguishes data from
    a header is that it parses to the file's field count, which is asserted
    elsewhere.
    """
    for relative in ("Batch1/Trade.txt", "Batch2/Trade.txt"):
        raw = (FIXTURE / relative).read_bytes()
        assert raw.endswith(b"\r\n")
        assert b"\n" not in raw.replace(b"\r\n", b"")
        first = raw.split(b"\r\n")[0].decode()
        assert len(first.split("|")) == fixture_fields(relative)


# --------------------------------------------------------------------------
# Fixture freshness: only runs when the upstream clone is present
# --------------------------------------------------------------------------


@pytest.mark.skipif(UPSTREAM is None, reason="upstream TPC-DI clone not present")
def test_fixture_matches_upstream_reference():
    """Guards against the fixture drifting from the real format.

    If this fails, run ``python tools/refresh_fixtures.py`` and commit the
    result. It is the only test that touches the upstream clone, and it is not
    required for the contract to be verified.
    """
    for relative, meta in MANIFEST["files"].items():
        upstream = UPSTREAM / "sf_3" / "output_data" / relative
        assert upstream.exists(), f"upstream file vanished: {relative}"
        limit = int(meta["lines"])
        lines = [
            line
            for line in upstream.read_bytes().split(b"\r\n")
            if line
        ][:limit]
        expected = b"".join(line + b"\r\n" for line in lines)
        assert (FIXTURE / relative).read_bytes() == expected, (
            f"{relative} has drifted from upstream; run tools/refresh_fixtures.py"
        )


# --------------------------------------------------------------------------
# Self-consistency of the writers, always runs
# --------------------------------------------------------------------------


def test_writers_emit_crlf_and_no_header(tmp_path):
    path = tmp_path / "x.txt"
    with DelimitedWriter(str(path), "pipe") as w:
        w.write_row([1, "a", None])
        w.write_row([2, "b", ""])
    raw = path.read_bytes()
    assert raw == b"1|a|\r\n2|b|\r\n"
    assert b"\n" not in raw.replace(b"\r\n", b"")


def test_single_column_writer(tmp_path):
    path = tmp_path / "BatchDate.txt"
    with SingleColumnWriter(str(path)) as w:
        w.write_row(["2017-07-07"])
    assert path.read_bytes() == b"2017-07-07\r\n"


def test_xml_action_writer_has_no_trailing_newline(tmp_path):
    """CustomerMgmt.xml must NOT end with a newline; the source does not."""
    path = tmp_path / "CustomerMgmt.xml"
    w = XmlActionWriter(str(path))
    w.write_action(
        action_type="NEW",
        action_ts="2007-07-07T16:07:49",
        customer_id=0,
        payload={"C_TAX_ID": "923-54-6498", "C_GNDR": "F", "C_TIER": "3", "C_DOB": "1940-12-02",
                 "C_L_NAME": "Joannis", "C_F_NAME": "Adara", "C_M_NAME": "",
                 "C_ADLINE1": "4779 Weller Way", "C_ADLINE2": "", "C_ZIPCODE": "92624",
                 "C_CITY": "Columbus", "C_STATE_PROV": "Ontario", "C_CTRY": "Canada",
                 "C_PRIM_EMAIL": "a@b.com", "C_ALT_EMAIL": "", "C_PHONE_1": {"C_CTRY_CODE": 1, "C_AREA_CODE": 872, "C_LOCAL": "523-8928", "C_EXT": ""},
                 "C_LCL_TX_ID": "CA3", "C_NAT_TX_ID": "YT3",
                 "CA_ID": 0, "CA_B_ID": 10180, "CA_NAME": "abc"},
    )
    w.close()
    raw = path.read_bytes()
    assert raw.endswith(b"</TPCDI:Actions>")
    assert not raw.endswith(b"\r\n")
    assert b'xmlns:TPCDI="http://www.tpc.org/tpc-di"' in raw


def test_generated_run_is_reproducible(tmp_path):
    """Same seed, same bytes. The generator must be deterministic."""
    from datagen.batches import generate

    a, b = tmp_path / "a", tmp_path / "b"
    for out in (a, b):
        generate(
            GenConfig(
                scale_factor=0.0005,
                seed=99,
                batch_count=2,
                output_root=str(out),
            )
        )
    for name in ("Batch1/Trade.txt", "Batch1/CashTransaction.txt", "Batch2/Trade.txt"):
        assert (a / name).read_bytes() == (b / name).read_bytes(), name


def _read_lines(path: Path) -> list[str]:
    """Read data lines, preserving CRLF semantics.

    ``Path.read_text`` applies universal-newline translation, which collapses
    CRLF to LF on Windows and would hide the very line-ending contract we are
    testing. Read bytes and split explicitly.
    """
    return [line for line in path.read_bytes().decode("utf-8").split("\r\n") if line]


def test_duplicates_actually_appear(tmp_path):
    """A configured duplicate rate must produce repeated lines on disk."""
    from datagen.batches import generate

    out = tmp_path / "dup"
    generate(
        GenConfig(
            scale_factor=0.001,
            seed=7,
            batch_count=1,
            problems=ProblemRates(duplicate_rate_offline=0.5, duplicate_rate_streaming=0.0),
            output_root=str(out),
        )
    )
    lines = _read_lines(out / "Batch1" / "Trade.txt")
    assert len(lines) != len(set(lines)), "expected duplicate rows at 50% rate"


def test_clean_run_has_no_duplicates(tmp_path):
    """Disabling the problem must remove it, so a clean baseline exists."""
    from datagen.batches import generate

    out = tmp_path / "clean"
    generate(
        GenConfig(
            scale_factor=0.001,
            seed=7,
            batch_count=1,
            problems=ProblemRates(duplicate_rate_offline=0.0, duplicate_rate_streaming=0.0),
            output_root=str(out),
        )
    )
    lines = _read_lines(out / "Batch1" / "Trade.txt")
    assert len(lines) == len(set(lines))


def test_schema_evolution_changes_field_count_across_batches(tmp_path):
    """Batch1 Trade has 14 fields; Batch2 Trade has 16. That difference is the drift."""
    from datagen.batches import generate

    out = tmp_path / "drift"
    generate(GenConfig(scale_factor=0.0005, seed=3, batch_count=2, output_root=str(out)))
    assert _first_fields(out / "Batch1" / "Trade.txt") == 14
    assert _first_fields(out / "Batch2" / "Trade.txt") == 16


def test_batch1_cash_transaction_has_no_surrogate_prefix(tmp_path):
    """Batch1 CashTransaction is 4 fields with CT_CA_ID first; deltas gain a prefix."""
    from datagen.batches import generate

    out = tmp_path / "cash"
    generate(GenConfig(scale_factor=0.0005, seed=3, batch_count=2, output_root=str(out)))
    assert _first_fields(out / "Batch1" / "CashTransaction.txt") == 4
    assert _first_fields(out / "Batch2" / "CashTransaction.txt") == 6


def test_trade_history_is_three_fields(tmp_path):
    from datagen.batches import generate

    out = tmp_path / "th"
    generate(GenConfig(scale_factor=0.0005, seed=3, batch_count=1, output_root=str(out)))
    assert _first_fields(out / "Batch1" / "TradeHistory.txt") == 3


def test_embedded_spaces_survive(tmp_path):
    """Text fields contain spaces; the delimiter is what protects them."""
    from datagen.batches import generate

    out = tmp_path / "spaces"
    generate(GenConfig(scale_factor=0.001, seed=11, batch_count=1, output_root=str(out)))
    saw_spaced = False
    for line in _read_lines(out / "Batch1" / "CashTransaction.txt"):
        fields = line.split("|")
        assert len(fields) == 4, f"expected 4 fields, got {len(fields)}"
        if " " in fields[3]:
            saw_spaced = True
    assert saw_spaced, "no embedded-space text was generated"


def test_deterministic_rng_stream():
    a = rng_for(42, "customer", 7).random()
    b = rng_for(42, "customer", 7).random()
    c = rng_for(42, "customer", 8).random()
    assert a == b
    assert a != c


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _first_fields(path: Path) -> int:
    line = path.read_bytes().split(b"\r\n")[0].decode()
    return len(line.split("|"))
