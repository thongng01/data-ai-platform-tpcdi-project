"""Deterministic randomness helpers.

Every generated entity draws from its own RNG stream seeded from the run seed
and a stable string key. That means a row's values depend on its own identity
rather than on generation order, so re-running with the same seed produces the
same file, and generating one table alone produces the same values it would
have produced inside a full run. Borrowed in spirit from benchbox's
per-record deterministic RNG pattern.
"""

from __future__ import annotations

import hashlib
import random
from typing import Sequence, TypeVar

T = TypeVar("T")

_ALPHA_LOWER = "abcdefghijklmnopqrstuvwxyz"
_ALPHA_UPPER = _ALPHA_LOWER.upper()
_ALPHA = _ALPHA_LOWER + _ALPHA_UPPER
_DIGITS = "0123456789"

_FIRST_NAMES = (
    "Adara Joannis Tandie Shostak Lynna Kempffer Margit Vernell Shaundae "
    "Rosario Kathlene Fletcher Israel Ike Tamiko"
).split()
_LAST_NAMES = (
    "Joannis Kempffer Shostak Vernell Shaundae Rosario Kathlene "
    "Fletcher Tamiko Oyelaran Chivukwu Danjou"
).split()
_STREETS = (
    "Weller Way Corsicana Road Absecon Boulevard Wekiva Drive Kempson Drive "
    "Elmwood Parkway Tatum Street Deneside Drive Ashburton Lane"
).split()
_CITIES = (
    "Columbus Overland Park Bedford Lakewood Dorchester Brockton "
    "San Carlos Sandy Hook Lander"
).split()
_STATES = ("Ontario DC CA NY TX FL WA OH PA MA IL CA".split())
_COUNTRIES = ("Canada United States of America Germany France Australia".split())
_WORDS = (
    "Diverted Intuitive Southridge Freight Northwind Lazyboy Willow "
    "Quotient Matrix Vantage Zenith Pinnacle Kestrel Harbourside"
).split()


def _seed_from(*parts: object) -> int:
    digest = hashlib.blake2b(
        "|".join(str(part) for part in parts).encode("utf-8"), digest_size=8
    ).digest()
    return int.from_bytes(digest, "big")


def rng_for(seed: int, *key: object) -> random.Random:
    """An RNG stream identified by ``seed`` plus a stable key path."""
    return random.Random(_seed_from(seed, *key))


def pick(rng: random.Random, options: Sequence[T]) -> T:
    return options[rng.randrange(len(options))]


def weighted_pick(rng: random.Random, options: Sequence[T], weights: Sequence[float]) -> T:
    total = sum(weights)
    if total <= 0:
        raise ValueError("weights must sum to a positive value")
    threshold = rng.random() * total
    cumulative = 0.0
    for option, weight in zip(options, weights):
        cumulative += weight
        if threshold < cumulative:
            return option
    return options[-1]


def alpha_string(rng: random.Random, length: int) -> str:
    """Random letters, uniform. Used for account names and similar opaque text."""
    return "".join(_ALPHA[rng.randrange(len(_ALPHA))] for _ in range(length))


def alpha_string_with_spaces(rng: random.Random, length: int) -> str:
    """Random letters that deliberately contains embedded spaces.

    TPC-DI source data has text fields with spaces in them (``CT_NAME``,
    ``CA_NAME``, HR branch, FINWIRE ``SEC.Name``). Generating space-free text
    would let a broken whitespace-tokenising parser pass, so we generate text
    that breaks it on purpose. The pipe delimiter is what protects these
    fields in real TPC-DI, and it does the same here.
    """
    if length < 2:
        return alpha_string(rng, length)
    pieces: list[str] = []
    remaining = length
    while remaining > 0:
        chunk = min(remaining, rng.randint(6, 24))
        pieces.append("".join(_ALPHA[rng.randrange(len(_ALPHA))] for _ in range(chunk)))
        remaining -= chunk
        if remaining > 0:
            pieces.append(" ")
            remaining -= 1
    return "".join(pieces)


def symbol(rng: random.Random) -> str:
    """A TPC-DI security symbol: 'A' followed by 14 alphanumerics."""
    tail = "".join(_ALPHA[rng.randrange(len(_ALPHA))] for _ in range(14))
    return "A" + tail


def person(rng: random.Random) -> tuple[str, str, str]:
    """First, last, and middle name. The middle name is empty roughly half the time."""
    first = pick(rng, _FIRST_NAMES)
    last = pick(rng, _LAST_NAMES)
    middle = pick(rng, _FIRST_NAMES) if rng.random() < 0.45 else ""
    return first, last, middle


def address(rng: random.Random) -> tuple[str, str, str, str, str, str]:
    """Line 1, line 2 (often empty), zip, city, state/province, country."""
    line1 = f"{rng.randint(1, 99999)} {pick(rng, _STREETS)}"
    line2 = f"Apt {rng.randint(1, 400)}" if rng.random() < 0.35 else ""
    zipcode = f"{rng.randint(10000, 99999)}"
    city = pick(rng, _CITIES)
    state = pick(rng, _STATES)
    country = pick(rng, _COUNTRIES)
    return line1, line2, zipcode, city, state, country


def phone(rng: random.Random) -> tuple[int, int, str, str]:
    """Country code, area code, local number, extension (often empty)."""
    country_code = rng.randint(1, 99)
    area_code = rng.randint(100, 999)
    local = f"{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"
    ext = str(rng.randint(1000, 9999)) if rng.random() < 0.3 else ""
    return country_code, area_code, local, ext


def tax_id(rng: random.Random) -> str:
    return f"{rng.randint(100, 999)}-{rng.randint(10, 99)}-{rng.randint(1000, 9999)}"


def gibberish_words(rng: random.Random, count: int) -> str:
    return " ".join(pick(rng, _WORDS) for _ in range(count))


def int_str(rng: random.Random, low: int, high: int) -> str:
    return str(rng.randint(low, high))


def money(rng: random.Random, low: float, high: float) -> str:
    """Money as a bare 2dp string. No thousands separators, ever."""
    return f"{rng.uniform(low, high):.2f}"


def bool_int(rng: random.Random) -> str:
    """Trade files encode booleans as 0/1 text, not true/false."""
    return "1" if rng.random() < 0.5 else "0"


def bool_word(rng: random.Random) -> str:
    """Date.txt and Time.txt encode booleans as true/false, the opposite."""
    return "true" if rng.random() < 0.5 else "false"


def zero_padded(rng: random.Random, width: int) -> str:
    return str(rng.randrange(10**width)).zfill(width)
