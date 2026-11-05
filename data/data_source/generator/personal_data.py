"""Select repeatable synthetic personal fields from small public-domain lists."""

import csv
from functools import cache, lru_cache
from pathlib import Path

import numpy as np

from .random_values import values


DEFINITIONS = Path(__file__).parent / "definitions"
STREET_NAMES = np.asarray([
    "Oak", "Maple", "Pine", "Cedar", "Elm", "Walnut", "Willow", "Cherry",
    "Birch", "Chestnut", "Hickory", "Magnolia", "Laurel", "Sycamore", "Aspen", "Poplar",
])
STREET_TYPES = np.asarray(["Street", "Avenue", "Road", "Lane", "Drive", "Court"])


@cache
def _rows(filename: str) -> tuple[dict[str, str], ...]:
    """Read a committed UTF-8 word list once in each worker process."""
    with (DEFINITIONS / filename).open(encoding="utf-8", newline="") as handle:
        return tuple(csv.DictReader(handle))


@cache
def _name_distribution(filename: str, gender: str = "") -> tuple[np.ndarray, np.ndarray]:
    """Keep names and cumulative counts together so selection preserves frequency."""
    rows = [row for row in _rows(filename) if not gender or row["gender"] == gender]
    names = np.asarray([row["name"] for row in rows])
    cumulative = np.cumsum([int(row["count"]) for row in rows], dtype=np.uint64)
    names.flags.writeable = cumulative.flags.writeable = False
    return names, cumulative


def _names(identifiers: np.ndarray, seed: int, field: str, filename: str, gender: str = "") -> np.ndarray:
    """Map each stable random key into the list's cumulative frequency counts."""
    names, cumulative = _name_distribution(filename, gender)
    draws = values(identifiers, seed, field) % cumulative[-1]
    return names[np.searchsorted(cumulative, draws, side="right")]


@cache
def _locations() -> dict[str, np.ndarray]:
    """Load whole location rows; independent city and ZIP draws would disagree."""
    rows = _rows("locations.csv")
    columns = {name: np.asarray([row[name] for row in rows]) for name in rows[0]}
    for column in columns.values():
        column.flags.writeable = False
    return columns


def _join(*parts) -> np.ndarray:
    """Join text columns without fixed-width assignment that could cut off text."""
    result = np.asarray(parts[0], dtype=str)
    for part in parts[1:]:
        result = np.strings.add(result, part)
    return result


def personal_columns(identifiers, seed: int = 0, namespace: str = "customer") -> dict[str, np.ndarray]:
    """Build independent personal columns from entity IDs, seed, and field purpose.

    Customers and matching prospects use the same namespace and identity. Employee
    keys use a different namespace, so an employee need not resemble a customer
    with the same numeric ID. Names and addresses describe synthetic people.
    """
    identifiers = np.asarray(identifiers, dtype=np.int64)
    if identifiers.ndim != 1:
        raise ValueError("Personal identities must be a one-dimensional array.")
    prefix = f"personal-{namespace}-"
    gender = np.where(values(identifiers, seed, prefix + "gender") % 2 == 0, "M", "F")
    first = _given_names(identifiers, seed, prefix + "first", gender)
    middle = _given_names(identifiers, seed, prefix + "middle", gender).astype("U1")
    middle = np.where(values(identifiers, seed, prefix + "has-middle") % 100 < 85, middle, "")
    last = _names(identifiers, seed, prefix + "last", "surnames.csv")
    location_rows = _locations()
    location_index = values(identifiers, seed, prefix + "location") % len(location_rows["city"])
    location = {name: column[location_index] for name, column in location_rows.items()}
    return _contact_columns(identifiers, seed, prefix, first, middle, last, gender, location)


def _given_names(identifiers: np.ndarray, seed: int, field: str, gender: np.ndarray) -> np.ndarray:
    """Select a first or middle name from the list for the recorded gender."""
    male = _names(identifiers, seed, field, "given_names.csv", "M")
    female = _names(identifiers, seed, field, "given_names.csv", "F")
    return np.where(gender == "M", male, female)


def _contact_columns(identifiers, seed, prefix, first, middle, last, gender, location) -> dict[str, np.ndarray]:
    """Combine synthetic streets with consistent places and safe contact details."""
    number = (values(identifiers, seed, prefix + "house") % 9999 + 1).astype(str)
    street = STREET_NAMES[values(identifiers, seed, prefix + "street") % len(STREET_NAMES)]
    kind = STREET_TYPES[values(identifiers, seed, prefix + "street-type") % len(STREET_TYPES)]
    apartment = _join("Apt ", (values(identifiers, seed, prefix + "unit") % 500 + 1).astype(str))
    apartment = np.where(values(identifiers, seed, prefix + "has-unit") % 100 < 25, apartment, "")
    # NANPA reserves only 555-0100 through 555-0199 for fictional use. Other
    # 555 numbers are not all reserved, so the final two digits vary, not four.
    ending = (100 + values(identifiers, seed, prefix + "phone") % 100).astype(str)
    local = _join("5550", ending)
    # These domains are reserved for examples. The ID keeps equal names from
    # producing equal email addresses, without relying on generation order.
    mailbox = _join(np.strings.lower(first.astype("U1")), ".", np.strings.lower(last), ".", identifiers.astype(str))
    return {
        "first_name": first, "last_name": last, "middle_initial": middle, "gender": gender,
        "address_line1": _join(number, " ", street, " ", kind), "address_line2": apartment,
        "postal_code": location["postal_code"], "city": location["city"], "state": location["state"],
        "country": np.full(len(identifiers), "USA"), "phone_country": np.full(len(identifiers), "1"),
        "phone_area": location["area_code"], "phone_local": local,
        "phone": _join(location["area_code"], local), "email": _join(mailbox, "@example.com"),
        "alternate_email": np.where(values(identifiers, seed, prefix + "has-email2") % 100 < 30,
                                    _join(mailbox, "@example.net"), ""),
    }


@lru_cache(maxsize=4096)
def _personal_items(identifier: int, seed: int, namespace: str) -> tuple[tuple[str, str], ...]:
    """Cache a bounded set of scalar views for repeated customer and account actions."""
    columns = personal_columns([identifier], seed, namespace)
    return tuple((name, str(column[0])) for name, column in columns.items())


def personal_values(identifier: int, seed: int = 0, namespace: str = "customer") -> dict[str, str]:
    """Return a fresh scalar record with exactly the same values as array selection."""
    return dict(_personal_items(int(identifier), int(seed), namespace))
