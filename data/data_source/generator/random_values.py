"""Stable named random values in scalar, NumPy, and Polars forms."""

import hashlib
from functools import cache

import numpy as np


@cache
def salt(field: str) -> int:
    """Map a purpose name to a stable 64-bit mixing salt."""
    return int.from_bytes(hashlib.blake2b(field.encode("ascii"), digest_size=8).digest(), "little")


def scalar(identifier: int, seed: int, field: str) -> int:
    """Mix one stable identity without shared random-number state."""
    mask = (1 << 64) - 1
    mixed = (int(identifier) + seed + salt(field)) & mask
    mixed = ((mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9) & mask
    mixed = ((mixed ^ (mixed >> 27)) * 0x94D049BB133111EB) & mask
    return mixed ^ (mixed >> 31)


def uniform(identifier: int, seed: int, field: str) -> float:
    """Return one draw in [0, 1) using the upper 53 random bits."""
    return (scalar(identifier, seed, field) >> 11) / (1 << 53)


def values(identifiers, seed: int, field: str) -> np.ndarray:
    """Mix whole identity arrays exactly like the scalar implementation."""
    field_salt = salt(field)
    identifiers = np.asarray(identifiers, dtype=np.uint64)
    with np.errstate(over="ignore"):
        mixed = identifiers + np.uint64(seed) + np.uint64(field_salt)
        mixed = (mixed ^ (mixed >> 30)) * np.uint64(0xBF58476D1CE4E5B9)
        mixed = (mixed ^ (mixed >> 27)) * np.uint64(0x94D049BB133111EB)
        return mixed ^ (mixed >> 31)


def uniforms(identifiers, seed: int, field: str) -> np.ndarray:
    """Return whole-array uniform draws that cannot round up to one."""
    return (values(identifiers, seed, field) >> np.uint64(11)).astype(np.float64) / 2**53
