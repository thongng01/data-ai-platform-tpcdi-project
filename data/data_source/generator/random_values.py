import hashlib
from functools import cache

import numpy as np
import polars as pl


@cache
def salt(field: str) -> int:
    return int.from_bytes(hashlib.blake2b(field.encode("ascii"), digest_size=8).digest(), "little")


def scalar(identifier: int, seed: int, field: str) -> int:
    mask = (1 << 64) - 1
    mixed = (int(identifier) + seed + salt(field)) & mask
    mixed = ((mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9) & mask
    mixed = ((mixed ^ (mixed >> 27)) * 0x94D049BB133111EB) & mask
    return mixed ^ (mixed >> 31)


def uniform(identifier: int, seed: int, field: str) -> float:
    return (scalar(identifier, seed, field) >> 11) / (1 << 53)


def values(identifiers, seed: int, field: str) -> np.ndarray:
    field_salt = salt(field)
    identifiers = np.asarray(identifiers, dtype=np.uint64)
    with np.errstate(over="ignore"):
        mixed = identifiers + np.uint64(seed) + np.uint64(field_salt)
        mixed = (mixed ^ (mixed >> 30)) * np.uint64(0xBF58476D1CE4E5B9)
        mixed = (mixed ^ (mixed >> 27)) * np.uint64(0x94D049BB133111EB)
        return mixed ^ (mixed >> 31)


def money(column: str) -> pl.Expr:
    amount = pl.col(column)
    magnitude = amount.abs()
    return pl.concat_str(
        pl.when(amount < 0).then(pl.lit("-")).otherwise(pl.lit("")),
        (magnitude // 100).cast(pl.String),
        pl.lit("."),
        (magnitude % 100).cast(pl.String).str.pad_start(2, "0"),
    )
