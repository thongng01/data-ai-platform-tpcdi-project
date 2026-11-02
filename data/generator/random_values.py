import hashlib

import numpy as np
import polars as pl


def values(identifiers, seed: int, field: str) -> np.ndarray:
    salt = int.from_bytes(hashlib.blake2b(field.encode("ascii"), digest_size=8).digest(), "little")
    identifiers = np.asarray(identifiers, dtype=np.uint64)
    with np.errstate(over="ignore"):
        mixed = identifiers + np.uint64(seed) + np.uint64(salt)
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
