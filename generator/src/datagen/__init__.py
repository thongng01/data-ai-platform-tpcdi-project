"""Synthetic TPC-DI-shaped data generator.

Own implementation. We do not wrap benchbox or any other generator; TPC-DI's
sf_3 sample is a *format* reference, and :mod:`datagen.emit` encodes the byte
level contract that every generated file must satisfy.

Layout:

- :mod:`datagen.config` -- the knob set and the scale model
- :mod:`datagen.rng` -- deterministic per-entity randomness
- :mod:`datagen.emit` -- the format contract (CRLF, no header, delimiters, NULL)
- :mod:`datagen.problems` -- skew, duplicates, schema evolution, lateness
- :mod:`datagen.specs_reference` / :mod:`datagen.specs_events` -- the tables
- :mod:`datagen.batches` -- Batch1 full load plus incremental deltas
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
