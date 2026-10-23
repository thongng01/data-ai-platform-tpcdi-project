# Build our own data generator rather than wrapping an existing one

Every row of data this project processes comes from a generator we write. We do
not wrap benchbox, and we do not adopt an off the shelf generator.

## Context

Two generators were available to build on. `benchbox` is a pip package already
installed in the repo and implements TPC-DI end to end.
`AJamal27891/synthetic_data_generator` is a reference the team pointed us at.

Both have the wrong shape. benchbox emits finished target warehouse tables and
skips the ETL, so it produces the answer rather than the inputs a data integration
project is about. It also injects no data problems, which is the one thing this
project exists to demonstrate. `synthetic_data_generator` is not a TPC-DI
generator. It is an unlicensed retail CSV generator whose scale is a hardcoded
constant per table, and whose own topological sort has circular dependencies.

## Decision

Write the generator. Borrow patterns from both: benchbox's deterministic per
record RNG and generation run manifest, and the declarative column type taxonomy
from `synthetic_data_generator`. Generate every record ourselves.

## Consequences

The TPC-DI sf_3 sample stops being a dataset and becomes a format reference. It
tells us field order, delimiters, record types, and batch structure, and nothing
more. We lose the convenience of a working corpus for free, and that is the
point of the exercise.

We inherit the cost of getting the format right, so the byte level contract is
pinned by tests that compare our output against real sf_3 bytes. Those bytes are
committed as a 4KB fixture at `generator/tests/fixtures/`, so the suite does not
need the upstream clone.

We also inherit the cost of injecting data problems deliberately, which is where
the rubric awards marks.

The licensing question on the reference implementation is accepted as a learning
project risk and noted per file.