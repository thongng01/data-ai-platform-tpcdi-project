# Measure the generator's characteristics instead of asserting configured rates

The rubric asks us to document, after generating: which column is skewed and at
what ratio, which column has high cardinality, what the schema looks like before
and after evolution, the duplicate ratio, and the data volume. None of that is a
property of the generator's configuration. All of it is a property of the files
that came out. So we measure the output instead of reporting what we asked for.

## Context

We could satisfy the rubric by printing the configured rates, saying "duplicate
rate 2 percent". That number describes our intent, not our data. It stays 2
percent even if the injector silently stops working.

## Decision

`generator/src/datagen/inspect.py` reads the produced files and computes every
characteristic from the bytes on disk: the skewed column and its share,
cardinality as distinct over rows, which is the rubric's own definition,
duplicate rate by counting repeated lines, field count per file, and total
bytes. A test asserts that a configured 10 percent rate measures between 8 and
12 percent, and that a configured 0 percent measures exactly zero.

## Consequences

The reported numbers are defensible because they come from the artifact rather
than the config. The tool also catches a broken injector instead of reporting the
intent.

It costs one pass over the data after generation, so large runs pay for it once.

Every problem the generator can inject therefore needs a measurable signature.
An injection we cannot measure is an injection we cannot claim, which constrains
what we add.