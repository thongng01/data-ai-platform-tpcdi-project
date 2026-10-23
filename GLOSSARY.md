# EDAI Coursework Domain Glossary

Shared vocabulary for the TPC-DI brokerage coursework. These are the words we
use when we mean a thing. Implementation detail (tool names, column names, file
formats, naming conventions) lives in `docs/`, not here.

## Source system

**TPC-DI**:
A benchmark that models a brokerage. It extracts from operational sources,
transforms them into a unified warehouse model, and loads a decision-support
warehouse.
_Avoid_: "the benchmark", "the data"

**Source file**:
One file produced by a source system in one batch. The generator writes these.
_Avoid_: "input file", "raw file" (a raw file has already landed in bronze)

**Batch**:
A dated set of source files representing one load. The first is the full
historical load; later ones carry only what changed.
_Avoid_: "load", "run"

**Incremental batch**:
A batch carrying only changes since the previous one. Each record is marked as
new or updated.
_Avoid_: "delta", "patch"

**Prospect**:
A potential customer with attributes but no trading history yet. Not a
Customer until onboarded.
_Avoid_: "lead", "customer"

**Broker**:
The employee who services an account, sourced from the HR system.
_Avoid_: "agent", "advisor", "salesperson"

**Account**:
A brokerage account belonging to exactly one Customer. One Customer may hold
several.
_Avoid_: "customer", "user"

## Zones

**Landing zone**:
Where generated files arrive before ingestion, standing in for the output of a
source system we do not control.
_Avoid_: "staging", "input", "bronze"

**Bronze zone**:
Source data as it was received, unmodified and untyped. The system of record
for what actually arrived.
_Avoid_: "raw zone"

**Silver zone**:
Cleaned, typed, deduplicated entities. One row per real-world thing.
_Avoid_: "staging zone", "cleansed layer"

**Gold zone**:
The modelled layer: dimensions, facts, and feature tables, shaped for query and
for ML.
_Avoid_: "warehouse" (the warehouse is the database; gold is the data in it)

## Modelling

**SCD2 (slowly changing dimension, type 2)**:
Versioning a dimension so each change adds a new version rather than overwriting
the old one, leaving history queryable. Applied to Customer, Account, Security,
Company.
_Avoid_: "scd", "versioned dim"

**Grain**:
What one row represents. Stating the grain is the first question about any
table; a table with two grains is two tables.
_Avoid_: "granularity", "level"

**Dim (dimension)**:
A descriptive entity we analyse *about*.
_Avoid_: "dimension table", "lookup"

**Fact**:
A measured event or snapshot we count or sum.
_Avoid_: "fact table", "measure"

**OBT (one big table)**:
A single wide denormalised table, used when a star schema is more joins than the
query needs.
_Avoid_: "wide table", "flat table"

**Surrogate key**:
A synthetic, stable identifier for a dimension row, so facts do not break when
the natural key changes.
_Avoid_: "technical key", "id"

**Partial payload**:
An update record carrying only the fields that changed. The reason dimension
versions cannot be read directly out of the source and must be reconstructed.
_Avoid_: "diff", "patch"

**Carry-forward**:
Filling a partial payload's empty fields from the previous version, to
reconstruct what a row looked like at a point in time.
_Avoid_: "fill down", "coalesce"

## Data problems

**Skew**:
An uneven distribution across a key, so some keys carry far more rows than
others.
_Avoid_: "hotspot", "data skew"

**Value skew** vs **join skew**: two different things. Value skew is one value
dominating a column. Join skew is one key dominating a join, which is what
actually costs processing time. A column can have the first and not the second.
_Avoid_: "skew" for either, unqualified

**High cardinality**:
Many distinct values relative to row count. Defined for this project as
distinct-values / total-values.
_Avoid_: "unique", "high-cardinality column"

**Duplicate**:
The same real-world record delivered more than once. Whole-record repeats, so
deduplication has to key on the right columns.
_Avoid_: "double", "repeat"

**Schema evolution**:
The source schema changing over time, so a column exists in a later batch and
not an earlier one.
_Avoid_: "schema drift", "migration"

**Late arrival**:
An event that reaches the stream after its window has already closed, so it
would otherwise be dropped.
_Avoid_: "out-of-order event", which is a different thing (see below)

**Out-of-order event**:
An event whose time is older than events already processed. About ordering
between records; late arrival is about arriving too late. Both can occur, and
they are handled differently.
_Avoid_: "late arrival"

**Window**:
A bounded time slice used to aggregate a stream. Tumbling windows do not
overlap; sliding windows do.
_Avoid_: "bucket", "batch" (in the streaming sense)

**Watermark**:
The point after which the system stops waiting for earlier events and closes a
window. The knob that trades completeness against latency.
_Avoid_: "delay", "threshold"

## Generation

**Data generator**:
Our own program that produces the source files. The centre of this project:
every row of project data comes from it.
_Avoid_: "the benchmark data", "sf_3", "seed data"

**Scale factor**:
A single multiplier driving how many rows the generator emits. Lets one codebase
produce a demo dataset or a large one.
_Avoid_: "size", "volume"

**Injection rate**:
The configured share of records carrying a given data problem. A rate of zero
produces clean data, which is the baseline for before/after comparisons.
_Avoid_: "error rate", "problem rate"

## Platform

**Feature**:
An input signal to a model.
_Avoid_: "column", "variable"

**Feature table**:
A table holding features, stamped with when the event happened and when the row
was written, so a model can never train on information it could not have had.
_Avoid_: "feature set"

**Feature view**:
A named group of features over one entity, with its freshness rules, that the
feature store serves.
_Avoid_: "feature table", "feature group"

**Materialization**:
Copying features from the offline store into the online store so a model can
read them at request time.
_Avoid_: "sync", "publishing"

**Label**:
The prediction target. Here: whether a customer trades in the next 7 days.
_Avoid_: "target", "y"

**Entity**:
The thing a prediction is about. Here: a customer.
_Avoid_: "subject", "key"

**Lineage**:
Where a piece of data came from and what was done to it.
_Avoid_: "lineage graph", "provenance"

**Validation**:
An automated check that a table or column still holds what it must. Fails the
run rather than waiting for a human to notice.
_Avoid_: "data quality check", "test"

**Novel idea**:
Something we add beyond the stated requirements, small enough to demo.
_Avoid_: "bonus feature", "extra credit"
