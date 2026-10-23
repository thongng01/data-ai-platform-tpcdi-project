"""Data-problem injectors.

Each injector takes a stream of rows and returns a stream of rows with one
specific problem applied. They compose: a table spec decides which problems
apply and in what order, and the pipeline applies them as the rows stream out,
so nothing is materialised in memory.

The problems are first-class because the rubric asks us to simulate them and
then handle them. We keep them separable from generation for two reasons: a
clean run (all rates zero) is a valid thing to produce and to test against, and
the same table spec then covers both the clean case and the broken case.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Sequence, TypeVar

from .rng import weighted_pick

T = TypeVar("T")

Row = Sequence[object]
RowTransform = Callable[[Row], Row]


def duplicate_rows(
    rows: Iterable[T], rate: float, seed: int, key: str
) -> Iterator[T]:
    """Repeat ``rate`` of rows verbatim.

    Duplicates are whole-row repeats, which is what makes them hard: a dedupe
    keyed on any subset that omits a differing column will keep the wrong copy.
    """
    import random

    rng = random.Random(seed)
    for row in rows:
        yield row
        if rate > 0 and rng.random() < rate:
            yield row


def inject_skew(
    rows: Iterable[T],
    rate: float,
    column: int,
    hot_value: object,
    seed: int,
) -> Iterator[T]:
    """Force ``rate`` of rows onto a single key in ``column``.

    This produces value-frequency skew: one key dominates the column. Note that
    a skewed column is not automatically a slow query. Whether Spark actually
    suffers depends on whether the column is the partition or join key, so skew
    on a lookup column and skew on a join key are separate concerns and are
    configured separately.
    """
    import random

    rng = random.Random(seed)
    for row in rows:
        if rate > 0 and rng.random() < rate:
            mutable = list(row)  # type: ignore[arg-type]
            mutable[column] = hot_value
            yield mutable  # type: ignore[misc]
        else:
            yield row


def apply_schema_evolution(
    rows: Iterable[Row],
    *,
    active: bool,
    column_name: str,
    value_for: Callable[[int], object],
) -> Iterator[Row]:
    """Append a new column to every row when the evolution is active.

    The point is that the column exists in later batches and not in earlier
    ones, so the ingest path has to cope with a schema that changes under it.
    The new value is derived from the row index so it is stable on re-run.
    """
    for index, row in enumerate(rows):
        if active:
            yield [*row, value_for(index)]
        else:
            yield row


def shift_event_time(
    rows: Iterable[Row],
    *,
    time_column: int,
    rate: float,
    max_delay_s: int,
    seed: int,
) -> Iterator[tuple[Row, datetime]]:
    """Push some rows' event time into the future, producing late arrivals.

    Returns ``(row, arrival_time)`` so a downstream replay can decide whether
    the arrival lands after the event's window has closed. Pairs the shifted
    event time with a later ingestion time.
    """
    import random

    rng = random.Random(seed)
    for row in rows:
        if rate > 0 and rng.random() < rate:
            delay = rng.randint(1, max_delay_s)
            value = row[time_column]
            if isinstance(value, datetime):
                yield [*row[:time_column], value + timedelta(seconds=delay), *row[time_column + 1:]], value + timedelta(
                    seconds=delay
                )
                continue
        yield row, None  # type: ignore[misc]


def shuffle_within_window(
    rows: Iterable[Row],
    *,
    rate: float,
    time_column: int,
    window: int,
    seed: int,
) -> Iterator[Row]:
    """Reorder rows so event times are locally non-monotonic.

    Out-of-order is distinct from late arrival: out-of-order is about ordering
    between records, late arrival is about a record showing up after its window
    closed. Both can be present and they are handled differently, so they are
    injected separately.
    """
    import random

    rng = random.Random(seed)
    buffer: list[Row] = []

    def flush() -> Iterator[Row]:
        while buffer:
            index = rng.randrange(len(buffer))
            if rate > 0 and rng.random() < rate:
                swap = min(len(buffer) - 1, index + rng.randint(1, window))
                buffer[index], buffer[swap] = buffer[swap], buffer[index]
            yield buffer.pop(index)

    for row in rows:
        buffer.append(row)
        if len(buffer) >= window:
            yield from flush()
    yield from flush()


@dataclass(frozen=True)
class ProblemReport:
    """What a run actually injected, so the number is measured rather than asserted."""

    table: str
    rows: int
    duplicates: int = 0
    late_arrivals: int = 0
    out_of_order: int = 0
    schema_evolved: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "table": self.table,
            "rows": self.rows,
            "duplicates": self.duplicates,
            "late_arrivals": self.late_arrivals,
            "out_of_order": self.out_of_order,
            "schema_evolved": self.schema_evolved,
        }


def skewed_distribution(
    rng,
    options: Sequence[str],
    weights: Sequence[float],
    skew: float,
) -> str:
    """Blend a base distribution toward a dominant value.

    ``skew`` of ``0.0`` returns the base distribution untouched; ``1.0`` returns
    the first option every time. In between, the head option absorbs the skew
    share and the rest keep their relative proportions.
    """
    if skew <= 0:
        return weighted_pick(rng, options, weights)
    head, rest = options[0], options[1:]
    rest_weights = weights[1:]
    total_rest = sum(rest_weights) or 1.0
    adjusted = [w / total_rest * (1.0 - skew) for w in rest_weights]
    return weighted_pick(rng, [head, *rest], [skew, *adjusted])
