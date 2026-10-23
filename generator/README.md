# datagen: synthetic TPC-DI data generator

Our own generator. We do not wrap benchbox or any other generator. The upstream
`sf_3` sample under `misc/tpcdi/` is a format reference, and
`datagen/emit.py` encodes the byte level contract every generated file must
meet.

## Run it

The project virtualenv lives at the repo root (`.venv/`, created with `uv`, WSL
Python 3.14). It holds `pytest`, `datagen` (installed editable), and `benchbox`.
Use it for everything:

```bash
cd generator
VENV=../.venv/bin

# small demo run, about 9 MB across 3 batches
$VENV/datagen --scale 0.02 --out ../misc/sample_run --clean

# full scale run
$VENV/datagen --scale 1.0 --out ../misc/data --clean

# clean data for ingest tests, no injected problems
$VENV/datagen --scale 0.02 --out ../misc/clean_run --clean \
    --no-duplicates --no-skew --no-schema-evolution

# measure what we produced
$VENV/python -m datagen.inspect ../misc/sample_run
```

Tests: `$VENV/python -m pytest tests/ -q`

After changing dependencies in `generator/pyproject.toml`, reinstall with:

```bash
uv pip install --python ../.venv/bin/python -e .
```

## What comes out

```
<out>/
  manifest.json          scale factor, seed, problem rates, file inventory
  Batch1/                full historical load
    BatchDate.txt
    CustomerMgmt.xml     the SCD2 source: 6 action types, partial payloads
    Trade.txt            14 columns
    TradeHistory.txt     3 columns
    CashTransaction.txt  4 columns
    HoldingHistory.txt   4 columns
    DailyMarket.txt      6 columns
    WatchHistory.txt     4 columns
    TradeType.txt, StatusType.txt, Industry.txt, TaxRate.txt
    Date.txt (18 cols), Time.txt (86,400 rows)
    FINWIRE<yyyyQq>      fixed width CMP, SEC, FIN records
  Batch2/                incremental deltas
    Trade.txt            16 columns: op code, row id, TH_TRADE_ID, then 14
    CashTransaction.txt  6 columns
    HoldingHistory.txt   6 columns
    DailyMarket.txt      8 columns
    WatchHistory.txt     6 columns
  Batch3/                same shape as Batch2
```

---

## The format contract

Every rule below is checked against real TPC-DI bytes by
`tests/test_contract.py`. Do not break them without regenerating the fixture.

| Rule | Why it matters |
|---|---|
| **CRLF everywhere** | A naive `split('\n')` leaves `\r` on every field. |
| **No header row, ever** | Only `*_audit.csv` in TPC-DI has headers, and we do not emit those. Spark `skip_leading_rows` must be 0. |
| **Pipe** for `.txt`, **comma** for `.csv`, **none** for FINWIRE | Delimiters differ across file families. |
| **Empty string means NULL** | Writers never quote, escape, or elide. `\|\|` is a NULL field. |
| **Text fields contain spaces** | `CT_NAME`, `CA_NAME`, HR branch, FINWIRE `SEC.Name`. Never split on whitespace. The delimiter is the only protection. |
| **Booleans: `0`/`1` in trade files, `true`/`false` in `Date.txt` and `Time.txt`** | TPC-DI is inconsistent here on purpose. |
| **`CustomerMgmt.xml` has no trailing newline** | The source file's last bytes are `ions>`. |
| **Batch1 and delta field counts differ** | Trade 14 becomes 16, Cash 4 becomes 6. This is the schema evolution. |

### The fixture

`tests/fixtures/tpcdi_sf3/` is a byte exact slice of the upstream `sf_3` sample:
13 files, about 4KB, plus a `manifest.json` recording field counts, delimiters,
and hashes. The contract tests read only this fixture, so the suite runs on a
fresh clone with no upstream checkout and nothing else installed.

Refresh it after updating the upstream reference:

```
python tools/refresh_fixtures.py                  # reads ../misc/tpcdi
python tools/refresh_fixtures.py /path/to/TPC-DI  # or an explicit path
```

Commit the regenerated fixture afterwards. `test_fixture_matches_upstream`
compares the fixture against the real clone when `misc/tpcdi` happens to exist,
so drift fails a test instead of going unnoticed. That is the only test that
touches upstream, and the contract stays verified without it.

---

## Injected data problems

Configured on `ProblemRates`, measured by `datagen.inspect`, disabled with
`--no-*` flags. The clean run matters as much as the dirty one, because the
clean run is the baseline the before and after evidence needs.

| Problem | Knob | Where |
|---|---|---|
| Value skew | `trade_status_skew`, `trade_type_skew` | `Trade.T_ST_ID`, 92 percent `CMPT`, and `T_TT_ID` |
| Join skew | `join_skew`, `join_skew_key` | `CashTransaction.CT_CA_ID`, `Trade.T_CA_ID` |
| Duplicates, offline | `duplicate_rate_offline` | whole row repeats in Batch1 |
| Duplicates, streaming | `duplicate_rate_streaming` | whole row repeats in Batch2 and 3 |
| Schema evolution | `schema_evolution_batch` | the op code and row id prefix appearing at Batch2 |
| Late arrivals | `late_arrival_rate`, `late_arrival_max_delay_s` | event time pushed into the future |
| Out of order | `out_of_order_rate` | local reordering against an otherwise sorted file |

Two distinctions that change what you do downstream:

- **Value skew is not a slow query.** A column can be 92 percent one value with
  no processing cost. Spark only suffers when the column is the partition or
  join key, which is why these are two separate knobs.
- **Late arrival is not out-of-order.** Late arrival means arriving after the
  window closed. Out-of-order means ordering relative to other records. They
  need different handling, so they inject separately.

---

## Layout

```
src/datagen/
  config.py           GenConfig and ProblemRates: the whole control surface
  rng.py              deterministic per entity RNG, text that breaks naive parsers
  emit.py             THE FORMAT CONTRACT, the only module that touches bytes
  problems.py         injectable problem transforms, composed per table
  specs_reference.py  reference tables and FINWIRE
  specs_events.py     trade, cash, market, watch, holding, CustomerMgmt.xml
  batches.py          Batch1 full load, incremental deltas, run manifest
  cli.py              the datagen entry point
  inspect.py          measures the produced dataset
tests/
  fixtures/           byte exact slice of upstream sf_3, about 4KB
  test_contract.py    format contract against real TPC-DI bytes
  test_inspect.py     measured characteristics
tools/
  refresh_fixtures.py regenerate the fixture from the upstream clone
```

---

## Provenance

Written for this project. Three references shaped decisions, and each is noted
where it applied:

- **`misc/tpcdi/`** gave us the format contract and the reference SCD2 and ETL
  logic.
- **benchbox** gave us the deterministic per record RNG pattern and the
  generation run manifest shape. Not its data emission, because it writes
  finished target tables.
- **`stewartbryson/dbt-tpcdi`** gave us medallion layering, SCD2 window columns,
  and temporal FK joins, for the dbt work downstream.

`github.com/AJamal27891/synthetic_data_generator` gave us the declarative
column type idea. Our specs are Python rather than YAML because fixed width
records, XML action streams, and per batch prefix shifts are too irregular for a
YAML schema to stay readable.

---

## Known gaps

- `HR.csv` and `Prospect.csv` are not generated yet, 22 and 9 columns.
- `Customer.txt` and `Account.txt` delta files are not generated. Only the XML
  action stream is. Note `Customer.txt` puts `C_ID` at field 3, not field 2.
- `late_arrival_rate` and `out_of_order_rate` are configured but not wired into
  the Trade writer. The injectors exist in `problems.py`.
- Market prices are uniform random, not a random walk, so one security's price
  does not persist across days.
- No Spark ingest yet. That is the next piece.