# Synthetic TPC-DI generator

`datagen` is the existing Python generator. It writes selected TPC-DI-shaped files and a run manifest. Its file shapes do not establish coherent brokerage history or complete benchmark behavior. The accepted redesign starts from the specification and a reference generator; see [Project Design](../docs/Project-Design.md#generator). [Status](../docs/Status.md#what-exists) records progress and defects.

The separate [Polars POC](poc_polars/README.md) generates connected source files with bounded calculations, YAML definitions, and independent validation. Its [evidence record](../docs/records/Polars-POC.md) gives the measured scale and limits. The commands below still operate the earlier `datagen` implementation.

## Run a small dataset

Run these commands from the repository root in WSL with `datagen` installed in `.venv`:

```bash
.venv/bin/datagen --scale 0.02 --out misc/tmp/generator-demo
.venv/bin/python -m datagen.inspect misc/tmp/generator-demo
```

Use a new output directory for a separate run. `--clean` deletes the entire selected output directory before generation. `make generate` uses scale 0.02 and replaces `misc/sample_run`, which is the existing pipeline's input directory.

Do not use the default scale of 1.0 for a full run with the current code. It requests 320 distinct tax-rate codes from a space of 90, so the generation loop cannot finish. See [known defects](../docs/Status.md#known-defects).

Run the generator tests with `make test-generator`. If dependency changes require an editable installation, run this from the repository root:

```bash
uv pip install --python .venv/bin/python -e generator
```

## Command interface

[cli.py](src/datagen/cli.py) defines the arguments. Explicit rate arguments override the corresponding disable flags if both are supplied.

| Argument | Default | Behavior |
|---|---|---|
| `--scale` | `1.0` | Multiplies the local base row counts. This is not a measured equivalence to DIGen scale. |
| `--out` | `data` | Selects the output directory relative to the current directory. |
| `--seed` | `20261018` | Supplies the deterministic generation seed. |
| `--batches` | `3` | Sets the number of batch directories. |
| `--incremental-ratio` | `0.02` | Sets later batch counts relative to Batch1 counts. |
| `--clean` | Disabled. | Removes the output directory before generation. |
| `--no-duplicates` | Disabled. | Sets both duplicate rates to zero. |
| `--no-skew` | Disabled. | Sets configurable skew values to zero. Base weighted distributions remain. |
| `--no-late` | Disabled. | Sets the lateness rate to zero; the writers do not apply that rate. |
| `--no-schema-evolution` | Disabled. | Sets the evolution batch value to zero. It does not remove native incremental prefixes. |
| `--late-arrival-rate` | `0.08` | Changes the configuration value; the writers do not apply it. |
| `--duplicate-rate-offline` | `0.02` | Controls repeated event rows in Batch1. |
| `--duplicate-rate-streaming` | `0.015` | Controls repeated event rows in later batch files. It does not publish a stream. |

[config.py](src/datagen/config.py) exposes additional Python configuration fields. Their presence does not prove that a writer uses them. For example, the explicit extra-column injector in the Trade writer is inactive, and lateness and out-of-order injection are not wired into the writer. A delivery process must measure arrival time separately from event time to demonstrate late stream events.

## Output files

Every batch has a `BatchDate.txt`. Batch1 uses date `2017-07-07`; later batches advance one day each. The output root also contains `manifest.json` with configuration, batch dates, and a file inventory.

| File family | Batch1 | Later batches |
|---|---|---|
| Trade | Contains 14 pipe-delimited fields. | Contains 16 fields: a CDC operation and sequence followed by the 14-field trade shape. |
| CashTransaction | Contains four pipe-delimited fields. | Contains six fields with a CDC prefix. |
| HoldingHistory | Contains four pipe-delimited fields. | Contains six fields with a CDC prefix. |
| DailyMarket | Contains six pipe-delimited fields. | Contains eight fields with a CDC prefix. |
| WatchHistory | Contains four pipe-delimited fields. | Contains six fields with a CDC prefix. |
| TradeHistory | Contains three pipe-delimited fields. | The writer does not emit it. |
| CustomerMgmt | Contains XML actions. | The writer does not emit customer or account delta files. |
| Reference files | Includes TradeType, StatusType, Industry, TaxRate, Date, and Time. | The writer does not repeat them. |
| FINWIRE | Contains fixed-width company, security, and financial records. | The writer does not emit later FINWIRE files. |

The generator does not emit `HR.csv`, `Prospect.csv`, `Customer.txt`, or `Account.txt`. Its code uses misleading incremental Trade variable names; the [data specification](../docs/Data-Specification.md#trade-lifecycle-updates) defines the source identity and change semantics.

Manifest counts are not measured output counts. Several file families use `-1`, and event entries use requested counts before duplicate insertion. Use the inspector for measured characteristics. The inspector reads whole files and assumes CRLF, so it needs a different reading strategy for a 100 GB dataset.

## Format checks

[emit.py](src/datagen/emit.py) defines delimited, fixed-width, and XML output. Delimited records use CRLF, no header, and empty fields for null values. Text can contain spaces, so readers must split on the declared delimiter. XML actions use a different logical record boundary from line-oriented files.

The committed fixture under `tests/fixtures/tpcdi_sf3/` contains slices of an upstream SF3 dataset. `tests/fixtures/manifest.json` records its formats and hashes. The contract tests check selected fixture and writer properties. They do not establish full source coverage, cross-table consistency, or correct trade histories.

From `generator/`, refresh a fixture only when intentionally changing its reference:

```bash
../.venv/bin/python tools/refresh_fixtures.py /path/to/TPC-DI
```

Without an explicit path, the helper uses `../misc/tpcdi`. Review fixture changes with the corresponding specification rules. The optional upstream comparison checks freshness when the upstream checkout is available.

## Code layout

| Module | Responsibility |
|---|---|
| `config.py` | Defines generation settings and local scale counts. |
| `rng.py` | Provides random value helpers. |
| `emit.py` | Serializes records. |
| `problems.py` | Defines duplicate, skew, schema, and timing transforms. Writers select which transforms run. |
| `specs_reference.py` | Writes reference and FINWIRE files. |
| `specs_events.py` | Writes event files and customer XML actions. |
| `batches.py` | Calls writers and records the manifest. |
| `cli.py` | Converts command arguments into configuration. |
| `inspect.py` | Measures supported file characteristics. |

The earlier implementation drew on the local `misc/tpcdi` sample for formats, benchbox for generation patterns, and `stewartbryson/dbt-tpcdi` for downstream transformation ideas. These are design influences, not evidence that this generator reproduces their behavior. The [generator feasibility study](../docs/research/Generator-Feasibility.md) evaluates the proposed replacement approach.
