# Generator Verification

This record covers the generator rebuild, clean publisher implementation, and measured factor 1 evidence on 2026-11-05. The [generator guide](../../data/data_source/generator/README.md) defines execution and controls. [Data Platform section 2](../../docs/Data-Platform.md#2-data-generation-and-problem-injection) holds the public workload estimates and measurements.

## Implementation and Checks

The generator implements stages 1 to 3. NumPy arrays hold mutable customer and account state; Polars handles export and validation. Named daily steps keep proposal selection separate from ordered settlement. Public-domain personal-data lists replace generated placeholder names. The clean publisher implements stage 4 from saved files and messages, without running the business simulation again.

| Check | Passed | Warnings | Seconds | Local evidence under `local/tmp/overnight-generator/` |
| --- | ---: | ---: | ---: | --- |
| Original stage 1 baseline | 155 | 116 | 135.48 | `supervisor/baseline-tests.log` |
| Earlier integrated suite | 325 | 172 | 143.53 | `supervisor/integrated-suite-3.log` |
| Final integrated suite at source freeze `76081d9` | 394 | 172 | 134.87 | `supervisor/integrated-suite-4.log` |
| Lead rerun after integration into `infra/compat-test` | 394 | 172 | 131.57 | Lead session tool output |
| Publisher component tests, controlled SDK adapters | 93 | 4 | 13.02 | `supervisor/publisher-tests.log` |
| Fixture refresh and pinned-format guards | 72 | 0 | 0.73 | Scoped pytest run; 22 unrelated tests deselected |

The final integrated suite reports 172 Pandas warnings. The documentation checker reports 0 issues, and the source AST check reports 0 missing module, function, or class docstrings. Both reviewers closed their assigned findings. The full-suite command uses isolated SDK pins `boto3==1.43.124` and `confluent-kafka==2.16.1`; it does not modify the shared virtual environment:

```bash
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 POLARS_MAX_THREADS=2 \
  PYTHONPATH="$PWD/.venv/lib/python3.14/site-packages:$PWD" \
  local/tmp/overnight-generator/publisher/sdk-env/bin/python \
  -m pytest data/data_source -q
```

The test and review fixes cover the following conditions:

- Personal fields remain deterministic across scalar and array generation, seed propagation, batching, and matching prospects. Source widths and schemas remain valid.
- Customer inactivation uses mean active-account risk and an expected-trait normalizer, not future realized outcomes. Investment returns exclude external cash flows and preserve a genuine wipeout loss when equity reaches zero or becomes negative.
- Only an executed sale followed by a positive withdrawal creates an outflow candidate. Later hidden classification does not change behavior or features. Account and customer hazards continue without immunity.
- Source recording changes recorded copies only. Validation independently checks exact data-error selections, audit identities, original values, and correct settlement fees. Missing, duplicate, unused, or altered evidence fails.
- Fixture guards require the exact 13-file inventory, positive integer line counts, and nonblank records. Refresh reads and validates every bounded sample before any write. Missing, empty, or blank samples leave all existing output bytes unchanged and create no new output tree. Tests do not refresh committed fixtures.
- Publisher tests cover independent cadence and hash checks, source snapshots, ordered saved cash messages, acknowledgment checkpoints, uncertain retries, replay timing, destination binding, and secret redaction. These tests use SDK stubs, not live S3 or Kafka services.

## Whole-Generator Typed Runtime

The runtime redesign covers all 18 source families. It keeps business execution, named random draws, financial arithmetic, attrition rules, source contracts, and the independent saved-file validator. High-volume financial producers emit typed tuples; market, prospect, HR, time, and reference producers emit bounded columns. Source recording applies errors and channels to separate frames. XML, FINWIRE, and selected audit rows retain their format-specific Python encoders.

The selected Polars path replaces the SQLite catalog, sorted text spools, multilevel fan-in, Python merge heap, and catalog adoption. Each group seals typed Parquet parts. Native ordering, bounded contract-batch formatting and checks, and final file writers share one output lifecycle. No dependency was added. DuckDB-owned storage and ordering were evaluated as a genuine replacement, not as another sorter behind the old exporter. A compiled financial executor remains a possible later change; causal dependencies do not require Python, but preserving event ties, holding insertion order, and floating-point operation order needs separate evidence.

### Bounded Comparison

The bounded calendar covers initial dates 2018-10-01 through 2018-12-31 and daily dates 2019-01-01 through 2019-07-31. It uses factor 1, anchor 3,601, seed `20261029`, 1,250 companies, 2,000 securities, two workers, two Polars threads, a sampled 4 GiB RSS guard, and a 16 GiB disk guard. Generation timing includes construction, recording, temporary writes, ordering, formatting, checks, source writes, and hashing. Comparison and saved-file validation run outside that timer.

| Run | Generation and export seconds | Final export seconds | Separate validation seconds |
| --- | ---: | ---: | ---: |
| Improved text control, opening | 37.5767 | 8.8201 | Not rerun |
| Whole typed candidate, per-file formatting | 34.9408 | 10.5246 | 75.8044, passed |
| Improved text control, closing | 50.4143 | 12.1489 | Not rerun |
| Whole typed candidate, batch formatting | 31.3051 | 5.9550 | 53.9252, passed |

Every run matches the validated reference's 1,515 actual source files, 2,057,139 rows, 150,184,502 bytes, 441 audited fault fields, ordered typed hidden records, and normalized business summary. The only excluded business field is the implementation-specific `numeric_state_buffer_bytes`. The opening control's hidden comparison was added as separate read-only evidence after independent review found that omission in the harness. Its original measurement remains unchanged. Neither control is labeled newly validated.

The one adjustment moves formatting and checks from file segments to bounded contract batches. A regression verifies that 40 rows across 20 files require six formatting calls with a seven-row bound, while each file retains CDC sequences 1 and 2. Mixed initial and daily Trade versions, global cash IDs across cutover, duplicate keys across batches, and invalid fields before file writes have separate regressions.

The adjusted candidate uses 903,962,624 sampled peak RSS bytes and 208,510,380 sampled peak temporary bytes during generation. Both controls and candidates remain below their guards without observed swap. The large control-time variation prevents a precise speedup estimate; Windows host load was not measured. The adjusted candidate is faster than both observed controls, and the replacement removes custom catalog and merge mechanisms. These bounded results do not establish full-calendar or factor 100 performance.

Evidence lives under `local/tmp/generator-whole/measurements/{improved1,candidate1,improved2,candidate2}/`; outputs live under `local/runs/generator-whole/`. The frozen improved control and source hashes remain under `local/tmp/generator-whole/baseline/` and `baseline-snapshot.json`. Design comparisons remain in `local/tmp/generator-whole/design-*.md`.

### Resource and Test Evidence

An actual fresh-process native spill probe processes 65,536 rows with about 64 MB of payload and a 1 MB out-of-core threshold. It observes 86,674,152 spill bytes and delivered batches no larger than 4,096 rows. With a 1 MB spill-disk budget, the query fails, retains the sealed input, and produces no final XML file or dataset manifest. Evidence is in `local/tmp/generator-whole/backend-probes/`.

Polars 2.0.0's batch iterator has no close or stop method in this environment. The backend therefore uses synchronous batch callbacks, stops on a captured failure, and propagates that failure. Private native settings stay in `output.py`. Spill budgets and sampled RSS guards are not a hard memory guarantee; population arrays, live business state, and native work also consume memory.

The integrated generator and publisher suite passes 435 tests with 188 Pandas dependency warnings in 117.66 seconds. The command is the same isolated-SDK command above; its log is `local/tmp/generator-whole/full-suite.log`. The changed Python files have module, class, and function docstrings. Three independent cross-file reviews cover all changed source and test files against base `352fac1`, through source freeze `682659c`. They report no material standards or specification findings. Reviewers checked callback failure propagation, partial-run retention, group completion, and publication after validation as well as source semantics.

The final-run wrapper's six tests cover explicit authorization, changed-source rejection, actual file counts and coverage, XML record counting, and ordered typed hidden comparisons. The authorized full-calendar run uses source digest `8820b38a0ceb9f77e77b54634152af1031d94bb73f47bda36dccfd915aa0a4cf`. Its output is `local/runs/generator-whole/final-factor1`; evidence is under `local/tmp/generator-whole/final/`.

### Final Typed-Runtime Factor 1 Result

Exactly one full-calendar candidate run completed from source freeze `682659c`. The normal public generator passed full saved-file validation and renamed the dataset. The measurement wrapper exited 0 with no failures, then independently counted and rehashed all 18,051 source files against `factor1-3601-calibrated`. All 23,488,236 source records, 5,574 audit rows, and eight ordered typed hidden files match. Offline bytes remain 1,438,704,592; stream bytes remain 339,074,412. The separate `business-comparison.json` also confirms identical activity summaries, excluding only `numeric_state_buffer_bytes`. Reference metadata and actual contents remain unchanged; the 29-file source digest matches before and after.

| Measure | Historical stock, calibrated anchor | Typed native export |
| --- | ---: | ---: |
| Generation phase seconds | 896.26 | 470.48 |
| Activity phase seconds | 442.52 | 369.18 |
| Final export phase seconds | 389.17 | 72.73 |
| Market phase seconds | 43.65 | 19.90 |
| Prospect phase seconds | 20.02 | 7.85 |
| Full validation seconds | 829.15 | 831.70 |
| Generator command seconds | 1,726.97 | 1,304.80 |
| Sampled runner-tree peak RSS MiB | 1,434.32 | 1,774.40 |
| Sampled peak temporary GiB | 2.585 | 3.463 |

The observed generation time is 47.5% lower and command time is 24.4% lower than the historical stock run. Validation time is essentially unchanged. These are single-run historical comparisons, not a controlled estimate of the native replacement's isolated gain. The improved text control has no full-calendar measurement; its comparison belongs to the bounded table above. Validation remains a separate required phase, not a performance option removed by this change.

The measurement wrapper brings sampled peak RSS to 1,909.45 MiB. Total sampled run disk peaks at 4.623 GiB. No swap use or resource-guard failure was observed. Native export reports 2,879 delivered batches, maximum buffer, frame, and stream-batch sizes of 8,192 rows, 16,731 fragments, and 1,823,415,364 sealed bytes. Its spool, seal, and sort-write totals are 67.76, 8.77, and 70.42 seconds; process totals overlap generation phases and must not be added to elapsed generation time. The manifest retains phase user and system CPU measurements.

The implementation adds a net 528 production Python lines against base `352fac1`, excluding tests and documentation. The added typed schemas, native checks, and failure handling replace custom catalog and merge code rather than retaining a second production exporter. It adds no dependency. Remaining Python work includes causal execution, sparse state updates, customer and account field helpers, XML and FINWIRE encoding, selected-error audit dictionaries, and file dispatch. Activity is the largest phase, but it includes recording and temporary writes, so its timing does not isolate the executor.

The final report is `local/tmp/generator-whole/final/run/measurement.json`; it links the candidate manifest, samples, source inventories, reference checks, and command log. The independent business comparison sits beside it. The full-suite spill evidence for this source freeze is preserved under `local/tmp/generator-whole/backend-probes/final-freeze/`. Factors 5 and 100 remain unmeasured. Native spill and bounded output batches do not establish a hard total-memory bound or large-factor throughput; population and hidden-state arrays remain part of the resource requirement.

### Independent Review and Proposed Next Work

A new review session read the diff `352fac1...7705d88` (production commit `682659c`, documentation commits `6a4e64e` and `7705d88`) and the enclosing functions. Two read-only helper reviews covered the Standards and Spec axes separately; the review session checked their line references and owns this synthesis. No code, test, dependency, or dataset changed, and no generation, validation, or test run occurred. One helper ran a small `/tmp` probe that confirms a `True` callback result stops a Polars 2.0.0 sorted `sink_batches` query after one batch.

**Spec axis.** No material finding and no integration blocker. Sort keys match the deleted key strings for every family. CT_ID runs over initial cash, then daily streams in path order. CDC_DSN restarts per daily file and carries across batches. Trade versions and channel draws use the delivery date. Selection draws, incorrect values, and audit units for data errors match the scalar rules. Group markers, scheduled-group discovery, partial retention, and publication after validation hold. Four low notes remain:

| ID | Where | Note | Origin |
| --- | --- | --- | --- |
| P1 | `output_formats.py:190-191` | Integer numerics format as cents only when the scale is 2. A future field with another scale would print a plain integer, and the lexical check would accept it. No current field is affected; `TX_RATE` stays text. | Introduced, latent |
| P2 | `output.py:334-342`, `396` | CT_ID order and endpoint-only Trade version splitting depend on `YYYYMMDD` paths sorting by date. A changed path layout would misorder cash IDs. | Introduced, latent |
| P3 | `runner.py:291-312` | Manifest and summary drop four old export keys and add `export` and `phase_cpu_seconds`. No repository code reads the old keys. | Introduced |
| P4 | `config.py:259` | The ignored `merge_fan_in` value still must lie between 2 and 64. | Preexisting check |

**Standards axis.** No missing file descriptions or docstrings. Findings are low impact; S1 and S2 break documented rules, the rest are judgment calls:

| ID | Where | Finding | Kind |
| --- | --- | --- | --- |
| S1 | `activity.py:753`; `output.py:76`, `138`, `308`; `output_formats.py:67`, `114`; `recording.py:151` | Comments and docstrings describe the removed design or editing history ("as before", "the old key", "without creating a tiny DataFrame"). `AGENTS.md` asks for lasting behavior instead. | Documented rule |
| S2 | `recording.py:201`, `216`; `formats.py:116` | `xml_action` and `finwire` keep unused `key` and `relative` parameters; `layout` defaults to `None` but is required. `Source.check_payload` has only a test caller. | Documented rule (clear names) |
| S3 | `recording.py:39-58` and `70-178`; `output.py:332`, `recording.py:166`, `formats.py:128`; `activity.py` six epoch-second sums | One rule lives in several places: scalar and batch error selection (the scalar Customer and Trade branches no longer serve production), the Trade channel date cut, cents formatting, and epoch-second arithmetic. A rule change needs coordinated edits. | Judgment |
| S4 | `output.py:119-196`, `219-221`, `455` | `TypedOutput` holds buffering, spooling, sealing, discovery, sequencing, formatting, and writing. It calls `Generation` private members and sets `owner.recording_summary`. Cash and Trade special cases recur in five methods. `_metadata` returns a positional tuple that `record_batch` fills with dummy order values and slices. | Judgment |
| S5 | `output.py:53`, `79`, `write_family` | "Delivery" names one file; the glossary uses it for the initial load or one daily delivery. | Glossary |
| S6 | Generator README, Data Platform section 2 | "Historical text" rows and "compared with the historical 1,726.97 seconds" compare the design with code that no longer exists. The Documentation Guide keeps such comparisons in Status and records. The calibration rows themselves remain valid dataset evidence. | Judgment |
| S7 | `output.py:20-29`, `400-412` | Budget arithmetic in `native_environment` and the ASCII-tuple order check in `consume` lack explanations. | Judgment |

The earlier cross-file reviews reported no material standards findings. That conclusion holds for correctness, but S1 and S2 are concrete breaches they did not report.

**Coherence.** The selected design has one producer boundary (`record`, `record_batch`, `finish`), one ordering mechanism, and one writer lifecycle. That is a real reduction in coordination mechanisms compared with catalog, spools, fan-in, heap, and adoption. Its main readability cost is S4: one large class with back-references. Splitting part writing from publication and moving the Cash and Trade rules into one family policy would follow the owner's definition of simplicity, but it does not block integration. The validator gained independence: the exporter now checks fields with native expressions, while the validator keeps the Python `Field.check` oracle.

**Measured runtime map.** The full-calendar command takes 1,304.80 seconds: validation 831.70 (63.7%), activity 369.18 (28.3%), export 72.73 (5.6%), market 19.90, and prospects 7.85. Activity uses 666.22 CPU seconds over its 369.18-second wall time, about 1.8 busy cores across two processes and four customer groups. Validation uses 1,028.06 CPU seconds over 831.70 seconds, so it runs mostly on one core. Export uses 144.07 user and 112.99 system CPU seconds; the system share is unexplained. Cumulative spool time is 67.76 seconds across all producers, so activity also contains recording frames and Parquet writes. Without validation changes, command time cannot fall below about 832 seconds.

The run covers 17,707,514 customer-days, 29,580,640 account-days, 2,618,469 trades, 3,484,311 cash transactions, and 3,348,179 watch changes. About 56,631 watches remain active for 6,198 active customers at the end.

**Inherent order versus representation cost.** These conclusions come from reading the code; none is measured.

- Inherent: each account's cash, reservations, settlements, and holdings run in time order, with same-second tie rules and holding insertion order. Daily attrition reads realized returns. Accounts in different customer groups share only read-only market data, which is why groups already run in separate processes and output does not depend on worker count.
- Representation: `_update_customers` visits every eligible customer every day with scalar draws, and `_update_watches` visits every live watch every day. If the end-of-run average of about 9 live watches per customer held throughout, that loop would run about 160 million times. `_plan_due_orders`, `AttritionPlanner.intents`, and `AttritionPlanner.after_day` also scan all open orders or holdings in Python every day. Other costs are Python objects for each intent and order, scalar draws and structured-array access for each event, tuple building for each record, and in the validator, per-row dictionaries, `_typed`, and `_source_schema` rebuilt for every row.
- Validation: replay partitions are independent by account or customer ID. Customer replays write lifetimes that account replays read, so the partitions can run as two parallel waves. The parse pass carries global checks (CT_ID sequence, cash, market, and initial trade order) that need ordered boundary checks if files are parsed in parallel.

**Compiled-kernel readiness.** The new boundary avoids per-event Python callbacks: `record_batch` accepts aligned arrays, order columns, and `_profile`. A kernel would call it once per family and day, split by CDC flag (or the interface gains a `_flag` column), and chunk at `export_rows`. The larger interface work lies upstream: `allow_trade`, `allow_cash`, `observe_cash`, `record_outflow_candidate`, and leaving intents are planner calls inside execution. A kernel needs them as daily leaving masks, an external-flow accumulator, and an outflow-event array. Orders and holdings need array forms that keep insertion order and first origin. Numba and Cython are not installed; GCC is. Support for Python 3.14.3 and NumPy 2.5.4 is unverified, and exact floating-point operation order must be proven.

**Proposed plan.** This is a proposal only; no item is accepted or authorized.

| Rank | Work | Measurable question | Smallest evidence | Gate |
| --- | --- | --- | --- | --- |
| 0 | Pre-merge cleanup | Do S1, S2, and P1 close without behavior change? | Comment, signature, and scale-guard edits; targeted tests; source bytes untouched | Owner chooses before or after integration |
| 1 | Attribution measurement | What exclusive CPU share goes to day scans, proposal expansion, ordered execution, record calls, and spooling? What wall share goes to parsing, fault restore, staging, market coverage, customer replay, account replay, and hashing? | Scratch timers and call counts, no cProfile. One full-calendar run that stops after activity, plus one validation pass over a hard-linked copy of `final-factor1` | Instrumented phases within 10% of 369.18 and 831.70 seconds, or report shares only |
| 2 | Parallel scheduling | How much wall time do more activity workers and two parallel validation replay waves save, at what RSS per process? | Raise the two-worker cap; process-pool replay calling the unchanged partition functions. Bounded comparison first | Identical sources, audit, hidden files, summaries, and validation report; RSS within the owner's budget |
| 3 | Validation parse cost | Does removing per-row schema and type resolution cut the parse share? | Stage-local conversion plans from the existing prototype (21 lines, 2,173 differential cases). File-parallel parsing only if parsing still dominates | Corruption matrix and audit tests unchanged; validator imports nothing from `output.py` or `output_formats.py` |
| 4 | Activity day scans | Does indexing watches by due day, drawing customer updates in arrays, and avoiding daily full scans reduce activity CPU? | Python and NumPy only, no dependency | Byte-identical bounded output, then one authorized full-calendar confirmation |
| 5 | Compiled financial core | Is ordered execution still a large share after items 2 to 4? | Design note on the interface changes above and a dependency check | Only with owner approval of a dependency or build step |
| 6 | Larger-factor readiness | Do validation partition math and the global family sort scale? | Code review plus a factor 5 run when authorized | `_row_group_rows` falls to about 32 rows at a projected 8,000 validation partitions; export sorts each family globally with a 256 MB engine budget |

Item 1 comes first because the activity phase mixes execution with recording, and the only validation profile is a bounded, heavily instrumented run (`local/tmp/generator-perf/profile/Report.md`). Items 2 and 3 address the largest phase without touching the validator's checks. Item 4 changes data structures, not business order. Item 5 waits for evidence. Business rules, named random keys, integer-cent arithmetic, source contracts, the typed output path, `Field.check`, `FaultAudit`, and the validator's independent replay stay. Daily full scans, per-row schema rebuilding, and the fixed two-worker ceiling would go.

Owner decisions: authorize item 1 (full calendar, or the cheaper bounded calendar with different phase shares); choose item 0 timing; set an RSS budget for more worker processes; confirm that scheduling and caching changes keep the validator independent while native lexical checks in the validator stay out; defer item 5.

### Implemented Runtime Work and Larger-Factor Runs

The owner accepted the plan and asked for all items. ChatGPT 6 Sol (`ncdg/gpt-6.1-sol`, xhigh) workers implemented them under the review session, in separate worktrees, with a shared lock for timed runs and an 8 GiB process-tree RSS budget. Scratch evidence is under `local/tmp/generator-next/`; each track has a `report.md`.

| Item | Commits on `perf/generator-whole` | Result |
| --- | --- | --- |
| 0, cleanup | `8b80ba3` | Comments, unused parameters, numeric-scale guard, glossary wording, and public-doc comparisons fixed. Bounded output identical to `candidate2`. |
| 1, attribution | none (scratch) | Instrumented shares only; timers exceeded the 10% overhead gate. Activity: customer-update subtree 31.83% of group CPU, with 160,612,858 watch-loop visits; ordered execution 36.11% without recording. Validation: parsing and per-file checks 69.85% of wall time, account replay 16.55%, staging 9.89%. |
| 2 and 4, activity | `9e5cb20`, `50fd30e`, merge `62d744b` | Arrays for customer-update and watch-count draws, watch due-day index, order indexes, NumPy holding lookups, and a shared pool of 1 to 8 workers. Defaults: 4 workers, 1,024 customers per group, 8,192 MiB guard. Bounded activity 20.29 s to 8.74 s; identical output at 1, 2, 4, and 8 workers. |
| 2 and 3, validation | `05509c7`, `21ddefe`, `3b1ccb1`, `5cb02cd`, `5b18ede`, merge `77cc5cd` | Ordered parallel replay waves, stage-local conversion plans, a 1,024-partition ceiling with byte-bounded row groups, file-parallel parsing, and one streaming market-price join that builds only the fixed-size market side. Full-calendar validation of `final-factor1` at 4 workers: 386.11 s versus 831.70 s, with an identical report. The file parser alone saves 203.83 s (29.62%); the join saves 98.19 s. |
| 5, compiled core | none | Note `local/tmp/generator-next/attribution/compiled-core.md`: deferred. Numba 0.68.1 and Cython 3.3.1 declare support for Python 3.14 and NumPy 2.5.4 according to their primary sources; nothing is installed. |
| Key fix | `1632206` | Preexisting CustomerMgmt sort-key tie, described below. |

**Combined factor 1 run** (`77cc5cd`, source digest `c12e8f34f2ebbed08f696810a4f57c5a9e667217dfa256820566056372035c27`, 4 workers, groups of 1,024). All 18,051 source files, 5,574 audit rows, canonical typed hidden records, and the business summary match `final-factor1`. Fourteen summary fields are excluded with both values recorded: runtime metadata, eight runtime settings, `numeric_state_buffer_bytes`, `customer_groups`, and three per-group peaks.

| Phase seconds | Previous typed runtime | Combined run |
| --- | ---: | ---: |
| Activity | 369.18 | 143.60 |
| Export | 72.73 | 67.15 |
| Validation | 831.70 | 404.00 |
| Command | 1,304.80 | 649.94 |

Command time falls 50.19%. Sampled peak RSS is 2,360.77 MiB for the generator tree and 2,559.74 MiB with the wrapper; sampled peak disk is 4.499 GiB. Evidence: `local/tmp/generator-next/final/report.md` and `factor1-run/`; dataset `local/runs/generator-next/final/factor1`.

**First factor 5 run: failed.** Export stopped after 803.78 seconds with `Nonunique or unordered source key in CustomerMgmt`. Two accounts of customer 6687 received their first update on 2015-10-01, so both `UPDACCT` actions had the key time/customer/1/`UPDACCT`. Base `352fac1` used the same key, so the defect predates this work and was simply unreached at factor 1. Commit `1632206` appends the single-account `CA_ID` as a final tie-breaker; factor 1 and bounded bytes stay identical. A key review of every source family (`order-key-audit.md`) found no other reachable source-key tie. The failed partial remains at `local/runs/generator-next/final/factor5.partial`.

**Factor 5 rerun: passed** (`1632206`, digest `d67a7b9e975e54e8ba0c233574f5be1eb6b1b9e2a786fbdbada5f50cb2d79548`). The public command exits 0 after full validation and renames the dataset to `local/runs/generator-next/final/factor5-rerun`; independent rehashing and coverage checks pass for all 18,051 files.

| Measure | Factor 5 |
| --- | ---: |
| Command seconds | 2,566.71 |
| Activity seconds | 706.47 |
| Export seconds | 302.18 |
| Validation seconds | 1,495.56 |
| Offline bytes | 5,568,776,184 |
| Offline records | 77,852,914 |
| Stream bytes | 1,694,595,301 |
| Stream records | 11,900,035 |
| Audit rows | 25,077 |
| Sampled peak RSS with wrapper | about 2,736 MiB |

Offline bytes are 1.75% above the planning projection of 5,472,843,872. Validation is 58% of the factor 5 command. From factor 1 to factor 5, activity grows 4.9 times, export 4.5 times, and validation 3.7 times. A linear extrapolation of these increments to factor 100 gives roughly 13 to 14 hours with validation, about 8 of them in validation; this is an inference, not a measurement, and the 7,200-second default timeout would need a per-run override.

**Open issue found during the key review.** Error-audit identities are not unique in two reachable cases: a daily `NEW` and a same-day `UPDCUST` for one customer, and several initial CMS account actions for one customer at one timestamp. The estimated chance that a run hits the first case is about 0.8% at factor 5 and 14.6% at factor 100. A collision fails the run rather than corrupting data. Evidence: `reachable-audit-tie.json` and `report.md` under `local/tmp/generator-next/final/`.

**Second phase (owner-approved, running).** The audit identity fix and parallel fault restoration during validation (`perf/generator-audit`); validation on by default with an explicit skip and an unvalidated label that the publisher refuses without an override, plus resumable runs with code- and configuration-checked checkpoints (`perf/generator-resume`); then a structural cleanup of `TypedOutput` and duplicated rules; then a factor 100 estimate and a short-calendar factor 100 scale check. The full factor 100 run and a compiled core each wait for the owner.

## Full-Calendar Factor 1 Runs

Both runs use seed `20261029`, two workers, two Polars threads per worker, 1,250 companies, and 2,000 securities. Their configurations differ only in `customers_per_unit`: 3,300 for `factor1-3300-first` and 3,601 for `factor1-3601-calibrated`. The initial-load period is 2014-01-01 through 2018-12-31; daily delivery covers 2019-01-01 through 2025-12-31. Each runner completes generation and full validation before the final dataset rename.

| Measure | First run, anchor 3,300 | Calibrated run, anchor 3,601 |
| --- | ---: | ---: |
| Initial offline bytes | 448,959,525 | 474,035,639 |
| Daily offline bytes | 897,733,975 | 964,668,953 |
| Total offline bytes | 1,346,693,500 | 1,438,704,592 |
| Fixed offline bytes | 430,169,772 | 430,169,772 |
| Variable offline bytes | 916,523,728 | 1,008,534,820 |
| Stream bytes | 307,655,539 | 339,074,412 |
| Offline records | 19,806,282 | 21,078,293 |
| Stream messages | 2,189,385 | 2,409,943 |
| Generation phase seconds, excluding validation | 954.77 | 896.26 |
| Validation seconds | 753.54 | 829.15 |
| Generator command wall seconds, including validation | 1,710.06 | 1,726.97 |
| Sampled aggregate peak RSS MiB | 1,374.79 | 1,434.32 |
| Sampled peak temporary GiB | 2.426 | 2.585 |
| Realized cutover customers | 3,278 | 3,597 |
| Realized cutover accounts | 5,488 | 6,028 |

First-run evidence is in `local/tmp/overnight-generator/measurements/factor1-3300-first/measurement-reconciled.json`; the dataset is `local/runs/overnight-generator/factor1-3300-first/`. Its dataset ID is `97364be037ab6c2e17c22390d31e8d00ff908ec80cfbdcd05b041bf1dabcfcf4`. Before and after digests match across 28 generator source files: `5b8edd1810090e946b7aeb7179417d1ee43c0f3c89ec2207d4c3030fed55255c`. The recorded commits differ (`b377ca9` before, `a310923` after) because documentation and test commits do not change that source digest.

Calibrated evidence is in `local/tmp/overnight-generator/measurements/factor1-3601-calibrated/measurement.json`; the dataset is `local/runs/overnight-generator/factor1-3601-calibrated/`. Its dataset ID is `18be6d78bb6e42b30bf1b08c036eb8ea57d917bb247896fa4e4fa30910a50947`. Its 28-file source digest is unchanged before and after: `0c7ed430eb023b3e28799b98ceeb99bd7c4f6edc4e8345c57fa4ac364cc77a31`. The recorded commits are `76081d9` before and documentation-only `4e627a3` after. The wrapper exits 0, reports no failures, and passes all 21 source cross-checks. No source changes occur during either measured run.

The original measurement wrapper exited 1 because source descriptions use `Trade settlement` while the activity summary calls that category `Settlement`. The generator and signal-check commands both exited 0. Separate analysis normalized the category label and reconciled the saved records with the summary. It did not regenerate data, rerun the signal check, or change any count. The original `measurement.json` remains unchanged; the reconciled report has no failed cross-checks.

The runtime is Python 3.14.3, NumPy 2.5.4, Polars 2.0.0, exchange-calendars 4.13.2, psutil 7.2.2, and PyYAML 6.0.3. BLAS and OpenMP thread limits are one; `POLARS_MAX_THREADS` is two. RSS sampling covers the supervisor and descendants every 0.1 seconds. Disk sampling occurs every two seconds, so the reported peaks are sampled observations, not exact instantaneous maxima.

## Behavior, Source Recording, and Signal Evidence

Four fixed security-selection slots constrain each account's repeatable buy universe; duplicate selections can reduce its distinct securities. They are not a limit that permits unrestricted security replacement after a sale. The dividend-paying share remains 40%. The first run's combined cash mix is 68.9982% settlements, 16.3917% deposits, 8.6178% dividends, and 5.9923% withdrawals. The calibrated mix is 69.189662% settlements, 16.302305% deposits, 8.572914% dividends, and 5.935119% withdrawals. Calibrated stream-only settlements are 69.179852%, and dividends are 8.924983%. The old 85% settlement estimate is not an acceptance target and is not the observed mix.

The provisional proposal multiplier `2.12` comes from a small-market pilot that executed 100 of 212 proposals: `212 / 100 = 2.12`. That pilot predates removal of hazard protection, so it does not calibrate the final unprotected model by itself. The intended confirmed annual rate remains 0.02; raw opportunity intensity is `0.02 * 2.12 = 0.0424`. Keep this provenance separate from the measured confirmed rate.

| Outcome | First run, anchor 3,300 | Calibrated run, anchor 3,601 |
| --- | ---: | ---: |
| Annual account-closure rate | 2.96528% | 2.925087% |
| Annual customer-inactivation rate | 2.06983% | 2.027576% |
| Actual closed accounts | 2,199 | 2,369 |
| Warning plans matched to actual closed accounts | 1,764, or 80.2183% | 1,891, or 79.82271% |
| Planned warning length | 7 to 56 days | 7 to 56 days |
| Median planned warning length | 30.5 days | 30 days |
| Look-alike proposals | 3,189 | 3,489 |
| Executed outflow candidates | 1,509 | 1,636 |
| Confirmed look-alikes | 1,498 | 1,620 |
| Closing candidates | 0 | 0 |
| Censored candidates | 11 | 16 |
| Fully observed account-day exposure | 26,803,706 | 29,271,599 |
| Annual confirmed look-alike rate | 2.04126% | 2.021389% |

Warning-plan counts use the final hidden plan row matched to an actual closed account. The lengths and steps are scheduled behavior, not proof that sales executed; an account without assets can have a warning plan but no sale. Outflow candidates require an executed sale and a positive withdrawal, unlike the warning-plan metric.

The confirmed rate uses account-days whose next 30 calendar days lie within the run. The final model has no immunity period. Zero observed closing candidates does not establish a zero probability of closure. All actual closures match hidden plans in both runs. At observation end, 18 final plans remain unclosed in the first run and 19 in the calibrated run.

The first audit contains 5,073 selected field errors: 20 tier errors, 16 date-of-birth errors, 633 credit-rating errors, 2,220 charge errors, and 2,184 commission errors. The calibrated audit contains 5,574: 19 tier errors, 13 date-of-birth errors, 633 credit-rating errors, 2,456 charge errors, and 2,453 commission errors. Initial trade records contain 14 business fields. The calibrated daily records contain 190,683 version 1 rows and 3,400,867 version 2 rows; the latter add `T_CHANNEL` according to delivery date. Audit files and hidden state stay outside ingestion.

Both signal checks sample 512 accounts, use chronological splits, and leave 30-day label gaps. The first test average precision is 0.157326 for the model and 0.005548 for the outflow rule; rule precision is 0.9024%, with 57,401 observations and 156 positive labels. The calibrated result is 0.228498 for the model and 0.005527 for the rule; rule precision is 0.9052%, with 55,621 observations and 162 positive labels. Both pass the synthetic signal gate. Neither establishes practical deployment precision or downstream model quality.

## Performance Investigation

The owner ended the first performance round on 2026-11-06 after profiling and scratch comparisons, then authorized only the focused alternative comparison below. No production optimization shipped, and no new full-calendar run occurred. The existing calibrated dataset remains unchanged.

The bounded workload uses factor 1, the 3,601-customer anchor, seed `20261029`, 1,250 companies, 2,000 securities, two workers, and two Polars threads. Initial load covers 2018-10-01 through 2018-12-31; daily delivery covers 2019-01-01 through 2019-07-31. Both unprofiled baselines pass full validation and take 178.79 and 170.68 seconds. All 1,515 source files and the separate error audit match across those runs and the profiled run. The bounded workload has more fixed data and shorter account histories than the full calendar.

Actual child-process profiles identify repeated field checks, schema construction, and per-file SQLite commits as larger costs than merge sorting. Native relational preparation is a small part of validation. The recommended direction is targeted reuse of immutable contracts and schemas, plus fewer catalog commits, rather than a broad engine replacement. This is a recommendation, not an implemented design.

These scratch exporter measurements replay the same captured inputs. They exclude business simulation and full saved-file validation. Each completed replay produces the same 1,515 source files, 2,057,139 rows, and 150,184,502 bytes, with matching hashes.

| Scratch treatment | Command wall seconds | Final-write seconds |
| --- | ---: | ---: |
| Stock exporter | 89.86 | 59.03 |
| Compiled field contracts | 80.26 | 49.27 |
| Batched output writes | 78.90 | 48.58 |
| Compiled contracts and deferred cleanup | 42.17 | 14.23 |
| Compiled contracts, deferred cleanup, and batched writes | 39.27 | 14.01 |

Each treatment has one completed replay, without the planned closing stock repeat. Background development activity and commit latency vary, so these observations do not establish a precise speedup or full-calendar throughput. The combined candidate's sampled total output storage increases from about 200 to 328 MiB because consumed runs remain until close. Sampled temporary storage stays near 198 MiB, peak RSS stays near 160 MiB, and the 8,192-row buffer and 16-input path bounds remain unchanged. The separate captured trace occupies about 411 MiB and is not included in those output-storage figures.

Compiled-check probes compare 476,495 edge and malformed field strings with the original checker. Deferred-cleanup probes cover catalog consistency around commit, deletion, and failed final checks. A separate stage-local schema-cache prototype passes 2,173 differential row cases but has no completed runtime measurement. At the end of that round, Polars and DuckDB exporters had small feasibility checks only. No production code was removed. Production integration and final full-calendar verification remain outside the later bounded continuation.

Evidence remains in ignored `local/tmp/generator-perf/profile/Report.md`, `local/tmp/generator-perf/export-options/measurements/`, and `local/tmp/generator-perf/validation-options/`. New datasets are under `local/runs/generator-perf/`. The prepared full-calendar wrapper was not executed.

### Focused Alternative Comparison

The continuation compares the improved text exporter with Polars and DuckDB using the same captured inputs, compiled source checks, safe deferred catalog cleanup, and no byte batching. Each serial replay has a 300-second ceiling, a 4 GiB aggregate RSS allowance, and a 16 GiB output-plus-trace disk allowance. All use two compute threads, an 8,192-row buffer, and a 16-input merge bound. Native adapters receive the same catalog improvement because they retain SQLite; the comparison does not leave the known per-file commit cost untreated.

| Candidate | Complete replay seconds | Final-write seconds | Peak RSS MiB | Peak temporary MiB | Peak total output MiB | Exact source equality |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Improved text, opening | 30.87 | 11.57 | 158.34 | 198.23 | 317.25 | Yes |
| Polars | 34.78 | 13.92 | 659.49 | 211.98 | 331.93 | Yes |
| DuckDB | 35.76 | 14.60 | 351.28 | 215.30 | 350.48 | Yes |
| Improved text, closing | 28.41 | 10.68 | 159.19 | 198.70 | 325.95 | Yes |

Every replay rehashes and matches all 1,515 source files, 2,057,139 rows, 150,184,502 source bytes, and the fault audit. Complete time includes trace decoding, ingress, encoding, temporary writes, compaction, adoption, ordering, final checks, sequence assignment, output writing, hashing, and comparison. It excludes business simulation and saved-file validation. DataFrame calls replay as bounded row iterators in all candidates. The closing text repeat checks timing variation because the first Polars gap was only 3.91 seconds. Both native runs remain slower than either text run; these few samples do not establish a precise speed ratio.

Temporary disk includes the catalog, temporary sorted files, and engine spill. Total output disk includes final files; the fixed trace adds 411.49 MiB to each budget. Peaks are sampled, and no budget guard fired. WSL CPU busy averaged 5.3 to 6.0 percent across 20 logical CPUs; available memory stayed above 19.8 GiB. Windows-side load was not measured. Native engines had a 128 MB memory threshold and 1,024 MB spill allowance, separate from the process-tree RSS guard. Sampled spill was zero, so larger-than-memory behavior remains unmeasured.

All candidates retain the SQLite catalog, fan-in tree, adoption, Python encoding, and final Python checks and transformations. Native adapters replace text temporary files with Parquet String columns and re-sort selected inputs through native queries. They do not remove the catalog machinery. Measured scratch sizes are 225 lines for existing `sorting.py`, 100 for the shared compiled-check experiment, 101 for the shared deferred-cleanup class, 60 additional lines for Polars, 49 further lines for DuckDB, and 12 for the comparison's composition classes. These counts include comments and copied code, not a production diff. Estimated production additions are 40 to 60 net contract lines and 10 to 25 cleanup lines; this Polars approach adds about 10 to 20 further lines, or 30 to 50 for DuckDB. No production simplification was implemented or measured by line deletion.

The recommendation is the improved text approach: it has the lowest measured time and memory, adds no dependency, and avoids another engine lifecycle. Polars adds streaming and private spill-setting reliance; DuckDB adds a package, connection handling, and version-specific batch calls while still using Polars to write temporary files. All reuse duplicate rejection and safe final cleanup. The continuation adds only a small composition-equivalence check, not native failure or spill stress testing. This result applies to these sorting and temporary-file substitutions inside the existing exporter; it does not show that a different engine-native architecture cannot reduce custom code. No redesign or production integration is authorized by this comparison.

The concise report is `local/tmp/generator-perf/export-options/focused-results.md`; it links four raw measurement records, load snapshots, code counts, and the composition check. All experiment processes finished, with none queued. Shared packages, existing datasets, and platform services remain unchanged.

### Combined Column-Based Prototype

The owner next authorized a compact generation-to-export investigation, not production integration. The partial Polars path connects NumPy columns to one bounded batch, native integer-cent and identifier formatting, checked ordering, unchanged compiled field checks, and native CSV output through a binary hashing sink. DailyMarket and Prospect already arrive in delivery and key order, so their records bypass the SQLite catalog, sorted text files, and custom merge tree. Batch-boundary checks reject duplicates or decreasing keys. Other sources and ordered financial execution remain on improved text.

This path covers 221 files, 1,017,770 rows, and 61,326,840 bytes: 49.48% of rows and 40.83% of bytes in the bounded workload, not the full calendar. It removes covered row dictionaries, per-row money strings, payload joins, and payload reparsing. About 7.67 million independent field checks remain in column loops, including delimiter checks before unquoted CSV output. No validation rule is removed.

One serial pair measures actual generation through export, not trace replay. Both use the same compiled checks and safe text cleanup, two workers, two threads, a 4 GiB aggregate RSS limit, a 16 GiB disk limit, and a 300-second generation ceiling.

| Measure | Improved text | Partial Polars |
| --- | ---: | ---: |
| Generation through export, seconds | 41.870 | 38.552 |
| Market, seconds | 6.954 | 5.834 |
| Ordered financial activity, seconds | 21.915 | 22.242 |
| Prospects, seconds | 1.113 | 1.570 |
| Final export, seconds | 11.021 | 8.041 |
| Generation peak aggregate RSS, MiB | 710.81 | 733.47 |
| Generation peak temporary disk, MiB | 241.82 | 159.13 |
| Generation peak total disk, MiB | 373.63 | 268.99 |
| Separate saved-file validation, seconds | Not rerun | 56.386, passed |

The measured generation difference is 3.32 seconds, or 7.9%. Final export alone overstates the benefit because covered checks move into the market and prospect phases; prospects become slower. Mean WSL CPU busy is 7.34% and 7.78% across 20 logical CPUs. Windows load is unmeasured, and one pair does not establish a repeatable percentage gain. RSS and disk are sampled every 0.1 and 2 seconds. No resource guard fires. The native batch peaks at 2,048 rows against the 8,192-row allowance, with the legacy coordinator buffer empty before allocation. No native batch remains during financial-worker execution.

Both outputs match all 1,515 source paths, counts, bytes, and hashes, with independent rehashes and an exact 441-row audit comparison. The prototype passes the existing saved-file validator once, including global cash IDs, financial replay, data-error evidence, and both dated Trade schemas. A small interface check matches 43 files and rejects five delimiter, width, duplicate, and ordering cases. Empty files and CDC sequences across chunks remain exact.

The improved run's harness exits after generation and successful source comparisons because an optional activity-summary comparison compares integer dictionary keys with JSON string keys. Original logs and exit status remain intact; a supplemental record rehashes the saved files without rerunning generation. That dataset is explicitly `generated-unvalidated`, with exact equality to the already validated reference. The prototype normalizes only this post-timing comparison, then passes it and validation. This reporting defect does not alter source bytes or measured generation time.

Recommend against this partial production integration for the observed gain. It reduces disk use and proves the column interface works, but adds a second export path while retaining the entire 225-line sorting module. The scratch runtime is 297 physical lines, including 132 copied producer lines, plus an 18-line binding hook. A direct integration would add an estimated 80 to 130 net maintained lines; no production deletion was demonstrated. Polars is already a dependency. The scratch overrides replace only `Market.build`, `generate_prospects`, and `Generation`, with the same compiled checks and deferred text cleanup in both modes. Financial execution, faults, cash IDs, Trade versions, XML, and FINWIRE stay unchanged. A complete column-native architecture remains unproven and was not implemented.

Evidence is in `local/tmp/generator-perf/columnar/results.md`, `results.json`, `comparison-data.json`, and `patch-manifest.json`; the manifest records exact overrides and file hashes. Datasets are under `local/runs/generator-perf/columnar/`. Production source hashes remain unchanged. All planned work finished; no experiment remains active or queued, and no further run or production rewrite is authorized by this result.

## Calibration and Remaining Evidence

Calibration subtracts the directly measured fixed families: `DailyMarket`, FINWIRE, `Date`, `Time`, `Industry`, `StatusType`, `TaxRate`, and `TradeType`. HR is variable because staffing scales with the firm. Decimal half-up rounding of `3300 * 1,000,000,000 / 916,523,728` gives the accepted default of 3,601 customers per unit. The repeat keeps the seed, market, and behavior controls unchanged. Its source freeze is `76081d9`, which includes the fixture tool fix and explanatory comments as well as the new default.

The repeat measures 1,008,534,820 variable offline bytes, or 8,534,820 bytes above the target (+0.853482%). The accepted default remains 3,601; the report's mechanical suggestion of 3,571 for another iteration is not applied. No further calibration or run is authorized. A factor 100 estimate uses the measured slope, `430,169,772 + 100 * 1,008,534,820 = 101,283,651,772` offline bytes, and `100 * 339,074,412 = 33,907,441,200` stream bytes. These are projections, not factor 100 measurements.

The historical original stage 1 measurement in the generator guide is separate evidence: it predates attrition and source-recording stages. It must not be substituted for either full-stage factor 1 run. The scoped implementation and factor 1 verification are integrated into `infra/compat-test`. The lead reran the full generator and publisher suite in the combined checkout; all 394 tests passed. The owner walkthrough remains pending. All larger-factor byte and runtime projections remain estimates.

Remaining evidence includes live clean-publisher integration with S3 and Kafka, the realistic publisher profile, the owner walkthrough, and larger-factor measurements. No factor 5 or factor 100 run is measured here. No generated dataset establishes the rubric's 100,000,000,000 offline-source-byte requirement. Streams, reports, hidden state, and temporary files do not count toward it. Platform compatibility, DP1, and later pipeline work remain on their separate track.
