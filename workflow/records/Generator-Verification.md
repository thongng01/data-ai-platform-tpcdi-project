# Generator Verification

This record covers the generator rebuild, clean publisher implementation, and measured factor 1 evidence on 2026-11-05. The [generator guide](../../data/data_source/generator/README.md) defines execution and controls. [Data Platform section 2](../../docs/Data-Platform.md#2-data-generation-and-problem-injection) holds the public workload estimates and measurements.

## Implementation and Checks

The generator implements stages 1 to 3. NumPy arrays hold mutable customer and account state; Polars handles export and validation. Named daily steps keep proposal selection separate from ordered settlement. Public-domain personal-data lists replace generated placeholder names. The clean publisher implements stage 4 from saved files and messages, without running the business simulation again.

| Check | Passed | Warnings | Seconds | Local evidence under `local/tmp/overnight-generator/` |
| --- | ---: | ---: | ---: | --- |
| Original stage 1 baseline | 155 | 116 | 135.48 | `supervisor/baseline-tests.log` |
| Earlier integrated suite | 325 | 172 | 143.53 | `supervisor/integrated-suite-3.log` |
| Final integrated suite at source freeze `76081d9` | 394 | 172 | 134.87 | `supervisor/integrated-suite-4.log` |
| Publisher component tests, controlled SDK adapters | 93 | 4 | 13.02 | `supervisor/publisher-tests.log` |
| Fixture refresh and pinned-format guards | 72 | 0 | 0.73 | Scoped pytest run; 22 unrelated tests deselected |

The final integrated suite reports 172 Pandas warnings. The documentation checker reports 0 issues, and the source AST check reports 0 missing module, function, or class docstrings. Both reviewers closed their assigned findings.

The test and review fixes cover the following conditions:

- Personal fields remain deterministic across scalar and array generation, seed propagation, batching, and matching prospects. Source widths and schemas remain valid.
- Customer inactivation uses mean active-account risk and an expected-trait normalizer, not future realized outcomes. Investment returns exclude external cash flows and preserve a genuine wipeout loss when equity reaches zero or becomes negative.
- Only an executed sale followed by a positive withdrawal creates an outflow candidate. Later hidden classification does not change behavior or features. Account and customer hazards continue without immunity.
- Source recording changes recorded copies only. Validation independently checks exact data-error selections, audit identities, original values, and correct settlement fees. Missing, duplicate, unused, or altered evidence fails.
- Fixture guards require the exact 13-file inventory, positive integer line counts, and nonblank records. Refresh reads and validates every bounded sample before any write. Missing, empty, or blank samples leave all existing output bytes unchanged and create no new output tree. Tests do not refresh committed fixtures.
- Publisher tests cover independent cadence and hash checks, source snapshots, ordered saved cash messages, acknowledgment checkpoints, uncertain retries, replay timing, destination binding, and secret redaction. These tests use SDK stubs, not live S3 or Kafka services.

## First Full-Calendar Factor 1 Run

The run `factor1-3300-first` uses 3,300 customers per unit, seed `20261029`, two workers, two Polars threads per worker, 1,250 companies, and 2,000 securities. The initial-load period is 2014-01-01 through 2018-12-31; daily delivery covers 2019-01-01 through 2025-12-31. The runner completes generation and full validation before the final dataset rename.

| Measure | Observed value |
| --- | ---: |
| Initial offline bytes | 448,959,525 |
| Daily offline bytes | 897,733,975 |
| Total offline bytes | 1,346,693,500 |
| Fixed offline bytes | 430,169,772 |
| Variable offline bytes | 916,523,728 |
| Stream bytes | 307,655,539 |
| Offline records | 19,806,282 |
| Stream messages | 2,189,385 |
| Generation phase seconds, excluding validation | 954.77 |
| Validation seconds | 753.54 |
| Generator command wall seconds, including validation | 1,710.06 |
| Sampled aggregate peak RSS MiB | 1,374.79 |
| Sampled peak temporary GiB | 2.426 |
| Realized cutover customers | 3,278 |
| Realized cutover accounts | 5,488 |

Evidence is in `local/tmp/overnight-generator/measurements/factor1-3300-first/measurement-reconciled.json`; the dataset is `local/runs/overnight-generator/factor1-3300-first/`. The dataset ID is `97364be037ab6c2e17c22390d31e8d00ff908ec80cfbdcd05b041bf1dabcfcf4`. The before and after digests match across 28 generator source files: `5b8edd1810090e946b7aeb7179417d1ee43c0f3c89ec2207d4c3030fed55255c`. The recorded commits differ (`b377ca9` before, `a310923` after) because documentation and test commits do not change that source digest.

The original measurement wrapper exited 1 because source descriptions use `Trade settlement` while the activity summary calls that category `Settlement`. The generator and signal-check commands both exited 0. Separate analysis normalized the category label and reconciled the saved records with the summary. It did not regenerate data, rerun the signal check, or change any count. The original `measurement.json` remains unchanged; the reconciled report has no failed cross-checks.

The runtime is Python 3.14.3, NumPy 2.5.4, Polars 2.0.0, exchange-calendars 4.13.2, psutil 7.2.2, and PyYAML 6.0.3. BLAS and OpenMP thread limits are one; `POLARS_MAX_THREADS` is two. RSS sampling covers the supervisor and descendants every 0.1 seconds. Disk sampling occurs every two seconds, so the reported peaks are sampled observations, not exact instantaneous maxima.

## Behavior, Source Recording, and Signal Evidence

Four fixed security-selection slots constrain each account's repeatable buy universe; duplicate selections can reduce its distinct securities. They are not a limit that permits unrestricted security replacement after a sale. The dividend-paying share remains 40%. The measured combined cash mix is 68.9982% settlements, 16.3917% deposits, 8.6178% dividends, and 5.9923% withdrawals. Stream-only settlements are 68.9601%, and dividends are 8.9831%. The old 85% settlement estimate is not an acceptance target and is not the observed mix.

The provisional proposal multiplier `2.12` comes from a small-market pilot that executed 100 of 212 proposals: `1 / (100 / 212) = 2.12`. That pilot predates removal of hazard protection, so it does not calibrate the final unprotected model by itself. The intended confirmed annual rate remains 0.02; raw opportunity intensity is `0.02 * 2.12 = 0.0424`. Keep this provenance separate from the measured confirmed rate.

| First-run outcome | Observed value |
| --- | ---: |
| Annual account-closure rate | 2.96528% |
| Annual customer-inactivation rate | 2.06983% |
| Actual closed accounts | 2,199 |
| Closures with warning | 1,764, or 80.2183% |
| Warning length | 7 to 56 days |
| Look-alike proposals | 3,189 |
| Executed outflow candidates | 1,509 |
| Confirmed look-alikes | 1,498 |
| Closing candidates | 0 |
| Censored candidates | 11 |
| Fully observed account-day exposure | 26,803,706 |
| Annual confirmed look-alike rate | 2.04126% |

The confirmed rate uses account-days whose next 30 calendar days lie within the run. The final model has no immunity period. Zero observed closing candidates does not establish a zero probability of closure. All 2,199 actual closures match hidden plans; 18 later plans remain unclosed at the end of observation. Plan classifications use actual outcomes, not scheduled sale dates as proof of execution.

The audit contains 5,073 selected field errors: 20 tier errors, 16 date-of-birth errors, 633 credit-rating errors, 2,220 charge errors, and 2,184 commission errors. Initial trade records contain 14 business fields. Daily records contain 172,170 version 1 rows and 3,080,607 version 2 rows; the latter add `T_CHANNEL` according to delivery date. Audit files and hidden state stay outside ingestion.

On 512 sampled accounts, chronological splits and 30-day label gaps give test average precision of 0.157326 for the signal model and 0.005548 for the outflow rule. Rule precision is 0.9024%, below the 30% ceiling. The test split contains 57,401 observations and 156 positive labels. This passes the synthetic signal gate; it does not establish practical deployment precision or downstream model quality.

## Calibration and Remaining Evidence

Calibration subtracts the directly measured fixed families: `DailyMarket`, FINWIRE, `Date`, `Time`, `Industry`, `StatusType`, `TaxRate`, and `TradeType`. HR is variable because staffing scales with the firm. Decimal half-up rounding of `3300 * 1,000,000,000 / 916,523,728` gives the accepted default of 3,601 customers per unit. The repeat factor 1 run keeps the seed, market, and behavior controls unchanged. Its source freeze is `76081d9`, which includes the fixture tool fix and explanatory comments as well as the new default.

The historical original stage 1 measurement in the generator guide is separate evidence: it predates attrition and source-recording stages. It must not be substituted for either full-stage factor 1 run. Higher-factor byte and runtime projections remain estimates.

Remaining evidence includes live clean-publisher integration with S3 and Kafka, the realistic publisher profile, the owner walkthrough, and larger-factor measurements. No factor 5 or factor 100 run is measured here. No generated dataset establishes the rubric's 100,000,000,000 offline-source-byte requirement. Streams, reports, hidden state, and temporary files do not count toward it. Platform compatibility, DP1, and later pipeline work remain on their separate track.
