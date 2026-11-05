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

## Calibration and Remaining Evidence

Calibration subtracts the directly measured fixed families: `DailyMarket`, FINWIRE, `Date`, `Time`, `Industry`, `StatusType`, `TaxRate`, and `TradeType`. HR is variable because staffing scales with the firm. Decimal half-up rounding of `3300 * 1,000,000,000 / 916,523,728` gives the accepted default of 3,601 customers per unit. The repeat keeps the seed, market, and behavior controls unchanged. Its source freeze is `76081d9`, which includes the fixture tool fix and explanatory comments as well as the new default.

The repeat measures 1,008,534,820 variable offline bytes, or 8,534,820 bytes above the target (+0.853482%). The accepted default remains 3,601; the report's mechanical suggestion of 3,571 for another iteration is not applied. No further calibration or run is authorized. A factor 100 estimate uses the measured slope, `430,169,772 + 100 * 1,008,534,820 = 101,283,651,772` offline bytes, and `100 * 339,074,412 = 33,907,441,200` stream bytes. These are projections, not factor 100 measurements.

The historical original stage 1 measurement in the generator guide is separate evidence: it predates attrition and source-recording stages. It must not be substituted for either full-stage factor 1 run. The scoped implementation and factor 1 verification are integrated into `infra/compat-test`. The lead reran the full generator and publisher suite in the combined checkout; all 394 tests passed. The owner walkthrough remains pending. All larger-factor byte and runtime projections remain estimates.

Remaining evidence includes live clean-publisher integration with S3 and Kafka, the realistic publisher profile, the owner walkthrough, and larger-factor measurements. No factor 5 or factor 100 run is measured here. No generated dataset establishes the rubric's 100,000,000,000 offline-source-byte requirement. Streams, reports, hidden state, and temporary files do not count toward it. Platform compatibility, DP1, and later pipeline work remain on their separate track.
