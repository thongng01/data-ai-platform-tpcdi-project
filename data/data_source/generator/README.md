# Brokerage Source Generator

The generator runs generation stages 1 to 3 for a retail brokerage: business behavior, hidden attrition signal, and source recording. It produces connected customers, accounts, trades, cash transactions, holdings, watches, market records, and references. The [publisher](../publisher/README.md) runs generation stage 4. Its `clean` profile delivers the saved files and cash transaction messages without adding delivery problems or repeating the business simulation.

[Data Platform section 2](../../../docs/Data-Platform.md#2-data-generation-and-problem-injection) owns the business model. [Data Specification](../../../docs/Data-Specification.md#3-physical-source-files-and-ingestion-rules) owns the source contracts. This guide explains how to run the generator and follow its implementation.

## Commands

Run commands from the repository root with Python 3.14. If `.venv` does not exist, create it with `uv venv --python 3.14 .venv`. Install the pinned dependencies:

```bash
uv pip install --python .venv/bin/python -r data/data_source/generator/requirements.txt
```

Each new run needs a new output directory. The small configuration covers `2018-12-20` through `2019-02-03`, with a fractional scale factor and a small market:

```bash
.venv/bin/python -m data.data_source.generator run \
  --config data/data_source/generator/configs/small.yaml \
  --output local/runs/small
.venv/bin/python -m data.data_source.generator validate local/runs/small
.venv/bin/python -m pytest data/data_source/generator/tests -q
```

`run` performs full saved-file validation by default. For a repeat run of unchanged code, `--skip-validation` omits only that full validator. Export still checks source contracts, ordering, duplicate keys, and expected file coverage, and records counts, byte sizes, and hashes. The runner publishes the final directory with `status: generated-unvalidated` in both `manifest.json` and `summary.json`. Independent error measurements and Trade schema counts stay null until validation succeeds. The flag is a command choice, not a configuration setting, so it does not change dataset identity.

```bash
.venv/bin/python -m data.data_source.generator run \
  --config data/data_source/generator/configs/small.yaml \
  --output local/runs/small-unvalidated --skip-validation
.venv/bin/python -m data.data_source.generator validate local/runs/small-unvalidated
```

`validate` reads the saved dataset without regeneration and records `status: validated` in both reports only after every check passes. The publisher refuses `generated-unvalidated` datasets unless the caller supplies `--allow-unvalidated`; its delivery report records that override.

To check repeatability, run the same business configuration into another directory and compare the saved bytes:

```bash
.venv/bin/python -m data.data_source.generator run \
  --config data/data_source/generator/configs/small.yaml \
  --output local/runs/small-repeat
.venv/bin/python -m data.data_source.generator compare \
  local/runs/small local/runs/small-repeat
```

To check the attrition signal on a small account sample from a validated run:

```bash
.venv/bin/python -m data.data_source.generator signal-check \
  local/runs/small --sample-accounts 512
```

The check builds features from observed account status and cash transactions, not hidden plans. It labels closures in the following 30 days and uses chronological training, validation, and test periods with 30-day label gaps. A short run can return `insufficient_data` because every split needs fully observed labels and both closures and nonclosures. That result does not establish signal quality. The check writes `summary/signal_check.json` and saves observations in `summary/signal_observations.parquet` when observations exist.

The benchmark command records phase timings and resource measurements. This command runs factor 1 over the full reference calendar:

```bash
.venv/bin/python -m data.data_source.generator.benchmark \
  --output local/runs/factor1-measurement \
  --configs data/data_source/generator/configs/medium.yaml
```

Choose an unused benchmark directory and check free disk before a measurement run. The benchmark runs only the configuration paths supplied to `--configs`. It also compares bytes when two configurations have the same semantic dataset identity.

## How to Read the Code

Start with `runner.worker`, which calls the generation phases in order. Then read `BusinessSimulation.run` in `activity.py`. It names the steps of one simulated day. Follow a method into the business planner or recorder when you need its rule; function docstrings and nearby comments explain the detailed calculations.

| Pipeline position | Modules and main types | Responsibility |
| --- | --- | --- |
| Run contract and supervision | `__main__.py`, `config.py`, `runner.py`; `Config` | Parse commands, validate settings, supervise resources, and write reports |
| Initial references | `reference.py`, `personal_data.py` | Build static reference files and the HR roster from repeatable personal fields |
| Shared market | `market.py`; `Market` | Build prices, exchange sessions, released FINWIRE values, and dividends |
| Population plan | `population.py`; `Population` | Plan joining groups, account traits, replacement capacity, and expected risk normalization |
| Daily business execution | `business.py`, `activity.py`; `BusinessDay`, `Intent`, `BusinessSimulation`, `Ledger`, `HoldingBook` | Select account-wide proposals in arrays and execute ordered money and holding changes |
| Attrition signal | `attrition.py`; `AttritionPlanner`, `ReturnHistory` | Select exits from observed history, add leaving and partial-outflow intents, and classify observed outcomes |
| Source recording | `customers.py`, `recording.py`; `SourceRecorder` | Encode CMS actions, customer and account fields, prospect snapshots, data errors, and trade channels |
| Bounded export | `generation.py`, `output.py`, `output_formats.py`, `formats.py`, `definitions/`; `Generation`, `TypedOutput` | Buffer typed records, seal source parts, order and check native batches, assign sequences, and write source bytes |
| Saved-file validation | `validation.py`, `validation_faults.py`; `FaultAudit` | Parse saved bytes, verify exact data-error evidence, and replay business invariants |
| Sampled signal check | `signal_check.py`; `check_signal` | Build source-observed features and compare a logistic model with the outflow rule using chronological splits |

`Stage1IntentPlanner` supplies ordinary business behavior. `AttritionPlanner` extends that interface without changing settlement rules. `SourceRecorder` receives correct business fields and records a separate copy, so a recorded fee error cannot change the ledger.

## Configuration and State

Configuration holds numbers and distribution choices. Source definitions hold field order, types, widths, separators, null rules, and dated format additions. The loader rejects duplicate or unknown keys, invalid dates, nonfinite numbers, invalid distributions, and values that exceed source widths.

| Settings | Meaning |
| --- | --- |
| `initial_start`, `initial_end`, `daily_start`, `daily_end` | Inclusive UTC dates; `daily_start` must be the day after `initial_end` |
| `scale_factor`, `customers_per_unit`, `seed` | Firm size at the expected cutover anchor and the stable random seed; `customers_per_unit` defaults to 3,601 |
| Population, profile, wealth, and activity settings | Joining, closure, inactivation, update, trade, cash-flow, margin, and watch distributions |
| `positions_per_account` | Repeatable security-selection slots per account; default 4; zero lets buys select from the full market |
| `account_generations` | Reserved account generations for each joining account slot; default 8; checked capacity prevents ID reuse |
| `attrition_enabled` | Select the attrition planner; disabling it selects ordinary stage 1 lifetime planning |
| Attrition settings | Warning share 80%, warning length 1 to 8 weeks, and 1 to 4 liquidation steps |
| `annual_lookalike` | Intended confirmed look-alike rate, default 0.02 per account-year; measured outcomes determine the realized rate |
| `lookalike_opportunity_multiplier` | Proposal-rate calibration factor, default 2.12; multiplies `annual_lookalike` to set the raw opportunity rate |
| Data-error settings | Five independent rates, listed under Source Recording below |
| `trade_channel_start` | First daily delivery date with `T_CHANNEL`; default `2019-07-01` |
| Market settings | Companies, securities, prices, volatility, popularity, shock days, and dividend rules, independent of firm size |
| `customer_group_size`, `workers`, `threads` | Customer group bound, up to eight business worker processes, and Polars thread count; defaults 1,024, 4, and 2 |
| `export_rows` | Aggregate typed-record buffer and maximum delivered native batch rows |
| `merge_fan_in` | Accepted for configuration compatibility; native export ignores this setting and reports its value |
| Resource settings | Aggregate process-tree memory, disk, and elapsed-time budgets; memory defaults to 8,192 MiB |

The expected cutover customer count is `scale_factor * customers_per_unit`. With the default account shares, each customer has 1.67 accounts on average. Factor 1 therefore anchors about 3,601 customers and 6,014 accounts; factor 100 anchors about 360,100 customers and 601,367 accounts. These are planning expectations, not exact realized populations.

Profile arrays use this order: dormant, long-term investor, retiree, active trader, and day trader. Account-count shares refer to one, two, and three accounts. Tier shares refer to tiers 1, 2, and 3. Tax-status shares refer to codes 0, 1, and 2.

With a positive `positions_per_account` value N, each account draws N fixed security selections from stable keys. Slots can select the same security, so the account's buy universe contains up to N distinct securities. Later buys use that same universe, including after a full sale; this setting does not simply cap simultaneous positions while allowing new securities over time.

NumPy arrays hold mutable customer and account state. The simulation selects daily activity over account arrays, while Polars builds export batches, hidden-state Parquet files, and validation queries. Shared memory-mapped market arrays and a bounded day cache avoid scalar reads from disk for each event. Each customer and all related accounts stay in one group, so one worker owns every change to an account.

With `workers: 1`, the coordinator simulates groups directly. With more workers and more than one group, a process pool simulates up to `workers` groups at once. The coordinator waits for results and does not simulate a group. Each free process takes the next group without waiting for other processes. A worker error cancels pending groups; running groups finish and retain their sealed parts before the error returns. The runner retains partial output and does not publish it.

The defaults use four workers and 1,024 customers per group. A factor 1 measurement over `2018-10-01` through `2019-07-31`, with two Polars threads and an 8 GiB memory budget, gives these single-run activity results. RSS includes the generator coordinator, pool workers, and resource tracker, but not the measurement wrapper.

| Workers | Customers per group | Groups | Activity wall seconds | Activity CPU seconds | Peak activity RSS MiB |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 1,024 | 4 | 8.744 | 35.410 | 1,167.29 |
| 4 | 512 | 8 | 8.787 | 34.638 | 1,144.43 |
| 8 | 512 | 8 | 8.126 | 61.743 | 2,012.98 |

Four workers limit CPU and memory cost while retaining most of the measured wall-time benefit. The 1,024-customer group bound uses fewer groups than 512 without a material activity-time difference in this measurement. Eight workers remain available up to the configured limit. These bounded measurements do not establish larger-workload memory limits; the sampled 8 GiB process-tree guard remains active for every run.

Customer-update and watch-count draws use arrays with the same named keys as scalar draws. The simulation visits customers only when an update, watch addition, due cancellation, or lifetime action can occur. Due-day indexes select watch cancellations and newly due orders. Account indexes select orders for leaving and liquidation. Unfilled due orders remain eligible on later trading days because the day's price range can permit execution.

| Retained state | Contents and lifetime |
| --- | --- |
| Population | Stable IDs, joining dates, hidden traits, joining tiers, borrowing limits, and replacement slots |
| Ledger | Integer-cent balances and reserved funds per account |
| Holding | One account and security pair with first-buy origin, quantity, cost, and reserved shares; a full sale releases the entry |
| Open trade | Identity, account, security, type, quantity, bid, due date, reservation, and status |
| Return history | Per-account equity indexes in a 96-day circular buffer for the three-calendar-month lookback |
| Watches and prospects | Live watch periods and persistent prospect identities; snapshot text uses bounded blocks |

A cash account cannot spend below zero. A margin account cannot exceed its borrowing limit. Reservations prevent two proposals from using the same money or shares. Each completed trade produces one cash transaction and one holding change. A full sale ends the holding; a later buy starts a new origin. No short selling occurs.

The runner refuses an existing output directory or its `.partial` counterpart for a new run. It writes under `.partial`, samples aggregate process-tree RSS every 0.1 seconds and disk use every two seconds, and stops the process group if a budget fails. Sampling is not a kernel memory limit. A failed run retains logs, checkpoints, and partial evidence. A successful run passes full validation before the runner removes temporary files and renames the directory, unless `--skip-validation` explicitly selects the unvalidated state.

Resume a retained run with its saved configuration, or supply `--config` to change permitted runtime settings:

```bash
.venv/bin/python -m data.data_source.generator run --resume local/runs/small.partial
```

Each completed step binds its result to `_checkpoints/context.json` through a SHA-256 context digest. That file records implementation file hashes, semantic configuration, `customer_group_size`, and `export_rows`. Resume refuses a changed code version, semantic configuration, or intermediate layout before it changes files. Worker count, thread count, resource budgets, and the ignored `merge_fan_in` setting can change. Reference records, market arrays, the population plan, each customer group, prospects, the coordinator part, export, and validation have completion receipts. Group receipts retain business counts and commit last, after actual lifetimes, hidden records, and sealed source files. Incomplete groups restart from immutable planned population rows, not partly changed lifetimes.

Resume keeps completed steps and redoes incomplete steps. If export did not complete, it deletes `offline/`, `stream/`, and `summary/data_errors.jsonl` before exporting all sealed parts again. It keeps hidden records and sealed parts. If export completed but validation failed, resume runs only validation. `run --resume <dataset>` also validates a final `generated-unvalidated` dataset without its temporary files. Resume always performs full validation and does not accept `--skip-validation`. Run only one command against a retained directory at a time.

`timeout_seconds` defaults to 7,200 seconds and applies separately to each run attempt, including validation. Keep it in the run configuration because workload size and machine speed determine the needed budget. A factor 100 full-calendar run is estimated to need more than 10 hours; set a budget above that estimate before starting it. This estimate is not a measured factor 100 runtime.

## One Simulated Day

`BusinessSimulation.run` repeats the following steps. Array selection handles independent account decisions. Ordered execution remains explicit because one receipt can fund a later payment, but not an earlier one.

1. Resolve the date, released market values, session hours, and due closures in `_begin_day`. The attrition planner uses only completed earlier days when it selects new exit plans.
2. Record customer and account openings in `_open_accounts`. New accounts start with zero balance and no holdings. Schedule opening funding at 08:00 UTC. Pay dividends at 06:00 UTC from start-of-day holdings in `_pay_dividends`.
3. Select ordinary trade, deposit, withdrawal, and update proposals in `_plan_activity`. Schedule due orders in `_plan_due_orders`, then add the attrition planner's cancellations, staged sales, and withdrawals.
4. Apply intents in timestamp, account, ordinal, and kind order in `_execute_intents`. Reserve money for buys and shares for sells. Reduce or reject proposals that exceed available funds or unreserved holdings.
5. Complete each trade as one business operation: release its reservation, settle cash, change the holding, and record the trade and holding change. Execution respects the daily low and high and the order's limit. Noncompleted trades keep execution amounts empty.
6. Finish due account closures in `close_accounts`: cancel orders, sell remaining holdings, deposit enough to repay borrowing, withdraw the remaining cash, then close. Update customer fields and watches in `_update_customers`; customer inactivation cancels all watches.
7. Save unfinished trades at cutover and record state peaks in `_finish_day`. The planner values end-of-day holdings, updates past-return history, and activates eligible replacement slots for a later day.

### Attrition Signal

`AttritionPlanner` applies the profile, tenure, tier, and loss multipliers from the system model to daily exit risk. `ReturnHistory` removes external deposits and withdrawals before it measures investment performance over the preceding three calendar months. Missing lookback history and a lack of prior positive capital do not by themselves add the loss multiplier.

With valid history at the lookback start, an investment loss that wipes out positive capital retains a 100% loss marker even when equity becomes zero or negative. An external withdrawal alone does not create that marker.

For a customer-wide exit, the planner takes the mean risk of the customer's active accounts. This prevents account count alone from multiplying customer risk. A deterministic curve of expected account traits normalizes risk across customer groups; it does not use future realized outcomes. The population plan anchors expected cutover size. Actual customer and account counts and annual exit rates come from the realized simulation, not an exact marginal-rate guarantee.

An exit plan stops ordinary deposits and buys, cancels open orders, schedules staged sales, and withdraws sale proceeds within the day. A customer exit gives the customer's active accounts one shared leaving period. Hidden plans stay under `summary/hidden/`; source records contain only their resulting business events.

The planner also proposes partial sales and withdrawals without using future closure labels. A proposal becomes an `outflow_candidate` only after a sale executes and a positive withdrawal follows. The candidate does not suppress later account-closure or customer-inactivation risk. An unexecuted proposal does not count as an observed outflow.

After the business simulation finishes, the planner classifies each candidate from the account's actual lifetime:

| Hidden classification | Condition |
| --- | --- |
| `confirmed_lookalike` | The account remains open through the next 30 fully observed calendar days |
| `closing` | The account closes on or before the end of the candidate's 30-day horizon |
| `censored` | The run ends before the full horizon is observed, without an observed closure in that horizon |

This classification changes only the hidden report. It does not change business behavior or feed the signal-check features. The signal check derives closure labels from observed account status instead.

The configured raw opportunity rate is `annual_lookalike * lookalike_opportunity_multiplier`, or `0.02 * 2.12 = 0.0424` per account-year with the defaults. The intended confirmed rate remains 0.02. The multiplier controls proposal intensity; it does not guarantee conversion into executed or confirmed outcomes. The summary reports proposal, executed-candidate, and confirmed rates separately. The confirmed rate uses account-day exposure whose next 30 calendar days lie within the run.

### Source Recording

`SourceRecorder` applies data errors after the business calculation. It covers initial CMS XML, daily customer rows, FINWIRE company records, and completed trades. Each eligible field has a separate stable draw; one source record can contain more than one selected error.

| Setting | Default | Recorded error |
| --- | ---: | --- |
| `customer_tier_error_rate` | 0.002 | Customer tier outside 1 to 3 |
| `customer_dob_error_rate` | 0.001 | Customer birth date after the record date |
| `company_rating_error_rate` | 0.01 | Company credit rating outside the accepted rating codes |
| `trade_charge_error_rate` | 0.001 | Completed-trade charge above price times quantity |
| `trade_commission_error_rate` | 0.001 | Completed-trade commission above price times quantity |

The recorder writes the source identity, field, correct value, and recorded value to `summary/data_errors.jsonl`. This local audit is separate from source delivery. The generator validator recomputes error selection, requires an exact match to the recorded value and supported mutation, and restores only the verified correct value for business replay. Missing, duplicate, extra, or mismatched evidence fails validation. An arbitrary fee mismatch does not become acceptable because data errors are enabled.

Each audit key contains source, day, identity, and field. The identity uses only unchanged fields in the delivered record:

| Audited source | Identity fields | Why records stay distinct |
| --- | --- | --- |
| Daily Customer | `C_ID`, `C_ST_ID`, `CDC_FLAG` | The insert uses `I`; an active update uses `U`; inactivation uses `INAC`. Each customer has at most one active update per day. |
| CustomerMgmt | `C_ID`, `ActionType`, `ActionTS`; also `CA_ID` for `ADDACCT`, `UPDACCT`, and `CLOSEACCT` | Each account-specific action contains one Account element. The other actions occur at most once per customer and timestamp. |
| Completed Trade | `T_ID`, `T_DTS`, `T_ST_ID` | Each unique trade completes once. |
| FINWIRE company | `CIK`, `PTS` | Each company appears once per posting. |

Error selection uses the entity ID, source, field, day, and action kind or status. It does not use the added audit discriminators. Same-day active customer versions can share a draw, as can same-kind account actions for one customer. These records receive the selected error together, but each has separate evidence.

The initial trade extract always has 14 business fields. Daily deliveries on or after `trade_channel_start` add `T_CHANNEL` as the 15th business field, after the original fields. Both daily versions also carry the two CDC fields. Channel selection uses stable trade identity and account profile; traders favor direct channels and retirees favor brokers. A later update to an older trade uses its delivery date's contract.

## Output, Ordering, and Identity

Only `offline/` files become objects in `upstream`, with that prefix removed from their object keys. The publisher sends saved `stream/` messages to Kafka. It does not upload generator reports, hidden state, or the data-error audit.

```text
<dataset>/
  offline/initial/                    # Original source filenames
  offline/daily/YYYYMMDD/             # Dated daily source filenames
  stream/cash_transactions/YYYYMMDD.jsonl
  summary.json
  summary/hidden/                     # Account traits, lifetimes, and attrition plans
  summary/data_errors.jsonl           # Exact data-error evidence
  config.json
  manifest.json
  validation.json                    # Present after full validation succeeds
  _checkpoints/                      # Run context and completed-step receipts
  supervision.json
  worker.log
  _work/                             # Typed source parts, native spill, and state
```

Every expected file exists, including a zero-record file for no activity. Prospects arrive at cutover and on the first day of each later month. HR and the six reference families load once. FINWIRE uses posting-quarter filenames; Q1 reports released April 30 belong to `FINWIREyyyyQ2`. Market history includes the calendar year before `initial_start`.

NYSE sessions come from pinned `exchange-calendars`, calendar `XNYS`. The fixed UTC clock uses 09:30 opening, 16:00 normal close, and 13:00 early close, without daylight saving time. The reference calendar has 1,258 trading days in 2014 through 2018 and 1,760 in 2019 through 2025. Every UTC calendar date has a daily delivery, including weekends and holidays.

| Output | Identity and order |
| --- | --- |
| Customers and accounts | Stable customer ordinals and account slots; IDs are not reused |
| Trades | Account ID times 1,000,000 plus lifetime trade ordinal; checked bounds below `10^15` |
| Initial trades and history | Trade ID, with history timestamp and transition order |
| Initial CMS actions | Action timestamp, customer ID, action ordinal, action kind, and account ID as the final tie-breaker; omitted and empty fields stay distinct |
| Cash transactions | Global `CT_ID` sequence in timestamp, account, local ordinal, and kind order, continuing across cutover |
| Daily CDC rows | `CDC_DSN` from 1 through N within each file after business-order sorting |
| FINWIRE | Posting timestamp, then company, security, and financial dependency order, then natural ID |
| Prospects and references | Stable natural ID; daily market rows sort by date and security |

Named random keys include entity identity, date, purpose, and local ordinal. The [personal word lists](definitions/README.md) also use stable keys, so matching prospects share customer names and addresses. No worker shares a mutable random stream. Group size, worker count, threads, export buffer size, merge fan-in, and output path do not change the source bytes for the same semantic settings.

`Generation` accepts typed tuples and named columns for all 18 source families. Money stays in integer cents, timestamps stay in epoch seconds, dates stay in epoch days, and security symbols use numeric IDs until export. `SourceRecorder` applies batch data errors and channel selection to a separate recorded frame. XML and FINWIRE keep their format-specific encoders.

`TypedOutput` bounds the aggregate row buffer by `export_rows` and writes typed Parquet fragments. Each customer group owns an isolated directory and publishes its completion marker only after it seals its parts. Export checks the scheduled groups, consolidates each family's fragments, and lets Polars order the records with disk spill. Duplicate complete sort keys fail export, including duplicates across native batch boundaries.

Each bounded contract batch undergoes native formatting and field checks before it splits into delivery files. Cash IDs continue across cutover; CDC sequences restart within each file. Writers measure rows, bytes, and SHA-256 hashes as they write. Native spill settings stay inside `output.py`; the runner installs them before it starts workers. Spill budgets and sampled process-tree guards do not constitute a hard RAM limit. Population arrays and business state also consume memory.

The semantic dataset identity includes business settings, seed, format version, source definitions, word lists, and calendar fingerprint. The manifest also records implementation hashes and dependency versions.

## Validation and Measurements

Validation reads the saved files and stream messages, derives its own expected-file list, and checks coverage, cadence, field widths, encoding, hashes, and identities. It stages typed records into bounded Parquet blocks, partitions replay by account and customer, and joins market prices by date.

The `workers` setting also controls validation parsing and replay processes. FINWIRE runs first in one ordered parser because its company and report checks span files and its security versions supply later checks. XML, Customer, Trade, and the other independent files then parse into separate staged blocks. One worker runs inline; multiple workers use spawned processes. The coordinator accepts file results in source-file order, including any FINWIRE failure found in the first wave. It checks the first and last cash and market keys before any later row failure, so global sequences and order remain exact.

Each parser reads a private copy of the sparse fault evidence, recomputes selection, verifies the mutation, and restores correct fields. It returns consumed audit keys and integer measurement counts, not source rows. The coordinator rejects a key consumed in two files, sums every field and delivery count exactly, and requires complete audit consumption. A parser also rejects repeated consumption inside its own file. Audit memory grows with the number of error entries and parser workers; the process-tree RSS guard covers these copies.

Trade pricing uses one streaming left join on market date and symbol. The join builds its hash state from only the market date, symbol, low price, and high price. This state grows with securities and sessions, not with customer scale. Missing market rows leave null prices for the coverage and replay checks. Market metadata cannot replace a trade's account partition, delivery day, or source order.

All customer partitions finish before account replay starts because accounts read the saved customer lifetimes. One worker runs inline. Multiple workers use spawned processes and report the error from the lowest failing partition, in the same order as serial replay. Every partition undergoes checks for legal trade transitions, settlement, cash and margin limits, holding conservation, dividends, watches, and lifetimes.

Hidden account files supply traits and lifetime attributes. They do not supply balances as proof: the validator reconstructs balances and holdings from source records after it verifies any audited data errors. It reads every `accounts-*.parquet` file regardless of the group count. The signal check reads source-visible lifetimes and cash transactions, not hidden plans.

Replay uses a 16 MiB average source-byte target with a ceiling of 1,024 partitions. This ceiling limits small files and directories as source volume grows. Each parse stage buffers at most 8,192 rows. `maximum_parse_buffer_rows` reports the largest single-stage peak, not the sum across parsing processes. Partition writers share a 262,144-row allowance and a 64 MiB estimated Arrow-data allowance, based on source field widths and staging metadata. These writer allowances exclude the market join state, encoder memory, and engine overhead. The source-byte target is an average, not a hard memory bound for one unusually large account history. Above the partition ceiling, average replay memory grows with source volume; worker count and the sampled RSS guard still need a measured resource budget.

Tests cover deterministic personal fields, XML omission and empty text, ledger and holding conservation, profile planning, attrition behavior, dated trade schemas, exact data-error evidence, corruption detection, resource failures, and repeatability across execution settings.

| Report | Contents |
| --- | --- |
| `manifest.json` | Semantic identity, configuration, dependency and code versions, file counts, byte totals, hashes, and phase times |
| `summary.json` | Counts and bytes by family, date, and delivery; business activity, cash transaction types, profile counts, rejected proposals, skew, and state peaks |
| `validation.json` | Saved-file validation results, written after a successful full check |
| `_checkpoints/` | Code and configuration context, completed-step receipts, and retained phase timings |
| `summary/signal_check.json` | Sample size, chronological split counts, model and rule metrics, or the reason for insufficient data |
| `supervision.json` | Sampled aggregate RSS, process count, disk use, temporary disk peaks, and sampling intervals |

Phase times cover references, market, population, activity, prospects, final export, and validation. Buffer flushes and group sealing count toward the phase that emits their records. Final export measures native ordering, formatting, checks, and writes, not all export work. Reports include phase user and system CPU seconds, native batch bounds, fragment counts, sealed bytes, and spool, seal, and sort-write times. Export subphase totals can include work from multiple processes; they are not separate elapsed phases to add to generation time.

`customer_groups` reports execution grouping. `peak_active_watches`, `peak_live_holdings`, and `peak_open_trades` report the largest per-group peaks, not whole-dataset peaks. These values and `numeric_state_buffer_bytes` can change with group size even when source bytes and business records remain identical. Hidden file names and counts also follow group boundaries; compare their typed records in stable account and event order when group sizes differ.

### Full-Calendar Measurements

These runs cover generation stages 1 to 3 over 2014-01-01 through 2025-12-31, with two workers, two Polars threads per worker, seed `20261029`, 1,250 companies, and 2,000 securities. Each run passes full saved-file validation. Generation seconds sum the measured phases before validation; peak RSS covers the supervisor and all descendants. Disk sampling occurs every two seconds, so temporary storage is a sampled peak.

| Measurement | Factor | Customers per unit | Cutover customers | Cutover accounts | Offline bytes | Stream bytes | Generation seconds | Validation seconds | Peak RSS MiB | Peak temporary GiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Calibration input | 1 | 3,300 | 3,278 | 5,488 | 1,346,693,500 | 307,655,539 | | | | |
| Calibrated volume | 1 | 3,601 | 3,597 | 6,028 | 1,438,704,592 | 339,074,412 | | | | |
| Runtime | 1 | 3,601 | 3,597 | 6,028 | 1,438,704,592 | 339,074,412 | 470.48 | 831.70 | 1,774.40 | 3.463 |

The runtime measurement covers all 18,051 source files, 5,574 audit rows, and eight ordered typed hidden files. Its full command takes 1,304.80 seconds, including validation. This single run does not establish larger-factor throughput. The RSS column covers the generator supervisor and descendants; including the separate measurement wrapper raises the sampled peak to 1,909.45 MiB.

### Volume Calibration

One unit targets 1,000,000,000 variable offline bytes over the full reference calendar. Fixed market and reference bytes come on top. A shorter calendar cannot calibrate this 12-year target.

1. Run factor 1 with the chosen business settings and seed. Measure offline bytes `B1` from the file inventory.
2. Sum fixed-family bytes `F` for `DailyMarket`, FINWIRE, `Date`, `Time`, `Industry`, `StatusType`, `TaxRate`, and `TradeType`. HR is variable because employee count scales with the firm.
3. Estimate variable bytes per unit as `V = B1 - F`. Set `customers_per_unit` to `old_customers_per_unit * 1,000,000,000 / V`, with decimal half-up rounding.
4. Repeat factor 1 with the calibrated customer count and unchanged market, behavior, and seed. Measure its variable bytes and report the difference from the one-GB target.

At the default anchor of 3,601 customers per unit, factor 1 measures 1,008,534,820 variable offline bytes, 0.853482% above the target, plus 430,169,772 fixed bytes. The realized cutover population is 3,597 customers and 6,028 accounts, separate from the expected anchor.

The projection at factor `s` is `F + s * V`, using that measured variable slope rather than forcing it to one GB. At factor 100, this estimates 101,283,651,772 offline bytes and 33,907,441,200 stream bytes. It does not measure factor 5 or establish factor 100 throughput. Population sampling and staffing rounding can change scaling. Do not change activity rates, duplicate rows, or change market size to force a byte total.

The large-dataset evidence requires at least 100,000,000,000 measured offline source bytes. Stream messages, reports, hidden files, and temporary files do not count toward that requirement. A projection does not satisfy it.
