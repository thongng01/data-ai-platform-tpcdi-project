# Brokerage Source Generator

The generator runs generation stages 1 to 3 for a retail brokerage: business behavior, hidden attrition signal, and source recording. It produces connected customers, accounts, trades, cash transactions, holdings, watches, market records, and references. The [publisher](../publisher/README.md) runs generation stage 4. Its `clean` profile delivers the saved files and cash transaction messages without adding delivery problems or repeating the business simulation.

[Data Platform section 2](../../../docs/Data-Platform.md#2-data-generation-and-problem-injection) owns the business model. [Data Specification](../../../docs/Data-Specification.md#3-physical-source-files-and-ingestion-rules) owns the source contracts. This guide explains how to run the generator and follow its implementation.

## Commands

Run commands from the repository root with Python 3.14. If `.venv` does not exist, create it with `uv venv --python 3.14 .venv`. Install the pinned dependencies:

```bash
uv pip install --python .venv/bin/python -r data/data_source/generator/requirements.txt
```

Each run needs a new output directory. The small configuration covers `2018-12-20` through `2019-02-03`, with a fractional scale factor and a small market:

```bash
.venv/bin/python -m data.data_source.generator run \
  --config data/data_source/generator/configs/small.yaml \
  --output local/runs/small
.venv/bin/python -m data.data_source.generator validate local/runs/small
.venv/bin/python -m pytest data/data_source/generator/tests -q
```

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
| Bounded export | `generation.py`, `sorting.py`, `formats.py`, `definitions/`; `Generation`, `SortedOutput` | Check dated contracts, spool sorted runs, merge bounded inputs, and assign source sequences |
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
| `customer_group_size`, `workers`, `threads` | Customer group bound, one or two business worker processes, and Polars thread count |
| `export_rows`, `merge_fan_in` | Aggregate export row buffer and maximum inputs to one merge |
| Resource settings | Aggregate process-tree memory, disk, and elapsed-time budgets |

The expected cutover customer count is `scale_factor * customers_per_unit`. With the default account shares, each customer has 1.67 accounts on average. Factor 1 therefore anchors about 3,601 customers and 6,014 accounts; factor 100 anchors about 360,100 customers and 601,367 accounts. These are planning expectations, not exact realized populations.

Profile arrays use this order: dormant, long-term investor, retiree, active trader, and day trader. Account-count shares refer to one, two, and three accounts. Tier shares refer to tiers 1, 2, and 3. Tax-status shares refer to codes 0, 1, and 2.

With a positive `positions_per_account` value N, each account draws N fixed security selections from stable keys. Slots can select the same security, so the account's buy universe contains up to N distinct securities. Later buys use that same universe, including after a full sale; this setting does not simply cap simultaneous positions while allowing new securities over time.

NumPy arrays hold mutable customer and account state. The simulation selects daily activity over account arrays, while Polars builds export batches, hidden-state Parquet files, and validation queries. Shared memory-mapped market arrays and a bounded day cache avoid scalar reads from disk for each event. Each customer and all related accounts stay in one group, so one worker owns every change to an account.

| Retained state | Contents and lifetime |
| --- | --- |
| Population | Stable IDs, joining dates, hidden traits, joining tiers, borrowing limits, and replacement slots |
| Ledger | Integer-cent balances and reserved funds per account |
| Holding | One account and security pair with first-buy origin, quantity, cost, and reserved shares; a full sale releases the entry |
| Open trade | Identity, account, security, type, quantity, bid, due date, reservation, and status |
| Return history | Per-account equity indexes in a 96-day circular buffer for the three-calendar-month lookback |
| Watches and prospects | Live watch periods and persistent prospect identities; snapshot text uses bounded blocks |

A cash account cannot spend below zero. A margin account cannot exceed its borrowing limit. Reservations prevent two proposals from using the same money or shares. Each completed trade produces one cash transaction and one holding change. A full sale ends the holding; a later buy starts a new origin. No short selling occurs.

The runner refuses an existing output directory or its `.partial` counterpart. It writes under `.partial`, samples aggregate process-tree RSS every 0.1 seconds and disk use every two seconds, and stops the process group if a budget fails. Sampling is not a kernel memory limit. A failed run retains logs and partial evidence. A successful run passes validation before the runner removes temporary files and renames the directory.

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
  validation.json
  supervision.json
  worker.log
  _work/                             # Temporary sorted runs and state
```

Every expected file exists, including a zero-record file for no activity. Prospects arrive at cutover and on the first day of each later month. HR and the six reference families load once. FINWIRE uses posting-quarter filenames; Q1 reports released April 30 belong to `FINWIREyyyyQ2`. Market history includes the calendar year before `initial_start`.

NYSE sessions come from pinned `exchange-calendars`, calendar `XNYS`. The fixed UTC clock uses 09:30 opening, 16:00 normal close, and 13:00 early close, without daylight saving time. The reference calendar has 1,258 trading days in 2014 through 2018 and 1,760 in 2019 through 2025. Every UTC calendar date has a daily delivery, including weekends and holidays.

| Output | Identity and order |
| --- | --- |
| Customers and accounts | Stable customer ordinals and account slots; IDs are not reused |
| Trades | Account ID times 1,000,000 plus lifetime trade ordinal; checked bounds below `10^15` |
| Initial trades and history | Trade ID, with history timestamp and transition order |
| Initial CMS actions | Action timestamp, customer ID, and action ordinal; omitted and empty fields stay distinct |
| Cash transactions | Global `CT_ID` sequence in timestamp, account, local ordinal, and kind order, continuing across cutover |
| Daily CDC rows | `CDC_DSN` from 1 through N within each file after business-order sorting |
| FINWIRE | Posting timestamp, then company, security, and financial dependency order, then natural ID |
| Prospects and references | Stable natural ID; daily market rows sort by date and security |

Named random keys include entity identity, date, purpose, and local ordinal. The [personal word lists](definitions/README.md) also use stable keys, so matching prospects share customer names and addresses. No worker shares a mutable random stream. Group size, worker count, threads, export buffer size, merge fan-in, and output path do not change the source bytes for the same semantic settings.

`Generation` bounds the aggregate row buffer by `export_rows`. `SortedOutput` keeps sorted runs and a disk-backed catalog. Multilevel merges open at most `merge_fan_in` inputs; duplicate complete sort keys fail export. Final writers apply dated source contracts, assign sequences, and measure rows, bytes, and SHA-256 hashes. The semantic dataset identity includes business settings, seed, format version, source definitions, word lists, and calendar fingerprint. The manifest also records implementation hashes and dependency versions.

## Validation and Measurements

Validation reads the saved files and stream messages, derives its own expected-file list, and checks coverage, cadence, field widths, encoding, hashes, and identities. It stages typed records into bounded Parquet blocks, partitions replay by account and customer, and joins market prices by date. Every partition undergoes checks for legal trade transitions, settlement, cash and margin limits, holding conservation, dividends, watches, and lifetimes.

Hidden account files supply traits and lifetime attributes. They do not supply balances as proof: the validator reconstructs balances and holdings from source records after it verifies any audited data errors. Parquet staging buffers have an aggregate row allowance. The source-byte partition target is an average, not a hard memory bound for one unusually large account history.

Tests cover deterministic personal fields, XML omission and empty text, ledger and holding conservation, profile planning, attrition behavior, dated trade schemas, exact data-error evidence, corruption detection, resource failures, and repeatability across execution settings.

| Report | Contents |
| --- | --- |
| `manifest.json` | Semantic identity, configuration, dependency and code versions, file counts, byte totals, hashes, and phase times |
| `summary.json` | Counts and bytes by family, date, and delivery; business activity, cash transaction types, profile counts, rejected proposals, skew, and state peaks |
| `validation.json` | Saved-file validation results |
| `summary/signal_check.json` | Sample size, chronological split counts, model and rule metrics, or the reason for insufficient data |
| `supervision.json` | Sampled aggregate RSS, process count, disk use, temporary disk peaks, and sampling intervals |

Phase times cover references, market, population, activity, prospects, final export, and validation. Buffer flushes and intermediate merges count toward the phase that emits their records. The final export phase measures final merges and writes, not all export work.

### Full-Calendar Measurements

These runs cover generation stages 1 to 3 over 2014-01-01 through 2025-12-31, with two workers, two Polars threads per worker, seed `20261029`, 1,250 companies, and 2,000 securities. Each run passes full saved-file validation. Generation seconds sum the measured phases before validation; peak RSS covers the supervisor and all descendants. Disk sampling occurs every two seconds, so temporary storage is a sampled peak.

| Factor | Customers per unit | Offline bytes | Stream bytes | Generation seconds | Validation seconds | Peak RSS MiB | Peak temporary GiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3,300 | 1,346,693,500 | 307,655,539 | 954.77 | 753.54 | 1,374.79 | 2.426 |

### Original Stage 1 Reference Measurement

This historical measurement covers the full 2014 through 2025 reference calendar with two workers and 3,300 cutover customers per unit. It records the original stage 1 behavior and source format, before the attrition and source-recording stages. It is not a runtime or volume guarantee for another configuration.

| Factor | Customers per unit | Offline bytes | Stream bytes | Generation seconds | Validation seconds | Peak RSS MiB | Peak temporary GiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3,300 | 1,302,815,601 | 561,699,602 | 1,284.40 | 948.49 | 1,279.64 | 2.562 |

### Volume Calibration

One unit targets 1,000,000,000 variable offline bytes over the full reference calendar. Fixed market and reference bytes come on top. A shorter calendar cannot calibrate this 12-year target.

1. Run factor 1 with the chosen business settings and seed. Measure offline bytes `B1` from the file inventory.
2. Sum fixed-family bytes `F` for `DailyMarket`, FINWIRE, `Date`, `Time`, `Industry`, `StatusType`, `TaxRate`, and `TradeType`. HR is variable because employee count scales with the firm.
3. Estimate variable bytes per unit as `V = B1 - F`. Set `customers_per_unit` to `old_customers_per_unit * 1,000,000,000 / V`, with decimal half-up rounding.
4. Repeat factor 1 with the calibrated customer count and unchanged market, behavior, and seed. Measure its variable bytes and report the difference from the one-GB target.

This factor 1 slope is provisional. The projection at factor `s` is `F + s * V`; it does not measure factor 5 or establish factor 100 throughput. Population sampling and staffing rounding can change scaling. Do not change activity rates, duplicate rows, or change market size to force a byte total.

The large-dataset evidence requires at least 100,000,000,000 measured offline source bytes. Stream messages, reports, hidden files, and temporary files do not count toward that requirement. A projection does not satisfy it.
