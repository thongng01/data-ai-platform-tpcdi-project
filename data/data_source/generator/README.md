# Brokerage Source Generator

The generator implements generation stage 1: business behavior and source encoding for a retail brokerage. It simulates customers, accounts, trades, cash transactions, holdings, and watches one day at a time. A separate publisher delivers the saved offline files to `upstream` and the cash transaction stream to Kafka without running the simulation again.

[Data Platform section 2](../../../docs/Data-Platform.md#2-data-generation-and-problem-injection) owns the business rules. [Data Specification](../../../docs/Data-Specification.md#3-physical-source-files-and-ingestion-rules) owns the source contract. Generation stages 2 and 3 attach at the planning and recording interfaces described below.

## Commands

Run commands from the repository root with Python 3.14. If `.venv` does not exist, create it with `uv venv --python 3.14 .venv`. Install the pinned dependencies into this checkout's environment:

```bash
uv pip install --python .venv/bin/python -r data/data_source/generator/requirements.txt
```

Each run requires a new output directory. The small configuration covers `2018-12-20` through `2019-02-03`, with a fractional scale factor and a small market:

```bash
.venv/bin/python -m data.data_source.generator run \
  --config data/data_source/generator/configs/small.yaml \
  --output local/runs/small
.venv/bin/python -m data.data_source.generator validate local/runs/small
.venv/bin/python -m pytest data/data_source/generator/tests -q
```

A second run with the same business settings supports byte comparison. The comparison checks actual file coverage and hashes, not only the saved metadata:

```bash
.venv/bin/python -m data.data_source.generator run \
  --config data/data_source/generator/configs/small.yaml \
  --output local/runs/small-repeat
.venv/bin/python -m data.data_source.generator compare \
  local/runs/small local/runs/small-repeat
```

The benchmark command runs its configurations sequentially and saves a combined summary. `medium.yaml` uses factor 1 over the reference calendar; `large.yaml` uses factor 5. Check free disk before these runs:

```bash
df -h local
.venv/bin/python -m data.data_source.generator.benchmark \
  --output local/runs/calibration-before \
  --configs data/data_source/generator/configs/medium.yaml \
            data/data_source/generator/configs/large.yaml
```

`medium-wide.yaml` changes execution settings for a factor 1 repeatability run. `long-history.yaml` also uses the reference calendar. The benchmark output directory and each run directory must be absent before the command starts.

## Output Layout

Only files under `offline/` become objects in `upstream`. Removing that prefix gives the object key. The publisher does not upload generator metadata or hidden state.

```text
<dataset>/
  offline/initial/                    # Original source names
  offline/daily/YYYYMMDD/             # Dated source names
  stream/cash_transactions/YYYYMMDD.jsonl
  summary.json
  summary/hidden/accounts-NNNNNNNNNNN.parquet
  config.json
  manifest.json
  validation.json
  supervision.json
  worker.log
  _work/                             # Temporary; removed after a successful run
```

| Delivery | Expected offline files |
| --- | --- |
| Initial load | `CustomerMgmt.xml`, `Trade.txt`, `TradeHistory.txt`, `CashTransaction.txt`, `HoldingHistory.txt`, `WatchHistory.txt`, `DailyMarket.txt`, `HR.csv`, `Prospect.csv`, six reference files, and released FINWIRE posting-quarter files |
| Every daily delivery | `Customer_YYYYMMDD.txt`, `Account_YYYYMMDD.txt`, `Trade_YYYYMMDD.txt`, `HoldingHistory_YYYYMMDD.txt`, `WatchHistory_YYYYMMDD.txt`, `DailyMarket_YYYYMMDD.txt` |
| First day of each month | The daily set plus `Prospect_YYYYMMDD.csv` |
| FINWIRE release date | The applicable daily set plus `FINWIREyyyyQq`, named by the posting quarter of `PTS` |

For example, Q1 2019 reports released April 30 go to `offline/daily/20190430/FINWIRE2019Q2`. The initial prospect snapshot represents `initial_end`. HR and reference files load once. Every expected file exists, including a zero-record file when nothing happens. The output contains no benchmark audit files or `BatchDate.txt`.

Each daily delivery date also has a saved stream file, empty when it has no cash transactions. Daily offline files never contain `CashTransaction.txt`. Saved stream records use compact UTF-8 JSON Lines with the following fixed field order and a final LF after each record:

| Field | Value |
| --- | --- |
| `ct_id` | Integer `CT_ID` from the global sequence shared with the initial file |
| `ct_ca_id` | Integer natural account ID; its decimal text is the publisher's Kafka key |
| `ct_dts` | UTC timestamp as `YYYY-MM-DDTHH:MM:SSZ` |
| `ct_amt` | Signed decimal text with exactly two decimal places |
| `ct_name` | `Deposit`, `Withdrawal`, `Trade settlement <trade_id>`, or `Dividend <symbol>` |
| `schema_version` | Integer `1` |

Stream files and records follow `(ct_dts, ct_id)` order. Business time stays separate from publisher delivery time. Initial cash transactions have `CT_ID` first and no CDC fields. Daily OLTP files have `CDC_FLAG` and `CDC_DSN` before their business fields; saved stream records have neither.

## Configuration

Configuration holds numbers and distribution choices. Code holds business rules. Source definitions hold field order, types, separators, widths, null rules, and CDC support. Unknown keys and executable expressions are not configuration interfaces.

| Settings | Meaning and defaults |
| --- | --- |
| `initial_start`, `initial_end`, `daily_start`, `daily_end` | Inclusive dates; `2014-01-01`, `2018-12-31`, `2019-01-01`, `2025-12-31` |
| `scale_factor`, `customers_per_unit`, `seed` | Firm size, 3,300 cutover customers per unit before calibration, and nonnegative 63-bit seed |
| `account_shares`, `profile_shares`, `tier_shares`, `tax_shares` | Account-count, behavior-profile, tier, and account-tax-status distributions |
| Annual population rates | Net growth 8%, account closures 3%, customer inactivation 2%, customer and account updates 5% each |
| `annual_trade_rates`, `order_shares` | Profile rates 4, 24, 12, 150, 750; market buy, limit buy, limit sell, market sell shares 20%, 30%, 30%, 20% |
| Activity and cash flow | Per-account activity variation; profile deposit and withdrawal rates; funding and trade-size fractions of wealth; weekend multiplier 0.4; tax-season multiplier |
| Wealth and margin | Wealth bounds, margin share 20%, and wealth-based borrowing-limit bounds |
| Fees | Charge 100 cents, commission 10 basis points, and zero transaction tax |
| Watches and prospects | Watch rate 0.1 per customer-day; watch lifetimes; account-closure cancellation share; prospects about twice active accounts; customer overlap about 30% |
| Market | 1,250 companies, 2,000 securities, price bounds, volatility, about four shock days per year, shock trading multiplier 3, security popularity |
| Dividends | Paying-security share 40%; annual yield bounds 1% to 4%; quarterly payments |
| `customer_group_size`, `workers`, `threads` | Execution settings: 2,048 customers, one worker, two Polars threads |
| `export_rows`, `merge_fan_in` | Aggregate export row buffer 8,192; at most 16 merge inputs |
| Resource budgets | Aggregate process-tree RSS 2,048 MiB, disk 65,536 MiB, timeout 43,200 seconds |

Profile arrays use this order: dormant, long-term investor, retiree, active trader, and day trader. Account-count shares refer to one, two, and three accounts. Tier shares refer to tiers 1, 2, and 3. Tax-status shares refer to codes 0, 1, and 2.

The loader rejects duplicate keys, unknown keys, booleans in numeric fields, nonfinite values, invalid probabilities, invalid distribution sums, and source-width overflow. It requires `initial_start <= initial_end`, `daily_start <= daily_end`, and `daily_start == initial_end + 1 day`. Both periods contain at least one date. Calendar dates outside the supported range fail before generation.

Population counts use explicit decimal rounding. A fractional factor must still produce at least one customer, account, prospect, and broker. The population plan anchors the firm at cutover, derives the earlier and later customer counts from net growth, and replaces eligible closed accounts. Customer inactivation contributes to account closures; it is not an additional 3% closure rate. Tier assignment ranks total joining wealth within monthly joining groups, with a separate group for customers present at `initial_start`; equal wealth sorts by customer ID.

Scale factor changes the firm, not market or reference size. A shorter calendar keeps the requested cutover firm size and produces less activity. Changing group size, worker count, threads, export row bound, or merge fan-in does not change source bytes.

## Simulation and State

The coordinator creates shared market history and a compact population plan before customer-group execution. NumPy structured arrays store the plan, and read-only memory-mapped arrays supply shared market values. Workers keep numeric balance, reservation, and holding buffers, with indexes for live orders, holdings, and watches. Python row functions encode source records. Polars builds hidden-state Parquet files and runs typed validation queries.

| State | Retained content |
| --- | --- |
| Customer and account plan | IDs, dates, hidden traits, joining tiers, borrowing limits, and base-rate closure dates |
| Ledger | Signed integer-cent balances and reserved funds per account |
| Holding | One account and security pair: first-buy origin, quantity, cost, and reserved shares; a full sale releases the entry |
| Open trade | Identity, account, security, type, quantity, bid, due date, reservation, status, and transition ordinal |
| Watch | Customer, security, and due removal date |
| Prospect | Persistent identity and customer match; snapshots rendered in bounded blocks |

The first buy creates a holding and supplies its `HH_H_T_ID` origin. Later buys and sells change that holding's quantity. A full sale ends it; a later buy creates a new origin. Every completed trade produces exactly one `HoldingHistory` row. A sell does not split into several trades to consume separate buy lots.

A planning allowance of 32 live holdings and two open trades per account gives `A * (1,024 + 32 * 64 + 2 * 128) + C * (1,024 + 8 * 32) + P * 64` bytes for `A` accounts, `C` customers, and `P` prospects. With 6,144 accounts, 2,048 customers, and a 2,048-prospect block, this is about 22.13 MiB. These allowances are estimates, not measured buffer sizes or behavior caps. Runtime indexes, text, shared pages, and export buffers also consume memory. The summary reports numeric state-buffer bytes and observed state peaks. The supervisor measures aggregate RSS.

Each customer and all related accounts stay in one group. One worker executes groups directly. With two workers, the coordinator and one spawned child execute separate groups. The spawn resource tracker counts toward memory use but does not simulate a group. More groups increase total work without requiring one resident history for the whole firm.

A worker performs these steps for each day:

1. Read released FINWIRE values, NYSE session status, price bounds, dividends, and market shock status. Apply released company and security values before dependent activity.
2. Apply customer and account openings and updates. New accounts start with zero balance and no holdings. Pay dividends at 06:00 UTC from start-of-day holdings; buys later that day do not qualify.
3. Create deposit, withdrawal, trade, and watch intents from stable named random values. Opening funding occurs at 08:00 before the first buy. Ordinary cash flow occurs between 08:00 and 22:00 UTC.
4. Execute trade and cash-flow intents in timestamp, account, ordinal, and kind order. Reserve funds for buys and shares for sells. Reduce or reject proposals that exceed available funds or unreserved holdings.
5. Complete a trade as one business operation: release its reservation, apply settlement, update the holding, and record the completed trade and holding change. Execution stays within the daily low and high and respects the limit. Buys subtract value and fees; sells add proceeds minus fees. Both account types settle.
6. On a closure date, cancel open orders, sell all holdings, deposit enough to repay borrowing, withdraw the remaining balance, then record closure. These actions occur within the closure day's hours, without an extended leaving period. Customer inactivation also cancels all watches; individual account closure cancels a configured share.
7. Finish watch placements and removals and customer changes. At cutover, export the latest state of each unfinished trade. Carry only live state and bounded export buffers into the next day.

A cash account cannot spend below zero. A margin account cannot exceed its borrowing limit. Reservations prevent a second proposal from spending the same funds or shares. No short selling occurs. Trades other than `CMPT` have NULL execution price, charge, commission, and tax.

Generation stage 2 has planned attachment points in population closure selection and `Stage1IntentPlanner`. The planner receives the simulation context before closure selection, ordinary deposits, withdrawals, trade submission, and account-related watch cancellations. Opening funding and required closure repayments still follow business constraints. Worker factories accept a replacement planner and recorder, so each group applies the same policy before business execution. Generation stage 3 has a planned attachment point in `SourceRecorder`. Its transform receives immutable correct business fields and returns recorded fields without changing the ledger. It covers trades, initial trade history, holding changes, watches, and daily customer and account rows. Initial customer XML and FINWIRE use their encoding functions directly. The identity transform and the 14-business-field trade format apply to all run dates, including dates after `2019-07-01`. These interfaces do not add data errors, attrition signals, or `T_CHANNEL`.

## Calendar, Identity, and Export

NYSE session dates come from pinned `exchange-calendars`, calendar `XNYS`. The simulation puts exchange wall-clock hours on a fixed UTC clock without daylight saving time: 09:30 opening, 16:00 normal close, and 13:00 early close. Every daily delivery date exists even when the market is closed.

| Inclusive dates | Calendar dates | NYSE trading days |
| --- | ---: | ---: |
| `2014-01-01` to `2018-12-31` | 1,826 | 1,258 |
| `2019-01-01` to `2025-12-31` | 2,557 | 1,760 |

Calendar tests include the unscheduled closures on `2018-12-05` and `2025-01-09`. Initial market prices include one prior calendar year. FINWIRE starts five quarters before the quarter containing `initial_start`: initial company and security postings precede released reports, and each report releases on the last day of the following month. Quarterly SEC postings carry effective dividend values. The metadata records the dependency version, session counts, and calendar fingerprint.

| Identity or output | Assignment and ordering |
| --- | --- |
| Customer and account IDs | Stable population ordinals and account slots; IDs are not reused |
| `T_ID` | Account ID times 1,000,000 plus lifetime trade ordinal, with checked bounds below `10^15` |
| Initial trades and trade history | Trade ID; history also sorts by status timestamp and transition ordinal |
| Initial customer XML | Action timestamp, customer ID, and action ordinal; omitted and empty elements stay distinct |
| `CT_ID` | Global `1..N` sequence in timestamp, account ID, local ordinal, and kind-rank order, continuing across cutover |
| Daily `CDC_DSN` | `1..N` within each file after full business-order sorting |
| FINWIRE | `PTS`, then `CMP`, `SEC`, `FIN` dependency order, then natural ID |
| Prospects and references | Stable natural ID; market rows sort by date and security |

Named random values include stable entity identities, dates, purposes, and local ordinals. No worker shares a mutable random-number stream. The semantic dataset identity includes business configuration, seed, format version, source definitions, and calendar fingerprint. Metadata also records code hashes and dependency versions. Runtime settings and output location do not enter semantic identity.

The exporter holds at most `export_rows` buffered records across outputs. Sorted runs and a SQLite catalog keep delivery-path metadata on disk. Multilevel merges open at most `merge_fan_in` inputs, rather than one file per date. Duplicate complete keys fail export. Final writers check source widths and encoding, assign sequences, and compute record counts, bytes, and SHA-256. The initial trade extract freezes each trade at its latest state through cutover; later outcomes appear only in daily CDC rows.

The runner refuses an existing output directory or its `.partial` counterpart. It writes under `.partial`, supervises aggregate process-tree RSS and worker count every 0.1 seconds, and samples disk use every two seconds. Resource sampling is not a kernel memory limit. A budget violation stops the process group. Failed runs retain logs, configuration, and partial evidence; they do not resume automatically. After file validation and metadata writes succeed, the supervisor removes `_work/` and renames the directory.

## Validation and Tests

Validation reads saved source files and stream payloads, derives its own expected-file list, and compares saved counts and hashes. It stages narrow typed records into bounded Parquet blocks, partitions account and customer replay, and joins market prices by date. Parquet writers share a 262,144-row buffer allowance across concurrent partitions, with at most 8,192 rows per row group. A streaming query alone does not bound these writer buffers. Every partition is checked. The 16 MiB source-byte partition target is an average, not a hard replay-memory bound; one large account history can exceed it. Hidden account Parquet files supply account type, wealth, borrowing limit, and lifetime attributes; the validator reconstructs balances and holdings from source records rather than accepting hidden balances as proof.

| Check | Evidence |
| --- | --- |
| File contracts | Exact coverage, cadence, widths, separators, ASCII-compatible source text, LF endings, XML structure, JSON field order, and saved hashes |
| Identities and lifetimes | Unique identities, valid references, customer and account versions, fixed account type, and no activity after closure |
| Trades | Legal transitions, equal-time transition order, cutover snapshots, NYSE session hours, limit constraints, and execution price bounds |
| Cash transactions | Global identity sequence, one correct settlement per completed trade, chronological cent replay, spending reservations, and margin limits |
| Holdings | Correct account and security origin, matching before and after quantities, exactly one change per completed trade, and no short selling |
| Dividends and watches | Start-of-day dividend obligations and amounts, effective released dividend values, watch lifetimes, and inactivation cancellation |

Tests cover strict configuration, calendar exceptions, decimal rounding, monthly joining tiers, population counts, XML null semantics, spending reservations, holding conservation, cash transaction sequences, bounded export, corruption detection, and retained failure evidence. Hypothesis checks holding changes, ledger spending, and stable random values. Complete-run tests compare bytes across group sizes 1, 7, and 2,048, worker counts 1 and 2, thread counts, merge fan-in, and export row bounds. Dates on both sides of `2019-07-01` confirm that M1 keeps one trade contract.

## Measurements and Volume Calibration

The generator summary separates settings from measured results. `manifest.json` holds file counts, bytes, hashes, implementation hashes, versions, phase times, and semantic identity. `summary.json` groups rows and bytes by source family, date, and initial, daily, or stream delivery. It also records trade counts by date, cash transaction types, profile counts, rejected proposals, state peaks, and account and security trade shares. `supervision.json` records sampled aggregate RSS, worker count, total disk, and combined temporary disk peaks for `_work/` and validation Parquet files.

Phase timings include the work below. Buffer flushes and intermediate merges occur inside the phase that emits the records. The `export` phase measures the final merges and file writes, not all export work.

| Phase | Work | Execution |
| --- | --- | --- |
| `reference` | HR, Date, Time, and lookup files | Coordinator |
| `market` | Shared prices, dividends, and FINWIRE | Coordinator; independent of firm size |
| `population` | Customer and account lifetime plans | Coordinator |
| `activity` | Business execution, source encoding, buffer flushes, and worker-run adoption | Customer groups; one or two workers |
| `prospects` | Prospect snapshots and source encoding | Coordinator |
| `export` | Final merges, CT_ID and CDC_DSN assignment, width checks, and hashes | Coordinator |
| `validation` | Saved-file parsing, typed staging, price joins, and state replay | Coordinator, with Polars query threads |

### Reference-Calendar Measurements

One uncalibrated factor 1 run over the reference calendar has been measured, with two workers. It passed full validation and produced the same bytes as a one-worker run. Factor 5 and the calibrated runs have not been measured.

| Calibration | Factor | Customers per unit | Offline bytes | Stream bytes | Generation seconds | Validation seconds | Peak RSS MiB | Peak temporary GiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before | 1 | 3,300 | 1,302,815,601 | 561,699,602 | 1,284.40 | 948.49 | 1,279.64 | 2.562 |

Fixed market and reference families make up 430,169,772 of the offline bytes, so the firm contributes about 0.87 GB per unit before calibration. The run has 19,948,464 offline rows and 4,095,861 stream rows. Cash transactions over the whole calendar split into 2,693,939 dividends, 2,110,974 settlements, 521,178 deposits, and 176,860 withdrawals. In 2018 a normal session has 641.8 trades and a market-shock session 1,850.5; the top 1% and 10% of accounts make 24.0% and 64.0% of trades, and the top 1% of securities receive 23.3%.

### Runtime Profile

A `cProfile` run of one 128-customer group over the reference calendar takes 79.83 seconds under profiling and records about 114.8 million calls. The cumulative times below overlap because a caller's time includes its callees. They are not unprofiled throughput estimates.

| Function work | Cumulative seconds |
| --- | ---: |
| Recording source rows | 26.96 |
| Checking source records | 18.77 |
| Scalar memory-map indexing | 8.73 |
| Flushing sorted buffers | 3.57 |
| Committing the sorted-run catalog | 1.49 |
| Creating 8,540 sorted-run files | 0.64 |

Python business execution, row encoding, field checks, and scalar indexing consume more time than file creation in this group. Large run counts also cause catalog operations, file metadata operations, and merge work. Final export and full-file validation have separate phase measurements above. The run catalog and bounded merge fan-in limit resident paths and open merge inputs; they do not remove the work needed to read and write records.

### Calibration Rule

One GB per unit is the firm's variable offline contribution; fixed market and reference bytes come on top. Use full-reference-calendar factor 1 and factor 5 runs with equal market settings, behavior settings, and seed. For measured offline byte totals `B1` and `B5`, variable bytes per unit are `(B5 - B1) / 4`, and fixed bytes are `B1 - variable_bytes`. Measure fixed source families directly as a cross-check.

Set the next `customers_per_unit` to `old_customers_per_unit * 1,000,000,000 / variable_bytes`, use decimal half-up rounding, and rerun factor 1. Project factor 5 from the fitted fixed and variable contributions. If the calibrated factor 1 offline bytes differ from their projected total by more than 10%, rerun factor 5. Population sampling and staffing rounding can make this fit approximate. A short calendar does not calibrate the 12-year target. Do not change activity rates, duplicate rows, or change fixed market size to force a byte total. The factor 100 requirement counts at least 100,000,000,000 offline bytes; saved stream bytes, metadata, and temporary files do not count.

## Accepted Business Choices

These choices define how the generator handles the source contracts and unspecified business policies.

| Choice | Rule | Reason |
| --- | --- | --- |
| Volume basis | Variable firm bytes target one GB per unit; fixed data adds a measured intercept | Market size does not depend on firm size |
| Prior prices | Initial `DailyMarket.txt` includes one prior calendar year | The first simulation date needs 52-week prices |
| Holdings | One holding per account and security; first buy supplies the origin; full sale ends it | One completed trade maps to one holding change without split sells |
| Closure order | Cancel orders, sell holdings, repay borrowing by deposit, withdraw, then close | No holding or debt disappears without a source record |
| Early close | Trading ends at 13:00 UTC on NYSE early-close dates | Session hours follow exchange dates under the fixed-clock rule |
| Tier, tax, and margin | Rank joining groups by wealth; configurable borrowing limits; zero transaction tax with nonzero charge and commission | Explicit policies keep execution repeatable and settlement checks precise |
| Watch cancellation | Customer inactivation cancels all watches; account closure cancels a configured share | Watches belong to customers, not individual accounts |

## Known Limits

This version implements generation stage 1 only. These limits are known and not yet addressed:

- **Personal text is placeholder text.** Customer, prospect, and account names use templates such as `Family<id>` and the single first name `Alex`. Realistic names and addresses need word lists picked by the repeatable random function.
- **Dividends dominate cash transactions.** Dividends are about 49% of all cash transactions, against about 85% settlements in the design. Accounts hold many paying positions, and each pays quarterly. This makes the saved stream about three times the designed size and inflates inflows.
- **Volume is uncalibrated.** `customers_per_unit` is still 3,300, which gives about 0.87 GB per unit instead of 1 GB.
- **Runtime.** Python per-event loops dominate. A factor 100 run would take about 60 hours with two workers; array processing and more workers are the planned remedies.
- **Readability.** `BusinessSimulation.run` and the validator's account replay are long functions with positional tuples and unnamed codes.
- **Unused pins.** `duckdb` and `pyarrow` are pinned and listed in run metadata but not imported.
- **Later stages.** The attrition signal (stage 2), data errors and the `T_CHANNEL` format change (stage 3), and the publisher (stage 4) are not built. The initial customer XML and FINWIRE do not yet pass through `SourceRecorder`.
