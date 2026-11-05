# Brokerage Source Generator

The generator simulates a retail brokerage one day at a time and writes connected source records with Polars. It runs generation stages 1 to 3: business behavior, attrition signal, and source recording. The separate publisher runs generation stage 4 and delivers the saved data without running the simulation again.

[Data Platform section 2](../../../docs/Data-Platform.md#2-data-generation-and-problem-injection) owns the business rules. [Data Specification](../../../docs/Data-Specification.md#3-physical-source-files-and-ingestion-rules) owns the source contract. This guide defines the execution design; the review points at the end need the owner's decision.

## Output Layout

Only files under `offline/` become objects in `upstream`. Removing the `offline/` prefix gives the exact object key. The publisher never uploads the summary, metadata, temporary files, or saved stream files.

```text
<dataset>/
  offline/
    initial/                         # Original source names
    daily/YYYYMMDD/                  # Dated source names
  stream/cash_transactions/YYYYMMDD.jsonl
  summary.json
  summary/hidden/                     # Partitioned hidden state and correct values
  config.json
  manifest.json
  validation.json
  supervision.json
  worker.log
  _work/                             # Temporary state and sorted runs
```

| Delivery | Expected offline files |
| --- | --- |
| Initial load | `CustomerMgmt.xml`, `Trade.txt`, `TradeHistory.txt`, `CashTransaction.txt`, `HoldingHistory.txt`, `WatchHistory.txt`, `DailyMarket.txt`, `HR.csv`, `Prospect.csv`, the six reference files, and each released FINWIRE posting-quarter file |
| Every daily delivery | `Customer_YYYYMMDD.txt`, `Account_YYYYMMDD.txt`, `Trade_YYYYMMDD.txt`, `HoldingHistory_YYYYMMDD.txt`, `WatchHistory_YYYYMMDD.txt`, `DailyMarket_YYYYMMDD.txt` |
| First day of each month | The daily set plus `Prospect_YYYYMMDD.csv` |
| FINWIRE release date | The applicable daily set plus `FINWIREyyyyQq`; its year and quarter follow `PTS`, not the financial reporting period |

For example, Q1 2019 reports released April 30 go to `daily/20190430/FINWIRE2019Q2`. The initial prospect snapshot represents `initial_end`. HR and the six reference files load once. The output contains no benchmark audit files or `BatchDate.txt`.

Every expected file exists, including a zero-record file when nothing happens. A source that is not due has no file. Each daily delivery date also has a saved stream file, empty when it has no cash transactions; this file is not an offline source file.

The saved stream uses UTF-8 JSON Lines, one compact JSON object per line, with fixed field order and a final LF after each record. Its six fields match the [cash transaction message contract](../../../docs/Data-Platform.md#topics-and-cash-transaction-contract):

| Saved field | Source value and encoding |
| --- | --- |
| `ct_id` | `CT_ID`, an integer from the global cash transaction sequence |
| `ct_ca_id` | `CT_CA_ID`, an integer; the publisher uses its decimal text as the Kafka key |
| `ct_dts` | `CT_DTS`, formatted as `YYYY-MM-DDTHH:MM:SSZ` |
| `ct_amt` | `CT_AMT`, signed decimal text with exactly two decimal places |
| `ct_name` | `Trade settlement <trade_id>`, `Deposit`, `Withdrawal`, or `Dividend <symbol>` |
| `schema_version` | Integer `1`; independent of the trade file's format version |

Files and records follow `(ct_dts, ct_id)` order across the whole daily delivery period. The publisher reads them in that order and maps business time to its replay clock. A clean replay preserves the JSON payload; a realistic publisher profile changes delivery time or repeats the same payload and `CT_ID`. Kafka timestamps describe delivery time, not `ct_dts`. Endpoint settings and delivery problem rates belong to the publisher, not generator configuration.

## Configuration

A run configuration holds numbers and choices of distributions. Code holds business rules. Source definitions hold field order, types, delimiters, widths, null rules, CDC support, and dated format versions. None of these files contains executable expressions.

| Settings | Meaning and default basis |
| --- | --- |
| `initial_start`, `initial_end`, `daily_start`, `daily_end` | Inclusive UTC dates; defaults `2014-01-01`, `2018-12-31`, `2019-01-01`, `2025-12-31` |
| `scale_factor`, `customers_per_unit`, `seed` | Firm size, initially 3,300 cutover customers per unit, and a nonnegative 63-bit seed; positive fractional factors support tests |
| Firm population | Account mix 50% with one account, 33% with two, 17% with three; net annual growth about 8%, new accounts about 11%, closures about 3%, customer inactivation about 2%, and customer and account updates about 5% each |
| Staffing and prospects | About 1,000 cutover accounts per broker, four HR employees per broker, prospects about twice the account count, and about 30% of joining customers present as earlier prospects |
| Account traits | Profile shares 30%, 45%, 15%, 9%, 1%; wealth distribution and within-profile variation; margin share about 20% with profile effects; tax shares 70%, 10%, 20% for codes 1, 0, 2; tier shares 10%, 30%, 60% |
| Trading | Profile annual rates 4, 24, 12, 150, 750; order shares 20% market buy, 30% limit buy, 30% limit sell, 20% market sell; outcome targets 92.7% complete, 5.8% canceled, 1.5% open; order lifetimes, quantity and fee distributions, and wealth-based margin limits |
| Cash flow and watches | Opening funding, profile deposit and withdrawal schedules and amounts, payday and tax-season multipliers, weekend activity multiplier 0.4, watch rate about 0.1 per customer per day, watch lifetimes, and security popularity distribution |
| Market, separate from firm size | 1,250 companies, 2,000 securities, price and earnings distributions, about four shock days per year with trading multiplier 3, popularity giving the top 1% about 25 to 35% of trades |
| Dividends and releases | About 40% paying securities, annual yield mostly 1 to 4%, quarterly payment dates, monthly prospects, FINWIRE release lag, and five quarters of earnings support before the first simulation quarter |
| Attrition signal | Profile, tenure, tier, and loss risk multipliers; 80% warned closures; leaving duration 1 to 8 weeks; 1 to 4 liquidation steps; look-alike rate 2% per year and sale share 20 to 50%; signal-check thresholds |
| Source recording | Tier error 0.2%, birth-date error 0.1%, rating error 1%, fee error 0.1%, commission error 0.1%; trade format change date `2019-07-01` and channel probabilities by profile |
| Execution only | Customer group size, worker count, Polars threads, export row bound, merge fan-in, memory and disk budgets, and timeout; these do not change source bytes |

The loader rejects unknown or duplicate keys, booleans in numeric fields, nonfinite values, invalid probabilities, shares that do not sum to one, and invalid source-width bounds. It requires `initial_start <= initial_end`, `daily_start <= daily_end`, and `daily_start == initial_end + 1 day`. Dates outside the calendar's supported range fail before generation. Both periods contain at least one date.

Population counts use explicit decimal rounding, not binary floating-point rounding. A fractional factor must still produce at least one customer, account, prospect, and broker. Market counts remain independent settings. The loader checks identifier capacity and worst-case configured event expansion before it starts a worker. Configuration records both the requested factor and the resulting integer counts.

The population plan anchors the requested firm size at `initial_end` and derives earlier and later populations from annual growth and closure exposure. A shorter time span keeps that cutover firm size and emits less activity. All annual rates use calendar-day exposure; trading activity uses the year's actual NYSE session count. Gross openings and closures produce net growth; the generator does not apply 8% and then add another 11%.

## State and Memory

A customer and all of that customer's accounts stay together because tier assignment and customer inactivation span accounts. Workers simulate bounded customer groups through the calendar. Completed history goes to sorted disk runs rather than an ever-growing in-memory history.

| State | Polars representation and retained content | Planning bytes |
| --- | --- | ---: |
| Customer and current version | Integer IDs, dates, tier, status, identity values, version counters; older versions on disk | 1,024 per customer |
| Account and current version | IDs, dates, profile code, type, wealth, signed integer-cent balance, borrowing limit, reservations, due actions, version counters, and a 90-day cash-flow-adjusted value index for loss risk | 1,024 per account |
| Holding lots | One row per live buy origin: account, security code, origin trade ID, quantity, cost, timestamps, and reserved shares; remove exhausted lots | 64 per lot |
| Open trades | IDs, account, security, type, quantity, bid, state, due time, and reserved funds or shares; discard terminal state after export | 128 per trade |
| Watches | Customer, security code, placement time, and scheduled removal time | 32 per active watch |
| Prospects | Stable ID, identity seed, matching customer ID, dates, and change counters; render snapshots in blocks | 64 per prospect |

State uses typed numeric columns, dictionary codes for repeated text, and explicit nulls, never Polars `Object` columns or one Python object per historical record. Money uses signed `Int64` cents with checked arithmetic. Customer and account updates append source versions before replacing current values. Prospect identities persist across snapshots; matching prospects precede joining dates without exposing future customer status.

The planning case uses 32 live lots and two open trades per account, eight watches per customer, 1.67 accounts per customer, and two prospects per account. At the reference calendar's end, `550,000 * 1.08^7` gives about 943,000 accounts at factor 100. Thus `A` accounts, `C` customers, and `P` prospects need `A * (1,024 + 32 * 64 + 2 * 128) + C * (1,024 + 8 * 32) + P * 64` bytes if all state is resident.

| Factor | End accounts | End customers | End prospects | Whole-firm state estimate, MiB |
| --- | ---: | ---: | ---: | ---: |
| 1 | 9,430 | 5,647 | 18,860 | 37.97 |
| 5 | 47,150 | 28,234 | 94,300 | 189.87 |
| 100 | 943,000 | 564,671 | 1,886,000 | 3,797.33 |

These are planning estimates, not measured simulator peaks. A numeric-buffer spike measured 64 bytes per lot, 128 per open trade, and 32 per watch. Its 196,608 lot rows occupied 12 MiB. It did not measure strings, joins, or simulation behavior.

A 2,048-customer group with 6,144 active accounts and a separate 2,048-prospect block needs about 22.13 MiB by the same formula. Three concurrent copies need 66.38 MiB. A worker budget reserves another 384 MiB for shared market pages, 256 MiB for bounded export and merge buffers, and 128 MiB for runtime overhead: about 835 MiB, rounded to a 1 GiB RSS limit. One worker is the default at factors 1, 5, and 100; larger factors add groups, not a larger resident firm.

The estimates are not caps on behavior. Live lots and pending orders can exceed them. The runner checks state high-water marks and aggregate process-tree RSS against a 2 GiB run budget. A run that exceeds a budget fails with retained partial evidence; it never drops lots or changes events to fit memory. Measurements compare smaller groups before increasing concurrency. A single customer's excess state requires a disk-backed state design before that workload is supported.

## One Simulated Day

A worker processes days in order within its customer group. It uses the same event executor for ordinary activity, leaving periods, and look-alikes, so all paths enforce the same balance and holding rules.

1. Read shared company and security postings due that date, the NYSE session flag, market shock state, and daily low, high, close, and dividend per share. Apply released FINWIRE values before dependent activity. The shared market calculation has no dependency on a worker's customer group.
2. Apply due customer and account changes. New accounts start with zero cash and no holdings. Customer inactivation closes all remaining accounts together; closure requires completed liquidation and cleared obligations under the policy below. Compute tier from total joining wealth across the customer's accounts, using a stable cohort-wide wealth ranking before group simulation.
3. At 06:00 UTC, pay dividends from start-of-day holdings and the effective annual dividend per share divided by four. New buys that day do not qualify. Record one cash transaction per account and paying security, rounded once to cents. A fixed quarterly payment date moves to the next NYSE trading day.
4. Create time-ordered deposit, withdrawal, trade, and watch intents from profile schedules. Process ordinary cash flow mostly from 08:00 to 22:00 UTC. An opening deposit must occur before a cash account's first buy. A deposit changes funds available only from its timestamp onward.
5. On a trading day, submit trades from 09:30 to 16:00 UTC with peaks at the open and close. Process older open orders and new orders in `(timestamp, account ID, local ordinal)` order. Market orders complete; limit orders can complete, cancel, or remain open. Completion prices stay within that day's low and high and respect the order's limit.
6. Reserve funds or shares at submission and recheck at completion. For a cash account, available spending equals balance minus reservations. For a margin account, add its wealth-based borrowing limit. Reduce or skip an unaffordable proposal before submission; record the reason in the summary. Release reservations on cancellation. A pending order cannot reserve the same money or shares twice.
7. At completion, apply settlement and the holding change as one business operation. Buys subtract execution value and correct charges; sells add proceeds minus correct charges. Both account types settle. A buy creates a lot; a sell uses one existing lot and cannot exceed its unreserved shares. Larger sales use separate trades, each with its own `T_ID`, so one completed trade produces one holding row.
8. Apply withdrawals only against funds available at their timestamp, including permitted margin borrowing. Process watches against current customer and security identities; a removal finds its original placement across source versions. Finish due updates, write that day's records, and carry balances, live lots, open orders, watches, current versions, and prospect changes forward.

No short selling occurs. A withdrawal or settlement cannot make a cash account negative or exceed a margin account's limit. The executor rejects an invalid operation before it changes state. Trades with status other than `CMPT` have NULL execution price, charge, commission, and tax.

Generation stage 2 attaches at intent planning, before business events commit. It selects closure candidates from profile, tenure, tier, and trailing investment losses excluding deposits and withdrawals, calibrates total closures to about 3% per account-year, and schedules leaving behavior before closure. Customer inactivations count within that total, not in addition to it. Warned accounts stop deposits and buys, sell in 1 to 4 steps, and withdraw proceeds within a day; look-alikes keep normal deposits and trading. Scheduling uses information available on the decision date, not future returns. End-of-run leaving periods remain right-censored in the summary.

Generation stage 2 changes future behavior, not an already committed business event or the executor's interface. Generation stage 3 receives immutable business events and changes only recorded fields. Its independent named random values select each data error; correct amounts remain in local summary partitions. Neither stage feeds a wrong recorded fee back into settlement. Hidden profiles, wealth, leaving periods, and look-alike flags never enter source records.

## Parallelism and Repeatability

The coordinator makes the shared market history and the compact population and tier plan first. Market history uses daily read-only columnar blocks and a bounded cache; workers read the same blocks. Prospect snapshots use a separate bounded pass. Workers own disjoint customer ID ranges, including those customers' future accounts, and return sorted runs. They do not share mutable balances or a global random-number stream.

Random values use the existing named BLAKE2 salt and fixed unsigned integer mixing. A draw includes stable entity identity, business date, purpose, and local event ordinal. Changing group size, worker scheduling, thread count, merge fan-in, or output location cannot change a draw. Global tier ranks break equal-wealth ties by customer ID, not input order. Prices and amounts quantize to integer cents before state changes; exports never reduce floating-point amounts in worker-dependent order.

| Identifier | Assignment and use |
| --- | --- |
| Customer and account IDs | Stable population-plan ordinals and account slots; the plan includes replacement accounts and never reuses an ID |
| `T_ID` | Checked composite of account ID and its lifetime trade ordinal, using a fixed reserved stride; preflight and runtime checks reject capacity overflow rather than hashing into a collision |
| Related trade fields | `TH_T_ID`, `HH_T_ID`, and `Trade settlement <trade_id>` carry exactly that `T_ID`; `HH_H_T_ID` carries the completed buy's ID for the same account and security |
| `CT_ID` | Final cash transaction merge assigns `1..N` in `(timestamp, account ID, local event ordinal, kind rank)` order, once across initial and streamed dates; cutover does not reset it |
| `CDC_DSN` | Final merge assigns `1..N` within each daily file after the full event-order sort; initial files and stream messages have no CDC fields |

The trade stride and event-kind order are versioned code constants, not execution settings. The reserved stride is 1,000,000 trade slots per account; checked account and ordinal bounds keep `T_ID` below 10^15. Source-width checks cover the largest composite ID. A local event ordinal preserves causal order when several cash transactions share a second. The cash transaction merge writes the initial file first, then dated JSON Lines files while carrying its sequence counter forward. It does not need a shared live ID allocator.

A semantic identity includes normalized business configuration, seeds, generator format version, source definitions, and the calendar fingerprint. Run metadata also records code hashes and dependency versions. Equal semantic settings, seed, and generator version give equal source bytes for every supported execution configuration. Wall time, logs, resource samples, and worker counts are not part of byte comparison.

## Ordered Export and Failure

The exporter extends the POC's bounded sorted runs and multilevel merge. It orders each bounded record frame by a complete, unique key, writes the run, and compacts levels with limited fan-in. It never collects all records for a source or keeps one open file per delivery date.

| Output | Final ordering |
| --- | --- |
| Initial customer XML | Action timestamp, customer ID, action ordinal; preserve omitted and explicitly empty elements |
| Initial trades and trade history | `T_ID`; history also uses status timestamp and transition ordinal |
| Initial holdings and cash transactions | Business timestamp, account ID, event ordinal; cash transactions also receive `CT_ID` |
| Daily CDC files | Business event timestamp, entity ID, transition ordinal, then assigned `CDC_DSN` |
| FINWIRE | `PTS`, dependency order `CMP` before `SEC` before `FIN` at equal timestamps, natural ID |
| Prospects, HR, and references | Stable natural ID; market rows use date and security ID |

The initial trade file freezes each trade at its latest state through `initial_end`; later outcomes appear only in daily CDC rows. Terminal initial trades can export immediately; unfinished trades retain only the state needed at cutover. All transitions through cutover appear in initial trade history. A trade that spans cutover keeps its ID and holding origins.

Duplicate complete sort keys fail export. Final writers enforce encoding, field counts, widths, and LF endings, assign sequence numbers, and compute rows, bytes, and SHA-256 while writing. A disk-backed run catalog bounds metadata across thousands of delivery paths; merging one output uses at most the configured fan-in and one row per input. Successful export removes sorted runs.

The runner refuses to overwrite an output or its `.partial` directory. It writes under `.partial`, validates the saved files, writes metadata, removes temporary validation state, and renames the directory only after success. A failed run retains its log, settings, and partial files. Completion is not automatic resume. The publisher accepts only a validated completed directory.

## Calendar

NYSE session dates come from `exchange-calendars==4.13.2`, calendar `XNYS`. This pinned dependency avoids maintaining observed-holiday rules, Good Friday, Juneteenth's start date, and special closures in project code. The generator uses session dates, not the package's UTC opening times, because the project puts NYSE wall-clock hours on a fixed UTC clock without daylight saving time.

| Inclusive dates | Calendar dates | NYSE trading days |
| --- | ---: | ---: |
| `2014-01-01` to `2018-12-31` | 1,826 | 1,258 |
| `2019-01-01` to `2025-12-31` | 2,557 | 1,760 |

A calendar spike reproduced both counts and excluded both unscheduled closures: [2018-12-05](https://ir.theice.com/press/news-details/2018/New-York-Stock-Exchange-to-Honor-President-George-H-W-Bush/default.aspx) and [2025-01-09](https://ir.theice.com/press/news-details/2024/The-New-York-Stock-Exchange-Will-Close-Markets-on-January-9-to-Honor-the-Passing-of-Former-President-Jimmy-Carter-on-National-Day-of-Mourning/default.aspx), documented by NYSE's owner, Intercontinental Exchange. Calendar tests assert these dates and counts, not just weekday rules. The metadata records the package version and a hash of session dates. Every UTC date still receives a daily delivery, and deposits and withdrawals can occur on closed-market dates.

## Validation and Tests

Validation parses exported files and saved stream payloads, not the generator's live frames. It derives the expected-file set independently from configuration and cadence. Passing generation without passing full validation does not produce a completed dataset.

| Check | Exported evidence |
| --- | --- |
| Files and formats | Exact expected-file set, row counts, widths, delimiters, XML structure, JSON contract, hashes, and source timestamps; CDC fields only on daily OLTP rows |
| Identities and lifetimes | Unique customer, account, trade, and `CT_ID` identities; legal insert and update order; valid references; no activity after closure; fixed account type across trades |
| Cash balances and margin | Ordered cent sums from zero; cash accounts never negative; reservations cannot overspend; margin exposure within its configured limit |
| Trades and settlements | Legal status transitions and cutover snapshots; one settlement per completed trade for both account types; signed value equals quantity times execution price with correct charges; non-completed amounts NULL |
| Holdings and prices | Completed buy origin in the same account and security; before quantity equals prior after quantity; trade quantity matches the change; no negative holdings or short selling; execution within daily high and low |
| Dividends and watches | Dividend equals start-of-day shares times effective quarterly amount; valid watch placement and removal across versions |
| Recording errors | Each exception matches a saved field-level correction and configured error kind; no blanket exemption for the row; all unrelated checks still apply |

The local correction records supply correct values only where generation stage 3 deliberately changes them. Hidden account type and margin limits support checks that source rows alone cannot prove. The validator cross-checks visible type values against `T_IS_CASH`; it does not treat hidden balances or holdings as proof of its own replay.

For large runs, bounded parsers first check formats and stage narrow typed records on disk. External sort and merge joins check global identities and references; stable customer partitions let the validator replay accounts, lots, and watches with bounded state. Shared market and FINWIRE indexes support date-sensitive joins. Validation writes columnar blocks in batches rather than doing a SQLite lookup for every row. Every partition is checked; sampling is only for exploratory summaries. The SQLite validator remains a small-run comparison during this change, not the 100 GB execution plan.

Unit tests cover date adjacency, count rounding, calendar exceptions, cent rounding, spending reservations, single-lot sales, lifecycle transitions, XML null semantics, and dated formats. Small end-to-end tests parse all source families, corrupt one exported value at a time, and check failure behavior. Property-based tests with `hypothesis` generate event sequences and assert conservation, no short selling, unique IDs, and unchanged results under grouping; its version is pinned when those tests are added. Repeatability tests compare actual bytes across group sizes 1, 7, and 2,048, worker counts 1 and 2, threads, and merge fan-in, including cutover and schema-change dates.

The [POC measurement](../../../workflow/records/Polars-POC.md) generated and exported 1.01 GB in 73.88 seconds; SQLite validation took 297.77 seconds, about four times longer. Holding that old validation cost per byte constant would take about 8.18 hours for 100 GB. This is a cost illustration, not a runtime forecast for the new model. Factor 1 and factor 5 measurements compare both validators' results and report validation time, temporary disk, parse throughput, and peak RSS before a large run.

## Measurements and Volume Calibration

The generator summary reports settings and measured results separately. Hidden records live in bounded local partitions, not one large JSON array. The platform reads neither them nor the summary.

| Measurement | Contents |
| --- | --- |
| Files and stream | Rows, uncompressed bytes, and hashes per path; totals by source and date; initial and daily offline bytes separate from saved JSON stream bytes |
| Firm and behavior | Population by date, account-years, growth, updates, closures, profile and tier counts, wealth distributions, rejected proposals, open trades, live lots, and watch counts |
| Data characteristics | Account and security key frequencies, top 1% and top 10% shares, shock-day peaks, transaction types, distinct-to-total ratios, completion and cancellation rates, and observed error rates with denominators |
| Hidden state and corrections | Profile and wealth assignments, closure and leaving decisions, look-alikes, right-censoring, and correct and recorded values keyed to each data error |
| Runtime | Phase times, aggregate process-tree RSS, per-table state high-water marks, largest frame, retained run paths, merge fan-in, temporary disk, and dependency and calendar fingerprints |

Calibration uses full-reference-calendar runs at factors 1 and 5 with the same market settings, behavior settings, and seed. Let their measured offline byte totals be `B1` and `B5`. The variable bytes per unit are `(B5 - B1) / 4`; fixed bytes are `B1 - variable_bytes`. Measure fixed source families directly as a cross-check. Set the next `customers_per_unit` to `old_customers_per_unit * 1,000,000,000 / variable_bytes`, round explicitly, and rerun both factors. Growth-related staffing rounding and population sampling can make the fit approximate.

A shorter calendar is not a calibration run for the 12-year volume target. Do not inflate activity rates, duplicate rows, or change fixed market size to force a byte total. Confirm the final factor 100 offline files reach at least 100,000,000,000 bytes; saved stream bytes, metadata, and temporary files do not count. Repeat final volume measurement after attrition and source recording change record counts or widths.

## POC Code to Keep and Replace

The POC supplies the bounded execution tools, not the final business schedule. Its historical rules and measurements remain in the [POC record](../../../workflow/records/Polars-POC.md).

| Existing module | Design use |
| --- | --- |
| `config.py`, `random_values.py` | Keep strict YAML loading and named repeatable values; replace POC population and date fields with the run contract |
| `formats.py`, `definitions/` | Keep declarative formats; add `CT_ID`, versioned trade fields, and daily-only CDC rules |
| `sorting.py` | Keep bounded sorted runs and merge; add the disk-backed path catalog and cash transaction sequence assignment |
| `runner.py`, `benchmark.py`, `__main__.py` | Keep `run`, `validate`, and `compare`, partial-output protection, measurements, and sequential benchmark runs; supervise all workers |
| `customers.py`, `reference.py` | Keep serializers and useful fixtures; replace fixed action schedules, reference counts, calendar rules, market prices, and release scheduling |
| `activity.py`, `generation.py` | Replace stateless buy and sell cycles and `BatchN` routing with stateful intent execution and the local dataset layout |
| `validation.py`, `tests/` | Keep format, corruption, repeatability, and failure tests where they fit; add account-ledger checks and the partitioned validator |

## Review Points

The documents leave the following details unsettled or inconsistent. These are recommendations, not new accepted business rules.

| Point | Recommended choice |
| --- | --- |
| One GB per unit versus fixed market bytes | Treat 1 GB as the variable firm contribution, consistent with the table that excludes about 0.4 GB fixed data. Expect about 1.4 GB total at factor 1 and 5.4 GB at factor 5 if that fixed estimate holds; use the measured intercept, not 0.4 GB by assumption |
| Price support before `initial_start` | Include one prior calendar year of `DailyMarket` in the initial file so the first simulation date has 52-week prices. The contract explicitly permits earlier FINWIRE and references but does not state this price extension |
| Closure with holdings, debt, or pending orders | Cancel open orders, liquidate holdings on preceding trading dates, repay borrowing with an explicit deposit if needed, withdraw the remaining balance, then close on a following date. Never erase holdings or forgive debt without a source record; the no-warning case has no extended leaving period |
| One holding row per completed trade | Use one origin lot per sell trade, splitting a larger liquidation into several trades. Supporting one sell that consumes several lots requires revising the stated source grain |
| Short NYSE sessions | Keep NYSE session dates and the fixed UTC clock; use a 13:00 UTC close on exchange early-close dates. The timing table states 16:00 and does not say whether early closes apply |
| Tier, tax, and margin details | Rank joining cohorts by total wealth; use explicit configurable wealth and borrowing distributions; start with zero transaction tax and nonzero charge and commission, then add tax rules only with an accepted policy |
| Customer watches during account leaving | Cancel all watches on customer inactivation; for an individual account leaving, cancel only a configured share because watches belong to the customer, not the account |
