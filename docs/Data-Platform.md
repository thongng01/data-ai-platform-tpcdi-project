# Layer 1: Data Platform

The data platform pulls TPC-DI brokerage files from a source department, consumes a cash transaction stream, and builds validated tables for analytics and machine learning. Spark processes Bronze and Silver Delta tables, dbt-postgres builds the warehouse in PostgreSQL, Flink processes the cash transaction stream, Airflow schedules the pipelines DP1 to DP3 with GX Core checks, and DataHub records lineage and quality results. The [data specification](Data-Specification.md) defines the source formats and integration rules.

---

## 1. Platform Architecture and Medallion Zones

The source department owns the files in `upstream` and the raw cash transaction topic. The platform pulls files into its own `staging` bucket before parsing them. This boundary prevents the source department from changing the platform's saved inputs.

Warehouse facts, live balance tables, and batch feature tables all use PostgreSQL's `gold` schema, with different writers. Flink Job 2 starts from the initial load's closing cash balances; it does not build the warehouse's daily cash transaction and balance facts. The following sections describe dependencies, validation, late data, and recovery.

| # | Zone | Purpose | Grain | Storage |
| --- | --- | --- | --- | --- |
| 1 | **Staging** | Immutable copy of each file version that DP1 pulls. Cash transactions in the daily delivery period enter through Kafka instead. | One pulled file version | SeaweedFS `staging` bucket |
| 2 | **Bronze** | Source fields as delivered text, including duplicates and rejected payloads, with provenance. | One source record or message | Delta tables under `lakehouse/bronze/` |
| 3 | **Silver** | Typed, deduplicated, and conformed records for joins. | One business record or change | Delta tables under `lakehouse/silver/` |
| 4 | **Gold** | The 19-table warehouse, live balance tables, and feature tables. | Per business entity, event, or window | PostgreSQL `gold` schema |

SeaweedFS exposes an S3 API. Hive Metastore resolves lakehouse table names, schemas, and locations for Spark and Trino. Trino queries Delta tables and can join them with PostgreSQL. PostgreSQL also holds a load schema for the rows that dbt needs and an `ops` schema for pipeline state. The `platform` bucket holds Spark event logs, Flink checkpoints, and MLflow artifacts.

### Versioning Baseline

- **Raw inputs**: `staging` keeps each pulled file version unchanged. Reruns use those saved files, not a new source delivery.
- **Provenance and rejects**: Bronze keeps source provenance and raw rejected payloads. Quarantine tables, quality metadata, GX results in DataHub, and `ops.load_status` replace TPC-DI's `DImessages` load log.
- **Table history**: Delta records Bronze and Silver table versions.
- **Business history**: Gold type 2 slowly changing dimensions (SCD2) record effective date ranges. Facts use the surrogate key valid on their event date.

The platform holds one active dataset at a time. Switching datasets or publisher profiles clears the buckets, Kafka topics, and Flink state. Baseline and treated experiments use the same timeline.

---

## 2. Data Generation and Problem Injection

The generator produces connected records for all 18 TPC-DI business and reference source families. Its references are the [TPC-DI specification](TPC-DI/TPC-DI_v1.1.0.pdf), the official scale factor 3 sample (SF3), and the [Databricks generator](https://github.com/shannon-barrow/databricks-tpc-di/tree/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen). It excludes benchmark audit files, the `Audit` table, and `BatchDate.txt`.

### Generation Model

Four generation stages separate business behavior from data errors and delivery problems. The generator runs stages 1 to 3. The publisher runs stage 4, so a new publisher profile does not require new business data. Both belong to the simulated source department under `data/data_source/`, in `generator/` and `publisher/`. The [generator guide](../data/data_source/generator/README.md) explains configuration and execution; the [publisher guide](../data/data_source/publisher/README.md) explains clean delivery of saved files and messages.

Offline files carry the initial load and daily deliveries. Kafka carries cash transactions for the daily delivery period. The generator's three stages form one day-by-day model, not three separate services or full-dataset rewrites.

| Generation stage | Question | Output |
| --- | --- | --- |
| 1. Business behavior | Who exists, and what do they do? | Customers, accounts, trades, cash transactions, holdings, watches, and market activity |
| 2. Attrition signal | What hidden state precedes an account closure? | Leaving periods and look-alike outflows within the business activity |
| 3. Source recording | How do source systems record events? | TPC-DI files, data errors, and versioned trade formats |
| 4. Delivery | How does data reach the platform? | Files in `upstream`, cash transactions in Kafka, retries, corrections, and delays |

Each stage uses the earlier stage's decisions without replacing them. A recorded fee error does not change the correct trade settlement. A publisher retry does not create a new business event.

```text
account traits -> behavior -> business events -> source records -> delivery -> measured results
profile, wealth   rate, size, trades, holdings,  file formats,     delays,     volume, skew,
(hidden), type    timing      cash transactions, errors, versions  retries     peaks, error rates
(visible)                     watches
```

Numbers belong in run configuration, rules belong in generator code, and formats belong in source definitions. Volume, skew, and peak rates are measured results, not direct settings. The local generator summary records settings, counts, hidden state, and the correct values behind data errors. The publisher report records each delivery problem that the publisher adds. Neither report enters the ingestion path.

### Generation Stage 1: Business Behavior

The generator simulates one retail brokerage day by day. Each account receives a hidden behavior profile and hidden wealth level when it opens. The profile sets activity frequency, the wealth level sets amounts, and the visible cash or margin account type constrains spending. Completed trades move cash and change holdings; deposits, withdrawals, and dividends also move cash. Source records show events, not hidden state.

#### The Firm and Calendar

| Property | Definition | Basis |
| --- | --- | --- |
| Business | A mid-size US retail brokerage serving individual investors | Simulation assumption |
| Cutover size | An expected 360,100 customers and about 601,367 accounts at factor 100 | Default anchor of 3,601 customers per unit; 1.67 accounts per customer on average |
| Growth | About 8% net account growth per year, before and after cutover; about 409,000 accounts at the start and 1.03 million at the end of the reference calendar | Assumed growth; counts derived from the expected cutover size |
| Initial load | 2014-01-01 to 2018-12-31: 1,826 calendar days and 1,258 trading days, delivered together | Chosen calendar; NYSE holiday calendar |
| Daily delivery period | 2019-01-01 to 2025-12-31: 2,557 daily deliveries and 1,760 trading days | Chosen calendar; NYSE holiday calendar |
| Run parameters | `initial_start`, `initial_end`, `daily_start`, `daily_end`: four inclusive UTC dates, with `daily_start` exactly one day after `initial_end` | Run contract; invalid or nonadjacent periods are rejected |
| Clock | UTC without daylight saving time; NYSE holidays apply to dates; trading uses local NYSE session hours on the UTC clock | Deliberate timing simplification |
| Reference volume | About 28.5 GB in the initial load and 72.8 GB in daily offline files at factor 100, or 101.3 GB overall; daily offline files rise from about 8 GB in 2019 to 13 GB in 2025 | Linear estimate from measured factor 1 bytes, not measured factor 100 output |
| Stream volume | About 34 GB of streamed cash transactions at factor 100, separate from offline volume | Linear estimate, not part of the 100 GB offline requirement |

The trading-day counts include the unscheduled NYSE closures on 2018-12-05 and 2025-01-09. Every calendar date has a daily delivery, including weekends and holidays. A zero-record file means no activity; a missing expected file means an incomplete delivery. The initial `Prospect.csv` snapshot represents `initial_end`.

#### Scale Factor and Workload

`scale_factor` changes the firm's customer, account, prospect, and broker counts and their activity. It does not change the market, reference data, calendar, profile shares, per-account rates, or error and delivery problem rates. The four run dates set the calendar separately. A shorter calendar produces less activity from the same firm.

One unit targets 1,000,000,000 variable offline bytes over the reference calendar, 2014-01-01 to 2025-12-31. Fixed market and reference data add about 0.43 GB at every factor. This factor is not the TPC-DI scale factor. The default `customers_per_unit=3601` sets the expected customer anchor; the account shares give 1.67 accounts per customer on average. Actual populations vary with joining and exit draws.

The byte columns summarize rounded factor 1 measurements and linear estimates at every higher factor. They multiply the calibrated run's measured variable bytes by the factor, then add the applicable fixed bytes. The measured full-calendar slope is 1,008,534,820 variable offline bytes per unit, not an assumed one GB. The trading-day column averages all 2019 trading dates, including monthly prospect snapshots and quarterly FINWIRE releases. GB and MB use decimal units.

| Factor | Expected cutover customers | Expected accounts | Initial load | Mean 2019 trading-day files | 2019 daily offline files | Full-calendar offline files | Streamed cash transactions, 2019 to 2025 | Intended use |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 3,601 | 6,014 | 0.47 GB | 0.41 MB | 0.11 GB | 1.44 GB | 0.34 GB | First implementation runs and tests |
| 5 | 18,005 | 30,068 | 1.61 GB | 1.52 MB | 0.43 GB | 5.47 GB | 1.70 GB | Daily development |
| 10 | 36,010 | 60,137 | 3.02 GB | 2.91 MB | 0.83 GB | 10.52 GB | 3.39 GB | Integration and first stream replays |
| 15 | 54,015 | 90,205 | 4.44 GB | 4.30 MB | 1.24 GB | 15.56 GB | 5.09 GB | Larger integration runs |
| 20 | 72,020 | 120,273 | 5.86 GB | 5.68 MB | 1.64 GB | 20.60 GB | 6.78 GB | Tuning with an initial load larger than a 5 GB Spark worker budget |
| 50 | 180,050 | 300,684 | 14.36 GB | 14.00 MB | 4.04 GB | 50.86 GB | 16.95 GB | Experiment runs and demonstrations |
| 100 | 360,100 | 601,367 | 28.53 GB | 27.87 MB | 8.04 GB | 101.28 GB | 33.91 GB | Designed firm and offline volume evidence |

These estimates do not establish runtime. Calibration uses a full-reference-calendar factor 1 run and subtracts measured fixed-family bytes from its offline total. Fixed families are `DailyMarket`, FINWIRE, and the six reference files; HR scales with the firm. The remainder estimates variable bytes per unit. The new anchor is `old_customers_per_unit * 1,000,000,000 / variable_offline_bytes`, rounded with decimal half-up rounding. A second factor 1 run checks that anchor with the market, behavior settings, and seed unchanged.

The factor 1 slope is provisional: projecting it to factor 5 is not a measurement at factor 5. Population sampling and staffing rounding can change scaling. The full-calendar large run must measure at least 100 GB of offline source files for rubric row `r5`; a factor 100 projection does not satisfy that requirement. If factor 100 falls short, a higher factor supplies the required volume. Stream bytes are counted separately.

Generation size and processed workload are separate. The large dataset is generated once. Each environment processes the largest factor it handles well, and each experiment processes only the dates and volume needed to show its effect. A typical experiment uses the initial load and some daily deliveries. Larger firms use the same partitioned processing principles. Unmeasured capacity projections are not established limits. Local service groups and resource limits are in [Infrastructure](IaC.md#2-development-environment).

#### Core Entities

| Entity | Source | Count at cutover, factor 100 | Relationships and cadence | Basis |
| --- | --- | --- | --- | --- |
| Customer | `CustomerMgmt.xml`, `Customer.txt` | An expected 360,100 | Owns 1 account (50%), 2 (33%), or 3 (17%) | Default population anchor and assumed account mix |
| Account | `CustomerMgmt.xml`, `Account.txt` | About 601,367 | One customer and one broker | Expected population; TPC-DI relationships |
| Broker | `HR.csv`, job code 314 | 602 among 2,408 employees | About 1,000 accounts per broker; initial load only | Staffing from the expected anchor, rounded up; TPC-DI broker code |
| Prospect | `Prospect.csv` | About 1.2 million, growing with the firm | Full snapshot on the first day of each month; about 30% of new customers appear before joining | Expected population, overlap, and monthly cadence |
| Company | FINWIRE `CMP`, `FIN` | About 1,250 at every factor | One or more securities; quarterly releases | Assumed market size; TPC-DI relationships |
| Security | FINWIRE `SEC` | About 2,000 at every factor | One issuing company | Assumed market size; TPC-DI relationship |

`HR.csv` and the six reference files (`Date.txt`, `Time.txt`, `Industry.txt`, `StatusType.txt`, `TaxRate.txt`, and `TradeType.txt`) load once and stay static. Market and reference settings remain independent of firm size.

Customers, prospects, and HR employees use small public-domain name lists from the Social Security Administration (SSA) and the Census Bureau. Stable identity and field-purpose keys select names and coherent city, state, and postal code combinations. Matching prospects share customer names and addresses. Synthetic streets, reserved fictional telephone numbers, and example-domain emails keep personal data reproducible without copying household records. The [word-list provenance](../data/data_source/generator/definitions/README.md) records sources and terms.

#### Attributes That Drive Behavior

| Attribute | Level | Rule | Visible in source records? | Basis |
| --- | --- | --- | --- | --- |
| Behavior profile | Account | Five profiles below; activity rates also vary within each profile | No | Simulation assumption |
| Wealth | Account | Many small accounts and a few very large accounts | No | Simulation assumption |
| Account type | Account | About 20% margin, more common among traders | `T_IS_CASH` stays the same on every trade of the account | Project rule; TPC-DI allows the flag to vary per trade |
| Tax status | Account | 1 = taxable (70%); 0 and 2 = tax-advantaged retirement accounts (10% and 20%) | `CA_TAX_ST` | Assumed mix using source codes |
| Tier | Customer | At joining, total wealth across accounts assigns the wealthiest 10% to tier 1, next 30% to tier 2, and the rest to tier 3; updates can move one step | `C_TIER` | Assumed assignment using source tiers |

| Profile | Share of accounts | Trades per year | Other cash transactions | Basis |
| --- | --- | --- | --- | --- |
| Dormant | 30% | About 4 | Rare | Simulation assumption |
| Long-term investor | 45% | About 24 | Monthly payday deposit and occasional withdrawal | Simulation assumption |
| Retiree | 15% | About 12 | Regular monthly withdrawal | Simulation assumption |
| Active trader | 9% | About 150 | Irregular, larger amounts | Simulation assumption |
| Day trader | 1% | About 750, about 3 per trading day | Frequent deposits and withdrawals | Simulation assumption |

The weighted rates yield about 0.1 trades per account per calendar day, or about 0.14 per trading day. Activity skew follows from these profiles and account variation; the generator does not add unrelated skewed rows after creating events.

#### Events and Consistency Rules

| Event | Source | Rule | Basis |
| --- | --- | --- | --- |
| Trade | `Trade.txt`, `TradeHistory.txt` | Market buy 20%, limit buy 30%, limit sell 30%, market sell 20%; market orders complete; limit orders may cancel or stay pending. About 92.7% complete, 5.8% cancel, 1.5% stay pending. | TPC-DI SF3 mix and outcomes |
| Settlement | Cash transaction | Every completed buy removes price times quantity plus fees; a completed sell adds price times quantity minus fees. Fees are the charge and commission; transaction tax is zero | Business consistency rule |
| Deposit | Cash transaction | Opening deposit, then profile schedule; retirement contributions increase before the April tax deadline and in December | Simulation assumption |
| Withdrawal | Cash transaction | Profile schedule and April tax withdrawals; cannot exceed available cash plus permitted margin borrowing | Simulation assumption and spending constraint |
| Dividend | Cash transaction | About 40% of securities pay, mostly at 1 to 4% yield. Each paying security has a fixed quarterly date moved to the next trading day; that date is also the record date. At 06:00 UTC, start-of-day holders receive one quarter of the annual dividend per share, times the shares held. Dividend per share changes only through FINWIRE `SEC`. | Simulation assumption and simplified dividend calendar |
| Holding change | `HoldingHistory.txt` | One holding per account and security: the first buy creates it, later trades change its quantity, and a full sale ends it. One holding change per completed trade | Consistency rule; matches the TPC-DI field meanings |
| Watch | `WatchHistory.txt` | About 0.1 per customer per day; popular securities receive more watches | TPC-DI SF3 rate; assumed popularity |
| Daily price | `DailyMarket.txt` | One row per security per trading day; random walk plus market-wide movement. The initial load also covers the year before `initial_start`, so 52-week values exist from the first day | Source grain; assumed price model |
| Financial report | FINWIRE `FIN` | One per company per quarter, released on the last day of the following month; file quarter follows posting timestamp `PTS`, while the record keeps its reporting period | Project cadence using TPC-DI fields |
| Earnings | FINWIRE `FIN` | Reports begin five quarters before the quarter containing `initial_start`, 2012 Q4 for the reference calendar. Prices set the earnings scale so most P/E values fall between 5 and 40, with a few loss-making companies. | Simulation assumption; four released quarters must be available from the first day of the initial load |

As a source, “Cash transaction” means `CashTransaction.txt` in the initial load and the raw Kafka topic in the daily delivery period. All completed trades settle, including those in margin accounts.

- Cash accounts never buy beyond available cash, so their balances never become negative. Margin accounts can borrow to a wealth-based limit.
- Accounts sell only securities they hold. The simulation has no short selling.
- Trade prices stay within the security's daily high and low.
- The top 1% of securities receive about 25 to 35% of trades, an assumed popularity distribution.
- The source system assigns each cash transaction a `CT_ID` from one global sequence. It is the first field of `CashTransaction.txt` and the `ct_id` field of each Kafka message. Duplicate messages keep it.
- `CT_NAME` uses fixed forms: `Trade settlement 123456`, `Deposit`, `Withdrawal`, and `Dividend SYMBOL`. DP2 and Flink Job 1 use one shared Python function to derive `cash_type` and `trade_id`.

#### Population and Time Patterns

| Population change | Annual rate | Basis |
| --- | --- | --- |
| Net account growth | About +8% | Assumption |
| New accounts | About 11% | Derived from growth plus closures |
| Account closures | About 3%, including closures when a customer becomes inactive | TPC-DI SF3 |
| Customers becoming inactive | About 2%; all their accounts close within the 3% above | TPC-DI SF3 |
| Customer updates | About 5%; each creates an SCD2 version in Gold | TPC-DI SF3 |
| Account updates | About 5% | TPC-DI SF3 |

| Time scale | Trades | Cash transactions | Basis |
| --- | --- | --- | --- |
| Day | 09:30 to 16:00 UTC, or to 13:00 UTC on NYSE early-close days; peaks at the open and close | Mostly 08:00 to 22:00 UTC; dividends at 06:00 UTC | Assumed timing; simplified session clock |
| Week | Weekdays except NYSE holidays | Every day; weekend deposits and withdrawals at about 40% of weekday levels | Holiday calendar and assumed cash-flow pattern |
| Month | No extra pattern | Payday deposits and withdrawals near month end | Simulation assumption |
| Year | About 4 market-shock days, each with a sharp market move and about 3 times normal trading | Tax-season contributions and withdrawals | Simulation assumption |

#### Worked Example

A long-term investor with a cash account receives a payday deposit and makes occasional buys. A completed buy at USD 40 for 10 shares with USD 2 in total fees removes USD 402 of cash and adds 10 shares to holdings. The trade, cash transaction, and holding records describe the same event. These amounts illustrate the rules; they are not fixed generator settings.

If that account enters a leaving period, it stops depositing and buying. It sells holdings in 1 to 4 steps and withdraws the proceeds within a day of each sale. The source records show sales, settlements, withdrawals, watch cancellations, and eventual closure. Only the generator summary says that the account was leaving. A look-alike account can sell part of its holdings and make a large withdrawal but continue deposits and normal trading, so a large outflow alone does not identify closure.

If the publisher resends a withdrawal, both messages carry the same `CT_ID`. Bronze keeps both messages; Silver and the live calculations count the cash transaction once. If the source records a wrong fee, the trade keeps that wrong value with quality metadata, while the settlement's cash transaction still uses the correct fee. These separate cases test recording and delivery handling without changing the business event.

#### Expected Results at Cutover

These are factor 100 expectations, not measured results. Counts and activity volume scale with the firm; skew shares do not.

| Characteristic | Expected value | Basis and measurement |
| --- | --- | --- |
| Account skew | Top 1% make about 20 to 25% of trades; top 10% make about 60% | Profile-model expectation; measure account-key counts |
| Security skew | Top 1% receive about 25 to 35% of trades | Assumed popularity; measure security-key counts |
| Trades | About 84,000 per trading day; about 250,000 on a shock day before spending and holding constraints | Expected account count and profile-rate estimate; measure executed activity |
| Daily offline files | About 28 MB per 2019 trading day on average, including monthly and quarterly sources | Linear estimate from factor 1; measure emitted bytes by source and date |
| Cash transaction stream | Every completed trade settles; deposits, withdrawals, and dividends add other messages | Measure transaction types and bytes separately from files; no fixed settlement share |
| Reference bytes per trade | About 280 source bytes including trade history, cash transactions, and holdings; cash transactions are about 30% | Historical TPC-DI SF3 measurement; the workload table uses measured generator bytes |

#### Measured Full-Calendar Runs

These factor 1 measurements use the full reference calendar, seed `20261029`, two workers, two Polars threads per worker, 1,250 companies, and 2,000 securities. The saved files pass full validation. The customer anchor is a planning input; the cutover counts below are realized observations.

| Measurement | Customers per unit | Cutover customers | Cutover accounts | Offline bytes | Stream bytes | Generation seconds | Validation seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Calibration input | 3,300 | 3,278 | 5,488 | 1,346,693,500 | 307,655,539 | | |
| Calibrated volume | 3,601 | 3,597 | 6,028 | 1,438,704,592 | 339,074,412 | | |
| Runtime | 3,601 | 3,597 | 6,028 | 1,438,704,592 | 339,074,412 | 470.48 | 831.70 |

The runtime measurement covers the calibrated source bytes, audit records, and ordered typed hidden records. Its command takes 1,304.80 seconds including validation. This single run does not establish larger-factor throughput.

All runs contain 430,169,772 fixed offline bytes. The first contains 916,523,728 variable offline bytes; the calibrated run contains 1,008,534,820, or 0.853482% above the one-GB variable target. At the default anchor, combined initial and streamed cash transactions are 69.189662% settlements, 16.302305% deposits, 8.572914% dividends, and 5.935119% withdrawals. Four fixed security-selection slots per account allow repeated buys in the same small universe; the dividend-paying share remains 40%.

The calibrated annual account-closure rate is 2.925087%, customer inactivation is 2.027576%, and confirmed look-alikes are 2.021389%. Candidates receive no closure immunity. Of 2,369 actual closed accounts, 1,891 have matched final warning plans (79.82271%). These plans schedule 7 to 56 warning days, with a median of 30; scheduled steps do not prove that sales occurred when an account held no assets. Outflow candidates, unlike warning-plan counts, require an executed sale and a positive withdrawal.

On 512 sampled accounts with chronological splits and 30-day label gaps, the calibrated signal model's test average precision is 0.228498, compared with 0.005527 for the outflow rule. The rule's precision is 0.9052%. This measures the synthetic signal check, not downstream model quality. The [generator guide](../data/data_source/generator/README.md#full-calendar-measurements) gives resource measurements.

### Generation Stage 2: Attrition Signal

The attrition signal stage selects account closures and customer inactivations from profile, tenure, tier, and past investment results. It adds the behavior before closure so observed events can support the [30-day account attrition task](ML-Platform.md). Risk selection uses completed earlier days, not future outcomes. Hidden state never enters platform features.

| Rule | Value | Basis |
| --- | --- | --- |
| Closure risk | Dormant x2; active and day traders x1.5; retirees x0.5; first-year tenure x2; tier 1 x0.7; value loss above 20% over 3 months x1.5 | Assumed relative risks, calibrated to about 3% annual closure |
| Warning pattern | 80% of closures have a leaving period; 20% have no warning | Signal assumption |
| Leaving period | 1 to 8 weeks: deposits and buys stop; holdings sell in 1 to 4 steps; each sale is followed by a withdrawal within a day; cash drains toward zero; a configured share of the customer's watches cancel | Signal assumption |
| Customer inactivation | All accounts of the customer leave together, and all of the customer's watches cancel | Customer inactivation closes all of the customer's accounts |
| Closure order | Before an account closes, its open orders cancel, its holdings sell, any borrowing is repaid by a deposit, and the remaining cash is withdrawn. A closure without warning does this within its last days | No holding or debt disappears without a source record |
| Look-alikes | Intended confirmed rate of 2% per account-year: an executed sale of 20 to 50% of holdings and a positive withdrawal, with the account still open through 30 observed days; ordinary closure hazards continue | Signal assumption to prevent an outflow rule from solving the task alone |
| Recording | Hidden leaving periods and outflow outcome classifications appear only in local generator reports | Leakage prevention |
| Signal check | On a small sample, a quick model must beat the rule “no deposit and net outflow over 30 days”; that rule's precision must stay below about 30% | Acceptance check for the synthetic signal, before pipeline work |

A deterministic expected-trait curve normalizes risk across customer groups. The plan anchors expected firm size at cutover; measurements report realized customer and account counts and exit rates. The configured annual rates do not guarantee exact rates in each sample.

### Generation Stage 3: Source Recording

The source recording stage writes TPC-DI formats and data errors. It changes recorded values, not the correct business state behind them.

| Rule | Definition | Basis |
| --- | --- | --- |
| Formats | Headerless pipe-delimited files, change data capture (CDC) fields on daily rows, XML with omitted and empty elements kept distinct, fixed-width FINWIRE | TPC-DI source contract |
| Invalid customer tier | About 0.2% of customer records | TPC-DI SF3 |
| Date of birth out of range | About 0.1% of customer records | TPC-DI SF3 |
| Invalid credit rating | About 1% of company records | TPC-DI SF3 |
| Fee or commission above trade value | About 0.1% of completed trades for each error; settlement still uses the correct amount | TPC-DI SF3 rates; consistent settlement rule |
| Schema change | From a configurable date, 2019-07-01 by default, `Trade.txt` adds `T_CHANNEL` at the end: `WEB`, `MOBILE`, or `BROKER`. Traders favor web and mobile; retirees use brokers more often. The initial load keeps the original format. | Project addition; TPC-DI itself has fixed formats |

The five data errors use independent stable draws and can occur together in one record. Customer errors apply to initial CMS XML and daily customer rows; rating errors apply to FINWIRE company records. Fee errors apply only to completed trades. The recorder keeps correct business values separate from recorded fields, so settlements and hidden state remain correct.

The local `summary/data_errors.jsonl` audit stores each source identity, field, correct value, and recorded value. It stays outside source delivery and ingestion. Generator parser workers read separate copies of the sparse audit, recompute error selection, verify exact evidence, and restore only verified values for business replay. The coordinator combines consumed keys in source-file order, rejects repeated consumption across files, sums error measurements exactly, and requires complete audit consumption. Missing, duplicate, unused, or mismatched evidence fails validation; enabling an error rate does not authorize arbitrary corruption.

Each audit key combines source, day, identity, and field. Daily Customer identity uses `C_ID`, `C_ST_ID`, and `CDC_FLAG`, which separates a same-day insert from an active update. CustomerMgmt identity uses `C_ID`, `ActionType`, and `ActionTS`, plus the XML Account element's `CA_ID` for `ADDACCT`, `UPDACCT`, and `CLOSEACCT`. Completed Trade identity uses `T_ID`, `T_DTS`, and `T_ST_ID`; company identity uses `CIK` and `PTS`. These unchanged source fields let validation derive each identity from saved records alone. Selection draws still use the entity, date, and action kind or status, so related records can receive an error together while retaining separate audit entries.

The trade format changes from 14 to 15 business fields according to the daily delivery date, even when a trade began earlier. The initial load always retains 14 business fields. Daily CDC fields are additional fields in both versions. The simulation has no text encoding problem and no change to `CT_NAME`. Section 3 defines platform error and schema handling.

### Generation Stage 4: Delivery

The publisher reads one generated dataset and applies a named publisher profile, such as `clean` or `realistic`. It publishes complete files to `upstream` and cash transactions for the daily delivery period to Kafka. Generated files contain no delivery problems. The publisher report compares delivered data with those files and records actual delays, repeats, and corrections.

The clean profile sends the saved bytes without adding delivery problems. The realistic profile applies the configurable defaults below.

| Problem | Realistic default | Basis |
| --- | --- | --- |
| Retried daily extracts | About 12 per year, each repeating a block of 5 to 20% of one file | Assumption |
| Retried initial extracts | One repeated chunk in each of two large initial files, about 0.5 to 1% of each file | Assumption |
| Corrected files | A corrected daily file about monthly or more; rerun its date and later dependent dates | Assumption |
| Late files | About one per month, 1 to 24 hours late; only dependent tables wait | Assumption |
| Late messages | About 97.5% within seconds, 2% delayed 1 to 10 minutes, 0.49% delayed 10 minutes to 1 hour, 0.01% delayed more than a day | Assumption |
| Outages | About 4 per year, holding messages for 30 to 60 minutes before releasing them together | Assumption |
| Duplicate messages | About 0.2% single resends seconds apart, plus resends of the last 5 to 15 minutes after outages; about 0.25 to 0.3% overall | Assumption; repeated messages keep `CT_ID` |
| Replay | One simulated clock with configurable stream speed; batch pipelines run as fast as they can; bulk mode sends initial data and backfills at maximum speed | Execution rule; demonstration speed follows measurement of one day of DP1 to DP3 |

### Simulated Data Problems and Required Evidence

Evidence comes from measured runs, not the configured rates alone. The publisher reports delivery problems because they do not exist in the generated files.

| Problem | What fails without handling | Required evidence |
| --- | --- | --- |
| Offline volume and cardinality | Small inputs hide the cost of data movement and optimization | At least 100 GB of offline source files; rows, bytes, and distinct-to-total ratios by relevant column |
| Account and security skew | Uneven shuffle partitions make a few Spark tasks slow or cause spill | Key distribution; maximum and median task duration and shuffle size; baseline and handled runtime |
| Trade schema evolution | A fixed reader drops the added channel field without failing | Source versions, non-null channel counts, Delta schema history, and handled output |
| Offline duplicates | Counts, balances, and SCD2 versions can be wrong | Duplicate keys, actual repeats, and row and metric counts before and after handling |
| Late messages | Closed windows miss late cash transactions without correction handling | Event and delivery times, delay distribution, late-message counts, and baseline and handled results |
| Stream duplicates | A cash transaction changes balances and windows more than once | `CT_ID`, measured duplicate rates, and corrected counts, sums, and balances |
| Event-time windows | Downstream consumers lack cash-flow totals per time window | Window code, explanation, and Flink UI evidence |

### Generator Engineering Rules

- **Bounded memory**: NumPy arrays hold mutable account state and select daily activity over bounded customer groups. Due-day and account indexes select watch and order changes without daily Python scans of all live state. Up to four business workers take groups of up to 1,024 customers by default; the supported worker limit is eight. Polars builds export batches, hidden-state files, and validation queries. The default 8 GiB process-tree budget covers the supervisor, coordinator, worker processes, and their business state. Ordered execution preserves spending and settlement rules.
- **Repeatability**: IDs and random draws use stable entity keys and named seeds, not chunk size or worker count. The same semantic settings, seeds, and generator version produce the same source bytes.
- **Ordered exports**: Buffer typed records for all source families, seal group-owned Parquet parts, and use native Polars ordering with disk spill. Format and check bounded contract batches before delivery-file writes. Preserve event order, global cash transaction IDs, and file-local CDC sequences across groups without collecting the entire dataset into one frame.
- **Validation and publication**: `run` performs full saved-file validation by default and publishes the final directory only after it passes. The explicit `--skip-validation` flag keeps generation-time contract, ordering, coverage, count, and hash checks, then publishes with `status: generated-unvalidated` in the manifest and summary. This command choice does not change dataset identity. `validate` checks saved bytes without regeneration and records `status: validated` after success. The publisher refuses unvalidated datasets unless `--allow-unvalidated` explicitly permits them; its delivery report records that override.
- **Checkpoints and resume**: Completed reference, market, population, customer-group, prospect, coordinator, export, and validation steps bind to implementation file hashes and the exact semantic and intermediate-layout configuration. `run --resume <partial path>` reuses matching completed work and refuses changed code, semantic settings, `customer_group_size`, or `export_rows`. Group markers commit last. Incomplete groups restart from immutable planned population rows. Export retry removes only final source trees and the local audit before rebuilding them from sealed parts. A completed export needs only validation. Failed runs retain partial evidence; final unvalidated datasets can also resume validation.
- **Run budgets**: RSS, disk, process-count, and timeout guards apply to each attempt, including resume. The timeout stays a per-run configuration setting and defaults to 7,200 seconds. Users set it above the expected command runtime, including validation; the estimate of more than 10 hours for factor 100 exceeds that default.
- **Separate reports**: The local generator summary records settings, validation status, per-file bytes and rows, hidden state, data errors, and measured characteristics. Ingestion never reads it. The publisher writes a separate delivery report.
- **Problem measurement**: Count emitted duplicates directly. If each original has `r` extra copies on average, the duplicate share among emitted rows is `r / (1 + r)`.

<details>
<summary>Reference Generator Corrections</summary>

Code adapted from the Databricks reference applies these corrections.

| Reference problem | Effect | Correction |
| --- | --- | --- |
| Non-completed trades retain execution amounts | Price, charge, commission, and tax violate the source contract | Keep those fields NULL unless status is `CMPT` |
| A sell chooses an arbitrary smaller trade ID as holding origin | Account, security, time, and holding continuity can be wrong | Derive holding changes from ordered completed trades and check before and after quantities |
| FINWIRE types go to separate, unordered files | Files do not match quarterly mixed-record exports | Write fixed-width quarterly files ordered by `PTS` |
| Data errors are incomplete | Quality handling is not exercised | Generate each data error separately and measure each rate |
| Audit code can copy saved counts | Counts can describe another run | Measure each produced run directly; do not emit benchmark audit files |
| Generic writing is unsorted and some seeds depend on runtime state | File bytes can change with execution settings | Sort exports and use stable configured seeds and IDs |

</details>

---

## 3. Batch Processing and Storage Optimization

Spark pulls and parses source files, builds Silver, and sends the Silver rows needed for a date into a PostgreSQL load schema. dbt-postgres then builds Gold from that schema. dbt executes SQL in PostgreSQL; it does not move rows from Delta itself.

| Pipeline | Ingest stage | Validate stage |
| --- | --- | --- |
| **DP1** | Pull new and changed files from `upstream` into versioned `staging` paths, then parse Bronze | Expected files, row counts, parse failures |
| **DP2** | Bronze to Silver, Silver rows to PostgreSQL load schema, then dbt Gold models | Duplicates, keys, relationships, SCD2 ranges, reconciliation |
| **DP3** | Build the batch `feat_*` tables from Gold history | Feature null rates and event-time and availability consistency |

Bronze has one table per source family, with source values stored as text and provenance added. Cash transactions have two Bronze tables, one for initial-load file records and one for stream messages. The stream table also retains Kafka partition, offset, and delivery time. Bronze can be rebuilt from `staging` or the raw topic. Silver converts types, handles CDC order, and keeps omitted and explicitly empty XML values distinct until the SCD2 logic applies them.

### Dependencies and Late Files

A table builds when its own input and the tables it depends on are complete for that date. Other tables can proceed. Dates run in order, initially with one active run per pipeline; independent tables can run within a date.

| Source | Depends on | Dependent outputs |
| --- | --- | --- |
| `Customer.txt` | No same-day table | Accounts, watches, cash transaction and balance facts |
| `Account.txt` | Customers | Trades, holdings, cash transaction and balance facts |
| `Trade.txt` | Accounts, securities | Holdings, cash transaction and balance facts, settlement checks |
| `HoldingHistory.txt` | Trades | Holdings |
| `WatchHistory.txt` | Customers, securities | Watches |
| `DailyMarket.txt` | Securities, financials | Market facts |
| Monthly `Prospect.csv` | Customers for matching | Customer marketing fields; they keep the last snapshot's values until arrival |
| Quarterly FINWIRE | No same-day table | Securities, financials, market facts, then dependent trades, watches, and cash transaction and balance facts |

The main chains are customers to accounts to trades to holdings and cash transactions, and FINWIRE to securities and financials to market facts. Trades and watches also need securities. A late FINWIRE release can therefore hold the cash transaction and balance facts through the trade dependency.

`ops.load_status` tracks expected inputs, arrival, selected file version, processing progress through each zone, and held or failed work with its reason. A completeness view marks dates complete or partial and lists missing sources. Dashboards use the latest complete date by default and label partial dates. Failed freshness checks raise DataHub incidents. A table held for a missing file builds for the first time when it arrives; it does not need a corrective rerun.

### Data Errors and Quarantine

Bronze keeps raw values. DP2 repairs a field only when the correct value is certain. It never invents a value or drops a record that other records reference. If the record remains usable, DP2 keeps it with quality metadata; an unusable record goes to quarantine with the rule and reason.

| Error | Handling |
| --- | --- |
| Invalid customer tier | Set tier to NULL and add quality metadata; the outreach policy uses its unknown-tier default |
| Date of birth out of range | Set the date to NULL and add quality metadata |
| Invalid credit rating | Set the rating to NULL and add quality metadata |
| Fee or commission above trade value | Keep the recorded amount with quality metadata; the settlement's cash transaction uses the correct amount |
| Unparseable row or unexpected field count | Keep the raw payload in Bronze quarantine |
| Unparseable message | Flink Job 1 writes it to Bronze quarantine |

DataHub catalogs quarantine tables, and GX measures rejected records. Quality metadata and raw records let users inspect a failed check without relying only on aggregate counts.

### Schema Evolution and Duplicate Rules

`Trade.txt` has two versioned contracts: 14 business fields through 2019-06-30 and 15 from 2019-07-01, plus the two CDC fields on daily records. The change date is configurable.

1. DP1 reads lines as text, splits on `|`, and checks the field count for the contract and `data_date`. Unexpected shapes go to quarantine.
2. Bronze stores the delivered version. Delta schema merge adds `t_channel` at the first version 2 write, and table history records the change.
3. Silver maps both versions to one schema. NULL in `channel` before the upgrade means “not recorded,” not “no channel.”
4. Gold `dim_trade` gains nullable `channel`. Adding a nullable PostgreSQL column does not rewrite the table.
5. Channel features apply only after the upgrade. Models must handle earlier missing values.

The comparison baseline uses a fixed version 1 schema. Spark's `PERMISSIVE` CSV reader can discard extra fields without an error, so a successful job can still lose the channel. Non-null channel counts and Delta history show the difference.

| Data | Duplicate key | Reason |
| --- | --- | --- |
| Cash transactions | `CT_ID` | One identifier covers file and stream deliveries |
| Other daily files | Full row, including `CDC_DSN` | A legitimate second update remains distinct |
| Initial files | Full row | Repeated extract rows have no CDC sequence |

Rare extract retries still support an experiment. The work to find duplicates depends on how much input the job compares; the duplicate rate determines how wrong untreated results become.

### Gold Model and Storage Constraints

The warehouse has 19 tables: TPC-DI's 19 minus `DImessages`, plus `fact_cash_transactions`. It contains five SCD2 dimensions, mutable `dim_trade`, five facts, `prospect`, `financial`, and six reference tables. Flink live tables and feature tables are additional Gold outputs, not part of that 19-table count.

`fact_cash_transactions` has one row per cash transaction, keyed by `CT_ID` and partitioned by date. It links to account, customer, date, and time dimensions, to `dim_trade` for settlements, and to `dim_security` for dividends. Each day, Spark pushes Silver cash transaction rows into PostgreSQL, and dbt builds the transaction fact and then `fact_cash_balances`. The same models handle initial and streamed dates. `fact_market_history` keeps P/E, 52-week high and low, and dividend yield.

| Concern | Constraint and rule |
| --- | --- |
| Bucket isolation | Table overwrites cannot remove raw inputs because `staging` is separate from `lakehouse` |
| SeaweedFS volumes | Each bucket uses a separate collection; volume growth and size limits must support four buckets |
| External tables | Use explicit table locations; do not depend on empty-prefix detection in SeaweedFS |
| Delta metadata | Use Delta-aware catalog and connector operations so Spark and Trino read the transaction log |
| Transactional updates | Delta supports row-level changes and schema history; plain Parquet does not |
| SCD2 versions | Collapse same-day changes; propagate customer versions to accounts and company versions to securities; facts resolve the version valid on the event date |
| Carry-forward values | Spark supports `last(col, ignorenulls=True)`; PostgreSQL 16 needs grouping or subqueries because its window functions lack `IGNORE NULLS` |
| Financial availability | Get year, quarter, and quarter start from each `FIN` record, not its file name; resolve the company at `PTS`; combine earnings by natural company identity across SCD2 versions; never use unreleased reports |
| Storage experiments | Compare lakehouse compaction, partitioning, and sorting, including Z-order, and PostgreSQL indexes with baseline layouts and query plans |

---

## 4. Stream Processing (Kafka and Flink)

In the daily delivery period, cash transactions reach the platform only through Kafka. Two Flink 2.2 jobs use the Python DataStream API. Job 1 lands and validates messages; Job 2 consumes the clean topic to build live balances and cash-flow features. A failure in Job 2 does not stop raw landing.

### Topics and Cash Transaction Contract

| Topic | Contents | Retention scope | Writer | Reader |
| --- | --- | --- | --- | --- |
| `brokerage.cash_transaction.raw` | Every message, including duplicates | Whole active dataset | Publisher | Job 1 |
| `brokerage.cash_transaction.clean` | Accepted messages with `cash_type` and `trade_id` | Whole active dataset | Job 1 | Job 2 and later consumers |
| `brokerage.account_cash.features` | Cash-flow feature rows | 7 days | Job 2 | Feast push job |
| `brokerage.account.rescore` | Requests to score one account | 7 days | Job 2 | Scoring worker |

All topics have 4 partitions and use account ID as the key. Kafka preserves delivery order within a partition; delayed delivery can still differ from event-time order. The source department owns the raw topic. Platform ownership of the other topics is a convention until Kafka authentication and topic permissions enforce it.

Messages use JSON with a JSON Schema contract file in the repository. Amounts use decimal text to avoid floating-point conversion. Kafka timestamps record delivery time, not event time.

| Field | Meaning |
| --- | --- |
| `ct_id` | Source-assigned `CT_ID`, stable across duplicate deliveries |
| `ct_ca_id` | Account ID |
| `ct_dts` | UTC business event timestamp |
| `ct_amt` | Signed decimal text; positive adds cash and negative removes cash |
| `ct_name` | Fixed description form |
| `schema_version` | Message contract version |

```json
{"ct_id": 81234567, "ct_ca_id": 1001, "ct_dts": "2019-03-05T14:30:00Z", "ct_amt": "-400.00", "ct_name": "Withdrawal", "schema_version": 1}
```

### Job 1: Raw Topic to Bronze and Silver

1. Append every message to Bronze with partition, offset, and delivery time.
2. Validate fields and parse `CT_NAME`. Write unusable messages to quarantine.
3. Check `CT_ID` in per-account state for the last 24 hours. Drop and count a repeated message.
4. Append accepted cash transactions to Silver and the clean topic. Delta commits and Kafka transactions use checkpoint recovery so a restart does not repeat committed output.
5. At checkpoints, write a progress marker to `ops.stream_progress`. DP2 waits until progress passes the day's end plus 10 minutes before building the cash transaction and balance facts.

The progress marker shows processing progress, not proof that no older event can arrive. Events delayed beyond a closed day still require correction. The permanent cash transaction identity remains `CT_ID`; the 24-hour state is the live deduplication horizon, not a new business identifier.

### Job 2: Live Tables, Features, and Triggers

| Part | Behavior | Output |
| --- | --- | --- |
| Live balance | Initialize from each account's Gold cash balance at the end of the initial load, then apply accepted cash transactions by natural account ID | Current balance and today's cash transactions |
| Hourly window | One-hour event-time windows per account: inflow, outflow, net, and count per `cash_type` | Live hourly table and window-processing evidence |
| Daily window | Keep the last 30 daily totals per account and derive 7- and 30-day sums and days since the last deposit | Daily cash-flow feature rows on the features topic |
| Rescoring trigger | Check each large withdrawal against a configurable policy | One-account request on the rescore topic |

- Watermarks run 30 seconds behind the newest observed event time.
- Allowed lateness is 10 minutes. Messages within that limit update windows; later messages go to a late-message table.
- PostgreSQL upserts write full totals, never increments, so repeating a write after restart does not add the same amount twice.
- Both jobs checkpoint every 30 seconds and restart from the last checkpoint. Parallelism 2 per job uses the TaskManager's 4 slots.
- The deployment declares Flink lineage in DataHub. Flink UI, Kafbat UI, Kafka lag, duplicate counts, late-message counts, and quarantine counts support operation.

The stream produces cash-flow features for Feast's offline and online stores. DP3 produces the batch features. Each feature has one definition and one producer, so training and serving do not use independently calculated versions.

### Cash Transaction Path and Daily Reconciliation

| Stage | Shape and writer | Timing |
| --- | --- | --- |
| Initial files | `CashTransaction.txt` with `CT_ID` first; publisher to `upstream`, DP1 to `staging` | Initial load |
| Bronze | Initial file table from DP1 and message table from Job 1 | Initial load, then checkpoints |
| Silver | One cash transaction table keyed by `CT_ID`, with `cash_type` and `trade_id`; DP2 writes initial history, then Job 1 writes streamed dates | Before stream startup, then checkpoints |
| Gold warehouse | `fact_cash_transactions` and `fact_cash_balances`, built by dbt in DP2 | Daily for closed dates |
| Gold live | Current balance, today's cash transactions, hourly windows from Job 2 | Continuous updates |
| Feast | Cash-flow features from a small push job consuming the features topic | Continuous delivery |

For a normal date such as 2019-03-05:

1. Cash transactions arrive on the stream during the day. Job 1 commits Silver at checkpoints, and Job 2 updates live tables.
2. After the day closes, the progress marker passes the day's end plus 10 minutes.
3. The publisher delivers the date's offline files. For example, they can arrive at 02:00 UTC on March 6 without changing their March 5 `data_date`.
4. DP1 and DP2 process the files. DP2 waits for stream progress, pushes the date's Silver cash transactions into PostgreSQL, and runs the cash transaction and balance models.
5. GX reconciles settlements with completed trades, dividends with start-of-day holdings and dividend per share, and the live end-of-day balance with the warehouse balance.
6. DP3 builds the batch features, then daily scoring covers all open accounts.

A late March 5 cash transaction still reaches Silver. Correcting the March 5 cash transaction facts also rebuilds later balances in date order. Late-window output and warehouse correction are separate paths; the live table is current operational state, not a replacement for corrected warehouse history.

| Consumer | Reads | Timing |
| --- | --- | --- |
| Business intelligence (BI) and analysts | Warehouse cash transactions and balances | After daily processing |
| Web API and agent | Live tables | On request |
| Daily scoring | Feast features | After DP3 |
| Triggered rescoring | Rescore topic and Feast features | After a large withdrawal |
| Model training | Feast offline features | Training run |
| GX reconciliation | Silver cash transactions, trades, holdings, live tables, and warehouse facts | Daily processing |

### Verification Rules

Use a clean replay before a seeded realistic replay. Measure publisher delays and duplicates, then compare Flink outputs with an independent batch calculation of unique cash transactions, window sums, and balances. Compare final state for equality, not the maximum emitted balance, because negative amounts can reduce balances. Event-time disorder must not replace newer state with an older snapshot.

Tests must check checkpoint recovery across Bronze, Silver, Kafka transactions, and PostgreSQL upserts. The connector set starts with Spark 3.5 and Delta 3.3, Flink 2.2 with the Delta 4.4 connector, Kafka connector 5.0, and JDBC connector 4.1. Before pipeline work, a compatibility test makes Flink write Delta on SeaweedFS and makes Spark and Trino read it. Spark 4 with Delta 4 is the fallback if that test rejects the starting set.

---

## 5. Orchestration, Quality, and Governance

Airflow 3 uses `LocalExecutor` to schedule DP1 to DP3, with shared Connections and Variables. Each pipeline has an ingest stage and a GX Core validate stage. Dates run in order because SCD2 versions and balances depend on earlier dates; initially only one run per pipeline is active, while independent tables can proceed within the date.

| Pipeline | Quality and lineage |
| --- | --- |
| **DP1** | GX checks source completeness, rows, and parsing; DataHub records `upstream` to `staging` to Bronze |
| **DP2** | GX checks deduplication, keys, relationships, SCD2 ranges, and reconciliation; DataHub records Bronze to Silver to PostgreSQL load tables to Gold |
| **DP3** | GX checks feature outputs and time consistency; DataHub records Gold to feature tables |

DataHub collects lineage through its Airflow plugin, Spark agent, dbt ingestion, and declared Flink jobs. Its GX plugin publishes check results as assertions. Hive Metastore remains the engine catalog; DataHub does not resolve tables during queries or enforce every engine's permissions.

The platform classifies cross-cutting work by need: rubric requirements include secrets, gateway access, HTTPS, dashboards, tests, CI/CD, Terraform, Ansible, and A/B rollout. Low-cost safeguards include safe reruns, versioning, marking personal-data columns, separate database users per service, and parameterized SQL in the API and Model Context Protocol (MCP) tools. [Infrastructure](IaC.md) owns deployment and service controls.

---

## 6. Layer 1 Design Decisions and Rationale

### 6.1 Source Data and Delivery

The source contract uses files and timestamps that the source department can supply. It does not depend on benchmark control files.

- **Source boundary**: DP1 pulls from source-owned storage; the source department cannot overwrite platform copies.
- **Date meaning**: Business date, event time, publication time, and arrival time remain separate.
- **Repeatability**: Retries and backfills use the same saved inputs; corrections create new file versions.
- **Scope**: Simulate a behavior when it changes processing or meets a coursework requirement.

| Decision | Selected choice | Main reason |
| --- | --- | --- |
| Delivery terms | Initial load and daily delivery | Describes the delivery schedule rather than implying that all initial records share a date |
| Initial paths | `initial/<Source>` with standard source names | Preserve actual source names and state the coverage in run parameters |
| Daily paths | `daily/YYYYMMDD/<Source>_YYYYMMDD.ext`; FINWIRE keeps its quarter name | Paths state `data_date` without a control file |
| Date parameters | Four inclusive dates with adjacent initial and daily periods | Quiet days cannot shift the cutoff; gaps and overlaps are rejected |
| Clock | UTC calendar dates, weekends included | Avoids daylight-saving day lengths and preserves non-market activity |
| FINWIRE | Release on the last day of the following month, in that date's daily folder; quarter name follows `PTS` | Delayed reports retain a distinct reporting period and availability |
| Static sources | HR and references load once | Ongoing updates add unused effective-date rules |
| Prospect | Initial snapshot at `initial_end`, then monthly snapshots | Fixes the snapshot date without guessing from activity |
| Generator and publisher | Generate locally, then publish to `upstream` and Kafka | Delivery rates and pacing can change without regenerating business events |
| Completeness | Pipeline-side expected-file list; empty file means no activity | No assumed source manifest or guessed wait timeout |
| Reports | Generator summary and publisher report stay local | Independent evidence does not become a hidden ingestion input |

<details>
<summary>Alternatives Considered</summary>

| Alternative | Why not selected | Revisit condition |
| --- | --- | --- |
| TPC-DI `Batch<N>/` and `BatchDate.txt` | Batch sequence needs another file to identify the date | A source department requires the benchmark contract |
| Extraction or arrival date as `data_date` | Delay changes the business date | A separate business-date contract changes |
| Infer initial bounds from record timestamps | Quiet final days shift snapshot and cutover dates | The source department supplies authoritative period metadata |
| Local New York clock | Daylight saving time changes day length | Business rules require local-time windows |
| Initial-only FINWIRE | Company and earnings data become stale | A bounded experiment needs no later reports |
| Detailed per-company filing calendars | More scheduling rules without a current consumer | A feature needs company-specific release timing |
| Separate quarterly prefix | Adds a second watched location and ordering dependency | Another source department uses a separate channel |
| Hive-style raw prefixes | Raw text and XML are read by explicit path, not as partitioned catalog tables | A raw-file query requirement appears |
| Source control file or fixed timeout | Assumes unavailable metadata or guesses completion | The source department supplies a reliable manifest |
| Publisher writes `staging` | Source could overwrite the platform's raw copy | The exercise no longer needs a department boundary |
| Simulated trading database with CDC | Replaces TPC-DI change files and adds generator and ingestion work | A separately accepted extension requires it |

</details>

### 6.2 Object Storage Layout and Dataset Scope

Buckets separate ownership and recovery requirements. Prefixes separate data with the same owner and storage role. Each extra SeaweedFS bucket uses a collection and consumes volume capacity.

| Bucket | Contents | Writer | Recovery and reason for boundary |
| --- | --- | --- | --- |
| `upstream` | Source-published initial and daily files | Publisher | Source-side delivery; platform identity has read-only access |
| `staging` | Every pulled file version | DP1 | Rerun input; separate from mutable source objects and table-overwrite jobs |
| `lakehouse` | Bronze and Silver Delta tables | Spark and Flink | Rebuild from staging and raw topic; common storage role |
| `platform` | Logs, checkpoints, model artifacts | Platform services | Operational state with different recovery requirements |

Staging paths are `initial/<version_id>/<source_filename>` and `daily/YYYYMMDD/<version_id>/<source_filename>`. Each pulled file gets a version ID. Retrying its pull keeps the same ID and path; a corrected source file gets a new ID. Reruns and backfills reuse saved file versions rather than creating duplicate inputs.

An ingestion record maps the source object key and source version information to the staging path. An object-store ETag can help identify a source version, but it is not guaranteed to be a content hash. The staging version ID identifies the platform's saved file independently of that assumption.

Resource names use the `brokerage` project identity and role-based names within their scope. Bucket names stay `upstream`, `staging`, `lakehouse`, and `platform`. Compose services use bare role names; Compose generates container, network, and volume names rather than requiring explicit `container_name` values. TPC-DI proper names and actual source fields and filenames remain unchanged.

<details>
<summary>Alternatives Considered</summary>

| Alternative | Why not selected | Revisit condition |
| --- | --- | --- |
| One bucket with prefixes only | A wrong table overwrite could remove raw inputs | Access and recovery boundaries no longer differ |
| Separate Bronze and Silver buckets | Both use the same engines and storage role; extra buckets consume volumes | Access or storage requirements diverge |
| Tables and operational state together | Checkpoints and logs differ from analytical tables | Operational storage requirements converge |
| Dataset IDs in every path and table | The platform uses one active timeline | Several datasets must coexist |
| Versions grouped beneath each source filename | The chosen date, version, and filename layout keeps paths shorter and preserves source filenames; the ingestion record supports lookup | Per-file browsing becomes the main access pattern |

</details>

### 6.3 Data Versioning and Safe Reruns

Corrections flow through zones and forward in time because later versions, keys, and balances depend on earlier dates. A rerun replaces the affected date range and every later processed date in order, not the entire Gold warehouse. Every write must be safe to repeat.

| Reason to run again | Scope |
| --- | --- |
| Partial failure | Retry the incomplete work |
| Corrected source file | Use the new saved version and rebuild dependent dates |
| Late input or event | Complete held work or correct already processed dates |
| A bug in processing logic | Backfill affected dates and downstream outputs |
| New feature logic | Build the required history |
| Downtime | Catch up in date order |

Manual business edits are outside this design.

| Write pattern | Use |
| --- | --- |
| Replace all rows | Static reference tables |
| Replace a date slice | Silver, facts, and feature outputs |
| Upsert full values | Flink live tables |
| Append deliveries | Bronze |
| Close and add versions | SCD2; corrections rebuild versions from the affected date onward |

| Zone | Correction from date D |
| --- | --- |
| `staging` | Save a new version and keep the old version |
| Bronze | Append the corrected delivery with provenance tied to its staging version |
| Silver | Rebuild D from the selected corrected delivery |
| Gold facts | Rebuild D and later affected dates |
| Gold SCD2 | Rebuild versions from D onward in date order |
| Feature tables | Replace affected feature rows with a new creation time |
| Flink live tables | Remain current operational state; replay of the clean topic rebuilds them |

Feature rows are replaceable inputs, not a complete record of past training. MLflow versions each training run's full dataset with its model. Delta history shows earlier versions of Bronze and Silver; dbt can rebuild Gold history from Silver. DataHub incidents record rerun context, and lineage shows affected outputs.

Feast's default point-in-time join filters by `event_timestamp` and can select the newest `created` value. Filtering by creation time requires `filter_by_created_timestamp=True` and offline-store support. A creation timestamp alone does not prevent the use of a later correction in an earlier training snapshot.

### 6.4 Streaming Source Selection

The stream carries cash transactions because they are additive amounts with second-resolution event time, a defined daily balance reference, and direct use in account attrition features. Streaming supplies cash transactions in the daily delivery period; live tables and triggered rescoring, rather than high event volume alone, justify continuous processing.

- **Correctness**: Keep stable event identity, recover committed output, and verify against independent balances.
- **Reviewable logic**: Keep warehouse SQL in dbt and stateful event logic in Flink.
- **One owner per output**: dbt writes the warehouse cash transaction and balance facts; Flink writes live balances; each feature has one producer.
- **Operational separation**: Failure in downstream features must not stop raw landing.

| Decision | Selected choice | Main reason |
| --- | --- | --- |
| Lakehouse format | Delta Lake | Learning goal for Databricks and Fabric; `MERGE`, compaction, Z-order, and Trino support |
| Gold location | PostgreSQL | Keys, foreign keys, atomic SCD2 loads, indexes, and reviewable SQL |
| Gold build | Spark to PostgreSQL load schema, then dbt-postgres | dbt does not move data between engines |
| SCD2 | SQL over effective dates, not dbt snapshots | Initial records already contain years of history; snapshots observe run-time changes |
| Orchestration | Airflow 3 | Airflow 2 reached end of life on 2026-04-22; DataHub's plugin supports Airflow 3 |
| Quality | GX Core | DataHub plugin publishes assertions without a custom integration |
| Stream engine | Flink 2.2 | Shared target of Kafka 5.0, JDBC 4.1, and Delta 4.4 connectors; writes Delta directly |
| Kafka inspection | Kafbat UI | Apache 2.0 license and one small service |
| Job split | Landing job, then clean-topic consumer | Landing continues if live processing fails; clean replay does not reappend Bronze |
| Flink API | Python DataStream | Explicit late-message handling and side outputs |
| Feature and rescore topics | Requests decouple consumers | Slow Feast or scoring consumers cannot stall the cash transaction stream |
| Rolling sums | Last 30 daily totals | Avoids 720 overlapping event copies in an hourly sliding 30-day window |
| Daily cash transaction and balance facts | dbt from Silver | One model for all dates; keys resolve after daily dimensions; live state supplies an independent check |
| Identity | Source-assigned `CT_ID` in files and messages | Same cash transaction identity across channels and retries |
| Message format | JSON with schema files | Readable contract without another registry service |

<details>
<summary>Alternatives Considered</summary>

| Alternative | Why not selected | Revisit condition |
| --- | --- | --- |
| Iceberg | Mature open format and Flink sink, but Delta meets needs and matches the learning goal | Delta compatibility test fails without a suitable supported combination |
| Plain Parquet | No transactional row updates or table history | Only immutable exports are needed |
| Gold in Delta with a PostgreSQL serving copy | Splits Gold logic and applies keys and indexes only to the copy | Warehouse scale or access patterns require a different location |
| dbt-spark | Adds Spark Thrift for SQL intended for Gold | Gold moves to Spark |
| dbt-trino writes to PostgreSQL | PostgreSQL connector `MERGE` lacks transaction guarantees; `UPDATE` only permits constants | Connector write guarantees meet the load requirements |
| Spark builds Gold directly | PySpark is harder to review for warehouse SQL | Models require Spark-specific processing |
| pg_duckdb reads Silver | Untested with SeaweedFS and Delta features; changes the shared PostgreSQL image | A separate accepted experiment demonstrates compatibility |
| Flink 1.20 plus Spark landing copies | Splits one stream landing path across engines | Direct Delta writing cannot meet recovery needs |
| Soda Core | Open-source results need a custom DataHub path | Its integration support changes |
| Redpanda Console | Business Source License rather than Apache 2.0 | Licensing requirements change |
| One Flink job for all zones | A single crash stops landing; replay reappends raw zones | The separation no longer helps recovery |
| Delta upserts from Flink | Cash transactions are immutable; upserts add experimental connector features | Mutable stream records require updates |
| Flink daily cash transaction facts plus a balance view | Splits warehouse logic and surrogate-key resolution | Warehouse ownership changes |
| Trade stream | Multi-step lifecycle and conditional execution fields | A trade-specific live use case needs it |
| Watch stream | No monetary measure or direct cash balance link | Watch activity needs live processing |
| Holding stream | Needs trade joins for timestamp, account, and security; quantity is a post-trade state | A source supplies complete holding events |
| Daily market stream | Daily closes are not intraday ticks | A source supplies intraday market events |
| Customer and account CDC stream | Low-frequency changes suit daily SCD2 | Live profile changes become necessary |

</details>
