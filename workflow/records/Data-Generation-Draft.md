# Data Generation Draft

This working record holds the generation model agreed in the design sessions of 2026-11-02 to 2026-11-04, so that its parameters and rules are not lost before the spec pass moves them into `docs/`. The platform that receives the data is in the [Platform Design Draft](Platform-Design-Draft.md). [Status](../Status.md) lists the decisions in short form and holds the open questions. DP1 to DP3 are the three Layer 1 data pipelines ([Platform Design Draft](Platform-Design-Draft.md#41-pipelines)). Values marked "TPC-DI" come from the official TPC-DI scale factor 3 sample dataset (SF3) in `local/tpcdi/sf_3`. Other values are assumptions, and the generator summary measures each one after a run.

## The Generation Model

The generation model has four layers. The generator runs layers 1 to 3, and the publisher runs layer 4. Both live in `data/data_source/` (`generator/` and `publisher/`), which stands for the source department. Each layer builds on the one before it and never changes what an earlier layer decided.

| Layer | Question | Examples |
|---|---|---|
| 1. Business behavior | Who exists, and what do they do? | Customers, accounts, profiles, trades, cash, growth, time patterns |
| 2. Attrition signal | What hidden state drives future outcomes? | The leaving period before an account closes, look-alike outflows |
| 3. Source recording | How do the source systems write events down? | File formats, TPC-DI data errors, schema versions |
| 4. Delivery | How does the data travel to the platform? | Duplicate rows, late files, corrected files, late and duplicate stream events |

- **Where each part lives:** numbers are run configuration, rules are generator code, and formats are source definitions. Skew, volume, and peaks are measured results, not settings.
- **Measurement:** the generator writes the generator summary, which also keeps the hidden traits and the correct value behind every data error. The publisher writes its own report of every problem it injects.
- **Code base:** the generator extends the Polars proof of concept (POC) in `data/generator/`. Its activity core becomes a day-by-day simulation that carries account state; its configuration loading, formats, sorting, validation, runner, and tests stay.

## Layer 1: Business Behavior

**Overview.** The generator simulates one brokerage firm day by day. When an account opens, it receives two hidden traits: a behavior profile, which sets how often it trades and moves cash, and a wealth level, which sets how large those amounts are. It also receives a visible account type, cash or margin. On trading days, accounts trade during the session at their profile's rate, and each completed trade moves cash and changes a holding. On every day, accounts deposit and withdraw cash on their profile's schedule. Customers join, change, and leave at steady yearly rates. Source files record only the events, never the hidden traits.

```text
account traits     ->  behavior          ->  events            ->  source files  ->  measured results
(profile, wealth:      (how often,           (trades, cash,        (Trade.txt,       (skew, volume,
 hidden; account        how much, when)       holdings, ...)        CashTransaction   peaks)
 type: visible)                                                     .txt, ...)
```

### The Firm

| Property | Definition |
|---|---|
| Business | A mid-size US retail brokerage serving individual investors |
| Cutover | The historical period ends on 2018-12-31, and the incremental period starts on 2019-01-01. The platform goes live with the historical load. |
| Size at cutover | At scale factor 100: about 330,000 customers and 550,000 accounts. Other factors scale these counts (see [Scale Factor](#scale-factor)). |
| Growth | Accounts grow about 8% per year before and after cutover. At factor 100: about 374,000 at the start of the historical period and about 943,000 at the end of the incremental period. |
| Periods | Historical period 2014-01-01 to 2018-12-31 (1,826 days, 1,258 trading days), delivered as one historical load. Incremental period 2019-01-01 to 2025-12-31 (2,557 daily batches, 1,760 trading days). The trading-day counts include the unscheduled NYSE closures on 2018-12-05 and 2025-01-09. |
| Run parameters | `historical_start` = 2014-01-01, `historical_end` = 2018-12-31, `incremental_start` = 2019-01-01, `incremental_end` = 2025-12-31. `incremental_start` must be the day after `historical_end`; the configuration loader rejects anything else. |
| Clock | UTC throughout, without daylight saving time. The NYSE holiday calendar applies to dates. The trading session uses NYSE's local hours on the UTC clock, a deliberate simplification. |
| Volume at factor 100 | Historical load about 29 GB. Incremental period: offline files about 8 GB in 2019, rising to about 13 GB in 2025, about 71 GB in total. Offline files about 100 GB overall. Cash in the incremental period travels through Kafka, not in files: an estimated 18 GB more, counted separately. |

### Scale Factor

The `scale_factor` run parameter sets only the firm's size. Customers, accounts, prospects, and brokers scale with it, and so does all their activity. The market (securities, companies, daily prices, FINWIRE), the reference files, the profile shares, the per-account rates, and the error and publisher problem rates stay the same at every factor. The four run dates set the calendar separately.

One unit is about 1 GB of offline files over the reference calendar, 2014-01-01 to 2025-12-31. Each day gets its share of that volume, so a run over a shorter calendar produces less data from the same firm. The fixed market and reference data add about 0.4 GB at every factor. Our factor is not TPC-DI's: the SF3 sample is about factor 0.3 on our scale.

| Factor | Customers at cutover | Accounts | Historical load | One trading-day batch | 2019 daily batches | Offline files, full calendar | Streamed cash, 2019 to 2025 | Typical use |
|---|---|---|---|---|---|---|---|---|
| 1 | 3,300 | 5,500 | 0.3 GB | 0.2 MB | 0.08 GB | 1 GB | 0.2 GB | Tests; the whole calendar in minutes |
| 5 | 16,500 | 27,500 | 1.5 GB | 0.9 MB | 0.4 GB | 5 GB | 0.9 GB | Daily development |
| 10 | 33,000 | 55,000 | 2.9 GB | 1.9 MB | 0.8 GB | 10 GB | 1.8 GB | Integration; first stream replays |
| 15 | 49,500 | 82,500 | 4.4 GB | 2.8 MB | 1.2 GB | 15 GB | 2.6 GB | Integration at a larger size |
| 20 | 66,000 | 110,000 | 5.8 GB | 3.7 MB | 1.6 GB | 20 GB | 3.5 GB | Tuning; the historical load is larger than the 5 GB Spark worker |
| 50 | 165,000 | 275,000 | 14.5 GB | 9.3 MB | 4 GB | 50 GB | 8.8 GB | Main experiment runs and demos |
| 100 | 330,000 | 550,000 | 29 GB | 19 MB | 8 GB | 100 GB | 17.6 GB | The designed firm; generated once for rubric row `r5` and the final Spark experiments |

These values are estimates scaled from the firm model, without the fixed 0.4 GB. After the first measured runs at factors 1 and 5, the customers per unit (about 3,300) are calibrated so that factor N produces about N GB. The factor 100 run must measure at least 100 GB of offline files; if it falls short, the factor is raised, for example to 105.

### Core Entities

| Entity | Source files | Count at cutover, factor 100 | Relationships |
|---|---|---|---|
| Customer | `CustomerMgmt.xml`, `Customer.txt` | About 330,000 | Owns 1 account (50%), 2 (33%), or 3 (17%) |
| Account | `CustomerMgmt.xml`, `Account.txt` | About 550,000 | Belongs to one customer, managed by one broker |
| Broker | `HR.csv` (job code 314) | About 550 among about 2,200 employees | Manages about 1,000 accounts |
| Prospect | `Prospect.csv`, a full snapshot on the first day of each month | About 1.1 million, growing with the firm | About 30% of new customers appear on the list before they open an account |
| Company | FINWIRE `CMP`, `FIN` | About 1,250, at every factor | Issues one or more securities |
| Security | FINWIRE `SEC` | About 2,000, at every factor | Issued by one company |

`HR.csv` and the six reference files (`Date.txt`, `Time.txt`, `Industry.txt`, `StatusType.txt`, `TaxRate.txt`, `TradeType.txt`) load once in the historical load and stay static.

### Attributes That Drive Behavior

| Attribute | Level | Values | In source files? |
|---|---|---|---|
| Behavior profile | Account | Five profiles (below) | No |
| Wealth | Account | Most accounts small, a few very large | No |
| Account type | Account | Cash, or margin (about 20%, more common among traders) | Yes, as `T_IS_CASH` on every trade of the account. This is a change from TPC-DI, where the flag varies per trade. |
| Tax status | Account | 1 = taxable (70%); 0 and 2 = two kinds of tax-advantaged retirement account (10% and 20%) | Yes, `CA_TAX_ST` |
| Tier | Customer | Assigned from the customer's total wealth across accounts when the customer joins: wealthiest 10% tier 1, next 30% tier 2, the rest tier 3. Customer updates can move it one step. | Yes, `C_TIER` |

| Profile | Share of accounts | Trades per year | Other cash movements |
|---|---|---|---|
| Dormant | 30% | About 4 | Rare |
| Long-term investor | 45% | About 24 | Monthly payday deposit, occasional withdrawal |
| Retiree | 15% | About 12 | Regular monthly withdrawal |
| Active trader | 9% | About 150 | Irregular, larger amounts |
| Day trader | 1% | About 750 (about 3 per trading day) | Frequent deposits and withdrawals |

The average is about 0.1 trades per account per calendar day, or about 0.14 per trading day. Rates also vary between accounts with the same profile.

### Events and Consistency Rules

| Event | Source | Rule |
|---|---|---|
| Trade | `Trade.txt`, `TradeHistory.txt` | TPC-DI type mix: market buy 20%, limit buy 30%, limit sell 30%, market sell 20%. Market orders always complete; limit orders may be canceled or stay pending. TPC-DI outcomes: about 92.7% complete, 5.8% canceled, 1.5% pending. |
| Trade settlement | Cash | Every completed trade moves cash at its real value: a buy takes out price times quantity plus fees; a sell brings in price times quantity minus fees. |
| Deposit | Cash | An opening deposit, then on the profile's schedule. Tax-advantaged accounts receive contributions before the April tax deadline and in December. |
| Withdrawal | Cash | On the profile's schedule, plus tax withdrawals in April. Never more than available cash; margin accounts may borrow within their limit. |
| Dividend | Cash | About 40% of securities pay, with yields mostly 1 to 4%. Each paying security has one fixed payment date per quarter, moved to the next trading day when needed; it is also the record date. An account that holds the security at the start of that date receives one quarter of the annual dividend per share for each share it holds. Payments post together at 06:00 UTC. The dividend per share changes only through FINWIRE `SEC` updates. |
| Holding change | `HoldingHistory.txt` | One per completed trade |
| Watch | `WatchHistory.txt` | About 0.1 per customer per day (TPC-DI), with popular securities chosen more often |
| Daily price | `DailyMarket.txt` | One row per security per trading day: a random walk plus a market-wide movement |
| Financial report | FINWIRE `FIN` | One per company per quarter. Each quarter is released on the last day of the following month, inside that day's `incremental/YYYYMMDD/` folder, in a file named by the posting quarter of `PTS`; each `FIN` row keeps its reporting period. Reports start five quarters before the quarter that contains `historical_start` (2012 Q4 when `historical_start` is 2014-01-01), so the price-to-earnings ratio (P/E) has four released quarters from the first historical day. Earnings per share are set from the price level, so most P/E values fall between 5 and 40, with a few loss-making companies. As in TPC-DI, Gold calculates P/E, 52-week high and low, and dividend yield in `fact_market_history`. |

"Cash" means `CashTransaction.txt` in the historical load and the raw Kafka topic in the incremental period.

- Cash accounts buy only with available cash, so their balance never goes negative.
- Margin accounts borrow down to a limit based on their wealth.
- There is no short selling: an account sells only securities it holds.
- Trade prices stay within the security's high and low for that day.
- A few securities receive a large share of trades: the top 1% get about 25 to 35%.
- The source system gives every cash movement a `CT_ID` from one global sequence when it creates the movement. `CT_ID` is the first field of `CashTransaction.txt`, and stream messages carry it as their event ID. A duplicate keeps its `CT_ID`.
- `CT_NAME` uses fixed description forms: "Trade settlement 123456", "Deposit", "Withdrawal", and "Dividend SYMBOL". DP2 and Flink Job 1 parse it into `cash_type` and `trade_id` with one shared Python function.

### Population Over Time

| Change | Rate per year | Basis |
|---|---|---|
| Net account growth | About +8% | Assumption |
| New accounts | About 11% of accounts | Derived |
| Account closures | About 3% of accounts, including the accounts closed when their customer becomes inactive | TPC-DI |
| Customers becoming inactive | About 2% of customers; all their accounts close, inside the 3% above | TPC-DI |
| Customer updates (each creates a new version in Gold's type 2 slowly changing dimensions, SCD2) | About 5% of customers | TPC-DI |
| Account updates | About 5% of accounts | TPC-DI |

### Time Patterns

| Time scale | Trades | Cash movements |
|---|---|---|
| Day | 09:30 to 16:00 UTC, busiest at the open and close | Any hour, mostly 08:00 to 22:00 UTC; dividends at 06:00 UTC |
| Week | Monday to Friday, except NYSE holidays | Every day; weekend deposits and withdrawals at about 40% of weekday levels |
| Month | None | Payday deposits; withdrawals near the end of the month |
| Year | About 4 market-shock days, each with a sharp market-wide move and about 3 times the normal number of trades | Tax-season contributions and withdrawals |

### Expected Results at Cutover

The counts below are for factor 100; they scale with the factor. Skew shares do not.

| Characteristic | Expected value |
|---|---|
| Account skew | Top 1% of accounts make about 20 to 25% of trades; top 10% make about 60% |
| Security skew | Top 1% of securities get about 25 to 35% of trades |
| Trades | About 77,000 per trading day; about 230,000 on a market-shock day |
| Daily source data | Offline files about 19 MB per trading day and about 2 MB per weekend day or holiday, plus a 228 MB prospect snapshot each month. Streamed cash adds about 8 MB per trading day (estimate). |
| Cash events | About 82,000 per trading day, about 85% of them trade settlements |
| Bytes per trade | About 280 bytes of source data per trade with its history, cash, and holding rows (TPC-DI SF3); cash is about 30% of it |

## Layer 2: Attrition Signal

**Overview.** Layer 1 sets how many accounts close. Layer 2 sets what happens before a closure. Most closures follow a hidden leaving period in which deposits stop, trading slows, holdings are sold, and cash leaves in steps. Some accounts that stay show similar outflows, and some closures come without warning.

| Rule | Value |
|---|---|
| Closure risk factors | Profile (dormant x2, active and day traders x1.5, retirees x0.5); tenure (first year x2); tier (tier 1 x0.7); recent losses (account value down more than 20% over 3 months: x1.5). Calibrated to about 3% of accounts per year. |
| Closures with a leaving period | 80%; the other 20% close without warning |
| Leaving period | 1 to 8 weeks: deposits stop, buys stop, holdings are sold in 1 to 4 steps, each followed by a withdrawal within a day, cash drains to about zero, and watches are canceled |
| Customer becomes inactive | All of the customer's accounts leave together |
| Look-alikes | About 2% of accounts per year sell 20 to 50% of their holdings and make one large withdrawal, while deposits and normal trading continue |
| Recording | Leaving periods and look-alikes appear only in the generator summary |
| Signal check | Before the pipelines are built, a quick model on a small sample must beat a simple rule ("no deposit and net outflow over 30 days"), and the rule's precision must stay below about 30% |

## Layer 3: Source Recording

| Rule | Value |
|---|---|
| Formats | TPC-DI formats: headerless pipe files, change data capture (CDC) fields on daily rows, XML that keeps omitted and empty elements distinct, fixed-width FINWIRE |
| Data errors (TPC-DI) | Invalid customer tier (about 0.2% of customer records), date of birth out of range (about 0.1%), invalid credit rating (about 1% of company records), fee larger than the trade value and commission larger than the trade value (about 0.1% of completed trades each). A trade with an invalid fee still settles at its correct value. Two other problems were considered and set aside: a text encoding problem and a change in the `CT_NAME` description format. |
| Schema evolution (our addition) | From 2019-07-01 (the date is a setting), `Trade.txt` rows carry a 15th field, `T_CHANNEL` (`WEB`, `MOBILE`, `BROKER`), added at the end. Traders mostly use web and mobile; retirees use their broker more often. The historical load keeps 14 fields. TPC-DI itself keeps each file's format fixed across batches. |
| Handling rules | Bronze keeps the raw value. DP2 never drops a record that other records reference. DP2 repairs a value only when the correct value is certain; otherwise it keeps the record, sets the field to NULL or keeps it as delivered, and adds quality metadata. Unusable records go to quarantine with the rule and reason. No step ever invents a value. |

## Layer 4: Delivery

Skew and the recording problems come from the generator (layers 1 to 3). The publisher injects the transport problems. It publishes the generated files into the source side's `upstream` bucket and publishes cash events to the raw Kafka topic; DP1 then pulls the files into `staging` ([source boundary](Platform-Design-Draft.md#2-source-boundary-and-storage)). The generated files therefore contain no transport problems, and the publisher report measures every injected problem against them.

The publisher reads one generated dataset and takes a named publisher profile, such as `clean` or `realistic`, so changing problem rates needs no regeneration. Each delivery writes complete files to `upstream`; cash events of the incremental period go to Kafka instead. All values below are defaults of the realistic profile and are configuration.

| Problem | Realistic default |
|---|---|
| Offline duplicates from retried extracts (the third offline problem) | About 12 retried extracts per year in the daily batches, each a block of 5 to 20% of one file that day; one retried chunk in each of two large historical files (about 0.5 to 1% of those files) |
| Corrected files | The provider resends a corrected daily file about once a month or more. The pipeline reruns the affected date and every later date. |
| Late files | About one per month, 1 to 24 hours late. A late file holds back only the tables that depend on it; `ops.load_status` and a completeness view show what is missing. |
| Late stream events (the rubric's late arrivals) | About 97.5% of events arrive within seconds, 2% arrive 1 to 10 minutes late, about 0.49% arrive 10 minutes to 1 hour late, and about 0.01% arrive more than a day late. About 4 outages per year hold events for 30 to 60 minutes, then release them at once. |
| Duplicate stream events (the second streaming problem) | Single resends of about 0.2% of events, seconds apart, plus resends of the last 5 to 15 minutes after each outage; about 0.25 to 0.3% of events overall. Duplicates keep `CT_ID`. |
| Replay speed | One simulated clock with a configurable stream speed. Batch pipelines run as fast as they can. The demo stream speed is set after measuring one day of DP1 to DP3. A bulk mode sends the historical load and backfills as fast as possible. A lockstep mode that keeps the stream and the batches in step is optional later. |
