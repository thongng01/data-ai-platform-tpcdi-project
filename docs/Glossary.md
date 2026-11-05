# Glossary

This glossary defines the terms that the project uses in its documents, code, and configuration. Each entry names one concept. The words under _Avoid_ name the same concept, or something that is easy to confuse with it, and are not used for it.

## Brokerage

**Customer**:
A person who owns one or more accounts at the brokerage.
_Avoid_: client, user

**Account**:
A brokerage account that one customer owns and one broker manages. It is a cash account or a margin account.

**Cash account**, **margin account**:
The two account types. A cash account buys only with its available cash; a margin account can borrow up to a limit set by the account's wealth.

**Broker**:
A brokerage employee with job code 314 who manages accounts.
_Avoid_: agent (a Layer 3 software agent), advisor

**Tier**:
A customer's service level, from 1 to 3. Tier 1 holds the wealthiest customers.
_Avoid_: segment, class

**Trade**:
An order to buy or sell a security for an account, from submission until it completes or is canceled.
_Avoid_: transaction

**Holding**:
The quantity of one security that an account owns.
_Avoid_: position

**Watch**:
A security on a customer's watch list, from when the customer adds it until the customer removes it.

**Prospect**:
A person on the marketing provider's list of potential customers. Some prospects are or become customers.

**Cash transaction**:
One movement of money into or out of an account's cash balance: a deposit, withdrawal, settlement, or dividend. Always write the full term.
_Avoid_: cash event, cash movement, cash (for the record), transaction (alone)

**Settlement**:
The cash transaction that a completed trade causes. A buy removes cash; a sell adds cash.

**Cash balance**:
The money that an account holds at a point in time: the sum of its cash transactions.
_Avoid_: cash (for the balance), live cash

**Cash flow**:
Money that moves into or out of accounts over a period, such as the net of deposits and withdrawals in 30 days.

**Account closure**:
An account becoming inactive. A closed account has no further activity.
_Avoid_: churn, cancellation (trades and watches are canceled)

**Customer inactivation**:
A customer becoming inactive, which closes all of the customer's accounts.

**Account attrition**:
The loss of accounts through account closure, and the name of the Layer 2 prediction task.
_Avoid_: churn

## Source Department and Data Generation

**Source department**:
The simulated part of the firm that owns the source systems and delivers their data. It is outside the platform.
_Avoid_: provider, vendor, upstream (the bucket)

**Source system**:
A system of the source department that records business activity: the customer management system (CMS), the online transaction processing (OLTP) trading database, human resources (HR), the marketing provider, or FINWIRE.

**Generator**:
The program that simulates the firm and writes its source data.

**Publisher**:
The program that delivers generated data to the platform: offline files to `upstream`, and cash transactions to Kafka.

**Generation stage**:
One of the four ordered parts of data generation: 1 business behavior, 2 attrition signal, 3 source recording, and 4 delivery. The generator runs stages 1 to 3; the publisher runs stage 4.
_Avoid_: generation layer, layer, stage (alone, because pipelines also have stages)

**Behavior profile**:
The hidden activity pattern of an account: dormant, long-term investor, retiree, active trader, or day trader.
_Avoid_: segment, persona

**Wealth**:
The hidden amount of money behind an account. It sets the size of deposits and trades and the margin limit.

**Leaving period**:
The hidden 1 to 8 weeks before most account closures, in which the account sells its holdings and withdraws the cash.
_Avoid_: churn period

**Look-alike**:
An account that sells holdings and makes a large withdrawal, as a leaving account does, but stays open.
_Avoid_: false positive (a model result, not an account type)

**Hidden state**:
Facts that the generator knows but never writes into source data, such as behavior profiles, wealth, leaving periods, and look-alikes.

**Data error**:
A wrong value, such as an invalid tier, that a source system records for a correct business event, at the rates that TPC-DI defines. Duplicates and late files are delivery problems, not data errors.
_Avoid_: native data error, TPC-DI data error, recording error

**Delivery problem**:
A transport fault that the publisher adds: a retried extract, a corrected or late file, a late or duplicate message, or an outage. It never changes business data.
_Avoid_: injected error

**Publisher profile**:
A named set of delivery problems that the publisher applies, such as `clean` or `realistic`.

**Scale factor**:
The project's run setting for firm size, not the TPC-DI scale factor. One unit gives about 1 GB of offline files over the reference calendar, and factor 100 is the designed firm.
_Avoid_: SF, scale

**Reference calendar**:
The default run dates: an initial load from 2014-01-01 to 2018-12-31 and a daily delivery period from 2019-01-01 to 2025-12-31.

**Market shock**:
A simulated trading day with a sharp market move and about three times normal trading.

**Generator summary**:
The generator's local report of settings, counts, and hidden state. The platform never reads it.

**Publisher report**:
The publisher's local record of each delivery problem it added. The platform never reads it.

## Delivery

**Initial load**:
The first delivery, in `initial/`: all history from `initial_start` to `initial_end`. TPC-DI calls it Batch 1.
_Avoid_: Batch 1, historical load

**Daily delivery**:
The files for one data date, in `daily/YYYYMMDD/`.
_Avoid_: daily batch, increment, Batch 2

**Daily delivery period**:
The dates from `daily_start` to `daily_end`, each with one daily delivery.
_Avoid_: daily period, daily-period, incremental period

**Cutover**:
The switch from the initial load to the daily delivery period, between `initial_end` and `daily_start`.

**Data date**:
The business date that a delivery or record represents, not the date it was extracted or arrived.
_Avoid_: batch date

**Source family**:
One kind of source data with one format, such as `Trade.txt` or FINWIRE. The project uses 18.
_Avoid_: dataset, source type

**Offline files**:
The source files that the publisher delivers to `upstream`. The cash transaction stream is not part of them.
_Avoid_: batch files

**Expected-file list**:
The platform's list of source files that a delivery must contain. A missing file makes the delivery incomplete; an empty file means no activity.
_Avoid_: manifest, control file

**File version**:
One pulled copy of a source file in `staging`, identified by its version ID. A corrected file is a new file version; a retried pull is not.
_Avoid_: version (alone)

**Ingestion record**:
The platform's record that connects a source file's key and version to its file version in `staging`.

**Cash transaction stream**:
The Kafka topics that carry cash transactions in the daily delivery period.
_Avoid_: cash stream, cash events

**Message**:
One Kafka record that carries one cash transaction. A duplicate message carries the same `CT_ID`.
_Avoid_: event (for the record), delivery

**Late message**:
A message delivered well after the time of its cash transaction. Within the allowed lateness it still updates its window; after that, it goes to the late-message table.
_Avoid_: late event, late arrival

## Platform

**Platform**:
The data, machine learning (ML), and agent system that this project builds, on the other side of the boundary from the source department. The `platform` bucket holds its operational files.

**Layer 1**, **Layer 2**, **Layer 3**:
The three parts of the platform: the data platform, the ML platform, and the agent platform.
_Avoid_: layer (for generation stages or zones)

**Zone**:
One stage of data refinement on the platform: staging, Bronze, Silver, or Gold.
_Avoid_: layer, medallion layer

**Upstream**:
The source department's bucket of delivered offline files. The platform can only read it.

**Staging**:
The zone and bucket that keep every file version that DP1 pulls. The `stg_` prefix of Silver tables is a naming rule from the rubric and does not refer to this zone.

**Warehouse**:
The 19 Gold tables that dbt builds in PostgreSQL.

**Live table**:
A Gold table that Flink Job 2 keeps current, such as the live balance of each account.
_Avoid_: real-time table, live cash

**Quarantine**:
The tables that keep unusable records with the reason they were rejected, so that they do not enter Silver.
_Avoid_: reject table, error table

**Quality metadata**:
Flags on a kept record that name its uncertain fields.

**Load status**:
The record of each data date's expected inputs, arrivals, and processing progress.

**Progress marker**:
Flink Job 1's record of how far it has processed the stream in event time. DP2 waits for it before it builds a date's cash transaction and balance facts.

**Rerun**:
Processing a range of data dates again from retained inputs, and then every later date in order.

**DP1**, **DP2**, **DP3**:
The three Airflow pipelines of Layer 1. DP1 pulls files into staging and Bronze; DP2 builds Silver and Gold; DP3 builds the batch features.

**Job 1**, **Job 2**:
The two Flink jobs. Job 1 lands and deduplicates cash transactions; Job 2 builds live tables, cash-flow features, and rescoring requests.

## Machine Learning and Agents

**Batch features**:
Features that DP3 builds from Gold, such as tenure, tier, and trading activity.
_Avoid_: non-cash features, offline features (Feast's offline store holds both kinds)

**Cash-flow features**:
Features that Flink Job 2 builds from cash transactions, such as 30-day deposits and the current cash balance.
_Avoid_: cash features

**Prediction time**:
The moment at which a prediction is made. Its features use only data available at that moment.

**Closure probability**:
The model's estimate that an open account closes within 30 days.
_Avoid_: churn score, risk score

**Tier threshold**:
The closure probability at which an account of a given tier gets an outreach task.

**Outreach task**:
A request for the account's broker to contact the customer, created when the closure probability reaches the tier threshold.
_Avoid_: alert, flag

**Rescoring**:
Scoring one account outside the daily run, after a large withdrawal or at an agent's request.

**Warning time**:
The time from an account's first outreach task to its closure.

**Agent**:
A Layer 3 program that uses a language model and tools to answer a broker's requests.
_Avoid_: broker (a person)
