# Streaming candidates from TPC-DI

Research note, 2026-10-27. This note compares source records and proposes a replay experiment. CashTransaction is now the selected stream; the accepted scope and remaining contracts are in the [project design](../Project-Design.md#streaming-candidates).

It supports the [project design](../Project-Design.md). The TPC-DI facts it uses are in the [data specification](../Data-Specification.md).

## Event meaning and replay constraints

TPC-DI supplies historical extracts and daily change extracts. Kafka can carry a timed replay of these records. That tests stream processing, but it does not turn the source into a live exchange feed. The event meaning and computation determine whether replayed records are sufficient or whether generation needs additional business behavior.

The main constraints are record identity, business time, change order, initial state, and reference data. A trade can appear many times because its status changes. If a consumer treats every row as a new trade, it counts the same trade repeatedly. If a replay starts after creation, the consumer needs the earlier trade state. If account or security enrichment arrives later, the consumer must wait, retry, or report a missing reference.

## TPC-DI facts this note relies on

The [data specification](../Data-Specification.md) states these facts with sources:

- A trade can have several source rows: one `TradeHistory.txt` row per historical status change, and one CDC row per incremental change. `T_ID` identifies the trade, and `CDC_DSN` orders its incremental changes. See [Trade updates](../Data-Specification.md#trade-lifecycle-updates).
- `TradeHistory.txt` exists only in Batch1 and is ordered by trade ID, not by time. See [TradeHistory.txt schema](../Data-Specification.md#tradehistorytxt).
- `CDC_DSN` orders changes within one file. The specification gives no event ID or clock across files. See [Physical Source Files & Ingestion Rules](../Data-Specification.md#3-physical-source-files--ingestion-rules).
- Execution price, fee, commission, and tax appear only in completed trade rows.
- `CashTransaction.txt` has no transaction ID, and `HoldingHistory.txt` has no timestamp. See [CashTransaction.txt schema](../Data-Specification.md#cashtransactiontxt) and [HoldingHistory.txt schema](../Data-Specification.md#holdinghistorytxt).
- `DATETIME` values have seconds and no time zone. See [Source File Schemas](../Data-Specification.md#92-source-file-schemas).

## What replay can recover

TradeHistory records can replay recorded status and time. Joining the final Trade row supplies identifiers and other extracted attributes, but it cannot reconstruct every earlier field value. In particular, attaching the final execution price or commission to a pending event leaks a later outcome into an earlier event. Keep final attributes separate from the status event, or label them explicitly as later enrichment.

Incremental Trade rows preserve insert/update order and the supplied row values. A replay can carry those rows as CDC messages without inventing a new lifecycle. Combining Batch1 state with Batch2 and Batch3 changes preserves [trade continuity](../Data-Specification.md#trade-lifecycle-updates). Starting from Batch2 alone produces updates for trades whose creation was earlier.

Neither source establishes exchange order-book depth, bid/ask quote changes, individual market ticks, partial fills, network arrival times, or actual Kafka failures. Accelerating timestamps changes delivery speed, not source detail. A new generator would be needed for events the files do not contain. That generator would be a project extension with a new contract, not recovered TPC-DI history.

## Candidate comparison

| Candidate | Natural computation | Why it may fit | Constraint |
|---|---|---|---|
| CashTransaction | Per-account cash change by event-time window; running cash with initial state | Insert-only changes and signed `CT_AMT` make duplicate errors easy to measure | No standalone transaction ID in the fields. Assign a stable event ID before publication and retain it on repeated delivery. A running balance needs an opening balance or full earlier history |
| WatchHistory | Active watches per customer/security; activation and cancellation counts | `W_DTS` and `W_ACTION` directly record actions | Cancellation is an inserted history row, not CDC delete. Ordering matters when deriving current active state |
| HoldingHistory | Holding changes and before/after continuity checks | `HH_H_T_ID` identifies the trade that opened the holding; `HH_T_ID` identifies the modifying trade | No native timestamp or account/security columns. Join current trade to obtain context and close time. Do not sum successive after-quantity snapshots |
| DailyMarket | Daily price movement, rolling daily highs/lows and volume checks | Clear security/day grain and insert-only increments | Date resolution only. Daily high, low, close and volume cannot reconstruct intraday ticks |
| Trade plus history | Lifecycle duration, transition counts, completed-trade totals, latest state | Richest explicit lifecycle in the available sources | Requires separating trade identity from change identity and handling historical versus incremental shapes |

Official definitions: CashTransaction clause 2.2.2.3, pp21-22; HoldingHistory clause 2.2.2.9, p30; DailyMarket clause 2.2.2.6, p28; WatchHistory clause 2.2.2.19, p35. Each of these four incremental formats permits `I` only. Cash balance loading explicitly adds daily transaction amounts to prior cash in clause 4.6.8, p75. Holdings enrichment uses the modifying trade under clause 4.5.10, p66. The comparison describes source capabilities. The [project design](../Project-Design.md#selected-use-case) records the selected cash forecast and minimum-cash policy.

## Proposed replay experiment

1. Pick one bounded cash cohort with complete earlier state. Compare deduplicated cash movements and balances. The trade cases below remain alternative experiments for lifecycle processing.
2. Preserve each source row, file name, batch, line number, and source-file hash. For file replay, assign a stable `event_id` from that provenance before publication. This identifies a source row; it does not establish that two different source rows represent different business events. Generated events need an identity that remains stable across file export and publication. `trade_id` groups changes for one trade; `event_id` identifies one source change. A repeated Kafka delivery keeps the same `event_id`. Kafka offset identifies a delivery position, not the business event.
3. Keep `event_time` from `TH_DTS`, `T_DTS`, `CT_DTS`, or `W_DTS`, according to the chosen contract. Keep `published_at` separately for actual replay delivery time. Preserve original timestamps if a separate shifted demonstration clock is used. Source DATETIME has seconds and no timezone offset (clause 2.2.1.1, pp20-21); document the interpretation instead of claiming it is UTC.
4. Establish a baseline with no injected faults. For CDC, preserve numeric DSN order within the source file. For historical status replay, explicitly sort by history time with deterministic ties; source file order is not a global time order. Partition Trade by `trade_id` if per-trade Kafka order is required. Do not claim global order across partitions.
5. Run a second replay with a fixed seed and a saved fault manifest. Duplicate selected event IDs and delay selected records by a fixed delivery-time amount. To test per-trade reordering, delay a selected event before producer send. Kafka normally preserves order within a partition, so the producer must create the intended fault. Retain business timestamps and payloads.
6. Compare each run with a finite reference result: distinct completions, quantities counted once per completion, latest trade state by source version, or deduplicated cash sums. Record missing events, duplicate effects, late events, and time to final agreement. Define watermark and correction rules before judging a late result as wrong. An old CDC update must not overwrite newer state merely because it arrived later.

Kafka provides a durable ordered log per partition, consumer offsets and independent consumers. It is useful here if the experiment needs replay, restarts, or separate consumers. A file loop can establish the computation's reference result first. Kafka is not required to discover what the source rows mean.

A Kafka message can carry source CDC fields during file replay. A normalized business-event format may make consumers simpler, but that alone does not require generating new facts. If the chosen computation needs facts the sources do not contain, we must extend generation as well as define the event format. The selected cash stream still needs its exact event, replay, and correction contracts, as recorded in the [project design](../Project-Design.md#open-questions).
