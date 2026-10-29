# Generator feasibility

Research note, 2026-10-27, Asia/Saigon. This report records a generator assessment and its compatibility probe. Its recommendations remain proposals. The [project design](../Project-Design.md#scope) records the accepted source scope, cash stream, and forecast task.

## Recommendation

Use the Databricks code as a selective Spark reference. First prove a small local adaptation, then decide how much to reuse. Do not adopt its workflow unchanged or treat its output as a validated business simulation. A Polars implementation can have a simpler runtime, but it would require new implementations of the customer scheduler, dependent datasets, histories and exporters. It does not remove those requirements.

Start with a local Spark runner, explicit configuration, Parquet intermediate tables, and local source-file exporters. Use standard historical plus two-increment mode first. Reuse generation expressions where their rules pass checks. Correct the trade and holding rules before treating the generated data as coherent history. Keep the existing generator until the replacement passes a small check covering all source families.

The compatibility probe stopped at module import: the tested Spark image had Python 3.8.10, while the reference evaluates Python 3.10 union annotations. This result identifies an import failure; it does not establish whether the generation logic runs after that failure is resolved. The probe produced no rows or throughput measurements.

## Scope and evidence

The reference is [shannon-barrow/databricks-tpc-di at commit `2ac319db6e62927c5d4f772cf5654ad8981f9ec4`](https://github.com/shannon-barrow/databricks-tpc-di/tree/2ac319db6e62927c5d4f772cf5654ad8981f9ec4). The isolated checkout is `misc/generator-feasibility/reference`. All code links below use that commit.

The governing source is [TPC-DI 1.1.0](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DI_v1.1.0.pdf), especially clauses 2.2, 2.4.5, 4.4 through 4.6, and 6. Existing [TPC-DI notes](../TPC-DI/TPC-DI.md) explain the full model; [streaming notes](Streaming-Candidates.md) explain replay limits.

The local rubric was read directly from `docs/course/rubric.xlsx`, sheet `edai`. Rows 5 through 8 require at least 100 GB of offline data, skew, schema evolution, another problem and saved source data. Rows 9 and 10 require late arrivals and another streaming problem. Rows 11 through 19 require Spark/Flink processing evidence. Generation therefore needs measured source characteristics and controlled fault settings, beyond a reference generator's expected counts. The minimum volume remains a requirement.

Clause 2.4.5 requires official DIGen files or exact copies for a compliant benchmark. Configurable distributions, new schema versions and extra faults make this a TPC-DI-based coursework generator. This report does not claim benchmark compliance or byte identity.

## What the reference actually does

The entry point is [`data_gen_tasks/data_gen.py`, lines 8 through 105][entry]. Its `native` branch runs DIGen.jar. Its `spark` branch initializes Databricks schemas and volumes; separate notebook tasks generate data. The `augmented_incremental` branch stages Delta tables for a different daily-feed workload. Those tables are intermediates, not the final analytical warehouse.

The actual task graph in [`workflow_builders/datagen_spark.py`, lines 270 through 358][workflow] has these dependencies:

1. Reference files, HR, FINWIRE and Prospect can start after initialization.
2. Customer generation reads HR brokers. It produces customer actions, account state and customer dates.
3. DailyMarket and the shared trade base read FINWIRE symbols.
4. Trade, TradeHistory, CashTransaction and HoldingHistory derive from the shared trade base.
5. WatchHistory depends on FINWIRE and Customer. Copy tasks finalize files; audit and cleanup tasks finish the run.

This is source-data generation. In standard mode it covers the named file families needed here: reference files and batch dates, HR, Prospect, CustomerMgmt XML, incremental Customer/Account, FINWIRE, market, trades, historical trade history, cash, holdings and watch history. File-family coverage does not prove semantic correctness. In augmented mode several generators skip Batch2/Batch3 and write Delta with `stg_target` partitions instead. Do not choose that mode merely to obtain configurable batches. See [`config.py`, lines 210 through 235][config], the [workflow builder][workflow], and the dataset modules.

### Runtime and configuration changes

| Mechanism | Current reference | Local adaptation needed |
|---|---|---|
| Entry and task state | Notebooks use `dbutils.widgets`, notebook context and task values. | A Python runner must pass configuration and gather results explicitly. |
| File access | Unity Catalog volume paths, `dbutils.fs`, and executor-side filesystem moves support final exports. | Export locally first. Add object-storage publication separately with a manifest. A POSIX move is not an S3A rename contract. |
| Shared data | The trade base always uses a three-part Delta table. Task wrappers persist brokers, symbols and customer dates in Delta. | Retain views within one Spark application or use run-specific Parquet paths. Avoid adding Unity Catalog or Delta only to reproduce task handoffs. |
| Caching | Classic Spark can use disk persistence; serverless uses Delta or Parquet materialization. | Select the local path explicitly and fail if required materialization fails. |
| Customer scheduling | A Pandas UDF uses NumPy and pandas. | Pin compatible Python, pandas, NumPy and PyArrow on driver and workers. These packages are not declared in the project's Spark requirements. |
| API compatibility | HR passes Column expressions as `substring` positions or lengths. | Spark 3.5's Python API needs a compatible expression form. This is a source finding, not an observed execution failure, because import failed first. |

Evidence: [entry][entry]; [`_shared.py`][shared]; [`utils.py`, lines 135 through 231, 560 through 760 and 781 through 896][utils]; [`trade_split.py`, lines 165 through 190][trade]; [`customer_scheduler_v2.py`, lines 43 through 48 and 268 through 291][scheduler]; [`hr.py`, lines 77 through 104][hr]. The current project image definition is [`infra/docker/spark/Dockerfile`](../../infra/docker/spark/Dockerfile), based on Spark 3.5.3; its requirements list boto3 only.

### Scale, distributions and reproducibility

[`ScaleConfig`][config] uses integer scale factor times 1,000, then derives dataset counts. For SF=10, the formulas request 50,000 HR rows, 5,000 companies, 8,000 securities, 1.3 million trades and 3 million watch rows. `num_customers` is a scheduling parameter, not the final count of customers created across all historical actions. Do not confuse it with observed entity cardinality. Calendar and history windows are largely module constants; the standard path uses two incremental batches. Very small factors can produce zero schedule terms, so a tiny fixture should use validated explicit sizes rather than assume arbitrary fractional scale works.

The reference does not expose the desired YAML/JSON distribution contract. For example, the trade base assigns roughly 30% limit buys, 30% limit sells, 20% market buys and 20% market sells; it cancels about 10% of limit trades, delays limit submission by up to 90 days, and completes after another 1 to 300 seconds. Securities are selected by a hash modulo count, then invalid time matches fall back to symbol zero. That fallback can create concentration without an explicit skew setting. Account selection uses an analytically estimated earlier account pool, including a 30-day safety margin. These are concrete implementation rules, not proof of realistic customer behaviour. See [trade lines 110 through 164][trade] and [config lines 290 onward][config].

[`seed_for` and `hash_key`, utils lines 253 through 277 and 419 through 442][utils], make many values depend on stable IDs and named seeds. But the base seed is a default argument in `seed_for`; changing `GLOBAL_SEED` alone will not redirect every call. Broker indexing uses `monotonically_increasing_id` in [HR lines 129 through 133][hr]. Driver-local `datetime.timestamp()` also depends on timezone interpretation. A reproducibility contract must therefore pin seed plumbing, dictionary hashes, engine/runtime versions, timezone and stable lookup ordering. Equal logical rows and equal output bytes are separate checks. Neither was established here.

No scale factor was converted into an asserted GB value. Measure source bytes on a small complete run, estimate the next factor, then measure again. Record temporary disk, peak memory, runtime and final source bytes. Compressed Parquet size does not prove 100 GB of source files. The repository's large-run timing claims were not reproduced.

## Correctness changes required before reuse

| Finding from pinned code | Cause and effect | Required check or change |
|---|---|---|
| Historical noncompleted trades retain execution amounts. | [Trade lines 199 through 217 and 245 through 263][trade] derive and select price, fee, commission and tax without masking by completion status. Spec table 2.2.16, p. 34 requires NULL except for `CMPT`. | Mask the fields by status. Verify every noncompleted row and each lifecycle transition. |
| Holding references do not establish coherent positions. | [Trade lines 654 through 662 and 720 through 727][trade] choose an arbitrary smaller trade ID for a sell. Smaller numeric ID does not imply the same account/security or an earlier completed buy. | Derive position changes from ordered trades within account/security and holding identity. Check origin, time and before/after continuity. This is a business-rule change, not an I/O patch. |
| FINWIRE layout differs from official source packaging. | [FINWIRE lines 504 through 534][finwire] emit separate record-type subsets as numbered files with arbitrary order. Spec clause 2.2.2.8 requires quarterly files ordered by posting timestamp. | Restore quarterly packaging and ordering, or explicitly approve a changed source contract. Preserve fixed field widths in either case. |
| Native error coverage is selective. | [Customer line 911][customer] reports no deliberate invalid DOB/tier injection. [FINWIRE line 179][finwire] draws from valid credit ratings. Trade invalid-fee/commission counts are zero. | Define native irregularities separately from coursework faults. Verify actual alert-triggering values rather than assume complete native coverage. |
| Audit code can copy saved observations. | [Audit lines 35 through 55 and 84 through 118][audit] can use precomputed counts. The inspected checkout had `.bak_sf=...` folders rather than active `sf=...` folders, so standard mode does not automatically use those backups. | Compute observations from each produced run. Never rename snapshots into place to make an altered generator appear verified. |

Generic [`write_file`][utils] does not sort records. Its caller must establish required source ordering. Numbered output parts do not establish timestamp or numeric CDC order. Also, analytic account-pool selection warrants a direct check that every selected account exists at the relevant trade time. This investigation did not prove all reference relationships invalid or valid; it identified specific mechanisms that need validation.

## Tiny compatibility probe

The probe used the already present `edai/spark:3.5.3` image (`b6d0ea4ba688`), with `--network none`, two CPUs, 2 GB RAM and only `misc/generator-feasibility/` mounted. It used no production volumes or service network. Its intended scope was 100 HR rows, with the reference writer replaced by a captured DataFrame and ordinary local CSV output. It did not reach that stage.

The first launch failed because the isolated container hostname did not resolve. Setting hostname/host mapping to localhost and `SPARK_LOCAL_IP=127.0.0.1` passed that launch barrier. The next attempt failed importing unchanged `config.py:211`: `TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'`. The captured runtime is Python 3.8.10 and PySpark 3.5.3. A final repeat saved this failure as structured evidence. No packages, images or production code were changed.

Recoverable files are [probe.py](../../misc/generator-feasibility/probe.py), [run-probe.sh](../../misc/generator-feasibility/run-probe.sh), [probe-result.json](../../misc/generator-feasibility/probe-result.json) and [probe.log](../../misc/generator-feasibility/probe.log). The script contains a later substring compatibility branch, but that branch **did not execute**. There is no successful generation result. All probe containers exited and were removed through `--rm`.

A follow-up probe would use a scratch runtime with Python 3.11 and Spark 3.5.3 plus pinned UDF dependencies. Run the same 100-row HR probe, record unmodified-expression failure or success, then exercise the isolated substring adjustment. Only after that should a small customer/account and trade cohort test shared views and Parquet export. Keep that probe isolated from the project image until compatibility is established.

## Spark versus Polars

| Question | Adapt Spark selectively | Implement a simpler Polars generator |
|---|---|---|
| What can remain? | Many hash expressions, dictionary joins, format rules and dataset dependency ideas can remain after checks. | Domain rules, dictionary data and field layouts can serve as references; PySpark code cannot run directly. |
| What must change? | The runner, Databricks storage/task boundary, runtime compatibility, configuration and the identified business rules must change. | Each dataset's expressions, scheduling/state, joins and exports need implementation, plus the same business-rule corrections. |
| What makes it simpler? | One local Spark application avoids the remote notebook task machinery. Spark already serves the required batch processing. | One Python process can avoid JVM/Spark deployment. Explicit bounded ID chunks can limit generation memory. |
| What can still fail at scale? | Shuffles, shared intermediates, export sorts and temporary disk can dominate. | A large join, sort or history operation can still exceed memory. Chunking output does not automatically bound cross-chunk business state. |
| What evidence exists here? | The import blocker is reproduced; generation and performance remain unproven. | No port, runtime probe or performance measurement was attempted. |

The [Polars streaming guide](https://docs.pola.rs/user-guide/concepts/streaming/) describes batch execution and fallback for operations that do not support streaming. [`LazyFrame.sink_csv`](https://docs.pola.rs/api/python/stable/reference/api/polars.LazyFrame.sink_csv.html) supports streaming CSV output larger than RAM. These capabilities make bounded generation plausible; they do not implement XML actions, fixed-width FINWIRE or coherent holding histories.

The Spark change surface is several infrastructure boundaries plus selected dataset rules. The Polars change surface is every dataset implementation plus those rules. A full faithful Polars port is therefore not the smaller default. A deliberately smaller source model could be easier in Polars, but it needs an owner-approved reduction in coverage. Neither approach has a demonstrated completion-time advantage here.

## Proposed configuration contract

This sketch is illustrative and is not accepted configuration or working code. Separating generation, source faults and delivery faults keeps cause and effect measurable.

```yaml
version: 1
generation:
  engine: spark
  seed: 20261027
  scale_factor: 10
  history_end: '2017-07-07'
  incremental_days: 2
  source_contract: tpcdi_based_v1
distributions:
  trade_type_weights: {TLB: 0.30, TLS: 0.30, TMB: 0.20, TMS: 0.20}
  account_activity: {hot_fraction: 0.01, activity_share: 0.50}
source_faults:
  duplicate_extra_per_original: 0.02
  schema_change: {batch: 2, dataset: Trade, add_nullable: T_STOP_PRICE}
delivery_faults:
  enabled: false
  duplicate_extra_per_event: 0.015
  delayed_fraction: 0.08
  delay_seconds: [60, 600]
```

Scale determines business entities and activity; it does not make reference codes arbitrary duplicates. Distribution weights must sum to one. Select only valid account/security identities, then derive dependent trades, cash and holdings from those choices. Do not rewrite a foreign key after generating its dependent facts merely to create skew. Native invalid values need a separately declared policy; zero added faults does not necessarily mean no native irregularities.

For every run, save the resolved configuration, reference commit, runtime versions, dictionary hashes, dataset schemas, row/byte counts and file hashes. A fault manifest must identify the original record, fault type and changed delivery or payload. Duplicate settings above mean extra copies divided by original records. The resulting duplicate fraction of emitted rows is approximately `r / (1 + r)`, not `r`. Define the denominator in every measurement.

Acceptance observations must include:

- The measured account activity share and the actual join-key frequency must match the configured skew within a stated tolerance. Status frequency alone does not demonstrate join skew.
- Schema versions must show the added field and its batch boundary. Parsers must distinguish an absent field from a present NULL where that distinction matters.
- Unique business identities and valid time-dependent references must survive added duplicates. Holding quantities and cash movements must still reconcile after deduplication.
- Delayed delivery must retain business event time and stable event identity. Measure delayed fraction and delay distribution separately from business history. Delivery settings remain disabled until the event contract is chosen.
- Final source bytes must meet the required volume. Record cardinality, duplicates and schema counts from actual output rather than configuration alone.

These settings do not establish learnable behavior for the selected cash forecast. Generation still needs defined account behavior, feature and label timing, and evaluation rules. See the [selected use case](../Project-Design.md#selected-use-case).

## Reuse and bounded implementation order

The current generator's [`config.py`](../../generator/src/datagen/config.py) offers useful configuration validation and fault-setting vocabulary. [`problems.py`](../../generator/src/datagen/problems.py) demonstrates separate duplicate/schema injectors. Reuse those interfaces only after defining denominators, stable IDs and dependency order. Do not retain its count formulas or independent record generation merely because they already exist.

Existing fixtures and contract tests can check shapes and parser regressions. They do not prove coherent history or byte identity. [`inspect.py`](../../generator/src/datagen/inspect.py) offers useful metric names, but `profile_file` reads whole files and holds exact distinct sets, while `_read_lines` assumes CRLF. It is not a suitable unchanged profiler for 100 GB or Spark's usual LF exports. Replace that execution method with distributed or bounded measurement if it is reused. No broad audit or repair of the old generator was performed.

1. Define the detailed generator contract within the accepted business and reference source scope. Preserve file families and business rules; record any packaging changes. Exclude metadata used only for formal benchmark validation, while retaining operational provenance. See the [project source scope](../Project-Design.md#scope).
2. Resolve the scratch Python/Spark compatibility probe. Stop if basic expressions and export cannot run without a large platform change, then reconsider Polars against a defined smaller scope.
3. Build one small complete dependency chain with deterministic IDs, customer/account actions, securities and trade lifecycle state. Correct holdings and noncompleted amounts. Check references, time boundaries and incremental continuity against independent expected examples.
4. Add the remaining source exporters, configuration and per-run manifest. Check XML partial updates, FINWIRE widths/quarters, numeric CDC ordering and daily batch boundaries.
5. Add source faults after the coherent baseline passes. Measure baseline/treatment data signatures. Add delivery faults after the selected cash stream has a defined event identity and replay contract.
6. Measure a bounded scale ladder, then schedule the required 100 GB run with a disk/runtime budget. Promote a replacement only after source contracts and measured output pass review.

Selective Spark reuse remains this study's recommendation. Cash streaming and cash-flow forecasting are selected; generator implementation, forecast timing, exact schemas, and tool responsibilities remain design work. The [project design](../Project-Design.md#open-questions) maintains those choices.

[entry]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/data_gen_tasks/data_gen.py
[workflow]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/workflow_builders/datagen_spark.py
[shared]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/data_gen_tasks/_shared.py
[config]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen/config.py
[utils]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen/utils.py
[scheduler]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen/customer_scheduler_v2.py
[hr]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen/hr.py
[trade]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen/trade_split.py
[finwire]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen/finwire.py
[customer]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen/customer.py
[audit]: https://github.com/shannon-barrow/databricks-tpc-di/blob/2ac319db6e62927c5d4f772cf5654ad8981f9ec4/src/tools/tpcdi_gen/audit.py
