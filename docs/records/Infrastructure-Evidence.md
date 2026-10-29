# Infrastructure evidence

This working record preserves observations from the earlier build that were previously mixed into `docs/IaC.md`. The 2026-10-28 documentation cleanup did not rerun services. These observations establish only what the earlier document reported, within the conditions stated below. Current mechanisms and commands are in [IaC](../IaC.md), and project progress is in [Status](../Status.md).

## Provenance

The source is the working copy of `docs/IaC.md` preserved before this cleanup under `misc/tmp/docs-cleanup-20261028/`. The earlier batch measurements in [Status](../Status.md#earlier-build-measurements) identify commit `ea4daf5`, Batch1, generator scale 0.02, and seed 20261018. The old IaC narrative did not attach a separate timestamp, command log, or image digest to each trial. Its findings must not be treated as newly verified results or behavior of every later image version.

## Environment snapshot

| Property | Earlier reported value |
|---|---|
| Host | WSL2 with Linux 6.6. |
| Docker | Engine 29.3.1 and Compose v5.1.1. |
| CPU | 20 cores. |
| Memory | 15 GB RAM and about 4 GB swap. |
| Disk | 935 GB free on the WSL root filesystem. |
| Host Python | WSL Python 3.14 in the repository's uv-managed virtual environment. |

The earlier document quoted 6.8 GB for core limits and 12.8 GB with compute. Those were configuration-limit totals, not observed memory use. It omitted some optional and one-shot services and incorrectly grouped Airflow limits. Use resolved Compose configuration for a new capacity calculation.

## Earlier trials

| Trial | Earlier observation | Interpretation and limit |
|---|---|---|
| Core smoke checks | The earlier build reported eight passing checks. | This covers the checked platform operations, not optional profiles or full pipeline correctness. |
| Partition registration | A wiring run wrote 100 rows into two partitions, read zero before repair, and read 100 after repair. | The path write had not registered partitions. The result does not imply that all table-aware writes need repair. |
| Thrift resource use | HiveServer2 held every worker core while a batch job logged `Initial job has not accepted any resources` about every 15 seconds. | This describes the tested resource configuration. Executor core limits do not by themselves limit total application cores. |
| SeaweedFS volume cap | S3 returned `InternalError ... reached max retries`; the server logged `failed to find writable volumes for collection:landing`. The configured volume cap was eight. | The earlier build changed the cap to zero. Available disk space still limits allocation. |
| Launch environment | The earlier narrative associated `PYSPARK_SUBMIT_ARGS` with a malformed launch and zero-core request. | The current Dockerfile leaves it unset. The original causal claim that every direct `spark-submit` appends the variable was too broad. |
| Bind mounts | Paths for `misc/` and `infra/docker/*/` were reported as wrong during the build. | A mount can exist while its host source is an unintended empty directory. The narrative gives no exact trial paths or command logs. |
| Schema union | Trade bronze held 15,960 rows for 7,980 source lines because both schema branches selected every row. | Filtering branches by field count removed that duplication in the recorded run. It did not fix the current unmatched-row accounting defect. |
| Deduplication | Earlier keys removed about 62 percent of WatchHistory rows and 58 percent of TradeHistory rows against an injected duplicate target of 2 percent. | The keys collapsed distinct events. Most keys were changed to whole-record keys; incremental Trade identity still needs review. |
| Stray output file | A job reported 7,820 rows while Trino counted 7,821. A 268 KB `part-00000` file remained at the table root from an unpartitioned write. | Whole-location deletion removed that file, but the resulting overwrite behavior can also delete valid earlier batches. |
| Spark shutdown | After writing ten silver tables, a run remained in `spark.stop()` for minutes while closing an event log against SeaweedFS. | The code now bounds its wait at 90 seconds. Source inspection shows that it writes the report after that wait, contrary to the old document's claim that it flushed the report first. |

The old schema-branch example was:

```text
Trade: 7980 rows, branches {'historical': 7980, 'delta': 0, 'unmatched': 0}
```

The zero unmatched count is not proof that the parser preserved unexpected shapes. It computes that count after replacing the original frame. The pure arithmetic test for variant counts does not exercise that Spark operation.

## Image and storage trials

The earlier narrative reported that the Hive image entrypoint required 23 environment variables, including unused Azure settings, accepted empty strings, and ignored `HIVE_CUSTOM_CONF_DIR`. The current configuration retains those variables and uses `SERVICE_OPTS`. The cleanup inspected the configuration but did not inspect a fresh container entrypoint.

The earlier SeaweedFS run returned HTTP 501 for Trino conditional writes with `If-None-Match: *`. The Hive catalog disables exclusive create. The earlier Spark run also rejected a managed table location with `LOCATION_ALREADY_EXISTS` when the object prefix listed as empty; the implementation uses external locations.

The old document attributed Hive compatibility to automatic compatibility between Spark's bundled Hive 2.3 client and Hive 3.1 without negotiation. It supplied no compatibility matrix or resolved client settings for that claim. The operational evidence is the earlier successful tested combination, not a universal compatibility guarantee.

The old text also claimed that all Spark start scripts wrap `spark-submit`. Master and worker daemon scripts and the Thrift application launcher serve different roles. The reference now describes those roles separately rather than preserving that explanation as fact.

## Source inspection on 2026-10-28

The documentation cleanup read Compose files, `infra/.env`, the Spark Dockerfile, the Makefile, and generator and DP1 code. It did not start services or change implementation code.

- Airflow selects LocalExecutor and also defines a `worker` service. LocalExecutor runs task processes from the scheduler; the separate worker definition needs review.
- Airflow initialization ignores connection and variable import errors with `|| true`. A successful initialization exit cannot prove those imports succeeded.
- `flink-submit` mounts `../../pipelines` relative to `infra/`, which points outside the coursework pipeline directory.
- The default generator scale is 1.0, but tax-rate generation requests 320 unique codes from a space of 90 codes and cannot finish at that count.
- `--no-schema-evolution` does not remove incremental CDC prefixes. The explicit extra-column injector is called with `active=False`.
- `--no-skew` removes configurable skew values but leaves the base weighted trade-status and trade-type distributions. It does not produce uniform categories.
- The manifest includes intended event counts and uses `-1` for several uncounted file families. It does not establish actual output row counts or injected rates.
