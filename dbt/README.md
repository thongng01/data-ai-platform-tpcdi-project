# dbt project

Empty. DP2 is the next ticket.

## What will land here

```
dbt/
  dbt_project.yml      models paths, materializations, naming
  profiles.yml         the Spark Thrift connection
  models/
    staging/           stg_*  one per silver table, renaming and typing only
    intermediate/      int_* joins and the SCD2 carry-forward
    marts/
      core/            dim_*  slowly changing dimensions
      facts/           fact_* events
      features/        feat_* offline feature tables
  macros/              the SCD2 window function, carried forward from the reference
  data-tests/          relationships and uniqueness, declared not coded
```

## Two decisions still open

**How dbt reaches Spark.** dbt-spark's connection methods are `thrift`, `http`,
`livy`, `odbc` and `session`. Only `thrift` fits a standalone cluster, and it
needs a HiveServer2 on port 10001. HiveServer2 takes every core of the Spark
worker, which starves DP1 and DP3. Either run DP2 alone under
`--profile compute,dbt`, or use `method: session` and accept that dbt runs
in-process rather than on the shared cluster. See `docs/IaC.md` section 8.2.

**How incremental works without `MERGE`.** SeaweedFS has no `MERGE` and Trino is
not a dbt write target, so `incremental_strategy: merge` is unavailable. The
options are `insert_overwrite` with `partition_by`, or append per batch.

## Why the directory has to exist now

`compose.compute.yaml` and `compose.orchestration.yaml` bind-mount `../dbt`.
Docker creates a missing bind source as an empty root-owned directory rather
than failing, so the containers would mount nothing and say nothing about it.
This directory was in exactly that state until now: created by Docker, owned by
root, empty.