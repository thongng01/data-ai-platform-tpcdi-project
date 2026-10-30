-- Prove Trino reads a table Spark wrote, through the shared metastore.
-- Run with:
--   docker compose --profile tools run --rm tools \
--     python -m tools.sql_file /opt/misc/tools/sql/trino_reads_spark.sql
SELECT dt, count(*) AS rows, round(avg(amount), 2) AS avg_amount
FROM hive.silver.wiring_check
GROUP BY dt
ORDER BY dt
