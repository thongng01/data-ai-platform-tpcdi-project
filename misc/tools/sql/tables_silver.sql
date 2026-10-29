-- What the Hive metastore believes exists in the silver database.
-- Run with:
--   docker compose --profile tools run --rm tools python -m tools.sql_file /opt/misc/tools/sql/tables_silver.sql
SHOW TABLES FROM hive.silver