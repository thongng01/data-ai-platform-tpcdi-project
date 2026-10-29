-- Silver after DP1: rows and partitions per table.
--
-- Not every silver table has a dt column. A table is partitioned on its event
-- timestamp, and holding history has none, so it is written unpartitioned.
-- Counting dt over the whole set fails on that one table.
--
-- Run with:
--   make sql FILE=misc/tools/sql/silver_counts.sql

SELECT 'stg_trade' AS table_name,
       count(*) AS rows,
       count(DISTINCT dt) AS partitions
FROM hive.silver.stg_trade
UNION ALL
SELECT 'stg_cash_transaction', count(*), count(DISTINCT dt)
FROM hive.silver.stg_cash_transaction
UNION ALL
SELECT 'stg_daily_market', count(*), count(DISTINCT dt)
FROM hive.silver.stg_daily_market
UNION ALL
SELECT 'stg_watch_history', count(*), count(DISTINCT dt)
FROM hive.silver.stg_watch_history
UNION ALL
SELECT 'stg_trade_history', count(*), count(DISTINCT dt)
FROM hive.silver.stg_trade_history
UNION ALL
SELECT 'stg_holding_history', count(*), CAST(NULL AS BIGINT)
FROM hive.silver.stg_holding_history
ORDER BY table_name