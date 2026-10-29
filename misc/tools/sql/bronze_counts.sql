-- Bronze rows per table, per batch.
--
-- Bronze is unpartitioned and keeps every arrival column, so this is the
-- "what actually arrived" count that the data-quality numbers compare against.
--
-- Run with:
--   make sql FILE=misc/tools/sql/bronze_counts.sql
SELECT 'raw_trade' AS table_name, ingest_batch_id, count(*) AS rows
FROM hive.bronze.raw_trade GROUP BY ingest_batch_id
UNION ALL
SELECT 'raw_cash_transaction', ingest_batch_id, count(*)
FROM hive.bronze.raw_cash_transaction GROUP BY ingest_batch_id
UNION ALL
SELECT 'raw_daily_market', ingest_batch_id, count(*)
FROM hive.bronze.raw_daily_market GROUP BY ingest_batch_id
UNION ALL
SELECT 'raw_watch_history', ingest_batch_id, count(*)
FROM hive.bronze.raw_watch_history GROUP BY ingest_batch_id
UNION ALL
SELECT 'raw_holding_history', ingest_batch_id, count(*)
FROM hive.bronze.raw_holding_history GROUP BY ingest_batch_id
ORDER BY table_name, ingest_batch_id