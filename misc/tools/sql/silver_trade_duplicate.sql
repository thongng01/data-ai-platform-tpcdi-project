-- Which trade appears twice in silver?
--
-- The DP1 job reports 7,820 rows and Trino counts 7,821. One row is duplicated
-- somewhere, and dedup said it had removed every duplicate it was asked to.
-- This finds it by comparing the distinct business key against the row count.
--
-- Run with:
--   make sql FILE=misc/tools/sql/silver_trade_duplicate.sql
SELECT row_id, business_id, status, count(*) AS rows
FROM hive.silver.stg_trade
GROUP BY row_id, business_id, status
HAVING count(*) > 1
ORDER BY rows DESC, row_id