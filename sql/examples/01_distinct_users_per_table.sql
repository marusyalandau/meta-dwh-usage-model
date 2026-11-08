-- Q1: How many distinct users queried table X during a given period?
-- Distinct counts are not additive, so we count over the user-level daily table.

SELECT
    table_key,
    count(DISTINCT user_name) AS distinct_users,
    sum(query_count)          AS queries
FROM iceberg.meta.agg_table_user_daily
WHERE table_key = 'iceberg.product.events'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY table_key;

-- Trend: distinct users per week for the same table
SELECT
    date_trunc('week', usage_date) AS week,
    count(DISTINCT user_name)      AS distinct_users
FROM iceberg.meta.agg_table_user_daily
WHERE table_key = 'iceberg.product.events'
GROUP BY 1
ORDER BY 1;
