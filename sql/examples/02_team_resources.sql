-- Q2: How many resources did team Y consume during a given period?
-- CPU time is stored in milliseconds: / 3.6e6 converts it to hours.

SELECT
    t.team_name,
    sum(a.query_count)                                     AS queries,
    round(sum(a.cpu_time_ms) / 3.6e6, 2)                   AS cpu_hours,  -- DOUBLE literal: 1000.0 would cut to one decimal
    round(sum(a.physical_input_bytes) / power(1024, 4), 3) AS tib_read
FROM iceberg.meta.agg_team_resources_daily a
JOIN iceberg.meta.dim_team t ON t.team_id = a.team_id
WHERE a.team_id = 'product_analytics'
  AND a.usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY t.team_name;

-- Top tables by CPU for that team in the same period
SELECT
    table_key,
    round(sum(allocated_cpu_time_ms) / 3.6e6, 2) AS cpu_hours,
    count(DISTINCT user_name)                    AS distinct_users
FROM iceberg.meta.agg_table_user_daily
WHERE team_id = 'product_analytics'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY table_key
ORDER BY cpu_hours DESC, table_key
LIMIT 10;

-- Trend: how the team's consumption changes week by week
SELECT
    date_trunc('week', usage_date)                       AS week,
    sum(query_count)                                     AS queries,
    round(sum(cpu_time_ms) / 3.6e6, 2)                   AS cpu_hours,
    round(sum(physical_input_bytes) / power(1024, 4), 3) AS tib_read
FROM iceberg.meta.agg_team_resources_daily
WHERE team_id = 'product_analytics'
GROUP BY 1
ORDER BY 1;

-- Trend for all teams side by side: CPU hours per team per week
SELECT
    date_trunc('week', a.usage_date)     AS week,
    t.team_name,
    round(sum(a.cpu_time_ms) / 3.6e6, 2) AS cpu_hours
FROM iceberg.meta.agg_team_resources_daily a
JOIN iceberg.meta.dim_team t ON t.team_id = a.team_id
GROUP BY 1, 2
ORDER BY 1, 3 DESC, 2;
