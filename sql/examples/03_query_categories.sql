-- Q3: What does a team spend its resources on?
-- Split by query category (read / write / maintenance / metadata) and client.
-- Sums and shares, not averages: one large MERGE and many small SELECTs
-- would give a meaningless average per query.

SELECT
    query_category,
    source,
    sum(query_count)                                                                 AS queries,
    round(sum(cpu_time_ms) / 3.6e6, 2)                                               AS cpu_hours,
    -- DOUBLE before dividing: DECIMAL division keeps the input scale (one decimal)
    round(100 * CAST(sum(cpu_time_ms) AS DOUBLE) / sum(sum(cpu_time_ms)) OVER (), 1) AS cpu_share_pct
FROM iceberg.meta.agg_team_resources_daily
WHERE team_id = 'data_engineering'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY query_category, source
ORDER BY cpu_hours DESC, query_category, source;

-- Metadata noise: many cheap queries (e.g. DataGrip introspection)
-- inflate query counts but cost almost nothing.
SELECT
    query_category,
    sum(query_count)                                                                 AS queries,
    round(100 * CAST(sum(query_count) AS DOUBLE) / sum(sum(query_count)) OVER (), 1) AS query_share_pct,
    round(100 * CAST(sum(cpu_time_ms) AS DOUBLE) / sum(sum(cpu_time_ms)) OVER (), 2) AS cpu_share_pct
FROM iceberg.meta.agg_team_resources_daily
WHERE usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY query_category
ORDER BY queries DESC, query_category;

-- Distinct users per category over the period.
-- Counted from the query-level fact: distinct counts cannot be summed
-- from the daily aggregate (active_user_count is per row only).
SELECT
    COALESCE(qt.query_category, 'other') AS query_category,
    count(DISTINCT f.user_name)          AS distinct_users
FROM iceberg.meta.fact_query f
LEFT JOIN iceberg.meta.dim_query_type qt ON qt.query_type = f.query_type
WHERE f.query_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY 1
ORDER BY distinct_users DESC, query_category;

-- Who writes each table: a hint for the table owner (dim_table.owner_team_id).
-- Only 'write' queries: maintenance (optimize) also rewrites tables,
-- but it is run by the platform, not by the table owner.
SELECT
    f.output_table_key,
    f.team_id,
    f.user_name,
    count(*) AS write_queries
FROM iceberg.meta.fact_query f
JOIN iceberg.meta.dim_query_type qt ON qt.query_type = f.query_type
WHERE qt.query_category = 'write'
  AND f.query_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31'
GROUP BY f.output_table_key, f.team_id, f.user_name
ORDER BY output_table_key, write_queries DESC, f.team_id, f.user_name;
