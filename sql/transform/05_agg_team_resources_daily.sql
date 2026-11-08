-- Daily resource usage per team, query category and client for {{ run_date }}.
-- Built from fact_query (not from table access) so that every query
-- is counted exactly once, even if it read several tables.
-- Depends on: fact_query, dim_query_type.

DELETE FROM iceberg.meta.agg_team_resources_daily
WHERE usage_date = DATE '{{ run_date }}';

INSERT INTO iceberg.meta.agg_team_resources_daily
SELECT
    f.query_date                            AS usage_date,
    f.team_id,
    COALESCE(qt.query_category, 'other')    AS query_category,
    f.source,
    count(*)                                AS query_count,
    count_if(f.query_state <> 'FINISHED')   AS failed_query_count,
    count(DISTINCT f.user_name)             AS active_user_count,
    sum(f.cpu_time_ms)                      AS cpu_time_ms,
    sum(f.wall_time_ms)                     AS wall_time_ms,
    sum(f.queued_time_ms)                   AS queued_time_ms,
    sum(f.physical_input_bytes)             AS physical_input_bytes,
    sum(f.output_bytes)                     AS output_bytes
FROM iceberg.meta.fact_query f
LEFT JOIN iceberg.meta.dim_query_type qt
    ON qt.query_type = f.query_type
WHERE f.query_date = DATE '{{ run_date }}'
GROUP BY 1, 2, 3, 4;
