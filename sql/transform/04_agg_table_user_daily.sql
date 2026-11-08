-- Daily usage per table, user, query category and client for {{ run_date }}.
-- Depends on: fact_query_table_access, dim_query_type.

DELETE FROM iceberg.meta.agg_table_user_daily
WHERE usage_date = DATE '{{ run_date }}';

INSERT INTO iceberg.meta.agg_table_user_daily
SELECT
    a.query_date                                    AS usage_date,
    a.table_key,
    a.user_name,
    a.team_id,
    COALESCE(qt.query_category, 'other')            AS query_category,
    a.source,
    count(*)                                        AS query_count,
    count_if(NOT a.is_successful)                   AS failed_query_count,
    sum(a.physical_input_bytes)                     AS physical_input_bytes,
    sum(a.allocated_cpu_time_ms)                    AS allocated_cpu_time_ms
FROM iceberg.meta.fact_query_table_access a
LEFT JOIN iceberg.meta.dim_query_type qt
    ON qt.query_type = a.query_type
WHERE a.query_date = DATE '{{ run_date }}'
GROUP BY 1, 2, 3, 4, 5, 6;
