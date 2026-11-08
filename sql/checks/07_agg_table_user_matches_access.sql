-- agg_table_user_daily adds up to fact_query_table_access for every day.
-- Compares the number of table reads, allocated CPU time and bytes read per day.
WITH access AS (
    SELECT
        query_date                 AS day,
        count(*)                   AS table_reads,
        sum(allocated_cpu_time_ms) AS cpu_time_ms,
        sum(physical_input_bytes)  AS input_bytes
    FROM iceberg.meta.fact_query_table_access
    WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY 1
),
agg AS (
    SELECT
        usage_date                 AS day,
        sum(query_count)           AS table_reads,
        sum(allocated_cpu_time_ms) AS cpu_time_ms,
        sum(physical_input_bytes)  AS input_bytes
    FROM iceberg.meta.agg_table_user_daily
    WHERE usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY 1
)
SELECT
    COALESCE(s.day, a.day) AS day,
    s.table_reads          AS access_table_reads,
    a.table_reads          AS agg_table_reads,
    s.cpu_time_ms          AS access_cpu_time_ms,
    a.cpu_time_ms          AS agg_cpu_time_ms,
    s.input_bytes          AS access_input_bytes,
    a.input_bytes          AS agg_input_bytes
FROM access s
FULL OUTER JOIN agg a ON a.day = s.day
WHERE s.table_reads IS DISTINCT FROM a.table_reads
   OR s.cpu_time_ms IS DISTINCT FROM a.cpu_time_ms
   OR s.input_bytes IS DISTINCT FROM a.input_bytes
ORDER BY 1
