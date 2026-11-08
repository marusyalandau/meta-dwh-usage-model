-- agg_team_resources_daily adds up to fact_query for every day.
-- Compares the number of queries, CPU time and bytes read per day.
WITH fact AS (
    SELECT
        query_date                AS day,
        count(*)                  AS queries,
        sum(cpu_time_ms)          AS cpu_time_ms,
        sum(physical_input_bytes) AS input_bytes
    FROM iceberg.meta.fact_query
    WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY 1
),
agg AS (
    SELECT
        usage_date                AS day,
        sum(query_count)          AS queries,
        sum(cpu_time_ms)          AS cpu_time_ms,
        sum(physical_input_bytes) AS input_bytes
    FROM iceberg.meta.agg_team_resources_daily
    WHERE usage_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY 1
)
SELECT
    COALESCE(f.day, a.day) AS day,
    f.queries              AS fact_queries,
    a.queries              AS agg_queries,
    f.cpu_time_ms          AS fact_cpu_time_ms,
    a.cpu_time_ms          AS agg_cpu_time_ms,
    f.input_bytes          AS fact_input_bytes,
    a.input_bytes          AS agg_input_bytes
FROM fact f
FULL OUTER JOIN agg a ON a.day = f.day
WHERE f.queries     IS DISTINCT FROM a.queries
   OR f.cpu_time_ms IS DISTINCT FROM a.cpu_time_ms
   OR f.input_bytes IS DISTINCT FROM a.input_bytes
ORDER BY 1
