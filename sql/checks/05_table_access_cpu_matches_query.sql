-- CPU split across tables adds up to the CPU of the query.
-- Every query that read tables has rows in fact_query_table_access, and
-- their allocated_cpu_time_ms sum to fact_query.cpu_time_ms. Each row is
-- rounded to whole milliseconds, so up to 0.5 ms per table is tolerated.
WITH access AS (
    SELECT
        query_id,
        sum(allocated_cpu_time_ms) AS allocated_cpu_time_ms,
        count(*)                   AS table_count
    FROM iceberg.meta.fact_query_table_access
    WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY query_id
)
SELECT
    f.query_id,
    f.query_date,
    f.cpu_time_ms,
    a.allocated_cpu_time_ms,
    a.table_count
FROM iceberg.meta.fact_query f
LEFT JOIN access a ON a.query_id = f.query_id
WHERE f.query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
  AND f.input_table_count > 0
  AND (
        a.query_id IS NULL
     OR abs(f.cpu_time_ms - a.allocated_cpu_time_ms) > 0.5 * a.table_count
  )
ORDER BY f.query_date, f.query_id
LIMIT 100
