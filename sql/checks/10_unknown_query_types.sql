-- Query types missing from dim_query_type.
-- severity: warn
-- They are reported in the 'other' category. Add them to
-- sql/seed/03_dim_query_type.sql and reload the aggregates.
SELECT
    f.query_type,
    count(*)           AS queries,
    sum(f.cpu_time_ms) AS cpu_time_ms
FROM iceberg.meta.fact_query f
LEFT JOIN iceberg.meta.dim_query_type qt ON qt.query_type = f.query_type
WHERE f.query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
  AND qt.query_type IS NULL
GROUP BY f.query_type
ORDER BY queries DESC
