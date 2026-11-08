-- Every table read in the period is registered in dim_table.
-- Also its last_seen_date must not be earlier than the last read.
SELECT
    a.table_key,
    max(a.query_date)     AS last_read_date,
    max(d.last_seen_date) AS dim_last_seen_date
FROM iceberg.meta.fact_query_table_access a
LEFT JOIN iceberg.meta.dim_table d ON d.table_key = a.table_key
WHERE a.query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY a.table_key
HAVING max(d.last_seen_date) IS NULL
    OR max(d.last_seen_date) < max(a.query_date)
ORDER BY 1
