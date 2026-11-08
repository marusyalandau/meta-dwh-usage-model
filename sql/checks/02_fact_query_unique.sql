-- fact_query has exactly one row per query_id.
-- A duplicate counts the resources of a query twice. It appears when
-- an event is delivered twice or a day is loaded without deleting it first.
SELECT
    query_id,
    count(*) AS row_count
FROM iceberg.meta.fact_query
WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY query_id
HAVING count(*) > 1
ORDER BY row_count DESC, query_id
