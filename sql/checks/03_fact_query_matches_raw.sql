-- Every query from the raw log is loaded into fact_query, once.
-- Compares distinct queries in raw with rows in fact_query per day.
WITH raw AS (
    SELECT event_date AS day, count(DISTINCT query_id) AS raw_queries
    FROM iceberg.raw.trino_query_completed
    WHERE event_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY 1
),
fact AS (
    SELECT query_date AS day, count(*) AS fact_rows
    FROM iceberg.meta.fact_query
    WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
    GROUP BY 1
)
SELECT
    COALESCE(r.day, f.day)      AS day,
    COALESCE(r.raw_queries, 0)  AS raw_queries,
    COALESCE(f.fact_rows, 0)    AS fact_rows
FROM raw r
FULL OUTER JOIN fact f ON f.day = r.day
WHERE COALESCE(r.raw_queries, 0) <> COALESCE(f.fact_rows, 0)
ORDER BY 1
