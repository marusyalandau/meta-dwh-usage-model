-- Load fact_query_table_access for one day ({{ run_date }}).
-- One row per query x table read. Query CPU is split across tables:
--   * proportionally to bytes read from each table, if bytes are known;
--   * equally between tables otherwise.
-- Depends on: fact_query (for team_id, query_type, source).

DELETE FROM iceberg.meta.fact_query_table_access
WHERE query_date = DATE '{{ run_date }}';

INSERT INTO iceberg.meta.fact_query_table_access
WITH exploded AS (
    -- One row per element of the `inputs` array
    SELECT
        q.query_id,
        q.query_state,
        q.cpu_time_ms,
        i.catalog_name || '.' || i.schema_name || '.' || i.table_name AS table_key,
        cardinality(i.columns)  AS column_count,
        i.physical_input_bytes  AS input_bytes
    FROM iceberg.raw.trino_query_completed q
    CROSS JOIN UNNEST(q.inputs) AS i (catalog_name, schema_name, table_name, columns, physical_input_bytes)
    WHERE q.event_date = DATE '{{ run_date }}'
),
per_table AS (
    -- The same table can appear several times in one query (e.g. self-join)
    SELECT
        query_id,
        table_key,
        max(query_state)        AS query_state,
        max(cpu_time_ms)        AS cpu_time_ms,
        max(column_count)       AS column_count,
        sum(input_bytes)        AS input_bytes
    FROM exploded
    GROUP BY query_id, table_key
),
weighted AS (
    SELECT
        *,
        sum(COALESCE(input_bytes, 0)) OVER (PARTITION BY query_id) AS query_input_bytes,
        count(*)                      OVER (PARTITION BY query_id) AS query_table_count
    FROM per_table
)
SELECT
    w.query_id,
    f.query_date,
    f.user_name,
    f.team_id,
    f.query_type,
    f.source,
    w.table_key,
    w.query_state = 'FINISHED'                       AS is_successful,
    CAST(w.column_count AS INTEGER)                  AS column_count,
    w.input_bytes                                    AS physical_input_bytes,
    CAST(round(
        CASE
            WHEN w.query_input_bytes > 0
                THEN w.cpu_time_ms * CAST(COALESCE(w.input_bytes, 0) AS DOUBLE) / w.query_input_bytes
            ELSE w.cpu_time_ms * 1.0 / w.query_table_count
        END
    ) AS BIGINT)                                     AS allocated_cpu_time_ms
FROM weighted w
JOIN iceberg.meta.fact_query f
    ON  f.query_id = w.query_id
    AND f.query_date = DATE '{{ run_date }}';
