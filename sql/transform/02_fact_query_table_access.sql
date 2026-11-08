-- Load fact_query_table_access for one day ({{ run_date }}).
-- One row per query x data table read. Query CPU is split across tables:
--   * proportionally to bytes read from each table, if bytes are known
--     for all tables of the query;
--   * equally between tables otherwise.
-- Shares are whole milliseconds and add up exactly to the query CPU.
-- Depends on: fact_query (for team_id, query_type, query_category, source).

DELETE FROM iceberg.meta.fact_query_table_access
WHERE query_date = DATE '{{ run_date }}';

INSERT INTO iceberg.meta.fact_query_table_access
WITH raw_dedup AS (
    -- Same deduplication as in 01_fact_query.sql: one row per query_id
    SELECT *
    FROM (
        SELECT
            q.*,
            row_number() OVER (PARTITION BY q.query_id ORDER BY q.end_time DESC) AS copy_number
        FROM iceberg.raw.trino_query_completed q
        WHERE q.event_date = DATE '{{ run_date }}'
    )
    WHERE copy_number = 1
),
exploded AS (
    -- One row per element of the `inputs` array
    SELECT
        q.query_id,
        q.query_state,
        q.cpu_time_ms,
        i.catalog_name || '.' || i.schema_name || '.' || i.table_name AS table_key,
        cardinality(i.columns)  AS column_count,
        i.physical_input_bytes  AS input_bytes
    FROM raw_dedup q
    CROSS JOIN UNNEST(q.inputs) AS i (catalog_name, schema_name, table_name, columns, physical_input_bytes)
    -- System tables are metadata, not data (same rule as in 01_fact_query.sql)
    WHERE NOT (i.catalog_name = 'system' OR i.schema_name = 'information_schema')
),
per_table AS (
    -- The same table can appear several times in one query (e.g. self-join)
    SELECT
        query_id,
        table_key,
        max(query_state)        AS query_state,
        max(cpu_time_ms)        AS cpu_time_ms,
        max(column_count)       AS column_count,
        sum(input_bytes)        AS input_bytes,
        count(*) = count(input_bytes) AS bytes_known
    FROM exploded
    GROUP BY query_id, table_key
),
weighted AS (
    SELECT
        *,
        CASE
            WHEN bool_and(bytes_known) OVER (PARTITION BY query_id)
             AND sum(input_bytes)      OVER (PARTITION BY query_id) > 0
                THEN CAST(input_bytes AS DOUBLE) / sum(input_bytes) OVER (PARTITION BY query_id)
            ELSE 1.0E0 / count(*) OVER (PARTITION BY query_id)
        END AS cpu_weight
    FROM per_table
),
rounded AS (
    -- Largest remainder rounding: every table gets the whole milliseconds of
    -- its exact share; the milliseconds left over go one by one to the tables
    -- with the largest fractional parts. The shares then sum to the query CPU.
    SELECT
        *,
        floor(cpu_time_ms * cpu_weight) AS floor_cpu,
        cpu_time_ms - sum(floor(cpu_time_ms * cpu_weight)) OVER (PARTITION BY query_id) AS leftover_ms,
        row_number() OVER (
            PARTITION BY query_id
            ORDER BY cpu_time_ms * cpu_weight - floor(cpu_time_ms * cpu_weight) DESC, table_key
        ) AS remainder_rank
    FROM weighted
)
SELECT
    r.query_id,
    f.query_date,
    f.user_name,
    f.team_id,
    f.query_type,
    f.query_category,
    f.source,
    r.table_key,
    r.query_state = 'FINISHED'                       AS is_successful,
    CAST(r.column_count AS INTEGER)                  AS column_count,
    r.input_bytes                                    AS physical_input_bytes,
    CAST(r.floor_cpu AS BIGINT)
        + IF(r.remainder_rank <= r.leftover_ms, 1, 0) AS allocated_cpu_time_ms
FROM rounded r
JOIN iceberg.meta.fact_query f
    ON  f.query_id = r.query_id
    AND f.query_date = DATE '{{ run_date }}';
