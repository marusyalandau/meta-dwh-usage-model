-- Load fact_query for one day ({{ run_date }}).
-- Idempotent: the day is deleted first, so re-runs and backfills are safe.
-- Depends on: dim_user_team, dim_query_type.

DELETE FROM iceberg.meta.fact_query
WHERE query_date = DATE '{{ run_date }}';

INSERT INTO iceberg.meta.fact_query
WITH raw_dedup AS (
    -- Event delivery is at-least-once, so the same query can arrive twice.
    -- Keep one row per query_id (duplicates are identical copies).
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
data_inputs AS (
    -- Data tables read by each query. System tables (information_schema of any
    -- catalog, the `system` catalog) only describe metadata: IDEs such as
    -- DataGrip read them to show the schema tree. Same rule as in
    -- 02_fact_query_table_access.sql.
    SELECT
        q.query_id,
        filter(
            q.inputs,
            i -> NOT (i.catalog_name = 'system' OR i.schema_name = 'information_schema')
        ) AS inputs
    FROM raw_dedup q
)
SELECT
    q.query_id,
    q.event_date                         AS query_date,
    q.user_name,
    COALESCE(ut.team_id, 'unknown')      AS team_id,
    q.query_state,
    q.query_type,
    CASE
        -- A SELECT that reads no data tables (IDE introspection of system
        -- tables, connection checks like SELECT 1) is metadata, not a read
        WHEN q.query_type = 'SELECT' AND COALESCE(cardinality(d.inputs), 0) = 0
            THEN 'metadata'
        ELSE COALESCE(qt.query_category, 'other')
    END                                  AS query_category,
    q.source,
    q.create_time,
    q.end_time,
    q.cpu_time_ms,
    q.wall_time_ms,
    q.queued_time_ms,
    q.peak_memory_bytes,
    q.physical_input_bytes,
    q.output_bytes,
    CAST(COALESCE(cardinality(array_distinct(transform(
        d.inputs, i -> i.catalog_name || '.' || i.schema_name || '.' || i.table_name
    ))), 0) AS INTEGER)                  AS input_table_count,
    CASE
        WHEN q.output_table IS NOT NULL
            THEN q.output_catalog || '.' || q.output_schema || '.' || q.output_table
    END                                  AS output_table_key
FROM raw_dedup q
JOIN data_inputs d
    ON d.query_id = q.query_id
-- Team membership on the query date (SCD Type 2 lookup)
LEFT JOIN iceberg.meta.dim_user_team ut
    ON  ut.user_name = q.user_name
    AND q.event_date >= ut.valid_from
    AND q.event_date <  ut.valid_to
LEFT JOIN iceberg.meta.dim_query_type qt
    ON qt.query_type = q.query_type;
