-- Load fact_query for one day ({{ run_date }}).
-- Idempotent: the day is deleted first, so re-runs and backfills are safe.

DELETE FROM iceberg.meta.fact_query
WHERE query_date = DATE '{{ run_date }}';

INSERT INTO iceberg.meta.fact_query
SELECT
    q.query_id,
    q.event_date                         AS query_date,
    q.user_name,
    COALESCE(ut.team_id, 'unknown')      AS team_id,
    q.query_state,
    q.query_type,
    q.source,
    q.create_time,
    q.end_time,
    q.cpu_time_ms,
    q.wall_time_ms,
    q.queued_time_ms,
    q.peak_memory_bytes,
    q.physical_input_bytes,
    q.output_bytes,
    CAST(COALESCE(cardinality(q.inputs), 0) AS INTEGER) AS input_table_count,
    CASE
        WHEN q.output_table IS NOT NULL
            THEN q.output_catalog || '.' || q.output_schema || '.' || q.output_table
    END                                  AS output_table_key
FROM iceberg.raw.trino_query_completed q
-- Team membership on the query date (SCD Type 2 lookup)
LEFT JOIN iceberg.meta.dim_user_team ut
    ON  ut.user_name = q.user_name
    AND q.event_date >= ut.valid_from
    AND q.event_date <  ut.valid_to
WHERE q.event_date = DATE '{{ run_date }}';
