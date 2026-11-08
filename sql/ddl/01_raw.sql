-- Raw layer: one row per completed Trino query (QueryCompletedEvent).
-- In production this table is filled by the event listener pipeline
-- (event listener -> Kafka/HTTP -> Iceberg). Locally it is filled by
-- pipeline/generate_events.py.
-- Grain: one row per query_id.
CREATE TABLE IF NOT EXISTS iceberg.raw.trino_query_completed (
    query_id              VARCHAR,
    query_state           VARCHAR,   -- FINISHED | FAILED
    error_code            VARCHAR,   -- NULL for successful queries
    query_type            VARCHAR,   -- SELECT | INSERT | MERGE | ...
    user_name             VARCHAR,   -- Trino principal / user
    source                VARCHAR,   -- client: datagrip, airflow, superset, ...
    resource_group_id     VARCHAR,
    query_text            VARCHAR,
    create_time           TIMESTAMP(6) WITH TIME ZONE,
    execution_start_time  TIMESTAMP(6) WITH TIME ZONE,
    end_time              TIMESTAMP(6) WITH TIME ZONE,
    -- Resource statistics for the whole query
    cpu_time_ms           BIGINT,
    wall_time_ms          BIGINT,
    queued_time_ms        BIGINT,
    peak_memory_bytes     BIGINT,
    physical_input_bytes  BIGINT,
    physical_input_rows   BIGINT,
    output_bytes          BIGINT,
    -- Tables read by the query (ioMetadata.inputs)
    inputs ARRAY(ROW(
        catalog_name          VARCHAR,
        schema_name           VARCHAR,
        table_name            VARCHAR,
        columns               ARRAY(VARCHAR),
        physical_input_bytes  BIGINT
    )),
    -- Table written by the query, if any (ioMetadata.output)
    output_catalog        VARCHAR,
    output_schema         VARCHAR,
    output_table          VARCHAR,
    -- Partition key: UTC date of end_time
    event_date            DATE
)
WITH (
    partitioning = ARRAY['event_date']
);
