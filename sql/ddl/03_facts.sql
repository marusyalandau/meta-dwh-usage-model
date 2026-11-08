-- Fact: one row per query. Holds full resource usage of the query.
-- Used for "how many resources did team Y consume".
-- team_id is resolved at load time from dim_user_team (team on the query date).
CREATE TABLE IF NOT EXISTS iceberg.meta.fact_query (
    query_id              VARCHAR,
    query_date            DATE,
    user_name             VARCHAR,
    team_id               VARCHAR,
    query_state           VARCHAR,
    query_type            VARCHAR,
    source                VARCHAR,
    create_time           TIMESTAMP(6) WITH TIME ZONE,
    end_time              TIMESTAMP(6) WITH TIME ZONE,
    cpu_time_ms           BIGINT,
    wall_time_ms          BIGINT,
    queued_time_ms        BIGINT,
    peak_memory_bytes     BIGINT,
    physical_input_bytes  BIGINT,
    output_bytes          BIGINT,
    input_table_count     INTEGER,
    output_table_key      VARCHAR    -- table written by the query (catalog.schema.table), NULL for reads
)
WITH (
    partitioning = ARRAY['query_date']
);

-- Fact (bridge): one row per query x table it read.
-- Used for "how many distinct users queried table X".
-- allocated_cpu_time_ms splits the query CPU across its tables
-- (proportional to bytes read, or equally if bytes are unknown).
CREATE TABLE IF NOT EXISTS iceberg.meta.fact_query_table_access (
    query_id              VARCHAR,
    query_date            DATE,
    user_name             VARCHAR,
    team_id               VARCHAR,
    query_type            VARCHAR,
    source                VARCHAR,
    table_key             VARCHAR,
    is_successful         BOOLEAN,
    column_count          INTEGER,
    physical_input_bytes  BIGINT,
    allocated_cpu_time_ms BIGINT
)
WITH (
    partitioning = ARRAY['query_date']
);
