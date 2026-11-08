-- Daily usage per table, user, query category and client.
-- Keeps user granularity so that distinct users can be counted
-- correctly over any period (distinct counts cannot be summed across days).
-- Grain: usage_date x table_key x user_name x query_category x source.
CREATE TABLE IF NOT EXISTS iceberg.meta.agg_table_user_daily (
    usage_date            DATE,
    table_key             VARCHAR,
    user_name             VARCHAR,
    team_id               VARCHAR,
    query_category        VARCHAR,   -- read | write | maintenance | metadata | other
    source                VARCHAR,   -- client: datagrip, airflow, superset, ...
    query_count           BIGINT,
    failed_query_count    BIGINT,
    physical_input_bytes  BIGINT,
    allocated_cpu_time_ms BIGINT
)
WITH (
    partitioning = ARRAY['month(usage_date)']
);

-- Daily resource usage per team, query category and client.
-- All metrics except active_user_count are additive and safe to sum.
-- Grain: usage_date x team_id x query_category x source.
CREATE TABLE IF NOT EXISTS iceberg.meta.agg_team_resources_daily (
    usage_date            DATE,
    team_id               VARCHAR,
    query_category        VARCHAR,   -- read | write | maintenance | metadata | other
    source                VARCHAR,   -- client: datagrip, airflow, superset, ...
    query_count           BIGINT,
    failed_query_count    BIGINT,
    active_user_count     BIGINT,   -- distinct users within this row only; do NOT sum over days, categories or sources
    cpu_time_ms           BIGINT,
    wall_time_ms          BIGINT,
    queued_time_ms        BIGINT,
    physical_input_bytes  BIGINT,
    output_bytes          BIGINT
)
WITH (
    partitioning = ARRAY['month(usage_date)']
);
