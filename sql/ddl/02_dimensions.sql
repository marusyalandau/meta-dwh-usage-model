-- Teams. Source: HR system / org chart export (assumption).
-- Grain: one row per team.
CREATE TABLE IF NOT EXISTS iceberg.meta.dim_team (
    team_id       VARCHAR,
    team_name     VARCHAR,
    department    VARCHAR
);

-- User -> team mapping with history (SCD Type 2).
-- A user can move between teams; each row is valid in [valid_from, valid_to).
-- The current row has valid_to = DATE '9999-12-31'.
-- Grain: one row per user per period of team membership.
CREATE TABLE IF NOT EXISTS iceberg.meta.dim_user_team (
    user_name     VARCHAR,
    team_id       VARCHAR,
    user_type     VARCHAR,   -- human | service
    valid_from    DATE,
    valid_to      DATE,
    is_current    BOOLEAN
);

-- Tables that were ever read through Trino.
-- Grain: one row per fully qualified table name (catalog.schema.table).
-- owner_team_id is optional metadata (e.g. from table properties or a config file).
CREATE TABLE IF NOT EXISTS iceberg.meta.dim_table (
    table_key       VARCHAR,   -- catalog.schema.table
    catalog_name    VARCHAR,
    schema_name     VARCHAR,
    table_name      VARCHAR,
    is_iceberg      BOOLEAN,
    owner_team_id   VARCHAR,
    first_seen_date DATE,
    last_seen_date  DATE
);

-- Trino query types grouped into a few categories for reporting.
-- Kept as a small reference table (not hard-coded in SQL), so categories
-- can be changed by editing the seed and reloading the aggregates.
-- Grain: one row per Trino query type.
CREATE TABLE IF NOT EXISTS iceberg.meta.dim_query_type (
    query_type      VARCHAR,   -- value of QueryCompletedEvent.queryType
    query_category  VARCHAR,   -- read | write | maintenance | metadata
    description     VARCHAR
);
