-- Maintain dim_table: add new tables, update last_seen_date for known ones.
-- Depends on: fact_query_table_access for {{ run_date }}.

MERGE INTO iceberg.meta.dim_table t
USING (
    SELECT
        table_key,
        split_part(table_key, '.', 1) AS catalog_name,
        split_part(table_key, '.', 2) AS schema_name,
        split_part(table_key, '.', 3) AS table_name,
        min(query_date)               AS seen_date
    FROM iceberg.meta.fact_query_table_access
    WHERE query_date = DATE '{{ run_date }}'
    GROUP BY table_key
) s
ON t.table_key = s.table_key
WHEN MATCHED THEN UPDATE SET
    first_seen_date = least(t.first_seen_date, s.seen_date),
    last_seen_date  = greatest(t.last_seen_date, s.seen_date)
WHEN NOT MATCHED THEN INSERT
    (table_key, catalog_name, schema_name, table_name, is_iceberg, owner_team_id, first_seen_date, last_seen_date)
VALUES
    (s.table_key, s.catalog_name, s.schema_name, s.table_name,
     s.catalog_name = 'iceberg',  -- assumption: catalog name tells the connector type
     NULL,                        -- owner comes from ownership metadata, see README (not implemented)
     s.seen_date, s.seen_date);
