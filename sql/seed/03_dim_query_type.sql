-- Mapping of Trino query types to reporting categories.
-- Types not listed here are reported as 'other'.
DELETE FROM iceberg.meta.dim_query_type;

INSERT INTO iceberg.meta.dim_query_type (query_type, query_category, description) VALUES
    ('SELECT',              'read',        'Reads data: analytics, dashboards, exports'),
    ('INSERT',              'write',       'INSERT and CREATE TABLE AS: pipelines producing tables'),
    ('MERGE',               'write',       'Upserts into existing tables'),
    ('UPDATE',              'write',       'Row updates'),
    ('DELETE',              'write',       'Row deletes, including retention'),
    ('ALTER_TABLE_EXECUTE', 'maintenance', 'Table procedures such as optimize and expire_snapshots'),
    ('ANALYZE',             'maintenance', 'Collecting table statistics'),
    ('DATA_DEFINITION',     'metadata',    'CREATE, DROP, ALTER of schemas and tables'),
    ('DESCRIBE',            'metadata',    'SHOW and DESCRIBE, e.g. IDE schema introspection'),
    ('EXPLAIN',             'metadata',    'Query plans');
