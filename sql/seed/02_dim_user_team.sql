-- Sample user -> team mapping (SCD Type 2).
-- Note: 'anna' moved from product_analytics to ml_research on 2026-10-15.
DELETE FROM iceberg.meta.dim_user_team;

INSERT INTO iceberg.meta.dim_user_team
    (user_name, team_id, user_type, valid_from, valid_to, is_current)
VALUES
    ('anna',    'product_analytics', 'human',   DATE '2020-01-01', DATE '2026-10-15', false),
    ('anna',    'ml_research',       'human',   DATE '2026-10-15', DATE '9999-12-31', true),
    ('boris',   'product_analytics', 'human',   DATE '2020-01-01', DATE '9999-12-31', true),
    ('clara',   'data_engineering',  'human',   DATE '2020-01-01', DATE '9999-12-31', true),
    ('dmitry',  'data_engineering',  'human',   DATE '2020-01-01', DATE '9999-12-31', true),
    ('elena',   'marketing',         'human',   DATE '2020-01-01', DATE '9999-12-31', true),
    ('felix',   'ml_research',       'human',   DATE '2020-01-01', DATE '9999-12-31', true),
    ('svc_airflow',  'data_engineering',  'service', DATE '2020-01-01', DATE '9999-12-31', true),
    ('svc_superset', 'product_analytics', 'service', DATE '2020-01-01', DATE '9999-12-31', true);
