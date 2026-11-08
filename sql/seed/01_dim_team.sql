-- Sample teams. In production this comes from the HR / org chart system.
DELETE FROM iceberg.meta.dim_team;

INSERT INTO iceberg.meta.dim_team (team_id, team_name, department) VALUES
    ('product_analytics', 'Product Analytics', 'Analytics'),
    ('data_engineering',  'Data Engineering',  'Data Platform'),
    ('marketing',         'Marketing Analytics', 'Marketing'),
    ('ml_research',       'ML Research',       'Research'),
    ('unknown',           'Unknown team',      'Unknown');
