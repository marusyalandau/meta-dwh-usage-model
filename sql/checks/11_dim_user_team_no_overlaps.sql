-- User -> team periods in dim_user_team do not overlap.
-- With overlapping periods a query matches two teams on the same date
-- and is loaded twice into fact_query.
WITH periods AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY user_name ORDER BY valid_from, valid_to, team_id) AS period_number
    FROM iceberg.meta.dim_user_team
)
SELECT
    a.user_name,
    a.team_id    AS team_a,
    a.valid_from AS valid_from_a,
    a.valid_to   AS valid_to_a,
    b.team_id    AS team_b,
    b.valid_from AS valid_from_b,
    b.valid_to   AS valid_to_b
FROM periods a
JOIN periods b
    ON  b.user_name = a.user_name
    AND b.period_number > a.period_number
    AND b.valid_from < a.valid_to
    AND a.valid_from < b.valid_to
ORDER BY a.user_name, a.valid_from
