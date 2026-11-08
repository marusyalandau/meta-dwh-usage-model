-- Users whose queries could not be attributed to a team.
-- severity: warn
-- Their usage is reported under the 'unknown' team. Usually a new employee
-- or service account that is not in the directory export yet.
SELECT
    f.user_name,
    f.team_id,
    min(f.query_date)  AS first_date,
    max(f.query_date)  AS last_date,
    count(*)           AS queries,
    sum(f.cpu_time_ms) AS cpu_time_ms
FROM iceberg.meta.fact_query f
LEFT JOIN iceberg.meta.dim_team t ON t.team_id = f.team_id
WHERE f.query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
  AND (f.team_id = 'unknown' OR t.team_id IS NULL)
GROUP BY f.user_name, f.team_id
ORDER BY cpu_time_ms DESC
