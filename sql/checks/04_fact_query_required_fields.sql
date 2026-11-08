-- Key columns of fact_query are filled and resource metrics are not negative.
SELECT
    query_id,
    query_date,
    user_name,
    team_id,
    query_state,
    query_type,
    cpu_time_ms,
    wall_time_ms,
    physical_input_bytes
FROM iceberg.meta.fact_query
WHERE query_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
  AND (
        query_id IS NULL
     OR user_name IS NULL
     OR team_id IS NULL
     OR query_state IS NULL
     OR query_type IS NULL
     OR cpu_time_ms IS NULL
     OR cpu_time_ms < 0
     OR wall_time_ms < 0
     OR physical_input_bytes < 0
  )
LIMIT 100
