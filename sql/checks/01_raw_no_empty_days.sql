-- Every day of the period has raw events.
-- An empty day almost always means that event delivery was down,
-- not that nobody used Trino.
SELECT d.day AS empty_day
FROM UNNEST(sequence(DATE '{{ start_date }}', DATE '{{ end_date }}')) AS d (day)
LEFT JOIN (
    SELECT DISTINCT event_date
    FROM iceberg.raw.trino_query_completed
    WHERE event_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
) r ON r.event_date = d.day
WHERE r.event_date IS NULL
ORDER BY 1
