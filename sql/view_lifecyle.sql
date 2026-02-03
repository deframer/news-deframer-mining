SET duckdb.force_execution = true;

-- Check the lifecycle of 'trump'
SELECT
    time_slice,
    frequency,
    growth_delta as velocity
FROM view_trend_metrics
WHERE stem = 'trump'
  AND "language" = 'de'
ORDER BY time_slice DESC;
