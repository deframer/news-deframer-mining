-- 1. Force DuckDB execution for speed
SET duckdb.force_execution = true;


ERROR 'this probably makes no sense'

-- 2. Query the Top 20 Trends for English
SELECT
    stem as trend_topic,
    frequency,
    utility,
    outlier_ratio
    --root_domain
FROM view_trend_metrics_by_domain
WHERE "language" = 'de'
  --AND root_domain = 'spiegel.de'
  AND stem_type = 'NOUN'
  AND time_slice = DATE_TRUNC('day', NOW()) -- Or specific date
  --AND utility > 3       -- Thesis: Filter out single-source noise
  AND outlier_ratio > 1.5 -- Thesis: It must be significantly higher than usual
ORDER BY outlier_ratio DESC
LIMIT 20;
