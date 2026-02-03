SET duckdb.force_execution = true;

SELECT
    stem as trend_topic,
    frequency,
    utility,
    outlier_ratio
FROM view_trend_metrics_by_domain  -- <--- Use the Domain View
WHERE "language" = 'de'
  AND root_domain = 'tagesschau.de'   -- <--- Filter by Domain
  AND stem_type = 'NOUN'
  AND time_slice = DATE_TRUNC('day', NOW())
  -- Note: Utility threshold might need to be lower for a single domain
  -- (e.g. > 1 means it appeared in at least 2 different RSS feeds on that site)
  AND utility >= 1
  AND outlier_ratio > 1.5
ORDER BY outlier_ratio DESC
LIMIT 20;