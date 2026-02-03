-- Force DuckDB execution
SET duckdb.force_execution = true;

WITH domain_a AS (
    SELECT stem, outlier_ratio
    FROM view_trend_metrics_by_domain
    WHERE root_domain = 'spiegel.de'       -- Source A
      AND "language" = 'de'             -- Mandatory Language Filter
      AND time_slice = DATE_TRUNC('day', NOW())
      AND outlier_ratio > 1.5           -- Threshold for "Trending"
),
domain_b AS (
    SELECT stem, outlier_ratio
    FROM view_trend_metrics_by_domain
    WHERE root_domain = 'tagesschau.de'   -- Source B
      AND "language" = 'de'
      AND time_slice = DATE_TRUNC('day', NOW())
      AND outlier_ratio > 1.5
),
all_joined AS (
    -- Combine and Classify
    SELECT
        COALESCE(a.stem, b.stem) as trend_topic,
        COALESCE(a.outlier_ratio, 0) as score_a,
        COALESCE(b.outlier_ratio, 0) as score_b,
        CASE
            WHEN a.stem IS NOT NULL AND b.stem IS NOT NULL THEN 'INTERSECT'
            WHEN a.stem IS NOT NULL AND b.stem IS NULL THEN 'BLINDSPOT_A'
            WHEN a.stem IS NULL AND b.stem IS NOT NULL THEN 'BLINDSPOT_B'
        END as classification
    FROM domain_a a
    FULL OUTER JOIN domain_b b ON a.stem = b.stem
),
ranked_trends AS (
    -- Rank them 1-N within their specific group
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY classification
            ORDER BY
                -- Dynamic ordering based on the group
                CASE
                    WHEN classification = 'INTERSECT' THEN (score_a + score_b) -- Strongest combined
                    WHEN classification = 'BLINDSPOT_A' THEN score_a           -- Strongest in A
                    WHEN classification = 'BLINDSPOT_B' THEN score_b           -- Strongest in B
                END DESC
        ) as rank_group
    FROM all_joined
)
-- Final Filter: Only keep the Top 5 for each group
SELECT
    classification,
    rank_group,
    trend_topic,
    ROUND(score_a::numeric, 2) as score_a,
    ROUND(score_b::numeric, 2) as score_b
FROM ranked_trends
WHERE rank_group <= 5
ORDER BY classification, rank_group;
