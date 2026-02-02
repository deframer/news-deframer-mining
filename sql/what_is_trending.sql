SET duckdb.force_execution = true;
SET SESSION "vars.domain" = 'bbc.com';
SET SESSION "vars.top" = 5;
SET SESSION "vars.last" = '30 days';

-- Parameters
WITH params AS (
    SELECT
        current_date - current_setting('vars.last')::INTERVAL AS start_date,
        current_date AS end_date,
        current_setting('vars.domain')::VARCHAR AS domain,
        current_setting('vars.top')::INT AS result_limit
),
filtered_docs AS (
    SELECT td.*, p.domain
    FROM trends td
    CROSS JOIN params p
    WHERE td.pub_date IS NOT NULL
      AND td.pub_date >= p.start_date
      AND td.pub_date < p.end_date + INTERVAL '1 day'
      AND (p.domain IS NULL OR td.root_domain = p.domain)
),
unnested AS (
    SELECT UNNEST(noun_stems) AS noun, domain
    FROM filtered_docs
    WHERE noun_stems IS NOT NULL
),
aggregated AS (
    SELECT
        noun AS word,
        COUNT(*) AS word_count
    FROM unnested
    WHERE noun IS NOT NULL AND noun <> ''
    GROUP BY 1
),
total_counts AS (
    SELECT SUM(word_count) AS total_noun_count FROM aggregated
),
ranked AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY word_count DESC, word ASC) AS rank,
        word,
        word_count,
        CASE WHEN total.total_noun_count > 0
             THEN word_count::FLOAT / total.total_noun_count
             ELSE NULL
        END AS relative_score
    FROM aggregated, total_counts total, params p
)
SELECT p.domain AS domain, rank, word, word_count, relative_score
FROM ranked
JOIN params p ON TRUE
WHERE rank <= p.result_limit
ORDER BY rank;
