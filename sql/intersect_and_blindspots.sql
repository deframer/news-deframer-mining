SET duckdb.force_execution = true;

SET SESSION "vars.last" = '30 days';
SET SESSION "vars.top" = 10;

SET SESSION "vars.domain_a" = 'spiegel.de';
SET SESSION "vars.domain_b" = 'tagesschau.de';

-- Parameters
WITH params AS (
    SELECT
        current_date - current_setting('vars.last')::INTERVAL AS start_date,
        current_date AS end_date,
        current_setting('vars.domain_a')::VARCHAR AS root_domain_a,
        current_setting('vars.domain_b')::VARCHAR AS root_domain_b,
        current_setting('vars.top')::INT AS result_limit
),
filtered_docs AS (
    SELECT td.*, p.root_domain_a, p.root_domain_b
    FROM trends td
    CROSS JOIN params p
    WHERE td.pub_date IS NOT NULL
      AND td.pub_date >= p.start_date
      AND td.pub_date < p.end_date + INTERVAL '1 day'
),
unnested AS (
    SELECT
        td.root_domain,
        unnested.noun
    FROM filtered_docs td
    CROSS JOIN UNNEST(noun_stems) AS unnested(noun)
    WHERE unnested.noun IS NOT NULL AND unnested.noun <> ''
),
ranked_a AS (
    SELECT
        noun AS word,
        COUNT(*) AS word_count,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, noun ASC) AS rank
    FROM unnested, params p
    WHERE root_domain = p.root_domain_a
    GROUP BY noun
),
ranked_b AS (
    SELECT
        noun AS word,
        COUNT(*) AS word_count,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, noun ASC) AS rank
    FROM unnested, params p
    WHERE root_domain = p.root_domain_b
    GROUP BY noun
),
set_a AS (
    SELECT word, word_count FROM ranked_a WHERE rank <= (SELECT result_limit FROM params)
),
set_b AS (
    SELECT word, word_count FROM ranked_b WHERE rank <= (SELECT result_limit FROM params)
),
intersection AS (
    SELECT a.word, a.word_count AS count_a, b.word_count AS count_b
    FROM set_a a
    INNER JOIN set_b b ON a.word = b.word
),
blindspots_a AS (
    SELECT a.word, a.word_count
    FROM set_a a
    LEFT JOIN set_b b ON a.word = b.word
    WHERE b.word IS NULL
),
blindspots_b AS (
    SELECT b.word, b.word_count
    FROM set_b b
    LEFT JOIN set_a a ON a.word = b.word
    WHERE a.word IS NULL
)
SELECT 'intersection' AS category, word, count_a, count_b FROM intersection
UNION ALL
SELECT 'blindspots_a_has' AS category, word, word_count, NULL FROM blindspots_a
UNION ALL
SELECT 'blindspots_b_has' AS category, word, NULL, word_count FROM blindspots_b
ORDER BY category, word;
