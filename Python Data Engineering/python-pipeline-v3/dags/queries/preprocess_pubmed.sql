CREATE TEMP FUNCTION PARSE_DATE_FUNC(date STRING) AS (
    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', date),
        SAFE.PARSE_DATE('%d/%m/%Y', date),
        SAFE.PARSE_DATE('%d %B %Y', date)
    )
);
WITH combined_pubmed AS (
    SELECT * FROM `{{ var.value.project_id }}.{{ var.value.raw_dataset }}.pubmed_json`
    UNION ALL
    SELECT * FROM `{{ var.value.project_id }}.{{ var.value.raw_dataset }}.pubmed_csv`
)
SELECT
    id,
    title,
    PARSE_DATE_FUNC(date) AS date,
    journal
FROM combined_pubmed