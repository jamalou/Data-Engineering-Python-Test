CREATE TEMP FUNCTION PARSE_DATE_FUNC(date STRING) AS (
    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', date),
        SAFE.PARSE_DATE('%d/%m/%Y', date),
        SAFE.PARSE_DATE('%d %B %Y', date)
    )
);
SELECT
    id,
    title,
    PARSE_DATE_FUNC(date) AS date,
    journal
FROM `{{ var.value.project_id }}.{{ var.value.raw_dataset }}.clinical_trials`
