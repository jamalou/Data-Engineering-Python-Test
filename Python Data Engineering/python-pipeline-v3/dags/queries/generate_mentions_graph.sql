WITH pubmed_mentions AS (
  SELECT
    INITCAP(d.drug) AS drug,
    LOWER(p.journal) AS journal,
    ARRAY_AGG(STRUCT(p.title AS title, p.date AS `date`)) AS PubMed,
  FROM
    `{{ var.value.project_id }}.{{ var.value.raw_dataset }}.drugs` d
    LEFT JOIN `{{ var.value.project_id }}.{{ var.value.staging_dataset }}.pubmed` p
    ON REGEXP_CONTAINS(LOWER(p.title), CONCAT(r'\b', LOWER(d.drug), r'\b'))
  GROUP BY drug, journal
),
trials_mentions AS (
  SELECT
    INITCAP(d.drug) AS drug,
    LOWER(t.journal) AS journal,
    ARRAY_AGG(STRUCT(t.title AS title, t.date AS `date`)) AS `Clinical Trials`,
  FROM
    `{{ var.value.project_id }}.{{ var.value.raw_dataset }}.drugs` d
    LEFT JOIN `{{ var.value.project_id }}.{{ var.value.staging_dataset }}.clinical_trials` t
    ON REGEXP_CONTAINS(LOWER(t.title), CONCAT(r'\b', LOWER(d.drug), r'\b'))
  GROUP BY drug, journal
),
all_mentions AS (
  SELECT
    drug,
    journal,
    PubMed,
    `Clinical Trials`
  FROM
    pubmed_mentions p
    FULL OUTER JOIN trials_mentions t
    USING(drug, journal)
  WHERE
    journal IS NOT NULL
)
SELECT
  drug,
  ARRAY_AGG(STRUCT(journal, PubMed, `Clinical Trials`)) AS mentions
FROM
  all_mentions
GROUP BY
  drug
