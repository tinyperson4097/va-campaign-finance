-- dim_entity: donors/vendors, one row per entity_name_normalized.
-- is_individual is TRUE if the entity was ever flagged individual on any row,
-- since the same normalized name occasionally has a null/inconsistent flag
-- across transactions.
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.dim_entity` AS
WITH entity_rows AS (
  SELECT
    entity_name_normalized,
    entity_is_individual,
    entity_employer,
    entity_occupation,
    entity_city,
    entity_state,
    report_year
  FROM `{{project_id}}.{{silver_dataset}}.cf_clean`
  WHERE entity_name_normalized IS NOT NULL AND entity_name_normalized != ''
),
variations AS (
  SELECT
    normalized_name AS entity_name_normalized,
    ARRAY_AGG(DISTINCT name_variation IGNORE NULLS) AS name_variations
  FROM `{{project_id}}.{{silver_dataset}}.name_variations`
  GROUP BY normalized_name
)
SELECT
  e.entity_name_normalized,
  LOGICAL_OR(COALESCE(e.entity_is_individual, FALSE)) AS is_individual,
  ANY_VALUE(e.entity_employer) AS entity_employer,
  ANY_VALUE(e.entity_occupation) AS entity_occupation,
  ANY_VALUE(e.entity_city) AS entity_city,
  ANY_VALUE(e.entity_state) AS entity_state,
  MIN(e.report_year) AS first_seen_year,
  MAX(e.report_year) AS last_seen_year,
  v.name_variations
FROM entity_rows e
LEFT JOIN variations v ON v.entity_name_normalized = e.entity_name_normalized
GROUP BY e.entity_name_normalized, v.name_variations;
