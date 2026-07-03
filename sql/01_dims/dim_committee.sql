-- dim_committee: one row per committee_code.
-- committee_name_normalized / candidate_name_normalized come from
-- committee_mappings (the most-frequent-name-wins table from
-- create_mapping_tables.py) rather than being recomputed here, so this
-- dimension always agrees with the lookup table the analysis scripts join
-- against.
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.dim_committee` AS
WITH committee_attrs AS (
  SELECT
    committee_code,
    committee_name,
    committee_type,
    party,
    report_year
  FROM `{{project_id}}.{{silver_dataset}}.campaign_finance`
  WHERE committee_code IS NOT NULL AND committee_code != ''
),
attr_counts AS (
  SELECT
    committee_code,
    committee_name,
    committee_type,
    party,
    COUNT(*) AS cnt
  FROM committee_attrs
  GROUP BY committee_code, committee_name, committee_type, party
),
most_common_attr AS (
  SELECT
    committee_code,
    committee_name,
    committee_type,
    party,
    ROW_NUMBER() OVER (PARTITION BY committee_code ORDER BY cnt DESC) AS rn
  FROM attr_counts
),
years AS (
  SELECT
    committee_code,
    MIN(report_year) AS first_seen_year,
    MAX(report_year) AS last_seen_year
  FROM committee_attrs
  GROUP BY committee_code
)
SELECT
  cm.committee_code,
  cm.committee_name_normalized,
  cm.candidate_name_normalized,
  cm.candidate_name_normalized != 'NOT A CC' AS is_candidate_committee,
  a.committee_name AS committee_name_raw,
  a.committee_type,
  a.party,
  y.first_seen_year,
  y.last_seen_year
FROM `{{project_id}}.{{silver_dataset}}.committee_mappings` cm
LEFT JOIN most_common_attr a ON a.committee_code = cm.committee_code AND a.rn = 1
LEFT JOIN years y ON y.committee_code = cm.committee_code;
