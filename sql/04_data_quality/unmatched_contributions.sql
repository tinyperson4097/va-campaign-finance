-- unmatched_contributions: Schedule D expenses that are political
-- contributions to another candidate committee, with no matching Schedule A
-- receipt on the recipient's side. Ports the intent of
-- python_analysis_scripts/unmatched_contributions_analysis_optimized.py's
-- exact-match logic (name match via committee_mappings, amount within a
-- cent, transaction_date within [-30, +60] days) into one SQL query. Where
-- the pandas version retried each of a candidate's committee codes in
-- sequence until one matched, this checks all of the candidate's committee
-- codes in one EXISTS clause -- same result, fewer moving parts. Non-candidate
-- recipients (candidate_name_normalized = 'NOT A CC' in committee_mappings)
-- are only checked against the one matched committee_code, matching the
-- original script's behavior of not searching for alternates for non-CCs.
--
-- Depends on `{{gold_dataset}}.parse_va_date` (sql/00_setup/udf_parse_date.sql).
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.unmatched_contributions` AS
WITH schedule_d AS (
  SELECT
    *,
    `{{project_id}}.{{gold_dataset}}.parse_va_date`(transaction_date) AS transaction_date_parsed
  FROM `{{project_id}}.{{silver_dataset}}.cf_clean`
  WHERE transaction_type = 'ScheduleD'
    AND report_year >= 2018
    AND committee_type IN ('Political Action Committee', 'Out of State Political Committee', 'Political Party Committee')
    AND (
      REGEXP_CONTAINS(LOWER(purpose), r'\b(political|campaign)\s+contribution\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bcontribution\s+(to|for)\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bpac\s+contribution\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\b(primary|general)\s+\d{4}\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bfundraiser\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bstate\s+committee\s+contribution\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bcontribution\b')
    )
    AND amount >= 1000
    AND entity_name IS NOT NULL AND entity_name != '' AND LENGTH(entity_name) > 1
),
matched AS (
  SELECT
    d.*,
    cm.committee_code AS matched_committee_code,
    cm.committee_name_normalized AS matched_committee_name_normalized,
    cm.candidate_name_normalized AS matched_candidate_name
  FROM schedule_d d
  JOIN `{{project_id}}.{{silver_dataset}}.committee_mappings` cm
    ON cm.committee_name_normalized = d.entity_name_normalized
),
candidate_committee_codes AS (
  SELECT candidate_name_normalized, ARRAY_AGG(committee_code) AS committee_codes
  FROM `{{project_id}}.{{silver_dataset}}.committee_mappings`
  WHERE candidate_name_normalized IS NOT NULL AND candidate_name_normalized != 'NOT A CC'
  GROUP BY candidate_name_normalized
),
schedule_a AS (
  SELECT
    committee_code AS recipient_committee_code,
    entity_name_normalized AS donor_name_normalized,
    amount,
    `{{project_id}}.{{gold_dataset}}.parse_va_date`(transaction_date) AS transaction_date_parsed
  FROM `{{project_id}}.{{silver_dataset}}.cf_clean`
  WHERE transaction_type = 'ScheduleA'
    AND report_year >= 2018
    AND committee_type = 'Candidate Campaign Committee'
    AND amount >= 1000
    AND entity_name IS NOT NULL AND entity_name != '' AND LENGTH(entity_name) > 5
)
SELECT
  m.report_id,
  m.committee_code AS donor_committee_code,
  m.committee_name AS donor_committee_name,
  m.committee_name_normalized AS donor_committee_name_normalized,
  m.entity_name AS recipient_name,
  m.entity_name_normalized AS recipient_name_normalized,
  m.amount,
  m.transaction_date,
  m.purpose,
  m.report_year,
  m.data_source,
  m.folder_name,
  m.matched_committee_code,
  m.matched_committee_name_normalized,
  m.matched_candidate_name
FROM matched m
LEFT JOIN candidate_committee_codes ccc ON ccc.candidate_name_normalized = m.matched_candidate_name
WHERE m.transaction_date_parsed IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM schedule_a a
    WHERE a.recipient_committee_code IN UNNEST(
            COALESCE(ccc.committee_codes, [m.matched_committee_code])
          )
      AND a.donor_name_normalized = m.committee_name_normalized
      AND a.transaction_date_parsed IS NOT NULL
      AND ABS(a.amount - m.amount) <= 0.01
      AND a.transaction_date_parsed BETWEEN
            DATE_SUB(m.transaction_date_parsed, INTERVAL 30 DAY)
            AND DATE_ADD(m.transaction_date_parsed, INTERVAL 60 DAY)
  );
