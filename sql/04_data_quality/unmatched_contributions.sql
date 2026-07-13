-- unmatched_contributions: Schedule D expenses that are political
-- contributions to another committee, with no matching Schedule A receipt
-- on the recipient's side ("committee reported, recipient did not").
--
-- Recipient scope: any recipient that maps to a committee in
-- committee_mappings. matched_candidate_name shows which kind each row is --
-- a real candidate name for candidate campaign committees, or the literal
-- 'NOT A CC' for non-candidate committees (PACs etc.), so results can be
-- filtered either way. Both kinds can genuinely match: the Schedule A pool
-- covers ALL committee types, because non-candidate committees file
-- Schedule A too. Candidate recipients are checked against receipts under
-- ANY of that candidate's committee codes (candidates re-register a new
-- code each cycle); non-candidate recipients are checked against their one
-- matched committee_code.
--
-- Match = exact normalized-name match of the donor committee on both sides,
-- amount within a cent, and transaction_date within [-30, +60] days of the
-- Schedule D date. Rows whose date string doesn't parse are still matched
-- on name + amount rather than dropped or auto-flagged.
--
-- Several committee codes can share one committee_name_normalized (a
-- candidate re-registers each cycle), which would fan each Schedule D row out
-- into multiple output rows via the join. Each source row gets a surrogate
-- d_row_id, and per row we keep the single mapping whose committee-code year
-- (the YY in 'CC-YY-...') is closest to the filing year. This only decides
-- which code is *reported* in matched_committee_code -- the match itself
-- checks all of the candidate's codes.
--
-- Depends on `{{gold_dataset}}.parse_va_date` (sql/00_setup/udf_parse_date.sql).
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.unmatched_contributions` AS
WITH schedule_d AS (
  SELECT
    *,
    ROW_NUMBER() OVER () AS d_row_id,
    `{{project_id}}.{{gold_dataset}}.parse_va_date`(transaction_date) AS transaction_date_parsed
  FROM `{{project_id}}.{{silver_dataset}}.cf_clean`
  WHERE transaction_type = 'ScheduleD'
    AND report_year >= 2018
    AND committee_type IN ('Political Action Committee', 'Out of State Political Committee', 'Political Party Committee')
    -- Purposes that mark a Schedule D expense as a political contribution
    -- (curated pattern list, kept verbatim -- changes are additive only).
    AND (
      REGEXP_CONTAINS(LOWER(purpose), r'\b(political|campaign)\s+contribution\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bcontribution\s+(to|for)\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bpac\s+contribution\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\b(primary|general)\s+\d{4}\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bfundraiser\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bstate\s+committee\s+contribution\b')
      OR REGEXP_CONTAINS(LOWER(purpose), r'\bcontribution\b')
    )
    -- Refund/share lines aren't incoming contributions and can't have a
    -- Schedule A receipt (owner-approved exclusion, 2026-07-13).
    AND NOT REGEXP_CONTAINS(LOWER(purpose), r'\b(refund|share)\b')
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
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY d.d_row_id
    ORDER BY
      COALESCE(
        ABS(
          COALESCE(CAST(d.report_year AS INT64), 2020) - (
            CASE
              WHEN CAST(REGEXP_EXTRACT(cm.committee_code, r'^CC-(\d{2})-') AS INT64) < 50
                THEN 2000 + CAST(REGEXP_EXTRACT(cm.committee_code, r'^CC-(\d{2})-') AS INT64)
              ELSE 1900 + CAST(REGEXP_EXTRACT(cm.committee_code, r'^CC-(\d{2})-') AS INT64)
            END
          )
        ),
        9999
      ),
      cm.committee_code
  ) = 1
),
candidate_committee_codes AS (
  SELECT candidate_name_normalized, ARRAY_AGG(committee_code) AS committee_codes
  FROM `{{project_id}}.{{silver_dataset}}.committee_mappings`
  WHERE candidate_name_normalized IS NOT NULL AND candidate_name_normalized != 'NOT A CC'
  GROUP BY candidate_name_normalized
),
schedule_a AS (
  -- The recipient side: Schedule A receipts from ALL committee types, since
  -- non-candidate committees (PACs etc.) file Schedule A too.
  -- LENGTH > 5 is a deliberate rule (matches the Python script) -- keep
  -- verbatim, changes are additive only.
  SELECT
    committee_code AS recipient_committee_code,
    entity_name_normalized AS donor_name_normalized,
    amount,
    `{{project_id}}.{{gold_dataset}}.parse_va_date`(transaction_date) AS transaction_date_parsed
  FROM `{{project_id}}.{{silver_dataset}}.cf_clean`
  WHERE transaction_type = 'ScheduleA'
    AND report_year >= 2018
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
-- Candidates: check all of that candidate's committee codes. Non-CC
-- recipients ('NOT A CC') never join here, so COALESCE falls back to the one
-- matched committee_code.
LEFT JOIN candidate_committee_codes ccc ON ccc.candidate_name_normalized = m.matched_candidate_name
WHERE NOT EXISTS (
    SELECT 1
    FROM schedule_a a
    WHERE a.recipient_committee_code IN UNNEST(
            COALESCE(ccc.committee_codes, [m.matched_committee_code])
          )
      AND a.donor_name_normalized = m.committee_name_normalized
      AND ABS(a.amount - m.amount) <= 0.01
      -- Unparseable dates (either side) fall back to name + amount only,
      -- rather than blocking the match or dropping the row.
      AND (
        m.transaction_date_parsed IS NULL
        OR a.transaction_date_parsed IS NULL
        OR a.transaction_date_parsed BETWEEN
             DATE_SUB(m.transaction_date_parsed, INTERVAL 30 DAY)
             AND DATE_ADD(m.transaction_date_parsed, INTERVAL 60 DAY)
      )
  );
