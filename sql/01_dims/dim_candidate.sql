-- dim_candidate: person-level rollup, one row per candidate_name_normalized.
-- A candidate can run multiple committees across cycles (committee_codes
-- array); "most recent" attributes are picked by the latest report_date /
-- report_year seen for that candidate in cf_clean.
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.dim_candidate` AS
WITH candidate_reports AS (
  SELECT
    candidate_name_normalized,
    office_sought_normal,
    district_normal,
    level,
    party,
    election_cycle,
    report_year,
    report_date,
    ROW_NUMBER() OVER (
      PARTITION BY candidate_name_normalized
      ORDER BY report_date DESC NULLS LAST, report_year DESC
    ) AS rn
  FROM `{{project_id}}.{{silver_dataset}}.cf_clean`
  WHERE candidate_name_normalized IS NOT NULL AND candidate_name_normalized != ''
),
committees_by_candidate AS (
  SELECT
    candidate_name_normalized,
    ARRAY_AGG(DISTINCT committee_code IGNORE NULLS) AS committee_codes,
    MIN(report_year) AS first_election_cycle_year,
    MAX(report_year) AS last_election_cycle_year
  FROM `{{project_id}}.{{silver_dataset}}.cf_clean`
  WHERE candidate_name_normalized IS NOT NULL AND candidate_name_normalized != ''
  GROUP BY candidate_name_normalized
)
SELECT
  r.candidate_name_normalized,
  c.committee_codes,
  r.office_sought_normal,
  r.district_normal,
  r.level,
  r.party,
  r.election_cycle AS most_recent_election_cycle,
  c.first_election_cycle_year,
  c.last_election_cycle_year
FROM candidate_reports r
JOIN committees_by_candidate c ON c.candidate_name_normalized = r.candidate_name_normalized
WHERE r.rn = 1;
