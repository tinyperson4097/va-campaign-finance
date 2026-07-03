-- local_election_costs: total/avg/max disbursements per local race (office x
-- district) per election cycle, from fact_report_summary (Schedule H). This
-- replaces scheduleh_analysis_cities.py + aggregate-local-financing.py's city
-- aggregation.
--
-- NOTE: the counties version of that legacy pipeline
-- (scheduleh_analysis_counties.py / aggregate-local-financing.py's
-- read_counties) grouped by a `mapped_county` column with no visible
-- derivation anywhere in this codebase -- it isn't produced by any processor
-- or analysis script here. Until that district->county mapping surfaces,
-- this mart only rolls up by district_normal (which already covers city/town
-- races); add a county-level mart once the mapping is found.
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.local_election_costs` AS
WITH latest_per_candidate_cycle AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY candidate_name_normalized, district_normal, office_sought_normal, election_cycle
      ORDER BY report_date DESC
    ) AS rn
  FROM `{{project_id}}.{{gold_dataset}}.fact_report_summary`
  WHERE level = 'local'
)
SELECT
  election_cycle,
  office_sought_normal,
  district_normal,
  MAX(total_disbursements) AS max_disbursements,
  AVG(total_disbursements) AS avg_disbursements,
  SUM(total_disbursements) AS total_disbursements,
  COUNT(DISTINCT candidate_name_normalized) AS num_candidates
FROM latest_per_candidate_cycle
WHERE rn = 1 AND total_disbursements IS NOT NULL AND total_disbursements > 0
GROUP BY election_cycle, office_sought_normal, district_normal;
