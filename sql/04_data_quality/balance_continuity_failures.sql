-- balance_continuity_failures: reports where starting_balance doesn't match
-- the previous report's ending_balance for the same committee. Ports
-- python_analysis_scripts/scheduleh_balance_continuity_check.py's
-- window-function query onto fact_report_summary, which is already
-- latest-amendment-only and pre-grouped by due_date -- so the due-date
-- rounding that script did for itself is unnecessary here; ordering directly
-- by due_date is safe because each due_date_group already collapsed to one
-- row upstream.
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.balance_continuity_failures` AS
WITH consecutive_reports AS (
  SELECT
    *,
    LAG(report_date) OVER (
      PARTITION BY committee_code ORDER BY due_date, report_date, report_year, folder_name
    ) AS previous_report_date,
    LAG(ending_balance) OVER (
      PARTITION BY committee_code ORDER BY due_date, report_date, report_year, folder_name
    ) AS previous_ending_balance,
    LAG(due_date) OVER (
      PARTITION BY committee_code ORDER BY due_date, report_date, report_year, folder_name
    ) AS previous_due_date,
    ROW_NUMBER() OVER (
      PARTITION BY committee_code ORDER BY due_date, report_date, report_year, folder_name
    ) AS report_sequence
  FROM `{{project_id}}.{{gold_dataset}}.fact_report_summary`
  WHERE starting_balance IS NOT NULL AND ending_balance IS NOT NULL AND due_date IS NOT NULL
)
SELECT
  committee_code,
  committee_name,
  candidate_name,
  candidate_name_normalized,
  office_sought,
  office_sought_normal,
  district,
  district_normal,
  level,
  candidate_city,
  party,
  election_cycle,
  primary_or_general,
  due_date AS current_due_date,
  previous_due_date,
  report_date AS current_report_date,
  previous_report_date,
  amendment_count AS current_amendment_count,
  starting_balance AS current_starting_balance,
  previous_ending_balance,
  ending_balance AS current_ending_balance,
  line_19,
  total_disbursements,
  report_year,
  data_source,
  folder_name,
  (starting_balance - previous_ending_balance) AS balance_discrepancy
FROM consecutive_reports
WHERE report_sequence > 1
  AND ABS(starting_balance - previous_ending_balance) > 0.01
ORDER BY ABS(starting_balance - previous_ending_balance) DESC, committee_code, due_date;
