-- fact_report_summary: one row per (committee_code, due_date_group), the
-- latest amendment kept per Schedule H "Summary of Disbursements" report.
-- Same due-date-rounding + ROW_NUMBER dedup pattern as
-- python_analysis_scripts/scheduleh_balance_continuity_check.py, applied here
-- to the raw schedule_h table so gold doesn't depend on the pandas-built
-- schedule_h_clean. (Schedule G/summary-of-receipts data isn't ingested
-- anywhere in the current pipeline -- both processors skip ScheduleG -- so
-- this table only covers the Schedule H side named in PLAN.md.)
--
-- Depends on `{{gold_dataset}}.parse_va_date` (sql/00_setup/udf_parse_date.sql).
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.fact_report_summary` AS
WITH parsed AS (
  SELECT
    *,
    `{{project_id}}.{{gold_dataset}}.parse_va_date`(due_date) AS due_date_parsed
  FROM `{{project_id}}.{{silver_dataset}}.schedule_h`
  WHERE committee_code IS NOT NULL AND committee_code != '' AND due_date IS NOT NULL
),
due_date_groups AS (
  SELECT
    *,
    DATE_SUB(
      due_date_parsed,
      INTERVAL MOD(DATE_DIFF(due_date_parsed, DATE('1900-01-01'), DAY), 7) DAY
    ) AS due_date_group
  FROM parsed
  WHERE due_date_parsed IS NOT NULL
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY committee_code, due_date_group
      ORDER BY COALESCE(amendment_count, 0) DESC, report_date DESC, folder_name DESC
    ) AS amendment_rank
  FROM due_date_groups
)
SELECT
  report_id,
  committee_code,
  committee_name,
  committee_name_normalized,
  candidate_name,
  candidate_name_normalized,
  report_year,
  report_date,
  due_date,
  COALESCE(amendment_count, 0) AS amendment_count,
  party,
  office_sought,
  office_sought_normal,
  district,
  district_normal,
  level,
  candidate_city,
  election_cycle,
  primary_or_general,
  total_disbursements,
  starting_balance,
  ending_balance,
  line_19,
  data_source,
  folder_name
FROM ranked
WHERE amendment_rank = 1;
