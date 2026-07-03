-- dim_report: one row per report_id, deduped from the transaction-level report
-- metadata already carried on cf_clean (committee/candidate/filing dates).
-- is_latest_amendment flags whether this is the most-amended report for its
-- (committee_code, due_date) -- the same grouping the balance-continuity check
-- uses -- so downstream queries can filter to it without repeating the window
-- function.
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.dim_report` AS
WITH report_rows AS (
  SELECT DISTINCT
    report_id,
    committee_code,
    committee_name,
    candidate_name,
    report_year,
    report_date,
    due_date,
    amendment_count,
    election_cycle,
    primary_or_general,
    data_source,
    folder_name
  FROM `{{project_id}}.{{silver_dataset}}.cf_clean`
  WHERE report_id IS NOT NULL
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY committee_code, due_date
      ORDER BY COALESCE(amendment_count, 0) DESC, report_date DESC
    ) AS amendment_rank
  FROM report_rows
)
SELECT
  report_id,
  committee_code,
  committee_name,
  candidate_name,
  report_year,
  report_date,
  due_date,
  COALESCE(amendment_count, 0) AS amendment_count,
  amendment_count > 0 AS is_amendment,
  amendment_rank = 1 AS is_latest_amendment,
  election_cycle,
  primary_or_general,
  data_source,
  folder_name
FROM ranked;
