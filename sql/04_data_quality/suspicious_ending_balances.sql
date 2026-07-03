-- suspicious_ending_balances: flags Schedule H reports whose numbers don't
-- add up internally. Two independent checks (a report can trip either or
-- both):
--   1. negative_ending_balance -- ending_balance < 0
--   2. line_19_mismatch -- EndingBalance and ExpendableFundsBalance (line 19
--      of the form) disagree by more than a cent; these are two different
--      reported figures for what should be the same cash position.
-- This is a new check with no prior script to port -- there was no existing
-- "suspicious ending balance" definition in this codebase. Criteria are
-- intentionally conservative and worth tuning once run against real data.
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.suspicious_ending_balances` AS
SELECT
  report_id,
  committee_code,
  committee_name,
  candidate_name,
  candidate_name_normalized,
  report_year,
  report_date,
  due_date,
  ending_balance,
  line_19,
  starting_balance,
  total_disbursements,
  ABS(COALESCE(ending_balance, 0) - COALESCE(line_19, ending_balance)) AS line_19_discrepancy,
  ending_balance < 0 AS negative_ending_balance,
  ABS(COALESCE(ending_balance, 0) - COALESCE(line_19, ending_balance)) > 0.01 AS line_19_mismatch,
  data_source,
  folder_name
FROM `{{project_id}}.{{gold_dataset}}.fact_report_summary`
WHERE ending_balance < 0
   OR ABS(COALESCE(ending_balance, 0) - COALESCE(line_19, ending_balance)) > 0.01
ORDER BY negative_ending_balance DESC, line_19_discrepancy DESC;
