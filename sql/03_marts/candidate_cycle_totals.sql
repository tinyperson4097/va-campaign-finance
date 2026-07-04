-- candidate_cycle_totals: total receipts and disbursements per candidate per
-- election cycle, from fact_transaction (already latest-amendment only).
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.candidate_cycle_totals` AS
SELECT
  candidate_name_normalized,
  election_cycle,
  report_year,
  office_sought_normal,
  district_normal,
  level,
  party,
  SUM(IF(transaction_category = 'receipt', amount, 0)) AS total_raised,
  SUM(IF(transaction_category = 'disbursement', amount, 0)) AS total_spent,
  COUNTIF(transaction_category = 'receipt') AS receipt_count,
  COUNTIF(transaction_category = 'disbursement') AS disbursement_count
FROM `{{project_id}}.{{gold_dataset}}.fact_transaction`
WHERE candidate_name_normalized IS NOT NULL AND candidate_name_normalized != ''
  AND NOT is_suspected_test_record
GROUP BY candidate_name_normalized, election_cycle, report_year, office_sought_normal,
         district_normal, level, party;
