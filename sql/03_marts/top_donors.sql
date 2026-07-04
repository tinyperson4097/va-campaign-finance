-- top_donors: contribution totals by donor, per candidate and per committee.
-- Query this filtered/sorted (e.g. WHERE candidate_name_normalized = ...
-- ORDER BY total_amount DESC LIMIT n) rather than pre-limiting to a fixed N
-- here, since "top N" depends on what the caller is looking at.
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.top_donors` AS
-- Grouped by donor identity only: employer/occupation are descriptive
-- attributes that vary in spelling across filings, and grouping by them would
-- split one donor's true total across several rows (undercounting every
-- "top donor" ranking). ANY_VALUE keeps one representative spelling.
SELECT
  candidate_name_normalized,
  committee_code,
  committee_name_normalized,
  entity_name_normalized,
  LOGICAL_OR(COALESCE(entity_is_individual, FALSE)) AS entity_is_individual,
  ANY_VALUE(entity_employer) AS entity_employer,
  ANY_VALUE(entity_occupation) AS entity_occupation,
  SUM(amount) AS total_amount,
  COUNT(*) AS contribution_count,
  MIN(transaction_date) AS first_contribution_date,
  MAX(transaction_date) AS last_contribution_date
FROM `{{project_id}}.{{gold_dataset}}.fact_transaction`
WHERE transaction_category = 'receipt'
  AND entity_name_normalized IS NOT NULL AND entity_name_normalized != ''
  AND NOT is_suspected_test_record
GROUP BY candidate_name_normalized, committee_code, committee_name_normalized,
         entity_name_normalized;
