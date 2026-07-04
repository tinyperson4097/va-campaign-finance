-- donor_recipient_edges: money-flow graph edges between entities and
-- committees. Receipts (A/B/C) point entity -> committee; disbursements
-- (D/F/I) point committee -> entity (the committee's vendor/payee, including
-- pass-through contributions from one committee to another via Schedule D).
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.donor_recipient_edges` AS
SELECT
  IF(transaction_category = 'receipt', entity_name_normalized, committee_name_normalized) AS source_name,
  IF(transaction_category = 'receipt', committee_name_normalized, entity_name_normalized) AS target_name,
  transaction_category AS edge_type,
  election_cycle,
  report_year,
  SUM(amount) AS total_amount,
  COUNT(*) AS transaction_count
FROM `{{project_id}}.{{gold_dataset}}.fact_transaction`
WHERE entity_name_normalized IS NOT NULL AND entity_name_normalized != ''
  AND committee_name_normalized IS NOT NULL AND committee_name_normalized != ''
  AND NOT is_suspected_test_record
GROUP BY source_name, target_name, edge_type, election_cycle, report_year;
