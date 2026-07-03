-- fact_transaction: schedules A/B/C/D/F/I unified, latest-amendment rows only.
-- Sources from the raw campaign_finance table (all amendments) rather than
-- cf_clean, because the dedup below replaces the fuzzywuzzy/pandas dedup in
-- processors/amendment_processor.py with the deterministic SQL window-function
-- version that script already drafted but left disabled (see
-- create_amendment_cleaned_table's `efficient_sql`) -- ported here verbatim per
-- PLAN.md Phase 3/guiding decision 4.
--
-- transaction_category follows VA SBE's schedule pairing: A/B/C are receipts
-- (itemized / in-kind / other), D/F/I are disbursements (itemized / debts &
-- obligations / in-kind).
--
-- Depends on `{{gold_dataset}}.parse_va_date` (sql/00_setup/udf_parse_date.sql).
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.fact_transaction` AS
WITH cleaned AS (
  SELECT
    *,
    `{{project_id}}.{{gold_dataset}}.parse_va_date`(transaction_date) AS parsed_transaction_date,
    TRIM(UPPER(COALESCE(entity_name_normalized, 'UNKNOWN'))) AS name_normalized_for_dedup,
    COALESCE(amendment_count, 0) AS amendment_count_clean,
    CAST(ROUND(COALESCE(amount, 0) * 100, 0) AS INT64) AS amount_cents
  FROM `{{project_id}}.{{silver_dataset}}.campaign_finance`
  WHERE committee_code IS NOT NULL AND committee_code != ''
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        committee_code,
        name_normalized_for_dedup,
        amount_cents,
        DATE_TRUNC(parsed_transaction_date, MONTH),
        COALESCE(zip_code, ''),
        COALESCE(committee_type, ''),
        COALESCE(transaction_type, ''),
        COALESCE(CAST(entity_is_individual AS STRING), ''),
        COALESCE(entity_zip, ''),
        COALESCE(schedule_type, ''),
        COALESCE(primary_or_general, ''),
        COALESCE(office_sought_normal, ''),
        COALESCE(district_normal, '')
      ORDER BY amendment_count_clean DESC, due_date DESC, report_date DESC
    ) AS amendment_rank
  FROM cleaned
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
  party,
  office_sought,
  office_sought_normal,
  district,
  district_normal,
  level,
  candidate_city,
  election_cycle,
  primary_or_general,
  schedule_type,
  transaction_type,
  CASE
    WHEN schedule_type IN ('ScheduleA', 'ScheduleB', 'ScheduleC') THEN 'receipt'
    WHEN schedule_type IN ('ScheduleD', 'ScheduleF', 'ScheduleI') THEN 'disbursement'
    ELSE 'unknown'
  END AS transaction_category,
  transaction_date,
  amount,
  total_to_date,
  entity_name,
  entity_name_normalized,
  entity_first_name,
  entity_last_name,
  entity_address,
  entity_city,
  entity_state,
  entity_zip,
  entity_employer,
  entity_occupation,
  entity_is_individual,
  purpose,
  committee_type,
  zip_code,
  submitted_date,
  due_date,
  amendment_count,
  data_source,
  folder_name,
  onTime
FROM ranked
WHERE amendment_rank = 1;
