-- Shared date parser for the raw string date columns coming out of the SBE
-- CSVs. Observed formats in silver: 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS',
-- 'YYYY-MM-DD HH:MM:SS.123456789' (report_date carries nanosecond text),
-- 'MM/DD/YYYY', and 'MM/DD/YY'. Mirrors the format handling drafted in
-- processors/amendment_processor.py's disabled SQL path, hardened two ways:
-- SAFE.PARSE_* everywhere (an unexpected format yields NULL instead of
-- failing the whole downstream query), and timestamps are truncated to their
-- first 19 chars so fractional seconds can't break the %H:%M:%S parse.
CREATE OR REPLACE FUNCTION `{{project_id}}.{{gold_dataset}}.parse_va_date`(raw_date STRING) AS (
  CASE
    WHEN raw_date IS NULL OR raw_date = '' THEN NULL
    WHEN REGEXP_CONTAINS(raw_date, r'^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}')
      THEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', REPLACE(SUBSTR(raw_date, 1, 19), 'T', ' ')))
    WHEN REGEXP_CONTAINS(raw_date, r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
      THEN SAFE.PARSE_DATE('%Y-%m-%d', raw_date)
    WHEN REGEXP_CONTAINS(raw_date, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')
      THEN SAFE.PARSE_DATE('%m/%d/%Y', SPLIT(raw_date, ' ')[OFFSET(0)])
    WHEN REGEXP_CONTAINS(raw_date, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$')
      THEN SAFE.PARSE_DATE('%m/%d/%y', raw_date)
    ELSE NULL
  END
);
