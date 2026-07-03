-- Shared date parser for the raw string date columns coming out of the SBE
-- CSVs (TransactionDate, DueDate, etc. show up as MM/DD/YYYY, MM/DD/YY, or
-- YYYY-MM-DD[ HH:MM:SS]). Mirrors the format handling already drafted in
-- processors/amendment_processor.py's disabled SQL path. Returns NULL instead
-- of erroring on an unrecognized format, unlike PARSE_DATE/PARSE_TIMESTAMP
-- called directly.
CREATE OR REPLACE FUNCTION `{{project_id}}.{{gold_dataset}}.parse_va_date`(raw_date STRING) AS (
  CASE
    WHEN raw_date IS NULL OR raw_date = '' THEN NULL
    WHEN REGEXP_CONTAINS(raw_date, r'^[0-9]{4}-[0-9]{2}-[0-9]{2} ')
      THEN DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', raw_date))
    WHEN REGEXP_CONTAINS(raw_date, r'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
      THEN PARSE_DATE('%Y-%m-%d', raw_date)
    WHEN REGEXP_CONTAINS(raw_date, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}')
      THEN DATE(PARSE_TIMESTAMP('%m/%d/%Y', raw_date))
    WHEN REGEXP_CONTAINS(raw_date, r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$')
      THEN DATE(PARSE_TIMESTAMP('%m/%d/%y', raw_date))
    ELSE NULL
  END
);
