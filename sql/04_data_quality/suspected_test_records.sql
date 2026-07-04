-- suspected_test_records: every fact_transaction row flagged as a likely
-- SBE test/QA record (the flag criteria live in fact_transaction.sql:
-- word-bounded TESTER/TESTY in an entity/candidate/committee normalized name,
-- or an entity address/city that is literally 'test'). The SBE demonstrably
-- ships such rows in its production CSV feed -- e.g. "Tammy Tester" at
-- "123 Find Street" purchasing "Testing CAB-1879", and "Mr Tester Mc Tester"
-- filed as a Governor candidate. Published as its own table so exclusions
-- from the marts are auditable rather than silent; review it for false
-- positives (a real person surnamed Tester would land here).
CREATE OR REPLACE TABLE `{{project_id}}.{{gold_dataset}}.suspected_test_records` AS
SELECT *
FROM `{{project_id}}.{{gold_dataset}}.fact_transaction`
WHERE is_suspected_test_record;
