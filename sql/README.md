# Gold layer

Run all transforms in order:

    python sql/run_gold.py --project-id <PROJECT> [--dry-run]

Files are grouped by numbered directory (`00_setup`, `01_dims`, `02_facts`,
`03_marts`, `04_data_quality`) and run in that order, since later files read
tables created by earlier ones. Placeholders `{{project_id}}`,
`{{silver_dataset}}` (default `virginia_elections`), and `{{gold_dataset}}`
(default `virginia_elections_gold`) are substituted before each query runs.

Use `--only <substring>` to rerun a single table while iterating, and
`--dry-run` to print the rendered SQL without hitting BigQuery.
