#!/usr/bin/env python3
"""
Run the gold-layer SQL transforms in order.

Executes every .sql file under sql/, sorted by directory prefix
(00_setup, 01_dims, 02_facts, 03_marts, 04_data_quality) then filename, against
BigQuery. Each file is a CREATE OR REPLACE TABLE/FUNCTION statement templated
with {{project_id}}, {{silver_dataset}}, and {{gold_dataset}} placeholders.
"""

import argparse
import logging
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SQL_ROOT = Path(__file__).parent


def render(sql_text: str, project_id: str, silver_dataset: str, gold_dataset: str) -> str:
    return (
        sql_text
        .replace("{{project_id}}", project_id)
        .replace("{{silver_dataset}}", silver_dataset)
        .replace("{{gold_dataset}}", gold_dataset)
    )


def discover_sql_files():
    return sorted(SQL_ROOT.glob("*/*.sql"))


def main():
    parser = argparse.ArgumentParser(description="Run gold-layer SQL transforms")
    parser.add_argument("--project-id", required=True, help="Google Cloud project ID")
    parser.add_argument("--silver-dataset", default="virginia_elections",
                        help="Source BigQuery dataset (default: virginia_elections)")
    parser.add_argument("--gold-dataset", default="virginia_elections_gold",
                        help="Destination BigQuery dataset (default: virginia_elections_gold)")
    parser.add_argument("--only", type=str, default=None,
                        help="Only run files whose name contains this substring")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print rendered SQL without executing")
    args = parser.parse_args()

    sql_files = discover_sql_files()
    if args.only:
        sql_files = [f for f in sql_files if args.only in f.name]

    if not sql_files:
        logger.warning("No matching SQL files found")
        return 1

    if args.dry_run:
        for sql_file in sql_files:
            rendered = render(sql_file.read_text(), args.project_id, args.silver_dataset, args.gold_dataset)
            print(f"\n-- {sql_file.relative_to(SQL_ROOT)} --\n{rendered}")
        return 0

    client = bigquery.Client(project=args.project_id)
    client.create_dataset(
        bigquery.Dataset(f"{args.project_id}.{args.gold_dataset}"),
        exists_ok=True,
    )

    for sql_file in sql_files:
        rendered = render(sql_file.read_text(), args.project_id, args.silver_dataset, args.gold_dataset)
        logger.info(f"Running {sql_file.relative_to(SQL_ROOT)}")
        job = client.query(rendered)
        job.result()
        logger.info("  done")

    logger.info(f"Ran {len(sql_files)} gold-layer transforms into {args.project_id}.{args.gold_dataset}")
    return 0


if __name__ == "__main__":
    exit(main())
