#!/usr/bin/env python3
"""
Create mapping tables for Virginia campaign finance data to eliminate fuzzy matching.

Creates two tables:
1. committee_mappings: Maps committee codes to normalized committee names and candidate names
2. name_variations: Maps all name variations to their normalized versions
"""

import pandas as pd
import argparse
import logging
from google.cloud import bigquery
from typing import Dict, Set
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def clean_and_validate_name(name: str) -> str:
    """Clean and validate a name, returning empty string if invalid."""
    if pd.isna(name) or not name or str(name).strip() == '':
        return ''

    cleaned = str(name).strip()

    # Filter out obviously invalid names
    if len(cleaned) < 2:
        return ''
    if cleaned.lower() in ['n/a', 'na', 'none', 'null', 'unknown', '']:
        return ''
    if re.match(r'^[\d\s\-\.]+$', cleaned):  # Only numbers, spaces, dashes, dots
        return ''

    return cleaned


def create_committee_mappings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create committee mappings table: committee_code -> normalized_committee_name -> normalized_candidate_name

    Rules:
    - Each committee code maps to at most one normalized committee name
    - Each committee code maps to at most one normalized candidate name
    - A candidate may have multiple committee codes
    - Non-candidate committees will have empty candidate_name
    """
    logger.info("Creating committee mappings table...")

    # Get unique combinations of committee_code, committee_name_normalized, candidate_name_normalized
    mappings = []

    # Group by committee_code to ensure consistency
    for committee_code, group in df.groupby('committee_code'):
        if pd.isna(committee_code) or committee_code.strip() == '':
            continue

        # Get the most common normalized committee name for this code
        committee_names = group['committee_name_normalized'].dropna()
        committee_names = [clean_and_validate_name(name) for name in committee_names if clean_and_validate_name(name)]

        if not committee_names:
            logger.debug(f"No valid committee names for code {committee_code}")
            continue

        # Use most frequent committee name
        committee_name_counts = pd.Series(committee_names).value_counts()
        normalized_committee_name = committee_name_counts.index[0]

        # Get the most common normalized candidate name for this code
        candidate_names = group['candidate_name_normalized'].dropna()
        candidate_names = [clean_and_validate_name(name) for name in candidate_names if clean_and_validate_name(name)]

        if candidate_names:
            # Use most frequent candidate name
            candidate_name_counts = pd.Series(candidate_names).value_counts()
            normalized_candidate_name = candidate_name_counts.index[0]
        else:
            normalized_candidate_name = 'NOT A CC'  # Not a candidate committee

        mappings.append({
            'committee_code': committee_code.strip(),
            'committee_name_normalized': normalized_committee_name,
            'candidate_name_normalized': normalized_candidate_name
        })

        logger.debug(f"Mapped {committee_code} -> {normalized_committee_name} -> {normalized_candidate_name}")

    mappings_df = pd.DataFrame(mappings)
    logger.info(f"Created {len(mappings_df)} committee mappings")

    # Check for conflicts (same code mapping to different names)
    duplicates = mappings_df[mappings_df.duplicated(['committee_code'], keep=False)]
    if not duplicates.empty:
        logger.warning(f"Found {len(duplicates)} potential conflicts in committee mappings")
        for _, row in duplicates.iterrows():
            logger.warning(f"  Conflict: {row['committee_code']} -> {row['normalized_committee_name']}")

    return mappings_df


def create_name_variations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create name variations table: variation -> normalized_name

    Includes variations from entity_name, committee_name, and candidate_name fields
    """
    logger.info("Creating name variations table...")

    variations = {}  # variation -> normalized_name

    # Process entity_name -> entity_name_normalized
    logger.info("Processing entity name variations...")
    entity_pairs = df[['entity_name', 'entity_name_normalized']].dropna()

    for _, row in entity_pairs.iterrows():
        variation = clean_and_validate_name(row['entity_name'])
        normalized = clean_and_validate_name(row['entity_name_normalized'])

        if variation and normalized and variation != normalized:
            if variation in variations and variations[variation] != normalized:
                logger.debug(f"Conflict: '{variation}' maps to both '{variations[variation]}' and '{normalized}'")
                # Keep the most common mapping or the longer normalized name
                if len(normalized) > len(variations[variation]):
                    variations[variation] = normalized
            else:
                variations[variation] = normalized

    # Process committee_name -> committee_name_normalized
    logger.info("Processing committee name variations...")
    committee_pairs = df[['committee_name', 'committee_name_normalized']].dropna()

    for _, row in committee_pairs.iterrows():
        variation = clean_and_validate_name(row['committee_name'])
        normalized = clean_and_validate_name(row['committee_name_normalized'])

        if variation and normalized and variation != normalized:
            if variation in variations and variations[variation] != normalized:
                logger.debug(f"Conflict: '{variation}' maps to both '{variations[variation]}' and '{normalized}'")
                if len(normalized) > len(variations[variation]):
                    variations[variation] = normalized
            else:
                variations[variation] = normalized

    # Process candidate_name -> candidate_name_normalized (if exists)
    if 'candidate_name' in df.columns:
        logger.info("Processing candidate name variations...")
        candidate_pairs = df[['candidate_name', 'candidate_name_normalized']].dropna()

        for _, row in candidate_pairs.iterrows():
            variation = clean_and_validate_name(row['candidate_name'])
            normalized = clean_and_validate_name(row['candidate_name_normalized'])

            if variation and normalized and variation != normalized:
                if variation in variations and variations[variation] != normalized:
                    logger.debug(f"Conflict: '{variation}' maps to both '{variations[variation]}' and '{normalized}'")
                    if len(normalized) > len(variations[variation]):
                        variations[variation] = normalized
                else:
                    variations[variation] = normalized

    # Convert to DataFrame
    variations_list = [
        {'name_variation': variation, 'normalized_name': normalized}
        for variation, normalized in variations.items()
    ]

    variations_df = pd.DataFrame(variations_list)
    logger.info(f"Created {len(variations_df)} name variations")

    return variations_df


def main():
    parser = argparse.ArgumentParser(description='Create mapping tables for Virginia campaign finance data')
    parser.add_argument('--project-id', type=str, required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--source-table', type=str, default='cf_clean',
                       help='Source BigQuery table name (default: cf_clean)')
    parser.add_argument('--committee-mappings-table', type=str, default='committee_mappings',
                       help='Output table for committee mappings (default: committee_mappings)')
    parser.add_argument('--name-variations-table', type=str, default='name_variations',
                       help='Output table for name variations (default: name_variations)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be created without actually creating tables')

    args = parser.parse_args()

    # Initialize BigQuery client
    client = bigquery.Client(project=args.project_id)

    try:
        # Query the source data
        logger.info(f"Querying source table {args.project_id}.{args.dataset}.{args.source_table}...")

        query = f"""
        SELECT DISTINCT
            committee_code,
            committee_name,
            committee_name_normalized,
            candidate_name_normalized,
            entity_name,
            entity_name_normalized
        FROM `{args.project_id}.{args.dataset}.{args.source_table}`
        WHERE 1=1
            AND (
                committee_code IS NOT NULL
                OR committee_name_normalized IS NOT NULL
                OR entity_name_normalized IS NOT NULL
            )
        """

        df = client.query(query).to_dataframe()
        logger.info(f"Retrieved {len(df)} records from source table")

        # Create committee mappings table
        committee_mappings_df = create_committee_mappings(df)

        # Create name variations table
        name_variations_df = create_name_variations(df)

        if args.dry_run:
            logger.info("DRY RUN - Would create the following tables:")
            logger.info(f"Committee mappings: {len(committee_mappings_df)} rows")
            print(committee_mappings_df.head(10))
            logger.info(f"Name variations: {len(name_variations_df)} rows")
            print(name_variations_df.head(10))
            return 0

        # Create the tables in BigQuery
        dataset_ref = client.dataset(args.dataset)

        # Create committee mappings table
        committee_table_id = f"{args.project_id}.{args.dataset}.{args.committee_mappings_table}"
        logger.info(f"Creating committee mappings table: {committee_table_id}")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("committee_code", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("committee_name_normalized", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("candidate_name_normalized", "STRING", mode="NULLABLE"),
            ]
        )

        job = client.load_table_from_dataframe(
            committee_mappings_df,
            committee_table_id,
            job_config=job_config
        )
        job.result()  # Wait for completion

        logger.info(f"Successfully created committee mappings table with {len(committee_mappings_df)} rows")

        # Create name variations table
        variations_table_id = f"{args.project_id}.{args.dataset}.{args.name_variations_table}"
        logger.info(f"Creating name variations table: {variations_table_id}")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("name_variation", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("normalized_name", "STRING", mode="REQUIRED"),
            ]
        )

        job = client.load_table_from_dataframe(
            name_variations_df,
            variations_table_id,
            job_config=job_config
        )
        job.result()  # Wait for completion

        logger.info(f"Successfully created name variations table with {len(name_variations_df)} rows")

        # Print summary
        print(f"\n✅ Successfully created mapping tables:")
        print(f"  📋 Committee mappings: {committee_table_id} ({len(committee_mappings_df)} rows)")
        print(f"  📝 Name variations: {variations_table_id} ({len(name_variations_df)} rows)")

        return 0

    except Exception as e:
        logger.error(f"Script failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())