#!/usr/bin/env python3
"""
Test the updated normalization function on existing name_variations table
and upload results to BigQuery
"""

import sys
import os
import pandas as pd
import argparse
import re
from google.cloud import bigquery
import logging

# Add the functions directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'functions'))

from name_normalization import normalize_name

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_normalization_on_table(project_id: str, dataset: str = 'virginia_elections',
                               source_table: str = 'name_variations',
                               output_table: str = 'name_variations_test',
                               limit: int = None):
    """Test normalization on existing name_variations table and upload to BigQuery."""

    client = bigquery.Client(project=project_id)

    # Query the existing name_variations table
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
    SELECT DISTINCT
        name_variation,
        normalized_name as current_normalized
    FROM `{project_id}.{dataset}.{source_table}`
    {limit_clause}
    """

    logger.info(f"Querying from {project_id}.{dataset}.{source_table}...")
    df = client.query(query).to_dataframe()

    if df.empty:
        logger.error("No data found in name_variations table")
        return

    logger.info(f"Testing normalization on {len(df)} name variations...")

    # Apply new normalization with space and singular/plural matching
    results = []

    # First pass: collect all normalized names (both current and new)
    all_normalized_names = set()
    for _, row in df.iterrows():
        current_norm = row['current_normalized']
        new_norm = normalize_name(row['name_variation'], is_individual=False)
        all_normalized_names.add(current_norm)
        all_normalized_names.add(new_norm)

    def create_match_variations(name):
        """Create variations for matching: no spaces, number-letter spaces, singular/plural"""
        variations = set()

        # Original
        variations.add(name)

        # No spaces
        variations.add(name.replace(' ', ''))

        # Add spaces between numbers and letters (48hour -> 48 hour)
        spaced_version = re.sub(r'(\d)([A-Za-z])', r'\1 \2', name)
        variations.add(spaced_version)
        variations.add(spaced_version.replace(' ', ''))

        # Singular/plural variations
        if name.endswith('S') and len(name) > 1:
            singular = name[:-1]
            variations.add(singular)
            variations.add(singular.replace(' ', ''))
            # Also add spaced version of singular
            singular_spaced = re.sub(r'(\d)([A-Za-z])', r'\1 \2', singular)
            variations.add(singular_spaced)
        else:
            plural = name + 'S'
            variations.add(plural)
            variations.add(plural.replace(' ', ''))
            # Also add spaced version of plural
            plural_spaced = re.sub(r'(\d)([A-Za-z])', r'\1 \2', plural)
            variations.add(plural_spaced)

        return variations

    # Create lookup: variation -> best normalized name (prefer spaced, prefer singular)
    variation_to_best = {}
    for norm_name in all_normalized_names:
        variations = create_match_variations(norm_name)
        for variation in variations:
            if variation not in variation_to_best:
                variation_to_best[variation] = norm_name
            else:
                # Choose the better option: prefer spaces, prefer singular
                current_best = variation_to_best[variation]

                # Prefer spaced over non-spaced
                if ' ' in norm_name and ' ' not in current_best:
                    variation_to_best[variation] = norm_name
                elif ' ' not in norm_name and ' ' in current_best:
                    continue  # Keep current best
                # If both have same spacing, prefer singular over plural
                elif (not norm_name.endswith('S') and current_best.endswith('S')):
                    variation_to_best[variation] = norm_name
                elif (norm_name.endswith('S') and not current_best.endswith('S')):
                    continue  # Keep current best

    # Second pass: process each variation
    for _, row in df.iterrows():
        original_variation = row['name_variation']
        current_normalized = row['current_normalized']

        # Test new normalization (assume non-individual for entity names)
        base_normalized = normalize_name(original_variation, is_individual=False)

        # Check for better match using variations
        variations = create_match_variations(base_normalized)
        new_normalized = base_normalized

        for variation in variations:
            if variation in variation_to_best:
                candidate = variation_to_best[variation]
                # Prefer spaced over non-spaced
                if ' ' in candidate and ' ' not in new_normalized:
                    new_normalized = candidate
                # If current has no spaces and candidate has spaces, use candidate
                elif ' ' not in base_normalized and ' ' in candidate:
                    new_normalized = candidate

        changed = new_normalized != current_normalized

        results.append({
            'name_variation': original_variation,
            'current_normalized': current_normalized,
            'new_normalized': new_normalized,
            'changed': changed
        })

    results_df = pd.DataFrame(results)

    # Log summary
    changes = results_df['changed'].sum()
    logger.info(f"Summary: {changes} out of {len(results_df)} normalizations would change ({changes/len(results_df)*100:.1f}%)")

    # Upload to BigQuery
    output_table_id = f"{project_id}.{dataset}.{output_table}"
    logger.info(f"Uploading results to {output_table_id}...")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("name_variation", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("current_normalized", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("new_normalized", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("changed", "BOOLEAN", mode="REQUIRED"),
        ]
    )

    job = client.load_table_from_dataframe(
        results_df,
        output_table_id,
        job_config=job_config
    )
    job.result()  # Wait for completion

    logger.info(f"Successfully uploaded {len(results_df)} rows to {output_table_id}")

    # Show sample of changes
    changed_rows = results_df[results_df['changed'] == True].head(20)
    if not changed_rows.empty:
        print(f"\nSample of changes (first 20):")
        print("=" * 120)
        print(f"{'Original Variation':40} | {'Current Normalized':30} | {'New Normalized':30}")
        print("=" * 120)
        for _, row in changed_rows.iterrows():
            print(f"{row['name_variation'][:39]:40} | {row['current_normalized'][:29]:30} | {row['new_normalized'][:29]:30}")

    print(f"\n✅ Results uploaded to: {output_table_id}")
    print(f"📊 Total rows: {len(results_df)}")
    print(f"🔄 Changed: {changes} ({changes/len(results_df)*100:.1f}%)")

def main():
    parser = argparse.ArgumentParser(description='Test normalization on existing name_variations table')
    parser.add_argument('--project-id', type=str, required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--source-table', type=str, default='name_variations',
                       help='Source name variations table name (default: name_variations)')
    parser.add_argument('--output-table', type=str, default='name_variations_test',
                       help='Output table name (default: name_variations_test)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Number of rows to test (default: all rows)')

    args = parser.parse_args()

    try:
        test_normalization_on_table(args.project_id, args.dataset, args.source_table,
                                   args.output_table, args.limit)
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())