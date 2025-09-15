#!/usr/bin/env python3
"""
Amendment Processor for Virginia Campaign Finance Data
Processes raw campaign finance data to create amendment-cleaned tables
by keeping only the latest amendment for each (committee_code, due_date) combination.
"""

import pandas as pd
import argparse
import logging
from google.cloud import bigquery
from typing import Optional
import sys
from pathlib import Path
import importlib.util
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_transaction_date(date_str) -> datetime:
    """Parse transaction date string into datetime object."""
    if pd.isna(date_str) or date_str == '' or date_str is None:
        return None
    
    date_str = str(date_str).strip()
    
    # Try different date formats
    formats = [
        "%m/%d/%Y",
        "%Y-%m-%d", 
        "%m/%d/%y",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

def dates_within_month(date1, date2) -> bool:
    """Check if two dates are within 30 days of each other."""
    if date1 is None or date2 is None:
        return False
    
    return abs((date1 - date2).days) <= 30

def fuzzy_name_match(name1, name2, threshold=85) -> bool:
    """Check if two names match within fuzzy threshold."""
    if pd.isna(name1) or pd.isna(name2) or name1 == '' or name2 == '':
        return name1 == name2  # Both empty or one empty
    
    name1_clean = str(name1).strip().upper()
    name2_clean = str(name2).strip().upper()
    
    if name1_clean == name2_clean:
        return True
    
    # Use fuzzywuzzy ratio for similarity
    similarity = fuzz.ratio(name1_clean, name2_clean)
    return similarity >= threshold

def get_latest_amendments(df: pd.DataFrame, fuzzy_threshold: int = 85) -> pd.DataFrame:
    """
    Return only transactions from the latest amendment reports using efficient vectorized operations.
    Groups transactions by (committee_code, entity_name_normalized, amount, transaction_date)
    using fuzzy name matching and date proximity, then keeps highest amendment_count.
    
    Args:
        df: DataFrame with required columns
        fuzzy_threshold: Similarity threshold for name matching (0-100)
        
    Returns:
        DataFrame with only transactions from latest amendment reports
    """
    if df.empty:
        return df
    
    # Handle missing values
    df_clean = df.copy().reset_index(drop=True)
    df_clean['committee_code'] = df_clean['committee_code'].fillna('UNKNOWN')
    df_clean['entity_name_normalized'] = df_clean['entity_name_normalized'].fillna('UNKNOWN')
    df_clean['amount'] = pd.to_numeric(df_clean['amount'], errors='coerce').fillna(0.0)
    df_clean['amendment_count'] = pd.to_numeric(df_clean['amendment_count'], errors='coerce').fillna(0)
    
    logger.info(f"Processing {len(df_clean)} total transactions")
    logger.info(f"Using fuzzy name matching threshold: {fuzzy_threshold}")
    
    # Parse transaction dates and create a normalized date for grouping (within 30 days)
    df_clean['parsed_date'] = df_clean['transaction_date'].apply(parse_transaction_date)
    
    # Create a date group by rounding to nearest month for efficient grouping
    df_clean['date_group'] = df_clean['parsed_date'].apply(
        lambda x: x.replace(day=1) if x is not None else None
    )
    
    # Round amounts to nearest cent to handle floating point precision issues
    df_clean['amount_rounded'] = df_clean['amount'].round(2)
    
    # Create fuzzy name groups using simple preprocessing
    df_clean['name_clean'] = df_clean['entity_name_normalized'].str.strip().str.upper()
    
    logger.info("Using efficient grouping strategy similar to Schedule H processor...")
    
    # Group by stricter criteria to match SQL version and prevent over-removal
    # This is similar to the efficient grouping in Schedule H continuity check
    grouped = df_clean.groupby([
        'committee_code', 
        'name_clean', 
        'amount_rounded', 
        'date_group',
        # Add stricter matching criteria
        df_clean['zip_code'].fillna(''),
        df_clean['committee_type'].fillna(''),
        df_clean['transaction_type'].fillna(''),
        df_clean['entity_is_individual'].fillna(''),
        df_clean['entity_zip'].fillna(''),
        df_clean['schedule_type'].fillna(''),
        df_clean['primary_or_general'].fillna(''),
        df_clean['office_sought_normal'].fillna(''),
        df_clean['district_normal'].fillna('')
    ], dropna=False)
    
    latest_transactions = []
    
    for group_key, group_df in grouped:
        if len(group_df) == 1:
            # Only one transaction in group, keep it
            latest_transactions.append(group_df.index[0])
        else:
            # Multiple transactions - need to check for fuzzy matches and date proximity
            group_df = group_df.copy()
            
            # Within each group, check for fuzzy matches with date proximity
            processed_indices = set()
            
            for i, row1 in group_df.iterrows():
                if i in processed_indices:
                    continue
                
                # Find similar transactions within this pre-filtered group
                similar_mask = pd.Series([True] * len(group_df), index=group_df.index)
                
                for j, row2 in group_df.iterrows():
                    if j == i or j in processed_indices:
                        continue
                    
                    # Check fuzzy name match and date proximity
                    name_match = fuzzy_name_match(
                        row1['entity_name_normalized'], 
                        row2['entity_name_normalized'], 
                        fuzzy_threshold
                    )
                    date_match = dates_within_month(row1['parsed_date'], row2['parsed_date'])
                    
                    if not (name_match and date_match):
                        similar_mask[j] = False
                
                # Get similar transactions
                similar_indices = group_df[similar_mask].index.tolist()
                
                if len(similar_indices) > 1:
                    # Multiple similar transactions - keep highest amendment_count
                    similar_df = group_df.loc[similar_indices]
                    max_amendment = similar_df['amendment_count'].max()
                    latest_indices = similar_df[
                        similar_df['amendment_count'] == max_amendment
                    ].index.tolist()
                    
                    latest_transactions.extend(latest_indices)
                    processed_indices.update(similar_indices)
                else:
                    # Single transaction, keep it
                    latest_transactions.append(i)
                    processed_indices.add(i)
    
    # Get final dataset
    latest_df = df_clean.loc[latest_transactions].copy()
    
    # Drop helper columns
    latest_df = latest_df.drop(['parsed_date', 'date_group', 'amount_rounded', 'name_clean'], axis=1)
    
    # Remove duplicates that might have been created
    latest_df = latest_df.drop_duplicates()
    
    # Calculate statistics
    original_transactions = len(df_clean)
    kept_transactions = len(latest_df)
    superseded_count = original_transactions - kept_transactions
    
    logger.info(f"Amendment processing summary:")
    logger.info(f"  Original transactions: {original_transactions:,}")
    logger.info(f"  Latest amendment transactions kept: {kept_transactions:,}")
    logger.info(f"  Superseded transactions removed: {superseded_count:,}")
    
    if superseded_count > 0:
        removal_rate = (superseded_count / original_transactions) * 100
        logger.info(f"  Removal rate: {removal_rate:.1f}%")
    
    # Show amendment count distribution
    amendment_dist = latest_df['amendment_count'].value_counts().sort_index()
    logger.info(f"Amendment count distribution in final dataset:")
    for amendment_count, count in amendment_dist.items():
        logger.info(f"  Amendment {int(amendment_count)}: {count:,} transactions")
    
    return latest_df.sort_values(['committee_code', 'entity_name_normalized', 'amount', 'amendment_count'])


def run_main_processor_and_clean(data_folder: str, project_id: str, dataset_id: str, 
                                raw_table_id: str, clean_table_id: str, 
                                processor_script: str = "processors/ScheduleABCDFI_processor.py") -> bool:
    """
    Run the main processor and then create amendment-cleaned table.
    
    Args:
        data_folder: Path to campaign finance data folder
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset name
        raw_table_id: Raw table name (output of main processor)
        clean_table_id: Clean table name (amendment-processed output)
        processor_script: Path to main processor script
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Step 1: Run main processor to create/update raw table
        logger.info(f"Step 1: Running main processor {processor_script}...")
        
        # Import and run the main processor
        spec = importlib.util.spec_from_file_location("main_processor", processor_script)
        main_processor = importlib.util.module_from_spec(spec)
        sys.modules["main_processor"] = main_processor
        spec.loader.exec_module(main_processor)
        
        # Call the main processing function (assuming it has a main() function)
        if hasattr(main_processor, 'main'):
            # Override sys.argv to pass arguments to main processor
            original_argv = sys.argv
            sys.argv = [
                processor_script,
                '--data-folder', data_folder,
                '--project-id', project_id,
                '--dataset', dataset_id,
                '--table', raw_table_id
            ]
            
            result = main_processor.main()
            sys.argv = original_argv
            
            if result != 0:
                logger.error("Main processor failed")
                return False
        else:
            logger.error(f"Main processor {processor_script} doesn't have a main() function")
            return False
        
        # Step 2: Process amendments and create clean table
        logger.info(f"Step 2: Processing amendments and creating clean table...")
        return create_amendment_cleaned_table(project_id, dataset_id, raw_table_id, clean_table_id)
        
    except Exception as e:
        logger.error(f"Error in run_main_processor_and_clean: {e}")
        return False


def create_amendment_cleaned_table(project_id: str, dataset_id: str, 
                                 raw_table_id: str, clean_table_id: str, fuzzy_threshold: int = 85) -> bool:
    """
    Create amendment-cleaned table from raw table using efficient SQL processing.
    
    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset name  
        raw_table_id: Source table with all amendments
        clean_table_id: Target table with latest amendments only
        fuzzy_threshold: Fuzzy matching threshold (currently unused in SQL version)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # SQL processing is disabled for now - pandas version works better
        use_sql_processing = False
        
        if use_sql_processing:
            logger.info(f"Processing amendments using efficient SQL approach similar to Schedule H...")
            
            # Use SQL window functions for efficient amendment processing
            # This approach is much faster than downloading all data to pandas
            efficient_sql = f"""
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{clean_table_id}` AS
            WITH cleaned_data AS (
                SELECT *,
                    -- Parse transaction date for grouping - handle multiple formats
                    CASE 
                        WHEN transaction_date IS NOT NULL AND transaction_date != '' THEN
                            CASE
                                -- Handle YYYY-MM-DD HH:MM:SS format
                                WHEN REGEXP_CONTAINS(transaction_date, r'^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}') 
                                    THEN DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', transaction_date))
                                -- Handle YYYY-MM-DD format  
                                WHEN REGEXP_CONTAINS(transaction_date, r'^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$')
                                    THEN DATE(PARSE_DATE('%Y-%m-%d', transaction_date))
                                -- Handle MM/DD/YYYY format
                                WHEN REGEXP_CONTAINS(transaction_date, r'^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}')
                                    THEN DATE(PARSE_TIMESTAMP('%m/%d/%Y', transaction_date))
                                -- Handle MM/DD/YY format  
                                WHEN REGEXP_CONTAINS(transaction_date, r'^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{2}}$')
                                    THEN DATE(PARSE_TIMESTAMP('%m/%d/%y', transaction_date))
                                ELSE NULL
                            END
                        ELSE NULL 
                    END as parsed_transaction_date,
                    
                    -- Normalize names for grouping
                    TRIM(UPPER(COALESCE(entity_name_normalized, 'UNKNOWN'))) as name_normalized,
                    
                    -- Handle missing amendment counts
                    COALESCE(amendment_count, 0) as amendment_count_clean,
                    
                    -- Convert amount to integer cents to avoid FLOAT64 partitioning issue
                    CAST(ROUND(COALESCE(amount, 0) * 100, 0) AS INT64) as amount_cents
                    
                FROM `{project_id}.{dataset_id}.{raw_table_id}`
                WHERE committee_code IS NOT NULL AND committee_code != ''
            ),
            transaction_groups AS (
                SELECT *,
                    -- Create transaction groups based on stricter criteria
                    -- Group transactions that are likely duplicates using additional fields
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            committee_code,
                            name_normalized,
                            amount_cents,
                            -- Group dates by month to handle slight date variations
                            DATE_TRUNC(parsed_transaction_date, MONTH),
                            -- Add stricter matching criteria
                            COALESCE(zip_code, ''),
                            COALESCE(committee_type, ''),
                            COALESCE(transaction_type, ''),
                            COALESCE(entity_is_individual, ''),
                            COALESCE(entity_zip, ''),
                            COALESCE(schedule_type, ''),
                            COALESCE(primary_or_general, ''),
                            COALESCE(office_sought_normal, ''),
                            COALESCE(district_normal, '')
                        ORDER BY 
                            amendment_count_clean DESC,
                            due_date DESC,
                            report_date DESC
                    ) as amendment_rank
                    
                FROM cleaned_data
            ),
            latest_amendments AS (
                -- Keep only the highest amendment for each transaction group
                SELECT * 
                FROM transaction_groups 
                WHERE amendment_rank = 1
            )
            SELECT 
                -- Remove helper columns and return original structure
                * EXCEPT (parsed_transaction_date, name_normalized, amendment_count_clean, amount_cents, amendment_rank)
            FROM latest_amendments
            ORDER BY committee_code, entity_name_normalized, amount, amendment_count
            """
            
            logger.info("Executing efficient SQL amendment processing query...")
            
            # Execute the query
            query_job = client.query(efficient_sql)
            query_job.result()  # Wait for completion
            
            # Get statistics
            stats_query = f"""
            SELECT 
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.{raw_table_id}`) as original_count,
                (SELECT COUNT(*) FROM `{project_id}.{dataset_id}.{clean_table_id}`) as clean_count
            """
            
            stats_df = client.query(stats_query).to_dataframe()
            original_count = stats_df['original_count'].iloc[0]
            clean_count = stats_df['clean_count'].iloc[0]
            removed_count = original_count - clean_count
            
            logger.info(f"Successfully created clean table {project_id}.{dataset_id}.{clean_table_id}")
            
            # Print summary statistics
            print(f"\n✅ Amendment Processing Complete!")
            print(f"Raw table: {raw_table_id} ({original_count:,} records)")
            print(f"Clean table: {clean_table_id} ({clean_count:,} records)")
            print(f"Records removed: {removed_count:,} superseded amendments")
            
            if removed_count > 0:
                removal_rate = (removed_count / original_count) * 100
                print(f"Removal rate: {removal_rate:.1f}%")
            
            return True
        else:
            logger.info("Using pandas processing (SQL disabled)...")
        
        # Use pandas processing approach
        
        # Read raw data from BigQuery
        query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{raw_table_id}`
        ORDER BY committee_code, due_date, amendment_count
        """
        
        df = client.query(query).to_dataframe()
        logger.info(f"Downloaded {len(df)} records from raw table for pandas processing")
        
        if df.empty:
            logger.warning("No data found in raw table")
            return True
        
        # Process amendments with optimized pandas function
        clean_df = get_latest_amendments(df, fuzzy_threshold)
        
        # Upload clean data to BigQuery
        logger.info(f"Uploading {len(clean_df)} records to {project_id}.{dataset_id}.{clean_table_id}...")
        
        # Configure job to overwrite the table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        
        table_ref = client.dataset(dataset_id).table(clean_table_id)
        job = client.load_table_from_dataframe(clean_df, table_ref, job_config=job_config)
        job.result()  # Wait for job to complete
        
        logger.info(f"Successfully created clean table with pandas processing")
        
        # Print summary statistics
        print(f"\n✅ Amendment Processing Complete!")
        print(f"Raw table: {raw_table_id} ({len(df):,} records)")
        print(f"Clean table: {clean_table_id} ({len(clean_df):,} records)")
        print(f"Records removed: {len(df) - len(clean_df):,} superseded amendments")
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating amendment-cleaned table: {e}")
        return False


def process_local_files_only(data_folder: str, output_csv: str) -> bool:
    """
    Process local CSV files and output amendment-cleaned CSV (no BigQuery).
    
    Args:
        data_folder: Path to campaign finance data folder
        output_csv: Path to output CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Import main processor to use its file processing functions
        spec = importlib.util.spec_from_file_location("main_processor", "processors/ScheduleABCDFI_processor.py")
        main_processor = importlib.util.module_from_spec(spec)
        sys.modules["main_processor"] = main_processor
        spec.loader.exec_module(main_processor)
        
        logger.info(f"Processing local files in {data_folder}...")
        
        # This would need to be adapted based on how your main processor structures its code
        # For now, let's assume we can get a DataFrame from the processor
        
        # You would need to extract the file processing logic from your main processor
        # and call it here to get a DataFrame instead of uploading to BigQuery
        
        logger.error("Local-only processing not yet implemented - requires refactoring main processor")
        return False
        
    except Exception as e:
        logger.error(f"Error in local file processing: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Process Virginia campaign finance data with amendment cleanup')
    
    # Mode selection
    parser.add_argument('--mode', choices=['full', 'clean-only', 'local'], default='full',
                       help='Processing mode: full (run processor + clean), clean-only (just clean existing raw table), local (CSV only)')
    
    # BigQuery parameters
    parser.add_argument('--project-id', type=str, required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--raw-table', type=str, default='campaign_finance',
                       help='Raw table name (default: campaign_finance)')
    parser.add_argument('--clean-table', type=str, default='campaign_finance_clean',
                       help='Clean table name (default: campaign_finance_clean)')
    
    # File processing parameters
    parser.add_argument('--data-folder', type=str,
                       help='Path to campaign finance data folder (required for full mode)')
    parser.add_argument('--processor-script', type=str, default='processors/ScheduleABCDFI_processor.py',
                       help='Path to main processor script (default: processors/ScheduleABCDFI_processor.py)')
    parser.add_argument('--output-csv', type=str,
                       help='Output CSV file path (for local mode)')
    parser.add_argument('--fuzzy-threshold', type=int, default=85,
                       help='Fuzzy name matching threshold (0-100, default: 85)')
    
    args = parser.parse_args()
    
    try:
        if args.mode == 'full':
            if not args.data_folder:
                logger.error("--data-folder is required for full mode")
                return 1
                
            success = run_main_processor_and_clean(
                args.data_folder,
                args.project_id,
                args.dataset,
                args.raw_table,
                args.clean_table,
                args.processor_script
            )
            
        elif args.mode == 'clean-only':
            success = create_amendment_cleaned_table(
                args.project_id,
                args.dataset,
                args.raw_table,
                args.clean_table,
                args.fuzzy_threshold
            )
            
        elif args.mode == 'local':
            if not args.data_folder or not args.output_csv:
                logger.error("--data-folder and --output-csv are required for local mode")
                return 1
                
            success = process_local_files_only(args.data_folder, args.output_csv)
        
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"Amendment processor failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())