#!/usr/bin/env python3
"""
Schedule H Latest Balances Extractor
Pulls each candidate's most recent Schedule H report from BigQuery
Returns CSV with ending balance, report date, and election cycle
"""

import pandas as pd
import argparse
import logging
from google.cloud import bigquery
from typing import List, Dict, Any
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_latest_schedule_h_balances(project_id: str, 
                                  dataset_id: str, 
                                  table_id: str,
                                  min_year: int = 2018) -> List[Dict[str, Any]]:
    """
    Get the most recent Schedule H report for each candidate from BigQuery.
    
    Parameters:
        project_id (str): Google Cloud Project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID
        min_year (int): Minimum year to include (default: 2018)
        
    Returns:
        List[Dict]: List of candidate data with most recent report info
    """
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    try:
        # Build query to get the latest report per candidate (one record per candidate only)
        query = f"""
        WITH ranked_reports AS (
            SELECT 
                candidate_name,
                candidate_name_normalized,
                office_sought,
                office_sought_normal,
                district,
                district_normal,
                level,
                candidate_city,
                party,
                election_cycle,
                primary_or_general,
                report_date,
                total_disbursements,
                starting_balance,
                ending_balance,
                report_year,
                data_source,
                folder_name,
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        committee_code
                    ORDER BY 
                        CASE WHEN report_date IS NULL THEN 1 ELSE 0 END,
                        report_date DESC NULLS LAST,
                        report_year DESC,
                        folder_name DESC
                ) as rn
            FROM `{project_id}.{dataset_id}.{table_id}`
            WHERE 1=1
                AND committee_code IS NOT NULL 
                AND committee_code != ''
                AND report_year >= @min_year
        )
        SELECT 
            candidate_name,
            candidate_name_normalized,
            office_sought,
            office_sought_normal,
            district,
            district_normal,
            level,
            candidate_city,
            party,
            election_cycle,
            primary_or_general,
            report_date,
            total_disbursements,
            starting_balance,
            ending_balance,
            report_year,
            data_source,
            folder_name,
        FROM ranked_reports 
        WHERE rn = 1
        ORDER BY 
            report_date DESC NULLS LAST,
            candidate_name
        """
        
        # Set up query parameters
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("min_year", "INT64", min_year),
        ])
        
        # Execute the query
        logger.info(f"Executing BigQuery query for latest Schedule H balances...")
        logger.info(f"Filtering for reports from {min_year} onwards")
        
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        
        if df.empty:
            logger.warning("No Schedule H results found with the specified filters")
            return []
        
        logger.info(f"Found {len(df)} unique candidates with Schedule H reports")
        
        # Convert to list of dictionaries
        results = df.to_dict('records')
        
        # Add some summary statistics
        total_ending_balance = df['ending_balance'].sum()
        avg_ending_balance = df['ending_balance'].mean()
        median_ending_balance = df['ending_balance'].median()
        
        logger.info(f"Total ending balance across all candidates: ${total_ending_balance:,.2f}")
        logger.info(f"Average ending balance per candidate: ${avg_ending_balance:,.2f}")
        logger.info(f"Median ending balance per candidate: ${median_ending_balance:,.2f}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        return []


def print_summary_stats(results: List[Dict[str, Any]]):
    """Print summary statistics about the results."""
    if not results:
        print("No results to summarize")
        return
    
    df = pd.DataFrame(results)


    # --- Filter out reports from the last 3 months ---
    df['report_date'] = pd.to_datetime(df['report_date'])
    cutoff_date = datetime.today() - timedelta(days=90)
    df = df[df['report_date'] < cutoff_date]
    results = df.to_dict('records')
    
    print(f"\nSchedule H Latest Balances Summary")
    print(f"{'=' * 60}")
    print(f"Total candidates: {len(df)}")
    print(f"Date range: {df['report_date'].min()} to {df['report_date'].max()}")
    print(f"Total ending balance: ${df['ending_balance'].sum():,.2f}")
    print(f"Average ending balance: ${df['ending_balance'].mean():,.2f}")
    print(f"Median ending balance: ${df['ending_balance'].median():,.2f}")
    
    # Government level breakdown
    print(f"\nBy Government Level:")
    level_stats = df.groupby('level').agg({
        'ending_balance': ['count', 'sum', 'mean'],
        'candidate_name': 'count'
    }).round(2)
    print(level_stats)
    
    # Office breakdown (top 10)
    print(f"\nTop 10 Offices by Candidate Count:")
    office_counts = df['office_sought_normal'].value_counts().head(10)
    for office, count in office_counts.items():
        avg_balance = df[df['office_sought_normal'] == office]['ending_balance'].mean()
        print(f"  {office}: {count} candidates (avg balance: ${avg_balance:,.2f})")
    
    # Top 10 candidates by ending balance
    print(f"\nTop 10 Candidates by Ending Balance:")
    top_candidates = df.nlargest(10, 'ending_balance')
    for _, candidate in top_candidates.iterrows():
        print(f"  {candidate['candidate_name']:<30} ${candidate['ending_balance']:>12,.2f} ({candidate['office_sought_normal']})")


def main():
    parser = argparse.ArgumentParser(description='Extract latest Schedule H balances from BigQuery')
    parser.add_argument('--project-id', type=str, required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--table', type=str, default='schedule_h_clean',
                       help='BigQuery table name (default: schedule_h_clean)')
    parser.add_argument('--output-csv', type=str, required=True,
                       help='Path to output CSV file (required)')
    parser.add_argument('--min-year', type=int, default=2018,
                       help='Minimum year to include in results (default: 2018)')
    parser.add_argument('--show-summary', action='store_true',
                       help='Display summary statistics')
    
    args = parser.parse_args()
    
    try:
        # Get the latest balances
        results = get_latest_schedule_h_balances(
            project_id=args.project_id,
            dataset_id=args.dataset,
            table_id=args.table,
            min_year=args.min_year
        )
        
        if not results:
            logger.error("No data retrieved from BigQuery")
            return 1
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Save to CSV
        df.to_csv(args.output_csv, index=False)
        logger.info(f"Successfully saved {len(results)} candidate records to {args.output_csv}")
        
        # Show summary if requested
        if args.show_summary:
            print_summary_stats(results)
        
        return 0
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())