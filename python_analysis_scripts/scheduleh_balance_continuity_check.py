#!/usr/bin/env python3
"""
Schedule H Balance Continuity Checker
Identifies discrepancies where a committee's report starting balance 
doesn't match the previous report's ending balance
"""

import pandas as pd
import argparse
import logging
from google.cloud import bigquery
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_balance_continuity_issues(project_id: str, 
                                 dataset_id: str, 
                                 table_id: str,
                                 min_year: int = 2018) -> List[Dict[str, Any]]:
    """
    Find balance continuity issues using efficient window functions.
    
    Parameters:
        project_id (str): Google Cloud Project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID
        min_year (int): Minimum year to include (default: 2018)
        
    Returns:
        List[Dict]: List of balance continuity issues
    """
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    try:
        # Efficient query using window functions with amendment handling
        query = f"""
        WITH due_date_groups AS (
            -- Step 1: Create groups for due dates within 4 days of each other
            -- We'll use a simple approach: round due dates to the nearest Monday
            SELECT 
                *,
                -- Group due dates by rounding to nearest Monday (within 4 days)
                DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', due_date)) AS due_date_parsed,
                DATE_SUB(
                    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', due_date)),
                    INTERVAL MOD(
                        DATE_DIFF(DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', due_date)), DATE('1900-01-01'), DAY),
                        7
                    ) DAY
                ) AS due_date_group
                FROM `{project_id}.{dataset_id}.{table_id}`
            WHERE 1=1
                AND committee_code IS NOT NULL 
                AND committee_code != ''
                AND report_year >= @min_year
                AND starting_balance IS NOT NULL
                AND ending_balance IS NOT NULL
                AND due_date IS NOT NULL
        ),
        deduplicated_reports AS (
            -- Step 2: Handle amendments by keeping only the highest amendment for each due date group
            SELECT 
                committee_code,
                committee_name,
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
                due_date,
                due_date_group,
                COALESCE(amendment_count, 0) as amendment_count,
                starting_balance,
                ending_balance,
                line_19,
                total_disbursements,
                report_year,
                data_source,
                folder_name,
                
                -- Rank amendments within each due date group for each committee
                ROW_NUMBER() OVER (
                    PARTITION BY committee_code, due_date_group
                    ORDER BY COALESCE(amendment_count, 0) DESC, report_date DESC, folder_name DESC
                ) as amendment_rank
                
            FROM due_date_groups
        ),
        final_reports AS (
            -- Step 3: Keep only the highest amendment for each due date group
            SELECT * FROM deduplicated_reports WHERE amendment_rank = 1
        ),
        consecutive_reports AS (
            -- Step 4: Compare consecutive reports after deduplication
            SELECT 
                *,
                -- Get previous report's data using LAG window function
                LAG(report_date) OVER (
                    PARTITION BY committee_code 
                    ORDER BY due_date_group, due_date, report_date, report_year, folder_name
                ) as previous_report_date,
                
                LAG(ending_balance) OVER (
                    PARTITION BY committee_code 
                    ORDER BY due_date_group, due_date, report_date, report_year, folder_name
                ) as previous_ending_balance,

                LAG(line_19) OVER (
                    PARTITION BY committee_code 
                    ORDER BY due_date_group, due_date, report_date, report_year, folder_name
                ) as previous_line_19,
                
                LAG(due_date) OVER (
                    PARTITION BY committee_code 
                    ORDER BY due_date_group, due_date, report_date, report_year, folder_name
                ) as previous_due_date,
                
                -- Row number to identify if this is the first report for committee
                ROW_NUMBER() OVER (
                    PARTITION BY committee_code 
                    ORDER BY due_date_group, due_date, report_date, report_year, folder_name
                ) as report_sequence
                
            FROM final_reports
        )
        SELECT 
            committee_code,
            committee_name,
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
            due_date as current_due_date,
            previous_due_date,
            report_date as current_report_date,
            previous_report_date,
            amendment_count as current_amendment_count,
            starting_balance as current_starting_balance,
            previous_line_19,
            previous_ending_balance,
            ending_balance as current_ending_balance,
            line_19,
            total_disbursements,
            report_year,
            data_source,
            folder_name,
            
            -- Calculate the discrepancy amount
            (starting_balance - previous_ending_balance) as balance_discrepancy
            
        FROM consecutive_reports
        WHERE 1=1
            -- Skip first report for each committee (no previous to compare)
            AND report_sequence > 1
            -- Only include where balances don't match (allowing for small rounding differences)
            AND ABS(starting_balance - previous_ending_balance) > 0.01
        ORDER BY 
            ABS(starting_balance - previous_ending_balance) DESC,
            committee_code,
            due_date
        """
        
        # Set up query parameters
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("min_year", "INT64", min_year),
        ])
        
        # Execute the query
        logger.info(f"Executing BigQuery query for balance continuity issues...")
        logger.info(f"Filtering for reports from {min_year} onwards")
        
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        
        if df.empty:
            logger.info("No balance continuity issues found - all reports have consistent balances!")
            return []
        
        logger.info(f"Found {len(df)} balance continuity issues")
        
        # Calculate summary statistics
        total_discrepancy = df['balance_discrepancy'].abs().sum()
        avg_discrepancy = df['balance_discrepancy'].abs().mean()
        max_discrepancy = df['balance_discrepancy'].abs().max()
        
        logger.info(f"Total absolute discrepancy: ${total_discrepancy:,.2f}")
        logger.info(f"Average absolute discrepancy: ${avg_discrepancy:,.2f}")
        logger.info(f"Maximum absolute discrepancy: ${max_discrepancy:,.2f}")
        
        # Convert to list of dictionaries
        results = df.to_dict('records')
        
        return results
        
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        return []


def print_continuity_summary(results: List[Dict[str, Any]]):
    """Print summary statistics about balance continuity issues."""
    if not results:
        print("✅ No balance continuity issues found!")
        return
    
    df = pd.DataFrame(results)
    
    print(f"\n❌ Balance Continuity Issues Summary")
    print(f"{'=' * 70}")
    print(f"Total issues found: {len(df)}")
    print(f"Unique committees with issues: {df['committee_code'].nunique()}")
    print(f"Total absolute discrepancy: ${df['balance_discrepancy'].abs().sum():,.2f}")
    print(f"Average absolute discrepancy: ${df['balance_discrepancy'].abs().mean():,.2f}")
    print(f"Median absolute discrepancy: ${df['balance_discrepancy'].abs().median():,.2f}")
    print(f"Maximum absolute discrepancy: ${df['balance_discrepancy'].abs().max():,.2f}")
    
    # Show distribution of issue types
    print(f"\nDiscrepancy Distribution:")
    positive_discrepancies = (df['balance_discrepancy'] > 0).sum()
    negative_discrepancies = (df['balance_discrepancy'] < 0).sum()
    print(f"  Positive discrepancies (starting > previous ending): {positive_discrepancies}")
    print(f"  Negative discrepancies (starting < previous ending): {negative_discrepancies}")
    
    # Top 10 largest discrepancies
    print(f"\n🔍 Top 10 Largest Discrepancies:")
    print(f"{'Committee Code':<15} {'Candidate':<25} {'Discrepancy':<15} {'Report Date'}")
    print(f"{'-' * 80}")
    
    top_issues = df.nlargest(10, 'balance_discrepancy', keep='all')
    for _, issue in top_issues.iterrows():
        discrepancy_str = f"${issue['balance_discrepancy']:,.2f}"
        print(f"{issue['committee_code']:<15} "
              f"{issue['candidate_name'][:24]:<25} "
              f"{discrepancy_str:<15} "
              f"{issue['current_report_date']}")


def main():
    parser = argparse.ArgumentParser(description='Check Schedule H balance continuity between consecutive reports')
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
        # Get balance continuity issues
        results = get_balance_continuity_issues(
            project_id=args.project_id,
            dataset_id=args.dataset,
            table_id=args.table,
            min_year=args.min_year
        )
        
        # Convert to DataFrame
        df = pd.DataFrame(results) if results else pd.DataFrame()
        
        # Save to CSV
        df.to_csv(args.output_csv, index=False)
        
        if results:
            logger.info(f"Successfully saved {len(results)} balance continuity issues to {args.output_csv}")
        else:
            logger.info(f"No issues found - empty CSV saved to {args.output_csv}")
        
        # Show summary if requested
        if args.show_summary:
            print_continuity_summary(results)
        
        return 0
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())