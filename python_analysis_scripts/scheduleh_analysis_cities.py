#!/usr/bin/env python3
"""
Schedule H Analysis for Virginia Campaign Finance data
Analyzes total disbursements at the end of election cycles
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
import argparse
import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_bigquery_disbursements(project_id: str, 
                               dataset_id: str, 
                               table_id: str,
                               districts: List[str] = None, 
                               offices: List[str] = None) -> List[Dict[str, Any]]:
    """
    Analyze local election disbursements by candidate using BigQuery Schedule H data.
    
    Parameters:
        project_id (str): Google Cloud Project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        districts (List[str]): List of district_normal values to filter by
        offices (List[str]): List of office_sought_normal values to filter by  
        
    Returns:
        List[Dict]: List of candidate data with total disbursements
    """
    
    # Default values if not provided
    if districts is None:
        districts = ["blacksburg", "leesburg", "winchester", "alexandria", "arlington", "richmond", "lynchburg", "newport news", "virginia beach", "roanoke"]
    if offices is None:
        offices = ["city council", "mayor", "town council"]
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    try:
        # Build the query to get the latest Schedule H record per candidate per election cycle
        query = f"""
        WITH ranked_reports AS (
            SELECT 
                candidate_name,
                district_normal,
                office_sought_normal,
                election_cycle,
                total_disbursements,
                report_date,
                ROW_NUMBER() OVER (
                    PARTITION BY candidate_name, district_normal, office_sought_normal, election_cycle 
                    ORDER BY report_date DESC
                ) as rn
            FROM `{project_id}.{dataset_id}.{table_id}`
            WHERE 1=1
        """
        
        # Add filters
        params = {}
        
        query += " AND level = @level"
        params['level'] = 'local'
        
        # Use the LIKE operator for partial matches on districts
        if districts:
            query += " AND EXISTS(SELECT 1 FROM UNNEST(@districts) as search_term WHERE LOWER(district_normal) LIKE CONCAT('%', LOWER(search_term), '%'))"
            params['districts'] = districts
            
        # Filter by offices (case insensitive)
        if offices:
            query += " AND LOWER(office_sought_normal) IN UNNEST(@offices)"
            params['offices'] = [o.lower() for o in offices]
            
        # Filter by election cycles 2018 and later
        query += """
        AND (election_cycle IS NULL 
        OR election_cycle LIKE '%/2018%' 
        OR election_cycle LIKE '%/2019%' 
        OR election_cycle LIKE '%/202_%')
        AND candidate_name IS NOT NULL AND candidate_name != ''
        )
        SELECT 
            candidate_name,
            district_normal,
            office_sought_normal,
            election_cycle,
            total_disbursements,
            report_date
        FROM ranked_reports 
        WHERE rn = 1
        AND total_disbursements IS NOT NULL
        AND total_disbursements > 0
        ORDER BY total_disbursements DESC
        """
        
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("level", "STRING", params.get("level")),
            bigquery.ArrayQueryParameter("districts", "STRING", params.get("districts", [])),
            bigquery.ArrayQueryParameter("offices", "STRING", params.get("offices", [])),
        ])
        
        # Execute the query
        logger.info(f"Executing BigQuery analysis query...")
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        
        if df.empty:
            logger.warning("No results found with the specified filters")
            return []
            
        # Convert to list of dictionaries
        results = df.to_dict('records')
            
        return results
        
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        return []


def print_disbursement_results(results: List[Dict[str, Any]]):
    """Print the disbursement results in a formatted way."""
    if not results:
        print("No results to display")
        return
        
    print(f"\nLocal Election Disbursements Analysis (Schedule H)")
    print(f"{'=' * 110}")
    print(f"{'Candidate Name':<30} {'District':<20} {'Office':<15} {'Election Cycle':<15} {'Total Disbursements':<18} {'Report Date'}")
    print(f"{'-' * 110}")
    
    total_disbursements = 0
    for result in results:
        total_disbursements += result['total_disbursements']
        election_cycle_str = str(result['election_cycle']) if result['election_cycle'] else 'N/A'
        report_date_str = str(result['report_date']) if result['report_date'] else 'N/A'
        print(f"{result['candidate_name']:<30} "
              f"{result['district_normal']:<20} "
              f"{result['office_sought_normal']:<15} "
              f"{election_cycle_str:<15} "
              f"${result['total_disbursements']:>16,.2f} "
              f"{report_date_str}")
              
    print(f"{'-' * 110}")
    print(f"{'Total:':<92} ${total_disbursements:>16,.2f}")
    print(f"Total candidates: {len(results)}")


def main():
    parser = argparse.ArgumentParser(description='Virginia Campaign Finance Schedule H Analyzer')
    parser.add_argument('--project-id', type=str, required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--table', type=str, default='schedule_h_clean',
                       help='BigQuery table name (default: schedule_h_clean)')
    parser.add_argument('--output-csv', type=str,
                       help='Path to a CSV file to save the results')
    parser.add_argument('--cities', nargs='+', 
                       default=["blacksburg", "leesburg", "winchester", "alexandria", "arlington", "richmond", "lynchburg", "newport news", "virginia beach", "roanoke"],
                       help='Cities to analyze (default: blacksburg leesburg winchester alexandria arlington richmond lynchburg "newport news" "virginia beach" roanoke)')
    
    args = parser.parse_args()
    
    try:
        results = get_bigquery_disbursements(
            project_id=args.project_id,
            dataset_id=args.dataset,
            table_id=args.table,
            districts=args.cities,
            offices=["city council", "mayor", "town council"]
        )

        # Convert the results list back to a DataFrame for export
        df = pd.DataFrame(results)

        # Check if an output path was provided
        if args.output_csv:
            df.to_csv(args.output_csv, index=False)
            logger.info(f"Successfully exported results to {args.output_csv}")
        
        # Only print if there is no output path
        else:
            print_disbursement_results(results)

    except Exception as e:
        logger.error(f"Analysis failed: {e}")

if __name__ == "__main__":
    main()