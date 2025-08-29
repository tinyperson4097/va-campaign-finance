#!/usr/bin/env python3
"""
Tests and analysis functions for Virginia Campaign Finance data
"""



import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Set, Tuple
import argparse
import logging
from google.cloud import bigquery # Import BigQuery client
import hashlib


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_record_hash(record: Dict[str, Any], exclude_fields: Set[str] = None) -> str:
    """
    Generate a hash for a record, excluding specified fields.
    
    Parameters:
        record (Dict[str, Any]): The record to hash
        exclude_fields (Set[str]): Fields to exclude from the hash
        
    Returns:
        str: MD5 hash of the record
    """
    if exclude_fields is None:
        exclude_fields = set()
    
    # Create a sorted string representation excluding specified fields
    items = []
    for key, value in sorted(record.items()):
        if key not in exclude_fields:
            # Convert to string and handle None values
            value_str = str(value) if value is not None else "NULL"
            items.append(f"{key}:{value_str}")
    
    record_string = "|".join(items)
    return hashlib.md5(record_string.encode()).hexdigest()


def count_field_differences(record1: Dict[str, Any], record2: Dict[str, Any], 
                          exclude_fields: Set[str] = None) -> int:
    """
    Count the number of fields that differ between two records, excluding specified fields.
    
    Parameters:
        record1, record2 (Dict[str, Any]): Records to compare
        exclude_fields (Set[str]): Fields to exclude from comparison
        
    Returns:
        int: Number of differing fields
    """
    if exclude_fields is None:
        exclude_fields = set()
    
    differences = 0
    all_keys = set(record1.keys()) | set(record2.keys())
    
    for key in all_keys:
        if key in exclude_fields:
            continue
            
        val1 = record1.get(key)
        val2 = record2.get(key)
        
        if val1 != val2:
            differences += 1
    
    return differences


def deduplicate_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove exact duplicates and near duplicates from records.
    
    Records are considered potential duplicates only if they have:
    - Same transaction amount
    - Same transaction date  
    - Same candidate/committee name
    - Same recipient/donor entity
    
    Parameters:
        records (List[Dict[str, Any]]): List of records to deduplicate
        
    Returns:
        List[Dict[str, Any]]: Deduplicated records
    """
    if not records:
        return records
    
    # Step 1: Remove exact duplicates
    exact_duplicate_hashes = {}
    exact_deduplicated = []
    exact_duplicates_found = []
    
    for i, record in enumerate(records):
        record_hash = get_record_hash(record)
        if record_hash not in exact_duplicate_hashes:
            exact_duplicate_hashes[record_hash] = i
            exact_deduplicated.append(record)
        else:
            # This is an exact duplicate
            original_index = exact_duplicate_hashes[record_hash]
            exact_duplicates_found.append({
                'original_record': records[original_index],
                'duplicate_record': record,
                'original_index': original_index,
                'duplicate_index': i
            })
    
    logger.info(f"Removed {len(records) - len(exact_deduplicated)} exact duplicates")
    
    # Step 2: Group records by the key fields that must match for duplicates
    # Group by: amount, transaction_date, candidate_name, and recipient/donor entity
    duplicate_groups = {}
    
    for record in exact_deduplicated:
        # Create a key from the fields that must match
        amount = record.get('amount')
        transaction_date = record.get('transaction_date')
        candidate_name = record.get('candidate_name')
        
        # Try different possible field names for recipient/donor entity
        entity = (record.get('entity_name') or 
                 record.get('recipient_name') or 
                 record.get('donor_name') or 
                 record.get('payee_name') or
                 record.get('vendor_name'))
        
        # Create group key
        group_key = f"{amount}|{transaction_date}|{candidate_name}|{entity}"
        
        if group_key not in duplicate_groups:
            duplicate_groups[group_key] = []
        duplicate_groups[group_key].append(record)
    
    # Step 3: Within each group, check for near duplicates
    final_records = []
    near_duplicates_found = []
    near_duplicates_removed = 0
    
    # Fields that can differ between near duplicates
    flexible_fields = {'reportid', 'report_date', 'ontime', 'folder_name'}
    
    for group_key, group in duplicate_groups.items():
        if len(group) == 1:
            # No potential duplicates in this group
            final_records.append(group[0])
        else:
            # Check for near duplicates within the group
            print(f"\n=== POTENTIAL DUPLICATE GROUP ({len(group)} records) ===")
            print(f"Group key: {group_key}")
            for i, record in enumerate(group):
                print(f"  Record {i+1}: {record}")
            
            group_unique = []
            for record in group:
                is_near_duplicate = False
                matched_record = None
                for existing_record in group_unique:
                    # Count differences excluding flexible fields and the key fields
                    extended_flexible_fields = flexible_fields | {'amount', 'transaction_date', 'candidate_name', 'entity_name', 'recipient_name', 'donor_name', 'payee_name', 'vendor_name'}
                    diff_count = count_field_differences(record, existing_record, extended_flexible_fields)
                    if diff_count <= 2:
                        is_near_duplicate = True
                        matched_record = existing_record
                        break
                
                if not is_near_duplicate:
                    group_unique.append(record)
                else:
                    # This is a near duplicate
                    differing_fields = []
                    extended_flexible_fields = flexible_fields | {'amount', 'transaction_date', 'candidate_name', 'entity_name', 'recipient_name', 'donor_name', 'payee_name', 'vendor_name'}
                    for key in record.keys():
                        if key not in extended_flexible_fields and record.get(key) != matched_record.get(key):
                            differing_fields.append({
                                'field': key,
                                'original_value': matched_record.get(key),
                                'duplicate_value': record.get(key)
                            })
                    
                    near_duplicates_found.append({
                        'original_record': matched_record,
                        'duplicate_record': record,
                        'differing_fields': differing_fields,
                        'diff_count': len(differing_fields)
                    })
                    near_duplicates_removed += 1
            
            final_records.extend(group_unique)
            print(f"  Kept {len(group_unique)} unique records from this group")
    
    # Print near duplicates summary
    print(f"\n=== NEAR DUPLICATES FOUND ({len(near_duplicates_found)}) ===")
    for i, dup in enumerate(near_duplicates_found, 1):
        print(f"\nNear Duplicate #{i} ({dup['diff_count']} fields differ):")
        print(f"  Differing fields:")
        for field in dup['differing_fields']:
            print(f"    - {field['field']}: '{field['original_value']}' -> '{field['duplicate_value']}'")
    
    logger.info(f"Removed {near_duplicates_removed} near duplicates")
    logger.info(f"Final record count: {len(final_records)} (from original {len(records)})")
    
    return final_records


def get_elnoubi_records(project_id: str, dataset_id: str, table_id: str) -> List[Dict[str, Any]]:
    """
    Pull all records for candidate containing 'elnoubi' to analyze duplicates.
    
    Parameters:
        project_id (str): Google Cloud Project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        
    Returns:
        List[Dict]: List of Elnoubi records
    """
    client = bigquery.Client(project=project_id)
    
    try:
        query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE LOWER(candidate_name) LIKE '%elnoubi%'
        ORDER BY transaction_date, amount
        """
        
        logger.info(f"Executing query to fetch Elnoubi records...")
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        if df.empty:
            logger.warning("No Elnoubi records found")
            return []
            
        records = df.to_dict('records')
        logger.info(f"Found {len(records)} Elnoubi records")
        
        return records
        
    except Exception as e:
        logger.error(f"Error fetching Elnoubi records: {e}")
        return []


def get_bigquery_election_costs(project_id: str, 
                                dataset_id: str, 
                                table_id: str,
                                districts: List[str] = None, 
                                offices: List[str] = None,
                                schedule_filter: str = "ScheduleD") -> List[Dict[str, Any]]:
    """
    Analyze local election costs by candidate using BigQuery.
    
    Parameters:
        project_id (str): Google Cloud Project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        districts (List[str]): List of district_normal values to filter by
        offices (List[str]): List of office_sought_normal values to filter by  
        schedule_filter (str): Schedule type to filter by (default: ScheduleD)
        
    Returns:
        List[Dict]: List of candidate data sorted by aggregate amount descending
    """
    
    # Default values if not provided
    if districts is None:
        districts = ["blacksburg", "leesburg", "winchester", "alexandria"]
    if offices is None:
        offices = ["city council", "mayor", "town council"]
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    try:
        # Build the query to fetch raw data for deduplication
        query = f"""
        SELECT 
            *
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE 1=1
        """
        
        # Add filters
        params = {}
        
        if schedule_filter:
            query += " AND schedule_type = @schedule_filter"
            params['schedule_filter'] = schedule_filter
            
        query += " AND level = @level"
        params['level'] = 'local'
        
        # 🆕 MODIFIED: Use the LIKE operator for partial matches
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
        """
        
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("schedule_filter", "STRING", params.get("schedule_filter")),
            bigquery.ScalarQueryParameter("level", "STRING", params.get("level")),
            bigquery.ArrayQueryParameter("districts", "STRING", params.get("districts", [])),
            bigquery.ArrayQueryParameter("offices", "STRING", params.get("offices", [])),
        ])
        
        # Execute the query
        logger.info(f"Executing BigQuery query to fetch raw data...")
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        
        if df.empty:
            logger.warning("No results found with the specified filters")
            return []
            
        # Convert to list of dictionaries for deduplication
        raw_records = df.to_dict('records')
        logger.info(f"Fetched {len(raw_records)} raw records")
        
        # Apply deduplication
        deduplicated_records = deduplicate_records(raw_records)
        
        # Convert back to DataFrame for aggregation
        deduped_df = pd.DataFrame(deduplicated_records)
        
        # Perform aggregation after deduplication
        if 'amount' not in deduped_df.columns:
            logger.error("Amount column not found in data")
            return []
            
        # Group by candidate and election cycle, aggregate amounts
        aggregated = deduped_df.groupby([
            'candidate_name', 
            'district_normal', 
            'office_sought_normal', 
            'election_cycle'
        ]).agg({
            'amount': ['sum', 'count']
        }).reset_index()
        
        # Flatten column names
        aggregated.columns = ['candidate_name', 'district_normal', 'office_sought_normal', 'election_cycle', 'total_amount', 'transaction_count']
        
        # Sort by total amount descending
        aggregated = aggregated.sort_values('total_amount', ascending=False)
        
        # Convert to list of dictionaries
        results = aggregated.to_dict('records')
            
        return results
        
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        return []


def print_local_election_results(results: List[Dict[str, Any]]):
    """Print the local election costs results in a formatted way."""
    if not results:
        print("No results to display")
        return
        
    print(f"\nLocal Election Costs Analysis (from BigQuery)")
    print(f"{'=' * 95}")
    print(f"{'Candidate Name':<30} {'District':<15} {'Office':<15} {'Election Cycle':<15} {'Amount':<12} {'Transactions'}")
    print(f"{'-' * 95}")
    
    total_amount = 0
    for result in results:
        total_amount += result['total_amount']
        election_cycle_str = str(result['election_cycle']) if result['election_cycle'] else 'N/A'
        print(f"{result['candidate_name']:<30} "
              f"{result['district_normal']:<15} "
              f"{result['office_sought_normal']:<15} "
              f"{election_cycle_str:<15} "
              f"${result['total_amount']:>10,.2f} "
              f"{result['transaction_count']:>11}")
              
    print(f"{'-' * 95}")
    print(f"{'Total:':<77} ${total_amount:>10,.2f}")
    print(f"Total candidate-cycle combinations: {len(results)}")


def main():
    parser = argparse.ArgumentParser(description='Virginia Campaign Finance Data Analyzer')
    parser.add_argument('--project-id', type=str, required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--table', type=str, default='campaign_finance',
                       help='BigQuery table name (default: campaign_finance)')
    parser.add_argument('--output-csv', type=str,
                       help='Path to a CSV file to save the results')
    parser.add_argument('--analyze-elnoubi', action='store_true',
                       help='Analyze Elnoubi records for duplicates')
    
    args = parser.parse_args()
    
    try:
        if args.analyze_elnoubi:
            # Get Elnoubi records and analyze duplicates
            records = get_elnoubi_records(
                project_id=args.project_id,
                dataset_id=args.dataset,
                table_id=args.table
            )
            
            print(f"\n=== ALL ELNOUBI RECORDS ({len(records)}) ===")
            for i, record in enumerate(records):
                print(f"\nRecord {i+1}:")
                for key, value in record.items():
                    print(f"  {key}: {value}")
            
            # Apply deduplication to see what gets filtered
            deduplicated = deduplicate_records(records)
            print(f"\nAfter deduplication: {len(deduplicated)} records remain")
            
        else:
            # Normal processing
            results = get_bigquery_election_costs(
                project_id=args.project_id,
                dataset_id=args.dataset,
                table_id=args.table,
                districts=["blacksburg", "leesburg", "winchester", "alexandria"],
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
                print_local_election_results(results)

    except Exception as e:
        logger.error(f"Analysis failed: {e}")

if __name__ == "__main__":
    main()