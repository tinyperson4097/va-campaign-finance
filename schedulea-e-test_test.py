#!/usr/bin/env python3
"""
Tests and analysis functions for Virginia Campaign Finance data
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any


def get_local_election_costs(districts: List[str] = None, 
                           offices: List[str] = None,
                           schedule_filter: str = "ScheduleD") -> List[Dict[str, Any]]:
    """
    Analyze local election costs by candidate.
    
    Parameters:
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
    
    # Connect to database
    db_path = Path(__file__).parent / 'data' / 'campaign_finance.db'
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return []
        
    conn = sqlite3.connect(str(db_path))
    
    try:
        # Build the query
        query = """
        SELECT 
            candidate_name,
            district_normal,
            office_sought_normal,
            election_cycle,
            SUM(amount) as total_amount,
            COUNT(*) as transaction_count
        FROM transactions
        WHERE 1=1
        """
        
        params = []
        
        # Add filters
        if schedule_filter:
            query += " AND schedule_type = ?"
            params.append(schedule_filter)
            
        # Filter by level = 'local'
        query += " AND level = ?"
        params.append('local')
        
        # Filter by districts (case insensitive)
        if districts:
            district_placeholders = ','.join(['?' for _ in districts])
            query += f" AND LOWER(district_normal) IN ({district_placeholders})"
            params.extend([d.lower() for d in districts])
            
        # Filter by offices (case insensitive)
        if offices:
            office_placeholders = ','.join(['?' for _ in offices])
            query += f" AND LOWER(office_sought_normal) IN ({office_placeholders})"
            params.extend([o.lower() for o in offices])
            
        # Filter by election cycles 2018 and later
        query += " AND (election_cycle IS NULL OR election_cycle LIKE '%/2018%' OR election_cycle LIKE '%/2019%' OR election_cycle LIKE '%/202_%')"
            
        # Group by candidate and election cycle, sort by total amount descending
        query += """
        GROUP BY candidate_name, district_normal, office_sought_normal, election_cycle
        HAVING candidate_name IS NOT NULL AND candidate_name != ''
        ORDER BY total_amount DESC
        """
        
        print(f"Executing query: {query}")
        print(f"Parameters: {params}")
        
        # Execute query
        df = pd.read_sql_query(query, conn, params=params)
        
        if df.empty:
            print("No results found with the specified filters")
            return []
            
        # Convert to list of dictionaries
        results = []
        for _, row in df.iterrows():
            results.append({
                'candidate_name': row['candidate_name'],
                'district_normal': row['district_normal'],
                'office_sought_normal': row['office_sought_normal'],
                'election_cycle': row['election_cycle'],
                'total_amount': float(row['total_amount']),
                'transaction_count': int(row['transaction_count'])
            })
            
        return results
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return []
    finally:
        conn.close()


def print_local_election_results(results: List[Dict[str, Any]]):
    """Print the local election costs results in a formatted way."""
    if not results:
        print("No results to display")
        return
        
    print(f"\nLocal Election Costs Analysis")
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


def test_local_election_analysis():
    """Test the local election cost analysis function."""
    print("Testing local election cost analysis...")
    
    # Test with specified districts and offices
    districts = ["blacksburg", "leesburg", "winchester", "alexandria"]
    offices = ["city council", "mayor", "town council"]
    
    results = get_local_election_costs(districts=districts, offices=offices)
    
    print(f"Found {len(results)} candidates matching criteria")
    print_local_election_results(results)
    
    return results


if __name__ == "__main__":
    # Run the test
    test_local_election_analysis()