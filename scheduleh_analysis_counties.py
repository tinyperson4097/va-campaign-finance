#!/usr/bin/env python3
"""
County Schedule H Analysis for Virginia Campaign Finance data
Analyzes total disbursements for county board elections in Loudoun and Prince William counties
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


def get_city_to_county_mapping() -> Dict[str, str]:
    """
    Map Virginia cities/districts to their counties.
    Returns a dictionary mapping city names (lowercase) to county names.
    """
    return {
        # Loudoun County
        'leesburg': 'loudoun',
        'sterling': 'loudoun',
        'ashburn': 'loudoun',
        'herndon': 'loudoun',  # Note: Herndon is independent but serves Loudoun area
        'purcellville': 'loudoun',
        'hamilton': 'loudoun',
        'lovettsville': 'loudoun',
        'middleburg': 'loudoun',
        'round hill': 'loudoun',
        'hillsboro': 'loudoun',
        'lansdowne': 'loudoun',
        'brambleton': 'loudoun',
        'dulles': 'loudoun',
        'broadlands': 'loudoun',
        'cascades': 'loudoun',
        'countryside': 'loudoun',
        'stone ridge': 'loudoun',
        'south riding': 'loudoun',
        'algonkian': 'loudoun',
        'broad run': 'loudoun',
        'catoctin': 'loudoun',
        'dulles south': 'loudoun',
        'sugarland run': 'loudoun',
        
        # Prince William County
        'manassas': 'prince william',
        'manassas park': 'prince william',
        'woodbridge': 'prince william',
        'dale city': 'prince william',
        'lake ridge': 'prince william',
        'dumfries': 'prince william',
        'haymarket': 'prince william',
        'occoquan': 'prince william',
        'quantico': 'prince william',
        'triangle': 'prince william',
        'bristow': 'prince william',
        'gainesville': 'prince william',
        'nokesville': 'prince william',
        'independent hill': 'prince william',
        'linton hall': 'prince william',
        'montclair': 'prince william',
        'potomac mills': 'prince william',
        'princes lakes': 'prince william',
        'rippon': 'prince william',
        'sudley': 'prince william',
        'wellington': 'prince william',
        'cherry hill': 'prince william',
        'coles': 'prince william',
        'neabsco': 'prince william',
        'occoquan': 'prince william',
        'potomac': 'prince william',
        'woodbridge': 'prince william',
        'brentsville': 'prince william',
        'coles magisterial': 'prince william',
        'gainesville magisterial': 'prince william',
        'neabsco magisterial': 'prince william',
        'occoquan magisterial': 'prince william',
        'potomac magisterial': 'prince william',
        'woodbridge magisterial': 'prince william',
        'brentsville magisterial': 'prince william',
        
        # Additional common district patterns
        'loudoun county': 'loudoun',
        'prince william county': 'prince william',
        'pw county': 'prince william',
        'pwc': 'prince william',
        'pwcounty': 'prince william'
    }


def map_district_to_county(district_normal: str, candidate_city: str = None) -> str:
    """
    Map a district or city to its county using the mapping function.
    
    Parameters:
        district_normal (str): The normalized district field
        candidate_city (str): The candidate's city (fallback option)
        
    Returns:
        str: County name or None if no match found
    """
    city_county_map = get_city_to_county_mapping()
    
    # Check district_normal first
    if district_normal:
        district_lower = str(district_normal).lower().strip()
        
        # Direct lookup
        if district_lower in city_county_map:
            return city_county_map[district_lower]
        
        # Check if any mapped city is contained in the district
        for city, county in city_county_map.items():
            if city in district_lower or district_lower in city:
                return county
    
    # Check candidate_city as fallback
    if candidate_city:
        city_lower = str(candidate_city).lower().strip()
        
        # Direct lookup
        if city_lower in city_county_map:
            return city_county_map[city_lower]
        
        # Check if any mapped city is contained in the candidate city
        for city, county in city_county_map.items():
            if city in city_lower or city_lower in city:
                return county
    
    return None


def get_bigquery_county_disbursements(project_id: str, 
                                     dataset_id: str, 
                                     table_id: str,
                                     target_counties: List[str] = None) -> List[Dict[str, Any]]:
    """
    Analyze county board election disbursements using BigQuery Schedule H data.
    
    Parameters:
        project_id (str): Google Cloud Project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        target_counties (List[str]): List of target counties to filter by
        
    Returns:
        List[Dict]: List of candidate data with total disbursements
    """
    
    # Default to Loudoun and Prince William counties
    if target_counties is None:
        target_counties = ["loudoun", "prince william"]
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    try:
        # Build the query to get all Schedule H records for county board elections
        query = f"""
        WITH ranked_reports AS (
            SELECT 
                candidate_name,
                district_normal,
                candidate_city,
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
            AND level = 'local'
            AND (
                LOWER(office_sought_normal) LIKE '%county board%'
                OR LOWER(office_sought_normal) LIKE '%board of supervisors%'
                OR LOWER(office_sought_normal) LIKE '%supervisor%'
                OR LOWER(office_sought_normal) LIKE '%chair%county%'
            )
            AND (election_cycle IS NULL 
                OR election_cycle LIKE '%/2018%' 
                OR election_cycle LIKE '%/2019%' 
                OR election_cycle LIKE '%/202_%')
            AND candidate_name IS NOT NULL AND candidate_name != ''
        )
        SELECT 
            candidate_name,
            district_normal,
            candidate_city,
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
        
        # Execute the query
        logger.info(f"Executing BigQuery query for county board elections...")
        query_job = client.query(query)
        df = query_job.to_dataframe()
        
        if df.empty:
            logger.warning("No county board results found")
            return []
        
        # Convert to list of dictionaries and apply county mapping
        all_results = df.to_dict('records')
        
        # Filter results by target counties using our mapping function
        filtered_results = []
        for result in all_results:
            county = map_district_to_county(
                result.get('district_normal'), 
                result.get('candidate_city')
            )
            
            if county and county.lower() in [c.lower() for c in target_counties]:
                # Add county information to the result
                result['mapped_county'] = county.title()
                filtered_results.append(result)
        
        logger.info(f"Found {len(filtered_results)} candidates in target counties from {len(all_results)} total county board candidates")
        
        # Sort by total disbursements descending
        filtered_results.sort(key=lambda x: x['total_disbursements'], reverse=True)
            
        return filtered_results
        
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        return []


def print_county_disbursement_results(results: List[Dict[str, Any]]):
    """Print the county disbursement results in a formatted way."""
    if not results:
        print("No results to display")
        return
        
    print(f"\nCounty Board Election Disbursements Analysis (Schedule H)")
    print(f"{'=' * 125}")
    print(f"{'Candidate Name':<30} {'County':<15} {'District':<20} {'Office':<20} {'Election Cycle':<15} {'Disbursements':<15} {'Report Date'}")
    print(f"{'-' * 125}")
    
    total_disbursements = 0
    county_totals = {}
    
    for result in results:
        total_disbursements += result['total_disbursements']
        county = result.get('mapped_county', 'Unknown')
        
        # Track county totals
        if county not in county_totals:
            county_totals[county] = 0
        county_totals[county] += result['total_disbursements']
        
        election_cycle_str = str(result['election_cycle']) if result['election_cycle'] else 'N/A'
        report_date_str = str(result['report_date']) if result['report_date'] else 'N/A'
        
        print(f"{result['candidate_name']:<30} "
              f"{county:<15} "
              f"{str(result.get('district_normal', '')):<20} "
              f"{result['office_sought_normal']:<20} "
              f"{election_cycle_str:<15} "
              f"${result['total_disbursements']:>13,.2f} "
              f"{report_date_str}")
              
    print(f"{'-' * 125}")
    print(f"{'Total:':<102} ${total_disbursements:>13,.2f}")
    print(f"Total candidates: {len(results)}")
    
    # Print county breakdown
    print(f"\nCounty Breakdown:")
    print(f"{'County':<15} {'Total Disbursements':<20} {'Candidates'}")
    print(f"{'-' * 50}")
    for county, total in sorted(county_totals.items()):
        candidate_count = len([r for r in results if r.get('mapped_county') == county])
        print(f"{county:<15} ${total:>18,.2f} {candidate_count:>9}")


def main():
    parser = argparse.ArgumentParser(description='Virginia County Board Schedule H Analyzer')
    parser.add_argument('--project-id', type=str, required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--table', type=str, default='schedule_h',
                       help='BigQuery table name (default: schedule_h)')
    parser.add_argument('--counties', nargs='+', default=['loudoun', 'prince william'],
                       help='Counties to analyze (default: loudoun prince william)')
    parser.add_argument('--output-csv', type=str,
                       help='Path to a CSV file to save the results')
    
    args = parser.parse_args()
    
    try:
        results = get_bigquery_county_disbursements(
            project_id=args.project_id,
            dataset_id=args.dataset,
            table_id=args.table,
            target_counties=args.counties
        )

        # Convert the results list back to a DataFrame for export
        df = pd.DataFrame(results)

        # Check if an output path was provided
        if args.output_csv:
            df.to_csv(args.output_csv, index=False)
            logger.info(f"Successfully exported results to {args.output_csv}")
        
        # Only print if there is no output path
        else:
            print_county_disbursement_results(results)

    except Exception as e:
        logger.error(f"Analysis failed: {e}")

if __name__ == "__main__":
    main()