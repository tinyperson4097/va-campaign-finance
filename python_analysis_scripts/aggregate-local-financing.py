import pandas as pd

import os
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime
import re
import io


"""Schedule H data processor class for Virginia Campaign Finance data."""
        

#-------------------CITIES------------------------------------
def read_cities(cities_csv: str) -> List[Dict[str, Any]]:
    # Load your CSV
    df = pd.read_csv(cities_csv)

    # Group by election cycle, office, and district
    agg_df = (
        df.groupby(['election_cycle', 'office_sought_normal', 'district_normal'])
        .agg(
            max_disbursements=('total_disbursements', 'max'),
            avg_disbursements=('total_disbursements', 'mean'),
            total_disbursements=('total_disbursements','sum'),
            num_candidates=('candidate_name', 'count')
        )
        .reset_index()
    )

    # Optional: sort for readability
    agg_df = agg_df.sort_values(['election_cycle', 'office_sought_normal', 'district_normal'])
    return agg_df.to_dict(orient='records')


#-------------------COUNTIES------------------------------------
def read_counties(counties_csv: str) -> List[Dict[str, Any]]:
    # Load your CSV
    df = pd.read_csv(counties_csv)
    # Group by election cycle, office, and district
    agg_df = (
        df.groupby(['election_cycle', 'office_sought_normal', 'mapped_county'])
        .agg(
            max_disbursements=('total_disbursements', 'max'),
            avg_disbursements=('total_disbursements', 'mean'),
            total_disbursements=('total_disbursements','sum'),
            num_candidates=('candidate_name', 'count')
        )
        .reset_index()
    )

    # Optional: sort for readability
    agg_df = agg_df.sort_values(['election_cycle', 'office_sought_normal', 'mapped_county'])
    return agg_df.to_dict(orient='records')
   

def main():
    parser = argparse.ArgumentParser(description='Aggregate Local Finance Data')
    parser.add_argument('--cities-csv', type=str,
                       help='Path to a CSV file to save the results')
    parser.add_argument('--counties-csv', type=str,
                       help='Path to a CSV file to save the results')
    parser.add_argument('--cities-csv-output', type=str,
                       help='Path to a CSV file to save the results')
    parser.add_argument('--counties-csv-output', type=str,
                       help='Path to a CSV file to save the results')
    
    args = parser.parse_args()
    
    # Create processor
    try:
        results = read_cities(
            cities_csv=args.cities_csv,
        )
   
    except (Exception) as e:
        #logger.error(f"Failed to initialize processor: {e}")
        return 1
     # Convert the results list back to a DataFrame for export
    
    
    df = pd.DataFrame(results)

    # Check if an output path was provided
    if args.cities_csv_output:
        df.to_csv(args.cities_csv_output, index=False)
        #logger.info(f"Successfully exported results to {args.output_csv}")
    
    # Only print if there is no output path
    else:
        print("lol")


    try:
        results = read_counties(
            counties_csv=args.counties_csv,
        )
   
    except (Exception) as e:
        #logger.error(f"Failed to initialize processor: {e}")
        return 1
    
    df=pd.DataFrame(results)
    # Check if an output path was provided
    if args.counties_csv_output:
        df.to_csv(args.counties_csv_output, index=False)
        #logger.info(f"Successfully exported results to {args.output_csv}")
    
    # Only print if there is no output path
    else:
        print("lol")
    


if __name__ == '__main__':
    exit(main())