#!/usr/bin/env python3
"""
Virginia Campaign Finance Data Processor
Supports two modes:
1. Test Mode: Reads from local 'data' folder (2024_01 through 2024_12)
2. Production Mode: Reads from GCS bucket 'va-cf-local' and uploads to BigQuery
"""

import os
import sys
import pandas as pd
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
import re
import io
import pandas_gbq

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import shared normalization functions
from functions.name_normalization import (
    normalize_name, normalize_office_sought, determine_government_level, 
    normalize_district, determine_primary_or_general
)



# Production mode imports (only imported when needed)
try:
    from google.cloud import storage
    from google.cloud import bigquery
    import pandas_gbq
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VirginiaDataProcessor:
    """Main data processor class for Virginia Campaign Finance data."""
    
    def __init__(self, test_mode: bool = True, project_id: str = None, bucket_name: str = "va-cf-local", process_old_folders: bool = True, folders_after_year: int = None):
        self.test_mode = test_mode
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.process_old_folders = process_old_folders
        self.folders_after_year = folders_after_year
        
        # Track logged report IDs to avoid duplicate warnings
        self.logged_missing_reports = set()
        
        # Define which schedules to process and skip
        self.transactional_schedules = {
            'ScheduleA', 'ScheduleB', 'ScheduleC', 'ScheduleD', 'ScheduleF', 'ScheduleI'
        }
        self.skip_schedules = {'ScheduleG', 'ScheduleH', 'ScheduleE'}  # Summary/loan schedules
        
        # Initialize clients for production mode
        if not test_mode:
            if not GCS_AVAILABLE:
                raise ImportError("Google Cloud dependencies not available. Install with: pip install google-cloud-storage google-cloud-bigquery pandas-gbq")
            if not project_id:
                raise ValueError("project_id is required for production mode")
            
            self.storage_client = storage.Client(project=project_id)
            self.bq_client = bigquery.Client(project=project_id)
        else:
            self.storage_client = None
            self.bq_client = None
    
    def is_old_folder(self, folder_name: str) -> bool:
        """Check if folder follows old naming convention (YYYY format, <= 2011)."""
        return re.match(r'^\d{4}$', folder_name) and int(folder_name) <= 2011
    
    def get_folder_year(self, folder_name: str) -> int:
        """Extract year from folder name (e.g., '2024_01' -> 2024, '1999' -> 1999)."""
        if re.match(r'^\d{4}$', folder_name):
            # Old format: YYYY
            return int(folder_name)
        elif re.match(r'^\d{4}_\d{2}$', folder_name):
            # New format: YYYY_MM
            return int(folder_name.split('_')[0])
        else:
            # Unknown format, return 0 to exclude
            return 0
    
    def _safe_bool_convert(self, value) -> Optional[bool]:
        """Safely convert a value to boolean."""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            numeric_val = pd.to_numeric(value, errors='coerce')
            if pd.isna(numeric_val):
                return None
            return bool(numeric_val)
        except:
            return None
    
    def _clean_embedded_quotes_2018_12(self, csv_data: str) -> str:
        """Clean embedded quotes in 2018_12 CSV data that break parsing."""
        #import re
        
        # Very specific fix for the known problematic pattern in AuthorizingName field
        # Pattern: "William E. "Bill" Moody, Jr." -> "William E. 'Bill' Moody, Jr."
        
        # Find and replace the specific problematic pattern
        # Look for quotes around a nickname within a quoted field
        problematic_pattern = r'"William E\. "Bill" Moody, Jr\."'
        replacement = '"William E. \'Bill\' Moody, Jr."'
        
        cleaned_data = csv_data.replace(problematic_pattern, replacement)
        
        # Also handle any other similar nickname patterns if they exist
        # Pattern for "FirstName "Nickname" LastName" within quotes
        nickname_pattern = r'"([^"]*) "([^"]+)" ([^"]*)"'
        cleaned_data = re.sub(nickname_pattern, r'"\1 \'\2\' \3"', cleaned_data)
        
        return cleaned_data
    
    def _fix_embedded_quotes_universal(self, csv_data: str) -> str:
        """Remove all quotes that are not directly before/after commas or newlines."""

        cleaned_data = re.sub(r'(?<!^)(?<!,)["\'](?!,)(?!$)(?!\r?\n)', '', csv_data, flags=re.MULTILINE)
        
        return cleaned_data
    

    def _remove_commas_newlines_within_quoted_strings(self, csv_data: str) -> str:
        """Remove commas and newlines that appear within quoted strings using parity tracking."""

        cleaned_data = re.sub(r'([,\n])(?!")', '', csv_data)
        
        return cleaned_data
    
    def _clean_embedded_quotes_2022_07(self, csv_data: str) -> str:
        """Clean embedded quotes in 2022_07 CSV data that break parsing."""
        # Fix embedded quotes in ItemOrService field
        # Pattern: "Prepayment of the "Barn" for FCRC monthly membership meetings" 
        # -> "Prepayment of the 'Barn' for FCRC monthly membership meetings"
        problematic_pattern = r'"Prepayment of the "Barn" for FCRC monthly membership meetings for Oct, Nov, Dec 2022"'
        replacement = '"Prepayment of the \'Barn\' for FCRC monthly membership meetings for Oct, Nov, Dec 2022"'
        
        cleaned_data = csv_data.replace(problematic_pattern, replacement)
        
        return cleaned_data

    def _clean_embedded_quotes_2023_10(self, csv_data: str) -> str:
        """Clean embedded quotes in 2023_10 CSV data that break parsing."""
        # Fix embedded quotes and parentheses in AuthorizingName field
        # Pattern: "FCRC HQ Sept 2023 rent ($1442) and Sept 2023 utilities ($200)"
        problematic_pattern = r'"FCRC HQ Sept 2023 rent \(\$1442\) and Sept 2023 utilities \(\$200\)"'
        replacement = '"FCRC HQ Sept 2023 rent (\\$1442) and Sept 2023 utilities (\\$200)"'
        
        cleaned_data = csv_data.replace(problematic_pattern, replacement)
        
        return cleaned_data

    def _handle_encoding_2023_11(self, csv_data: str) -> str:
        """Handle encoding issues in 2023_11 CSV data."""
        # Replace smart quotes and other problematic characters
        replacements = {
            '\u2019': "'",  # Right single quotation mark
            '\u2018': "'",  # Left single quotation mark  
            '\u201c': '"',  # Left double quotation mark
            '\u201d': '"',  # Right double quotation mark
            '\u2013': '-',  # En dash
            '\u2014': '-',  # Em dash
        }
        
        cleaned_data = csv_data
        for bad_char, good_char in replacements.items():
            cleaned_data = cleaned_data.replace(bad_char, good_char)
        
        return cleaned_data
    
    def should_process_folder(self, folder_name: str) -> bool:
        """Check if folder should be processed based on year filter."""
        if self.folders_after_year is None:
            return True
        folder_year = self.get_folder_year(folder_name)
        return folder_year >= self.folders_after_year
    
    def extract_schedule_type(self, filename: str) -> str:
        """Extract schedule type from filename."""
        match = re.match(r'^(Schedule[A-Z])(_PAC)?\.csv$', filename, re.IGNORECASE)
        return match.group(1) if match else filename.replace('.csv', '')
    
    def process_data(self) -> pd.DataFrame:
        """Main processing function that returns a cleaned DataFrame."""
        logger.info(f"Starting data processing in {'TEST' if self.test_mode else 'PRODUCTION'} mode")
        
        if self.test_mode:
            return self._process_test_mode()
        else:
            return self._process_production_mode()

    def _process_test_mode(self) -> pd.DataFrame:
        """Process data from local 'data' folder (2024_01 through 2024_12)."""
        logger.info("Processing local data from 'data' folder")
        
        data_dir = Path(__file__).parent / 'data'
        if not data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
        
        # Get folders for 2024_01 through 2024_12
        folders = [f"2024_{i:02d}" for i in range(1, 13)]
        existing_folders = [f for f in folders if (data_dir / f).exists()]
        
        # Apply year filter if specified
        if self.folders_after_year is not None:
            existing_folders = [f for f in existing_folders if self.should_process_folder(f)]
            logger.info(f"Applied year filter (>= {self.folders_after_year}): {len(existing_folders)} folders remaining")
        
        logger.info(f"Found {len(existing_folders)} folders to process: {existing_folders}")
        
        all_data = []
        reports_data = []
        
        for folder in existing_folders:
            folder_path = data_dir / folder
            logger.info(f"Processing folder: {folder}")
            
            # Process this folder (all 2024 folders use new format)
            folder_reports, folder_transactions = self._process_new_folder(folder_path, folder)
            reports_data.extend(folder_reports)
            all_data.extend(folder_transactions)
        
        # Convert to DataFrame
        df_transactions = pd.DataFrame(all_data)
        df_reports = pd.DataFrame(reports_data)
        
        # Fix column types for BigQuery compatibility
        if not df_transactions.empty:
            # Fix entity_is_individual column type (should be bool)
            if 'entity_is_individual' in df_transactions.columns:
                df_transactions['entity_is_individual'] = df_transactions['entity_is_individual'].astype('boolean')
            
            # Fix onTime column type (should be int 0/1)  
            if 'onTime' in df_transactions.columns:
                # Convert boolean-like values to 0/1 integers
                df_transactions['onTime'] = df_transactions['onTime'].map(
                    lambda x: 1 if x is True or x == 1 or str(x).lower() in ['true', '1', 'yes'] 
                    else 0 if x is False or x == 0 or str(x).lower() in ['false', '0', 'no'] 
                    else None
                ).astype('Int64')  # Nullable integer type
            
            if 'entity_zip' in df_transactions.columns:
                df_transactions['entity_zip'] = df_transactions['entity_zip'].astype(str)
            
            if 'purpose' in df_transactions.columns:
                df_transactions['purpose'] = df_transactions['purpose'].astype(str)  
        
        logger.info(f"Processed {len(df_transactions)} transactions from {len(existing_folders)} folders")
        return df_transactions
    
    def _process_production_mode(self) -> pd.DataFrame:
        """Process data from GCS bucket and return cleaned DataFrame."""
        logger.info(f"Processing data from GCS bucket: {self.bucket_name}")
        
        bucket = self.storage_client.bucket(self.bucket_name)
        
        # List all folders in the raw_data directory
        print("Debugging GCS bucket listing...")
        print(f"Bucket name: {self.bucket_name}")
        print(f"Project ID: {self.project_id}")
        
        # Test basic bucket access
        try:
            bucket.reload()  # Test if bucket exists and is accessible
            print(f"Bucket exists and is accessible")
        except Exception as e:
            print(f"Error accessing bucket: {e}")
            return pd.DataFrame()
        
        # List ALL objects (without delimiter) to see what's actually in the bucket
        try:
            all_blobs = list(bucket.list_blobs(max_results=50))
            print(f"Total blobs found: {len(all_blobs)}")
            print("First 10 blob names:")
            for blob in all_blobs[:10]:
                print(f"  {blob.name}")
        except Exception as e:
            print(f"Error listing all blobs: {e}")
            
        # Try to list with raw_data prefix specifically
        try:
            raw_data_blobs = list(bucket.list_blobs(prefix='raw_data/', max_results=20))
            print(f"Raw data blobs found: {len(raw_data_blobs)}")
            print("Raw data blob names:")
            for blob in raw_data_blobs[:5]:
                print(f"  {blob.name}")
        except Exception as e:
            print(f"Error listing raw_data blobs: {e}")
        
        # Extract unique folder names from blob paths
        prefixes = set()
        for blob in bucket.list_blobs(prefix='raw_data/'):
            # Extract folder name from path like 'raw_data/1999/ScheduleA.csv' -> '1999'
            path_parts = blob.name.split('/')
            if len(path_parts) >= 3 and path_parts[0] == 'raw_data':
                folder_name = path_parts[1]
                prefixes.add(folder_name)
        
        prefixes = list(prefixes)
        print(f"Extracted folder names: {prefixes}")
        
        # Separate old (1999-2011) and new (2012_03-2025_08) folders
        old_folders = []
        new_folders = []
        
        for prefix in prefixes:
            folder_name = prefix.rstrip('/')
            # Apply year filter first
            if not self.should_process_folder(folder_name):
                continue
                
            if self.is_old_folder(folder_name):
                old_folders.append(folder_name)
            elif re.match(r'^\d{4}_\d{2}$', folder_name):
                new_folders.append(folder_name)
        
        if self.folders_after_year is not None:
            logger.info(f"Applied year filter (>= {self.folders_after_year})")
        
        logger.info(f"Found {len(old_folders)} old folders and {len(new_folders)} new folders")
        
        all_data = []
        reports_data = []
        
        # Process old folders (only if enabled)
        if self.process_old_folders:
            for folder in sorted(old_folders):
                if self.is_old_folder(folder):
                    logger.info(f"Processing old folder: {folder}")
                    folder_data = self._process_old_folder_gcs(bucket, folder)
                    all_data.extend(folder_data)
        else:
            logger.info(f"Skipping {len(old_folders)} old folders (process_old_folders=False)")
        
        # Process new folders
        for folder in sorted(new_folders):
            if not self.is_old_folder(folder):
                logger.info(f"Processing new folder: {folder}")
                folder_reports, folder_transactions = self._process_new_folder_gcs(bucket, folder)
                reports_data.extend(folder_reports)
                all_data.extend(folder_transactions)
        
        # Convert to DataFrame
        df_transactions = pd.DataFrame(all_data)
        df_reports = pd.DataFrame(reports_data)
        
        # Fix column types for BigQuery compatibility
        if not df_transactions.empty:
            # Fix entity_is_individual column type (should be bool)
            if 'entity_is_individual' in df_transactions.columns:
                df_transactions['entity_is_individual'] = df_transactions['entity_is_individual'].astype('boolean')
            
            # Fix onTime column type (should be int 0/1)  
            if 'onTime' in df_transactions.columns:
                # Convert boolean-like values to 0/1 integers
                df_transactions['onTime'] = df_transactions['onTime'].map(
                    lambda x: 1 if x is True or x == 1 or str(x).lower() in ['true', '1', 'yes'] 
                    else 0 if x is False or x == 0 or str(x).lower() in ['false', '0', 'no'] 
                    else None
                ).astype('Int64')  # Nullable integer type
            
            if 'entity_zip' in df_transactions.columns:
                df_transactions['entity_zip'] = df_transactions['entity_zip'].astype(str) 
            
            if 'purpose' in df_transactions.columns:
                df_transactions['purpose'] = df_transactions['purpose'].astype(str) 
        
        logger.info(f"Processed {len(df_transactions)} transactions from GCS")
        return df_transactions
    
    def _process_new_folder(self, folder_path: Path, folder_name: str) -> Tuple[List[Dict], List[Dict]]:
        """Process a new format folder from local filesystem."""
        csv_files = list(folder_path.glob('*.csv'))
        reports = {}
        transactions = []
        
        # First process Report.csv
        report_file = folder_path / 'Report.csv'
        if report_file.exists():
            logger.info(f"  Processing Report.csv")
            df_report = pd.read_csv(report_file, encoding='latin-1', on_bad_lines="skip", low_memory=False)
            

            # Drop NaN early so nothing gets concatenated
            df_report = df_report.fillna("").replace("nan", "")

            for _, row in df_report.iterrows():
                report_data = {
                    'report_id': pd.to_numeric(row.get('ReportId'), errors='coerce'),
                    'committee_code': row.get('CommitteeCode'),
                    'committee_name': row.get('CommitteeName'),
                    'candidate_name': row.get('CandidateName'),
                    'report_year': pd.to_numeric(row.get('ReportYear'), errors='coerce'),
                    'filing_date': row.get('FilingDate'),
                    'start_date': row.get('StartDate'),
                    'end_date': row.get('EndDate'),
                    'party': row.get('Party'),
                    'office_sought': row.get('OfficeSought'),
                    'district': row.get('District'),
                    'candidate_city': row.get('City'),
                    'election_cycle': row.get('ElectionCycle'),
                    'election_cycle_start_date': row.get('ElectionCycleStartDate'),
                    'election_cycle_end_date': row.get('ElectionCycleEndDate'),
                    'due_date': row.get('DueDate'),
                    'amendment_count': pd.to_numeric(row.get('AmendmentCount'), errors='coerce'),
                    'committee_type': row.get('CommitteeType'),
                    'zip_code': row.get('ZipCode'),
                    'submitted_date': row.get('SubmittedDate'),
                    'data_source': 'new',
                    'folder_name': folder_name
                }
                reports[row.get('ReportId')] = report_data
        
        # Process transactional schedules
        for csv_file in csv_files:
            if csv_file.name.lower() == 'report.csv':
                continue
                
            schedule_type = self.extract_schedule_type(csv_file.name)
            
            if schedule_type in self.skip_schedules:
                logger.info(f"    Skipping {csv_file.name} (summary/loan schedule)")
                continue
            
            if schedule_type not in self.transactional_schedules:
                logger.info(f"    Skipping {csv_file.name} (unknown schedule type)")
                continue
            
            logger.info(f"    Processing {csv_file.name}")
            df = pd.read_csv(csv_file, encoding='latin-1', on_bad_lines="skip", low_memory=False)
            
            # Drop NaN early so nothing gets concatenated
            df = df.fillna("").replace("nan", "")

            for _, row in df.iterrows():
                transaction = self._map_new_row_to_transaction(row, folder_name, schedule_type, reports)
                if transaction:
                    transactions.append(transaction)
        
        return list(reports.values()), transactions
    
    def _process_old_folder_gcs(self, bucket, folder_name: str) -> List[Dict]:
        """Process an old format folder from GCS."""
        transactions = []
        
        # List CSV files in the folder
        blobs = bucket.list_blobs(prefix=f"raw_data/{folder_name}/") #prefix=f"{folder_name}/")
        csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]
        
        for blob in csv_blobs:
            filename = blob.name.split('/')[-1]
            schedule_type = self.extract_schedule_type(filename)
            
            if schedule_type in self.skip_schedules:
                logger.info(f"    Skipping {filename} (summary/loan schedule)")
                continue
            
            if schedule_type not in self.transactional_schedules:
                logger.info(f"    Skipping {filename} (unknown schedule type)")
                continue
            
            logger.info(f"    Processing {filename}")
            
            try:
                # Download and process CSV with optimization
                csv_data = blob.download_as_text(encoding='latin-1')
                
                # Apply universal quote fixing to ALL CSV data first
                csv_data = csv_data.replace("\r\n", "\n").replace("\r", "\n")
                csv_data = self._fix_embedded_quotes_universal(csv_data)
                csv_data = self._remove_commas_newlines_within_quoted_strings(csv_data)
                csv_data = self._fix_embedded_quotes_universal(csv_data)
            
                
                df = pd.read_csv(
                    io.StringIO(csv_data), 
                    engine='c',  # Use faster C engine
                    on_bad_lines="skip", 
                    low_memory=False,
                    skip_blank_lines=True,
                    skipinitialspace=True,
                    quotechar='"',
                    doublequote=True,  # Handle embedded quotes properly
                    escapechar=None
                )
                
                # Drop NaN early so nothing gets concatenated
                df = df.fillna("").replace("nan", "")
                
                # Optimize dtypes for better performance
                for col in df.select_dtypes(['object']).columns:
                    unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
                    if unique_ratio < 0.5 and df[col].nunique() > 1:
                        df[col] = df[col].astype('category')
                for _, row in df.iterrows():
                    transaction = self._map_old_row_to_transaction(row, folder_name, schedule_type)
                    if transaction:
                        transactions.append(transaction)
            except UnicodeDecodeError as e:
                logger.warning(f"    Encoding error in {filename}: {e} - skipping file")
                continue
            except Exception as e:
                if "EOF inside string" in str(e) or "Error tokenizing data" in str(e):
                    logger.warning(f"    Quote parsing error in {filename}: {e} - skipping file")
                    logger.warning(f"    This indicates embedded quotes that couldn't be automatically fixed")
                else:
                    logger.warning(f"    Error processing {filename}: {e} - skipping file")
                continue
        
        return transactions
    
    def _process_new_folder_gcs(self, bucket, folder_name: str) -> Tuple[List[Dict], List[Dict]]:
        """Process a new format folder from GCS."""
        reports = {}
        transactions = []
        
        # List CSV files in the folder
        blobs = list(bucket.list_blobs(prefix=f"raw_data/{folder_name}/"))#prefix=f"{folder_name}/"))
        csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]
        
        # First process Report.csv
        report_blob = next((blob for blob in csv_blobs if blob.name.endswith('Report.csv')), None)
        if report_blob:
            logger.info(f"  Processing Report.csv")
            try:
                csv_data = report_blob.download_as_text(encoding='latin-1')
                
                # Apply universal quote fixing to ALL CSV data first
                csv_data = csv_data.replace("\r\n", "\n").replace("\r", "\n")

                csv_data = self._fix_embedded_quotes_universal(csv_data)
                csv_data = self._remove_commas_newlines_within_quoted_strings(csv_data)
                csv_data = self._fix_embedded_quotes_universal(csv_data)
                
                # Then apply special handling for specific folders with additional issues
                '''if folder_name == '2018_12':
                    csv_data = self._clean_embedded_quotes_2018_12(csv_data)
                elif folder_name == '2022_07':
                    csv_data = self._clean_embedded_quotes_2022_07(csv_data)
                elif folder_name == '2023_10':
                    csv_data = self._clean_embedded_quotes_2023_10(csv_data)
                elif folder_name == '2023_11':
                    csv_data = self._handle_encoding_2023_11(csv_data)'''
                
                df_report = pd.read_csv(
                    io.StringIO(csv_data), 
                    engine='c',
                    on_bad_lines="skip", 
                    low_memory=False,
                    skip_blank_lines=True,
                    skipinitialspace=True,
                    quotechar='"',
                    doublequote=True,  # Handle embedded quotes properly
                    escapechar=None
                )
                
                # Drop NaN early so nothing gets concatenated
                df_report = df_report.fillna("").replace("nan", "")
                
                # Optimize dtypes for report data
                for col in df_report.select_dtypes(['object']).columns:
                    unique_ratio = df_report[col].nunique() / len(df_report) if len(df_report) > 0 else 0
                    if unique_ratio < 0.5 and df_report[col].nunique() > 1:
                        df_report[col] = df_report[col].astype('category')
                for _, row in df_report.iterrows():
                    report_data = {
                        'report_id': pd.to_numeric(row.get('ReportId'), errors='coerce'),
                        'committee_code': row.get('CommitteeCode'),
                        'committee_name': row.get('CommitteeName'),
                        'candidate_name': row.get('CandidateName'),
                        'report_year': pd.to_numeric(row.get('ReportYear'), errors='coerce'),
                        'filing_date': row.get('FilingDate'),
                        'start_date': row.get('StartDate'),
                        'end_date': row.get('EndDate'),
                        'party': row.get('Party'),
                        'office_sought': row.get('OfficeSought'),
                        'district': row.get('District'),
                        'candidate_city': row.get('City'),
                        'election_cycle': row.get('ElectionCycle'),
                        'election_cycle_start_date': row.get('ElectionCycleStartDate'),
                        'election_cycle_end_date': row.get('ElectionCycleEndDate'),
                        'due_date': row.get('DueDate'),
                        'amendment_count': pd.to_numeric(row.get('AmendmentCount'), errors='coerce'),
                        'committee_type': row.get('CommitteeType'),
                        'zip_code': row.get('ZipCode'),
                        'submitted_date': row.get('SubmittedDate'),
                        'data_source': 'new',
                        'folder_name': folder_name
                    }
                    reports[row.get('ReportId')] = report_data
            except UnicodeDecodeError as e:
                logger.warning(f"  Encoding error in Report.csv for {folder_name}: {e} - skipping Report.csv")
            except Exception as e:
                logger.warning(f"  Error processing Report.csv for {folder_name}: {e} - skipping Report.csv")
        
        # Process transactional schedules
        for blob in csv_blobs:
            filename = blob.name.split('/')[-1]
            if filename.lower() == 'report.csv':
                continue
                
            schedule_type = self.extract_schedule_type(filename)
            
            if schedule_type in self.skip_schedules:
                logger.info(f"    Skipping {filename} (summary/loan schedule)")
                continue
            
            if schedule_type not in self.transactional_schedules:
                logger.info(f"    Skipping {filename} (unknown schedule type)")
                continue
            
            logger.info(f"    Processing {filename}")
            
            try:
                csv_data = blob.download_as_text(encoding='latin-1')
                
                # Apply universal quote fixing to ALL CSV data first
                csv_data = csv_data.replace("\r\n", "\n").replace("\r", "\n")

                csv_data = self._fix_embedded_quotes_universal(csv_data)
                csv_data = self._remove_commas_newlines_within_quoted_strings(csv_data)
                csv_data = self._fix_embedded_quotes_universal(csv_data)
                
                # Then apply special handling for specific folders with additional issues
                '''if folder_name == '2018_12':
                    csv_data = self._clean_embedded_quotes_2018_12(csv_data)
                elif folder_name == '2022_07':
                    csv_data = self._clean_embedded_quotes_2022_07(csv_data)
                elif folder_name == '2023_10':
                    csv_data = self._clean_embedded_quotes_2023_10(csv_data)
                elif folder_name == '2023_11':
                    csv_data = self._handle_encoding_2023_11(csv_data)'''
                
                df = pd.read_csv(
                    io.StringIO(csv_data), 
                    engine='c',
                    on_bad_lines="skip",
                    low_memory=False,
                    skip_blank_lines=True,
                    skipinitialspace=True,
                    quotechar='"',
                    doublequote=True,  # Handle embedded quotes properly
                    escapechar=None
                )
                
                # Drop NaN early so nothing gets concatenated
                df = df.fillna("").replace("nan", "")
                
                # Optimize dtypes
                for col in df.select_dtypes(['object']).columns:
                    unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
                    if unique_ratio < 0.5 and df[col].nunique() > 1:
                        df[col] = df[col].astype('category')
                for _, row in df.iterrows():
                    transaction = self._map_new_row_to_transaction(row, folder_name, schedule_type, reports)
                    if transaction:
                        transactions.append(transaction)
            except UnicodeDecodeError as e:
                logger.warning(f"    Encoding error in {filename}: {e} - skipping file")
                continue
            except Exception as e:
                if "EOF inside string" in str(e) or "Error tokenizing data" in str(e):
                    logger.warning(f"    Quote parsing error in {filename}: {e} - skipping file")
                    logger.warning(f"    This indicates embedded quotes that couldn't be automatically fixed")
                else:
                    logger.warning(f"    Error processing {filename}: {e} - skipping file")
                continue
        
        return list(reports.values()), transactions
    
    def _map_old_row_to_transaction(self, row: pd.Series, folder_name: str, schedule_type: str) -> Optional[Dict]:
        """Map old format row to transaction dictionary."""
        # Extract amount - try different column names, allow $0 transactions
        amount = 0.0  # Default to 0 if no amount found
        amount_fields = ['Trans Amount', 'TRANS_AMNT', 'Trans_Amount']
        for field in amount_fields:
            if field in row and pd.notna(row[field]):
                raw_amount = str(row[field]).strip()
                if raw_amount != '':
                    try:
                        # Clean common formatting issues
                        cleaned_amount = raw_amount.replace('$', '').replace(',', '').replace('(', '-').replace(')', '')
                        
                        # Handle negative amounts in parentheses format
                        if raw_amount.strip().startswith('(') and raw_amount.strip().endswith(')'):
                            cleaned_amount = '-' + raw_amount.strip()[1:-1].replace('$', '').replace(',', '')
                        
                        amount = float(cleaned_amount)
                        break
                    except (ValueError, TypeError):
                        # Log parsing error for debugging
                        logger.warning(f"Failed to parse amount '{raw_amount}' in field '{field}' for transaction")
                        continue
        
        # Don't filter out transactions - even $0 transactions are valid
        
        # Extract and normalize names
        candidate_name = self._extract_candidate_name_old(row)
        entity_name = self._extract_entity_name_old(row)
        
        # Get office sought and district for new columns
        office_sought = row.get('Office Code') or row.get('OFFICE_CODE')
        district = row.get('Office Sub Code') or row.get('OFFICE_SUB_CODE')
        office_sought_normal = normalize_office_sought(office_sought)
        level = determine_government_level(office_sought_normal, district)
        district_normal = normalize_district(district, level=level, office_sought=office_sought)
        
        return {
            'report_id': pd.to_numeric(row.get('Committee Code') or row.get('COMMITTEE_CODE'), errors='coerce'),
            'committee_code': row.get('Committee Code') or row.get('COMMITTEE_CODE'),
            'committee_name': row.get('Committee Name') or row.get('COMMITTEE_NAME'),
            'committee_name_normalized': normalize_name(row.get('Committee Name'), is_individual=False),
            'candidate_name': candidate_name,
            'candidate_name_normalized': normalize_name(candidate_name, is_individual=True),
            'report_year': pd.to_numeric(row.get('Report Year') or row.get('REPORT_YEAR') or folder_name, errors='coerce'),
            'report_date': row.get('Date Received') or row.get('DATE_RECEIVED'),
            'party': row.get('Party') or row.get('Party_Desc'),
            'office_sought': office_sought,
            'office_sought_normal': office_sought_normal,
            'district': district,
            'district_normal': district_normal,
            'level': level,
            'schedule_type': schedule_type,
            'transaction_date': row.get('Trans Date') or row.get('TRANS_DATE'),
            'amount': amount,
            'total_to_date': pd.to_numeric(row.get('Trans Agg To Date') or row.get('TRANS_AGG_TO_DATE'), errors='coerce'),
            'entity_name': entity_name,
            'entity_name_normalized': normalize_name(entity_name, is_individual=None),
            'entity_first_name': row.get('First Name') or row.get('FIRSTNAME'),
            'entity_last_name': row.get('Last Name') or row.get('LASTNAME'),
            'entity_address': row.get('Entity Address') or row.get('ENTITY_ADDRESS'),
            'entity_city': row.get('Entity City') or row.get('ENTITY_CITY'),
            'entity_state': row.get('Entity State') or row.get('ENTITY_STATE'),
            'entity_zip': row.get('Entity Zip') or row.get('ENTITY_ZIP'),
            'entity_employer': row.get('Entity Employer') or row.get('ENTITY_EMPLOYER'),
            'entity_occupation': row.get('Entity Occupation') or row.get('ENTITY_OCCUPATION'),
            'entity_is_individual': None,  # Not available in old format
            'transaction_type': row.get('Trans Type') or row.get('TRANS_TYPE'),
            'purpose': row.get('Trans Service Or Goods') or row.get('TRANS_ITEM_OR_SERVICE'),
            'committee_type': None,  # Not available in old format
            'zip_code': None,  # Not available in old format
            'submitted_date': None,  # Not available in old format
            'due_date': None,  # Not available in old format
            'amendment_count': None,  # Not available in old format
            'data_source': 'old',
            'folder_name': folder_name,
            'onTime': self._determine_on_time_status(
                row.get('Trans Date') or row.get('TRANS_DATE'),
                row.get('Date Received') or row.get('DATE_RECEIVED'),
                None,  # election_cycle not available in old format
                row.get('Report Year') or row.get('REPORT_YEAR') or folder_name
            )
        }
    
    def _map_new_row_to_transaction(self, row: pd.Series, folder_name: str, schedule_type: str, reports: Dict) -> Optional[Dict]:
        """Map new format row to transaction dictionary."""
        # Extract amount, allowing $0 transactions
        amount = 0.0  # Default to 0 if no amount found
        amount_value = row.get('Amount', 0)
        if pd.notna(amount_value):
            raw_amount = str(amount_value).strip()
            if raw_amount != '':
                try:
                    # Clean common formatting issues
                    cleaned_amount = raw_amount.replace('$', '').replace(',', '').replace('(', '-').replace(')', '')
                    
                    # Handle negative amounts in parentheses format
                    if raw_amount.strip().startswith('(') and raw_amount.strip().endswith(')'):
                        cleaned_amount = '-' + raw_amount.strip()[1:-1].replace('$', '').replace(',', '')
                    
                    amount = float(cleaned_amount)
                except (ValueError, TypeError):
                    # Log parsing error for debugging
                    logger.warning(f"Failed to parse amount '{raw_amount}' in Amount field for ReportId {row.get('ReportId', 'unknown')}")
                    # Keep the transaction but with 0 amount rather than filtering it out
                    amount = 0.0
        
        # Don't filter out transactions - even $0 transactions are valid
        
        report_info = reports.get(row.get('ReportId'), {})
        
        # Debug logging for missing reports (only log each report ID once)
        report_id = row.get('ReportId')
        if not report_info and report_id and report_id not in self.logged_missing_reports:
            logger.warning(f"Schedule A/E record found but no matching Report.csv entry for ReportId: {report_id}")
            self.logged_missing_reports.add(report_id)
        
        entity_name = self._build_entity_name_new(row)
        
        # Get office sought and district for new columns
        office_sought = report_info.get('office_sought')
        district = report_info.get('district')
        candidate_city = report_info.get('candidate_city')
        election_cycle = report_info.get('election_cycle')
        office_sought_normal = normalize_office_sought(office_sought)
        level = determine_government_level(office_sought_normal, district)
        district_normal = normalize_district(district, candidate_city, level, office_sought)
        primary_or_general = determine_primary_or_general(election_cycle)
        
        return {
            'report_id': pd.to_numeric(row.get('ReportId'), errors='coerce'),
            'committee_code': report_info.get('committee_code'),
            'committee_name': report_info.get('committee_name'),
            'committee_name_normalized': normalize_name(report_info.get('committee_name'), is_individual=False),
            'candidate_name': report_info.get('candidate_name'),
            'candidate_name_normalized': normalize_name(report_info.get('candidate_name'), is_individual=True),
            'report_year': report_info.get('report_year'),
            'report_date': report_info.get('filing_date'),
            'party': report_info.get('party'),
            'office_sought': office_sought,
            'office_sought_normal': office_sought_normal,
            'district': district,
            'district_normal': district_normal,
            'level': level,
            'candidate_city': candidate_city,
            'election_cycle': election_cycle,
            'primary_or_general': primary_or_general,
            'election_cycle_start_date': report_info.get('election_cycle_start_date'),
            'election_cycle_end_date': report_info.get('election_cycle_end_date'),
            'schedule_type': schedule_type,
            'transaction_date': row.get('TransactionDate'),
            'amount': amount,
            'total_to_date': pd.to_numeric(row.get('TotalToDate'), errors='coerce'),
            'entity_name': entity_name,
            'entity_name_normalized': normalize_name(entity_name, is_individual=self._safe_bool_convert(row.get('IsIndividual'))),
            'entity_first_name': row.get('FirstName'),
            'entity_last_name': row.get('LastOrCompanyName'),
            'entity_address': row.get('AddressLine1'),
            'entity_city': row.get('City'),
            'entity_state': row.get('StateCode'),
            'entity_zip': row.get('ZipCode'),
            'entity_employer': row.get('NameOfEmployer'),
            'entity_occupation': row.get('OccupationOrTypeOfBusiness'),
            'entity_is_individual': self._safe_bool_convert(row.get('IsIndividual')),
            'transaction_type': schedule_type,
            'purpose': row.get('ItemOrService') or row.get('ProductOrService') or row.get('PurposeOfObligation'),
            'committee_type': report_info.get('committee_type'),
            'zip_code': report_info.get('zip_code'),
            'submitted_date': report_info.get('submitted_date'),
            'due_date': report_info.get('due_date'),
            'amendment_count': report_info.get('amendment_count'),
            'data_source': 'new',
            'folder_name': folder_name,
            'onTime': self._determine_on_time_status(
                row.get('TransactionDate'),
                report_info.get('filing_date'),
                election_cycle,
                report_info.get('report_year')
            )
        }
    
    def _extract_candidate_name_old(self, row: pd.Series) -> str:
        """Extract candidate name from old format row."""
        first_name = str(row.get('First Name') or row.get('FIRSTNAME') or '').strip()
        last_name = str(row.get('Last Name') or row.get('LASTNAME') or '').strip()
        middle_name = str(row.get('Middle Name') or row.get('MIDDLENAME') or '').strip()
        
        if first_name and last_name:
            name_parts = [first_name, middle_name, last_name]
            return ' '.join(part for part in name_parts if part).strip()
        
        # Fall back to committee name
        return str(row.get('Committee Name') or row.get('COMMITTEE_NAME') or '').strip()
    
    def _extract_entity_name_old(self, row: pd.Series) -> str:
        """Extract entity name from old format row."""
        entity_name = str(row.get('Entity Name') or row.get('ENTITY_NAME') or '').strip()
        if entity_name:
            return entity_name
        
        # Build from name parts
        first_name = str(row.get('First Name') or row.get('FIRSTNAME') or '').strip()
        last_name = str(row.get('Last Name') or row.get('LASTNAME') or '').strip()
        middle_name = str(row.get('Middle Name') or row.get('MIDDLENAME') or '').strip()
        
        if first_name or last_name:
            name_parts = [first_name, middle_name, last_name]
            return ' '.join(part for part in name_parts if part).strip()
        
        return ''
    
    def _build_entity_name_new(self, row: pd.Series) -> str:
        """Build entity name from new format row."""
        last_or_company = str(row.get('LastOrCompanyName') or '').strip()
        if last_or_company:
            first_name = str(row.get('FirstName') or '').strip()
            middle_name = str(row.get('MiddleName') or '').strip()
            
            if first_name:
                name_parts = [first_name, middle_name, last_or_company]
                return ' '.join(part for part in name_parts if part).strip()
            return last_or_company
        return ''
    
    
    
    
    
    
    
    
    def _determine_on_time_status(self, transaction_date, reported_date, election_cycle, report_year):
        """
        Check if a transaction was reported on time based on filing deadlines.
        
        Parameters:
            transaction_date (str): Date of the transaction
            reported_date (str): Date the transaction was reported  
            election_cycle (str): Election cycle information (e.g., "11/2024")
            report_year (str/int): Year of the report
            
        Returns:
            bool: True if reported on time, False if late, None if cannot determine
        """
        from functions.filing_deadlines import get_filing_periods_for_year
        from datetime import datetime
        
        # Handle missing data
        if not transaction_date or not reported_date or not report_year:
            return None
            
        # Convert dates to datetime objects
        def to_date(val):
            if val is None or val == '':
                return None
            if isinstance(val, datetime):
                return val.date()
            if isinstance(val, str):
                # Try different date formats
                val_stripped = val.strip()
                # Handle microseconds format by truncating
                if '.' in val_stripped and len(val_stripped.split('.')[-1]) > 6:
                    # Truncate microseconds to 6 digits
                    parts = val_stripped.split('.')
                    val_stripped = parts[0] + '.' + parts[1][:6]
                
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%m/%d/%y"):
                    try:
                        return datetime.strptime(val_stripped, fmt).date()
                    except ValueError:
                        continue
                return None
            return None

        tx_date = to_date(transaction_date)
        rep_date = to_date(reported_date)
        
        # Ensure we have valid dates
        if not tx_date or not rep_date:
            return None
            
        # Get report year as integer
        try:
            report_year_int = int(report_year)
        except (ValueError, TypeError):
            return None
            
        # Determine election year from election_cycle
        election_year = None
        if election_cycle:
            if isinstance(election_cycle, str) and '/' in election_cycle:
                # Extract year from date format like "11/2024"
                try:
                    election_year = int(election_cycle.split('/')[-1])
                except (ValueError, IndexError):
                    pass
            else:
                # Try to convert directly to int
                try:
                    election_year = int(election_cycle)
                except (ValueError, TypeError):
                    pass
                    
        # Get filing periods for the report year
        filing_periods = get_filing_periods_for_year(report_year_int)
        
        # Check which filing period the transaction falls into
        for period in filing_periods:
            start = to_date(period["filingPeriodStart"])
            end = to_date(period["filingPeriodEnd"]) 
            deadline = to_date(period["filingPeriodDeadline"])
            on_cycle = period["onCycle"]
            
            if not start or not end or not deadline:
                continue
                
            # Check if transaction falls in this period
            if start <= tx_date <= end:
                # onCycle if report_year equals election_year, offCycle if not
                is_on_cycle = (report_year_int == election_year) if election_year else False
                
                # Use this period if it matches the cycle type we need
                if (is_on_cycle and on_cycle) or (not is_on_cycle and not on_cycle):
                    return rep_date <= deadline
                
        # If no matching period found, assume not on time
        return False
    
    def upload_to_bigquery2(self, df: pd.DataFrame, table_id: str, dataset_id: str = 'virginia_elections') -> None:
        """Upload DataFrame to BigQuery with optimized performance."""
        if self.test_mode:
            logger.warning("Cannot upload to BigQuery in test mode")
            return
        
        full_table_id = f"{self.project_id}.{dataset_id}.{table_id}"
        total_rows = len(df)
        logger.info(f"Uploading {total_rows} rows to BigQuery table: {full_table_id}")
        
        if total_rows == 0:
            logger.warning("No data to upload")
            return
        
        # Optimize DataFrame dtypes for faster upload and less memory
        for col in df.select_dtypes(['object']).columns:
            unique_ratio = df[col].nunique() / len(df)
            if unique_ratio < 0.5:  # Convert to category if less than 50% unique
                df[col] = df[col].astype('category')
        
        try:
            # Use native BigQuery client for better performance
            client = bigquery.Client(project=self.project_id)
            
            # Configure job for optimal performance
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                autodetect=True,
                max_bad_records=100
            )
            
            if total_rows <= 50000:
                # Small dataset - upload directly
                job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
                job.result()
                logger.info(f"Successfully uploaded {total_rows} rows")
            else:
                # Large dataset - upload in chunks for better performance
                chunk_size = 50000
                logger.info(f"Large dataset detected. Uploading in chunks of {chunk_size} rows...")
                
                # First chunk replaces the table
                first_chunk = df.iloc[:chunk_size]
                job = client.load_table_from_dataframe(first_chunk, full_table_id, job_config=job_config)
                job.result()
                logger.info(f"Uploaded chunk 1/{(total_rows-1)//chunk_size + 1}")
                
                # Subsequent chunks append to the table
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                
                for i in range(chunk_size, total_rows, chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    chunk_num = i // chunk_size + 1
                    job = client.load_table_from_dataframe(chunk, full_table_id, job_config=job_config)
                    job.result()
                    logger.info(f"Uploaded chunk {chunk_num + 1}/{(total_rows-1)//chunk_size + 1}")
                
                logger.info(f"Successfully uploaded all {total_rows} rows")
        
        except Exception as e:
            logger.error(f"Native BigQuery upload failed: {e}")
            # Fallback to pandas-gbq for smaller datasets
            if total_rows < 100000:
                logger.info("Falling back to pandas-gbq...")
                pandas_gbq.to_gbq(
                    df,
                    destination_table=full_table_id,
                    project_id=self.project_id,
                    if_exists='replace',
                    progress_bar=True,
                    chunksize=10000
                )
                logger.info("Fallback upload successful")
            else:
                raise

    def upload_to_bigquery(
        self, df: pd.DataFrame, table_id: str, dataset_id: str = "virginia_elections"
    ) -> None:
        """Upload DataFrame to BigQuery using pandas-gbq."""

        if self.test_mode:
            logger.warning("Cannot upload to BigQuery in test mode")
            return

        full_table_id = f"{dataset_id}.{table_id}"
        total_rows = len(df)
        logger.info(f"Uploading {total_rows} rows to BigQuery table: {self.project_id}.{full_table_id}")

        if total_rows == 0:
            logger.warning("No data to upload")
            return

        # Optimize object dtypes before upload
        for col in df.select_dtypes(["object"]).columns:
            unique_ratio = df[col].nunique(dropna=True) / len(df)
            if unique_ratio < 0.5:
                df[col] = df[col].astype("category")

        try:
            # Upload using pandas-gbq
            pandas_gbq.to_gbq(
                df,
                destination_table=full_table_id,
                project_id=self.project_id,
                if_exists="replace",   # overwrite each run
                progress_bar=True,
                chunksize=10000,       # tune as needed
            )
            logger.info(f"Successfully uploaded {total_rows} rows to {full_table_id}")
        except Exception as e:
            logger.error(f"pandas-gbq upload failed: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description='Virginia Campaign Finance Data Processor')
    parser.add_argument('--mode', choices=['test', 'production'], default='test',
                       help='Processing mode: test (local data) or production (GCS + BigQuery)')
    parser.add_argument('--project-id', type=str,
                       help='Google Cloud project ID (required for production mode)')
    parser.add_argument('--bucket-name', type=str, default='va-cf-local',
                       help='GCS bucket name (default: va-cf-local)')
    parser.add_argument('--bq-dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--bq-table', type=str, default='campaign_finance',
                       help='BigQuery table name (default: campaign_finance)')
    parser.add_argument('--skip-old-folders', action='store_true',
                       help='Skip processing old format folders (1999-2011)')
    parser.add_argument('--folders-after', type=int, metavar='YEAR',
                       help='Only process folders from this year onwards (e.g., 2018)')
    
    args = parser.parse_args()
    
    # Determine if we're in test mode
    test_mode = args.mode == 'test'
    
    # Create processor
    try:
        processor = VirginiaDataProcessor(
            test_mode=test_mode,
            project_id=args.project_id,
            bucket_name=args.bucket_name,
            process_old_folders=not args.skip_old_folders,
            folders_after_year=args.folders_after
        )
    except (ImportError, ValueError) as e:
        logger.error(f"Failed to initialize processor: {e}")
        return 1
    
    try:
        # Process data
        df = processor.process_data()
        
        if df.empty:
            logger.warning("No data was processed")
            return 1
        
        # Display summary
        logger.info(f"Processing complete:")
        logger.info(f"  Total transactions: {len(df)}")
        logger.info(f"  Date range: {df['report_year'].min():.0f} - {df['report_year'].max():.0f}")
        logger.info(f"  Total amount: ${df['amount'].sum():,.2f}")
        logger.info(f"  Unique candidates: {df['candidate_name'].nunique()}")
        logger.info(f"  Unique entities: {df['entity_name'].nunique()}")
        
        # Upload to BigQuery if in production mode
        if not test_mode:
            processor.upload_to_bigquery(df, args.bq_table, args.bq_dataset)
        else:
            logger.info("Test mode: Data processing complete. Use production mode to upload to BigQuery.")
            
            # Save to database in data folder
            import sqlite3
            db_path = Path(__file__).parent / 'data' / 'campaign_finance.db'
            db_path.parent.mkdir(exist_ok=True)
            conn = sqlite3.connect(str(db_path))
            df.to_sql("transactions", conn, if_exists="replace", index=False)
            conn.close()
            logger.info(f"Saved {len(df)} records to local SQLite DB: {db_path}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
