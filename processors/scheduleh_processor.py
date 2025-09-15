#!/usr/bin/env python3
"""
Virginia Campaign Finance Schedule H Data Processor
Processes only Schedule H files and uploads to BigQuery
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

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import shared normalization functions
from functions.name_normalization import (
    normalize_name, normalize_office_sought, determine_government_level, 
    normalize_district, determine_primary_or_general
)

# Production mode imports
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

class ScheduleHProcessor:
    """Schedule H data processor class for Virginia Campaign Finance data."""
    
    def __init__(self, project_id: str, bucket_name: str = "va-cf-local", folders_after_year: int = None):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.folders_after_year = folders_after_year
        
        if not GCS_AVAILABLE:
            raise ImportError("Google Cloud dependencies not available. Install with: pip install google-cloud-storage google-cloud-bigquery pandas-gbq")
        if not project_id:
            raise ValueError("project_id is required")
        
        self.storage_client = storage.Client(project=project_id)
        self.bq_client = bigquery.Client(project=project_id)
    
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
    
    def _fix_embedded_quotes_universal(self, csv_data: str) -> str:
        """Remove all quotes that are not directly before/after commas or newlines."""

        cleaned_data = re.sub(r'(?<!^)(?<!,)["\'](?!,)(?!$)(?!\r?\n)', '', csv_data, flags=re.MULTILINE)
        
        return cleaned_data
    

    def _remove_commas_newlines_within_quoted_strings(self, csv_data: str) -> str:
        """Remove commas and newlines that appear within quoted strings using parity tracking."""

        cleaned_data = re.sub(r'([,\n])(?!")', '', csv_data)
        
        return cleaned_data
    
    def process_data(self) -> pd.DataFrame:
        """Main processing function that returns Schedule H DataFrame."""
        logger.info("Starting Schedule H processing")
        
        bucket = self.storage_client.bucket(self.bucket_name)
        
        # List all folders in the raw_data directory
        logger.info(f"Processing data from GCS bucket: {self.bucket_name}")
        
        # Extract unique folder names from blob paths
        prefixes = set()
        for blob in bucket.list_blobs(prefix='raw_data/'):
            # Extract folder name from path like 'raw_data/1999/ScheduleH.csv' -> '1999'
            path_parts = blob.name.split('/')
            if len(path_parts) >= 3 and path_parts[0] == 'raw_data':
                folder_name = path_parts[1]
                prefixes.add(folder_name)
        
        prefixes = list(prefixes)
        logger.info(f"Found {len(prefixes)} folders in bucket")
        
        # Filter folders based on year
        valid_folders = []
        for prefix in prefixes:
            folder_name = prefix.rstrip('/')
            if self.should_process_folder(folder_name):
                valid_folders.append(folder_name)
        
        if self.folders_after_year is not None:
            logger.info(f"Applied year filter (>= {self.folders_after_year}): {len(valid_folders)} folders remaining")
        
        logger.info(f"Processing {len(valid_folders)} folders for Schedule H data")
        
        all_schedule_h_data = []
        reports_data = []
        
        # Process each folder
        for folder in sorted(valid_folders):
            logger.info(f"Processing folder: {folder}")
            
            if self.is_old_folder(folder):
                folder_data = self._process_old_folder_gcs(bucket, folder)
                all_schedule_h_data.extend(folder_data)
            else:
                folder_reports, folder_schedule_h = self._process_new_folder_gcs(bucket, folder)
                reports_data.extend(folder_reports)
                all_schedule_h_data.extend(folder_schedule_h)
        
        # Convert to DataFrame
        df_schedule_h = pd.DataFrame(all_schedule_h_data)
        df_reports = pd.DataFrame(reports_data)
        
        logger.info(f"Processed {len(df_schedule_h)} Schedule H records from {len(valid_folders)} folders")
        return df_schedule_h
    
    def _process_old_folder_gcs(self, bucket, folder_name: str) -> List[Dict]:
        """Process an old format folder from GCS, looking only for Schedule H."""
        schedule_h_records = []
        
        # List CSV files in the folder
        blobs = bucket.list_blobs(prefix=f"raw_data/{folder_name}/")
        csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]
        
        for blob in csv_blobs:
            filename = blob.name.split('/')[-1]
            schedule_type = self.extract_schedule_type(filename)
            
            # Only process Schedule H
            if schedule_type.upper() != 'SCHEDULEH':
                continue
            
            logger.info(f"    Processing {filename}")
            
            try:
                # Download and process CSV with optimization
                csv_data = blob.download_as_text(encoding='latin-1')
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
                    skipinitialspace=True
                )
                
                # Drop NaN early so nothing gets concatenated
                df = df.fillna("").replace("nan", "")
                
                # Optimize dtypes for better performance
                for col in df.select_dtypes(['object']).columns:
                    unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
                    if unique_ratio < 0.5 and df[col].nunique() > 1:
                        df[col] = df[col].astype('category')
                records_processed = 0
                records_created = 0
                for _, row in df.iterrows():
                    records_processed += 1
                    schedule_h_record = self._map_old_row_to_schedule_h(row, folder_name)
                    if schedule_h_record:
                        records_created += 1
                        schedule_h_records.append(schedule_h_record)
                
                logger.info(f"    {filename}: {records_processed} rows processed, {records_created} records created")
            except UnicodeDecodeError as e:
                logger.warning(f"    Encoding error in {filename}: {e} - skipping file")
                continue
            except Exception as e:
                logger.warning(f"    Error processing {filename}: {e} - skipping file")
                continue
        
        return schedule_h_records
    
    def _process_new_folder_gcs(self, bucket, folder_name: str) -> Tuple[List[Dict], List[Dict]]:
        """Process a new format folder from GCS, looking for Report.csv and Schedule H."""
        reports = {}
        schedule_h_records = []
        
        # List CSV files in the folder
        blobs = list(bucket.list_blobs(prefix=f"raw_data/{folder_name}/"))
        csv_blobs = [blob for blob in blobs if blob.name.endswith('.csv')]
        
        # First process Report.csv
        report_blob = next((blob for blob in csv_blobs if blob.name.endswith('Report.csv')), None)
        if report_blob:
            logger.info(f"  Processing Report.csv")
            try:
                csv_data = report_blob.download_as_text(encoding='latin-1')
                csv_data = csv_data.replace("\r\n", "\n").replace("\r", "\n")
                csv_data = self._fix_embedded_quotes_universal(csv_data)
                csv_data = self._remove_commas_newlines_within_quoted_strings(csv_data)
                csv_data = self._fix_embedded_quotes_universal(csv_data)
                df_report = pd.read_csv(
                    io.StringIO(csv_data), 
                    engine='c',
                    on_bad_lines="skip", 
                    low_memory=False,
                    skip_blank_lines=True,
                    skipinitialspace=True
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
                        'report_id': row.get('ReportId'),
                        'committee_code': row.get('CommitteeCode'),
                        'committee_name': row.get('CommitteeName'),
                        'candidate_name': row.get('CandidateName'),
                        'report_year': pd.to_numeric(row.get('ReportYear'), errors='coerce'),
                        'filing_date': row.get('FilingDate'),
                        'start_date': row.get('StartDate'),
                        'end_date': row.get('EndDate'),
                        'due_date': row.get('DueDate'),
                        'amendment_count': pd.to_numeric(row.get('AmendmentCount'), errors='coerce') or 0,
                        'party': row.get('Party'),
                        'office_sought': row.get('OfficeSought'),
                        'district': row.get('District'),
                        'candidate_city': row.get('City'),
                        'election_cycle': row.get('ElectionCycle'),
                        'election_cycle_start_date': row.get('ElectionCycleStartDate'),
                        'election_cycle_end_date': row.get('ElectionCycleEndDate'),
                        'data_source': 'new',
                        'folder_name': folder_name
                    }
                    reports[row.get('ReportId')] = report_data
            except UnicodeDecodeError as e:
                logger.warning(f"  Encoding error in Report.csv for {folder_name}: {e} - skipping Report.csv")
            except Exception as e:
                logger.warning(f"  Error processing Report.csv for {folder_name}: {e} - skipping Report.csv")
        
        # Process Schedule H files
        for blob in csv_blobs:
            filename = blob.name.split('/')[-1]
            if filename.lower() == 'report.csv':
                continue
                
            schedule_type = self.extract_schedule_type(filename)
            
            # Only process Schedule H
            if schedule_type.upper() != 'SCHEDULEH':
                continue
            
            logger.info(f"    Processing {filename}")
            
            try:
                csv_data = blob.download_as_text(encoding='latin-1')
                csv_data = csv_data.replace("\r\n", "\n").replace("\r", "\n")
                csv_data = self._fix_embedded_quotes_universal(csv_data)
                csv_data = self._remove_commas_newlines_within_quoted_strings(csv_data)
                csv_data = self._fix_embedded_quotes_universal(csv_data)
                df = pd.read_csv(
                    io.StringIO(csv_data), 
                    engine='c',
                    on_bad_lines="skip",
                    low_memory=False,
                    skip_blank_lines=True,
                    skipinitialspace=True
                )
                
                # Drop NaN early so nothing gets concatenated
                df = df.fillna("").replace("nan", "")
                
                # Optimize dtypes
                for col in df.select_dtypes(['object']).columns:
                    unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
                    if unique_ratio < 0.5 and df[col].nunique() > 1:
                        df[col] = df[col].astype('category')
                records_processed = 0
                records_created = 0
                for _, row in df.iterrows():
                    records_processed += 1
                    schedule_h_record = self._map_new_row_to_schedule_h(row, folder_name, reports)
                    if schedule_h_record:
                        records_created += 1
                        schedule_h_records.append(schedule_h_record)
                
                logger.info(f"    {filename}: {records_processed} rows processed, {records_created} records created")
            except UnicodeDecodeError as e:
                logger.warning(f"    Encoding error in {filename}: {e} - skipping file")
                continue
            except Exception as e:
                logger.warning(f"    Error processing {filename}: {e} - skipping file")
                continue
        
        return list(reports.values()), schedule_h_records
    
    def _map_old_row_to_schedule_h(self, row: pd.Series, folder_name: str) -> Optional[Dict]:
        """Map old format row to Schedule H dictionary."""
        # Extract total disbursements - try different column names
        total_disbursements = None
        disbursement_fields = ['Total Disbursements', 'TOTAL_DISBURSEMENTS', 'TotalDisbursements']
        for field in disbursement_fields:
            if field in row and pd.notna(row[field]):
                try:
                    total_disbursements = float(row[field])
                    break
                except (ValueError, TypeError):
                    continue
        
        # Include all records - don't filter based on disbursements
        
        starting_balance = None
        sb_fields = ['Starting Balance', 'STARTING_BALANCE', 'BeginnningBalance', 'Begin Cash Bal']
        for field in sb_fields:
            if field in row and pd.notna(row[field]):
                try:
                    starting_balance  = float(row[field])
                    break
                except (ValueError, TypeError):
                    continue
        
        ending_balance = None
        eb_fields = ['Ending Balance', 'ENDING_BALANCE', 'EndingBalance']
        for field in eb_fields:
            if field in row and pd.notna(row[field]):
                try:
                    ending_balance  = float(row[field])
                    break
                except (ValueError, TypeError):
                    continue
        
        # Extract and normalize names
        candidate_name = self._extract_candidate_name_old(row)
        
        # Get office sought and district for normalization
        office_sought = row.get('Office Code') or row.get('OFFICE_CODE')
        district = row.get('Office Sub Code') or row.get('OFFICE_SUB_CODE')
        office_sought_normal = normalize_office_sought(office_sought)
        level = determine_government_level(office_sought_normal, district)
        district_normal = normalize_district(district, level=level, office_sought=office_sought)
        
        return {
            'report_id': row.get('Committee Code') or row.get('COMMITTEE_CODE'),
            'committee_code': row.get('Committee Code') or row.get('COMMITTEE_CODE'),
            'committee_name_normalized': normalize_name(row.get('Committee Name'), is_individual=False),
            'committee_name': row.get('Committee Name') or row.get('COMMITTEE_NAME'),
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
            'schedule_type': 'ScheduleH',
            'total_disbursements': total_disbursements,
            'starting_balance': starting_balance,
            'ending_balance': ending_balance,
            'data_source': 'old',
            'folder_name': folder_name
        }
    
    def _map_new_row_to_schedule_h(self, row: pd.Series, folder_name: str, reports: Dict) -> Optional[Dict]:
        """Map new format row to Schedule H dictionary."""
        # Extract total disbursements - allow all values including None
        try:
            total_disbursements = float(row.get('TotalDisbursements', 0))
        except (ValueError, TypeError):
            total_disbursements = None
        
        # Include all records - don't filter out zero or missing disbursements
        
        try:
            starting_balance = float(row.get('BeginningBalance', 0))
        except (ValueError, TypeError):
            starting_balance = None
        
        try:
            ending_balance = float(row.get('EndingBalance', 0))
        except (ValueError, TypeError):
            ending_balance = None

        try:
            line_19= float(row.get('ExpendableFundsBalance', 0))
        except (ValueError, TypeError):
            line_19 = None
        
        report_info = reports.get(row.get('ReportId'), {})
        
        # Debug logging for missing reports
        report_id = row.get('ReportId')
        if not report_info and report_id:
            logger.warning(f"Schedule H record found but no matching Report.csv entry for ReportId: {report_id}")
        
        # Get office sought and district for normalization
        office_sought = report_info.get('office_sought')
        district = report_info.get('district')
        candidate_city = report_info.get('candidate_city')
        election_cycle = report_info.get('election_cycle')
        office_sought_normal = normalize_office_sought(office_sought)
        level = determine_government_level(office_sought_normal, district)
        district_normal = normalize_district(district, candidate_city, level, office_sought)
        primary_or_general = determine_primary_or_general(election_cycle)
        
        return {
            'report_id': row.get('ReportId'),
            'committee_code': report_info.get('committee_code'),
            'committee_name': report_info.get('committee_name'),
            'committee_name_normalized': normalize_name(report_info.get('commitee_name'), is_individual=False),
            'candidate_name': report_info.get('candidate_name'),
            'candidate_name_normalized': normalize_name(report_info.get('candidate_name'), is_individual=True),
            'report_year': report_info.get('report_year'),
            'report_date': report_info.get('filing_date'),
            'due_date': report_info.get('due_date'),
            'amendment_count': report_info.get('amendment_count', 0),
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
            'schedule_type': 'ScheduleH',
            'total_disbursements': total_disbursements,
            'starting_balance': starting_balance,
            'ending_balance': ending_balance,
            'line_19': line_19,
            'data_source': 'new',
            'folder_name': folder_name
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
    
    
    
    
    
    
    
    
    def upload_to_bigquery(self, df: pd.DataFrame, table_id: str, dataset_id: str = 'virginia_elections') -> None:
        """Upload DataFrame to BigQuery with optimized performance."""
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


def main():
    parser = argparse.ArgumentParser(description='Virginia Campaign Finance Schedule H Data Processor')
    parser.add_argument('--project-id', type=str, required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--bucket-name', type=str, default='va-cf-local',
                       help='GCS bucket name (default: va-cf-local)')
    parser.add_argument('--bq-dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--bq-table', type=str, default='schedule_h',
                       help='BigQuery table name (default: schedule_h)')
    parser.add_argument('--folders-after', type=int, metavar='YEAR',
                       help='Only process folders from this year onwards (e.g., 2018)')
    
    args = parser.parse_args()
    
    # Create processor
    try:
        processor = ScheduleHProcessor(
            project_id=args.project_id,
            bucket_name=args.bucket_name,
            folders_after_year=args.folders_after
        )
    except (ImportError, ValueError) as e:
        logger.error(f"Failed to initialize processor: {e}")
        return 1
    
    try:
        # Process data
        df = processor.process_data()
        
        if df.empty:
            logger.warning("No Schedule H data was processed")
            return 1
        
        # Display summary
        logger.info(f"Processing complete:")
        logger.info(f"  Total Schedule H records: {len(df)}")
        logger.info(f"  Date range: {df['report_year'].min():.0f} - {df['report_year'].max():.0f}")
        logger.info(f"  Total disbursements: ${df['total_disbursements'].sum():,.2f}")
        logger.info(f"  Unique candidates: {df['candidate_name'].nunique()}")
        
        # Upload to BigQuery
        processor.upload_to_bigquery(df, args.bq_table, args.bq_dataset)
        
        return 0
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())