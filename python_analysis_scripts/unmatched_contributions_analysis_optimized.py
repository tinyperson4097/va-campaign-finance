#!/usr/bin/env python3
"""
Unmatched Contributions Analysis — "Committee Reported, Candidate Did Not"

Identifies transactions where the donor committee reported it (Schedule D)
but the recipient did not (Schedule A). Recipients that map to a candidate
campaign committee are matched against receipts under any of that candidate's
committee codes; non-candidate recipients (matched_candidate_name =
'NOT A CC') are matched against their one mapped committee code -- PACs and
other non-candidate committees file Schedule A too, so both kinds can match,
and the output shows which kind each unmatched row is.

Options:
  --output-csv      output csv file
  --min-year        only consider reports after this year
  --committee-only  only consider transactions donated by this committee
  --min-amount      only considers transactions greater than this $ amount
  --test-mode       stops after 100 missing transactions found
  --debug           prints extensive debugging logs
"""

import pandas as pd
import argparse
import logging
from google.cloud import bigquery
from typing import List, Dict, Any, Tuple
from datetime import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class OptimizedContributionMatcher:
    """Exact matcher keyed on the committee_mappings lookup table."""

    def __init__(self):
        # Lookup tables
        self.committee_mappings = {}  # committee_name_normalized -> {committee_code, candidate_name_normalized}

        # Cache for committee name cleaning
        self.cleaned_names_cache = {}

    def load_lookup_tables(self, client, project_id: str, dataset: str):
        """Load committee_mappings table for exact lookups."""
        logger.info("Loading committee_mappings lookup table...")

        # Load committee_mappings table
        committee_mappings_query = f"""
        SELECT committee_code, committee_name_normalized, candidate_name_normalized
        FROM `{project_id}.{dataset}.committee_mappings`
        """
        committee_mappings_df = client.query(committee_mappings_query).to_dataframe()

        for _, row in committee_mappings_df.iterrows():
            committee_code = row['committee_code']
            committee_name = row['committee_name_normalized']
            candidate_name = row.get('candidate_name_normalized', '')

            # Store as committee_name_normalized -> {committee_code, candidate_name_normalized}
            if committee_name:
                self.committee_mappings[committee_name] = {
                    'committee_code': committee_code,
                    'candidate_name_normalized': candidate_name
                }

        logger.info(f"Loaded {len(self.committee_mappings)} committee mappings")

    
    def clean_committee_name(self, name: str) -> str:
        """Clean committee name with caching."""
        if pd.isna(name):
            return ''
        
        name_str = str(name)
        if name_str in self.cleaned_names_cache:
            return self.cleaned_names_cache[name_str]
        
        # Convert to lowercase and remove extra whitespace
        cleaned = name_str.lower().strip()
        
        # Fix double spaces and normalize whitespace FIRST
        cleaned = ' '.join(cleaned.split())
        
        # Remove common patterns that create matching issues
        # BUT be more careful about PAC removal - only remove if it's clearly a suffix
        patterns_to_remove = [
            r'\bfor\s+(va|virginia|delegate|house|senate|governor|attorney\s+general|lt\.?\s*governor)\b',
            r'\b(delegate|house|senate|governor|attorney\s+general|lt\.?\s*governor)\s+campaign\b',
            r'\bcampaign\s+(committee|fund)\b',
            r'\b(the\s+)?committee\s+(to\s+elect|for)\b',
            r'\belect\b',
            r'\b\d{4}\s+(primary|general|election)\b',
            r'\b(primary|general)\s+\d{4}\b',
            r'[^\w\s]'  # Remove punctuation
        ]
        
        for pattern in patterns_to_remove:
            cleaned = re.sub(pattern, ' ', cleaned)
        
        # Minimal normalization since most should be handled by processor
        # Just basic cleanup and use normalized names from database
        
        # Only remove PAC/Inc if it's at the end (to preserve distinctions like "Old Dominion PAC" vs "Dominion Energy PAC")  
        cleaned = re.sub(r'\s+pac\s*$', '', cleaned)
        cleaned = re.sub(r'\s+(inc|llc|corp|corporation)\s*$', '', cleaned)
        
        # Final whitespace normalization
        cleaned = ' '.join(cleaned.split())
        
        # Cache result
        self.cleaned_names_cache[name_str] = cleaned
        return cleaned
    
    def find_matching_committee_batch(self, recipient_names: List[str], filing_years: List[int]) -> List[Dict]:
        """Find matching committees for a batch of recipient names using committee_mappings lookup table."""
        if not self.committee_mappings:
            logger.warning("No committee_mappings loaded - returning no matches")
            return [None] * len(recipient_names)

        results = []

        for i, recipient_name in enumerate(recipient_names):
            filing_year = filing_years[i] if i < len(filing_years) else 2020

            if not recipient_name or recipient_name.strip() == '':
                results.append(None)
                continue

            # Try exact lookup in committee_mappings
            recipient_clean = recipient_name.strip() if recipient_name else ''

            if not recipient_clean:
                results.append(None)
                continue

            matched_committee = None

            # Direct lookup in committee_mappings (committee_name_normalized -> committee_code, candidate_name_normalized)
            if recipient_clean in self.committee_mappings:
                committee_info = self.committee_mappings[recipient_clean]
                matched_committee = {
                    'committee_code': committee_info['committee_code'],
                    'committee_name_normalized': recipient_clean,
                    'candidate_name_normalized': committee_info['candidate_name_normalized']
                }

                logger.debug(f"DIRECT MATCH: '{recipient_name}' -> {matched_committee['committee_code']}")

            if matched_committee:
                candidate_name = matched_committee.get('candidate_name_normalized', '')

                if candidate_name != 'NOT A CC':
                    # For candidate committees: Find all committee codes for this candidate
                    all_candidate_committees = []
                    for norm_name, info in self.committee_mappings.items():
                        if info.get('candidate_name_normalized') == candidate_name:
                            all_candidate_committees.append({
                                'committee_code': info['committee_code'],
                                'committee_name_normalized': norm_name,
                                'candidate_name_normalized': info['candidate_name_normalized']
                            })

                    if len(all_candidate_committees) > 1:
                        # Select committee code with year closest to filing year
                        best_committee = self.select_closest_committee_by_year(all_candidate_committees, filing_year)
                        results.append(best_committee if best_committee else matched_committee)
                    else:
                        results.append(matched_committee)
                else:
                    # Non-CC recipient (matched_candidate_name = 'NOT A CC'):
                    # kept in the analysis so the output shows whether each
                    # unmatched row's recipient is a candidate committee or
                    # not. Matched as-is against its one mapped committee
                    # code -- the Schedule A pool covers all committee types
                    # (PACs etc. file Schedule A too), so a match is real.
                    results.append(matched_committee)
            else:
                results.append(None)

        return results
    
    def select_closest_committee_by_year(self, committees_list: List[Dict], filing_year: int) -> Dict:
        """
        Select committee code with year closest to filing year.
        Committee codes follow pattern 'CC-XX-' where XX is the year.
        """
        best_committee = None
        min_year_diff = float('inf')
        
        logger.debug(f"\n=== COMMITTEE YEAR SELECTION DEBUG ===")
        logger.debug(f"Filing year: {filing_year}")
        logger.debug(f"Available committees: {len(committees_list)}")
        
        for committee in committees_list:
            committee_code = committee['committee_code']
            committee_name = committee.get('committee_name_normalized', 'N/A')
            candidate_name = committee.get('candidate_name_normalized', 'N/A')
            
            if pd.isna(committee_code):
                logger.debug(f"  SKIPPED: {candidate_name} - No committee code")
                continue
                
            # Extract year from committee code (CC-XX-...)
            match = re.match(r'CC-(\d{2})-', str(committee_code))
            if match:
                committee_year_suffix = int(match.group(1))
                # Convert 2-digit year to 4-digit (assuming 20XX for years < 50, 19XX for >= 50)
                if committee_year_suffix < 50:
                    committee_year = 2000 + committee_year_suffix
                else:
                    committee_year = 1900 + committee_year_suffix
                
                year_diff = abs(filing_year - committee_year)
                
                logger.debug(f"  {committee_code}: {candidate_name} ({committee_year}) - diff: {year_diff} years")
                
                if year_diff < min_year_diff:
                    min_year_diff = year_diff
                    best_committee = committee
                    logger.debug(f"    -> NEW BEST MATCH (diff: {year_diff})")
            #else:
                #logger.debug(f"  SKIPPED: {committee_code} - Invalid format")
        
        if best_committee:
            logger.debug(f"SELECTED: {best_committee['committee_code']} ({best_committee.get('candidate_name_normalized', 'N/A')}) - {min_year_diff} year difference")
        else:
            logger.debug("NO COMMITTEE SELECTED")
        
        return best_committee
    
    def find_matching_schedule_a_batch(self, batch_data: List[Tuple]) -> List[Tuple[bool, Dict]]:
        """Find matching Schedule A receipts for a batch of Schedule D records."""
        results = []
        
        for d_row_dict, matched_committee, schedule_a_subset in batch_data:
            if not matched_committee:
                results.append((False, {}))
                continue

            donor_name = d_row_dict['donor_committee_name']
            amount = d_row_dict['amount']
            transaction_date = pd.to_datetime(d_row_dict['transaction_date'])

            if pd.isna(donor_name) or donor_name.strip() == '':
                results.append((False, {}))
                continue
            

            if schedule_a_subset.empty:
                results.append((False, {
                    'reason': 'no_schedule_a_receipts',
                    'committee_code': matched_committee['committee_code'],
                    'candidate_name_normalized': matched_committee['candidate_name_normalized'],
                    'matched_committee_name': matched_committee.get('committee_name_normalized', 'N/A')
                }))
                continue
            
            best_match_info = {
                'best_score': 0,
                'best_candidate': None,
                'amount_matches': 0,
                'date_matches': 0,
                'total_candidates': len(schedule_a_subset),
                'reasons': []
            }
            
            found_match = False
            date_amount_match = False
            
            # Look for matching receipts and track best match
            for _, a_row in schedule_a_subset.iterrows():
                receipt_donor_name = a_row['donor_name']
                if pd.isna(receipt_donor_name) or receipt_donor_name.strip() == '':
                    continue
                    
                # Use normalized names from database instead of doing our own normalization
                receipt_donor_normalized = a_row.get('donor_name_normalized', receipt_donor_name)
                donor_normalized = d_row_dict.get('donor_committee_name_normalized', donor_name)
                
                # Fallback to basic cleaning if normalized versions not available
                if pd.isna(receipt_donor_normalized) or receipt_donor_normalized == '':
                    receipt_donor_normalized = self.clean_committee_name(receipt_donor_name)
                if pd.isna(donor_normalized) or donor_normalized == '':
                    donor_normalized = self.clean_committee_name(donor_name)
                    
                # Use exact match only (no fuzzy matching)
                name_match = (donor_normalized == receipt_donor_normalized)
                name_score = 100 if name_match else 0


                # Track best match regardless of other criteria
                if name_score > best_match_info['best_score']:
                    best_match_info['best_score'] = name_score
                    best_match_info['best_candidate'] = receipt_donor_normalized
                
                # Check amount match (exact)
                amount_match = abs(a_row['amount'] - amount) <= 0.01
                if amount_match:
                    best_match_info['amount_matches'] += 1
                
                # Check date match (within 30 days)
                try:
                    a_date = pd.to_datetime(a_row['transaction_date'], errors='coerce')
                    d_date = pd.to_datetime(transaction_date, errors='coerce')
                    
                    # Flag parsing issues
                    if pd.isna(a_date) and pd.notna(a_row['transaction_date']):
                        logger.debug(f"Failed to parse Schedule A date: {a_row['transaction_date']}")
                    if pd.isna(d_date) and pd.notna(transaction_date):
                        logger.debug(f"Failed to parse Schedule D date: {transaction_date}")
                    
                    date_match = True
                    if pd.notna(a_date) and pd.notna(d_date):
                        date_match = ((a_date - d_date).days <= 60) and ((a_date - d_date).days >= -30)
                       
                        if date_match:
                            best_match_info['date_matches'] += 1
                    elif pd.isna(a_date) or pd.isna(d_date):
                        best_match_info['date_matches'] += 1  # Count missing dates as matches
                except Exception as e:
                    logger.debug(f"Date parsing error: {e}")
                    date_match = True
                
                # Check if this is a full match
                if amount_match:
                    if date_match:
                        date_amount_match = True

                        if name_match:  # Exact match required
                            found_match = True
                            break
            
            if found_match:
                results.append((True, {}))
            else:
                # No match found - determine likely reasons
                reasons = []
                if date_amount_match:
                    reasons.append("yes date + amount match combo")
                
                if not date_amount_match:
                    reasons.append("no date + amount match combo")
                    
                best_match_info['reasons'] = reasons


                results.append((False, best_match_info))
        
        return results


def get_unmatched_contributions_optimized(project_id: str,
                                        dataset_id: str,
                                        table_id: str,
                                        min_year: int = 2018,
                                        batch_size: int = 1000,
                                        test_mode: bool = False,
                                        min_amount: int = 1000,
                                        committee_only: str = None) -> List[Dict[str, Any]]:
    """
    Find Schedule D expenses that are political contributions to candidate
    campaign committees with no matching Schedule A receipt.
    """

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    matcher = OptimizedContributionMatcher()

    
    try:
        # Build committee filter if requested
        committee_filter = ""
        if committee_only:
            # Handle specific hard-coded committee name mappings
            if committee_only.upper() == "DOMINION ENERGY":
                committee_filter = """
                AND (
                    UPPER(committee_name_normalized) LIKE '%DOMINION ENERGY%'
                )
                """
            
            else:
                # Generic filter for any committee name
                committee_name_escaped = committee_only.replace("'", "''")
                committee_filter = f"""
                AND committee_name_normalized LIKE UPPER('%{committee_name_escaped}%')
                """
        
        # Step 1: Get Schedule D expenses with stricter filtering
        logger.info("Executing optimized BigQuery queries...")
        schedule_d_query = f"""
        SELECT 
            committee_code as donor_committee_code,
            committee_name as donor_committee_name,
            committee_name_normalized as donor_committee_name_normalized,
            committee_type as donor_committee_type,
            candidate_name_normalized as donor_candidate_name,
            entity_name as recipient_name,
            entity_name_normalized as recipient_name_normalized,
            amount,
            transaction_date,
            purpose,
            report_year,
            data_source,
            folder_name,
            report_date
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE 1=1
            AND transaction_type = 'ScheduleD'
            AND report_year >= @min_year
            AND committee_type IN (
                'Political Action Committee',
                'Out of State Political Committee', 
                'Political Party Committee'
            )
            -- More specific purpose filtering for better precision
            AND (
                REGEXP_CONTAINS(LOWER(purpose), r'\\b(political|campaign)\\s+contribution\\b')
                OR REGEXP_CONTAINS(LOWER(purpose), r'\\bcontribution\\s+(to|for)\\b')
                OR REGEXP_CONTAINS(LOWER(purpose), r'\\bpac\\s+contribution\\b')
                OR REGEXP_CONTAINS(LOWER(purpose), r'\\b(primary|general)\\s+\\d{{4}}\\b')
                -- OR REGEXP_CONTAINS(LOWER(purpose), r'\\bin-kind\\s+(campaign\\s+)?contribution\\b')
                OR REGEXP_CONTAINS(LOWER(purpose), r'\\bfundraiser\\b')
                OR REGEXP_CONTAINS(LOWER(purpose), r'\\bstate\\s+committee\\s+contribution\\b')
                OR REGEXP_CONTAINS(LOWER(purpose), r'\\bcontribution\\b')
            )
            AND amount >= @min_amount  -- Only consider contributions $1,000 and above
            AND entity_name IS NOT NULL
            AND entity_name != ''
            AND LENGTH(entity_name) > 1  -- Filter out very short names
            {committee_filter}
        ORDER BY amount DESC, transaction_date DESC
        """
        
        # Step 2: Get candidate committees (unchanged - small dataset)
        candidate_committees_query = f"""
        SELECT DISTINCT
            committee_code,
            committee_name,
            committee_name_normalized,
            candidate_name,
            candidate_name_normalized
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE committee_type = 'Candidate Campaign Committee'
            AND committee_code IS NOT NULL
            AND committee_code != ''
            AND candidate_name IS NOT NULL
            AND candidate_name != ''
        """
        
        # Step 3: Get Schedule A receipts with pre-filtering
        schedule_a_query = f"""
        SELECT 
            committee_code as recipient_committee_code,
            committee_name as recipient_committee_name,
            committee_name_normalized as recipient_committee_name_normalized,
            candidate_name as recipient_candidate_name,
            candidate_name_normalized as recipient_candidate_name_normalized,
            entity_name as donor_name,
            entity_name_normalized as donor_name_normalized,
            amount,
            transaction_date,
            report_year,
            data_source,
            folder_name
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE 1=1
            AND transaction_type = 'ScheduleA'
            AND report_year >= @min_year
            -- All committee types: non-candidate committees (PACs etc.) file
            -- Schedule A too, so their receipts must be in the pool for
            -- NOT-A-CC recipients to match.
            AND amount >= @min_amount  -- Match the Schedule D filter
            AND entity_name IS NOT NULL
            AND entity_name != ''
            AND LENGTH(entity_name) > 5  -- Match the Schedule D filter
        """
        
        # Set up query parameters
        job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("min_year", "INT64", min_year),
            bigquery.ScalarQueryParameter("min_amount", "INT64", min_amount),
        ])
        
        # Execute queries in parallel (BigQuery handles this efficiently)
        logger.info("Getting filtered Schedule D political contributions...")
        schedule_d_df = client.query(schedule_d_query, job_config=job_config).to_dataframe()
        
        logger.info("Getting candidate campaign committees...")
        candidates_df = client.query(candidate_committees_query, job_config=job_config).to_dataframe()
        
        logger.info("Getting filtered Schedule A receipts...")
        schedule_a_df = client.query(schedule_a_query, job_config=job_config).to_dataframe()
        
        if schedule_d_df.empty:
            logger.info("No Schedule D political contributions found")
            return []
        
        logger.info(f"Found {len(schedule_d_df)} filtered Schedule D contributions")
        logger.info(f"Found {len(candidates_df)} candidate campaign committees")
        logger.info(f"Found {len(schedule_a_df)} filtered Schedule A receipts")

        # Load lookup tables for exact matching
        matcher.load_lookup_tables(client, project_id, dataset_id)

        # Process in batches for better memory management
        unmatched_contributions = []
        total_batches = len(schedule_d_df) // batch_size + (1 if len(schedule_d_df) % batch_size else 0)

        logger.info(f"Processing {len(schedule_d_df)} records in {total_batches} batches of {batch_size}")
        if test_mode:
            logger.info("Test mode: Will stop after finding 100 unmatched contributions")
        
        for batch_idx in range(total_batches):
            # Test mode: stop early if we have enough results
            if test_mode and len(unmatched_contributions) >= 100:
                logger.info(f"Test mode: Found 100 unmatched contributions, stopping early at batch {batch_idx + 1}")
                break
                
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(schedule_d_df))
            batch_df = schedule_d_df.iloc[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_idx + 1}/{total_batches}")
            
            # Step 1: Find matching committees for batch using lookup tables
            recipient_names = []

            for _, row in batch_df.iterrows():
                normalized_name = row.get('recipient_name_normalized', '')
                if pd.notna(normalized_name) and normalized_name.strip():
                    recipient_names.append(normalized_name.strip())
                else:
                    # Fallback to original name but log warning
                    original_name = row.get('recipient_name', '')
                    logger.warning(f"Using fallback for recipient without normalized name: '{original_name}'")
                    recipient_names.append(original_name)

            filing_years = [
                row['report_year'] if pd.notna(row['report_year'])
                else pd.to_datetime(row['transaction_date']).year if pd.notna(row['transaction_date'])
                else 2020
                for _, row in batch_df.iterrows()
            ]
            matched_committees = matcher.find_matching_committee_batch(recipient_names, filing_years)

            # Step 2: Prepare data for Schedule A matching
            batch_data = []
            valid_indices = []
            
            for idx, (_, d_row) in enumerate(batch_df.iterrows()):
                matched_committee = matched_committees[idx]
                if not matched_committee:
                    continue
                
                # Get relevant Schedule A subset for this committee
                committee_code = matched_committee['committee_code']
                schedule_a_subset = schedule_a_df[
                    schedule_a_df['recipient_committee_code'] == committee_code
                ]

                batch_data.append((d_row.to_dict(), matched_committee, schedule_a_subset))
                valid_indices.append(idx)

            # Step 3: Find matching Schedule A receipts
            if batch_data:
                match_results = matcher.find_matching_schedule_a_batch(batch_data)
                
                # Process all results and try alternate committees
                for i, (has_match, match_info) in enumerate(match_results):
                    original_idx = valid_indices[i]
                    d_row_dict, matched_committee, original_schedule_a_subset = batch_data[i]

                    # Only try alternate committee codes if this is a candidate (has candidate_name) and it's not blank
                    found_alternate_match = False
                    alternate_match_failure_reason = None
                    candidate_name = matched_committee.get('candidate_name_normalized', '')
                    original_committee_code = matched_committee['committee_code']

                    # Only check alternates for actual candidates (not PACs or other non-candidate committees)
                    if candidate_name and candidate_name.strip() and candidate_name != 'NOT A CC':
                        # Find all committee codes for this candidate using committee_mappings
                        all_candidate_committees = []
                        for norm_name, info in matcher.committee_mappings.items():
                            if info.get('candidate_name_normalized') == candidate_name:
                                all_candidate_committees.append({
                                    'committee_code': info['committee_code'],
                                    'committee_name_normalized': norm_name,
                                    'candidate_name_normalized': info['candidate_name_normalized']
                                })

                        # Try each alternate committee code
                        for alt_committee in all_candidate_committees:
                            alt_code = alt_committee['committee_code']
                            if alt_code != original_committee_code:  # Don't retry the same code
                                alt_schedule_a = schedule_a_df[
                                    schedule_a_df['recipient_committee_code'] == alt_code
                                ]

                                if not alt_schedule_a.empty:
                                    # Try matching with this alternate committee
                                    alt_batch_data = [(d_row_dict, alt_committee, alt_schedule_a)]
                                    alt_match_results = matcher.find_matching_schedule_a_batch(alt_batch_data)

                                    if alt_match_results and alt_match_results[0][0]:  # has_match is True
                                        found_alternate_match = True
                                        break
                                    else:
                                        # Store the failure reason from the alternate committee attempt
                                        if alt_match_results and len(alt_match_results) > 0:
                                            alternate_match_failure_reason = alt_match_results[0][1]

                    # Only add to unmatched if no match was found (original or alternate)
                    if not has_match and not found_alternate_match:

                        # Log debugging info for unmatched contributions
                        logger.debug(f"\n=== UNMATCHED CONTRIBUTION ===")
                        logger.debug(f"Donor: {d_row_dict['donor_committee_name']}")
                        logger.debug(f"Recipient: {d_row_dict['recipient_name']} -> {matched_committee['candidate_name_normalized']}")
                        logger.debug(f"Amount: ${d_row_dict['amount']}")
                        logger.debug(f"Date: {d_row_dict['transaction_date']}")
                        logger.debug(f"Purpose: {d_row_dict['purpose']}")
                        
                        if match_info:
                            if 'reason' in match_info:
                                logger.debug(f"Issue: {match_info['reason']}")
                                if 'committee_code' in match_info:
                                    logger.debug(f"Committee code: {match_info['committee_code']}")
                                    logger.debug(f"Committee name: {match_info.get('matched_committee_name', 'N/A')}")
                            else:
                                logger.debug(f"Schedule A candidates checked: {match_info['total_candidates']}")
                                logger.debug(f"Amount matches found: {match_info['amount_matches']}")
                                logger.debug(f"Date matches found: {match_info['date_matches']}")
                                if match_info['best_candidate']:
                                    logger.debug(f"Best name match: '{match_info['best_candidate']}' (score: {match_info['best_score']})")
                                    donor_norm = d_row_dict.get('donor_committee_name_normalized', matcher.clean_committee_name(d_row_dict['donor_committee_name']))
                                    logger.debug(f"  Normalized: '{donor_norm}' vs '{match_info['best_candidate']}'")
                                logger.debug(f"Failure reasons: {', '.join(match_info['reasons']) if match_info['reasons'] else 'TRULY, NO MATCH!'}")
                        
                        logger.debug(f"Matched committee code: {matched_committee['committee_code']}")
                        logger.debug(f"Matched committee name: {matched_committee['committee_name_normalized']}")
                        
                        contribution_record = d_row_dict.copy()
                        contribution_record['matched_committee_code'] = matched_committee['committee_code']
                        contribution_record['matched_committee_name_normalized'] = matched_committee['committee_name_normalized']
                        contribution_record['matched_candidate_name'] = matched_committee['candidate_name_normalized']

                        # Add failure reason to output - prefer alternate committee failure reason if available
                        failure_info = alternate_match_failure_reason if alternate_match_failure_reason else match_info

                        if failure_info and 'reason' in failure_info:
                            contribution_record['failure_reason'] = failure_info['reason']
                        elif failure_info and 'reasons' in failure_info:
                            contribution_record['failure_reason'] = '; '.join(failure_info['reasons']) if failure_info['reasons'] else 'no_match_found'
                            if failure_info.get('best_score', 0) > 0:
                                contribution_record['best_name_match_score'] = failure_info['best_score']
                                contribution_record['best_name_match_candidate'] = failure_info.get('best_candidate', '')
                        else:
                            contribution_record['failure_reason'] = 'unknown'
                        
                        unmatched_contributions.append(contribution_record)
                        
                        # Test mode: stop if we reach 100
                        if test_mode and len(unmatched_contributions) >= 100:
                            break
        
        logger.info(f"Found {len(unmatched_contributions)} unmatched contributions")
        return unmatched_contributions

    except Exception as e:
        logger.error(f"Error in optimized analysis: {e}")
        return []


def print_unmatched_summary_optimized(results: List[Dict[str, Any]]):
    """Print enhanced summary statistics about unmatched contributions."""
    if not results:
        print("✅ No unmatched contributions found!")
        return
    
    df = pd.DataFrame(results)
    
    print(f"\n❌ Unmatched Contributions Summary (Optimized)")
    print(f"{'=' * 70}")
    print(f"Total unmatched contributions: {len(df)}")
    print(f"Total unmatched amount: ${df['amount'].sum():,.2f}")
    print(f"Average contribution amount: ${df['amount'].mean():,.2f}")
    print(f"Median contribution amount: ${df['amount'].median():.2f}")
    print(f"Date range: {df['transaction_date'].min()} to {df['transaction_date'].max()}")
    
    # Amount distribution
    print(f"\n💰 Amount Distribution:")
    print(f"  $25-$100:     {len(df[(df['amount'] >= 25) & (df['amount'] < 100)])} contributions")
    print(f"  $100-$500:    {len(df[(df['amount'] >= 100) & (df['amount'] < 500)])} contributions")
    print(f"  $500-$1,000:  {len(df[(df['amount'] >= 500) & (df['amount'] < 1000)])} contributions")
    print(f"  $1,000-$5,000: {len(df[(df['amount'] >= 1000) & (df['amount'] < 5000)])} contributions")
    print(f"  $5,000+:      {len(df[df['amount'] >= 5000])} contributions")
    
    # Top donors by count and amount
    print(f"\n🏢 Top 10 Donors by Total Unmatched Amount:")
    donor_stats = df.groupby('donor_committee_name_normalized').agg({
        'amount': ['sum', 'count'],
        'matched_candidate_name': 'nunique'
    }).round(2)
    donor_stats.columns = ['total_amount', 'count', 'unique_recipients']
    donor_stats = donor_stats.sort_values('total_amount', ascending=False).head(10)
    
    for donor_name, row in donor_stats.iterrows():
        print(f"  ${row['total_amount']:>10,.2f} - {donor_name[:50]} ({row['count']} contributions to {row['unique_recipients']} recipients)")
    
    # Top recipients by amount
    print(f"\n🎯 Top 10 Recipients by Total Unmatched Amount:")
    recipient_stats = df.groupby(['matched_candidate_name', 'matched_committee_name_normalized']).agg({
        'amount': ['sum', 'count'],
        'donor_committee_name_normalized': 'nunique'
    }).round(2)
    recipient_stats.columns = ['total_amount', 'count', 'unique_donors']
    recipient_stats = recipient_stats.sort_values('total_amount', ascending=False).head(10)
    
    for (candidate, committee), row in recipient_stats.iterrows():
        committee_short = committee[:45] + '...' if len(committee) > 45 else committee
        print(f"  ${row['total_amount']:>10,.2f} - {candidate} ({committee_short}) ({row['count']} from {row['unique_donors']} donors)")


def main():
    parser = argparse.ArgumentParser(description='Optimized analysis of unmatched political contributions')
    parser.add_argument('--project-id', type=str, required=True,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='virginia_elections',
                       help='BigQuery dataset name (default: virginia_elections)')
    parser.add_argument('--table', type=str, default='cf_clean',
                       help='BigQuery table name (default: cf_clean)')
    parser.add_argument('--output-csv', type=str, required=True,
                       help='Path to output CSV file (required)')
    parser.add_argument('--min-year', type=int, default=2018,
                       help='Minimum year to include in results (default: 2018)')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Batch size for processing (default: 1000)')
    parser.add_argument('--show-summary', action='store_true',
                       help='Display summary statistics')
    parser.add_argument('--test-mode', action='store_true',
                       help='Test mode: stop after finding 100 unmatched contributions')
    parser.add_argument('--committee-only', type=str, default=None,
                       help='Only analyze contributions from a specific committee (e.g., "DOMINION ENERGY" or "CLEAN VA FUND")')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging to see matching details')
    parser.add_argument('--min-amount', type=int, default=1000,
                       help='Amount minimum flagged (default: 1000)')

    args = parser.parse_args()
    
    # Set logging level based on debug flag
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug logging enabled")
    
    start_time = datetime.now()
    
    try:
        # Get unmatched contributions with optimizations
        results = get_unmatched_contributions_optimized(
            project_id=args.project_id,
            dataset_id=args.dataset,
            table_id=args.table,
            min_year=args.min_year,
            batch_size=args.batch_size,
            test_mode=args.test_mode,
            committee_only=args.committee_only,
            min_amount=args.min_amount
        )

        # Convert to DataFrame
        df = pd.DataFrame(results) if results else pd.DataFrame()

        # Save to CSV
        df.to_csv(args.output_csv, index=False)

        elapsed_time = datetime.now() - start_time

        if results:
            logger.info(f"Successfully saved {len(results)} unmatched contributions to {args.output_csv}")
        else:
            logger.info(f"No unmatched contributions found - empty CSV saved to {args.output_csv}")
        
        logger.info(f"Total execution time: {elapsed_time}")
        
        # Show summary if requested
        if args.show_summary:
            print_unmatched_summary_optimized(results)
        
        return 0
        
    except Exception as e:
        logger.error(f"Optimized script failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())