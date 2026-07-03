# Virginia Campaign Finance Database

A Python + Google BigQuery pipeline for scraping, parsing, and analyzing Virginia campaign finance data from the State Board of Elections.

## Overview

This project automatically:
1. **Scrapes** CSV files from Virginia SBE (`apps.elections.virginia.gov/SBE_CSV/CF/`)
2. **Normalizes** candidate and entity names using domain-specific rules
3. **Loads** cleaned data into Google BigQuery for analysis and querying
4. **Analyzes** patterns like unmatched contributions, balance continuity, and local spending

The legacy Node/SQLite prototype has been moved to `legacy/` and is no longer in use.

## Data Pipeline

### Bronze Layer (Raw CSVs)
- Scraper: `ingest/download_to_gcs.py` (plain script, runnable locally or as a scheduled job)
- Output: Raw CSVs uploaded to GCS bucket `va-cf-local/raw_data/`
- Source: https://apps.elections.virginia.gov/SBE_CSV/CF/

### Silver Layer (Parsed & Normalized)
- **Schedule A–F, I Transactions**: `processors/ScheduleABCDFI_processor.py`
  - Parses CSVs with proper quote/escape handling
  - Applies normalization from `functions/name_normalization.py`
  - Loads to BigQuery table `virginia_elections.campaign_finance`
  
- **Schedule H (Totals)**: `processors/scheduleh_processor.py`
  - Loads to `virginia_elections.schedule_h`

- **Deduplication**: `processors/amendment_processor.py`
  - Removes duplicate amended reports (keeps latest per committee/due_date)
  - Outputs to `cf_clean` and `schedule_h_clean` tables

### Analysis Scripts
Located in `python_analysis_scripts/`:

- **Unmatched Contributions** (`unmatched_contributions_analysis_optimized.py`): Identifies Schedule D ↔ Schedule A mismatches
- **Balance Continuity** (`scheduleh_balance_continuity_check.py`): Flags reports where starting balance ≠ prior ending balance
- **Latest Balances** (`scheduleh_latest_balances.py`): Extracts most recent Schedule H for each committee
- **Local Spending** (`scheduleh_analysis_cities.py`, `scheduleh_analysis_counties.py`): Aggregates spending by geography
- **Mapping Tables** (`create_mapping_tables.py`): Builds lookup tables for normalized names and committee codes

## Setup

### Requirements
- Python 3.8+
- Google Cloud credentials (BigQuery + Cloud Storage access)
- See `requirements.txt` for the full dependency list

### Environment
```bash
# Set up credentials (service account key)
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"

# Install dependencies
pip install -r requirements.txt
```

## Usage

### 1. Download Raw Data to GCS
```bash
python3 ingest/download_to_gcs.py
```
Scrapes the latest CSVs from the VA SBE site into `gs://va-cf-local/raw_data/`.

### 2. Process into BigQuery

**Schedule A–F, I (Individual Transactions):**
```bash
python3 processors/ScheduleABCDFI_processor.py \
  --project-id va-campaign-finance \
  --mode production \
  --folders-after 2015
```

Options:
- `--mode [test|production]`: test mode is default
- `--skip-old-folders`: skip 1999–2011 data
- `--folders-after YEAR`: process only folders from this year onward

**Schedule H (Committee Totals):**
```bash
python3 processors/scheduleh_processor.py \
  --project-id va-campaign-finance
```

**Deduplication (Amendment Cleanup):**
```bash
# Default: deduplicate existing tables
python3 processors/amendment_processor.py \
  --project-id va-campaign-finance \
  --mode clean-only

# Or: full reprocess from scratch
python3 processors/amendment_processor.py \
  --project-id va-campaign-finance \
  --mode full \
  --processor-script ScheduleABCDFI_processor.py
```

Options:
- `--mode [clean-only|full]`: clean-only dedupes existing tables, full reprocesses
- `--raw-table TABLE_NAME`: source table (default: `campaign_finance`)
- `--clean-table TABLE_NAME`: output table (default: `cf_clean`)
- `--processor-script SCRIPT`: processor to use for full reprocess

### 3. Run Analyses

**Unmatched Contributions (Schedule D → Schedule A gaps):**
```bash
python3 python_analysis_scripts/unmatched_contributions_analysis_optimized.py \
  --project-id va-campaign-finance \
  --output-csv results.csv \
  --min-year 2015 \
  --committee-only "DOMINION ENERGY"
```

**Balance Continuity Check:**
```bash
python3 python_analysis_scripts/scheduleh_balance_continuity_check.py \
  --project-id va-campaign-finance \
  --output-csv continuity_check.csv \
  --min-year 2015
```

**Latest Balances:**
```bash
python3 python_analysis_scripts/scheduleh_latest_balances.py \
  --project-id va-campaign-finance \
  --output-csv latest_balances.csv \
  --min-year 2015
```

**Local Spending (Cities):**
```bash
python3 python_analysis_scripts/scheduleh_analysis_cities.py \
  --project-id va-campaign-finance \
  --output-csv cities_spending.csv \
  --cities "Richmond" "Arlington" "Alexandria"
```

**Local Spending (Counties):**
```bash
python3 python_analysis_scripts/scheduleh_analysis_counties.py \
  --project-id va-campaign-finance \
  --output-csv counties_spending.csv \
  --counties "Loudoun" "Prince William"
```

**Create Mapping Tables:**
```bash
python3 python_analysis_scripts/create_mapping_tables.py \
  --project-id va-campaign-finance \
  --dry-run
```

**Test Normalization Rules:**
```bash
python3 test_normalization_on_table.py \
  --project-id va-campaign-finance \
  --output-table normalization_test_results
```

## Name Normalization

The `functions/name_normalization.py` module applies consistent transformations to candidate and entity names:
- Uppercase conversion
- Removes common suffixes ("FOR DELEGATE", "COMMITTEE", etc.)
- Standardizes punctuation and spacing
- Handles known aliases (e.g., "Jay Leftwich for Delegate" → "JAY LEFTWICH")

The normalization rules are maintained as hard-coded patterns refined over months of iteration. New rules are added via the `test_normalization_on_table.py` workflow.

## Data Schema (BigQuery)

### Main Tables
- `virginia_elections.campaign_finance`: Schedule A–F, I transactions (raw parsed data)
- `virginia_elections.cf_clean`: Deduped version of above
- `virginia_elections.schedule_h`: Committee totals (raw)
- `virginia_elections.schedule_h_clean`: Deduped committee totals

### Key Columns
- `committee_code`: Unique committee identifier
- `committee_name` / `committee_name_normalized`: Raw and normalized names
- `candidate_name` / `candidate_name_normalized`: Candidate names
- `entity_name` / `entity_name_normalized`: Donor/vendor names
- `amount`: Transaction amount
- `contribution_date`: Date of transaction
- `schedule_type`: Schedule A, B, C, D, F, I
- `report_year`: Election year

## Known Issues

See `PLAN.md` section 2 for detailed tech debt inventory. High-priority items:
1. CSV parser can corrupt data (embedded quotes/newlines handled incorrectly)
2. Stale normalized columns when normalization rules improve (needs cheap re-normalization pipeline)
3. Amendment dedup uses fuzzy matching with manual thresholds (should be deterministic SQL)

## Development

### Project Structure
```
.
├── ingest/
│   └── download_to_gcs.py                # Scraper (bronze ingestion)
├── processors/
│   ├── ScheduleABCDFI_processor.py       # Main transaction parser
│   ├── scheduleh_processor.py            # Schedule H parser
│   └── amendment_processor.py            # Deduplication
├── python_analysis_scripts/              # Analysis tools
├── functions/
│   └── name_normalization.py             # Name normalization rules
├── test_normalization_on_table.py        # Rule validation
└── PLAN.md                               # Roadmap (Phases 0–5)
```

### Roadmap

See `PLAN.md` for the full 5-phase roadmap:
- **Phase 0**: Repo hygiene (this branch)
- **Phase 1**: Fix CSV parser corruption
- **Phase 2**: Cheap re-normalization (no full reprocess needed)
- **Phase 3**: Gold layer (SQL transforms for analytics)
- **Phase 4**: Streamlit MVP (simple search + SQL editor interface)
- **Phase 5**: Scheduled refreshes (monthly auto-update)

## Contributing

All normalization rule updates must pass `test_normalization_on_table.py` before landing. Hard-coded fixes for specific known-bad CSVs are acceptable and encouraged.

## License

ISC
