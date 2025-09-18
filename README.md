# Virginia Campaign Finance Database

A queryable database system for Virginia campaign finance data, with automated download and natural language query interface.

## 🚀 Key Features

- **🤖 Automated Download**: Selenium-based scraper bypasses access restrictions
- **💬 Natural Language Queries**: Ask questions in plain English
- **🔍 Smart Name Matching**: Handles candidate name variations automatically  
- **📊 Comprehensive Data**: Covers years 1999-2025 from official Virginia sources
- **⚡ Fast Queries**: SQLite database with optimized indexes
- **🎯 User Friendly**: No programming experience required

## Data Sources

**Primary**: https://apps.elections.virginia.gov/SBE_CSV/CF/
- Old format: 1999-2011 (by year)
- New format: 2012-2025 (by year/month)

**Secondary**: https://cfreports.elections.virginia.gov/

## 🎯 Quick Start

### Option 0: Just run the processors for local election data (Lazy)
1. Set up your environment. Will need to ask for serviceaccount access.
``` bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/key.json" 
```
2. Run the processor you want

```bash
#Process Schedules ABCDFI, it will upload to BigQuery under campaign_finance in virginia_elections
python3 ScheduleABCDFI_processor.py --project-id va-campaign-finance 
#Options --mode [test]|[production] --skip-old-folders --folders-after [YEAR]
```

``` bash
#Process Schedules H, it will upload to BigQuery under schedule_h in virginia_elections
python3 scheduleh_processor.py --project-id va-campaign-finance 
#Options --mode [test]|[production] --folders-after [YEAR]
```

2b. Run the cleaner to clean amendments.
```bash
#on the Schedules ABCDFI processor
python3 amendment_processor.py --project-id va-campaign-finance --mode clean-only
#Options --mode [clean-only]|[full]|[local] and --raw-table [table_name] --clean-table [table_name_clean]
```
```bash
python3 amendment_processor.py --project-id va-campaign-finance --mode clean-only --raw-table schedule_h --clean-table schedule_h_clean --processor-script scheduleh_processor.py
#Options --mode [clean-only]|[full]|[local] and --raw-table [table_name] --clean-table [table_name_clean]
```


3. Run the analysis on local elections to get a list of candidates

``` bash
#Analyze Schedules H production mode for cities
python3 scheduleh_analysis_cities.py --project-id va-campaign-finance --output-csv ./analysis_results/cities_year.csv
#Options: --cities [CITY_ONE] [CITY_TWO]...[CITY_N]
# Default is "blacksburg", "leesburg", "winchester", "alexandria", "arlington", "richmond", "lynchburg", "newport news", "virginia beach", "roanoke"
```
``` bash
#Analyze Schedules H prodcution mode for counties 
python3 scheduleh_analysis_counties.py --project-id va-campaign-finance --output-csv ./analysis_results/counties_year.csv
#Options: --counties [COUNTY_ONE] [COUNTY_TWO]...[COUNTY_N]
#Default is "loudoun", "prince william"
```
4. Use that list of candidates to aggregate local finance data (total cost, max cost, average cost, number of candidates)

```bash
#Analyze Aggregate Data for Cities & Counties
python3 aggregate-local-financing.py --cities-csv ./analysis_results/cities_year.csv --counties-csv ./analysis_results/counties_year.csv --cities-csv-output ./analysis_results/agg_cities_year.csv --counties-csv-output ./analysis_results/agg_counties_year.csv
```

```bash
#Analyze Aggregate Data Example
python3 aggregate-local-financing.py --cities-csv ./analysis_results/cities_2018.csv --counties-csv ./analysis_results/counties_2018.csv --cities-csv-output ./analysis_results/agg_cities_2018.csv --counties-csv-output ./analysis_results/agg_counties_2018.csv
```

3. Run the analysis on campaign finance for schedule H
```bash
python3 scheduleh_latest_balances.py --project-id va-campaign-finance --output-csv ./analysis_results/latest_balances_2015.csv --min-year 2015
```

```bash
python3 scheduleh_balance_continuity_check.py --project-id va-campaign-finance --output-csv ./analysis_results/continuity_check_2015.csv --min-year 2015
```

3. Run it on unmatched_contributions
```bash
python3 ./python_analysis_scripts/unmatched_contributions_analysis_optimized.py --project-id va-campaign-finance --output-csv dom.csv --min-year 2015 --committee-only "CLEAN VA FUND"
#Options --test-mode --committee-only [COMITTEE NAME] --debug
```
### Option 1: Complete Setup (COMING SOON)

```bash
# 1. Install dependencies
npm install

# 2. Download recent data and build database (automated)
npm run full-build

# 3. Start querying!
npm start
```

### Option 2: Step by Step (COMING SOON)
```bash
# 1. Install dependencies
npm install

# 2. Download data (choose one):
npm run download-recent    # Last 2 years (recommended)
npm run download-all       # All data 1999-2025 (slow)
npm run download-test      # Just 2 folders for testing

# 3. Validate downloads (optional)
npm run validate

# 4. Build database from downloaded files
npm run build --skip-download

# 5. Start querying
npm start
```

## 💬 Natural Language Queries

Ask questions in plain English! The system understands natural language and converts it to database queries.

### Command Line (Direct)
```bash
# Just ask your question directly:
npm start "How much did Glenn Youngkin spend in 2024?"
npm start "List candidates in Arlington County Board 2024 election"
npm start "Top Dominion Energy recipients in 2023"
npm start "Who spent the most money in Virginia in 2024?"
```

### Interactive Mode
```bash
npm start  # Opens menu interface
# Choose option 7: "Natural language query"
# Then ask: "How much did each candidate spend in the Arlington County Board primary election in 2024?"
```

### Example Questions You Can Ask
- **Candidate Spending**: "How much did [candidate] spend in [year]?"
- **Election Analysis**: "List spending in [location] [office] [year] election"  
- **Contributor Analysis**: "Who received money from [company/person] in [year]?"
- **Top Lists**: "Highest spending candidates in [year]"
- **Specific Transactions**: "How much did [candidate] receive from [entity]?"

### Pre-Built Examples (Still Available)
```bash
npm start example arlington   # Arlington County Board 2024
npm start example dominion    # Dominion Energy contributions  
npm start example youngkin    # Glenn Youngkin - Dominion money
npm start example highest     # Highest spending candidates
```

### Programmatic Usage

```javascript
const QueryEngine = require('./src/query-engine');

const query = new QueryEngine();
await query.connect();

// Get candidate spending by office/district
const results = await query.getCandidateSpending({
    year: 2024,
    office: 'County Board',
    district: 'Arlington',
    topN: 10
});

// Search for contributions from specific entity
const dominion = await query.searchByEntity('Dominion Energy', {
    year: 2024,
    topN: 20
});

// Custom aggregation
const custom = await query.sumBy({
    groupBy: ['candidate_name_normalized'],
    year: 2024,
    filters: {
        office_sought: 'Governor',
        entity_name_normalized: 'Dominion'
    },
    topN: 5
});

await query.close();
```

## Build Options

```bash
# Full build (download + process)
npm run build

# Skip download, process existing data
node src/build-database.js --skip-download

# Clear existing data and rebuild  
node src/build-database.js --clear
```

## Query API

### Core Methods

#### `sumBy(options)`
Flexible aggregation with filtering:
```javascript
{
    groupBy: ['candidate_name_normalized'],  // Group by these columns
    year: 2024,                             // Filter by year
    years: [2023, 2024],                    // Or multiple years
    topN: 10,                               // Limit results
    filters: {                              // Additional filters
        office_sought: 'Governor',
        entity_name_normalized: 'Dominion'
    },
    scheduleTypes: ['ScheduleA'],           // Contribution types
    orderBy: 'total_amount',                // Sort column
    orderDirection: 'DESC'                  // Sort direction
}
```

#### `getCandidateSpending(options)`
Analyze candidate expenditures:
```javascript
{
    year: 2024,
    office: 'House of Delegates',
    district: '31',
    topN: 10,
    includeCommittees: false
}
```

#### `getTopContributors(options)`
Find major contributors:
```javascript
{
    year: 2024,
    candidateName: 'Glenn Youngkin',
    topN: 20,
    minAmount: 1000
}
```

#### `searchByEntity(entityName, options)`
Find all recipients from an entity:
```javascript
await query.searchByEntity('Dominion Energy', {
    year: 2024,
    topN: 15
});
```

### Schedule Types

The system processes these transaction schedules:

- **ScheduleA**: Monetary contributions received
- **ScheduleB**: In-kind contributions received  
- **ScheduleC**: Other receipts
- **ScheduleD**: Expenditures for goods/services
- **ScheduleF**: Debts and obligations
- **ScheduleI**: Surplus funds disposition

Summary schedules (G, H) and loans (E) are excluded to avoid double-counting.

## Name Normalization

The system automatically normalizes candidate and entity names:

- Converts to uppercase
- Removes common committee suffixes ("FOR DELEGATE", "COMMITTEE", etc.)
- Standardizes punctuation
- Handles name variations ("Leftwich for Delegate - Jay" → "JAY LEFTWICH")

## Database Schema

### Main Tables

- **transactions**: All financial transactions with normalized names
- **reports**: Report metadata 
- **candidates**: Normalized candidate information
- **entities**: Contributors and payees

### Key Fields

- `candidate_name_normalized`: Standardized candidate names
- `entity_name_normalized`: Standardized contributor/payee names  
- `amount`: Transaction amount
- `schedule_type`: Transaction type (ScheduleA, ScheduleB, etc.)
- `report_year`: Election year
- `office_sought`: Office being sought
- `district`: Electoral district

## Development

### Project Structure
```
src/
├── index.js              # Main query interface
├── build-database.js     # Database builder
├── data-fetcher.js       # Downloads CSV files
├── data-processor.js     # Processes and loads data
├── database.js           # SQLite database wrapper
├── query-engine.js       # Query API
└── name-normalizer.js    # Name standardization


```

### Adding New Queries

1. Add method to `QueryEngine` class in `src/query-engine.js`
2. Add example in `src/index.js` 
3. Update README with usage example

### Data Updates

Re-run the build process to fetch latest data:
```bash
npm run build
```

The system will download any new files and update the database.

## Troubleshooting

### Common Issues

**No data found for recent years**: Virginia may not have released the data yet, or it may be in a different format.

**Name matching issues**: The normalization process handles many variations, but some edge cases may need manual review.

**Large downloads**: The full dataset is substantial. Use `--skip-download` to work with existing data during development.
