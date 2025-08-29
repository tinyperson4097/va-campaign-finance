"""
Virginia Campaign Finance Filing Deadlines by Year
This module contains filing period data for determining if reports were filed on time.
"""

# Filing periods for 2024
FILING_PERIODS_2024 = [
    # --- Off-Cycle Reports for Committees Not on the Ballot ---
    {
        'filingPeriodStart': '2024-01-01',
        'filingPeriodEnd': '2024-06-30',
        'filingPeriodDeadline': '2024-07-15',
        'onCycle': False
    },
    {
        'filingPeriodStart': '2024-07-01',
        'filingPeriodEnd': '2024-12-31',
        'filingPeriodDeadline': '2025-01-15',
        'onCycle': False
    },
    
    # --- On-Cycle Reports for Candidates on the November 5, 2024 Ballot ---
    {
        'filingPeriodStart': '2024-01-01',
        'filingPeriodEnd': '2024-03-31',
        'filingPeriodDeadline': '2024-04-15',
        'onCycle': True
    },
    {
        'filingPeriodStart': '2024-04-01',
        'filingPeriodEnd': '2024-06-06',
        'filingPeriodDeadline': '2024-06-10',
        'onCycle': True
    },
    {
        'filingPeriodStart': '2024-06-07',
        'filingPeriodEnd': '2024-06-30',
        'filingPeriodDeadline': '2024-07-15',
        'onCycle': True
    },
    {
        'filingPeriodStart': '2024-07-01',
        'filingPeriodEnd': '2024-08-31',
        'filingPeriodDeadline': '2024-09-16',
        'onCycle': True
    },
    {
        'filingPeriodStart': '2024-09-01',
        'filingPeriodEnd': '2024-09-30',
        'filingPeriodDeadline': '2024-10-15',
        'onCycle': True
    },
    {
        'filingPeriodStart': '2024-10-01',
        'filingPeriodEnd': '2024-10-24',
        'filingPeriodDeadline': '2024-10-28',
        'onCycle': True
    },
    {
        'filingPeriodStart': '2024-10-25',
        'filingPeriodEnd': '2024-11-28',
        'filingPeriodDeadline': '2024-12-05',
        'onCycle': True
    },
    {
        'filingPeriodStart': '2024-11-29',
        'filingPeriodEnd': '2024-12-31',
        'filingPeriodDeadline': '2025-01-15',
        'onCycle': True
    },

    # --- Pre-Election Large Contribution Reports ---
    {
        'filingPeriodStart': '2024-06-07',
        'filingPeriodEnd': '2024-06-17',
        'filingPeriodDeadline': '2024-06-17',
        'onCycle': True
    },
    {
        'filingPeriodStart': '2024-10-25',
        'filingPeriodEnd': '2024-11-04',
        'filingPeriodDeadline': '2024-11-04',
        'onCycle': True
    }
]

# Placeholder filing periods for other years (using generic semi-annual pattern)
# These would need to be researched and populated with actual historical data
def _generate_generic_filing_periods(year):
    """Generate generic filing periods for years without specific data."""
    return [
        {
            'filingPeriodStart': f'{year}-01-01',
            'filingPeriodEnd': f'{year}-06-30',
            'filingPeriodDeadline': f'{year}-07-15',
            'onCycle': False
        },
        {
            'filingPeriodStart': f'{year}-07-01',
            'filingPeriodEnd': f'{year}-12-31',
            'filingPeriodDeadline': f'{year + 1}-01-15',
            'onCycle': False
        }
    ]

# Dictionary mapping years to their filing periods
FILING_PERIODS_BY_YEAR = {
    2024: FILING_PERIODS_2024,
}

# Generate generic periods for other years (1999-2025)
for year in range(1999, 2026):
    if year not in FILING_PERIODS_BY_YEAR:
        FILING_PERIODS_BY_YEAR[year] = _generate_generic_filing_periods(year)

def get_filing_periods_for_year(year):
    """
    Get filing periods for a specific year.
    
    Args:
        year (int): The year to get filing periods for
        
    Returns:
        list: List of filing period dictionaries for the year
    """
    return FILING_PERIODS_BY_YEAR.get(year, _generate_generic_filing_periods(year))