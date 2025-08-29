import pandas as pd

#-------------------CITIES------------------------------------
# Load your CSV
df = pd.read_csv("all-city2018-2024-edited.csv")

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

# Save to CSV or print
agg_df.to_csv("election_cost_summary_cities.csv", index=False)
print(agg_df)

#-------------------COUNTIES------------------------------------
df = pd.read_csv("county-disbursements-2020-edited.csv")

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

# Save to CSV or print
agg_df.to_csv("election_cost_summary_counties.csv", index=False)
print(agg_df)