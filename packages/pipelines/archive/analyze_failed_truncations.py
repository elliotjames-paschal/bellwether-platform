#!/usr/bin/env python3
"""
Analyze which markets failed to truncate
"""

import pandas as pd

BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"

markets_df = pd.read_csv(f"{DATA_DIR}/us_electoral_markets_for_truncation.csv")

# Markets with missing office/location/year
missing_data = markets_df[
    markets_df['office'].isna() |
    markets_df['location'].isna() |
    markets_df['election_year'].isna()
]

print(f"Total markets needing truncation: {len(markets_df)}")
print(f"Markets with missing office/location/year: {len(missing_data)}")
print(f"Markets with complete data: {len(markets_df) - len(missing_data)}")

print("\n" + "="*80)
print("SAMPLE MARKETS WITH MISSING DATA (first 20):")
print("="*80)
for idx, row in missing_data.head(20).iterrows():
    print(f"\nMarket ID: {row['market_id']}")
    print(f"  Question: {row['question']}")
    print(f"  Office: {row['office']}")
    print(f"  Location: {row['location']}")
    print(f"  Year: {row['election_year']}")
    print(f"  Is Primary: {row['is_primary']}")

# Group by pattern
print("\n" + "="*80)
print("BREAKDOWN BY DATA COMPLETENESS:")
print("="*80)

has_year = markets_df[markets_df['election_year'].notna() & (markets_df['office'].isna() | markets_df['location'].isna())]
no_year = markets_df[markets_df['election_year'].isna()]

print(f"\nHas year but missing office/location: {len(has_year)}")
print(f"Missing year (and likely office/location): {len(no_year)}")
