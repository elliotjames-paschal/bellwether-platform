#!/usr/bin/env python3
"""
Fix House and Senate election years using API ticker probing
Also adds correct ticker column
"""

import pandas as pd
import json
import os
import time
import requests
from datetime import datetime

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
INPUT_FILE = f"{DATA_DIR}/kalshi_official_with_electoral_details_FULL_with_years.csv"
OUTPUT_FILE = f"{DATA_DIR}/kalshi_official_with_corrected_house_senate.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/ticker_probing_checkpoint.json"

# Configuration
DELAY_BETWEEN_REQUESTS = 0.3  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 5

# State code mapping
STATE_CODES = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY'
}

print("=" * 80)
print("FIXING HOUSE & SENATE YEARS WITH TICKER PROBING")
print("=" * 80)

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

df = pd.read_csv(INPUT_FILE)
print(f"✓ Loaded {len(df):,} total markets")

# Add ticker column if it doesn't exist
if 'ticker' not in df.columns:
    df['ticker'] = None

# Filter to US House and Senate
us_elections = df[df['country'] == 'United States'].copy()
house_senate = us_elections[us_elections['office'].isin(['House', 'Senate'])].copy()

print(f"✓ US elections: {len(us_elections):,}")
print(f"✓ House + Senate: {len(house_senate):,}")

# Load checkpoint
processed_indices = set()
ticker_data = {}

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint_data = json.load(f)
        processed_indices = set(checkpoint_data.get('processed_indices', []))
        ticker_data = checkpoint_data.get('ticker_data', {})
    print(f"✓ Loaded checkpoint: {len(processed_indices):,} markets already processed")

# Get remaining markets
remaining = house_senate[~house_senate.index.isin(processed_indices)].copy()

print(f"\nTotal House + Senate: {len(house_senate):,}")
print(f"Already processed: {len(processed_indices):,}")
print(f"Remaining: {len(remaining):,}")

def probe_ticker(office, location, current_year):
    """
    Probe API to find correct year and ticker
    Returns (year, ticker, status)
    """

    # Construct ticker pattern based on office
    if office == 'House':
        # Parse district from location (e.g., "CA-9" or "AK-AL")
        if pd.isna(location):
            return None, None, "no_location"

        # Extract state and district
        parts = str(location).split('-')
        if len(parts) != 2:
            return None, None, "invalid_location_format"

        state_code = parts[0]
        district = parts[1]

        # Remove leading zeros from district (CA-01 -> CA1)
        if district != 'AL':  # Keep AL for at-large
            district = str(int(district)) if district.isdigit() else district

        ticker_base = f"HOUSE{state_code}{district}"

    elif office == 'Senate':
        # Get state code from location
        if pd.isna(location):
            return None, None, "no_location"

        state_name = str(location).strip()
        state_code = STATE_CODES.get(state_name)

        if not state_code:
            return None, None, "unknown_state"

        ticker_base = f"SENATE{state_code}"

    else:
        return None, None, "unsupported_office"

    # Try years 2024-2030, prioritizing current_year if it exists
    years_to_try = []
    if pd.notna(current_year):
        years_to_try.append(int(current_year))

    for year in range(2024, 2031):
        if year not in years_to_try:
            years_to_try.append(year)

    # Try both parties
    for year in years_to_try:
        year_code = str(year)[2:]  # 2026 -> 26

        for party in ['R', 'D']:
            ticker = f"{ticker_base}-{year_code}-{party}"
            url = f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}"

            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    # Found it!
                    return year, ticker, "success"
            except:
                pass

            time.sleep(0.1)  # Small delay between attempts

    return None, None, "not_found"

# Process markets
if len(remaining) == 0:
    print("\n✓ All House + Senate markets already processed!")
else:
    print(f"\n{'=' * 80}")
    print("PROBING TICKERS")
    print(f"{'=' * 80}")

    total_markets = len(remaining)
    processed_count = 0
    success_count = 0
    failed_count = 0

    start_time = time.time()

    for idx, row in remaining.iterrows():
        processed_count += 1
        office = row['office']
        location = row['location']
        current_year = row['election_year']
        title = row['title']

        print(f"\n[{processed_count}/{total_markets}] {office} {location}")
        print(f"  Current year: {int(current_year) if pd.notna(current_year) else 'N/A'}")

        # Calculate progress
        progress_pct = (processed_count / total_markets) * 100
        elapsed_time = time.time() - start_time
        if processed_count > 1:
            avg_time_per_market = elapsed_time / (processed_count - 1)
            remaining_markets = total_markets - processed_count
            est_time_remaining = avg_time_per_market * remaining_markets
            est_minutes = int(est_time_remaining / 60)
            est_seconds = int(est_time_remaining % 60)
            print(f"  Progress: {progress_pct:.1f}% | Est. time remaining: {est_minutes}m {est_seconds}s")

        # Probe for correct ticker
        year, ticker, status = probe_ticker(office, location, current_year)

        # Store result
        ticker_data[str(idx)] = {
            'year': year,
            'ticker': ticker,
            'status': status,
            'old_year': current_year if pd.notna(current_year) else None
        }
        processed_indices.add(idx)

        # Log result
        if status == "success":
            if pd.notna(current_year) and int(current_year) != year:
                print(f"  ✓ Year corrected: {int(current_year)} → {year}")
            else:
                print(f"  ✓ Year confirmed: {year}")
            print(f"  ✓ Ticker: {ticker}")
            success_count += 1
        else:
            print(f"  ⚠ {status}")
            failed_count += 1

        # Save checkpoint
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'processed_indices': list(processed_indices),
                'ticker_data': ticker_data
            }, f)

        # Rate limiting
        time.sleep(DELAY_BETWEEN_REQUESTS)

# Apply updates to dataframe
print(f"\n{'=' * 80}")
print("APPLYING UPDATES")
print(f"{'=' * 80}")

years_changed = 0
years_unchanged = 0
years_failed = 0
tickers_added = 0

for idx_str, data in ticker_data.items():
    idx = int(idx_str)
    new_year = data['year']
    new_ticker = data['ticker']
    old_year = data['old_year']

    # Update year
    if new_year is not None:
        if old_year != new_year or pd.isna(old_year):
            df.at[idx, 'election_year'] = new_year
            years_changed += 1
        else:
            years_unchanged += 1
    else:
        years_failed += 1

    # Update ticker
    if new_ticker is not None:
        df.at[idx, 'ticker'] = new_ticker
        tickers_added += 1

print(f"\nYears changed: {years_changed:,}")
print(f"Years unchanged: {years_unchanged:,}")
print(f"Years failed: {years_failed:,}")
print(f"Tickers added: {tickers_added:,}")

# Save output
print(f"\nSaving to: {OUTPUT_FILE}")
df.to_csv(OUTPUT_FILE, index=False)
print(f"✓ Saved {len(df):,} markets with corrected House/Senate data")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

us_elections_updated = df[df['country'] == 'United States'].copy()
house_senate_updated = us_elections_updated[us_elections_updated['office'].isin(['House', 'Senate'])].copy()

print(f"\nHouse + Senate markets: {len(house_senate_updated):,}")
print(f"Successfully probed: {success_count:,}")
print(f"Failed to probe: {failed_count:,}")

if len(house_senate_updated) > 0:
    with_tickers = house_senate_updated['ticker'].notna().sum()
    print(f"\nMarkets with tickers: {with_tickers:,} ({with_tickers/len(house_senate_updated)*100:.1f}%)")

    print(f"\nHouse + Senate year distribution:")
    print(house_senate_updated['election_year'].value_counts().sort_index())

print(f"\n✓ COMPLETE")
