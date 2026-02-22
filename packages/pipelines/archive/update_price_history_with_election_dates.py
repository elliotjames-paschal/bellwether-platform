#!/usr/bin/env python3
"""
Update Price History with Election Date Cutoffs

This script:
1. Creates backups of original price history files
2. Identifies unique elections from the master CSV
3. Uses OpenAI API to determine actual election dates (including special elections)
4. Truncates price history to 24 hours before election day for those markets
5. Processes in batches of 25 elections

Note: Set your OPENAI_API_KEY environment variable before running
"""

import pandas as pd
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import time
from openai import OpenAI

# Paths
BASE_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi")
DATA_DIR = BASE_DIR / "data"

# Initialize OpenAI client - read API key from file
api_key_path = BASE_DIR / "openai_api_key.txt"
with open(api_key_path, 'r') as f:
    api_key = f.read().strip()
client = OpenAI(api_key=api_key)

BATCH_SIZE = 25

print("=" * 80)
print("UPDATING PRICE HISTORY WITH ELECTION DATE CUTOFFS")
print("=" * 80)

# ============================================================================
# 1. Create Backups
# ============================================================================

print("\n1. Creating backups of price history files...")

price_files = [
    "polymarket_all_political_prices_CORRECTED_v3.json",
    "kalshi_all_political_prices_CORRECTED_v3.json"
]

for filename in price_files:
    original_path = DATA_DIR / filename
    backup_path = DATA_DIR / f"{filename}.backup"

    if original_path.exists():
        if not backup_path.exists():
            print(f"   Creating backup: {backup_path.name}")
            with open(original_path, 'r') as f:
                data = json.load(f)
            with open(backup_path, 'w') as f:
                json.dump(data, f)
            print(f"   ✓ Backup created")
        else:
            print(f"   ⚠ Backup already exists: {backup_path.name} (skipping)")
    else:
        print(f"   ⚠ Original file not found: {filename}")

# ============================================================================
# 2. Load Data and Identify Unique Elections
# ============================================================================

print("\n2. Loading master CSV and identifying unique elections...")

master_df = pd.read_csv(DATA_DIR / "combined_political_markets_with_electoral_details.csv", low_memory=False)

# Filter to elections only (has electoral details)
elections_df = master_df[
    (pd.notna(master_df['democrat_vote_share'])) &
    (pd.notna(master_df['republican_vote_share'])) &
    (master_df['is_primary'] == False) &
    (master_df['country'] == 'United States')
].copy()

print(f"   ✓ {len(elections_df)} election market records")

# Group by unique elections
election_cols = ['country', 'office', 'location', 'election_year']
unique_elections = elections_df[election_cols].drop_duplicates().sort_values(election_cols).reset_index(drop=True)

print(f"   ✓ {len(unique_elections)} unique elections")

# ============================================================================
# 3. Use OpenAI API to Get Election Dates
# ============================================================================

print(f"\n3. Using OpenAI API to determine election dates (batches of {BATCH_SIZE})...")

def get_election_dates_batch(elections_batch, start_idx=0):
    """
    Query OpenAI API for election dates for a batch of elections

    Args:
        elections_batch: DataFrame of elections
        start_idx: Starting index for numbering (0-based)

    Returns: dict mapping (office, location, year) -> election_date string
    """
    # Format elections for prompt - use sequential numbering starting from start_idx
    elections_list = []
    for i, (idx, row) in enumerate(elections_batch.iterrows()):
        elections_list.append(
            f"{start_idx + i + 1}. {row['election_year']:.0f} {row['office']} - {row['location']}"
        )
    elections_text = "\n".join(elections_list)

    prompt = f"""For each of the following US elections, provide the exact election date in YYYY-MM-DD format.

Elections:
{elections_text}

Respond with ONLY a JSON object where each key is the election number (1, 2, 3, etc.) and the value is the date in YYYY-MM-DD format. For example:
{{"1": "2024-11-05", "2": "2023-11-07", "3": "2024-02-13"}}

Remember:
- US general elections are typically the first Tuesday after the first Monday in November
- Special elections vary by state and circumstances
- 2024 general election was November 5, 2024
- 2023 general elections were November 7, 2023"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that provides accurate US election dates. Respond only with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )

        result_text = response.choices[0].message.content.strip()

        # Debug print
        print(f"      API Response (first 200 chars): {result_text[:200]}")

        # Parse JSON response
        if result_text.startswith("```json"):
            result_text = result_text.split("```json")[1].split("```")[0].strip()
        elif result_text.startswith("```"):
            result_text = result_text.split("```")[1].split("```")[0].strip()

        dates_dict = json.loads(result_text)

        # Map back to elections
        election_dates = {}
        for i, (idx, row) in enumerate(elections_batch.iterrows()):
            # The API uses keys like "1", "2", "26", "27" etc.
            api_key = str(start_idx + i + 1)
            date_str = dates_dict.get(api_key)
            if date_str:
                key = (row['office'], row['location'], row['election_year'])
                election_dates[key] = date_str

        return election_dates

    except Exception as e:
        print(f"   ⚠ Error querying OpenAI API: {e}")
        print(f"   ⚠ Response text: {result_text if 'result_text' in locals() else 'N/A'}")
        return {}

# Process in batches
all_election_dates = {}
num_batches = (len(unique_elections) + BATCH_SIZE - 1) // BATCH_SIZE

for batch_num in range(num_batches):
    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(unique_elections))
    batch = unique_elections.iloc[start_idx:end_idx]

    print(f"\n   Batch {batch_num + 1}/{num_batches} (elections {start_idx + 1}-{end_idx})...")

    batch_dates = get_election_dates_batch(batch, start_idx=start_idx)
    all_election_dates.update(batch_dates)

    print(f"   ✓ Got {len(batch_dates)} election dates")

    # Rate limiting - wait 1 second between batches
    if batch_num < num_batches - 1:
        time.sleep(1)

print(f"\n   ✓ Total election dates obtained: {len(all_election_dates)}")

# Save election dates to CSV
election_dates_df = pd.DataFrame([
    {
        'office': k[0],
        'location': k[1],
        'election_year': k[2],
        'election_date': v
    }
    for k, v in all_election_dates.items()
])
election_dates_csv_path = DATA_DIR / "election_dates_lookup.csv"
election_dates_df.to_csv(election_dates_csv_path, index=False)
print(f"   ✓ Saved election dates to: {election_dates_csv_path}")

# ============================================================================
# 4. Update Price History Files
# ============================================================================

print("\n4. Updating price history files with election date cutoffs...")

# Add election_date to ALL election markets by mapping through the election grouping
elections_df['election_key'] = elections_df.apply(
    lambda row: (row['office'], row['location'], row['election_year']), axis=1
)
elections_df['election_date'] = elections_df['election_key'].map(all_election_dates)

# Create mapping: token_id/ticker -> cutoff_timestamp for ALL markets in these elections
market_cutoffs = {'Polymarket': {}, 'Kalshi': {}}

for _, row in elections_df.iterrows():
    if pd.notna(row['election_date']):
        election_dt = datetime.strptime(row['election_date'], '%Y-%m-%d')
        cutoff_dt = election_dt  # Keep prices up to start of election day (includes all of day before)
        cutoff_timestamp = int(cutoff_dt.timestamp())

        platform = row['platform']

        if platform == 'Polymarket':
            # For Polymarket, use token_ids (both Yes and No)
            token_yes = str(row['pm_token_id_yes']) if pd.notna(row.get('pm_token_id_yes')) else None
            token_no = str(row['pm_token_id_no']) if pd.notna(row.get('pm_token_id_no')) else None

            if token_yes:
                market_cutoffs['Polymarket'][token_yes] = cutoff_timestamp
            if token_no:
                market_cutoffs['Polymarket'][token_no] = cutoff_timestamp

        elif platform == 'Kalshi':
            # For Kalshi, use ticker (market_id)
            ticker = str(row['market_id'])
            market_cutoffs['Kalshi'][ticker] = cutoff_timestamp

print(f"   ✓ Total election markets to update: {len(elections_df)}")
print(f"   ✓ Polymarket: {len(market_cutoffs['Polymarket'])} markets")
print(f"   ✓ Kalshi: {len(market_cutoffs['Kalshi'])} markets")

# Update Polymarket price history
print("\n   Updating Polymarket price history...")
pm_file = DATA_DIR / "polymarket_all_political_prices_CORRECTED_v3.json"
with open(pm_file, 'r') as f:
    pm_prices = json.load(f)

pm_updated_count = 0
for token_id, price_list in pm_prices.items():
    if token_id in market_cutoffs.get('Polymarket', {}):
        cutoff_ts = market_cutoffs['Polymarket'][token_id]

        # Filter price history (price_list is directly a list of {'t': ..., 'p': ...})
        if isinstance(price_list, list) and price_list:
            original_count = len(price_list)
            pm_prices[token_id] = [
                p for p in price_list
                if p['t'] <= cutoff_ts
            ]
            new_count = len(pm_prices[token_id])

            if new_count < original_count:
                pm_updated_count += 1

with open(pm_file, 'w') as f:
    json.dump(pm_prices, f)

print(f"   ✓ Updated {pm_updated_count} Polymarket tokens")

# Update Kalshi price history
print("\n   Updating Kalshi price history...")
kalshi_file = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"
with open(kalshi_file, 'r') as f:
    kalshi_prices = json.load(f)

kalshi_updated_count = 0
for ticker, candlesticks in kalshi_prices.items():
    if ticker in market_cutoffs.get('Kalshi', {}):
        cutoff_ts = market_cutoffs['Kalshi'][ticker]

        # Filter candlesticks (candlesticks is directly a list of {'end_period_ts': ..., ...})
        if isinstance(candlesticks, list) and candlesticks:
            original_count = len(candlesticks)
            kalshi_prices[ticker] = [
                c for c in candlesticks
                if c['end_period_ts'] <= cutoff_ts
            ]
            new_count = len(kalshi_prices[ticker])

            if new_count < original_count:
                kalshi_updated_count += 1

with open(kalshi_file, 'w') as f:
    json.dump(kalshi_prices, f)

print(f"   ✓ Updated {kalshi_updated_count} Kalshi markets")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
print("\nNext steps:")
print("1. Review the election_dates_lookup.csv file to verify dates are correct")
print("2. Re-run calculate_all_political_brier_scores.py to regenerate prediction CSVs")
print("3. Re-run election_winner_markets_comparison.py to regenerate tables")
print("\nTo restore original files:")
print("  - Rename .backup files back to original names")
print("=" * 80)
