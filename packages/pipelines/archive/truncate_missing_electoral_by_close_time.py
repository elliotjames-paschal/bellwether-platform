#!/usr/bin/env python3
"""
Truncate markets without electoral details to 1 day before trading_close_time

For markets that couldn't be truncated via ChatGPT (no office/location data),
truncate to 24 hours before trading_close_time to match the election logic.
"""

import pandas as pd
import json
from datetime import datetime, timedelta

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MARKETS_FILE = f"{DATA_DIR}/us_electoral_markets_for_truncation.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"
ELECTION_DATES_FILE = f"{DATA_DIR}/us_election_dates_chatgpt_new.json"

print("=" * 80)
print("TRUNCATING MARKETS WITHOUT ELECTORAL DETAILS")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

markets_df = pd.read_csv(MARKETS_FILE)
print(f"✓ Markets to process: {len(markets_df)}")

with open(ELECTION_DATES_FILE, 'r') as f:
    election_dates = json.load(f)
print(f"✓ Election dates: {len(election_dates)}")

with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)
print(f"✓ Loaded price data: {len(price_data):,} tokens")

# Identify markets that need truncation by close_time
# These are markets without office/location or whose election wasn't in the ChatGPT results

markets_to_truncate = []

for idx, market in markets_df.iterrows():
    office = str(market['office']) if pd.notna(market['office']) else 'nan'
    location = str(market['location']) if pd.notna(market['location']) else 'nan'
    year = str(market['election_year']) if pd.notna(market['election_year']) else 'nan'
    is_primary = market['is_primary']

    # Check if this market has an election date from ChatGPT
    lookup_key = f"{office}|{location}|{year}|{is_primary}"

    if lookup_key not in election_dates:
        # This market needs truncation by close_time
        markets_to_truncate.append(market)

print(f"\n✓ Markets needing close_time truncation: {len(markets_to_truncate)}")

# Truncate by trading_close_time - 24 hours
print(f"\n{'=' * 80}")
print("TRUNCATING PRICE DATA")
print(f"{'=' * 80}")

truncated_count = 0
no_close_time = 0
no_price_data = 0

for market in markets_to_truncate:
    close_time_val = market['trading_close_time']

    if pd.isna(close_time_val):
        no_close_time += 1
        continue

    try:
        close_time = pd.to_datetime(close_time_val, utc=True).replace(tzinfo=None)
    except:
        no_close_time += 1
        continue

    # Calculate cutoff: 1 day before trading_close_time
    cutoff_dt = close_time - timedelta(days=1)
    cutoff_timestamp = int(cutoff_dt.timestamp())

    # Truncate YES token
    token_yes = str(market['pm_token_id_yes'])
    if token_yes in price_data:
        original_len = len(price_data[token_yes])
        price_data[token_yes] = [p for p in price_data[token_yes] if p['t'] <= cutoff_timestamp]
        new_len = len(price_data[token_yes])

        if new_len < original_len:
            truncated_count += 1
    else:
        no_price_data += 1

    # Truncate NO token if exists
    token_no = str(market['pm_token_id_no'])
    if pd.notna(token_no) and token_no != '' and token_no in price_data:
        price_data[token_no] = [p for p in price_data[token_no] if p['t'] <= cutoff_timestamp]

print(f"\n✓ Truncation complete")
print(f"  Markets truncated: {truncated_count}")
print(f"  No close_time: {no_close_time}")
print(f"  No price data: {no_price_data}")

# Save updated price data
print(f"\n{'=' * 80}")
print("SAVING UPDATED PRICE DATA")
print(f"{'=' * 80}")

# Backup original
backup_file = PRICE_FILE.replace('.json', f'_backup_close_time_truncate_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
with open(PRICE_FILE, 'r') as f:
    original_data = json.load(f)
with open(backup_file, 'w') as f:
    json.dump(original_data, f)
print(f"✓ Backed up original to: {backup_file}")

# Save updated data
with open(PRICE_FILE, 'w') as f:
    json.dump(price_data, f)
print(f"✓ Saved updated price data")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
