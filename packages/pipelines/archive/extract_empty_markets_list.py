#!/usr/bin/env python3
"""
Extract list of 733 empty markets with condition_ids for DomeAPI testing

Creates a small CSV to avoid loading entire price file every time.
"""

import pandas as pd
import json

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"

print("Loading price data to find empty tokens...")
with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)

empty_tokens = [token_id for token_id, history in price_data.items() if len(history) == 0]
print(f"✓ Found {len(empty_tokens):,} empty tokens")

print("Loading master file...")
master_df = pd.read_csv(MASTER_FILE, low_memory=False)
pm_markets = master_df[master_df['platform'] == 'Polymarket'].copy()

print("Finding markets with empty tokens...")
empty_markets = []
for token_id in empty_tokens:
    market_row = pm_markets[pm_markets['pm_token_id_yes'].astype(str) == token_id]
    if not market_row.empty:
        market = market_row.iloc[0]
        empty_markets.append({
            'market_id': str(market['market_id']),
            'question': market['question'],
            'condition_id': market['pm_condition_id'],
            'token_id': token_id,
            'political_category': market.get('political_category', 'Unknown')
        })

empty_df = pd.DataFrame(empty_markets)
output_file = f"{DATA_DIR}/empty_markets_to_test.csv"
empty_df.to_csv(output_file, index=False)

print(f"✓ Saved {len(empty_markets)} empty markets to: {output_file}")
