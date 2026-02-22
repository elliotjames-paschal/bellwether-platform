#!/usr/bin/env python3
"""
Pull Polymarket price history using GraphQL for markets with missing data
"""

import pandas as pd
import requests
import json
import time
from datetime import datetime
import pytz

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"

# API Configuration
GAMMA_API_URL = "https://gamma-api.polymarket.com"
SUBGRAPH_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

# Rate limiting
GRAPHQL_RATE_LIMIT = 2  # requests per second
GAMMA_RATE_LIMIT = 5
MAX_RETRIES = 3
PAGE_SIZE = 1000  # GraphQL pagination size

print("=" * 80)
print("PULLING POLYMARKET PRICE HISTORY VIA GRAPHQL")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded master file: {len(master_df):,} rows")

# Get all Polymarket markets
pm_markets = master_df[master_df['platform'] == 'Polymarket'].copy()
print(f"✓ Polymarket markets in master: {len(pm_markets):,}")

# Load existing price data
with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)
print(f"✓ Loaded existing price data: {len(price_data):,} tokens")

# Find markets with tokens but missing or insufficient price data (≤1 points)
markets_with_tokens = pm_markets[pm_markets['pm_token_id_yes'].notna()].copy()
print(f"✓ Markets with token IDs: {len(markets_with_tokens):,}")

# Apply date filters
cutoff_date = datetime(2025, 11, 10, 23, 59, 59, tzinfo=pytz.UTC)
markets_with_tokens['end_dt'] = pd.to_datetime(markets_with_tokens['scheduled_end_time'], errors='coerce')

# Check for missing or insufficient price data (≤1 points) AND date filters
missing_markets = []
for idx, row in markets_with_tokens.iterrows():
    token_yes = str(row['pm_token_id_yes'])
    token_no = str(row['pm_token_id_no']) if pd.notna(row['pm_token_id_no']) else None

    # Check if yes token is missing or has ≤1 price points
    needs_data = False
    if token_yes not in price_data:
        needs_data = True
    elif len(price_data.get(token_yes, [])) <= 1:
        needs_data = True

    if needs_data:
        # Apply date filters: scheduled_end_time <= Nov 10, 2025 OR election_year <= 2025
        passes_filter = False

        # Filter 1: scheduled_end_time <= Nov 10, 2025
        if pd.notna(row['end_dt']) and row['end_dt'] <= cutoff_date:
            passes_filter = True

        # Filter 2: election_year <= 2025
        if pd.notna(row.get('election_year')) and row.get('election_year') <= 2025:
            passes_filter = True

        if passes_filter:
            missing_markets.append(row)

missing_df = pd.DataFrame(missing_markets)

print(f"✓ Markets with ≤1 price points matching date filters: {len(missing_df):,}")

if len(missing_df) == 0:
    print("\n✓ All markets already have price data!")
    exit(0)

print(f"\n{'=' * 80}")
print("FETCHING PRICE HISTORY FROM GRAPHQL")
print(f"{'=' * 80}")


class PolymarketHistoricalCollector:
    def __init__(self):
        self.gamma_url = GAMMA_API_URL
        self.subgraph_url = SUBGRAPH_URL
        self.last_graphql_call = 0
        self.last_gamma_call = 0

    def _rate_limit_graphql(self):
        """Rate limit GraphQL calls"""
        elapsed = time.time() - self.last_graphql_call
        min_interval = 1.0 / GRAPHQL_RATE_LIMIT
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_graphql_call = time.time()

    def _rate_limit_gamma(self):
        """Rate limit Gamma API calls"""
        elapsed = time.time() - self.last_gamma_call
        min_interval = 1.0 / GAMMA_RATE_LIMIT
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self.last_gamma_call = time.time()

    def get_all_trades_for_tokens(self, token_ids, market_id):
        """Get ALL historical trades for specific token IDs via GraphQL with pagination"""
        self._rate_limit_graphql()

        if not token_ids:
            return []

        all_trades = []
        skip = 0

        # Build where clause for these token IDs
        token_id_conditions = []
        for token_id in token_ids:
            token_id_conditions.extend([
                f'{{makerAssetId: "{token_id}"}}',
                f'{{takerAssetId: "{token_id}"}}'
            ])

        where_clause = f"or: [{', '.join(token_id_conditions)}]"

        while True:
            query = f'''
            {{
              orderFilledEvents(
                first: {PAGE_SIZE}
                skip: {skip}
                orderBy: timestamp
                orderDirection: asc
                where: {{{where_clause}}}
              ) {{
                id
                timestamp
                makerAssetId
                takerAssetId
                makerAmountFilled
                takerAmountFilled
              }}
            }}
            '''

            page_trades = []

            for attempt in range(MAX_RETRIES):
                try:
                    response = requests.post(
                        self.subgraph_url,
                        json={'query': query},
                        headers={'Content-Type': 'application/json'},
                        timeout=30
                    )

                    if response.status_code == 200:
                        data = response.json()

                        if 'errors' in data:
                            return all_trades  # Return what we have so far

                        page_trades = data.get('data', {}).get('orderFilledEvents', [])
                        break
                    else:
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(2 ** attempt)

                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)

            if not page_trades:
                break  # No more trades

            all_trades.extend(page_trades)

            # If we got less than page_size, we're done
            if len(page_trades) < PAGE_SIZE:
                break

            skip += PAGE_SIZE

            # Rate limit between pagination requests
            time.sleep(0.5)

        return all_trades

    def reconstruct_price_history(self, trades, token_yes, token_no):
        """Reconstruct price history from trades data"""
        if not trades:
            return [], []

        # Sort trades by timestamp (oldest first)
        sorted_trades = sorted(trades, key=lambda x: int(x['timestamp']))

        yes_prices = []
        no_prices = []

        for trade in sorted_trades:
            timestamp = int(trade['timestamp'])

            maker_asset = trade['makerAssetId']
            taker_asset = trade['takerAssetId']

            try:
                maker_amount = float(trade['makerAmountFilled'])
                taker_amount = float(trade['takerAmountFilled'])
            except (ValueError, TypeError):
                continue

            # Calculate price for YES token
            yes_price = None
            if maker_asset == token_yes and maker_amount > 0:
                yes_price = taker_amount / maker_amount
            elif taker_asset == token_yes and taker_amount > 0:
                yes_price = maker_amount / taker_amount

            if yes_price is not None and 0 <= yes_price <= 1:
                yes_prices.append({'t': timestamp, 'p': str(yes_price)})

            # Calculate price for NO token if exists
            if token_no:
                no_price = None
                if maker_asset == token_no and maker_amount > 0:
                    no_price = taker_amount / maker_amount
                elif taker_asset == token_no and taker_amount > 0:
                    no_price = maker_amount / taker_amount

                if no_price is not None and 0 <= no_price <= 1:
                    no_prices.append({'t': timestamp, 'p': str(no_price)})

        return yes_prices, no_prices


collector = PolymarketHistoricalCollector()

# Process markets
success_count = 0
empty_count = 0
error_count = 0

for idx, row in missing_df.iterrows():
    market_id = str(row['market_id'])
    token_yes = str(row['pm_token_id_yes'])
    token_no = str(row['pm_token_id_no']) if pd.notna(row['pm_token_id_no']) else None

    print(f"\n[{success_count + empty_count + error_count + 1}/{len(missing_df)}] Market {market_id}")
    print(f"  {row['question'][:70]}...")

    try:
        # Get tokens to query
        tokens_to_query = [token_yes]
        if token_no:
            tokens_to_query.append(token_no)

        # Get all trades for these tokens
        trades = collector.get_all_trades_for_tokens(tokens_to_query, market_id)

        if trades:
            # Reconstruct price history
            yes_prices, no_prices = collector.reconstruct_price_history(trades, token_yes, token_no)

            if yes_prices or no_prices:
                # Add to price data
                if yes_prices:
                    price_data[token_yes] = yes_prices
                if no_prices and token_no:
                    price_data[token_no] = no_prices

                success_count += 1
                print(f"  ✓ Got {len(yes_prices)} YES prices, {len(no_prices)} NO prices from {len(trades)} trades")
            else:
                # Empty - trades exist but couldn't reconstruct prices
                price_data[token_yes] = []
                if token_no:
                    price_data[token_no] = []
                empty_count += 1
                print(f"  ⚠ {len(trades)} trades but no valid prices")
        else:
            # No trades
            price_data[token_yes] = []
            if token_no:
                price_data[token_no] = []
            empty_count += 1
            print(f"  ⚠ No trades found")

    except Exception as e:
        error_count += 1
        print(f"  ✗ Error: {str(e)[:100]}")

# Save updated price data
print(f"\n{'=' * 80}")
print("SAVING UPDATED PRICE DATA")
print(f"{'=' * 80}")

# Backup original file first
backup_file = PRICE_FILE.replace('.json', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
with open(PRICE_FILE, 'r') as f:
    original_data = json.load(f)
with open(backup_file, 'w') as f:
    json.dump(original_data, f)
print(f"✓ Backed up original to: {backup_file}")

# Save updated data
with open(PRICE_FILE, 'w') as f:
    json.dump(price_data, f)
print(f"✓ Saved updated price data: {len(price_data):,} tokens")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

print(f"\nMarkets processed: {len(missing_df):,}")
print(f"  Successful: {success_count:,}")
print(f"  Empty (no trades/prices): {empty_count:,}")
print(f"  Errors: {error_count:,}")

print(f"\nPrice file updated:")
print(f"  Before: {len(original_data):,} tokens")
print(f"  After: {len(price_data):,} tokens")
print(f"  Added: {len(price_data) - len(original_data):,}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
