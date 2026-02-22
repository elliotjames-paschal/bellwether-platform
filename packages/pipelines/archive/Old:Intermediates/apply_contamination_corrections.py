#!/usr/bin/env python3
"""
Apply contamination corrections to historical price data.
Replicates exact logic from combined_pipeline notebook Cell 7.
"""

import json
from datetime import datetime
from pathlib import Path

# Paths
BASE_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi")
DATA_DIR = BASE_DIR / "data"
DESKTOP_DIR = Path("/Users/paschal/Desktop/Polymarket:Kalshi")

# Configuration: Set to True to include uncertain Polymarket event dates
INCLUDE_UNCERTAIN_POLYMARKET_DATES = True  # Matches notebook setting

print("\n🔧 ADJUSTING DATA TO REMOVE POST-EVENT CONTAMINATION")
print("=" * 80)

# ============================================================================
# Load Price History Data
# ============================================================================

print("\n1. Loading price history data...")

# Polymarket - merge election and non-election prices
with open(DATA_DIR / "polymarket_election_prices_20251113_164348.json", 'r') as f:
    pm_prices_elections = json.load(f)
print(f"   ✓ Loaded {len(pm_prices_elections):,} Polymarket election token prices")

with open(DATA_DIR / "polymarket_non_election_prices_20251114_051342.json", 'r') as f:
    pm_prices_non_elections = json.load(f)
print(f"   ✓ Loaded {len(pm_prices_non_elections):,} Polymarket non-election token prices")

poly_price_history = {**pm_prices_elections, **pm_prices_non_elections}
print(f"   ✓ Total: {len(poly_price_history):,} Polymarket tokens")

# Kalshi - merge election and non-election prices
with open(DATA_DIR / "kalshi_election_prices_20251113_164348.json", 'r') as f:
    kalshi_prices_elections = json.load(f)
print(f"   ✓ Loaded {len(kalshi_prices_elections):,} Kalshi election market prices")

with open(DATA_DIR / "kalshi_non_election_prices_20251114_051342.json", 'r') as f:
    kalshi_prices_non_elections = json.load(f)
print(f"   ✓ Loaded {len(kalshi_prices_non_elections):,} Kalshi non-election market prices")

kalshi_price_history = {**kalshi_prices_elections, **kalshi_prices_non_elections}
print(f"   ✓ Total: {len(kalshi_price_history):,} Kalshi markets")

# ============================================================================
# 1. KALSHI: Remove last day for time-bounded markets
# ============================================================================

print("\n2. KALSHI ADJUSTMENT: Removing last day for time-bounded markets...")

with open(DESKTOP_DIR / 'kalshi_time_bounded_markets.json', 'r') as f:
    time_bounded_data = json.load(f)

# Extract tickers where is_time_bounded is True
time_bounded_tickers = {
    ticker for ticker, data in time_bounded_data.items()
    if data.get('is_time_bounded', False)
}

print(f"   Loaded {len(time_bounded_tickers)} time-bounded Kalshi markets")

kalshi_adjusted_count = 0
kalshi_removed_count = 0

for ticker in time_bounded_tickers:
    if ticker not in kalshi_price_history:
        continue

    candlesticks = kalshi_price_history[ticker]

    if not candlesticks or len(candlesticks) == 0:
        continue

    # Find the last (most recent) timestamp
    last_timestamp = max(c['end_period_ts'] for c in candlesticks)

    # Remove all candlesticks from that last day
    original_count = len(candlesticks)
    filtered_candlesticks = [c for c in candlesticks if c['end_period_ts'] < last_timestamp]

    if len(filtered_candlesticks) < original_count:
        kalshi_price_history[ticker] = filtered_candlesticks
        kalshi_adjusted_count += 1
        kalshi_removed_count += (original_count - len(filtered_candlesticks))

print(f"   ✓ Adjusted {kalshi_adjusted_count} Kalshi markets (removed last day)")
print(f"   ✓ Removed {kalshi_removed_count} candlestick data points")

# ============================================================================
# 2. POLYMARKET: Truncate price histories to event dates
# ============================================================================

print("\n3. POLYMARKET ADJUSTMENT: Truncating to event dates...")

# Load market metadata to map market_id to token_id
import pandas as pd
market_metadata = pd.read_csv(DATA_DIR / "market_categories_with_outcomes.csv")
print(f"   Loaded market metadata: {len(market_metadata)} records")

# Create mapping from market_id to list of token_ids
market_to_tokens = {}
for _, row in market_metadata.iterrows():
    market_id = str(row['market_id'])
    token_id = str(row['token_id'])
    if market_id not in market_to_tokens:
        market_to_tokens[market_id] = []
    market_to_tokens[market_id].append(token_id)

print(f"   Created market_id → token_id mapping for {len(market_to_tokens)} markets")

# Load event dates
with open(DESKTOP_DIR / 'event_dates_chatgpt.json', 'r') as f:
    event_dates = json.load(f)

# Load uncertain markets list
with open(DESKTOP_DIR / 'needs_manual_review.json', 'r') as f:
    needs_review = json.load(f)

uncertain_market_ids = {m['market_id'] for m in needs_review}
print(f"   Loaded event dates for {len(event_dates)} Polymarket markets")
print(f"   Loaded {len(uncertain_market_ids)} markets needing manual review")

poly_adjusted_markets = 0
poly_truncated_count = 0

for market_id, event_data in event_dates.items():
    # Skip uncertain markets if configured
    if not INCLUDE_UNCERTAIN_POLYMARKET_DATES and market_id in uncertain_market_ids:
        continue

    event_date_str = event_data.get('event_date')
    if not event_date_str or event_date_str == 'N/A':
        continue

    # Get all token_ids for this market_id
    if market_id not in market_to_tokens:
        continue

    token_ids = market_to_tokens[market_id]

    # Parse event date
    try:
        event_date = datetime.fromisoformat(event_date_str)
        event_timestamp = int(event_date.timestamp())
    except:
        continue

    # Process each token for this market
    for token_id in token_ids:
        if token_id not in poly_price_history:
            continue

        # Get the token's price history
        price_history = poly_price_history[token_id]

        if not price_history or len(price_history) == 0:
            continue

        # Get current last timestamp
        last_price_point = price_history[-1]
        current_last_timestamp = last_price_point.get('t')

        if not current_last_timestamp:
            continue

        # If event occurred before market close, truncate
        if event_timestamp < current_last_timestamp:
            truncated_prices = [p for p in price_history if p.get('t', 0) <= event_timestamp]

            if len(truncated_prices) > 0:
                poly_price_history[token_id] = truncated_prices
                poly_truncated_count += 1

    poly_adjusted_markets += 1

print(f"   ✓ Processed {poly_adjusted_markets} Polymarket markets with event dates")
print(f"   ✓ Truncated {poly_truncated_count} token price histories")

if INCLUDE_UNCERTAIN_POLYMARKET_DATES:
    print(f"   ℹ️  Including {len(uncertain_market_ids)} markets with uncertain dates")
else:
    print(f"   ⚠️  Excluded {len(uncertain_market_ids)} markets with uncertain dates")

# ============================================================================
# Save Corrected Price Histories
# ============================================================================

print("\n4. Saving corrected price histories...")

# Save Polymarket
poly_output = DATA_DIR / "polymarket_all_political_prices_CORRECTED_20251114.json"
with open(poly_output, 'w') as f:
    json.dump(poly_price_history, f)
print(f"   ✓ Saved: {poly_output}")
print(f"     {len(poly_price_history):,} tokens")

# Save Kalshi
kalshi_output = DATA_DIR / "kalshi_all_political_prices_CORRECTED_20251114.json"
with open(kalshi_output, 'w') as f:
    json.dump(kalshi_price_history, f)
print(f"   ✓ Saved: {kalshi_output}")
print(f"     {len(kalshi_price_history):,} markets")

print("\n" + "=" * 80)
print("✅ DATA ADJUSTMENT COMPLETE")
print("=" * 80)
print("\nSummary:")
print(f"  Kalshi: {kalshi_adjusted_count} markets adjusted (removed last day)")
print(f"  Polymarket: {poly_truncated_count} tokens truncated to event dates")
print("\nNext step: Update calculate_all_political_brier_scores.py to use corrected files")
print("=" * 80)
