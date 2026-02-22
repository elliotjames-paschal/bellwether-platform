#!/usr/bin/env python3
"""
Fix Missing Panel B Elections - Price Data Recovery

Pulls price data for 4 Polymarket markets that have 0 prices,
truncates at election dates, and updates the corrected price files.

Note: Kalshi MN Senate 2024 (SENATEMN-24-R) has 0 volume - no trades ever occurred,
so no price history is available to recover.
"""

import requests
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Paths
BASE_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi")
DATA_DIR = BASE_DIR / "data"

PM_PRICE_FILE = DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json"

# DomeAPI Configuration (authenticated endpoint)
DOME_API_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_API_KEY = "Bearer d2d4d3b2cd3b0424bd2145a57d6f34a3661050e2"

# Markets to fix
PM_MARKETS = [
    {
        'name': 'Minnesota Senate (Dem)',
        'market_id': '500689',
        'condition_id': '0x6a425b7b03692e2409f1c9471a9de354c1a9d5989f56c5dae0ef66b15f589de9',
        'token_id_yes': '92812238895308027890405867210709566991891324530386758606116551297602774304541',
        'election_date': '2024-11-05',
    },
    {
        'name': 'Wisconsin Supreme Court',
        'market_id': '527848',
        'condition_id': '0x354ebe41d75446cd43eb2dbebacd344cfb9a8abe9b26ce39c5074e0841e5a885',
        'token_id_yes': '8482778105598978702482860615499306660276940779107944683038963310770224106864',
        'election_date': '2025-04-01',
    },
    {
        'name': 'CA-27',
        'market_id': '505157',
        'condition_id': '0xc25e59d89a54d4cf156eec9a8fb485b831d927c28702f24b67665089534c7e21',
        'token_id_yes': '91185558637433545319929408005961748855542339385322666155845562934483186143095',
        'election_date': '2024-11-05',
    },
    {
        'name': 'CO-8',
        'market_id': '512628',
        'condition_id': '0x565b93f16cdeb61ba3d477320e708dc8524b05993cbdc86a3a02914b1ff935ef',
        'token_id_yes': '9093215244912254816377663646197356805579066249602841657643717096794150687701',
        'election_date': '2024-11-05',
    },
]

print("=" * 80)
print("FIX MISSING PANEL B ELECTIONS - PRICE DATA RECOVERY")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\nNote: Kalshi MN Senate 2024 has 0 volume - no price history available")

# ============================================================================
# Step 1: Pull Polymarket prices from DomeAPI (authenticated)
# ============================================================================

print(f"\n{'=' * 80}")
print("STEP 1: PULLING POLYMARKET PRICES FROM DOMEAPI")
print(f"{'=' * 80}")

# Load existing PM price data
with open(PM_PRICE_FILE, 'r') as f:
    pm_prices = json.load(f)
print(f"Loaded {len(pm_prices):,} tokens from PM price file")

pm_success = 0
pm_errors = []

for market in PM_MARKETS:
    print(f"\n  {market['name']}...")
    condition_id = market['condition_id']
    token_id_yes = market['token_id_yes']
    election_date = market['election_date']

    # Calculate time range: 364 days before election to end of election day
    # (DomeAPI has 1-year limit for daily granularity)
    election_dt = datetime.strptime(election_date, '%Y-%m-%d').replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )
    end_ts = int(election_dt.timestamp())
    start_dt = election_dt - timedelta(days=364)
    start_ts = int(start_dt.timestamp())

    # Call DomeAPI candlesticks endpoint (authenticated)
    url = f"{DOME_API_BASE}/candlesticks/{condition_id}"
    headers = {"Authorization": DOME_API_KEY}
    params = {
        'start_time': start_ts,
        'end_time': end_ts,
        'interval': 1440  # daily
    }

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()
            candlesticks = data.get('candlesticks', [])

            if candlesticks and len(candlesticks) > 0:
                prices_found = []

                for token_data in candlesticks:
                    if len(token_data) != 2:
                        continue

                    candle_array = token_data[0]
                    token_info = token_data[1]
                    this_token_id = str(token_info.get('token_id', ''))

                    if this_token_id == token_id_yes:
                        # Convert candlesticks to our format
                        for candle in candle_array:
                            ts = candle.get('end_period_ts')
                            price_info = candle.get('price', {})
                            price_cents = price_info.get('close', 0)
                            price_decimal = price_cents / 100.0  # Convert cents to 0-1 range

                            if ts:
                                prices_found.append({'t': ts, 'p': price_decimal})
                        break

                if prices_found:
                    # Truncate at election date
                    prices_truncated = [p for p in prices_found if p['t'] <= end_ts]
                    pm_prices[token_id_yes] = prices_truncated
                    pm_success += 1
                    print(f"    Found {len(prices_truncated)} prices (truncated at {election_date})")
                else:
                    pm_errors.append(f"{market['name']}: No matching token in response")
                    print(f"    ERROR: No matching token found")
            else:
                pm_errors.append(f"{market['name']}: Empty candlesticks")
                print(f"    ERROR: Empty candlesticks")
        else:
            pm_errors.append(f"{market['name']}: HTTP {response.status_code} - {response.text[:100]}")
            print(f"    ERROR: HTTP {response.status_code}")

    except Exception as e:
        pm_errors.append(f"{market['name']}: {str(e)[:50]}")
        print(f"    ERROR: {str(e)[:50]}")

    time.sleep(0.5)

print(f"\nPolymarket: {pm_success}/{len(PM_MARKETS)} successful")
if pm_errors:
    print("Errors:")
    for err in pm_errors:
        print(f"  - {err}")

# ============================================================================
# Step 2: Save updated price file
# ============================================================================

print(f"\n{'=' * 80}")
print("STEP 2: SAVING UPDATED PRICE FILE")
print(f"{'=' * 80}")

# Backup and save PM prices
pm_backup = str(PM_PRICE_FILE).replace('.json', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
with open(pm_backup, 'w') as f:
    with open(PM_PRICE_FILE, 'r') as orig:
        f.write(orig.read())
print(f"Backed up PM prices to: {pm_backup}")

with open(PM_PRICE_FILE, 'w') as f:
    json.dump(pm_prices, f)
print(f"Saved PM prices: {len(pm_prices):,} tokens")

# ============================================================================
# Step 3: Verify
# ============================================================================

print(f"\n{'=' * 80}")
print("STEP 3: VERIFICATION")
print(f"{'=' * 80}")

print("\nPolymarket token price counts:")
for market in PM_MARKETS:
    count = len(pm_prices.get(market['token_id_yes'], []))
    print(f"  {market['name']}: {count} prices")

# ============================================================================
# Summary
# ============================================================================

print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")
print(f"\nPolymarket: {pm_success}/{len(PM_MARKETS)} markets recovered")

if pm_success == len(PM_MARKETS):
    print("\nAll Polymarket markets successfully recovered!")
    print("\nNext steps:")
    print("  1. Run calculate_all_political_brier_scores.py to regenerate prediction accuracy")
    print("  2. Run election_winner_markets_comparison.py to verify Panel B count")
else:
    print("\nSome markets failed - check errors above")

print(f"\n{'=' * 80}")
print("DONE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
