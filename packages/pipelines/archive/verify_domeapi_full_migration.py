#!/usr/bin/env python3
"""
Verify Full DomeAPI Migration Results

Validates the full DomeAPI migration and generates a comprehensive report
comparing with CLOB data and analyzing success rates.
"""

import pandas as pd
import json
import os
from datetime import datetime

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"

# DomeAPI files
DOMEAPI_PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_DOMEAPI_v1.json"
DOMEAPI_TRACKING_FILE = f"{DATA_DIR}/domeapi_full_migration_results.json"
DOMEAPI_ERROR_FILE = f"{DATA_DIR}/domeapi_full_migration_errors.csv"

# CLOB files (for comparison)
CLOB_PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"

# Output
REPORT_FILE = f"{DATA_DIR}/domeapi_full_migration_report.txt"

print("=" * 80)
print("DOMEAPI FULL MIGRATION VERIFICATION")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
pm_markets = master_df[master_df['platform'] == 'Polymarket'].copy()
pm_markets['market_id'] = pm_markets['market_id'].astype(str)

print(f"✓ Loaded master file: {len(master_df):,} total markets")
print(f"✓ Polymarket markets: {len(pm_markets):,}")

# Load DomeAPI data
with open(DOMEAPI_PRICE_FILE, 'r') as f:
    domeapi_prices = json.load(f)
print(f"✓ Loaded DomeAPI prices: {len(domeapi_prices):,} tokens")

with open(DOMEAPI_TRACKING_FILE, 'r') as f:
    tracking = json.load(f)
print(f"✓ Loaded tracking file: {len(tracking['markets']):,} successful markets")

errors_df = pd.read_csv(DOMEAPI_ERROR_FILE)
print(f"✓ Loaded error file: {len(errors_df):,} errors")

# Load CLOB data for comparison
with open(CLOB_PRICE_FILE, 'r') as f:
    clob_prices = json.load(f)
print(f"✓ Loaded CLOB prices: {len(clob_prices):,} tokens")

# Verification checks
print(f"\n{'=' * 80}")
print("VERIFICATION CHECKS")
print(f"{'=' * 80}")

# 1. Processing completeness
print(f"\n1. Processing completeness:")
pm_with_close = pm_markets[pm_markets['trading_close_time'].notna()]
print(f"   PM markets with trading_close_time: {len(pm_with_close):,}")
print(f"   Successfully pulled: {len(tracking['markets']):,}")
print(f"   Failed: {len(errors_df):,}")
total_processed = len(tracking['markets']) + len(errors_df)
print(f"   Total processed: {total_processed:,}")

if total_processed == len(pm_with_close):
    print(f"   ✓ All markets processed")
else:
    diff = len(pm_with_close) - total_processed
    print(f"   ⚠ {diff} markets unaccounted for")

# 2. Success rate analysis
print(f"\n2. Success rate analysis:")
success_rate = (len(tracking['markets']) / len(pm_with_close)) * 100
print(f"   Overall success rate: {success_rate:.1f}%")
print(f"   Overall failure rate: {100-success_rate:.1f}%")

# By year
tracking_df = pd.DataFrame(tracking['markets'])
tracking_df['market_id'] = tracking_df['market_id'].astype(str)

merged_success = tracking_df.merge(
    pm_markets[['market_id', 'trading_close_time']],
    on='market_id',
    how='left'
)
merged_success['year'] = pd.to_datetime(merged_success['trading_close_time'], errors='coerce').dt.year

errors_df['market_id'] = errors_df['market_id'].astype(str)
merged_errors = errors_df.merge(
    pm_markets[['market_id', 'trading_close_time']],
    on='market_id',
    how='left'
)
merged_errors['year'] = pd.to_datetime(merged_errors['trading_close_time'], errors='coerce').dt.year

print(f"\n   Success by year:")
for year in sorted(merged_success['year'].dropna().unique()):
    success_count = len(merged_success[merged_success['year'] == year])
    error_count = len(merged_errors[merged_errors['year'] == year])
    total_year = success_count + error_count
    if total_year > 0:
        year_success_rate = (success_count / total_year) * 100
        print(f"     {int(year)}: {success_count}/{total_year} ({year_success_rate:.1f}%)")

# 3. Data quality validation
print(f"\n3. Data quality validation:")
# Check format
valid_format = True
sample_tokens = list(domeapi_prices.keys())[:100]
for token_id in sample_tokens:
    prices = domeapi_prices[token_id]
    if len(prices) > 0:
        first_price = prices[0]
        if not (isinstance(first_price, dict) and 't' in first_price and 'p' in first_price):
            valid_format = False
            break
        if not (0.0 <= first_price['p'] <= 1.0):
            valid_format = False
            break

if valid_format:
    print(f"   ✓ Price data format valid ({len(sample_tokens)} tokens checked)")
else:
    print(f"   ✗ Price data format invalid")

# 4. Error analysis
print(f"\n4. Error analysis:")
error_counts = errors_df['error'].value_counts()
for error, count in error_counts.items():
    percent = (count / len(errors_df)) * 100
    print(f"   {error}: {count} ({percent:.1f}%)")

# 5. Comparison with CLOB
print(f"\n5. Comparison with CLOB data:")
# Find tokens in both datasets
common_tokens = set(domeapi_prices.keys()) & set(clob_prices.keys())
print(f"   Tokens in both datasets: {len(common_tokens):,}")
print(f"   Tokens only in DomeAPI: {len(set(domeapi_prices.keys()) - set(clob_prices.keys())):,}")
print(f"   Tokens only in CLOB: {len(set(clob_prices.keys()) - set(domeapi_prices.keys())):,}")

# Compare coverage for common tokens
domeapi_more = 0
clob_more = 0
similar = 0

for token_id in list(common_tokens)[:1000]:  # Sample 1000
    dome_count = len(domeapi_prices[token_id])
    clob_count = len(clob_prices[token_id])

    if dome_count > clob_count * 1.1:
        domeapi_more += 1
    elif clob_count > dome_count * 1.1:
        clob_more += 1
    else:
        similar += 1

if len(common_tokens) > 0:
    print(f"\n   Coverage comparison (sample of 1000):")
    print(f"     DomeAPI has more data: {domeapi_more}")
    print(f"     CLOB has more data: {clob_more}")
    print(f"     Similar coverage: {similar}")

# 6. Summary statistics
print(f"\n6. Summary statistics:")
if tracking['markets']:
    total_candlesticks = sum(m['candlesticks_count'] for m in tracking['markets'])
    avg_candlesticks = tracking.get('avg_candlesticks', total_candlesticks / len(tracking['markets']))
    print(f"   Total candlesticks pulled: {total_candlesticks:,}")
    print(f"   Average candlesticks per market: {avg_candlesticks:.1f}")

    # Distribution
    candlestick_counts = [m['candlesticks_count'] for m in tracking['markets']]
    candlestick_counts.sort()
    median_idx = len(candlestick_counts) // 2
    print(f"   Median candlesticks: {candlestick_counts[median_idx]}")
    print(f"   Min candlesticks: {min(candlestick_counts)}")
    print(f"   Max candlesticks: {max(candlestick_counts)}")

# File sizes
dome_size_mb = os.path.getsize(DOMEAPI_PRICE_FILE) / 1024 / 1024
clob_size_mb = os.path.getsize(CLOB_PRICE_FILE) / 1024 / 1024
print(f"\n   File sizes:")
print(f"     DomeAPI price file: {dome_size_mb:.1f} MB")
print(f"     CLOB price file: {clob_size_mb:.1f} MB")

# Generate report
print(f"\n{'=' * 80}")
print("GENERATING REPORT")
print(f"{'=' * 80}")

report = f"""
================================================================================
DOMEAPI FULL MIGRATION VERIFICATION REPORT
================================================================================

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

OVERVIEW
--------
Total Polymarket markets: {len(pm_markets):,}
Markets with trading_close_time: {len(pm_with_close):,} ({len(pm_with_close)/len(pm_markets)*100:.1f}%)
Markets processed: {total_processed:,}

RESULTS
-------
Successfully pulled: {len(tracking['markets']):,} ({success_rate:.1f}%)
Failed pulls: {len(errors_df):,} ({100-success_rate:.1f}%)
Average candlesticks per market: {tracking.get('avg_candlesticks', 0):.1f}

SUCCESS BY YEAR
---------------
"""

for year in sorted(merged_success['year'].dropna().unique()):
    success_count = len(merged_success[merged_success['year'] == year])
    error_count = len(merged_errors[merged_errors['year'] == year])
    total_year = success_count + error_count
    if total_year > 0:
        year_success_rate = (success_count / total_year) * 100
        report += f"{int(year)}: {success_count}/{total_year} markets ({year_success_rate:.1f}% success)\n"

report += f"""
ERROR BREAKDOWN
---------------
"""

for error, count in error_counts.items():
    percent = (count / len(errors_df)) * 100
    report += f"{error}: {count} ({percent:.1f}%)\n"

report += f"""

COMPARISON WITH CLOB
--------------------
Tokens in both datasets: {len(common_tokens):,}
Tokens only in DomeAPI: {len(set(domeapi_prices.keys()) - set(clob_prices.keys())):,}
Tokens only in CLOB: {len(set(clob_prices.keys()) - set(domeapi_prices.keys())):,}

Coverage comparison (sample):
- DomeAPI has more data: {domeapi_more} tokens
- CLOB has more data: {clob_more} tokens
- Similar coverage: {similar} tokens

FILE SIZES
----------
DomeAPI price file: {dome_size_mb:.1f} MB ({len(domeapi_prices):,} tokens)
CLOB price file: {clob_size_mb:.1f} MB ({len(clob_prices):,} tokens)

FILES CREATED
-------------
- {DOMEAPI_PRICE_FILE}
- {DOMEAPI_TRACKING_FILE}
- {DOMEAPI_ERROR_FILE}

CONCLUSION
----------
{"✓ Migration successful!" if success_rate >= 85 else "⚠ Lower than expected success rate"}
Expected: 90-95% success rate
Actual: {success_rate:.1f}% success rate

================================================================================
END OF REPORT
================================================================================
"""

# Save report
with open(REPORT_FILE, 'w') as f:
    f.write(report)

print(f"✓ Saved verification report to: {REPORT_FILE}")

# Print summary
print(f"\n{'=' * 80}")
print("VERIFICATION COMPLETE")
print(f"{'=' * 80}")
print(f"\n✓ {len(tracking['markets']):,} markets successfully migrated")
print(f"✗ {len(errors_df):,} markets failed")
print(f"\nSuccess rate: {success_rate:.1f}%")
print(f"Expected: 90-95%")

if success_rate >= 85:
    print(f"\n✓ Migration successful!")
else:
    print(f"\n⚠ Success rate lower than expected - review error log")

print(f"\n{'=' * 80}")
print(f"Report saved to: {REPORT_FILE}")
print(f"{'=' * 80}")

print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
