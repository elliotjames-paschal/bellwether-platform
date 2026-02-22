#!/usr/bin/env python3
"""
B5: Add 403 new Polymarket markets to master dataset
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
NEW_MARKETS_FILE = f"{DATA_DIR}/polymarket_untagged_with_api_data.csv"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"

print("=" * 80)
print("ADDING 403 NEW POLYMARKET MARKETS TO MASTER DATASET")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load new markets
print(f"\n{'=' * 80}")
print("LOADING NEW POLYMARKET MARKETS")
print(f"{'=' * 80}")

new_df = pd.read_csv(NEW_MARKETS_FILE)
print(f"✓ Loaded {len(new_df):,} new markets")

# Load master file
print(f"\n{'=' * 80}")
print("LOADING MASTER FILE")
print(f"{'=' * 80}")

master_df = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded master file: {len(master_df):,} rows")

# Get existing Polymarket market IDs
existing_pm = master_df[master_df['platform'] == 'Polymarket']
existing_market_ids = set(existing_pm['market_id'].astype(str))
print(f"✓ Found {len(existing_market_ids):,} existing Polymarket markets in master")

# Check for duplicates
print(f"\n{'=' * 80}")
print("CHECKING FOR DUPLICATES")
print(f"{'=' * 80}")

new_market_ids = set(new_df['id'].astype(str))
duplicates = new_market_ids.intersection(existing_market_ids)

if duplicates:
    print(f"\n⚠️  Found {len(duplicates)} duplicate markets")
    print(f"Sample duplicate IDs: {list(duplicates)[:10]}")
    print(f"These will be skipped")
    # Filter out duplicates
    new_df = new_df[~new_df['id'].astype(str).isin(duplicates)].copy()
    print(f"✓ Filtered to {len(new_df):,} non-duplicate markets")
else:
    print(f"✓ No duplicates found - all {len(new_df):,} markets are new")

if len(new_df) == 0:
    print("\n⚠️  No new markets to add!")
    exit(0)

# Transform new markets to master schema
print(f"\n{'=' * 80}")
print("TRANSFORMING TO MASTER SCHEMA")
print(f"{'=' * 80}")

# Create DataFrame with master columns
master_markets = pd.DataFrame()

# Basic fields
master_markets['platform'] = 'Polymarket'
master_markets['market_id'] = new_df['id'].astype(str)
master_markets['question'] = new_df['question']
master_markets['is_closed'] = new_df['closed']
master_markets['political_category'] = new_df['political_category']

# Volume (already in USD)
master_markets['volume_usd'] = new_df['volume'].astype(float)

# Electoral details (only for electoral markets)
master_markets['country'] = new_df['country']
master_markets['office'] = new_df['office']
master_markets['location'] = new_df['location']
master_markets['election_year'] = new_df['election_year']
master_markets['is_primary'] = new_df['is_primary']
master_markets['election_type'] = new_df['election_type']

# Polymarket API fields
master_markets['pm_token_id_yes'] = new_df['pm_token_id_yes']
master_markets['pm_token_id_no'] = new_df['pm_token_id_no']
master_markets['pm_outcome_prices'] = new_df['pm_outcome_prices']
master_markets['pm_closed'] = new_df['pm_closed']
master_markets['pm_uma_resolution_status'] = new_df['pm_uma_resolution_status']

# Set pm_has_price_data based on whether token IDs exist
master_markets['pm_has_price_data'] = new_df['pm_token_id_yes'].notna()

# Fields to be filled later (set to None/NaN)
# These will be populated in subsequent steps (B7, B8)
master_markets['democrat_vote_share'] = np.nan
master_markets['republican_vote_share'] = np.nan
master_markets['other_vote_share'] = np.nan
master_markets['vote_share_source'] = None
master_markets['party_affiliation'] = None
master_markets['resolution_outcome'] = None
master_markets['winning_outcome'] = None
master_markets['scheduled_end_time'] = None
master_markets['trading_close_time'] = None

print(f"✓ Transformed {len(master_markets):,} markets to master schema")

# Summary by category
print(f"\nBreakdown by category:")
print(master_markets['political_category'].value_counts().head(10))

print(f"\nElectoral markets by type:")
electoral = master_markets[master_markets['political_category'] == '1. ELECTORAL']
print(f"  Total electoral: {len(electoral)}")
if len(electoral) > 0:
    print(electoral['election_type'].value_counts().head(10))

# Add to master
print(f"\n{'=' * 80}")
print("ADDING TO MASTER FILE")
print(f"{'=' * 80}")

# Backup master file
backup_file = MASTER_FILE.replace('.csv', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
master_df.to_csv(backup_file, index=False)
print(f"✓ Backed up master file to: {backup_file}")

# Append new markets
updated_df = pd.concat([master_df, master_markets], ignore_index=True)
updated_df.to_csv(MASTER_FILE, index=False)

print(f"✓ Added {len(master_markets):,} new Polymarket markets")
print(f"✓ Updated master file: {len(updated_df):,} total rows")
print(f"✓ Saved to: {MASTER_FILE}")

# Final summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

pm_total = updated_df[updated_df['platform'] == 'Polymarket']
print(f"\nPolymarket markets in master: {len(pm_total):,} (was {len(existing_pm):,})")
print(f"New markets added: {len(master_markets):,}")

print(f"\nNew markets breakdown:")
print(f"  Electoral: {len(electoral):,}")
print(f"  Non-electoral: {len(master_markets) - len(electoral):,}")

print(f"\n{'=' * 80}")
print("NEXT STEPS")
print(f"{'=' * 80}")

print(f"\nRemaining tasks:")
print(f"  B6: Verify/pull price data for new markets")
print(f"  B7: Add vote share data for electoral markets (manual)")
print(f"  B8: Populate party_affiliation")
print(f"  B9: Verify close_time accuracy")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
