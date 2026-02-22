#!/usr/bin/env python3
"""
Scrape Kalshi politics page to extract election years for each market
"""

import pandas as pd
import json
import os
import time
import re
from pathlib import Path
from bs4 import BeautifulSoup
import requests
from datetime import datetime

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
INPUT_FILE = f"{DATA_DIR}/kalshi_official_with_electoral_details_FULL_with_years.csv"
OUTPUT_FILE = f"{DATA_DIR}/kalshi_official_with_electoral_details_SCRAPED.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/scraping_checkpoint.json"

# Configuration
DELAY_BETWEEN_REQUESTS = 2  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 5

print("=" * 80)
print("SCRAPING KALSHI FOR ELECTION YEARS")
print("=" * 80)

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

df = pd.read_csv(INPUT_FILE)
print(f"✓ Loaded {len(df):,} total markets")

# Filter to US elections
us_elections = df[df['country'] == 'United States'].copy()
print(f"✓ US elections: {len(us_elections):,}")

# Load checkpoint
processed_indices = set()
year_updates = {}

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint_data = json.load(f)
        processed_indices = set(checkpoint_data.get('processed_indices', []))
        year_updates = checkpoint_data.get('year_updates', {})
    print(f"✓ Loaded checkpoint: {len(processed_indices):,} markets already processed")

# Get remaining markets to process
remaining = us_elections[~us_elections.index.isin(processed_indices)].copy()

print(f"\nTotal US elections: {len(us_elections):,}")
print(f"Already processed: {len(processed_indices):,}")
print(f"Remaining: {len(remaining):,}")

def search_kalshi_api(title):
    """
    Search for a market using Kalshi API
    Returns list of matching markets with their details
    """
    base_url = "https://api.elections.kalshi.com/trade-api/v2/markets"

    try:
        # Get all markets (no text search, so we'll filter client-side)
        response = requests.get(base_url, params={
            "limit": 1000,
            "status": "all"  # Get both active and closed
        }, timeout=10)

        if response.status_code != 200:
            print(f"  ⚠ API returned status {response.status_code}")
            return []

        data = response.json()
        markets = data.get('markets', [])

        # Filter markets by title similarity
        # Clean title for comparison
        title_clean = title.lower().strip()
        matches = []

        for market in markets:
            market_title = market.get('title', '').lower().strip()

            # Exact match or very close match
            if title_clean in market_title or market_title in title_clean:
                matches.append(market)

        return matches

    except Exception as e:
        print(f"  ❌ Error searching API: {e}")
        return []

def extract_year_from_market(market_data):
    """
    Extract election year from market data
    """
    # Try to extract from close_time first
    close_time = market_data.get('close_time')
    if close_time:
        try:
            # Parse ISO format: "2024-11-05T23:59:00Z"
            dt = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
            year = dt.year
            # Election close time is typically on or shortly after election day
            return year
        except:
            pass

    # Try to extract from title
    title = market_data.get('title', '')
    year_match = re.search(r'\b(202[4-9]|203[0-9])\b', title)
    if year_match:
        return int(year_match.group(1))

    # Try to extract from subtitle
    subtitle = market_data.get('subtitle', '')
    year_match = re.search(r'\b(202[4-9]|203[0-9])\b', subtitle)
    if year_match:
        return int(year_match.group(1))

    return None

def scrape_market_year(title):
    """
    Scrape Kalshi to find election year for a given market title
    """
    # Search using API
    matches = search_kalshi_api(title)

    if not matches:
        return None, "no_match"

    if len(matches) > 1:
        # Multiple matches - try to find best match
        # Prefer exact title match
        exact_matches = [m for m in matches if m.get('title', '').strip() == title.strip()]
        if exact_matches:
            matches = exact_matches
        else:
            # Use first match
            print(f"  ⚠ Multiple matches found, using first")

    # Extract year from best match
    market = matches[0]
    year = extract_year_from_market(market)

    if year:
        return year, "success"
    else:
        return None, "year_not_found"

# Process markets
if len(remaining) == 0:
    print("\n✓ All markets already processed!")
else:
    print(f"\n{'=' * 80}")
    print("SCRAPING MARKETS")
    print(f"{'=' * 80}")

    total_markets = len(remaining)
    processed_count = 0
    success_count = 0
    failed_count = 0

    start_time = time.time()

    for idx, row in remaining.iterrows():
        processed_count += 1
        title = row['title']

        print(f"\n[{processed_count}/{total_markets}] {title[:80]}")

        # Calculate progress
        progress_pct = (processed_count / total_markets) * 100
        elapsed_time = time.time() - start_time
        if processed_count > 1:
            avg_time_per_market = elapsed_time / (processed_count - 1)
            remaining_markets = total_markets - processed_count
            est_time_remaining = avg_time_per_market * remaining_markets
            est_minutes = int(est_time_remaining / 60)
            est_seconds = int(est_time_remaining % 60)
            print(f"Progress: {progress_pct:.1f}% | Est. time remaining: {est_minutes}m {est_seconds}s")

        # Try with retries
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                year, status = scrape_market_year(title)

                # Store result
                year_updates[str(idx)] = {
                    'year': year,
                    'status': status,
                    'old_year': row['election_year'] if pd.notna(row['election_year']) else None
                }
                processed_indices.add(idx)

                # Log result
                if year:
                    old_year = row['election_year']
                    if pd.notna(old_year) and int(old_year) != year:
                        print(f"  ✓ Year updated: {int(old_year)} → {year}")
                    else:
                        print(f"  ✓ Year found: {year}")
                    success_count += 1
                else:
                    print(f"  ⚠ {status}")
                    failed_count += 1

                # Save checkpoint
                with open(CHECKPOINT_FILE, 'w') as f:
                    json.dump({
                        'processed_indices': list(processed_indices),
                        'year_updates': year_updates
                    }, f)

                success = True
                break

            except Exception as e:
                print(f"  ❌ Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        if not success:
            print(f"  ⚠ Failed after {MAX_RETRIES} attempts")
            failed_count += 1
            # Mark as processed even if failed
            processed_indices.add(idx)
            year_updates[str(idx)] = {
                'year': None,
                'status': 'failed',
                'old_year': row['election_year'] if pd.notna(row['election_year']) else None
            }

        # Rate limiting
        time.sleep(DELAY_BETWEEN_REQUESTS)

# Apply year updates to dataframe
print(f"\n{'=' * 80}")
print("APPLYING YEAR UPDATES")
print(f"{'=' * 80}")

years_changed = 0
years_unchanged = 0
years_not_found = 0

for idx_str, update in year_updates.items():
    idx = int(idx_str)
    new_year = update['year']
    old_year = update['old_year']

    if new_year is not None:
        if old_year != new_year or pd.isna(old_year):
            df.at[idx, 'election_year'] = new_year
            years_changed += 1
        else:
            years_unchanged += 1
    else:
        years_not_found += 1

print(f"\nYears changed: {years_changed:,}")
print(f"Years unchanged: {years_unchanged:,}")
print(f"Years not found: {years_not_found:,}")

# Save output
print(f"\nSaving to: {OUTPUT_FILE}")
df.to_csv(OUTPUT_FILE, index=False)
print(f"✓ Saved {len(df):,} markets with scraped years")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

us_elections_updated = df[df['country'] == 'United States'].copy()
still_missing = us_elections_updated['election_year'].isna().sum()

print(f"\nUS Elections: {len(us_elections_updated):,}")
print(f"Elections with years: {len(us_elections_updated) - still_missing:,}")
print(f"Elections still missing years: {still_missing:,}")

if len(us_elections_updated[us_elections_updated['election_year'].notna()]) > 0:
    print(f"\nYear distribution after scraping:")
    print(us_elections_updated['election_year'].value_counts().sort_index())

print(f"\n✓ COMPLETE")
