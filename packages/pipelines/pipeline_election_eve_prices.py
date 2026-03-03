#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Fetch Election Eve Prices
================================================================================

Part of the Bellwether Pipeline

This script:
1. Loads the master CSV and election_dates_lookup.csv
2. Joins to find all electoral markets with a known election date
3. For each market missing an election_eve_price, looks up the last
   daily close price before election day from local price JSON files
4. Updates the master CSV with the new election_eve_price column

The "election eve price" is the market's last recorded price before
00:00 UTC on election day. For US elections on Nov 5, this corresponds
to ~7-8 PM ET the night before — a natural "final forecast" cutoff.

This is the academically standard approach: UTC is unambiguous and
reproducible without assumptions about local time zones.

Usage:
    python pipeline_election_eve_prices.py [--force]

    --force    Re-fetch prices even for markets that already have one

Input:
    - data/combined_political_markets_with_electoral_details_UPDATED.csv
    - data/election_dates_lookup.csv
    - data/polymarket_all_political_prices_CORRECTED.json
    - data/kalshi_all_political_prices_CORRECTED_v3.json

Output:
    - Updates master CSV with election_eve_price column

================================================================================
"""

import pandas as pd
import numpy as np
import json
import os
import sys
import argparse
from datetime import datetime, timezone
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR, rotate_backups

BACKUP_DIR = DATA_DIR / "backups"
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
ELECTION_DATES_FILE = DATA_DIR / "election_dates_lookup.csv"

# Local price files
PM_PRICES_FILE = DATA_DIR / "polymarket_all_political_prices_CORRECTED.json"
KALSHI_PRICES_FILE = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# =============================================================================
# PRICE LOOKUP FUNCTIONS
# =============================================================================

def election_date_to_unix(date_str):
    """Convert election date string (YYYY-MM-DD) to Unix timestamp at UTC midnight."""
    if not isinstance(date_str, str) or not date_str.strip():
        return None
    try:
        dt = datetime.strptime(str(date_str).strip()[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return None


def lookup_pm_price(token_id, unix_ts, pm_local):
    """Look up the last Polymarket price before the given timestamp from local data."""
    token_str = str(token_id).split('.')[0]  # Remove .0 if float
    if token_str not in pm_local:
        return None

    prices = pm_local[token_str]
    best = None
    for pt in prices:
        if pt["t"] < unix_ts:
            best = pt
    if best is not None:
        return best["p"]
    return None


def lookup_kalshi_price(market_ticker, unix_ts, kal_local):
    """Look up the last Kalshi price before the given timestamp from local data."""
    if market_ticker not in kal_local:
        return None

    candles = kal_local[market_ticker]
    best = None
    for c in candles:
        if c.get("end_period_ts", c.get("t", 0)) <= unix_ts:
            price_data = c.get("price", {})
            close = price_data.get("close_dollars")
            if close is not None:
                best = float(close)
            else:
                bid = c.get("yes_bid", {}).get("close_dollars")
                if bid is not None:
                    best = float(bid)
    return best


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Fetch election eve prices from local price files")
    parser.add_argument("--force", action="store_true", help="Re-fetch all prices, even existing ones")
    args = parser.parse_args()

    print("=" * 70)
    print("PIPELINE: FETCH ELECTION EVE PRICES")
    print("=" * 70)
    log("Loading data...")

    # Load master CSV
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"  Master CSV: {len(df):,} markets")

    # Ensure column exists
    if "election_eve_price" not in df.columns:
        df["election_eve_price"] = np.nan

    # Load election dates
    dates = pd.read_csv(ELECTION_DATES_FILE)
    log(f"  Election dates: {len(dates)} elections")

    # Align is_primary types for clean merge (both to boolean)
    if "is_primary" in df.columns:
        df["is_primary"] = df["is_primary"].astype(str).str.strip().str.lower().eq("true")
    else:
        df["is_primary"] = False

    if "is_primary" in dates.columns:
        dates["is_primary"] = dates["is_primary"].astype(str).str.strip().str.lower().eq("true")

    # Track original index so we can write back to df after merge
    df["_orig_idx"] = df.index

    # Join election dates onto master CSV
    join_cols = ["country", "office", "location", "election_year", "is_primary"]
    df_with_dates = df.merge(
        dates[join_cols + ["election_date"]],
        on=join_cols,
        how="left",
        suffixes=("", "_lookup")
    )

    # Filter to electoral markets with a known election date
    has_date = df_with_dates["election_date"].notna()
    is_electoral = df_with_dates["election_year"].notna()
    electoral_with_date = df_with_dates[has_date & is_electoral].copy()

    log(f"  Electoral markets with election date: {len(electoral_with_date):,}")

    # Filter to markets that need fetching
    if args.force:
        to_fetch = electoral_with_date
        log(f"  --force: re-fetching all {len(to_fetch):,} markets")
    else:
        needs_price = electoral_with_date["election_eve_price"].isna()
        to_fetch = electoral_with_date[needs_price]
        already_have = len(electoral_with_date) - len(to_fetch)
        log(f"  Already have price: {already_have:,}")
        log(f"  Need to fetch: {len(to_fetch):,}")

    if len(to_fetch) == 0:
        log("\nNo markets need fetching. Done.")
        return

    # Split by platform
    pm_markets = to_fetch[to_fetch["platform"] == "Polymarket"]
    kalshi_markets = to_fetch[to_fetch["platform"] == "Kalshi"]
    log(f"\n  Polymarket to look up: {len(pm_markets):,}")
    log(f"  Kalshi to look up: {len(kalshi_markets):,}")

    # =========================================================================
    # Load local price files
    # =========================================================================
    pm_local = {}
    kal_local = {}

    if len(pm_markets) > 0 and PM_PRICES_FILE.exists():
        log(f"\nLoading Polymarket local prices...")
        with open(PM_PRICES_FILE) as f:
            pm_local = json.load(f)
        log(f"  Loaded {len(pm_local):,} markets from local file")

    if len(kalshi_markets) > 0 and KALSHI_PRICES_FILE.exists():
        log(f"\nLoading Kalshi local prices...")
        with open(KALSHI_PRICES_FILE) as f:
            kal_local = json.load(f)
        log(f"  Loaded {len(kal_local):,} markets from local file")

    # =========================================================================
    # Look up Polymarket prices
    # =========================================================================
    pm_success = 0
    pm_failed = 0

    if len(pm_markets) > 0:
        log(f"\nLooking up Polymarket prices...")

        for i, (idx, row) in enumerate(pm_markets.iterrows()):
            token_id = row.get("pm_token_id_yes")
            election_date = row["election_date"]

            if pd.isna(token_id):
                pm_failed += 1
                continue

            unix_ts = election_date_to_unix(election_date)
            if unix_ts is None:
                pm_failed += 1
                continue
            price = lookup_pm_price(token_id, unix_ts, pm_local)

            if price is not None:
                df.loc[row["_orig_idx"], "election_eve_price"] = price
                pm_success += 1
            else:
                pm_failed += 1

            if (i + 1) % 500 == 0 or (i + 1) == len(pm_markets):
                log(f"  PM progress: {i + 1}/{len(pm_markets)} "
                    f"(success: {pm_success}, failed: {pm_failed})")

    # =========================================================================
    # Look up Kalshi prices
    # =========================================================================
    kalshi_success = 0
    kalshi_failed = 0

    if len(kalshi_markets) > 0:
        log(f"\nLooking up Kalshi prices...")

        for i, (idx, row) in enumerate(kalshi_markets.iterrows()):
            market_ticker = row["market_id"]
            election_date = row["election_date"]

            if pd.isna(market_ticker):
                kalshi_failed += 1
                continue

            unix_ts = election_date_to_unix(election_date)
            if unix_ts is None:
                kalshi_failed += 1
                continue
            price = lookup_kalshi_price(str(market_ticker), unix_ts, kal_local)

            if price is not None:
                df.loc[row["_orig_idx"], "election_eve_price"] = price
                kalshi_success += 1
            else:
                kalshi_failed += 1

            if (i + 1) % 500 == 0 or (i + 1) == len(kalshi_markets):
                log(f"  Kalshi progress: {i + 1}/{len(kalshi_markets)} "
                    f"(success: {kalshi_success}, failed: {kalshi_failed})")

    # =========================================================================
    # Save results
    # =========================================================================
    total_success = pm_success + kalshi_success
    total_failed = pm_failed + kalshi_failed

    log(f"\n{'=' * 50}")
    log(f"RESULTS")
    log(f"{'=' * 50}")
    log(f"  Polymarket: {pm_success} success, {pm_failed} failed")
    log(f"  Kalshi:     {kalshi_success} success, {kalshi_failed} failed")
    log(f"  Total:      {total_success} success, {total_failed} still missing")

    # Drop internal tracking column before saving
    df.drop(columns=["_orig_idx"], inplace=True, errors="ignore")

    if total_success > 0:
        # Backup
        BACKUP_DIR.mkdir(exist_ok=True)
        backup_file = BACKUP_DIR / f"master_backup_eve_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(backup_file, index=False)
        log(f"\n  Created backup: {backup_file.name}")

        deleted = rotate_backups("master_backup_eve_prices_*.csv")
        if deleted > 0:
            log(f"  Rotated {deleted} old backup(s)")

        # Save master
        df.to_csv(MASTER_FILE, index=False)
        log(f"  Updated master CSV with {total_success} election eve prices")

        # Summary stats
        filled = df["election_eve_price"].notna().sum()
        log(f"  Total markets with election_eve_price: {filled:,} / {len(df):,}")
    else:
        log("\n  No prices found — master CSV not modified")

    log(f"\n{'=' * 70}")
    log("DONE")
    log(f"{'=' * 70}")


if __name__ == "__main__":
    main()
