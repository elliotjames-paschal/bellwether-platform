#!/usr/bin/env python3
"""
Compute Price Variants for Degrees of Freedom Analysis

Computes multiple price types for each market:
- Spot price (last trade before cutoff)
- VWAP-1h, VWAP-3h, VWAP-6h, VWAP-24h (volume-weighted average prices)
- Midpoint (from order book bid/ask spread)

Uses existing prediction accuracy files for spot prices, and computes
additional price variants from historical price data and order books.

Output: output/computed_prices.csv
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Add scripts dir to path
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR, get_market_anchor_time

# Output paths
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "computed_prices.csv"

# Input files
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
PM_PRED_FILE = DATA_DIR / "polymarket_prediction_accuracy_all_political.csv"
KALSHI_PRED_FILE = DATA_DIR / "kalshi_prediction_accuracy_all_political.csv"
PM_PRICES_FILE = DATA_DIR / "polymarket_all_political_prices_CORRECTED.json"
PM_PRICES_V3_FILE = DATA_DIR / "polymarket_all_political_prices_CORRECTED_v3.json"
KALSHI_PRICES_FILE = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"
PM_ORDERBOOK_FILE = DATA_DIR / "orderbook_history_polymarket.json"
KALSHI_ORDERBOOK_FILE = DATA_DIR / "orderbook_history_kalshi.json"
ELECTION_DATES_FILE = DATA_DIR / "election_dates_lookup.csv"

# Truncation offsets (hours before anchor)
PM_TRUNCATION_OFFSETS = [-48, -24, -12]  # Conservative, Moderate, Aggressive
KALSHI_TRUNCATION_OFFSETS = [-24, -12, -3]

# VWAP windows (seconds)
VWAP_WINDOWS = {
    'vwap_1h': 3600,
    'vwap_3h': 3600 * 3,
    'vwap_6h': 3600 * 6,
    'vwap_24h': 3600 * 24,
}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def compute_vwap(price_history, end_ts, window_seconds, platform='polymarket'):
    """
    Compute VWAP from price history.

    Since we don't have trade-level volume data, we approximate VWAP
    by weighting prices by time intervals between observations.
    """
    if not price_history:
        return None

    start_ts = end_ts - window_seconds

    if platform == 'kalshi':
        # Kalshi format: {'end_period_ts': ts, 'price': {'close': p}}
        window_prices = [p for p in price_history if start_ts <= p.get('end_period_ts', 0) <= end_ts]
        if not window_prices:
            before = [p for p in price_history if p.get('end_period_ts', 0) <= end_ts]
            if before:
                last = max(before, key=lambda x: x.get('end_period_ts', 0))
                close_price = last.get('price', {}).get('close')
                if close_price is not None:
                    return float(close_price) / 100.0  # Kalshi prices are in cents
            return None
        prices = []
        for p in window_prices:
            close_price = p.get('price', {}).get('close')
            if close_price is not None:
                prices.append(float(close_price) / 100.0)
        return np.mean(prices) if prices else None
    else:
        # Polymarket format: {'t': timestamp, 'p': price}
        window_prices = [p for p in price_history if start_ts <= p['t'] <= end_ts]

        if not window_prices:
            # Fall back to last price before window
            before = [p for p in price_history if p['t'] <= end_ts]
            if before:
                return float(max(before, key=lambda x: x['t'])['p'])
            return None

        # Sort by timestamp
        window_prices = sorted(window_prices, key=lambda x: x['t'])

        # Simple average (without volume, this is our best approximation)
        prices = [float(p['p']) for p in window_prices]
        return np.mean(prices)


def get_midpoint_at_time(orderbook_data, target_ts):
    """
    Get midpoint price from order book at a specific time.
    Returns the midpoint from the closest snapshot before target_ts.
    """
    if not orderbook_data or 'metrics' not in orderbook_data:
        return None

    metrics = orderbook_data['metrics']
    if not metrics:
        return None

    # Find snapshots before target timestamp
    # Orderbook timestamps are in milliseconds
    target_ms = target_ts * 1000

    valid_snapshots = [m for m in metrics if m.get('timestamp', 0) <= target_ms]

    if not valid_snapshots:
        return None

    # Get closest snapshot
    closest = max(valid_snapshots, key=lambda x: x.get('timestamp', 0))

    midpoint = closest.get('midpoint')
    if midpoint is not None and 0 <= midpoint <= 1:
        return midpoint

    # Compute from bid/ask if midpoint not available
    bid = closest.get('best_bid')
    ask = closest.get('best_ask')
    if bid is not None and ask is not None and bid > 0 and ask <= 1:
        return (bid + ask) / 2

    return None


def get_spot_price_at_time(price_history, target_ts, platform='polymarket'):
    """Get the last trade price before target timestamp."""
    if not price_history:
        return None

    if platform == 'kalshi':
        # Kalshi format: {'end_period_ts': ts, 'price': {'close': p}}
        valid = [p for p in price_history if p.get('end_period_ts', 0) <= target_ts]
        if not valid:
            return None
        last = max(valid, key=lambda x: x.get('end_period_ts', 0))
        close_price = last.get('price', {}).get('close')
        if close_price is not None:
            return float(close_price) / 100.0  # Kalshi prices are in cents
        return None
    else:
        # Polymarket format: {'t': timestamp, 'p': price}
        valid = [p for p in price_history if p['t'] <= target_ts]
        if not valid:
            return None
        return float(max(valid, key=lambda x: x['t'])['p'])


def load_prices():
    """Load all price history files."""
    log("Loading price history files...")

    # Polymarket
    pm_prices = {}
    if PM_PRICES_FILE.exists():
        with open(PM_PRICES_FILE, 'r') as f:
            pm_main = json.load(f)
        log(f"  Loaded {len(pm_main):,} Polymarket tokens from CORRECTED")
    else:
        pm_main = {}

    if PM_PRICES_V3_FILE.exists():
        with open(PM_PRICES_V3_FILE, 'r') as f:
            pm_v3 = json.load(f)
        log(f"  Loaded {len(pm_v3):,} Polymarket tokens from v3")
    else:
        pm_v3 = {}

    # Merge: prefer CORRECTED if available
    all_tokens = set(pm_main.keys()) | set(pm_v3.keys())
    for token in all_tokens:
        main_data = pm_main.get(token, [])
        v3_data = pm_v3.get(token, [])
        pm_prices[token] = main_data if main_data else v3_data

    log(f"  Total Polymarket tokens: {len(pm_prices):,}")

    # Kalshi
    kalshi_prices = {}
    if KALSHI_PRICES_FILE.exists():
        with open(KALSHI_PRICES_FILE, 'r') as f:
            kalshi_prices = json.load(f)
        log(f"  Loaded {len(kalshi_prices):,} Kalshi markets")

    return pm_prices, kalshi_prices


def load_orderbooks():
    """Load order book history files."""
    log("Loading order book files...")

    pm_ob = {}
    if PM_ORDERBOOK_FILE.exists():
        with open(PM_ORDERBOOK_FILE, 'r') as f:
            pm_ob = json.load(f)
        log(f"  Loaded {len(pm_ob):,} Polymarket order books")

    kalshi_ob = {}
    if KALSHI_ORDERBOOK_FILE.exists():
        with open(KALSHI_ORDERBOOK_FILE, 'r') as f:
            kalshi_ob = json.load(f)
        log(f"  Loaded {len(kalshi_ob):,} Kalshi order books")

    return pm_ob, kalshi_ob


def load_election_dates():
    """Load election dates lookup and return a lookup function."""
    log("Loading election dates lookup...")

    if not ELECTION_DATES_FILE.exists():
        log("  Warning: election_dates_lookup.csv not found")
        return {}

    election_dates_df = pd.read_csv(ELECTION_DATES_FILE)
    log(f"  Loaded {len(election_dates_df):,} election date records")

    lookup = {}
    for _, row in election_dates_df.iterrows():
        key = (
            str(row['country']).strip(),
            str(row['office']).strip(),
            str(row['location']).strip(),
            int(row['election_year']) if pd.notna(row.get('election_year')) else None
        )
        dt = pd.to_datetime(row['election_date'])
        lookup[key] = dt.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)

    return lookup


def get_election_date(market_row, election_dates_lookup):
    """Look up election date for an electoral market."""
    country = str(market_row.get('country', '')).strip() if pd.notna(market_row.get('country')) else ''
    office = str(market_row.get('office', '')).strip() if pd.notna(market_row.get('office')) else ''
    location = str(market_row.get('location', '')).strip() if pd.notna(market_row.get('location')) else ''
    year = int(market_row.get('election_year')) if pd.notna(market_row.get('election_year')) else None

    key = (country, office, location, year)
    return election_dates_lookup.get(key)


def main():
    log("=" * 70)
    log("COMPUTING PRICE VARIANTS FOR SPECIFICATION CURVE ANALYSIS")
    log("=" * 70)

    # Load data
    pm_prices, kalshi_prices = load_prices()
    pm_ob, kalshi_ob = load_orderbooks()
    election_dates_lookup = load_election_dates()

    # Load prediction accuracy files (for list of resolved markets + outcomes)
    log("\nLoading prediction accuracy files...")
    pm_pred = pd.read_csv(PM_PRED_FILE, dtype={'token_id': str, 'market_id': str})
    kalshi_pred = pd.read_csv(KALSHI_PRED_FILE, dtype={'ticker': str, 'market_id': str})

    log(f"  Polymarket predictions: {len(pm_pred):,}")
    log(f"  Kalshi predictions: {len(kalshi_pred):,}")

    # Load master CSV for market metadata and anchor times
    log("\nLoading master CSV...")
    master_df = pd.read_csv(MASTER_CSV, low_memory=False)
    log(f"  Total markets: {len(master_df):,}")

    # Build lookup from master CSV: market_id -> row
    master_by_id = {}
    for _, mrow in master_df.iterrows():
        mid = str(mrow['market_id'])
        master_by_id[mid] = mrow

    # Helper to get election date for a market row
    def election_date_fn(market_row):
        return get_election_date(market_row, election_dates_lookup)

    # Get unique markets per platform with their reference info
    log("\nProcessing markets...")

    all_prices = []
    processed = 0
    errors = 0

    # Process Polymarket markets
    log("\nProcessing Polymarket markets...")
    pm_markets = pm_pred[pm_pred['days_before_event'] == 1].drop_duplicates(subset='token_id')

    for _, row in pm_markets.iterrows():
        try:
            token_id = str(row['token_id'])
            market_id = str(row['market_id'])

            # Get price history
            price_history = pm_prices.get(token_id, [])

            # Get anchor time from master CSV (trading_close_time or election date)
            master_row = master_by_id.get(market_id)
            if master_row is None:
                errors += 1
                continue
            category = str(master_row.get('political_category', ''))
            is_election = category.startswith('1.') or 'ELECTORAL' in category.upper()
            ref_dt = get_market_anchor_time(master_row, is_election, election_date_fn)
            if ref_dt is None:
                errors += 1
                continue
            if ref_dt.tzinfo is None:
                ref_dt = ref_dt.tz_localize('UTC')
            ref_ts = int(ref_dt.timestamp())

            # Get outcome
            outcome = row['actual_outcome']

            # For each truncation offset
            for offset in PM_TRUNCATION_OFFSETS:
                cutoff_ts = ref_ts + (offset * 3600)

                # Compute price variants
                spot = get_spot_price_at_time(price_history, cutoff_ts)
                vwap_1h = compute_vwap(price_history, cutoff_ts, VWAP_WINDOWS['vwap_1h'])
                vwap_3h = compute_vwap(price_history, cutoff_ts, VWAP_WINDOWS['vwap_3h'])
                vwap_6h = compute_vwap(price_history, cutoff_ts, VWAP_WINDOWS['vwap_6h'])
                vwap_24h = compute_vwap(price_history, cutoff_ts, VWAP_WINDOWS['vwap_24h'])

                # Midpoint from order book
                ob_data = pm_ob.get(market_id, {})
                midpoint = get_midpoint_at_time(ob_data, cutoff_ts)

                all_prices.append({
                    'market_id': market_id,
                    'token_id': token_id,
                    'platform': 'Polymarket',
                    'category': row.get('category', ''),
                    'truncation_hours': offset,
                    'truncation_label': 'Conservative' if offset == -48 else 'Moderate' if offset == -24 else 'Aggressive',
                    'spot': spot,
                    'vwap_1h': vwap_1h,
                    'vwap_3h': vwap_3h,
                    'vwap_6h': vwap_6h,
                    'vwap_24h': vwap_24h,
                    'midpoint': midpoint,
                    'outcome': outcome
                })

            # Also add resolution-time prices (offset = 0)
            spot_res = get_spot_price_at_time(price_history, ref_ts)
            vwap_1h_res = compute_vwap(price_history, ref_ts, VWAP_WINDOWS['vwap_1h'])
            ob_data = pm_ob.get(market_id, {})
            midpoint_res = get_midpoint_at_time(ob_data, ref_ts)

            all_prices.append({
                'market_id': market_id,
                'token_id': token_id,
                'platform': 'Polymarket',
                'category': row.get('category', ''),
                'truncation_hours': 0,
                'truncation_label': 'Resolution',
                'spot': spot_res,
                'vwap_1h': vwap_1h_res,
                'vwap_3h': compute_vwap(price_history, ref_ts, VWAP_WINDOWS['vwap_3h']),
                'vwap_6h': compute_vwap(price_history, ref_ts, VWAP_WINDOWS['vwap_6h']),
                'vwap_24h': compute_vwap(price_history, ref_ts, VWAP_WINDOWS['vwap_24h']),
                'midpoint': midpoint_res,
                'outcome': outcome
            })

            processed += 1
            if processed % 1000 == 0:
                log(f"  Processed {processed:,} Polymarket markets...")

        except Exception as e:
            errors += 1

    log(f"  Polymarket: {processed:,} processed, {errors:,} errors")

    # Process Kalshi markets
    log("\nProcessing Kalshi markets...")
    kalshi_markets = kalshi_pred[kalshi_pred['days_before_event'] == 1].drop_duplicates(subset='ticker')

    processed_k = 0
    errors_k = 0

    for _, row in kalshi_markets.iterrows():
        try:
            ticker = str(row['ticker']) if 'ticker' in row else str(row['market_id'])
            market_id = ticker

            # Get price history
            price_history = kalshi_prices.get(ticker, [])

            # Get anchor time from master CSV (trading_close_time or election date)
            master_row = master_by_id.get(market_id)
            if master_row is None:
                errors_k += 1
                continue
            category = str(master_row.get('political_category', ''))
            is_election = category.startswith('1.') or 'ELECTORAL' in category.upper()
            ref_dt = get_market_anchor_time(master_row, is_election, election_date_fn)
            if ref_dt is None:
                errors_k += 1
                continue
            if ref_dt.tzinfo is None:
                ref_dt = ref_dt.tz_localize('UTC')
            ref_ts = int(ref_dt.timestamp())

            # Get outcome
            outcome = row['actual_outcome']

            # For each truncation offset
            for offset in KALSHI_TRUNCATION_OFFSETS:
                cutoff_ts = ref_ts + (offset * 3600)

                # Compute price variants (Kalshi format)
                spot = get_spot_price_at_time(price_history, cutoff_ts, platform='kalshi')
                vwap_1h = compute_vwap(price_history, cutoff_ts, VWAP_WINDOWS['vwap_1h'], platform='kalshi')
                vwap_3h = compute_vwap(price_history, cutoff_ts, VWAP_WINDOWS['vwap_3h'], platform='kalshi')
                vwap_6h = compute_vwap(price_history, cutoff_ts, VWAP_WINDOWS['vwap_6h'], platform='kalshi')
                vwap_24h = compute_vwap(price_history, cutoff_ts, VWAP_WINDOWS['vwap_24h'], platform='kalshi')

                # Midpoint from order book
                ob_data = kalshi_ob.get(market_id, {})
                midpoint = get_midpoint_at_time(ob_data, cutoff_ts)

                all_prices.append({
                    'market_id': market_id,
                    'token_id': ticker,
                    'platform': 'Kalshi',
                    'category': row.get('category', ''),
                    'truncation_hours': offset,
                    'truncation_label': 'Conservative' if offset == -24 else 'Moderate' if offset == -12 else 'Aggressive',
                    'spot': spot,
                    'vwap_1h': vwap_1h,
                    'vwap_3h': vwap_3h,
                    'vwap_6h': vwap_6h,
                    'vwap_24h': vwap_24h,
                    'midpoint': midpoint,
                    'outcome': outcome
                })

            # Resolution-time prices
            spot_res = get_spot_price_at_time(price_history, ref_ts, platform='kalshi')
            ob_data = kalshi_ob.get(market_id, {})

            all_prices.append({
                'market_id': market_id,
                'token_id': ticker,
                'platform': 'Kalshi',
                'category': row.get('category', ''),
                'truncation_hours': 0,
                'truncation_label': 'Resolution',
                'spot': spot_res,
                'vwap_1h': compute_vwap(price_history, ref_ts, VWAP_WINDOWS['vwap_1h'], platform='kalshi'),
                'vwap_3h': compute_vwap(price_history, ref_ts, VWAP_WINDOWS['vwap_3h'], platform='kalshi'),
                'vwap_6h': compute_vwap(price_history, ref_ts, VWAP_WINDOWS['vwap_6h'], platform='kalshi'),
                'vwap_24h': compute_vwap(price_history, ref_ts, VWAP_WINDOWS['vwap_24h'], platform='kalshi'),
                'midpoint': get_midpoint_at_time(ob_data, ref_ts),
                'outcome': outcome
            })

            processed_k += 1
            if processed_k % 1000 == 0:
                log(f"  Processed {processed_k:,} Kalshi markets...")

        except Exception as e:
            errors_k += 1

    log(f"  Kalshi: {processed_k:,} processed, {errors_k:,} errors")

    # Create output DataFrame
    log("\nCreating output file...")
    df = pd.DataFrame(all_prices)

    # Summary stats
    log(f"\nOutput summary:")
    log(f"  Total rows: {len(df):,}")
    log(f"  Unique markets: {df['market_id'].nunique():,}")
    log(f"  By platform: PM={len(df[df['platform']=='Polymarket']):,}, K={len(df[df['platform']=='Kalshi']):,}")
    log(f"  By truncation: {df['truncation_label'].value_counts().to_dict()}")

    # Price coverage
    for price_col in ['spot', 'vwap_1h', 'vwap_24h', 'midpoint']:
        coverage = df[price_col].notna().mean() * 100
        log(f"  {price_col} coverage: {coverage:.1f}%")

    # Save
    df.to_csv(OUTPUT_FILE, index=False)
    log(f"\nSaved to {OUTPUT_FILE}")

    log("\n" + "=" * 70)
    log("COMPLETE")
    log("=" * 70)


if __name__ == '__main__':
    main()
