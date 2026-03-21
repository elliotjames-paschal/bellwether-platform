#!/usr/bin/env python3
"""
Calculate Liquidity Metrics from Orderbook Summary

Derives per-market aggregate metrics from the orderbook summary's
running statistics (mean, std, min, max from sum/sum_sq/count).

Input:
    data/orderbook_summary.json

Output:
    data/liquidity_metrics_by_market.csv
"""

import pandas as pd
import numpy as np
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR
from category_utils import old_to_new_category

try:
    import orjson
    def _load_json(f):
        return orjson.loads(f.read())
except ImportError:
    def _load_json(f):
        return json.load(f)

SUMMARY_FILE = DATA_DIR / "orderbook_summary.json"
OUTPUT_FILE = DATA_DIR / "liquidity_metrics_by_market.csv"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def stat_mean(s):
    """Compute mean from running stat dict."""
    if s["n"] == 0:
        return None
    return s["sum"] / s["n"]


def stat_std(s):
    """Compute std from running stat dict (population std)."""
    if s["n"] < 2:
        return 0
    mean = s["sum"] / s["n"]
    variance = s["sum_sq"] / s["n"] - mean * mean
    # Guard against floating-point negative variance
    if variance < 0:
        variance = 0
    return math.sqrt(variance)


def derive_market_metrics(market_id, entry):
    """Derive CSV row from summary running stats."""
    category = entry.get('category', '')
    if category:
        category = old_to_new_category(category)
    else:
        category = 'MISC'

    result = {
        'market_id': market_id,
        'question': entry.get('question', ''),
        'category': category,
        'volume_usd': entry.get('volume_usd', 0),
        'trading_close_time': '',
        'n_snapshots': entry.get('n_snapshots', 0),
        'platform': 'Polymarket' if entry.get('platform') == 'polymarket' else 'Kalshi',
    }

    # ID fields
    if entry.get('platform') == 'polymarket':
        result['token_id'] = entry.get('id_value', '')
        result['ticker'] = ''
    else:
        result['token_id'] = ''
        result['ticker'] = entry.get('id_value', '')

    # Time span
    first_ts = entry.get('first_timestamp')
    last_ts = entry.get('last_timestamp')
    if first_ts and last_ts and last_ts > first_ts:
        result['time_span_hours'] = (last_ts - first_ts) / 1000 / 3600
    else:
        result['time_span_hours'] = 0

    # Spread metrics
    sp = entry.get('spread', {})
    if sp.get('n', 0) > 0:
        result['spread_mean'] = stat_mean(sp)
        result['spread_median'] = stat_mean(sp)  # approximate
        result['spread_std'] = stat_std(sp)
        result['spread_min'] = sp.get('min')
        result['spread_max'] = sp.get('max')
    else:
        result['spread_mean'] = None
        result['spread_median'] = None
        result['spread_std'] = None
        result['spread_min'] = None
        result['spread_max'] = None

    # Relative spread (as percentage)
    rs = entry.get('rel_spread', {})
    if rs.get('n', 0) > 0:
        result['rel_spread_mean'] = stat_mean(rs) * 100
        result['rel_spread_median'] = stat_mean(rs) * 100  # approximate
        result['rel_spread_std'] = stat_std(rs) * 100
    else:
        result['rel_spread_mean'] = None
        result['rel_spread_median'] = None
        result['rel_spread_std'] = None

    # Depth metrics
    dp = entry.get('depth', {})
    if dp.get('n', 0) > 0:
        result['depth_mean'] = stat_mean(dp)
        result['depth_median'] = stat_mean(dp)  # approximate
        result['depth_std'] = stat_std(dp)
        result['depth_max'] = dp.get('max')
    else:
        result['depth_mean'] = None
        result['depth_median'] = None
        result['depth_std'] = None
        result['depth_max'] = None

    # Bid/ask balance
    bd = entry.get('bid_depth', {})
    ad = entry.get('ask_depth', {})
    if bd.get('n', 0) > 0 and ad.get('n', 0) > 0:
        result['bid_depth_mean'] = stat_mean(bd)
        result['ask_depth_mean'] = stat_mean(ad)
        imb = entry.get('imbalance', {})
        result['depth_imbalance_mean'] = stat_mean(imb) if imb.get('n', 0) > 0 else None
    else:
        result['bid_depth_mean'] = None
        result['ask_depth_mean'] = None
        result['depth_imbalance_mean'] = None

    # Price level (midpoint)
    mp = entry.get('midpoint', {})
    if mp.get('n', 0) > 0:
        result['price_mean'] = stat_mean(mp)
        result['price_std'] = stat_std(mp)
    else:
        result['price_mean'] = None
        result['price_std'] = None

    return result


def main():
    log("=" * 60)
    log("CALCULATING LIQUIDITY METRICS (from summary)")
    log("=" * 60)

    # Load summary
    log("\n1. Loading orderbook summary...")
    if not SUMMARY_FILE.exists():
        log(f"   ERROR: {SUMMARY_FILE} not found.")
        log("   Run bootstrap_orderbook_summary.py or fetch_orderbooks.py first.")
        # Write empty CSV
        col_order = [
            'platform', 'market_id', 'token_id', 'ticker', 'question', 'category',
            'volume_usd', 'trading_close_time',
            'n_snapshots', 'time_span_hours',
            'spread_mean', 'spread_median', 'spread_std', 'spread_min', 'spread_max',
            'rel_spread_mean', 'rel_spread_median', 'rel_spread_std',
            'depth_mean', 'depth_median', 'depth_std', 'depth_max',
            'bid_depth_mean', 'ask_depth_mean', 'depth_imbalance_mean',
            'price_mean', 'price_std'
        ]
        pd.DataFrame(columns=col_order).to_csv(OUTPUT_FILE, index=False)
        return

    with open(SUMMARY_FILE, 'rb') as f:
        summary = _load_json(f)

    markets = summary.get('markets', {})
    log(f"   Loaded {len(markets):,} markets from summary")

    # Derive metrics
    log("\n2. Deriving per-market metrics...")
    all_metrics = []
    for market_id, entry in markets.items():
        if entry.get('n_snapshots', 0) == 0:
            continue
        row = derive_market_metrics(market_id, entry)
        all_metrics.append(row)

    log(f"   Derived metrics for {len(all_metrics):,} markets")

    if not all_metrics:
        log("WARNING: No markets with data. Creating empty output.")
        col_order = [
            'platform', 'market_id', 'token_id', 'ticker', 'question', 'category',
            'volume_usd', 'trading_close_time',
            'n_snapshots', 'time_span_hours',
            'spread_mean', 'spread_median', 'spread_std', 'spread_min', 'spread_max',
            'rel_spread_mean', 'rel_spread_median', 'rel_spread_std',
            'depth_mean', 'depth_median', 'depth_std', 'depth_max',
            'bid_depth_mean', 'ask_depth_mean', 'depth_imbalance_mean',
            'price_mean', 'price_std'
        ]
        pd.DataFrame(columns=col_order).to_csv(OUTPUT_FILE, index=False)
        return

    # Create DataFrame
    log("\n3. Creating output DataFrame...")
    df = pd.DataFrame(all_metrics)

    col_order = [
        'platform', 'market_id', 'token_id', 'ticker', 'question', 'category',
        'volume_usd', 'trading_close_time',
        'n_snapshots', 'time_span_hours',
        'spread_mean', 'spread_median', 'spread_std', 'spread_min', 'spread_max',
        'rel_spread_mean', 'rel_spread_median', 'rel_spread_std',
        'depth_mean', 'depth_median', 'depth_std', 'depth_max',
        'bid_depth_mean', 'ask_depth_mean', 'depth_imbalance_mean',
        'price_mean', 'price_std'
    ]
    existing_cols = [c for c in col_order if c in df.columns]
    df = df[existing_cols]

    # Save
    df.to_csv(OUTPUT_FILE, index=False)
    log(f"   Saved {len(df):,} markets to {OUTPUT_FILE.name}")

    # Summary statistics
    log("\n" + "=" * 60)
    log("SUMMARY STATISTICS")
    log("=" * 60)

    for platform in ['Polymarket', 'Kalshi']:
        pdf = df[df['platform'] == platform]
        if len(pdf) == 0:
            continue

        log(f"\n{platform} ({len(pdf):,} markets):")

        spreads = pdf['spread_mean'].dropna()
        if len(spreads) > 0:
            log(f"  Spread (absolute):")
            log(f"    Mean: {spreads.mean():.4f}")
            log(f"    Median: {spreads.median():.4f}")

        rel_spreads = pdf['rel_spread_mean'].dropna()
        if len(rel_spreads) > 0:
            log(f"  Spread (relative %):")
            log(f"    Mean: {rel_spreads.mean():.2f}%")
            log(f"    Median: {rel_spreads.median():.2f}%")

        depths = pdf['depth_mean'].dropna()
        if len(depths) > 0:
            log(f"  Depth:")
            log(f"    Mean: {depths.mean():,.0f}")
            log(f"    Median: {depths.median():,.0f}")

        snapshots = pdf['n_snapshots'].dropna()
        if len(snapshots) > 0:
            log(f"  Snapshots per market:")
            log(f"    Mean: {snapshots.mean():.1f}")
            log(f"    Median: {snapshots.median():.1f}")

    # Category breakdown
    log("\n" + "-" * 40)
    log("BY CATEGORY (all platforms):")

    cat_stats = df.groupby('category').agg({
        'market_id': 'count',
        'spread_mean': 'mean',
        'rel_spread_mean': 'mean',
        'depth_mean': 'mean'
    }).round(4)
    cat_stats.columns = ['n_markets', 'avg_spread', 'avg_rel_spread_%', 'avg_depth']
    cat_stats = cat_stats.sort_values('n_markets', ascending=False)

    log(cat_stats.to_string())

    log("\nDone!")


if __name__ == "__main__":
    main()
