#!/usr/bin/env python3
"""
Calculate Liquidity Metrics from Orderbook History

Processes the raw orderbook snapshots into aggregate metrics per market.

Input:
    data/orderbook_history_polymarket.json
    data/orderbook_history_kalshi.json

Output:
    data/liquidity_metrics_by_market.csv
"""

import pandas as pd
import numpy as np
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR

PM_INPUT_FILE = DATA_DIR / "orderbook_history_polymarket.json"
KALSHI_INPUT_FILE = DATA_DIR / "orderbook_history_kalshi.json"
OUTPUT_FILE = DATA_DIR / "liquidity_metrics_by_market.csv"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def calculate_market_metrics(market_data):
    """
    Calculate aggregate metrics for a single market from its snapshots.

    Returns dict with:
        - Spread metrics (mean, median, std, min, max)
        - Depth metrics (mean, median, std)
        - Sample stats (n_snapshots, time_span)
    """
    metrics_list = market_data.get('metrics', [])

    if not metrics_list:
        return None

    # Extract arrays
    spreads = [m['spread'] for m in metrics_list if m.get('spread') is not None]
    rel_spreads = [m['relative_spread'] for m in metrics_list if m.get('relative_spread') is not None]
    total_depths = [m['total_depth'] for m in metrics_list if m.get('total_depth') is not None]
    bid_depths = [m['bid_depth'] for m in metrics_list if m.get('bid_depth') is not None]
    ask_depths = [m['ask_depth'] for m in metrics_list if m.get('ask_depth') is not None]
    midpoints = [m['midpoint'] for m in metrics_list if m.get('midpoint') is not None]

    # Filter non-finite values
    spreads = [s for s in spreads if np.isfinite(s)]
    rel_spreads = [s for s in rel_spreads if np.isfinite(s)]
    total_depths = [d for d in total_depths if np.isfinite(d)]

    timestamps = [m['timestamp'] for m in metrics_list if m.get('timestamp')]

    result = {
        'market_id': market_data.get('market_id', ''),
        'question': market_data.get('question', ''),
        'category': market_data.get('category', ''),
        'volume_usd': market_data.get('volume_usd', 0),
        'trading_close_time': market_data.get('trading_close_time', ''),
        'n_snapshots': len(metrics_list),
    }

    # Time span in hours
    if len(timestamps) >= 2:
        time_span_hours = (max(timestamps) - min(timestamps)) / 1000 / 3600
        result['time_span_hours'] = time_span_hours
    else:
        result['time_span_hours'] = 0

    # Spread metrics
    if spreads:
        result['spread_mean'] = np.mean(spreads)
        result['spread_median'] = np.median(spreads)
        result['spread_std'] = np.std(spreads) if len(spreads) > 1 else 0
        result['spread_min'] = np.min(spreads)
        result['spread_max'] = np.max(spreads)
    else:
        result['spread_mean'] = None
        result['spread_median'] = None
        result['spread_std'] = None
        result['spread_min'] = None
        result['spread_max'] = None

    # Relative spread metrics (as percentage)
    if rel_spreads:
        result['rel_spread_mean'] = np.mean(rel_spreads) * 100
        result['rel_spread_median'] = np.median(rel_spreads) * 100
        result['rel_spread_std'] = np.std(rel_spreads) * 100 if len(rel_spreads) > 1 else 0
    else:
        result['rel_spread_mean'] = None
        result['rel_spread_median'] = None
        result['rel_spread_std'] = None

    # Depth metrics
    if total_depths:
        result['depth_mean'] = np.mean(total_depths)
        result['depth_median'] = np.median(total_depths)
        result['depth_std'] = np.std(total_depths) if len(total_depths) > 1 else 0
        result['depth_max'] = np.max(total_depths)
    else:
        result['depth_mean'] = None
        result['depth_median'] = None
        result['depth_std'] = None
        result['depth_max'] = None

    # Bid/ask balance
    if bid_depths and ask_depths:
        result['bid_depth_mean'] = np.mean(bid_depths)
        result['ask_depth_mean'] = np.mean(ask_depths)
        # Imbalance: positive = more bids, negative = more asks
        imbalances = []
        for m in metrics_list:
            b, a = m.get('bid_depth'), m.get('ask_depth')
            if b is not None and a is not None and np.isfinite(b) and np.isfinite(a) and (b + a) > 0:
                imbalances.append((b - a) / (b + a))
        result['depth_imbalance_mean'] = np.mean(imbalances)
    else:
        result['bid_depth_mean'] = None
        result['ask_depth_mean'] = None
        result['depth_imbalance_mean'] = None

    # Price level (midpoint)
    if midpoints:
        result['price_mean'] = np.mean(midpoints)
        result['price_std'] = np.std(midpoints) if len(midpoints) > 1 else 0
    else:
        result['price_mean'] = None
        result['price_std'] = None

    return result


def main():
    log("=" * 60)
    log("CALCULATING LIQUIDITY METRICS")
    log("=" * 60)

    all_metrics = []

    # Process Polymarket
    log("\n1. Processing Polymarket orderbooks...")
    if PM_INPUT_FILE.exists():
        try:
            with open(PM_INPUT_FILE) as f:
                pm_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"   Error reading {PM_INPUT_FILE}: {e}")
            pm_data = {}

        log(f"   Loaded {len(pm_data):,} markets")

        for market_id, market_data in pm_data.items():
            market_data['market_id'] = market_id
            metrics = calculate_market_metrics(market_data)
            if metrics:
                metrics['platform'] = 'Polymarket'
                metrics['token_id'] = market_data.get('token_id', '')
                all_metrics.append(metrics)

        log(f"   Calculated metrics for {len([m for m in all_metrics if m['platform'] == 'Polymarket']):,} markets")
    else:
        log(f"   File not found: {PM_INPUT_FILE}")

    # Process Kalshi
    log("\n2. Processing Kalshi orderbooks...")
    if KALSHI_INPUT_FILE.exists():
        try:
            with open(KALSHI_INPUT_FILE) as f:
                kalshi_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"   Error reading {KALSHI_INPUT_FILE}: {e}")
            kalshi_data = {}

        log(f"   Loaded {len(kalshi_data):,} markets")

        for market_id, market_data in kalshi_data.items():
            market_data['market_id'] = market_id
            metrics = calculate_market_metrics(market_data)
            if metrics:
                metrics['platform'] = 'Kalshi'
                metrics['ticker'] = market_data.get('ticker', '')
                all_metrics.append(metrics)

        log(f"   Calculated metrics for {len([m for m in all_metrics if m['platform'] == 'Kalshi']):,} markets")
    else:
        log(f"   File not found: {KALSHI_INPUT_FILE}")

    # Create DataFrame
    log("\n3. Creating output DataFrame...")
    if not all_metrics:
        log("WARNING: No liquidity metrics calculated (no orderbook data found). Creating empty output.")
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
        df = pd.DataFrame(columns=col_order)
        df.to_csv(OUTPUT_FILE, index=False)
        log(f"   Saved empty file to {OUTPUT_FILE.name}")
        return

    df = pd.DataFrame(all_metrics)

    # Reorder columns
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

        # Spread stats
        spreads = pdf['spread_mean'].dropna()
        if len(spreads) > 0:
            log(f"  Spread (absolute):")
            log(f"    Mean: {spreads.mean():.4f}")
            log(f"    Median: {spreads.median():.4f}")
            log(f"    Std: {spreads.std():.4f}")

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
