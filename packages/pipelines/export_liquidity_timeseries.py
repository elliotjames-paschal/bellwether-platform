#!/usr/bin/env python3
"""
Export Liquidity Time Series for Website

Aggregates orderbook snapshots by day to show spread and depth over time.

Input:
    data/orderbook_history_polymarket.json
    data/orderbook_history_kalshi.json

Output:
    website/data/liquidity_timeseries.json
"""

import pandas as pd
import numpy as np
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR

PM_FILE = DATA_DIR / "orderbook_history_polymarket.json"
KALSHI_FILE = DATA_DIR / "orderbook_history_kalshi.json"
OUTPUT_FILE = BASE_DIR / "website" / "data" / "liquidity_timeseries.json"


def process_orderbook_file(filepath, platform):
    """Process orderbook JSON and aggregate by day."""
    if not filepath.exists():
        print(f"   Warning: {filepath.name} not found")
        return {}

    with open(filepath) as f:
        data = json.load(f)

    # Aggregate by day
    daily_data = defaultdict(lambda: {'spreads': [], 'depths': [], 'n_snapshots': 0})

    for market_id, market in data.items():
        for metric in market.get('metrics', []):
            ts = metric.get('timestamp', 0)
            if ts == 0:
                continue

            # Convert to date string
            date_str = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d')

            # Get spread and depth
            rel_spread = metric.get('relative_spread')
            depth = metric.get('total_depth')

            if rel_spread is not None and rel_spread > 0:
                daily_data[date_str]['spreads'].append(rel_spread * 100)  # Convert to %
            if depth is not None and depth > 0:
                daily_data[date_str]['depths'].append(depth)

            daily_data[date_str]['n_snapshots'] += 1

    # Calculate daily medians
    result = {}
    for date_str, day in sorted(daily_data.items()):
        if day['spreads'] and day['depths']:
            result[date_str] = {
                'spread_median': round(np.median(day['spreads']), 2),
                'spread_mean': round(np.mean(day['spreads']), 2),
                'depth_median': round(np.median(day['depths']), 0),
                'depth_mean': round(np.mean(day['depths']), 0),
                'n_snapshots': day['n_snapshots'],
                'n_spread_obs': len(day['spreads']),
                'n_depth_obs': len(day['depths'])
            }

    return result


def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Exporting liquidity time series...")

    # Process each platform
    print("   Processing Polymarket...")
    pm_daily = process_orderbook_file(PM_FILE, 'polymarket')
    print(f"   -> {len(pm_daily)} days")

    print("   Processing Kalshi...")
    kalshi_daily = process_orderbook_file(KALSHI_FILE, 'kalshi')
    print(f"   -> {len(kalshi_daily)} days")

    # Get all dates
    all_dates = sorted(set(pm_daily.keys()) | set(kalshi_daily.keys()))

    # Build output
    output = {
        'dates': all_dates,
        'polymarket': {
            'spread': [pm_daily.get(d, {}).get('spread_median') for d in all_dates],
            'depth': [pm_daily.get(d, {}).get('depth_median') for d in all_dates],
            'n_snapshots': [pm_daily.get(d, {}).get('n_snapshots', 0) for d in all_dates]
        },
        'kalshi': {
            'spread': [kalshi_daily.get(d, {}).get('spread_median') for d in all_dates],
            'depth': [kalshi_daily.get(d, {}).get('depth_median') for d in all_dates],
            'n_snapshots': [kalshi_daily.get(d, {}).get('n_snapshots', 0) for d in all_dates]
        },
        'summary': {
            'date_range': f"{all_dates[0]} to {all_dates[-1]}" if all_dates else "",
            'n_days': len(all_dates),
            'pm_avg_spread': round(np.mean([v['spread_median'] for v in pm_daily.values()]), 2) if pm_daily else None,
            'kalshi_avg_spread': round(np.mean([v['spread_median'] for v in kalshi_daily.values()]), 2) if kalshi_daily else None
        }
    }

    # Save
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n   Saved to {OUTPUT_FILE.name}")
    print(f"   Date range: {output['summary']['date_range']}")
    print(f"   PM avg spread: {output['summary']['pm_avg_spread']}%")
    print(f"   Kalshi avg spread: {output['summary']['kalshi_avg_spread']}%")


if __name__ == "__main__":
    main()
