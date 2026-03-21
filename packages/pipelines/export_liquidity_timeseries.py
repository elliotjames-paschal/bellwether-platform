#!/usr/bin/env python3
"""
Export Liquidity Time Series for Website

Reads daily aggregates from orderbook_summary.json to produce
a time series of spread and depth by platform.

Input:
    data/orderbook_summary.json

Output:
    website/data/liquidity_timeseries.json
"""

import numpy as np
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR

try:
    import orjson
    def _load_json(f):
        return orjson.loads(f.read())
except ImportError:
    def _load_json(f):
        return json.load(f)

SUMMARY_FILE = DATA_DIR / "orderbook_summary.json"
OUTPUT_FILE = BASE_DIR / "website" / "data" / "liquidity_timeseries.json"


def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Exporting liquidity time series...")

    if not SUMMARY_FILE.exists():
        print(f"   ERROR: {SUMMARY_FILE} not found.")
        return

    with open(SUMMARY_FILE, 'rb') as f:
        summary = _load_json(f)

    daily = summary.get('daily', {})
    print(f"   Loaded {len(daily)} days of daily data")

    if not daily:
        print("   No daily data found. Skipping.")
        return

    all_dates = sorted(daily.keys())

    # Build per-platform arrays
    pm_spreads = []
    pm_depths = []
    pm_counts = []
    k_spreads = []
    k_depths = []
    k_counts = []

    for d in all_dates:
        day = daily[d]

        pm = day.get('polymarket', {})
        pm_spreads.append(pm.get('spread_median'))
        pm_depths.append(pm.get('depth_median'))
        pm_counts.append(pm.get('n_snapshots', 0))

        k = day.get('kalshi', {})
        k_spreads.append(k.get('spread_median'))
        k_depths.append(k.get('depth_median'))
        k_counts.append(k.get('n_snapshots', 0))

    # Compute summary stats
    pm_valid_spreads = [s for s in pm_spreads if s is not None]
    k_valid_spreads = [s for s in k_spreads if s is not None]

    output = {
        'dates': all_dates,
        'polymarket': {
            'spread': pm_spreads,
            'depth': pm_depths,
            'n_snapshots': pm_counts
        },
        'kalshi': {
            'spread': k_spreads,
            'depth': k_depths,
            'n_snapshots': k_counts
        },
        'summary': {
            'date_range': f"{all_dates[0]} to {all_dates[-1]}" if all_dates else "",
            'n_days': len(all_dates),
            'pm_avg_spread': round(float(np.mean(pm_valid_spreads)), 2) if pm_valid_spreads else None,
            'kalshi_avg_spread': round(float(np.mean(k_valid_spreads)), 2) if k_valid_spreads else None
        }
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2, allow_nan=False)

    print(f"\n   Saved to {OUTPUT_FILE.name}")
    print(f"   Date range: {output['summary']['date_range'] or 'N/A'}")
    pm_spread = output['summary']['pm_avg_spread']
    k_spread = output['summary']['kalshi_avg_spread']
    print(f"   PM avg spread: {f'{pm_spread}%' if pm_spread is not None else 'N/A'}")
    print(f"   Kalshi avg spread: {f'{k_spread}%' if k_spread is not None else 'N/A'}")


if __name__ == "__main__":
    main()
