#!/usr/bin/env python3
"""
Export Liquidity Metrics as JSON for Website

Transforms the liquidity CSV into JSON format for the dashboard.

Input:
    data/liquidity_metrics_by_market.csv

Output:
    website/data/liquidity_by_category.json
    website/data/liquidity_platform_comparison.json
    website/data/liquidity_spread_vs_volume.json
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
from category_utils import format_category_name

INPUT_FILE = DATA_DIR / "liquidity_metrics_by_market.csv"
WEBSITE_DATA_DIR = BASE_DIR / "website" / "data"


def clean_category(cat):
    """Get display name for a category code."""
    return format_category_name(cat)


def export_liquidity_by_category(df):
    """Export spread and depth by category for bar charts."""
    # Filter to valid data
    valid = df[df['rel_spread_mean'].notna() & df['depth_mean'].notna()].copy()

    # Group by category
    cat_stats = valid.groupby('category').agg({
        'market_id': 'count',
        'rel_spread_mean': ['mean', 'median'],
        'depth_mean': ['mean', 'median'],
        'volume_usd': 'sum'
    }).reset_index()

    cat_stats.columns = ['category', 'n_markets', 'spread_mean', 'spread_median',
                         'depth_mean', 'depth_median', 'total_volume']

    # Filter to categories with enough markets
    cat_stats = cat_stats[cat_stats['n_markets'] >= 10].copy()

    # Sort by spread (tightest first)
    cat_stats = cat_stats.sort_values('spread_median')

    # Also compute by platform
    pm_stats = valid[valid['platform'] == 'Polymarket'].groupby('category').agg({
        'rel_spread_mean': 'median',
        'depth_mean': 'median',
        'market_id': 'count'
    }).reset_index()
    pm_stats.columns = ['category', 'pm_spread', 'pm_depth', 'pm_count']

    k_stats = valid[valid['platform'] == 'Kalshi'].groupby('category').agg({
        'rel_spread_mean': 'median',
        'depth_mean': 'median',
        'market_id': 'count'
    }).reset_index()
    k_stats.columns = ['category', 'k_spread', 'k_depth', 'k_count']

    # Merge
    cat_stats = cat_stats.merge(pm_stats, on='category', how='left')
    cat_stats = cat_stats.merge(k_stats, on='category', how='left')

    # Build output
    output = {
        'categories': [clean_category(c) for c in cat_stats['category'].tolist()],
        'n_markets': cat_stats['n_markets'].tolist(),
        'spread_median': [round(x, 2) for x in cat_stats['spread_median'].tolist()],
        'depth_median': [round(x, 0) for x in cat_stats['depth_median'].tolist()],
        'polymarket': {
            'spread': [round(x, 2) if pd.notna(x) else None for x in cat_stats['pm_spread'].tolist()],
            'depth': [round(x, 0) if pd.notna(x) else None for x in cat_stats['pm_depth'].tolist()],
            'count': [int(x) if pd.notna(x) else 0 for x in cat_stats['pm_count'].tolist()]
        },
        'kalshi': {
            'spread': [round(x, 2) if pd.notna(x) else None for x in cat_stats['k_spread'].tolist()],
            'depth': [round(x, 0) if pd.notna(x) else None for x in cat_stats['k_depth'].tolist()],
            'count': [int(x) if pd.notna(x) else 0 for x in cat_stats['k_count'].tolist()]
        },
        'total_markets': int(valid['market_id'].count()),
        'total_volume': float(valid['volume_usd'].sum())
    }

    return output


def export_platform_comparison(df):
    """Export platform comparison data for histograms."""
    valid = df[df['rel_spread_mean'].notna()].copy()

    pm = valid[valid['platform'] == 'Polymarket']['rel_spread_mean']
    k = valid[valid['platform'] == 'Kalshi']['rel_spread_mean']

    # Create histogram bins - extend to 200% to capture full distribution
    bins = np.linspace(0, 200, 41)  # 0-200% in 5% bins

    pm_hist, _ = np.histogram(pm.clip(upper=200), bins=bins)
    k_hist, _ = np.histogram(k.clip(upper=200), bins=bins)

    # Depth comparison
    pm_depth = valid[valid['platform'] == 'Polymarket']['depth_mean'].dropna()
    k_depth = valid[valid['platform'] == 'Kalshi']['depth_mean'].dropna()

    # Log bins for depth
    depth_bins = np.logspace(0, 7, 51)  # 1 to 10M
    pm_depth_hist, _ = np.histogram(pm_depth.clip(lower=1, upper=1e7), bins=depth_bins)
    k_depth_hist, _ = np.histogram(k_depth.clip(lower=1, upper=1e7), bins=depth_bins)

    output = {
        'spread': {
            'bins': [round(b, 1) for b in bins[:-1].tolist()],
            'polymarket': pm_hist.tolist(),
            'kalshi': k_hist.tolist(),
            'pm_median': round(float(pm.median()), 2) if len(pm) > 0 else 0,
            'k_median': round(float(k.median()), 2) if len(k) > 0 else 0,
            'pm_count': int(len(pm)),
            'k_count': int(len(k))
        },
        'depth': {
            'bins': [round(b, 0) for b in depth_bins[:-1].tolist()],
            'polymarket': pm_depth_hist.tolist(),
            'kalshi': k_depth_hist.tolist(),
            'pm_median': round(float(pm_depth.median()), 0) if len(pm_depth) > 0 else 0,
            'k_median': round(float(k_depth.median()), 0) if len(k_depth) > 0 else 0,
            'pm_count': int(len(pm_depth)),
            'k_count': int(len(k_depth))
        }
    }

    return output


def export_spread_vs_volume(df):
    """Export spread vs volume scatter data."""
    valid = df[(df['rel_spread_mean'].notna()) & (df['volume_usd'] > 0)].copy()

    # Sample if too many points
    max_points = 2000

    output = {'polymarket': None, 'kalshi': None}

    for platform in ['Polymarket', 'Kalshi']:
        plat_df = valid[valid['platform'] == platform].copy()

        if len(plat_df) == 0:
            continue

        # Sample if needed
        if len(plat_df) > max_points:
            plat_df = plat_df.sample(n=max_points, random_state=42)

        # Calculate correlation
        log_vol = np.log10(plat_df['volume_usd'])
        corr = np.corrcoef(log_vol, plat_df['rel_spread_mean'])[0, 1]
        if not np.isfinite(corr):
            corr = 0.0

        if len(plat_df) < 2:
            continue

        # Create binned trend line
        plat_df['vol_bin'] = pd.qcut(plat_df['volume_usd'], q=min(20, len(plat_df)), duplicates='drop')
        trend = plat_df.groupby('vol_bin').agg({
            'volume_usd': 'median',
            'rel_spread_mean': 'median'
        }).reset_index()

        output[platform.lower()] = {
            'points': [
                {
                    'volume': round(float(row['volume_usd']), 2),
                    'spread': round(float(row['rel_spread_mean']), 2),
                    'category': clean_category(row['category']) if pd.notna(row.get('category')) else ''
                }
                for _, row in plat_df.iterrows()
            ],
            'trend': {
                'volume': [round(float(x), 2) for x in trend['volume_usd'].tolist()],
                'spread': [round(float(x), 2) for x in trend['rel_spread_mean'].tolist()]
            },
            'correlation': round(float(corr), 3),
            'n': int(len(plat_df))
        }

    return output


def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Exporting liquidity data for website...")

    # Load data
    if not INPUT_FILE.exists():
        print(f"   ERROR: Input file not found: {INPUT_FILE}")
        print("   Please run calculate_liquidity_metrics.py first.")
        sys.exit(1)

    df = pd.read_csv(INPUT_FILE)
    print(f"   Loaded {len(df):,} markets")

    # Ensure output directory exists
    WEBSITE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Export each dataset
    print("   Exporting liquidity_by_category.json...")
    cat_data = export_liquidity_by_category(df)
    with open(WEBSITE_DATA_DIR / "liquidity_by_category.json", 'w') as f:
        json.dump(cat_data, f, indent=2, allow_nan=False)

    print("   Exporting liquidity_platform_comparison.json...")
    platform_data = export_platform_comparison(df)
    with open(WEBSITE_DATA_DIR / "liquidity_platform_comparison.json", 'w') as f:
        json.dump(platform_data, f, indent=2, allow_nan=False)

    print("   Exporting liquidity_spread_vs_volume.json...")
    scatter_data = export_spread_vs_volume(df)
    with open(WEBSITE_DATA_DIR / "liquidity_spread_vs_volume.json", 'w') as f:
        json.dump(scatter_data, f, indent=2, allow_nan=False)

    print(f"\n   Done! Exported to {WEBSITE_DATA_DIR}")

    # Summary
    print(f"\n   Summary:")
    print(f"   - Categories with data: {len(cat_data['categories'])}")
    print(f"   - Total markets: {cat_data['total_markets']:,}")
    print(f"   - PM median spread: {platform_data['spread']['pm_median']:.1f}%")
    print(f"   - Kalshi median spread: {platform_data['spread']['k_median']:.1f}%")


if __name__ == "__main__":
    main()
