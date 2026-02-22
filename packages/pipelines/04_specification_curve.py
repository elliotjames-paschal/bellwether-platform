#!/usr/bin/env python3
"""
Specification Curve Analysis for Degrees of Freedom Paper

Generates 192 unique specifications by varying:
- Truncation: Conservative, Moderate, Aggressive, Resolution (4)
- Price: Spot, VWAP-1h, VWAP-3h, VWAP-6h, VWAP-24h, Midpoint (6)
- Volume Threshold: $0, $1K, $10K, $100K (4)
- Metric: Brier, Log-loss (2)

Total: 4 × 6 × 4 × 2 = 192 specifications

For each specification, computes accuracy metrics for both platforms
and determines which platform "wins" (has better score).

Output: output/specification_results.csv
"""

import sys
from datetime import datetime
from pathlib import Path
from itertools import product

import numpy as np
import pandas as pd

# Add scripts dir to path
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR

# Paths
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
COMPUTED_PRICES_FILE = OUTPUT_DIR / "computed_prices.csv"
OUTPUT_FILE = OUTPUT_DIR / "specification_results.csv"

# Also use prediction accuracy files for volume data
PM_PRED_FILE = DATA_DIR / "polymarket_prediction_accuracy_all_political.csv"
KALSHI_PRED_FILE = DATA_DIR / "kalshi_prediction_accuracy_all_political.csv"
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"

# Specification grid
TRUNCATIONS = ['Conservative', 'Moderate', 'Aggressive', 'Resolution']
PRICE_TYPES = ['spot', 'vwap_1h', 'vwap_3h', 'vwap_6h', 'vwap_24h', 'midpoint']
VOLUME_THRESHOLDS = [0, 1000, 10000, 100000]  # $0, $1K, $10K, $100K
METRICS = ['brier', 'logloss']


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def compute_brier_score(prices, outcomes):
    """Compute mean Brier score: (p - o)^2"""
    prices = np.array(prices)
    outcomes = np.array(outcomes)
    valid = ~np.isnan(prices) & ~np.isnan(outcomes)
    if valid.sum() == 0:
        return np.nan, 0
    return np.mean((prices[valid] - outcomes[valid]) ** 2), valid.sum()


def compute_logloss(prices, outcomes, eps=1e-15):
    """Compute mean log-loss: -[o*log(p) + (1-o)*log(1-p)]"""
    prices = np.array(prices)
    outcomes = np.array(outcomes)
    valid = ~np.isnan(prices) & ~np.isnan(outcomes)
    if valid.sum() == 0:
        return np.nan, 0

    p = np.clip(prices[valid], eps, 1 - eps)
    o = outcomes[valid]
    ll = -(o * np.log(p) + (1 - o) * np.log(1 - p))
    return np.mean(ll), valid.sum()


def main():
    log("=" * 70)
    log("SPECIFICATION CURVE ANALYSIS")
    log("=" * 70)

    # Load computed prices
    log("\nLoading computed prices...")
    if not COMPUTED_PRICES_FILE.exists():
        log(f"ERROR: {COMPUTED_PRICES_FILE} not found. Run 03_compute_prices.py first.")
        sys.exit(1)

    prices_df = pd.read_csv(COMPUTED_PRICES_FILE)
    log(f"  Loaded {len(prices_df):,} price records")

    # Load master data for volume information
    log("\nLoading volume data from master CSV...")
    master_df = pd.read_csv(MASTER_CSV, low_memory=False)

    # Create market -> volume lookup
    volume_lookup = {}
    for _, row in master_df.iterrows():
        mid = str(row['market_id'])
        vol = row.get('volume_usd', 0)
        if pd.notna(vol):
            volume_lookup[mid] = float(vol)

    log(f"  Volume data for {len(volume_lookup):,} markets")

    # Add volume to prices dataframe
    prices_df['volume_usd'] = prices_df['market_id'].astype(str).map(volume_lookup)
    prices_df['volume_usd'] = prices_df['volume_usd'].fillna(0)

    # Generate all specifications
    log("\nGenerating specification grid...")
    specs = list(product(TRUNCATIONS, PRICE_TYPES, VOLUME_THRESHOLDS, METRICS))
    log(f"  Total specifications: {len(specs)}")

    # Process each specification
    results = []

    for spec_id, (truncation, price_type, threshold, metric) in enumerate(specs):
        # Filter by truncation
        spec_df = prices_df[prices_df['truncation_label'] == truncation].copy()

        # Filter by volume threshold
        spec_df = spec_df[spec_df['volume_usd'] >= threshold]

        # Get price column
        price_col = price_type

        # Split by platform
        pm_df = spec_df[spec_df['platform'] == 'Polymarket']
        kalshi_df = spec_df[spec_df['platform'] == 'Kalshi']

        # Compute metrics
        if metric == 'brier':
            pm_score, pm_n = compute_brier_score(pm_df[price_col].values, pm_df['outcome'].values)
            k_score, k_n = compute_brier_score(kalshi_df[price_col].values, kalshi_df['outcome'].values)
        else:  # logloss
            pm_score, pm_n = compute_logloss(pm_df[price_col].values, pm_df['outcome'].values)
            k_score, k_n = compute_logloss(kalshi_df[price_col].values, kalshi_df['outcome'].values)

        # Determine winner (lower is better for both metrics)
        if pd.isna(pm_score) or pd.isna(k_score):
            pm_wins = np.nan
            winner = 'neither'
        elif pm_score < k_score:
            pm_wins = 1
            winner = 'Polymarket'
        elif k_score < pm_score:
            pm_wins = 0
            winner = 'Kalshi'
        else:
            pm_wins = 0.5
            winner = 'tie'

        results.append({
            'spec_id': spec_id,
            'truncation': truncation,
            'price_type': price_type,
            'threshold': threshold,
            'metric': metric,
            'pm_score': pm_score,
            'pm_n': pm_n,
            'k_score': k_score,
            'k_n': k_n,
            'pm_wins': pm_wins,
            'winner': winner,
            'score_diff': pm_score - k_score if not (pd.isna(pm_score) or pd.isna(k_score)) else np.nan
        })

        if (spec_id + 1) % 20 == 0:
            log(f"  Processed {spec_id + 1}/{len(specs)} specifications...")

    # Create output DataFrame
    results_df = pd.DataFrame(results)

    # Summary statistics
    log("\n" + "=" * 70)
    log("SUMMARY STATISTICS")
    log("=" * 70)

    valid_specs = results_df[results_df['pm_wins'].notna()]
    pm_win_rate = valid_specs['pm_wins'].mean() * 100

    log(f"\nOverall Results ({len(valid_specs)} valid specifications):")
    log(f"  Polymarket wins: {(valid_specs['pm_wins'] == 1).sum()} ({(valid_specs['pm_wins'] == 1).mean()*100:.1f}%)")
    log(f"  Kalshi wins: {(valid_specs['pm_wins'] == 0).sum()} ({(valid_specs['pm_wins'] == 0).mean()*100:.1f}%)")
    log(f"  Ties: {(valid_specs['pm_wins'] == 0.5).sum()}")

    # By truncation
    log("\nBy Truncation:")
    for trunc in TRUNCATIONS:
        subset = valid_specs[valid_specs['truncation'] == trunc]
        if len(subset) > 0:
            pm_pct = subset['pm_wins'].mean() * 100
            log(f"  {trunc}: PM wins {pm_pct:.1f}% (n={len(subset)})")

    # By price type
    log("\nBy Price Type:")
    for pt in PRICE_TYPES:
        subset = valid_specs[valid_specs['price_type'] == pt]
        if len(subset) > 0:
            pm_pct = subset['pm_wins'].mean() * 100
            log(f"  {pt}: PM wins {pm_pct:.1f}% (n={len(subset)})")

    # By threshold
    log("\nBy Volume Threshold:")
    for thresh in VOLUME_THRESHOLDS:
        subset = valid_specs[valid_specs['threshold'] == thresh]
        if len(subset) > 0:
            pm_pct = subset['pm_wins'].mean() * 100
            log(f"  ${thresh:,}: PM wins {pm_pct:.1f}% (n={len(subset)})")

    # By metric
    log("\nBy Metric:")
    for m in METRICS:
        subset = valid_specs[valid_specs['metric'] == m]
        if len(subset) > 0:
            pm_pct = subset['pm_wins'].mean() * 100
            log(f"  {m}: PM wins {pm_pct:.1f}% (n={len(subset)})")

    # Score ranges
    log("\nScore Ranges:")
    for m in METRICS:
        subset = valid_specs[valid_specs['metric'] == m]
        pm_scores = subset['pm_score'].dropna()
        k_scores = subset['k_score'].dropna()
        if len(pm_scores) > 0:
            log(f"  {m.upper()} - PM: {pm_scores.min():.4f} to {pm_scores.max():.4f}")
            log(f"  {m.upper()} - K:  {k_scores.min():.4f} to {k_scores.max():.4f}")

    # Save results
    log(f"\nSaving to {OUTPUT_FILE}...")
    results_df.to_csv(OUTPUT_FILE, index=False)

    log("\n" + "=" * 70)
    log("COMPLETE")
    log(f"  Output: {OUTPUT_FILE}")
    log(f"  Total specs: {len(results_df)}")
    log(f"  Valid specs: {len(valid_specs)}")
    log("=" * 70)


if __name__ == '__main__':
    main()
