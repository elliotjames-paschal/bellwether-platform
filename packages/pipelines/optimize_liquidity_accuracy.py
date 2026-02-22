#!/usr/bin/env python3
"""
Optimize liquidity score weights to best predict market accuracy.

This script:
1. Loads per-market liquidity metrics and Brier scores
2. Tests different weight combinations for spread/depth
3. Finds optimal weights that maximize correlation with accuracy
4. Generates per-category thresholds
5. Outputs data for the website
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats
from scipy.optimize import minimize

# Paths
DATA_DIR = Path(__file__).parent.parent / "data"
WEBSITE_DATA_DIR = Path(__file__).parent.parent / "website" / "data"

def load_data():
    """Load and merge liquidity + accuracy data."""

    # Load liquidity metrics
    liquidity_df = pd.read_csv(DATA_DIR / "liquidity_metrics_by_market.csv")
    print(f"Loaded {len(liquidity_df)} markets with liquidity data")

    # Load accuracy data (Polymarket)
    pm_accuracy = pd.read_csv(DATA_DIR / "polymarket_prediction_accuracy_all_political.csv", low_memory=False)
    pm_accuracy_1d = pm_accuracy[pm_accuracy['days_before_event'] == 1].copy()
    pm_accuracy_1d = pm_accuracy_1d.drop_duplicates(subset=['market_id'], keep='first')
    pm_accuracy_1d['platform'] = 'Polymarket'
    print(f"Loaded {len(pm_accuracy_1d)} Polymarket markets with 1-day Brier scores")

    # Load accuracy data (Kalshi)
    kalshi_accuracy = pd.read_csv(DATA_DIR / "kalshi_prediction_accuracy_all_political.csv", low_memory=False)
    kalshi_accuracy_1d = kalshi_accuracy[kalshi_accuracy['days_before_event'] == 1].copy()
    # Kalshi uses 'ticker' instead of 'market_id'
    if 'ticker' in kalshi_accuracy_1d.columns and 'market_id' not in kalshi_accuracy_1d.columns:
        kalshi_accuracy_1d['market_id'] = kalshi_accuracy_1d['ticker']
    kalshi_accuracy_1d = kalshi_accuracy_1d.drop_duplicates(subset=['market_id'], keep='first')
    kalshi_accuracy_1d['platform'] = 'Kalshi'
    print(f"Loaded {len(kalshi_accuracy_1d)} Kalshi markets with 1-day Brier scores")

    # Combine accuracy data
    accuracy_1d = pd.concat([
        pm_accuracy_1d[['market_id', 'brier_score', 'actual_outcome', 'platform']],
        kalshi_accuracy_1d[['market_id', 'brier_score', 'actual_outcome', 'platform']]
    ], ignore_index=True)
    print(f"Combined: {len(accuracy_1d)} total accuracy records")

    # Merge on market_id
    liquidity_df['market_id'] = liquidity_df['market_id'].astype(str)
    accuracy_1d['market_id'] = accuracy_1d['market_id'].astype(str)

    merged = liquidity_df.merge(
        accuracy_1d[['market_id', 'brier_score', 'actual_outcome']],
        on='market_id',
        how='inner'
    )
    print(f"Merged: {len(merged)} markets with both liquidity and accuracy")

    # Clean category names
    merged['category_clean'] = merged['category'].str.replace(r'^\d+\.\s*', '', regex=True)

    return merged


def compute_liquidity_score(df, spread_weight, depth_weight, within_category=True):
    """
    Compute liquidity score for each market.

    Uses percentile ranks within each category (if within_category=True).
    Score = spread_weight * (1 - spread_percentile) + depth_weight * depth_percentile

    Higher score = more liquid
    """
    df = df.copy()

    if within_category:
        # Percentile rank within each category
        df['spread_pct'] = df.groupby('category_clean')['rel_spread_median'].rank(pct=True)
        df['depth_pct'] = df.groupby('category_clean')['depth_median'].rank(pct=True)
    else:
        # Global percentile rank
        df['spread_pct'] = df['rel_spread_median'].rank(pct=True)
        df['depth_pct'] = df['depth_median'].rank(pct=True)

    # Lower spread = better, so invert
    df['liquidity_score'] = spread_weight * (1 - df['spread_pct']) + depth_weight * df['depth_pct']

    return df


def evaluate_weights(weights, df):
    """
    Evaluate how well given weights predict accuracy.
    Returns negative correlation (for minimization).
    """
    spread_w, depth_w = weights

    # Normalize weights to sum to 1
    total = spread_w + depth_w
    if total == 0:
        return 0
    spread_w, depth_w = spread_w / total, depth_w / total

    df_scored = compute_liquidity_score(df, spread_w, depth_w)

    # Correlation between liquidity score and Brier score
    # We expect: higher liquidity -> lower Brier (negative correlation)
    # So we want to maximize the magnitude of negative correlation
    corr, _ = stats.pearsonr(df_scored['liquidity_score'], df_scored['brier_score'])

    # Return negative of absolute correlation (minimize -> maximize magnitude)
    # We want negative correlation, so return positive if corr is negative
    return corr  # Minimize this (most negative = best)


def grid_search_weights(df, resolution=20):
    """Grid search for optimal weights."""
    best_corr = float('inf')
    best_weights = (0.5, 0.5)

    results = []

    for spread_w in np.linspace(0, 1, resolution):
        for depth_w in np.linspace(0, 1, resolution):
            if spread_w + depth_w == 0:
                continue

            corr = evaluate_weights([spread_w, depth_w], df)
            results.append({
                'spread_weight': spread_w,
                'depth_weight': depth_w,
                'correlation': corr
            })

            if corr < best_corr:
                best_corr = corr
                best_weights = (spread_w, depth_w)

    # Normalize best weights
    total = best_weights[0] + best_weights[1]
    best_weights = (best_weights[0] / total, best_weights[1] / total)

    return best_weights, best_corr, pd.DataFrame(results)


def compute_category_thresholds(df, spread_weight, depth_weight, threshold_pct=25):
    """
    Compute per-category liquidity thresholds.

    Returns the liquidity score threshold (bottom X percentile) for each category.
    """
    df_scored = compute_liquidity_score(df, spread_weight, depth_weight)

    thresholds = {}
    stats_by_category = []

    for cat in df_scored['category_clean'].unique():
        cat_df = df_scored[df_scored['category_clean'] == cat]

        if len(cat_df) < 10:
            continue

        # Threshold at bottom X percentile
        threshold = np.percentile(cat_df['liquidity_score'], threshold_pct)

        # Split into low vs high liquidity
        low_liq = cat_df[cat_df['liquidity_score'] <= threshold]
        high_liq = cat_df[cat_df['liquidity_score'] > threshold]

        if len(low_liq) < 5 or len(high_liq) < 5:
            continue

        thresholds[cat] = {
            'threshold': threshold,
            'n_markets': len(cat_df),
            'n_below_threshold': len(low_liq),
            'brier_below': low_liq['brier_score'].mean(),
            'brier_above': high_liq['brier_score'].mean(),
            'accuracy_lift': (low_liq['brier_score'].mean() - high_liq['brier_score'].mean()) / high_liq['brier_score'].mean() * 100
        }

        stats_by_category.append({
            'category': cat,
            'n_markets': len(cat_df),
            'threshold': threshold,
            'low_liquidity_brier': low_liq['brier_score'].mean(),
            'high_liquidity_brier': high_liq['brier_score'].mean(),
            'n_low': len(low_liq),
            'n_high': len(high_liq)
        })

    return thresholds, pd.DataFrame(stats_by_category)


def generate_binned_data(df, metric_col, metric_name):
    """Generate binned data for a given metric (spread or depth percentile)."""

    n_bins = 50  # 2% each
    bin_edges = np.linspace(0, 1, n_bins + 1)
    bin_labels = [f"{int(bin_edges[i]*100)}-{int(bin_edges[i+1]*100)}%" for i in range(n_bins)]
    bin_centers = [(bin_edges[i] + bin_edges[i+1]) / 2 * 100 for i in range(n_bins)]

    df = df.copy()

    # For spread, lower is better so we invert (1 - pct) for consistent "higher = more liquid" interpretation
    # For depth, higher is better so we use pct directly
    if metric_name == 'spread':
        # Invert spread so higher percentile = more liquid (lower spread)
        df['metric_pct'] = 1 - df[metric_col].rank(pct=True)
    else:
        df['metric_pct'] = df[metric_col].rank(pct=True)

    category_lines = {}

    for cat in df['category_clean'].unique():
        cat_df = df[df['category_clean'] == cat].copy()

        if len(cat_df) < 20:
            continue

        # Compute within-category percentile
        if metric_name == 'spread':
            cat_df['cat_pct'] = 1 - cat_df[metric_col].rank(pct=True)
        else:
            cat_df['cat_pct'] = cat_df[metric_col].rank(pct=True)

        cat_df['liq_bin'] = pd.cut(cat_df['cat_pct'], bins=bin_edges, labels=bin_labels, include_lowest=True)

        bin_stats = cat_df.groupby('liq_bin', observed=True).agg({
            'brier_score': ['mean', 'count']
        }).reset_index()
        bin_stats.columns = ['bin', 'brier_mean', 'n']

        brier_values = []
        counts = []

        for label in bin_labels:
            row = bin_stats[bin_stats['bin'] == label]
            if len(row) > 0:
                brier_values.append(round(float(row['brier_mean'].iloc[0]), 4))
                counts.append(int(row['n'].iloc[0]))
            else:
                brier_values.append(None)
                counts.append(0)

        category_lines[cat] = {
            'brier': brier_values,
            'n': counts,
            'total_markets': len(cat_df)
        }

    # Overall
    df['liq_bin'] = pd.cut(df['metric_pct'], bins=bin_edges, labels=bin_labels, include_lowest=True)
    overall_stats = df.groupby('liq_bin', observed=True).agg({
        'brier_score': ['mean', 'count']
    }).reset_index()
    overall_stats.columns = ['bin', 'brier_mean', 'n']

    overall_brier = []
    overall_n = []
    for label in bin_labels:
        row = overall_stats[overall_stats['bin'] == label]
        if len(row) > 0:
            overall_brier.append(round(float(row['brier_mean'].iloc[0]), 4))
            overall_n.append(int(row['n'].iloc[0]))
        else:
            overall_brier.append(None)
            overall_n.append(0)

    # Compute correlation
    valid_mask = df['metric_pct'].notna() & df['brier_score'].notna()
    corr = stats.pearsonr(df.loc[valid_mask, 'metric_pct'], df.loc[valid_mask, 'brier_score'])[0]

    return {
        'correlation': round(float(corr), 3),
        'overall': {
            'brier': overall_brier,
            'n': overall_n,
            'total_markets': len(df)
        },
        'categories': category_lines
    }, bin_labels, bin_centers


def generate_website_data(df, spread_weight, depth_weight, thresholds, category_stats):
    """Generate JSON data for the website."""

    # Generate data for both spread and depth
    spread_data, bin_labels, bin_centers = generate_binned_data(df, 'rel_spread_median', 'spread')
    depth_data, _, _ = generate_binned_data(df, 'depth_median', 'depth')

    output = {
        'optimized_weights': {
            'spread': round(spread_weight, 3),
            'depth': round(depth_weight, 3)
        },
        'bin_labels': bin_labels,
        'bin_centers': bin_centers,
        'spread': spread_data,
        'depth': depth_data,
        'thresholds': {k: {
            'threshold': round(v['threshold'], 3),
            'brier_below': round(v['brier_below'], 4),
            'brier_above': round(v['brier_above'], 4),
            'accuracy_lift_pct': round(v['accuracy_lift'], 1)
        } for k, v in thresholds.items()}
    }

    return output


def main():
    print("=" * 60)
    print("LIQUIDITY ACCURACY OPTIMIZATION")
    print("=" * 60)

    # Load data
    df = load_data()

    # Filter to markets with valid data
    df = df.dropna(subset=['rel_spread_median', 'depth_median', 'brier_score'])
    df = df[df['rel_spread_median'] > 0]
    df = df[df['depth_median'] > 0]
    print(f"After filtering: {len(df)} markets")

    # Grid search for optimal weights
    print("\nOptimizing weights...")
    best_weights, best_corr, results_df = grid_search_weights(df, resolution=21)

    spread_w, depth_w = best_weights
    print(f"\nOptimal weights:")
    print(f"  Spread weight: {spread_w:.2f}")
    print(f"  Depth weight:  {depth_w:.2f}")
    print(f"  Correlation:   {best_corr:.3f}")

    # Compare to equal weights
    equal_corr = evaluate_weights([0.5, 0.5], df)
    print(f"\nEqual weights correlation: {equal_corr:.3f}")
    print(f"Improvement: {(abs(best_corr) - abs(equal_corr)) / abs(equal_corr) * 100:.1f}%")

    # Compute per-category thresholds
    print("\nComputing per-category thresholds...")
    thresholds, category_stats = compute_category_thresholds(df, spread_w, depth_w, threshold_pct=25)

    print("\nCategory Results (Low vs High Liquidity Brier Scores):")
    print("-" * 70)
    for _, row in category_stats.sort_values('low_liquidity_brier', ascending=False).iterrows():
        lift = (row['low_liquidity_brier'] - row['high_liquidity_brier']) / row['high_liquidity_brier'] * 100
        print(f"{row['category']:<25} Low: {row['low_liquidity_brier']:.3f}  High: {row['high_liquidity_brier']:.3f}  Lift: {lift:+.0f}%")

    # Generate website data
    print("\nGenerating website data...")
    website_data = generate_website_data(df, spread_w, depth_w, thresholds, category_stats)

    output_path = WEBSITE_DATA_DIR / "liquidity_accuracy_analysis.json"
    with open(output_path, 'w') as f:
        json.dump(website_data, f, indent=2)
    print(f"Saved to: {output_path}")

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)


if __name__ == "__main__":
    main()
