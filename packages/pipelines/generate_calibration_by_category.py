#!/usr/bin/env python3
"""
Generate Calibration Comparison by Category

Creates a multi-panel figure showing calibration comparison
(truncated vs resolution prices) for each political category.
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
FIGURES_DIR = PROJECT_DIR / "figures"

def load_truncated_prices():
    """Load truncated prices from prediction accuracy files (both platforms)."""
    # Polymarket
    pm_file = DATA_DIR / "polymarket_prediction_accuracy_all_political.csv"
    pm_df = pd.read_csv(pm_file, dtype={'token_id': str, 'market_id': str})
    pm_1d = pm_df[pm_df['days_before_event'] == 1].copy()
    pm_1d['platform'] = 'polymarket'
    pm_1d['id'] = pm_1d['token_id'].astype(str)

    # Kalshi
    kalshi_file = DATA_DIR / "kalshi_prediction_accuracy_all_political.csv"
    kalshi_df = pd.read_csv(kalshi_file, dtype={'ticker': str})
    kalshi_1d = kalshi_df[kalshi_df['days_before_event'] == 1].copy()
    kalshi_1d['platform'] = 'kalshi'
    kalshi_1d['id'] = kalshi_1d['ticker'].astype(str)
    kalshi_1d['market_id'] = kalshi_1d['ticker'].astype(str)  # For winner matching

    return pd.concat([pm_1d, kalshi_1d], ignore_index=True)

def load_resolution_prices():
    """Load resolution prices from fetched data (both platforms)."""
    resolution_file = DATA_DIR / "resolution_prices.json"
    with open(resolution_file) as f:
        data = json.load(f)

    records = []
    for key, val in data.items():
        if val['platform'] == 'polymarket':
            records.append({
                'id': val['token_id'],
                'resolution_price': val['resolution_price']
            })
        elif val['platform'] == 'kalshi':
            records.append({
                'id': val['ticker'],
                'resolution_price': val['resolution_price']
            })
    return pd.DataFrame(records)

def compute_calibration_bins_paired(df, trunc_col, res_col, outcome_col='actual_outcome', num_bins=20):
    """Compute calibration bins for both truncated and resolution prices using same bin assignments."""
    df = df.dropna(subset=[trunc_col, res_col, outcome_col]).copy()
    if len(df) < 50:
        return pd.DataFrame(), pd.DataFrame()

    # Sort by truncated price and assign bins
    df_sorted = df.sort_values(trunc_col).reset_index(drop=True)
    samples_per_bin = max(1, len(df_sorted) // num_bins)
    df_sorted['bin'] = df_sorted.index // samples_per_bin
    df_sorted.loc[df_sorted['bin'] >= num_bins, 'bin'] = num_bins - 1

    # Compute stats for truncated
    trunc_stats = df_sorted.groupby('bin').agg({
        trunc_col: 'mean',
        outcome_col: ['mean', 'count']
    }).reset_index()
    trunc_stats.columns = ['bin', 'predicted', 'actual', 'count']

    # Compute stats for resolution (same bins)
    res_stats = df_sorted.groupby('bin').agg({
        res_col: 'mean',
        outcome_col: ['mean', 'count']
    }).reset_index()
    res_stats.columns = ['bin', 'predicted', 'actual', 'count']

    return trunc_stats, res_stats

def main():
    import sys
    winner_only = '--winner' in sys.argv

    # Parse platform filter
    platform_filter = None
    for arg in sys.argv[1:]:
        if arg.startswith('--platform='):
            platform_filter = arg.split('=')[1].lower()

    platform_label = platform_filter.capitalize() if platform_filter else "All"

    print("Loading data...")
    truncated_df = load_truncated_prices()
    resolution_df = load_resolution_prices()

    # Merge
    merged = truncated_df.merge(resolution_df[['id', 'resolution_price']], on='id', how='inner')
    merged = merged[merged['actual_outcome'].isin([0, 1])].copy()

    print(f"Matched {len(merged):,} predictions")

    # Filter by platform if specified
    if platform_filter:
        merged = merged[merged['platform'] == platform_filter].copy()
        print(f"Filtered to {platform_filter}: {len(merged):,}")

    # Filter to winner markets only if requested
    if winner_only:
        panel_a = pd.read_csv(DATA_DIR / "election_winner_panel_a_detailed.csv")
        # Panel A has market_ids - need to match against our data
        winner_market_ids = set(panel_a['market_id'].astype(str).unique())

        # merged has market_id column from truncated_df
        if 'market_id' in merged.columns:
            merged = merged[merged['market_id'].astype(str).isin(winner_market_ids)].copy()
        else:
            # Try matching via market_id_str if exists
            merged = merged[merged['market_id_str'].astype(str).isin(winner_market_ids)].copy()

        print(f"Filtered to winner markets: {len(merged):,} predictions")
        print(f"  (Panel A has {len(panel_a)} winner markets)")

    # Get categories
    categories = sorted(merged['category'].dropna().unique())
    print(f"Found {len(categories)} categories")

    # Create figure with subplots
    # Handle single category case specially for better layout
    if len(categories) == 1:
        n_cols = 1
        n_rows = 1
        fig, ax_single = plt.subplots(1, 1, figsize=(8, 8))
        axes = [ax_single]
    else:
        n_cols = 4
        n_rows = (len(categories) + n_cols - 1) // n_cols
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 4 * n_rows))
        axes = axes.flatten()

    for idx, category in enumerate(categories):
        ax = axes[idx]
        cat_data = merged[merged['category'] == category]

        if len(cat_data) < 50:
            ax.text(0.5, 0.5, f'Not enough data\n(n={len(cat_data)})',
                    ha='center', va='center', transform=ax.transAxes)
            ax.set_title(category.split('. ')[-1] if '. ' in category else category, fontsize=10)
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            continue

        # Create secondary y-axis for distribution
        ax2 = ax.twinx()

        # Add distribution histograms
        hist_bins = 25
        alpha_hist = 0.3

        trunc_hist, trunc_edges = np.histogram(cat_data['prediction_price'].dropna(), bins=hist_bins, range=(0, 1))
        res_hist, res_edges = np.histogram(cat_data['resolution_price'].dropna(), bins=hist_bins, range=(0, 1))

        ax2.bar(trunc_edges[:-1], trunc_hist, width=1/hist_bins, alpha=alpha_hist,
               color='#2563eb', align='edge')
        ax2.bar(res_edges[:-1], res_hist, width=1/hist_bins, alpha=alpha_hist,
               color='#dc2626', align='edge')

        max_count = max(trunc_hist.max(), res_hist.max())
        ax2.set_ylim(0, max_count * 1.3)
        ax2.tick_params(axis='y', labelsize=6, colors='gray')

        # Compute bins (paired - same bins for both)
        trunc_bins, res_bins = compute_calibration_bins_paired(cat_data, 'prediction_price', 'resolution_price', num_bins=20)

        # Perfect calibration line
        ax.plot([0, 1], [0, 1], 'k--', linewidth=1, alpha=0.5)

        # Scatter points
        if len(trunc_bins) > 0:
            ax.scatter(trunc_bins['predicted'], trunc_bins['actual'],
                      s=20, alpha=0.8, color='#2563eb', label='Truncated', zorder=5)
        if len(res_bins) > 0:
            ax.scatter(res_bins['predicted'], res_bins['actual'],
                      s=20, alpha=0.8, color='#dc2626', label='Resolution', zorder=5)

        # Calculate MAE
        trunc_mae = np.abs(trunc_bins['predicted'] - trunc_bins['actual']).mean() if len(trunc_bins) > 0 else 0
        res_mae = np.abs(res_bins['predicted'] - res_bins['actual']).mean() if len(res_bins) > 0 else 0

        # Title with category name (cleaned up)
        cat_name = category.split('. ')[-1] if '. ' in category else category
        ax.set_title(f'{cat_name}\n(n={len(cat_data):,})', fontsize=9)

        # Add MAE annotation
        ax.text(0.98, 0.02, f'T:{trunc_mae:.3f}\nR:{res_mae:.3f}',
                transform=ax.transAxes, fontsize=7, ha='right', va='bottom',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8), zorder=10)

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.3)

        if idx == 0:
            ax.legend(fontsize=7, loc='upper left')

    # Hide empty subplots
    for idx in range(len(categories), len(axes)):
        axes[idx].set_visible(False)

    # Add overall labels and title
    title_suffix = " (Winner Markets Only)" if winner_only else ""

    if len(categories) == 1:
        # Single panel - use standard axis labels
        axes[0].set_xlabel('Predicted Probability', fontsize=12)
        axes[0].set_ylabel('Actual Outcome Rate', fontsize=12)
        plt.suptitle(f'Calibration: Truncated vs Resolution Prices{title_suffix}', fontsize=14, fontweight='bold')
        plt.tight_layout()
    else:
        # Multi-panel - use figure-level labels
        fig.text(0.5, 0.02, 'Predicted Probability', ha='center', fontsize=12)
        fig.text(0.02, 0.5, 'Actual Outcome Rate', va='center', rotation='vertical', fontsize=12)
        plt.suptitle(f'Calibration by Category: Truncated vs Resolution Prices{title_suffix}', fontsize=14, fontweight='bold', y=0.98)
        plt.tight_layout(rect=[0.03, 0.03, 1, 0.96])

    # Build filename with optional platform and winner suffixes
    base_name = "calibration_by_category"
    if platform_filter:
        base_name += f"_{platform_filter}"
    if winner_only:
        base_name += "_winner"
    filename = f"{base_name}.png"
    output_path = FIGURES_DIR / filename
    plt.savefig(output_path, dpi=200, bbox_inches='tight', facecolor='white')
    plt.close()

    print(f"\nSaved: {output_path}")

    # Print summary
    print("\nCategory Summary (MAE):")
    print(f"{'Category':<25} {'N':>8} {'Bins':>6} {'Trunc MAE':>10} {'Res MAE':>10} {'Diff':>10}")
    print("-" * 75)

    for category in categories:
        cat_data = merged[merged['category'] == category]
        if len(cat_data) < 50:
            continue
        trunc_bins, res_bins = compute_calibration_bins_paired(cat_data, 'prediction_price', 'resolution_price', num_bins=20)
        trunc_mae = np.abs(trunc_bins['predicted'] - trunc_bins['actual']).mean() if len(trunc_bins) > 0 else 0
        res_mae = np.abs(res_bins['predicted'] - res_bins['actual']).mean() if len(res_bins) > 0 else 0
        cat_name = category.split('. ')[-1][:20] if '. ' in category else category[:20]
        n_bins = len(trunc_bins)
        print(f"{cat_name:<25} {len(cat_data):>8,} {n_bins:>6} {trunc_mae:>10.4f} {res_mae:>10.4f} {trunc_mae-res_mae:>+10.4f}")

if __name__ == "__main__":
    main()
