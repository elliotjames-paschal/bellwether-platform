#!/usr/bin/env python3
"""
Prediction vs Volume Scatterplots

Creates scatterplots showing the relationship between prediction confidence
(price near 0 or 1) and trading volume.

Output:
- graphs/combined/prediction_vs_volume_polymarket_all.png
- graphs/combined/prediction_vs_volume_kalshi_all.png
- graphs/combined/prediction_vs_volume_polymarket_electoral.png
- graphs/combined/prediction_vs_volume_kalshi_electoral.png
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from scipy import stats
import os

from config import BASE_DIR, DATA_DIR, get_latest_file
from paper_config import load_master_csv, load_prediction_accuracy, PAPER_GRAPHS_DIR

# Paths
GRAPHS_DIR = PAPER_GRAPHS_DIR
GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 70)
print("PREDICTION VS VOLUME SCATTERPLOTS")
print("=" * 70)

# Load master CSV for volume and category data
print(f"\nLoading master CSV...")
df_master = load_master_csv()
print(f"  Loaded {len(df_master):,} markets")

# Determine volume column
volume_col = 'volume_usd' if 'volume_usd' in df_master.columns else 'volume'

# Create market metadata lookup
pm_meta = df_master[df_master['platform'] == 'Polymarket'][['market_id', volume_col, 'political_category']].copy()
pm_meta['market_id'] = pm_meta['market_id'].astype(str)

kalshi_meta = df_master[df_master['platform'] == 'Kalshi'][['market_id', volume_col, 'political_category']].copy()
kalshi_meta['market_id'] = kalshi_meta['market_id'].astype(str)


def create_scatterplot(pred_df, meta_df, platform, electoral_only=False, output_path=None):
    """Create a prediction vs volume scatterplot."""

    # Filter to 1 day before close
    df = pred_df[pred_df['days_before_event'] == 1].copy()

    # Get market ID column
    id_col = 'market_id' if 'market_id' in df.columns else 'ticker'
    df['market_id'] = df[id_col].astype(str)

    # Merge with metadata
    df = df.merge(meta_df, on='market_id', how='inner')

    # Filter to electoral if requested
    if electoral_only:
        df = df[
            (df['political_category'].str.startswith('1.', na=False)) |
            (df['political_category'].str.contains('ELECTORAL', case=False, na=False))
        ]

    # Filter to valid data
    df = df[df[volume_col].notna() & (df[volume_col] > 0)]

    if len(df) == 0:
        print(f"  No data for {platform} {'electoral' if electoral_only else 'all'}")
        return

    print(f"  {platform} {'electoral' if electoral_only else 'all'}: {len(df):,} markets")

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))

    # Color by whether prediction was correct
    if 'actual_outcome' in df.columns:
        correct = (
            ((df['prediction_price'] > 0.5) & (df['actual_outcome'] == 1)) |
            ((df['prediction_price'] <= 0.5) & (df['actual_outcome'] == 0))
        )
        colors = ['#2ECC71' if c else '#E74C3C' for c in correct]
        alpha = 0.5
    else:
        colors = '#3498DB'
        alpha = 0.5

    # Scatterplot with log scale for volume
    scatter = ax.scatter(
        df['prediction_price'],
        df[volume_col],
        c=colors,
        alpha=alpha,
        s=30,
        edgecolors='none'
    )

    # Log scale for y-axis
    ax.set_yscale('log')

    # Add trend line (LOWESS or linear)
    try:
        # Bin data and calculate mean
        bins = np.linspace(0, 1, 21)
        df['price_bin'] = pd.cut(df['prediction_price'], bins=bins)
        binned = df.groupby('price_bin')[volume_col].median().dropna()

        if len(binned) > 3:
            bin_centers = [(b.left + b.right) / 2 for b in binned.index]
            ax.plot(bin_centers, binned.values, 'k-', linewidth=2, label='Median trend')
    except Exception:
        pass

    # Formatting
    ax.set_xlabel('Prediction Price (1 Day Before Resolution)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Trading Volume (USD, log scale)', fontsize=12, fontweight='bold')

    title = f'Prediction Confidence vs Trading Volume\n{platform}'
    if electoral_only:
        title += ' - Electoral Markets Only'
    else:
        title += ' - All Political Markets'

    ax.set_title(title, fontsize=14, fontweight='bold', pad=15)

    ax.set_xlim(-0.05, 1.05)
    ax.grid(True, alpha=0.3, linestyle='--')

    # Add legend for colors
    if 'actual_outcome' in df.columns:
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='#2ECC71', alpha=0.7, label='Correct prediction'),
            Patch(facecolor='#E74C3C', alpha=0.7, label='Incorrect prediction')
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

    # Add correlation annotation
    corr, p_value = stats.pearsonr(df['prediction_price'], np.log10(df[volume_col]))
    ax.annotate(f'Correlation (price vs log volume): {corr:.3f}',
                xy=(0.02, 0.02), xycoords='axes fraction',
                fontsize=10, ha='left', va='bottom',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
        print(f"  Saved: {output_path}")

    plt.close()


# Load and process Polymarket data
pm_pred = load_prediction_accuracy("polymarket")
if pm_pred is not None:
    print(f"\nProcessing Polymarket predictions...")

    create_scatterplot(
        pm_pred, pm_meta, 'Polymarket', electoral_only=False,
        output_path=GRAPHS_DIR / "prediction_vs_volume_polymarket_all.png"
    )
    create_scatterplot(
        pm_pred, pm_meta, 'Polymarket', electoral_only=True,
        output_path=GRAPHS_DIR / "prediction_vs_volume_polymarket_electoral.png"
    )
else:
    print("\nNo Polymarket prediction file found")

# Load and process Kalshi data
kalshi_pred = load_prediction_accuracy("kalshi")
if kalshi_pred is not None:
    print(f"\nProcessing Kalshi predictions...")

    create_scatterplot(
        kalshi_pred, kalshi_meta, 'Kalshi', electoral_only=False,
        output_path=GRAPHS_DIR / "prediction_vs_volume_kalshi_all.png"
    )
    create_scatterplot(
        kalshi_pred, kalshi_meta, 'Kalshi', electoral_only=True,
        output_path=GRAPHS_DIR / "prediction_vs_volume_kalshi_electoral.png"
    )
else:
    print("\nNo Kalshi prediction file found")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
