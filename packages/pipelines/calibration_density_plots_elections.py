#!/usr/bin/env python3
"""
Calibration Density Plot Visualizations - ELECTIONS ONLY
Creates two calibration analysis plots filtered to election markets only:
1. 1-Day Before Calibration with Quantile Bins (equal sample sizes)
2. Multi-Day Calibration Distribution (7 consecutive days, overlaid)

Uses the same color scheme as Polymarket/Kalshi pipeline for consistency.
"""

import sys
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for LaTeX
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

# Color scheme from Polymarket pipeline
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#34495E',
    'tertiary': '#7F8C8D',
    'light_gray': '#95A5A6',
    'lighter_gray': '#BDC3C7',
    'lightest_gray': '#D5DBDB',
    'dark': '#1a1a1a',
    'accent': '#546E7A'
}

# Day colors for multi-day plot (gradient from light to dark)
DAY_COLORS = ['#D5DBDB', '#BDC3C7', '#95A5A6', '#7F8C8D', '#546E7A', '#34495E', '#2C3E50']

# Paths
from config import BASE_DIR, DATA_DIR, get_latest_file
from paper_config import load_prediction_accuracy, PAPER_GRAPHS_DIR

GRAPH_DIR = str(PAPER_GRAPHS_DIR)
os.makedirs(GRAPH_DIR, exist_ok=True)

print("="*80)
print("CALIBRATION DENSITY PLOT ANALYSIS - ELECTIONS ONLY (POLYMARKET + KALSHI)")
print("="*80)

# ============================================================================
# Load Data
# ============================================================================

print(f"\n📊 Loading Polymarket prediction data...")
df_pm = load_prediction_accuracy("polymarket")
if df_pm is None:
    print("ERROR: Polymarket prediction accuracy file not found. Run Brier score calculation first.")
    sys.exit(1)
df_pm['platform'] = 'Polymarket'
print(f"✓ Loaded {len(df_pm):,} Polymarket all-political prediction records")

# Filter to elections only (election_type is not 'NA')
df_pm = df_pm[(df_pm['election_type'].notna()) & (df_pm['election_type'] != 'NA')].copy()
print(f"✓ Filtered to {len(df_pm):,} Polymarket election prediction records")

# Load Kalshi data
print(f"\n📊 Loading Kalshi prediction data...")
df_kalshi = load_prediction_accuracy("kalshi")
if df_kalshi is None:
    print("ERROR: Kalshi prediction accuracy file not found. Run Brier score calculation first.")
    sys.exit(1)
df_kalshi['platform'] = 'Kalshi'
print(f"✓ Loaded {len(df_kalshi):,} Kalshi all-political prediction records")

# Filter to elections only (election_type is not 'NA')
df_kalshi = df_kalshi[(df_kalshi['election_type'].notna()) & (df_kalshi['election_type'] != 'NA')].copy()
print(f"✓ Filtered to {len(df_kalshi):,} Kalshi election prediction records")

# Combine both platforms
df = pd.concat([df_pm, df_kalshi], ignore_index=True)
print(f"\n✓ Combined: {len(df):,} total election prediction records")
print(f"  - Polymarket: {len(df_pm):,}")
print(f"  - Kalshi: {len(df_kalshi):,}")

# Convert prediction_price to float and filter valid data
df['prediction_price'] = pd.to_numeric(df['prediction_price'], errors='coerce')
df['actual_outcome'] = pd.to_numeric(df['actual_outcome'], errors='coerce')
df['days_before_event'] = pd.to_numeric(df['days_before_event'], errors='coerce')

# Filter to valid predictions (0-1 range)
df = df[(df['prediction_price'] >= 0) & (df['prediction_price'] <= 1)].copy()
df = df[(df['actual_outcome'].isin([0, 1]))].copy()

# Filter to only YES outcomes to avoid double-counting markets (Polymarket only)
# Kalshi doesn't have Yes/No pairs, so we keep all Kalshi records
df_pm_filtered = df[df['platform'] == 'Polymarket']
df_pm_filtered = df_pm_filtered[df_pm_filtered['outcome_name'] == 'Yes'].copy()
df_kalshi_filtered = df[df['platform'] == 'Kalshi'].copy()
df = pd.concat([df_pm_filtered, df_kalshi_filtered], ignore_index=True)

print(f"✓ Final filtered dataset: {len(df):,} valid election predictions")
print(f"  - Polymarket: {len(df_pm_filtered):,} (YES outcomes only)")
print(f"  - Kalshi: {len(df_kalshi_filtered):,}")
print(f"  Unique election markets: {df['market_id'].nunique() if 'market_id' in df.columns else len(df):,}")


# ============================================================================
# GRAPH 1: 1-Day Before Calibration with Quantile Bins
# ============================================================================

print(f"\n{'='*80}")
print("GRAPH 1: 1-Day Before Calibration (Quantile Bins) - ELECTIONS")
print(f"{'='*80}")

# Filter to 1 day before
df_1day = df[df['days_before_event'] == 1].copy()
print(f"Records at 1 day before: {len(df_1day):,}")

# Create bins with exactly equal sample sizes (manual approach)
num_bins = 100
# Sort by prediction_price and assign bin based on rank
df_1day = df_1day.sort_values('prediction_price').reset_index(drop=True)
samples_per_bin = len(df_1day) // num_bins
df_1day['bin'] = df_1day.index // samples_per_bin
# Handle any remainder by putting them in the last bin
df_1day.loc[df_1day['bin'] >= num_bins, 'bin'] = num_bins - 1

# Calculate bin statistics
bin_stats = df_1day.groupby('bin').agg({
    'prediction_price': ['mean', 'min', 'max', 'count'],
    'actual_outcome': 'mean'
}).reset_index()

bin_stats.columns = ['bin', 'predicted_prob', 'bin_min', 'bin_max', 'count', 'actual_freq']

print(f"\nBin Statistics:")
print(bin_stats[['predicted_prob', 'actual_freq', 'count']].to_string(index=False))

# Create the plot
fig, ax = plt.subplots(figsize=(12, 8))

# Plot perfect calibration line
ax.plot([0, 1], [0, 1], 'k--', linewidth=2, label='Perfect Calibration', alpha=0.5, zorder=1)

# Plot binned data with capped dot sizes to avoid massive dots
# Normalize sizes: min 50, max 200 to keep dots readable
min_size, max_size = 50, 200
counts = bin_stats['count'].values
normalized_sizes = min_size + (max_size - min_size) * (counts - counts.min()) / (counts.max() - counts.min() + 1e-6)

scatter = ax.scatter(bin_stats['predicted_prob'], bin_stats['actual_freq'],
                     s=normalized_sizes,
                     c=COLORS['primary'],
                     alpha=0.7,
                     edgecolors=COLORS['dark'],
                     linewidth=2,
                     zorder=3,
                     label='Quantile Bins (equal n)')

# Add grid
ax.grid(True, alpha=0.3, zorder=0)

# Labels and formatting
ax.set_xlabel('Predicted Probability', fontsize=14, fontweight='bold')
ax.set_ylabel('Actual Frequency', fontsize=14, fontweight='bold')
ax.set_title('Calibration Analysis: 1 Day Before Resolution (Elections Only)\n(Quantile Binning - Equal Sample Sizes)',
             fontsize=16, fontweight='bold', pad=20)
ax.set_xlim(-0.05, 1.05)
ax.set_ylim(-0.05, 1.05)
ax.legend(fontsize=12, loc='upper left', markerscale=0.2)

# Add text showing total samples
ax.text(0.98, 0.02, f'Total Election Predictions: {len(df_1day):,}',
        transform=ax.transAxes,
        fontsize=11,
        verticalalignment='bottom',
        horizontalalignment='right',
        bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

plt.tight_layout()
output_file_1 = f"{GRAPH_DIR}/calibration_1day_quantile_bins_elections.png"
plt.savefig(output_file_1, dpi=300, bbox_inches='tight')
print(f"\n✓ Saved: {output_file_1}")
plt.close()


# ============================================================================
# GRAPH 2: Calibration Distribution (1 Day Before Resolution) - ELECTIONS
# ============================================================================

print(f"\n{'='*80}")
print("GRAPH 2: Calibration Distribution (1 Day Before) - ELECTIONS")
print(f"{'='*80}")

# Filter to 1 day before
df_1day_dist = df[df['days_before_event'] == 1].copy()
print(f"Total election records at 1 day before: {len(df_1day_dist):,}")

# Create the plot
fig, ax = plt.subplots(figsize=(14, 8))

# Plot density for day 1
if len(df_1day_dist) > 0:
    # Create density plot
    sns.kdeplot(data=df_1day_dist['prediction_price'],
               fill=True,
               color=COLORS['primary'],
               alpha=0.5,
               linewidth=3,
               label=f'1 Day Before (n={len(df_1day_dist):,})',
               ax=ax)

    print(f"  1 Day Before: {len(df_1day_dist):,} predictions, "
          f"mean={df_1day_dist['prediction_price'].mean():.3f}, "
          f"std={df_1day_dist['prediction_price'].std():.3f}")

# Labels and formatting
ax.set_xlabel('Predicted Probability', fontsize=14, fontweight='bold')
ax.set_ylabel('Density', fontsize=14, fontweight='bold')
ax.set_title('Calibration Distribution 1 Day Before Resolution (Elections Only)\n(Showing Bimodality Pattern)',
             fontsize=16, fontweight='bold', pad=20)
ax.set_xlim(-0.05, 1.05)
ax.legend(fontsize=12, loc='upper center', framealpha=0.9)
ax.grid(True, alpha=0.3)

plt.tight_layout()
output_file_2 = f"{GRAPH_DIR}/calibration_timeseries_density_elections.png"
plt.savefig(output_file_2, dpi=300, bbox_inches='tight')
print(f"\n✓ Saved: {output_file_2}")
plt.close()


# ============================================================================
# Summary
# ============================================================================

print(f"\n{'='*80}")
print("SUMMARY")
print(f"{'='*80}")
print(f"Total valid election predictions: {len(df):,}")
print(f"Unique election markets: {df['market_id'].nunique():,}")
print(f"Predictions at 1 day before: {len(df_1day):,}")
print(f"\nGraphs saved to: {GRAPH_DIR}")
print(f"  1. calibration_1day_quantile_bins_elections.png")
print(f"  2. calibration_timeseries_density_elections.png")
print(f"{'='*80}")
