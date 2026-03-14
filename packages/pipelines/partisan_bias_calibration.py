#!/usr/bin/env python3
"""
Partisan Bias Calibration Plot Analysis

Uses Panel A elections (all elections per platform from election_winner_panel_a_detailed.csv)
to evaluate partisan bias:
- X-axis: Republican win probability (1 day before resolution)
- Y-axis: Percent of races where Republican actually won

If markets are unbiased, points should follow the 45-degree line.
Deviations suggest systematic bias favoring one party.
"""

import sys
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

COLORS = {
    'republican': '#E74C3C',
}

# Paths
from paper_config import PAPER_GRAPHS_DIR, PAPER_DATA_DIR

GRAPH_DIR = str(PAPER_GRAPHS_DIR)
os.makedirs(GRAPH_DIR, exist_ok=True)

print("="*80)
print("PARTISAN BIAS CALIBRATION ANALYSIS")
print("="*80)

# ============================================================================
# 1. Load Panel A elections (with party columns)
# ============================================================================

print("\n1. Loading Panel A elections (election_winner_panel_a_detailed.csv)...")

panel_a_path = f"{PAPER_DATA_DIR}/election_winner_panel_a_detailed.csv"
if not os.path.exists(panel_a_path):
    print(f"ERROR: {panel_a_path} not found. Run election_winner_markets_comparison.py first.")
    sys.exit(1)
panel_a = pd.read_csv(panel_a_path)
print(f"   Loaded {len(panel_a)} total elections (all platforms)")

# Derive republican_won from winning_party if not already present
if 'republican_won' not in panel_a.columns:
    panel_a['republican_won'] = (panel_a['winning_party'] == 'Republican').astype(int)
    # Rows with no winning_party should be NaN, not 0
    panel_a.loc[panel_a['winning_party'].isna(), 'republican_won'] = np.nan

# Split by platform
pm_data = panel_a[panel_a['platform'] == 'Polymarket'].copy()
kalshi_data = panel_a[panel_a['platform'] == 'Kalshi'].copy()

print(f"   Polymarket: {len(pm_data)} elections, winning_party assigned: {pm_data['winning_party'].notna().sum()}/{len(pm_data)}")
print(f"   Kalshi: {len(kalshi_data)} elections, winning_party assigned: {kalshi_data['winning_party'].notna().sum()}/{len(kalshi_data)}")

# ============================================================================
# 2. Calculate Republican win probability for each platform
# ============================================================================

print("\n2. Calculating Republican win probabilities...")

# Since winner_prediction is the probability assigned to the actual winner,
# we can calculate Republican probability directly:
# - If Republican won: republican_prob = winner_prediction
# - If Democrat won: republican_prob = 1 - winner_prediction
# This works for ALL elections without needing party extraction from question text.

def calc_republican_prob_from_outcome(winner_prediction, republican_won):
    """Convert winner prediction to Republican win probability based on actual outcome."""
    if pd.isna(winner_prediction) or pd.isna(republican_won):
        return np.nan
    if republican_won == 1:
        return winner_prediction
    else:
        return 1 - winner_prediction

# Calculate republican probability for each platform
pm_data['republican_prob'] = pm_data.apply(
    lambda row: calc_republican_prob_from_outcome(row['winner_prediction'], row['republican_won']), axis=1
)
kalshi_data['republican_prob'] = kalshi_data.apply(
    lambda row: calc_republican_prob_from_outcome(row['winner_prediction'], row['republican_won']), axis=1
)

# Filter to elections with valid republican probability
pm_valid = pm_data[pd.notna(pm_data['republican_prob'])].copy()
kalshi_valid = kalshi_data[pd.notna(kalshi_data['republican_prob'])].copy()

print(f"   ✓ Polymarket: {len(pm_valid)} elections")
print(f"   ✓ Kalshi: {len(kalshi_valid)} elections")
print(f"   ✓ Republicans won in Polymarket: {pm_valid['republican_won'].sum()} of {len(pm_valid)} races")
print(f"   ✓ Republicans won in Kalshi: {kalshi_valid['republican_won'].sum()} of {len(kalshi_valid)} races")

# ============================================================================
# 3. Create calibration plots by platform
# ============================================================================

print("\n3. Creating calibration plots...")

platform_configs = [
    ('Polymarket', pm_valid, 'republican_prob'),
    ('Kalshi', kalshi_valid, 'republican_prob')
]

for platform, data, prob_col in platform_configs:
    if len(data) < 10:
        print(f"   ⚠ Skipping {platform} (only {len(data)} elections)")
        continue

    n_elections = len(data)

    # Quantile binning with equal sample sizes
    n_bins = min(10, len(data) // 5)
    data = data.copy()
    data['bin'] = pd.qcut(data[prob_col], q=n_bins, duplicates='drop')

    # Calculate stats per bin
    bins_stats = data.groupby('bin', observed=True).agg({
        prob_col: 'mean',
        'republican_won': ['mean', 'count']
    }).reset_index()

    bins_stats.columns = ['bin', 'mean_prob', 'actual_rate', 'count']

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))

    # Scatter plot
    ax.scatter(bins_stats['mean_prob'], bins_stats['actual_rate'],
               s=bins_stats['count']*20, alpha=0.6,
               color=COLORS['republican'], edgecolors='white', linewidth=1.5,
               label=f'{platform} (N={n_elections} elections)')

    # Perfect calibration line
    ax.plot([0, 1], [0, 1], 'k--', linewidth=2, alpha=0.3, label='Perfect Calibration')

    ax.set_xlabel('Predicted Republican Win Probability', fontsize=14, fontweight='bold')
    ax.set_ylabel('Actual Republican Win Rate', fontsize=14, fontweight='bold')
    ax.set_title(f'{platform}: Partisan Calibration (Panel A All Elections)\n(1 Day Before Resolution)',
                 fontsize=16, fontweight='bold', pad=20)

    ax.set_xlim(-0.05, 1.05)
    ax.set_ylim(-0.05, 1.05)
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=12, loc='upper left')

    # Add text box with stats
    textstr = f'N Elections: {n_elections}\nBins: {len(bins_stats)}'
    ax.text(0.95, 0.05, textstr, transform=ax.transAxes, fontsize=11,
            verticalalignment='bottom', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    plt.tight_layout()
    output_file = f"{GRAPH_DIR}/partisan_bias_calibration_{platform.lower()}.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"   ✓ Saved {platform} plot to {output_file} ({n_elections} elections)")
    plt.close()

print("\n" + "="*80)
print("DONE")
print("="*80)
