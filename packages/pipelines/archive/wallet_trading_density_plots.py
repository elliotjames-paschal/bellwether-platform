#!/usr/bin/env python3
"""
Density Plot Visualizations for Wallet Trading Analysis
Creates three core distribution plots:
1. Distribution of Partisanship (% of money bet on Democrat Yes)
2. Distribution of Accuracy (% of money bet in the correct direction)
3. Partisanship Comparison: Actual vs Perfect Accuracy (counterfactual)

Each row in the data represents one trader/wallet with their aggregated trading statistics.
Uses the same color scheme as Polymarket/Kalshi pipeline for consistency.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for LaTeX
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

# Color scheme from Polymarket pipeline
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#34495E',
    'tertiary': '#7F8C8D',
    'light_gray': '#95A5A6',
    'lighter_gray': '#BDC3C7',
    'lightest_gray': '#D5DBDB',
    'dark': '#1a1a1a',
    'accent': '#546E7A',
    'counterfactual': '#E74C3C'  # Red for counterfactual distribution
}

# Output directory for graphs
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
GRAPH_DIR = f"{BASE_DIR}/graphs/combined"
os.makedirs(GRAPH_DIR, exist_ok=True)

# ============================================================================
# Load the wallet trading analysis data
# ============================================================================

# Load the wallet election trading analysis data
data_file = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi/data/wallet_election_trading_analysis.csv"
print(f"Loading data from: {data_file}")

df = pd.read_csv(data_file)
print(f"✓ Loaded {len(df):,} wallets")

# Filter out wallets with zero volume
df_with_volume = df[df['total_volume_usdc'] > 0].copy()
print(f"✓ Filtered to {len(df_with_volume):,} wallets with trading volume")

print(f"\nData columns: {list(df.columns)}")


# ============================================================================
# PLOT 1: Distribution of Partisanship
# ============================================================================

# Filter to only traders who actually bet FOR Democrats (not just against)
# This ensures we're only looking at traders who have some pro-Democrat position
df_democrat_traders = df_with_volume[
    df_with_volume['volume_for_democrat'] > 0
].copy()

print(f"\n{'='*80}")
print("PLOT 1: Distribution of Partisanship")
print(f"{'='*80}")
print(f"Total traders with volume: {len(df_with_volume):,}")
print(f"Traders who bet FOR Democrats (at least once): {len(df_democrat_traders):,}")
print(f"Filtered out: {len(df_with_volume) - len(df_democrat_traders):,} traders who never bet FOR Democrats")
print(f"\nSummary statistics for % money bet on Democrat Yes:")
print(df_democrat_traders['pct_volume_for_democrat'].describe())

# Create the density plot
fig, ax = plt.subplots(figsize=(12, 6))

# Plot density
sns.kdeplot(data=df_democrat_traders['pct_volume_for_democrat'],
            fill=True,
            color=COLORS['primary'],
            alpha=0.6,
            linewidth=2,
            ax=ax)

# Add histogram overlay for better context
ax.hist(df_democrat_traders['pct_volume_for_democrat'],
        bins=50,
        density=True,
        alpha=0.3,
        color=COLORS['lighter_gray'],
        edgecolor=COLORS['dark'])

# Add vertical lines for key statistics
median_val = df_democrat_traders['pct_volume_for_democrat'].median()
mean_val = df_democrat_traders['pct_volume_for_democrat'].mean()

ax.axvline(median_val, color=COLORS['secondary'], linestyle='--', linewidth=2,
           label=f'Median: {median_val:.1f}%')
ax.axvline(mean_val, color=COLORS['tertiary'], linestyle=':', linewidth=2,
           label=f'Mean: {mean_val:.1f}%')

# Labels and formatting
ax.set_xlabel('% of Money Bet on Democrat Yes', fontsize=14, fontweight='bold')
ax.set_ylabel('Density', fontsize=14, fontweight='bold')
ax.set_title('Polymarket Election Markets: Distribution of Trader Partisanship',
             fontsize=16, fontweight='bold', pad=20)
ax.legend(fontsize=12, loc='upper right')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(f"{GRAPH_DIR}/partisanship_distribution.png", dpi=300, bbox_inches='tight')
print(f"\n✓ Saved: {GRAPH_DIR}/partisanship_distribution.png")
plt.close()

# Print insights
print(f"\nKey insights:")
print(f"  • Median trader bets {median_val:.1f}% of money on Democrat Yes")
print(f"  • Mean trader bets {mean_val:.1f}% of money on Democrat Yes")
print(f"  • Traders with >50% on Democrats: {(df_democrat_traders['pct_volume_for_democrat'] > 50).sum():,} ({(df_democrat_traders['pct_volume_for_democrat'] > 50).mean()*100:.1f}%)")
print(f"  • Traders with >75% on Democrats: {(df_democrat_traders['pct_volume_for_democrat'] > 75).sum():,} ({(df_democrat_traders['pct_volume_for_democrat'] > 75).mean()*100:.1f}%)")


# ============================================================================
# PLOT 2: Distribution of Accuracy
# ============================================================================

# Note: Accuracy is calculated from ALL trades (volume_correct + volume_incorrect)
# So if a trader has ANY volume, they will have an accuracy percentage
# This does NOT have the same zero-inflation problem as partisanship

print(f"\n{'='*80}")
print("PLOT 2: Distribution of Accuracy")
print(f"{'='*80}")
print(f"Traders analyzed: {len(df_with_volume):,}")
print(f"Note: Accuracy includes ALL trades across all markets (Democrat, Republican, and other)")
print(f"\nSummary statistics for % money bet in correct direction:")
print(df_with_volume['pct_volume_correct'].describe())

# Create the density plot
fig, ax = plt.subplots(figsize=(12, 6))

# Plot density
sns.kdeplot(data=df_with_volume['pct_volume_correct'],
            fill=True,
            color=COLORS['primary'],
            alpha=0.6,
            linewidth=2,
            ax=ax)

# Add histogram overlay
ax.hist(df_with_volume['pct_volume_correct'],
        bins=50,
        density=True,
        alpha=0.3,
        color=COLORS['lighter_gray'],
        edgecolor=COLORS['dark'])

# Add vertical lines for key statistics
median_accuracy = df_with_volume['pct_volume_correct'].median()
mean_accuracy = df_with_volume['pct_volume_correct'].mean()

ax.axvline(median_accuracy, color=COLORS['secondary'], linestyle='--', linewidth=2,
           label=f'Median: {median_accuracy:.1f}%')
ax.axvline(mean_accuracy, color=COLORS['tertiary'], linestyle=':', linewidth=2,
           label=f'Mean: {mean_accuracy:.1f}%')
ax.axvline(50, color=COLORS['light_gray'], linestyle='-', linewidth=1.5, alpha=0.7,
           label='Random chance (50%)')

# Labels and formatting
ax.set_xlabel('% of Money Bet in Correct Direction', fontsize=14, fontweight='bold')
ax.set_ylabel('Density', fontsize=14, fontweight='bold')
ax.set_title('Polymarket Election Markets: Distribution of Trader Accuracy',
             fontsize=16, fontweight='bold', pad=20)
ax.legend(fontsize=12, loc='upper right')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(f"{GRAPH_DIR}/accuracy_distribution.png", dpi=300, bbox_inches='tight')
print(f"\n✓ Saved: {GRAPH_DIR}/accuracy_distribution.png")
plt.close()

# Print insights
print(f"\nKey insights:")
print(f"  • Median trader has {median_accuracy:.1f}% of money bet correctly")
print(f"  • Mean trader has {mean_accuracy:.1f}% of money bet correctly")
print(f"  • Traders with >50% accuracy: {(df_with_volume['pct_volume_correct'] > 50).sum():,} ({(df_with_volume['pct_volume_correct'] > 50).mean()*100:.1f}%)")
print(f"  • Traders with >75% accuracy: {(df_with_volume['pct_volume_correct'] > 75).sum():,} ({(df_with_volume['pct_volume_correct'] > 75).mean()*100:.1f}%)")
print(f"  • Traders with >90% accuracy: {(df_with_volume['pct_volume_correct'] > 90).sum():,} ({(df_with_volume['pct_volume_correct'] > 90).mean()*100:.1f}%)")


# ============================================================================
# PLOT 3: Partisanship Comparison (Actual vs Perfect Accuracy)
# ============================================================================

# Load counterfactual data
counterfactual_file = f"{BASE_DIR}/data/wallet_election_trading_counterfactual.csv"
print(f"\n{'='*80}")
print("PLOT 3: Partisanship Comparison (Actual vs Perfect Accuracy)")
print(f"{'='*80}")
print(f"Loading counterfactual data from: {counterfactual_file}")

df_cf = pd.read_csv(counterfactual_file)
print(f"✓ Loaded {len(df_cf):,} wallets with counterfactual data")

# Filter to the SAME traders as Plot 1: traders who bet FOR Democrats at least once
# This ensures the blue line matches Plot 1 exactly
df_democrat_traders_plot3 = df_with_volume[
    df_with_volume['volume_for_democrat'] > 0
].copy()

print(f"Traders who bet FOR Democrats (matching Plot 1): {len(df_democrat_traders_plot3):,}")

# Get the wallet_ids of these traders
democrat_wallet_ids = df_democrat_traders_plot3['wallet'].values

# Filter counterfactual data to the exact same wallet_ids
df_cf_matched = df_cf[df_cf['wallet'].isin(democrat_wallet_ids)].copy()

print(f"Matched traders in counterfactual data: {len(df_cf_matched):,}")

# Extract partisanship percentages for the SAME set of traders
actual_partisanship = df_democrat_traders_plot3['pct_volume_for_democrat'].dropna()
cf_partisanship = df_cf_matched['cf_pct_volume_for_democrat'].dropna()

print(f"\nSummary statistics:")
print(f"\nACTUAL DISTRIBUTION:")
print(f"  Mean: {actual_partisanship.mean():.2f}%")
print(f"  Median: {actual_partisanship.median():.2f}%")
print(f"  Std Dev: {actual_partisanship.std():.2f}%")
print(f"\nPERFECT ACCURACY DISTRIBUTION:")
print(f"  Mean: {cf_partisanship.mean():.2f}%")
print(f"  Median: {cf_partisanship.median():.2f}%")
print(f"  Std Dev: {cf_partisanship.std():.2f}%")
mean_shift = cf_partisanship.mean() - actual_partisanship.mean()
median_shift = cf_partisanship.median() - actual_partisanship.median()
print(f"\nDIFFERENCE:")
print(f"  Δ Mean: {mean_shift:+.2f}%")
print(f"  Δ Median: {median_shift:+.2f}%")

# Create the comparison plot
fig, ax = plt.subplots(figsize=(14, 8))

# Plot counterfactual distribution FIRST (so it's in the back)
sns.kdeplot(
    data=cf_partisanship,
    fill=True,
    color=COLORS['counterfactual'],
    alpha=0.5,
    linewidth=3,
    label=f'Perfect Accuracy Distribution (n={len(cf_partisanship):,})',
    ax=ax
)

# Plot actual distribution SECOND (so it's in the front)
sns.kdeplot(
    data=actual_partisanship,
    fill=True,
    color=COLORS['primary'],
    alpha=0.5,
    linewidth=3,
    label=f'Actual Distribution (n={len(actual_partisanship):,})',
    ax=ax
)

# Add vertical lines for means
ax.axvline(
    actual_partisanship.mean(),
    color=COLORS['primary'],
    linestyle='--',
    linewidth=2,
    alpha=0.7,
    label=f'Actual Mean: {actual_partisanship.mean():.1f}%'
)

ax.axvline(
    cf_partisanship.mean(),
    color=COLORS['counterfactual'],
    linestyle='--',
    linewidth=2,
    alpha=0.7,
    label=f'Perfect Accuracy Mean: {cf_partisanship.mean():.1f}%'
)

# Labels and formatting
ax.set_xlabel('% of Volume for Democrat', fontsize=14, fontweight='bold')
ax.set_ylabel('Density', fontsize=14, fontweight='bold')
ax.set_title(
    'Trader Partisanship Distribution: Actual vs Perfect Accuracy\n' +
    '(How would partisanship change if all traders had been correct?)',
    fontsize=16,
    fontweight='bold',
    pad=20
)
ax.set_xlim(-5, 105)
ax.legend(fontsize=11, loc='upper right', framealpha=0.95)
ax.grid(True, alpha=0.3)

# Add annotation explaining the difference
ax.text(
    0.02, 0.98,
    f'Mean shift: {mean_shift:+.1f}% toward Democrat\n' +
    f'(when all traders forced to be correct)',
    transform=ax.transAxes,
    fontsize=11,
    verticalalignment='top',
    bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='gray')
)

plt.tight_layout()
plt.savefig(f"{GRAPH_DIR}/partisanship_actual_vs_perfect.png", dpi=300, bbox_inches='tight')
print(f"\n✓ Saved: {GRAPH_DIR}/partisanship_actual_vs_perfect.png")
plt.close()

# Print insights
print(f"\nKey insight: Perfect accuracy shifts mean partisanship {mean_shift:+.1f}% toward Democrat")
if mean_shift > 0:
    print("This suggests traders were more often WRONG when betting FOR Democrat")
    print("or more often CORRECT when betting AGAINST Democrat.")
else:
    print("This suggests traders were more often WRONG when betting FOR Republican")
    print("or more often CORRECT when betting AGAINST Republican.")


# ============================================================================
# Summary
# ============================================================================

print(f"\n{'='*80}")
print("SUMMARY")
print(f"{'='*80}")
print(f"Total traders analyzed: {len(df_with_volume):,}")
print(f"Total trading volume: ${df_with_volume['total_volume_usdc'].sum():,.2f}")
print(f"Average volume per trader: ${df_with_volume['total_volume_usdc'].mean():,.2f}")
print(f"Median volume per trader: ${df_with_volume['total_volume_usdc'].median():,.2f}")
print(f"\nGraphs saved to: {GRAPH_DIR}")
print(f"  1. partisanship_distribution.png")
print(f"  2. accuracy_distribution.png")
print(f"  3. partisanship_actual_vs_perfect.png")
print(f"{'='*80}")
