#!/usr/bin/env python3
"""
Recreate the original exploration graphs for the liquidity article,
styled to match the website theme (calibration chart colors).
"""

import json
import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path
import matplotlib.pyplot as plt

DATA_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi/data")
OUTPUT_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi/website/assets/liquidity-article")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Website theme colors (from calibration charts)
BLUE = '#6495ED'   # Cornflower blue
RED = '#CD6B6B'    # Soft coral red
GRAY = '#666666'
LIGHT_GRAY = '#cccccc'
GREEN = '#6BAF6B'  # Soft green
ORANGE = '#E8A838' # Warm orange

plt.rcParams.update({
    'figure.facecolor': 'white',
    'axes.facecolor': 'white',
    'axes.edgecolor': '#333333',
    'axes.labelcolor': '#333333',
    'axes.titlecolor': '#111111',
    'text.color': '#333333',
    'xtick.color': '#333333',
    'ytick.color': '#333333',
    'font.family': 'sans-serif',
    'font.size': 11,
    'axes.titlesize': 14,
    'axes.titleweight': 'bold',
    'axes.labelsize': 12,
    'legend.frameon': True,
    'legend.facecolor': 'white',
    'legend.edgecolor': LIGHT_GRAY,
    'axes.spines.top': True,
    'axes.spines.right': True,
})

# =============================================================================
# LOAD DATA (using final prices for each market, not just 1-day)
# =============================================================================
print("Loading data...")
liquidity_df = pd.read_csv(DATA_DIR / "liquidity_metrics_by_market.csv")

# Get the final (closest to event) price for each market
pm_accuracy = pd.read_csv(DATA_DIR / "polymarket_prediction_accuracy_all_political.csv", low_memory=False)
pm_final = pm_accuracy.groupby('market_id').apply(
    lambda x: x.loc[x['days_before_event'].idxmin()], include_groups=False
).reset_index()
pm_final['price'] = pm_final['prediction_price']

kalshi_accuracy = pd.read_csv(DATA_DIR / "kalshi_prediction_accuracy_all_political.csv", low_memory=False)
kalshi_final = kalshi_accuracy.groupby('ticker').apply(
    lambda x: x.loc[x['days_before_event'].idxmin()], include_groups=False
).reset_index()
kalshi_final['market_id'] = kalshi_final['ticker']
kalshi_final['price'] = kalshi_final['prediction_price']

# Merge with liquidity data
pm_merged = liquidity_df[liquidity_df['platform'] == 'Polymarket'].merge(
    pm_final[['market_id', 'brier_score', 'price']], on='market_id', how='inner'
)
kalshi_merged = liquidity_df[liquidity_df['platform'] == 'Kalshi'].merge(
    kalshi_final[['ticker', 'brier_score', 'price']], on='ticker', how='inner'
)

merged = pd.concat([pm_merged, kalshi_merged], ignore_index=True)
merged = merged.dropna(subset=['spread_median', 'depth_median', 'brier_score', 'price'])
merged = merged[merged['spread_median'] > 0]
merged = merged[merged['depth_median'] > 0]
# Convert absolute spread to cents (spread_median is in dollars)
merged['abs_spread_cents'] = merged['spread_median'] * 100
# Keep all prices including 0 and 1 - they have valid Brier scores
print(f"Markets: {len(merged)}")

# Compute residuals
merged['expected_brier'] = merged['price'] * (1 - merged['price'])
merged['residual_brier'] = merged['brier_score'] - merged['expected_brier']
merged['log_depth'] = np.log10(merged['depth_median'] + 1)

# =============================================================================
# GRAPH 1: Absolute Spread vs Price Diagnostic (NO CONFOUND)
# =============================================================================
print("Graph 1: Absolute Spread vs Price Diagnostic...")
fig, ax = plt.subplots(figsize=(10, 6))

# Scatter plot with transparency
ax.scatter(merged['price'], merged['abs_spread_cents'],
           alpha=0.3, s=20, c=BLUE, edgecolors='none')

# Add binned means to show the pattern clearly - using 100 bins for high resolution
price_bins = np.linspace(0, 1, 101)  # 100 bins
bin_centers = (price_bins[:-1] + price_bins[1:]) / 2
merged['price_bin_idx'] = pd.cut(merged['price'], bins=price_bins, labels=False)
bin_means = merged.groupby('price_bin_idx')['abs_spread_cents'].mean()
bin_counts = merged.groupby('price_bin_idx').size()

# Only plot bins with at least 3 observations
valid_bins = bin_means[bin_counts >= 3].dropna()
ax.plot([bin_centers[int(i)] for i in valid_bins.index],
        valid_bins.values,
        '-', color=RED, linewidth=2, label='Bin means')

ax.set_xlabel('Final Price (Probability)', fontsize=12)
ax.set_ylabel('Absolute Spread (cents)', fontsize=12)
ax.set_title('Absolute Spread vs Price: No Confound', fontsize=14)
ax.set_xlim(0, 1)
ax.set_ylim(0, min(20, merged['abs_spread_cents'].quantile(0.95)))

# Compute correlation for annotation
corr, pval = stats.pearsonr(merged['price'], merged['abs_spread_cents'])
ax.text(0.95, 0.95, f'r = {corr:.2f}\nn = {len(merged):,}',
        transform=ax.transAxes, ha='right', va='top',
        fontsize=11, fontweight='bold', color=GRAY,
        bbox=dict(boxstyle='round', facecolor='white', edgecolor=LIGHT_GRAY, alpha=0.9))

ax.legend(loc='upper left')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / '1_spread_vs_price_diagnostic.png', dpi=150, facecolor='white', bbox_inches='tight')
plt.close()

# =============================================================================
# GRAPH 2: The Null Result (Spread and Depth vs Residual Brier)
# =============================================================================
print("Graph 2: The Null Result (2-panel)...")
fig, axes = plt.subplots(1, 2, figsize=(12, 5))

# Panel 1: Spread vs Residual Brier
ax = axes[0]
ax.scatter(merged['abs_spread_cents'], merged['residual_brier'], alpha=0.3, s=20, c=BLUE, edgecolors='none')
r, p = stats.pearsonr(merged['abs_spread_cents'], merged['residual_brier'])
ax.axhline(0, color=GRAY, linestyle='--', linewidth=1)
ax.set_xlabel('Absolute Spread (cents)', fontsize=12)
ax.set_ylabel('Residual Brier Score', fontsize=12)
ax.set_title('Spread vs Residual Accuracy', fontsize=14)
ax.set_xlim(0, min(20, merged['abs_spread_cents'].quantile(0.95)))
ax.text(0.95, 0.95, f'r = {r:.2f}\nn = {len(merged):,}',
        transform=ax.transAxes, ha='right', va='top',
        fontsize=11, fontweight='bold', color=GRAY,
        bbox=dict(boxstyle='round', facecolor='white', edgecolor=LIGHT_GRAY, alpha=0.9))
ax.grid(True, alpha=0.3)

# Panel 2: Depth vs Residual Brier
ax = axes[1]
ax.scatter(merged['log_depth'], merged['residual_brier'], alpha=0.3, s=20, c=BLUE, edgecolors='none')
r, p = stats.pearsonr(merged['log_depth'], merged['residual_brier'])
ax.axhline(0, color=GRAY, linestyle='--', linewidth=1)
ax.set_xlabel('Log₁₀(Order Book Depth)', fontsize=12)
ax.set_ylabel('Residual Brier Score', fontsize=12)
ax.set_title('Depth vs Residual Accuracy', fontsize=14)
ax.text(0.95, 0.95, f'r = {r:.2f}\nn = {len(merged):,}',
        transform=ax.transAxes, ha='right', va='top',
        fontsize=11, fontweight='bold', color=GRAY,
        bbox=dict(boxstyle='round', facecolor='white', edgecolor=LIGHT_GRAY, alpha=0.9))
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / '2_residual_analysis.png', dpi=150, facecolor='white', bbox_inches='tight')
plt.close()

# =============================================================================
# GRAPH 3: Time-Binned Correlations (EXACT original structure from timeseries script)
# =============================================================================
print("Graph 3: Time-Binned Correlations...")

# Load the actual data from the timeseries analysis
time_bins = ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%']
correlations = {
    'Spread': [-0.1987, -0.1861, -0.2080, -0.2678, -0.3783],
    'Depth': [0.0690, 0.1031, 0.0901, 0.1237, 0.1427],
    'Depth Imbalance': [-0.2126, -0.1878, -0.1635, -0.1420, -0.1198],
    'Abs Imbalance': [0.2389, 0.2025, 0.1868, 0.1711, 0.1597],
}
colors = {'Spread': BLUE, 'Depth': GREEN, 'Depth Imbalance': ORANGE, 'Abs Imbalance': RED}
markers = {'Spread': 'o', 'Depth': 's', 'Depth Imbalance': '^', 'Abs Imbalance': 'D'}

fig, ax = plt.subplots(figsize=(10, 6))
x = range(len(time_bins))
for name, vals in correlations.items():
    ax.plot(x, vals, marker=markers[name], linewidth=2, markersize=8, color=colors[name], label=name)

ax.axhline(0, color=GRAY, linestyle='--', linewidth=1.5)
ax.set_xticks(x)
ax.set_xticklabels(time_bins)
ax.set_xlabel('Market Life Fraction', fontsize=12)
ax.set_ylabel('Spearman Correlation with Residual Error', fontsize=12)
ax.set_title('Liquidity-Accuracy Relationship Over Market Lifetime', fontsize=14)
ax.legend(loc='best')
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / '3_time_binned_correlations.png', dpi=150, facecolor='white')
plt.close()

# =============================================================================
# GRAPH 4: Feature Importance (EXACT original structure from timeseries script)
# =============================================================================
print("Graph 4: Feature Importance...")

features = ['price_extremity', 'spread_t', 'abs_depth_imbalance_t', 'life_fraction',
            'depth_imbalance_t', 'category_encoded', 'log_depth_t']
importance = [0.046891, 0.045330, -0.003264, -0.012390, -0.015996, -0.017796, -0.060852]

fig, ax = plt.subplots(figsize=(10, 6))
colors_bar = [GREEN if v > 0 else RED for v in importance]
y_pos = range(len(features))
ax.barh(y_pos, importance, color=colors_bar, alpha=0.7)
ax.axvline(0, color='black', linewidth=0.5)
ax.set_yticks(y_pos)
ax.set_yticklabels(features)
ax.set_xlabel('Permutation Importance', fontsize=12)
ax.set_title('Random Forest Feature Importance (Test R² = -20.9%)', fontsize=14)
ax.invert_yaxis()
ax.grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.savefig(OUTPUT_DIR / '4_feature_importance.png', dpi=150, facecolor='white')
plt.close()

# =============================================================================
# GRAPH 5: Train vs Test Performance (key finding)
# =============================================================================
print("Graph 5: Train vs Test Performance...")

fig, ax = plt.subplots(figsize=(8, 6))
labels = ['Train R²\n(same markets)', 'OOB R²\n(bootstrap)', 'Test R²\n(new markets)']
values = [0.556, 0.554, -0.209]
colors_bar = [GREEN, GREEN, RED]
bars = ax.bar(labels, values, color=colors_bar, width=0.5, alpha=0.8)
ax.axhline(0, color='#333333', linewidth=1.5)
ax.set_ylabel('R² Score', fontsize=12)
ax.set_title('Model Generalization: The Critical Test', fontsize=14)
ax.set_ylim(-0.35, 0.7)

for bar, val in zip(bars, values):
    y = val + 0.025 if val > 0 else val - 0.045
    ax.text(bar.get_x() + bar.get_width()/2, y, f'{val:.1%}',
            ha='center', va='bottom' if val > 0 else 'top', fontsize=14, fontweight='bold')

ax.grid(True, alpha=0.3, axis='y')
plt.tight_layout()
plt.savefig(OUTPUT_DIR / '5_train_vs_test.png', dpi=150, facecolor='white')
plt.close()

print(f"\nAll graphs saved to: {OUTPUT_DIR}")
