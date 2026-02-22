#!/usr/bin/env python3
"""
Volume Time Series by Market Category
Creates a time series plot showing total trading volumes by political category over time.
Combines both Polymarket and Kalshi data.

Uses the same color scheme as Polymarket/Kalshi pipeline for consistency.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for LaTeX
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json
import os
from datetime import datetime

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

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

# Category colors (professional palette matching paper theme)
CATEGORY_COLORS = {
    '1. ELECTORAL': '#5B8DEE',           # Polymarket blue
    '2. MONETARY_POLICY': '#E85D75',     # Muted red
    '11. PARTY_POLITICS': '#2CB67D',     # Kalshi green
    '8. MILITARY_SECURITY': '#F6A96C',   # Muted orange
    '6. INTERNATIONAL': '#9B72CB',       # Muted purple
    '4. APPOINTMENTS': '#4ECDC4',        # Muted turquoise
    '15. POLITICAL_SPEECH': '#E89F5B',   # Muted gold
    '5. REGULATORY': '#36B3A8',          # Muted teal
}

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details.csv"
GRAPH_DIR = f"{BASE_DIR}/graphs/combined"
os.makedirs(GRAPH_DIR, exist_ok=True)

print("="*80)
print("VOLUME TIME SERIES BY MARKET CATEGORY")
print("="*80)

# ============================================================================
# Load Data from Master CSV
# ============================================================================

print(f"\nLoading data from master CSV: {MASTER_FILE}")
df_master = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded {len(df_master):,} total market records")

# Filter to Polymarket
df_pm = df_master[df_master['platform'] == 'Polymarket'].copy()
print(f"✓ Filtered to {len(df_pm):,} Polymarket rows")

# Ensure market_id types match for merging
df_pm['market_id'] = df_pm['market_id'].astype(str)

# Filter to only closed markets with valid closedTime
df_pm['closed'] = df_pm['closed'].fillna(False)
df_pm = df_pm[df_pm['closed'] == True].copy()
print(f"  After closed filter: {len(df_pm):,} rows")

# Fix timezone format: "+00" -> "+00:00" for proper parsing
df_pm['trading_close_time'] = df_pm['trading_close_time'].astype(str).str.replace('+00', '+00:00', regex=False)
df_pm['trading_close_time'] = pd.to_datetime(df_pm['trading_close_time'], format='mixed', errors='coerce')
df_pm = df_pm[df_pm['trading_close_time'].notna()].copy()
print(f"✓ Filtered to {len(df_pm):,} closed Polymarket rows with valid dates")

# Use trading_close_time as the date
df_pm['date'] = df_pm['trading_close_time']

# IMPORTANT: Deduplicate by market_id to avoid double-counting
# Each market has Yes/No rows with the same volume
print(f"  Before deduplication: {len(df_pm):,} rows")
df_pm_markets = df_pm.drop_duplicates(subset=['market_id'], keep='first')[['market_id', 'political_category', 'volume_usd', 'date']].copy()
df_pm_markets = df_pm_markets.rename(columns={'volume_usd': 'volume'})
print(f"✓ After deduplication: {len(df_pm_markets):,} unique Polymarket markets")

if len(df_pm_markets) > 0:
    print(f"✓ Date range: {df_pm_markets['date'].min()} to {df_pm_markets['date'].max()}")
    print(f"✓ Total Polymarket volume: ${df_pm_markets['volume'].sum()/1e6:.1f}M")

# ============================================================================
# Load Kalshi Data
# ============================================================================

print(f"\nFiltering to Kalshi data...")
df_kalshi = df_master[df_master['platform'] == 'Kalshi'].copy()
print(f"✓ Filtered to {len(df_kalshi):,} Kalshi markets")

# Ensure market_id types match for merging
df_kalshi['market_id'] = df_kalshi['market_id'].astype(str)

# Filter to only finalized/closed markets
df_kalshi = df_kalshi[df_kalshi['status'].isin(['finalized', 'closed'])].copy()
print(f"✓ Filtered to {len(df_kalshi):,} finalized/closed Kalshi markets")

# Parse dates - use close_time for when market actually closed
df_kalshi['close_time'] = pd.to_datetime(df_kalshi['close_time'], format='mixed', errors='coerce', utc=True)
df_kalshi = df_kalshi[df_kalshi['close_time'].notna()].copy()
print(f"  After date filter: {len(df_kalshi):,} markets with valid close_time")

df_kalshi['date'] = df_kalshi['close_time']

# Keep only relevant columns (market_id already exists)
df_kalshi_markets = df_kalshi[['market_id', 'political_category', 'volume_usd', 'date']].copy()
df_kalshi_markets = df_kalshi_markets.rename(columns={'volume_usd': 'volume'})

if len(df_kalshi_markets) > 0:
    print(f"✓ Kalshi markets: {len(df_kalshi_markets):,}")
    print(f"✓ Date range: {df_kalshi_markets['date'].min()} to {df_kalshi_markets['date'].max()}")
    print(f"✓ Total Kalshi volume: ${df_kalshi_markets['volume'].sum()/1e6:.1f}M")

# ============================================================================
# Combine and Process Data
# ============================================================================

print(f"\nCombining data...")

# Add source column
df_pm_markets['source'] = 'Polymarket'
df_kalshi_markets['source'] = 'Kalshi'

# Combine
df_combined = pd.concat([
    df_pm_markets[['market_id', 'political_category', 'volume', 'date', 'source']],
    df_kalshi_markets[['market_id', 'political_category', 'volume', 'date', 'source']]
], ignore_index=True)

# Filter out rows with missing dates or volumes
df_combined = df_combined[df_combined['date'].notna()].copy()
df_combined = df_combined[df_combined['volume'].notna()].copy()
df_combined = df_combined[df_combined['volume'] > 0].copy()

print(f"✓ Combined dataset: {len(df_combined):,} markets")
print(f"✓ Date range: {df_combined['date'].min()} to {df_combined['date'].max()}")

# Extract year-month for aggregation
df_combined['year_month'] = df_combined['date'].dt.to_period('M')

# ============================================================================
# Aggregate by Category and Time
# ============================================================================

print(f"\nAggregating volumes by category and month...")

# Group by category and month
volume_by_category = df_combined.groupby(['political_category', 'year_month']).agg({
    'volume': 'sum',
    'market_id': 'count'
}).reset_index()

volume_by_category.columns = ['political_category', 'year_month', 'total_volume', 'num_markets']

# Convert period back to datetime for plotting
volume_by_category['date'] = volume_by_category['year_month'].dt.to_timestamp()

print(f"✓ Aggregated to {len(volume_by_category):,} category-month combinations")

# Get top categories by total volume
top_categories = df_combined.groupby('political_category')['volume'].sum().sort_values(ascending=False).head(8).index.tolist()
print(f"\nTop 8 categories by volume:")
for cat in top_categories:
    total_vol = df_combined[df_combined['political_category'] == cat]['volume'].sum()
    print(f"  • {cat}: ${total_vol/1e6:.1f}M")

# ============================================================================
# Create Time Series Plot
# ============================================================================

print(f"\n{'='*80}")
print("Creating time series plot...")
print(f"{'='*80}")

fig, ax = plt.subplots(figsize=(14, 8))

# Find the first date for International category
international_data = volume_by_category[volume_by_category['political_category'] == '6. INTERNATIONAL'].copy()
if len(international_data) > 0:
    international_start = international_data['date'].min()
    print(f"\n✓ International category starts at: {international_start}")
else:
    international_start = None
    print(f"\n⚠ International category not found in top categories")

# Plot each top category
for i, category in enumerate(top_categories):
    cat_data = volume_by_category[volume_by_category['political_category'] == category].copy()
    cat_data = cat_data.sort_values('date')

    # Get color for this category
    color = CATEGORY_COLORS.get(category, COLORS['light_gray'])

    # Clean label: remove number prefix and format nicely
    clean_label = category.split('. ', 1)[-1] if '. ' in category else category
    clean_label = clean_label.replace('_', ' ').title()

    ax.plot(cat_data['date'], cat_data['total_volume'] / 1e6,
            marker='o',
            markersize=4,
            linewidth=2,
            color=color,
            label=clean_label,
            alpha=0.8)

    print(f"✓ Plotted {category}: {len(cat_data)} time points")

# Set x-axis limits starting from International category's first date
if international_start is not None:
    ax.set_xlim(left=international_start)

# Formatting
ax.set_xlabel('Month', fontsize=14, fontweight='bold')
ax.set_ylabel('Total Volume ($M, log scale)', fontsize=14, fontweight='bold')
ax.set_title('Trading Volume Over Time by Political Category\n(Polymarket + Kalshi Combined)',
             fontsize=16, fontweight='bold', pad=20)

# Use log scale for Y-axis to handle Electoral dominance
ax.set_yscale('log')

# Set minimum y-axis value to avoid visual issues with very small values
ax.set_ylim(bottom=0.0001)

# Format y-axis - use K for values < 1M, M for values >= 1M
def format_volume(x, p):
    if x >= 1:
        return f'${x:.0f}M'
    elif x >= 0.001:
        return f'${x*1000:.0f}K'
    else:
        return f'${x*1000:.1f}K'

ax.yaxis.set_major_formatter(plt.FuncFormatter(format_volume))

# Add grid
ax.grid(True, alpha=0.3, zorder=0, which='both')

# Legend
ax.legend(fontsize=10, loc='upper left', framealpha=0.9, ncol=2)

# Rotate x-axis labels
plt.xticks(rotation=45, ha='right')

# Add note about log scale
ax.text(0.98, 0.02,
        'Note: Y-axis uses log scale due to Electoral dominance',
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment='bottom',
        horizontalalignment='right',
        bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

plt.tight_layout()
output_file = f"{GRAPH_DIR}/volume_timeseries_by_category.png"
plt.savefig(output_file, dpi=300, bbox_inches='tight')
print(f"\n✓ Saved: {output_file}")
plt.close()

# ============================================================================
# Summary Statistics
# ============================================================================

print(f"\n{'='*80}")
print("SUMMARY")
print(f"{'='*80}")
print(f"Total markets: {len(df_combined):,}")
print(f"Total volume: ${df_combined['volume'].sum()/1e6:.1f}M")
print(f"Date range: {df_combined['date'].min().strftime('%Y-%m')} to {df_combined['date'].max().strftime('%Y-%m')}")
print(f"Categories plotted: {len(top_categories)}")
print(f"\nGraph saved to: {GRAPH_DIR}")
print(f"  • volume_timeseries_by_category.png")
print(f"{'='*80}")
