#!/usr/bin/env python3
"""
Election Winner Markets Volume Distribution Over Time

Creates a scatter plot showing volume vs election date for all Panel A markets,
with log scale on y-axis to show the skewed distribution.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import re
import os

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
GRAPHS_DIR = f"{BASE_DIR}/graphs/combined"
os.makedirs(GRAPHS_DIR, exist_ok=True)

# Polymarket and Kalshi brand colors
POLYMARKET_BLUE = '#3B82F6'  # Polymarket blue
KALSHI_GREEN = '#10B981'     # Kalshi green

print("="*80)
print("ELECTION WINNER MARKETS VOLUME OVER TIME")
print("="*80)

# Scoring function (same as comparison script)
def score_market_for_election(question):
    if pd.isna(question):
        return -1000

    question_lower = question.lower()

    exclude_patterns = [r'drop out', r'drops out', r'withdraw', r'vote share', r'popular vote',
        r'\d+%', r'announce', r'by \w+ \d+', r'before \w+', r'in office on',
        r'president of .* on', r'be .* on \w+ \d+', r'out as']

    for pattern in exclude_patterns:
        if re.search(pattern, question_lower):
            return -1000

    score = 0

    if re.search(r'which party (win|wins)', question_lower):
        score += 1000
    if re.search(r'will a (democrat|republican) win', question_lower):
        score += 800
    if re.search(r'will (the )?(democratic|republican|democrats|republicans)( party)? win', question_lower):
        score += 800
    if re.search(r'will \w+ \w+ (or another (republican|democrat))? ?win the \d{4}', question_lower):
        score += 600
    if re.search(r'will \w+ \w+ (or another (republican|democrat))? ?win the presidency', question_lower):
        score += 600
    if re.search(r'election:.*vs\.', question_lower):
        score += 400

    if re.search(r'another party', question_lower) and not re.search(r'another (republican|democrat)', question_lower):
        score -= 500
    if re.search(r'electoral (college|votes)', question_lower):
        score -= 200
    if re.search(r'margin', question_lower):
        score -= 200

    return score

# Load master data
print("\n1. Loading master data...")
master_df = pd.read_csv(f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv", low_memory=False)
master_df['scheduled_end_time'] = pd.to_datetime(master_df['scheduled_end_time'])

# Filter to elections
elections_df = master_df[
    (pd.notna(master_df['democrat_vote_share'])) &
    (pd.notna(master_df['republican_vote_share'])) &
    (master_df['scheduled_end_time'] <= '2025-11-10') &
    (master_df['is_primary'] == False) &
    (master_df['country'] == 'United States')
].copy()

# Score markets
elections_df['market_score'] = elections_df['question'].apply(score_market_for_election)
elections_df = elections_df[elections_df['market_score'] >= 0].copy()

print(f"   ✓ {len(elections_df)} markets with score >= 0")

# Select best market per platform per election (with volume-based Presidential selection)
election_cols = ['country', 'office', 'location', 'election_year']

def select_best_market_per_election(group):
    """Select best market from a group, considering both score and volume"""
    if 'President' in group['office'].iloc[0]:
        max_vol_market = group.loc[group['volume_usd'].idxmax()]
        max_score_market = group.loc[group['market_score'].idxmax()]

        if (max_vol_market['volume_usd'] >= 10 * max_score_market['volume_usd'] and
            max_vol_market['market_score'] >= 600):
            return max_vol_market

    group_sorted = group.sort_values(['market_score', 'volume_usd'], ascending=[False, False])
    return group_sorted.iloc[0]

best_per_platform = elections_df.groupby(election_cols + ['platform']).apply(select_best_market_per_election).reset_index(drop=True)

print(f"   ✓ Polymarket: {(best_per_platform['platform'] == 'Polymarket').sum()} elections")
print(f"   ✓ Kalshi: {(best_per_platform['platform'] == 'Kalshi').sum()} elections")

# Load prediction data to match (ensures we only include markets with predictions)
print("\n2. Matching with prediction data...")
pm_pred = pd.read_csv(f"{DATA_DIR}/polymarket_prediction_accuracy_all_political_20260121_170951.csv")
kalshi_pred = pd.read_csv(f"{DATA_DIR}/kalshi_prediction_accuracy_all_political_20260121_170951.csv")

pm_pred_1d = pm_pred[pm_pred['days_before_close'] == 1].copy()
kalshi_pred_1d = kalshi_pred[kalshi_pred['days_before_close'] == 1].copy()

# Match Polymarket
pm_markets = best_per_platform[best_per_platform['platform'] == 'Polymarket'].copy()
pm_markets['market_id'] = pm_markets['market_id'].astype(str)
pm_pred_1d['market_id'] = pm_pred_1d['market_id'].astype(str)
pm_panel_a = pm_markets.merge(pm_pred_1d, on='market_id', how='inner', suffixes=('', '_pred'))
# Deduplicate by market_id (keep first to avoid double-counting Yes/No tokens)
pm_panel_a = pm_panel_a.drop_duplicates(subset='market_id', keep='first')

# Match Kalshi
kalshi_markets = best_per_platform[best_per_platform['platform'] == 'Kalshi'].copy()
kalshi_markets['market_id'] = kalshi_markets['market_id'].astype(str)
kalshi_pred_1d['ticker'] = kalshi_pred_1d['ticker'].astype(str)
kalshi_panel_a = kalshi_markets.merge(kalshi_pred_1d, left_on='market_id', right_on='ticker', how='inner', suffixes=('', '_pred'))

print(f"   ✓ Polymarket Panel A: {len(pm_panel_a)} markets")
print(f"   ✓ Kalshi Panel A: {len(kalshi_panel_a)} markets")

# Find top volume markets
pm_top = pm_panel_a.loc[pm_panel_a['volume_usd'].idxmax()]
kalshi_top = kalshi_panel_a.loc[kalshi_panel_a['volume_usd'].idxmax()]

print(f"\n3. Top volume markets:")
print(f"   Polymarket: {pm_top['question']} (${pm_top['volume_usd']:,.0f})")
print(f"   Kalshi: {kalshi_top['question']} (${kalshi_top['volume_usd']:,.0f})")

# Create scatter plot
print("\n4. Creating scatter plot...")

# Sort by volume (descending) to show distribution
pm_sorted = pm_panel_a.sort_values('volume_usd', ascending=False).reset_index(drop=True)
kalshi_sorted = kalshi_panel_a.sort_values('volume_usd', ascending=False).reset_index(drop=True)

fig, ax = plt.subplots(figsize=(12, 7))

# Plot Polymarket markets (x = market rank by volume)
ax.scatter(range(len(pm_sorted)),
           pm_sorted['volume_usd'],
           c=POLYMARKET_BLUE,
           alpha=0.6,
           s=100,
           label='Polymarket',
           edgecolors='white',
           linewidths=0.5)

# Plot Kalshi markets (x = market rank by volume)
ax.scatter(range(len(kalshi_sorted)),
           kalshi_sorted['volume_usd'],
           c=KALSHI_GREEN,
           alpha=0.6,
           s=100,
           label='Kalshi',
           edgecolors='white',
           linewidths=0.5)

# Set log scale on y-axis
ax.set_yscale('log')

# Format axes
ax.set_xlabel('Market Rank (by Volume)', fontsize=12, fontweight='bold')
ax.set_ylabel('Trading Volume (USD, log scale)', fontsize=12, fontweight='bold')
ax.set_title('Election Winner Markets: Volume Distribution',
             fontsize=14, fontweight='bold', pad=20)

# Format y-axis with currency
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

# Add grid
ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
ax.set_axisbelow(True)

# Add legend
ax.legend(loc='upper right', fontsize=11, framealpha=0.9)

plt.tight_layout()

# Save
output_file = f"{GRAPHS_DIR}/election_winner_volume_over_time.png"
plt.savefig(output_file, dpi=300, bbox_inches='tight')
print(f"   ✓ Saved to {output_file}")

print("\n" + "="*80)
print("DONE")
print("="*80)
