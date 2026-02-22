#!/usr/bin/env python3
"""
Calibration Analysis by Race Closeness

Updated approach:
1. Load elections with primary markets (from select_primary_markets_for_elections.py)
2. Match to prediction accuracy data by market_id
3. Create table and graph by race margin
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import re

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

# Color scheme
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#34495E',
    'tertiary': '#7F8C8D',
    'light_gray': '#95A5A6',
    'lighter_gray': '#BDC3C7',
    'lightest_gray': '#D5DBDB',
    'dark': '#1a1a1a',
    'accent': '#546E7A',
    'polymarket': '#5B8DEE',  # Platform-specific blue
    'kalshi': '#2CB67D'       # Platform-specific green
}

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
GRAPH_DIR = f"{BASE_DIR}/graphs/combined"
os.makedirs(GRAPH_DIR, exist_ok=True)

def score_market_for_election(question):
    """
    Score a market question to determine how well it represents the main election.
    Higher scores = better representation of the actual election outcome.

    Updated to recognize both Polymarket and Kalshi phrasing patterns fairly.

    Returns:
        int: Priority score (higher is better)
    """
    if pd.isna(question):
        return -1000

    question_lower = question.lower()

    # Exclude derivative markets (score = -1000, basically excluded)
    exclude_patterns = [
        r'drop out',
        r'drops out',
        r'withdraw',
        r'vote share',
        r'popular vote',
        r'\d+%',  # Percentage predictions like ">10%"
        r'announce',
        r'by \w+ \d+',  # "by March 5" type timing questions
        r'before \w+',
        r'in office on',  # "Will X be in office on DATE"
        r'president of .* on',  # "Will X be President of USA on DATE"
        r'be .* on \w+ \d+',  # "be [office] on [date]"
        r'out as',  # "out as Congressman" - about resignation/removal, not election
        r'tipping point',  # "tipping point jurisdiction" - about electoral college decisiveness, not race outcome
    ]

    for pattern in exclude_patterns:
        if re.search(pattern, question_lower):
            return -1000

    # Priority scoring (higher = better)
    score = 0

    # Tier 1: "Which party wins" questions (best - Polymarket pattern)
    if re.search(r'which party (win|wins)', question_lower):
        score += 1000

    # Tier 2: "Will a Democrat/Republican win" questions (Polymarket/mixed pattern)
    if re.search(r'will a (democrat|republican) win', question_lower):
        score += 800

    # Tier 2: "Will [the] Democratic/Republican/Democrats/Republicans [party] win" (Kalshi standard pattern)
    # Handles: "Will Democratic win", "Will the Democratic party win", "Will Republicans win"
    if re.search(r'will (the )?(democratic|republican|democrats|republicans)( party)? win', question_lower):
        score += 800

    # Tier 3: "Will [candidate name] win the [year]/presidency" questions
    # Updated to handle "or another Republican/Democrat" phrasing
    if re.search(r'will \w+ \w+ (or another (republican|democrat))? ?win the \d{4}', question_lower):
        score += 600
    if re.search(r'will \w+ \w+ (or another (republican|democrat))? ?win the presidency', question_lower):
        score += 600

    # Tier 4: "[Location] election: [Candidate A] vs [Candidate B]" format (Polymarket pattern)
    if re.search(r'election:.*vs\.', question_lower):
        score += 400

    # Penalties for less desirable features
    # Only penalize "another party" if it's actually about third parties, not major parties
    if re.search(r'another party', question_lower) and not re.search(r'another (republican|democrat)', question_lower):
        score -= 500  # Third party questions are less desirable

    if re.search(r'electoral (college|votes)', question_lower):
        score -= 200  # Electoral college specifics less desirable than general win

    if re.search(r'margin', question_lower):
        score -= 200  # Margin predictions are derivative

    # State-specific penalty for presidential markets
    # Penalize questions that ask about specific US states/districts in presidential election
    us_states = [
        'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut',
        'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa',
        'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan',
        'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new hampshire',
        'new jersey', 'new mexico', 'new york', 'north carolina', 'north dakota', 'ohio',
        'oklahoma', 'oregon', 'pennsylvania', 'rhode island', 'south carolina', 'south dakota',
        'tennessee', 'texas', 'utah', 'vermont', 'virginia', 'washington', 'west virginia',
        'wisconsin', 'wyoming', 'district of columbia', 'washington, district of columbia'
    ]

    # Check if question mentions a state or congressional district (for presidential questions)
    if 'president' in question_lower or 'presidency' in question_lower:
        # Check for congressional districts (e.g., "NE-2", "ME-1")
        if re.search(r'\b[A-Z]{2}-\d+\b', question_lower) or re.search(r'congressional district', question_lower):
            score -= 300  # Penalty for district-specific presidential markets
        else:
            # Check for state names
            for state in us_states:
                # Match "win [state]" or "[state] in the"
                if re.search(rf'\bwin {state}\b|\b{state} in the', question_lower):
                    score -= 300  # Heavy penalty for state-specific presidential markets
                    break

    return score


print("="*80)
print("CALIBRATION ANALYSIS BY RACE CLOSENESS")
print("="*80)

# ============================================================================
# 1. Load master CSV with electoral markets
# ============================================================================

print("\n1. Loading master CSV with electoral markets...")
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
master_df = pd.read_csv(MASTER_FILE, low_memory=False)

# Convert scheduled_end_time to datetime for filtering
master_df['scheduled_end_time'] = pd.to_datetime(master_df['scheduled_end_time'], format='mixed', errors='coerce')

# Filter to:
# 1. Electoral markets that have vote share data
# 2. Elections on or before November 10, 2025
# 3. United States elections only (includes primaries)
# 4. Main election markets (not derivative questions)
elections_df = master_df[
    (pd.notna(master_df['democrat_vote_share'])) &
    (pd.notna(master_df['republican_vote_share'])) &
    (master_df['scheduled_end_time'] <= '2025-11-10') &
    (master_df['country'] == 'United States')
].copy()

print(f"   ✓ {len(elections_df)} electoral markets with vote share data (before selecting best market per election)")

# Score each market for how well it represents the main election
elections_df['market_score'] = elections_df['question'].apply(score_market_for_election)

# IMPORTANT: Filter to score >= 0 (matching Table 5 Panel A logic)
election_cols = ['country', 'office', 'location', 'election_year']
elections_df = elections_df[elections_df['market_score'] >= 0].copy()

print(f"   ✓ {len(elections_df)} markets with score >= 0")

# Select best market per PLATFORM per election (matching Table 5 Panel A)
# For Presidential elections: prioritize volume if a candidate market has 10x+ volume
# For other elections: use score first, then volume as tiebreaker
def select_best_market_per_election(group):
    """Select best market from a group, considering both score and volume"""
    # If this is a Presidential election, check if a high-volume candidate market exists
    if 'President' in group['office'].iloc[0]:
        # Find highest volume market
        max_vol_market = group.loc[group['volume_usd'].idxmax()]
        # Find highest score market
        max_score_market = group.loc[group['market_score'].idxmax()]

        # If the high-volume market has 10x+ volume and score >= 600, prefer it
        if (max_vol_market['volume_usd'] >= 10 * max_score_market['volume_usd'] and
            max_vol_market['market_score'] >= 600):
            return max_vol_market

    # Default: sort by score (descending), then volume (descending)
    group_sorted = group.sort_values(['market_score', 'volume_usd'], ascending=[False, False])
    return group_sorted.iloc[0]

# Group by election_cols + ['platform'] to get best market per platform per election
# This matches Table 5 Panel A logic exactly
best_per_platform = elections_df.groupby(election_cols + ['platform']).apply(select_best_market_per_election).reset_index(drop=True)

print(f"   ✓ Polymarket: {(best_per_platform['platform'] == 'Polymarket').sum()} elections")
print(f"   ✓ Kalshi: {(best_per_platform['platform'] == 'Kalshi').sum()} elections")
print(f"   ✓ TOTAL: {len(best_per_platform)} markets (matching Table 5 Panel A)")

# Use best_per_platform for the rest of the analysis
elections_df = best_per_platform

# Show distribution of market scores
print(f"   ✓ Score distribution:")
print(f"      - Score >= 800 (top tier): {(elections_df['market_score'] >= 800).sum()}")
print(f"      - Score 400-799: {((elections_df['market_score'] >= 400) & (elections_df['market_score'] < 800)).sum()}")
print(f"      - Score < 400: {(elections_df['market_score'] < 400).sum()}")

# Calculate vote margin
elections_df['vote_margin'] = abs(
    elections_df['democrat_vote_share'] - elections_df['republican_vote_share']
)

# ============================================================================
# 2. Load prediction accuracy data (1 day before)
# ============================================================================

print("\n2. Loading prediction accuracy data (1 day before resolution)...")
pm_pred = pd.read_csv(f"{DATA_DIR}/polymarket_prediction_accuracy_all_political_20260121_170951.csv")
kalshi_pred = pd.read_csv(f"{DATA_DIR}/kalshi_prediction_accuracy_all_political_20260121_170951.csv")

# Filter to 1 day before
pm_pred_1d = pm_pred[pm_pred['days_before_close'] == 1].copy()
kalshi_pred_1d = kalshi_pred[kalshi_pred['days_before_close'] == 1].copy()

print(f"   ✓ {len(pm_pred_1d):,} Polymarket predictions (1d before)")
print(f"   ✓ {len(kalshi_pred_1d):,} Kalshi predictions (1d before)")

# ============================================================================
# 3. Match elections to predictions by market_id
# ============================================================================

print("\n3. Matching elections to predictions by market_id...")

# Separate by platform
pm_elections = elections_df[elections_df['platform'] == 'Polymarket'].copy()
kalshi_elections = elections_df[elections_df['platform'] == 'Kalshi'].copy()

# Match Polymarket - convert market_id to string for matching
pm_elections['market_id'] = pm_elections['market_id'].astype(str)
pm_pred_1d['market_id'] = pm_pred_1d['market_id'].astype(str)

pm_with_pred = pm_elections.merge(
    pm_pred_1d,
    on='market_id',
    how='inner',
    suffixes=('', '_pred')
)

# IMPORTANT: Deduplicate by market_id to handle YES/NO tokens (matching Table 5 line 185)
pm_unique = pm_with_pred.drop_duplicates(subset='market_id')
print(f"   ✓ Polymarket: {len(pm_unique)} unique elections matched")

# Match Kalshi - ticker in prediction file, market_id in master CSV
kalshi_elections['market_id'] = kalshi_elections['market_id'].astype(str)
kalshi_pred_1d['ticker'] = kalshi_pred_1d['ticker'].astype(str)

kalshi_with_pred = kalshi_elections.merge(
    kalshi_pred_1d,
    left_on='market_id',
    right_on='ticker',
    how='inner',
    suffixes=('', '_pred')
)

# IMPORTANT: Deduplicate by market_id (matching Table 5 line 199)
kalshi_unique = kalshi_with_pred.drop_duplicates(subset='market_id')
print(f"   ✓ Kalshi: {len(kalshi_unique)} unique elections matched")

# Combine deduplicated data
combined = pd.concat([pm_unique, kalshi_unique], ignore_index=True)
print(f"   ✓ TOTAL: {len(combined)} markets with predictions (matching Table 5 Panel A)")

# No additional filtering needed - we already filtered to score >= 0 above

# ============================================================================
# 4. Calculate metrics
# ============================================================================

print("\n4. Calculating metrics...")

# The prediction accuracy files already have brier_score calculated
# We just need to ensure we're using the right values

# For winner markets (actual_outcome=1), use prediction_price directly
# For loser markets (actual_outcome=0), flip to get winner prediction
combined['winner_prediction'] = combined.apply(
    lambda row: row['prediction_price'] if row['actual_outcome'] == 1 else 1 - row['prediction_price'],
    axis=1
)

print(f"   ✓ {(combined['actual_outcome'] == 1).sum()} winner markets used directly")
print(f"   ✓ {(combined['actual_outcome'] == 0).sum()} loser markets flipped to get winner prediction")

# Calculate metrics for winner prediction
combined['prediction'] = combined['winner_prediction']
combined['actual_outcome_winner'] = 1  # Winner always has outcome = 1
combined['pred_error'] = combined['prediction'] - combined['actual_outcome_winner']
combined['abs_error'] = abs(combined['pred_error'])
combined['brier'] = combined['pred_error'] ** 2

print(f"   ✓ Mean Brier: {combined['brier'].mean():.4f}")
print(f"   ✓ Mean Abs Error: {combined['abs_error'].mean():.4f}")

# ============================================================================
# 5. Stratify by margin
# ============================================================================

print("\n5. Creating table by race margin...")

bins = [0, 5, 10, 20, 100]
labels = ['<5%', '5-10%', '10-20%', '>20%']
combined['margin_bin'] = pd.cut(combined['vote_margin'], bins=bins, labels=labels, include_lowest=True)

results = []
for margin_bin in labels:
    data = combined[combined['margin_bin'] == margin_bin]
    if len(data) == 0:
        continue

    # Count unique elections (some may appear twice, once per platform)
    n_unique_elections = data.groupby(election_cols).ngroups

    results.append({
        'Margin': margin_bin,
        'N_Markets': len(data),
        'N_Elections': n_unique_elections,
        'Brier': data['brier'].mean(),
        'Abs_Error': data['abs_error'].mean(),
        'Mean_Pred': data['prediction'].mean(),
        'Accuracy': (data['prediction'].round() == data['actual_outcome_winner']).mean()
    })

results_df = pd.DataFrame(results)
print(results_df.to_string(index=False))

# Save CSV
output_csv = f"{DATA_DIR}/calibration_by_race_margin.csv"
results_df.to_csv(output_csv, index=False)
print(f"\n   ✓ Saved to {output_csv}")

# Save LaTeX
results_df_latex = results_df.copy()
results_df_latex.columns = ['Margin', 'N Markets', 'N Elections', 'Brier', 'Abs Error', 'Mean Pred', 'Accuracy']

# Escape < and > and % symbols
results_df_latex['Margin'] = results_df_latex['Margin'].str.replace('<', '$<$').str.replace('>', '$>$').str.replace('%', '\\%')

os.makedirs(f"{BASE_DIR}/tables", exist_ok=True)
latex_content = results_df_latex.to_latex(
    index=False,
    float_format="%.4f",
    caption='Calibration Metrics by Race Margin (1 Day Before Resolution)',
    label='tab:calibration_margin',
    escape=False
)
# Add \centering after \begin{table}
latex_content = latex_content.replace(r'\begin{table}', r'\begin{table}' + '\n' + r'\centering')
with open(f"{BASE_DIR}/tables/calibration_by_margin.tex", 'w') as f:
    f.write(latex_content)

print(f"   ✓ Saved LaTeX to tables/calibration_by_margin.tex")

# ============================================================================
# 6. Create scatter plot
# ============================================================================

print("\n6. Creating scatter plot...")

fig, ax = plt.subplots(figsize=(14, 8))

# Scatter plot: Brier Score vs Vote Margin - with platform-specific colors
for platform, color in [('Polymarket', COLORS['polymarket']), ('Kalshi', COLORS['kalshi'])]:
    platform_data = combined[combined['platform'] == platform]
    ax.scatter(
        platform_data['vote_margin'],
        platform_data['brier'],
        color=color,
        alpha=0.6,
        s=80,
        edgecolors='white',
        linewidth=0.5,
        label=f'{platform} (N={len(platform_data)})'
    )

# Reference line at Brier = 0 (perfect calibration)
ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.3)

ax.set_xlabel('Actual Vote Margin (%)', fontsize=14, fontweight='bold')
ax.set_ylabel('Brier Score', fontsize=14, fontweight='bold')
ax.set_title('Market Calibration vs Race Margin\n(1 Day Before Resolution, Includes Primaries)',
             fontsize=16, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)
ax.legend(fontsize=11, loc='upper right')

# Set y-axis with padding below 0 to show all points
ax.set_ylim(-0.01, None)

# Set x-axis to show all data with some padding
x_max = combined['vote_margin'].max()
ax.set_xlim(-1, max(60, x_max + 5))

# Count primaries included
n_primaries = combined['is_primary'].sum() if 'is_primary' in combined.columns else 0
ax.text(0.02, 0.98,
        f'N = {len(combined):,} markets\n(includes {n_primaries} primaries)',
        transform=ax.transAxes,
        fontsize=11,
        verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

plt.tight_layout()
output_plot = f"{GRAPH_DIR}/brier_vs_margin.png"
plt.savefig(output_plot, dpi=300, bbox_inches='tight')
print(f"   ✓ Saved to {output_plot}")

print("\n" + "="*80)
print("DONE")
print("="*80)
