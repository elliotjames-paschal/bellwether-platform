#!/usr/bin/env python3
"""
Table 4: Brier Score Comparison by Election Type
Creates two tables comparing Polymarket and Kalshi Brier scores for election markets:
- Table 4a: Standard Brier scores (equal weight per market)
- Table 4b: Volume-weighted Brier scores

Uses 1-day before resolution data.
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

# Color scheme
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#34495E',
    'tertiary': '#7F8C8D',
    'light_gray': '#95A5A6',
    'dark': '#1a1a1a',
}

# Paths
from config import BASE_DIR, DATA_DIR, get_latest_file
from paper_config import load_master_csv, load_prediction_accuracy, PAPER_GRAPHS_DIR, PAPER_TABLES_DIR

GRAPH_DIR = str(PAPER_GRAPHS_DIR)
os.makedirs(GRAPH_DIR, exist_ok=True)

print("="*80)
print("TABLE 4: BRIER SCORE COMPARISON BY ELECTION TYPE")
print("="*80)

# ============================================================================
# Load Market Metadata (All Electoral Markets: US + International)
# ============================================================================
print(f"\n📊 Loading market metadata from master CSV...")

# Load master CSV
df_master = load_master_csv()
print(f"✓ Loaded {len(df_master):,} total market records")

# Filter to Polymarket electoral markets
df_pm_markets = df_master[df_master['platform'] == 'Polymarket'].copy()
print(f"✓ Filtered to {len(df_pm_markets):,} total Polymarket markets")

df_pm_electoral = df_pm_markets[df_pm_markets['political_category'] == '1. ELECTORAL'].copy()
print(f"✓ Filtered to {len(df_pm_electoral):,} Polymarket electoral markets")

# Keep only needed columns from metadata
pm_electoral_meta = df_pm_electoral[['market_id', 'election_type', 'volume_usd']].copy()
pm_electoral_meta = pm_electoral_meta.rename(columns={'volume_usd': 'volume'})

# Ensure market_id types match for merging
pm_electoral_meta['market_id'] = pm_electoral_meta['market_id'].astype(str)

# Filter to Kalshi electoral markets
df_kalshi_markets = df_master[df_master['platform'] == 'Kalshi'].copy()
print(f"✓ Filtered to {len(df_kalshi_markets):,} total Kalshi markets")

df_kalshi_electoral = df_kalshi_markets[
    (df_kalshi_markets['election_type'].notna()) &
    (df_kalshi_markets['election_type'] != 'NA')
].copy()
print(f"✓ Filtered to {len(df_kalshi_electoral):,} Kalshi electoral markets")

# Keep only needed columns from metadata
kalshi_electoral_meta = df_kalshi_electoral[['market_id', 'election_type', 'volume_usd']].copy()
kalshi_electoral_meta = kalshi_electoral_meta.rename(columns={'volume_usd': 'volume'})

# Ensure market_id types match for merging
kalshi_electoral_meta['market_id'] = kalshi_electoral_meta['market_id'].astype(str)

# ============================================================================
# Load Polymarket Prediction Data
# ============================================================================
print(f"\n📊 Loading Polymarket prediction data...")

# Load prediction accuracy
df_pm_pred = load_prediction_accuracy("polymarket")
print(f"✓ Loaded {len(df_pm_pred):,} Polymarket prediction rows")

# Ensure market_id types match for merging
df_pm_pred['market_id'] = df_pm_pred['market_id'].astype(str)

# Join prediction data with metadata
df_pm = df_pm_pred.merge(pm_electoral_meta, on='market_id', how='inner', suffixes=('_pred', ''))
print(f"✓ Joined: {len(df_pm):,} rows with election type and volume")

# Use election_type and volume from market metadata (not prediction file)
# Market metadata has the updated categories after recategorization
if 'election_type_pred' in df_pm.columns:
    df_pm = df_pm.drop(columns=['election_type_pred'])
if 'volume_pred' in df_pm.columns:
    df_pm = df_pm.drop(columns=['volume_pred'])

# Filter to elections only (election_type is not 'NA')
df_pm = df_pm[(df_pm['election_type'].notna()) & (df_pm['election_type'] != 'NA')].copy()
print(f"✓ Filtered to {len(df_pm):,} election prediction rows")

# ============================================================================
# Load Kalshi Prediction Data
# ============================================================================
print(f"\n📊 Loading Kalshi prediction data...")

# Load prediction accuracy
df_kalshi_pred = load_prediction_accuracy("kalshi")
print(f"✓ Loaded {len(df_kalshi_pred):,} Kalshi prediction rows")

# Ensure market_id types match for merging
if 'ticker' in df_kalshi_pred.columns and 'market_id' not in df_kalshi_pred.columns:
    df_kalshi_pred['market_id'] = df_kalshi_pred['ticker'].astype(str)
elif 'market_id' in df_kalshi_pred.columns:
    df_kalshi_pred['market_id'] = df_kalshi_pred['market_id'].astype(str)

# Join prediction data with metadata to get volume and standardized election types
df_kalshi = df_kalshi_pred.merge(kalshi_electoral_meta[['market_id', 'election_type', 'volume']],
                                  on='market_id', how='inner', suffixes=('_pred', ''))
print(f"✓ Joined: {len(df_kalshi):,} rows with election type and volume")

# ============================================================================
# Filter to 1 Day Before Resolution and Elections Only
# ============================================================================
print(f"\n{'='*80}")
print("FILTERING DATA")
print(f"{'='*80}")

# Filter Polymarket
print(f"\nPolymarket:")
print(f"  Before filtering: {len(df_pm):,} rows")
df_pm = df_pm[
    (df_pm['days_before_event'] == 1) &
    (df_pm['outcome_name'] == 'Yes') &  # Avoid double-counting
    (df_pm['election_type'].notna()) &
    (df_pm['election_type'] != 'NA')
].copy()
print(f"  After filtering (1d before, Yes only, has election_type): {len(df_pm):,} rows")

# Filter Kalshi
print(f"\nKalshi:")
print(f"  Before filtering: {len(df_kalshi):,} rows")
df_kalshi = df_kalshi[
    (df_kalshi['days_before_event'] == 1) &
    (df_kalshi['election_type'].notna()) &
    (df_kalshi['election_type'] != 'NA')
].copy()
print(f"  After filtering (1d before, has election_type): {len(df_kalshi):,} rows")

# ============================================================================
# Calculate Brier Scores by Election Type
# ============================================================================
print(f"\n{'='*80}")
print("CALCULATING BRIER SCORES BY ELECTION TYPE")
print(f"{'='*80}")

# Calculate Brier score (prediction_price - actual_outcome)^2
df_pm['brier_score'] = (df_pm['prediction_price'] - df_pm['actual_outcome']) ** 2
df_kalshi['brier_score'] = (df_kalshi['prediction_price'] - df_kalshi['actual_outcome']) ** 2

# Get all unique election types
all_election_types = sorted(set(df_pm['election_type'].unique()) |
                            set(df_kalshi['election_type'].unique()))

print(f"\n📋 Found {len(all_election_types)} unique election types")

results_standard = []
results_weighted = []

for election_type in all_election_types:
    # Polymarket data for this election type
    pm_elec = df_pm[df_pm['election_type'] == election_type]
    n_pm = len(pm_elec)

    # Kalshi data for this election type
    k_elec = df_kalshi[df_kalshi['election_type'] == election_type]
    n_k = len(k_elec)

    # Standard Brier scores (equal weight)
    pm_brier_std = pm_elec['brier_score'].mean() if n_pm > 0 else np.nan
    k_brier_std = k_elec['brier_score'].mean() if n_k > 0 else np.nan

    # Volume-weighted Brier scores
    if n_pm > 0:
        pm_brier_weighted = (pm_elec['brier_score'] * pm_elec['volume']).sum() / pm_elec['volume'].sum()
    else:
        pm_brier_weighted = np.nan

    if n_k > 0:
        k_brier_weighted = (k_elec['brier_score'] * k_elec['volume']).sum() / k_elec['volume'].sum()
    else:
        k_brier_weighted = np.nan

    # Standard table row
    results_standard.append({
        'Election Type': election_type,
        'PM Markets': n_pm,
        'Kalshi Markets': n_k,
        'Total Markets': n_pm + n_k,
        'PM Brier': pm_brier_std,
        'Kalshi Brier': k_brier_std
    })

    # Weighted table row
    results_weighted.append({
        'Election Type': election_type,
        'PM Markets': n_pm,
        'Kalshi Markets': n_k,
        'Total Markets': n_pm + n_k,
        'PM Brier': pm_brier_weighted,
        'Kalshi Brier': k_brier_weighted
    })

    print(f"\n{election_type}:")
    print(f"  PM: {n_pm:4,} markets, Brier={pm_brier_std:.4f} (std), {pm_brier_weighted:.4f} (weighted)")
    print(f"  Kalshi: {n_k:4,} markets, Brier={k_brier_std:.4f} (std), {k_brier_weighted:.4f} (weighted)")

# Create DataFrames
table_std = pd.DataFrame(results_standard)
table_weighted = pd.DataFrame(results_weighted)

# Sort by total markets
table_std = table_std.sort_values('Total Markets', ascending=False).reset_index(drop=True)
table_weighted = table_weighted.sort_values('Total Markets', ascending=False).reset_index(drop=True)

# Calculate overall totals for standard Brier scores
total_pm_markets = table_std['PM Markets'].sum()
total_k_markets = table_std['Kalshi Markets'].sum()
total_all_markets = table_std['Total Markets'].sum()

# Filter to 1 day before for overall calculation
df_pm_1d = df_pm[df_pm['days_before_event'] == 1].copy()
df_kalshi_1d = df_kalshi[df_kalshi['days_before_event'] == 1].copy()

# Overall standard Brier score (average across all markets, 1 day before)
overall_pm_brier_std = df_pm_1d['brier_score'].mean()
overall_k_brier_std = df_kalshi_1d['brier_score'].mean()

# Overall volume-weighted Brier score
overall_pm_brier_weighted = (df_pm_1d['brier_score'] * df_pm_1d['volume']).sum() / df_pm_1d['volume'].sum()
overall_k_brier_weighted = (df_kalshi_1d['brier_score'] * df_kalshi_1d['volume']).sum() / df_kalshi_1d['volume'].sum()

# Add totals row to standard table
totals_std = pd.DataFrame([{
    'Election Type': '\\textbf{Overall}',
    'PM Markets': total_pm_markets,
    'Kalshi Markets': total_k_markets,
    'Total Markets': total_all_markets,
    'PM Brier': overall_pm_brier_std,
    'Kalshi Brier': overall_k_brier_std
}])
table_std = pd.concat([table_std, totals_std], ignore_index=True)

# Add totals row to weighted table
totals_weighted = pd.DataFrame([{
    'Election Type': '\\textbf{Overall}',
    'PM Markets': total_pm_markets,
    'Kalshi Markets': total_k_markets,
    'Total Markets': total_all_markets,
    'PM Brier': overall_pm_brier_weighted,
    'Kalshi Brier': overall_k_brier_weighted
}])
table_weighted = pd.concat([table_weighted, totals_weighted], ignore_index=True)

print(f"\n{'='*80}")
print(f"OVERALL BRIER SCORES:")
print(f"{'='*80}")
print(f"Standard Brier Scores:")
print(f"  Polymarket: {overall_pm_brier_std:.4f} ({total_pm_markets:,} markets)")
print(f"  Kalshi:     {overall_k_brier_std:.4f} ({total_k_markets:,} markets)")
print(f"\nVolume-Weighted Brier Scores:")
print(f"  Polymarket: {overall_pm_brier_weighted:.4f}")
print(f"  Kalshi:     {overall_k_brier_weighted:.4f}")

# ============================================================================
# Generate LaTeX Tables
# ============================================================================
print(f"\n{'='*80}")
print("GENERATING LATEX TABLES")
print(f"{'='*80}")

# Table 4a: Standard Brier Scores
latex_output_a = f"{PAPER_TABLES_DIR}/table_4a_brier_election_standard.tex"
with open(latex_output_a, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Brier Score Comparison by Election Type (All Electoral Markets: US and International, Standard, 1 Day Before Resolution)}' + '\n')
    f.write(r'\label{tab:brier_election_standard}' + '\n')
    f.write(r'\footnotesize' + '\n')
    f.write(r'\begin{tabular}{lrrrr}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'Election Type & PM Markets & Kalshi Markets & PM Brier & Kalshi Brier \\' + '\n')
    f.write(r'\midrule' + '\n')

    for idx, row in table_std.iterrows():
        # Add midrule before the Overall row
        if row['Election Type'] == '\\textbf{Overall}':
            f.write(r'\midrule' + '\n')

        pm_brier_str = f"{row['PM Brier']:.4f}" if pd.notna(row['PM Brier']) else '-'
        k_brier_str = f"{row['Kalshi Brier']:.4f}" if pd.notna(row['Kalshi Brier']) else '-'
        f.write(f"{row['Election Type']} & {row['PM Markets']:,} & {row['Kalshi Markets']:,} & "
                f"{pm_brier_str} & {k_brier_str} \\\\\n")

    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\begin{tablenotes}' + '\n')
    f.write(r'\small' + '\n')
    f.write(r'\item Note: Brier score = mean of (prediction - outcome)$^2$. Lower is better. Calculated 1 day before resolution.' + '\n')
    f.write(r'\end{tablenotes}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"✓ Table 4a saved: {latex_output_a}")

# Table 4b: Volume-Weighted Brier Scores
latex_output_b = f"{PAPER_TABLES_DIR}/table_4b_brier_election_weighted.tex"
with open(latex_output_b, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Brier Score Comparison by Election Type (All Electoral Markets: US and International, Volume-Weighted, 1 Day Before Resolution)}' + '\n')
    f.write(r'\label{tab:brier_election_weighted}' + '\n')
    f.write(r'\footnotesize' + '\n')
    f.write(r'\begin{tabular}{lrrrr}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'Election Type & PM Markets & Kalshi Markets & PM Brier & Kalshi Brier \\' + '\n')
    f.write(r'\midrule' + '\n')

    for idx, row in table_weighted.iterrows():
        # Add midrule before the Overall row
        if row['Election Type'] == '\\textbf{Overall}':
            f.write(r'\midrule' + '\n')

        pm_brier_str = f"{row['PM Brier']:.4f}" if pd.notna(row['PM Brier']) else '-'
        k_brier_str = f"{row['Kalshi Brier']:.4f}" if pd.notna(row['Kalshi Brier']) else '-'
        f.write(f"{row['Election Type']} & {row['PM Markets']:,} & {row['Kalshi Markets']:,} & "
                f"{pm_brier_str} & {k_brier_str} \\\\\n")

    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\begin{tablenotes}' + '\n')
    f.write(r'\small' + '\n')
    f.write(r'\item Note: Brier scores weighted by market trading volume. Lower is better. Calculated 1 day before resolution.' + '\n')
    f.write(r'\end{tablenotes}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"✓ Table 4b saved: {latex_output_b}")

# ============================================================================
# Generate Bar Graph Visualizations
# ============================================================================
print(f"\n{'='*80}")
print("GENERATING BAR GRAPH VISUALIZATIONS")
print(f"{'='*80}")

# Platform colors - grayscale to match paper style
PM_COLOR = '#2C3E50'  # Dark gray for Polymarket
KALSHI_COLOR = '#95A5A6'  # Light gray for Kalshi

def create_brier_bar_chart(table_data, title, output_file, weighted=False):
    """Create a grouped horizontal bar chart comparing PM and Kalshi Brier scores."""
    import seaborn as sns
    sns.set_style("whitegrid")

    # Filter out the Overall row for the chart
    chart_data = table_data[table_data['Election Type'] != '\\textbf{Overall}'].copy()

    # Sort by total markets (largest at top)
    chart_data = chart_data.sort_values('Total Markets', ascending=True)

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 10))

    # Bar positions
    y_pos = np.arange(len(chart_data))
    bar_height = 0.35

    # Create bars with edge colors for clarity
    pm_bars = ax.barh(y_pos + bar_height/2, chart_data['PM Brier'].fillna(0),
                      bar_height, label='Polymarket', color=PM_COLOR,
                      edgecolor='#1a1a1a', linewidth=0.5, alpha=0.85)
    kalshi_bars = ax.barh(y_pos - bar_height/2, chart_data['Kalshi Brier'].fillna(0),
                          bar_height, label='Kalshi', color=KALSHI_COLOR,
                          edgecolor='#1a1a1a', linewidth=0.5, alpha=0.85)

    # Add value labels on bars
    for i, (pm_val, k_val, pm_n, k_n) in enumerate(zip(
            chart_data['PM Brier'], chart_data['Kalshi Brier'],
            chart_data['PM Markets'], chart_data['Kalshi Markets'])):
        # PM label
        if pd.notna(pm_val) and pm_val > 0:
            ax.text(pm_val + 0.003, y_pos[i] + bar_height/2,
                   f'{pm_val:.3f} (n={pm_n:,})', va='center', fontsize=9, color='#1a1a1a')
        # Kalshi label
        if pd.notna(k_val) and k_val > 0:
            ax.text(k_val + 0.003, y_pos[i] - bar_height/2,
                   f'{k_val:.3f} (n={k_n:,})', va='center', fontsize=9, color='#1a1a1a')

    # Labels and formatting
    ax.set_yticks(y_pos)
    ax.set_yticklabels(chart_data['Election Type'], fontsize=11)
    ax.set_xlabel('Brier Score (Lower is Better)', fontsize=13, fontweight='bold')
    ax.set_title(title, fontsize=15, fontweight='bold', pad=20)

    # Legend
    ax.legend(loc='lower right', fontsize=11, framealpha=0.9)

    # Grid
    ax.grid(True, axis='x', alpha=0.3, zorder=0)
    ax.set_axisbelow(True)

    # Adjust x-axis to show full labels
    max_brier = max(chart_data['PM Brier'].max(), chart_data['Kalshi Brier'].max())
    ax.set_xlim(0, max_brier * 1.5)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✓ Bar chart saved: {output_file}")
    plt.close()

# Bar chart for Standard Brier Scores
png_output_a = f"{GRAPH_DIR}/brier_by_election_type_standard.png"
create_brier_bar_chart(
    table_std,
    'Brier Score Comparison by Election Type\n(Standard, 1 Day Before Resolution)',
    png_output_a
)

# Bar chart for Volume-Weighted Brier Scores
png_output_b = f"{GRAPH_DIR}/brier_by_election_type_weighted.png"
create_brier_bar_chart(
    table_weighted,
    'Brier Score Comparison by Election Type\n(Volume-Weighted, 1 Day Before Resolution)',
    png_output_b,
    weighted=True
)

# ============================================================================
# Summary
# ============================================================================
print(f"\n{'='*80}")
print("SUMMARY")
print(f"{'='*80}")
print(f"Election types analyzed: {len(all_election_types)}")
print(f"Total Polymarket markets: {table_std['PM Markets'].sum():,}")
print(f"Total Kalshi markets: {table_std['Kalshi Markets'].sum():,}")
print(f"\nOutputs:")
print(f"  LaTeX: {latex_output_a}")
print(f"  LaTeX: {latex_output_b}")
print(f"  Bar Chart: {png_output_a}")
print(f"  Bar Chart: {png_output_b}")
print(f"{'='*80}")
