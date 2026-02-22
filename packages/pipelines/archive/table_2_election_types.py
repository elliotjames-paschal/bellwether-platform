#!/usr/bin/env python3
"""
Table 2: Aggregate Statistics by Election Type
Similar to Table 1 but grouped by election type rather than political category.
Combines Polymarket and Kalshi data.
"""

import pandas as pd
import numpy as np
import json
import os

# Color scheme (for potential PNG output)
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#34495E',
    'tertiary': '#7F8C8D',
    'light_gray': '#95A5A6',
    'dark': '#1a1a1a',
}

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"

print("="*80)
print("TABLE 2: AGGREGATE STATISTICS BY ELECTION TYPE")
print("="*80)

# ============================================================================
# Load Electoral Markets from Master CSV (US + International)
# ============================================================================
print(f"\n📊 Loading electoral markets from master CSV...")
print(f"  File: {MASTER_FILE}")
df_master = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded {len(df_master):,} total market records")

# Filter to Polymarket electoral markets
df_pm_all = df_master[df_master['platform'] == 'Polymarket'].copy()
print(f"✓ Filtered to {len(df_pm_all):,} total Polymarket markets")

# Ensure market_id types match for merging
df_pm_all['market_id'] = df_pm_all['market_id'].astype(str)

pm_electoral_all = df_pm_all[df_pm_all['political_category'] == '1. ELECTORAL'].copy()
print(f"  Found {len(pm_electoral_all):,} Polymarket electoral rows")

# Deduplicate by market_id (each market has Yes/No rows)
pm_electoral = pm_electoral_all.drop_duplicates(subset='market_id', keep='first').copy()
pm_electoral['platform'] = 'Polymarket'
print(f"✓ Deduplicated to {len(pm_electoral):,} unique Polymarket electoral markets")

# Filter to Kalshi electoral markets
df_kalshi_all = df_master[df_master['platform'] == 'Kalshi'].copy()
print(f"✓ Filtered to {len(df_kalshi_all):,} total Kalshi markets")

# Ensure market_id types match for merging
df_kalshi_all['market_id'] = df_kalshi_all['market_id'].astype(str)

kalshi_electoral = df_kalshi_all[
    (df_kalshi_all['election_type'].notna()) &
    (df_kalshi_all['election_type'] != 'NA')
].copy()
kalshi_electoral['platform'] = 'Kalshi'
print(f"✓ Filtered to {len(kalshi_electoral):,} Kalshi electoral markets")

# ============================================================================
# Combine Polymarket and Kalshi Electoral Markets
# ============================================================================
print(f"\n📊 Combining Polymarket and Kalshi electoral markets...")
# Keep only needed columns and ensure they match
pm_subset = pm_electoral[['election_type', 'volume_usd', 'platform']].copy()
pm_subset = pm_subset.rename(columns={'volume_usd': 'volume'})
kalshi_subset = kalshi_electoral[['election_type', 'volume_usd', 'platform']].copy()
kalshi_subset = kalshi_subset.rename(columns={'volume_usd': 'volume'})

# Combine
electoral = pd.concat([pm_subset, kalshi_subset], ignore_index=True)
print(f"✓ Combined total: {len(electoral):,} electoral markets (PM: {len(pm_subset):,}, Kalshi: {len(kalshi_subset):,})")

print(f"\n{'='*80}")
print("CALCULATING AGGREGATE STATISTICS BY ELECTION TYPE")
print(f"{'='*80}\n")

# Filter to markets with election_type (exclude NA/null)
electoral_with_type = electoral[
    (electoral['election_type'].notna()) &
    (electoral['election_type'] != 'NA') &
    (electoral['election_type'] != '')
].copy()

print(f"Electoral markets with election_type: {len(electoral_with_type):,}")

all_election_types = sorted(electoral_with_type['election_type'].unique())

results = []
total_markets_counted = 0

for election_type in all_election_types:
    # Filter to this election type
    type_markets = electoral_with_type[electoral_with_type['election_type'] == election_type]

    total_markets = len(type_markets)
    total_vol = type_markets['volume'].sum()
    avg_vol = type_markets['volume'].mean() if total_markets > 0 else 0
    med_vol = type_markets['volume'].median() if total_markets > 0 else 0

    results.append({
        'Election Type': election_type,  # Already standardized, no need to clean
        'Total Markets': total_markets,
        'Average Volume ($K)': avg_vol / 1_000,
        'Median Volume ($K)': med_vol / 1_000,
        'Total Volume ($M)': total_vol / 1_000_000
    })

    total_markets_counted += total_markets
    print(f"{election_type:30s} | Markets: {total_markets:5,} | Avg: ${avg_vol/1e3:8.1f}K | Total: ${total_vol/1e6:8.1f}M")

# Create DataFrame
table = pd.DataFrame(results)

# Sort by total markets (descending)
table = table.sort_values('Total Markets', ascending=False).reset_index(drop=True)

# Use full table - no "Other" category
table_final = table
other_types = pd.DataFrame()  # Empty dataframe for compatibility with later code

# Display summary
print(f"\n{'='*80}")
print("TABLE SUMMARY")
print(f"{'='*80}\n")
print(f"Total election types: {len(all_election_types)}")
print(f"Total markets in table: {table_final['Total Markets'].sum():,}")
print(f"Total volume: ${table_final['Total Volume ($M)'].sum():,.1f}M")

# ============================================================================
# Generate LaTeX Table
# ============================================================================

print(f"\n{'='*80}")
print("GENERATING LATEX TABLE")
print(f"{'='*80}\n")

latex_output = f"{BASE_DIR}/tables/table_2_election_types.tex"

with open(latex_output, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Aggregate Statistics by Election Type (Polymarket and Kalshi Combined, All Electoral Markets: US and International)}' + '\n')
    f.write(r'\label{tab:election_types}' + '\n')
    f.write(r'\footnotesize' + '\n')
    f.write(r'\begin{tabular}{lrrrr}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'Election Type & Total Markets & Avg Volume (\$K) & Median Volume (\$K) & Total Volume (\$M) \\' + '\n')
    f.write(r'\midrule' + '\n')

    for idx, row in table_final.iterrows():
        f.write(f"{row['Election Type']} & {row['Total Markets']:,} & {row['Average Volume ($K)']:,.1f} & {row['Median Volume ($K)']:,.1f} & {row['Total Volume ($M)']:,.1f} \\\\\n")

    # Add totals row
    total_markets = table_final['Total Markets'].sum()
    total_volume = table_final['Total Volume ($M)'].sum()
    # Weighted average for average volume
    weighted_avg_volume = (table_final['Total Markets'] * table_final['Average Volume ($K)']).sum() / total_markets
    # Overall median
    overall_median = table_final['Median Volume ($K)'].median()

    f.write(r'\midrule' + '\n')
    f.write(f"\\textbf{{Total}} & \\textbf{{{total_markets:,}}} & \\textbf{{{weighted_avg_volume:,.1f}}} & \\textbf{{{overall_median:,.1f}}} & \\textbf{{{total_volume:,.1f}}} \\\\\n")

    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"✓ LaTeX table saved to: {latex_output}")

print(f"\n{'='*80}")
print("COMPLETE")
print(f"{'='*80}")
