#!/usr/bin/env python3
"""
Table 3: Platform Comparison by Political Category
Side-by-side comparison of Polymarket and Kalshi showing market counts and volumes
for each platform separately, allowing direct comparison of platform characteristics.
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
print("TABLE 3: PLATFORM COMPARISON BY POLITICAL CATEGORY")
print("="*80)

# ============================================================================
# Load Data from Master CSV
# ============================================================================
print(f"\n📊 Loading data from master CSV: {MASTER_FILE}")
df_master = pd.read_csv(MASTER_FILE, low_memory=False)
print(f"✓ Loaded {len(df_master):,} total market records")

# Filter to Polymarket
df_pm = df_master[df_master['platform'] == 'Polymarket'].copy()
print(f"✓ Filtered to {len(df_pm):,} Polymarket rows")

# Ensure market_id types match for merging
df_pm['market_id'] = df_pm['market_id'].astype(str)

# Deduplicate by market_id (each market has Yes/No rows)
print(f"  Deduplicating by market_id...")
pm_unique = df_pm.drop_duplicates(subset='market_id', keep='first').copy()
print(f"✓ {len(pm_unique):,} unique Polymarket markets")

# Filter to Kalshi
df_kalshi = df_master[df_master['platform'] == 'Kalshi'].copy()
print(f"✓ Filtered to {len(df_kalshi):,} Kalshi markets")

# Ensure market_id types match for merging
df_kalshi['market_id'] = df_kalshi['market_id'].astype(str)

print(f"\n{'='*80}")
print("CALCULATING STATISTICS BY CATEGORY AND PLATFORM")
print(f"{'='*80}\n")

# Get all unique categories across both platforms
all_categories = sorted(set(pm_unique['political_category'].unique()) |
                       set(df_kalshi['political_category'].unique()))

results = []

for category in all_categories:
    # Polymarket stats
    pm_cat = pm_unique[pm_unique['political_category'] == category]
    pm_markets = len(pm_cat)
    pm_total_vol = pm_cat['volume_usd'].sum() if pm_markets > 0 else 0
    pm_avg_vol = pm_cat['volume_usd'].mean() if pm_markets > 0 else 0

    # Kalshi stats
    k_cat = df_kalshi[df_kalshi['political_category'] == category]
    k_markets = len(k_cat)
    k_total_vol = k_cat['volume_usd'].sum() if k_markets > 0 else 0
    k_avg_vol = k_cat['volume_usd'].mean() if k_markets > 0 else 0

    # Combined
    total_markets = pm_markets + k_markets
    total_vol = pm_total_vol + k_total_vol

    # Clean category name - remove number prefix (e.g., "1. ELECTORAL" -> "Electoral")
    clean_category = category
    if '. ' in category:
        clean_category = category.split('. ', 1)[1]
    clean_category = clean_category.replace('_', ' ').title()

    results.append({
        'Category': clean_category,
        'PM Markets': pm_markets,
        'Kalshi Markets': k_markets,
        'Total Markets': total_markets,
        'PM Avg Vol ($K)': pm_avg_vol / 1_000,
        'Kalshi Avg Vol ($K)': k_avg_vol / 1_000,
        'PM Total Vol ($M)': pm_total_vol / 1_000_000,
        'Kalshi Total Vol ($M)': k_total_vol / 1_000_000,
        'Total Vol ($M)': total_vol / 1_000_000
    })

    print(f"{category:30s} | PM: {pm_markets:5,} mkts (${pm_total_vol/1e6:7.1f}M) | Kalshi: {k_markets:5,} mkts (${k_total_vol/1e6:7.1f}M)")

# Create DataFrame
table = pd.DataFrame(results)

# Sort by total volume (descending)
table = table.sort_values('Total Vol ($M)', ascending=False).reset_index(drop=True)

# Display summary
print(f"\n{'='*80}")
print("TABLE SUMMARY")
print(f"{'='*80}\n")
print(f"Total categories: {len(all_categories)}")
print(f"Total Polymarket markets: {table['PM Markets'].sum():,}")
print(f"Total Kalshi markets: {table['Kalshi Markets'].sum():,}")
print(f"Total Polymarket volume: ${table['PM Total Vol ($M)'].sum():,.1f}M")
print(f"Total Kalshi volume: ${table['Kalshi Total Vol ($M)'].sum():,.1f}M")
print(f"Combined total volume: ${table['Total Vol ($M)'].sum():,.1f}M")

# ============================================================================
# Generate LaTeX Table
# ============================================================================

print(f"\n{'='*80}")
print("GENERATING LATEX TABLE")
print(f"{'='*80}\n")

latex_output = f"{BASE_DIR}/tables/table_3_platform_comparison.tex"

with open(latex_output, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Platform Comparison by Political Category}' + '\n')
    f.write(r'\label{tab:platform_comparison}' + '\n')
    f.write(r'\footnotesize' + '\n')
    f.write(r'\begin{tabular}{lrrrrrr}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'Category & \multicolumn{2}{c}{Markets} & \multicolumn{2}{c}{Avg Volume (\$K)} & \multicolumn{2}{c}{Total Volume (\$M)} \\' + '\n')
    f.write(r'\cmidrule(lr){2-3} \cmidrule(lr){4-5} \cmidrule(lr){6-7}' + '\n')
    f.write(r' & PM & Kalshi & PM & Kalshi & PM & Kalshi \\' + '\n')
    f.write(r'\midrule' + '\n')

    for idx, row in table.iterrows():
        f.write(f"{row['Category']} & {row['PM Markets']:,} & {row['Kalshi Markets']:,} & "
                f"{row['PM Avg Vol ($K)']:,.1f} & {row['Kalshi Avg Vol ($K)']:,.1f} & "
                f"{row['PM Total Vol ($M)']:,.1f} & {row['Kalshi Total Vol ($M)']:,.1f} \\\\\n")

    # Add totals row
    total_pm_markets = table['PM Markets'].sum()
    total_kalshi_markets = table['Kalshi Markets'].sum()
    total_pm_vol = table['PM Total Vol ($M)'].sum()
    total_kalshi_vol = table['Kalshi Total Vol ($M)'].sum()
    # Weighted averages
    pm_weighted_avg = (table['PM Markets'] * table['PM Avg Vol ($K)']).sum() / total_pm_markets if total_pm_markets > 0 else 0
    kalshi_weighted_avg = (table['Kalshi Markets'] * table['Kalshi Avg Vol ($K)']).sum() / total_kalshi_markets if total_kalshi_markets > 0 else 0

    f.write(r'\midrule' + '\n')
    f.write(f"\\textbf{{Total}} & \\textbf{{{total_pm_markets:,}}} & \\textbf{{{total_kalshi_markets:,}}} & "
            f"\\textbf{{{pm_weighted_avg:,.1f}}} & \\textbf{{{kalshi_weighted_avg:,.1f}}} & "
            f"\\textbf{{{total_pm_vol:,.1f}}} & \\textbf{{{total_kalshi_vol:,.1f}}} \\\\\n")

    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"✓ LaTeX table saved to: {latex_output}")

print(f"\n{'='*80}")
print("COMPLETE")
print(f"{'='*80}")
