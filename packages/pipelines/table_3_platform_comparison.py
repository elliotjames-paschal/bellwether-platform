#!/usr/bin/env python3
"""
Table 3: Platform Comparison Statistics

Compares Polymarket and Kalshi across key metrics to show how the platforms differ.

Output:
- tables/table_3_platform_comparison.tex (LaTeX table)
- data/table_3_platform_comparison.csv (for website)
"""

import pandas as pd
import numpy as np
import os

from config import BASE_DIR, DATA_DIR
from paper_config import load_master_csv, PAPER_TABLES_DIR, PAPER_DATA_DIR

# Paths
TABLES_DIR = PAPER_TABLES_DIR
TABLES_DIR.mkdir(exist_ok=True)

print("=" * 70)
print("TABLE 3: PLATFORM COMPARISON STATISTICS")
print("=" * 70)

# Load data
print(f"\nLoading master CSV...")
df = load_master_csv()
df['trading_close_time'] = pd.to_datetime(df['trading_close_time'], format='mixed', utc=True, errors='coerce')
print(f"  Loaded {len(df):,} total markets")

# Split by platform
pm = df[df['platform'] == 'Polymarket']
kalshi = df[df['platform'] == 'Kalshi']

print(f"  Polymarket: {len(pm):,}")
print(f"  Kalshi: {len(kalshi):,}")

# Calculate metrics
metrics = []

# 1. Total number of political markets
metrics.append({
    'Metric': 'Total Political Markets',
    'Polymarket': len(pm),
    'Kalshi': len(kalshi)
})

# 2. Number of resolved markets
pm_resolved = pm[
    (pm['is_closed'] == True) |
    (pm['winning_outcome'].notna())
]
kalshi_resolved = kalshi[
    (kalshi['is_closed'] == True) |
    (kalshi['winning_outcome'].notna())
]
metrics.append({
    'Metric': 'Resolved Markets',
    'Polymarket': len(pm_resolved),
    'Kalshi': len(kalshi_resolved)
})

# 3. Number of electoral markets
pm_electoral = pm[
    (pm['political_category'].str.startswith('1.', na=False)) |
    (pm['political_category'].str.contains('ELECTORAL', case=False, na=False))
]
kalshi_electoral = kalshi[
    (kalshi['political_category'].str.startswith('1.', na=False)) |
    (kalshi['political_category'].str.contains('ELECTORAL', case=False, na=False))
]
metrics.append({
    'Metric': 'Electoral Markets',
    'Polymarket': len(pm_electoral),
    'Kalshi': len(kalshi_electoral)
})

# 4. Number of non-electoral political markets
metrics.append({
    'Metric': 'Non-Electoral Markets',
    'Polymarket': len(pm) - len(pm_electoral),
    'Kalshi': len(kalshi) - len(kalshi_electoral)
})

# 5. Date range
pm_dates = pm['trading_close_time'].dropna()
kalshi_dates = kalshi['trading_close_time'].dropna()

pm_earliest = pm_dates.min().strftime('%Y-%m-%d') if len(pm_dates) > 0 else 'N/A'
pm_latest = pm_dates.max().strftime('%Y-%m-%d') if len(pm_dates) > 0 else 'N/A'
kalshi_earliest = kalshi_dates.min().strftime('%Y-%m-%d') if len(kalshi_dates) > 0 else 'N/A'
kalshi_latest = kalshi_dates.max().strftime('%Y-%m-%d') if len(kalshi_dates) > 0 else 'N/A'

metrics.append({
    'Metric': 'Earliest Market Close',
    'Polymarket': pm_earliest,
    'Kalshi': kalshi_earliest
})
metrics.append({
    'Metric': 'Latest Market Close',
    'Polymarket': pm_latest,
    'Kalshi': kalshi_latest
})

# 6. Number of unique election types
pm_election_types = pm_electoral['election_type'].dropna().nunique()
kalshi_election_types = kalshi_electoral['election_type'].dropna().nunique()
metrics.append({
    'Metric': 'Unique Election Types',
    'Polymarket': pm_election_types,
    'Kalshi': kalshi_election_types
})

# 7. Number of unique political categories
pm_categories = pm['political_category'].dropna().nunique()
kalshi_categories = kalshi['political_category'].dropna().nunique()
metrics.append({
    'Metric': 'Unique Political Categories',
    'Polymarket': pm_categories,
    'Kalshi': kalshi_categories
})

# 8. Total trading volume (if available)
volume_col = 'volume_usd' if 'volume_usd' in df.columns else 'volume'
if volume_col in df.columns:
    pm_volume = pm[volume_col].sum()
    kalshi_volume = kalshi[volume_col].sum()
    metrics.append({
        'Metric': 'Total Volume (USD)',
        'Polymarket': f"${pm_volume:,.0f}",
        'Kalshi': f"${kalshi_volume:,.0f}"
    })

# 9. Average volume per market
if volume_col in df.columns:
    pm_avg_volume = pm[volume_col].mean()
    kalshi_avg_volume = kalshi[volume_col].mean()
    metrics.append({
        'Metric': 'Avg Volume per Market',
        'Polymarket': f"${pm_avg_volume:,.0f}",
        'Kalshi': f"${kalshi_avg_volume:,.0f}"
    })

# Create DataFrame
metrics_df = pd.DataFrame(metrics)

# Print summary
print(f"\n{'Metric':<30} {'Polymarket':>20} {'Kalshi':>20}")
print("-" * 75)
for _, row in metrics_df.iterrows():
    pm_val = row['Polymarket']
    k_val = row['Kalshi']
    if isinstance(pm_val, (int, float)) and not isinstance(pm_val, bool):
        pm_str = f"{pm_val:,}" if isinstance(pm_val, int) else f"{pm_val:,.0f}"
        k_str = f"{k_val:,}" if isinstance(k_val, int) else f"{k_val:,.0f}"
    else:
        pm_str = str(pm_val)
        k_str = str(k_val)
    print(f"{row['Metric']:<30} {pm_str:>20} {k_str:>20}")

# Save CSV for website
csv_output = PAPER_DATA_DIR / "table_3_platform_comparison.csv"
metrics_df.to_csv(csv_output, index=False)
print(f"\nSaved CSV: {csv_output}")

# Generate LaTeX table
latex_output = TABLES_DIR / "table_3_platform_comparison.tex"

with open(latex_output, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Platform Comparison: Polymarket vs Kalshi}' + '\n')
    f.write(r'\label{tab:platform_comparison}' + '\n')
    f.write(r'\begin{tabular}{lrr}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'Metric & Polymarket & Kalshi \\' + '\n')
    f.write(r'\midrule' + '\n')

    for _, row in metrics_df.iterrows():
        metric = row['Metric'].replace('_', r'\_').replace('&', r'\&')
        pm_val = row['Polymarket']
        k_val = row['Kalshi']

        # Format values
        if isinstance(pm_val, (int, np.integer)):
            pm_str = f"{pm_val:,}"
            k_str = f"{k_val:,}"
        else:
            pm_str = str(pm_val).replace('$', r'\$')
            k_str = str(k_val).replace('$', r'\$')

        f.write(f"{metric} & {pm_str} & {k_str} \\\\\n")

    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\begin{tablenotes}' + '\n')
    f.write(r'\small' + '\n')
    f.write(r'\item Note: Statistics for all political prediction markets in our dataset.' + '\n')
    f.write(r'\end{tablenotes}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"Saved LaTeX: {latex_output}")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
