#!/usr/bin/env python3
"""
Table 1: Market Counts by Political Category

Creates a summary table showing how many markets exist in each of the 15
political categories, broken down by platform (Polymarket vs Kalshi).

Output:
- tables/table_1_aggregate.tex (LaTeX table)
- data/table_1_aggregate.csv (for website)
"""

import pandas as pd
import os

from config import BASE_DIR, DATA_DIR
from paper_config import load_master_csv, PAPER_TABLES_DIR, PAPER_DATA_DIR

# Paths
TABLES_DIR = PAPER_TABLES_DIR
TABLES_DIR.mkdir(exist_ok=True)

print("=" * 70)
print("TABLE 1: MARKET COUNTS BY POLITICAL CATEGORY")
print("=" * 70)

# Load data
print(f"\nLoading master CSV...")
df = load_master_csv()
print(f"  Loaded {len(df):,} total markets")

# Count by category and platform
print(f"\nCounting markets by category and platform...")
counts = df.groupby(['political_category', 'platform']).size().unstack(fill_value=0)

# Ensure both platforms exist as columns
for platform in ['Polymarket', 'Kalshi']:
    if platform not in counts.columns:
        counts[platform] = 0

# Calculate totals
counts['Total'] = counts['Polymarket'] + counts['Kalshi']

# Sort by total descending
counts = counts.sort_values('Total', ascending=False)

# Calculate percentages
total_pm = counts['Polymarket'].sum()
total_kalshi = counts['Kalshi'].sum()
total_all = counts['Total'].sum()

counts['PM_Pct'] = (counts['Polymarket'] / total_pm * 100).round(1)
counts['Kalshi_Pct'] = (counts['Kalshi'] / total_kalshi * 100).round(1)
counts['Total_Pct'] = (counts['Total'] / total_all * 100).round(1)

# Reset index to make category a column
counts = counts.reset_index()

# Print summary
print(f"\n{'Category':<35} {'PM':>8} {'Kalshi':>8} {'Total':>8}")
print("-" * 70)
for _, row in counts.iterrows():
    print(f"{row['political_category']:<35} {row['Polymarket']:>8,} {row['Kalshi']:>8,} {row['Total']:>8,}")
print("-" * 70)
print(f"{'TOTAL':<35} {total_pm:>8,} {total_kalshi:>8,} {total_all:>8,}")

# Save CSV for website
csv_output = PAPER_DATA_DIR / "table_1_aggregate.csv"
counts.to_csv(csv_output, index=False)
print(f"\nSaved CSV: {csv_output}")

# Generate LaTeX table
latex_output = TABLES_DIR / "table_1_aggregate.tex"

with open(latex_output, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Market Counts by Political Category}' + '\n')
    f.write(r'\label{tab:market_counts_category}' + '\n')
    f.write(r'\footnotesize' + '\n')
    f.write(r'\begin{tabular}{lrrrrrr}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'Category & PM & \% & Kalshi & \% & Total & \% \\' + '\n')
    f.write(r'\midrule' + '\n')

    for _, row in counts.iterrows():
        cat = row['political_category'].replace('_', r'\_').replace('&', r'\&')
        f.write(f"{cat} & {row['Polymarket']:,} & {row['PM_Pct']:.1f} & "
                f"{row['Kalshi']:,} & {row['Kalshi_Pct']:.1f} & "
                f"{row['Total']:,} & {row['Total_Pct']:.1f} \\\\\n")

    f.write(r'\midrule' + '\n')
    f.write(f"\\textbf{{Total}} & {total_pm:,} & 100.0 & "
            f"{total_kalshi:,} & 100.0 & {total_all:,} & 100.0 \\\\\n")
    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\begin{tablenotes}' + '\n')
    f.write(r'\small' + '\n')
    f.write(r'\item Note: Markets classified into 15 political categories using GPT-4o. PM = Polymarket.' + '\n')
    f.write(r'\end{tablenotes}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"Saved LaTeX: {latex_output}")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
