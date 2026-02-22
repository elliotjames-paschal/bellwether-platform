#!/usr/bin/env python3
"""
Table 2: Election Types Breakdown

For ELECTORAL markets only, shows how many markets exist for each election type
(Presidential, Senate, House, Gubernatorial, etc.) by platform.

Output:
- tables/table_2_election_types.tex (LaTeX table)
- data/table_2_election_types.csv (for website)
"""

import pandas as pd
import os

from config import BASE_DIR, DATA_DIR
from paper_config import load_master_csv, PAPER_TABLES_DIR, PAPER_DATA_DIR

# Paths
TABLES_DIR = PAPER_TABLES_DIR
TABLES_DIR.mkdir(exist_ok=True)

print("=" * 70)
print("TABLE 2: ELECTION TYPES BREAKDOWN")
print("=" * 70)

# Load data
print(f"\nLoading master CSV...")
df = load_master_csv()
print(f"  Loaded {len(df):,} total markets")

# Filter to electoral markets
electoral = df[
    (df['political_category'].str.startswith('1.', na=False)) |
    (df['political_category'].str.contains('ELECTORAL', case=False, na=False))
].copy()
print(f"  Electoral markets: {len(electoral):,}")

# Filter to markets with election_type
electoral = electoral[electoral['election_type'].notna() & (electoral['election_type'] != 'NA')]
print(f"  With election_type: {len(electoral):,}")

# Count by election_type and platform
counts = electoral.groupby(['election_type', 'platform']).size().unstack(fill_value=0)

# Ensure both platforms exist as columns
for platform in ['Polymarket', 'Kalshi']:
    if platform not in counts.columns:
        counts[platform] = 0

# Calculate totals
counts['Total'] = counts['Polymarket'] + counts['Kalshi']

# Rename OTHER_NEEDS_REVIEW to "Other" for display
counts.index = counts.index.map(lambda x: 'Other' if x == 'OTHER_NEEDS_REVIEW' else x)

# Sort by total descending, but force "Other" to the bottom
counts = counts.sort_values('Total', ascending=False)
if 'Other' in counts.index:
    other_row = counts.loc[['Other']]
    counts = pd.concat([counts.drop('Other'), other_row])

# Reset index
counts = counts.reset_index()

# Calculate totals
total_pm = counts['Polymarket'].sum()
total_kalshi = counts['Kalshi'].sum()
total_all = counts['Total'].sum()

# Print summary
print(f"\n{'Election Type':<25} {'PM':>10} {'Kalshi':>10} {'Total':>10}")
print("-" * 60)
for _, row in counts.iterrows():
    print(f"{row['election_type']:<25} {row['Polymarket']:>10,} {row['Kalshi']:>10,} {row['Total']:>10,}")
print("-" * 60)
print(f"{'TOTAL':<25} {total_pm:>10,} {total_kalshi:>10,} {total_all:>10,}")

# Save CSV for website
csv_output = PAPER_DATA_DIR / "table_2_election_types.csv"
counts.to_csv(csv_output, index=False)
print(f"\nSaved CSV: {csv_output}")

# Generate LaTeX table
latex_output = TABLES_DIR / "table_2_election_types.tex"

with open(latex_output, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Electoral Markets by Election Type}' + '\n')
    f.write(r'\label{tab:election_types}' + '\n')
    f.write(r'\footnotesize' + '\n')
    f.write(r'\begin{tabular}{lrrr}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'Election Type & Polymarket & Kalshi & Total \\' + '\n')
    f.write(r'\midrule' + '\n')

    for _, row in counts.iterrows():
        election_type = str(row['election_type']).replace('_', r'\_').replace('&', r'\&')
        f.write(f"{election_type} & {row['Polymarket']:,} & {row['Kalshi']:,} & {row['Total']:,} \\\\\n")

    f.write(r'\midrule' + '\n')
    f.write(f"\\textbf{{Total}} & {total_pm:,} & {total_kalshi:,} & {total_all:,} \\\\\n")
    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\begin{tablenotes}' + '\n')
    f.write(r'\small' + '\n')
    f.write(r'\item Note: Election types for markets in the ELECTORAL political category. Includes US and international elections. ``Other'' includes markets that could not be cleanly categorized into a specific election type.' + '\n')
    f.write(r'\end{tablenotes}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"Saved LaTeX: {latex_output}")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
