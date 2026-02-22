#!/usr/bin/env python3
"""
Table 1: Aggregate Statistics by Political Category (Combined Polymarket + Kalshi)
Creates a comprehensive table showing market statistics across all political categories
for both Polymarket and Kalshi platforms.

Rows: Political categories (ELECTORAL, MONETARY_POLICY, etc.)
Columns: Market counts, volumes (avg, median, total) for each platform and combined

Outputs:
- CSV file with the table data
- Formatted table visualization (PNG)
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import json

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)

# Color scheme (matching other scripts)
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

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
GRAPH_DIR = f"{BASE_DIR}/graphs/combined"
os.makedirs(GRAPH_DIR, exist_ok=True)

print("="*80)
print("TABLE 1: AGGREGATE STATISTICS BY POLITICAL CATEGORY")
print("TABLE: POLYMARKET + KALSHI COMBINED")
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
print("CALCULATING AGGREGATE STATISTICS")
print(f"{'='*80}\n")

# Function to clean category names
def clean_category_name(cat):
    """Remove number prefix and underscores, format properly"""
    # Remove number prefix like "1. "
    cat_clean = cat.split('. ', 1)[-1] if '. ' in cat else cat
    # Replace underscores with spaces
    cat_clean = cat_clean.replace('_', ' ')
    # Title case
    cat_clean = cat_clean.title()
    return cat_clean

# Get all unique categories from both platforms
all_categories = sorted(set(pm_unique['political_category'].unique()) | set(df_kalshi['political_category'].unique()))

results = []

for category in all_categories:
    # Combine both platforms
    pm_cat = pm_unique[pm_unique['political_category'] == category]
    k_cat = df_kalshi[df_kalshi['political_category'] == category]

    # Combine volumes from both platforms
    all_volumes = pd.concat([pm_cat['volume_usd'], k_cat['volume_usd']])

    total_markets = len(pm_cat) + len(k_cat)
    total_vol = all_volumes.sum()
    avg_vol = all_volumes.mean() if total_markets > 0 else 0
    med_vol = all_volumes.median() if total_markets > 0 else 0

    results.append({
        'Category': clean_category_name(category),
        'Total Markets': total_markets,
        'Average Volume ($K)': avg_vol / 1_000,
        'Median Volume ($K)': med_vol / 1_000,
        'Total Volume ($M)': total_vol / 1_000_000
    })

    print(f"{category:30s} | Markets: {total_markets:5,} | Avg: ${avg_vol/1e3:8.1f}K | Total: ${total_vol/1e6:8.1f}M")

# Create DataFrame
table = pd.DataFrame(results)

# Sort by total markets (descending)
table = table.sort_values('Total Markets', ascending=False).reset_index(drop=True)

# Display summary
print(f"\n{'='*80}")
print("TABLE SUMMARY")
print(f"{'='*80}\n")
print(table.to_string(index=False))

# ============================================================================
# VISUALIZATION: Create formatted table image
# ============================================================================

print(f"\n{'='*80}")
print("CREATING TABLE VISUALIZATION")
print(f"{'='*80}\n")

fig, ax = plt.subplots(figsize=(14, 10))
ax.axis('tight')
ax.axis('off')

# Prepare data for display (round numbers for cleaner display)
display_table = table.copy()
display_table['Average Volume ($K)'] = display_table['Average Volume ($K)'].apply(lambda x: f"{x:,.1f}")
display_table['Median Volume ($K)'] = display_table['Median Volume ($K)'].apply(lambda x: f"{x:,.1f}")
display_table['Total Volume ($M)'] = display_table['Total Volume ($M)'].apply(lambda x: f"{x:,.1f}")
display_table['Total Markets'] = display_table['Total Markets'].apply(lambda x: f"{x:,}")

# Create table
table_obj = ax.table(
    cellText=display_table.values,
    colLabels=display_table.columns,
    cellLoc='left',
    loc='center',
    bbox=[0, 0, 1, 1],
    edges='horizontal'
)

# Style the table - Clean academic look matching example paper
table_obj.auto_set_font_size(False)
table_obj.set_fontsize(10)
table_obj.scale(1, 2.2)

# Styling cells
for (i, j), cell in table_obj.get_celld().items():
    # Use minimal borders - only horizontal lines
    cell.set_edgecolor('#000000')

    if i == 0:  # Header row
        # Clean header with subtle color from graph palette
        cell.set_facecolor('white')
        cell.set_text_props(weight='bold', color=COLORS['dark'], fontsize=10)
        cell.set_linewidth(1.5)  # Thicker line under header
        # Left-align category header, right-align numeric headers
        if j == 0:
            cell.set_text_props(weight='bold', color=COLORS['dark'], fontsize=10, ha='left')
        else:
            cell.set_text_props(weight='bold', color=COLORS['dark'], fontsize=10, ha='right')
    else:
        # All data rows - white background, minimal lines
        cell.set_facecolor('white')
        cell.set_text_props(fontsize=10, color=COLORS['dark'])
        cell.set_linewidth(0.5)

        # Left-align category, right-align numeric columns
        if j == 0:  # Category column
            cell.set_text_props(fontsize=10, color=COLORS['dark'], ha='left')
        else:  # Numeric columns
            cell.set_text_props(fontsize=10, color=COLORS['dark'], ha='right')

plt.title('Table 1: Aggregate Statistics by Political Category',
          fontsize=14, weight='bold', pad=20, color='#000000', loc='left')

# Save visualization
output_file = f"{GRAPH_DIR}/table_1_aggregate_statistics.png"
plt.savefig(output_file, bbox_inches='tight', dpi=300, facecolor='white')
print(f"✓ Saved table visualization: {output_file}")

# ============================================================================
# GENERATE LATEX TABLE
# ============================================================================

print(f"\n{'='*80}")
print("GENERATING LATEX TABLE")
print(f"{'='*80}\n")

latex_output = f"{BASE_DIR}/tables/table_1_aggregate.tex"

with open(latex_output, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Aggregate Statistics by Political Category (Polymarket + Kalshi Combined)}' + '\n')
    f.write(r'\label{tab:aggregate_stats}' + '\n')
    f.write(r'\footnotesize' + '\n')
    f.write(r'\begin{tabular}{lrrrr}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'Category & Total Markets & Avg Volume (\$K) & Median Volume (\$K) & Total Volume (\$M) \\' + '\n')
    f.write(r'\midrule' + '\n')

    for idx, row in table.iterrows():
        f.write(f"{row['Category']} & {row['Total Markets']:,} & {row['Average Volume ($K)']:,.1f} & {row['Median Volume ($K)']:,.1f} & {row['Total Volume ($M)']:,.1f} \\\\\n")

    # Add totals row
    total_markets = table['Total Markets'].sum()
    total_volume = table['Total Volume ($M)'].sum()
    # Weighted average for average volume
    weighted_avg_volume = (table['Total Markets'] * table['Average Volume ($K)']).sum() / total_markets
    # Overall median - use median of all individual market volumes
    overall_median = table['Median Volume ($K)'].median()

    f.write(r'\midrule' + '\n')
    f.write(f"\\textbf{{Total}} & \\textbf{{{total_markets:,}}} & \\textbf{{{weighted_avg_volume:,.1f}}} & \\textbf{{{overall_median:,.1f}}} & \\textbf{{{total_volume:,.1f}}} \\\\\n")

    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"✓ Saved LaTeX table: {latex_output}")

print(f"\n{'='*80}")
print("✓ TABLE 1 GENERATION COMPLETE")
print(f"{'='*80}")
print(f"\nOutput:")
print(f"  - Table image: {GRAPH_DIR}/table_1_aggregate_statistics.png")
print(f"  - LaTeX table: {latex_output}")
