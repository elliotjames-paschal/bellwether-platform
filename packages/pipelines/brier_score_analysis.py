#!/usr/bin/env python3
"""
Brier Score Analysis Visualizations
Creates two Brier score analysis plots:
1. Time Series: Brier scores for 4 cohorts (7d, 14d, 30d, 60d) from 60 days to 0
2. Category Breakdown: 7-day cohort Brier scores by political category (1 day before)

Uses the same color scheme as Polymarket/Kalshi pipeline for consistency.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for LaTeX
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

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

# Cohort colors (4 distinct colors from palette)
COHORT_COLORS = {
    '7d': '#2C3E50',   # Darkest
    '14d': '#34495E',
    '30d': '#7F8C8D',
    '60d': '#95A5A6'   # Lightest
}

# Paths
from config import BASE_DIR, DATA_DIR, get_latest_file
from paper_config import load_master_csv, load_prediction_accuracy, PAPER_GRAPHS_DIR, PAPER_TABLES_DIR, PAPER_DATA_DIR

OVERALL_FILE = f"{PAPER_DATA_DIR}/combined_brier_overall_cohorts.csv"
CATEGORIES_FILE = f"{PAPER_DATA_DIR}/combined_brier_categories_cohorts.csv"

GRAPH_DIR = str(PAPER_GRAPHS_DIR)
TABLES_DIR = str(PAPER_TABLES_DIR)
os.makedirs(GRAPH_DIR, exist_ok=True)
os.makedirs(TABLES_DIR, exist_ok=True)

# Platform-specific category cohort files
PM_CATEGORIES_FILE = f"{PAPER_DATA_DIR}/polymarket_brier_categories_cohorts.csv"
KALSHI_CATEGORIES_FILE = f"{PAPER_DATA_DIR}/kalshi_brier_categories_cohorts.csv"

print("="*80)
print("BRIER SCORE ANALYSIS")
print("="*80)

# ============================================================================
# Load Data
# ============================================================================

print(f"\nLoading data...")
print(f"  Overall cohorts: {OVERALL_FILE}")
print(f"  Category cohorts: {CATEGORIES_FILE}")

df_overall = pd.read_csv(OVERALL_FILE)
df_categories = pd.read_csv(CATEGORIES_FILE)

print(f"✓ Loaded overall cohorts: {len(df_overall)} rows")
print(f"✓ Loaded category cohorts: {len(df_categories)} rows")

# Load prediction accuracy data to filter for elections
print(f"\nLoading prediction accuracy data for elections filtering...")

df_pm_pred = load_prediction_accuracy("polymarket")
df_kalshi_pred = load_prediction_accuracy("kalshi")

print(f"✓ Loaded {len(df_pm_pred):,} Polymarket prediction rows")
print(f"✓ Loaded {len(df_kalshi_pred):,} Kalshi prediction rows")

# Load market metadata from master CSV
print(f"\nLoading market metadata...")

df_master = load_master_csv()

# Split by platform and extract metadata
pm_meta = df_master[df_master['platform'] == 'Polymarket'][['market_id', 'election_type']].copy()
kalshi_meta = df_master[df_master['platform'] == 'Kalshi'][['market_id', 'election_type']].copy()

# Ensure market_id types match for merging
pm_meta['market_id'] = pm_meta['market_id'].astype(str)
kalshi_meta['market_id'] = kalshi_meta['market_id'].astype(str)

print(f"✓ Loaded {len(pm_meta):,} Polymarket market metadata")
print(f"✓ Loaded {len(kalshi_meta):,} Kalshi market metadata")

# Join prediction data with metadata
df_pm_pred['market_id'] = df_pm_pred['market_id'].astype(str)
df_pm = df_pm_pred.merge(pm_meta, on='market_id', how='inner', suffixes=('', '_market'))
if 'ticker' in df_kalshi_pred.columns:
    df_kalshi_pred['market_id'] = df_kalshi_pred['ticker']
df_kalshi_pred['market_id'] = df_kalshi_pred['market_id'].astype(str)
df_kalshi = df_kalshi_pred.merge(kalshi_meta, on='market_id', how='inner', suffixes=('', '_market'))

print(f"✓ Joined Polymarket: {len(df_pm):,} rows with election_type")
print(f"✓ Joined Kalshi: {len(df_kalshi):,} rows with election_type")

# If election_type has suffix, rename it
if 'election_type_market' in df_pm.columns and 'election_type' not in df_pm.columns:
    df_pm['election_type'] = df_pm['election_type_market']
if 'election_type_market' in df_kalshi.columns and 'election_type' not in df_kalshi.columns:
    df_kalshi['election_type'] = df_kalshi['election_type_market']

# Filter to elections only (election_type is not null/NA)
df_pm_elections = df_pm[
    (df_pm['election_type'].notna()) &
    (df_pm['election_type'] != 'NA')
].copy()
df_kalshi_elections = df_kalshi[
    (df_kalshi['election_type'].notna()) &
    (df_kalshi['election_type'] != 'NA')
].copy()

print(f"\n✓ Filtered to elections only:")
print(f"  Polymarket: {len(df_pm_elections):,} election market predictions")
print(f"  Kalshi: {len(df_kalshi_elections):,} election market predictions")
print(f"  Total: {len(df_pm_elections) + len(df_kalshi_elections):,} election predictions")


# ============================================================================
# GRAPH 1: Brier Score Time Series (4 Cohorts)
# ============================================================================

print(f"\n{'='*80}")
print("GRAPH 1: Brier Score Time Series by Cohort")
print(f"{'='*80}")

# Expected day columns in order from far to near
day_columns = ['60d', '30d', '20d', '14d', '12d', '10d', '8d', '7d', '6d', '5d', '4d', '3d', '2d', '1d']

# Filter to cohorts we want
cohorts = ['7d', '14d', '30d', '60d']

fig, ax = plt.subplots(figsize=(14, 8))

# Plot each cohort
for cohort in cohorts:
    cohort_data = df_overall[df_overall['Cohort'] == cohort]

    if len(cohort_data) == 0:
        print(f"⚠️  No data for cohort: {cohort}")
        continue

    # Extract the Brier scores for available day columns
    days = []
    scores = []

    for day_col in day_columns:
        if day_col in cohort_data.columns:
            value = cohort_data[day_col].iloc[0]
            if pd.notna(value):  # Only include non-null values
                # Convert day column to numeric (e.g., '60d' -> 60)
                day_num = int(day_col.replace('d', ''))
                days.append(day_num)
                scores.append(value)

    if len(days) > 0:
        # Plot line (reverse days so X-axis goes from 60 to 0)
        ax.plot(days[::-1], scores[::-1],
                marker='o',
                markersize=6,
                linewidth=2.5,
                color=COHORT_COLORS[cohort],
                label=f'{cohort} cohort (n={cohort_data["N"].iloc[0]:,})',
                alpha=0.8)

        print(f"✓ Plotted {cohort}: {len(days)} data points, final Brier = {scores[-1]:.4f}")

# Formatting
ax.set_xlabel('Days Before Resolution', fontsize=14, fontweight='bold')
ax.set_ylabel('Brier Score (Mean Squared Error)', fontsize=14, fontweight='bold')
ax.set_title('Brier Score Evolution by Cohort\n(Lower is Better - Perfect Calibration = 0)',
             fontsize=16, fontweight='bold', pad=20)

# Reverse x-axis so it goes from 60 to 0
ax.invert_xaxis()

# Add grid
ax.grid(True, alpha=0.3, zorder=0)

# Legend
ax.legend(fontsize=12, loc='lower left', framealpha=0.9)

plt.tight_layout()
output_file_1 = f"{GRAPH_DIR}/brier_scores_timeseries.png"
plt.savefig(output_file_1, dpi=300, bbox_inches='tight')
print(f"\n✓ Saved: {output_file_1}")
plt.close()


# ============================================================================
# GRAPH 2: Brier Score Time Series (4 Cohorts) - ELECTIONS ONLY
# ============================================================================

print(f"\n{'='*80}")
print("GRAPH 2: Brier Score Time Series by Cohort - ELECTIONS ONLY")
print(f"{'='*80}")

# Get unique election market IDs
election_market_ids = set(df_pm_elections['market_id'].unique()) | set(df_kalshi_elections['market_id'].unique())
print(f"✓ Found {len(election_market_ids):,} unique election markets")

# For elections-only graph, we need to use the category data and filter for ELECTORAL category
# The categories file has Brier scores by category, and ELECTORAL is the election category
df_electoral = df_categories[df_categories['Category'] == '1. ELECTORAL'].copy()

if len(df_electoral) == 0:
    print("⚠️  No ELECTORAL category found in categories data")
else:
    print(f"✓ Found ELECTORAL category data with {len(df_electoral)} cohort(s)")

    # Plot the elections-only time series using ELECTORAL category data
    fig, ax = plt.subplots(figsize=(14, 8))

    for cohort in cohorts:
        cohort_data = df_electoral[df_electoral['Cohort'] == cohort]

        if len(cohort_data) == 0:
            print(f"⚠️  No data for cohort: {cohort}")
            continue

        # Extract the Brier scores for available day columns
        days = []
        scores = []

        for day_col in day_columns:
            if day_col in cohort_data.columns:
                value = cohort_data[day_col].iloc[0]
                if pd.notna(value):  # Only include non-null values
                    # Convert day column to numeric (e.g., '60d' -> 60)
                    day_num = int(day_col.replace('d', ''))
                    days.append(day_num)
                    scores.append(value)

        if len(days) > 0:
            # Plot line (reverse days so X-axis goes from 60 to 0)
            ax.plot(days[::-1], scores[::-1],
                    marker='o',
                    markersize=6,
                    linewidth=2.5,
                    color=COHORT_COLORS[cohort],
                    label=f'{cohort} cohort (n={cohort_data["N"].iloc[0]:,})',
                    alpha=0.8)

            print(f"✓ Plotted {cohort} elections: {len(days)} data points, final Brier = {scores[-1]:.4f}")

# Formatting
ax.set_xlabel('Days Before Resolution', fontsize=14, fontweight='bold')
ax.set_ylabel('Brier Score (Mean Squared Error)', fontsize=14, fontweight='bold')
ax.set_title('Brier Score Evolution by Cohort - Elections Only\n(Lower is Better - Perfect Calibration = 0)',
             fontsize=16, fontweight='bold', pad=20)

# Reverse x-axis so it goes from 60 to 0
ax.invert_xaxis()

# Add grid
ax.grid(True, alpha=0.3, zorder=0)

# Legend
ax.legend(fontsize=12, loc='lower left', framealpha=0.9)

plt.tight_layout()
output_file_2 = f"{GRAPH_DIR}/brier_scores_timeseries_elections_only.png"
plt.savefig(output_file_2, dpi=300, bbox_inches='tight')
print(f"\n✓ Saved: {output_file_2}")
plt.close()


# ============================================================================
# TABLE: Brier Scores by Category for Each Platform (LaTeX)
# ============================================================================

print(f"\n{'='*80}")
print("TABLES: Brier Scores by Political Category (Separate by Platform)")
print(f"{'='*80}")

# Load platform-specific category data
print(f"\nLoading platform-specific category data...")
print(f"  Polymarket: {PM_CATEGORIES_FILE}")
print(f"  Kalshi: {KALSHI_CATEGORIES_FILE}")

df_pm_cat = pd.read_csv(PM_CATEGORIES_FILE)
df_kalshi_cat = pd.read_csv(KALSHI_CATEGORIES_FILE)

print(f"✓ Loaded Polymarket categories: {len(df_pm_cat)} rows")
print(f"✓ Loaded Kalshi categories: {len(df_kalshi_cat)} rows")

def generate_category_latex_table(df_platform, platform_name, output_file):
    """Generate a LaTeX table for Brier scores by category for a single platform."""

    # Filter to 7-day cohort
    df_7d = df_platform[df_platform['Cohort'] == '7d'].copy()

    if len(df_7d) == 0:
        print(f"⚠️  No 7-day cohort data for {platform_name}")
        return

    # Extract 1-day before Brier scores
    if '1d' not in df_7d.columns:
        print(f"⚠️  Column '1d' not found for {platform_name}")
        return

    df_7d['brier_1d'] = df_7d['1d']
    df_7d = df_7d[df_7d['brier_1d'].notna()].copy()

    # Sort by category name for consistent ordering
    df_7d = df_7d.sort_values('Category')

    print(f"\n{platform_name} Category Brier Scores (1 day before):")
    for idx, row in df_7d.iterrows():
        print(f"  • {row['Category']}: {row['brier_1d']:.4f} (n={row['N']:,})")

    # Generate LaTeX table
    label_suffix = platform_name.lower()
    latex = f"""\\begin{{table}}[htbp]
\\centering
\\caption{{{platform_name}: Brier Scores by Political Category (7-Day Cohort, 1 Day Before Resolution)}}
\\label{{tab:brier_category_{label_suffix}}}
\\begin{{tabular}}{{lcc}}
\\toprule
Category & N Markets & Brier Score \\\\
\\midrule
"""

    for idx, row in df_7d.iterrows():
        # Clean up category name (remove number prefix if present)
        category = row['Category']
        if '. ' in category:
            category = category.split('. ', 1)[1]
        latex += f"{category} & {int(row['N']):,} & {row['brier_1d']:.4f} \\\\\n"

    # Add total/average row
    total_n = df_7d['N'].sum()
    avg_brier = df_7d['brier_1d'].mean()
    latex += f"""\\midrule
\\textit{{Average}} & {int(total_n):,} & {avg_brier:.4f} \\\\
\\bottomrule
\\end{{tabular}}
\\begin{{tablenotes}}
\\small
\\item Notes: Brier score is mean squared prediction error (lower is better). N is number of unique markets in each category. Based on predictions 1 day before market resolution.
\\end{{tablenotes}}
\\end{{table}}
"""

    # Save to file
    with open(output_file, 'w') as f:
        f.write(latex)

    print(f"✓ Saved LaTeX table: {output_file}")

# Generate tables for each platform
pm_table_file = f"{TABLES_DIR}/brier_by_category_polymarket.tex"
kalshi_table_file = f"{TABLES_DIR}/brier_by_category_kalshi.tex"

generate_category_latex_table(df_pm_cat, "Polymarket", pm_table_file)
generate_category_latex_table(df_kalshi_cat, "Kalshi", kalshi_table_file)

# Also keep the combined categories for reference (but not as a graph)
df_7d = df_categories[df_categories['Cohort'] == '7d'].copy()
if '1d' in df_7d.columns:
    df_7d['brier_1d'] = df_7d['1d']
    df_7d = df_7d[df_7d['brier_1d'].notna()].copy()
    print(f"\nCombined category stats (for reference):")
    for idx, row in df_7d.sort_values('Category').iterrows():
        print(f"  • {row['Category']}: {row['brier_1d']:.4f} (n={row['N']:,})")


# ============================================================================
# BAR GRAPH: Brier Scores by Category - Platform Comparison (All Markets, 1 Day Out)
# ============================================================================

print(f"\n{'='*80}")
print("BAR GRAPH: Brier Scores by Political Category (All Markets, 1 Day Before)")
print(f"{'='*80}")

# Platform colors - grayscale to match paper style
PM_COLOR = '#2C3E50'  # Dark gray for Polymarket
KALSHI_COLOR = '#95A5A6'  # Light gray for Kalshi

# Load raw prediction data and filter to 1 day before resolution
print("Loading raw prediction data for 1-day analysis...")
df_pm_raw = load_prediction_accuracy("polymarket")
df_kalshi_raw = load_prediction_accuracy("kalshi")

# Filter to 1 day before resolution
df_pm_1d = df_pm_raw[df_pm_raw['days_before_event'] == 1].copy()
df_kalshi_1d = df_kalshi_raw[df_kalshi_raw['days_before_event'] == 1].copy()

print(f"  Polymarket: {len(df_pm_1d):,} markets with 1-day predictions")
print(f"  Kalshi: {len(df_kalshi_1d):,} markets with 1-day predictions")

# Calculate Brier scores by category for each platform
pm_by_cat = df_pm_1d.groupby('category').agg({
    'brier_score': 'mean',
    'market_id': 'nunique'
}).rename(columns={'brier_score': 'brier', 'market_id': 'n'}).reset_index()

kalshi_by_cat = df_kalshi_1d.groupby('category').agg({
    'brier_score': 'mean',
    'ticker': 'nunique'
}).rename(columns={'brier_score': 'brier', 'ticker': 'n'}).reset_index()

# Get all categories present in either platform
all_categories = sorted(set(pm_by_cat['category'].dropna().unique()) | set(kalshi_by_cat['category'].dropna().unique()))

# Build comparison data
comparison_data = []
for cat in all_categories:
    if pd.isna(cat):
        continue

    pm_row = pm_by_cat[pm_by_cat['category'] == cat]
    kalshi_row = kalshi_by_cat[kalshi_by_cat['category'] == cat]

    pm_brier = pm_row['brier'].iloc[0] if len(pm_row) > 0 else np.nan
    pm_n = int(pm_row['n'].iloc[0]) if len(pm_row) > 0 else 0
    kalshi_brier = kalshi_row['brier'].iloc[0] if len(kalshi_row) > 0 else np.nan
    kalshi_n = int(kalshi_row['n'].iloc[0]) if len(kalshi_row) > 0 else 0

    # Clean category name
    cat_clean = cat.split('. ', 1)[1] if '. ' in cat else cat

    comparison_data.append({
        'Category': cat_clean,
        'PM Brier': pm_brier,
        'PM N': pm_n,
        'Kalshi Brier': kalshi_brier,
        'Kalshi N': kalshi_n,
        'Total N': pm_n + kalshi_n
    })

df_comparison = pd.DataFrame(comparison_data)

# Sort by total N (largest at top when plotted)
df_comparison = df_comparison.sort_values('Total N', ascending=True)

# Create the bar chart
fig, ax = plt.subplots(figsize=(14, 10))

y_pos = np.arange(len(df_comparison))
bar_height = 0.35

# Create bars
pm_bars = ax.barh(y_pos + bar_height/2, df_comparison['PM Brier'].fillna(0),
                  bar_height, label='Polymarket', color=PM_COLOR,
                  edgecolor='#1a1a1a', linewidth=0.5, alpha=0.85)
kalshi_bars = ax.barh(y_pos - bar_height/2, df_comparison['Kalshi Brier'].fillna(0),
                      bar_height, label='Kalshi', color=KALSHI_COLOR,
                      edgecolor='#1a1a1a', linewidth=0.5, alpha=0.85)

# Add value labels
for i, row in df_comparison.reset_index().iterrows():
    # PM label
    if pd.notna(row['PM Brier']) and row['PM Brier'] > 0:
        ax.text(row['PM Brier'] + 0.003, y_pos[i] + bar_height/2,
               f"{row['PM Brier']:.3f} (n={row['PM N']:,})", va='center', fontsize=9, color='#1a1a1a')
    # Kalshi label
    if pd.notna(row['Kalshi Brier']) and row['Kalshi Brier'] > 0:
        ax.text(row['Kalshi Brier'] + 0.003, y_pos[i] - bar_height/2,
               f"{row['Kalshi Brier']:.3f} (n={row['Kalshi N']:,})", va='center', fontsize=9, color='#1a1a1a')

# Labels and formatting
ax.set_yticks(y_pos)
ax.set_yticklabels(df_comparison['Category'], fontsize=11)
ax.set_xlabel('Brier Score (Lower is Better)', fontsize=13, fontweight='bold')
ax.set_title('Brier Score Comparison by Political Category\n(All Markets, 1 Day Before Resolution)',
             fontsize=15, fontweight='bold', pad=20)

# Legend
ax.legend(loc='lower right', fontsize=11, framealpha=0.9)

# Grid
ax.grid(True, axis='x', alpha=0.3, zorder=0)
ax.set_axisbelow(True)

# Adjust x-axis
max_brier = max(df_comparison['PM Brier'].max(), df_comparison['Kalshi Brier'].max())
ax.set_xlim(0, max_brier * 1.5)

plt.tight_layout()
category_graph_file = f"{GRAPH_DIR}/brier_by_category_comparison.png"
plt.savefig(category_graph_file, dpi=300, bbox_inches='tight', facecolor='white')
print(f"✓ Saved bar chart: {category_graph_file}")
plt.close()


# ============================================================================
# Summary
# ============================================================================

print(f"\n{'='*80}")
print("SUMMARY")
print(f"{'='*80}")
print(f"Cohorts analyzed: {', '.join(cohorts)}")
print(f"Categories analyzed: {len(df_7d)} for 7-day cohort")
print(f"\nGraphs saved to: {GRAPH_DIR}")
print(f"  1. brier_scores_timeseries.png (all markets)")
print(f"  2. brier_scores_timeseries_elections_only.png (elections only)")
print(f"  3. brier_by_category_comparison.png (platform comparison by category)")
print(f"\nTables saved to: {TABLES_DIR}")
print(f"  1. brier_by_category_polymarket.tex")
print(f"  2. brier_by_category_kalshi.tex")
print(f"{'='*80}")
