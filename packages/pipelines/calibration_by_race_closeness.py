#!/usr/bin/env python3
"""
Calibration by Race Closeness

Analyzes whether prediction markets are more accurate for landslide elections
vs close races. Uses actual vote share data to measure how close each race was.

Output:
- tables/calibration_by_margin.tex
- graphs/combined/brier_vs_margin.png
- data/calibration_by_race_margin.csv
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

from config import BASE_DIR, DATA_DIR, get_latest_file
from paper_config import load_master_csv, load_prediction_accuracy, PAPER_GRAPHS_DIR, PAPER_TABLES_DIR, PAPER_DATA_DIR

# Paths
TABLES_DIR = PAPER_TABLES_DIR
GRAPHS_DIR = PAPER_GRAPHS_DIR
TABLES_DIR.mkdir(exist_ok=True)
GRAPHS_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 70)
print("CALIBRATION BY RACE CLOSENESS")
print("=" * 70)

# Load master CSV
print(f"\nLoading master CSV...")
df_master = load_master_csv()
print(f"  Loaded {len(df_master):,} markets")

# Filter to markets with vote share data
df_votes = df_master[
    df_master['democrat_vote_share'].notna() &
    df_master['republican_vote_share'].notna()
].copy()
print(f"  Markets with vote share data: {len(df_votes):,}")

# Calculate margin
df_votes['margin'] = abs(df_votes['democrat_vote_share'] - df_votes['republican_vote_share'])

# Create margin buckets
def get_margin_bucket(margin):
    if margin < 5:
        return '< 5%'
    elif margin < 10:
        return '5-10%'
    elif margin < 20:
        return '10-20%'
    else:
        return '> 20%'

df_votes['margin_bucket'] = df_votes['margin'].apply(get_margin_bucket)

# Create lookup by market_id
pm_votes = df_votes[df_votes['platform'] == 'Polymarket'][['market_id', 'margin', 'margin_bucket']].copy()
pm_votes['market_id'] = pm_votes['market_id'].astype(str)

kalshi_votes = df_votes[df_votes['platform'] == 'Kalshi'][['market_id', 'margin', 'margin_bucket']].copy()
kalshi_votes['market_id'] = kalshi_votes['market_id'].astype(str)

# Load prediction data
all_data = []

pm_pred = load_prediction_accuracy("polymarket")
if pm_pred is not None:
    print(f"\nLoading Polymarket predictions...")

    pm_pred = pm_pred[pm_pred['days_before_event'] == 1].copy()
    pm_pred['market_id'] = pm_pred['market_id'].astype(str)
    pm_pred = pm_pred.merge(pm_votes, on='market_id', how='inner')
    pm_pred['platform'] = 'Polymarket'
    all_data.append(pm_pred)
    print(f"  Matched {len(pm_pred):,} markets with vote share")

kalshi_pred = load_prediction_accuracy("kalshi")
if kalshi_pred is not None:
    print(f"\nLoading Kalshi predictions...")

    kalshi_pred = kalshi_pred[kalshi_pred['days_before_event'] == 1].copy()
    id_col = 'market_id' if 'market_id' in kalshi_pred.columns else 'ticker'
    kalshi_pred['market_id'] = kalshi_pred[id_col].astype(str)
    kalshi_pred = kalshi_pred.merge(kalshi_votes, on='market_id', how='inner')
    kalshi_pred['platform'] = 'Kalshi'
    all_data.append(kalshi_pred)
    print(f"  Matched {len(kalshi_pred):,} markets with vote share")

if not all_data:
    print("\nNo prediction data found!")
    exit(1)

# Combine all data
df = pd.concat(all_data, ignore_index=True)
print(f"\nTotal markets with margin data: {len(df):,}")

# Calculate Brier score if not present
if 'brier_score' not in df.columns:
    df['brier_score'] = (df['prediction_price'] - df['actual_outcome']) ** 2

# Aggregate by margin bucket
bucket_order = ['< 5%', '5-10%', '10-20%', '> 20%']
results = []

for bucket in bucket_order:
    bucket_data = df[df['margin_bucket'] == bucket]

    pm_data = bucket_data[bucket_data['platform'] == 'Polymarket']
    kalshi_data = bucket_data[bucket_data['platform'] == 'Kalshi']

    results.append({
        'Margin Bucket': bucket,
        'PM N': len(pm_data),
        'PM Brier': pm_data['brier_score'].mean() if len(pm_data) > 0 else np.nan,
        'Kalshi N': len(kalshi_data),
        'Kalshi Brier': kalshi_data['brier_score'].mean() if len(kalshi_data) > 0 else np.nan,
        'Total N': len(bucket_data),
        'Combined Brier': bucket_data['brier_score'].mean() if len(bucket_data) > 0 else np.nan
    })

results_df = pd.DataFrame(results)

# Add overall row
overall = {
    'Margin Bucket': 'Overall',
    'PM N': len(df[df['platform'] == 'Polymarket']),
    'PM Brier': df[df['platform'] == 'Polymarket']['brier_score'].mean(),
    'Kalshi N': len(df[df['platform'] == 'Kalshi']),
    'Kalshi Brier': df[df['platform'] == 'Kalshi']['brier_score'].mean(),
    'Total N': len(df),
    'Combined Brier': df['brier_score'].mean()
}
results_df = pd.concat([results_df, pd.DataFrame([overall])], ignore_index=True)

# Print summary
print(f"\n{'Margin':<12} {'PM N':>8} {'PM Brier':>10} {'Kalshi N':>10} {'K Brier':>10} {'Total':>8} {'Combined':>10}")
print("-" * 80)
for _, row in results_df.iterrows():
    pm_brier = f"{row['PM Brier']:.4f}" if pd.notna(row['PM Brier']) else 'N/A'
    k_brier = f"{row['Kalshi Brier']:.4f}" if pd.notna(row['Kalshi Brier']) else 'N/A'
    comb_brier = f"{row['Combined Brier']:.4f}" if pd.notna(row['Combined Brier']) else 'N/A'
    print(f"{row['Margin Bucket']:<12} {row['PM N']:>8,} {pm_brier:>10} {row['Kalshi N']:>10,} {k_brier:>10} {row['Total N']:>8,} {comb_brier:>10}")

# Save CSV
csv_output = PAPER_DATA_DIR / "calibration_by_race_margin.csv"
results_df.to_csv(csv_output, index=False)
print(f"\nSaved CSV: {csv_output}")

# Generate LaTeX table
latex_output = TABLES_DIR / "calibration_by_margin.tex"

with open(latex_output, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Brier Scores by Race Margin (1 Day Before Resolution)}' + '\n')
    f.write(r'\label{tab:brier_by_margin}' + '\n')
    f.write(r'\begin{tabular}{lrrrrrr}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'Margin & PM N & PM Brier & Kalshi N & Kalshi Brier & Total N & Combined \\' + '\n')
    f.write(r'\midrule' + '\n')

    for _, row in results_df.iterrows():
        if row['Margin Bucket'] == 'Overall':
            f.write(r'\midrule' + '\n')
            bucket = r'\textbf{Overall}'
        else:
            bucket = row['Margin Bucket'].replace('%', r'\%')

        pm_brier = f"{row['PM Brier']:.4f}" if pd.notna(row['PM Brier']) else '-'
        k_brier = f"{row['Kalshi Brier']:.4f}" if pd.notna(row['Kalshi Brier']) else '-'
        comb_brier = f"{row['Combined Brier']:.4f}" if pd.notna(row['Combined Brier']) else '-'

        f.write(f"{bucket} & {row['PM N']:,} & {pm_brier} & {row['Kalshi N']:,} & {k_brier} & {row['Total N']:,} & {comb_brier} \\\\\n")

    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\begin{tablenotes}' + '\n')
    f.write(r'\small' + '\n')
    f.write(r'\item Note: Margin = |Democrat vote share - Republican vote share|. Lower Brier scores indicate better calibration. Includes only markets with available vote share data.' + '\n')
    f.write(r'\end{tablenotes}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"Saved LaTeX: {latex_output}")

# Create figure - SCATTER PLOT ONLY per user request
fig, ax = plt.subplots(figsize=(10, 8))

# Scatterplot of Brier vs margin
scatter = ax.scatter(df['margin'], df['brier_score'],
                      c=['#2C3E50' if p == 'Polymarket' else '#95A5A6' for p in df['platform']],
                      alpha=0.5, s=30)

# Add legend for platforms
from matplotlib.lines import Line2D
legend_elements = [Line2D([0], [0], marker='o', color='w', markerfacecolor='#2C3E50', markersize=10, label='Polymarket'),
                   Line2D([0], [0], marker='o', color='w', markerfacecolor='#95A5A6', markersize=10, label='Kalshi')]
ax.legend(handles=legend_elements, loc='upper right')

ax.set_xlabel('Race Margin (%)', fontsize=12, fontweight='bold')
ax.set_ylabel('Brier Score', fontsize=12, fontweight='bold')
ax.set_title('Prediction Accuracy vs Election Margin', fontsize=14, fontweight='bold')
ax.grid(True, alpha=0.3)

plt.tight_layout()

output_path = GRAPHS_DIR / "brier_vs_margin.png"
plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
plt.close()
print(f"Saved figure: {output_path}")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
