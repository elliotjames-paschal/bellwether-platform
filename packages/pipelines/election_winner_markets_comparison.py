#!/usr/bin/env python3
"""
Election Winner Markets Comparison: Polymarket vs Kalshi

Reads GPT-selected winner markets from pipeline_select_election_winners.py
and generates:
- Panel A: All election winner markets per platform
- Panel B: Head-to-head comparison on elections available on both platforms
- LaTeX table, CSV exports, scatterplot

This is Step 3 of the combined pipeline:
  Step 1+2+3 selection: pipeline_select_election_winners.py
  Step 3 analysis:      THIS SCRIPT (reads selections, computes metrics)

Uses election_eve_price from master CSV (UTC midnight on election day,
populated by pipeline_election_eve_prices.py) as the prediction price.

Metrics: N Elections, Brier Score, Accuracy, Average Volume
Statistical tests: Paired t-test, Win rate, Correlation, Mean difference
"""

import pandas as pd
import numpy as np
import os
import sys
import json
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend

# Paths
from config import DATA_DIR
from paper_config import load_master_csv, PAPER_GRAPHS_DIR, PAPER_TABLES_DIR, PAPER_DATA_DIR

TABLES_DIR = str(PAPER_TABLES_DIR)
GRAPHS_DIR = str(PAPER_GRAPHS_DIR)
os.makedirs(TABLES_DIR, exist_ok=True)
os.makedirs(GRAPHS_DIR, exist_ok=True)

# Input files
SELECTIONS_FILE = f"{DATA_DIR}/election_winner_selections.json"

def normalize_party(party):
    """Normalize party string to 'Republican' or 'Democrat'."""
    if pd.isna(party) or party is None:
        return None
    party_str = str(party).strip().lower()
    if party_str in ['republican', 'r', 'gop']:
        return 'Republican'
    elif party_str in ['democrat', 'd', 'democratic', 'dem']:
        return 'Democrat'
    return None


print("=" * 80)
print("ELECTION WINNER MARKETS COMPARISON: POLYMARKET VS KALSHI")
print("=" * 80)

# ============================================================================
# 1. Load GPT selections + master data
# ============================================================================

print("\n1. Loading data...")

# Load selections from pipeline_select_election_winners.py
if not os.path.exists(SELECTIONS_FILE):
    print(f"   ERROR: {SELECTIONS_FILE} not found.")
    print("   Run pipeline_select_election_winners.py first!")
    sys.exit(1)

with open(SELECTIONS_FILE, 'r') as f:
    selections = json.load(f)

print(f"   Loaded {len(selections):,} election selections")

# Load master CSV for market metadata
df = load_master_csv()
df['market_id'] = df['market_id'].astype(str)
df['location'] = df['location'].replace('AK-AL', 'AK-1')
print(f"   Loaded {len(df):,} markets from master CSV")

print(f"   Using election_eve_price (UTC midnight on election day) as prediction price")

# ============================================================================
# 2. Build selected markets from GPT selections
# ============================================================================

print("\n2. Building market selections from GPT results...")

pm_rows = []
kalshi_rows = []
skipped_not_found = 0

for election_key, sel in selections.items():
    if not sel.get('election_found', False):
        skipped_not_found += 1
        continue

    # Parse election info
    # Key format: country|office|location|election_year|is_primary (from ELECTION_COLS)
    parts = election_key.split("|")
    election_info = {
        'country': parts[0] if parts[0] else None,
        'office': parts[1] if len(parts) > 1 and parts[1] else None,
        'location': parts[2] if len(parts) > 2 and parts[2] else None,
        'election_year': float(parts[3]) if len(parts) > 3 and parts[3] else None,
        'is_primary': parts[4] if len(parts) > 4 and parts[4] else None,
    }

    # Get election result info
    winning_party = normalize_party(sel.get('winning_party'))
    winning_candidate = sel.get('winning_candidate')
    d_share = sel.get('democrat_vote_share')
    r_share = sel.get('republican_vote_share')

    # Get selected market IDs
    pm_id = sel.get('polymarket_winner_market_id')
    k_id = sel.get('kalshi_winner_market_id')

    if pm_id and str(pm_id) != 'null':
        pm_market = df[df['market_id'] == str(pm_id)]
        if len(pm_market) > 0:
            row = pm_market.iloc[0].to_dict()
            row['winning_party'] = winning_party
            row['winning_candidate'] = winning_candidate
            row['democrat_vote_share'] = d_share if d_share else row.get('democrat_vote_share')
            row['republican_vote_share'] = r_share if r_share else row.get('republican_vote_share')
            row['election_key'] = election_key
            pm_rows.append(row)

    if k_id and str(k_id) != 'null':
        k_market = df[df['market_id'] == str(k_id)]
        if len(k_market) > 0:
            row = k_market.iloc[0].to_dict()
            row['winning_party'] = winning_party
            row['winning_candidate'] = winning_candidate
            row['democrat_vote_share'] = d_share if d_share else row.get('democrat_vote_share')
            row['republican_vote_share'] = r_share if r_share else row.get('republican_vote_share')
            row['election_key'] = election_key
            kalshi_rows.append(row)

pm_selected = pd.DataFrame(pm_rows)
kalshi_selected = pd.DataFrame(kalshi_rows)

print(f"   Elections not found / future: {skipped_not_found}")
print(f"   Polymarket selections: {len(pm_selected)}")
print(f"   Kalshi selections: {len(kalshi_selected)}")

if len(pm_selected) == 0 and len(kalshi_selected) == 0:
    print("\n   ERROR: No winner markets selected. Run pipeline_select_election_winners.py first.")
    sys.exit(1)

# ============================================================================
# 3. Filter to markets with election_eve_price
# ============================================================================

print("\n3. Filtering to markets with election_eve_price...")

pm_selected['market_id'] = pm_selected['market_id'].astype(str)
kalshi_selected['market_id'] = kalshi_selected['market_id'].astype(str)

pm_with_pred = pm_selected[pm_selected['election_eve_price'].notna()].copy()
kalshi_with_pred = kalshi_selected[kalshi_selected['election_eve_price'].notna()].copy()

print(f"   Polymarket with election_eve_price: {len(pm_with_pred)} elections")
print(f"   Kalshi with election_eve_price: {len(kalshi_with_pred)} elections")

pm_missing = len(pm_selected) - len(pm_with_pred)
kalshi_missing = len(kalshi_selected) - len(kalshi_with_pred)
if pm_missing > 0 or kalshi_missing > 0:
    print(f"   Missing election_eve_price: {pm_missing} PM, {kalshi_missing} Kalshi")

# ============================================================================
# 4. Calculate winner prediction and Brier score
# ============================================================================

print("\n4. Calculating metrics...")


def calc_winner_metrics(df_in):
    """
    Calculate winner prediction, Brier score, and accuracy.

    election_eve_price is the "Yes" token price. If the winner is the
    "No" outcome (winning_outcome == "No"), flip to 1 - eve_price.
    winning_outcome is normalized to Yes/No in the master CSV.
    """
    df_out = df_in.copy()
    df_out['winner_prediction'] = df_out.apply(
        lambda row: 1.0 - row['election_eve_price']
                     if str(row.get('winning_outcome', '')).strip().lower() == 'no'
                     else row['election_eve_price'],
        axis=1
    )
    df_out['brier'] = (df_out['winner_prediction'] - 1.0) ** 2
    df_out['correct'] = df_out['winner_prediction'] > 0.5
    return df_out


pm_with_pred = calc_winner_metrics(pm_with_pred)
kalshi_with_pred = calc_winner_metrics(kalshi_with_pred)

# Derive republican_won from winning_party for downstream partisan analysis
for df_part in [pm_with_pred, kalshi_with_pred]:
    df_part['republican_won'] = (df_part['winning_party'] == 'Republican').astype(int)
    df_part.loc[df_part['winning_party'].isna(), 'republican_won'] = np.nan

# Deduplicate by market_id
pm_with_pred = pm_with_pred.drop_duplicates(subset='market_id', keep='first')
kalshi_with_pred = kalshi_with_pred.drop_duplicates(subset='market_id', keep='first')

print(f"   Polymarket: {len(pm_with_pred)} unique elections with predictions")
print(f"   Kalshi: {len(kalshi_with_pred)} unique elections with predictions")

# Log incorrect predictions
pm_incorrect = pm_with_pred[~pm_with_pred['correct']]
kalshi_incorrect = kalshi_with_pred[~kalshi_with_pred['correct']]
print(f"\n   Incorrect predictions (predicted loser would win):")
print(f"      Polymarket: {len(pm_incorrect)}")
print(f"      Kalshi: {len(kalshi_incorrect)}")

# ============================================================================
# 5. Panel A: All Elections
# ============================================================================

print("\n5. Creating Panel A: All Elections...")


def calc_panel_metrics(df_in, platform_name):
    """Calculate metrics for a platform."""
    return {
        'Platform': platform_name,
        'N_Elections': len(df_in),
        'Mean_Brier': df_in['brier'].mean(),
        'Accuracy': df_in['correct'].mean(),
        'Avg_Volume_K': df_in['volume_usd'].mean() / 1000 if 'volume_usd' in df_in.columns else np.nan,
        'Median_Volume_K': df_in['volume_usd'].median() / 1000 if 'volume_usd' in df_in.columns else np.nan
    }


panel_a_pm = calc_panel_metrics(pm_with_pred, 'Polymarket')
panel_a_kalshi = calc_panel_metrics(kalshi_with_pred, 'Kalshi')

panel_a = pd.DataFrame([panel_a_pm, panel_a_kalshi])
print(panel_a)

# ============================================================================
# 6. Panel B: Head-to-Head Comparison (Shared Elections)
# ============================================================================

print("\n6. Creating Panel B: Shared Elections...")

# Find elections on both platforms using election_key
pm_keys = set(pm_with_pred['election_key'])
kalshi_keys = set(kalshi_with_pred['election_key'])
shared_keys = pm_keys.intersection(kalshi_keys)

print(f"   {len(shared_keys)} elections on both platforms")

if len(shared_keys) == 0:
    print("   WARNING: No shared elections found!")
    print("   Skipping Panel B, stats, and scatterplot.")
    # Still save Panel A
    panel_a.to_csv(f"{PAPER_DATA_DIR}/election_winner_panel_a.csv", index=False)
    # Save detailed Panel A (needed by downstream scripts)
    detail_cols = ['platform', 'country', 'office', 'location', 'election_year', 'is_primary',
                   'market_id', 'question', 'volume_usd', 'winner_prediction', 'brier', 'correct',
                   'winning_party', 'winning_candidate', 'republican_won',
                   'democrat_vote_share', 'republican_vote_share']
    pm_detail_cols = [c for c in detail_cols if c in pm_with_pred.columns]
    kalshi_detail_cols = [c for c in detail_cols if c in kalshi_with_pred.columns]
    pm_details = pm_with_pred[pm_detail_cols].sort_values(['election_year', 'office', 'location']).reset_index(drop=True)
    kalshi_details = kalshi_with_pred[kalshi_detail_cols].sort_values(['election_year', 'office', 'location']).reset_index(drop=True)
    panel_a_all = pd.concat([pm_details, kalshi_details], ignore_index=True)
    panel_a_all.to_csv(f"{PAPER_DATA_DIR}/election_winner_panel_a_detailed.csv", index=False)
    print(f"   Saved Panel A detailed: {len(panel_a_all)} markets")
    print("\n" + "=" * 80)
    print("DONE (Panel A only)")
    print("=" * 80)
    sys.exit(0)

# Filter to shared
pm_shared = pm_with_pred[pm_with_pred['election_key'].isin(shared_keys)].copy()
kalshi_shared = kalshi_with_pred[kalshi_with_pred['election_key'].isin(shared_keys)].copy()

# Sort by election key for proper pairing
pm_shared = pm_shared.sort_values('election_key').reset_index(drop=True)
kalshi_shared = kalshi_shared.sort_values('election_key').reset_index(drop=True)

print(f"   After filtering: PM={len(pm_shared)}, Kalshi={len(kalshi_shared)}")
assert len(pm_shared) == len(kalshi_shared), f"Mismatch: PM={len(pm_shared)}, Kalshi={len(kalshi_shared)}"

# Metrics
panel_b_pm = calc_panel_metrics(pm_shared, 'Polymarket')
panel_b_kalshi = calc_panel_metrics(kalshi_shared, 'Kalshi')
panel_b = pd.DataFrame([panel_b_pm, panel_b_kalshi])

# Statistical comparisons
brier_diff = pm_shared['brier'].values - kalshi_shared['brier'].values
t_stat, p_value = stats.ttest_rel(pm_shared['brier'], kalshi_shared['brier'])

pm_wins = (pm_shared['brier'].values < kalshi_shared['brier'].values).sum()
kalshi_wins = (kalshi_shared['brier'].values < pm_shared['brier'].values).sum()
ties = (pm_shared['brier'].values == kalshi_shared['brier'].values).sum()

correlation = np.corrcoef(pm_shared['brier'], kalshi_shared['brier'])[0, 1]
mean_diff = brier_diff.mean()

print("\nPanel B: Head-to-Head Statistics")
print(f"  Paired t-test p-value: {p_value:.4f}")
print(f"  Polymarket wins: {pm_wins} ({pm_wins / len(shared_keys) * 100:.1f}%)")
print(f"  Kalshi wins: {kalshi_wins} ({kalshi_wins / len(shared_keys) * 100:.1f}%)")
print(f"  Ties: {ties}")
print(f"  Correlation: {correlation:.4f}")
print(f"  Mean Brier difference (PM - Kalshi): {mean_diff:.4f}")

# ============================================================================
# 7. Generate LaTeX Table
# ============================================================================

print("\n7. Generating LaTeX table...")

latex = r"""\begin{table}[htbp]
\centering
\caption{Election Winner Markets Comparison: Polymarket vs Kalshi}
\label{tab:election_winner_comparison}
\begin{threeparttable}
\begin{tabular}{lccccc}
\toprule
Platform & N Elections & Brier Score & Accuracy & Avg Vol (\$K) & Median Vol (\$K) \\
\midrule
\multicolumn{6}{l}{\textbf{Panel A: All Election Winner Markets}} \\
"""

for _, row in panel_a.iterrows():
    avg_vol_str = f"{row['Avg_Volume_K']:,.0f}" if not pd.isna(row['Avg_Volume_K']) else "---"
    median_vol_str = f"{row['Median_Volume_K']:,.0f}" if not pd.isna(row['Median_Volume_K']) else "---"
    accuracy_str = f"{row['Accuracy']:.2%}".replace('%', '\\%')
    latex += f"{row['Platform']} & {int(row['N_Elections'])} & {row['Mean_Brier']:.4f} & {accuracy_str} & {avg_vol_str} & {median_vol_str} \\\\\n"

latex += r"""\midrule
\multicolumn{6}{l}{\textbf{Panel B: Head-to-Head Comparison (Elections on Both Platforms)}} \\
"""

for _, row in panel_b.iterrows():
    avg_vol_str = f"{row['Avg_Volume_K']:,.0f}" if not pd.isna(row['Avg_Volume_K']) else "---"
    median_vol_str = f"{row['Median_Volume_K']:,.0f}" if not pd.isna(row['Median_Volume_K']) else "---"
    accuracy_str = f"{row['Accuracy']:.2%}".replace('%', '\\%')
    latex += f"{row['Platform']} & {int(row['N_Elections'])} & {row['Mean_Brier']:.4f} & {accuracy_str} & {avg_vol_str} & {median_vol_str} \\\\\n"

latex += r"""\midrule
\multicolumn{6}{l}{\textit{Comparison Statistics:}} \\
"""
latex += f"\\multicolumn{{6}}{{l}}{{\\quad Paired t-test p-value: {p_value:.4f}}} \\\\\n"
latex += f"\\multicolumn{{6}}{{l}}{{\\quad Lower Brier (more accurate): Polymarket {pm_wins} ({pm_wins / len(shared_keys) * 100:.1f}\\%), Kalshi {kalshi_wins} ({kalshi_wins / len(shared_keys) * 100:.1f}\\%)}} \\\\\n"
latex += f"\\multicolumn{{6}}{{l}}{{\\quad Correlation: {correlation:.4f}}} \\\\\n"
latex += f"\\multicolumn{{6}}{{l}}{{\\quad Mean Brier difference (PM - Kalshi): {mean_diff:.4f}}} \\\\\n"

latex += r"""\bottomrule
\end{tabular}
\begin{tablenotes}
\small
\item Notes: Panel A shows all election winner markets on each platform. Panel B shows markets for elections available on both platforms. Brier score is squared prediction error (lower is better). Prediction prices are election eve prices (UTC midnight on election day). Accuracy is percent of correct winner predictions (price $>$ 0.5 for winner). Avg Vol is average trading volume in thousands of dollars.
\end{tablenotes}
\end{threeparttable}
\end{table}
"""

output_file = f"{TABLES_DIR}/election_winner_comparison.tex"
with open(output_file, 'w') as f:
    f.write(latex)
print(f"   Saved to {output_file}")

# Save panel CSVs
panel_a.to_csv(f"{PAPER_DATA_DIR}/election_winner_panel_a.csv", index=False)
panel_b.to_csv(f"{PAPER_DATA_DIR}/election_winner_panel_b.csv", index=False)

# Save detailed Panel A
print("\n   Saving Panel A market details...")

detail_cols = ['platform', 'country', 'office', 'location', 'election_year', 'is_primary',
               'market_id', 'question', 'volume_usd', 'winner_prediction', 'brier', 'correct',
               'winning_party', 'winning_candidate', 'republican_won',
               'democrat_vote_share', 'republican_vote_share']

# Use only columns that exist
pm_detail_cols = [c for c in detail_cols if c in pm_with_pred.columns]
kalshi_detail_cols = [c for c in detail_cols if c in kalshi_with_pred.columns]

pm_details = pm_with_pred[pm_detail_cols].sort_values(['election_year', 'office', 'location']).reset_index(drop=True)
kalshi_details = kalshi_with_pred[kalshi_detail_cols].sort_values(['election_year', 'office', 'location']).reset_index(drop=True)

panel_a_all = pd.concat([pm_details, kalshi_details], ignore_index=True)
panel_a_all = panel_a_all.sort_values(['platform', 'election_year', 'office', 'location']).reset_index(drop=True)
panel_a_all.to_csv(f"{PAPER_DATA_DIR}/election_winner_panel_a_detailed.csv", index=False)
print(f"   Saved Panel A: {len(panel_a_all)} markets ({len(pm_details)} PM + {len(kalshi_details)} Kalshi)")

# Save comparison stats
comparison_stats = pd.DataFrame([{
    'n_shared': len(shared_keys),
    't_statistic': t_stat,
    'p_value': p_value,
    'pm_wins': pm_wins,
    'kalshi_wins': kalshi_wins,
    'ties': ties,
    'correlation': correlation,
    'mean_brier_diff': mean_diff
}])
comparison_stats.to_csv(f"{PAPER_DATA_DIR}/election_winner_comparison_stats.csv", index=False)

# ============================================================================
# 8. Export Shared Markets (elections on both platforms)
# ============================================================================

print("\n8. Exporting shared markets data...")

shared_wide = []
for key in sorted(shared_keys):
    pm_row = pm_shared[pm_shared['election_key'] == key].iloc[0]
    kalshi_row = kalshi_shared[kalshi_shared['election_key'] == key].iloc[0]

    wide_row = {
        'country': pm_row.get('country'),
        'office': pm_row.get('office'),
        'location': pm_row.get('location'),
        'election_year': pm_row.get('election_year'),
        'is_primary': pm_row.get('is_primary'),

        'democrat_vote_share': pm_row.get('democrat_vote_share'),
        'republican_vote_share': pm_row.get('republican_vote_share'),
        'winning_party': pm_row.get('winning_party'),
        'winning_candidate': pm_row.get('winning_candidate'),
        'republican_won': 1 if pm_row.get('winning_party') == 'Republican' else 0,

        'pm_market_id': pm_row['market_id'],
        'pm_question': pm_row.get('question'),
        'pm_volume_usd': pm_row.get('volume_usd'),
        'pm_prediction': pm_row['winner_prediction'],
        'pm_brier': pm_row['brier'],
        'pm_correct': pm_row['correct'],

        'kalshi_market_id': kalshi_row['market_id'],
        'kalshi_question': kalshi_row.get('question'),
        'kalshi_volume_usd': kalshi_row.get('volume_usd'),
        'kalshi_prediction': kalshi_row['winner_prediction'],
        'kalshi_brier': kalshi_row['brier'],
        'kalshi_correct': kalshi_row['correct'],
    }
    shared_wide.append(wide_row)

shared_export = pd.DataFrame(shared_wide)
shared_export = shared_export.sort_values(['election_year', 'office', 'location']).reset_index(drop=True)

output_path = f"{PAPER_DATA_DIR}/shared_election_markets_detailed.csv"
shared_export.to_csv(output_path, index=False)
print(f"   Saved {len(shared_export)} shared elections")

# ============================================================================
# 9. Create Scatterplot
# ============================================================================

print("\n9. Creating scatterplot...")


def create_label(row):
    year = int(row['election_year'])
    location = row['location']
    return f"{location} '{str(year)[2:]}"


shared_export['election_label'] = shared_export.apply(create_label, axis=1)

fig, ax = plt.subplots(figsize=(12, 10))

scatter = ax.scatter(
    shared_export['kalshi_prediction'],
    shared_export['pm_prediction'],
    s=100, alpha=0.6, c='steelblue',
    edgecolors='black', linewidth=0.5
)

ax.plot([0, 1], [0, 1], 'k--', alpha=0.3, linewidth=1.5, label='Perfect Agreement')

# Label top 10 furthest from diagonal
shared_export['distance_from_diagonal'] = abs(shared_export['kalshi_prediction'] - shared_export['pm_prediction'])
top_10 = shared_export.nlargest(10, 'distance_from_diagonal')

for _, row in top_10.iterrows():
    ax.annotate(
        row['election_label'],
        (row['kalshi_prediction'], row['pm_prediction']),
        xytext=(5, 5), textcoords='offset points',
        fontsize=7, alpha=0.7
    )

ax.set_xlabel('Kalshi Prediction (Probability of Winner)', fontsize=11, fontweight='bold')
ax.set_ylabel('Polymarket Prediction (Probability of Winner)', fontsize=11, fontweight='bold')
ax.set_title('Platform Comparison: Kalshi vs Polymarket Election Predictions\n(Election Eve — UTC Midnight on Election Day)',
             fontsize=12, fontweight='bold', pad=15)
ax.set_xlim(-0.05, 1.05)
ax.set_ylim(-0.05, 1.05)
ax.grid(True, alpha=0.2, linestyle='--')

corr_scatter = shared_export['kalshi_prediction'].corr(shared_export['pm_prediction'])
diff_scatter = (shared_export['pm_prediction'] - shared_export['kalshi_prediction']).mean()
ax.legend(loc='upper left',
          title=f'Correlation: {corr_scatter:.3f}\nMean Diff (PM-K): {diff_scatter:.3f}',
          framealpha=0.9)
ax.set_aspect('equal', adjustable='box')

scatter_path = f"{GRAPHS_DIR}/shared_election_scatterplot.png"
plt.tight_layout()
plt.savefig(scatter_path, dpi=300, bbox_inches='tight')
plt.close()

print(f"   Saved scatterplot: {scatter_path}")
print(f"   Correlation: {corr_scatter:.4f}")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
