#!/usr/bin/env python3
"""
Partisan Bias Regression Table

Runs a regression to formally test whether prediction markets show systematic
bias toward Republican or Democrat candidates, controlling for other factors.

Output:
- tables/table_partisan_bias_regression.tex
- data/partisan_bias_regression_results.csv
"""

import pandas as pd
import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf
import os

from config import BASE_DIR, DATA_DIR, get_latest_file
from paper_config import load_master_csv, load_prediction_accuracy, PAPER_TABLES_DIR, PAPER_DATA_DIR

# Paths
PANEL_A_FILE = PAPER_DATA_DIR / "election_winner_panel_a_detailed.csv"
TABLES_DIR = PAPER_TABLES_DIR
TABLES_DIR.mkdir(exist_ok=True)

print("=" * 70)
print("PARTISAN BIAS REGRESSION TABLE")
print("=" * 70)

# Try to load panel A file first (has party affiliation)
if PANEL_A_FILE.exists():
    print(f"\nLoading Panel A detailed file: {PANEL_A_FILE}")
    df = pd.read_csv(PANEL_A_FILE)
    print(f"  Loaded {len(df):,} markets")

    # Filter to markets with party affiliation
    df = df[df['winning_party'].notna()].copy()
    print(f"  With party affiliation: {len(df):,}")

    # Create variables for regression
    df['is_republican'] = (df['winning_party'] == 'Republican').astype(int)
    df['is_polymarket'] = (df['platform'] == 'Polymarket').astype(int)

    # Prediction error = republican probability - republican actual outcome
    # Positive error means market was biased toward Republican
    # Negative error means market was biased toward Democrat
    if 'winner_prediction' in df.columns and 'republican_won' in df.columns:
        # Derive republican probability from winner_prediction:
        # If Republican won: r_prob = winner_prediction (market was predicting R winner)
        # If Democrat won: r_prob = 1 - winner_prediction (invert to get R probability)
        df['r_prob'] = df.apply(
            lambda row: row['winner_prediction'] if row['republican_won'] == 1
            else 1 - row['winner_prediction'], axis=1
        )
        df['prediction_error'] = df['r_prob'] - df['republican_won']

    elif 'brier' in df.columns:
        # Use Brier-based error
        df['prediction_error'] = np.sqrt(df['brier'])  # Root Brier as proxy for error magnitude

    else:
        print("  Warning: Could not find prediction columns, skipping regression")
        exit(1)

else:
    print(f"\nPanel A file not found, loading from master CSV...")
    df_master = load_master_csv()

    # Filter to markets with party affiliation
    df = df_master[df_master['party_affiliation'].notna()].copy()
    print(f"  With party affiliation: {len(df):,}")

    if len(df) == 0:
        print("  No markets with party affiliation found!")
        exit(1)

    # Merge with prediction data
    all_pred = []

    pm_pred = load_prediction_accuracy("polymarket")
    if pm_pred is not None:
        pm_pred = pm_pred[pm_pred['days_before_event'] == 1]
        pm_pred['platform'] = 'Polymarket'
        all_pred.append(pm_pred)

    kalshi_pred = load_prediction_accuracy("kalshi")
    if kalshi_pred is not None:
        kalshi_pred = kalshi_pred[kalshi_pred['days_before_event'] == 1]
        kalshi_pred['platform'] = 'Kalshi'
        if 'ticker' in kalshi_pred.columns:
            kalshi_pred['market_id'] = kalshi_pred['ticker']
        all_pred.append(kalshi_pred)

    if not all_pred:
        print("  No prediction files found!")
        exit(1)

    pred_df = pd.concat(all_pred, ignore_index=True)
    pred_df['market_id'] = pred_df['market_id'].astype(str)

    df['market_id'] = df['market_id'].astype(str)
    df = df.merge(pred_df[['market_id', 'prediction_price', 'actual_outcome', 'platform']],
                  on=['market_id', 'platform'], how='inner')

    df['is_republican'] = (df['party_affiliation'] == 'Republican').astype(int)
    df['is_polymarket'] = (df['platform'] == 'Polymarket').astype(int)
    df['prediction_error'] = df['prediction_price'] - df['actual_outcome']

print(f"\nFinal dataset for regression: {len(df):,} observations")
print(f"  Republicans: {df['is_republican'].sum():,}")
print(f"  Democrats: {(1 - df['is_republican']).sum():,}")
print(f"  Polymarket: {df['is_polymarket'].sum():,}")
print(f"  Kalshi: {(1 - df['is_polymarket']).sum():,}")

# Handle missing values
df = df.dropna(subset=['prediction_error', 'is_republican', 'is_polymarket'])

if len(df) < 10:
    print("\n  Not enough data for regression!")
    exit(1)

# Run regressions
print("\n" + "-" * 60)
print("REGRESSION RESULTS")
print("-" * 60)

# Model 1: Just party
print("\nModel 1: Prediction Error ~ Party")
df['const'] = 1
X1 = df[['const', 'is_republican']]
y = df['prediction_error']
model1 = sm.OLS(y, X1).fit()
print(model1.summary().tables[1])

# Model 2: Party + Platform
print("\nModel 2: Prediction Error ~ Party + Platform")
X2 = df[['const', 'is_republican', 'is_polymarket']]
model2 = sm.OLS(y, X2).fit()
print(model2.summary().tables[1])

# Model 3: Party + Platform + Interaction
print("\nModel 3: Prediction Error ~ Party * Platform")
df['party_x_platform'] = df['is_republican'] * df['is_polymarket']
X3 = df[['const', 'is_republican', 'is_polymarket', 'party_x_platform']]
model3 = sm.OLS(y, X3).fit()
print(model3.summary().tables[1])

# Save regression results to CSV
results_data = []
for i, (name, model) in enumerate([('Model 1', model1), ('Model 2', model2), ('Model 3', model3)], 1):
    for var in model.params.index:
        results_data.append({
            'Model': name,
            'Variable': var,
            'Coefficient': model.params[var],
            'Std Error': model.bse[var],
            't-stat': model.tvalues[var],
            'p-value': model.pvalues[var],
            'CI Lower': model.conf_int().loc[var, 0],
            'CI Upper': model.conf_int().loc[var, 1]
        })
    results_data.append({
        'Model': name,
        'Variable': 'R-squared',
        'Coefficient': model.rsquared,
        'Std Error': np.nan,
        't-stat': np.nan,
        'p-value': np.nan,
        'CI Lower': np.nan,
        'CI Upper': np.nan
    })
    results_data.append({
        'Model': name,
        'Variable': 'N',
        'Coefficient': model.nobs,
        'Std Error': np.nan,
        't-stat': np.nan,
        'p-value': np.nan,
        'CI Lower': np.nan,
        'CI Upper': np.nan
    })

results_df = pd.DataFrame(results_data)
csv_output = PAPER_DATA_DIR / "partisan_bias_regression_results.csv"
results_df.to_csv(csv_output, index=False)
print(f"\nSaved CSV: {csv_output}")

# Generate LaTeX regression table
latex_output = TABLES_DIR / "table_partisan_bias_regression.tex"


def format_coef(coef, se, pval):
    """Format coefficient with stars for significance."""
    stars = ''
    if pval < 0.01:
        stars = '***'
    elif pval < 0.05:
        stars = '**'
    elif pval < 0.10:
        stars = '*'
    return f"{coef:.4f}{stars}", f"({se:.4f})"


with open(latex_output, 'w') as f:
    f.write(r'\begin{table}[htbp]' + '\n')
    f.write(r'\centering' + '\n')
    f.write(r'\caption{Partisan Bias in Prediction Markets: Regression Results}' + '\n')
    f.write(r'\label{tab:partisan_bias_regression}' + '\n')
    f.write(r'\begin{tabular}{lccc}' + '\n')
    f.write(r'\toprule' + '\n')
    f.write(r'& (1) & (2) & (3) \\' + '\n')
    f.write(r'\midrule' + '\n')

    # Republican indicator
    f.write(r'Republican & ')
    for model in [model1, model2, model3]:
        coef, se = format_coef(model.params['is_republican'],
                               model.bse['is_republican'],
                               model.pvalues['is_republican'])
        f.write(f'{coef} & ' if model != model3 else f'{coef} ')
    f.write(r'\\' + '\n')
    f.write(r'& ')
    for model in [model1, model2, model3]:
        _, se = format_coef(model.params['is_republican'],
                            model.bse['is_republican'],
                            model.pvalues['is_republican'])
        f.write(f'{se} & ' if model != model3 else f'{se} ')
    f.write(r'\\' + '\n')
    f.write(r'[0.5em]' + '\n')

    # Polymarket indicator (Models 2 and 3)
    f.write(r'Polymarket & & ')
    for model in [model2, model3]:
        coef, se = format_coef(model.params['is_polymarket'],
                               model.bse['is_polymarket'],
                               model.pvalues['is_polymarket'])
        f.write(f'{coef} & ' if model != model3 else f'{coef} ')
    f.write(r'\\' + '\n')
    f.write(r'& & ')
    for model in [model2, model3]:
        _, se = format_coef(model.params['is_polymarket'],
                            model.bse['is_polymarket'],
                            model.pvalues['is_polymarket'])
        f.write(f'{se} & ' if model != model3 else f'{se} ')
    f.write(r'\\' + '\n')
    f.write(r'[0.5em]' + '\n')

    # Interaction (Model 3 only)
    f.write(r'Republican $\times$ Polymarket & & & ')
    coef, se = format_coef(model3.params['party_x_platform'],
                           model3.bse['party_x_platform'],
                           model3.pvalues['party_x_platform'])
    f.write(f'{coef} ' + r'\\' + '\n')
    f.write(r'& & & ')
    f.write(f'{se} ' + r'\\' + '\n')
    f.write(r'[0.5em]' + '\n')

    # Constant
    f.write(r'Constant & ')
    for model in [model1, model2, model3]:
        coef, se = format_coef(model.params['const'],
                               model.bse['const'],
                               model.pvalues['const'])
        f.write(f'{coef} & ' if model != model3 else f'{coef} ')
    f.write(r'\\' + '\n')
    f.write(r'& ')
    for model in [model1, model2, model3]:
        _, se = format_coef(model.params['const'],
                            model.bse['const'],
                            model.pvalues['const'])
        f.write(f'{se} & ' if model != model3 else f'{se} ')
    f.write(r'\\' + '\n')

    f.write(r'\midrule' + '\n')
    f.write(f'Observations & {int(model1.nobs):,} & {int(model2.nobs):,} & {int(model3.nobs):,} ' + r'\\' + '\n')
    f.write(f'R-squared & {model1.rsquared:.4f} & {model2.rsquared:.4f} & {model3.rsquared:.4f} ' + r'\\' + '\n')
    f.write(r'\bottomrule' + '\n')
    f.write(r'\end{tabular}' + '\n')
    f.write(r'\begin{tablenotes}' + '\n')
    f.write(r'\small' + '\n')
    f.write(r'\item Note: Dependent variable is prediction error (prediction - actual outcome). Positive values indicate over-prediction. Standard errors in parentheses. *** p$<$0.01, ** p$<$0.05, * p$<$0.10.' + '\n')
    f.write(r'\end{tablenotes}' + '\n')
    f.write(r'\end{table}' + '\n')

print(f"Saved LaTeX: {latex_output}")

# Print key findings
print("\n" + "=" * 60)
print("KEY FINDINGS")
print("=" * 60)
republican_coef = model2.params['is_republican']
republican_pval = model2.pvalues['is_republican']
print(f"\nRepublican coefficient: {republican_coef:.4f} (p={republican_pval:.4f})")
if republican_pval < 0.05:
    if republican_coef > 0:
        print("  -> Markets OVER-predict Republican candidates (pro-Republican bias)")
    else:
        print("  -> Markets UNDER-predict Republican candidates (pro-Democrat bias)")
else:
    print("  -> No statistically significant partisan bias detected")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
