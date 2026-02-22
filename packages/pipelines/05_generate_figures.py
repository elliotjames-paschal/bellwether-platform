#!/usr/bin/env python3
"""
Generate All 13 Figures for Degrees of Freedom Paper

Figures:
1. Calibration curves (PM vs Kalshi)
2. Brier score KDE distributions
3. Platform ranking flips by specification
4. Convergence curves (accuracy over time)
5. VWAP vs spot price comparison
6. Price divergence by liquidity depth
7. Cost-to-move CDF
8. Brier vs volume threshold
9. Accuracy by political category
10. Category composition decomposition
11. Shared vs unique market analysis
12. Platform price agreement
13. Specification curve (Simonsohn-style)

Outputs: figures/fig01_*.{pdf,png} through figures/fig13_*.{pdf,png}
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats

# Add scripts dir to path
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR

# Output paths
OUTPUT_DIR = BASE_DIR / "output"
FIGURES_DIR = BASE_DIR / "figures"
FIGURES_DIR.mkdir(exist_ok=True)

# Input files
COMPUTED_PRICES_FILE = OUTPUT_DIR / "computed_prices.csv"
SPEC_RESULTS_FILE = OUTPUT_DIR / "specification_results.csv"
PM_PRED_FILE = DATA_DIR / "polymarket_prediction_accuracy_all_political.csv"
KALSHI_PRED_FILE = DATA_DIR / "kalshi_prediction_accuracy_all_political.csv"
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
PM_ORDERBOOK_FILE = DATA_DIR / "orderbook_history_polymarket.json"
KALSHI_ORDERBOOK_FILE = DATA_DIR / "orderbook_history_kalshi.json"
LIQUIDITY_FILE = DATA_DIR / "liquidity_metrics_by_market.csv"

# Styling (consistent with existing codebase)
sns.set_style("whitegrid")
COLORS = {
    'primary': '#2C3E50',      # Polymarket (dark blue-gray)
    'secondary': '#E74C3C',    # Kalshi (red)
    'gray': '#95A5A6',
    'light_gray': '#BDC3C7',
    'accent': '#3498DB',
}
plt.rcParams.update({
    'figure.figsize': (10, 6),
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 11,
    'figure.dpi': 150,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.facecolor': 'white',
})


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def save_figure(fig, name):
    """Save figure as both PDF and PNG."""
    pdf_path = FIGURES_DIR / f"{name}.pdf"
    png_path = FIGURES_DIR / f"{name}.png"
    fig.savefig(pdf_path, format='pdf')
    fig.savefig(png_path, format='png')
    plt.close(fig)
    log(f"  Saved: {name}.{{pdf,png}}")


# ============================================================================
# Figure 1: Calibration Curves
# ============================================================================

def fig01_calibration_curves(pm_pred, kalshi_pred):
    """Calibration comparison: predicted probability vs actual outcome rate."""
    log("Figure 1: Calibration curves")

    fig, ax = plt.subplots(figsize=(10, 8))

    # Filter to 1 day before
    pm_1d = pm_pred[pm_pred['days_before_event'] == 1].copy()
    kalshi_1d = kalshi_pred[kalshi_pred['days_before_event'] == 1].copy()

    def compute_calibration_bins(df, price_col='prediction_price', outcome_col='actual_outcome', n_bins=10):
        df = df[[price_col, outcome_col]].dropna()
        if len(df) < 50:
            return pd.DataFrame()

        df['bin'] = pd.qcut(df[price_col], n_bins, labels=False, duplicates='drop')
        bins = df.groupby('bin').agg({
            price_col: 'mean',
            outcome_col: ['mean', 'count']
        }).reset_index()
        bins.columns = ['bin', 'predicted', 'actual', 'count']
        return bins

    pm_bins = compute_calibration_bins(pm_1d)
    kalshi_bins = compute_calibration_bins(kalshi_1d)

    # Perfect calibration line
    ax.plot([0, 1], [0, 1], 'k--', linewidth=1.5, alpha=0.5, label='Perfect calibration')

    # Plot calibration points
    if len(pm_bins) > 0:
        ax.scatter(pm_bins['predicted'], pm_bins['actual'],
                  s=pm_bins['count'] / 10, alpha=0.7,
                  color=COLORS['primary'], label=f'Polymarket (n={len(pm_1d):,})')
        ax.plot(pm_bins['predicted'], pm_bins['actual'],
               color=COLORS['primary'], alpha=0.5, linewidth=2)

    if len(kalshi_bins) > 0:
        ax.scatter(kalshi_bins['predicted'], kalshi_bins['actual'],
                  s=kalshi_bins['count'] / 10, alpha=0.7,
                  color=COLORS['secondary'], label=f'Kalshi (n={len(kalshi_1d):,})')
        ax.plot(kalshi_bins['predicted'], kalshi_bins['actual'],
               color=COLORS['secondary'], alpha=0.5, linewidth=2)

    ax.set_xlabel('Predicted Probability', fontsize=12)
    ax.set_ylabel('Actual Outcome Rate', fontsize=12)
    ax.set_title('Calibration Comparison: Polymarket vs Kalshi\n(1 Day Before Resolution)', fontsize=14, fontweight='bold')
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect('equal')
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)

    save_figure(fig, 'fig01_calibration')


# ============================================================================
# Figure 2: Brier Score KDE
# ============================================================================

def fig02_brier_kde(pm_pred, kalshi_pred):
    """Kernel density estimate of Brier score distributions."""
    log("Figure 2: Brier score KDE")

    fig, ax = plt.subplots(figsize=(10, 6))

    # Filter to 1 day before
    pm_1d = pm_pred[pm_pred['days_before_event'] == 1].copy()
    kalshi_1d = kalshi_pred[kalshi_pred['days_before_event'] == 1].copy()

    pm_brier = pm_1d['brier_score'].dropna()
    kalshi_brier = kalshi_1d['brier_score'].dropna()

    # Plot KDEs
    if len(pm_brier) > 10:
        sns.kdeplot(pm_brier, ax=ax, color=COLORS['primary'],
                   label=f'Polymarket (μ={pm_brier.mean():.4f})', linewidth=2, fill=True, alpha=0.3)
    if len(kalshi_brier) > 10:
        sns.kdeplot(kalshi_brier, ax=ax, color=COLORS['secondary'],
                   label=f'Kalshi (μ={kalshi_brier.mean():.4f})', linewidth=2, fill=True, alpha=0.3)

    ax.set_xlabel('Brier Score', fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.set_title('Distribution of Brier Scores by Platform\n(1 Day Before Resolution)', fontsize=14, fontweight='bold')
    ax.legend(loc='upper right', fontsize=10)
    ax.set_xlim(0, 0.5)
    ax.grid(True, alpha=0.3)

    # Add vertical lines for means
    if len(pm_brier) > 0:
        ax.axvline(pm_brier.mean(), color=COLORS['primary'], linestyle='--', alpha=0.7)
    if len(kalshi_brier) > 0:
        ax.axvline(kalshi_brier.mean(), color=COLORS['secondary'], linestyle='--', alpha=0.7)

    save_figure(fig, 'fig02_brier_kde')


# ============================================================================
# Figure 3: Platform Ranking Flips
# ============================================================================

def fig03_ranking_flips(spec_results):
    """Heatmap showing which platform wins under different specifications."""
    log("Figure 3: Platform ranking flips")

    # Pivot to create matrix
    # Rows: truncation, Columns: price type
    # Color by PM win rate

    pivot_data = []
    for truncation in ['Conservative', 'Moderate', 'Aggressive', 'Resolution']:
        for price_type in ['spot', 'vwap_1h', 'vwap_3h', 'vwap_6h', 'vwap_24h', 'midpoint']:
            subset = spec_results[(spec_results['truncation'] == truncation) &
                                 (spec_results['price_type'] == price_type) &
                                 (spec_results['metric'] == 'brier') &
                                 (spec_results['threshold'] == 0)]
            if len(subset) > 0 and subset['pm_wins'].notna().any():
                pm_wins = subset['pm_wins'].mean()
            else:
                pm_wins = np.nan
            pivot_data.append({
                'truncation': truncation,
                'price_type': price_type,
                'pm_win_rate': pm_wins
            })

    pivot_df = pd.DataFrame(pivot_data)
    matrix = pivot_df.pivot(index='truncation', columns='price_type', values='pm_win_rate')

    # Reorder
    matrix = matrix.reindex(['Conservative', 'Moderate', 'Aggressive', 'Resolution'])
    matrix = matrix[['spot', 'vwap_1h', 'vwap_3h', 'vwap_6h', 'vwap_24h', 'midpoint']]

    fig, ax = plt.subplots(figsize=(10, 6))

    # Create heatmap
    cmap = sns.diverging_palette(240, 10, as_cmap=True)
    sns.heatmap(matrix, ax=ax, cmap=cmap, center=0.5, vmin=0, vmax=1,
               annot=True, fmt='.2f', linewidths=0.5,
               cbar_kws={'label': 'Polymarket Win Rate'})

    ax.set_xlabel('Price Type', fontsize=12)
    ax.set_ylabel('Truncation', fontsize=12)
    ax.set_title('Platform Winner by Specification\n(Brier Score, $0 Volume Threshold)', fontsize=14, fontweight='bold')

    save_figure(fig, 'fig03_ranking_flips')


# ============================================================================
# Figure 4: Convergence Curves
# ============================================================================

def fig04_convergence_curves(pm_pred, kalshi_pred):
    """Brier scores over time (days before resolution)."""
    log("Figure 4: Convergence curves")

    fig, ax = plt.subplots(figsize=(10, 6))

    # Aggregate by days before event
    pm_by_days = pm_pred.groupby('days_before_event')['brier_score'].mean().reset_index()
    kalshi_by_days = kalshi_pred.groupby('days_before_event')['brier_score'].mean().reset_index()

    # Filter to reasonable range
    pm_by_days = pm_by_days[pm_by_days['days_before_event'] <= 60]
    kalshi_by_days = kalshi_by_days[kalshi_by_days['days_before_event'] <= 60]

    ax.plot(pm_by_days['days_before_event'], pm_by_days['brier_score'],
           color=COLORS['primary'], linewidth=2, marker='o', markersize=4, label='Polymarket')
    ax.plot(kalshi_by_days['days_before_event'], kalshi_by_days['brier_score'],
           color=COLORS['secondary'], linewidth=2, marker='s', markersize=4, label='Kalshi')

    ax.invert_xaxis()
    ax.set_xlabel('Days Before Resolution', fontsize=12)
    ax.set_ylabel('Mean Brier Score', fontsize=12)
    ax.set_title('Prediction Accuracy Convergence Over Time', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)

    save_figure(fig, 'fig04_convergence')


# ============================================================================
# Figure 5: VWAP vs Spot
# ============================================================================

def fig05_vwap_vs_spot(prices_df):
    """Scatter plot comparing VWAP-24h to spot prices."""
    log("Figure 5: VWAP vs spot comparison")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for idx, platform in enumerate(['Polymarket', 'Kalshi']):
        ax = axes[idx]
        df = prices_df[(prices_df['platform'] == platform) &
                       (prices_df['truncation_label'] == 'Moderate')]

        valid = df[['spot', 'vwap_24h']].dropna()
        if len(valid) < 10:
            ax.text(0.5, 0.5, 'Insufficient data', ha='center', va='center', transform=ax.transAxes)
            continue

        # Scatter plot
        ax.scatter(valid['spot'], valid['vwap_24h'], alpha=0.3, s=20,
                  color=COLORS['primary'] if platform == 'Polymarket' else COLORS['secondary'])

        # Perfect agreement line
        ax.plot([0, 1], [0, 1], 'k--', linewidth=1, alpha=0.5)

        # Compute correlation
        corr = valid['spot'].corr(valid['vwap_24h'])

        ax.set_xlabel('Spot Price', fontsize=11)
        ax.set_ylabel('VWAP-24h Price', fontsize=11)
        ax.set_title(f'{platform}\n(r={corr:.3f}, n={len(valid):,})', fontsize=12, fontweight='bold')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.3)

    fig.suptitle('VWAP-24h vs Spot Price Comparison', fontsize=14, fontweight='bold', y=1.02)
    plt.tight_layout()

    save_figure(fig, 'fig05_vwap_vs_spot')


# ============================================================================
# Figure 6: Price Divergence by Depth
# ============================================================================

def fig06_price_divergence_by_depth(prices_df, master_df):
    """Price divergence (spot - midpoint) grouped by liquidity quintiles."""
    log("Figure 6: Price divergence by liquidity depth")

    fig, ax = plt.subplots(figsize=(10, 6))

    # Merge volume data
    volume_lookup = dict(zip(master_df['market_id'].astype(str), master_df['volume_usd']))
    prices_df['volume'] = prices_df['market_id'].astype(str).map(volume_lookup)

    # Compute divergence
    df = prices_df[prices_df['truncation_label'] == 'Moderate'].copy()
    df['divergence'] = (df['spot'] - df['midpoint']).abs()
    df = df.dropna(subset=['divergence', 'volume'])

    if len(df) < 100:
        ax.text(0.5, 0.5, 'Insufficient data with midpoint prices', ha='center', va='center', transform=ax.transAxes)
        save_figure(fig, 'fig06_divergence_by_depth')
        return

    # Create quintiles
    df['volume_quintile'] = pd.qcut(df['volume'], 5, labels=['Q1 (Low)', 'Q2', 'Q3', 'Q4', 'Q5 (High)'])

    # Group by platform and quintile
    divergence_data = []
    for platform in ['Polymarket', 'Kalshi']:
        for quintile in ['Q1 (Low)', 'Q2', 'Q3', 'Q4', 'Q5 (High)']:
            subset = df[(df['platform'] == platform) & (df['volume_quintile'] == quintile)]
            if len(subset) > 0:
                divergence_data.append({
                    'platform': platform,
                    'quintile': quintile,
                    'mean_divergence': subset['divergence'].mean(),
                    'n': len(subset)
                })

    divergence_df = pd.DataFrame(divergence_data)

    if len(divergence_df) == 0:
        ax.text(0.5, 0.5, 'Insufficient data', ha='center', va='center', transform=ax.transAxes)
        save_figure(fig, 'fig06_divergence_by_depth')
        return

    # Plot
    x = np.arange(5)
    width = 0.35

    pm_data = divergence_df[divergence_df['platform'] == 'Polymarket']
    kalshi_data = divergence_df[divergence_df['platform'] == 'Kalshi']

    if len(pm_data) > 0:
        ax.bar(x - width/2, pm_data['mean_divergence'], width, label='Polymarket', color=COLORS['primary'])
    if len(kalshi_data) > 0:
        ax.bar(x + width/2, kalshi_data['mean_divergence'], width, label='Kalshi', color=COLORS['secondary'])

    ax.set_xlabel('Volume Quintile', fontsize=12)
    ax.set_ylabel('Mean |Spot - Midpoint|', fontsize=12)
    ax.set_title('Price Divergence by Liquidity Depth', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(['Q1\n(Low)', 'Q2', 'Q3', 'Q4', 'Q5\n(High)'])
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')

    save_figure(fig, 'fig06_divergence_by_depth')


# ============================================================================
# Figure 7: Cost-to-Move CDF
# ============================================================================

def fig07_cost_to_move_cdf(master_df):
    """CDF of market depth/liquidity by platform."""
    log("Figure 7: Cost-to-move CDF (liquidity)")

    fig, ax = plt.subplots(figsize=(10, 6))

    pm_data = master_df[master_df['platform'] == 'Polymarket']['volume_usd'].dropna()
    kalshi_data = master_df[master_df['platform'] == 'Kalshi']['volume_usd'].dropna()

    # Filter to positive volumes
    pm_data = pm_data[pm_data > 0]
    kalshi_data = kalshi_data[kalshi_data > 0]

    if len(pm_data) > 0:
        pm_sorted = np.sort(pm_data)
        pm_cdf = np.arange(1, len(pm_sorted) + 1) / len(pm_sorted)
        ax.plot(pm_sorted, pm_cdf, color=COLORS['primary'], linewidth=2,
               label=f'Polymarket (median=${np.median(pm_data):,.0f})')

    if len(kalshi_data) > 0:
        kalshi_sorted = np.sort(kalshi_data)
        kalshi_cdf = np.arange(1, len(kalshi_sorted) + 1) / len(kalshi_sorted)
        ax.plot(kalshi_sorted, kalshi_cdf, color=COLORS['secondary'], linewidth=2,
               label=f'Kalshi (median=${np.median(kalshi_data):,.0f})')

    ax.set_xscale('log')
    ax.set_xlabel('Trading Volume (USD)', fontsize=12)
    ax.set_ylabel('Cumulative Probability', fontsize=12)
    ax.set_title('Market Liquidity Distribution by Platform', fontsize=14, fontweight='bold')
    ax.legend(loc='lower right', fontsize=10)
    ax.grid(True, alpha=0.3)

    save_figure(fig, 'fig07_liquidity_cdf')


# ============================================================================
# Figure 8: Brier vs Volume Threshold
# ============================================================================

def fig08_brier_vs_threshold(spec_results):
    """Mean Brier score as a function of volume threshold."""
    log("Figure 8: Brier vs volume threshold")

    fig, ax = plt.subplots(figsize=(10, 6))

    # Filter to Brier metric, spot price, moderate truncation
    subset = spec_results[(spec_results['metric'] == 'brier') &
                          (spec_results['price_type'] == 'spot') &
                          (spec_results['truncation'] == 'Moderate')]

    thresholds = [0, 1000, 10000, 100000]

    pm_scores = []
    kalshi_scores = []

    for thresh in thresholds:
        row = subset[subset['threshold'] == thresh]
        if len(row) > 0:
            pm_scores.append(row['pm_score'].values[0])
            kalshi_scores.append(row['k_score'].values[0])
        else:
            pm_scores.append(np.nan)
            kalshi_scores.append(np.nan)

    x = np.arange(len(thresholds))

    ax.plot(x, pm_scores, color=COLORS['primary'], linewidth=2, marker='o', markersize=8, label='Polymarket')
    ax.plot(x, kalshi_scores, color=COLORS['secondary'], linewidth=2, marker='s', markersize=8, label='Kalshi')

    ax.set_xlabel('Minimum Volume Threshold', fontsize=12)
    ax.set_ylabel('Mean Brier Score', fontsize=12)
    ax.set_title('Brier Score by Volume Threshold\n(Spot Price, Moderate Truncation)', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(['$0', '$1K', '$10K', '$100K'])
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    save_figure(fig, 'fig08_brier_vs_threshold')


# ============================================================================
# Figure 9: Accuracy by Category
# ============================================================================

def fig09_accuracy_by_category(pm_pred, kalshi_pred):
    """Grouped bar chart of Brier scores by political category."""
    log("Figure 9: Accuracy by category")

    fig, ax = plt.subplots(figsize=(14, 8))

    # Filter to 1 day before
    pm_1d = pm_pred[pm_pred['days_before_event'] == 1].copy()
    kalshi_1d = kalshi_pred[kalshi_pred['days_before_event'] == 1].copy()

    # Get categories
    pm_by_cat = pm_1d.groupby('category')['brier_score'].agg(['mean', 'count']).reset_index()
    kalshi_by_cat = kalshi_1d.groupby('category')['brier_score'].agg(['mean', 'count']).reset_index()

    # Merge
    all_cats = sorted(set(pm_by_cat['category'].dropna()) | set(kalshi_by_cat['category'].dropna()))

    # Filter out empty/NA categories
    all_cats = [c for c in all_cats if pd.notna(c) and c != '']

    bar_data = []
    for cat in all_cats:
        pm_row = pm_by_cat[pm_by_cat['category'] == cat]
        k_row = kalshi_by_cat[kalshi_by_cat['category'] == cat]

        pm_brier = pm_row['mean'].values[0] if len(pm_row) > 0 else np.nan
        k_brier = k_row['mean'].values[0] if len(k_row) > 0 else np.nan

        # Clean category name
        cat_clean = cat.split('. ')[-1] if '. ' in cat else cat

        bar_data.append({
            'category': cat_clean[:20],  # Truncate long names
            'pm_brier': pm_brier,
            'k_brier': k_brier
        })

    bar_df = pd.DataFrame(bar_data)
    bar_df = bar_df.sort_values('pm_brier', ascending=True)

    x = np.arange(len(bar_df))
    width = 0.35

    ax.barh(x - width/2, bar_df['pm_brier'], width, label='Polymarket', color=COLORS['primary'])
    ax.barh(x + width/2, bar_df['k_brier'], width, label='Kalshi', color=COLORS['secondary'])

    ax.set_yticks(x)
    ax.set_yticklabels(bar_df['category'], fontsize=9)
    ax.set_xlabel('Mean Brier Score', fontsize=12)
    ax.set_title('Brier Score by Political Category\n(1 Day Before Resolution)', fontsize=14, fontweight='bold')
    ax.legend(loc='lower right', fontsize=10)
    ax.grid(True, alpha=0.3, axis='x')

    plt.tight_layout()
    save_figure(fig, 'fig09_accuracy_by_category')


# ============================================================================
# Figure 10: Category Composition
# ============================================================================

def fig10_category_composition(master_df):
    """Stacked bar showing category composition by platform."""
    log("Figure 10: Category composition")

    fig, ax = plt.subplots(figsize=(12, 6))

    # Count by platform and category
    pm_data = master_df[master_df['platform'] == 'Polymarket']['political_category'].value_counts()
    kalshi_data = master_df[master_df['platform'] == 'Kalshi']['political_category'].value_counts()

    # Get all categories
    all_cats = sorted(set(pm_data.index.dropna()) | set(kalshi_data.index.dropna()))
    all_cats = [c for c in all_cats if pd.notna(c)]

    # Create stacked bar data
    pm_counts = [pm_data.get(c, 0) for c in all_cats]
    kalshi_counts = [kalshi_data.get(c, 0) for c in all_cats]

    # Clean category names
    cat_labels = [c.split('. ')[-1][:15] if '. ' in c else c[:15] for c in all_cats]

    x = np.arange(len(all_cats))
    width = 0.35

    ax.bar(x - width/2, pm_counts, width, label='Polymarket', color=COLORS['primary'])
    ax.bar(x + width/2, kalshi_counts, width, label='Kalshi', color=COLORS['secondary'])

    ax.set_xlabel('Political Category', fontsize=12)
    ax.set_ylabel('Number of Markets', fontsize=12)
    ax.set_title('Market Composition by Category and Platform', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(cat_labels, rotation=45, ha='right', fontsize=8)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    save_figure(fig, 'fig10_category_composition')


# ============================================================================
# Figure 11: Shared vs Unique Markets
# ============================================================================

def fig11_shared_vs_unique(pm_pred, kalshi_pred):
    """Analysis of markets that exist on both platforms vs unique markets."""
    log("Figure 11: Shared vs unique markets")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Get unique market identifiers (use title as proxy for matching)
    pm_1d = pm_pred[pm_pred['days_before_event'] == 1].copy()
    kalshi_1d = kalshi_pred[kalshi_pred['days_before_event'] == 1].copy()

    # For now, just show counts as we don't have exact matching
    pm_unique = len(pm_1d['market_id'].unique())
    kalshi_unique = len(kalshi_1d.get('ticker', kalshi_1d.get('market_id', pd.Series())).unique())

    # Left plot: Market counts
    ax = axes[0]
    platforms = ['Polymarket', 'Kalshi']
    counts = [pm_unique, kalshi_unique]
    colors = [COLORS['primary'], COLORS['secondary']]

    ax.bar(platforms, counts, color=colors)
    ax.set_ylabel('Number of Markets', fontsize=12)
    ax.set_title('Total Markets by Platform\n(1 Day Before Resolution)', fontsize=12, fontweight='bold')

    for i, v in enumerate(counts):
        ax.text(i, v + 100, f'{v:,}', ha='center', fontsize=11)

    # Right plot: Brier score distributions
    ax = axes[1]
    pm_brier = pm_1d['brier_score'].dropna()
    kalshi_brier = kalshi_1d['brier_score'].dropna()

    violin_data = [pm_brier.values, kalshi_brier.values]
    parts = ax.violinplot(violin_data, positions=[1, 2], showmeans=True, showmedians=True)

    for i, pc in enumerate(parts['bodies']):
        pc.set_facecolor([COLORS['primary'], COLORS['secondary']][i])
        pc.set_alpha(0.7)

    ax.set_xticks([1, 2])
    ax.set_xticklabels(['Polymarket', 'Kalshi'])
    ax.set_ylabel('Brier Score', fontsize=12)
    ax.set_title('Brier Score Distribution by Platform', fontsize=12, fontweight='bold')

    plt.tight_layout()
    save_figure(fig, 'fig11_shared_vs_unique')


# ============================================================================
# Figure 12: Platform Price Agreement
# ============================================================================

def fig12_platform_agreement(prices_df):
    """Scatter plot with marginal histograms for price agreement."""
    log("Figure 12: Platform price agreement")

    # This would require matched markets - for now show correlation analysis
    fig, ax = plt.subplots(figsize=(10, 6))

    # Use spot prices from same truncation
    pm_prices = prices_df[(prices_df['platform'] == 'Polymarket') &
                          (prices_df['truncation_label'] == 'Moderate')]
    kalshi_prices = prices_df[(prices_df['platform'] == 'Kalshi') &
                              (prices_df['truncation_label'] == 'Moderate')]

    # Histogram of price differences within each platform
    pm_spot = pm_prices['spot'].dropna()
    kalshi_spot = kalshi_prices['spot'].dropna()

    ax.hist(pm_spot, bins=50, alpha=0.6, label='Polymarket', color=COLORS['primary'], density=True)
    ax.hist(kalshi_spot, bins=50, alpha=0.6, label='Kalshi', color=COLORS['secondary'], density=True)

    ax.set_xlabel('Spot Price', fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.set_title('Price Distribution by Platform\n(Moderate Truncation)', fontsize=14, fontweight='bold')
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    save_figure(fig, 'fig12_platform_agreement')


# ============================================================================
# Figure 13: Specification Curve (Simonsohn-style)
# ============================================================================

def fig13_specification_curve(spec_results):
    """Simonsohn-style specification curve showing all 192 specifications."""
    log("Figure 13: Specification curve")

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), gridspec_kw={'height_ratios': [3, 1]})

    # Top panel: Sorted effect sizes (score difference)
    ax_top = axes[0]

    # Filter to Brier metric only for cleaner visualization
    brier_specs = spec_results[spec_results['metric'] == 'brier'].copy()
    brier_specs = brier_specs.dropna(subset=['score_diff'])
    brier_specs = brier_specs.sort_values('score_diff')
    brier_specs['spec_rank'] = range(len(brier_specs))

    # Color by winner
    colors = []
    for _, row in brier_specs.iterrows():
        if row['score_diff'] < 0:  # PM wins (lower is better)
            colors.append(COLORS['primary'])
        else:
            colors.append(COLORS['secondary'])

    ax_top.scatter(brier_specs['spec_rank'], brier_specs['score_diff'],
                  c=colors, s=30, alpha=0.7)
    ax_top.axhline(0, color='black', linestyle='--', linewidth=1)

    ax_top.set_ylabel('Brier Score Difference\n(PM - Kalshi)', fontsize=12)
    ax_top.set_title('Specification Curve: Brier Score Difference Across 96 Specifications\n(Negative = Polymarket Better)',
                    fontsize=14, fontweight='bold')

    # Add legend
    pm_patch = mpatches.Patch(color=COLORS['primary'], label='Polymarket better')
    k_patch = mpatches.Patch(color=COLORS['secondary'], label='Kalshi better')
    ax_top.legend(handles=[pm_patch, k_patch], loc='upper left', fontsize=10)

    # Bottom panel: Specification indicators
    ax_bot = axes[1]

    # Create indicator matrix
    indicators = []
    for _, row in brier_specs.iterrows():
        indicators.append({
            'rank': row['spec_rank'],
            'truncation': row['truncation'],
            'price_type': row['price_type'],
            'threshold': row['threshold']
        })

    ind_df = pd.DataFrame(indicators)

    # Plot indicators as colored bands
    y_positions = {'truncation': 3, 'price_type': 2, 'threshold': 1}
    truncation_colors = {'Conservative': '#1a9850', 'Moderate': '#91cf60', 'Aggressive': '#fee08b', 'Resolution': '#d73027'}
    price_colors = {'spot': '#4575b4', 'vwap_1h': '#74add1', 'vwap_3h': '#abd9e9',
                   'vwap_6h': '#e0f3f8', 'vwap_24h': '#fee090', 'midpoint': '#fdae61'}
    threshold_colors = {0: '#f7f7f7', 1000: '#cccccc', 10000: '#969696', 100000: '#525252'}

    for _, row in ind_df.iterrows():
        ax_bot.scatter(row['rank'], 3, color=truncation_colors.get(row['truncation'], 'gray'), s=50, marker='s')
        ax_bot.scatter(row['rank'], 2, color=price_colors.get(row['price_type'], 'gray'), s=50, marker='s')
        ax_bot.scatter(row['rank'], 1, color=threshold_colors.get(row['threshold'], 'gray'), s=50, marker='s')

    ax_bot.set_yticks([1, 2, 3])
    ax_bot.set_yticklabels(['Threshold', 'Price Type', 'Truncation'], fontsize=10)
    ax_bot.set_xlabel('Specification (sorted by effect size)', fontsize=12)
    ax_bot.set_xlim(ax_top.get_xlim())

    plt.tight_layout()
    save_figure(fig, 'fig13_specification_curve')


# ============================================================================
# Main
# ============================================================================

def main():
    log("=" * 70)
    log("GENERATING ALL 13 FIGURES FOR DEGREES OF FREEDOM PAPER")
    log("=" * 70)

    # Load data
    log("\nLoading data files...")

    # Prediction accuracy files
    pm_pred = pd.read_csv(PM_PRED_FILE, dtype={'token_id': str, 'market_id': str})
    kalshi_pred = pd.read_csv(KALSHI_PRED_FILE, dtype={'ticker': str, 'market_id': str})
    log(f"  Predictions: PM={len(pm_pred):,}, Kalshi={len(kalshi_pred):,}")

    # Master CSV
    master_df = pd.read_csv(MASTER_CSV, low_memory=False)
    log(f"  Master markets: {len(master_df):,}")

    # Computed prices (if available)
    if COMPUTED_PRICES_FILE.exists():
        prices_df = pd.read_csv(COMPUTED_PRICES_FILE)
        log(f"  Computed prices: {len(prices_df):,}")
    else:
        prices_df = pd.DataFrame()
        log("  WARNING: computed_prices.csv not found - some figures will be skipped")

    # Specification results (if available)
    if SPEC_RESULTS_FILE.exists():
        spec_results = pd.read_csv(SPEC_RESULTS_FILE)
        log(f"  Specification results: {len(spec_results):,}")
    else:
        spec_results = pd.DataFrame()
        log("  WARNING: specification_results.csv not found - some figures will be skipped")

    # Generate figures
    log("\n" + "=" * 70)
    log("GENERATING FIGURES")
    log("=" * 70 + "\n")

    # Figure 1: Calibration curves
    fig01_calibration_curves(pm_pred, kalshi_pred)

    # Figure 2: Brier KDE
    fig02_brier_kde(pm_pred, kalshi_pred)

    # Figure 3: Ranking flips
    if len(spec_results) > 0:
        fig03_ranking_flips(spec_results)
    else:
        log("  Skipping Figure 3 (no spec results)")

    # Figure 4: Convergence curves
    fig04_convergence_curves(pm_pred, kalshi_pred)

    # Figure 5: VWAP vs spot
    if len(prices_df) > 0:
        fig05_vwap_vs_spot(prices_df)
    else:
        log("  Skipping Figure 5 (no price data)")

    # Figure 6: Price divergence by depth
    if len(prices_df) > 0:
        fig06_price_divergence_by_depth(prices_df, master_df)
    else:
        log("  Skipping Figure 6 (no price data)")

    # Figure 7: Liquidity CDF
    fig07_cost_to_move_cdf(master_df)

    # Figure 8: Brier vs threshold
    if len(spec_results) > 0:
        fig08_brier_vs_threshold(spec_results)
    else:
        log("  Skipping Figure 8 (no spec results)")

    # Figure 9: Accuracy by category
    fig09_accuracy_by_category(pm_pred, kalshi_pred)

    # Figure 10: Category composition
    fig10_category_composition(master_df)

    # Figure 11: Shared vs unique
    fig11_shared_vs_unique(pm_pred, kalshi_pred)

    # Figure 12: Platform agreement
    if len(prices_df) > 0:
        fig12_platform_agreement(prices_df)
    else:
        log("  Skipping Figure 12 (no price data)")

    # Figure 13: Specification curve
    if len(spec_results) > 0:
        fig13_specification_curve(spec_results)
    else:
        log("  Skipping Figure 13 (no spec results)")

    log("\n" + "=" * 70)
    log("COMPLETE")
    log(f"Figures saved to: {FIGURES_DIR}")
    log("=" * 70)


if __name__ == '__main__':
    main()
