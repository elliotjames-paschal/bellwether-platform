#!/usr/bin/env python3
"""
Generate Figures for "Researcher Degrees of Freedom in Prediction Market Evaluation"
Hall & Paschal — Version 2 (Revised per feedback)

All figures follow consistent style:
- White background, no top/right spines
- Serif font (Times New Roman / DejaVu Serif)
- Minimal gridlines (light gray horizontal only if needed)
- 300 DPI PNG + PDF output
- Figure size: 8×5 inches unless noted
"""

import json
import sys
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from itertools import product

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from scipy import stats
from scipy.signal import savgol_filter
from scipy.ndimage import gaussian_filter1d

warnings.filterwarnings('ignore')

# Paths
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR

OUTPUT_DIR = BASE_DIR / "output" / "dof_figures_v2"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Data files
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
PM_PRED_FILE = DATA_DIR / "polymarket_prediction_accuracy_all_political.csv"
KALSHI_PRED_FILE = DATA_DIR / "kalshi_prediction_accuracy_all_political.csv"
PM_PRICES_FILE = DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json"
PM_PRICES_V3_FILE = DATA_DIR / "polymarket_all_political_prices_CORRECTED_v3.json"
KALSHI_PRICES_FILE = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"

CUTOFF_DATE = pd.Timestamp('2026-02-10', tz='UTC')

# === STYLE SETTINGS ===
COLORS = {
    'conservative': '#4A7FB5',
    'resolution': '#D4726A',
    'polymarket': '#4A7FB5',
    'kalshi': '#5DAA68',
    'gray': '#888888',
    'light_gray': '#CCCCCC',
    'electoral': '#2E4057',
}

plt.rcParams.update({
    'font.family': 'serif',
    'font.serif': ['Times New Roman', 'DejaVu Serif', 'Georgia'],
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 11,
    'axes.labelweight': 'normal',
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.edgecolor': '#888888',
    'axes.grid': False,
    'figure.facecolor': 'white',
    'figure.figsize': (8, 5),
    'figure.dpi': 150,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.facecolor': 'white',
    'legend.frameon': False,
    'legend.fontsize': 10,
})

CAPTIONS = {}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def save_fig(fig, name, caption):
    fig.savefig(OUTPUT_DIR / f"{name}.pdf", format='pdf')
    fig.savefig(OUTPUT_DIR / f"{name}.png", format='png')
    plt.close(fig)
    CAPTIONS[name] = caption
    log(f"  Saved {name}")


def load_all_data():
    """Load all required data."""
    log("Loading data...")
    data = {}

    # Master CSV
    master = pd.read_csv(MASTER_CSV, low_memory=False)
    master['close_dt'] = pd.to_datetime(master['trading_close_time'], format='mixed', utc=True, errors='coerce')
    master = master[master['close_dt'] < CUTOFF_DATE]
    master = master[master['resolution_outcome'].notna()]
    data['master'] = master
    log(f"  Master: {len(master):,} resolved markets")

    # Prediction files
    pm_pred = pd.read_csv(PM_PRED_FILE, dtype={'token_id': str, 'market_id': str}, low_memory=False)
    kalshi_pred = pd.read_csv(KALSHI_PRED_FILE, dtype={'ticker': str}, low_memory=False)

    pm_pred['ref_dt'] = pd.to_datetime(pm_pred['reference_datetime'], format='mixed', utc=True, errors='coerce')
    kalshi_pred['ref_dt'] = pd.to_datetime(kalshi_pred['reference_datetime'], format='mixed', utc=True, errors='coerce')

    pm_pred = pm_pred[pm_pred['ref_dt'] < CUTOFF_DATE]
    kalshi_pred = kalshi_pred[kalshi_pred['ref_dt'] < CUTOFF_DATE]

    data['pm_pred'] = pm_pred
    data['kalshi_pred'] = kalshi_pred
    log(f"  PM pred: {len(pm_pred):,}, Kalshi pred: {len(kalshi_pred):,}")

    # Unique markets
    pm_markets = pm_pred[pm_pred['days_before_event'] == 1]['market_id'].nunique()
    k_markets = kalshi_pred[kalshi_pred['days_before_event'] == 1]['ticker'].nunique()
    log(f"  Unique markets at 1d: PM={pm_markets:,}, K={k_markets:,}")

    return data


# ============================================================================
# FIGURE 1: Calibration Curves
# ============================================================================

def fig01_calibration(data):
    """Calibration under two truncation regimes with proper separation."""
    log("Figure 1: Calibration curves (revised)")

    # Get prices at different truncation points
    # For "conservative" use 7 days (well before outcome known for most)
    # For "resolution" use 0 days (at close)

    pm_7d = data['pm_pred'][data['pm_pred']['days_before_event'] == 7].copy()
    pm_0d = data['pm_pred'][data['pm_pred']['days_before_event'] == 0].copy()
    k_7d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 7].copy()
    k_0d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 0].copy()

    conservative = pd.concat([
        pm_7d[['prediction_price', 'actual_outcome']],
        k_7d[['prediction_price', 'actual_outcome']]
    ]).dropna()
    conservative.columns = ['price', 'outcome']

    resolution = pd.concat([
        pm_0d[['prediction_price', 'actual_outcome']],
        k_0d[['prediction_price', 'actual_outcome']]
    ]).dropna()
    resolution.columns = ['price', 'outcome']

    def compute_calibration(df, n_bins=100):
        df = df.sort_values('price').reset_index(drop=True)
        n_per_bin = max(1, len(df) // n_bins)
        df['bin'] = df.index // n_per_bin
        df.loc[df['bin'] >= n_bins, 'bin'] = n_bins - 1
        bins = df.groupby('bin').agg({'price': 'mean', 'outcome': 'mean'}).reset_index()
        return bins['price'].values, bins['outcome'].values

    fig, ax = plt.subplots(figsize=(8, 5))

    # Mirrored histograms: conservative above x-axis, resolution below
    ax2 = ax.twinx()

    # Conservative histogram (above)
    cons_hist, cons_edges = np.histogram(conservative['price'], bins=50, range=(0, 1))
    cons_hist_norm = cons_hist / cons_hist.max() * 0.15
    ax2.bar(cons_edges[:-1], cons_hist_norm, width=0.02, alpha=0.3,
           color=COLORS['conservative'], align='edge', bottom=0)

    # Resolution histogram (below - mirrored)
    res_hist, res_edges = np.histogram(resolution['price'], bins=50, range=(0, 1))
    res_hist_norm = res_hist / res_hist.max() * 0.15
    ax2.bar(res_edges[:-1], -res_hist_norm, width=0.02, alpha=0.3,
           color=COLORS['resolution'], align='edge', bottom=0)

    ax2.set_ylim(-0.2, 1.1)
    ax2.set_yticks([])
    ax2.spines['right'].set_visible(False)
    ax2.spines['top'].set_visible(False)

    # Perfect calibration line
    ax.plot([0, 1], [0, 1], '--', color=COLORS['gray'], linewidth=1.5, zorder=5)

    # Calibration curves with smoothing
    if len(conservative) > 100:
        cons_x, cons_y = compute_calibration(conservative)
        # Apply light smoothing
        if len(cons_y) > 5:
            cons_y_smooth = savgol_filter(cons_y, min(11, len(cons_y)//2*2+1), 3)
        else:
            cons_y_smooth = cons_y
        cons_mae = np.mean(np.abs(cons_x - cons_y))
        ax.plot(cons_x, cons_y_smooth, color=COLORS['conservative'], linewidth=2.5,
               label=f'Conservative (7d before, MAE={cons_mae:.3f})', zorder=10)

    if len(resolution) > 100:
        res_x, res_y = compute_calibration(resolution)
        if len(res_y) > 5:
            res_y_smooth = savgol_filter(res_y, min(11, len(res_y)//2*2+1), 3)
        else:
            res_y_smooth = res_y
        res_mae = np.mean(np.abs(res_x - res_y))
        ax.plot(res_x, res_y_smooth, color=COLORS['resolution'], linewidth=2.5,
               label=f'Resolution (at close, MAE={res_mae:.3f})', zorder=10)

    ax.set_xlabel('Predicted Probability')
    ax.set_ylabel('Actual Outcome Rate')
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect('equal')
    ax.legend(loc='upper left', framealpha=0.9)

    caption = (f"Calibration curves for conservative truncation (7 days before close) versus "
               f"resolution prices (at close) across {len(conservative)+len(resolution):,} predictions. "
               f"Histograms show price distributions (conservative above axis, resolution below). "
               f"Resolution prices show spikes at 0 and 1 from post-outcome convergence.")

    save_fig(fig, 'fig01_calibration', caption)


# ============================================================================
# FIGURE 2: Brier Score KDE
# ============================================================================

def fig02_brier_kde(data):
    """Brier score distributions with proper styling."""
    log("Figure 2: Brier KDE (revised)")

    pm_7d = data['pm_pred'][data['pm_pred']['days_before_event'] == 7]
    pm_0d = data['pm_pred'][data['pm_pred']['days_before_event'] == 0]
    k_7d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 7]
    k_0d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 0]

    conservative_brier = pd.concat([pm_7d['brier_score'], k_7d['brier_score']]).dropna()
    resolution_brier = pd.concat([pm_0d['brier_score'], k_0d['brier_score']]).dropna()

    fig, ax = plt.subplots(figsize=(8, 5))

    # KDE with thick lines, no fill
    from scipy.stats import gaussian_kde

    x = np.linspace(0, 0.3, 200)  # Zoomed to 0-0.3

    if len(conservative_brier) > 10:
        cons_kde = gaussian_kde(conservative_brier.clip(0, 0.3))
        cons_mean = conservative_brier.mean()
        cons_median = conservative_brier.median()
        cons_p90 = conservative_brier.quantile(0.9)
        ax.plot(x, cons_kde(x), color=COLORS['conservative'], linewidth=2.5,
               label=f'Conservative (μ={cons_mean:.4f})')
        ax.axvline(cons_mean, color=COLORS['conservative'], linestyle='--', linewidth=1.5, alpha=0.7)

    if len(resolution_brier) > 10:
        res_kde = gaussian_kde(resolution_brier.clip(0, 0.3))
        res_mean = resolution_brier.mean()
        res_median = resolution_brier.median()
        res_p90 = resolution_brier.quantile(0.9)
        ax.plot(x, res_kde(x), color=COLORS['resolution'], linewidth=2.5,
               label=f'Resolution (μ={res_mean:.4f})')
        ax.axvline(res_mean, color=COLORS['resolution'], linestyle='--', linewidth=1.5, alpha=0.7)

    # Shade between means
    if len(conservative_brier) > 10 and len(resolution_brier) > 10:
        ax.axvspan(res_mean, cons_mean, alpha=0.1, color=COLORS['gray'])
        delta = cons_mean - res_mean
        mid = (cons_mean + res_mean) / 2
        ax.annotate(f'Δμ = {delta:.4f}', xy=(mid, ax.get_ylim()[1]*0.8),
                   ha='center', fontsize=10, color=COLORS['gray'])

    ax.set_xlabel('Brier Score')
    ax.set_ylabel('Density')
    ax.set_xlim(0, 0.3)
    ax.legend(loc='upper right')

    # Stats box
    stats_text = (f"Conservative: med={cons_median:.4f}, p90={cons_p90:.4f}\n"
                  f"Resolution: med={res_median:.4f}, p90={res_p90:.4f}")
    ax.text(0.98, 0.65, stats_text, transform=ax.transAxes, fontsize=9,
           va='top', ha='right', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8, edgecolor='none'))

    caption = (f"Distribution of Brier scores under conservative (7d before) vs resolution truncation. "
               f"The leftward shift under resolution timing reflects mechanical convergence. "
               f"X-axis zoomed to 0–0.3 where most density lies.")

    save_fig(fig, 'fig02_brier_kde', caption)


# ============================================================================
# FIGURE 3: Platform Rankings by Truncation
# ============================================================================

def fig03_platform_rankings(data):
    """Platform Brier scores under different truncation regimes."""
    log("Figure 3: Platform ranking shifts (revised)")

    truncations = [
        ('Conservative\n(7d)', 7),
        ('Moderate\n(3d)', 3),
        ('Aggressive\n(1d)', 1),
        ('Resolution\n(0d)', 0)
    ]

    pm_scores, pm_cis = [], []
    k_scores, k_cis = [], []

    for label, days in truncations:
        pm_data = data['pm_pred'][data['pm_pred']['days_before_event'] == days]['brier_score'].dropna()
        k_data = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == days]['brier_score'].dropna()

        pm_scores.append(pm_data.mean() if len(pm_data) > 0 else np.nan)
        k_scores.append(k_data.mean() if len(k_data) > 0 else np.nan)

        # Bootstrap CIs
        if len(pm_data) > 10:
            boots = [np.random.choice(pm_data, len(pm_data), replace=True).mean() for _ in range(1000)]
            pm_cis.append((np.percentile(boots, 2.5), np.percentile(boots, 97.5)))
        else:
            pm_cis.append((np.nan, np.nan))

        if len(k_data) > 10:
            boots = [np.random.choice(k_data, len(k_data), replace=True).mean() for _ in range(1000)]
            k_cis.append((np.percentile(boots, 2.5), np.percentile(boots, 97.5)))
        else:
            k_cis.append((np.nan, np.nan))

    fig, ax = plt.subplots(figsize=(8, 5))

    x = np.arange(len(truncations))
    labels = [t[0] for t in truncations]

    # Plot with CI bands
    ax.plot(x, pm_scores, 'o-', color=COLORS['polymarket'], linewidth=2, markersize=8, label='Polymarket')
    ax.fill_between(x, [c[0] for c in pm_cis], [c[1] for c in pm_cis], color=COLORS['polymarket'], alpha=0.2)

    ax.plot(x, k_scores, 's-', color=COLORS['kalshi'], linewidth=2, markersize=8, label='Kalshi')
    ax.fill_between(x, [c[0] for c in k_cis], [c[1] for c in k_cis], color=COLORS['kalshi'], alpha=0.2)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_xlabel('Truncation Regime')
    ax.set_ylabel('Mean Brier Score')
    ax.legend(loc='upper right')

    # Light horizontal grid
    ax.yaxis.grid(True, linestyle='-', alpha=0.3, color=COLORS['light_gray'])
    ax.set_axisbelow(True)

    # Annotate gap at widest point
    max_gap_idx = np.argmax(np.abs(np.array(pm_scores) - np.array(k_scores)))
    gap = pm_scores[max_gap_idx] - k_scores[max_gap_idx]
    mid_y = (pm_scores[max_gap_idx] + k_scores[max_gap_idx]) / 2
    ax.annotate(f'Δ = {abs(gap):.4f}', xy=(max_gap_idx + 0.1, mid_y), fontsize=9, color=COLORS['gray'])

    # Note about y-axis
    ax.text(0.02, 0.02, 'Note: y-axis does not start at zero', transform=ax.transAxes,
           fontsize=8, color=COLORS['gray'])

    caption = (f"Aggregate Brier score by platform under four truncation regimes. "
               f"Relative accuracy shifts depending on when prices are sampled. "
               f"Shaded bands show 95% bootstrap confidence intervals.")

    save_fig(fig, 'fig03_platform_ranking_shifts', caption)


# ============================================================================
# FIGURE 4: Convergence with Cliff
# ============================================================================

def fig04_convergence(data):
    """Convergence curve with post-outcome cliff."""
    log("Figure 4: Convergence cliff (revised)")

    # Use balanced panel: only markets with data at all horizons
    horizons = [60, 30, 14, 7, 3, 1, 0]

    pm = data['pm_pred'].copy()
    k = data['kalshi_pred'].copy()

    # Find markets present at all horizons
    pm_at_all = None
    for h in horizons:
        ids = set(pm[pm['days_before_event'] == h]['market_id'].unique())
        pm_at_all = ids if pm_at_all is None else pm_at_all & ids

    k_at_all = None
    for h in horizons:
        ids = set(k[k['days_before_event'] == h]['ticker'].unique())
        k_at_all = ids if k_at_all is None else k_at_all & ids

    log(f"    Balanced panel: PM={len(pm_at_all)}, K={len(k_at_all)}")

    # Compute mean Brier at each horizon for balanced panel
    brier_by_horizon = []
    for h in horizons:
        pm_h = pm[(pm['days_before_event'] == h) & (pm['market_id'].isin(pm_at_all))]['brier_score']
        k_h = k[(k['days_before_event'] == h) & (k['ticker'].isin(k_at_all))]['brier_score']
        combined = pd.concat([pm_h, k_h]).dropna()
        brier_by_horizon.append(combined.mean() if len(combined) > 0 else np.nan)

    fig, ax = plt.subplots(figsize=(8, 5))

    # Pre-event (days > 0)
    pre_idx = [i for i, h in enumerate(horizons) if h > 0]
    post_idx = [i for i, h in enumerate(horizons) if h == 0]

    pre_x = [horizons[i] for i in pre_idx]
    pre_y = [brier_by_horizon[i] for i in pre_idx]

    ax.plot(pre_x, pre_y, 'o-', color=COLORS['conservative'], linewidth=2, markersize=8, label='Pre-outcome')

    # Post-event
    if post_idx:
        post_x = [horizons[i] for i in post_idx]
        post_y = [brier_by_horizon[i] for i in post_idx]
        ax.plot(post_x, post_y, 's', color=COLORS['resolution'], markersize=12, label='At resolution', zorder=10)

    # Shade post-outcome region
    ax.axvspan(-2, 0.5, color=COLORS['resolution'], alpha=0.1)
    ax.axvline(0, color=COLORS['gray'], linestyle='--', linewidth=1)
    ax.text(0.2, ax.get_ylim()[0] + 0.002, 'Event', fontsize=9, color=COLORS['gray'])

    ax.invert_xaxis()
    ax.set_xlabel('Days Before Event')
    ax.set_ylabel('Mean Brier Score')
    ax.legend(loc='upper left')

    # Light grid
    ax.yaxis.grid(True, linestyle='-', alpha=0.3, color=COLORS['light_gray'])
    ax.set_axisbelow(True)

    caption = (f"Brier score convergence using balanced panel ({len(pm_at_all)+len(k_at_all):,} markets "
               f"with data at all horizons). Pre-outcome improvement followed by sharp cliff at resolution. "
               f"Shaded region indicates post-outcome trading.")

    save_fig(fig, 'fig04_convergence_cliff', caption)


# ============================================================================
# FIGURE 5: Price Divergence Scatter (Fixed)
# ============================================================================

def fig05_price_divergence_scatter(data):
    """Compare Brier at different smoothing windows - properly computed."""
    log("Figure 5: Price smoothing comparison (revised)")

    # Compare 1-day spot to 7-day (longer averaging window)
    # This shows how smoothing affects Brier, not VWAP vs spot directly

    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1].copy()
    pm_7d = data['pm_pred'][data['pm_pred']['days_before_event'] == 7].copy()

    merged = pm_1d.merge(pm_7d[['market_id', 'brier_score']], on='market_id', suffixes=('_1d', '_7d'))

    # Get volume
    volume_map = dict(zip(data['master']['market_id'].astype(str), data['master']['volume_usd']))
    merged['volume'] = merged['market_id'].astype(str).map(volume_map).fillna(0)

    def tier(v):
        if v >= 100000: return 'High (≥$100K)'
        elif v >= 10000: return 'Medium ($10K–$100K)'
        else: return 'Low (<$10K)'

    merged['tier'] = merged['volume'].apply(tier)

    fig, axes = plt.subplots(1, 3, figsize=(12, 4), sharey=True, sharex=True)

    tier_order = ['Low (<$10K)', 'Medium ($10K–$100K)', 'High (≥$100K)']
    tier_colors = {
        'Low (<$10K)': COLORS['resolution'],
        'Medium ($10K–$100K)': '#FFB347',
        'High (≥$100K)': COLORS['conservative']
    }

    for i, tier_name in enumerate(tier_order):
        ax = axes[i]
        subset = merged[merged['tier'] == tier_name]

        if len(subset) > 0:
            ax.scatter(subset['brier_score_7d'], subset['brier_score_1d'],
                      alpha=0.3, s=8, color=tier_colors[tier_name])

        ax.plot([0, 0.5], [0, 0.5], '--', color=COLORS['gray'], linewidth=1)
        ax.set_xlim(0, 0.5)
        ax.set_ylim(0, 0.5)
        ax.set_aspect('equal')
        ax.set_title(f'{tier_name}\n(n={len(subset):,})', fontsize=10)
        ax.set_xlabel('Brier (7-day price)')
        if i == 0:
            ax.set_ylabel('Brier (1-day price)')

    plt.tight_layout()

    caption = (f"Brier scores using 7-day vs 1-day prices by volume tier. "
               f"High-volume markets (right) cluster on diagonal; low-volume markets show more divergence. "
               f"Divergence reflects both information arrival and price construction noise.")

    save_fig(fig, 'fig05_price_divergence_scatter', caption)


# ============================================================================
# FIGURE 6: Divergence by Volume Decile
# ============================================================================

def fig06_divergence_by_depth(data):
    """Price divergence by volume decile using proper measure."""
    log("Figure 6: Divergence by depth (revised)")

    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1].copy()
    pm_7d = data['pm_pred'][data['pm_pred']['days_before_event'] == 7].copy()

    merged = pm_1d.merge(pm_7d[['market_id', 'prediction_price']], on='market_id', suffixes=('_1d', '_7d'))
    merged['price_div'] = np.abs(merged['prediction_price_1d'] - merged['prediction_price_7d'])

    volume_map = dict(zip(data['master']['market_id'].astype(str), data['master']['volume_usd']))
    merged['volume'] = merged['market_id'].astype(str).map(volume_map).fillna(0)
    merged = merged[merged['volume'] > 0]

    if len(merged) < 100:
        log("    Insufficient data")
        return

    merged['decile'] = pd.qcut(merged['volume'], 10, labels=False, duplicates='drop')

    fig, ax = plt.subplots(figsize=(8, 5))

    # Violin plots
    decile_data = [merged[merged['decile'] == d]['price_div'].values for d in range(10)]
    decile_data = [d[~np.isnan(d)] for d in decile_data]

    parts = ax.violinplot([d for d in decile_data if len(d) > 0],
                          positions=[i+1 for i, d in enumerate(decile_data) if len(d) > 0],
                          showmeans=True, showmedians=True)

    for pc in parts['bodies']:
        pc.set_facecolor(COLORS['conservative'])
        pc.set_alpha(0.6)

    # Means as connected line
    means = [np.mean(d) if len(d) > 0 else np.nan for d in decile_data]
    valid_means = [(i+1, m) for i, m in enumerate(means) if not np.isnan(m)]
    if valid_means:
        ax.plot([v[0] for v in valid_means], [v[1] for v in valid_means],
               'o-', color=COLORS['resolution'], linewidth=2, markersize=6)

    ax.axhline(0.05, color=COLORS['gray'], linestyle='--', linewidth=1, label='5¢ threshold')
    ax.set_xlabel('Volume Decile (1=lowest, 10=highest)')
    ax.set_ylabel('|Price 1-day − Price 7-day|')
    ax.legend(loc='upper right')

    # Light grid
    ax.yaxis.grid(True, linestyle='-', alpha=0.3, color=COLORS['light_gray'])
    ax.set_axisbelow(True)

    caption = (f"Price divergence between 1-day and 7-day measurements by volume decile. "
               f"Violin plots show distributions; connected points show means. "
               f"Thin markets (left) show larger divergence but the relationship is weak.")

    save_fig(fig, 'fig06_price_divergence_by_depth', caption)


# ============================================================================
# FIGURE 7: Cost-to-Move / Volume CDF
# ============================================================================

def fig07_volume_cdf(data):
    """CDF of market volume (proxy for manipulation cost)."""
    log("Figure 7: Volume CDF (revised)")

    volume = data['master']['volume_usd'].dropna()
    volume = volume[volume > 0]

    fig, ax = plt.subplots(figsize=(8, 5))

    sorted_vol = np.sort(volume)
    cdf = np.arange(1, len(sorted_vol) + 1) / len(sorted_vol)

    # Fill below curve
    ax.fill_between(sorted_vol, 0, cdf, color=COLORS['conservative'], alpha=0.1)
    ax.plot(sorted_vol, cdf, color=COLORS['conservative'], linewidth=2.5)

    ax.set_xscale('log')
    ax.set_xlabel('Market Volume (USD)')
    ax.set_ylabel('Cumulative Fraction of Markets')

    # Threshold lines with dots on curve
    for thresh, label in [(10000, '$10K'), (100000, '$100K')]:
        frac = np.mean(sorted_vol <= thresh)
        ax.axvline(thresh, color=COLORS['gray'], linestyle='--', linewidth=1)
        ax.plot(thresh, frac, 'o', color=COLORS['resolution'], markersize=8, zorder=10)
        ax.text(thresh * 1.2, frac, label, fontsize=9, va='center')

    # Tier percentages in lower right
    frac_fragile = np.mean(sorted_vol < 10000)
    frac_caution = np.mean((sorted_vol >= 10000) & (sorted_vol < 100000))
    frac_reportable = np.mean(sorted_vol >= 100000)

    stats_text = (f"Fragile (<$10K): {frac_fragile:.1%}\n"
                  f"Caution ($10K–$100K): {frac_caution:.1%}\n"
                  f"Reportable (≥$100K): {frac_reportable:.1%}")
    ax.text(0.98, 0.25, stats_text, transform=ax.transAxes, fontsize=9,
           va='bottom', ha='right', bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='none'))

    caption = (f"Cumulative distribution of market volume across {len(volume):,} markets. "
               f"Only {frac_reportable:.1%} exceed the $100K 'Reportable' threshold. "
               f"Most markets are thin enough to be moved by a motivated individual.")

    save_fig(fig, 'fig07_volume_cdf', caption)


# ============================================================================
# FIGURE 8: Brier by Volume Threshold
# ============================================================================

def fig08_brier_by_threshold(data):
    """Aggregate Brier as function of volume threshold."""
    log("Figure 8: Brier by threshold (revised)")

    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1].copy()
    k_1d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 1].copy()

    # Use only unique markets (dedupe)
    pm_1d = pm_1d.drop_duplicates(subset='market_id')
    k_1d = k_1d.drop_duplicates(subset='ticker')

    volume_map = dict(zip(data['master']['market_id'].astype(str), data['master']['volume_usd']))
    pm_1d['volume'] = pm_1d['market_id'].astype(str).map(volume_map).fillna(0)
    k_1d['volume'] = k_1d['ticker'].astype(str).map(volume_map).fillna(0)

    thresholds = [0, 1000, 10000, 100000]

    all_brier, all_n, all_cis = [], [], []
    electoral_brier, electoral_n = [], []

    for thresh in thresholds:
        combined = pd.concat([
            pm_1d[pm_1d['volume'] >= thresh]['brier_score'],
            k_1d[k_1d['volume'] >= thresh]['brier_score']
        ]).dropna()

        all_brier.append(combined.mean() if len(combined) > 0 else np.nan)
        all_n.append(len(combined))

        # Bootstrap CI
        if len(combined) > 10:
            boots = [np.random.choice(combined, len(combined), replace=True).mean() for _ in range(500)]
            all_cis.append((np.percentile(boots, 2.5), np.percentile(boots, 97.5)))
        else:
            all_cis.append((np.nan, np.nan))

        # Electoral
        pm_elec = pm_1d[(pm_1d['volume'] >= thresh) & (pm_1d['category'].str.contains('ELECTORAL', na=False))]
        k_elec = k_1d[(k_1d['volume'] >= thresh) & (k_1d['category'].str.contains('ELECTORAL', na=False))]
        elec_combined = pd.concat([pm_elec['brier_score'], k_elec['brier_score']]).dropna()
        electoral_brier.append(elec_combined.mean() if len(elec_combined) > 0 else np.nan)
        electoral_n.append(len(elec_combined))

    fig, ax = plt.subplots(figsize=(8, 5))

    x = np.arange(len(thresholds))

    # All political with CI band
    ax.plot(x, all_brier, 'o-', color=COLORS['gray'], linewidth=2, markersize=8, label='All political')
    ax.fill_between(x, [c[0] for c in all_cis], [c[1] for c in all_cis], color=COLORS['gray'], alpha=0.2)

    # Electoral
    ax.plot(x, electoral_brier, 's-', color=COLORS['conservative'], linewidth=2, markersize=8, label='Electoral only')

    # Annotate N
    for i, n in enumerate(all_n):
        ax.annotate(f'n={n:,}', (x[i], all_brier[i]), xytext=(0, 10),
                   textcoords='offset points', ha='center', fontsize=8, color=COLORS['gray'])

    ax.set_xticks(x)
    ax.set_xticklabels(['$0', '$1K', '$10K', '$100K'])
    ax.set_xlabel('Minimum Volume Threshold')
    ax.set_ylabel('Mean Brier Score')
    ax.legend(loc='upper right')

    # Light grid
    ax.yaxis.grid(True, linestyle='-', alpha=0.3, color=COLORS['light_gray'])
    ax.set_axisbelow(True)

    caption = (f"Aggregate Brier score by volume threshold. Sample restricted to unique markets. "
               f"Higher thresholds reduce sample size but may improve signal quality.")

    save_fig(fig, 'fig08_brier_by_threshold', caption)


# ============================================================================
# FIGURE 9: Accuracy by Category
# ============================================================================

def fig09_accuracy_by_category(data):
    """Brier by political category with clean labels."""
    log("Figure 9: Accuracy by category (revised)")

    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1].copy()
    k_1d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 1].copy()

    combined = pd.concat([pm_1d[['category', 'brier_score']], k_1d[['category', 'brier_score']]])

    by_cat = combined.groupby('category')['brier_score'].agg(['mean', 'count', 'std']).reset_index()
    by_cat = by_cat[by_cat['count'] >= 50]
    by_cat = by_cat.sort_values('mean')

    # Clean names
    name_map = {
        'ELECTORAL': 'Electoral',
        'LEGISLATIVE': 'Legislative',
        'EXECUTIVE': 'Executive',
        'INTERNATIONAL': 'International',
        'PARTISAN_CONTROL': 'Partisan Control',
        'REFERENDUMS': 'Referendums',
        'LEGAL_JUDICIAL': 'Legal/Judicial',
        'POLLING_APPROVAL': 'Polling & Approval',
        'POLITICAL_SPEECH': 'Political Speech',
        'POLICY': 'Policy',
        'CANDIDACY': 'Candidacy',
    }

    def clean_name(x):
        if pd.isna(x):
            return 'Other'
        x = str(x)
        # Remove number prefix
        if '. ' in x:
            x = x.split('. ', 1)[1]
        return name_map.get(x.upper(), x.title()[:20])

    by_cat['clean_name'] = by_cat['category'].apply(clean_name)

    overall_mean = combined['brier_score'].mean()

    fig, ax = plt.subplots(figsize=(8, 6))

    y_pos = np.arange(len(by_cat))

    # Color by relation to mean
    colors = []
    for _, row in by_cat.iterrows():
        if row['mean'] < overall_mean - row['std']/np.sqrt(row['count']):
            colors.append(COLORS['conservative'])  # Better than average
        elif row['mean'] > overall_mean + row['std']/np.sqrt(row['count']):
            colors.append(COLORS['resolution'])  # Worse than average
        else:
            colors.append(COLORS['gray'])

    # Bootstrap CIs
    cis = []
    for _, row in by_cat.iterrows():
        se = row['std'] / np.sqrt(row['count'])
        cis.append(1.96 * se)

    ax.barh(y_pos, by_cat['mean'], xerr=cis, color=colors, alpha=0.8, capsize=3)

    # Overall mean line
    ax.axvline(overall_mean, color=COLORS['gray'], linestyle='--', linewidth=2,
              label=f'Overall: {overall_mean:.3f}')

    # Labels
    for i, (_, row) in enumerate(by_cat.iterrows()):
        ax.text(row['mean'] + cis[i] + 0.005, i, f"n={int(row['count']):,}", va='center', fontsize=8)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(by_cat['clean_name'], fontsize=10)
    ax.set_xlabel('Mean Brier Score')
    ax.legend(loc='lower right')

    caption = (f"Mean Brier score by political category. Blue = better than average, "
               f"coral = worse than average. Error bars show 95% CIs. "
               f"Wide variance shows aggregate accuracy depends on category composition.")

    save_fig(fig, 'fig09_accuracy_by_category', caption)


# ============================================================================
# FIGURE 11: Platform Comparison (Redesigned)
# ============================================================================

def fig11_platform_comparison(data):
    """Platform comparison showing gap varies by truncation."""
    log("Figure 11: Platform comparison (revised)")

    truncations = [7, 3, 1, 0]
    labels = ['7d', '3d', '1d', 'Res']

    gaps = []
    pm_scores = []
    k_scores = []

    for days in truncations:
        pm_data = data['pm_pred'][data['pm_pred']['days_before_event'] == days]['brier_score'].dropna()
        k_data = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == days]['brier_score'].dropna()

        pm_mean = pm_data.mean() if len(pm_data) > 0 else np.nan
        k_mean = k_data.mean() if len(k_data) > 0 else np.nan

        pm_scores.append(pm_mean)
        k_scores.append(k_mean)
        gaps.append(pm_mean - k_mean)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    x = np.arange(len(truncations))

    # Left panel: Platform scores
    ax1.bar(x - 0.2, pm_scores, 0.35, color=COLORS['polymarket'], label='Polymarket', alpha=0.8)
    ax1.bar(x + 0.2, k_scores, 0.35, color=COLORS['kalshi'], label='Kalshi', alpha=0.8)

    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.set_xlabel('Truncation')
    ax1.set_ylabel('Mean Brier Score')
    ax1.legend(loc='upper right')
    ax1.yaxis.grid(True, linestyle='-', alpha=0.3, color=COLORS['light_gray'])
    ax1.set_axisbelow(True)
    ax1.set_title('Platform Brier by Truncation')

    # Right panel: Gap
    colors = [COLORS['polymarket'] if g > 0 else COLORS['kalshi'] for g in gaps]
    ax2.bar(x, gaps, color=colors, alpha=0.8)
    ax2.axhline(0, color=COLORS['gray'], linewidth=1)

    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)
    ax2.set_xlabel('Truncation')
    ax2.set_ylabel('Brier Gap (PM − Kalshi)')
    ax2.set_title('Gap Varies by Truncation')

    # Annotate
    for i, g in enumerate(gaps):
        label = 'PM worse' if g > 0 else 'Kalshi worse'
        ax2.annotate(f'{g:+.4f}', (i, g), xytext=(0, 5 if g > 0 else -15),
                    textcoords='offset points', ha='center', fontsize=9)

    plt.tight_layout()

    caption = (f"Platform comparison under different truncations. Left: absolute Brier scores. "
               f"Right: Brier gap (positive = Polymarket worse). The gap reverses depending on truncation, "
               f"showing platform rankings are methodology-dependent.")

    save_fig(fig, 'fig11_platform_comparison', caption)


# ============================================================================
# FIGURE 13: Specification Curve (Fixed)
# ============================================================================

def fig13_specification_curve(data):
    """Specification curve with proper indicator panel - BRIER ONLY."""
    log("Figure 13: Specification curve (revised)")

    # Generate specifications (Brier only for clarity)
    truncations = ['7d', '3d', '1d', '0d']
    trunc_days = {'7d': 7, '3d': 3, '1d': 1, '0d': 0}

    # Simplified price types (since we don't have true VWAP)
    price_types = ['spot']  # Just use spot for now

    thresholds = [0, 1000, 10000, 100000]
    thresh_labels = ['$0', '$1K', '$10K', '$100K']

    pm = data['pm_pred'].copy()
    k = data['kalshi_pred'].copy()

    volume_map = dict(zip(data['master']['market_id'].astype(str), data['master']['volume_usd']))
    pm['volume'] = pm['market_id'].astype(str).map(volume_map).fillna(0)
    k['volume'] = k['ticker'].astype(str).map(volume_map).fillna(0)

    specs = []

    for trunc in truncations:
        days = trunc_days[trunc]
        pm_t = pm[pm['days_before_event'] == days]
        k_t = k[k['days_before_event'] == days]

        for thresh_idx, thresh in enumerate(thresholds):
            pm_f = pm_t[pm_t['volume'] >= thresh]['brier_score'].dropna()
            k_f = k_t[k_t['volume'] >= thresh]['brier_score'].dropna()
            combined = pd.concat([pm_f, k_f])

            if len(combined) > 10:
                specs.append({
                    'truncation': trunc,
                    'threshold': thresh_labels[thresh_idx],
                    'thresh_idx': thresh_idx,
                    'trunc_idx': truncations.index(trunc),
                    'brier': combined.mean(),
                    'n': len(combined)
                })

    spec_df = pd.DataFrame(specs)
    spec_df = spec_df.sort_values('brier').reset_index(drop=True)
    spec_df['rank'] = range(len(spec_df))

    # Save data
    spec_df.to_csv(OUTPUT_DIR / 'spec_curve_data.csv', index=False)

    # Create figure
    fig = plt.figure(figsize=(10, 8))
    gs = fig.add_gridspec(3, 1, height_ratios=[3, 0.8, 0.8], hspace=0.05)

    ax_curve = fig.add_subplot(gs[0])
    ax_trunc = fig.add_subplot(gs[1], sharex=ax_curve)
    ax_thresh = fig.add_subplot(gs[2], sharex=ax_curve)

    # Top panel: the curve
    ax_curve.scatter(spec_df['rank'], spec_df['brier'], s=50, color=COLORS['conservative'], alpha=0.8, zorder=10)

    # Connect with line
    ax_curve.plot(spec_df['rank'], spec_df['brier'], color=COLORS['conservative'], alpha=0.3, linewidth=1)

    ax_curve.set_ylabel('Aggregate Brier Score')
    ax_curve.set_xlim(-1, len(spec_df))
    plt.setp(ax_curve.get_xticklabels(), visible=False)

    # Light grid
    ax_curve.yaxis.grid(True, linestyle='-', alpha=0.3, color=COLORS['light_gray'])
    ax_curve.set_axisbelow(True)

    # Mark key specs
    # Resolution + $0 (worst practice)
    worst = spec_df[(spec_df['truncation'] == '0d') & (spec_df['threshold'] == '$0')]
    if len(worst) > 0:
        wr = worst.iloc[0]['rank']
        wb = worst.iloc[0]['brier']
        ax_curve.annotate('Resolution + $0\n(inflate accuracy)', (wr, wb),
                         xytext=(wr - 3, wb + 0.01),
                         arrowprops=dict(arrowstyle='->', color=COLORS['gray']),
                         fontsize=8, ha='right')

    # Conservative + $100K (strict)
    best = spec_df[(spec_df['truncation'] == '7d') & (spec_df['threshold'] == '$100K')]
    if len(best) > 0:
        br = best.iloc[0]['rank']
        bb = best.iloc[0]['brier']
        ax_curve.annotate('7d + $100K\n(strict)', (br, bb),
                         xytext=(br + 2, bb - 0.005),
                         arrowprops=dict(arrowstyle='->', color=COLORS['gray']),
                         fontsize=8)

    # Indicator panels
    trunc_colors = {'7d': '#1f77b4', '3d': '#2ca02c', '1d': '#ff7f0e', '0d': '#d62728'}
    thresh_colors = {'$0': '#f7f7f7', '$1K': '#cccccc', '$10K': '#969696', '$100K': '#525252'}

    # Truncation row
    for _, row in spec_df.iterrows():
        ax_trunc.scatter(row['rank'], 0, c=trunc_colors[row['truncation']], s=80, marker='s')
    ax_trunc.set_yticks([0])
    ax_trunc.set_yticklabels(['Truncation'], fontsize=10)
    ax_trunc.set_ylim(-0.5, 0.5)
    plt.setp(ax_trunc.get_xticklabels(), visible=False)

    # Legend for truncation
    trunc_handles = [mpatches.Patch(color=trunc_colors[t], label=t) for t in truncations]
    ax_trunc.legend(handles=trunc_handles, loc='center left', bbox_to_anchor=(1.01, 0.5),
                   fontsize=8, frameon=False)

    # Threshold row
    for _, row in spec_df.iterrows():
        ax_thresh.scatter(row['rank'], 0, c=thresh_colors[row['threshold']], s=80, marker='s',
                         edgecolors=COLORS['gray'], linewidths=0.5)
    ax_thresh.set_yticks([0])
    ax_thresh.set_yticklabels(['Threshold'], fontsize=10)
    ax_thresh.set_ylim(-0.5, 0.5)
    ax_thresh.set_xlabel('Specification (sorted by Brier score)')

    # Legend for threshold
    thresh_handles = [mpatches.Patch(color=thresh_colors[t], label=t, edgecolor=COLORS['gray'], linewidth=0.5)
                     for t in thresh_labels]
    ax_thresh.legend(handles=thresh_handles, loc='center left', bbox_to_anchor=(1.01, 0.5),
                    fontsize=8, frameon=False)

    # Remove spines from indicator panels
    for ax in [ax_trunc, ax_thresh]:
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.tick_params(left=False, bottom=False)

    plt.tight_layout()

    caption = (f"Specification curve across {len(spec_df)} specifications (Brier score only). "
               f"Each dot is one truncation × threshold combination. "
               f"Bottom panels show which choices produced each specification. "
               f"The spread quantifies researcher degrees of freedom in accuracy reporting.")

    save_fig(fig, 'fig13_specification_curve', caption)


# ============================================================================
# MAIN
# ============================================================================

def main():
    log("=" * 70)
    log("GENERATING FIGURES V2 (REVISED)")
    log("=" * 70)

    data = load_all_data()

    log("\nGenerating figures...")

    fig01_calibration(data)
    fig02_brier_kde(data)
    fig03_platform_rankings(data)
    fig04_convergence(data)
    fig05_price_divergence_scatter(data)
    fig06_divergence_by_depth(data)
    fig07_volume_cdf(data)
    fig08_brier_by_threshold(data)
    fig09_accuracy_by_category(data)
    fig11_platform_comparison(data)
    fig13_specification_curve(data)

    # Save captions
    with open(OUTPUT_DIR / 'captions.md', 'w') as f:
        f.write("# Figure Captions\n\n")
        for name, caption in CAPTIONS.items():
            f.write(f"## {name}\n{caption}\n\n")

    log(f"\nSaved to {OUTPUT_DIR}")
    log("=" * 70)


if __name__ == '__main__':
    main()
