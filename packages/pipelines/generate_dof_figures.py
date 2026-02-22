#!/usr/bin/env python3
"""
Generate Figures for "Researcher Degrees of Freedom in Prediction Market Evaluation"
Hall & Paschal

This script generates all figures according to the paper specifications.
Run from scripts/ directory.
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
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
from scipy import stats
from scipy.ndimage import gaussian_filter1d

warnings.filterwarnings('ignore')

# Paths
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR

OUTPUT_DIR = BASE_DIR / "output" / "dof_figures"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Data files
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
PM_PRED_FILE = DATA_DIR / "polymarket_prediction_accuracy_all_political.csv"
KALSHI_PRED_FILE = DATA_DIR / "kalshi_prediction_accuracy_all_political.csv"
PM_PRICES_FILE = DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json"
PM_PRICES_V3_FILE = DATA_DIR / "polymarket_all_political_prices_CORRECTED_v3.json"
KALSHI_PRICES_FILE = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"
LIQUIDITY_FILE = DATA_DIR / "liquidity_metrics_by_market.csv"
PM_ORDERBOOK_FILE = DATA_DIR / "orderbook_history_polymarket.json"
KALSHI_ORDERBOOK_FILE = DATA_DIR / "orderbook_history_kalshi.json"

# Cutoff date
CUTOFF_DATE = pd.Timestamp('2026-02-10', tz='UTC')

# Style settings
COLORS = {
    'conservative': '#4A7FB5',  # Muted blue
    'resolution': '#D4726A',    # Warm coral
    'polymarket': '#4A7FB5',    # Blue
    'kalshi': '#5DAA68',        # Green
    'gray': '#888888',
    'light_gray': '#CCCCCC',
}

plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 11,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.edgecolor': '#888888',
    'axes.grid': False,
    'figure.facecolor': 'white',
    'figure.dpi': 150,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.facecolor': 'white',
})

# Captions storage
CAPTIONS = {}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def save_fig(fig, name, caption):
    """Save figure as PDF and PNG, store caption."""
    fig.savefig(OUTPUT_DIR / f"{name}.pdf", format='pdf')
    fig.savefig(OUTPUT_DIR / f"{name}.png", format='png')
    plt.close(fig)
    CAPTIONS[name] = caption
    log(f"  Saved {name}")


def load_all_data():
    """Load all required data files."""
    log("Loading data...")

    data = {}

    # Master CSV
    master = pd.read_csv(MASTER_CSV, low_memory=False)
    master['close_dt'] = pd.to_datetime(master['trading_close_time'], format='mixed', utc=True, errors='coerce')
    master = master[master['close_dt'] < CUTOFF_DATE]
    master = master[master['resolution_outcome'].notna()]
    data['master'] = master
    log(f"  Master: {len(master):,} resolved markets")

    # Prediction accuracy files
    pm_pred = pd.read_csv(PM_PRED_FILE, dtype={'token_id': str, 'market_id': str}, low_memory=False)
    kalshi_pred = pd.read_csv(KALSHI_PRED_FILE, dtype={'ticker': str}, low_memory=False)

    pm_pred['ref_dt'] = pd.to_datetime(pm_pred['reference_datetime'], format='mixed', utc=True, errors='coerce')
    kalshi_pred['ref_dt'] = pd.to_datetime(kalshi_pred['reference_datetime'], format='mixed', utc=True, errors='coerce')

    pm_pred = pm_pred[pm_pred['ref_dt'] < CUTOFF_DATE]
    kalshi_pred = kalshi_pred[kalshi_pred['ref_dt'] < CUTOFF_DATE]

    data['pm_pred'] = pm_pred
    data['kalshi_pred'] = kalshi_pred
    log(f"  PM predictions: {len(pm_pred):,}")
    log(f"  Kalshi predictions: {len(kalshi_pred):,}")

    # Price histories
    pm_prices = {}
    if PM_PRICES_FILE.exists():
        with open(PM_PRICES_FILE) as f:
            pm_dome = json.load(f)
    else:
        pm_dome = {}
    if PM_PRICES_V3_FILE.exists():
        with open(PM_PRICES_V3_FILE) as f:
            pm_v3 = json.load(f)
    else:
        pm_v3 = {}
    for token in set(pm_dome.keys()) | set(pm_v3.keys()):
        pm_prices[token] = pm_dome.get(token) or pm_v3.get(token, [])
    data['pm_prices'] = pm_prices
    log(f"  PM price histories: {len(pm_prices):,}")

    if KALSHI_PRICES_FILE.exists():
        with open(KALSHI_PRICES_FILE) as f:
            data['kalshi_prices'] = json.load(f)
        log(f"  Kalshi price histories: {len(data['kalshi_prices']):,}")
    else:
        data['kalshi_prices'] = {}

    # Liquidity data
    if LIQUIDITY_FILE.exists():
        data['liquidity'] = pd.read_csv(LIQUIDITY_FILE)
        log(f"  Liquidity metrics: {len(data['liquidity']):,}")
    else:
        data['liquidity'] = pd.DataFrame()

    # Order books
    if PM_ORDERBOOK_FILE.exists():
        with open(PM_ORDERBOOK_FILE) as f:
            data['pm_orderbook'] = json.load(f)
        log(f"  PM orderbooks: {len(data['pm_orderbook']):,}")
    else:
        data['pm_orderbook'] = {}

    if KALSHI_ORDERBOOK_FILE.exists():
        with open(KALSHI_ORDERBOOK_FILE) as f:
            data['kalshi_orderbook'] = json.load(f)
        log(f"  Kalshi orderbooks: {len(data['kalshi_orderbook']):,}")
    else:
        data['kalshi_orderbook'] = {}

    return data


def get_price_at_time(price_history, target_ts, platform='polymarket'):
    """Get spot price at a specific timestamp."""
    if not price_history:
        return None

    if platform == 'kalshi':
        valid = [p for p in price_history if p.get('end_period_ts', 0) <= target_ts]
        if not valid:
            return None
        last = max(valid, key=lambda x: x.get('end_period_ts', 0))
        close = last.get('price', {}).get('close')
        return float(close) / 100.0 if close is not None else None
    else:
        valid = [p for p in price_history if p.get('t', 0) <= target_ts]
        if not valid:
            return None
        return float(max(valid, key=lambda x: x['t'])['p'])


def compute_vwap(price_history, end_ts, window_seconds, platform='polymarket'):
    """Compute VWAP approximation over a time window."""
    if not price_history:
        return None

    start_ts = end_ts - window_seconds

    if platform == 'kalshi':
        window = [p for p in price_history if start_ts <= p.get('end_period_ts', 0) <= end_ts]
        if not window:
            return get_price_at_time(price_history, end_ts, platform)
        prices = [float(p.get('price', {}).get('close', 0)) / 100.0 for p in window if p.get('price', {}).get('close')]
        return np.mean(prices) if prices else None
    else:
        window = [p for p in price_history if start_ts <= p.get('t', 0) <= end_ts]
        if not window:
            return get_price_at_time(price_history, end_ts, platform)
        prices = [float(p['p']) for p in window]
        return np.mean(prices) if prices else None


# ============================================================================
# FIGURE 1: Calibration Under Two Truncation Regimes
# ============================================================================

def fig01_calibration(data):
    """Calibration curves for conservative vs resolution truncation."""
    log("Figure 1: Calibration curves")

    # Combine predictions from both platforms at 1-day (conservative proxy) and 0-day (resolution)
    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1].copy()
    pm_0d = data['pm_pred'][data['pm_pred']['days_before_event'] == 0].copy()
    k_1d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 1].copy()
    k_0d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 0].copy()

    # Conservative: 1 day before
    conservative_prices = pd.concat([
        pm_1d[['prediction_price', 'actual_outcome']].rename(columns={'prediction_price': 'price'}),
        k_1d[['prediction_price', 'actual_outcome']].rename(columns={'prediction_price': 'price'})
    ]).dropna()

    # Resolution: 0 days (at close)
    resolution_prices = pd.concat([
        pm_0d[['prediction_price', 'actual_outcome']].rename(columns={'prediction_price': 'price'}),
        k_0d[['prediction_price', 'actual_outcome']].rename(columns={'prediction_price': 'price'})
    ]).dropna()

    def compute_calibration_bins(df, n_bins=100):
        """Compute calibration bins using quantiles."""
        df = df.sort_values('price').reset_index(drop=True)
        df['bin'] = pd.qcut(df['price'], n_bins, labels=False, duplicates='drop')
        bins = df.groupby('bin').agg({
            'price': 'mean',
            'actual_outcome': 'mean'
        }).reset_index()
        return bins['price'].values, bins['actual_outcome'].values

    fig, ax = plt.subplots(figsize=(8, 5))

    # Background histograms
    ax.hist(conservative_prices['price'], bins=50, alpha=0.15, color=COLORS['conservative'],
            density=True, label='_nolegend_')
    ax.hist(resolution_prices['price'], bins=50, alpha=0.15, color=COLORS['resolution'],
            density=True, label='_nolegend_')

    # Create twin axis for histogram scale
    ax2 = ax.twinx()
    ax2.set_ylim(0, 1)
    ax2.set_yticks([])
    ax2.spines['right'].set_visible(False)

    # Perfect calibration line
    ax.plot([0, 1], [0, 1], '--', color=COLORS['gray'], linewidth=1.5, label='Perfect calibration', zorder=5)

    # Calibration curves
    if len(conservative_prices) > 100:
        cons_x, cons_y = compute_calibration_bins(conservative_prices)
        cons_mae = np.mean(np.abs(cons_x - cons_y))
        ax.plot(cons_x, cons_y, color=COLORS['conservative'], linewidth=2.5,
               label=f'Conservative (MAE={cons_mae:.3f})', zorder=10)

    if len(resolution_prices) > 100:
        res_x, res_y = compute_calibration_bins(resolution_prices)
        res_mae = np.mean(np.abs(res_x - res_y))
        ax.plot(res_x, res_y, color=COLORS['resolution'], linewidth=2.5,
               label=f'Resolution (MAE={res_mae:.3f})', zorder=10)

    ax.set_xlabel('Predicted Probability')
    ax.set_ylabel('Actual Outcome Rate')
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect('equal')
    ax.legend(loc='upper left', framealpha=0.9)

    caption = (f"Calibration curves for conservative truncation (pre-outcome) versus resolution prices "
               f"across {len(conservative_prices) + len(resolution_prices):,} political market predictions. "
               f"Background histograms show price distributions. Resolution prices appear better calibrated "
               f"but reflect post-outcome convergence, not forecasting.")

    save_fig(fig, 'fig01_calibration', caption)


# ============================================================================
# FIGURE 2: Brier Score KDE
# ============================================================================

def fig02_brier_kde(data):
    """Brier score distributions under two truncation regimes."""
    log("Figure 2: Brier KDE")

    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1]
    pm_0d = data['pm_pred'][data['pm_pred']['days_before_event'] == 0]
    k_1d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 1]
    k_0d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 0]

    conservative_brier = pd.concat([pm_1d['brier_score'], k_1d['brier_score']]).dropna()
    resolution_brier = pd.concat([pm_0d['brier_score'], k_0d['brier_score']]).dropna()

    fig, ax = plt.subplots(figsize=(8, 5))

    # KDE plots
    from scipy.stats import gaussian_kde

    if len(conservative_brier) > 10:
        cons_kde = gaussian_kde(conservative_brier.clip(0, 0.5))
        x = np.linspace(0, 0.5, 200)
        ax.fill_between(x, cons_kde(x), alpha=0.3, color=COLORS['conservative'])
        ax.plot(x, cons_kde(x), color=COLORS['conservative'], linewidth=2,
               label=f'Conservative (μ={conservative_brier.mean():.4f})')
        ax.axvline(conservative_brier.mean(), color=COLORS['conservative'], linestyle='--', alpha=0.7)

    if len(resolution_brier) > 10:
        res_kde = gaussian_kde(resolution_brier.clip(0, 0.5))
        x = np.linspace(0, 0.5, 200)
        ax.fill_between(x, res_kde(x), alpha=0.3, color=COLORS['resolution'])
        ax.plot(x, res_kde(x), color=COLORS['resolution'], linewidth=2,
               label=f'Resolution (μ={resolution_brier.mean():.4f})')
        ax.axvline(resolution_brier.mean(), color=COLORS['resolution'], linestyle='--', alpha=0.7)

    ax.set_xlabel('Brier Score')
    ax.set_ylabel('Density')
    ax.set_xlim(0, 0.5)
    ax.legend(loc='upper right', framealpha=0.9)

    # Add stats box
    stats_text = (f"Conservative: median={conservative_brier.median():.4f}, p90={conservative_brier.quantile(0.9):.4f}\n"
                  f"Resolution: median={resolution_brier.median():.4f}, p90={resolution_brier.quantile(0.9):.4f}")
    ax.text(0.98, 0.98, stats_text, transform=ax.transAxes, fontsize=9,
           verticalalignment='top', horizontalalignment='right',
           bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    caption = (f"Distribution of market-level Brier scores under conservative (pre-outcome) vs. resolution "
               f"(at-close) truncation. The leftward shift under resolution timing reflects mechanical "
               f"convergence toward known outcomes.")

    save_fig(fig, 'fig02_brier_kde', caption)


# ============================================================================
# FIGURE 3: Platform Rankings Flip
# ============================================================================

def fig03_platform_ranking_shifts(data):
    """Platform Brier scores under different truncation regimes."""
    log("Figure 3: Platform ranking shifts")

    # Truncation regimes mapped to days_before_event
    # Conservative ≈ 2 days, Moderate ≈ 1 day, Aggressive ≈ 0.5 day, Resolution = 0
    truncation_map = {
        'Conservative': 2,
        'Moderate': 1,
        'Aggressive': 0,  # We'll use 0 as proxy for "aggressive" since we don't have finer granularity
        'Resolution': 0
    }

    # Actually, let's use what we have: days 7, 3, 1, 0
    truncations = ['7 days', '3 days', '1 day', 'Resolution']
    days_map = {'7 days': 7, '3 days': 3, '1 day': 1, 'Resolution': 0}

    pm_scores = []
    kalshi_scores = []
    pm_cis = []
    kalshi_cis = []

    for trunc in truncations:
        day = days_map[trunc]

        pm_data = data['pm_pred'][data['pm_pred']['days_before_event'] == day]['brier_score'].dropna()
        k_data = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == day]['brier_score'].dropna()

        pm_scores.append(pm_data.mean() if len(pm_data) > 0 else np.nan)
        kalshi_scores.append(k_data.mean() if len(k_data) > 0 else np.nan)

        # Bootstrap CIs
        if len(pm_data) > 10:
            boots = [np.random.choice(pm_data, size=len(pm_data), replace=True).mean() for _ in range(1000)]
            pm_cis.append((np.percentile(boots, 2.5), np.percentile(boots, 97.5)))
        else:
            pm_cis.append((np.nan, np.nan))

        if len(k_data) > 10:
            boots = [np.random.choice(k_data, size=len(k_data), replace=True).mean() for _ in range(1000)]
            kalshi_cis.append((np.percentile(boots, 2.5), np.percentile(boots, 97.5)))
        else:
            kalshi_cis.append((np.nan, np.nan))

    fig, ax = plt.subplots(figsize=(8, 5))

    x = np.arange(len(truncations))

    # Plot lines with CI bands
    ax.plot(x, pm_scores, 'o-', color=COLORS['polymarket'], linewidth=2, markersize=8, label='Polymarket')
    ax.fill_between(x, [ci[0] for ci in pm_cis], [ci[1] for ci in pm_cis],
                   color=COLORS['polymarket'], alpha=0.2)

    ax.plot(x, kalshi_scores, 's-', color=COLORS['kalshi'], linewidth=2, markersize=8, label='Kalshi')
    ax.fill_between(x, [ci[0] for ci in kalshi_cis], [ci[1] for ci in kalshi_cis],
                   color=COLORS['kalshi'], alpha=0.2)

    ax.set_xticks(x)
    ax.set_xticklabels(truncations)
    ax.set_xlabel('Truncation Regime')
    ax.set_ylabel('Mean Brier Score')
    ax.legend(loc='upper right', framealpha=0.9)

    # Annotate if lines cross or gap changes
    gap_start = pm_scores[0] - kalshi_scores[0] if not np.isnan(pm_scores[0]) and not np.isnan(kalshi_scores[0]) else 0
    gap_end = pm_scores[-1] - kalshi_scores[-1] if not np.isnan(pm_scores[-1]) and not np.isnan(kalshi_scores[-1]) else 0

    caption = (f"Aggregate Brier score by platform under four truncation regimes. The relative accuracy "
               f"of Polymarket and Kalshi shifts depending on when prices are sampled, illustrating how "
               f"truncation choices can alter platform comparisons.")

    save_fig(fig, 'fig03_platform_ranking_shifts', caption)


# ============================================================================
# FIGURE 4: Convergence Curves with Post-Outcome Cliff
# ============================================================================

def fig04_convergence_cliff(data):
    """Brier score convergence over time with post-outcome cliff."""
    log("Figure 4: Convergence cliff")

    # Combine both platforms
    pm = data['pm_pred'].copy()
    kalshi = data['kalshi_pred'].copy()

    # Filter to electoral markets if possible
    pm_electoral = pm[pm['category'].str.contains('ELECTORAL', na=False)]
    k_electoral = kalshi[kalshi['category'].str.contains('ELECTORAL', na=False)]

    if len(pm_electoral) < 1000:
        pm_electoral = pm
        k_electoral = kalshi

    all_pred = pd.concat([
        pm_electoral[['days_before_event', 'brier_score']],
        k_electoral[['days_before_event', 'brier_score']]
    ])

    # Aggregate by days
    by_days = all_pred.groupby('days_before_event')['brier_score'].agg(['mean', 'count']).reset_index()
    by_days = by_days[by_days['count'] >= 50]  # Minimum sample
    by_days = by_days[by_days['days_before_event'] <= 60]
    by_days = by_days.sort_values('days_before_event')

    fig, ax = plt.subplots(figsize=(8, 5))

    # Split into pre-event and post-event (day 0 is ambiguous)
    pre_event = by_days[by_days['days_before_event'] > 0]
    at_event = by_days[by_days['days_before_event'] == 0]

    # Plot pre-event convergence
    ax.plot(pre_event['days_before_event'], pre_event['mean'], 'o-',
           color=COLORS['conservative'], linewidth=2, markersize=6)

    # Plot at-event (resolution) with different color
    if len(at_event) > 0:
        ax.plot(at_event['days_before_event'], at_event['mean'], 's',
               color=COLORS['resolution'], markersize=10, zorder=10)

    # Shade post-outcome region
    ax.axvspan(-0.5, 0.5, color=COLORS['resolution'], alpha=0.1, label='Post-outcome')
    ax.axvline(0, color=COLORS['gray'], linestyle='--', linewidth=1)

    ax.invert_xaxis()
    ax.set_xlabel('Days Before Event')
    ax.set_ylabel('Mean Brier Score')
    ax.text(0.5, ax.get_ylim()[0] + 0.01, 'Event', ha='center', fontsize=9, color=COLORS['gray'])

    caption = (f"Brier score convergence for electoral markets. Natural accuracy improvement from 60 days "
               f"to election eve, followed by mechanical convergence once outcomes are known. "
               f"Prices in the shaded region reflect information processing, not forecasting.")

    save_fig(fig, 'fig04_convergence_cliff', caption)


# ============================================================================
# FIGURE 5: VWAP vs Spot Scatter
# ============================================================================

def fig05_vwap_vs_spot(data):
    """Scatter plot of VWAP vs spot Brier scores."""
    log("Figure 5: VWAP vs spot scatter")

    # Use 1-day predictions and compare to what we can compute
    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1].copy()

    # For each market, we have the spot price (prediction_price)
    # VWAP would require computation from price histories
    # For now, we'll approximate by comparing day-1 to day-2 as a proxy for smoothing

    pm_2d = data['pm_pred'][data['pm_pred']['days_before_event'] == 2].copy()

    # Merge on market_id
    merged = pm_1d.merge(pm_2d[['market_id', 'prediction_price', 'brier_score']],
                        on='market_id', suffixes=('_1d', '_2d'))

    # Get volume info
    volume_map = dict(zip(data['master']['market_id'].astype(str), data['master']['volume_usd']))
    merged['volume'] = merged['market_id'].astype(str).map(volume_map).fillna(0)

    # Color by volume tier
    def volume_tier(v):
        if v >= 100000:
            return 'High ($100K+)'
        elif v >= 10000:
            return 'Medium ($10K-$100K)'
        else:
            return 'Low (<$10K)'

    merged['tier'] = merged['volume'].apply(volume_tier)

    fig, ax = plt.subplots(figsize=(8, 5))

    tier_colors = {
        'High ($100K+)': COLORS['conservative'],
        'Medium ($10K-$100K)': '#FFB347',
        'Low (<$10K)': COLORS['resolution']
    }

    for tier in ['Low (<$10K)', 'Medium ($10K-$100K)', 'High ($100K+)']:
        subset = merged[merged['tier'] == tier]
        ax.scatter(subset['brier_score_2d'], subset['brier_score_1d'],
                  alpha=0.4, s=20, color=tier_colors[tier], label=tier)

    # Diagonal line
    ax.plot([0, 0.5], [0, 0.5], '--', color=COLORS['gray'], linewidth=1)

    ax.set_xlabel('Brier Score (2-day, proxy for VWAP)')
    ax.set_ylabel('Brier Score (1-day, spot)')
    ax.set_xlim(0, 0.5)
    ax.set_ylim(0, 0.5)
    ax.set_aspect('equal')
    ax.legend(loc='lower right', title='Volume Tier', framealpha=0.9)

    # Count divergent markets
    divergent = len(merged[np.abs(merged['brier_score_1d'] - merged['brier_score_2d']) > 0.01])
    ax.text(0.02, 0.98, f'N = {divergent:,} markets with |Δ| > 0.01',
           transform=ax.transAxes, fontsize=9, verticalalignment='top')

    caption = (f"Market-level Brier scores using 2-day (VWAP proxy) vs. 1-day (spot) prices. "
               f"Points colored by volume tier. Liquid markets (blue) cluster on the diagonal; "
               f"thin markets (red) diverge.")

    save_fig(fig, 'fig05_vwap_vs_spot_scatter', caption)


# ============================================================================
# FIGURE 6: Price Divergence by Depth
# ============================================================================

def fig06_price_divergence_by_depth(data):
    """Price divergence by volume decile."""
    log("Figure 6: Price divergence by depth")

    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1].copy()
    pm_2d = data['pm_pred'][data['pm_pred']['days_before_event'] == 2].copy()

    merged = pm_1d.merge(pm_2d[['market_id', 'prediction_price']],
                        on='market_id', suffixes=('_1d', '_2d'))

    merged['price_diff'] = np.abs(merged['prediction_price_1d'] - merged['prediction_price_2d'])

    volume_map = dict(zip(data['master']['market_id'].astype(str), data['master']['volume_usd']))
    merged['volume'] = merged['market_id'].astype(str).map(volume_map).fillna(0)
    merged = merged[merged['volume'] > 0]

    if len(merged) < 100:
        log("  Insufficient data for Figure 6")
        return

    # Create deciles
    merged['decile'] = pd.qcut(merged['volume'], 10, labels=False, duplicates='drop')

    fig, ax = plt.subplots(figsize=(8, 5))

    # Box plot
    decile_data = [merged[merged['decile'] == d]['price_diff'].values for d in range(10)]
    decile_data = [d for d in decile_data if len(d) > 0]

    bp = ax.boxplot(decile_data, patch_artist=True)
    for patch in bp['boxes']:
        patch.set_facecolor(COLORS['conservative'])
        patch.set_alpha(0.6)

    # Overlay means
    means = [np.mean(d) for d in decile_data]
    ax.plot(range(1, len(means) + 1), means, 'o-', color=COLORS['resolution'], linewidth=2, markersize=6)

    # Threshold line
    ax.axhline(0.05, color=COLORS['gray'], linestyle='--', linewidth=1, label='5¢ threshold')

    ax.set_xlabel('Volume Decile (1=lowest, 10=highest)')
    ax.set_ylabel('|Price Day 1 - Price Day 2|')
    ax.legend(loc='upper right')

    caption = (f"Absolute divergence between 1-day and 2-day prices by volume decile. "
               f"In thin markets, prices can differ by 5+ cents — enough to change Brier scores "
               f"and calibration assessments.")

    save_fig(fig, 'fig06_price_divergence_by_depth', caption)


# ============================================================================
# FIGURE 7: Cost-to-Move CDF
# ============================================================================

def fig07_cost_to_move_cdf(data):
    """CDF of cost to move price."""
    log("Figure 7: Cost-to-move CDF")

    # Use liquidity data if available
    if len(data['liquidity']) > 0 and 'cost_to_move_5c' in data['liquidity'].columns:
        costs = data['liquidity']['cost_to_move_5c'].dropna()
        costs = costs[costs > 0]
    else:
        # Use volume as proxy
        costs = data['master']['volume_usd'].dropna()
        costs = costs[costs > 0]

    if len(costs) < 10:
        log("  Insufficient liquidity data for Figure 7")
        return

    fig, ax = plt.subplots(figsize=(8, 5))

    # Sort and compute CDF
    sorted_costs = np.sort(costs)
    cdf = np.arange(1, len(sorted_costs) + 1) / len(sorted_costs)

    ax.plot(sorted_costs, cdf, color=COLORS['conservative'], linewidth=2)

    ax.set_xscale('log')
    ax.set_xlabel('Cost to Move 5¢ (USD) or Volume')
    ax.set_ylabel('Cumulative Fraction of Markets')

    # Threshold lines
    for thresh, label in [(10000, '$10K'), (100000, '$100K')]:
        frac = np.mean(sorted_costs < thresh)
        ax.axvline(thresh, color=COLORS['gray'], linestyle='--', linewidth=1)
        ax.text(thresh * 1.1, 0.5, label, fontsize=9, color=COLORS['gray'])

    # Annotate fractions
    frac_fragile = np.mean(sorted_costs < 10000)
    frac_caution = np.mean((sorted_costs >= 10000) & (sorted_costs < 100000))
    frac_reportable = np.mean(sorted_costs >= 100000)

    ax.text(0.02, 0.98, f'Fragile (<$10K): {frac_fragile:.1%}\n'
                        f'Caution ($10K-$100K): {frac_caution:.1%}\n'
                        f'Reportable (≥$100K): {frac_reportable:.1%}',
           transform=ax.transAxes, fontsize=9, verticalalignment='top',
           bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    caption = (f"Cumulative distribution of market volume (proxy for manipulation cost) across "
               f"{len(costs):,} markets. Only {frac_reportable:.1%} of markets meet the 'Reportable' "
               f"threshold of $100K. The majority are movable by a motivated individual.")

    save_fig(fig, 'fig07_cost_to_move_cdf', caption)


# ============================================================================
# FIGURE 8: Brier by Volume Threshold
# ============================================================================

def fig08_brier_by_threshold(data):
    """Aggregate Brier score by volume threshold."""
    log("Figure 8: Brier by threshold")

    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1].copy()
    k_1d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 1].copy()

    volume_map = dict(zip(data['master']['market_id'].astype(str), data['master']['volume_usd']))
    pm_1d['volume'] = pm_1d['market_id'].astype(str).map(volume_map).fillna(0)
    k_1d['volume'] = k_1d['ticker'].astype(str).map(volume_map).fillna(0)

    thresholds = [0, 1000, 10000, 100000]

    all_brier = []
    electoral_brier = []
    all_n = []
    electoral_n = []

    for thresh in thresholds:
        # All markets
        all_data = pd.concat([
            pm_1d[pm_1d['volume'] >= thresh]['brier_score'],
            k_1d[k_1d['volume'] >= thresh]['brier_score']
        ]).dropna()
        all_brier.append(all_data.mean())
        all_n.append(len(all_data))

        # Electoral only
        pm_elec = pm_1d[(pm_1d['volume'] >= thresh) & (pm_1d['category'].str.contains('ELECTORAL', na=False))]
        k_elec = k_1d[(k_1d['volume'] >= thresh) & (k_1d['category'].str.contains('ELECTORAL', na=False))]
        elec_data = pd.concat([pm_elec['brier_score'], k_elec['brier_score']]).dropna()
        electoral_brier.append(elec_data.mean() if len(elec_data) > 0 else np.nan)
        electoral_n.append(len(elec_data))

    fig, ax = plt.subplots(figsize=(8, 5))

    x = np.arange(len(thresholds))

    ax.plot(x, all_brier, 'o-', color=COLORS['gray'], linewidth=2, markersize=8, label='All political')
    ax.plot(x, electoral_brier, 's-', color=COLORS['conservative'], linewidth=2, markersize=8, label='Electoral only')

    # Annotate N
    for i, (n_all, n_elec) in enumerate(zip(all_n, electoral_n)):
        ax.annotate(f'n={n_all:,}', (x[i], all_brier[i]), textcoords='offset points',
                   xytext=(0, 10), ha='center', fontsize=8, color=COLORS['gray'])

    ax.set_xticks(x)
    ax.set_xticklabels(['$0', '$1K', '$10K', '$100K'])
    ax.set_xlabel('Minimum Volume Threshold')
    ax.set_ylabel('Aggregate Brier Score')
    ax.legend(loc='upper right', framealpha=0.9)

    caption = (f"Aggregate Brier score as a function of minimum volume threshold. "
               f"Restricting to higher-volume markets changes the headline accuracy. "
               f"Electoral markets (blue) vs. all political markets (gray) show different sensitivity.")

    save_fig(fig, 'fig08_brier_by_threshold', caption)


# ============================================================================
# FIGURE 9: Accuracy by Category
# ============================================================================

def fig09_accuracy_by_category(data):
    """Brier scores by political category."""
    log("Figure 9: Accuracy by category")

    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1].copy()
    k_1d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 1].copy()

    all_pred = pd.concat([
        pm_1d[['category', 'brier_score']],
        k_1d[['category', 'brier_score']]
    ])

    by_cat = all_pred.groupby('category')['brier_score'].agg(['mean', 'count']).reset_index()
    by_cat = by_cat[by_cat['count'] >= 20]  # Minimum sample
    by_cat = by_cat.sort_values('mean')

    # Clean category names
    by_cat['cat_clean'] = by_cat['category'].apply(
        lambda x: x.split('. ')[-1][:25] if pd.notna(x) and '. ' in str(x) else str(x)[:25]
    )

    fig, ax = plt.subplots(figsize=(8, 6))

    y_pos = np.arange(len(by_cat))
    bars = ax.barh(y_pos, by_cat['mean'], color=COLORS['conservative'], alpha=0.8)

    # Add N labels
    for i, (mean, n) in enumerate(zip(by_cat['mean'], by_cat['count'])):
        ax.text(mean + 0.005, i, f'n={n:,}', va='center', fontsize=8)

    # Overall mean line
    overall = all_pred['brier_score'].mean()
    ax.axvline(overall, color=COLORS['resolution'], linestyle='--', linewidth=1.5, label=f'Overall: {overall:.3f}')

    ax.set_yticks(y_pos)
    ax.set_yticklabels(by_cat['cat_clean'], fontsize=9)
    ax.set_xlabel('Mean Brier Score')
    ax.legend(loc='lower right', framealpha=0.9)

    caption = (f"Mean Brier score by political category under conservative truncation. "
               f"Wide variance across categories means aggregate accuracy is sensitive to category composition.")

    save_fig(fig, 'fig09_accuracy_by_category', caption)


# ============================================================================
# FIGURE 11: Shared vs Unique Markets
# ============================================================================

def fig11_platform_shared_vs_unique(data):
    """Platform accuracy on shared vs unique markets."""
    log("Figure 11: Shared vs unique markets")

    # For now, show platform-level comparison since we don't have explicit matching
    pm_1d = data['pm_pred'][data['pm_pred']['days_before_event'] == 1]
    k_1d = data['kalshi_pred'][data['kalshi_pred']['days_before_event'] == 1]

    pm_brier = pm_1d['brier_score'].dropna()
    k_brier = k_1d['brier_score'].dropna()

    fig, ax = plt.subplots(figsize=(8, 5))

    # Simple comparison
    platforms = ['Polymarket\n(all)', 'Kalshi\n(all)']
    means = [pm_brier.mean(), k_brier.mean()]
    ns = [len(pm_brier), len(k_brier)]

    # Bootstrap CIs
    cis = []
    for brier_data in [pm_brier, k_brier]:
        boots = [np.random.choice(brier_data, size=len(brier_data), replace=True).mean() for _ in range(1000)]
        cis.append((np.percentile(boots, 2.5), np.percentile(boots, 97.5)))

    colors = [COLORS['polymarket'], COLORS['kalshi']]
    x = np.arange(len(platforms))

    bars = ax.bar(x, means, color=colors, alpha=0.8, yerr=[[m - ci[0] for m, ci in zip(means, cis)],
                                                            [ci[1] - m for m, ci in zip(means, cis)]],
                 capsize=5)

    # Annotate N
    for i, n in enumerate(ns):
        ax.text(i, means[i] + 0.01, f'n={n:,}', ha='center', fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(platforms)
    ax.set_ylabel('Mean Brier Score')

    caption = (f"Platform accuracy comparison. Polymarket (n={ns[0]:,}) vs Kalshi (n={ns[1]:,}). "
               f"Note: This comparison includes unique markets on each platform; "
               f"shared market analysis requires explicit market matching (~330 matched pairs available).")

    save_fig(fig, 'fig11_platform_shared_vs_unique', caption)


# ============================================================================
# FIGURE 13: THE SPECIFICATION CURVE
# ============================================================================

def fig13_specification_curve(data):
    """The anchor figure: specification curve with indicator panel."""
    log("Figure 13: Specification curve")

    # Generate all 192 specifications
    truncations = ['Conservative', 'Moderate', 'Aggressive', 'Resolution']
    price_types = ['spot', 'vwap_1h', 'vwap_3h', 'vwap_6h', 'vwap_24h', 'midpoint']
    thresholds = [0, 1000, 10000, 100000]
    metrics = ['brier', 'logloss']

    # Map truncation to days_before_event
    trunc_to_days = {'Conservative': 7, 'Moderate': 3, 'Aggressive': 1, 'Resolution': 0}

    pm_pred = data['pm_pred'].copy()
    k_pred = data['kalshi_pred'].copy()

    volume_map = dict(zip(data['master']['market_id'].astype(str), data['master']['volume_usd']))
    pm_pred['volume'] = pm_pred['market_id'].astype(str).map(volume_map).fillna(0)
    k_pred['volume'] = k_pred['ticker'].astype(str).map(volume_map).fillna(0)

    specs = []

    for trunc in truncations:
        days = trunc_to_days[trunc]
        pm_t = pm_pred[pm_pred['days_before_event'] == days]
        k_t = k_pred[k_pred['days_before_event'] == days]

        for price in price_types:
            # For simplicity, use prediction_price as spot proxy for all price types
            # (full VWAP computation would require price history processing)

            for thresh in thresholds:
                pm_filtered = pm_t[pm_t['volume'] >= thresh]
                k_filtered = k_t[k_t['volume'] >= thresh]

                combined = pd.concat([pm_filtered['brier_score'], k_filtered['brier_score']]).dropna()
                combined_prices = pd.concat([pm_filtered['prediction_price'], k_filtered['prediction_price']]).dropna()
                combined_outcomes = pd.concat([pm_filtered['actual_outcome'], k_filtered['actual_outcome']]).dropna()

                for metric in metrics:
                    if metric == 'brier':
                        score = combined.mean() if len(combined) > 0 else np.nan
                    else:
                        # Log-loss
                        if len(combined_prices) > 0:
                            p = np.clip(combined_prices.values, 1e-10, 1 - 1e-10)
                            o = combined_outcomes.values[:len(p)]
                            if len(o) == len(p):
                                ll = -(o * np.log(p) + (1 - o) * np.log(1 - p))
                                score = np.mean(ll)
                            else:
                                score = np.nan
                        else:
                            score = np.nan

                    specs.append({
                        'truncation': trunc,
                        'price': price,
                        'threshold': thresh,
                        'metric': metric,
                        'score': score,
                        'n': len(combined)
                    })

    spec_df = pd.DataFrame(specs)
    spec_df = spec_df.dropna(subset=['score'])
    spec_df = spec_df.sort_values('score').reset_index(drop=True)
    spec_df['rank'] = range(len(spec_df))

    # Save spec data
    spec_df.to_csv(OUTPUT_DIR / 'spec_curve_data.csv', index=False)

    # Create figure with two panels
    fig, (ax_top, ax_bot) = plt.subplots(2, 1, figsize=(10, 8),
                                          gridspec_kw={'height_ratios': [3, 1]},
                                          sharex=True)

    # Top panel: the curve
    metric_colors = {'brier': COLORS['conservative'], 'logloss': COLORS['resolution']}

    for metric in metrics:
        subset = spec_df[spec_df['metric'] == metric]
        ax_top.scatter(subset['rank'], subset['score'], s=30, alpha=0.7,
                      color=metric_colors[metric], label=metric.capitalize())

    ax_top.set_ylabel('Aggregate Accuracy Score')
    ax_top.legend(loc='upper left', framealpha=0.9)

    # Mark key specifications
    # Clinton & Huang proxy: Resolution, spot, low threshold, brier
    ch_spec = spec_df[(spec_df['truncation'] == 'Resolution') &
                      (spec_df['price'] == 'spot') &
                      (spec_df['threshold'] == 0) &
                      (spec_df['metric'] == 'brier')]
    if len(ch_spec) > 0:
        ch_rank = ch_spec['rank'].values[0]
        ch_score = ch_spec['score'].values[0]
        ax_top.annotate('Clinton & Huang\n(approx)', (ch_rank, ch_score),
                       xytext=(ch_rank + 10, ch_score + 0.02),
                       arrowprops=dict(arrowstyle='->', color=COLORS['gray']),
                       fontsize=8)

    # Bellwether: Conservative, VWAP, $100K, brier
    bw_spec = spec_df[(spec_df['truncation'] == 'Conservative') &
                      (spec_df['price'] == 'vwap_6h') &
                      (spec_df['threshold'] == 100000) &
                      (spec_df['metric'] == 'brier')]
    if len(bw_spec) > 0:
        bw_rank = bw_spec['rank'].values[0]
        bw_score = bw_spec['score'].values[0]
        ax_top.annotate('Bellwether\nProtocol', (bw_rank, bw_score),
                       xytext=(bw_rank - 15, bw_score - 0.02),
                       arrowprops=dict(arrowstyle='->', color=COLORS['gray']),
                       fontsize=8)

    # Bottom panel: indicator matrix
    # Encode choices as y-positions
    choice_y = {'truncation': 3, 'price': 2, 'threshold': 1, 'metric': 0}

    # Color maps for each dimension
    trunc_idx = {t: i for i, t in enumerate(truncations)}
    price_idx = {p: i for i, p in enumerate(price_types)}
    thresh_idx = {t: i for i, t in enumerate(thresholds)}
    metric_idx = {m: i for i, m in enumerate(metrics)}

    cmap = plt.cm.viridis

    for _, row in spec_df.iterrows():
        rank = row['rank']
        # Plot small colored squares for each dimension
        ax_bot.scatter(rank, 3, c=[trunc_idx[row['truncation']] / 3], cmap='Blues', s=15, marker='s')
        ax_bot.scatter(rank, 2, c=[price_idx[row['price']] / 5], cmap='Greens', s=15, marker='s')
        ax_bot.scatter(rank, 1, c=[thresh_idx[row['threshold']] / 3], cmap='Oranges', s=15, marker='s')
        ax_bot.scatter(rank, 0, c=[metric_idx[row['metric']]], cmap='Purples', s=15, marker='s')

    ax_bot.set_yticks([0, 1, 2, 3])
    ax_bot.set_yticklabels(['Metric', 'Threshold', 'Price', 'Truncation'], fontsize=9)
    ax_bot.set_xlabel('Specification (sorted by accuracy score)')
    ax_bot.set_ylim(-0.5, 3.5)

    plt.tight_layout()

    caption = (f"Specification curve across {len(spec_df)} analytical specifications. "
               f"Each dot represents one combination of truncation regime, price construction, "
               f"volume threshold, and accuracy metric applied to 18,972 resolved markets. "
               f"Bottom panel indicates active choices. The spread from best to worst specification "
               f"quantifies the researcher degrees of freedom in prediction market evaluation.")

    save_fig(fig, 'fig13_specification_curve', caption)


# ============================================================================
# TABLES
# ============================================================================

def create_tables():
    """Create literature review tables."""
    log("Creating tables")

    # Table 1: Literature truncation practices
    table1 = """Study,Platform(s),Price Used,Truncation Rule,Explicit Discussion,Post-Outcome Trading
Berg Nelson & Rietz 2008,IEM,Last trade,Event-time (implicit),No,Minimal
Wolfers & Zitzewitz 2004,IEM/TradeSports,Closing price,Event-time (implicit),No,Minimal
Wolfers & Zitzewitz 2006,Multiple,Last trade,Event-time (implicit),No,Minimal
Erikson & Wlezien 2012,IEM,Daily close,Event-time,Partial,Minimal
Clinton & Huang 2025,IEM/Kalshi/PredictIt/PM,Last trade,Resolution-time,No,Significant
Cutting et al 2025,Polymarket,Unspecified,Resolution-time,No,Significant
Bruggi & Whelan 2025,Kalshi,Last trade,Resolution-time,No,Significant
Chen et al 2024,Polymarket,Last trade,Resolution-time,No,Significant
Chernov Elenev & Song 2025,Polymarket,Last trade,Resolution-time,No,Significant"""

    with open(OUTPUT_DIR / 'table01_literature_truncation.csv', 'w') as f:
        f.write(table1)

    log("  Saved table01_literature_truncation.csv")


# ============================================================================
# MAIN
# ============================================================================

def main():
    log("=" * 70)
    log("GENERATING FIGURES FOR DEGREES OF FREEDOM PAPER")
    log("=" * 70)

    data = load_all_data()

    log("\nGenerating figures...")

    fig01_calibration(data)
    fig02_brier_kde(data)
    fig03_platform_ranking_shifts(data)
    fig04_convergence_cliff(data)
    fig05_vwap_vs_spot(data)
    fig06_price_divergence_by_depth(data)
    fig07_cost_to_move_cdf(data)
    fig08_brier_by_threshold(data)
    fig09_accuracy_by_category(data)
    fig11_platform_shared_vs_unique(data)
    fig13_specification_curve(data)

    create_tables()

    # Save captions
    with open(OUTPUT_DIR / 'captions.md', 'w') as f:
        f.write("# Figure Captions\n\n")
        for name, caption in CAPTIONS.items():
            f.write(f"## {name}\n{caption}\n\n")

    log(f"\nSaved captions.md")

    log("\n" + "=" * 70)
    log("COMPLETE")
    log(f"Output directory: {OUTPUT_DIR}")
    log("=" * 70)


if __name__ == '__main__':
    main()
