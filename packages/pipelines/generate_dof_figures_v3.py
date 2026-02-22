#!/usr/bin/env python3
"""
Generate Figures for "Researcher Degrees of Freedom in Prediction Market Evaluation"
Version 3 - Using proper trade data with correct truncation offsets

Uses trade-level data from:
- pm_trades_for_vwap.json (14,856 markets, 8.8M trades)
- kalshi_trades_for_vwap.json (7,981 markets, 796K trades)

Truncation offsets (hours before anchor):
- Polymarket: Conservative=-48h, Moderate=-24h, Aggressive=-12h
- Kalshi: Conservative=-24h, Moderate=-12h, Aggressive=-3h
- Resolution: 0h (at anchor)

Anchor point:
- Electoral markets: min(election_date, trading_close_time)
- Non-electoral: trading_close_time
"""

import json
import sys
import warnings
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from scipy import stats
from scipy.signal import savgol_filter

warnings.filterwarnings('ignore')

# Paths
SCRIPT_DIR = Path(__file__).parent
BASE_DIR = SCRIPT_DIR.parent
DOF_DATA_DIR = BASE_DIR / "papers" / "degrees_of_freedom" / "data"
ROOT_DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output" / "dof_figures_v3"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Data files
PM_TRADES_FILE = DOF_DATA_DIR / "pm_trades_for_vwap.json"
KALSHI_TRADES_FILE = DOF_DATA_DIR / "kalshi_trades_for_vwap.json"
MASTER_SNAPSHOT = DOF_DATA_DIR / "master_snapshot.csv"
RESOLUTION_PRICES_FILE = ROOT_DATA_DIR / "resolution_prices.json"
PM_ORDERBOOK_FILE = ROOT_DATA_DIR / "orderbook_history_polymarket.json"
KALSHI_ORDERBOOK_FILE = ROOT_DATA_DIR / "orderbook_history_kalshi.json"

# Truncation offsets (hours)
PM_TRUNCATIONS = {
    'conservative': -48,
    'moderate': -24,
    'aggressive': -12,
    'resolution': 0
}

KALSHI_TRUNCATIONS = {
    'conservative': -24,
    'moderate': -12,
    'aggressive': -3,
    'resolution': 0
}

VWAP_WINDOW_HOURS = 24

# Style
COLORS = {
    'conservative': '#4A7FB5',
    'moderate': '#7FB54A',
    'aggressive': '#B5A04A',
    'resolution': '#D4726A',
    'polymarket': '#4A7FB5',
    'kalshi': '#5DAA68',
    'gray': '#888888',
    'light_gray': '#CCCCCC',
}

plt.rcParams.update({
    'font.family': 'serif',
    'font.serif': ['Times New Roman', 'DejaVu Serif', 'Georgia'],
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 11,
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


def compute_spot(trades, cutoff_ts, fallback_to_earliest=False):
    """Get last trade price before cutoff timestamp.

    If fallback_to_earliest=True and no trades before cutoff,
    returns the earliest available trade price instead of None.
    """
    valid = [t for t in trades if t.get('timestamp') is not None and t['timestamp'] <= cutoff_ts and t['price'] is not None]
    if not valid:
        if fallback_to_earliest:
            # Fall back to earliest available trade
            all_valid = [t for t in trades if t.get('timestamp') is not None and t['price'] is not None]
            if all_valid:
                earliest = min(all_valid, key=lambda x: x['timestamp'])
                return earliest['price']
        return None
    last_trade = max(valid, key=lambda x: x['timestamp'])
    return last_trade['price']


def compute_vwap(trades, start_ts, end_ts):
    """Compute volume-weighted average price in time window."""
    valid = [t for t in trades
             if t.get('timestamp') is not None
             and start_ts <= t['timestamp'] <= end_ts
             and t['price'] is not None
             and t.get('shares') is not None
             and t['shares'] > 0]
    if not valid:
        return None

    total_value = sum(t['price'] * t['shares'] for t in valid)
    total_volume = sum(t['shares'] for t in valid)

    if total_volume == 0:
        return None
    return total_value / total_volume


def compute_midpoint(orderbook_snapshots, target_ts):
    """Find the closest orderbook snapshot to target timestamp and return midpoint.

    Args:
        orderbook_snapshots: list of dicts with 'timestamp' and 'midpoint' keys
        target_ts: target timestamp in seconds (will convert to ms if needed)

    Returns:
        midpoint price or None if no snapshots available
    """
    if not orderbook_snapshots:
        return None

    # Convert target to milliseconds if it looks like seconds
    if target_ts < 1e12:
        target_ts_ms = target_ts * 1000
    else:
        target_ts_ms = target_ts

    # Find closest snapshot
    closest = None
    min_diff = float('inf')

    for snap in orderbook_snapshots:
        ts = snap.get('timestamp')
        if ts is None:
            continue
        diff = abs(ts - target_ts_ms)
        if diff < min_diff:
            min_diff = diff
            closest = snap

    if closest is None:
        return None

    return closest.get('midpoint')


def compute_prices_for_market(market_data, truncations, platform, yes_token_id=None):
    """
    Compute spot and VWAP prices at each truncation point.

    For Polymarket, filters to only YES token trades using yes_token_id.

    Returns dict: {truncation_name: {'spot': price, 'vwap': price}}
    """
    anchor_ts = market_data['anchor_ts']
    raw_trades = market_data.get('trades', [])

    if not raw_trades or not anchor_ts:
        return None

    # Normalize trade format between platforms
    trades = []
    if platform == 'kalshi':
        # Kalshi format: yes_price (cents), count, created_time
        for t in raw_trades:
            price = t.get('yes_price')
            if price is not None:
                price = price / 100.0  # Convert cents to probability
            ts = t.get('created_time')
            if isinstance(ts, str):
                ts = int(ts) if ts.isdigit() else None
            trades.append({
                'price': price,
                'shares': t.get('count', 0),
                'timestamp': ts
            })
    else:
        # Polymarket format: price, shares, timestamp
        # Filter to only YES token trades
        for t in raw_trades:
            token_id = t.get('token_id')
            # Only include YES token trades
            if yes_token_id and token_id != yes_token_id:
                continue
            trades.append(t)

    results = {}
    for trunc_name, offset_hours in truncations.items():
        cutoff_ts = anchor_ts + (offset_hours * 3600)

        # Spot: last trade before cutoff (fall back to earliest if truncation not possible)
        spot = compute_spot(trades, cutoff_ts, fallback_to_earliest=True)

        # VWAP: volume-weighted average over 24h before cutoff
        vwap_start = cutoff_ts - (VWAP_WINDOW_HOURS * 3600)
        vwap = compute_vwap(trades, vwap_start, cutoff_ts)

        results[trunc_name] = {
            'spot': spot,
            'vwap': vwap if vwap is not None else spot  # Fall back to spot
        }

    return results


def load_all_data():
    """Load trade data and master snapshot."""
    log("Loading data...")
    data = {}

    # Load master snapshot for outcomes (read token IDs as strings to preserve precision)
    master = pd.read_csv(MASTER_SNAPSHOT, low_memory=False,
                         dtype={'pm_token_id_yes': str, 'pm_token_id_no': str})
    log(f"  Master snapshot: {len(master):,} markets")

    # Build outcome lookup (prefer winning_outcome, fallback to resolution_outcome)
    outcome_lookup = {}
    for _, row in master.iterrows():
        mid = str(row['market_id'])
        # Try winning_outcome first (more complete), then resolution_outcome
        outcome = row.get('winning_outcome')
        if pd.isna(outcome):
            outcome = row.get('resolution_outcome')
        if pd.notna(outcome):
            # Convert to 0/1
            if outcome in [1, 1.0, '1', 'Yes', 'yes', True]:
                outcome_lookup[mid] = 1
            elif outcome in [0, 0.0, '0', 'No', 'no', False]:
                outcome_lookup[mid] = 0
    log(f"  Outcomes: {len(outcome_lookup):,} markets")

    # Load volume and platform from master
    volume_lookup = {}
    platform_lookup = {}
    for _, row in master.iterrows():
        mid = str(row['market_id'])
        vol = row.get('volume_usd')
        if pd.notna(vol):
            volume_lookup[mid] = float(vol)
        plat = row.get('platform')
        if pd.notna(plat):
            platform_lookup[mid] = str(plat).lower()

    # Load category from master
    category_lookup = {}
    for _, row in master.iterrows():
        mid = str(row['market_id'])
        cat = row.get('political_category')
        if pd.notna(cat):
            category_lookup[mid] = str(cat)

    data['outcomes'] = outcome_lookup
    data['volumes'] = volume_lookup
    data['platforms'] = platform_lookup
    data['categories'] = category_lookup
    data['master'] = master

    # Build YES and NO token lookups for PM
    yes_token_lookup = {}
    no_token_lookup = {}
    for _, row in master.iterrows():
        mid = str(row['market_id'])
        yes_token = row.get('pm_token_id_yes')
        no_token = row.get('pm_token_id_no')
        if pd.notna(yes_token) and yes_token != '':
            yes_token_lookup[mid] = str(yes_token).strip()
        if pd.notna(no_token) and no_token != '':
            no_token_lookup[mid] = str(no_token).strip()
    log(f"  YES token lookup: {len(yes_token_lookup):,} markets")
    log(f"  NO token lookup: {len(no_token_lookup):,} markets")
    data['yes_tokens'] = yes_token_lookup
    data['no_tokens'] = no_token_lookup

    # Load resolution prices (actual resolution prices, not computed from trades)
    log("  Loading resolution prices...")
    with open(RESOLUTION_PRICES_FILE, 'r') as f:
        res_data = json.load(f)

    # Build resolution price lookup by market_id
    # For PM: key by token_id, for Kalshi: key by ticker
    pm_res_prices = {}  # token_id -> price
    k_res_prices = {}   # ticker -> price
    for key, val in res_data.items():
        if val['platform'] == 'polymarket':
            pm_res_prices[str(val['token_id'])] = val['resolution_price']
        else:
            k_res_prices[str(val['ticker'])] = val['resolution_price']
    log(f"    PM resolution prices: {len(pm_res_prices):,}")
    log(f"    Kalshi resolution prices: {len(k_res_prices):,}")
    data['pm_res_prices'] = pm_res_prices
    data['k_res_prices'] = k_res_prices

    # Load PM trades
    log("  Loading PM trades...")
    with open(PM_TRADES_FILE, 'r') as f:
        pm_data = json.load(f)

    pm_markets = pm_data['markets']
    log(f"    {len(pm_markets):,} markets, {pm_data['metadata']['total_trades']:,} trades")

    # Compute prices at each truncation for PM (YES tokens, then derive NO)
    log("  Computing PM prices at truncation points...")
    pm_prices_yes = {}
    pm_prices_no = {}
    for market in pm_markets:
        mid = str(market['market_id'])
        yes_token_id = yes_token_lookup.get(mid)
        prices = compute_prices_for_market(market, PM_TRUNCATIONS, 'polymarket', yes_token_id)
        if prices:
            pm_prices_yes[mid] = {
                'prices': prices,
                'anchor_type': market.get('anchor_type'),
                'anchor_ts': market.get('anchor_ts')
            }
            # Derive NO prices as 1 - YES price
            no_prices = {}
            for trunc, price_dict in prices.items():
                no_prices[trunc] = {
                    'spot': 1.0 - price_dict['spot'] if price_dict['spot'] is not None else None,
                    'vwap': 1.0 - price_dict['vwap'] if price_dict['vwap'] is not None else None
                }
            pm_prices_no[mid] = {
                'prices': no_prices,
                'anchor_type': market.get('anchor_type'),
                'anchor_ts': market.get('anchor_ts')
            }
    log(f"    Computed prices for {len(pm_prices_yes):,} markets (YES and NO)")

    # Override resolution prices with actual resolution prices (not computed from trades)
    pm_res_override_count = 0
    for mid in pm_prices_yes.keys():
        yes_token = yes_token_lookup.get(mid)
        if yes_token and yes_token in pm_res_prices:
            res_price = pm_res_prices[yes_token]
            pm_prices_yes[mid]['prices']['resolution'] = {'spot': res_price, 'vwap': res_price}
            pm_prices_no[mid]['prices']['resolution'] = {'spot': 1.0 - res_price, 'vwap': 1.0 - res_price}
            pm_res_override_count += 1
    log(f"    Overrode {pm_res_override_count:,} PM resolution prices (YES and NO)")

    data['pm_prices_yes'] = pm_prices_yes
    data['pm_prices_no'] = pm_prices_no

    # Load Kalshi trades
    log("  Loading Kalshi trades...")
    with open(KALSHI_TRADES_FILE, 'r') as f:
        kalshi_data = json.load(f)

    kalshi_markets = kalshi_data['markets']
    log(f"    {len(kalshi_markets):,} markets, {kalshi_data['metadata']['total_trades']:,} trades")

    # Compute prices at each truncation for Kalshi (YES and derive NO)
    log("  Computing Kalshi prices at truncation points...")
    kalshi_prices_yes = {}
    kalshi_prices_no = {}
    for market in kalshi_markets:
        mid = str(market['market_id'])
        prices = compute_prices_for_market(market, KALSHI_TRUNCATIONS, 'kalshi', None)
        if prices:
            kalshi_prices_yes[mid] = {
                'prices': prices,
                'anchor_type': market.get('anchor_type'),
                'anchor_ts': market.get('anchor_ts')
            }
            # Derive NO prices as 1 - YES price
            no_prices = {}
            for trunc, price_dict in prices.items():
                no_prices[trunc] = {
                    'spot': 1.0 - price_dict['spot'] if price_dict['spot'] is not None else None,
                    'vwap': 1.0 - price_dict['vwap'] if price_dict['vwap'] is not None else None
                }
            kalshi_prices_no[mid] = {
                'prices': no_prices,
                'anchor_type': market.get('anchor_type'),
                'anchor_ts': market.get('anchor_ts')
            }
    log(f"    Computed prices for {len(kalshi_prices_yes):,} markets (YES and NO)")

    # Override resolution prices with actual resolution prices for Kalshi
    k_res_override_count = 0
    for mid in kalshi_prices_yes.keys():
        # For Kalshi, market_id is the ticker
        if mid in k_res_prices:
            res_price = k_res_prices[mid]
            kalshi_prices_yes[mid]['prices']['resolution'] = {'spot': res_price, 'vwap': res_price}
            kalshi_prices_no[mid]['prices']['resolution'] = {'spot': 1.0 - res_price, 'vwap': 1.0 - res_price}
            k_res_override_count += 1
    log(f"    Overrode {k_res_override_count:,} Kalshi resolution prices (YES and NO)")

    data['kalshi_prices_yes'] = kalshi_prices_yes
    data['kalshi_prices_no'] = kalshi_prices_no

    # Load orderbook data for midpoint computation
    log("  Loading orderbook data for midpoint...")

    # PM orderbook - keyed by market_id in file, orderbook contains token_id
    # pm_prices_yes is keyed by market_id, so we need token_id -> market_id mapping
    pm_midpoints = {}  # market_id -> {truncation: midpoint}
    if PM_ORDERBOOK_FILE.exists():
        with open(PM_ORDERBOOK_FILE, 'r') as f:
            pm_ob = json.load(f)
        log(f"    PM orderbook: {len(pm_ob):,} markets")

        # Build token_id -> market_id mapping from master
        token_to_market = {}
        for _, row in master.iterrows():
            if row.get('platform') == 'Polymarket':
                token_yes = str(row.get('pm_token_id_yes', ''))
                mid = str(row.get('market_id', ''))
                if token_yes and mid and token_yes != 'nan':
                    token_to_market[token_yes] = mid

        log(f"    Token-to-market mapping: {len(token_to_market):,} entries")

        matched = 0
        for ob_market_id, market_data in pm_ob.items():
            snapshots = market_data.get('metrics', [])
            if not snapshots:
                continue

            # The orderbook entry contains the token_id - map it to market_id
            token_id = str(market_data.get('token_id', ''))
            if not token_id:
                continue

            # Convert token_id to market_id
            market_id = token_to_market.get(token_id)
            if not market_id:
                continue

            # pm_prices_yes is keyed by market_id
            yes_data = pm_prices_yes.get(market_id)
            if not yes_data:
                continue
            anchor_ts = yes_data.get('anchor_ts')
            if not anchor_ts:
                continue

            matched += 1
            midpoints = {}
            for trunc_name, offset_hours in PM_TRUNCATIONS.items():
                cutoff_ts = anchor_ts + (offset_hours * 3600)
                mp = compute_midpoint(snapshots, cutoff_ts)
                midpoints[trunc_name] = mp

            if any(v is not None for v in midpoints.values()):
                pm_midpoints[market_id] = midpoints
        log(f"    PM midpoints computed: {len(pm_midpoints):,} markets (matched {matched:,})")
    else:
        log("    PM orderbook file not found")

    # Kalshi orderbook - keyed by ticker
    kalshi_midpoints = {}
    if KALSHI_ORDERBOOK_FILE.exists():
        with open(KALSHI_ORDERBOOK_FILE, 'r') as f:
            k_ob = json.load(f)
        log(f"    Kalshi orderbook: {len(k_ob):,} markets")

        for ticker, market_data in k_ob.items():
            snapshots = market_data.get('metrics', [])
            if not snapshots:
                continue

            # Get anchor_ts from our prices data if available
            yes_data = kalshi_prices_yes.get(ticker)
            if not yes_data:
                continue
            anchor_ts = yes_data.get('anchor_ts')
            if not anchor_ts:
                continue

            midpoints = {}
            for trunc_name, offset_hours in KALSHI_TRUNCATIONS.items():
                cutoff_ts = anchor_ts + (offset_hours * 3600)
                mp = compute_midpoint(snapshots, cutoff_ts)
                midpoints[trunc_name] = mp

            if any(v is not None for v in midpoints.values()):
                kalshi_midpoints[ticker] = midpoints
        log(f"    Kalshi midpoints computed: {len(kalshi_midpoints):,} markets")
    else:
        log("    Kalshi orderbook file not found")

    data['pm_midpoints'] = pm_midpoints
    data['kalshi_midpoints'] = kalshi_midpoints

    return data


def build_analysis_df(data, platform, truncation, price_type='spot'):
    """
    Build DataFrame with price, outcome, brier for analysis.
    For Polymarket, includes BOTH YES and NO tokens.

    Args:
        platform: 'pm' or 'kalshi'
        truncation: 'conservative', 'moderate', 'aggressive', 'resolution'
        price_type: 'spot', 'vwap', or 'midpoint'
    """
    outcomes = data['outcomes']
    volumes = data['volumes']
    categories = data['categories']

    # Get midpoint data if needed
    pm_midpoints = data.get('pm_midpoints', {})
    kalshi_midpoints = data.get('kalshi_midpoints', {})

    rows = []

    if platform == 'pm':
        # Include both YES and NO tokens for Polymarket
        prices_yes = data['pm_prices_yes']
        prices_no = data['pm_prices_no']

        for mid, pdata in prices_yes.items():
            yes_outcome = outcomes.get(mid)  # 1 if Yes won, 0 if No won
            if yes_outcome is None:
                continue

            # Determine which truncation to use (fall back to resolution if requested truncation unavailable)
            use_trunc = truncation if truncation in pdata['prices'] else 'resolution'
            if use_trunc not in pdata['prices']:
                continue

            # Get price based on price_type
            if price_type == 'midpoint':
                if mid in pm_midpoints and use_trunc in pm_midpoints[mid]:
                    yes_price = pm_midpoints[mid][use_trunc]
                elif mid in pm_midpoints and 'resolution' in pm_midpoints[mid]:
                    yes_price = pm_midpoints[mid]['resolution']
                else:
                    continue
            else:
                yes_price = pdata['prices'][use_trunc].get(price_type)

            if yes_price is None:
                continue

            # YES token: price as-is, outcome as-is
            yes_price = max(0, min(1, yes_price))
            rows.append({
                'market_id': mid,
                'token_type': 'YES',
                'price': yes_price,
                'outcome': yes_outcome,
                'brier': (yes_price - yes_outcome) ** 2,
                'volume': volumes.get(mid, 0),
                'category': categories.get(mid, 'Unknown'),
                'anchor_type': pdata.get('anchor_type')
            })

            # NO token: price = 1 - yes_price, outcome = 1 - yes_outcome
            if price_type == 'midpoint':
                no_price = 1.0 - yes_price if yes_price is not None else None
            elif mid in prices_no:
                # Use same truncation fallback logic for NO token
                no_trunc = use_trunc if use_trunc in prices_no[mid]['prices'] else 'resolution'
                if no_trunc in prices_no[mid]['prices']:
                    no_price = prices_no[mid]['prices'][no_trunc].get(price_type)
                else:
                    no_price = None
            else:
                no_price = None

            if no_price is not None:
                no_price = max(0, min(1, no_price))
                no_outcome = 1 - yes_outcome
                rows.append({
                    'market_id': mid,
                    'token_type': 'NO',
                    'price': no_price,
                    'outcome': no_outcome,
                    'brier': (no_price - no_outcome) ** 2,
                    'volume': volumes.get(mid, 0),
                    'category': categories.get(mid, 'Unknown'),
                    'anchor_type': pdata.get('anchor_type')
                })
    else:
        # Kalshi: include both YES and NO tokens
        prices_yes = data['kalshi_prices_yes']
        prices_no = data['kalshi_prices_no']

        for mid, pdata in prices_yes.items():
            yes_outcome = outcomes.get(mid)  # 1 if Yes won, 0 if No won
            if yes_outcome is None:
                continue

            # Determine which truncation to use (fall back to resolution if requested truncation unavailable)
            use_trunc = truncation if truncation in pdata['prices'] else 'resolution'
            if use_trunc not in pdata['prices']:
                continue

            # Get price based on price_type
            if price_type == 'midpoint':
                if mid in kalshi_midpoints and use_trunc in kalshi_midpoints[mid]:
                    yes_price = kalshi_midpoints[mid][use_trunc]
                elif mid in kalshi_midpoints and 'resolution' in kalshi_midpoints[mid]:
                    yes_price = kalshi_midpoints[mid]['resolution']
                else:
                    continue
            else:
                yes_price = pdata['prices'][use_trunc].get(price_type)

            if yes_price is None:
                continue

            # YES token
            yes_price = max(0, min(1, yes_price))
            rows.append({
                'market_id': mid,
                'token_type': 'YES',
                'price': yes_price,
                'outcome': yes_outcome,
                'brier': (yes_price - yes_outcome) ** 2,
                'volume': volumes.get(mid, 0),
                'category': categories.get(mid, 'Unknown'),
                'anchor_type': pdata.get('anchor_type')
            })

            # NO token
            if price_type == 'midpoint':
                no_price = 1.0 - yes_price if yes_price is not None else None
            elif mid in prices_no:
                # Use same truncation fallback logic for NO token
                no_trunc = use_trunc if use_trunc in prices_no[mid]['prices'] else 'resolution'
                if no_trunc in prices_no[mid]['prices']:
                    no_price = prices_no[mid]['prices'][no_trunc].get(price_type)
                else:
                    no_price = None
            else:
                no_price = None

            if no_price is not None:
                no_price = max(0, min(1, no_price))
                no_outcome = 1 - yes_outcome
                rows.append({
                    'market_id': mid,
                    'token_type': 'NO',
                    'price': no_price,
                    'outcome': no_outcome,
                    'brier': (no_price - no_outcome) ** 2,
                    'volume': volumes.get(mid, 0),
                    'category': categories.get(mid, 'Unknown'),
                    'anchor_type': pdata.get('anchor_type')
                })

    return pd.DataFrame(rows)


# ============================================================================
# FIGURE 1: Calibration Curves (Main - Combined Both Platforms)
# ============================================================================

def fig01_calibration(data):
    """
    Main calibration figure: Combined both platforms, 4 truncation levels.

    - Conservative: PM -48h / K -24h (boldest)
    - Moderate: PM -24h / K -12h
    - Aggressive: PM -12h / K -3h
    - Resolution: 0h (lightest)

    Neutral color palette (gray scale), boldest = farthest truncation.
    """
    log("Figure 1: Calibration (combined)")

    # Neutral colors - darker = farther truncation (more bold)
    colors = {
        'conservative': '#1a1a2e',  # Very dark (boldest)
        'moderate': '#4a4a6a',      # Dark gray
        'aggressive': '#8a8aa0',    # Medium gray
        'resolution': '#c0c0d0',    # Light gray
    }

    # Labels with platform-specific offsets
    labels = {
        'conservative': 'Conservative (PM: -48h, K: -24h)',
        'moderate': 'Moderate (PM: -24h, K: -12h)',
        'aggressive': 'Aggressive (PM: -12h, K: -3h)',
        'resolution': 'Resolution (0h)',
    }

    z_orders = {
        'conservative': 10,
        'moderate': 8,
        'aggressive': 6,
        'resolution': 4,
    }

    linewidths = {
        'conservative': 3.0,
        'moderate': 2.5,
        'aggressive': 2.0,
        'resolution': 1.5,
    }

    def build_combined_df(pm_prices_yes, pm_prices_no, k_prices_yes, k_prices_no, outcomes, truncation):
        """Build combined df for both platforms at given truncation.
        Includes both YES and NO tokens for both platforms."""
        rows = []

        # PM YES tokens
        for mid, pdata in pm_prices_yes.items():
            if truncation not in pdata['prices']:
                continue
            price = pdata['prices'][truncation].get('spot')
            outcome = outcomes.get(mid)
            if price is not None and outcome is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': outcome})

        # PM NO tokens
        for mid, pdata in pm_prices_no.items():
            if truncation not in pdata['prices']:
                continue
            price = pdata['prices'][truncation].get('spot')
            yes_outcome = outcomes.get(mid)
            if price is not None and yes_outcome is not None:
                no_outcome = 1 - yes_outcome
                rows.append({'price': max(0, min(1, price)), 'outcome': no_outcome})

        # Kalshi YES tokens
        for mid, pdata in k_prices_yes.items():
            if truncation not in pdata['prices']:
                continue
            price = pdata['prices'][truncation].get('spot')
            outcome = outcomes.get(mid)
            if price is not None and outcome is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': outcome})

        # Kalshi NO tokens
        for mid, pdata in k_prices_no.items():
            if truncation not in pdata['prices']:
                continue
            price = pdata['prices'][truncation].get('spot')
            yes_outcome = outcomes.get(mid)
            if price is not None and yes_outcome is not None:
                no_outcome = 1 - yes_outcome
                rows.append({'price': max(0, min(1, price)), 'outcome': no_outcome})

        return pd.DataFrame(rows)

    def compute_bins(df, num_bins=50):
        if len(df) < num_bins:
            return None, None
        df_sorted = df.sort_values('price').reset_index(drop=True)
        samples_per_bin = max(1, len(df_sorted) // num_bins)
        df_sorted['bin'] = df_sorted.index // samples_per_bin
        df_sorted.loc[df_sorted['bin'] >= num_bins, 'bin'] = num_bins - 1
        bins = df_sorted.groupby('bin').agg({'price': 'mean', 'outcome': 'mean'}).reset_index()
        return bins['price'].values, bins['outcome'].values

    fig, ax = plt.subplots(figsize=(10, 10))

    # Perfect calibration line
    ax.plot([0, 1], [0, 1], 'k--', linewidth=1.5, alpha=0.7, label='Perfect', zorder=15)

    truncations = ['conservative', 'moderate', 'aggressive', 'resolution']

    # Plot in reverse order so conservative ends up on top
    for trunc in reversed(truncations):
        df = build_combined_df(data['pm_prices_yes'], data['pm_prices_no'], data['kalshi_prices_yes'], data['kalshi_prices_no'], data['outcomes'], trunc)
        if len(df) < 50:
            log(f"    {trunc}: {len(df)} markets (skipped)")
            continue

        log(f"    {trunc}: {len(df):,} markets")
        pred, actual = compute_bins(df)
        if pred is None:
            continue

        # Scatter points
        scatter_alpha = 0.5 if trunc == 'conservative' else 0.25
        ax.scatter(pred, actual, s=15, alpha=scatter_alpha, color=colors[trunc], zorder=z_orders[trunc])

        # Polynomial trend line
        x = np.linspace(0.02, 0.98, 100)
        z = np.polyfit(pred, actual, 3)
        ax.plot(x, np.clip(np.poly1d(z)(x), 0, 1), color=colors[trunc],
               linewidth=linewidths[trunc], alpha=0.95, label=labels[trunc],
               zorder=z_orders[trunc] + 0.5)

    ax.set_xlabel('Predicted Probability', fontsize=12)
    ax.set_ylabel('Actual Outcome Rate', fontsize=12)
    ax.set_title('Calibration by Truncation Regime', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', fontsize=9)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)

    caption = (f"Calibration curves combining both platforms by truncation regime. "
               f"Darker = farther truncation from anchor. "
               f"Conservative uses PM -48h / K -24h; Resolution uses 0h for both.")

    save_fig(fig, 'fig01_calibration', caption)


# ============================================================================
# FIGURE 1B: Price Distribution by Truncation Regime
# ============================================================================

def fig01b_distribution(data):
    """
    Distribution of predicted probabilities by truncation regime.
    2x2 grid of histograms, one per truncation.
    """
    log("Figure 1b: Price Distribution by Truncation")

    # Same colors as calibration
    colors = {
        'conservative': '#1a1a2e',
        'moderate': '#4a4a6a',
        'aggressive': '#8a8aa0',
        'resolution': '#c0c0d0',
    }

    titles = {
        'conservative': 'Conservative (PM: -48h, K: -24h)',
        'moderate': 'Moderate (PM: -24h, K: -12h)',
        'aggressive': 'Aggressive (PM: -12h, K: -3h)',
        'resolution': 'Resolution (0h)',
    }

    def build_combined_df(pm_prices_yes, pm_prices_no, k_prices_yes, k_prices_no, outcomes, truncation):
        rows = []
        for mid, pdata in pm_prices_yes.items():
            if truncation not in pdata['prices']:
                continue
            price = pdata['prices'][truncation].get('spot')
            outcome = outcomes.get(mid)
            if price is not None and outcome is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': outcome})
        for mid, pdata in pm_prices_no.items():
            if truncation not in pdata['prices']:
                continue
            price = pdata['prices'][truncation].get('spot')
            yes_outcome = outcomes.get(mid)
            if price is not None and yes_outcome is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': 1 - yes_outcome})
        for mid, pdata in k_prices_yes.items():
            if truncation not in pdata['prices']:
                continue
            price = pdata['prices'][truncation].get('spot')
            outcome = outcomes.get(mid)
            if price is not None and outcome is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': outcome})
        for mid, pdata in k_prices_no.items():
            if truncation not in pdata['prices']:
                continue
            price = pdata['prices'][truncation].get('spot')
            yes_outcome = outcomes.get(mid)
            if price is not None and yes_outcome is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': 1 - yes_outcome})
        return pd.DataFrame(rows)

    fig, axes = plt.subplots(2, 2, figsize=(10, 8))
    axes = axes.flatten()

    truncations = ['conservative', 'moderate', 'aggressive', 'resolution']
    num_bins = 50
    bin_edges = np.linspace(0, 1, num_bins + 1)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    bar_width = 1.0 / num_bins * 0.9

    for i, trunc in enumerate(truncations):
        ax = axes[i]
        df = build_combined_df(data['pm_prices_yes'], data['pm_prices_no'],
                               data['kalshi_prices_yes'], data['kalshi_prices_no'],
                               data['outcomes'], trunc)

        counts, _ = np.histogram(df['price'], bins=bin_edges)

        ax.bar(bin_centers, counts, width=bar_width, alpha=0.7,
               color=colors[trunc], edgecolor='none')

        ax.set_title(titles[trunc], fontsize=11, fontweight='bold')
        ax.set_xlim(0, 1)
        ax.set_xlabel('Predicted Probability', fontsize=10)
        ax.set_ylabel('Count', fontsize=10)
        ax.grid(True, alpha=0.3, axis='y')

        # Add n label
        ax.text(0.98, 0.95, f'n={len(df):,}', transform=ax.transAxes,
                fontsize=9, ha='right', va='top')

    plt.tight_layout()

    caption = "Distribution of predicted probabilities by truncation regime. Resolution prices cluster at extremes."
    save_fig(fig, 'fig01b_distribution', caption)


# ============================================================================
# APPENDIX FIGURE A: Polymarket Calibration (Platform-Specific)
# ============================================================================

def fig_appendix_pm_calibration(data):
    """
    Appendix figure: Polymarket only, 4 truncation levels.
    Blue color palette.
    """
    log("Appendix A: Polymarket Calibration")

    # Blue shades - darker = farther truncation
    colors = {
        'conservative': '#1e3a5f',  # Dark blue (-48h)
        'moderate': '#3b6ea5',      # Medium blue (-24h)
        'aggressive': '#7ba3c9',    # Light blue (-12h)
        'resolution': '#b8d4e8',    # Very light blue (0h)
    }

    labels = {
        'conservative': '-48h',
        'moderate': '-24h',
        'aggressive': '-12h',
        'resolution': '0h',
    }

    z_orders = {'conservative': 10, 'moderate': 8, 'aggressive': 6, 'resolution': 4}
    linewidths = {'conservative': 3.0, 'moderate': 2.5, 'aggressive': 2.0, 'resolution': 1.5}

    def build_df(prices_yes, prices_no, outcomes, truncation):
        """Build df for PM with both YES and NO tokens. Falls back to resolution price if truncation unavailable."""
        rows = []
        # YES tokens
        for mid, pdata in prices_yes.items():
            outcome = outcomes.get(mid)
            if outcome is None:
                continue
            # Try requested truncation, fall back to resolution
            if truncation in pdata['prices']:
                price = pdata['prices'][truncation].get('spot')
            elif 'resolution' in pdata['prices']:
                price = pdata['prices']['resolution'].get('spot')
            else:
                price = None
            if price is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': outcome})
        # NO tokens
        for mid, pdata in prices_no.items():
            yes_outcome = outcomes.get(mid)
            if yes_outcome is None:
                continue
            # Try requested truncation, fall back to resolution
            if truncation in pdata['prices']:
                price = pdata['prices'][truncation].get('spot')
            elif 'resolution' in pdata['prices']:
                price = pdata['prices']['resolution'].get('spot')
            else:
                price = None
            if price is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': 1 - yes_outcome})
        return pd.DataFrame(rows)

    def compute_bins(df, num_bins=50):
        if len(df) < num_bins:
            return None, None
        df_sorted = df.sort_values('price').reset_index(drop=True)
        samples_per_bin = max(1, len(df_sorted) // num_bins)
        df_sorted['bin'] = df_sorted.index // samples_per_bin
        df_sorted.loc[df_sorted['bin'] >= num_bins, 'bin'] = num_bins - 1
        bins = df_sorted.groupby('bin').agg({'price': 'mean', 'outcome': 'mean'}).reset_index()
        return bins['price'].values, bins['outcome'].values

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.plot([0, 1], [0, 1], 'k--', linewidth=1.5, alpha=0.7, label='Perfect', zorder=15)

    truncations = ['conservative', 'moderate', 'aggressive', 'resolution']

    for trunc in reversed(truncations):
        df = build_df(data['pm_prices_yes'], data['pm_prices_no'], data['outcomes'], trunc)
        if len(df) < 50:
            continue

        log(f"    PM {trunc}: {len(df):,} markets")
        pred, actual = compute_bins(df)
        if pred is None:
            continue

        scatter_alpha = 0.5 if trunc == 'conservative' else 0.25
        ax.scatter(pred, actual, s=15, alpha=scatter_alpha, color=colors[trunc], zorder=z_orders[trunc])

        x = np.linspace(0.02, 0.98, 100)
        z = np.polyfit(pred, actual, 3)
        ax.plot(x, np.clip(np.poly1d(z)(x), 0, 1), color=colors[trunc],
               linewidth=linewidths[trunc], alpha=0.95, label=labels[trunc],
               zorder=z_orders[trunc] + 0.5)

    ax.set_xlabel('Predicted Probability', fontsize=12)
    ax.set_ylabel('Actual Outcome Rate', fontsize=12)
    ax.set_title('Polymarket Calibration by Truncation', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', fontsize=10, title='Truncation')
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)

    caption = "Polymarket calibration by truncation. Truncations: -48h, -24h, -12h, 0h before anchor."
    save_fig(fig, 'fig_appendix_a_pm_calibration', caption)


# ============================================================================
# APPENDIX FIGURE B: Kalshi Calibration (Platform-Specific)
# ============================================================================

def fig_appendix_k_calibration(data):
    """
    Appendix figure: Kalshi only, 4 truncation levels.
    Green color palette.
    """
    log("Appendix B: Kalshi Calibration")

    # Green shades - darker = farther truncation
    colors = {
        'conservative': '#1a4d2e',  # Dark green (-24h)
        'moderate': '#2d8a4e',      # Medium green (-12h)
        'aggressive': '#6dc48a',    # Light green (-3h)
        'resolution': '#b5e2c4',    # Very light green (0h)
    }

    labels = {
        'conservative': '-24h',
        'moderate': '-12h',
        'aggressive': '-3h',
        'resolution': '0h',
    }

    z_orders = {'conservative': 10, 'moderate': 8, 'aggressive': 6, 'resolution': 4}
    linewidths = {'conservative': 3.0, 'moderate': 2.5, 'aggressive': 2.0, 'resolution': 1.5}

    def build_df(prices_yes, prices_no, outcomes, truncation):
        """Build df for Kalshi with both YES and NO tokens. Falls back to resolution price if truncation unavailable."""
        rows = []
        # YES tokens
        for mid, pdata in prices_yes.items():
            outcome = outcomes.get(mid)
            if outcome is None:
                continue
            # Try requested truncation, fall back to resolution
            if truncation in pdata['prices']:
                price = pdata['prices'][truncation].get('spot')
            elif 'resolution' in pdata['prices']:
                price = pdata['prices']['resolution'].get('spot')
            else:
                price = None
            if price is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': outcome})
        # NO tokens
        for mid, pdata in prices_no.items():
            yes_outcome = outcomes.get(mid)
            if yes_outcome is None:
                continue
            # Try requested truncation, fall back to resolution
            if truncation in pdata['prices']:
                price = pdata['prices'][truncation].get('spot')
            elif 'resolution' in pdata['prices']:
                price = pdata['prices']['resolution'].get('spot')
            else:
                price = None
            if price is not None:
                rows.append({'price': max(0, min(1, price)), 'outcome': 1 - yes_outcome})
        return pd.DataFrame(rows)

    def compute_bins(df, num_bins=50):
        if len(df) < num_bins:
            return None, None
        df_sorted = df.sort_values('price').reset_index(drop=True)
        samples_per_bin = max(1, len(df_sorted) // num_bins)
        df_sorted['bin'] = df_sorted.index // samples_per_bin
        df_sorted.loc[df_sorted['bin'] >= num_bins, 'bin'] = num_bins - 1
        bins = df_sorted.groupby('bin').agg({'price': 'mean', 'outcome': 'mean'}).reset_index()
        return bins['price'].values, bins['outcome'].values

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.plot([0, 1], [0, 1], 'k--', linewidth=1.5, alpha=0.7, label='Perfect', zorder=15)

    truncations = ['conservative', 'moderate', 'aggressive', 'resolution']

    for trunc in reversed(truncations):
        df = build_df(data['kalshi_prices_yes'], data['kalshi_prices_no'], data['outcomes'], trunc)
        if len(df) < 50:
            continue

        log(f"    K {trunc}: {len(df):,} markets")
        pred, actual = compute_bins(df)
        if pred is None:
            continue

        scatter_alpha = 0.5 if trunc == 'conservative' else 0.25
        ax.scatter(pred, actual, s=15, alpha=scatter_alpha, color=colors[trunc], zorder=z_orders[trunc])

        x = np.linspace(0.02, 0.98, 100)
        z = np.polyfit(pred, actual, 3)
        ax.plot(x, np.clip(np.poly1d(z)(x), 0, 1), color=colors[trunc],
               linewidth=linewidths[trunc], alpha=0.95, label=labels[trunc],
               zorder=z_orders[trunc] + 0.5)

    ax.set_xlabel('Predicted Probability', fontsize=12)
    ax.set_ylabel('Actual Outcome Rate', fontsize=12)
    ax.set_title('Kalshi Calibration by Truncation', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', fontsize=10, title='Truncation')
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)

    caption = "Kalshi calibration by truncation. Truncations: -24h, -12h, -3h, 0h before anchor."
    save_fig(fig, 'fig_appendix_b_k_calibration', caption)


# ============================================================================
# FIGURE 2: Brier Score KDE
# ============================================================================

def fig02_brier_kde(data):
    """Brier score distributions comparing all truncation regimes."""
    log("Figure 2: Brier KDE")

    # Same colors as calibration figure
    colors = {
        'conservative': '#1a1a2e',
        'moderate': '#4a4a6a',
        'aggressive': '#8a8aa0',
        'resolution': '#c0c0d0',
    }

    labels = {
        'conservative': 'Conservative (PM: -48h, K: -24h)',
        'moderate': 'Moderate (PM: -24h, K: -12h)',
        'aggressive': 'Aggressive (PM: -12h, K: -3h)',
        'resolution': 'Resolution (0h)',
    }

    truncations = ['conservative', 'moderate', 'aggressive', 'resolution']

    fig, ax = plt.subplots(figsize=(10, 6))

    from scipy.stats import gaussian_kde
    x = np.linspace(0, 0.3, 200)

    for trunc in truncations:
        pm_df = build_analysis_df(data, 'pm', trunc, 'spot')
        k_df = build_analysis_df(data, 'kalshi', trunc, 'spot')
        brier = pd.concat([pm_df['brier'], k_df['brier']]).dropna()

        if len(brier) > 10:
            kde = gaussian_kde(brier.clip(0, 0.3))
            mean_brier = brier.mean()
            ax.plot(x, kde(x), color=colors[trunc], linewidth=2.5,
                   label=f'{labels[trunc]} (μ={mean_brier:.4f})')
            ax.axvline(mean_brier, color=colors[trunc], linestyle='--', linewidth=1.5, alpha=0.7)

    ax.set_xlabel('Brier Score', fontsize=12)
    ax.set_ylabel('Probability Density (area under curve = 1)', fontsize=11)
    ax.set_title('Distribution of Brier Scores by Truncation Regime', fontsize=14, fontweight='bold')
    ax.set_xlim(0, 0.3)
    ax.legend(loc='upper right', fontsize=9)
    ax.grid(True, alpha=0.3, axis='y')

    caption = (f"Distribution of Brier scores by truncation regime. "
               f"The leftward shift under resolution timing reflects mechanical convergence toward outcomes.")

    save_fig(fig, 'fig02_brier_kde', caption)


# ============================================================================
# FIGURE 3: Platform Rankings by Truncation
# ============================================================================

def fig03_platform_rankings(data):
    """Platform Brier scores under different truncation regimes."""
    log("Figure 3: Platform ranking shifts")

    truncations = ['conservative', 'moderate', 'aggressive', 'resolution']
    labels = ['Cons.\n(48h/24h)', 'Mod.\n(24h/12h)', 'Aggr.\n(12h/3h)', 'Resolution\n(0h)']

    pm_scores, pm_cis = [], []
    k_scores, k_cis = [], []

    for trunc in truncations:
        pm_df = build_analysis_df(data, 'pm', trunc, 'spot')
        k_df = build_analysis_df(data, 'kalshi', trunc, 'spot')

        pm_brier = pm_df['brier'].dropna()
        k_brier = k_df['brier'].dropna()

        pm_scores.append(pm_brier.mean() if len(pm_brier) > 0 else np.nan)
        k_scores.append(k_brier.mean() if len(k_brier) > 0 else np.nan)

        # Bootstrap CIs
        if len(pm_brier) > 10:
            boots = [np.random.choice(pm_brier, len(pm_brier), replace=True).mean() for _ in range(1000)]
            pm_cis.append((np.percentile(boots, 2.5), np.percentile(boots, 97.5)))
        else:
            pm_cis.append((np.nan, np.nan))

        if len(k_brier) > 10:
            boots = [np.random.choice(k_brier, len(k_brier), replace=True).mean() for _ in range(1000)]
            k_cis.append((np.percentile(boots, 2.5), np.percentile(boots, 97.5)))
        else:
            k_cis.append((np.nan, np.nan))

    fig, ax = plt.subplots(figsize=(8, 5))

    x = np.arange(len(truncations))

    ax.plot(x, pm_scores, 'o-', color=COLORS['polymarket'], linewidth=2, markersize=8, label='Polymarket')
    ax.fill_between(x, [c[0] for c in pm_cis], [c[1] for c in pm_cis], color=COLORS['polymarket'], alpha=0.2)

    ax.plot(x, k_scores, 's-', color=COLORS['kalshi'], linewidth=2, markersize=8, label='Kalshi')
    ax.fill_between(x, [c[0] for c in k_cis], [c[1] for c in k_cis], color=COLORS['kalshi'], alpha=0.2)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_xlabel('Truncation Regime')
    ax.set_ylabel('Mean Brier Score')
    ax.legend(loc='upper right')

    ax.yaxis.grid(True, linestyle='-', alpha=0.3, color=COLORS['light_gray'])
    ax.set_axisbelow(True)

    caption = (f"Aggregate Brier score by platform under four truncation regimes. "
               f"PM uses 48h/24h/12h offsets; Kalshi uses 24h/12h/3h. "
               f"Shaded bands show 95% bootstrap CIs.")

    save_fig(fig, 'fig03_platform_ranking_shifts', caption)


# ============================================================================
# FIGURE 3B: Brier Score Table by Category and Platform
# ============================================================================

def fig03b_category_table(data):
    """Table of mean Brier scores by category, platform, and truncation."""
    log("Figure 3b: Category Brier Table")

    truncations = ['conservative', 'moderate', 'aggressive', 'resolution']
    trunc_labels = ['Conservative', 'Moderate', 'Aggressive', 'Resolution']

    # Collect data by category
    results = {}

    for platform in ['pm', 'kalshi']:
        for trunc in truncations:
            df = build_analysis_df(data, platform, trunc, 'spot')

            # By category
            for cat in df['category'].unique():
                if cat not in results:
                    results[cat] = {}
                key = f"{platform}_{trunc}"
                cat_brier = df[df['category'] == cat]['brier'].dropna()
                results[cat][key] = cat_brier.mean() if len(cat_brier) > 0 else np.nan
                results[cat][f"{key}_n"] = len(cat_brier)

            # Total
            if 'TOTAL' not in results:
                results['TOTAL'] = {}
            key = f"{platform}_{trunc}"
            results['TOTAL'][key] = df['brier'].dropna().mean()
            results['TOTAL'][f"{key}_n"] = len(df['brier'].dropna())

    # Sort categories by total n (exclude PARTISAN_CONTROL)
    cat_order = sorted([c for c in results.keys() if c != 'TOTAL' and 'PARTISAN_CONTROL' not in c.upper()],
                       key=lambda c: sum(results[c].get(f"pm_{t}_n", 0) + results[c].get(f"kalshi_{t}_n", 0)
                                        for t in truncations), reverse=True)

    # Build table data and track winners
    table_data = []
    row_labels = []
    winner_map = []  # Track which platform wins for each cell

    for cat in cat_order + ['TOTAL']:
        row = []
        row_winners = []
        for trunc in truncations:
            pm_val = results[cat].get(f"pm_{trunc}", np.nan)
            k_val = results[cat].get(f"kalshi_{trunc}", np.nan)

            # Determine winner for this truncation
            if not np.isnan(pm_val) and not np.isnan(k_val):
                if pm_val < k_val:
                    winner = 'pm'
                elif k_val < pm_val:
                    winner = 'kalshi'
                else:
                    winner = 'tie'
            else:
                winner = 'none'

            row_winners.append(winner)

            # Format values
            pm_str = f"{pm_val:.4f}" if not np.isnan(pm_val) else "-"
            k_str = f"{k_val:.4f}" if not np.isnan(k_val) else "-"
            row.append(pm_str)
            row.append(k_str)

        table_data.append(row)
        winner_map.append(row_winners)

        # Clean up category name - remove leading numbers and underscores
        if cat != 'TOTAL':
            import re
            label = re.sub(r'^\d+\.\s*', '', cat)  # Remove leading "1. " etc.
            label = label.replace('_', ' ').title()
        else:
            label = 'TOTAL'
        if len(label) > 25:
            label = label[:23] + '...'
        row_labels.append(label)

    # Create figure with table
    fig, ax = plt.subplots(figsize=(16, max(6, len(row_labels) * 0.4)))
    ax.axis('off')

    # Column headers - grouped by truncation
    col_labels = []
    for tl in trunc_labels:
        col_labels.append(f"Polymarket\n{tl}")
        col_labels.append(f"Kalshi\n{tl}")

    table = ax.table(
        cellText=table_data,
        rowLabels=row_labels,
        colLabels=col_labels,
        cellLoc='center',
        rowLoc='right',
        loc='center'
    )

    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.1, 1.5)

    # Colors for winners
    pm_win_color = '#d4e6f1'   # Light blue for PM wins
    k_win_color = '#d5f5e3'    # Light green for Kalshi wins

    # Style header row
    for j in range(len(col_labels)):
        table[(0, j)].set_facecolor('#E6E6E6')
        table[(0, j)].set_text_props(fontweight='bold')

    # Color cells based on winner
    for i, row_winners in enumerate(winner_map):
        row_idx = i + 1  # +1 for header row
        for j, winner in enumerate(row_winners):
            pm_col = j * 2
            k_col = j * 2 + 1

            if winner == 'pm':
                table[(row_idx, pm_col)].set_facecolor(pm_win_color)
                table[(row_idx, pm_col)].set_text_props(fontweight='bold')
            elif winner == 'kalshi':
                table[(row_idx, k_col)].set_facecolor(k_win_color)
                table[(row_idx, k_col)].set_text_props(fontweight='bold')

    # Style TOTAL row
    total_row_idx = len(row_labels)
    for j in range(-1, len(col_labels)):
        cell = table[(total_row_idx, j)]
        current_color = cell.get_facecolor()
        # Keep winner highlighting but make text bold
        cell.set_text_props(fontweight='bold')

    ax.set_title('Mean Brier Score by Category and Truncation\n(Lower is better; highlighted = winner)',
                 fontsize=13, fontweight='bold', pad=20)

    caption = "Mean Brier scores by category. Blue highlight = Polymarket wins, Green = Kalshi wins."
    save_fig(fig, 'fig03b_category_table', caption)


# ============================================================================
# FIGURE 4: Spot vs VWAP Comparison
# ============================================================================

def fig04_spot_vs_vwap(data):
    """Compare spot vs VWAP with different window sizes at aggressive truncation."""
    log("Figure 4: Spot vs VWAP comparison")

    # Load trade data to compute VWAP with different windows
    with open(PM_TRADES_FILE, 'r') as f:
        pm_trade_data = json.load(f)

    # VWAP windows to compare (in hours)
    vwap_windows = [1, 6, 12, 24]

    # Get YES token lookup
    yes_tokens = data['yes_tokens']
    outcomes = data['outcomes']

    # Build market data lookup
    market_trades = {}
    for market in pm_trade_data['markets']:
        mid = str(market['market_id'])
        market_trades[mid] = market

    # Compute prices at aggressive truncation with different VWAP windows
    results = {w: [] for w in vwap_windows}

    pm_prices = data['pm_prices_yes']
    for mid, pdata in pm_prices.items():
        if 'aggressive' not in pdata['prices']:
            continue

        spot = pdata['prices']['aggressive'].get('spot')
        outcome = outcomes.get(mid)

        if spot is None or outcome is None:
            continue

        # Get trades for this market
        if mid not in market_trades:
            continue

        market = market_trades[mid]
        anchor_ts = market.get('anchor_ts')
        raw_trades = market.get('trades', [])

        if not anchor_ts or not raw_trades:
            continue

        # Filter to YES token trades
        yes_token_id = yes_tokens.get(mid)
        trades = [t for t in raw_trades if t.get('token_id') == yes_token_id]

        # Cutoff for aggressive truncation (PM: -12h)
        cutoff_ts = anchor_ts + (-12 * 3600)

        # Compute VWAP for each window
        for window_hours in vwap_windows:
            vwap_start = cutoff_ts - (window_hours * 3600)
            valid = [t for t in trades
                     if t.get('timestamp') is not None
                     and vwap_start <= t['timestamp'] <= cutoff_ts
                     and t['price'] is not None
                     and t.get('shares') is not None
                     and t['shares'] > 0]

            if valid:
                total_value = sum(t['price'] * t['shares'] for t in valid)
                total_volume = sum(t['shares'] for t in valid)
                if total_volume > 0:
                    vwap = total_value / total_volume
                    vwap = max(0, min(1, vwap))
                    spot_clipped = max(0, min(1, spot))
                    results[window_hours].append({
                        'spot': spot_clipped,
                        'vwap': vwap,
                        'outcome': outcome
                    })

    # Create 2x2 figure
    fig, axes = plt.subplots(2, 2, figsize=(10, 10))
    axes = axes.flatten()

    for i, window_hours in enumerate(vwap_windows):
        ax = axes[i]
        df = pd.DataFrame(results[window_hours])

        if len(df) > 0:
            ax.scatter(df['spot'], df['vwap'], alpha=0.3, s=10, color='#2563eb')
            ax.plot([0, 1], [0, 1], '--', color=COLORS['gray'], linewidth=1)

            # Stats
            corr = df['spot'].corr(df['vwap'])
            mae = np.mean(np.abs(df['spot'] - df['vwap']))
            ax.text(0.05, 0.95, f'r = {corr:.3f}\nMAE = {mae:.4f}\nn = {len(df):,}',
                   transform=ax.transAxes, va='top', fontsize=10,
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

        ax.set_xlabel('Spot Price', fontsize=10)
        ax.set_ylabel(f'VWAP ({window_hours}h)', fontsize=10)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_aspect('equal')
        ax.set_title(f'Spot vs VWAP ({window_hours}h window)', fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.3)

    plt.suptitle('Spot vs VWAP Price Comparison (Aggressive Truncation: PM -12h)',
                 fontsize=13, fontweight='bold', y=1.02)
    plt.tight_layout()

    log(f"    {len(results[24]):,} markets with spot and VWAP data")

    caption = (f"Comparison of spot vs VWAP with different window sizes at aggressive truncation. "
               f"Longer VWAP windows smooth more noise but may lag price movements.")

    save_fig(fig, 'fig04_spot_vs_vwap', caption)


# ============================================================================
# FIGURE 4B: Brier Score by Price Construction Method
# ============================================================================

def fig04b_brier_by_price_method(data):
    """Compare Brier scores across different price construction methods."""
    log("Figure 4b: Brier by Price Method")

    # Load trade data to compute VWAP with different windows
    with open(PM_TRADES_FILE, 'r') as f:
        pm_trade_data = json.load(f)

    # Price methods: Midpoint, Spot, VWAP(1h), VWAP(6h), VWAP(12h), VWAP(24h)
    vwap_windows = [1, 6, 12, 24]
    methods = ['Midpoint', 'Spot'] + [f'VWAP ({w}h)' for w in vwap_windows]

    # Get lookups
    yes_tokens = data['yes_tokens']
    outcomes = data['outcomes']
    pm_midpoints = data.get('pm_midpoints', {})

    # Build market data lookup
    market_trades = {}
    for market in pm_trade_data['markets']:
        mid = str(market['market_id'])
        market_trades[mid] = market

    # Compute Brier scores for each method
    brier_scores = {m: [] for m in methods}

    pm_prices = data['pm_prices_yes']
    for mid, pdata in pm_prices.items():
        if 'aggressive' not in pdata['prices']:
            continue

        spot = pdata['prices']['aggressive'].get('spot')
        outcome = outcomes.get(mid)

        if spot is None or outcome is None:
            continue

        # Get trades for this market
        if mid not in market_trades:
            continue

        market = market_trades[mid]
        anchor_ts = market.get('anchor_ts')
        raw_trades = market.get('trades', [])

        if not anchor_ts or not raw_trades:
            continue

        # Filter to YES token trades
        yes_token_id = yes_tokens.get(mid)
        trades = [t for t in raw_trades if t.get('token_id') == yes_token_id]

        # Cutoff for aggressive truncation (PM: -12h)
        cutoff_ts = anchor_ts + (-12 * 3600)

        # Midpoint Brier (if available)
        if mid in pm_midpoints and 'aggressive' in pm_midpoints[mid]:
            midpoint = pm_midpoints[mid]['aggressive']
            if midpoint is not None:
                midpoint_clipped = max(0, min(1, midpoint))
                brier_scores['Midpoint'].append((midpoint_clipped - outcome) ** 2)

        # Spot Brier
        spot_clipped = max(0, min(1, spot))
        brier_scores['Spot'].append((spot_clipped - outcome) ** 2)

        # VWAP Brier for each window
        for window_hours in vwap_windows:
            vwap_start = cutoff_ts - (window_hours * 3600)
            valid = [t for t in trades
                     if t.get('timestamp') is not None
                     and vwap_start <= t['timestamp'] <= cutoff_ts
                     and t['price'] is not None
                     and t.get('shares') is not None
                     and t['shares'] > 0]

            if valid:
                total_value = sum(t['price'] * t['shares'] for t in valid)
                total_volume = sum(t['shares'] for t in valid)
                if total_volume > 0:
                    vwap = total_value / total_volume
                    vwap = max(0, min(1, vwap))
                    brier_scores[f'VWAP ({window_hours}h)'].append((vwap - outcome) ** 2)

    # Compute means and bootstrap CIs
    means = []
    cis = []
    ns = []

    for method in methods:
        scores = brier_scores[method]
        if len(scores) > 0:
            means.append(np.mean(scores))
            ns.append(len(scores))
            # Bootstrap CI
            boots = [np.mean(np.random.choice(scores, len(scores), replace=True)) for _ in range(1000)]
            cis.append((np.percentile(boots, 2.5), np.percentile(boots, 97.5)))
        else:
            means.append(np.nan)
            ns.append(0)
            cis.append((np.nan, np.nan))

    # Create bar chart
    fig, ax = plt.subplots(figsize=(10, 6))

    x = np.arange(len(methods))
    colors = ['#9333ea', '#2563eb'] + ['#10b981'] * len(vwap_windows)  # Purple for midpoint, Blue for spot, green for VWAP

    bars = ax.bar(x, means, color=colors, alpha=0.7, edgecolor='none')

    # Error bars
    ci_lower = [m - ci[0] for m, ci in zip(means, cis)]
    ci_upper = [ci[1] - m for m, ci in zip(means, cis)]
    ax.errorbar(x, means, yerr=[ci_lower, ci_upper], fmt='none', color='black', capsize=5)

    ax.set_xticks(x)
    ax.set_xticklabels(methods, fontsize=11)
    ax.set_ylabel('Mean Brier Score', fontsize=12)
    ax.set_title('Brier Score by Price Construction Method\n(Aggressive Truncation: PM -12h)',
                 fontsize=13, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')

    # Add value labels on bars
    for i, (bar, mean, n) in enumerate(zip(bars, means, ns)):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001,
                f'{mean:.4f}', ha='center', va='bottom', fontsize=10)

    # Add n at bottom
    ax.text(0.02, 0.02, f'n = {ns[0]:,} markets', transform=ax.transAxes,
            fontsize=10, va='bottom')

    plt.tight_layout()

    caption = (f"Mean Brier score by price construction method at aggressive truncation. "
               f"Error bars show 95% bootstrap CIs.")

    save_fig(fig, 'fig04b_brier_by_price_method', caption)


# ============================================================================
# FIGURE 4C: Brier Score Table by Price Method, Category, and Platform
# ============================================================================

def fig04c_price_method_table(data):
    """Table of mean Brier scores by price method, category, and platform."""
    log("Figure 4c: Price Method Brier Table")

    # Load trade data
    with open(PM_TRADES_FILE, 'r') as f:
        pm_trade_data = json.load(f)
    with open(KALSHI_TRADES_FILE, 'r') as f:
        k_trade_data = json.load(f)

    price_methods = ['Midpoint', 'Spot', 'VWAP 1h', 'VWAP 6h', 'VWAP 12h', 'VWAP 24h']
    vwap_windows = {'VWAP 1h': 1, 'VWAP 6h': 6, 'VWAP 12h': 12, 'VWAP 24h': 24}

    # Get lookups
    yes_tokens = data['yes_tokens']
    outcomes = data['outcomes']
    categories = data['categories']
    pm_midpoints = data.get('pm_midpoints', {})
    kalshi_midpoints = data.get('kalshi_midpoints', {})


    # Build market trade lookups
    pm_market_trades = {str(m['market_id']): m for m in pm_trade_data['markets']}
    k_market_trades = {str(m['market_id']): m for m in k_trade_data['markets']}

    # Collect Brier scores by category, platform, and price method
    pm_prices = data['pm_prices_yes']
    results = {}

    # Process Polymarket
    for mid, pdata in pm_prices.items():
        if 'aggressive' not in pdata['prices']:
            continue

        spot = pdata['prices']['aggressive'].get('spot')
        outcome = outcomes.get(mid)
        cat = categories.get(mid, 'Unknown')

        if spot is None or outcome is None:
            continue

        if mid not in pm_market_trades:
            continue

        market = pm_market_trades[mid]
        anchor_ts = market.get('anchor_ts')
        raw_trades = market.get('trades', [])

        if not anchor_ts or not raw_trades:
            continue

        yes_token_id = yes_tokens.get(mid)
        trades = [t for t in raw_trades if t.get('token_id') == yes_token_id]
        cutoff_ts = anchor_ts + (-12 * 3600)  # Aggressive: -12h

        if cat not in results:
            results[cat] = {}

        # Midpoint (if available)
        if mid in pm_midpoints and 'aggressive' in pm_midpoints[mid]:
            midpoint = pm_midpoints[mid]['aggressive']
            if midpoint is not None:
                midpoint_clipped = max(0, min(1, midpoint))
                key = ('pm', 'Midpoint')
                if key not in results[cat]:
                    results[cat][key] = []
                results[cat][key].append((midpoint_clipped - outcome) ** 2)

        # Spot
        spot_clipped = max(0, min(1, spot))
        key = ('pm', 'Spot')
        if key not in results[cat]:
            results[cat][key] = []
        results[cat][key].append((spot_clipped - outcome) ** 2)

        # VWAP methods
        for method, window_hours in vwap_windows.items():
            vwap_start = cutoff_ts - (window_hours * 3600)
            valid = [t for t in trades
                     if t.get('timestamp') is not None
                     and vwap_start <= t['timestamp'] <= cutoff_ts
                     and t['price'] is not None
                     and t.get('shares') is not None
                     and t['shares'] > 0]

            if valid:
                total_value = sum(t['price'] * t['shares'] for t in valid)
                total_volume = sum(t['shares'] for t in valid)
                if total_volume > 0:
                    vwap = max(0, min(1, total_value / total_volume))
                    key = ('pm', method)
                    if key not in results[cat]:
                        results[cat][key] = []
                    results[cat][key].append((vwap - outcome) ** 2)

    # Process Kalshi
    k_prices = data['kalshi_prices_yes']
    for mid, pdata in k_prices.items():
        if 'aggressive' not in pdata['prices']:
            continue

        spot = pdata['prices']['aggressive'].get('spot')
        outcome = outcomes.get(mid)
        cat = categories.get(mid, 'Unknown')

        if spot is None or outcome is None:
            continue

        if mid not in k_market_trades:
            continue

        market = k_market_trades[mid]
        anchor_ts = market.get('anchor_ts')
        raw_trades = market.get('trades', [])

        if not anchor_ts or not raw_trades:
            continue

        # Kalshi trades format
        trades = []
        for t in raw_trades:
            price = t.get('yes_price')
            if price is not None:
                price = price / 100.0
            ts = t.get('created_time')
            if isinstance(ts, str):
                ts = int(ts) if ts.isdigit() else None
            trades.append({'price': price, 'shares': t.get('count', 0), 'timestamp': ts})

        cutoff_ts = anchor_ts + (-3 * 3600)  # Aggressive for Kalshi: -3h

        if cat not in results:
            results[cat] = {}

        # Midpoint (if available)
        if mid in kalshi_midpoints and 'aggressive' in kalshi_midpoints[mid]:
            midpoint = kalshi_midpoints[mid]['aggressive']
            if midpoint is not None:
                midpoint_clipped = max(0, min(1, midpoint))
                key = ('kalshi', 'Midpoint')
                if key not in results[cat]:
                    results[cat][key] = []
                results[cat][key].append((midpoint_clipped - outcome) ** 2)

        # Spot
        spot_clipped = max(0, min(1, spot))
        key = ('kalshi', 'Spot')
        if key not in results[cat]:
            results[cat][key] = []
        results[cat][key].append((spot_clipped - outcome) ** 2)

        # VWAP methods
        for method, window_hours in vwap_windows.items():
            vwap_start = cutoff_ts - (window_hours * 3600)
            valid = [t for t in trades
                     if t.get('timestamp') is not None
                     and vwap_start <= t['timestamp'] <= cutoff_ts
                     and t['price'] is not None
                     and t.get('shares') is not None
                     and t['shares'] > 0]

            if valid:
                total_value = sum(t['price'] * t['shares'] for t in valid)
                total_volume = sum(t['shares'] for t in valid)
                if total_volume > 0:
                    vwap = max(0, min(1, total_value / total_volume))
                    key = ('kalshi', method)
                    if key not in results[cat]:
                        results[cat][key] = []
                    results[cat][key].append((vwap - outcome) ** 2)

    # Compute totals
    results['TOTAL'] = {}
    for cat in results:
        if cat == 'TOTAL':
            continue
        for key, scores in results[cat].items():
            if key not in results['TOTAL']:
                results['TOTAL'][key] = []
            results['TOTAL'][key].extend(scores)

    # Sort categories by total n (exclude PARTISAN_CONTROL)
    cat_order = sorted([c for c in results.keys() if c != 'TOTAL' and 'PARTISAN_CONTROL' not in c.upper()],
                       key=lambda c: sum(len(results[c].get(('pm', m), [])) + len(results[c].get(('kalshi', m), []))
                                        for m in price_methods), reverse=True)

    # Build table data
    table_data = []
    row_labels = []
    winner_map = []

    for cat in cat_order + ['TOTAL']:
        row = []
        row_winners = []

        for method in price_methods:
            pm_scores = results[cat].get(('pm', method), [])
            k_scores = results[cat].get(('kalshi', method), [])

            pm_val = np.mean(pm_scores) if len(pm_scores) > 0 else np.nan
            k_val = np.mean(k_scores) if len(k_scores) > 0 else np.nan

            # Determine winner
            if not np.isnan(pm_val) and not np.isnan(k_val):
                if pm_val < k_val:
                    winner = 'pm'
                elif k_val < pm_val:
                    winner = 'kalshi'
                else:
                    winner = 'tie'
            else:
                winner = 'none'

            row_winners.append(winner)

            pm_str = f"{pm_val:.4f}" if not np.isnan(pm_val) else "-"
            k_str = f"{k_val:.4f}" if not np.isnan(k_val) else "-"
            row.append(pm_str)
            row.append(k_str)

        table_data.append(row)
        winner_map.append(row_winners)

        # Clean category name
        import re
        if cat != 'TOTAL':
            label = re.sub(r'^\d+\.\s*', '', cat)
            label = label.replace('_', ' ').title()
        else:
            label = 'TOTAL'
        if len(label) > 25:
            label = label[:23] + '...'
        row_labels.append(label)

    # Create figure
    fig, ax = plt.subplots(figsize=(16, max(6, len(row_labels) * 0.4)))
    ax.axis('off')

    # Column headers
    col_labels = []
    for method in price_methods:
        col_labels.append(f"Polymarket\n{method}")
        col_labels.append(f"Kalshi\n{method}")

    table = ax.table(
        cellText=table_data,
        rowLabels=row_labels,
        colLabels=col_labels,
        cellLoc='center',
        rowLoc='right',
        loc='center'
    )

    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1.1, 1.5)

    # Colors
    pm_win_color = '#d4e6f1'
    k_win_color = '#d5f5e3'

    # Style header row
    for j in range(len(col_labels)):
        table[(0, j)].set_facecolor('#E6E6E6')
        table[(0, j)].set_text_props(fontweight='bold')

    # Color cells based on winner
    for i, row_winners in enumerate(winner_map):
        row_idx = i + 1
        for j, winner in enumerate(row_winners):
            pm_col = j * 2
            k_col = j * 2 + 1

            if winner == 'pm':
                table[(row_idx, pm_col)].set_facecolor(pm_win_color)
                table[(row_idx, pm_col)].set_text_props(fontweight='bold')
            elif winner == 'kalshi':
                table[(row_idx, k_col)].set_facecolor(k_win_color)
                table[(row_idx, k_col)].set_text_props(fontweight='bold')

    # Style TOTAL row
    total_row_idx = len(row_labels)
    for j in range(-1, len(col_labels)):
        table[(total_row_idx, j)].set_text_props(fontweight='bold')

    ax.set_title('Mean Brier Score by Price Method and Category\n(Aggressive Truncation; Lower is better; highlighted = winner)',
                 fontsize=13, fontweight='bold', pad=20)

    caption = "Mean Brier scores by price method and category. Blue = Polymarket wins, Green = Kalshi wins."
    save_fig(fig, 'fig04c_price_method_table', caption)


# ============================================================================
# FIGURE 5: Volume CDF
# ============================================================================

def fig05_volume_cdf(data):
    """CDF of market volume by platform."""
    log("Figure 5: Volume CDF")

    volumes = data['volumes']
    platforms = data['platforms']

    # Split by platform
    pm_volumes = [v for mid, v in volumes.items() if v > 0 and platforms.get(mid) == 'polymarket']
    k_volumes = [v for mid, v in volumes.items() if v > 0 and platforms.get(mid) == 'kalshi']

    log(f"    PM: {len(pm_volumes):,} markets, Kalshi: {len(k_volumes):,} markets")

    fig, ax = plt.subplots(figsize=(8, 5))

    # Plot PM CDF
    pm_sorted = np.sort(pm_volumes)
    pm_cdf = np.arange(1, len(pm_sorted) + 1) / len(pm_sorted)
    ax.plot(pm_sorted, pm_cdf, color=COLORS['polymarket'], linewidth=2.5, label='Polymarket')

    # Plot Kalshi CDF
    k_sorted = np.sort(k_volumes)
    k_cdf = np.arange(1, len(k_sorted) + 1) / len(k_sorted)
    ax.plot(k_sorted, k_cdf, color=COLORS['kalshi'], linewidth=2.5, label='Kalshi')

    ax.set_xscale('log')
    ax.set_xlabel('Total Traded Volume (USD)')
    ax.set_ylabel('Cumulative Fraction of Markets')
    ax.set_title('Distribution of Historical Trading Volume')

    # Threshold lines
    for thresh, label in [(10000, '$10K'), (100000, '$100K')]:
        ax.axvline(thresh, color=COLORS['gray'], linestyle='--', linewidth=1, alpha=0.7)
        ax.text(thresh * 1.1, 0.05, label, fontsize=9, va='bottom', color=COLORS['gray'])

    # Stats for each platform
    pm_frac_10k = np.mean(np.array(pm_sorted) >= 10000)
    pm_frac_100k = np.mean(np.array(pm_sorted) >= 100000)
    k_frac_10k = np.mean(np.array(k_sorted) >= 10000)
    k_frac_100k = np.mean(np.array(k_sorted) >= 100000)

    stats_text = (f"≥$10K: PM {pm_frac_10k:.1%}, K {k_frac_10k:.1%}\n"
                  f"≥$100K: PM {pm_frac_100k:.1%}, K {k_frac_100k:.1%}")
    ax.text(0.98, 0.25, stats_text, transform=ax.transAxes, fontsize=9,
           va='bottom', ha='right', bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='none'))

    ax.legend(loc='lower right')

    caption = (f"Cumulative distribution of total traded volume (distinct from orderbook depth). "
               f"PM: {len(pm_volumes):,} markets, Kalshi: {len(k_volumes):,} markets.")

    save_fig(fig, 'fig05_volume_cdf', caption)


# ============================================================================
# FIGURE 5b: Log-Log Rank Plot (Volume Concentration)
# ============================================================================

def fig05b_lorenz_curve(data):
    """Log-log rank plot showing power law distribution of volume."""
    log("Figure 5b: Log-Log Rank Plot (Volume Concentration)")

    volumes = data['volumes']
    platforms = data['platforms']

    # Build volume arrays by platform
    pm_vols = np.array(sorted([v for mid, v in volumes.items()
                                if v > 0 and platforms.get(mid) == 'polymarket'], reverse=True))
    k_vols = np.array(sorted([v for mid, v in volumes.items()
                               if v > 0 and platforms.get(mid) == 'kalshi'], reverse=True))

    pm_ranks = np.arange(1, len(pm_vols) + 1)
    k_ranks = np.arange(1, len(k_vols) + 1)

    # Compute concentration stats
    pm_total = pm_vols.sum()
    k_total = k_vols.sum()
    pm_top1_n = max(1, len(pm_vols) // 100)
    k_top1_n = max(1, len(k_vols) // 100)
    pm_top10_n = max(1, len(pm_vols) // 10)
    k_top10_n = max(1, len(k_vols) // 10)

    pm_top1_share = pm_vols[:pm_top1_n].sum() / pm_total * 100
    k_top1_share = k_vols[:k_top1_n].sum() / k_total * 100
    pm_top10_share = pm_vols[:pm_top10_n].sum() / pm_total * 100
    k_top10_share = k_vols[:k_top10_n].sum() / k_total * 100

    fig, ax = plt.subplots(figsize=(10, 6))

    # Title with key statistic
    title = f"Top 1% of Markets Account for {pm_top1_share:.0f}% of PM Volume and {k_top1_share:.0f}% of Kalshi Volume"
    ax.set_title(title, fontsize=11, fontweight='bold', pad=15)

    # Plot rank vs volume on log-log
    ax.scatter(pm_ranks, pm_vols, s=12, alpha=0.4, color=COLORS['polymarket'],
               label=f'Polymarket ({len(pm_vols):,} markets)', edgecolors='none')
    ax.scatter(k_ranks, k_vols, s=12, alpha=0.4, color=COLORS['kalshi'],
               label=f'Kalshi ({len(k_vols):,} markets)', edgecolors='none')

    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Market Rank (1 = highest volume)')
    ax.set_ylabel('Volume (USD)')

    # Clean grid
    ax.grid(True, alpha=0.2, which='major', linestyle='-')
    ax.grid(True, alpha=0.1, which='minor', linestyle='-')

    # Legend in upper right, outside the data
    ax.legend(loc='upper right', frameon=True, fancybox=False,
              edgecolor='#cccccc', fontsize=9)

    # Add reference lines for top 1% and top 10%
    ax.axvline(pm_top1_n, color=COLORS['polymarket'], linestyle='--', linewidth=1, alpha=0.5)
    ax.axvline(pm_top10_n, color=COLORS['polymarket'], linestyle=':', linewidth=1, alpha=0.5)

    # Stats box - cleaner positioning in lower left
    stats_text = (f"Concentration:\n"
                  f"Top 1%:   PM {pm_top1_share:.0f}%  |  K {k_top1_share:.0f}%\n"
                  f"Top 10%: PM {pm_top10_share:.0f}%  |  K {k_top10_share:.0f}%")
    ax.text(0.98, 0.02, stats_text, transform=ax.transAxes, fontsize=9,
           va='bottom', ha='right', family='monospace',
           bbox=dict(boxstyle='round,pad=0.4', facecolor='white', alpha=0.9, edgecolor='#cccccc'))

    caption = (f"Log-log rank plot showing power law distribution of volume. "
               f"Top 1% of PM markets = {pm_top1_share:.0f}% of volume; "
               f"Top 1% of Kalshi = {k_top1_share:.0f}% of volume.")

    save_fig(fig, 'fig05b_volume_rank', caption)


# ============================================================================
# FIGURE 6: Brier by Volume Threshold
# ============================================================================

def fig06_brier_by_threshold(data):
    """Aggregate Brier as function of volume threshold."""
    log("Figure 6: Brier by threshold")

    thresholds = [0, 1000, 10000, 100000]

    cons_brier, cons_n = [], []
    res_brier, res_n = [], []

    for thresh in thresholds:
        pm_cons = build_analysis_df(data, 'pm', 'conservative', 'spot')
        pm_res = build_analysis_df(data, 'pm', 'resolution', 'spot')
        k_cons = build_analysis_df(data, 'kalshi', 'conservative', 'spot')
        k_res = build_analysis_df(data, 'kalshi', 'resolution', 'spot')

        cons_df = pd.concat([pm_cons, k_cons])
        res_df = pd.concat([pm_res, k_res])

        cons_filtered = cons_df[cons_df['volume'] >= thresh]['brier'].dropna()
        res_filtered = res_df[res_df['volume'] >= thresh]['brier'].dropna()

        cons_brier.append(cons_filtered.mean() if len(cons_filtered) > 0 else np.nan)
        cons_n.append(len(cons_filtered))
        res_brier.append(res_filtered.mean() if len(res_filtered) > 0 else np.nan)
        res_n.append(len(res_filtered))

    fig, ax = plt.subplots(figsize=(8, 5))

    x = np.arange(len(thresholds))

    ax.plot(x, cons_brier, 'o-', color=COLORS['conservative'], linewidth=2, markersize=8,
           label='Conservative')
    ax.plot(x, res_brier, 's-', color=COLORS['resolution'], linewidth=2, markersize=8,
           label='Resolution')

    for i, n in enumerate(cons_n):
        ax.annotate(f'n={n:,}', (x[i], cons_brier[i]), xytext=(0, 10),
                   textcoords='offset points', ha='center', fontsize=8, color=COLORS['gray'])

    ax.set_xticks(x)
    ax.set_xticklabels(['$0', '$1K', '$10K', '$100K'])
    ax.set_xlabel('Minimum Traded Volume Threshold')
    ax.set_ylabel('Mean Brier Score')
    ax.set_title('Accuracy by Sample Selection Threshold')
    ax.legend(loc='upper right')

    ax.yaxis.grid(True, linestyle='-', alpha=0.3, color=COLORS['light_gray'])
    ax.set_axisbelow(True)

    caption = (f"Aggregate Brier score by traded volume threshold (sample selection criterion). "
               f"Higher thresholds reduce sample size but may improve signal quality.")

    save_fig(fig, 'fig06_brier_by_threshold', caption)


# ============================================================================
# FIGURE 6b: Volume Threshold Table by Category
# ============================================================================

def fig06b_threshold_table(data):
    """Table showing Brier by volume threshold, category, and platform."""
    log("Figure 6b: Volume Threshold Table")

    thresholds = [0, 1000, 10000, 100000]
    threshold_labels = ['$0', '$1K', '$10K', '$100K']

    # Get unique categories
    pm_df = build_analysis_df(data, 'pm', 'aggressive', 'spot')
    k_df = build_analysis_df(data, 'kalshi', 'aggressive', 'spot')

    # Clean category names
    def clean_cat(x):
        if pd.isna(x):
            return 'Other'
        x = str(x)
        import re
        x = re.sub(r'^\d+\.\s*', '', x)
        return x.replace('_', ' ').title()[:25]

    pm_df['category_clean'] = pm_df['category'].apply(clean_cat)
    k_df['category_clean'] = k_df['category'].apply(clean_cat)

    all_cats = sorted(set(pm_df['category_clean'].unique()) | set(k_df['category_clean'].unique()))
    # Filter to categories with enough data (exclude PARTISAN_CONTROL)
    all_cats = [c for c in all_cats if c != 'Other' and 'Partisan Control' not in c and
                (len(pm_df[pm_df['category_clean'] == c]) >= 20 or
                 len(k_df[k_df['category_clean'] == c]) >= 20)]

    # Build table data
    table_data = []
    for cat in all_cats + ['TOTAL']:
        row = [cat]
        for thresh in thresholds:
            if cat == 'TOTAL':
                pm_filtered = pm_df[pm_df['volume'] >= thresh]['brier']
                k_filtered = k_df[k_df['volume'] >= thresh]['brier']
            else:
                pm_filtered = pm_df[(pm_df['category_clean'] == cat) & (pm_df['volume'] >= thresh)]['brier']
                k_filtered = k_df[(k_df['category_clean'] == cat) & (k_df['volume'] >= thresh)]['brier']

            pm_brier = pm_filtered.mean() if len(pm_filtered) >= 10 else np.nan
            k_brier = k_filtered.mean() if len(k_filtered) >= 10 else np.nan
            row.extend([pm_brier, k_brier])
        table_data.append(row)

    # Create figure
    fig, ax = plt.subplots(figsize=(14, len(table_data) * 0.4 + 1.5))
    ax.axis('off')

    # Column headers
    col_labels = ['Category']
    for label in threshold_labels:
        col_labels.extend([f'{label}\nPM', f'{label}\nK'])

    # Create table
    table = ax.table(
        cellText=[[row[0]] + [f'{v:.3f}' if not np.isnan(v) else '—' for v in row[1:]] for row in table_data],
        colLabels=col_labels,
        loc='center',
        cellLoc='center'
    )

    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.5)

    # Style header
    for j in range(len(col_labels)):
        table[(0, j)].set_facecolor('#E8E8E8')
        table[(0, j)].set_text_props(fontweight='bold')

    # Color code cells (blue = PM wins, green = Kalshi wins)
    for i, row in enumerate(table_data):
        # Style TOTAL row
        if row[0] == 'TOTAL':
            for j in range(len(col_labels)):
                table[(i + 1, j)].set_facecolor('#F0F0F0')
                table[(i + 1, j)].set_text_props(fontweight='bold')

        # Color code PM vs K comparisons for each threshold
        for t_idx in range(len(thresholds)):
            pm_val = row[1 + t_idx * 2]
            k_val = row[2 + t_idx * 2]

            if not np.isnan(pm_val) and not np.isnan(k_val):
                pm_col = 1 + t_idx * 2
                k_col = 2 + t_idx * 2

                if pm_val < k_val:  # PM wins (lower Brier is better)
                    table[(i + 1, pm_col)].set_facecolor('#D4E6F1')  # Light blue
                elif k_val < pm_val:  # Kalshi wins
                    table[(i + 1, k_col)].set_facecolor('#D5F5E3')  # Light green

    ax.set_title('Brier Score by Volume Threshold, Category, and Platform\n(Blue = PM better, Green = Kalshi better)',
                fontsize=11, fontweight='bold', pad=20)

    caption = "Brier scores by volume threshold, category, and platform. Lower is better."
    save_fig(fig, 'fig06b_threshold_table', caption)


# ============================================================================
# FIGURE 7: Accuracy by Category
# ============================================================================

def fig07_accuracy_by_category(data):
    """Brier by political category."""
    log("Figure 7: Accuracy by category")

    pm_cons = build_analysis_df(data, 'pm', 'conservative', 'spot')
    k_cons = build_analysis_df(data, 'kalshi', 'conservative', 'spot')

    combined = pd.concat([pm_cons, k_cons])

    by_cat = combined.groupby('category')['brier'].agg(['mean', 'count', 'std']).reset_index()
    by_cat = by_cat[by_cat['count'] >= 50]
    by_cat = by_cat.sort_values('mean')

    # Clean names
    def clean_name(x):
        if pd.isna(x):
            return 'Other'
        x = str(x)
        if '. ' in x:
            x = x.split('. ', 1)[1]
        return x.replace('_', ' ').title()[:20]

    by_cat['clean_name'] = by_cat['category'].apply(clean_name)

    overall_mean = combined['brier'].mean()

    fig, ax = plt.subplots(figsize=(8, 6))

    y_pos = np.arange(len(by_cat))

    colors = []
    for _, row in by_cat.iterrows():
        se = row['std'] / np.sqrt(row['count'])
        if row['mean'] < overall_mean - 1.96 * se:
            colors.append(COLORS['conservative'])
        elif row['mean'] > overall_mean + 1.96 * se:
            colors.append(COLORS['resolution'])
        else:
            colors.append(COLORS['gray'])

    cis = [1.96 * row['std'] / np.sqrt(row['count']) for _, row in by_cat.iterrows()]

    ax.barh(y_pos, by_cat['mean'], xerr=cis, color=colors, alpha=0.8, capsize=3)

    ax.axvline(overall_mean, color=COLORS['gray'], linestyle='--', linewidth=2,
              label=f'Overall: {overall_mean:.3f}')

    for i, (_, row) in enumerate(by_cat.iterrows()):
        ax.text(row['mean'] + cis[i] + 0.005, i, f"n={int(row['count']):,}", va='center', fontsize=8)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(by_cat['clean_name'], fontsize=10)
    ax.set_xlabel('Mean Brier Score')
    ax.legend(loc='lower right')

    caption = (f"Mean Brier score by political category at conservative truncation. "
               f"Blue = better than average, coral = worse. Error bars show 95% CIs.")

    save_fig(fig, 'fig07_accuracy_by_category', caption)


# ============================================================================
# FIGURE 8: Platform Comparison (Our Chosen Specs)
# ============================================================================

def fig08_platform_comparison(data):
    """Platform comparison with chosen specs: aggressive truncation, spot price, $10K threshold."""
    log("Figure 8: Platform comparison (chosen specs)")

    # Our chosen specifications
    TRUNCATION = 'aggressive'
    PRICE_TYPE = 'spot'
    VOLUME_THRESHOLD = 10000

    # Get data
    pm_df = build_analysis_df(data, 'pm', TRUNCATION, PRICE_TYPE)
    k_df = build_analysis_df(data, 'kalshi', TRUNCATION, PRICE_TYPE)

    # Apply volume threshold
    pm_df = pm_df[pm_df['volume'] >= VOLUME_THRESHOLD].copy()
    k_df = k_df[k_df['volume'] >= VOLUME_THRESHOLD].copy()

    log(f"    After $10K threshold: PM {len(pm_df):,}, Kalshi {len(k_df):,}")

    # Clean category names
    def clean_cat(x):
        if pd.isna(x):
            return 'Other'
        x = str(x)
        import re
        x = re.sub(r'^\d+\.\s*', '', x)
        return x.replace('_', ' ').title()[:20]

    pm_df['category_clean'] = pm_df['category'].apply(clean_cat)
    k_df['category_clean'] = k_df['category'].apply(clean_cat)

    # Overall stats
    pm_overall = pm_df['brier'].mean()
    k_overall = k_df['brier'].mean()
    pm_n = len(pm_df)
    k_n = len(k_df)

    # Bootstrap CIs for overall
    def bootstrap_ci(values, n_boot=1000):
        means = []
        vals = values.dropna().values
        for _ in range(n_boot):
            sample = np.random.choice(vals, size=len(vals), replace=True)
            means.append(sample.mean())
        return np.percentile(means, [2.5, 97.5])

    pm_ci = bootstrap_ci(pm_df['brier'])
    k_ci = bootstrap_ci(k_df['brier'])

    # By category (exclude PARTISAN_CONTROL)
    all_cats = sorted(set(pm_df['category_clean'].unique()) | set(k_df['category_clean'].unique()))
    all_cats = [c for c in all_cats if c != 'Other' and 'Partisan Control' not in c]

    cat_data = []
    for cat in all_cats:
        pm_cat = pm_df[pm_df['category_clean'] == cat]['brier']
        k_cat = k_df[k_df['category_clean'] == cat]['brier']

        if len(pm_cat) >= 20 or len(k_cat) >= 20:
            cat_data.append({
                'category': cat,
                'pm_brier': pm_cat.mean() if len(pm_cat) >= 10 else np.nan,
                'k_brier': k_cat.mean() if len(k_cat) >= 10 else np.nan,
                'pm_n': len(pm_cat),
                'k_n': len(k_cat)
            })

    cat_df = pd.DataFrame(cat_data)
    cat_df = cat_df.sort_values('pm_brier', ascending=True)

    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Left: Overall comparison
    x_overall = [0, 1]
    bars = ax1.bar(x_overall, [pm_overall, k_overall],
                   color=[COLORS['polymarket'], COLORS['kalshi']], alpha=0.8, width=0.6)

    # Add error bars
    ax1.errorbar(0, pm_overall, yerr=[[pm_overall - pm_ci[0]], [pm_ci[1] - pm_overall]],
                 fmt='none', color='black', capsize=5, capthick=2)
    ax1.errorbar(1, k_overall, yerr=[[k_overall - k_ci[0]], [k_ci[1] - k_overall]],
                 fmt='none', color='black', capsize=5, capthick=2)

    ax1.set_xticks(x_overall)
    ax1.set_xticklabels(['Polymarket', 'Kalshi'], fontsize=11)
    ax1.set_ylabel('Mean Brier Score (lower = better)', fontsize=10)
    ax1.set_title('Overall Platform Accuracy', fontsize=12, fontweight='bold')

    # Add values on bars - Brier score above error bars, n inside bar
    for i, (val, n, ci) in enumerate([(pm_overall, pm_n, pm_ci), (k_overall, k_n, k_ci)]):
        # Brier score above the error bar
        ax1.text(i, ci[1] + 0.005, f'{val:.4f}', ha='center', va='bottom', fontsize=12, fontweight='bold')
        # Sample size inside bar
        ax1.text(i, val / 2, f'n={n:,}', ha='center', va='center', fontsize=11,
                color='white', fontweight='bold')

    # Winner annotation
    winner = 'Polymarket' if pm_overall < k_overall else 'Kalshi'
    diff = abs(pm_overall - k_overall)
    ax1.text(0.5, 0.95, f'{winner} more accurate by {diff:.4f}',
             transform=ax1.transAxes, ha='center', fontsize=10,
             bbox=dict(boxstyle='round', facecolor='#f0f0f0', edgecolor='none'))

    ax1.set_ylim(0, max(pm_overall, k_overall) * 1.3)
    ax1.yaxis.grid(True, linestyle='-', alpha=0.3)
    ax1.set_axisbelow(True)

    # Right: By category
    y_pos = np.arange(len(cat_df))
    bar_height = 0.35

    ax2.barh(y_pos - bar_height/2, cat_df['pm_brier'], bar_height,
             color=COLORS['polymarket'], label='Polymarket', alpha=0.8)
    ax2.barh(y_pos + bar_height/2, cat_df['k_brier'], bar_height,
             color=COLORS['kalshi'], label='Kalshi', alpha=0.8)

    ax2.set_yticks(y_pos)
    ax2.set_yticklabels(cat_df['category'], fontsize=9)
    ax2.set_xlabel('Mean Brier Score (lower = better)', fontsize=10)
    ax2.set_title('Accuracy by Category', fontsize=12, fontweight='bold')
    ax2.legend(loc='lower right', fontsize=9)
    ax2.xaxis.grid(True, linestyle='-', alpha=0.3)
    ax2.set_axisbelow(True)

    plt.tight_layout()

    # Subtitle with specs
    fig.suptitle('Platform Comparison: Aggressive Truncation, Spot Price, ≥$10K Volume',
                 fontsize=11, y=1.02, style='italic', color=COLORS['gray'])

    caption = (f"Platform accuracy comparison using aggressive truncation, spot prices, and $10K volume threshold. "
               f"PM: {pm_overall:.4f} (n={pm_n:,}), Kalshi: {k_overall:.4f} (n={k_n:,}).")

    save_fig(fig, 'fig08_platform_comparison', caption)


# ============================================================================
# FIGURE 9: Specification Curve
# ============================================================================

def fig09_specification_curve(data):
    """
    Specification curve in Simonsohn, Simmons & Nelson (2020) format.
    192 specifications: 4 truncation × 6 price × 4 threshold × 2 metric
    Creates TWO separate figures: Brier (fig09a) and Logloss (fig09b)
    """
    log("Figure 9: Specification curves (Brier & Logloss)")

    # Define all specification dimensions
    truncations = ['resolution', 'aggressive', 'moderate', 'conservative']
    price_types = ['midpoint', 'spot', 'vwap_1h', 'vwap_6h', 'vwap_12h', 'vwap_24h']
    thresholds = [0, 1000, 10000, 100000]
    thresh_labels = ['$0', '$1K', '$10K', '$100K']
    metrics = ['brier', 'logloss']

    # Load trade data for VWAP computation
    with open(PM_TRADES_FILE, 'r') as f:
        pm_trade_data = json.load(f)
    with open(KALSHI_TRADES_FILE, 'r') as f:
        k_trade_data = json.load(f)

    pm_market_trades = {str(m['market_id']): m for m in pm_trade_data['markets']}
    k_market_trades = {str(m['market_id']): m for m in k_trade_data['markets']}

    yes_tokens = data['yes_tokens']
    outcomes = data['outcomes']
    volumes = data['volumes']
    pm_midpoints = data.get('pm_midpoints', {})
    kalshi_midpoints = data.get('kalshi_midpoints', {})

    # Truncation offsets
    pm_truncs = {'conservative': -48, 'moderate': -24, 'aggressive': -12, 'resolution': 0}
    k_truncs = {'conservative': -24, 'moderate': -12, 'aggressive': -3, 'resolution': 0}

    def compute_logloss(p, y, eps=1e-15):
        """Compute log loss for a single prediction."""
        p = np.clip(p, eps, 1 - eps)
        return -(y * np.log(p) + (1 - y) * np.log(1 - p))

    def get_vwap(trades, anchor_ts, offset_hours, window_hours):
        """Compute VWAP for a specific window."""
        cutoff_ts = anchor_ts + (offset_hours * 3600)
        vwap_start = cutoff_ts - (window_hours * 3600)
        valid = [t for t in trades
                 if t.get('timestamp') is not None
                 and vwap_start <= t['timestamp'] <= cutoff_ts
                 and t.get('price') is not None
                 and t.get('shares') is not None
                 and t['shares'] > 0]
        if not valid:
            return None
        total_value = sum(t['price'] * t['shares'] for t in valid)
        total_volume = sum(t['shares'] for t in valid)
        if total_volume > 0:
            return np.clip(total_value / total_volume, 0, 1)
        return None

    specs = []
    spec_id = 0

    for trunc in truncations:
        for price_type in price_types:
            for thresh_idx, thresh in enumerate(thresholds):
                for metric in metrics:
                    scores = []

                    # Process Polymarket
                    pm_prices = data['pm_prices_yes']
                    for mid, pdata in pm_prices.items():
                        if trunc not in pdata.get('prices', {}):
                            continue

                        outcome = outcomes.get(mid)
                        vol = volumes.get(mid, 0)
                        if outcome is None or vol < thresh:
                            continue

                        # Get price based on price_type
                        price = None
                        if price_type == 'midpoint':
                            if mid in pm_midpoints and trunc in pm_midpoints[mid]:
                                price = pm_midpoints[mid][trunc]
                        elif price_type == 'spot':
                            price = pdata['prices'][trunc].get('spot')
                        elif price_type.startswith('vwap_'):
                            window = int(price_type.split('_')[1].replace('h', ''))
                            if mid in pm_market_trades:
                                market = pm_market_trades[mid]
                                anchor_ts = market.get('anchor_ts')
                                raw_trades = market.get('trades', [])
                                yes_token = yes_tokens.get(mid)
                                if anchor_ts and raw_trades and yes_token:
                                    trades = [t for t in raw_trades if t.get('token_id') == yes_token]
                                    offset = pm_truncs[trunc]
                                    price = get_vwap(trades, anchor_ts, offset, window)

                        if price is not None:
                            price = np.clip(price, 0, 1)
                            if metric == 'brier':
                                scores.append((price - outcome) ** 2)
                            else:
                                scores.append(compute_logloss(price, outcome))

                    # Process Kalshi
                    k_prices = data['kalshi_prices_yes']
                    for mid, pdata in k_prices.items():
                        if trunc not in pdata.get('prices', {}):
                            continue

                        outcome = outcomes.get(mid)
                        vol = volumes.get(mid, 0)
                        if outcome is None or vol < thresh:
                            continue

                        price = None
                        if price_type == 'midpoint':
                            if mid in kalshi_midpoints and trunc in kalshi_midpoints[mid]:
                                price = kalshi_midpoints[mid][trunc]
                        elif price_type == 'spot':
                            price = pdata['prices'][trunc].get('spot')
                        elif price_type.startswith('vwap_'):
                            window = int(price_type.split('_')[1].replace('h', ''))
                            if mid in k_market_trades:
                                market = k_market_trades[mid]
                                anchor_ts = market.get('anchor_ts')
                                raw_trades = market.get('trades', [])
                                if anchor_ts and raw_trades:
                                    # Kalshi trades format
                                    trades = []
                                    for t in raw_trades:
                                        p = t.get('yes_price')
                                        if p is not None:
                                            p = p / 100.0
                                        ts = t.get('created_time')
                                        if isinstance(ts, str):
                                            ts = int(ts) if ts.isdigit() else None
                                        trades.append({'price': p, 'shares': t.get('count', 0), 'timestamp': ts})
                                    offset = k_truncs[trunc]
                                    price = get_vwap(trades, anchor_ts, offset, window)

                        if price is not None:
                            price = np.clip(price, 0, 1)
                            if metric == 'brier':
                                scores.append((price - outcome) ** 2)
                            else:
                                scores.append(compute_logloss(price, outcome))

                    if len(scores) >= 10:
                        specs.append({
                            'spec_id': spec_id,
                            'truncation': trunc,
                            'price': price_type,
                            'threshold': thresh_labels[thresh_idx],
                            'metric': metric,
                            'score': np.mean(scores),
                            'n': len(scores)
                        })
                        spec_id += 1

    spec_df = pd.DataFrame(specs)

    log(f"    Generated {len(spec_df)} specifications total")

    # Save full data
    spec_df.to_csv(OUTPUT_DIR / 'spec_curve_data.csv', index=False)

    # Colors by truncation regime
    trunc_colors = {
        'resolution': '#d62728',    # Red
        'aggressive': '#ff7f0e',    # Orange
        'moderate': '#2ca02c',      # Green
        'conservative': '#1f77b4'   # Blue
    }

    # Create SEPARATE figures for Brier and Logloss
    for metric in metrics:
        metric_df = spec_df[spec_df['metric'] == metric].copy()
        metric_df = metric_df.sort_values('score').reset_index(drop=True)
        metric_df['rank'] = range(len(metric_df))

        log(f"    {metric.title()}: {len(metric_df)} specifications")

        fig, ax = plt.subplots(figsize=(10, 6))

        # Plot dots colored by truncation
        for trunc in truncations:
            subset = metric_df[metric_df['truncation'] == trunc]
            ax.scatter(subset['rank'], subset['score'], s=25, color=trunc_colors[trunc],
                      alpha=0.8, label=trunc.title(), zorder=5)

        # Connect with thin line
        ax.plot(metric_df['rank'], metric_df['score'], color='#888888', linewidth=0.5, alpha=0.5, zorder=1)

        ax.set_xlabel('Specification (sorted by score)', fontsize=11)
        metric_label = 'Brier Score' if metric == 'brier' else 'Log Loss'
        ax.set_ylabel(metric_label, fontsize=11)
        ax.set_xlim(-2, len(metric_df) + 1)
        ax.yaxis.grid(True, linestyle='-', alpha=0.3, color=COLORS['light_gray'])
        ax.set_axisbelow(True)

        # Legend
        ax.legend(loc='lower right', fontsize=10, frameon=True, fancybox=False, edgecolor='#CCCCCC')

        # Score range annotation
        ax.text(0.02, 0.95, f"Score range: {metric_df['score'].min():.3f} – {metric_df['score'].max():.3f}\nn = {len(metric_df)} specifications",
                transform=ax.transAxes, fontsize=9, ha='left', va='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8, edgecolor='#CCCCCC'))

        plt.tight_layout()

        fig_suffix = 'a' if metric == 'brier' else 'b'
        caption = (f"Specification curve for {metric_label} across {len(metric_df)} specifications "
                   f"(4 truncation × 6 price × 4 threshold). "
                   f"Each point represents one analytical choice combination, sorted by score. "
                   f"Colors indicate truncation regime.")

        save_fig(fig, f'fig09{fig_suffix}_specification_curve_{metric}', caption)


# ============================================================================
# MAIN
# ============================================================================

def main():
    log("=" * 70)
    log("GENERATING FIGURES V3 (USING TRADE DATA)")
    log("=" * 70)

    data = load_all_data()

    log("\nGenerating figures...")

    fig01_calibration(data)
    fig01b_distribution(data)
    fig_appendix_pm_calibration(data)
    fig_appendix_k_calibration(data)
    fig02_brier_kde(data)
    fig03_platform_rankings(data)
    fig03b_category_table(data)
    fig04_spot_vs_vwap(data)
    fig04b_brier_by_price_method(data)
    fig04c_price_method_table(data)
    fig05_volume_cdf(data)
    fig05b_lorenz_curve(data)
    fig06_brier_by_threshold(data)
    fig06b_threshold_table(data)
    fig07_accuracy_by_category(data)
    fig08_platform_comparison(data)
    fig09_specification_curve(data)

    # Save captions
    with open(OUTPUT_DIR / 'captions.md', 'w') as f:
        f.write("# Figure Captions\n\n")
        for name, caption in CAPTIONS.items():
            f.write(f"## {name}\n{caption}\n\n")

    log(f"\nSaved to {OUTPUT_DIR}")
    log("=" * 70)


if __name__ == '__main__':
    main()
