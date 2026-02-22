#!/usr/bin/env python3
"""
Aggregate Trader Partisanship

Process trade data from fetch_panel_a_trades.py into wallet-level aggregates
for trader partisanship analysis.

Computes for each wallet:
- pct_volume_for_republican: % of money bet on Republican Yes
- pct_volume_correct: % of money bet on the correct outcome
- cf_pct_volume_for_republican: counterfactual partisanship if all bets were correct

Usage:
    python aggregate_trader_partisanship.py
"""

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

# Add scripts dir to path for config import
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR

# Files
TRADES_FILE = DATA_DIR / "panel_a_trades.json"
OUTPUT_FILE = DATA_DIR / "panel_a_trader_analysis.csv"
PANEL_A_CSV = DATA_DIR / "election_winner_panel_a_detailed.csv"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def load_trades():
    """Load trades from JSON file."""
    if not TRADES_FILE.exists():
        log(f"Error: Trades file not found: {TRADES_FILE}")
        log("Run fetch_panel_a_trades.py first")
        return None

    with open(TRADES_FILE, 'r') as f:
        data = json.load(f)

    trades = data.get('trades', [])
    log(f"Loaded {len(trades)} trades")
    return trades


def load_panel_a_metadata():
    """Load Panel A metadata to get winning party and candidate for each market."""
    panel_a = pd.read_csv(PANEL_A_CSV)
    panel_a = panel_a[panel_a['platform'] == 'Polymarket'].copy()
    panel_a['market_id'] = panel_a['market_id'].astype(str)

    # Create market_id -> metadata mapping
    metadata = {}
    for _, row in panel_a.iterrows():
        winning_candidate = str(row.get('winning_candidate', '')) if pd.notna(row.get('winning_candidate')) else ''
        metadata[row['market_id']] = {
            'winning_party': row['winning_party'],
            'winning_candidate': winning_candidate.lower(),
            'republican_won': row.get('republican_won', row['winning_party'] == 'Republican')
        }

    log(f"Loaded metadata for {len(metadata)} Panel A markets")
    return metadata


def determine_token_party(token_label, winning_party, winning_candidate):
    """
    Determine the party of a token in candidate-name markets.

    For markets with candidate names (not Yes/No), we need to figure out
    which party each candidate belongs to.

    Returns 'Republican', 'Democrat', or None
    """
    token = token_label.strip().lower()

    # Check if token matches party name variations
    if token in ['republican', 'republicans', 'gop']:
        return 'Republican'
    if token in ['democrat', 'democrats', 'democratic']:
        return 'Democrat'

    # Check if token is contained in winning_candidate name
    if winning_candidate and token in winning_candidate:
        return winning_party

    # If not the winner, must be the other party (two-way race)
    if winning_party == 'Republican':
        return 'Democrat'
    elif winning_party == 'Democrat':
        return 'Republican'

    return None


def compute_trade_volume(trade):
    """
    Compute the USD volume for a trade.

    Uses shares_normalized * price if available, otherwise falls back to other fields.
    """
    # Try shares_normalized * price first
    shares = trade.get('shares_normalized') or trade.get('shares')
    price = trade.get('price')

    if shares is not None and price is not None:
        try:
            return float(shares) * float(price)
        except (ValueError, TypeError):
            pass

    # Fallback to other volume fields
    for field in ['volume', 'amount', 'size']:
        if field in trade and trade[field] is not None:
            try:
                return float(trade[field])
            except (ValueError, TypeError):
                pass

    return 0.0


def determine_bet_direction(trade, candidate_party, winning_party=None, winning_candidate=None):
    """
    Determine if the trade is betting FOR or AGAINST Republicans.

    Handles both Yes/No markets and candidate-name markets.

    For Yes/No markets about a Republican candidate:
    - BUY Yes = betting FOR Republican
    - BUY No = betting AGAINST Republican
    - SELL Yes = betting AGAINST Republican
    - SELL No = betting FOR Republican

    For candidate-name markets:
    - BUY [Republican candidate] = betting FOR Republican
    - BUY [Democrat candidate] = betting AGAINST Republican
    - SELL flips the direction

    Returns: 'for_republican', 'against_republican', or None
    """
    side = trade.get('side', '').upper()
    token_label = trade.get('token_label', '').strip().lower()

    is_buy = side in ['BUY', 'B']
    is_sell = side in ['SELL', 'S']

    if not (is_buy or is_sell):
        return None

    # Check if it's a Yes/No token
    is_yes = token_label in ['yes', 'y', '1', 'true']
    is_no = token_label in ['no', 'n', '0', 'false']

    if is_yes or is_no:
        # Yes/No market - use candidate_party
        if not candidate_party or candidate_party not in ['Republican', 'Democrat']:
            return None

        if candidate_party == 'Republican':
            if (is_buy and is_yes) or (is_sell and is_no):
                return 'for_republican'
            elif (is_buy and is_no) or (is_sell and is_yes):
                return 'against_republican'
        else:
            # Democrat candidate market
            if (is_buy and is_yes) or (is_sell and is_no):
                return 'against_republican'
            elif (is_buy and is_no) or (is_sell and is_yes):
                return 'for_republican'
    else:
        # Candidate-name market - determine party of the token
        token_party = determine_token_party(token_label, winning_party, winning_candidate)
        if not token_party:
            return None

        if token_party == 'Republican':
            return 'for_republican' if is_buy else 'against_republican'
        else:
            return 'against_republican' if is_buy else 'for_republican'

    return None


def determine_if_correct(trade, winning_party, candidate_party, winning_candidate=None):
    """
    Determine if the trade was on the correct (winning) side.

    Returns True if the bet was correct, False if incorrect, None if undetermined.
    """
    direction = determine_bet_direction(trade, candidate_party, winning_party, winning_candidate)
    if direction is None:
        return None

    if winning_party == 'Republican':
        return direction == 'for_republican'
    elif winning_party == 'Democrat':
        return direction == 'against_republican'

    return None


def aggregate_by_wallet(trades, metadata):
    """
    Aggregate trades by wallet address.

    Returns DataFrame with wallet-level statistics.
    """
    # Group trades by wallet
    wallet_stats = defaultdict(lambda: {
        'volume_for_republican': 0.0,
        'volume_against_republican': 0.0,
        'volume_correct': 0.0,
        'volume_incorrect': 0.0,
        'total_volume': 0.0,
        'num_trades': 0,
        'num_markets': set(),
        # Counterfactual: what if all bets were correct?
        'cf_volume_for_republican': 0.0,
        'cf_volume_against_republican': 0.0,
        # Party-specific counterfactuals: only flip incorrect bets for that party
        'cf_rep_only_volume_for_republican': 0.0,  # Only flip incorrect FOR Rep bets
        'cf_rep_only_volume_against_republican': 0.0,
        'cf_dem_only_volume_for_republican': 0.0,  # Only flip incorrect AGAINST Rep bets
        'cf_dem_only_volume_against_republican': 0.0
    })

    processed = 0
    skipped_no_wallet = 0
    skipped_no_metadata = 0

    for trade in trades:
        # Get wallet address - try both 'user' and 'taker'
        wallet = trade.get('user') or trade.get('maker')
        if not wallet:
            skipped_no_wallet += 1
            continue

        # Get market metadata
        market_id = trade.get('_market_id')
        winning_party = trade.get('_winning_party')
        candidate_party = trade.get('_candidate_party')
        winning_candidate = ''

        if not market_id or not winning_party:
            # Try to look up from metadata
            if market_id and market_id in metadata:
                winning_party = metadata[market_id]['winning_party']
                winning_candidate = metadata[market_id].get('winning_candidate', '')
            else:
                skipped_no_metadata += 1
                continue
        else:
            # Get winning_candidate from metadata even if we have winning_party from trade
            if market_id and market_id in metadata:
                winning_candidate = metadata[market_id].get('winning_candidate', '')

        # Compute volume
        volume = compute_trade_volume(trade)
        if volume <= 0:
            continue

        # Determine bet direction (now handles candidate-name tokens)
        direction = determine_bet_direction(trade, candidate_party, winning_party, winning_candidate)
        if direction is None:
            continue

        # Determine if correct
        is_correct = determine_if_correct(trade, winning_party, candidate_party, winning_candidate)

        # Update wallet stats
        stats = wallet_stats[wallet]
        stats['total_volume'] += volume
        stats['num_trades'] += 1
        stats['num_markets'].add(market_id)

        if direction == 'for_republican':
            stats['volume_for_republican'] += volume
        else:
            stats['volume_against_republican'] += volume

        if is_correct is True:
            stats['volume_correct'] += volume
            # Counterfactual: same as actual if correct
            if direction == 'for_republican':
                stats['cf_volume_for_republican'] += volume
                stats['cf_rep_only_volume_for_republican'] += volume
                stats['cf_dem_only_volume_for_republican'] += volume
            else:
                stats['cf_volume_against_republican'] += volume
                stats['cf_rep_only_volume_against_republican'] += volume
                stats['cf_dem_only_volume_against_republican'] += volume
        elif is_correct is False:
            stats['volume_incorrect'] += volume
            # Full counterfactual: flip the direction (as if they had bet correctly)
            if direction == 'for_republican':
                stats['cf_volume_against_republican'] += volume
            else:
                stats['cf_volume_for_republican'] += volume

            # Party-specific counterfactuals: only flip for that party's bets
            if direction == 'for_republican':
                # Pro-Rep view: flip incorrect FOR Rep bets to AGAINST Rep
                stats['cf_rep_only_volume_against_republican'] += volume
                # Pro-Dem view: keep incorrect FOR Rep bets as FOR Rep (no flip)
                stats['cf_dem_only_volume_for_republican'] += volume
            else:
                # Pro-Rep view: keep incorrect AGAINST Rep bets as AGAINST Rep (no flip)
                stats['cf_rep_only_volume_against_republican'] += volume
                # Pro-Dem view: flip incorrect AGAINST Rep bets to FOR Rep
                stats['cf_dem_only_volume_for_republican'] += volume

        processed += 1

    log(f"Processed {processed} trades into {len(wallet_stats)} wallets")
    if skipped_no_wallet > 0:
        log(f"  Skipped {skipped_no_wallet} trades without wallet address")
    if skipped_no_metadata > 0:
        log(f"  Skipped {skipped_no_metadata} trades without market metadata")

    # Convert to DataFrame
    rows = []
    for wallet, stats in wallet_stats.items():
        total = stats['total_volume']
        cf_total = stats['cf_volume_for_republican'] + stats['cf_volume_against_republican']
        cf_rep_total = stats['cf_rep_only_volume_for_republican'] + stats['cf_rep_only_volume_against_republican']
        cf_dem_total = stats['cf_dem_only_volume_for_republican'] + stats['cf_dem_only_volume_against_republican']

        rows.append({
            'wallet': wallet,
            'total_volume': total,
            'volume_for_republican': stats['volume_for_republican'],
            'volume_against_republican': stats['volume_against_republican'],
            'volume_correct': stats['volume_correct'],
            'volume_incorrect': stats['volume_incorrect'],
            'num_trades': stats['num_trades'],
            'num_markets': len(stats['num_markets']),
            'pct_volume_for_republican': (stats['volume_for_republican'] / total * 100) if total > 0 else np.nan,
            'pct_volume_correct': (stats['volume_correct'] / total * 100) if total > 0 else np.nan,
            'cf_volume_for_republican': stats['cf_volume_for_republican'],
            'cf_volume_against_republican': stats['cf_volume_against_republican'],
            'cf_pct_volume_for_republican': (stats['cf_volume_for_republican'] / cf_total * 100) if cf_total > 0 else np.nan,
            # Party-specific counterfactuals
            'cf_rep_only_pct_for_republican': (stats['cf_rep_only_volume_for_republican'] / cf_rep_total * 100) if cf_rep_total > 0 else np.nan,
            'cf_dem_only_pct_for_republican': (stats['cf_dem_only_volume_for_republican'] / cf_dem_total * 100) if cf_dem_total > 0 else np.nan
        })

    return pd.DataFrame(rows)


def main():
    log("=" * 60)
    log("AGGREGATE TRADER PARTISANSHIP")
    log("=" * 60)

    # Load trades
    trades = load_trades()
    if trades is None:
        return 1

    # Load metadata
    metadata = load_panel_a_metadata()

    # Aggregate by wallet
    df = aggregate_by_wallet(trades, metadata)

    if len(df) == 0:
        log("No wallets found after aggregation")
        return 1

    # Summary statistics
    log("")
    log("Summary Statistics:")
    log(f"  Total wallets: {len(df)}")
    log(f"  Wallets with Republican volume > 0: {len(df[df['volume_for_republican'] > 0])}")

    # Filter to wallets with meaningful volume
    df_filtered = df[df['total_volume'] >= 10].copy()  # At least $10
    log(f"  Wallets with >= $10 volume: {len(df_filtered)}")

    if len(df_filtered) > 0:
        log(f"  Mean partisanship: {df_filtered['pct_volume_for_republican'].mean():.1f}%")
        log(f"  Mean accuracy: {df_filtered['pct_volume_correct'].mean():.1f}%")

    # Save
    df.to_csv(OUTPUT_FILE, index=False)
    log(f"Saved: {OUTPUT_FILE}")

    log("=" * 60)
    log("COMPLETE")
    log("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
