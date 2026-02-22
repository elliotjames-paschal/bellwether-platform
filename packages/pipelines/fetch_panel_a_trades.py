#!/usr/bin/env python3
"""
Fetch Panel A Trades from Dome API

Fetches trade/order data from Dome API for Polymarket Panel A election markets.
This data is used for trader-level partisanship analysis.

Usage:
    python fetch_panel_a_trades.py --backfill    # Full historical fetch
    python fetch_panel_a_trades.py               # Incremental update

Note: Only Polymarket is supported - Kalshi /trades endpoint does not include wallet addresses.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# Add scripts dir to path for config import
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR, get_dome_api_key

# Constants
DOME_API_BASE = "https://api.domeapi.io/v1"
TRADES_FILE = DATA_DIR / "panel_a_trades.json"
STATE_FILE = DATA_DIR / "panel_a_trades_state.json"
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
PANEL_A_CSV = DATA_DIR / "election_winner_panel_a_detailed.csv"

# Rate limiting
REQUESTS_PER_SECOND = 2
REQUEST_DELAY = 1.0 / REQUESTS_PER_SECOND


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def derive_candidate_party(winning_party, winning_outcome):
    """
    Derive which party the market question is asking about using resolution logic.

    If the market resolved YES, the candidate in the question won → candidate_party = winning_party
    If the market resolved NO, the candidate in the question lost → candidate_party = opposite

    Returns 'Republican', 'Democrat', or None.
    """
    if pd.isna(winning_party) or pd.isna(winning_outcome):
        return None

    winning_outcome_str = str(winning_outcome).lower().strip()

    if winning_outcome_str == 'yes':
        # The candidate the market asked about won
        return winning_party
    elif winning_outcome_str == 'no':
        # The candidate the market asked about lost
        if winning_party == 'Republican':
            return 'Democrat'
        elif winning_party == 'Democrat':
            return 'Republican'

    return None


def get_panel_a_markets():
    """
    Get Panel A Polymarket markets with condition IDs.

    Returns DataFrame with: market_id, pm_condition_id, winning_party, question, candidate_party
    """
    # Load Panel A detailed to get winning_party
    panel_a = pd.read_csv(PANEL_A_CSV)

    # Filter to Polymarket only
    panel_a = panel_a[panel_a['platform'] == 'Polymarket'].copy()

    # Filter to R/D elections
    panel_a = panel_a[panel_a['winning_party'].isin(['Republican', 'Democrat'])].copy()

    # Load master CSV to get condition IDs and winning_outcome
    master = pd.read_csv(MASTER_CSV, low_memory=False)
    master = master[master['platform'] == 'Polymarket'].copy()
    master['market_id'] = master['market_id'].astype(str)

    # Merge to get condition IDs and winning_outcome
    panel_a['market_id'] = panel_a['market_id'].astype(str)
    merged = panel_a.merge(
        master[['market_id', 'pm_condition_id', 'question', 'winning_outcome']].drop_duplicates(subset=['market_id']),
        on='market_id',
        how='left',
        suffixes=('', '_master')
    )

    # Filter to rows with valid condition IDs
    merged = merged[merged['pm_condition_id'].notna()].copy()
    merged = merged[merged['pm_condition_id'] != ''].copy()

    # Derive candidate party from winning_party + winning_outcome (100% accurate)
    merged['candidate_party'] = merged.apply(
        lambda row: derive_candidate_party(row['winning_party'], row.get('winning_outcome')),
        axis=1
    )

    # Log coverage
    inferred = merged['candidate_party'].notna().sum()
    log(f"Found {len(merged)} Panel A Polymarket markets with condition IDs")
    log(f"  Derived candidate party for {inferred} markets ({inferred/len(merged)*100:.1f}%)")

    return merged[['market_id', 'pm_condition_id', 'winning_party', 'question', 'candidate_party']]


def fetch_orders_for_condition(condition_id, api_key, last_offset=0, max_pages=100, max_retries=3):
    """
    Fetch all orders for a condition ID from Dome API.

    Args:
        condition_id: Polymarket condition ID
        api_key: Dome API bearer token
        last_offset: Starting offset for pagination
        max_pages: Maximum pages to fetch (safety limit)
        max_retries: Number of retries per request

    Returns:
        List of order records, new offset
    """
    headers = {"Authorization": api_key}
    all_orders = []
    offset = last_offset
    limit = 100  # Dome API default

    for page in range(max_pages):
        url = f"{DOME_API_BASE}/polymarket/orders"
        params = {
            "condition_id": condition_id,
            "limit": limit,
            "offset": offset
        }

        success = False
        for retry in range(max_retries):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()

                orders = data if isinstance(data, list) else data.get('orders', data.get('data', []))
                success = True
                break

            except requests.exceptions.Timeout:
                if retry < max_retries - 1:
                    log(f"  Timeout on page {page}, retry {retry + 1}/{max_retries}...")
                    time.sleep(2 ** retry)  # Exponential backoff
                else:
                    log(f"  Timeout after {max_retries} retries for {condition_id[:30]}...")
                    return all_orders, offset

            except requests.exceptions.RequestException as e:
                log(f"  Error fetching orders for {condition_id[:30]}...: {e}")
                return all_orders, offset

        if not success:
            break

        if not orders:
            break

        all_orders.extend(orders)
        offset += len(orders)

        # If we got fewer than limit, we've reached the end
        if len(orders) < limit:
            break

        time.sleep(REQUEST_DELAY)

    return all_orders, offset


def load_state():
    """Load fetch state from file."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"market_offsets": {}, "last_run": None}


def save_state(state):
    """Save fetch state to file."""
    state["last_run"] = datetime.now().isoformat()
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def load_existing_trades():
    """Load existing trades from file."""
    if TRADES_FILE.exists():
        try:
            with open(TRADES_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"trades": [], "markets": {}}


def save_trades(trades_data):
    """Save trades to file."""
    with open(TRADES_FILE, 'w') as f:
        json.dump(trades_data, f)


def main():
    parser = argparse.ArgumentParser(description="Fetch Panel A trades from Dome API")
    parser.add_argument("--backfill", action="store_true", help="Full historical fetch")
    parser.add_argument("--limit", type=int, default=0, help="Limit to N markets (for testing)")
    parser.add_argument("--parallel", type=int, default=1, help="Number of parallel workers (default: 1)")
    args = parser.parse_args()

    log("=" * 60)
    log("FETCH PANEL A TRADES")
    log("=" * 60)

    # Get API key
    try:
        api_key = get_dome_api_key()
    except ValueError as e:
        log(f"Error: {e}")
        return 1

    # Get Panel A markets
    markets = get_panel_a_markets()
    if len(markets) == 0:
        log("No Panel A markets found with condition IDs")
        return 1

    # Load state and existing trades
    state = load_state()
    trades_data = load_existing_trades()

    if args.backfill:
        log("Mode: BACKFILL (full historical fetch)")
        state["market_offsets"] = {}
    else:
        log("Mode: INCREMENTAL")

    # Apply limit if specified
    if args.limit > 0:
        markets = markets.head(args.limit)
        log(f"Limited to {args.limit} markets (testing mode)")

    # Fetch orders for each market
    total_new_orders = 0
    markets_processed = 0

    for idx, row in markets.iterrows():
        market_id = str(row['market_id'])
        condition_id = str(row['pm_condition_id'])
        winning_party = row['winning_party']
        candidate_party = row.get('candidate_party', '')  # Party of the candidate the market is about

        # Get last offset for this market
        last_offset = state["market_offsets"].get(condition_id, 0)

        if not args.backfill and last_offset > 0:
            # Skip if we've already fetched this market (unless backfilling)
            continue

        log(f"Fetching orders for {market_id} ({condition_id[:20]}...)")

        orders, new_offset = fetch_orders_for_condition(condition_id, api_key, last_offset)

        if orders:
            # Add market metadata to each order
            for order in orders:
                order['_market_id'] = market_id
                order['_condition_id'] = condition_id
                order['_winning_party'] = winning_party
                order['_candidate_party'] = candidate_party  # Party of the candidate

            trades_data["trades"].extend(orders)
            total_new_orders += len(orders)
            log(f"  Fetched {len(orders)} orders")

        # Update state
        state["market_offsets"][condition_id] = new_offset

        # Save metadata about the market
        trades_data["markets"][condition_id] = {
            "market_id": market_id,
            "winning_party": winning_party,
            "candidate_party": candidate_party if pd.notna(candidate_party) else None,
            "orders_fetched": new_offset
        }

        markets_processed += 1

        # Periodic save
        if markets_processed % 10 == 0:
            save_trades(trades_data)
            save_state(state)
            log(f"  Progress: {markets_processed}/{len(markets)} markets")

    # Final save
    save_trades(trades_data)
    save_state(state)

    log("=" * 60)
    log(f"COMPLETE: {total_new_orders} new orders from {markets_processed} markets")
    log(f"Total orders in file: {len(trades_data['trades'])}")
    log(f"Output: {TRADES_FILE}")
    log("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
