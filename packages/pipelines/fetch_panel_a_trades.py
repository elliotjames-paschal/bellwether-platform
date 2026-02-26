#!/usr/bin/env python3
"""
Fetch Panel A Trades from Polymarket Data API

Fetches trade data from the public Polymarket Data API for Panel A election markets.
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
import threading
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

# Add scripts dir to path for config import
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR, atomic_write_json

# Constants
DATA_API_BASE = "https://data-api.polymarket.com"
TRADES_FILE = DATA_DIR / "panel_a_trades.json"
STATE_FILE = DATA_DIR / "panel_a_trades_state.json"
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
PANEL_A_CSV = DATA_DIR / "election_winner_panel_a_detailed.csv"

# Rate limiting & parallelism
MAX_WORKERS = 5
RATE_LIMIT_DELAY = 0.2  # 200ms between requests
MAX_RETRIES = 3


class RateLimiter:
    """Thread-safe rate limiter."""
    def __init__(self, delay):
        self._delay = delay
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            if elapsed < self._delay:
                time.sleep(self._delay - elapsed)
            self._last = time.monotonic()


_rate_limiter = RateLimiter(RATE_LIMIT_DELAY)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def derive_candidate_party(winning_party, winning_outcome):
    """
    Derive which party the market question is asking about using resolution logic.

    If the market resolved YES, the candidate in the question won -> candidate_party = winning_party
    If the market resolved NO, the candidate in the question lost -> candidate_party = opposite

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


def fetch_trades_for_condition(condition_id, max_retries=3):
    """
    Fetch all trades for a condition ID from Polymarket Data API.

    Args:
        condition_id: Polymarket condition ID
        max_retries: Number of retries per request

    Returns:
        List of trade records (mapped to downstream-compatible format), new offset
    """
    all_trades = []
    offset = 0
    limit = 1000  # Data API caps at 1000 per page
    max_offset = 10000  # Data API max offset

    while offset <= max_offset:
        for retry in range(max_retries):
            try:
                _rate_limiter.wait()
                resp = requests.get(
                    f"{DATA_API_BASE}/trades",
                    params={
                        "market": condition_id,
                        "limit": limit,
                        "offset": offset
                    },
                    timeout=60
                )
                resp.raise_for_status()
                trades = resp.json()

                if not isinstance(trades, list):
                    trades = trades.get('data', [])

                if not trades:
                    return all_trades, offset

                # Map Data API fields to downstream-compatible format
                for trade in trades:
                    all_trades.append({
                        'user': trade.get('proxyWallet', ''),       # proxyWallet -> user
                        'price': trade.get('price'),
                        'shares_normalized': trade.get('size'),      # size -> shares_normalized
                        'timestamp': trade.get('timestamp'),
                        'token_id': trade.get('asset', ''),          # asset -> token_id
                        'token_label': trade.get('outcome', ''),     # outcome -> token_label
                        'side': trade.get('side', ''),
                        'condition_id': trade.get('conditionId', condition_id),
                        'transaction_hash': trade.get('transactionHash', ''),
                    })

                # If we got fewer than limit, we've reached the end
                if len(trades) < limit:
                    return all_trades, offset + len(trades)

                offset += len(trades)
                break

            except requests.exceptions.Timeout:
                if retry < max_retries - 1:
                    log(f"  Timeout, retry {retry + 1}/{max_retries}...")
                    time.sleep(2 ** retry)
                else:
                    log(f"  Timeout after {max_retries} retries for {condition_id[:30]}...")
                    return all_trades, offset

            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    time.sleep(2 ** retry)
                else:
                    log(f"  Error fetching trades for {condition_id[:30]}...: {e}")
                    return all_trades, offset

    return all_trades, offset


def fetch_market_trades(market_row):
    """Fetch trades for a single market (for parallel execution)."""
    market_id = str(market_row['market_id'])
    condition_id = str(market_row['pm_condition_id'])
    winning_party = market_row['winning_party']
    candidate_party = market_row.get('candidate_party', '')

    trades, final_offset = fetch_trades_for_condition(condition_id)

    # Add market metadata to each trade
    for trade in trades:
        trade['_market_id'] = market_id
        trade['_condition_id'] = condition_id
        trade['_winning_party'] = winning_party
        trade['_candidate_party'] = candidate_party

    return market_id, condition_id, winning_party, candidate_party, trades, final_offset


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
    atomic_write_json(STATE_FILE, state, indent=2)


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
    atomic_write_json(TRADES_FILE, trades_data)


def main():
    parser = argparse.ArgumentParser(description="Fetch Panel A trades from Polymarket Data API")
    parser.add_argument("--backfill", action="store_true", help="Full historical fetch")
    parser.add_argument("--limit", type=int, default=0, help="Limit to N markets (for testing)")
    parser.add_argument("--parallel", type=int, default=MAX_WORKERS, help=f"Number of parallel workers (default: {MAX_WORKERS})")
    args = parser.parse_args()

    log("=" * 60)
    log("FETCH PANEL A TRADES (Polymarket Data API)")
    log("=" * 60)

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

    # Filter to markets that need fetching
    markets_to_fetch = []
    for _, row in markets.iterrows():
        condition_id = str(row['pm_condition_id'])
        last_offset = state["market_offsets"].get(condition_id, 0)
        if args.backfill or last_offset == 0:
            markets_to_fetch.append(row)

    log(f"Markets to fetch: {len(markets_to_fetch)}")

    if not markets_to_fetch:
        log("No new markets to fetch")
        return 0

    # Fetch trades in parallel
    total_new_trades = 0
    markets_processed = 0
    num_workers = min(args.parallel, len(markets_to_fetch))

    log(f"Fetching with {num_workers} parallel workers...")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(fetch_market_trades, row): row for row in markets_to_fetch}

        for future in as_completed(futures):
            market_id, condition_id, winning_party, candidate_party, trades, final_offset = future.result()
            markets_processed += 1

            if trades:
                trades_data["trades"].extend(trades)
                total_new_trades += len(trades)
                log(f"  {market_id}: {len(trades)} trades")

            # Update state
            state["market_offsets"][condition_id] = final_offset

            # Save metadata about the market
            trades_data["markets"][condition_id] = {
                "market_id": market_id,
                "winning_party": winning_party,
                "candidate_party": candidate_party if pd.notna(candidate_party) else None,
                "orders_fetched": final_offset
            }

            # Periodic save
            if markets_processed % 10 == 0:
                save_trades(trades_data)
                save_state(state)
                log(f"  Progress: {markets_processed}/{len(markets_to_fetch)} markets, {total_new_trades} trades")

    # Final save
    save_trades(trades_data)
    save_state(state)

    log("=" * 60)
    log(f"COMPLETE: {total_new_trades} new trades from {markets_processed} markets")
    log(f"Total trades in file: {len(trades_data['trades'])}")
    log(f"Output: {TRADES_FILE}")
    log("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
