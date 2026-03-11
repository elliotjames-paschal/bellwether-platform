#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Check Market Resolutions
================================================================================

Part of the Bellwether V2 Pipeline

This script:
1. Gets all markets from master CSV where is_closed = False
2. Queries native Kalshi API + Polymarket Gamma API to check if they have closed
3. Updates winning_outcome and is_closed for resolved markets
4. Saves updates back to master CSV

No Dome API required — uses native platform APIs directly.
Uses parallel requests (10 workers, 40 req/sec) for speed.

Usage:
    python pipeline_check_resolutions.py

Output:
    - Updates master CSV in place with resolution data
    - Logs number of newly resolved markets

================================================================================
"""

import pandas as pd
import requests
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

from config import DATA_DIR, rotate_backups

# Input/Output files
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
BACKUP_DIR = DATA_DIR / "backups"

# Native API endpoints
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"

# Parallel fetch settings
MAX_WORKERS = 10        # Concurrent threads per platform
MAX_REQ_PER_SEC = 40    # Global rate limit per platform
MAX_RETRIES = 3


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# =============================================================================
# RATE LIMITER
# =============================================================================

class RateLimiter:
    """Thread-safe rate limiter. Ensures max N requests per second globally."""

    def __init__(self, max_per_second):
        self.min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last_time = 0.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            wait = self._last_time + self.min_interval - now
            if wait > 0:
                time.sleep(wait)
            self._last_time = time.monotonic()


# =============================================================================
# KALSHI — Native API
# =============================================================================

def fetch_kalshi_market(ticker, rate_limiter=None):
    """Fetch market data from Kalshi native API.

    Endpoint: GET /trade-api/v2/markets/{ticker}
    No auth required for public market data.
    """
    for attempt in range(MAX_RETRIES):
        try:
            if rate_limiter:
                rate_limiter.acquire()

            response = requests.get(
                f"{KALSHI_API_BASE}/markets/{ticker}",
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                m = data.get("market", {})

                if m:
                    return {
                        "status": m.get("status"),
                        "result": m.get("result"),
                        "close_time": m.get("close_time"),
                        "expiration_time": m.get("expiration_time"),
                        "volume": m.get("volume"),
                        "volume_24h": m.get("volume_24h"),
                        "open_interest": m.get("open_interest"),
                        "liquidity": m.get("liquidity"),
                        "last_price": m.get("last_price"),
                        "yes_bid": m.get("yes_bid"),
                        "yes_ask": m.get("yes_ask"),
                        "no_bid": m.get("no_bid"),
                        "no_ask": m.get("no_ask"),
                        "title": m.get("title"),
                        "subtitle": m.get("subtitle"),
                        "market_ticker": m.get("ticker"),
                        "event_ticker": m.get("event_ticker"),
                        "category": m.get("category"),
                        "yes_sub_title": m.get("yes_sub_title"),
                        "no_sub_title": m.get("no_sub_title"),
                    }

                return None

            elif response.status_code == 429:
                time.sleep(5 * (2 ** attempt))
                continue

            elif response.status_code == 404:
                return None

            else:
                time.sleep(2)
                continue

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return None

    return None


# =============================================================================
# POLYMARKET — Gamma API
# =============================================================================

def fetch_polymarket_market(market_id, rate_limiter=None):
    """Fetch market data from Polymarket Gamma API.

    Uses GET /markets/{id} for numeric IDs, GET /markets?slug={slug} for slugs.
    No auth required.
    """
    for attempt in range(MAX_RETRIES):
        try:
            if rate_limiter:
                rate_limiter.acquire()

            # Numeric IDs use path param, slugs use query param
            if str(market_id).isdigit():
                response = requests.get(
                    f"{POLYMARKET_API_BASE}/markets/{market_id}",
                    timeout=15
                )
            else:
                response = requests.get(
                    f"{POLYMARKET_API_BASE}/markets",
                    params={"slug": market_id},
                    timeout=15
                )

            if response.status_code == 200:
                data = response.json()

                # Slug query returns array, numeric returns dict
                if isinstance(data, list):
                    if not data:
                        return None
                    m = data[0]
                elif isinstance(data, dict):
                    m = data
                else:
                    return None

                # Determine winning outcome from outcomePrices
                winning_side = None
                last_price_yes = None
                last_price_no = None

                outcome_prices = m.get("outcomePrices")
                if outcome_prices:
                    try:
                        prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
                        if len(prices) >= 2:
                            last_price_yes = float(prices[0])
                            last_price_no = float(prices[1])
                            # If market is closed and one outcome is ~1.0, that's the winner
                            if m.get("closed"):
                                if last_price_yes >= 0.99:
                                    winning_side = "Yes"
                                elif last_price_no >= 0.99:
                                    winning_side = "No"
                    except (json.JSONDecodeError, TypeError, ValueError):
                        pass

                # Extract token IDs
                token_yes = None
                token_no = None
                clob_tokens = m.get("clobTokenIds")
                if clob_tokens:
                    try:
                        tokens = json.loads(clob_tokens) if isinstance(clob_tokens, str) else clob_tokens
                        if len(tokens) >= 2:
                            token_yes = tokens[0]
                            token_no = tokens[1]
                    except (json.JSONDecodeError, TypeError):
                        pass

                return {
                    "closed": bool(m.get("closed")),
                    "winning_side": winning_side,
                    "end_time": m.get("endDate"),
                    "close_time": m.get("closedTime"),
                    "volume_total": m.get("volumeNum") or m.get("volume") or 0,
                    "last_price_yes": last_price_yes,
                    "last_price_no": last_price_no,
                    "token_id_yes": token_yes,
                    "token_id_no": token_no,
                    "tags": [m.get("category")] if m.get("category") else [],
                    "market_slug": m.get("slug"),
                }

            elif response.status_code == 429:
                time.sleep(5 * (2 ** attempt))
                continue

            else:
                time.sleep(2)
                continue

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return None

    return None


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main function to check market resolutions."""
    print("\n" + "=" * 70)
    print("PIPELINE: CHECK MARKET RESOLUTIONS")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Workers: {MAX_WORKERS} per platform, rate limit: {MAX_REQ_PER_SEC} req/sec")
    print("=" * 70 + "\n")

    # Load master CSV
    log("Loading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"  Total markets: {len(df):,}")

    # Get open markets
    # Polymarket: use is_closed flag
    # Kalshi: use k_status since is_closed is stale (all True due to historical bug)
    pm_open = df[(df['platform'] == 'Polymarket') & (df['is_closed'] == False)].copy()
    kalshi_open = df[(df['platform'] == 'Kalshi') & (df['k_status'].isin(['active', 'open']))].copy()
    open_markets = pd.concat([pm_open, kalshi_open])
    log(f"  Open markets to check: {len(open_markets):,}")

    if len(open_markets) == 0:
        log("No open markets to check!")
        return 0

    log(f"    Polymarket: {len(pm_open):,}")
    log(f"    Kalshi: {len(kalshi_open):,}")

    # Create backup before modifying
    BACKUP_DIR.mkdir(exist_ok=True)
    backup_file = BACKUP_DIR / f"master_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(backup_file, index=False)
    log(f"\n  Created backup: {backup_file.name}")

    # Rotate old backups (keep last 5)
    deleted = rotate_backups("master_backup_*.csv")
    if deleted > 0:
        log(f"  Rotated {deleted} old backup(s)")

    # Track updates
    resolved_count = 0

    # ==========================================================================
    # CHECK POLYMARKET MARKETS (parallel)
    # ==========================================================================

    log("\n" + "=" * 50)
    log("CHECKING POLYMARKET MARKETS (Gamma API)")
    log("=" * 50)

    pm_limiter = RateLimiter(MAX_REQ_PER_SEC)
    pm_resolved = 0
    pm_checked = 0

    # Build work items: (df_index, row_data)
    pm_work = []
    for idx, row in pm_open.iterrows():
        market_id = row.get('market_id')
        if pd.isna(market_id):
            continue
        pm_work.append((idx, row, str(market_id)))

    log(f"  Fetching {len(pm_work):,} markets ({MAX_WORKERS} workers)...")

    # Parallel fetch
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_polymarket_market, mid, pm_limiter): (idx, row)
            for idx, row, mid in pm_work
        }

        for future in as_completed(futures):
            idx, row = futures[future]
            pm_checked += 1
            market_data = future.result()

            if market_data and (market_data.get("closed") or market_data.get("winning_side") is not None):
                # Core resolution fields
                df.loc[idx, 'is_closed'] = True
                df.loc[idx, 'winning_outcome'] = market_data.get("winning_side")

                # Time fields
                if market_data.get("close_time"):
                    df.loc[idx, 'trading_close_time'] = market_data["close_time"]
                if market_data.get("end_time"):
                    df.loc[idx, 'scheduled_end_time'] = market_data["end_time"]

                # Volume
                if market_data.get("volume_total"):
                    df.loc[idx, 'volume_usd'] = market_data["volume_total"]

                # Final prices
                if market_data.get("last_price_yes") is not None:
                    df.loc[idx, 'pm_last_price_yes'] = market_data["last_price_yes"]
                if market_data.get("last_price_no") is not None:
                    df.loc[idx, 'pm_last_price_no'] = market_data["last_price_no"]

                # Token IDs (if not already set)
                if pd.isna(row.get('pm_token_id_yes')) and market_data.get("token_id_yes"):
                    df.loc[idx, 'pm_token_id_yes'] = market_data["token_id_yes"]
                if pd.isna(row.get('pm_token_id_no')) and market_data.get("token_id_no"):
                    df.loc[idx, 'pm_token_id_no'] = market_data["token_id_no"]

                # Tags
                if market_data.get("tags"):
                    df.loc[idx, 'tags'] = json.dumps(market_data["tags"])

                # Slug
                if market_data.get("market_slug"):
                    df.loc[idx, 'pm_market_slug'] = market_data["market_slug"]

                pm_resolved += 1
                resolved_count += 1

            if pm_checked % 500 == 0:
                log(f"  Checked {pm_checked:,}/{len(pm_work):,} PM markets ({pm_resolved} resolved)...")

    log(f"  Polymarket: {pm_resolved} newly resolved (of {pm_checked:,} checked)")

    # ==========================================================================
    # CHECK KALSHI MARKETS (parallel)
    # ==========================================================================

    log("\n" + "=" * 50)
    log("CHECKING KALSHI MARKETS (native API)")
    log("=" * 50)

    kalshi_limiter = RateLimiter(MAX_REQ_PER_SEC)
    kalshi_resolved = 0
    kalshi_checked = 0

    # Build work items
    kalshi_work = []
    for idx, row in kalshi_open.iterrows():
        ticker = row.get('market_id')
        if pd.isna(ticker):
            continue
        kalshi_work.append((idx, row, str(ticker)))

    log(f"  Fetching {len(kalshi_work):,} markets ({MAX_WORKERS} workers)...")

    # Parallel fetch
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_kalshi_market, ticker, kalshi_limiter): (idx, row)
            for idx, row, ticker in kalshi_work
        }

        for future in as_completed(futures):
            idx, row = futures[future]
            kalshi_checked += 1
            market_data = future.result()

            if market_data and (market_data.get("status") in ("closed", "finalized", "settled") or market_data.get("result") is not None):
                # Core resolution fields
                df.loc[idx, 'is_closed'] = True
                if market_data.get("status"):
                    df.loc[idx, 'k_status'] = market_data["status"]
                elif market_data.get("result") is not None:
                    df.loc[idx, 'k_status'] = "finalized"
                if market_data.get("result") is not None:
                    result = market_data["result"]
                    if result in ("yes", "no"):
                        result = result.title()
                    df.loc[idx, 'winning_outcome'] = result

                # Time fields
                if market_data.get("close_time"):
                    df.loc[idx, 'trading_close_time'] = market_data["close_time"]
                if market_data.get("expiration_time"):
                    df.loc[idx, 'k_expiration_time'] = market_data["expiration_time"]

                # Volume
                if market_data.get("volume"):
                    df.loc[idx, 'volume_usd'] = market_data["volume"]

                # Final prices
                if market_data.get("last_price") is not None:
                    df.loc[idx, 'k_last_price'] = market_data["last_price"]
                if market_data.get("yes_bid") is not None:
                    df.loc[idx, 'k_yes_bid'] = market_data["yes_bid"]
                if market_data.get("yes_ask") is not None:
                    df.loc[idx, 'k_yes_ask'] = market_data["yes_ask"]

                # Open interest
                if market_data.get("open_interest") is not None:
                    df.loc[idx, 'k_open_interest'] = market_data["open_interest"]

                # Event ticker (if not already set)
                if pd.isna(row.get('k_event_ticker')) and market_data.get("event_ticker"):
                    df.loc[idx, 'k_event_ticker'] = market_data["event_ticker"]

                kalshi_resolved += 1
                resolved_count += 1

            if kalshi_checked % 500 == 0:
                log(f"  Checked {kalshi_checked:,}/{len(kalshi_work):,} Kalshi markets ({kalshi_resolved} resolved)...")

    log(f"  Kalshi: {kalshi_resolved} newly resolved (of {kalshi_checked:,} checked)")

    # ==========================================================================
    # SAVE UPDATES
    # ==========================================================================

    log("\n" + "=" * 50)
    log("SAVING UPDATES")
    log("=" * 50)

    if resolved_count > 0:
        df.to_csv(MASTER_FILE, index=False)
        log(f"Updated master CSV with {resolved_count} newly resolved markets")
    else:
        log("No markets resolved - no changes to save")

    # Summary
    print("\n" + "=" * 70)
    print("RESOLUTION CHECK COMPLETE")
    print("=" * 70)
    print(f"Markets checked: {len(open_markets):,}")
    print(f"Newly resolved: {resolved_count}")
    print(f"  Polymarket: {pm_resolved}")
    print(f"  Kalshi: {kalshi_resolved}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return resolved_count


if __name__ == "__main__":
    main()
