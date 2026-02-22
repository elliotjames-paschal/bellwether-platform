#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Check Market Resolutions
================================================================================

Part of the NEW Bellwether Pipeline (January 2026+)

This script:
1. Gets all markets from master CSV where is_closed = False
2. Queries Dome API to check if they have closed
3. Updates winning_outcome and is_closed for resolved markets
4. Saves updates back to master CSV

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
import os
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

from config import DATA_DIR, get_dome_api_key, rotate_backups

# Input/Output files
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
BACKUP_DIR = DATA_DIR / "backups"

DOME_API_KEY = get_dome_api_key()
DOME_PM_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_KALSHI_BASE = "https://api.domeapi.io/v1/kalshi"
KALSHI_DIRECT_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Rate limiting (dev tier: 100 req/sec)
RATE_LIMIT_DELAY = float(os.environ.get('DOME_RATE_LIMIT', '0.01'))
MAX_RETRIES = 3

# Track fallback usage for logging
_kalshi_fallback_count = 0


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def fetch_kalshi_direct(ticker):
    """Fetch market data directly from Kalshi API (fallback when Dome is stale).

    Kalshi's direct API: https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}
    No auth required for public market data.
    """
    global _kalshi_fallback_count

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                f"{KALSHI_DIRECT_BASE}/markets/{ticker}",
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                m = data.get("market", {})

                if m:
                    _kalshi_fallback_count += 1

                    # Map Kalshi direct API fields to our format
                    # Kalshi uses ISO timestamps, convert to unix
                    close_time = None
                    if m.get("close_time"):
                        try:
                            close_time = int(datetime.fromisoformat(
                                m["close_time"].replace("Z", "+00:00")
                            ).timestamp())
                        except:
                            pass

                    expiration_time = None
                    if m.get("expiration_time"):
                        try:
                            expiration_time = int(datetime.fromisoformat(
                                m["expiration_time"].replace("Z", "+00:00")
                            ).timestamp())
                        except:
                            pass

                    return {
                        "status": m.get("status"),
                        "result": m.get("result"),
                        "close_time": close_time,
                        "expiration_time": expiration_time,
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
                        "_source": "kalshi_direct",
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


def is_dome_stale(dome_data):
    """Detect if Dome data is stale for a Kalshi market.

    Stale = close_time has passed but status is still 'open' with no result.
    """
    if not dome_data:
        return False

    close_time = dome_data.get("close_time", 0)
    status = dome_data.get("status")
    result = dome_data.get("result")

    now = time.time()

    # Stale if: close_time passed AND still showing open AND no result
    return (close_time > 0) and (close_time < now) and (status == "open") and (result is None)


def fetch_polymarket_full(condition_id):
    """Fetch FULL market data for a Polymarket market from Dome API.

    Returns all available fields for updating the master CSV when market closes.

    Note: The Dome API defaults to returning only open markets. To find markets
    that have closed, we first try without a filter, then try with closed=true.
    """
    for attempt in range(MAX_RETRIES):
        try:
            markets = []

            # First try without filter (finds still-open markets)
            response = requests.get(
                f"{DOME_PM_BASE}/markets",
                headers={"Authorization": DOME_API_KEY},
                params={
                    "limit": 1,
                    "condition_id": condition_id,
                },
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                markets = data.get("markets", [])
            elif response.status_code == 429:
                wait_time = 10 * (2 ** attempt)
                time.sleep(wait_time)
                continue

            # If not found, try with closed=true (finds newly-closed markets)
            if not markets:
                response = requests.get(
                    f"{DOME_PM_BASE}/markets",
                    headers={"Authorization": DOME_API_KEY},
                    params={
                        "limit": 1,
                        "condition_id": condition_id,
                        "closed": "true",
                    },
                    timeout=15
                )
                if response.status_code == 200:
                    data = response.json()
                    markets = data.get("markets", [])
                elif response.status_code == 429:
                    wait_time = 10 * (2 ** attempt)
                    time.sleep(wait_time)
                    continue

            if markets:
                m = markets[0]
                # Extract all relevant fields
                result = {
                    # Status fields
                    "status": m.get("status"),
                    "winning_side": m.get("winning_side", {}).get("label") if m.get("winning_side") else None,
                    "winning_side_id": m.get("winning_side", {}).get("id") if m.get("winning_side") else None,

                    # Time fields
                    "completed_time": m.get("completed_time"),
                    "start_time": m.get("start_time"),
                    "end_time": m.get("end_time"),
                    "close_time": m.get("close_time"),

                    # Volume and liquidity
                    "volume_total": m.get("volume_total"),
                    "volume_24h": m.get("volume_24h"),
                    "liquidity": m.get("liquidity"),

                    # Price data
                    "last_price_yes": m.get("side_a", {}).get("last_price") if m.get("side_a") else None,
                    "last_price_no": m.get("side_b", {}).get("last_price") if m.get("side_b") else None,

                    # Token IDs (for reference)
                    "token_id_yes": m.get("side_a", {}).get("id") if m.get("side_a") else None,
                    "token_id_no": m.get("side_b", {}).get("id") if m.get("side_b") else None,

                    # Market info
                    "title": m.get("title"),
                    "description": m.get("description"),
                    "tags": m.get("tags", []),
                    "market_slug": m.get("market_slug"),
                    "event_slug": m.get("event_slug"),
                }
                return result

            return None

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return None

    return None


def fetch_kalshi_full(ticker):
    """Fetch FULL market data for a Kalshi market from Dome API.

    Returns all available fields for updating the master CSV when market closes.

    Note: The Dome API defaults to returning only open markets. To find markets
    that have closed, we first try without a filter, then try with status=closed.
    """
    for attempt in range(MAX_RETRIES):
        try:
            markets = []

            # First try without filter (finds still-open markets)
            response = requests.get(
                f"{DOME_KALSHI_BASE}/markets",
                headers={"Authorization": DOME_API_KEY},
                params={
                    "limit": 1,
                    "market_ticker": ticker,
                },
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                markets = data.get("markets", [])
            elif response.status_code == 429:
                wait_time = 10 * (2 ** attempt)
                time.sleep(wait_time)
                continue

            # If not found, try with status=closed (finds closed markets)
            if not markets:
                response = requests.get(
                    f"{DOME_KALSHI_BASE}/markets",
                    headers={"Authorization": DOME_API_KEY},
                    params={
                        "limit": 1,
                        "market_ticker": ticker,
                        "status": "closed",
                    },
                    timeout=15
                )
                if response.status_code == 200:
                    data = response.json()
                    markets = data.get("markets", [])
                elif response.status_code == 429:
                    wait_time = 10 * (2 ** attempt)
                    time.sleep(wait_time)
                    continue

            if markets:
                m = markets[0]
                # Extract all relevant fields
                result = {
                    # Status fields
                    "status": m.get("status"),
                    "result": m.get("result"),

                    # Time fields
                    "start_time": m.get("start_time"),
                    "end_time": m.get("end_time"),
                    "close_time": m.get("close_time"),
                    "expiration_time": m.get("expiration_time"),

                    # Volume and liquidity
                    "volume": m.get("volume"),
                    "volume_24h": m.get("volume_24h"),
                    "open_interest": m.get("open_interest"),
                    "liquidity": m.get("liquidity"),

                    # Price data
                    "last_price": m.get("last_price"),
                    "yes_bid": m.get("yes_bid"),
                    "yes_ask": m.get("yes_ask"),
                    "no_bid": m.get("no_bid"),
                    "no_ask": m.get("no_ask"),

                    # Market info
                    "title": m.get("title"),
                    "subtitle": m.get("subtitle"),
                    "market_ticker": m.get("market_ticker"),
                    "event_ticker": m.get("event_ticker"),
                    "category": m.get("category"),
                    "yes_sub_title": m.get("yes_sub_title"),
                    "no_sub_title": m.get("no_sub_title"),
                }
                return result

            return None

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return None

    return None


def main():
    """Main function to check market resolutions."""
    print("\n" + "=" * 70)
    print("PIPELINE: CHECK MARKET RESOLUTIONS")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    # Load master CSV
    log("Loading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"  Total markets: {len(df):,}")

    # Get open markets
    open_markets = df[df['is_closed'] == False].copy()
    log(f"  Open markets to check: {len(open_markets):,}")

    if len(open_markets) == 0:
        log("No open markets to check!")
        return 0

    # Split by platform
    pm_open = open_markets[open_markets['platform'] == 'Polymarket']
    kalshi_open = open_markets[open_markets['platform'] == 'Kalshi']

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
    # CHECK POLYMARKET MARKETS
    # ==========================================================================

    log("\n" + "=" * 50)
    log("CHECKING POLYMARKET MARKETS")
    log("=" * 50)

    pm_resolved = 0

    for idx, row in pm_open.iterrows():
        condition_id = row.get('pm_condition_id')

        if pd.isna(condition_id):
            continue

        market_data = fetch_polymarket_full(str(condition_id))

        if market_data and (market_data.get("status") == "closed" or market_data.get("winning_side") is not None):
            # Market has closed or has a winning side - update ALL available fields in master CSV

            # Core resolution fields
            df.loc[idx, 'is_closed'] = True
            df.loc[idx, 'winning_outcome'] = market_data.get("winning_side")

            # Time fields
            if market_data.get("completed_time"):
                completed_dt = datetime.fromtimestamp(market_data["completed_time"])
                df.loc[idx, 'trading_close_time'] = completed_dt.isoformat()

            if market_data.get("end_time"):
                df.loc[idx, 'scheduled_end_time'] = datetime.fromtimestamp(market_data["end_time"]).isoformat()

            # Volume (update with final volume)
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

            # Update tags if available
            if market_data.get("tags"):
                df.loc[idx, 'tags'] = json.dumps(market_data["tags"])

            # Slugs (for disambiguation in GPT election labelling)
            if market_data.get("market_slug"):
                df.loc[idx, 'pm_market_slug'] = market_data["market_slug"]
            if market_data.get("event_slug"):
                df.loc[idx, 'pm_event_slug'] = market_data["event_slug"]

            pm_resolved += 1
            resolved_count += 1

            if pm_resolved % 10 == 0:
                log(f"  Resolved {pm_resolved} Polymarket markets...")

        time.sleep(RATE_LIMIT_DELAY)

    log(f"  Polymarket: {pm_resolved} newly resolved")

    # ==========================================================================
    # CHECK KALSHI MARKETS
    # ==========================================================================

    log("\n" + "=" * 50)
    log("CHECKING KALSHI MARKETS")
    log("=" * 50)

    global _kalshi_fallback_count
    _kalshi_fallback_count = 0
    kalshi_resolved = 0

    for idx, row in kalshi_open.iterrows():
        ticker = row.get('market_id')

        if pd.isna(ticker):
            continue

        # First try Dome API
        market_data = fetch_kalshi_full(str(ticker))

        # Detect stale Dome data and fallback to Kalshi direct API
        if is_dome_stale(market_data):
            direct_data = fetch_kalshi_direct(str(ticker))
            if direct_data:
                market_data = direct_data

        if market_data and (market_data.get("status") in ("closed", "finalized", "settled") or market_data.get("result") is not None):
            # Market has closed or has a result - update ALL available fields in master CSV

            # Core resolution fields
            df.loc[idx, 'is_closed'] = True
            # Update k_status (use status from API, or "finalized" if we have a result)
            if market_data.get("status"):
                df.loc[idx, 'k_status'] = market_data["status"]
            elif market_data.get("result") is not None:
                df.loc[idx, 'k_status'] = "finalized"
            if market_data.get("result") is not None:
                # Normalize to title case (Yes/No) for consistency
                result = market_data["result"]
                if result in ("yes", "no"):
                    result = result.title()
                df.loc[idx, 'winning_outcome'] = result

            # Time fields
            if market_data.get("close_time"):
                df.loc[idx, 'trading_close_time'] = datetime.fromtimestamp(market_data["close_time"]).isoformat()

            if market_data.get("expiration_time"):
                df.loc[idx, 'k_expiration_time'] = datetime.fromtimestamp(market_data["expiration_time"]).isoformat()

            # Volume (update with final volume)
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

            if kalshi_resolved % 10 == 0:
                log(f"  Resolved {kalshi_resolved} Kalshi markets...")

        time.sleep(RATE_LIMIT_DELAY)

    log(f"  Kalshi: {kalshi_resolved} newly resolved")
    if _kalshi_fallback_count > 0:
        log(f"  (Used Kalshi direct API fallback for {_kalshi_fallback_count} stale Dome responses)")

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
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return resolved_count


if __name__ == "__main__":
    main()
