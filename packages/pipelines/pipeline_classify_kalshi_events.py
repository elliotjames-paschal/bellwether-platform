#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Discover Kalshi Political Markets via Search
================================================================================

Part of the NEW Bellwether Pipeline (January 2026+)

This script:
1. Searches the Dome API for Kalshi markets matching 221 political keywords
2. Deduplicates by market_ticker, extracts unique event_tickers
3. Updates kalshi_political_event_tickers.json (marks found tickers as political)
4. Saves a market cache for downstream pipeline steps (avoids re-fetching)

No GPT/OpenAI required — search terms are hand-curated to cover all 15 political
categories. Coverage: 99.3% of previously-identified political event_tickers.

Usage:
    python pipeline_classify_kalshi_events.py [--full-refresh]

Options:
    --full-refresh  Search both open AND closed markets (default: open only)

Output:
    - data/kalshi_political_event_tickers.json
    - data/kalshi_political_markets_cache.json

================================================================================
"""

import requests
import json
import time
import os
import sys
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

from config import BASE_DIR, DATA_DIR, get_dome_api_key

# Output files
EVENT_TICKERS_FILE = DATA_DIR / "kalshi_political_event_tickers.json"
MARKET_CACHE_FILE = DATA_DIR / "kalshi_political_markets_cache.json"

DOME_API_KEY = get_dome_api_key()
DOME_KALSHI_BASE = "https://api.domeapi.io/v1/kalshi"
RATE_LIMIT_DELAY = float(os.environ.get('DOME_RATE_LIMIT', '0.15'))
MAX_RETRIES = 3

# =============================================================================
# POLITICAL SEARCH TERMS (221 verified, all return ≥1 result)
#
# Dome API search does EXACT WORD matching (case-insensitive) on market titles.
# "election" does NOT match "elections" or "electoral" — each form is separate.
# =============================================================================

KALSHI_SEARCH_TERMS = [
    # Elections & Offices (22)
    "election", "elections", "elected", "electoral",
    "senate", "governor", "governorship", "gubernatorial",
    "congress", "congressional", "president", "presidential", "presidency",
    "nominee", "nomination", "primary", "mayor", "candidate", "candidates",
    "speaker", "attorney general", "secretary of state",
    # Parties (8)
    "democrat", "democratic", "Democrats", "republican", "Republicans",
    "party", "GOP", "convention",
    # Key Figures (21)
    "trump", "biden", "musk", "vance", "putin", "Zelenskyy",
    "powell", "netanyahu", "maduro", "xi jinping",
    "RFK", "Kennedy", "Pelosi", "McConnell", "Schumer", "AOC",
    "DeSantis", "Newsom", "Haley", "Obama", "Harris",
    # Economic / Fed (24)
    "federal reserve", "Fed meeting", "funds rate",
    "tariff", "GDP", "inflation", "recession", "CPI", "rate", "rates",
    "basis points", "employment", "unemployment", "jobs added", "NFP", "ADP",
    "treasury", "balance sheet", "yields", "credit rating", "central bank",
    "debt", "credit card",
    # Government Bodies (18)
    "DOGE", "USAID", "CFPB", "FDIC", "CDC", "DOJ", "FTC", "FDA",
    "SEC", "EPA", "FBI", "ATF", "FCC", "NPR", "IRS",
    "Medicare", "social security",
    # Legislative (11)
    "legislation", "becomes law", "bill", "bills",
    "filibuster", "reconciliation", "repeal",
    "constitutional", "amendment", "act of 2025", "act of 2026",
    # Judicial & Legal (9)
    "supreme court", "lawsuit", "antitrust",
    "charged", "arrested", "indictment", "pardon", "pardoned", "subpoena",
    # Executive (10)
    "executive", "cabinet", "secretary", "white house", "veto",
    "impeach", "impeached", "impeachment", "confirm", "confirmed",
    # Government Ops (8)
    "shutdown", "debt ceiling", "budget", "spending",
    "sovereign", "congestion", "fund", "51st",
    # International (27)
    "NATO", "sanctions", "parliament", "prime minister", "chancellor",
    "embassy", "diplomat", "BRICS", "Schengen", "eurozone",
    "EU", "European", "United Nations", "NordStream",
    "agreement", "deal", "peace",
    "Israel", "Ukraine", "Russia", "China", "Iran", "Syria",
    "Greenland", "Canada", "Canadian",
    # Countries (15)
    "India", "UK", "Britain", "Germany", "France", "Japan", "Korea",
    "Mexico", "Brazil", "Argentina", "Turkey", "Italy",
    "Australia", "Nigeria", "South Africa",
    # Tech Companies (6)
    "TikTok", "Google", "Apple", "Microsoft", "Meta", "Amazon",
    # Policy Areas (29)
    "tax", "taxes", "crypto", "marijuana", "vaccine",
    "concealed carry", "border", "deportations", "visa", "citizenship",
    "climate", "carbon", "energy", "nuclear",
    "artificial intelligence", "minimum wage", "child care",
    "ban", "banned", "bans", "social media",
    "rent", "healthcare", "Obamacare", "ACA",
    "trade", "military", "war", "defense",
    # Approval / Polling (2)
    "approval rating", "favorability",
    # Political Actions (11)
    "resign", "leave office", "endorse", "referendum", "vote", "ballot",
    "eliminated", "commissioner",
    # General (2)
    "political", "government",
]


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# =============================================================================
# SEARCH-BASED MARKET FETCHING
# =============================================================================

def fetch_kalshi_markets_by_search(term, status="open"):
    """Fetch Kalshi markets matching a search term via Dome API.

    Uses offset-based pagination (the search endpoint returns offset/total,
    not pagination_key). Each page fetches up to 100 markets.
    """
    markets = []
    offset = 0

    while True:
        params = {"search": term, "status": status, "limit": 100, "offset": offset}

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    f"{DOME_KALSHI_BASE}/markets",
                    headers={"Authorization": DOME_API_KEY},
                    params=params,
                    timeout=30
                )

                if response.status_code == 200:
                    data = response.json()
                    batch = data.get("markets", [])
                    markets.extend(batch)

                    pagination = data.get("pagination", {})
                    if pagination.get("has_more") and len(batch) > 0:
                        offset += len(batch)
                    else:
                        return markets
                    break

                elif response.status_code == 429:
                    wait_time = 10 * (2 ** attempt)
                    log(f"    Rate limited on '{term}', waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    log(f"    API error {response.status_code} for '{term}': {response.text[:200]}")
                    return markets

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                log(f"    Error fetching '{term}': {e}")
                return markets

        time.sleep(RATE_LIMIT_DELAY)

    return markets


# =============================================================================
# MAIN
# =============================================================================

def main():
    full_refresh = "--full-refresh" in sys.argv
    statuses = ["open", "closed"] if full_refresh else ["open"]

    print("\n" + "=" * 70)
    print("PIPELINE: SEARCH KALSHI POLITICAL MARKETS")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'FULL REFRESH (open + closed)' if full_refresh else 'INCREMENTAL (open only)'}")
    print(f"Search terms: {len(KALSHI_SEARCH_TERMS)}")
    print("=" * 70 + "\n")

    # ------------------------------------------------------------------
    # Step 1: Search for political markets across all terms & statuses
    # ------------------------------------------------------------------
    all_markets = {}  # market_ticker -> market dict (deduplicated)
    total_api_calls = 0

    for status in statuses:
        log(f"Searching {status} markets across {len(KALSHI_SEARCH_TERMS)} terms...")

        for i, term in enumerate(KALSHI_SEARCH_TERMS):
            markets = fetch_kalshi_markets_by_search(term, status=status)
            pages = max(1, (len(markets) + 99) // 100)
            total_api_calls += pages

            new_count = 0
            for m in markets:
                ticker = m.get("market_ticker")
                if ticker and ticker not in all_markets:
                    all_markets[ticker] = m
                    new_count += 1

            log(f"  [{status}] {i+1}/{len(KALSHI_SEARCH_TERMS)} '{term}' → "
                f"{len(markets)} results ({new_count} new, {pages} pages) | "
                f"Total unique: {len(all_markets):,}")

    log(f"\nSearch complete: {len(all_markets):,} unique markets from {total_api_calls} API calls")

    if not all_markets:
        log("No markets found!")
        return 0

    # ------------------------------------------------------------------
    # Step 2: Extract unique event_tickers with sample titles
    # ------------------------------------------------------------------
    event_tickers_found = {}  # event_ticker -> sample_title
    for m in all_markets.values():
        et = m.get("event_ticker")
        if et and et not in event_tickers_found:
            event_tickers_found[et] = m.get("title", "")

    log(f"Unique political event_tickers found: {len(event_tickers_found):,}")

    # ------------------------------------------------------------------
    # Step 3: Update the event_tickers JSON
    # ------------------------------------------------------------------
    existing_classifications = {}
    if EVENT_TICKERS_FILE.exists():
        with open(EVENT_TICKERS_FILE, 'r') as f:
            existing_classifications = json.load(f)
        log(f"Loaded existing classifications: {len(existing_classifications):,}")

    # Update: mark search-found tickers as political, revert stale search entries
    now = datetime.now().isoformat()
    new_political = 0
    upgraded_political = 0
    reverted = 0

    # First: revert any entries that were previously set by search but aren't found now
    for et, entry in existing_classifications.items():
        if entry.get("source") == "search" and et not in event_tickers_found:
            entry["is_political"] = False
            entry["source"] = "search-reverted"
            reverted += 1

    # Then: mark search-found tickers as political
    for et, sample_title in event_tickers_found.items():
        if et in existing_classifications:
            entry = existing_classifications[et]
            if not entry.get("is_political"):
                entry["is_political"] = True
                entry["source"] = "search"
                entry["classified_at"] = now
                upgraded_political += 1
            elif entry.get("source") not in ("search", "gpt"):
                entry["source"] = "search"
        else:
            existing_classifications[et] = {
                "is_political": True,
                "sample_title": sample_title,
                "classified_at": now,
                "source": "search",
                "votes": 3
            }
            new_political += 1

    log(f"New political event_tickers: {new_political}")
    log(f"Upgraded to political: {upgraded_political}")
    log(f"Reverted (stale search): {reverted}")

    # Save updated JSON
    with open(EVENT_TICKERS_FILE, 'w') as f:
        json.dump(existing_classifications, f, indent=2)
    log(f"Saved event_tickers JSON: {EVENT_TICKERS_FILE}")

    # ------------------------------------------------------------------
    # Step 4: Save market cache for downstream pipeline steps
    # ------------------------------------------------------------------
    cache_data = {
        "generated_at": now,
        "search_terms_count": len(KALSHI_SEARCH_TERMS),
        "statuses_searched": statuses,
        "total_markets": len(all_markets),
        "markets": {
            ticker: {
                "market_ticker": m.get("market_ticker"),
                "event_ticker": m.get("event_ticker"),
                "title": m.get("title"),
                "subtitle": m.get("subtitle", ""),
                "status": m.get("status"),
                "yes_ask": m.get("yes_ask"),
                "yes_bid": m.get("yes_bid"),
                "no_ask": m.get("no_ask"),
                "no_bid": m.get("no_bid"),
                "volume": m.get("volume"),
                "open_interest": m.get("open_interest"),
                "close_time": m.get("close_time"),
                "expiration_time": m.get("expiration_time"),
                "category": m.get("category", ""),
                "result": m.get("result", ""),
            }
            for ticker, m in all_markets.items()
        }
    }

    with open(MARKET_CACHE_FILE, 'w') as f:
        json.dump(cache_data, f, indent=2)
    log(f"Saved market cache: {MARKET_CACHE_FILE} ({len(all_markets):,} markets)")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    political_count = sum(1 for v in existing_classifications.values() if v.get("is_political"))
    non_political_count = sum(1 for v in existing_classifications.values() if v.get("is_political") is False)

    print("\n" + "=" * 70)
    print("SEARCH DISCOVERY COMPLETE")
    print("=" * 70)
    print(f"API calls made: {total_api_calls}")
    print(f"Unique markets found: {len(all_markets):,}")
    print(f"Political event_tickers (total): {political_count:,}")
    print(f"  - New this run: {new_political}")
    print(f"  - Reclassified: {upgraded_political}")
    print(f"Non-political event_tickers: {non_political_count:,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return political_count


if __name__ == "__main__":
    main()
