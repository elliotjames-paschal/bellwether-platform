#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Discover New Political Markets from Dome API
================================================================================

Part of the NEW Bellwether Pipeline (January 2026+)

This script:
1. Pulls OPEN political markets from Dome API (Polymarket + Kalshi)
2. Compares against existing master CSV to find NEW markets only
3. Auto-labels markets with Elections tags as "1. ELECTORAL"
4. For Kalshi: Uses kalshi_political_event_tickers.json (GPT-classified)
5. Outputs new markets for further classification

Usage:
    python pipeline_discover_markets.py [--full-refresh]

Options:
    --full-refresh  Pull all markets (open + closed), not just open
                    Use this for the first run to catch everything

Output:
    - data/new_markets_discovered.csv (new markets to process)
    - Note: market_id_index.json is updated by pipeline_merge_to_master.py

Depends on:
    - data/kalshi_political_event_tickers.json (created by pipeline_classify_kalshi_events.py)

================================================================================
"""

import pandas as pd
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

# Input/Output files
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
INDEX_FILE = DATA_DIR / "market_id_index.json"
OUTPUT_FILE = DATA_DIR / "new_markets_discovered.csv"

# Kalshi event_ticker classifications (created by pipeline_classify_kalshi_events.py)
KALSHI_EVENT_TICKERS_FILE = DATA_DIR / "kalshi_political_event_tickers.json"
KALSHI_MARKET_CACHE_FILE = DATA_DIR / "kalshi_political_markets_cache.json"
DOME_API_KEY = get_dome_api_key()

DOME_PM_BASE = "https://api.domeapi.io/v1/polymarket"
DOME_KALSHI_BASE = "https://api.domeapi.io/v1/kalshi"

# Rate limiting (dev tier: 100 req/sec, free tier: 1 req/sec)
RATE_LIMIT_DELAY = float(os.environ.get('DOME_RATE_LIMIT', '0.01'))
MAX_RETRIES = 3

# =============================================================================
# POLYMARKET POLITICAL SEARCH TERMS (416 terms, 97.3% coverage)
# =============================================================================
# TODO: Future improvement - connect directly to API for live data, implement
# more comprehensive and faster discovery methods. Current search-based approach
# achieves ~97% coverage of known political markets.
#
# Search terms are matched against market titles (case-insensitive word match).
# NOTE: Intentionally NOT including year-based terms (2024, 2025, etc.) as they
# match too many non-political markets and are not forward-looking.
# =============================================================================

POLYMARKET_SEARCH_TERMS = [
    # Elections & Offices
    "election", "elections", "elected", "electoral",
    "senate", "senator", "governor", "governorship", "gubernatorial",
    "congress", "congressional", "representative", "representatives",
    "president", "presidential", "presidency",
    "nominee", "nomination", "primary", "primaries", "mayor", "caucus",
    "candidate", "candidates", "speaker", "attorney general",
    "secretary of state", "House", "RNC", "DNC", "feds",
    "tipping point", "inauguration",
    # Parties
    "democrat", "democratic", "Democrats", "republican", "Republicans",
    "party", "GOP", "convention", "libertarian",
    # Key Figures - US
    "trump", "biden", "musk", "vance", "putin", "Zelenskyy", "Zelensky",
    "powell", "netanyahu", "maduro", "xi jinping", "Xi",
    "RFK", "Kennedy", "Pelosi", "McConnell", "Schumer", "AOC",
    "DeSantis", "Newsom", "Haley", "Obama", "Harris", "Pence", "Kamala",
    "Fauci", "Hillary", "Clinton", "Menendez", "SBF", "Santos",
    "Gaetz", "MTG", "Bannon", "Giuliani", "Hunter", "Altman",
    "Ramaswamy", "Vivek", "Epstein", "Assange", "Sinwar",
    "Fani Willis", "Jay-Z", "Do Kwon", "world leader",
    "DJT", "Elon", "Bernie", "Sanders", "POTUS", "Walz", "Durov",
    # Key Figures - International
    "Trudeau", "Macron", "Starmer", "Sunak", "Modi", "Milei",
    # Organizations & Groups
    "Hamas", "Hezbollah", "Houthis", "Taliban", "ISIS",
    "Binance", "OpenAI", "UNRWA", "Boeing", "Maersk",
    # Political parties (international)
    "SMER", "Labour", "Tory", "Tories",
    # Economic / Fed
    "federal reserve", "Fed", "FOMC", "funds rate", "interest rate",
    "tariff", "tariffs", "GDP", "inflation", "recession", "CPI",
    "basis points", "bps", "employment", "unemployment", "jobs", "NFP",
    "treasury", "yields", "debt", "default", "S&P", "Dow", "Nasdaq",
    "settle", "settlement", "ECB",
    # Government Bodies
    "DOGE", "DOJ", "FTC", "FDA", "SEC", "EPA", "FBI", "CDC", "ATF",
    "Medicare", "social security", "IRS", "CFPB", "TSA", "ICE",
    # Legislative
    "legislation", "bill", "bills", "law", "laws", "filibuster",
    "reconciliation", "repeal", "constitutional", "amendment", "Act",
    "pass", "passed", "veto", "vetoed", "vote", "votes", "voting",
    "testified", "testimony", "hearing", "speech",
    # Judicial & Legal
    "supreme court", "SCOTUS", "lawsuit", "antitrust", "Alito", "Thomas",
    "charged", "charges", "arrested", "indictment", "indicted",
    "pardon", "pardoned", "guilty", "convicted", "sentenced",
    "trial", "verdict", "jail", "prison", "criminal",
    "detain", "detained", "custody", "subpoenaed",
    # Executive
    "executive", "cabinet", "secretary", "white house", "oval office",
    "impeach", "impeached", "impeachment", "confirm", "confirmed",
    "resign", "resignation", "fired", "removed",
    # Government Ops
    "shutdown", "debt ceiling", "budget", "spending", "stimulus",
    "lockdown", "National Guard", "evacuate", "evacuates",
    # International / Conflict
    "NATO", "sanctions", "parliament", "prime minister", "chancellor",
    "EU", "European", "United Nations", "UN", "G7", "G20", "BRICS",
    "Israel", "Israeli", "Ukraine", "Ukrainian", "Russia", "Russian",
    "China", "Chinese", "Iran", "Iranian", "Syria", "Syrian",
    "Gaza", "Gazan", "Palestine", "Palestinian",
    "Greenland", "Canada", "Canadian", "Taiwan", "Taiwanese",
    "North Korea", "DPRK", "Azerbaijan", "Armenia", "Armenian",
    "Venezuela", "Venezuelan", "Guyana", "Haiti", "Haitian", "Guatemala",
    "hostage", "hostages", "ceasefire", "invade", "invasion",
    "seize", "seized", "attack", "attacked", "Crimea", "Crimean",
    "Suez", "canal", "migrants", "migrant", "NYC", "Beirut", "coup",
    # Countries
    "Germany", "German", "France", "French", "UK", "Britain", "British",
    "Japan", "Japanese", "Korea", "Korean", "Mexico", "Mexican",
    "Brazil", "Brazilian", "Argentina", "Turkey", "Turkish",
    "India", "Indian", "Australia", "Australian", "Egypt", "Egyptian",
    # Tech
    "TikTok", "Google", "Apple", "Meta", "Microsoft", "Amazon", "Tesla",
    "Bitcoin", "BTC", "ETH", "Ethereum", "crypto", "cryptocurrency", "ETF",
    # Policy
    "tax", "taxes", "marijuana", "weed", "cannabis", "vaccine", "vaccination",
    "border", "deportation", "deportations", "immigration", "immigrant",
    "visa", "asylum", "climate", "energy", "nuclear", "oil", "gas",
    "ban", "banned", "bans", "unban", "trade", "military", "war", "defense",
    "abortion", "Roe", "healthcare", "Obamacare", "ACA",
    "antisemitism", "antisemitic",
    # Polling & outcomes
    "approval", "poll", "polling", "favorability", "rating",
    "majority", "minority", "control", "flip", "flipped",
    "win", "wins", "debate", "MAGA", "538", "ActBlue", "Oligarchy",
    # Labor & social
    "strike", "strikes", "UAW", "union", "labor",
    "Pope", "Vatican",
    # Actions
    "endorse", "endorsed", "endorsement",
    "ballot", "referendum", "recount", "certify", "certified",
    "surrender",
    # General
    "political", "government", "administration",
    "intelligence", "Doomsday", "balloon", "extradited", "extradition",
    "Texas", "Florida", "California", "New York",
    "Baltimore", "bank failure", "Columbia", "protest", "protests",
    "Balance of Power", "NDA",
    "reopen", "university", "college",
]

# Tags that auto-map to ELECTORAL category (used when processing markets)
ELECTORAL_TAGS = {
    "Elections",
    "US Election",
    "World Elections",
    "Global Elections",
    "Primaries",
}



def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def load_kalshi_event_ticker_classifications():
    """Load classified Kalshi event_tickers.

    Returns:
        dict: Mapping of event_ticker -> {is_political, category, sample_title}
              Returns empty dict if file doesn't exist.
    """
    if KALSHI_EVENT_TICKERS_FILE.exists():
        with open(KALSHI_EVENT_TICKERS_FILE, 'r') as f:
            return json.load(f)
    return {}


def load_kalshi_market_cache():
    """Load cached Kalshi political markets from pipeline_classify_kalshi_events.py.

    Returns:
        dict: Mapping of market_ticker -> market data, or None if cache doesn't exist.
    """
    if KALSHI_MARKET_CACHE_FILE.exists():
        with open(KALSHI_MARKET_CACHE_FILE, 'r') as f:
            cache = json.load(f)
        return cache.get("markets", {})
    return None


def load_market_index():
    """Load or create market ID index for fast lookups."""
    if INDEX_FILE.exists():
        with open(INDEX_FILE, 'r') as f:
            return json.load(f)
    return {"polymarket": [], "kalshi": [], "last_updated": None}


def save_market_index(index):
    """Save market ID index."""
    index["last_updated"] = datetime.now().isoformat()
    with open(INDEX_FILE, 'w') as f:
        json.dump(index, f, indent=2)


def build_index_from_master():
    """Build market ID index from master CSV."""
    log("Building market ID index from master CSV...")

    df = pd.read_csv(MASTER_FILE, low_memory=False)

    pm_markets = df[df['platform'] == 'Polymarket']
    kalshi_markets = df[df['platform'] == 'Kalshi']

    # For Polymarket, we use pm_condition_id (condition_id from Dome API)
    pm_ids = pm_markets['pm_condition_id'].dropna().astype(str).tolist()

    # Also include market_id as fallback
    pm_market_ids = pm_markets['market_id'].dropna().astype(str).tolist()
    pm_ids = list(set(pm_ids + pm_market_ids))

    # For Kalshi, we use market_id (ticker)
    kalshi_ids = kalshi_markets['market_id'].dropna().astype(str).tolist()

    index = {
        "polymarket": pm_ids,
        "kalshi": kalshi_ids,
        "last_updated": datetime.now().isoformat()
    }

    log(f"  Polymarket IDs: {len(pm_ids):,}")
    log(f"  Kalshi IDs: {len(kalshi_ids):,}")

    save_market_index(index)
    return index


def fetch_polymarket_markets_by_search(search_term, status="open", max_pages=20):
    """Fetch Polymarket markets matching a search term via Dome API.

    Uses pagination with a page limit to avoid spending too long on broad terms.
    With 416 overlapping search terms, limiting pages per term still captures
    most political markets while keeping total runtime reasonable.

    Args:
        search_term: Term to search for in market titles
        status: "open" or "closed"
        max_pages: Maximum pages to fetch (100 results per page). Default 20 = 2000 results max.
    """
    markets = []
    pagination_key = None
    pages_fetched = 0

    while pages_fetched < max_pages:
        params = {
            "limit": 100,
            "search": search_term,
            "status": status,
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    f"{DOME_PM_BASE}/markets",
                    headers={"Authorization": DOME_API_KEY},
                    params=params,
                    timeout=30
                )

                if response.status_code == 200:
                    data = response.json()
                    markets.extend(data.get("markets", []))
                    pages_fetched += 1

                    pagination = data.get("pagination", {})
                    if pagination.get("has_more"):
                        pagination_key = pagination.get("pagination_key")
                    else:
                        return markets
                    break

                elif response.status_code == 429:
                    wait_time = 10 * (2 ** attempt)
                    log(f"  Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    log(f"  API error {response.status_code}: {response.text[:100]}")
                    return markets

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                log(f"  Error fetching '{search_term}': {e}")
                return markets

        time.sleep(RATE_LIMIT_DELAY)

    return markets


def fetch_kalshi_markets_by_prefix(prefix, status="open"):
    """Fetch all markets with event_ticker starting with prefix from Kalshi via Dome API."""
    markets = []
    pagination_key = None

    while True:
        params = {
            "limit": 100,
            "event_ticker": prefix,  # Dome API supports event_ticker prefix filtering
            "status": status,
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

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
                    fetched = data.get("markets", [])
                    # Filter to only markets with event_ticker starting with prefix
                    for m in fetched:
                        if m.get("event_ticker", "").startswith(prefix):
                            markets.append(m)

                    pagination = data.get("pagination", {})
                    if pagination.get("has_more"):
                        pagination_key = pagination.get("pagination_key")
                    else:
                        return markets
                    break

                elif response.status_code == 429:
                    wait_time = 10 * (2 ** attempt)
                    log(f"  Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    # Prefix filtering may not be supported - fall back to search
                    return markets

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                log(f"  Error fetching prefix {prefix}: {e}")
                return markets

        time.sleep(RATE_LIMIT_DELAY)

    return markets


def fetch_kalshi_markets(search_term, status="open"):
    """Fetch all markets matching a search term from Kalshi via Dome API."""
    markets = []
    pagination_key = None

    while True:
        params = {
            "limit": 100,
            "search": search_term,
            "status": status,
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

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
                    markets.extend(data.get("markets", []))

                    pagination = data.get("pagination", {})
                    if pagination.get("has_more"):
                        pagination_key = pagination.get("pagination_key")
                    else:
                        return markets
                    break

                elif response.status_code == 429:
                    wait_time = 10 * (2 ** attempt)
                    log(f"  Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    log(f"  API error {response.status_code}: {response.text[:100]}")
                    return markets

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                log(f"  Error fetching {search_term}: {e}")
                return markets

        time.sleep(RATE_LIMIT_DELAY)

    return markets


def process_polymarket_market(market):
    """Convert Dome API Polymarket market to our format."""
    tags = market.get("tags", [])

    # Check if any tag indicates ELECTORAL
    is_electoral = any(t in ELECTORAL_TAGS for t in tags)

    return {
        "platform": "Polymarket",
        "market_id": market.get("market_slug"),
        "pm_condition_id": market.get("condition_id"),
        "pm_token_id_yes": market.get("side_a", {}).get("id"),
        "pm_token_id_no": market.get("side_b", {}).get("id"),
        "question": market.get("title"),
        "tags": json.dumps(tags),
        "volume_usd": market.get("volume_total", 0),
        "scheduled_end_time": datetime.fromtimestamp(market.get("end_time", 0)).isoformat() if market.get("end_time") else None,
        "trading_close_time": datetime.fromtimestamp(market.get("close_time", 0)).isoformat() if market.get("close_time") else None,
        "is_closed": market.get("status") == "closed",
        "pm_closed": market.get("status") == "closed",  # Map to existing column
        "winning_outcome": market.get("winning_side", {}).get("label") if market.get("winning_side") else None,
        "political_category": "1. ELECTORAL" if is_electoral else None,
    }


def process_kalshi_market(market):
    """Convert Dome API Kalshi market to our format.

    Args:
        market: Market data from Dome API

    Note: political_category is left as None - it will be set by pipeline_classify_categories.py
    """
    event_ticker = market.get("event_ticker", "")

    return {
        "platform": "Kalshi",
        "market_id": market.get("market_ticker"),
        "k_event_ticker": event_ticker,
        "question": market.get("title"),
        "volume_usd": market.get("volume", 0),
        "k_expiration_time": datetime.fromtimestamp(market.get("end_time", 0)).isoformat() if market.get("end_time") else None,
        "trading_close_time": datetime.fromtimestamp(market.get("close_time", 0)).isoformat() if market.get("close_time") else None,
        "is_closed": market.get("status") in ("closed", "finalized", "settled"),
        "k_status": market.get("status"),  # Map to existing column
        "k_last_price": market.get("last_price"),  # Map to existing column
        "winning_outcome": market.get("result"),
        "political_category": None,  # Set by pipeline_classify_categories.py
    }


def main():
    """Main function to discover new political markets."""
    full_refresh = "--full-refresh" in sys.argv

    print("\n" + "=" * 70)
    print("PIPELINE: DISCOVER NEW POLITICAL MARKETS")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'FULL REFRESH' if full_refresh else 'DAILY (open markets only)'}")
    print("=" * 70 + "\n")

    # Load or build market ID index
    log("Loading market ID index...")
    if not INDEX_FILE.exists():
        index = build_index_from_master()
    else:
        index = load_market_index()
        log(f"  Loaded index: {len(index['polymarket']):,} PM, {len(index['kalshi']):,} Kalshi")

    existing_pm_ids = set(index["polymarket"])
    existing_kalshi_ids = set(index["kalshi"])

    # ==========================================================================
    # POLYMARKET: Fetch markets by political search terms
    # ==========================================================================
    # NOTE: Using search-based discovery (~98.7% coverage) instead of tag-based
    # fetching. Tag-based was pulling 300K+ results and taking 6+ hours.
    # Search-based completes in ~5-10 minutes.
    # ==========================================================================

    log("\n" + "=" * 50)
    log("FETCHING POLYMARKET MARKETS (SEARCH-BASED)")
    log("=" * 50)

    all_pm_markets = {}
    statuses = ["open", "closed"] if full_refresh else ["open"]
    total_api_calls = 0

    for status in statuses:
        log(f"Searching {status} markets across {len(POLYMARKET_SEARCH_TERMS)} terms...")

        for i, term in enumerate(POLYMARKET_SEARCH_TERMS):
            markets = fetch_polymarket_markets_by_search(term, status=status)
            total_api_calls += 1

            new_count = 0
            for m in markets:
                condition_id = m.get("condition_id")
                if condition_id and condition_id not in all_pm_markets:
                    all_pm_markets[condition_id] = m
                    new_count += 1

            # Log every term with results count
            hit_limit = len(markets) == 2000  # Hit max_pages limit (20 pages * 100 results)
            limit_note = " [HIT LIMIT]" if hit_limit else ""
            log(f"  [{status}] {i+1}/{len(POLYMARKET_SEARCH_TERMS)} '{term}' → "
                f"{len(markets)} results ({new_count} new){limit_note} | Total unique: {len(all_pm_markets):,}")

            time.sleep(RATE_LIMIT_DELAY)

    log(f"\nSearch complete: {len(all_pm_markets):,} unique markets from {total_api_calls} API calls")

    # Filter to NEW markets only, and collect volume updates for existing
    new_pm_markets = []
    pm_volume_updates = {}  # condition_id -> volume_total
    for condition_id, market in all_pm_markets.items():
        if condition_id not in existing_pm_ids:
            new_pm_markets.append(process_polymarket_market(market))
        else:
            # Track volume update for existing market
            vol = market.get("volume_total", 0)
            if vol:
                pm_volume_updates[condition_id] = vol

    log(f"NEW Polymarket markets (not in master): {len(new_pm_markets):,}")
    log(f"Existing PM markets with volume updates: {len(pm_volume_updates):,}")

    # ==========================================================================
    # KALSHI: Load markets from cache or fall back to per-ticker fetch
    # ==========================================================================

    log("\n" + "=" * 50)
    log("FETCHING KALSHI MARKETS")
    log("=" * 50)

    all_kalshi_markets = {}

    # Try loading from market cache first (created by pipeline_classify_kalshi_events.py)
    cached_markets = load_kalshi_market_cache()

    if cached_markets:
        log(f"Loaded market cache: {len(cached_markets):,} political markets")
        all_kalshi_markets = cached_markets
    else:
        # Fallback: load event_ticker classifications and fetch per-ticker
        log("No market cache found, falling back to per-ticker fetch...")
        event_ticker_classifications = load_kalshi_event_ticker_classifications()

        if event_ticker_classifications:
            log(f"Loaded {len(event_ticker_classifications):,} event_ticker classifications")

            political_event_tickers = {
                et for et, info in event_ticker_classifications.items()
                if info.get("is_political")
            }
            log(f"Political event_tickers: {len(political_event_tickers):,}")

            processed_event_tickers = set()

            for event_ticker in political_event_tickers:
                if event_ticker in processed_event_tickers:
                    continue

                for status in statuses:
                    markets = fetch_kalshi_markets_by_prefix(event_ticker, status)

                    for m in markets:
                        ticker = m.get("market_ticker")
                        et = m.get("event_ticker", "")
                        if ticker and ticker not in all_kalshi_markets and et in political_event_tickers:
                            all_kalshi_markets[ticker] = m

                    time.sleep(RATE_LIMIT_DELAY)

                processed_event_tickers.add(event_ticker)

                if len(processed_event_tickers) % 50 == 0:
                    log(f"    Processed {len(processed_event_tickers)}/{len(political_event_tickers)} event_tickers, found {len(all_kalshi_markets)} markets")

            log(f"  Found {len(all_kalshi_markets):,} markets from political event_tickers")
        else:
            log("WARNING: No event_ticker classifications found!")
            log("Run pipeline_classify_kalshi_events.py first.")

    log(f"\nTotal unique Kalshi markets: {len(all_kalshi_markets):,}")

    # Filter to NEW markets only, and collect volume updates for existing
    new_kalshi_markets = []
    kalshi_volume_updates = {}  # market_ticker -> volume
    for ticker, market in all_kalshi_markets.items():
        if ticker not in existing_kalshi_ids:
            new_kalshi_markets.append(process_kalshi_market(market))
        else:
            # Track volume update for existing market
            vol = market.get("volume", 0)
            if vol:
                kalshi_volume_updates[ticker] = vol

    log(f"NEW Kalshi markets (not in master): {len(new_kalshi_markets):,}")
    log(f"Existing Kalshi markets with volume updates: {len(kalshi_volume_updates):,}")

    # ==========================================================================
    # COMBINE AND SAVE
    # ==========================================================================

    log("\n" + "=" * 50)
    log("SAVING RESULTS")
    log("=" * 50)

    all_new_markets = new_pm_markets + new_kalshi_markets

    if all_new_markets:
        df_new = pd.DataFrame(all_new_markets)
        df_new.to_csv(OUTPUT_FILE, index=False)
        log(f"Saved {len(all_new_markets):,} new markets to: {OUTPUT_FILE}")

        # Summary
        auto_electoral = df_new[df_new['political_category'] == '1. ELECTORAL']
        needs_classification = df_new[df_new['political_category'].isna()]

        log(f"\nSummary:")
        log(f"  Auto-labeled as ELECTORAL (Polymarket tags): {len(auto_electoral):,}")
        log(f"  Needs GPT classification: {len(needs_classification):,}")
        log(f"  By platform:")
        log(f"    Polymarket: {len(new_pm_markets):,}")
        log(f"    Kalshi: {len(new_kalshi_markets):,} (all need classification)")
    else:
        log("No new markets found!")

    # NOTE: Index is updated by pipeline_merge_to_master.py AFTER successful merge
    # This prevents index/master desync if pipeline fails between discovery and merge
    log(f"\nNote: Index will be updated by pipeline_merge_to_master.py after merge")

    # ==========================================================================
    # UPDATE VOLUME FOR EXISTING MARKETS
    # ==========================================================================
    if pm_volume_updates or kalshi_volume_updates:
        log("\n" + "=" * 50)
        log("UPDATING VOLUME FOR EXISTING MARKETS")
        log("=" * 50)

        # Load master CSV
        master_df = pd.read_csv(MASTER_FILE, low_memory=False)
        updates_applied = 0

        # Update Polymarket volumes
        if pm_volume_updates:
            for idx, row in master_df.iterrows():
                if row.get('platform') == 'Polymarket':
                    cid = row.get('pm_condition_id')
                    if cid and cid in pm_volume_updates:
                        old_vol = row.get('volume_usd', 0)
                        new_vol = pm_volume_updates[cid]
                        if new_vol != old_vol:
                            master_df.at[idx, 'volume_usd'] = new_vol
                            updates_applied += 1

        # Update Kalshi volumes
        if kalshi_volume_updates:
            for idx, row in master_df.iterrows():
                if row.get('platform') == 'Kalshi':
                    mid = row.get('market_id')
                    if mid and mid in kalshi_volume_updates:
                        old_vol = row.get('volume_usd', 0)
                        new_vol = kalshi_volume_updates[mid]
                        if new_vol != old_vol:
                            master_df.at[idx, 'volume_usd'] = new_vol
                            updates_applied += 1

        # Save updated master
        if updates_applied > 0:
            master_df.to_csv(MASTER_FILE, index=False)
            log(f"Updated volume for {updates_applied:,} existing markets in master CSV")
        else:
            log("No volume changes detected")

    print("\n" + "=" * 70)
    print("DISCOVERY COMPLETE")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return len(all_new_markets)


if __name__ == "__main__":
    sys.exit(0 if main() >= 0 else 1)
