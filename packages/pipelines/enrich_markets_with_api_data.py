#!/usr/bin/env python3
"""
================================================================================
Market Enrichment Script
================================================================================

Enriches existing political markets from the master CSV with full native API
data from Kalshi and Polymarket.

INPUT:
  - /data/combined_political_markets_with_electoral_details_UPDATED.csv
  - ~4,534 active Kalshi markets (k_status IN ('active', 'open'))
  - ~8,574 active Polymarket markets (pm_closed != 'True')

OUTPUT:
  - /data/enriched_political_markets.json

PHASES:
  1. Fetch market-level data in parallel
  2. Fetch event-level data (deduplicated by event_ticker/event_id)
  3. Fetch additional metadata (series for Kalshi, tags for Polymarket)
  4. Stitch all responses together by foreign keys

Usage:
    python packages/pipelines/enrich_markets_with_api_data.py              # incremental (default)
    python packages/pipelines/enrich_markets_with_api_data.py --full-refresh  # re-fetch everything

================================================================================
"""

import argparse
import asyncio
import aiohttp
import pandas as pd
import json
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Set, Tuple
from dataclasses import dataclass, field
import time

# =============================================================================
# CONFIGURATION
# =============================================================================

# Data directory
from config import DATA_DIR

# API Base URLs
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"

# Input/Output Files
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
OUTPUT_FILE = DATA_DIR / "enriched_political_markets.json"
CHECKPOINT_FILE = DATA_DIR / ".enrichment_checkpoint.json"
ARCHIVE_FILE = DATA_DIR / "enriched_political_markets_archive.json"

# Rate Limiting & Parallelism
SEMAPHORE_LIMIT = 10  # Max concurrent requests per platform
REQUEST_DELAY_MS = 50  # Minimum ms between requests
MAX_RETRIES = 3
CHECKPOINT_INTERVAL = 500  # Save checkpoint every N markets


# =============================================================================
# LOGGING
# =============================================================================

def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# =============================================================================
# ASYNC API CLIENT
# =============================================================================

@dataclass
class APIClient:
    """Async API client with rate limiting and retry logic."""

    base_url: str
    semaphore: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(SEMAPHORE_LIMIT))
    last_request_time: float = 0.0

    async def _rate_limit(self):
        """Enforce minimum delay between requests."""
        now = time.time()
        elapsed_ms = (now - self.last_request_time) * 1000
        if elapsed_ms < REQUEST_DELAY_MS:
            await asyncio.sleep((REQUEST_DELAY_MS - elapsed_ms) / 1000)
        self.last_request_time = time.time()

    async def get(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make a GET request with rate limiting and retry logic.

        Returns:
            Dict with 'data' on success, 'error' on failure
        """
        url = f"{self.base_url}{endpoint}"

        async with self.semaphore:
            await self._rate_limit()

            for attempt in range(MAX_RETRIES):
                try:
                    async with session.get(url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            return {"data": data, "error": None}

                        elif response.status == 429:
                            # Rate limited - exponential backoff
                            wait_time = (2 ** attempt) * 1
                            log(f"  Rate limited on {endpoint}, waiting {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue

                        elif response.status == 404:
                            # Not found - don't retry
                            return {"data": None, "error": f"404 Not Found: {endpoint}"}

                        else:
                            text = await response.text()
                            return {"data": None, "error": f"HTTP {response.status}: {text[:200]}"}

                except asyncio.TimeoutError:
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return {"data": None, "error": "Request timeout"}

                except Exception as e:
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return {"data": None, "error": str(e)}

            return {"data": None, "error": "Max retries exceeded"}


# =============================================================================
# KALSHI FETCHERS
# =============================================================================

async def fetch_kalshi_market(
    session: aiohttp.ClientSession,
    client: APIClient,
    ticker: str
) -> Dict[str, Any]:
    """Fetch full market data for a Kalshi ticker."""
    result = await client.get(session, f"/markets/{ticker}")
    if result["data"]:
        return {"data": result["data"].get("market", result["data"]), "error": None}
    return result


async def fetch_kalshi_event(
    session: aiohttp.ClientSession,
    client: APIClient,
    event_ticker: str
) -> Dict[str, Any]:
    """Fetch event data for a Kalshi event ticker."""
    result = await client.get(session, f"/events/{event_ticker}")
    if result["data"]:
        return {"data": result["data"].get("event", result["data"]), "error": None}
    return result


async def fetch_kalshi_series(
    session: aiohttp.ClientSession,
    client: APIClient,
    series_ticker: str
) -> Dict[str, Any]:
    """Fetch series data for a Kalshi series ticker."""
    result = await client.get(session, f"/series/{series_ticker}")
    if result["data"]:
        return {"data": result["data"].get("series", result["data"]), "error": None}
    return result


async def fetch_kalshi_event_metadata(
    session: aiohttp.ClientSession,
    client: APIClient,
    event_ticker: str
) -> Dict[str, Any]:
    """Fetch election-specific metadata for a Kalshi event."""
    return await client.get(session, f"/events/{event_ticker}/metadata")


# =============================================================================
# POLYMARKET FETCHERS
# =============================================================================

# CLOB API for condition_id lookups
POLYMARKET_CLOB_BASE = "https://clob.polymarket.com"


async def fetch_polymarket_market_by_slug(
    session: aiohttp.ClientSession,
    client: APIClient,
    slug: str
) -> Dict[str, Any]:
    """Fetch market data by slug."""
    result = await client.get(session, "/markets", params={"slug": slug})
    if result["data"]:
        markets = result["data"] if isinstance(result["data"], list) else [result["data"]]
        if markets and markets[0]:
            return {"data": markets[0], "error": None}
    return {"data": None, "error": result.get("error", "Empty response")}


async def fetch_polymarket_market_by_id(
    session: aiohttp.ClientSession,
    client: APIClient,
    market_id: str
) -> Dict[str, Any]:
    """Fetch market data by numeric ID."""
    result = await client.get(session, f"/markets/{market_id}")
    if result["data"]:
        return {"data": result["data"], "error": None}
    return result


async def fetch_polymarket_market_by_condition_id(
    session: aiohttp.ClientSession,
    condition_id: str
) -> Dict[str, Any]:
    """Fetch market data from CLOB API by condition_id."""
    url = f"{POLYMARKET_CLOB_BASE}/markets/{condition_id}"
    try:
        async with session.get(url, timeout=15) as response:
            if response.status == 200:
                data = await response.json()
                return {"data": data, "error": None}
            return {"data": None, "error": f"HTTP {response.status}"}
    except Exception as e:
        return {"data": None, "error": str(e)}


async def fetch_polymarket_market(
    session: aiohttp.ClientSession,
    client: APIClient,
    pm_market_slug: Optional[str],
    market_id: str,
    condition_id: Optional[str]
) -> Dict[str, Any]:
    """
    Fetch Polymarket market data with fallback chain:
    1. Try pm_market_slug via ?slug= endpoint
    2. Try market_id via /markets/{id} if numeric
    3. Try condition_id via CLOB API
    """
    # Strategy 1: Use pm_market_slug if available
    if pm_market_slug and str(pm_market_slug).strip():
        result = await fetch_polymarket_market_by_slug(session, client, pm_market_slug)
        if result["data"]:
            return result

    # Strategy 2: Use market_id directly if it's numeric (gamma /markets/{id})
    if market_id and str(market_id).isdigit():
        result = await fetch_polymarket_market_by_id(session, client, market_id)
        if result["data"]:
            return result

    # Strategy 3: Use market_id as slug if it looks like a slug (non-numeric)
    if market_id and not str(market_id).isdigit():
        result = await fetch_polymarket_market_by_slug(session, client, market_id)
        if result["data"]:
            return result

    # Strategy 4: Use condition_id via CLOB API
    if condition_id and str(condition_id).startswith("0x"):
        result = await fetch_polymarket_market_by_condition_id(session, condition_id)
        if result["data"]:
            return result

    return {"data": None, "error": "All lookup strategies failed"}


async def fetch_polymarket_event(
    session: aiohttp.ClientSession,
    client: APIClient,
    event_slug: str
) -> Dict[str, Any]:
    """Fetch event data for a Polymarket event by slug."""
    result = await client.get(session, "/events", params={"slug": event_slug})
    if result["data"]:
        events = result["data"] if isinstance(result["data"], list) else [result["data"]]
        return {"data": events[0] if events else None, "error": None}
    return result


# =============================================================================
# CHECKPOINT MANAGEMENT
# =============================================================================

def save_checkpoint(
    processed_kalshi: Set[str],
    processed_polymarket: Set[str],
    kalshi_results: Dict[str, Any],
    polymarket_results: Dict[str, Any],
    kalshi_events: Dict[str, Any],
    polymarket_events: Dict[str, Any],
    kalshi_series: Dict[str, Any],
    errors: List[Dict]
):
    """Save checkpoint to allow resuming."""
    checkpoint = {
        "timestamp": datetime.now().isoformat(),
        "processed_kalshi": list(processed_kalshi),
        "processed_polymarket": list(processed_polymarket),
        "kalshi_results": kalshi_results,
        "polymarket_results": polymarket_results,
        "kalshi_events": kalshi_events,
        "polymarket_events": polymarket_events,
        "kalshi_series": kalshi_series,
        "errors": errors
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f)
    log(f"  Checkpoint saved: {len(processed_kalshi)} Kalshi, {len(processed_polymarket)} Polymarket")


def load_checkpoint() -> Optional[Dict]:
    """Load checkpoint if exists."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE) as f:
                return json.load(f)
        except:
            pass
    return None


def clear_checkpoint():
    """Remove checkpoint file."""
    if CHECKPOINT_FILE.exists():
        os.remove(CHECKPOINT_FILE)


# =============================================================================
# PHASE 1: FETCH MARKET-LEVEL DATA
# =============================================================================

async def phase1_fetch_markets(
    df: pd.DataFrame,
    checkpoint: Optional[Dict] = None
) -> tuple:
    """
    Phase 1: Fetch all market-level data in parallel.

    Returns:
        (kalshi_results, polymarket_results, errors, event_tickers, pm_event_ids)
    """
    log("\n" + "=" * 60)
    log("PHASE 1: Fetching Market-Level Data")
    log("=" * 60)

    # Initialize from checkpoint if available
    if checkpoint:
        processed_kalshi = set(checkpoint.get("processed_kalshi", []))
        processed_polymarket = set(checkpoint.get("processed_polymarket", []))
        kalshi_results = checkpoint.get("kalshi_results", {})
        polymarket_results = checkpoint.get("polymarket_results", {})
        errors = checkpoint.get("errors", [])
        log(f"  Resuming from checkpoint: {len(processed_kalshi)} Kalshi, {len(processed_polymarket)} Polymarket already done")
    else:
        processed_kalshi = set()
        processed_polymarket = set()
        kalshi_results = {}
        polymarket_results = {}
        errors = []

    # Split by platform
    kalshi_df = df[df['platform'].str.lower() == 'kalshi'].copy()
    pm_df = df[df['platform'].str.lower() == 'polymarket'].copy()

    kalshi_tickers = [t for t in kalshi_df['market_id'].astype(str).unique() if t not in processed_kalshi]

    # For Polymarket, collect all identifiers for multi-strategy lookup
    pm_markets_to_fetch = []  # List of dicts with all identifiers
    pm_market_id_to_row = {}  # Map market_id -> row for event_slug lookup
    for _, row in pm_df.iterrows():
        market_id = str(row.get('market_id', ''))
        if market_id and market_id not in processed_polymarket:
            pm_markets_to_fetch.append({
                'market_id': market_id,
                'pm_market_slug': row.get('pm_market_slug') if pd.notna(row.get('pm_market_slug')) else None,
                'pm_condition_id': row.get('pm_condition_id') if pd.notna(row.get('pm_condition_id')) else None,
            })
            pm_market_id_to_row[market_id] = row

    log(f"  Kalshi markets to fetch: {len(kalshi_tickers)}")
    log(f"  Polymarket markets to fetch: {len(pm_markets_to_fetch)}")

    # Create clients
    kalshi_client = APIClient(KALSHI_API_BASE)
    pm_client = APIClient(POLYMARKET_API_BASE)

    connector = aiohttp.TCPConnector(limit=20, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Fetch Kalshi markets
        log("\n  Fetching Kalshi markets...")
        kalshi_count = 0
        for i in range(0, len(kalshi_tickers), 100):
            batch = kalshi_tickers[i:i+100]
            tasks = [fetch_kalshi_market(session, kalshi_client, t) for t in batch]
            results = await asyncio.gather(*tasks)

            for ticker, result in zip(batch, results):
                if result["data"]:
                    kalshi_results[ticker] = result["data"]
                else:
                    errors.append({
                        "platform": "kalshi",
                        "market_id": ticker,
                        "error": result["error"],
                        "phase": 1
                    })
                processed_kalshi.add(ticker)

            kalshi_count += len(batch)
            if kalshi_count % CHECKPOINT_INTERVAL == 0:
                log(f"    Processed {kalshi_count}/{len(kalshi_tickers)} Kalshi markets")
                # We'll save checkpoint at the end of Phase 1

        log(f"    Completed: {len(kalshi_results)} Kalshi markets fetched, {len([e for e in errors if e['platform'] == 'kalshi'])} errors")

        # Fetch Polymarket markets with multi-strategy lookup
        log("\n  Fetching Polymarket markets...")
        pm_count = 0
        for i in range(0, len(pm_markets_to_fetch), 100):
            batch = pm_markets_to_fetch[i:i+100]
            tasks = [
                fetch_polymarket_market(
                    session, pm_client,
                    m['pm_market_slug'],
                    m['market_id'],
                    m['pm_condition_id']
                ) for m in batch
            ]
            results = await asyncio.gather(*tasks)

            for market_info, result in zip(batch, results):
                market_id = market_info['market_id']
                if result["data"]:
                    polymarket_results[market_id] = result["data"]
                else:
                    errors.append({
                        "platform": "polymarket",
                        "market_id": market_id,
                        "error": result["error"],
                        "phase": 1
                    })
                processed_polymarket.add(market_id)

            pm_count += len(batch)
            if pm_count % CHECKPOINT_INTERVAL == 0:
                log(f"    Processed {pm_count}/{len(pm_markets_to_fetch)} Polymarket markets")

        log(f"    Completed: {len(polymarket_results)} Polymarket markets fetched, {len([e for e in errors if e['platform'] == 'polymarket'])} errors")

    # Extract unique event tickers for Phase 2
    kalshi_event_tickers = set()
    for market_data in kalshi_results.values():
        event_ticker = market_data.get("event_ticker")
        if event_ticker:
            kalshi_event_tickers.add(event_ticker)

    # For Polymarket, extract event slugs from market responses or CSV
    pm_event_slugs = set()
    for market_id, market_data in polymarket_results.items():
        # Check events array in market response
        events = market_data.get("events", [])
        for event in events:
            event_slug = event.get("slug")
            if event_slug:
                pm_event_slugs.add(event_slug)
        # Also check CSV for pm_event_slug
        row = pm_market_id_to_row.get(market_id)
        if row is not None:
            csv_event_slug = row.get('pm_event_slug')
            if pd.notna(csv_event_slug) and csv_event_slug:
                pm_event_slugs.add(str(csv_event_slug))

    log(f"\n  Unique Kalshi events to fetch: {len(kalshi_event_tickers)}")
    log(f"  Unique Polymarket events to fetch: {len(pm_event_slugs)}")

    return (
        kalshi_results,
        polymarket_results,
        errors,
        kalshi_event_tickers,
        pm_event_slugs
    )


# =============================================================================
# PHASE 2: FETCH EVENT-LEVEL DATA
# =============================================================================

async def phase2_fetch_events(
    kalshi_event_tickers: Set[str],
    pm_event_slugs: Set[str],
    checkpoint: Optional[Dict] = None
) -> tuple:
    """
    Phase 2: Fetch event-level data for unique event tickers/slugs.

    Returns:
        (kalshi_events, polymarket_events, errors)
    """
    log("\n" + "=" * 60)
    log("PHASE 2: Fetching Event-Level Data")
    log("=" * 60)

    # Initialize from checkpoint if available
    if checkpoint:
        kalshi_events = checkpoint.get("kalshi_events", {})
        polymarket_events = checkpoint.get("polymarket_events", {})
    else:
        kalshi_events = {}
        polymarket_events = {}

    errors = []

    # Filter to unfetched events
    kalshi_to_fetch = [e for e in kalshi_event_tickers if e not in kalshi_events]
    pm_to_fetch = [e for e in pm_event_slugs if e not in polymarket_events]

    log(f"  Kalshi events to fetch: {len(kalshi_to_fetch)}")
    log(f"  Polymarket events to fetch: {len(pm_to_fetch)}")

    # Create clients
    kalshi_client = APIClient(KALSHI_API_BASE)
    pm_client = APIClient(POLYMARKET_API_BASE)

    connector = aiohttp.TCPConnector(limit=20, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Fetch Kalshi events
        log("\n  Fetching Kalshi events...")
        for i in range(0, len(kalshi_to_fetch), 50):
            batch = kalshi_to_fetch[i:i+50]
            tasks = [fetch_kalshi_event(session, kalshi_client, e) for e in batch]
            results = await asyncio.gather(*tasks)

            for event_ticker, result in zip(batch, results):
                if result["data"]:
                    kalshi_events[event_ticker] = result["data"]
                else:
                    errors.append({
                        "platform": "kalshi",
                        "event_ticker": event_ticker,
                        "error": result["error"],
                        "phase": 2
                    })

        log(f"    Completed: {len(kalshi_events)} Kalshi events fetched")

        # Fetch Polymarket events
        log("\n  Fetching Polymarket events...")
        for i in range(0, len(pm_to_fetch), 50):
            batch = pm_to_fetch[i:i+50]
            tasks = [fetch_polymarket_event(session, pm_client, e) for e in batch]
            results = await asyncio.gather(*tasks)

            for event_slug, result in zip(batch, results):
                if result["data"]:
                    polymarket_events[event_slug] = result["data"]
                else:
                    errors.append({
                        "platform": "polymarket",
                        "event_slug": event_slug,
                        "error": result["error"],
                        "phase": 2
                    })

        log(f"    Completed: {len(polymarket_events)} Polymarket events fetched")

    return kalshi_events, polymarket_events, errors


# =============================================================================
# PHASE 3: FETCH ADDITIONAL METADATA
# =============================================================================

async def phase3_fetch_metadata(
    kalshi_results: Dict[str, Any],
    kalshi_events: Dict[str, Any],
    checkpoint: Optional[Dict] = None
) -> tuple:
    """
    Phase 3: Fetch additional metadata.
    - Kalshi: series, event metadata
    - Polymarket: tags (already included in market response typically)

    Returns:
        (kalshi_series, kalshi_metadata, errors)
    """
    log("\n" + "=" * 60)
    log("PHASE 3: Fetching Additional Metadata")
    log("=" * 60)

    # Initialize from checkpoint if available
    if checkpoint:
        kalshi_series = checkpoint.get("kalshi_series", {})
    else:
        kalshi_series = {}

    kalshi_metadata = {}
    errors = []

    # Extract unique series tickers
    series_tickers = set()
    for market_data in kalshi_results.values():
        series_ticker = market_data.get("series_ticker")
        if series_ticker:
            series_tickers.add(series_ticker)

    # Also check events
    for event_data in kalshi_events.values():
        series_ticker = event_data.get("series_ticker")
        if series_ticker:
            series_tickers.add(series_ticker)

    series_to_fetch = [s for s in series_tickers if s not in kalshi_series]

    log(f"  Kalshi series to fetch: {len(series_to_fetch)}")
    log(f"  Kalshi event metadata to fetch: {len(kalshi_events)}")

    # Create client
    kalshi_client = APIClient(KALSHI_API_BASE)

    connector = aiohttp.TCPConnector(limit=20, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Fetch Kalshi series
        log("\n  Fetching Kalshi series...")
        for i in range(0, len(series_to_fetch), 50):
            batch = series_to_fetch[i:i+50]
            tasks = [fetch_kalshi_series(session, kalshi_client, s) for s in batch]
            results = await asyncio.gather(*tasks)

            for series_ticker, result in zip(batch, results):
                if result["data"]:
                    kalshi_series[series_ticker] = result["data"]
                else:
                    errors.append({
                        "platform": "kalshi",
                        "series_ticker": series_ticker,
                        "error": result["error"],
                        "phase": 3
                    })

        log(f"    Completed: {len(kalshi_series)} Kalshi series fetched")

        # Fetch Kalshi event metadata
        log("\n  Fetching Kalshi event metadata...")
        event_tickers = list(kalshi_events.keys())
        for i in range(0, len(event_tickers), 50):
            batch = event_tickers[i:i+50]
            tasks = [fetch_kalshi_event_metadata(session, kalshi_client, e) for e in batch]
            results = await asyncio.gather(*tasks)

            for event_ticker, result in zip(batch, results):
                if result["data"]:
                    kalshi_metadata[event_ticker] = result["data"]
                # Don't record as error - metadata endpoint may not exist for all events

        log(f"    Completed: {len(kalshi_metadata)} Kalshi event metadata fetched")

    return kalshi_series, kalshi_metadata, errors


# =============================================================================
# PHASE 4: STITCH RESULTS TOGETHER
# =============================================================================

def phase4_stitch_results(
    df: pd.DataFrame,
    kalshi_results: Dict[str, Any],
    polymarket_results: Dict[str, Any],
    kalshi_events: Dict[str, Any],
    polymarket_events: Dict[str, Any],
    kalshi_series: Dict[str, Any],
    kalshi_metadata: Dict[str, Any],
    all_errors: List[Dict]
) -> Dict[str, Any]:
    """
    Phase 4: Stitch all API responses together with original CSV data.

    Returns:
        Complete output structure for JSON
    """
    log("\n" + "=" * 60)
    log("PHASE 4: Stitching Results Together")
    log("=" * 60)

    markets = []

    for _, row in df.iterrows():
        market_entry = {
            "original_csv": row.to_dict(),
            "api_data": {
                "market": None,
                "event": None,
                "series": None,
                "metadata": None,
                "tags": None
            },
            "fetch_errors": []
        }

        platform = str(row.get('platform', '')).lower()
        market_id = str(row.get('market_id', ''))
        slug = None  # For Polymarket

        if platform == 'kalshi':
            # Get market data
            ticker = market_id
            if ticker in kalshi_results:
                market_data = kalshi_results[ticker]
                market_entry["api_data"]["market"] = market_data

                # Get event data
                event_ticker = market_data.get("event_ticker")
                if event_ticker and event_ticker in kalshi_events:
                    market_entry["api_data"]["event"] = kalshi_events[event_ticker]

                    # Get metadata
                    if event_ticker in kalshi_metadata:
                        market_entry["api_data"]["metadata"] = kalshi_metadata[event_ticker]

                # Get series data
                series_ticker = market_data.get("series_ticker")
                if series_ticker and series_ticker in kalshi_series:
                    market_entry["api_data"]["series"] = kalshi_series[series_ticker]
            else:
                # Record error
                market_entry["fetch_errors"].append(f"Market data not found for ticker: {ticker}")

        elif platform == 'polymarket':
            # Get market data by market_id (now keyed by market_id)
            if market_id and market_id in polymarket_results:
                market_data = polymarket_results[market_id]
                market_entry["api_data"]["market"] = market_data

                # Tags are typically included in market response
                if "tags" in market_data:
                    market_entry["api_data"]["tags"] = market_data["tags"]

                # Get event data - check events array in market response for slug
                events = market_data.get("events", [])
                for event in events:
                    event_slug = event.get("slug")
                    if event_slug and event_slug in polymarket_events:
                        market_entry["api_data"]["event"] = polymarket_events[event_slug]
                        break

                # Also try pm_event_slug from CSV
                if market_entry["api_data"]["event"] is None:
                    csv_event_slug = row.get('pm_event_slug')
                    if pd.notna(csv_event_slug) and csv_event_slug and str(csv_event_slug) in polymarket_events:
                        market_entry["api_data"]["event"] = polymarket_events[str(csv_event_slug)]
            else:
                # Record error
                market_entry["fetch_errors"].append(f"Market data not found for market_id: {market_id}")

        # Add any phase errors for this market
        for error in all_errors:
            if error.get("market_id") == market_id:
                market_entry["fetch_errors"].append(error.get("error", "Unknown error"))

        markets.append(market_entry)

    # Calculate stats
    kalshi_count = len([m for m in markets if str(m["original_csv"].get("platform", "")).lower() == "kalshi"])
    pm_count = len([m for m in markets if str(m["original_csv"].get("platform", "")).lower() == "polymarket"])
    failed_count = len([m for m in markets if m["fetch_errors"]])

    log(f"  Total markets: {len(markets)}")
    log(f"  Kalshi: {kalshi_count}")
    log(f"  Polymarket: {pm_count}")
    log(f"  Markets with fetch errors: {failed_count}")

    return {
        "generated_at": datetime.now().isoformat(),
        "stats": {
            "total_markets": len(markets),
            "kalshi_markets": kalshi_count,
            "polymarket_markets": pm_count,
            "failed_fetches": failed_count,
            "kalshi_events_fetched": len(kalshi_events),
            "polymarket_events_fetched": len(polymarket_events),
            "kalshi_series_fetched": len(kalshi_series),
            "total_phase_errors": len(all_errors)
        },
        "markets": markets
    }


# =============================================================================
# INCREMENTAL ENRICHMENT FILTER
# =============================================================================

def filter_to_unenriched(
    active_df: pd.DataFrame,
    retry_days: int = 7
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Load existing enriched data and filter active_df to only markets that
    need enrichment (new or recently failed).

    Returns:
        (filtered_df, existing_by_id) where existing_by_id maps market_id
        to its enriched entry for later merging.
    """
    existing_by_id: Dict[str, Any] = {}

    if not OUTPUT_FILE.exists():
        log("  No existing enriched file found — will enrich all active markets")
        return active_df, existing_by_id

    log("  Loading existing enriched data...")
    with open(OUTPUT_FILE) as f:
        existing_data = json.load(f)

    # Index existing entries by market_id
    for entry in existing_data.get("markets", []):
        mid = str(entry.get("original_csv", {}).get("market_id", ""))
        if mid:
            existing_by_id[mid] = entry

    log(f"  Existing enriched markets: {len(existing_by_id):,}")

    cutoff_date = datetime.now() - timedelta(days=retry_days)

    needs_enrichment = []
    skipped = 0
    retry = 0
    new = 0

    for _, row in active_df.iterrows():
        market_id = str(row.get("market_id", ""))
        existing = existing_by_id.get(market_id)

        if existing is None:
            # New market — not in enriched data yet
            needs_enrichment.append(row)
            new += 1
            continue

        # Check if enrichment was successful
        api_market = (existing.get("api_data") or {}).get("market")
        if api_market is not None:
            has_rules = bool(api_market.get("rules_primary") or api_market.get("description"))
            if has_rules:
                # Successfully enriched — skip
                skipped += 1
                continue

        # Failed enrichment — retry if market is recent enough
        date_added = str(row.get("date_added", ""))
        try:
            added_dt = datetime.strptime(date_added, "%Y-%m-%d")
            if added_dt >= cutoff_date:
                needs_enrichment.append(row)
                retry += 1
                continue
        except (ValueError, TypeError):
            pass

        # Old failed market — give up, keep existing entry as-is
        skipped += 1

    log(f"  New markets: {new}")
    log(f"  Retrying failed: {retry}")
    log(f"  Already enriched (skipped): {skipped}")
    log(f"  Total to enrich: {new + retry}")

    if not needs_enrichment:
        return pd.DataFrame(columns=active_df.columns), existing_by_id

    filtered_df = pd.DataFrame(needs_enrichment)
    return filtered_df, existing_by_id


def archive_pruned(pruned_entries: List[Dict[str, Any]]):
    """Append pruned entries to the archive file (deduped by market_id)."""
    if not pruned_entries:
        return

    # Load existing archive
    archive_by_id: Dict[str, Any] = {}
    if ARCHIVE_FILE.exists():
        with open(ARCHIVE_FILE) as f:
            archive_data = json.load(f)
        for entry in archive_data.get("markets", []):
            mid = str(entry.get("original_csv", {}).get("market_id", ""))
            if mid:
                archive_by_id[mid] = entry

    # Add/overwrite with newly pruned entries
    for entry in pruned_entries:
        mid = str(entry.get("original_csv", {}).get("market_id", ""))
        if mid:
            entry["archived_at"] = datetime.now().isoformat()
            archive_by_id[mid] = entry

    archive_output = {
        "generated_at": datetime.now().isoformat(),
        "description": "Enriched markets pruned from the active file after closing/settling",
        "total_markets": len(archive_by_id),
        "markets": list(archive_by_id.values())
    }

    with open(ARCHIVE_FILE, 'w') as f:
        json.dump(archive_output, f, indent=2, default=str)

    log(f"    Archived {len(pruned_entries)} pruned entries → {ARCHIVE_FILE.name} ({len(archive_by_id):,} total)")


def merge_results(
    new_output: Dict[str, Any],
    existing_by_id: Dict[str, Any],
    active_ids: Set[str]
) -> Dict[str, Any]:
    """
    Merge newly enriched markets with existing enriched data.
    - New/re-fetched entries overwrite existing ones
    - Entries for inactive markets are pruned and archived
    """
    # Overwrite existing with new results
    for entry in new_output.get("markets", []):
        mid = str(entry.get("original_csv", {}).get("market_id", ""))
        if mid:
            existing_by_id[mid] = entry

    # Prune markets no longer active → archive them
    pruned = []
    for mid in list(existing_by_id.keys()):
        if mid not in active_ids:
            pruned.append(existing_by_id.pop(mid))

    if pruned:
        archive_pruned(pruned)

    markets = list(existing_by_id.values())

    # Recalculate stats
    kalshi_count = len([m for m in markets if str((m.get("original_csv") or {}).get("platform", "")).lower() == "kalshi"])
    pm_count = len([m for m in markets if str((m.get("original_csv") or {}).get("platform", "")).lower() == "polymarket"])
    failed_count = len([m for m in markets if m.get("fetch_errors")])

    log(f"\n  Merge complete:")
    log(f"    Total markets: {len(markets):,}")
    log(f"    Kalshi: {kalshi_count:,}")
    log(f"    Polymarket: {pm_count:,}")
    log(f"    Pruned (archived): {len(pruned)}")
    log(f"    Markets with errors: {failed_count}")

    return {
        "generated_at": datetime.now().isoformat(),
        "stats": {
            "total_markets": len(markets),
            "kalshi_markets": kalshi_count,
            "polymarket_markets": pm_count,
            "failed_fetches": failed_count,
        },
        "markets": markets
    }


# =============================================================================
# MAIN
# =============================================================================

async def main(full_refresh: bool = False):
    """Main enrichment pipeline."""
    mode = "FULL REFRESH" if full_refresh else "INCREMENTAL"
    print("\n" + "=" * 70)
    print(f"MARKET ENRICHMENT PIPELINE ({mode})")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    # Load master CSV
    log("Loading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    total_markets = len(df)
    log(f"  Loaded {total_markets:,} total markets")

    # Filter to active markets
    log("\nFiltering to active markets...")

    # Kalshi: k_status IN ('active', 'open')
    kalshi_active = df[
        (df['platform'].str.lower() == 'kalshi') &
        (df['k_status'].isin(['active', 'open']))
    ]

    # Polymarket: pm_closed != 'True'
    polymarket_active = df[
        (df['platform'].str.lower() == 'polymarket') &
        (df['pm_closed'].astype(str) != 'True')
    ]

    # Combine active markets
    active_df = pd.concat([kalshi_active, polymarket_active])
    active_ids = set(active_df['market_id'].astype(str))

    log(f"  Active Kalshi markets: {len(kalshi_active):,}")
    log(f"  Active Polymarket markets: {len(polymarket_active):,}")
    log(f"  Total active markets: {len(active_df):,}")

    # Incremental filtering
    existing_by_id: Dict[str, Any] = {}
    if not full_refresh:
        log("\nFiltering to unenriched markets...")
        active_df, existing_by_id = filter_to_unenriched(active_df)

        if len(active_df) == 0:
            log("\nAll active markets are already enriched!")
            # Still prune inactive markets from existing data
            log("\nPruning inactive markets...")
            output = merge_results({"markets": []}, existing_by_id, active_ids)

            log("\n" + "=" * 60)
            log("Saving Output")
            log("=" * 60)
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(output, f, indent=2, default=str)
            log(f"  Saved to: {OUTPUT_FILE}")
            file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
            log(f"  File size: {file_size_mb:.1f} MB")

            print("\n" + "=" * 70)
            print("ENRICHMENT COMPLETE — nothing to fetch")
            print(f"  Total markets in output: {output['stats']['total_markets']:,}")
            print("=" * 70 + "\n")
            return 0

    log(f"\n  Markets to enrich: {len(active_df):,}")

    # Check for checkpoint
    checkpoint = load_checkpoint()
    if checkpoint:
        log(f"\nFound checkpoint from {checkpoint.get('timestamp', 'unknown')}")

    # Phase 1: Fetch market-level data
    (
        kalshi_results,
        polymarket_results,
        phase1_errors,
        kalshi_event_tickers,
        pm_event_ids
    ) = await phase1_fetch_markets(active_df, checkpoint)

    # Phase 2: Fetch event-level data
    kalshi_events, polymarket_events, phase2_errors = await phase2_fetch_events(
        kalshi_event_tickers,
        pm_event_ids,
        checkpoint
    )

    # Phase 3: Fetch additional metadata
    kalshi_series, kalshi_metadata, phase3_errors = await phase3_fetch_metadata(
        kalshi_results,
        kalshi_events,
        checkpoint
    )

    # Combine all errors
    all_errors = phase1_errors + phase2_errors + phase3_errors

    # Phase 4: Stitch results together
    output = phase4_stitch_results(
        active_df,
        kalshi_results,
        polymarket_results,
        kalshi_events,
        polymarket_events,
        kalshi_series,
        kalshi_metadata,
        all_errors
    )

    # Merge with existing data (incremental mode) or use output directly (full refresh)
    if not full_refresh and existing_by_id:
        log("\n" + "=" * 60)
        log("Merging with Existing Data")
        log("=" * 60)
        output = merge_results(output, existing_by_id, active_ids)

    # Save output
    log("\n" + "=" * 60)
    log("Saving Output")
    log("=" * 60)

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    log(f"  Saved to: {OUTPUT_FILE}")
    file_size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    log(f"  File size: {file_size_mb:.1f} MB")

    # Clear checkpoint on success
    clear_checkpoint()
    log("  Checkpoint cleared")

    # Summary
    print("\n" + "=" * 70)
    print("ENRICHMENT COMPLETE")
    print("=" * 70)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nStats:")
    print(f"  Total markets in output: {output['stats']['total_markets']:,}")
    print(f"  Kalshi: {output['stats']['kalshi_markets']:,}")
    print(f"  Polymarket: {output['stats']['polymarket_markets']:,}")
    print(f"  Failed fetches: {output['stats']['failed_fetches']:,}")
    print(f"\nOutput: {OUTPUT_FILE}")
    print("=" * 70 + "\n")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich political markets with API data")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Re-fetch all active markets instead of only unenriched ones"
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main(full_refresh=args.full_refresh)))
