#!/usr/bin/env python3
"""
================================================================================
Backfill PM Event Slugs from Dome API
================================================================================

Fetches event_slug for Polymarket markets by downloading all events.

Strategy:
- Fetch ALL events from Dome API's /events endpoint with include_markets=true
- Each event contains markets with condition_id
- Build condition_id → event_slug mapping
- Save for use in generate_monitor_data.py

Output:
    data/pm_event_slug_mapping.json

Usage:
    python fetch_pm_event_slugs.py [--limit N]

================================================================================
"""

import requests
import json
import time
import sys
import os
from datetime import datetime
from pathlib import Path

# Configuration
from config import DATA_DIR, get_dome_api_key

OUTPUT_FILE = DATA_DIR / "pm_event_slug_mapping.json"

DOME_API_KEY = get_dome_api_key()
DOME_PM_BASE = "https://api.domeapi.io/v1/polymarket"

# Rate limiting (dev tier: 100 req/sec)
RATE_LIMIT_DELAY = float(os.environ.get('DOME_RATE_LIMIT', '0.05'))
MAX_RETRIES = 3


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def fetch_all_events(limit_pages=None):
    """Fetch all events from Dome API with their markets.

    Returns:
        List of events with markets included
    """
    all_events = []
    pagination_key = None
    page = 0

    while True:
        if limit_pages and page >= limit_pages:
            break

        params = {
            "limit": 100,
            "include_markets": "true",
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    f"{DOME_PM_BASE}/events",
                    headers={"Authorization": DOME_API_KEY},
                    params=params,
                    timeout=60
                )

                if response.status_code == 200:
                    data = response.json()
                    events = data.get("events", [])
                    all_events.extend(events)
                    page += 1

                    if page % 10 == 0:
                        log(f"  Fetched {page} pages, {len(all_events)} events...")

                    pagination = data.get("pagination", {})
                    if pagination.get("has_more"):
                        pagination_key = pagination.get("pagination_key")
                    else:
                        return all_events
                    break

                elif response.status_code == 429:
                    wait_time = 10 * (2 ** attempt)
                    log(f"  Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                else:
                    log(f"  API error {response.status_code}: {response.text[:200]}")
                    return all_events

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                log(f"  Error: {e}")
                return all_events

        time.sleep(RATE_LIMIT_DELAY)

    return all_events


def build_mapping(events):
    """Build condition_id → event_slug mapping from events.

    Returns:
        dict: {condition_id: event_slug}
    """
    mapping = {}

    for event in events:
        event_slug = event.get("event_slug")
        if not event_slug:
            continue

        markets = event.get("markets", [])
        for market in markets:
            condition_id = market.get("condition_id")
            if condition_id:
                mapping[condition_id] = event_slug

    return mapping


def main():
    # Parse args
    limit_pages = None
    if '--limit' in sys.argv:
        idx = sys.argv.index('--limit')
        if idx + 1 < len(sys.argv):
            limit_pages = int(sys.argv[idx + 1])

    log("Fetching all events from Dome API...")

    events = fetch_all_events(limit_pages)
    log(f"  Total events: {len(events):,}")

    # Count markets
    total_markets = sum(len(e.get("markets", [])) for e in events)
    log(f"  Total markets in events: {total_markets:,}")

    # Build mapping
    mapping = build_mapping(events)
    log(f"  Unique condition_id mappings: {len(mapping):,}")

    # Save output
    save_output(mapping, len(events))

    log("Done!")


def save_output(mapping, event_count):
    """Save final mapping file."""
    output = {
        "generated_at": datetime.now().isoformat(),
        "event_count": event_count,
        "mapping_count": len(mapping),
        "mapping": mapping,
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    log(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
