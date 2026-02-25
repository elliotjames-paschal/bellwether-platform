#!/usr/bin/env python3
"""
================================================================================
Backfill PM Event Slugs from Gamma API
================================================================================

Fetches event_slug for Polymarket markets by downloading all events from
the public Gamma API.

Strategy:
- Fetch ALL events from Gamma API's /events endpoint (public, no auth)
- Each event contains markets with conditionId
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
import threading
from datetime import datetime
from pathlib import Path

# Configuration
from config import DATA_DIR

OUTPUT_FILE = DATA_DIR / "pm_event_slug_mapping.json"

# Gamma API (public, no auth required)
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

MAX_RETRIES = 3


class RateLimiter:
    """Thread-safe rate limiter."""
    def __init__(self, calls_per_second=10):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()


rate_limiter = RateLimiter(10)  # 10 req/sec for Gamma API


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def fetch_all_events(limit_pages=None):
    """Fetch all events from Gamma API with their markets.

    Returns:
        List of events with markets included
    """
    all_events = []
    offset = 0
    page = 0
    limit = 100

    while True:
        if limit_pages and page >= limit_pages:
            break

        params = {
            "limit": limit,
            "offset": offset,
        }

        for attempt in range(MAX_RETRIES):
            try:
                rate_limiter.wait()
                response = requests.get(
                    f"{GAMMA_API_BASE}/events",
                    params=params,
                    headers={"Accept": "application/json"},
                    timeout=60
                )

                if response.status_code == 200:
                    events = response.json()

                    if not events:
                        return all_events

                    all_events.extend(events)
                    page += 1

                    if page % 10 == 0:
                        log(f"  Fetched {page} pages, {len(all_events)} events...")

                    if len(events) < limit:
                        return all_events

                    offset += limit
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

    return all_events


def build_mapping(events):
    """Build condition_id → event_slug mapping from events.

    Returns:
        dict: {condition_id: event_slug}
    """
    mapping = {}

    for event in events:
        event_slug = event.get("slug")
        if not event_slug:
            continue

        markets = event.get("markets", [])
        for market in markets:
            condition_id = market.get("conditionId") or market.get("condition_id")
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

    log("Fetching all events from Gamma API...")

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
