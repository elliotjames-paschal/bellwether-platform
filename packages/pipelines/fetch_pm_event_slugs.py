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
    python fetch_pm_event_slugs.py [--limit N] [--incremental]

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
CHECKPOINT_FILE = DATA_DIR / "pm_event_slug_checkpoint.json"
CHECKPOINT_INTERVAL = 50  # Save checkpoint every N pages

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


def load_checkpoint():
    """Load checkpoint from file, if it exists."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return None


def save_checkpoint(data):
    """Save or clear checkpoint file."""
    if data is None:
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
        return
    data["saved_at"] = datetime.now().isoformat()
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)


def fetch_all_events(limit_pages=None, resume_offset=0, incremental_cutoff=None):
    """Fetch all events from Gamma API with their markets.

    Args:
        limit_pages: Max pages to fetch (for testing)
        resume_offset: Offset to resume from (checkpoint)
        incremental_cutoff: ISO timestamp; stop when events are older

    Returns:
        List of events with markets included
    """
    all_events = []
    offset = resume_offset
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
                        save_checkpoint(None)
                        return all_events

                    # Incremental mode: stop if all events on this page are older
                    if incremental_cutoff:
                        newest_on_page = max(
                            (e.get("updatedAt", e.get("createdAt", "")) for e in events),
                            default=""
                        )
                        if newest_on_page and newest_on_page < incremental_cutoff:
                            log(f"  Reached events older than cutoff, stopping")
                            save_checkpoint(None)
                            return all_events

                    all_events.extend(events)
                    page += 1

                    # Checkpoint every N pages
                    if page % CHECKPOINT_INTERVAL == 0:
                        save_checkpoint({"offset": offset + limit, "events_so_far": len(all_events)})
                        log(f"  Checkpoint saved at offset {offset + limit}")

                    if page % 10 == 0:
                        log(f"  Fetched {page} pages, {len(all_events)} events...")

                    if len(events) < limit:
                        save_checkpoint(None)
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
                    save_checkpoint({"offset": offset, "events_so_far": len(all_events)})
                    return all_events

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
                    continue
                log(f"  Error: {e}")
                save_checkpoint({"offset": offset, "events_so_far": len(all_events)})
                return all_events

    save_checkpoint(None)
    return all_events


def build_mapping(events):
    """Build condition_id -> event_slug mapping from events.

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


def main():
    # Parse args
    limit_pages = None
    incremental = "--incremental" in sys.argv
    if '--limit' in sys.argv:
        idx = sys.argv.index('--limit')
        if idx + 1 < len(sys.argv):
            limit_pages = int(sys.argv[idx + 1])

    # Check for checkpoint (resume after crash)
    checkpoint = load_checkpoint()
    resume_offset = 0
    if checkpoint:
        resume_offset = checkpoint.get("offset", 0)
        log(f"Resuming from checkpoint at offset {resume_offset}")

    # Incremental mode: only fetch events newer than last run
    incremental_cutoff = None
    if incremental and OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, 'r') as f:
                existing = json.load(f)
            incremental_cutoff = existing.get("generated_at")
            if incremental_cutoff:
                log(f"Incremental mode: fetching events updated after {incremental_cutoff}")
        except (json.JSONDecodeError, IOError):
            log("Could not read existing output, doing full fetch")

    log("Fetching events from Gamma API...")
    events = fetch_all_events(limit_pages, resume_offset, incremental_cutoff)
    log(f"  Total events fetched: {len(events):,}")

    total_markets = sum(len(e.get("markets", [])) for e in events)
    log(f"  Total markets in events: {total_markets:,}")

    # Build mapping from fetched events
    new_mapping = build_mapping(events)

    # In incremental mode, merge with existing mapping
    if incremental and OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, 'r') as f:
                existing = json.load(f)
            old_mapping = existing.get("mapping", {})
            old_count = len(old_mapping)
            old_mapping.update(new_mapping)  # New data overwrites old
            mapping = old_mapping
            log(f"  Merged: {len(new_mapping):,} new + {old_count:,} existing = {len(mapping):,} total")
        except (json.JSONDecodeError, IOError):
            mapping = new_mapping
    else:
        mapping = new_mapping

    log(f"  Unique condition_id mappings: {len(mapping):,}")
    save_output(mapping, len(events))
    log("Done!")


if __name__ == "__main__":
    main()
