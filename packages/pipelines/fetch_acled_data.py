#!/usr/bin/env python3
"""
Fetch ACLED Conflict Events Data

Fetches recent conflict events from the ACLED API for display on the globe.

ACLED API requires registration at https://acleddata.com/register/
After registration, set environment variables:
    export ACLED_EMAIL=your_email
    export ACLED_KEY=your_api_key

Usage:
    python fetch_acled_data.py

Output:
    website/data/acled_events.json
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, WEBSITE_DIR

# ACLED API config
ACLED_API_URL = "https://api.acleddata.com/acled/read"
ACLED_EMAIL = os.environ.get('ACLED_EMAIL')
ACLED_KEY = os.environ.get('ACLED_KEY')

OUTPUT_FILE = WEBSITE_DIR / "data" / "acled_events.json"

# Event type colors
EVENT_COLORS = {
    'Battles': '#dc2626',           # Red
    'Violence against civilians': '#991b1b',  # Dark red
    'Explosions/Remote violence': '#ea580c',  # Orange
    'Protests': '#f59e0b',          # Amber
    'Riots': '#d97706',             # Dark amber
    'Strategic developments': '#6b7280',  # Gray
}


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def fetch_acled_events(days=90):
    """Fetch ACLED events from the last N days."""
    if not ACLED_EMAIL or not ACLED_KEY:
        log("  ERROR: ACLED API credentials not set")
        log("  Register at https://acleddata.com/register/")
        log("  Then set: export ACLED_EMAIL=your_email ACLED_KEY=your_key")
        return None

    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    params = {
        'email': ACLED_EMAIL,
        'key': ACLED_KEY,
        'event_date': f"{start_date.strftime('%Y-%m-%d')}|{end_date.strftime('%Y-%m-%d')}",
        'event_date_where': 'BETWEEN',
        'limit': 10000,  # Max events to fetch
        'fields': 'event_id_cnty|event_date|year|event_type|actor1|country|latitude|longitude|fatalities|notes',
    }

    log(f"  Fetching ACLED events from {start_date.date()} to {end_date.date()}...")

    try:
        response = requests.get(ACLED_API_URL, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        if 'data' in data:
            events = data['data']
            log(f"  Fetched {len(events)} events")
            return events
        else:
            log(f"  ERROR: Unexpected response format")
            return None

    except requests.RequestException as e:
        log(f"  ERROR: API request failed: {e}")
        return None


def process_events(events):
    """Process ACLED events for globe display."""
    processed = []

    for event in events:
        try:
            lat = float(event.get('latitude', 0))
            lng = float(event.get('longitude', 0))

            if lat == 0 and lng == 0:
                continue

            event_type = event.get('event_type', 'Unknown')
            fatalities = int(event.get('fatalities', 0))

            processed.append({
                'lat': lat,
                'lng': lng,
                'date': event.get('event_date'),
                'type': event_type,
                'country': event.get('country'),
                'actor': event.get('actor1'),
                'fatalities': fatalities,
                'color': EVENT_COLORS.get(event_type, '#6b7280'),
                'notes': event.get('notes', '')[:200] if event.get('notes') else None,
            })

        except (ValueError, TypeError):
            continue

    # Sort by date (most recent first)
    processed.sort(key=lambda e: e.get('date', ''), reverse=True)

    return processed


def generate_acled_data():
    """Generate ACLED conflict events JSON for globe display."""
    log("Generating ACLED conflict events data...")

    events = fetch_acled_events(days=90)

    if not events:
        log("  No events fetched - creating placeholder file")
        # Create placeholder with sample structure
        output = {
            'generated_at': datetime.now().isoformat(),
            'source': 'ACLED (Armed Conflict Location & Event Data)',
            'source_url': 'https://acleddata.com/',
            'note': 'API credentials required - register at acleddata.com',
            'event_colors': EVENT_COLORS,
            'events': [],
        }
    else:
        processed = process_events(events)
        log(f"  Processed {len(processed)} events with coordinates")

        # Aggregate stats
        by_type = {}
        by_country = {}
        total_fatalities = 0

        for e in processed:
            t = e['type']
            c = e['country']
            by_type[t] = by_type.get(t, 0) + 1
            by_country[c] = by_country.get(c, 0) + 1
            total_fatalities += e.get('fatalities', 0)

        output = {
            'generated_at': datetime.now().isoformat(),
            'source': 'ACLED (Armed Conflict Location & Event Data)',
            'source_url': 'https://acleddata.com/',
            'date_range': {
                'start': min(e['date'] for e in processed) if processed else None,
                'end': max(e['date'] for e in processed) if processed else None,
            },
            'stats': {
                'total_events': len(processed),
                'total_fatalities': total_fatalities,
                'by_type': by_type,
                'top_countries': dict(sorted(by_country.items(), key=lambda x: -x[1])[:20]),
            },
            'event_colors': EVENT_COLORS,
            'events': processed[:5000],  # Limit for performance
        }

        log(f"  Total fatalities: {total_fatalities:,}")
        log(f"  Events by type: {by_type}")

    # Save output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    log(f"  Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_acled_data()
