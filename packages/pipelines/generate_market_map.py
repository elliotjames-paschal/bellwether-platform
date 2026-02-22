#!/usr/bin/env python3
"""
Generate market_map.json for the commercial API worker.

Extracts matched markets (available on both Polymarket and Kalshi) from
active_markets.json and outputs a JSON file that can be uploaded to Workers KV.

Usage:
    python scripts/generate_market_map.py

Output:
    api/market_map.json - JSON file ready for KV upload
"""

import json
import re
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
ACTIVE_MARKETS = PROJECT_ROOT / "website" / "data" / "active_markets.json"
OUTPUT_FILE = PROJECT_ROOT / "api" / "market_map.json"
WEBSITE_OUTPUT = PROJECT_ROOT / "website" / "data" / "market_map.json"  # Deployed with website


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug."""
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')[:60]


def generate_market_map():
    """Generate market map from active markets data."""

    with open(ACTIVE_MARKETS, 'r') as f:
        data = json.load(f)

    matched_markets = []

    for m in data.get('markets', []):
        # Only include markets that:
        # 1. Have both platforms (has_both=true)
        # 2. Are not completed
        # 3. Have valid token IDs for both platforms
        if not m.get('has_both'):
            continue
        if m.get('is_completed'):
            continue

        pm_token = m.get('pm_token_id', '').strip()
        k_ticker = m.get('k_ticker', '').strip()

        if not pm_token or not k_ticker:
            continue

        # Generate a slug from the label or key
        label = m.get('label', '') or m.get('key', '')
        slug = slugify(label)

        # Use the election details to build a cleaner title
        title = label
        if m.get('pm_question'):
            # Try to extract a cleaner title from the question
            title = m.get('pm_question')

        matched_markets.append({
            'slug': slug,
            'title': title,
            'k_ticker': k_ticker,
            'pm_token': pm_token,
            'category': m.get('category_display', 'Electoral'),
            'country': m.get('country', ''),
            'total_volume': m.get('total_volume', 0),
        })

    # Sort by volume descending
    matched_markets.sort(key=lambda x: x.get('total_volume', 0), reverse=True)

    # Build output structure
    output = {
        'generated_at': data.get('generated_at'),
        'count': len(matched_markets),
        'markets': matched_markets,
    }

    # Write output to both locations
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    WEBSITE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(WEBSITE_OUTPUT, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"Generated {len(matched_markets)} matched markets")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Website: {WEBSITE_OUTPUT}")

    # Show top 10
    print("\nTop 10 by volume:")
    for i, m in enumerate(matched_markets[:10], 1):
        vol = m['total_volume'] / 1_000_000
        print(f"  {i:2}. {m['slug'][:40]:42} ${vol:.1f}M")

    return matched_markets


if __name__ == '__main__':
    generate_market_map()
