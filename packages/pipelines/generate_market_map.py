#!/usr/bin/env python3
"""
Generate market_map.json from the ticker-based matching system.

This uses canonical BWR tickers from bellwether-matcher to match markets
across Kalshi and Polymarket. Two markets match if they have the same ticker.

Usage:
    python packages/pipelines/generate_market_map.py

Input:
    bellwether-matcher/data/tickers.json - Tickers from create_tickers.py
    data/enriched_political_markets.json.gz - Enriched market data (for volume)

Output:
    api/market_map.json - JSON file for commercial API
    website/data/market_map.json - JSON file for website
"""

import json
import gzip
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent  # bellwether-platform/packages
BELLWETHER_PLATFORM = PROJECT_ROOT.parent  # bellwether-platform
MATCHER_DIR = BELLWETHER_PLATFORM / "bellwether-matcher"
DATA_DIR = BELLWETHER_PLATFORM / "data"

TICKERS_FILE = MATCHER_DIR / "data" / "tickers.json"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
OUTPUT_FILE = PROJECT_ROOT / "api" / "market_map.json"
WEBSITE_OUTPUT = PROJECT_ROOT / "website" / "data" / "market_map.json"


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug."""
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')[:60]


def load_enriched_markets() -> dict:
    """Load enriched markets indexed by market_id for volume lookup."""
    if not ENRICHED_FILE.exists():
        print(f"Warning: {ENRICHED_FILE} not found, volumes will be 0")
        return {}

    with gzip.open(ENRICHED_FILE, "rt", encoding="utf-8") as f:
        data = json.load(f)

    markets = data.get("markets", data)
    indexed = {}

    for m in markets:
        csv = m.get("original_csv", m)
        market_id = csv.get("market_id")
        if market_id:
            # Extract volume from various fields
            volume = 0
            api_data = m.get("api_data", {})
            mkt = api_data.get("market", {}) if isinstance(api_data.get("market"), dict) else {}

            # Try different volume fields
            volume = (
                mkt.get("volume") or
                mkt.get("volumeNum") or
                csv.get("k_volume") or
                csv.get("pm_volume") or
                0
            )
            try:
                volume = float(volume) if volume else 0
            except (ValueError, TypeError):
                volume = 0

            indexed[str(market_id)] = {
                "volume": volume,
                "category": csv.get("political_category", ""),
                "country": csv.get("country", ""),
            }

    return indexed


def _infer_category(ticker: str) -> str:
    """Infer category from ticker."""
    ticker_upper = ticker.upper()

    if 'WIN' in ticker_upper and any(x in ticker_upper for x in ['PRES', 'GOV', 'SEN', 'HOUSE']):
        return 'ELECTORAL'
    if 'CONTROL' in ticker_upper:
        return 'PARTISAN_CONTROL'
    if 'CUT' in ticker_upper or 'HIKE' in ticker_upper:
        return 'MONETARY_POLICY'
    if 'REPORT' in ticker_upper:
        return 'ECONOMIC_DATA'
    if 'LEAVE' in ticker_upper or 'RESIGN' in ticker_upper:
        return 'PERSONNEL'

    return 'OTHER'


def generate_market_map():
    """Generate market map from ticker-based matches."""

    print(f"Loading tickers from {TICKERS_FILE}...")
    if not TICKERS_FILE.exists():
        print(f"ERROR: Tickers file not found: {TICKERS_FILE}")
        print("Run bellwether-matcher/pipeline/create_tickers.py first")
        return []

    with open(TICKERS_FILE, 'r') as f:
        tickers_data = json.load(f)

    print(f"Loading enriched markets for volume data...")
    enriched = load_enriched_markets()

    # Group tickers by ticker string
    by_ticker = defaultdict(list)
    for t in tickers_data['tickers']:
        by_ticker[t['ticker']].append(t)

    # Find cross-platform matches
    matched_markets = []

    for ticker, markets in by_ticker.items():
        platforms = {m['platform'] for m in markets}

        # Only include if both platforms have this ticker
        if 'Kalshi' not in platforms or 'Polymarket' not in platforms:
            continue

        kalshi_markets = [m for m in markets if m['platform'] == 'Kalshi']
        poly_markets = [m for m in markets if m['platform'] == 'Polymarket']

        # Use the first market from each platform
        k_market = kalshi_markets[0]
        pm_market = poly_markets[0]

        # Get market IDs
        k_ticker = k_market['market_id']
        pm_token = str(pm_market['market_id'])

        # Get volume from enriched data
        k_vol = enriched.get(str(k_ticker), {}).get('volume', 0)
        pm_vol = enriched.get(str(pm_token), {}).get('volume', 0)
        total_volume = k_vol + pm_vol

        # Get category and country
        category = enriched.get(str(k_ticker), {}).get('category', '')
        country = enriched.get(str(k_ticker), {}).get('country', '')

        # Generate slug from ticker
        # BWR-DEM-WIN-GOV_CA-CERTIFIED-ANY-2026 -> dem-win-gov-ca
        slug_parts = ticker.replace('BWR-', '').split('-')[:4]
        slug = slugify('-'.join(slug_parts))

        # Use Kalshi question as title (usually cleaner)
        title = k_market.get('original_question', ticker)

        matched_markets.append({
            'slug': slug,
            'title': title,
            'ticker': ticker,  # Our canonical BWR ticker
            'k_ticker': k_ticker,
            'pm_token': pm_token,
            'category': category or _infer_category(ticker),
            'country': country,
            'total_volume': total_volume,
            # Include both questions for debugging
            'k_question': k_market.get('original_question', ''),
            'pm_question': pm_market.get('original_question', ''),
        })

    # Sort by volume descending
    matched_markets.sort(key=lambda x: x.get('total_volume', 0), reverse=True)

    # Build output structure
    output = {
        'generated_at': datetime.now().isoformat(),
        'count': len(matched_markets),
        'matching_system': 'ticker_v2',
        'ticker_format': 'BWR-{AGENT}-{ACTION}-{TARGET}-{MECHANISM}-{THRESHOLD}-{TIMEFRAME}',
        'markets': matched_markets,
    }

    # Write output to both locations
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    WEBSITE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(WEBSITE_OUTPUT, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n=== RESULTS ===")
    print(f"Total tickers: {len(tickers_data['tickers'])}")
    print(f"Unique tickers: {len(by_ticker)}")
    print(f"Cross-platform matches: {len(matched_markets)}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Website: {WEBSITE_OUTPUT}")

    # Show top 10
    print("\nTop 10 by volume:")
    for i, m in enumerate(matched_markets[:10], 1):
        vol = m['total_volume'] / 1_000_000 if m['total_volume'] else 0
        print(f"  {i:2}. {m['ticker'][:50]:52} ${vol:.1f}M")

    # Show sample matches
    print("\nSample matches:")
    for m in matched_markets[:5]:
        print(f"  {m['ticker']}")
        print(f"    K: {m['k_question'][:60]}")
        print(f"    P: {m['pm_question'][:60]}")
        print()

    return matched_markets


if __name__ == '__main__':
    generate_market_map()
