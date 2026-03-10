#!/usr/bin/env python3
"""
Generate market_map.json from the ticker-based matching system.

This uses canonical BWR tickers to match markets across Kalshi and Polymarket.
Two markets match if they have the same ticker.

Usage:
    python packages/pipelines/generate_market_map.py

Input:
    data/tickers.json - Tickers from create_tickers.py
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
from config import DATA_DIR, PACKAGES_DIR

TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
OUTPUT_FILE = PACKAGES_DIR / "api" / "market_map.json"
WEBSITE_OUTPUT = PACKAGES_DIR / "website" / "data" / "market_map.json"


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


def load_pm_token_lookup() -> dict:
    """Load pm_token_id_yes indexed by market_id from master CSV."""
    csv_file = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
    if not csv_file.exists():
        print(f"Warning: {csv_file} not found, pm_token_id will be missing")
        return {}

    import pandas as pd
    df = pd.read_csv(csv_file, usecols=['market_id', 'pm_token_id_yes'], dtype=str, low_memory=False)
    df = df[df['pm_token_id_yes'].notna()]
    # market_id can be float-like ("559671.0"), clean it
    lookup = {}
    for _, row in df.iterrows():
        mid = str(row['market_id']).split('.')[0]
        token = str(row['pm_token_id_yes']).split('.')[0]
        lookup[mid] = token
    return lookup


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
        print("Run packages/pipelines/create_tickers.py first")
        return []

    with open(TICKERS_FILE, 'r') as f:
        tickers_data = json.load(f)

    print(f"Loading enriched markets for volume data...")
    enriched = load_enriched_markets()

    print(f"Loading Polymarket token ID lookup from master CSV...")
    pm_token_lookup = load_pm_token_lookup()
    print(f"  Found {len(pm_token_lookup)} market_id → pm_token_id_yes mappings")

    # Group tickers by ticker string
    by_ticker = defaultdict(list)
    for t in tickers_data['tickers']:
        by_ticker[t['ticker']].append(t)

    # Build all market entries (not just cross-platform)
    matched_markets = []
    seen_slugs = set()

    for ticker, markets in by_ticker.items():
        platforms = {m['platform'] for m in markets}
        has_kalshi = 'Kalshi' in platforms
        has_poly = 'Polymarket' in platforms

        if has_kalshi and has_poly:
            platform = 'both'
        elif has_kalshi:
            platform = 'kalshi'
        else:
            platform = 'polymarket'

        kalshi_markets = [m for m in markets if m['platform'] == 'Kalshi']
        poly_markets = [m for m in markets if m['platform'] == 'Polymarket']

        # Pick best market per platform by volume
        def best_by_volume(mlist):
            if not mlist:
                return None
            return max(mlist, key=lambda m: enriched.get(str(m['market_id']), {}).get('volume', 0))

        k_market = best_by_volume(kalshi_markets)
        pm_market = best_by_volume(poly_markets)

        # Get market IDs
        k_ticker_id = k_market['market_id'] if k_market else None
        pm_token = str(pm_market['market_id']) if pm_market else None

        # Get volume from enriched data
        k_vol = enriched.get(str(k_ticker_id), {}).get('volume', 0) if k_ticker_id else 0
        pm_vol = enriched.get(str(pm_token), {}).get('volume', 0) if pm_token else 0
        total_volume = k_vol + pm_vol

        # Get category and country from whichever platform is available
        ref_id = str(k_ticker_id) if k_ticker_id else str(pm_token)
        category = enriched.get(ref_id, {}).get('category', '')
        country = enriched.get(ref_id, {}).get('country', '')

        # Generate unique slug from ticker
        slug_parts = ticker.split('-')[:4]
        slug = slugify('-'.join(slug_parts))
        base_slug = slug
        counter = 2
        while slug in seen_slugs:
            slug = f"{base_slug}-{counter}"
            counter += 1
        seen_slugs.add(slug)

        # Use Kalshi question as title (usually cleaner), fall back to PM
        title_market = k_market or pm_market
        title = title_market.get('original_question', ticker)

        # Look up the actual CLOB API token for Polymarket
        pm_token_id = pm_token_lookup.get(str(pm_token)) if pm_token else None

        # Determine match provenance from constituent markets
        match_sources = {m.get('match_source', 'auto_ticker') for m in markets}
        if 'human' in match_sources:
            match_source = 'human'
        elif 'auto_embedding_gpt' in match_sources:
            match_source = 'auto_embedding_gpt'
        else:
            match_source = 'auto_ticker'

        matched_markets.append({
            'slug': slug,
            'title': title,
            'ticker': ticker,  # Our canonical BWR ticker
            'k_ticker': k_ticker_id,
            'pm_token': pm_token,          # Short market_id (for URLs/display)
            'pm_token_id': pm_token_id,    # CLOB API token (77-digit)
            'category': category or _infer_category(ticker),
            'country': country,
            'platform': platform,
            'total_volume': total_volume,
            'match_source': match_source,
            # Include both questions for debugging
            'k_question': k_market.get('original_question', '') if k_market else '',
            'pm_question': pm_market.get('original_question', '') if pm_market else '',
        })

    # Filter to valid political categories only
    VALID_CATEGORIES = {
        '1. ELECTORAL', '2. MONETARY_POLICY', '3. LEGISLATIVE', '4. APPOINTMENTS',
        '5. REGULATORY', '6. INTERNATIONAL', '7. JUDICIAL', '8. MILITARY_SECURITY',
        '9. CRISIS_EMERGENCY', '10. GOVERNMENT_OPERATIONS', '11. PARTY_POLITICS',
        '12. STATE_LOCAL', '13. TIMING_EVENTS', '14. POLLING_APPROVAL', '15. POLITICAL_SPEECH',
    }
    before = len(matched_markets)
    matched_markets = [m for m in matched_markets if m.get('category', '') in VALID_CATEGORIES]
    excluded = before - len(matched_markets)
    if excluded:
        print(f"  Excluded {excluded} markets with invalid categories")

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

    # Count pm_token_id coverage
    with_token_id = sum(1 for m in matched_markets if m.get('pm_token_id'))
    without_token_id = len(matched_markets) - with_token_id

    both_count = sum(1 for m in matched_markets if m.get('platform') == 'both')
    kalshi_only = sum(1 for m in matched_markets if m.get('platform') == 'kalshi')
    poly_only = sum(1 for m in matched_markets if m.get('platform') == 'polymarket')

    print(f"\n=== RESULTS ===")
    print(f"Total tickers: {len(tickers_data['tickers'])}")
    print(f"Unique tickers: {len(by_ticker)}")
    print(f"Total entries: {len(matched_markets)}")
    print(f"  Cross-platform (both): {both_count}")
    print(f"  Kalshi only: {kalshi_only}")
    print(f"  Polymarket only: {poly_only}")
    print(f"With pm_token_id: {with_token_id}/{len(matched_markets)} ({100*with_token_id/max(len(matched_markets),1):.1f}%)")
    if without_token_id:
        print(f"  Missing pm_token_id: {without_token_id} markets")
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
