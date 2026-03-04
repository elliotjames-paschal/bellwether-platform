#!/usr/bin/env python3
"""
Generate worker-ready market index and upload to KV.

Produces a full index of ALL active markets (not just cross-platform matches)
for use by both the V2 website worker and the commercial API worker.

Usage:
    python packages/pipelines/generate_worker_index.py
    python packages/pipelines/generate_worker_index.py --skip-kv-upload

Input:
    data/tickers.json - Tickers from create_tickers.py
    data/combined_political_markets_with_electoral_details_UPDATED.csv - Master CSV

Output:
    KV key `market_map:latest` - JSON market index uploaded to Cloudflare KV
    packages/api/market_map.json - Local copy for backward compat
    docs/data/market_map.json - Local copy for website
"""

import json
import math
import re
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from config import DATA_DIR, PACKAGES_DIR

TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
API_OUTPUT = PACKAGES_DIR / "api" / "market_map.json"
WEBSITE_OUTPUT = PACKAGES_DIR / "website" / "data" / "market_map.json"

KV_NAMESPACE_ID = "2ce167f19ce748e0bf09b513eaafe9ad"


def slugify(text: str) -> str:
    """Convert text to URL-friendly slug."""
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text.strip('-')[:60]


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


def _sanitize_nans(obj):
    """Recursively replace NaN/Infinity floats with None for valid JSON."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_nans(v) for v in obj]
    return obj


def _json_default(obj):
    """Fallback serializer for types json.dumps doesn't handle."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return str(obj)


def load_master_csv():
    """Load active markets from master CSV."""
    import pandas as pd

    if not MASTER_CSV.exists():
        print(f"ERROR: Master CSV not found: {MASTER_CSV}")
        return None

    df = pd.read_csv(MASTER_CSV, low_memory=False)
    # Filter to active markets only
    active = df[df['is_closed'] != True].copy()
    # Exclude non-political and non-standard categories
    VALID_CATEGORIES = {
        '1. ELECTORAL', '2. MONETARY_POLICY', '3. LEGISLATIVE', '4. APPOINTMENTS',
        '5. REGULATORY', '6. INTERNATIONAL', '7. JUDICIAL', '8. MILITARY_SECURITY',
        '9. CRISIS_EMERGENCY', '10. GOVERNMENT_OPERATIONS', '11. PARTY_POLITICS',
        '12. STATE_LOCAL', '13. TIMING_EVENTS', '14. POLLING_APPROVAL', '15. POLITICAL_SPEECH',
    }
    active = active[active['political_category'].isin(VALID_CATEGORIES)]
    print(f"  Master CSV: {len(df)} total, {len(active)} active (valid categories)")
    return active


def load_tickers():
    """Load tickers from tickers.json."""
    if not TICKERS_FILE.exists():
        print(f"ERROR: Tickers file not found: {TICKERS_FILE}")
        print("Run packages/pipelines/create_tickers.py first")
        return None

    with open(TICKERS_FILE, 'r') as f:
        data = json.load(f)

    tickers = data.get('tickers', [])
    print(f"  Tickers: {len(tickers)} entries")
    return tickers


def build_market_index(active_df, tickers):
    """Build the worker-ready market index."""
    import pandas as pd

    # Build market_id → ticker lookup
    mid_to_ticker = {}
    for t in tickers:
        mid = str(t['market_id'])
        mid_to_ticker[mid] = t

    # Build market_id → row lookup from active markets
    mid_to_row = {}
    for _, row in active_df.iterrows():
        mid = str(row['market_id']).split('.')[0]
        mid_to_row[mid] = row

    # Build pm_token_id_yes lookup
    pm_token_lookup = {}
    for _, row in active_df.iterrows():
        mid = str(row['market_id']).split('.')[0]
        token = row.get('pm_token_id_yes')
        if pd.notna(token):
            pm_token_lookup[mid] = str(token).split('.')[0]

    # Group tickers by ticker string
    by_ticker = defaultdict(list)
    for t in tickers:
        mid = str(t['market_id'])
        # Only include if this market is active
        if mid in mid_to_row:
            by_ticker[t['ticker']].append(t)

    # Build index entries
    entries = []
    seen_slugs = set()

    for ticker, markets in by_ticker.items():
        platforms = {m['platform'] for m in markets}
        has_kalshi = 'Kalshi' in platforms
        has_poly = 'Polymarket' in platforms

        if has_kalshi and has_poly:
            platform = "both"
        elif has_kalshi:
            platform = "kalshi"
        else:
            platform = "polymarket"

        # Get platform-specific IDs
        k_market = next((m for m in markets if m['platform'] == 'Kalshi'), None)
        pm_market = next((m for m in markets if m['platform'] == 'Polymarket'), None)

        k_ticker = k_market['market_id'] if k_market else None
        pm_token = str(pm_market['market_id']) if pm_market else None
        pm_token_id = pm_token_lookup.get(pm_token) if pm_token else None

        # Use Kalshi question if available, otherwise Polymarket
        title_market = k_market or pm_market
        title = title_market.get('original_question', ticker)

        # Get volume and metadata from master CSV
        ref_mid = (k_market or pm_market)['market_id']
        ref_mid_str = str(ref_mid).split('.')[0]
        ref_row = mid_to_row.get(ref_mid_str)

        total_volume = 0
        category = ''
        country = ''

        if ref_row is not None:
            vol = ref_row.get('volume_usd', 0)
            total_volume = float(vol) if pd.notna(vol) else 0

            cat = ref_row.get('political_category', '')
            category = str(cat) if pd.notna(cat) else ''

            cty = ref_row.get('country', '')
            country = str(cty) if pd.notna(cty) else ''

        # If cross-platform, sum volumes from both
        if has_kalshi and has_poly:
            k_row = mid_to_row.get(str(k_market['market_id']).split('.')[0])
            pm_row = mid_to_row.get(str(pm_market['market_id']).split('.')[0])
            k_vol = float(k_row['volume_usd']) if k_row is not None and pd.notna(k_row.get('volume_usd')) else 0
            pm_vol = float(pm_row['volume_usd']) if pm_row is not None and pd.notna(pm_row.get('volume_usd')) else 0
            total_volume = k_vol + pm_vol
            # Prefer Kalshi metadata for country/category
            if k_row is not None:
                cat = k_row.get('political_category', '')
                if pd.notna(cat) and cat:
                    category = str(cat)
                cty = k_row.get('country', '')
                if pd.notna(cty) and cty:
                    country = str(cty)

        # Generate unique slug
        slug_parts = ticker.replace('BWR-', '').split('-')[:4]
        slug = slugify('-'.join(slug_parts))

        # Ensure uniqueness
        base_slug = slug
        counter = 2
        while slug in seen_slugs:
            slug = f"{base_slug}-{counter}"
            counter += 1
        seen_slugs.add(slug)

        # Infer category from ticker if missing
        if not category:
            category = _infer_category(ticker)

        entries.append({
            'slug': slug,
            'ticker': ticker,
            'title': title,
            'k_ticker': k_ticker,
            'pm_token': pm_token,
            'pm_token_id': pm_token_id,
            'category': category,
            'country': country,
            'platform': platform,
            'total_volume': round(total_volume, 2),
        })

    # Sort by volume descending
    entries.sort(key=lambda x: x.get('total_volume', 0), reverse=True)

    return entries


def upload_to_kv(data_json: str):
    """Upload market index to Cloudflare KV via wrangler."""
    try:
        result = subprocess.run(
            [
                "npx", "wrangler", "kv", "key", "put",
                "--namespace-id", KV_NAMESPACE_ID,
                "--remote",
                "market_map:latest",
                data_json,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode == 0:
            print(f"  Uploaded to KV key market_map:latest")
            return True
        else:
            print(f"  KV upload failed: {result.stderr[:200]}")
            return False
    except FileNotFoundError:
        print("  wrangler not found - skipping KV upload")
        return False
    except subprocess.TimeoutExpired:
        print("  KV upload timed out")
        return False


def main():
    skip_kv = "--skip-kv-upload" in sys.argv

    print("=== Generate Worker Index ===")
    print()

    # Load data
    print("Loading data...")
    active_df = load_master_csv()
    if active_df is None:
        return 1

    tickers = load_tickers()
    if tickers is None:
        return 1

    # Build index
    print("\nBuilding market index...")
    entries = build_market_index(active_df, tickers)

    # Build output structure
    output = {
        'generated_at': datetime.now().isoformat(),
        'count': len(entries),
        'matching_system': 'ticker_v2_full',
        'markets': entries,
    }

    output = _sanitize_nans(output)
    output_json = json.dumps(output, default=_json_default, allow_nan=False)

    # Write local files
    API_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(API_OUTPUT, 'w') as f:
        f.write(output_json)

    WEBSITE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(WEBSITE_OUTPUT, 'w') as f:
        f.write(output_json)

    # Upload to KV
    if not skip_kv:
        print("\nUploading to KV...")
        upload_to_kv(output_json)
    else:
        print("\nSkipping KV upload (--skip-kv-upload)")

    # Stats
    both_count = sum(1 for e in entries if e['platform'] == 'both')
    kalshi_only = sum(1 for e in entries if e['platform'] == 'kalshi')
    poly_only = sum(1 for e in entries if e['platform'] == 'polymarket')

    print(f"\n=== RESULTS ===")
    print(f"Total entries: {len(entries)}")
    print(f"  Cross-platform (both): {both_count}")
    print(f"  Kalshi only: {kalshi_only}")
    print(f"  Polymarket only: {poly_only}")
    print(f"Size: {len(output_json) / 1024 / 1024:.1f} MB")
    print(f"Output: {API_OUTPUT}")
    print(f"Website: {WEBSITE_OUTPUT}")

    # Show top 10
    print("\nTop 10 by volume:")
    for i, e in enumerate(entries[:10], 1):
        vol = e['total_volume'] / 1_000_000 if e['total_volume'] else 0
        print(f"  {i:2}. [{e['platform']:10}] {e['ticker'][:50]:52} ${vol:.1f}M")

    return 0


if __name__ == '__main__':
    sys.exit(main())
