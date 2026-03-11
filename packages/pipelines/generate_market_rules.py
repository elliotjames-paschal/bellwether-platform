#!/usr/bin/env python3
"""
Generate Market Rules JSON

Extracts contract rules text from enriched_political_markets.json.gz
and writes a lightweight lookup file for the Market Monitor UI.

Input:  data/enriched_political_markets.json.gz
Output: docs/data/market_rules.json

Rules sources by platform:
  - Kalshi: api_data.market.rules_primary, api_data.market.rules_secondary
  - Polymarket: api_data.market.description

Usage:
    python generate_market_rules.py
"""

import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, WEBSITE_DIR

ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
OUTPUT_FILE = WEBSITE_DIR / "data" / "market_rules.json"


def extract_rules(enriched_entry: dict) -> dict | None:
    """Extract rules from a single enriched market entry. Returns None if no rules found."""
    csv_data = enriched_entry.get("original_csv", enriched_entry)
    platform = str(csv_data.get("platform", "")).lower()
    api_data = enriched_entry.get("api_data", {})
    market = api_data.get("market", {}) if isinstance(api_data.get("market"), dict) else {}

    if platform == "kalshi":
        rules_primary = market.get("rules_primary", "")
        rules_secondary = market.get("rules_secondary", "")
        if rules_primary or rules_secondary:
            entry = {"platform": "Kalshi"}
            if rules_primary:
                entry["rules_primary"] = rules_primary
            if rules_secondary:
                entry["rules_secondary"] = rules_secondary
            return entry

    elif platform == "polymarket":
        description = market.get("description", "")
        if description:
            return {"platform": "Polymarket", "description": description}

    return None


def main():
    if not ENRICHED_FILE.exists():
        print(f"Error: {ENRICHED_FILE} not found")
        sys.exit(1)

    print(f"Loading {ENRICHED_FILE}...")
    with gzip.open(ENRICHED_FILE, "rt", encoding="utf-8") as f:
        data = json.load(f)

    markets = data.get("markets", data)
    print(f"Processing {len(markets)} enriched markets...")

    rules = {}
    for entry in markets:
        csv_data = entry.get("original_csv", entry)
        market_id = csv_data.get("market_id")
        if not market_id:
            continue

        extracted = extract_rules(entry)
        if extracted:
            rules[str(market_id)] = extracted

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(rules),
        "rules": rules,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, separators=(",", ":"))

    file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
    kalshi_count = sum(1 for r in rules.values() if r["platform"] == "Kalshi")
    pm_count = sum(1 for r in rules.values() if r["platform"] == "Polymarket")
    print(f"Wrote {OUTPUT_FILE} ({file_size_mb:.1f} MB)")
    print(f"  Kalshi: {kalshi_count} markets with rules")
    print(f"  Polymarket: {pm_count} markets with description")
    print(f"  Total: {len(rules)} markets")


if __name__ == "__main__":
    main()
