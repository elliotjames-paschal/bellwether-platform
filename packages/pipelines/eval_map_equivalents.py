#!/usr/bin/env python3
"""
Map equivalents dataset UUIDs to BWR market_ids via fuzzy title matching.

Input:  data/equivalents_eval/equivalents_polymarket_kalshi/equivalents_shared_data.bson
        data/enriched_political_markets.json.gz
Output: data/equivalents_eval/uuid_to_market_id.json
"""

import sys
import json
import gzip
import re
import bson
from pathlib import Path
from datetime import datetime, timezone
from difflib import SequenceMatcher

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR

BSON_FILE = DATA_DIR / "equivalents_eval" / "equivalents_polymarket_kalshi" / "equivalents_shared_data.bson"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
OUTPUT_FILE = DATA_DIR / "equivalents_eval" / "uuid_to_market_id.json"

# Matching thresholds
FUZZY_THRESHOLD = 0.80
TIMESTAMP_TOLERANCE_DAYS = 90


def normalize_title(text: str) -> str:
    """Normalize a title for comparison: lowercase, strip, collapse whitespace."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text


def extract_title(document: str) -> str:
    """Extract the main title from a document string.

    Equivalents format: "Question? Suffix text In 2025" or "Title. Description..."
    We want just the question up to and including the first '?'.
    """
    # First try: grab everything up to (and including) the first '?'
    idx = document.find('?')
    if idx != -1:
        return document[:idx + 1].strip()
    # Fallback: split on first ". " to get the title
    idx = document.find('. ')
    if idx != -1:
        return document[:idx].strip()
    return document.strip()


def _trigrams(text: str) -> set:
    """Return set of character trigrams for fast pre-filtering."""
    return {text[i:i+3] for i in range(len(text) - 2)} if len(text) >= 3 else {text}


def load_enriched_markets():
    """Load enriched markets, indexed by platform with normalized titles and hash lookups."""
    print("Loading enriched markets...")
    with gzip.open(ENRICHED_FILE, 'rt', encoding='utf-8') as f:
        data = json.load(f)

    by_platform = {"kalshi": {}, "polymarket": {}}  # exact_index: norm → market_id
    candidates = {"kalshi": [], "polymarket": []}

    for m in data.get("markets", []):
        csv = m.get("original_csv", {})
        market_id = csv.get("market_id", "")
        platform = csv.get("platform", "").lower()
        question = str(csv.get("question", ""))

        if not market_id or not question or platform not in by_platform:
            continue

        norm = normalize_title(question)
        by_platform[platform][norm] = market_id
        candidates[platform].append({
            "market_id": market_id,
            "normalized": norm,
            "trigrams": _trigrams(norm),
        })

    print(f"  Kalshi: {len(candidates['kalshi'])} markets")
    print(f"  Polymarket: {len(candidates['polymarket'])} markets")
    return by_platform, candidates


def find_best_match(title: str, platform: str, exact_index: dict, candidate_list: list) -> dict | None:
    """Find the best matching market for a given title and platform."""
    norm_title = normalize_title(title)
    if not norm_title:
        return None

    # Fast exact match via hash lookup
    if norm_title in exact_index:
        return {"market_id": exact_index[norm_title], "confidence": 1.0, "method": "exact"}

    # Trigram pre-filter: only fuzzy-match candidates sharing ≥30% of trigrams
    query_tri = _trigrams(norm_title)
    if not query_tri:
        return None

    best_match = None
    best_score = 0

    for c in candidate_list:
        overlap = len(query_tri & c["trigrams"])
        union = len(query_tri | c["trigrams"])
        if union == 0 or overlap / union < 0.2:
            continue

        score = SequenceMatcher(None, norm_title, c["normalized"]).ratio()
        if score > best_score:
            best_score = score
            best_match = c

    if best_match and best_score >= FUZZY_THRESHOLD:
        return {
            "market_id": best_match["market_id"],
            "confidence": round(best_score, 4),
            "method": "fuzzy",
        }

    return None


def main():
    print(f"Mapping equivalents UUIDs to BWR market_ids...")
    print(f"  BSON: {BSON_FILE}")
    print(f"  Enriched: {ENRICHED_FILE}")

    if not BSON_FILE.exists():
        print(f"  ERROR: BSON file not found")
        return
    if not ENRICHED_FILE.exists():
        print(f"  ERROR: Enriched file not found")
        return

    # Load enriched markets
    exact_index, candidates = load_enriched_markets()

    # Load equivalents BSON
    print("Loading equivalents BSON...")
    with open(BSON_FILE, 'rb') as f:
        docs = bson.decode_all(f.read())
    print(f"  {len(docs)} equivalent groups")

    # Map each market UUID
    mapping = {}
    matched = 0
    unmatched = 0
    unmatched_samples = []

    total_markets = sum(len(doc.get("markets", [])) for doc in docs)
    print(f"  {total_markets} total markets to map")

    for doc in docs:
        markets = doc.get("markets", [])
        for m in markets:
            uuid = m.get("uuid", "")
            platform = m.get("platform", "").lower()
            document = m.get("document", "")
            timestamp = m.get("time_start", 0)

            if not uuid or platform not in exact_index:
                unmatched += 1
                continue

            title = extract_title(document)
            result = find_best_match(title, platform, exact_index[platform], candidates[platform])

            if result:
                mapping[uuid] = {
                    "market_id": result["market_id"],
                    "platform": platform,
                    "confidence": result["confidence"],
                    "method": result["method"],
                    "equiv_title": title[:100],
                }
                matched += 1
            else:
                unmatched += 1
                if len(unmatched_samples) < 20:
                    unmatched_samples.append({
                        "uuid": uuid,
                        "platform": platform,
                        "title": title[:100],
                    })

    # Stats
    print(f"\nResults:")
    print(f"  Matched:   {matched} / {total_markets} ({100*matched/total_markets:.1f}%)")
    print(f"  Unmatched: {unmatched} / {total_markets} ({100*unmatched/total_markets:.1f}%)")

    exact = sum(1 for v in mapping.values() if v["method"] == "exact")
    fuzzy = sum(1 for v in mapping.values() if v["method"] == "fuzzy")
    print(f"  Exact matches: {exact}")
    print(f"  Fuzzy matches: {fuzzy}")

    if mapping:
        confidences = [v["confidence"] for v in mapping.values()]
        confidences.sort()
        print(f"  Confidence: min={confidences[0]:.2f}, median={confidences[len(confidences)//2]:.2f}, max={confidences[-1]:.2f}")

    if unmatched_samples:
        print(f"\n  Sample unmatched ({len(unmatched_samples)}):")
        for s in unmatched_samples[:10]:
            print(f"    [{s['platform']}] {s['title']}")

    # Write output
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "matched": matched,
        "unmatched": unmatched,
        "total_markets": total_markets,
        "fuzzy_threshold": FUZZY_THRESHOLD,
        "mapping": mapping,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print(f"\n  Wrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
