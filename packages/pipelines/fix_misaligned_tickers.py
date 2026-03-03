#!/usr/bin/env python3
"""
Fix Misaligned Tickers
======================

Identifies tickers where GPT's agent field doesn't match the market question
(due to batch positional misalignment), removes them from tickers.json,
so that create_tickers.py can re-process them in incremental mode.

Usage:
    python3 fix_misaligned_tickers.py              # Dry run (report only)
    python3 fix_misaligned_tickers.py --fix         # Remove bad tickers from tickers.json
"""

import json
import gzip
import re
import unicodedata
import argparse
from pathlib import Path
from collections import Counter

from config import DATA_DIR

TICKERS_FILE = DATA_DIR / "tickers.json"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"

# Agents that are conceptual / won't appear literally in the question
SKIP_AGENTS = {
    # Countries / broad entities
    "US", "USA", "UK", "EU", "UN", "NATO", "WHO", "IMF",
    # Common political figures (first name or last name often not the full agent)
    "FED", "FED_RESERVE", "SCOTUS", "CONGRESS", "SENATE", "HOUSE",
    "SEC", "DOJ", "FBI", "CIA", "NSA", "EPA", "FDA", "CDC", "ICE",
    "IRS", "DOD", "DHS", "FEMA", "USPS", "NASA", "NOAA", "NIH",
    # Political parties
    "DEM", "GOP", "REP", "REPUBLICAN", "DEMOCRAT",
    # Very common names that may appear differently
    "TRUMP", "BIDEN", "OBAMA", "HARRIS",
}

# Actions where the agent is often an abstract concept
SKIP_ACTIONS = {
    "REPORT",    # e.g., BWR-CPI-REPORT... agent is the indicator, not in question
    "HIT",       # e.g., BWR-SP500-HIT... agent is an index/metric
    "CLOSE",     # e.g., BWR-NASDAQ-CLOSE...
    "REACH",     # e.g., BWR-BITCOIN-REACH...
    "EXCEED",    # e.g., BWR-GDP-EXCEED...
    "STRIKE",    # e.g., BWR-BOEING-STRIKE...
}


def normalize_text(text: str) -> str:
    """Normalize text for comparison: unicode normalize, uppercase, strip accents."""
    # Unicode normalize (NFD decomposes accented chars)
    text = unicodedata.normalize("NFD", text)
    # Remove combining marks (accents)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text.upper()


def fuzzy_match(agent_part: str, text: str) -> bool:
    """Check if agent_part appears in text with fuzzy tolerance for minor spelling.

    Handles:
    - Exact substring match
    - Off-by-one character differences (Levenshtein distance 1)
    - Accent normalization (already handled by normalize_text)
    - Insertion/deletion (SHERRILL vs SHERILL)
    """
    if len(agent_part) < 3:
        return False

    # Exact substring
    if agent_part in text:
        return True

    # For short parts, only do exact match
    if len(agent_part) <= 4:
        return False

    # Try windows of size part_len-1, part_len, part_len+1 to catch ins/del
    part_len = len(agent_part)
    for window_size in (part_len - 1, part_len, part_len + 1):
        if window_size < 3:
            continue
        for i in range(max(0, len(text) - window_size + 1)):
            window = text[i:i + window_size]
            if len(window) == window_size:
                dist = _quick_distance(agent_part, window)
                if dist <= 1:
                    return True

    return False


def _quick_distance(s1: str, s2: str) -> int:
    """Quick Levenshtein distance for short strings (optimized for distance <= 1)."""
    if s1 == s2:
        return 0
    len1, len2 = len(s1), len(s2)
    if abs(len1 - len2) > 1:
        return 2  # More than 1 edit

    # Same length: count substitutions
    if len1 == len2:
        diffs = sum(1 for a, b in zip(s1, s2) if a != b)
        return min(diffs, 2)

    # Length differs by 1: check for single insertion/deletion
    longer, shorter = (s1, s2) if len1 > len2 else (s2, s1)
    i = j = diffs = 0
    while i < len(longer) and j < len(shorter):
        if longer[i] != shorter[j]:
            diffs += 1
            if diffs > 1:
                return 2
            i += 1  # Skip char in longer string
        else:
            i += 1
            j += 1
    return diffs + (len(longer) - i)


def load_enriched_subtitles() -> dict:
    """Load enriched data and build market_id → subtitle/rules text lookup."""
    if not ENRICHED_FILE.exists():
        return {}

    with gzip.open(ENRICHED_FILE, "rt", encoding="utf-8") as f:
        data = json.load(f)

    markets = data.get("markets", data)
    lookup = {}
    for m in markets:
        csv = m.get("original_csv", m)
        mid = str(csv.get("market_id", ""))
        if not mid:
            continue

        # Gather all text fields that might contain the agent name
        parts = []
        for field in ("k_subtitle", "k_yes_sub_title", "k_no_sub_title",
                       "k_rules_primary", "pm_description", "candidate"):
            val = csv.get(field, "")
            if val and str(val) != "nan":
                parts.append(str(val))

        # Also check api_data
        api = m.get("api_data", {})
        if isinstance(api, dict):
            mkt = api.get("market", {})
            if isinstance(mkt, dict):
                for field in ("subtitle", "yes_sub_title", "rules_primary", "description"):
                    val = mkt.get(field, "")
                    if val and str(val) != "nan":
                        parts.append(str(val))

        if parts:
            lookup[mid] = " ".join(parts)

    return lookup


def enhanced_sanity_check(ticker: dict, extra_text: str = "") -> bool:
    """Enhanced sanity check with unicode normalization and fuzzy matching.

    Returns True if the ticker looks correct, False if likely misaligned.
    """
    agent = ticker.get("agent", "")
    if not agent:
        return True

    action = ticker.get("action", "")

    # Skip known abstract/conceptual agents
    if agent in SKIP_AGENTS:
        return True

    # Skip if any part of compound agent is in skip list (e.g., US_CONGRESS → US)
    agent_parts_raw = agent.replace("_", " ").split()
    if any(p in SKIP_AGENTS for p in agent_parts_raw):
        return True

    # Skip 2-char agents — these are always country/state codes (valid)
    if len(agent) <= 2:
        return True

    # Skip actions where agent is often an index/metric name
    if action in SKIP_ACTIONS:
        return True

    # Compound agents with hyphens (e.g., IL-HAMAS, ZELENSKYY-PUTIN, RU_UA, CN_JP)
    # Check each hyphen/underscore-separated part independently
    if "-" in agent:
        hyphen_parts = agent.split("-")
        if all(len(p) <= 3 or p in SKIP_AGENTS for p in hyphen_parts):
            return True  # All parts are short codes or skip agents

    # Build the text to search in (question + market_id + subtitles/rules)
    question = ticker.get("original_question", "")
    market_id = ticker.get("market_id", "")
    search_text = normalize_text(f"{question} {market_id} {extra_text}")

    # Split agent on underscores AND hyphens and check each part
    agent_normalized = normalize_text(agent)
    parts = agent_normalized.replace("_", " ").replace("-", " ").split()

    # Check if any meaningful part of the agent appears in the search text
    for p in parts:
        if len(p) < 3:
            continue
        if fuzzy_match(p, search_text):
            return True

    # Also check full agent as one string (e.g., "DE_ALVARADO" → "DEALVARADO")
    full_agent = agent_normalized.replace("_", "").replace(" ", "")
    if len(full_agent) >= 4 and full_agent in search_text.replace(" ", ""):
        return True

    return False


def main():
    parser = argparse.ArgumentParser(description="Fix misaligned tickers")
    parser.add_argument("--fix", action="store_true", help="Actually remove bad tickers from file")
    args = parser.parse_args()

    # Load tickers
    with open(TICKERS_FILE) as f:
        data = json.load(f)

    tickers = data.get("tickers", [])
    print(f"Loaded {len(tickers)} tickers")

    # Load enriched data for subtitle/rules checking
    print("Loading enriched market data for subtitle matching...")
    subtitle_lookup = load_enriched_subtitles()
    print(f"  Loaded subtitles for {len(subtitle_lookup)} markets")

    # Build multi-outcome question map (questions with >1 market = multi-outcome)
    from collections import defaultdict
    question_counts = defaultdict(int)
    for t in tickers:
        if "error" not in t:
            question_counts[t.get("original_question", "")] += 1
    multi_outcome_questions = {q for q, c in question_counts.items() if c > 1}
    print(f"  Multi-outcome questions: {len(multi_outcome_questions)}")

    # Run enhanced sanity check
    bad_tickers = []
    good_tickers = []

    for t in tickers:
        if "error" in t:
            good_tickers.append(t)  # Keep error entries — they'll be retried anyway
            continue

        # Multi-outcome markets: agent won't be in the generic question text,
        # but should appear in subtitles/rules from enriched data
        extra = subtitle_lookup.get(t.get("market_id", ""), "")
        if enhanced_sanity_check(t, extra):
            good_tickers.append(t)
        else:
            bad_tickers.append(t)

    print(f"\nResults:")
    print(f"  Good: {len(good_tickers)}")
    print(f"  Bad (misaligned): {len(bad_tickers)}")

    # Show breakdown by action
    action_counts = Counter(t.get("action", "?") for t in bad_tickers)
    print(f"\nBad tickers by action:")
    for action, count in action_counts.most_common(15):
        print(f"  {action}: {count}")

    # Show some examples
    print(f"\nSample bad tickers:")
    for t in bad_tickers[:20]:
        agent = t.get("agent", "?")
        q = t.get("original_question", "?")[:80]
        mid = t.get("market_id", "?")
        ticker_str = t.get("ticker", "?")
        print(f"  Agent={agent:20s} | Q={q}")
        print(f"    Ticker: {ticker_str}")
        print()

    if args.fix:
        # Remove bad tickers from file
        print(f"\nRemoving {len(bad_tickers)} bad tickers from {TICKERS_FILE}...")
        data["tickers"] = good_tickers
        data["bad_tickers_removed"] = len(bad_tickers)
        data["removal_timestamp"] = __import__("datetime").datetime.now().isoformat()

        with open(TICKERS_FILE, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Done. {len(good_tickers)} tickers remaining.")
        print(f"Run `python3 create_tickers.py` to re-process the {len(bad_tickers)} removed markets.")
    else:
        print(f"\nDry run. Use --fix to actually remove bad tickers.")


if __name__ == "__main__":
    main()
