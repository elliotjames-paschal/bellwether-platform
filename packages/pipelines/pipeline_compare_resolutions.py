#!/usr/bin/env python3
"""
Phase 2.5d: GPT Resolution Comparison

For Bucket B candidates (same event, different mechanism/threshold),
compare resolution criteria via GPT-4o and classify as IDENTICAL,
OVERLAPPING, or DIFFERENT.

Reads: cross_platform_candidates.json, enriched_political_markets.json.gz
Writes: cross_platform_resolution_verdicts.json
"""

import sys
import json
import gzip
import asyncio
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, get_openai_api_key, atomic_write_json

try:
    from openai import AsyncOpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

# --- Paths ---
CANDIDATES_FILE = DATA_DIR / "cross_platform_candidates.json"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
VERDICTS_FILE = DATA_DIR / "cross_platform_resolution_verdicts.json"

# --- Constants ---
GPT_MODEL = "gpt-4o"
MAX_CONCURRENT = 5
MAX_PAIRS_PER_RUN = 500

SYSTEM_PROMPT = """You are a prediction market resolution analyst. Given two markets from different platforms, compare their resolution criteria and determine if they resolve under the same conditions.

Return ONLY valid JSON with this exact structure:
{"verdict": "IDENTICAL", "correct_ticker": "BWR-...", "explanation": "..."}

Or:
{"verdict": "OVERLAPPING", "explanation": "..."}

Or:
{"verdict": "DIFFERENT", "explanation": "..."}

Definitions:
- IDENTICAL: Both markets resolve Yes/No under exactly the same conditions. Minor wording differences don't matter if the resolution logic is equivalent. Include "correct_ticker" with the more precise ticker of the two.
- OVERLAPPING: The markets are about the same event but resolve differently (e.g., different thresholds, different time windows, one is a subset of the other).
- DIFFERENT: The markets are about fundamentally different events or outcomes despite surface similarity."""


def extract_json_from_response(text):
    """Extract JSON from GPT response that may include markdown fences."""
    if "```json" in text:
        start = text.find("```json") + 7
        end = text.find("```", start)
        if end > start:
            return text[start:end].strip()

    if "```" in text:
        start = text.find("```") + 3
        if text[start:start + 1] == "\n":
            start += 1
        end = text.find("```", start)
        if end > start:
            return text[start:end].strip()

    brace_start = text.find("{")
    if brace_start >= 0:
        depth = 0
        for i, c in enumerate(text[brace_start:]):
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return text[brace_start:brace_start + i + 1]

    return text


def load_resolution_lookup():
    """Build market_id -> resolution text from enriched data."""
    if not ENRICHED_FILE.exists():
        return {}

    if str(ENRICHED_FILE).endswith(".gz"):
        with gzip.open(ENRICHED_FILE, "rt") as f:
            enriched = json.load(f)
    else:
        with open(ENRICHED_FILE) as f:
            enriched = json.load(f)

    markets = enriched.get("markets", enriched)
    lookup = {}

    for m in markets:
        csv_data = m.get("original_csv", m)
        api = m.get("api_data", {})
        mkt = api.get("market", {}) if isinstance(api.get("market"), dict) else {}

        market_id = str(csv_data.get("market_id", ""))
        rules = mkt.get("rules_primary") or mkt.get("description") or ""
        if not rules:
            rules = csv_data.get("k_rules_primary", "") or csv_data.get("pm_description", "")

        lookup[market_id] = str(rules)

    return lookup


def load_existing_verdicts():
    """Load already-computed verdicts keyed by pair_key."""
    if VERDICTS_FILE.exists():
        with open(VERDICTS_FILE) as f:
            data = json.load(f)
        return {v["pair_key"]: v for v in data.get("verdicts", [])}
    return {}


def build_user_prompt(pair, resolution_lookup):
    """Build the GPT user prompt for comparing one pair."""
    k_mid = pair["kalshi_market_id"]
    p_mid = pair["poly_market_id"]

    k_rules = resolution_lookup.get(k_mid, "(no resolution rules available)")
    p_rules = resolution_lookup.get(p_mid, "(no resolution rules available)")

    diffs = ", ".join(pair.get("diffs", []))

    return f"""Compare these two prediction markets from different platforms:

**Market A (Kalshi)**
Question: {pair['kalshi_question']}
Resolution Rules: {k_rules[:1500]}

**Market B (Polymarket)**
Question: {pair['poly_question']}
Resolution Rules: {p_rules[:1500]}

**Ticker Components:**
- Kalshi ticker: {pair['kalshi_ticker']}
- Polymarket ticker: {pair['poly_ticker']}
- Differences: {diffs}

Do these markets resolve under the same conditions?"""


async def compare_pair_async(client, pair, resolution_lookup, semaphore, model=GPT_MODEL):
    """Call GPT-4o to compare one pair's resolution criteria."""
    async with semaphore:
        try:
            user_prompt = build_user_prompt(pair, resolution_lookup)
            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=300,
            )
            text = response.choices[0].message.content
            json_str = extract_json_from_response(text)
            result = json.loads(json_str)

            return {
                "pair_key": pair["pair_key"],
                "kalshi_market_id": pair["kalshi_market_id"],
                "poly_market_id": pair["poly_market_id"],
                "kalshi_ticker": pair["kalshi_ticker"],
                "poly_ticker": pair["poly_ticker"],
                "kalshi_question": pair["kalshi_question"],
                "poly_question": pair["poly_question"],
                "cosine_similarity": pair["cosine_similarity"],
                "diffs": pair.get("diffs", []),
                "verdict": result.get("verdict", "ERROR"),
                "correct_ticker": result.get("correct_ticker"),
                "explanation": result.get("explanation", ""),
                "reviewed_at": datetime.now().isoformat(),
            }
        except json.JSONDecodeError as e:
            print(f"    JSON parse error for {pair['pair_key']}: {e}")
            return {
                "pair_key": pair["pair_key"],
                "kalshi_market_id": pair["kalshi_market_id"],
                "poly_market_id": pair["poly_market_id"],
                "kalshi_ticker": pair["kalshi_ticker"],
                "poly_ticker": pair["poly_ticker"],
                "verdict": "ERROR",
                "error": f"JSON parse error: {e}",
                "reviewed_at": datetime.now().isoformat(),
            }
        except Exception as e:
            error_type = type(e).__name__
            print(f"    {error_type} for {pair['pair_key']}: {e}")
            # Check for rate limit - signal to stop
            if "rate_limit" in error_type.lower() or "ratelimit" in error_type.lower():
                raise  # Re-raise to stop the batch
            return {
                "pair_key": pair["pair_key"],
                "kalshi_market_id": pair["kalshi_market_id"],
                "poly_market_id": pair["poly_market_id"],
                "kalshi_ticker": pair["kalshi_ticker"],
                "poly_ticker": pair["poly_ticker"],
                "verdict": "ERROR",
                "error": str(e),
                "reviewed_at": datetime.now().isoformat(),
            }


async def compare_all_pairs(pairs, resolution_lookup, model=GPT_MODEL):
    """Compare all Bucket B pairs concurrently (rate-limited)."""
    client = AsyncOpenAI(api_key=get_openai_api_key())
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    results = []
    tasks = [
        compare_pair_async(client, pair, resolution_lookup, semaphore, model=model)
        for pair in pairs
    ]

    completed = 0
    try:
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1
            if completed % 10 == 0:
                print(f"    Compared {completed}/{len(pairs)} pairs...")
    except Exception as e:
        print(f"  Stopping early due to error: {e}")
        print(f"  Completed {completed}/{len(pairs)} pairs before stopping")

    return results


def main():
    parser = argparse.ArgumentParser(description="GPT resolution comparison for Bucket B pairs")
    parser.add_argument("--max-pairs", type=int, default=MAX_PAIRS_PER_RUN,
                        help=f"Maximum pairs to compare per run (default: {MAX_PAIRS_PER_RUN})")
    parser.add_argument("--model", default=GPT_MODEL, help=f"GPT model (default: {GPT_MODEL})")
    args = parser.parse_args()

    if not HAS_OPENAI:
        print("WARNING: openai package not installed.")
        sys.exit(0)

    if not CANDIDATES_FILE.exists():
        print("No candidates file found. Run pipeline_discover_cross_platform.py first.")
        sys.exit(0)

    print(f"[{datetime.now().isoformat()}] Resolution comparison starting...")

    # Load candidates
    with open(CANDIDATES_FILE) as f:
        candidates = json.load(f)

    bucket_b = candidates.get("bucket_b", [])
    if not bucket_b:
        print("  No Bucket B pairs to compare.")
        sys.exit(0)

    print(f"  {len(bucket_b)} Bucket B pairs found")

    # Load existing verdicts to skip already-compared pairs
    existing_verdicts = load_existing_verdicts()
    if existing_verdicts:
        print(f"  {len(existing_verdicts)} pairs already have verdicts")

    # Filter to unreviewed pairs
    to_compare = [p for p in bucket_b if p["pair_key"] not in existing_verdicts]
    # Also skip ERROR verdicts so they get retried
    error_keys = {k for k, v in existing_verdicts.items() if v.get("verdict") == "ERROR"}
    to_compare.extend([p for p in bucket_b if p["pair_key"] in error_keys])

    if not to_compare:
        print("  All pairs already compared. Nothing to do.")
        sys.exit(0)

    # Cap per run
    if len(to_compare) > args.max_pairs:
        print(f"  Capping at {args.max_pairs} pairs (of {len(to_compare)} remaining)")
        to_compare = to_compare[:args.max_pairs]

    print(f"  Comparing {len(to_compare)} pairs with {args.model}...")

    # Load resolution text
    resolution_lookup = load_resolution_lookup()
    print(f"  Resolution text available for {len(resolution_lookup)} markets")

    # Run async comparisons
    new_verdicts = asyncio.run(compare_all_pairs(to_compare, resolution_lookup, model=args.model))

    # Merge with existing verdicts
    all_verdicts = dict(existing_verdicts)
    for v in new_verdicts:
        all_verdicts[v["pair_key"]] = v

    # Count verdict types
    verdict_counts = {}
    for v in all_verdicts.values():
        vtype = v.get("verdict", "UNKNOWN")
        verdict_counts[vtype] = verdict_counts.get(vtype, 0) + 1

    print(f"\n  Verdict summary:")
    for vtype, count in sorted(verdict_counts.items()):
        print(f"    {vtype}: {count}")

    # Save
    output = {
        "generated_at": datetime.now().isoformat(),
        "model": args.model,
        "verdicts": list(all_verdicts.values()),
        "stats": verdict_counts,
    }
    atomic_write_json(VERDICTS_FILE, output, indent=2)
    print(f"\n  Saved {len(all_verdicts)} verdicts to {VERDICTS_FILE}")

    print(f"[{datetime.now().isoformat()}] Resolution comparison complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
