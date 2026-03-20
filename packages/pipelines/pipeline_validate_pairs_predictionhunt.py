#!/usr/bin/env python3
"""
Validate Cross-Platform Market Pairs Using PredictionHunt API

Checks whether our Kalshi-Polymarket pairings (shown in the divergences tab)
are correct by querying PredictionHunt's matching-markets endpoint.

PredictionHunt independently maps markets across platforms. If their mapping
disagrees with ours, the pair is likely a mismatch.

A 3-layer local pre-filter runs first to skip obvious matches/mismatches:
  Layer 1: Keyword extraction — catches geographic, candidate, topic mismatches
  Layer 2: Sentence embedding cosine similarity (all-MiniLM-L6-v2)
  Layer 3: Ticker structure comparison — state codes, office types

Usage:
    python pipeline_validate_pairs_predictionhunt.py [--limit N] [--dry-run] [--auto-exclude] [--force]
    python pipeline_validate_pairs_predictionhunt.py --local-only

Options:
    --limit N        Max pairs to check (default: all divergence pairs)
    --dry-run        Print what would be checked without calling the API
    --auto-exclude   Write confirmed mismatches to match_exclusions.json
    --force          Re-check pairs already in the cache
    --min-spread F   Min spread to check (default: 0.05 = 5%)
    --local-only     Run only the local pre-filter, no API calls
"""

import argparse
import hashlib
import json
import re
import sys
import time
import requests
import numpy as np
from datetime import datetime, timezone
from pathlib import Path
from unidecode import unidecode

try:
    from sentence_transformers import SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, WEBSITE_DIR, get_predictionhunt_api_key

# Paths
ACTIVE_MARKETS_FILE = WEBSITE_DIR / "data" / "active_markets.json"
CHECKED_FILE = DATA_DIR / "predictionhunt_checked.json"
VALIDATION_FILE = DATA_DIR / "predictionhunt_validation.json"
USAGE_FILE = DATA_DIR / "predictionhunt_usage.json"
MATCH_EXCLUSIONS_FILE = DATA_DIR / "match_exclusions.json"

# PredictionHunt API
PH_BASE_URL = "https://predictionhunt.com/api/v1"
MONTHLY_LIMIT = 1000
REQUEST_DELAY = 1.0  # seconds between requests

# Local pre-filter thresholds
SIM_CONFIRMED = 0.80   # >= this → local_confirmed
SIM_AMBIGUOUS = 0.50   # >= this (but < confirmed) → ambiguous
SIM_MISMATCH = 0.50    # < this → local_mismatch
SIM_AUTO_EXCLUDE = 0.30  # < this → auto_exclude

EMBED_MODEL_NAME = "all-MiniLM-L6-v2"

# US states and territories for geographic entity extraction
US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
    "puerto rico": "PR", "guam": "GU",
}
US_STATE_CODES = {v: k for k, v in US_STATES.items()}

OFFICE_KEYWORDS = {
    "governor": "GOV", "senate": "SEN", "senator": "SEN",
    "house": "HOUSE", "representative": "HOUSE", "congressional": "HOUSE",
    "president": "PRES", "presidential": "PRES",
    "mayor": "MAYOR", "attorney general": "AG",
}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def load_json(path, default=None):
    if not path.exists():
        return default if default is not None else {}
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default if default is not None else {}


def save_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


def compute_exclusion_id(market_id_a: str, market_id_b: str) -> str:
    ids = sorted([str(market_id_a), str(market_id_b)])
    key = f"{ids[0]}|{ids[1]}"
    return "exc_" + hashlib.sha256(key.encode()).hexdigest()[:12]


def get_cross_platform_pairs(min_spread=0.05):
    """Load cross-platform pairs from active_markets.json, sorted by spread desc."""
    data = load_json(ACTIVE_MARKETS_FILE)
    if not data:
        print("ERROR: active_markets.json not found or empty")
        return []

    pairs = []
    for m in data.get("markets", []):
        if not m.get("has_both"):
            continue
        pm_id = m.get("pm_market_id")
        k_ticker = m.get("k_ticker")
        spread = m.get("spread")
        if not pm_id or not k_ticker:
            continue
        if spread is not None and spread < min_spread:
            continue
        pairs.append({
            "key": m.get("key", ""),
            "ticker": m.get("ticker", ""),
            "label": m.get("label", ""),
            "pm_market_id": pm_id,
            "k_ticker": k_ticker,
            "pm_price": m.get("pm_price"),
            "k_price": m.get("k_price"),
            "spread": spread,
            "pm_question": m.get("pm_question", ""),
            "k_question": m.get("k_question", ""),
        })

    # Sort by spread descending (largest divergences first)
    pairs.sort(key=lambda p: p.get("spread") or 0, reverse=True)
    return pairs


def check_quota():
    """Check remaining API quota for the current month."""
    usage = load_json(USAGE_FILE, {
        "monthly_limit": MONTHLY_LIMIT,
        "current_month": "",
        "requests_used": 0,
        "requests_log": [],
    })
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    if usage.get("current_month") != current_month:
        usage["current_month"] = current_month
        usage["requests_used"] = 0
    remaining = MONTHLY_LIMIT - usage.get("requests_used", 0)
    return remaining, usage


def record_usage(usage, count, pipeline_name="validate_pairs"):
    """Record API usage."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    usage["requests_used"] = usage.get("requests_used", 0) + count
    usage["requests_log"].append({
        "date": today,
        "count": count,
        "pipeline": pipeline_name,
    })
    save_json(USAGE_FILE, usage)


def call_predictionhunt(api_key, pm_slug):
    """Call PredictionHunt matching-markets endpoint with a Polymarket slug.

    Queries by Polymarket slug (better coverage than Kalshi ticker).
    Returns dict with 'data' (JSON response) or 'error' string.
    """
    headers = {
        "X-API-Key": api_key,
        "Accept": "application/json",
    }

    try:
        resp = requests.get(
            f"{PH_BASE_URL}/matching-markets",
            params={"polymarket_slugs": pm_slug},
            headers=headers,
            timeout=15,
        )

        if resp.status_code == 200:
            return {"data": resp.json(), "status": resp.status_code}
        elif resp.status_code == 429:
            return {"error": "rate_limited", "status": 429}
        elif resp.status_code == 401:
            return {"error": "unauthorized", "status": 401}
        elif resp.status_code == 404:
            return {"error": "not_found", "status": 404}
        else:
            return {"error": f"http_{resp.status_code}", "status": resp.status_code,
                    "body": resp.text[:200]}
    except requests.Timeout:
        return {"error": "timeout"}
    except requests.RequestException as e:
        return {"error": str(e)}


def extract_kalshi_ids_for_pm_slug(ph_response, pm_slug):
    """Extract Kalshi ticker IDs that PredictionHunt groups with our PM slug.

    Response structure: {events: [{groups: [{markets: [{id, source, source_url}]}]}]}
    Find the group containing our PM slug, then return Kalshi IDs from same group.
    """
    if not ph_response or "data" not in ph_response:
        return []

    data = ph_response["data"]
    events = data.get("events", [])
    kalshi_ids = []

    for event in events:
        for group in event.get("groups", []):
            markets = group.get("markets", [])

            # Check if our PM slug is in this group
            pm_in_group = False
            group_kalshi = []
            for m in markets:
                source = m.get("source", "")
                mid = m.get("id", "")
                url = m.get("source_url", "")

                if source == "polymarket":
                    # Extract slug from URL or use id
                    slug = url.rstrip("/").split("/")[-1].split("?")[0] if "polymarket.com" in url else mid
                    if slug == pm_slug or pm_slug in slug or slug in pm_slug:
                        pm_in_group = True
                elif source == "kalshi":
                    group_kalshi.append(mid)

            if pm_in_group:
                kalshi_ids.extend(group_kalshi)

    return list(set(kalshi_ids))


def validate_pair(pair, ph_kalshi_ids):
    """Compare our k_ticker against PredictionHunt's Kalshi matches.

    Returns one of: 'confirmed', 'mismatch', 'no_match'
    """
    our_k = pair["k_ticker"]

    if not ph_kalshi_ids:
        return "no_match"

    # Check if our Kalshi ticker is in PH's results
    for ph_id in ph_kalshi_ids:
        if our_k == ph_id or our_k in ph_id or ph_id in our_k:
            return "confirmed"

    return "mismatch"


##############################################################################
# Local Pre-Filter (Layers 1-3)
##############################################################################

def normalize_text(text):
    """Lowercase, strip accents, collapse whitespace."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', unidecode(text).lower().strip())


def extract_entities(question):
    """Layer 1: Extract key entities from a market question text.

    Returns dict with sets of: states, state_codes, offices, candidates (words),
    and a bag of normalized topic words.
    """
    text = normalize_text(question)
    entities = {
        "states": set(),
        "state_codes": set(),
        "offices": set(),
        "topic_words": set(),
    }
    if not text:
        return entities

    # Extract state names
    for state_name, code in US_STATES.items():
        if state_name in text:
            entities["states"].add(code)
            entities["state_codes"].add(code)

    # Extract 2-letter state codes (standalone, e.g. "OH" "NH")
    for match in re.finditer(r'\b([A-Z]{2})\b', question or ""):
        code = match.group(1)
        if code in US_STATE_CODES:
            entities["state_codes"].add(code)

    # Extract offices
    for keyword, office_code in OFFICE_KEYWORDS.items():
        if keyword in text:
            entities["offices"].add(office_code)

    # Topic words: remove stopwords, keep meaningful tokens
    stopwords = {
        "the", "a", "an", "in", "of", "to", "for", "and", "or", "will",
        "be", "is", "are", "was", "were", "by", "on", "at", "it", "this",
        "that", "with", "from", "as", "yes", "no", "if", "do", "does",
        "did", "has", "have", "had", "not", "but", "so", "than", "what",
        "who", "which", "when", "where", "how", "before", "after", "during",
        "between", "market", "price", "contract",
    }
    words = re.findall(r'[a-z]{3,}', text)
    entities["topic_words"] = {w for w in words if w not in stopwords}

    return entities


def detect_keyword_mismatch(pm_entities, k_entities):
    """Layer 1: Detect obvious mismatches between extracted entities.

    Returns (is_mismatch: bool, reason: str).
    """
    # Geographic mismatch: both have states but they don't overlap
    pm_states = pm_entities["states"] | pm_entities["state_codes"]
    k_states = k_entities["states"] | k_entities["state_codes"]
    if pm_states and k_states and not pm_states & k_states:
        return True, f"geographic_mismatch: PM={pm_states} K={k_states}"

    # Office mismatch: both mention offices but they differ
    pm_offices = pm_entities["offices"]
    k_offices = k_entities["offices"]
    if pm_offices and k_offices and not pm_offices & k_offices:
        return True, f"office_mismatch: PM={pm_offices} K={k_offices}"

    # Topic word overlap: if both have 5+ topic words but <10% overlap, flag it
    pm_words = pm_entities["topic_words"]
    k_words = k_entities["topic_words"]
    if len(pm_words) >= 5 and len(k_words) >= 5:
        overlap = pm_words & k_words
        union = pm_words | k_words
        if len(union) > 0 and len(overlap) / len(union) < 0.10:
            return True, f"topic_mismatch: overlap={len(overlap)}/{len(union)}"

    return False, ""


def parse_ticker_components(ticker):
    """Layer 3: Parse BWR ticker into structured components.

    Handles patterns like:
      GOV_AR_2026_SHS → office=GOV, state=AR, year=2026, candidate_code=SHS
      SEN_OH_2026_BRO → office=SEN, state=OH, year=2026, candidate_code=BRO
      HOUSE_CA36_2026_KIM → office=HOUSE, state=CA, district=36, year=2026
    """
    parts = ticker.upper().split("_") if ticker else []
    components = {"office": None, "state": None, "year": None, "candidate_code": None}

    if not parts:
        return components

    # First part is often the office
    if parts[0] in ("GOV", "SEN", "HOUSE", "PRES", "MAYOR", "AG"):
        components["office"] = parts[0]

    # Look for state code
    for p in parts:
        # Handle "CA36" → state=CA
        state_match = re.match(r'^([A-Z]{2})(\d*)$', p)
        if state_match and state_match.group(1) in US_STATE_CODES:
            components["state"] = state_match.group(1)
            break

    # Look for year
    for p in parts:
        if re.match(r'^20\d{2}$', p):
            components["year"] = p
            break

    # Last part is often candidate code
    if len(parts) >= 3:
        last = parts[-1]
        if not re.match(r'^20\d{2}$', last) and last not in ("GOV", "SEN", "HOUSE", "PRES"):
            components["candidate_code"] = last

    return components


def detect_ticker_mismatch(pair):
    """Layer 3: Detect mismatches between ticker structure and question text."""
    ticker = pair.get("ticker", "")
    pm_q = normalize_text(pair.get("pm_question", ""))
    k_q = normalize_text(pair.get("k_question", ""))

    tc = parse_ticker_components(ticker)
    if not tc["state"] and not tc["office"]:
        return False, ""

    combined_text = f"{pm_q} {k_q}"

    # If ticker has a state, check if either question mentions a DIFFERENT state
    if tc["state"]:
        ticker_state_name = US_STATE_CODES.get(tc["state"], "").lower()
        # Check if questions mention a different state but not the ticker's state
        for state_name, code in US_STATES.items():
            if code == tc["state"]:
                continue
            if state_name in combined_text and ticker_state_name not in combined_text:
                return True, f"ticker_state={tc['state']} but question mentions {code}"

    return False, ""


def compute_question_similarity(pm_questions, k_questions, model=None):
    """Layer 2: Compute cosine similarity between PM and K question texts.

    Accepts lists for batch processing. Returns list of float similarities.
    Loads the model on first call if not provided.
    """
    if not HAS_SENTENCE_TRANSFORMERS:
        print("  WARNING: sentence-transformers not installed, skipping similarity")
        return [0.65] * len(pm_questions)  # Return ambiguous score

    if model is None:
        model = SentenceTransformer(EMBED_MODEL_NAME)

    pm_texts = [normalize_text(q) or "unknown" for q in pm_questions]
    k_texts = [normalize_text(q) or "unknown" for q in k_questions]

    # Encode all texts in one batch for efficiency
    all_texts = pm_texts + k_texts
    embeddings = model.encode(all_texts, batch_size=256, normalize_embeddings=True,
                              show_progress_bar=len(all_texts) > 200)

    n = len(pm_texts)
    pm_emb = embeddings[:n]
    k_emb = embeddings[n:]

    # Cosine similarity (dot product of normalized vectors)
    similarities = np.sum(pm_emb * k_emb, axis=1).tolist()
    return similarities


def local_prefilter(pairs):
    """Run all 3 layers of local pre-filtering on pairs.

    Returns dict with keys: local_confirmed, local_mismatch, ambiguous, auto_exclude.
    Each value is a list of (pair, reason, similarity) tuples.
    """
    buckets = {
        "local_confirmed": [],
        "local_mismatch": [],
        "ambiguous": [],
        "auto_exclude": [],
    }

    if not pairs:
        return buckets

    # Layer 1: Keyword extraction for all pairs
    keyword_mismatches = {}
    for i, pair in enumerate(pairs):
        pm_ent = extract_entities(pair.get("pm_question", ""))
        k_ent = extract_entities(pair.get("k_question", ""))
        is_mismatch, reason = detect_keyword_mismatch(pm_ent, k_ent)
        if is_mismatch:
            keyword_mismatches[i] = reason

    # Layer 3: Ticker structure for all pairs
    ticker_mismatches = {}
    for i, pair in enumerate(pairs):
        is_mismatch, reason = detect_ticker_mismatch(pair)
        if is_mismatch:
            ticker_mismatches[i] = reason

    # Layer 2: Sentence embedding similarity (batch)
    pm_questions = [p.get("pm_question", "") for p in pairs]
    k_questions = [p.get("k_question", "") for p in pairs]
    print("  Computing sentence embeddings...")
    similarities = compute_question_similarity(pm_questions, k_questions)

    # Classify each pair
    for i, pair in enumerate(pairs):
        sim = similarities[i]
        kw_reason = keyword_mismatches.get(i)
        tk_reason = ticker_mismatches.get(i)

        # Strong keyword/ticker mismatch overrides moderate similarity
        if kw_reason and sim < SIM_CONFIRMED:
            reason = f"keyword: {kw_reason} (sim={sim:.3f})"
            if sim < SIM_AUTO_EXCLUDE:
                buckets["auto_exclude"].append((pair, reason, sim))
            else:
                buckets["local_mismatch"].append((pair, reason, sim))
        elif tk_reason and sim < SIM_CONFIRMED:
            reason = f"ticker: {tk_reason} (sim={sim:.3f})"
            buckets["local_mismatch"].append((pair, reason, sim))
        elif sim < SIM_AUTO_EXCLUDE:
            buckets["auto_exclude"].append((pair, f"very_low_similarity (sim={sim:.3f})", sim))
        elif sim < SIM_MISMATCH:
            buckets["local_mismatch"].append((pair, f"low_similarity (sim={sim:.3f})", sim))
        elif sim >= SIM_CONFIRMED:
            buckets["local_confirmed"].append((pair, f"high_similarity (sim={sim:.3f})", sim))
        else:
            buckets["ambiguous"].append((pair, f"moderate_similarity (sim={sim:.3f})", sim))

    return buckets


def main():
    parser = argparse.ArgumentParser(description="Validate cross-platform pairs via PredictionHunt")
    parser.add_argument("--limit", type=int, default=0, help="Max pairs to check (0=all)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without API calls")
    parser.add_argument("--auto-exclude", action="store_true", help="Auto-create exclusions for mismatches")
    parser.add_argument("--force", action="store_true", help="Re-check already-cached pairs")
    parser.add_argument("--min-spread", type=float, default=0.05, help="Min spread threshold (default: 0.05)")
    parser.add_argument("--local-only", action="store_true", help="Run only the local pre-filter, no API calls")
    args = parser.parse_args()

    print(f"[{now_iso()}] Validating cross-platform pairs via PredictionHunt...")

    # Load pairs
    pairs = get_cross_platform_pairs(min_spread=args.min_spread)
    print(f"  Cross-platform pairs (spread >= {args.min_spread}): {len(pairs)}")

    if not pairs:
        print("  No pairs to validate.")
        return

    # Load cache
    checked = load_json(CHECKED_FILE, {"checked": {}, "updated_at": ""})

    # Filter out already-checked pairs unless --force
    to_check = []
    for p in pairs:
        pair_key = f"{p['k_ticker']}|{p['pm_market_id']}"
        if not args.force and pair_key in checked.get("checked", {}):
            continue
        to_check.append(p)

    print(f"  Pairs to check: {len(to_check)} (skipping {len(pairs) - len(to_check)} cached)")

    if args.limit > 0 and not args.local_only:
        to_check = to_check[:args.limit]
        print(f"  Limited to: {args.limit}")

    if not to_check:
        print("  Nothing to check.")
        return

    # ── Local Pre-Filter ──────────────────────────────────────────────────
    print(f"\n  Running local pre-filter on {len(to_check)} pairs...")
    buckets = local_prefilter(to_check)

    n_confirmed = len(buckets["local_confirmed"])
    n_mismatch = len(buckets["local_mismatch"])
    n_auto_excl = len(buckets["auto_exclude"])
    n_ambiguous = len(buckets["ambiguous"])

    print(f"\n  === Local Pre-Filter Results ===")
    print(f"    Confirmed (skip API):   {n_confirmed}")
    print(f"    Mismatch (skip API):    {n_mismatch}")
    print(f"    Auto-exclude:           {n_auto_excl}")
    print(f"    Ambiguous (need API):   {n_ambiguous}")

    # Show local mismatches
    if buckets["local_mismatch"] or buckets["auto_exclude"]:
        print(f"\n  Local mismatches:")
        for pair, reason, sim in (buckets["local_mismatch"] + buckets["auto_exclude"])[:20]:
            print(f"    {pair['ticker']}: {reason}")
            print(f"      PM: {(pair.get('pm_question') or '')[:70]}")
            print(f"      K:  {(pair.get('k_question') or '')[:70]}")

    # Cache local results
    for bucket_name in ("local_confirmed", "local_mismatch", "auto_exclude"):
        for pair, reason, sim in buckets[bucket_name]:
            pair_key = f"{pair['k_ticker']}|{pair['pm_market_id']}"
            checked["checked"][pair_key] = {
                "status": bucket_name,
                "reason": reason,
                "similarity": round(sim, 4),
                "checked_at": now_iso(),
            }

    # Auto-exclude extremely low similarity pairs
    if args.auto_exclude and buckets["auto_exclude"]:
        exclusions = load_json(MATCH_EXCLUSIONS_FILE, {
            "schema_version": 1,
            "updated_at": "",
            "exclusions": [],
        })
        existing_ids = {e["exclusion_id"] for e in exclusions.get("exclusions", [])}
        new_excl = 0
        for pair, reason, sim in buckets["auto_exclude"]:
            exc_id = compute_exclusion_id(pair["k_ticker"], pair["pm_market_id"])
            if exc_id in existing_ids:
                continue
            exclusions["exclusions"].append({
                "exclusion_id": exc_id,
                "market_id_a": sorted([pair["k_ticker"], pair["pm_market_id"]])[0],
                "market_id_b": sorted([pair["k_ticker"], pair["pm_market_id"]])[1],
                "ticker": pair.get("ticker", ""),
                "reason": f"local_prefilter: {reason}",
                "source_label_id": f"local_prefilter_{now_iso()[:10]}",
                "created_at": now_iso(),
                "batch_id": f"local_prefilter_{now_iso()[:10]}",
            })
            new_excl += 1
        if new_excl:
            exclusions["updated_at"] = now_iso()
            save_json(MATCH_EXCLUSIONS_FILE, exclusions)
            print(f"\n  Auto-excluded {new_excl} pairs from local pre-filter")

    if args.local_only:
        # Save cache and exit
        checked["updated_at"] = now_iso()
        save_json(CHECKED_FILE, checked)
        print(f"\n  Local-only mode — skipping PredictionHunt API calls.")
        return

    # ── PredictionHunt API (ambiguous pairs only) ─────────────────────────
    api_pairs = [pair for pair, _, _ in buckets["ambiguous"]]

    # Check quota
    remaining, usage = check_quota()
    print(f"\n  API quota: {remaining} requests remaining this month")

    if args.limit > 0:
        api_pairs = api_pairs[:args.limit]
        print(f"  Limited API checks to: {args.limit}")

    if not api_pairs:
        print("  No ambiguous pairs to send to PredictionHunt.")
        checked["updated_at"] = now_iso()
        save_json(CHECKED_FILE, checked)
        return

    if remaining < len(api_pairs):
        print(f"  WARNING: Only {remaining} API calls left, need {len(api_pairs)}")
        api_pairs = api_pairs[:remaining]
        print(f"  Reduced to {len(api_pairs)} pairs")

    if args.dry_run:
        print("\n  DRY RUN — would check these ambiguous pairs:")
        for p in api_pairs:
            print(f"    {p['k_ticker']} | {p['pm_market_id']}")
            print(f"      {p['label'][:70]}")
            print(f"      spread={p['spread']:.1%}" if p['spread'] else "")
        return

    # Get API key
    try:
        api_key = get_predictionhunt_api_key()
    except ValueError as e:
        print(f"  ERROR: {e}")
        return

    # Validate ambiguous pairs via PredictionHunt
    results = {"confirmed": [], "mismatch": [], "no_match": [], "error": []}
    api_calls = 0

    for i, pair in enumerate(api_pairs):
        pair_key = f"{pair['k_ticker']}|{pair['pm_market_id']}"
        pm_slug = pair["pm_market_id"]
        print(f"\n  [{i+1}/{len(api_pairs)}] {pair['k_ticker']} <-> {pm_slug}")
        print(f"    Label: {pair['label'][:60]}")

        # Call PredictionHunt with PM slug
        ph_resp = call_predictionhunt(api_key, pm_slug)
        api_calls += 1

        if "error" in ph_resp:
            print(f"    ERROR: {ph_resp['error']}")
            if ph_resp["error"] == "unauthorized":
                print("    API key invalid. Stopping.")
                break
            if ph_resp["error"] == "rate_limited":
                print("    Rate limited. Waiting 30s...")
                time.sleep(30)
                ph_resp = call_predictionhunt(api_key, pm_slug)
                api_calls += 1
                if "error" in ph_resp:
                    results["error"].append({
                        "pair_key": pair_key, **pair,
                        "ph_error": ph_resp["error"],
                        "checked_at": now_iso(),
                    })
                    checked["checked"][pair_key] = {"status": "error", "checked_at": now_iso()}
                    continue
            else:
                results["error"].append({
                    "pair_key": pair_key, **pair,
                    "ph_error": ph_resp["error"],
                    "checked_at": now_iso(),
                })
                checked["checked"][pair_key] = {"status": "error", "checked_at": now_iso()}
                time.sleep(REQUEST_DELAY)
                continue

        # Extract Kalshi IDs that PH groups with our PM slug
        ph_kalshi_ids = extract_kalshi_ids_for_pm_slug(ph_resp, pm_slug)
        ph_count = ph_resp.get("data", {}).get("count", 0)
        print(f"    PH events: {ph_count}, Kalshi matches: {ph_kalshi_ids if ph_kalshi_ids else '(none)'}")

        # Log full response on first call for debugging
        if i == 0:
            print(f"    [DEBUG] Full PH response: {json.dumps(ph_resp.get('data', {}))[:500]}")

        # Classify result
        verdict = validate_pair(pair, ph_kalshi_ids)
        print(f"    Verdict: {verdict.upper()}")

        result_entry = {
            "pair_key": pair_key,
            **pair,
            "ph_kalshi_ids": ph_kalshi_ids,
            "verdict": verdict,
            "checked_at": now_iso(),
        }
        results[verdict].append(result_entry)
        checked["checked"][pair_key] = {"status": verdict, "checked_at": now_iso()}

        time.sleep(REQUEST_DELAY)

    # Save results
    checked["updated_at"] = now_iso()
    save_json(CHECKED_FILE, checked)

    record_usage(usage, api_calls)

    validation = {
        "generated_at": now_iso(),
        "total_checked": sum(len(v) for v in results.values()),
        "confirmed": len(results["confirmed"]),
        "mismatch": len(results["mismatch"]),
        "no_match": len(results["no_match"]),
        "error": len(results["error"]),
        "results": results,
    }
    save_json(VALIDATION_FILE, validation)

    # Summary
    print(f"\n  === Combined Results ===")
    print(f"    Local confirmed:  {n_confirmed}")
    print(f"    Local mismatch:   {n_mismatch}")
    print(f"    Local auto-excl:  {n_auto_excl}")
    print(f"    API confirmed:    {len(results['confirmed'])}")
    print(f"    API mismatch:     {len(results['mismatch'])}")
    print(f"    API no match:     {len(results['no_match'])}")
    print(f"    API error:        {len(results['error'])}")
    print(f"    API calls used:   {api_calls} (saved ~{n_confirmed + n_mismatch + n_auto_excl})")

    # Auto-exclude mismatches
    if args.auto_exclude and results["mismatch"]:
        exclusions = load_json(MATCH_EXCLUSIONS_FILE, {
            "schema_version": 1,
            "updated_at": "",
            "exclusions": [],
        })

        existing_ids = {e["exclusion_id"] for e in exclusions.get("exclusions", [])}
        new_count = 0

        for r in results["mismatch"]:
            exc_id = compute_exclusion_id(r["k_ticker"], r["pm_market_id"])
            if exc_id in existing_ids:
                continue
            exclusions["exclusions"].append({
                "exclusion_id": exc_id,
                "market_id_a": sorted([r["k_ticker"], r["pm_market_id"]])[0],
                "market_id_b": sorted([r["k_ticker"], r["pm_market_id"]])[1],
                "ticker": r.get("ticker", ""),
                "reason": "predictionhunt_mismatch",
                "source_label_id": f"ph_validation_{now_iso()[:10]}",
                "created_at": now_iso(),
                "batch_id": f"ph_validate_{now_iso()[:10]}",
            })
            new_count += 1

        if new_count:
            exclusions["updated_at"] = now_iso()
            save_json(MATCH_EXCLUSIONS_FILE, exclusions)
            print(f"\n  Auto-excluded {new_count} mismatched pairs")

    if results["mismatch"]:
        print(f"\n  Mismatched pairs:")
        for r in results["mismatch"]:
            print(f"    {r['k_ticker']} <-> {r['pm_market_id']}")
            print(f"      PH Kalshi matches: {r['ph_kalshi_ids']}")
            print(f"      Label: {r['label'][:60]}")


if __name__ == "__main__":
    main()
