#!/usr/bin/env python3
"""
Phase 2.5a-c: Cross-Platform Market Discovery

Identifies unmatched markets (single-platform tickers), computes embeddings
via sentence-transformers, finds candidate cross-platform pairs via cosine
similarity, and triages into buckets A/B/C.

Reads: tickers_postprocessed.json, enriched_political_markets.json.gz
Writes: cross_platform_candidates.json, embeddings_cache/
"""

import sys
import json
import gzip
import argparse
import numpy as np
from pathlib import Path
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

try:
    from sentence_transformers import SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False

# --- Paths ---
TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
CANDIDATES_FILE = DATA_DIR / "cross_platform_candidates.json"
REVIEWED_PAIRS_FILE = DATA_DIR / "cross_platform_reviewed_pairs.json"
EMBEDDINGS_DIR = DATA_DIR / "embeddings_cache"
EMBEDDINGS_FILE = EMBEDDINGS_DIR / "embeddings.npz"
INDEX_FILE = EMBEDDINGS_DIR / "index.json"

# --- Constants ---
SIMILARITY_THRESHOLD = 0.70
TOP_K = 3
MODEL_NAME = "all-MiniLM-L6-v2"
EMBED_BATCH_SIZE = 256


def make_pair_key(id_a: str, id_b: str) -> str:
    """Canonical pair key: sorted alphabetically, pipe-delimited."""
    ids = sorted([str(id_a), str(id_b)])
    return f"{ids[0]}|{ids[1]}"


def load_tickers():
    """Load tickers from tickers_postprocessed.json."""
    with open(TICKERS_FILE) as f:
        data = json.load(f)
    return data["tickers"]


def identify_unmatched_markets(tickers):
    """
    Step 2.5a: Find markets whose ticker string exists on only one platform.

    For each single-platform ticker group, pick the highest-volume market
    to avoid duplicate comparisons.

    Returns (kalshi_unmatched, poly_unmatched) lists of ticker dicts.
    """
    by_ticker = defaultdict(lambda: {"kalshi": [], "polymarket": []})
    for t in tickers:
        platform_key = "kalshi" if t.get("platform") == "Kalshi" else "polymarket"
        by_ticker[t["ticker"]][platform_key].append(t)

    kalshi_unmatched = []
    poly_unmatched = []

    for ticker_str, groups in by_ticker.items():
        if groups["kalshi"] and not groups["polymarket"]:
            # Pick best by volume (if available), else just first
            best = max(groups["kalshi"], key=lambda x: x.get("volume", 0))
            kalshi_unmatched.append(best)
        elif groups["polymarket"] and not groups["kalshi"]:
            best = max(groups["polymarket"], key=lambda x: x.get("volume", 0))
            poly_unmatched.append(best)

    return kalshi_unmatched, poly_unmatched


def load_resolution_lookup(enriched_file):
    """Build market_id -> resolution text from enriched data."""
    if str(enriched_file).endswith(".gz"):
        with gzip.open(enriched_file, "rt") as f:
            enriched = json.load(f)
    else:
        with open(enriched_file) as f:
            enriched = json.load(f)

    markets = enriched.get("markets", enriched)
    lookup = {}

    for m in markets:
        csv_data = m.get("original_csv", m)
        api = m.get("api_data", {})
        mkt = api.get("market", {}) if isinstance(api.get("market"), dict) else {}

        market_id = str(csv_data.get("market_id", ""))

        # Kalshi: rules_primary; Polymarket: description
        rules = mkt.get("rules_primary") or mkt.get("description") or ""
        if not rules:
            rules = csv_data.get("k_rules_primary", "") or csv_data.get("pm_description", "")

        lookup[market_id] = str(rules)

    return lookup


def build_embed_text(ticker_dict, resolution_lookup):
    """Build text to embed: question + truncated resolution rules."""
    question = ticker_dict.get("original_question", "")
    mid = str(ticker_dict.get("market_id", ""))
    rules = resolution_lookup.get(mid, "")
    # Truncate rules to 500 chars to keep embedding focused
    if rules:
        return f"{question} | {rules[:500]}"
    return question


def load_embedding_cache():
    """Load cached embeddings. Returns (embeddings_matrix, id_to_row_index)."""
    if EMBEDDINGS_FILE.exists() and INDEX_FILE.exists():
        data = np.load(EMBEDDINGS_FILE)
        embeddings = data["embeddings"]
        with open(INDEX_FILE) as f:
            index = json.load(f)
        return embeddings, index
    return np.empty((0, 384), dtype=np.float32), {}


def save_embedding_cache(embeddings, index):
    """Save embeddings to disk."""
    EMBEDDINGS_DIR.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(EMBEDDINGS_FILE, embeddings=embeddings)
    atomic_write_json(INDEX_FILE, index)


def compute_embeddings_incremental(markets, resolution_lookup, existing_embeddings, existing_index):
    """
    Compute embeddings only for markets not in cache.
    Returns updated (embeddings_matrix, index).
    """
    # Find new market IDs
    new_markets = [m for m in markets if str(m["market_id"]) not in existing_index]

    if not new_markets:
        print(f"  All {len(markets)} markets already cached")
        return existing_embeddings, existing_index

    print(f"  Computing embeddings for {len(new_markets)} new markets ({len(markets) - len(new_markets)} cached)...")

    # Build texts to embed
    texts = [build_embed_text(m, resolution_lookup) for m in new_markets]

    # Load model and encode
    model = SentenceTransformer(MODEL_NAME)
    new_embeddings = model.encode(
        texts,
        batch_size=EMBED_BATCH_SIZE,
        show_progress_bar=len(texts) > 500,
        normalize_embeddings=True,  # L2 normalize for cosine sim via dot product
    )
    new_embeddings = new_embeddings.astype(np.float32)

    # Append to existing
    if existing_embeddings.shape[0] > 0:
        combined = np.vstack([existing_embeddings, new_embeddings])
    else:
        combined = new_embeddings

    # Update index
    updated_index = dict(existing_index)
    start_row = len(existing_index)
    for i, m in enumerate(new_markets):
        updated_index[str(m["market_id"])] = start_row + i

    return combined, updated_index


def find_candidate_pairs(kalshi_unmatched, poly_unmatched, embeddings, index,
                         reviewed_pairs, threshold=SIMILARITY_THRESHOLD):
    """
    Step 2.5b: Find cross-platform candidate pairs via cosine similarity.

    For each Kalshi market, find top-K Polymarket matches above threshold
    (and vice versa). Deduplicate into unique pairs.
    """
    # Build sub-matrices for each platform
    k_ids = [str(t["market_id"]) for t in kalshi_unmatched if str(t["market_id"]) in index]
    p_ids = [str(t["market_id"]) for t in poly_unmatched if str(t["market_id"]) in index]

    if not k_ids or not p_ids:
        print("  No embeddable markets on one or both platforms")
        return []

    k_rows = np.array([index[mid] for mid in k_ids])
    p_rows = np.array([index[mid] for mid in p_ids])

    k_emb = embeddings[k_rows]  # (K, 384)
    p_emb = embeddings[p_rows]  # (P, 384)

    # Handle any zero-norm vectors (from empty questions)
    k_norms = np.linalg.norm(k_emb, axis=1, keepdims=True)
    p_norms = np.linalg.norm(p_emb, axis=1, keepdims=True)
    k_norms[k_norms == 0] = 1.0
    p_norms[p_norms == 0] = 1.0
    k_emb = k_emb / k_norms
    p_emb = p_emb / p_norms

    # Dot product of normalized vectors = cosine similarity
    print(f"  Computing similarity matrix ({len(k_ids)} x {len(p_ids)})...")
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        sim = k_emb @ p_emb.T  # (K, P)
    # Replace any NaN/inf values with 0
    np.nan_to_num(sim, copy=False, nan=0.0, posinf=0.0, neginf=0.0)

    # Build lookups for ticker info
    k_lookup = {str(t["market_id"]): t for t in kalshi_unmatched}
    p_lookup = {str(t["market_id"]): t for t in poly_unmatched}

    # Collect pairs: for each K market, top-K PM matches above threshold
    seen_pairs = set()
    candidates = []

    # From Kalshi side
    for ki, k_mid in enumerate(k_ids):
        row = sim[ki]
        top_indices = np.argsort(row)[::-1][:TOP_K]
        for pi in top_indices:
            if row[pi] < threshold:
                break
            p_mid = p_ids[pi]
            pair_key = make_pair_key(k_mid, p_mid)
            if pair_key in reviewed_pairs or pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            k_t = k_lookup[k_mid]
            p_t = p_lookup[p_mid]
            candidates.append({
                "pair_key": pair_key,
                "kalshi_market_id": k_mid,
                "poly_market_id": p_mid,
                "cosine_similarity": round(float(row[pi]), 4),
                "kalshi_ticker": k_t["ticker"],
                "poly_ticker": p_t["ticker"],
                "kalshi_question": k_t.get("original_question", ""),
                "poly_question": p_t.get("original_question", ""),
            })

    # From Polymarket side (catch pairs where PM→K was top but K→PM wasn't)
    for pi, p_mid in enumerate(p_ids):
        col = sim[:, pi]
        top_indices = np.argsort(col)[::-1][:TOP_K]
        for ki in top_indices:
            if col[ki] < threshold:
                break
            k_mid = k_ids[ki]
            pair_key = make_pair_key(k_mid, p_mid)
            if pair_key in reviewed_pairs or pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            k_t = k_lookup[k_mid]
            p_t = p_lookup[p_mid]
            candidates.append({
                "pair_key": pair_key,
                "kalshi_market_id": k_mid,
                "poly_market_id": p_mid,
                "cosine_similarity": round(float(col[ki]), 4),
                "kalshi_ticker": k_t["ticker"],
                "poly_ticker": p_t["ticker"],
                "kalshi_question": k_t.get("original_question", ""),
                "poly_question": p_t.get("original_question", ""),
            })

    candidates.sort(key=lambda x: x["cosine_similarity"], reverse=True)
    return candidates


def parse_ticker_components(ticker_str):
    """
    Parse 'BWR-AGENT-ACTION-TARGET-MECHANISM-THRESHOLD-TIMEFRAME' into dict.

    Handles targets with hyphens (e.g., BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028).
    The ticker has at least 7 parts: BWR + 6 components. If there are more than 7 parts,
    the extra parts belong to TARGET (which can contain hyphens).
    """
    parts = ticker_str.split("-")
    if len(parts) < 7:
        return {
            "agent": parts[1] if len(parts) > 1 else "",
            "action": parts[2] if len(parts) > 2 else "",
            "target": "-".join(parts[3:]) if len(parts) > 3 else "",
            "mechanism": "",
            "threshold": "",
            "timeframe": "",
        }

    # BWR is parts[0], agent=1, action=2, then target is everything
    # in the middle, mechanism=-3, threshold=-2, timeframe=-1
    return {
        "agent": parts[1],
        "action": parts[2],
        "target": "-".join(parts[3:-3]),
        "mechanism": parts[-3],
        "threshold": parts[-2],
        "timeframe": parts[-1],
    }


def triage_candidates(candidates):
    """
    Step 2.5c: Triage candidates into 3 buckets.

    Bucket A: Identical tickers (should already match — bug detector)
    Bucket B: Same AGENT+ACTION+TARGET+TIMEFRAME, different MECHANISM or THRESHOLD
    Bucket C: Different AGENT, ACTION, or TARGET (genuinely different events)
    """
    bucket_a, bucket_b, bucket_c = [], [], []

    for pair in candidates:
        k_parts = parse_ticker_components(pair["kalshi_ticker"])
        p_parts = parse_ticker_components(pair["poly_ticker"])

        # Bucket A: identical tickers
        if pair["kalshi_ticker"] == pair["poly_ticker"]:
            pair["bucket"] = "A"
            pair["reason"] = "identical_ticker"
            bucket_a.append(pair)
            continue

        # Check core identity match
        agent_match = k_parts["agent"] == p_parts["agent"]
        action_match = k_parts["action"] == p_parts["action"]
        target_match = k_parts["target"] == p_parts["target"]
        timeframe_match = k_parts["timeframe"] == p_parts["timeframe"]

        if agent_match and action_match and target_match and timeframe_match:
            # Bucket B: same event, different mechanism/threshold
            diffs = []
            if k_parts["mechanism"] != p_parts["mechanism"]:
                diffs.append(f"mechanism: {k_parts['mechanism']} vs {p_parts['mechanism']}")
            if k_parts["threshold"] != p_parts["threshold"]:
                diffs.append(f"threshold: {k_parts['threshold']} vs {p_parts['threshold']}")
            pair["bucket"] = "B"
            pair["reason"] = "same_event_different_resolution"
            pair["diffs"] = diffs
            bucket_b.append(pair)
        else:
            # Bucket C: different events
            pair["bucket"] = "C"
            pair["reason"] = "different_events"
            bucket_c.append(pair)

    return {"bucket_a": bucket_a, "bucket_b": bucket_b, "bucket_c": bucket_c}


def load_reviewed_pairs():
    """Load set of previously reviewed pair keys."""
    if REVIEWED_PAIRS_FILE.exists():
        with open(REVIEWED_PAIRS_FILE) as f:
            data = json.load(f)
        return set(data.get("pairs", {}).keys())
    return set()


def main():
    parser = argparse.ArgumentParser(description="Cross-platform market discovery via embeddings")
    parser.add_argument("--threshold", type=float, default=SIMILARITY_THRESHOLD,
                        help=f"Cosine similarity threshold (default: {SIMILARITY_THRESHOLD})")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Rebuild embedding cache from scratch")
    args = parser.parse_args()

    if not HAS_SENTENCE_TRANSFORMERS:
        print("WARNING: sentence-transformers not installed.")
        print("Run: pip install sentence-transformers")
        print("Skipping cross-platform discovery.")
        sys.exit(0)

    if not TICKERS_FILE.exists():
        print(f"Tickers file not found: {TICKERS_FILE}")
        sys.exit(0)

    print(f"[{datetime.now().isoformat()}] Cross-platform discovery starting...")

    # Step 2.5a: Find unmatched markets
    print("\n--- Step 2.5a: Identifying unmatched markets ---")
    tickers = load_tickers()
    kalshi_unmatched, poly_unmatched = identify_unmatched_markets(tickers)
    print(f"  Kalshi-only tickers: {len(kalshi_unmatched)} markets")
    print(f"  Polymarket-only tickers: {len(poly_unmatched)} markets")

    if not kalshi_unmatched or not poly_unmatched:
        print("  No unmatched markets on one or both platforms. Nothing to do.")
        sys.exit(0)

    # Load resolution text for embedding enrichment
    resolution_lookup = {}
    if ENRICHED_FILE.exists():
        print("  Loading resolution text from enriched data...")
        resolution_lookup = load_resolution_lookup(ENRICHED_FILE)
        print(f"  Resolution text available for {len(resolution_lookup)} markets")

    # Step 2.5b: Compute embeddings and find candidates
    print("\n--- Step 2.5b: Computing embeddings & finding candidates ---")
    all_unmatched = kalshi_unmatched + poly_unmatched

    if args.full_refresh:
        existing_emb = np.empty((0, 384), dtype=np.float32)
        existing_idx = {}
    else:
        existing_emb, existing_idx = load_embedding_cache()
        if existing_emb.shape[0] > 0:
            print(f"  Loaded cache: {existing_emb.shape[0]} embeddings")

    embeddings, index = compute_embeddings_incremental(
        all_unmatched, resolution_lookup, existing_emb, existing_idx
    )
    save_embedding_cache(embeddings, index)
    print(f"  Total embeddings cached: {embeddings.shape[0]}")

    # Load reviewed pairs for deduplication
    reviewed_pairs = load_reviewed_pairs()
    if reviewed_pairs:
        print(f"  Skipping {len(reviewed_pairs)} previously reviewed pairs")

    candidates = find_candidate_pairs(
        kalshi_unmatched, poly_unmatched, embeddings, index,
        reviewed_pairs, threshold=args.threshold
    )
    print(f"  Found {len(candidates)} candidate pairs (threshold={args.threshold})")

    if not candidates:
        print("  No new candidate pairs found.")
        # Write empty candidates file so downstream scripts don't error
        atomic_write_json(CANDIDATES_FILE, {
            "generated_at": datetime.now().isoformat(),
            "threshold": args.threshold,
            "bucket_a": [], "bucket_b": [], "bucket_c": [],
            "stats": {"total": 0, "bucket_a": 0, "bucket_b": 0, "bucket_c": 0},
        }, indent=2)
        sys.exit(0)

    # Step 2.5c: Triage into buckets
    print("\n--- Step 2.5c: Triaging candidates ---")
    buckets = triage_candidates(candidates)

    print(f"  Bucket A (identical tickers): {len(buckets['bucket_a'])}")
    print(f"  Bucket B (same event, diff resolution): {len(buckets['bucket_b'])}")
    print(f"  Bucket C (different events): {len(buckets['bucket_c'])}")

    # Save results
    output = {
        "generated_at": datetime.now().isoformat(),
        "threshold": args.threshold,
        "bucket_a": buckets["bucket_a"],
        "bucket_b": buckets["bucket_b"],
        "bucket_c": buckets["bucket_c"],
        "stats": {
            "total": len(candidates),
            "bucket_a": len(buckets["bucket_a"]),
            "bucket_b": len(buckets["bucket_b"]),
            "bucket_c": len(buckets["bucket_c"]),
            "kalshi_unmatched": len(kalshi_unmatched),
            "poly_unmatched": len(poly_unmatched),
        },
    }
    atomic_write_json(CANDIDATES_FILE, output, indent=2)
    print(f"\n  Saved to {CANDIDATES_FILE}")

    # Print top-5 Bucket B pairs for quick review
    if buckets["bucket_b"]:
        print("\n  Top Bucket B candidates:")
        for p in buckets["bucket_b"][:5]:
            print(f"    sim={p['cosine_similarity']:.3f}  K: {p['kalshi_ticker']}")
            print(f"                    PM: {p['poly_ticker']}")
            print(f"                    Diffs: {', '.join(p.get('diffs', []))}")

    print(f"\n[{datetime.now().isoformat()}] Cross-platform discovery complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
