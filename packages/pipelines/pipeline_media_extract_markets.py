#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Extract & Match Market References from Citations
================================================================================

For each raw citation, extract which prediction market contract is referenced
using regex patterns, then fuzzy-match to Bellwether's enriched market database.

Input:  data/media_citations_raw.json
Input:  data/enriched_political_markets.json.gz
Output: data/media_citations_matched.json
================================================================================
"""

import gzip
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json, get_openai_client

# ─── Import shared logic from media_pipeline.core ───────────────────────────
from media_pipeline.core import (
    # Constants
    FUZZY_CANDIDATE_THRESHOLD,
    FUZZY_CANDIDATE_LIMIT,
    LLM_MODEL,
    LLM_MATCH_ENABLED,
    PLATFORM_PROB_PATTERNS,
    GENERIC_PROB_PATTERNS,
    PLATFORM_MENTION,
    POLYMARKET_URL,
    KALSHI_URL,
    POLYMARKET_MARKET_URL,
    STOP_WORDS,
    OUTLET_AUTHORITY,
    TOPIC_PATTERNS,
    # Helpers
    _is_missing,
    _to_float,
    _safe_set,
    flatten_market,
    extract_keywords,
    build_market_search_text,
    build_market_indices,
    filter_markets_by_platform,
    keyword_prefilter,
    generate_market_url,
    normalize_title,
    # Extraction
    extract_market_references,
    # Matching
    MarketSearchIndex,
    match_by_url,
    get_fuzzy_candidates,
    match_with_llm,
    match_reference_to_market,
    validate_probability_match,
    classify_citation_topic,
    build_topic_clusters,
    # Dedup
    deduplicate_citations,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Configuration ───────────────────────────────────────────────────────────

RAW_FILE = DATA_DIR / "media_citations_raw.json"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
MARKET_MAP_FILE = Path(__file__).resolve().parent.parent.parent / "docs" / "data" / "market_map.json"
OUTPUT_FILE = DATA_DIR / "media_citations_matched.json"
CROSS_PLATFORM_FILE = DATA_DIR / "cross_platform_reviewed_pairs.json"


# ─── I/O Functions (stay in this script) ─────────────────────────────────────

def _save_output(output_citations, matched_count, unmatched_count, no_reference_count, skipped_prior, total):
    """Save current progress to OUTPUT_FILE (used for periodic checkpoints and final save)."""
    output = {
        "citations": output_citations,
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_citations": len(output_citations),
            "matched": matched_count,
            "unmatched": unmatched_count,
            "no_reference": no_reference_count,
        },
    }
    atomic_write_json(OUTPUT_FILE, output, indent=2, ensure_ascii=False)
    logger.info(f"Checkpoint: saved {len(output_citations)}/{total} citations ({matched_count} matched, {skipped_prior} reused)")


def load_enriched_markets():
    """Load enriched markets, flatten nested structures, and build search index."""
    logger.info(f"Loading enriched markets from {ENRICHED_FILE}...")

    if not ENRICHED_FILE.exists():
        logger.error(f"Enriched markets file not found: {ENRICHED_FILE}")
        return []

    with gzip.open(ENRICHED_FILE, "rt", encoding="utf-8") as f:
        data = json.load(f)

    # Handle both list and dict-with-markets formats
    if isinstance(data, list):
        raw_markets = data
    elif isinstance(data, dict):
        raw_markets = data.get("markets", data.get("data", []))
    else:
        raw_markets = []

    # Flatten nested original_csv/api_data structure
    markets = [flatten_market(m) for m in raw_markets]

    logger.info(f"Loaded and flattened {len(markets)} markets")
    return markets


def load_bwr_ticker_map():
    """Load market_map.json and build lookup dicts from k_ticker/pm_token_id to BWR ticker."""
    if not MARKET_MAP_FILE.exists():
        logger.warning(f"Market map not found: {MARKET_MAP_FILE} — BWR tickers will be empty")
        return {}

    data = json.loads(MARKET_MAP_FILE.read_text(encoding="utf-8"))
    lookup = {}
    for m in data.get("markets", []):
        bwr = m.get("ticker", "")
        if not bwr:
            continue
        kt = m.get("k_ticker")
        if kt:
            lookup[("k", kt)] = bwr
        pt = m.get("pm_token_id")
        if pt:
            lookup[("pm", str(pt))] = bwr
    logger.info(f"Loaded BWR ticker map with {len(lookup)} entries")
    return lookup


def resolve_bwr_ticker(matched_market, bwr_lookup):
    """Look up BWR ticker for a matched market using k_ticker or pm_token_id."""
    # Try k_ticker, then market_id (which is the Kalshi ticker in CSV data)
    for field in ("k_ticker", "market_id"):
        kt = matched_market.get(field, "")
        if kt and not _is_missing(kt) and ("k", kt) in bwr_lookup:
            return bwr_lookup[("k", kt)]
    # Try pm_token_id_yes (CSV field), then pm_token_id
    for field in ("pm_token_id_yes", "pm_token_id"):
        pt = matched_market.get(field, "")
        if pt and not _is_missing(pt) and ("pm", str(pt)) in bwr_lookup:
            return bwr_lookup[("pm", str(pt))]
    return ""


def load_cross_platform_pairs():
    """Load cross_platform_reviewed_pairs.json and build bidirectional lookup.

    Returns dict mapping market_id -> counterpart market_id for same_event_same_rules verdicts.
    """
    if not CROSS_PLATFORM_FILE.exists():
        return {}

    try:
        data = json.loads(CROSS_PLATFORM_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

    lookup = {}
    pairs = data if isinstance(data, list) else data.get("pairs", [])
    if isinstance(pairs, dict):
        pairs = list(pairs.values())
    for pair in pairs:
        verdict = pair.get("verdict", "")
        if verdict != "same_event_same_rules":
            continue
        k_id = pair.get("kalshi_ticker") or pair.get("k_ticker", "")
        pm_id = pair.get("pm_market_id") or pair.get("polymarket_id", "")
        if k_id and pm_id:
            lookup[k_id] = pm_id
            lookup[str(pm_id)] = k_id

    return lookup


def find_cross_platform_counterpart(matched_market, platform_mentioned, lookup,
                                     markets, ticker_index, slug_index):
    """Find the cross-platform counterpart for a matched market.

    When citation mentions "Polymarket" but match is Kalshi (or vice versa),
    find the counterpart market.

    Returns dict with counterpart info or None.
    """
    if not lookup or not matched_market:
        return None

    # Determine matched market's identifier
    k_ticker = matched_market.get("k_ticker") or matched_market.get("market_id", "")
    pm_id = matched_market.get("pm_market_id", "")

    counterpart_id = None
    if k_ticker and k_ticker in lookup:
        counterpart_id = lookup[k_ticker]
    elif pm_id and str(pm_id) in lookup:
        counterpart_id = lookup[str(pm_id)]

    if not counterpart_id:
        return None

    # Find the counterpart market in our market list
    idx = None
    if isinstance(counterpart_id, str) and counterpart_id.upper() in ticker_index:
        idx = ticker_index[counterpart_id.upper()]
    elif str(counterpart_id) in slug_index:
        idx = slug_index[str(counterpart_id)]
    else:
        # Try to find by pm_market_id or k_ticker scan
        for i, m in enumerate(markets):
            if str(m.get("pm_market_id", "")) == str(counterpart_id):
                idx = i
                break
            if m.get("k_ticker", "") == counterpart_id:
                idx = i
                break

    if idx is None:
        return None

    counterpart = markets[idx]
    cp_platform = "polymarket" if counterpart.get("pm_market_id") else "kalshi"
    return {
        "market_id": counterpart.get("pm_market_id") or counterpart.get("k_ticker") or "",
        "question": counterpart.get("question") or counterpart.get("title", ""),
        "platform": cp_platform,
        "market_url": generate_market_url(counterpart),
    }


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("MEDIA CITATION: EXTRACT & MATCH MARKETS")
    logger.info("=" * 60)

    # Load raw citations
    if not RAW_FILE.exists():
        logger.error(f"Raw citations not found: {RAW_FILE}")
        return 1

    raw_data = json.loads(RAW_FILE.read_text(encoding="utf-8"))
    citations = raw_data.get("citations", [])
    logger.info(f"Loaded {len(citations)} raw citations")

    # Deduplicate syndicated articles
    citations, syndication_map = deduplicate_citations(citations)
    if syndication_map:
        logger.info(f"Identified {len(syndication_map)} syndicated copies")

    # Load enriched markets
    markets = load_enriched_markets()
    if not markets:
        logger.error("No enriched markets loaded, cannot match")
        return 1

    # Load BWR ticker lookup
    bwr_lookup = load_bwr_ticker_map()

    # Pre-build search text and keyword index for all markets
    market_texts = [build_market_search_text(m) for m in markets]
    market_keywords = [extract_keywords(t) for t in market_texts]
    slug_index, ticker_index, pm_id_index = build_market_indices(markets)
    logger.info(f"Built indices: {len(slug_index)} slugs, {len(ticker_index)} tickers, {len(pm_id_index)} pm_ids")

    # Build TF-IDF search index
    search_index = None
    try:
        search_index = MarketSearchIndex(market_texts)
        logger.info("Built TF-IDF search index")
    except Exception as e:
        logger.warning(f"TF-IDF index build failed, falling back to fuzzy-only: {e}")

    # Build topic clusters
    topic_clusters = build_topic_clusters(markets, market_texts)
    logger.info(f"Built topic clusters: {len(topic_clusters)} topics")

    # Load cross-platform pairs
    cross_platform_lookup = load_cross_platform_pairs()
    if cross_platform_lookup:
        logger.info(f"Loaded {len(cross_platform_lookup)} cross-platform pair entries")

    # Initialize OpenAI client for LLM matching
    openai_client = None
    if LLM_MATCH_ENABLED:
        try:
            openai_client = get_openai_client()
            logger.info(f"LLM matching enabled (model: {LLM_MODEL})")
        except Exception as e:
            logger.warning(f"OpenAI client init failed, falling back to fuzzy-only: {e}")

    # Load previous matched results to skip already-processed citations
    prior_by_url = {}
    if OUTPUT_FILE.exists():
        try:
            prev = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
            for c in prev.get("citations", []):
                if c.get("match_status") and c.get("url"):
                    prior_by_url[c["url"]] = c
            logger.info(f"Loaded {len(prior_by_url)} previously matched citations (will skip)")
        except (json.JSONDecodeError, IOError):
            pass

    # Process each citation
    matched_count = 0
    unmatched_count = 0
    no_reference_count = 0
    skipped_prior = 0

    output_citations = []

    # Track primary citation outputs for syndication inheritance
    primary_outputs = {}

    last_save = 0

    for i, citation in enumerate(citations):
        if i % 100 == 0 and i > 0:
            logger.info(f"Progress: {i}/{len(citations)} citations processed")

        # Save progress every 2000 citations so timeout doesn't lose work
        if i - last_save >= 2000 and i > 0:
            _save_output(output_citations, matched_count, unmatched_count, no_reference_count, skipped_prior, len(citations))
            last_save = i

        # Skip citations already matched in a prior run
        url = citation.get("url", "")
        if url and url in prior_by_url:
            prior = prior_by_url[url]
            output_citations.append(prior)
            if prior.get("match_status") == "MATCHED":
                matched_count += 1
            elif prior.get("match_status") == "NO_REFERENCE":
                no_reference_count += 1
            else:
                unmatched_count += 1
            skipped_prior += 1
            primary_outputs[i] = prior
            continue

        # Syndicated copies inherit match from primary
        if i in syndication_map:
            primary_idx = syndication_map[i]
            if primary_idx in primary_outputs:
                primary_out = primary_outputs[primary_idx]
                output_citations.append({
                    **citation,
                    "market_references": primary_out.get("market_references", []),
                    "match_status": primary_out.get("match_status", "UNMATCHED"),
                    "syndicated_from": citation.get("syndicated_from", ""),
                })
                if primary_out.get("match_status") == "MATCHED":
                    matched_count += 1
                else:
                    unmatched_count += 1
                continue

        # Extract market references from text
        references = extract_market_references(citation)

        if not references:
            no_reference_count += 1
            output_citations.append({
                **citation,
                "market_references": [],
                "match_status": "NO_REFERENCE",
            })
            continue

        # Match each reference to a Bellwether market
        matched_refs = []
        has_match = False

        for ref in references:
            market, confidence, score = match_reference_to_market(
                ref, markets, market_texts, market_keywords,
                slug_index, ticker_index, openai_client,
                pm_id_index=pm_id_index, search_index=search_index,
                topic_clusters=topic_clusters,
                cross_platform_lookup=cross_platform_lookup,
            )

            matched_ref = {
                "raw_text": ref["raw_text"],
                "platform_mentioned": ref["platform_mentioned"],
                "probability_cited": ref["probability_cited"],
                "match_confidence": confidence,
                "match_score": score,
            }

            if market:
                has_match = True
                # Use pm_token_id_yes (CSV field name) for Polymarket token ID
                pm_token = market.get("pm_token_id_yes") or market.get("pm_token_id", "")
                # Ensure slug is a string, not None
                pm_slug = market.get("pm_market_slug") or ""
                if pm_slug in ("nan", "None"):
                    pm_slug = ""
                matched_ref["matched_market"] = {
                    "market_id": market.get("pm_market_id") or market.get("k_ticker") or market.get("market_id") or "",
                    "bwr_ticker": resolve_bwr_ticker(market, bwr_lookup),
                    "question": market.get("question") or market.get("title", ""),
                    "platform": "polymarket" if market.get("pm_market_id") else "kalshi",
                    "category": market.get("category", ""),
                    "k_ticker": market.get("k_ticker") or market.get("market_id", ""),
                    "pm_token_id": pm_token,
                    "pm_market_id": market.get("pm_market_id", ""),
                    "pm_market_slug": pm_slug,
                    "k_yes_price": market.get("k_yes_price"),
                    "pm_yes_price": market.get("pm_yes_price"),
                    "total_volume": market.get("total_volume") or market.get("volume_usd") or market.get("volume", 0),
                    "k_liquidity_dollars": market.get("k_liquidity_dollars"),
                    "status": market.get("status", ""),
                    "market_url": generate_market_url(market),
                }

                # Cross-platform counterpart
                platform_mentioned = ref.get("platform_mentioned", "generic")
                counterpart = find_cross_platform_counterpart(
                    market, platform_mentioned, cross_platform_lookup,
                    markets, ticker_index, slug_index
                )
                if counterpart:
                    matched_ref["matched_market"]["cross_platform_market"] = counterpart

            matched_refs.append(matched_ref)

        if has_match:
            matched_count += 1
        else:
            unmatched_count += 1

        out_entry = {
            **citation,
            "market_references": matched_refs,
            "match_status": "MATCHED" if has_match else "UNMATCHED",
        }
        output_citations.append(out_entry)
        primary_outputs[i] = out_entry

    # Summary
    logger.info(f"Results: {matched_count} matched, {unmatched_count} unmatched, {no_reference_count} no reference, {skipped_prior} reused from prior run")

    # Final save
    _save_output(output_citations, matched_count, unmatched_count, no_reference_count, skipped_prior, len(citations))

    return 0


if __name__ == "__main__":
    sys.exit(main())
