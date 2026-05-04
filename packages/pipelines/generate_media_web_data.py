#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Generate Media Section Website Data
================================================================================

Aggregates citation fragility data into JSON files for the media.html page.

Input:  data/media_citations_with_fragility.json
Output: docs/data/media_summary.json    (~2KB  - hero stats + timeline)
Output: docs/data/media_outlets.json    (~20KB - outlet leaderboard)
Output: docs/data/media_citations.json  (~200KB - 500 most recent citations)
================================================================================
"""

import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, WEBSITE_DIR, atomic_write_json

# ─── Import shared logic from media_pipeline ────────────────────────────────
from media_pipeline.core import (
    PROMO_PATTERNS,
    TOPIC_PATTERNS,
    _safe_str,
    is_promotional,
    classify_topic,
    compute_outlet_grade,
    domain_to_name,
)
from media_pipeline.schema import is_duplicate

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─── Configuration ───────────────────────────────────────────────────────────

INPUT_FILE = DATA_DIR / "media_citations_with_fragility.json"
OUTPUT_DIR = WEBSITE_DIR / "data"

MAX_CITATIONS_WEB = 500  # Cap individual citations in web JSON
MAX_TOPICS = 5  # Top N topics to include


# ─── Aggregation Functions ────────────────────────────────────────────────────

def _parse_date(pub_date):
    """Parse an ISO date string to a timezone-aware datetime, or None."""
    if not pub_date:
        return None
    try:
        return datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def generate_outlet_leaderboard(citations):
    """Aggregate citation metrics by outlet with 24h / 30d time windows.

    Quality stats (fragility, tiers, brier) are computed over the 30-day window.
    Citation counts are tracked for both 24h and 30d windows.
    """
    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)
    cutoff_30d = now - timedelta(days=30)

    outlets = defaultdict(lambda: {
        "total_citations": 0,
        "citations_24h": 0,
        "citations_30d": 0,
        # 30d quality accumulators
        "fragility_scores": [],
        "cost_to_move_values": [],
        "brier_scores": [],
        "tiers": {"reportable": 0, "caution": 0, "fragile": 0},
        "platforms": {"polymarket": 0, "kalshi": 0, "generic": 0},
        "latest_date": "",
        "source_type": "article",
    })

    for c in citations:
        domain = c.get("domain", "unknown")
        source_type = c.get("source_type", "article")

        # Use station for TV clips
        if source_type == "tv":
            domain = c.get("station", domain)

        entry = outlets[domain]
        entry["total_citations"] += 1
        entry["source_type"] = source_type

        pub_date = c.get("published_date", "")
        if pub_date > entry["latest_date"]:
            entry["latest_date"] = pub_date

        dt = _parse_date(pub_date)
        if dt and dt >= cutoff_24h:
            entry["citations_24h"] += 1
        is_30d = dt and dt >= cutoff_30d

        if dt and dt >= cutoff_30d:
            entry["citations_30d"] += 1

        # Quality stats — only accumulate from 30d window
        for ref in c.get("market_references", []):
            platform = ref.get("platform_mentioned", "generic")
            if platform in entry["platforms"]:
                entry["platforms"][platform] += 1

            if not is_30d:
                continue

            matched = ref.get("matched_market", {})
            frag = matched.get("fragility", {})

            if "fragility_score" in frag:
                entry["fragility_scores"].append(frag["fragility_score"])

            ctm = frag.get("cost_to_move_5c")
            if ctm is not None:
                entry["cost_to_move_values"].append(ctm)

            tier = frag.get("price_tier")
            if tier == 1:
                entry["tiers"]["reportable"] += 1
            elif tier == 2:
                entry["tiers"]["caution"] += 1
            elif tier == 3:
                entry["tiers"]["fragile"] += 1

            # Brier score: requires both cited probability and resolved outcome
            prob_cited = ref.get("probability_cited")
            outcome = matched.get("outcome")  # 0 or 1 if resolved
            if prob_cited is not None and outcome is not None:
                entry["brier_scores"].append((prob_cited - outcome) ** 2)

    # Build output list
    result = []
    for domain, data in outlets.items():
        scores = data["fragility_scores"]
        avg_fragility = round(sum(scores) / len(scores), 1) if scores else None

        ctm_values = data["cost_to_move_values"]
        avg_cost_to_move = round(sum(ctm_values) / len(ctm_values), 0) if ctm_values else None

        brier = data["brier_scores"]
        avg_brier = round(sum(brier) / len(brier), 3) if brier else None

        total_scored = sum(data["tiers"].values())
        pct_reportable = round(data["tiers"]["reportable"] / total_scored * 100, 1) if total_scored > 0 else None

        result.append({
            "domain": domain,
            "domain_name": domain_to_name(domain),
            "citations_24h": data["citations_24h"],
            "citations_30d": data["citations_30d"],
            "total_citations": data["total_citations"],
            "avg_fragility": avg_fragility,
            "avg_cost_to_move_5c": avg_cost_to_move,
            "avg_brier": avg_brier,
            "pct_reportable": pct_reportable,
            "tier_breakdown": data["tiers"],
            "platforms": data["platforms"],
            "latest_date": data["latest_date"],
            "source_type": data["source_type"],
        })

    # Sort by 30d citations descending, then 24h
    result.sort(key=lambda x: (-x["citations_30d"], -x["citations_24h"]))
    return result


def generate_timeline(citations):
    """Aggregate citations by ISO week for timeline chart."""
    weeks = defaultdict(lambda: {"count": 0, "fragility_sum": 0, "fragility_n": 0, "tiers": {1: 0, 2: 0, 3: 0}})

    for c in citations:
        pub_date = c.get("published_date", "")
        if not pub_date:
            continue

        try:
            dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            continue

        # ISO week start (Monday)
        week_start = dt - __import__("datetime").timedelta(days=dt.weekday())
        week_key = week_start.strftime("%Y-%m-%d")

        weeks[week_key]["count"] += 1

        for ref in c.get("market_references", []):
            frag = ref.get("matched_market", {}).get("fragility", {})
            if "fragility_score" in frag:
                weeks[week_key]["fragility_sum"] += frag["fragility_score"]
                weeks[week_key]["fragility_n"] += 1
            tier = frag.get("price_tier")
            if tier in (1, 2, 3):
                weeks[week_key]["tiers"][tier] += 1

    # Build sorted list
    result = []
    for week, data in sorted(weeks.items()):
        avg_frag = round(data["fragility_sum"] / data["fragility_n"], 1) if data["fragility_n"] > 0 else None
        result.append({
            "week": week,
            "count": data["count"],
            "avg_fragility": avg_frag,
            "tiers": {
                "reportable": data["tiers"][1],
                "caution": data["tiers"][2],
                "fragile": data["tiers"][3],
            },
        })

    return result


def generate_topics(citations):
    """Cluster citations by topic and return top N."""
    topics = defaultdict(lambda: {
        "count": 0,
        "platforms": set(),
        "example_sentence": "",
        "domains": set(),
    })

    for c in citations:
        topic = classify_topic(c)
        entry = topics[topic]
        entry["count"] += 1

        # Collect platforms mentioned
        for ref in c.get("market_references", []):
            plat = ref.get("platform_mentioned", "")
            if plat:
                entry["platforms"].add(plat)

        entry["domains"].add(c.get("domain", ""))

        # Keep the longest sentence as example
        sent = c.get("sentence", "")
        if len(sent) > len(entry["example_sentence"]):
            entry["example_sentence"] = sent

    # Filter out generic/catch-all topics, sort by count, take top N
    EXCLUDED_TOPICS = {"Industry News", "Other"}
    filtered = {k: v for k, v in topics.items() if k not in EXCLUDED_TOPICS}
    sorted_topics = sorted(filtered.items(), key=lambda x: -x[1]["count"])[:MAX_TOPICS]

    return [
        {
            "name": name,
            "count": data["count"],
            "platforms": sorted(data["platforms"]),
            "outlet_count": len(data["domains"]),
            "example_sentence": data["example_sentence"][:200],
        }
        for name, data in sorted_topics
    ]


def generate_hero_stats(citations, outlets, raw_citations=None):
    """Top-level summary statistics with 24h and 30d windows.

    raw_citations: pre-filter citations (deduped only) for raw mention counts.
    """
    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)
    cutoff_30d = now - timedelta(days=30)

    citations_24h = 0
    citations_30d = 0
    outlets_24h = set()
    outlets_30d = set()

    # Tier counters for 30d window
    tiers_reportable = 0
    tiers_caution = 0
    tiers_fragile = 0

    for c in citations:
        domain = c.get("domain", "unknown")
        if c.get("source_type") == "tv":
            domain = c.get("station", domain)

        dt = _parse_date(c.get("published_date", ""))
        if dt and dt >= cutoff_24h:
            citations_24h += 1
            outlets_24h.add(domain)
        is_30d = dt and dt >= cutoff_30d
        if is_30d:
            citations_30d += 1
            outlets_30d.add(domain)

        # Count tiers from matched market references (30d window only)
        if is_30d:
            for ref in c.get("market_references", []):
                matched = ref.get("matched_market", {})
                frag = matched.get("fragility", {})
                tier = frag.get("price_tier")
                if tier == 1:
                    tiers_reportable += 1
                elif tier == 2:
                    tiers_caution += 1
                elif tier == 3:
                    tiers_fragile += 1

    tiers_total = tiers_reportable + tiers_caution + tiers_fragile
    pct_not_reportable = round((tiers_caution + tiers_fragile) / tiers_total * 100) if tiers_total > 0 else 0

    # Citation quality breakdown
    cite_prob = sum(1 for c in citations if c.get("market_references") and
                    any(r.get("probability_cited") is not None for r in c.get("market_references", [])))
    cite_matched = sum(1 for c in citations if c.get("match_status") == "MATCHED"
                       and any(r.get("match_confidence") not in (None, "MEDIUM")
                               and r.get("matched_market")
                               for r in c.get("market_references", [])))
    cite_mention = len(citations) - cite_prob  # general mentions (no probability)

    # Raw mention counts (before promo/topic filtering, after dedup)
    raw_mentions_30d = 0
    raw_outlets_30d = set()
    for c in (raw_citations or citations):
        dt = _parse_date(c.get("published_date", ""))
        if dt and dt >= cutoff_30d:
            raw_mentions_30d += 1
            domain = c.get("domain", "unknown")
            if c.get("source_type") == "tv":
                domain = c.get("station", domain)
            raw_outlets_30d.add(domain)

    return {
        "total_citations_24h": citations_24h,
        "total_citations_30d": citations_30d,
        "total_outlets_24h": len(outlets_24h),
        "total_outlets_30d": len(outlets_30d),
        "total_outlets": len(outlets),
        "total_raw_mentions_30d": raw_mentions_30d,
        "total_raw_outlets_30d": len(raw_outlets_30d),
        "citations_with_probability": cite_prob,
        "citations_matched": cite_matched,
        "citations_mention_only": cite_mention,
        "tiers_reportable": tiers_reportable,
        "tiers_caution": tiers_caution,
        "tiers_fragile": tiers_fragile,
        "tiers_total": tiers_total,
        "pct_not_reportable": pct_not_reportable,
    }


def prepare_web_citations(citations, limit=MAX_CITATIONS_WEB):
    """
    Flatten citations for web display. Keep only essential fields.
    Returns most recent `limit` citations.
    """
    flat = []
    for c in citations:
        refs = c.get("market_references", [])
        # Get the first HIGH-confidence matched reference (primary).
        # MEDIUM confidence matches (fuzzy-only, no LLM confirmation) are
        # too unreliable for display — treat them as unmatched.
        primary_ref = None
        for ref in refs:
            if ref.get("matched_market") and ref.get("match_confidence") != "MEDIUM":
                primary_ref = ref
                break
        if not primary_ref:
            primary_ref = refs[0] if refs else None

        matched = primary_ref.get("matched_market", {}) if primary_ref else {}
        frag = matched.get("fragility", {})
        vol = matched.get("volatility", {})

        flat.append({
            "id": c.get("id", ""),
            "source_type": c.get("source_type", "article"),
            "title": c.get("title", ""),
            "url": c.get("url", ""),
            "domain": c.get("domain", ""),
            "domain_name": domain_to_name(c.get("domain", "")),
            "station": c.get("station", ""),
            "topic": classify_topic(c),
            "date": c.get("published_date", ""),
            "sentence": c.get("sentence", ""),
            "match_status": c.get("match_status", ""),
            "platform": primary_ref.get("platform_mentioned", "") if primary_ref else "",
            "probability_cited": primary_ref.get("probability_cited") if primary_ref else None,
            "match_confidence": primary_ref.get("match_confidence", "") if primary_ref else "",
            "market_question": _safe_str(matched.get("question")),
            "market_ticker": _safe_str(matched.get("bwr_ticker")),
            "k_ticker": _safe_str(matched.get("k_ticker")),
            "pm_token_id": _safe_str(matched.get("pm_token_id")),
            "pm_market_id": _safe_str(matched.get("pm_market_id")),
            "pm_slug": _safe_str(matched.get("pm_market_slug")),
            "market_url": _safe_str(matched.get("market_url")),
            "price_at_citation": matched.get("price_at_citation"),
            "fragility_score": frag.get("fragility_score"),
            "price_tier": frag.get("price_tier"),
            "tier_label": frag.get("tier_label", ""),
            "volume_usd": frag.get("volume_usd"),
            "cost_to_move_5c": frag.get("cost_to_move_5c"),
            "volatility_24h": vol.get("24h", {}).get("max_swing") if vol else None,
        })

    # Sort by date descending, take most recent
    flat.sort(key=lambda x: x.get("date", ""), reverse=True)
    return flat[:limit]


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("GENERATE MEDIA WEB DATA")
    logger.info("=" * 60)

    if not INPUT_FILE.exists():
        logger.error(f"Input not found: {INPUT_FILE}")
        return 1

    data = json.loads(INPUT_FILE.read_text(encoding="utf-8"))
    all_citations = data.get("citations", [])
    logger.info(f"Loaded {len(all_citations)} raw citations")

    # Step 0: Deduplicate by URL hash (catches cross-source duplicates)
    seen = set()
    deduped = [c for c in all_citations if not is_duplicate(c, seen)]
    dedup_removed = len(all_citations) - len(deduped)
    if dedup_removed:
        logger.info(f"Removed {dedup_removed} duplicate citations by URL")

    # Step 1: Remove promotional/affiliate content (ads, promo codes, sign-up bonuses)
    non_promo = [c for c in deduped if not is_promotional(c)]
    promo_removed = len(deduped) - len(non_promo)
    if promo_removed:
        logger.info(f"Filtered {promo_removed} promotional/affiliate citations")

    # Step 2: Reject future-dated citations (bad seendate parses from Media Cloud)
    max_date = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    non_future = [c for c in non_promo if c.get("published_date", "") <= max_date]
    future_removed = len(non_promo) - len(non_future)
    if future_removed:
        logger.info(f"Filtered {future_removed} future-dated citations")

    # Step 3: Filter out generic platform coverage (op-eds about prediction
    # markets, industry news). Only keep citations that reference markets in the
    # context of a real-world event.
    EXCLUDED_TOPICS = {"Industry News", "Other"}
    citations = [c for c in non_future if classify_topic(c) not in EXCLUDED_TOPICS]
    topic_removed = len(non_future) - len(citations)
    logger.info(f"After filtering: {len(citations)} citations ({dedup_removed} dedup + {promo_removed} promo + {future_removed} future + {topic_removed} non-event removed)")

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now(timezone.utc).isoformat()

    # 1. Outlet leaderboard
    outlets = generate_outlet_leaderboard(citations)
    logger.info(f"Generated leaderboard for {len(outlets)} outlets")

    # 2. Timeline
    timeline = generate_timeline(citations)
    logger.info(f"Generated timeline with {len(timeline)} weeks")

    # 3. Hero stats (pass deduped as raw_citations for unfiltered mention count)
    hero = generate_hero_stats(citations, outlets, raw_citations=deduped)
    logger.info(f"Hero stats: {hero['total_raw_mentions_30d']} raw mentions, {hero['total_citations_30d']} filtered (30d), {hero['total_outlets']} outlets")

    # 4. Topics
    topics = generate_topics(citations)
    logger.info(f"Generated {len(topics)} topics (top: {topics[0]['name'] if topics else 'none'})")

    # 5. Web citations (flattened, capped)
    web_citations = prepare_web_citations(citations)
    logger.info(f"Prepared {len(web_citations)} citations for web")

    # Write output files
    summary_file = OUTPUT_DIR / "media_summary.json"
    atomic_write_json(summary_file, {
        "hero": hero,
        "topics": topics,
        "timeline": timeline,
        "generated_at": generated_at,
    }, indent=2)
    logger.info(f"Wrote {summary_file}")

    outlets_file = OUTPUT_DIR / "media_outlets.json"
    atomic_write_json(outlets_file, {
        "outlets": outlets,
        "generated_at": generated_at,
    }, indent=2)
    logger.info(f"Wrote {outlets_file}")

    citations_file = OUTPUT_DIR / "media_citations.json"
    atomic_write_json(citations_file, {
        "citations": web_citations,
        "total_count": len(citations),
        "generated_at": generated_at,
    }, indent=2)
    logger.info(f"Wrote {citations_file}")

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
