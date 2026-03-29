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
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, WEBSITE_DIR, atomic_write_json

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Configuration ───────────────────────────────────────────────────────────

INPUT_FILE = DATA_DIR / "media_citations_with_fragility.json"
OUTPUT_DIR = WEBSITE_DIR / "data"

MAX_CITATIONS_WEB = 500  # Cap individual citations in web JSON
MAX_TOPICS = 5  # Top N topics to include

# ─── Promotional / Affiliate Detection ───────────────────────────────────────
# Citations matching these patterns are filtered out entirely — they're ads,
# not journalism citing market data.
PROMO_PATTERNS = [
    re.compile(r'\bpromo\s*code\b', re.I),
    re.compile(r'\breferral\s*(code|link|bonus)\b', re.I),
    re.compile(r'\bsign[\s-]*up\s+bonus\b', re.I),
    re.compile(r'\buse\s+code\b', re.I),
    re.compile(r'\bbonus\s+(offer|deal|credit)\b', re.I),
    re.compile(r'\btrade\s+\$?\d+[,.]?\d*\s*,?\s*get\s+\$?\d+', re.I),
    re.compile(r'\bfree\s+(?:bet|trade|credit|bonus)\b', re.I),
    re.compile(r'\baffiliate\b', re.I),
    re.compile(r'\bsponsored\s+(?:content|post|article)\b', re.I),
]

def is_promotional(citation):
    """Return True if a citation is promotional/affiliate content, not journalism."""
    text = " ".join(filter(None, [
        citation.get("title", ""),
        citation.get("sentence", ""),
        citation.get("context", ""),
    ]))
    return any(p.search(text) for p in PROMO_PATTERNS)


# ─── Topic Patterns ──────────────────────────────────────────────────────────
# Ordered by specificity: first match wins.
# classify_topic() tries title first, then falls back to sentence/context,
# so broader patterns won't misfire on passing mentions in article body.
TOPIC_PATTERNS = [
    # Specific topics first (highest priority)
    (re.compile(r'\b(iran|tehran|khamenei|hormuz|kharg|ayatollah)\b', re.I), 'Iran Conflict'),
    (re.compile(r'\b(fed\b|rate cut|interest rate|federal reserve|no rate cut)\b', re.I), 'Fed & Rates'),
    (re.compile(r'\b(march madness|ncaa|final four)\b', re.I), 'March Madness'),
    (re.compile(r'\b(spacex|starlink)\b', re.I), 'SpaceX IPO'),
    (re.compile(r'\b(peace prize|nobel)\b', re.I), 'Nobel Prize'),
    (re.compile(r'\b(insider trading|regulation|sec\b|cftc)\b', re.I), 'Regulation'),
    # Broader topics
    (re.compile(r'\b(election|trump|president|senate|governor|democrat|republican|congress)\b', re.I), 'US Politics'),
    (re.compile(r'\b(crypto|bitcoin|btc|ethereum|blockchain)\b', re.I), 'Crypto'),
    (re.compile(r'\b(war|military|troops|ceasefire|strike|bombing|invasion)\b', re.I), 'Military & Defense'),
    (re.compile(r'\b(tariff|trade war|import|export|trade policy)\b', re.I), 'Trade & Tariffs'),
    # Catch-all last (only if nothing else matched)
    (re.compile(r'\b(prediction market|betting market|event contract)\b', re.I), 'Industry News'),
]


# ─── Grading ─────────────────────────────────────────────────────────────────

def compute_outlet_grade(pct_reportable, avg_fragility, total_citations):
    """
    Assign A-F grade to an outlet based on citation quality.

    Factors:
      - pct_reportable: % of citations that were Tier 1 (higher = better)
      - avg_fragility: average fragility score (lower = better)
      - total_citations: minimum threshold for meaningful grade

    Returns grade string (A, B, C, D, F) and numeric score (0-100).
    """
    if total_citations < 3:
        return "N/A", None  # Not enough data

    # Score: weighted blend (0-100, higher = better)
    reportable_score = pct_reportable  # 0-100
    fragility_score = max(0, 100 - avg_fragility)  # Invert: low fragility = high score
    combined = 0.6 * reportable_score + 0.4 * fragility_score

    if combined >= 80:
        return "A", combined
    elif combined >= 60:
        return "B", combined
    elif combined >= 40:
        return "C", combined
    elif combined >= 20:
        return "D", combined
    else:
        return "F", combined


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


def classify_topic(citation):
    """Classify a citation into a topic using keyword patterns.

    Uses a title-first strategy: if the title alone matches a topic, use that.
    Falls back to sentence/context only if the title doesn't match.
    This prevents misclassification from passing mentions in article body
    (e.g., an article about Wealthsimple that mentions 'military' in passing).
    """
    title = citation.get("title", "")

    # First pass: try title only (most topically focused)
    if title:
        for pattern, topic_name in TOPIC_PATTERNS:
            if pattern.search(title):
                return topic_name

    # Second pass: try sentence + context
    body = " ".join(filter(None, [
        citation.get("sentence", ""),
        citation.get("context", ""),
    ]))
    if body:
        for pattern, topic_name in TOPIC_PATTERNS:
            if pattern.search(body):
                return topic_name

    return "Other"


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


def generate_hero_stats(citations, outlets):
    """Top-level summary statistics with 24h and 30d windows."""
    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)
    cutoff_30d = now - timedelta(days=30)

    citations_24h = 0
    citations_30d = 0
    outlets_24h = set()
    outlets_30d = set()

    for c in citations:
        domain = c.get("domain", "unknown")
        if c.get("source_type") == "tv":
            domain = c.get("station", domain)

        dt = _parse_date(c.get("published_date", ""))
        if dt and dt >= cutoff_24h:
            citations_24h += 1
            outlets_24h.add(domain)
        if dt and dt >= cutoff_30d:
            citations_30d += 1
            outlets_30d.add(domain)

    return {
        "total_citations_24h": citations_24h,
        "total_citations_30d": citations_30d,
        "total_outlets_24h": len(outlets_24h),
        "total_outlets_30d": len(outlets_30d),
        "total_outlets": len(outlets),
    }


def domain_to_name(domain):
    """Convert a domain like 'finance.yahoo.com' to a display name like 'Yahoo Finance'."""
    DOMAIN_NAMES = {
        "finance.yahoo.com": "Yahoo Finance",
        "yahoo.com": "Yahoo",
        "nypost.com": "New York Post",
        "bloomberg.com": "Bloomberg",
        "coindesk.com": "CoinDesk",
        "arstechnica.com": "Ars Technica",
        "businessday.co.za": "BusinessDay",
        "cp24.com": "CP24",
        "dailyforex.com": "DailyForex",
        "benzinga.com": "Benzinga",
        "banklesstimes.com": "Bankless Times",
        "theglobeandmail.com": "The Globe and Mail",
        "investinglive.com": "Investing Live",
        "freemalaysiatoday.com": "Free Malaysia Today",
        "rotowire.com": "RotoWire",
        "ibtimes.com.au": "IB Times",
        "lowellsun.com": "Lowell Sun",
        "townhall.com": "Townhall",
        "el-balad.com": "El Balad",
        "thestreet.com": "TheStreet",
        "barrons.com": "Barron's",
        "washingtonpost.com": "Washington Post",
        "nytimes.com": "New York Times",
        "wsj.com": "Wall Street Journal",
        "reuters.com": "Reuters",
        "apnews.com": "AP News",
        "cnn.com": "CNN",
        "foxnews.com": "Fox News",
        "nbcnews.com": "NBC News",
        "cbsnews.com": "CBS News",
        "abcnews.go.com": "ABC News",
        "bbc.com": "BBC",
        "cnbc.com": "CNBC",
        "politico.com": "Politico",
        "thehill.com": "The Hill",
        "axios.com": "Axios",
        "tnp.no": "TNP",
        "kgou.org": "KGOU",
        "fortune.com": "Fortune",
        "marketwatch.com": "MarketWatch",
        # TV stations (from IA TV News integration)
        "cnn": "CNN (TV)",
        "msnbc": "MSNBC (TV)",
        "foxnews": "Fox News (TV)",
        "foxbusiness": "Fox Business (TV)",
        "bbcnews": "BBC News (TV)",
        "cnbc": "CNBC (TV)",
        "bloomberg": "Bloomberg (TV)",
        "cbs(kpix)": "CBS (TV)",
        "abc(kgo)": "ABC (TV)",
    }
    if domain in DOMAIN_NAMES:
        return DOMAIN_NAMES[domain]
    # Auto-generate: strip TLD, capitalize
    parts = domain.replace("www.", "").split(".")
    if len(parts) >= 2:
        name = parts[0]
        # Capitalize first letter, keep rest
        return name[0].upper() + name[1:] if name else domain
    return domain


def prepare_web_citations(citations, limit=MAX_CITATIONS_WEB):
    """
    Flatten citations for web display. Keep only essential fields.
    Returns most recent `limit` citations.
    """
    flat = []
    for c in citations:
        refs = c.get("market_references", [])
        # Get the first matched reference (primary)
        primary_ref = None
        for ref in refs:
            if ref.get("matched_market"):
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
            "market_question": matched.get("question", ""),
            "market_ticker": matched.get("bwr_ticker", ""),
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

    # Step 1: Remove promotional/affiliate content (ads, promo codes, sign-up bonuses)
    non_promo = [c for c in all_citations if not is_promotional(c)]
    promo_removed = len(all_citations) - len(non_promo)
    if promo_removed:
        logger.info(f"Filtered {promo_removed} promotional/affiliate citations")

    # Step 2: Filter out generic platform coverage (op-eds about prediction
    # markets, industry news). Only keep citations that reference markets in the
    # context of a real-world event.
    EXCLUDED_TOPICS = {"Industry News", "Other"}
    citations = [c for c in non_promo if classify_topic(c) not in EXCLUDED_TOPICS]
    topic_removed = len(non_promo) - len(citations)
    logger.info(f"After filtering: {len(citations)} citations ({promo_removed} promo + {topic_removed} non-event removed)")

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now(timezone.utc).isoformat()

    # 1. Outlet leaderboard
    outlets = generate_outlet_leaderboard(citations)
    logger.info(f"Generated leaderboard for {len(outlets)} outlets")

    # 2. Timeline
    timeline = generate_timeline(citations)
    logger.info(f"Generated timeline with {len(timeline)} weeks")

    # 3. Hero stats
    hero = generate_hero_stats(citations, outlets)
    logger.info(f"Hero stats: {hero['total_citations_24h']} citations (24h), {hero['total_citations_30d']} (30d), {hero['total_outlets']} outlets")

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
