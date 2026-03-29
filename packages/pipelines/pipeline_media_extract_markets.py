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
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from thefuzz import fuzz

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json, get_openai_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Configuration ───────────────────────────────────────────────────────────

RAW_FILE = DATA_DIR / "media_citations_raw.json"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
MARKET_MAP_FILE = Path(__file__).resolve().parent.parent.parent / "docs" / "data" / "market_map.json"
OUTPUT_FILE = DATA_DIR / "media_citations_matched.json"

# Confidence thresholds (used for fuzzy pre-filter candidate selection)
FUZZY_CANDIDATE_THRESHOLD = 40  # Low bar for candidate shortlisting
FUZZY_CANDIDATE_LIMIT = 15      # Max candidates to send to LLM

# LLM matching config
LLM_MODEL = "gpt-4o-mini"
LLM_MATCH_ENABLED = True  # Set False to skip LLM and use fuzzy-only

# ─── Regex Patterns for Market Reference Extraction ──────────────────────────

# Matches: "62% on Polymarket", "72 percent on Polymarket", "Polymarket... 62%"
PLATFORM_PROB_PATTERNS = [
    # "X% on Polymarket/Kalshi"
    re.compile(
        r'(\d{1,3})[\s]*(?:%|percent|cents?)\s+(?:on|at|via)\s+(Polymarket|Kalshi|PredictIt)',
        re.IGNORECASE
    ),
    # "Polymarket/Kalshi... X%"  (within 80 chars)
    re.compile(
        r'(Polymarket|Kalshi|PredictIt).{0,80}?(\d{1,3})[\s]*(?:%|percent|cents?)',
        re.IGNORECASE
    ),
    # "Polymarket traders give/show X%"
    re.compile(
        r'(Polymarket|Kalshi|PredictIt)\s+(?:traders?|bettors?|users?)\s+(?:give|show|put|price|see)\w*\s+.{0,40}?(\d{1,3})[\s]*(?:%|percent|cents?)',
        re.IGNORECASE
    ),
    # "trading at X cents on Kalshi"
    re.compile(
        r'trading\s+at\s+(\d{1,3})\s*(?:cents?|%)\s+(?:on|at)\s+(Polymarket|Kalshi|PredictIt)',
        re.IGNORECASE
    ),
    # "odds of X%" near platform name (within 100 chars)
    re.compile(
        r'(Polymarket|Kalshi|PredictIt).{0,100}?odds\s+(?:of|at)\s+(\d{1,3})\s*(?:%|percent)',
        re.IGNORECASE
    ),
    # "0.62 on Polymarket" (decimal probability)
    re.compile(
        r'(0\.\d{1,3})\s+(?:on|at|via)\s+(Polymarket|Kalshi|PredictIt)',
        re.IGNORECASE
    ),
    # "Polymarket gives Trump 0.62" or "gives 62% chance"
    re.compile(
        r'(Polymarket|Kalshi|PredictIt)\s+(?:gives?|shows?|puts?|has).{0,40}?(0\.\d{1,3}|\d{1,3}\s*(?:%|percent))',
        re.IGNORECASE
    ),
]

# Matches generic prediction market references with probability
GENERIC_PROB_PATTERNS = [
    re.compile(
        r'(?:prediction\s+market|betting\s+(?:market|odds?)).{0,60}?(\d{1,3})[\s]*(?:%|percent)',
        re.IGNORECASE
    ),
    re.compile(
        r'(\d{1,3})[\s]*(?:%|percent)\s+(?:chance|probability|likelihood|odds?).{0,40}?(?:prediction\s+market|betting)',
        re.IGNORECASE
    ),
    # "prediction market odds suggest X%"
    re.compile(
        r'(?:prediction\s+market|event\s+contract)\s+odds?\s+(?:suggest|show|indicate|put).{0,40}?(\d{1,3})\s*(?:%|percent)',
        re.IGNORECASE
    ),
]

# Platform detection (without probability)
PLATFORM_MENTION = re.compile(r'\b(Polymarket|Kalshi|PredictIt)\b', re.IGNORECASE)

# URL patterns for direct market ID extraction
POLYMARKET_URL = re.compile(
    r'polymarket\.com/event/([a-z0-9-]+)(?:/([a-z0-9-]+))?', re.IGNORECASE
)
KALSHI_URL = re.compile(
    r'kalshi\.com/markets/([A-Z0-9_-]+)', re.IGNORECASE
)

# ─── Stopwords for keyword extraction ────────────────────────────────────────

STOP_WORDS = frozenset({
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "will", "would", "could", "should", "has", "have", "had", "may", "might",
    "on", "at", "in", "to", "for", "of", "by", "with", "from", "about",
    "that", "this", "it", "its", "and", "or", "but", "not", "nor",
    "as", "if", "than", "more", "most", "very", "so", "up", "out",
    "into", "over", "after", "before", "between", "under", "through",
    "also", "just", "even", "only", "some", "such", "other", "all",
    "can", "each", "which", "their", "there", "then", "these", "those",
    "been", "being", "both", "here", "how", "who", "what", "when", "where",
    "prediction", "market", "markets", "betting", "bettors", "traders",
    "odds", "percent", "probability", "chance", "wager", "bet", "bets",
    "polymarket", "kalshi", "predictit", "platform", "contract", "contracts",
})


# ─── Market Reference Extraction ─────────────────────────────────────────────

def extract_market_references(citation):
    """
    From a citation's text (title + sentence + context), extract market references.

    Returns list of dicts with:
      - raw_text: matched text fragment
      - platform_mentioned: "polymarket" | "kalshi" | "predictit" | "generic"
      - probability_cited: float 0-1 or None
      - subject_text: surrounding text for fuzzy matching
    """
    # Build search text from available fields
    texts = []
    for field in ("sentence", "context", "title", "snippet"):
        val = citation.get(field, "")
        if val:
            texts.append(val)
    search_text = " ".join(texts)

    if not search_text:
        return []

    references = []
    seen_probs = set()  # Avoid duplicate extractions

    # 1. Platform-specific patterns with probability
    for pattern in PLATFORM_PROB_PATTERNS:
        for match in pattern.finditer(search_text):
            groups = match.groups()
            # Determine which group is platform and which is number
            platform = None
            prob_val = None
            for g in groups:
                if not g:
                    continue
                if g.lower() in ("polymarket", "kalshi", "predictit"):
                    platform = g.lower()
                elif g.replace('.', '', 1).replace('%', '').replace(' ', '').replace('percent', '').isdigit():
                    # Handle both "62" and "0.62" and "62%" formats
                    clean = g.replace('%', '').replace('percent', '').strip()
                    try:
                        val = float(clean)
                        if 0 < val < 1:
                            prob_val = val  # Already 0-1 decimal
                        elif 1 <= val <= 99:
                            prob_val = val / 100.0
                    except ValueError:
                        pass

            if platform and prob_val is not None:
                # Round for dedup key
                prob_key = round(prob_val * 100)
                key = (platform, prob_key)
                if key not in seen_probs:
                    seen_probs.add(key)
                    # Get surrounding context for subject matching
                    start = max(0, match.start() - 150)
                    end = min(len(search_text), match.end() + 150)
                    references.append({
                        "raw_text": match.group(0),
                        "platform_mentioned": platform,
                        "probability_cited": prob_val,
                        "subject_text": search_text[start:end],
                        "article_title": citation.get("title", ""),
                        "article_sentence": citation.get("sentence", ""),
                    })

    # 2. Generic prediction market patterns
    for pattern in GENERIC_PROB_PATTERNS:
        for match in pattern.finditer(search_text):
            prob_str = match.group(1)
            prob = int(prob_str)
            if 1 <= prob <= 99:
                key = ("generic", prob)
                if key not in seen_probs:
                    seen_probs.add(key)
                    start = max(0, match.start() - 100)
                    end = min(len(search_text), match.end() + 100)
                    references.append({
                        "raw_text": match.group(0),
                        "platform_mentioned": "generic",
                        "probability_cited": prob / 100.0,
                        "subject_text": search_text[start:end],
                        "article_title": citation.get("title", ""),
                        "article_sentence": citation.get("sentence", ""),
                    })

    # 3. Platform mentions without probability (still track them)
    if not references:
        for match in PLATFORM_MENTION.finditer(search_text):
            platform = match.group(1).lower()
            start = max(0, match.start() - 200)
            end = min(len(search_text), match.end() + 200)
            references.append({
                "raw_text": match.group(0),
                "platform_mentioned": platform,
                "probability_cited": None,
                "subject_text": search_text[start:end],
                "article_title": citation.get("title", ""),
                "article_sentence": citation.get("sentence", ""),
            })
            break  # One mention is enough

    # Prepend article title to subject_text for better fuzzy matching
    title = citation.get("title", "")
    if title:
        for ref in references:
            subj = ref.get("subject_text", "")
            if title not in subj:
                ref["subject_text"] = title + " | " + subj

    return references


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _is_missing(val):
    """Check if a value is effectively missing (None, NaN, 'nan', empty)."""
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    if isinstance(val, str) and val.strip().lower() in ("nan", ""):
        return True
    return False


def _to_float(val):
    """Safely convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        result = float(val)
        return None if math.isnan(result) else result
    except (ValueError, TypeError):
        return None


def _safe_set(flat, key, value):
    """Set key in flat dict only if the existing value is missing."""
    if _is_missing(flat.get(key)):
        flat[key] = value


def flatten_market(raw):
    """
    Flatten a nested enriched market entry into a flat dict.

    The enriched_political_markets.json.gz stores each market as:
      {"original_csv": {...}, "api_data": {"market": {...}, "event": {...}}, "fetch_errors": [...]}

    This merges original_csv with relevant api_data fields into a single flat dict
    that build_market_search_text() and match_reference_to_market() can use.
    """
    # If already flat (has 'question' at top level), return as-is
    if "question" in raw and "original_csv" not in raw:
        return raw

    csv_data = raw.get("original_csv", {}) or {}
    api = raw.get("api_data", {}) or {}
    api_market = api.get("market", {}) or {}
    api_event = api.get("event", {}) or {}

    # Start with all original_csv fields
    flat = dict(csv_data)

    platform = csv_data.get("platform", "")

    if platform == "Kalshi":
        _safe_set(flat, "k_ticker", api_market.get("ticker"))
        _safe_set(flat, "title", api_market.get("title"))
        _safe_set(flat, "k_rules_primary", api_market.get("rules_primary"))
        _safe_set(flat, "k_yes_price", _to_float(api_market.get("last_price_dollars")))
        _safe_set(flat, "status", api_market.get("status"))
        _safe_set(flat, "event_title", api_event.get("title"))
        _safe_set(flat, "k_event_ticker", api_market.get("event_ticker"))
        _safe_set(flat, "k_yes_sub_title", api_market.get("yes_sub_title"))
        _safe_set(flat, "k_no_sub_title", api_market.get("no_sub_title"))
    elif platform == "Polymarket":
        _safe_set(flat, "pm_market_id", str(api_market.get("id", "")))
        _safe_set(flat, "pm_market_slug", api_market.get("slug"))
        _safe_set(flat, "description", api_market.get("description"))
        _safe_set(flat, "title", api_market.get("question"))
        _safe_set(flat, "pm_yes_price", _to_float(api_market.get("lastTradePrice")))
        _safe_set(flat, "pm_event_slug", api_event.get("slug"))
        _safe_set(flat, "event_title", api_event.get("title"))
        _safe_set(flat, "status", "active" if api_market.get("active") else "closed")
        _safe_set(flat, "total_volume", _to_float(api_market.get("volumeNum") or api_market.get("volume")))

    # Ensure question is always populated
    if _is_missing(flat.get("question")):
        flat["question"] = api_market.get("question") or api_market.get("title") or ""

    return flat


def extract_keywords(text):
    """Extract meaningful keywords from text, removing stopwords and short tokens."""
    words = re.findall(r'[A-Za-z]{3,}', text.lower())
    return " ".join(w for w in words if w not in STOP_WORDS)


# ─── Market Matching ─────────────────────────────────────────────────────────

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


def build_market_search_text(market):
    """Build a searchable text string from market fields."""
    parts = []
    for field in ("question", "title", "description", "event_title",
                  "k_rules_primary", "k_yes_sub_title", "k_no_sub_title"):
        val = market.get(field)
        if val and isinstance(val, str) and not _is_missing(val):
            parts.append(val)
    # Include slugs with hyphens converted to spaces for better matching
    for field in ("pm_market_slug", "pm_event_slug"):
        val = market.get(field)
        if val and isinstance(val, str) and not _is_missing(val):
            parts.append(val.replace("-", " "))
    return " ".join(parts)


def build_market_indices(markets):
    """Build lookup indices for URL-based matching."""
    slug_index = {}
    ticker_index = {}

    for i, m in enumerate(markets):
        # Polymarket slug index
        for field in ("pm_market_slug", "pm_event_slug"):
            slug = m.get(field)
            if slug and isinstance(slug, str) and not _is_missing(slug):
                slug_index[slug.lower()] = i

        # Kalshi ticker index
        for field in ("k_ticker", "market_id", "k_event_ticker"):
            ticker = m.get(field)
            if ticker and isinstance(ticker, str) and not _is_missing(ticker):
                ticker_index[ticker.upper()] = i

    return slug_index, ticker_index


def match_by_url(reference, markets, slug_index, ticker_index):
    """
    Try to match a citation reference by extracting market URLs from the text.

    Returns (matched_market, "HIGH", 100) or None if no URL match found.
    """
    subject = reference.get("subject_text", "")
    if not subject:
        return None

    # Try Polymarket URLs
    for match in POLYMARKET_URL.finditer(subject):
        event_slug = match.group(1).lower()
        market_slug = (match.group(2) or "").lower()

        # Try market-level slug first, then event-level
        for slug in (market_slug, event_slug):
            if slug and slug in slug_index:
                idx = slug_index[slug]
                return markets[idx], "HIGH", 100

    # Try Kalshi URLs
    for match in KALSHI_URL.finditer(subject):
        ticker = match.group(1).upper()
        if ticker in ticker_index:
            idx = ticker_index[ticker]
            return markets[idx], "HIGH", 100

    return None


def get_fuzzy_candidates(reference, markets, market_texts, market_keywords):
    """
    Use fuzzy matching to generate a shortlist of candidate markets for LLM matching.

    Returns list of (index, score) tuples sorted by score descending.
    """
    subject = reference.get("subject_text", "")
    if not subject:
        return []

    subject_kw = extract_keywords(subject)
    candidates = []

    for i, (market, text, kw) in enumerate(zip(markets, market_texts, market_keywords)):
        if not text:
            continue

        kw_score = fuzz.token_set_ratio(subject_kw, kw) if kw else 0
        partial_score = fuzz.partial_ratio(subject.lower(), text.lower())
        score = max(kw_score, int(partial_score * 0.9))

        if score >= FUZZY_CANDIDATE_THRESHOLD:
            candidates.append((i, score))

    # Second pass: title-only keywords to catch markets missed by context window
    title_text = reference.get("article_title", "")
    if title_text:
        title_kw = extract_keywords(title_text)
        if title_kw:
            seen = {idx for idx, _ in candidates}
            for i, (market, text, kw) in enumerate(zip(markets, market_texts, market_keywords)):
                if i in seen or not text:
                    continue
                title_score = fuzz.token_set_ratio(title_kw, kw) if kw else 0
                if title_score >= FUZZY_CANDIDATE_THRESHOLD:
                    candidates.append((i, title_score))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:FUZZY_CANDIDATE_LIMIT]


def match_with_llm(reference, candidates, markets, openai_client):
    """
    Use GPT-4o-mini to pick the best matching market from a candidate list.

    Returns (matched_market, confidence, score) or (None, "UNMATCHED", 0)
    """
    subject = reference.get("subject_text", "")
    platform = reference.get("platform_mentioned", "generic")
    prob_cited = reference.get("probability_cited")
    article_title = reference.get("article_title", "")
    article_sentence = reference.get("article_sentence", "")

    # Build candidate descriptions
    candidate_lines = []
    for idx, (market_idx, fuzzy_score) in enumerate(candidates):
        m = markets[market_idx]
        q = m.get("question") or m.get("title") or ""
        p = m.get("platform", "")
        candidate_lines.append(f"  [{idx}] ({p}) {q}")

    candidates_text = "\n".join(candidate_lines)

    prob_info = ""
    if prob_cited is not None:
        prob_info = f"\nThe article cites a probability of {prob_cited:.0%}."

    # Build structured citation context
    context_parts = []
    if article_title:
        context_parts.append(f'ARTICLE HEADLINE: "{article_title}"')
    if article_sentence:
        context_parts.append(f'SENTENCE: "{article_sentence}"')
    context_parts.append(f'SURROUNDING CONTEXT: "{subject}"')
    citation_block = "\n".join(context_parts)

    prompt = f"""A news article mentions a prediction market. Determine which specific market contract the article is referring to.

{citation_block}

Platform mentioned: {platform}{prob_info}

CANDIDATE MARKETS:
{candidates_text}

INSTRUCTIONS:
- Match if the citation discusses a specific real-world event or outcome that one of these market contracts covers.
  Example: "betting on the ouster of Venezuelan President Maduro" matches a "Will Maduro leave office?" market.
- Even if the article is about regulation, insider trading, or industry news, still match if it references a specific event covered by a candidate market.
- Respond with ONLY the number in brackets (e.g. "0" or "3") if you find a match.
- Respond with "NONE" only if:
  (a) The citation discusses prediction markets in general without mentioning any specific event or outcome, OR
  (b) None of the candidate markets cover the event discussed in the citation.
- Only match if the TOPIC of the citation clearly aligns with the market question.

Your answer (number or NONE):"""

    try:
        response = openai_client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=10,
            temperature=0,
        )
        answer = response.choices[0].message.content.strip()

        if answer.upper() == "NONE":
            return None, "UNMATCHED", 0

        # Parse the index
        match_idx = int(answer)
        if 0 <= match_idx < len(candidates):
            market_idx, fuzzy_score = candidates[match_idx]
            return markets[market_idx], "HIGH", 95
        else:
            logger.warning(f"LLM returned out-of-range index: {answer}")
            return None, "UNMATCHED", 0

    except (ValueError, IndexError) as e:
        logger.warning(f"LLM response parse error: {answer!r} -> {e}")
        return None, "UNMATCHED", 0
    except Exception as e:
        logger.warning(f"LLM call failed: {e}")
        return None, "UNMATCHED", 0


def match_reference_to_market(reference, markets, market_texts, market_keywords,
                               slug_index, ticker_index, openai_client=None):
    """
    Match a citation reference to the best Bellwether market.

    Strategy:
    1. URL-based matching (highest confidence, instant)
    2. Fuzzy pre-filter to get candidate shortlist
    3. LLM-based selection from candidates (if enabled)

    Returns (matched_market, confidence, score) or (None, "UNMATCHED", 0)
    """
    # 1. Try URL-based matching first
    url_result = match_by_url(reference, markets, slug_index, ticker_index)
    if url_result:
        return url_result

    subject = reference.get("subject_text", "")
    if not subject:
        return None, "UNMATCHED", 0

    # 2. Get fuzzy candidates
    candidates = get_fuzzy_candidates(reference, markets, market_texts, market_keywords)
    if not candidates:
        return None, "UNMATCHED", 0

    # 3. Use LLM to pick the best match from candidates
    if LLM_MATCH_ENABLED and openai_client:
        return match_with_llm(reference, candidates, markets, openai_client)

    # Fallback: return top fuzzy candidate if score is high enough
    best_idx, best_score = candidates[0]
    if best_score >= 65:
        return markets[best_idx], "MEDIUM", best_score

    return None, "UNMATCHED", best_score


# ─── Main Pipeline ───────────────────────────────────────────────────────────

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
    slug_index, ticker_index = build_market_indices(markets)
    logger.info(f"Built indices: {len(slug_index)} slugs, {len(ticker_index)} tickers")

    # Initialize OpenAI client for LLM matching
    openai_client = None
    if LLM_MATCH_ENABLED:
        try:
            openai_client = get_openai_client()
            logger.info(f"LLM matching enabled (model: {LLM_MODEL})")
        except Exception as e:
            logger.warning(f"OpenAI client init failed, falling back to fuzzy-only: {e}")

    # Process each citation
    matched_count = 0
    unmatched_count = 0
    no_reference_count = 0

    output_citations = []

    for i, citation in enumerate(citations):
        if i % 100 == 0 and i > 0:
            logger.info(f"Progress: {i}/{len(citations)} citations processed")

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
                slug_index, ticker_index, openai_client
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
                }

            matched_refs.append(matched_ref)

        if has_match:
            matched_count += 1
        else:
            unmatched_count += 1

        output_citations.append({
            **citation,
            "market_references": matched_refs,
            "match_status": "MATCHED" if has_match else "UNMATCHED",
        })

    # Summary
    logger.info(f"Results: {matched_count} matched, {unmatched_count} unmatched, {no_reference_count} no reference")

    # Save output
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
    logger.info(f"Saved {len(output_citations)} citations to {OUTPUT_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
