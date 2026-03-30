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
from unicodedata import normalize as unicode_normalize

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json, get_openai_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Configuration ───────────────────────────────────────────────────────────

RAW_FILE = DATA_DIR / "media_citations_raw.json"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
MARKET_MAP_FILE = Path(__file__).resolve().parent.parent.parent / "docs" / "data" / "market_map.json"
OUTPUT_FILE = DATA_DIR / "media_citations_matched.json"
CROSS_PLATFORM_FILE = DATA_DIR / "cross_platform_reviewed_pairs.json"

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
POLYMARKET_MARKET_URL = re.compile(
    r'polymarket\.com/market/([a-z0-9-]+)', re.IGNORECASE
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

    # 0. Direct URL extraction — scan full text for platform URLs
    seen_urls = set()
    for pattern in (POLYMARKET_URL, POLYMARKET_MARKET_URL, KALSHI_URL):
        for match in pattern.finditer(search_text):
            url_text = match.group(0)
            if url_text in seen_urls:
                continue
            seen_urls.add(url_text)
            platform = "polymarket" if "polymarket" in url_text.lower() else "kalshi"
            start = max(0, match.start() - 200)
            end = min(len(search_text), match.end() + 200)
            references.append({
                "raw_text": url_text,
                "platform_mentioned": platform,
                "probability_cited": None,
                "subject_text": search_text[start:end],
                "article_title": citation.get("title", ""),
                "article_sentence": citation.get("sentence", ""),
                "match_method": "url_extraction",
            })

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
    """Build lookup indices for URL-based matching.

    Returns (slug_index, ticker_index, pm_id_index).
    """
    slug_index = {}
    ticker_index = {}
    pm_id_index = {}

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

        # Polymarket numeric market ID index
        pm_id = m.get("pm_market_id")
        if pm_id and not _is_missing(pm_id):
            pm_id_index[str(pm_id)] = i

    return slug_index, ticker_index, pm_id_index


def match_by_url(reference, markets, slug_index, ticker_index, pm_id_index=None):
    """
    Try to match a citation reference by extracting market URLs from the text.

    Returns (matched_market, "HIGH", 100) or None if no URL match found.
    """
    subject = reference.get("subject_text", "")
    if not subject:
        return None

    # Try Polymarket event/slug URLs
    for match in POLYMARKET_URL.finditer(subject):
        event_slug = match.group(1).lower()
        market_slug = (match.group(2) or "").lower()

        # Try market-level slug first, then event-level
        for slug in (market_slug, event_slug):
            if slug and slug in slug_index:
                idx = slug_index[slug]
                return markets[idx], "HIGH", 100

    # Try Polymarket /market/{ID} URLs
    if pm_id_index:
        for match in POLYMARKET_MARKET_URL.finditer(subject):
            market_id = match.group(1)
            if market_id in pm_id_index:
                idx = pm_id_index[market_id]
                return markets[idx], "HIGH", 100

    # Try Kalshi URLs
    for match in KALSHI_URL.finditer(subject):
        ticker = match.group(1).upper()
        if ticker in ticker_index:
            idx = ticker_index[ticker]
            return markets[idx], "HIGH", 100

    return None


def filter_markets_by_platform(markets, platform_mentioned):
    """Return list of market indices matching the mentioned platform, or None for no filter."""
    if platform_mentioned in ("polymarket", "kalshi"):
        indices = []
        for i, m in enumerate(markets):
            if platform_mentioned == "polymarket" and m.get("pm_market_id"):
                indices.append(i)
            elif platform_mentioned == "kalshi" and not m.get("pm_market_id"):
                indices.append(i)
        return indices if indices else None
    return None


def keyword_prefilter(subject_text, market_texts, candidate_indices=None):
    """Pre-filter markets by requiring all top keywords to be present.

    Extracts the 3-4 longest keywords (>=4 chars) from subject_text.
    Returns filtered list of indices, or falls back to candidate_indices
    if too few matches (<3) or too few keywords (<2).
    """
    words = re.findall(r'[A-Za-z]{4,}', subject_text.lower())
    # Remove stopwords and prediction-market jargon
    filtered = [w for w in words if w not in STOP_WORDS]
    # Take top 4 longest unique keywords
    unique = list(dict.fromkeys(sorted(filtered, key=len, reverse=True)))[:4]

    if len(unique) < 2:
        return candidate_indices

    indices_to_check = candidate_indices if candidate_indices is not None else range(len(market_texts))
    matches = []
    for i in indices_to_check:
        text_lower = market_texts[i].lower() if market_texts[i] else ""
        if all(kw in text_lower for kw in unique):
            matches.append(i)

    if len(matches) < 3:
        return candidate_indices

    return matches


def get_fuzzy_candidates(reference, markets, market_texts, market_keywords,
                         platform_indices=None, search_index=None):
    """
    Use fuzzy matching to generate a shortlist of candidate markets for LLM matching.

    Returns list of (index, score) tuples sorted by score descending.
    """
    subject = reference.get("subject_text", "")
    if not subject:
        return []

    # TF-IDF search when available
    if search_index is not None:
        tfidf_results = search_index.search(subject, top_n=FUZZY_CANDIDATE_LIMIT * 2,
                                            candidate_indices=platform_indices)
        if len(tfidf_results) >= 3:
            # Apply keyword pre-filter on TF-IDF results
            tfidf_indices = [idx for idx, _ in tfidf_results]
            filtered = keyword_prefilter(subject, market_texts, tfidf_indices)
            if filtered is not None and filtered is not tfidf_indices:
                filtered_set = set(filtered)
                tfidf_results = [(idx, s) for idx, s in tfidf_results if idx in filtered_set]
            return tfidf_results[:FUZZY_CANDIDATE_LIMIT]

    # Determine which indices to search
    kw_prefiltered = keyword_prefilter(subject, market_texts, platform_indices)
    if kw_prefiltered is not None:
        search_set = set(kw_prefiltered)
    else:
        search_set = None  # Search all

    subject_kw = extract_keywords(subject)
    candidates = []

    for i, (market, text, kw) in enumerate(zip(markets, market_texts, market_keywords)):
        if not text:
            continue
        if search_set is not None and i not in search_set:
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
                if search_set is not None and i not in search_set:
                    continue
                title_score = fuzz.token_set_ratio(title_kw, kw) if kw else 0
                if title_score >= FUZZY_CANDIDATE_THRESHOLD:
                    candidates.append((i, title_score))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:FUZZY_CANDIDATE_LIMIT]


# ─── TF-IDF Search Index ────────────────────────────────────────────────────

class MarketSearchIndex:
    """TF-IDF vectorized search over market texts, replacing brute-force fuzzy."""

    def __init__(self, market_texts):
        from sklearn.feature_extraction.text import TfidfVectorizer
        self._vectorizer = TfidfVectorizer(
            max_features=20000, stop_words="english",
            ngram_range=(1, 2), sublinear_tf=True,
        )
        self._matrix = self._vectorizer.fit_transform(market_texts)
        self._market_texts = market_texts

    def search(self, query, top_n=30, candidate_indices=None):
        """Return list of (index, score_0_100) tuples sorted by score desc."""
        from sklearn.metrics.pairwise import cosine_similarity
        q_vec = self._vectorizer.transform([query])
        if candidate_indices is not None:
            sub_matrix = self._matrix[candidate_indices]
            sims = cosine_similarity(q_vec, sub_matrix).flatten()
            results = []
            for j, sim in enumerate(sims):
                score = int(sim * 100)
                if score >= FUZZY_CANDIDATE_THRESHOLD:
                    results.append((candidate_indices[j], score))
        else:
            sims = cosine_similarity(q_vec, self._matrix).flatten()
            results = []
            for i, sim in enumerate(sims):
                score = int(sim * 100)
                if score >= FUZZY_CANDIDATE_THRESHOLD:
                    results.append((i, score))
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_n]


# ─── Topic Clusters ─────────────────────────────────────────────────────────

# Import TOPIC_PATTERNS from generate_media_web_data.py
try:
    from generate_media_web_data import TOPIC_PATTERNS
except ImportError:
    TOPIC_PATTERNS = []


def build_topic_clusters(markets, market_texts):
    """Classify each market into a topic, return dict[topic, list[int]]."""
    clusters = {}
    if not TOPIC_PATTERNS:
        return clusters

    for i, text in enumerate(market_texts):
        if not text:
            continue
        topic = "Other"
        for pattern, topic_name in TOPIC_PATTERNS:
            if pattern.search(text):
                topic = topic_name
                break
        clusters.setdefault(topic, []).append(i)

    return clusters


def classify_citation_topic(reference):
    """Classify a citation reference into a topic using TOPIC_PATTERNS."""
    if not TOPIC_PATTERNS:
        return "Other"

    # Try title first, then subject_text
    title = reference.get("article_title", "")
    if title:
        for pattern, topic_name in TOPIC_PATTERNS:
            if pattern.search(title):
                return topic_name

    subject = reference.get("subject_text", "")
    if subject:
        for pattern, topic_name in TOPIC_PATTERNS:
            if pattern.search(subject):
                return topic_name

    return "Other"


# ─── Probability Validation ─────────────────────────────────────────────────

def validate_probability_match(reference, matched_market, confidence, score):
    """Validate match by comparing cited probability to market price.

    Gap >20pp: downgrade HIGH/95 to MEDIUM/70; otherwise subtract 20 from score.
    Returns (matched_market, confidence, score).
    """
    prob_cited = reference.get("probability_cited")
    if prob_cited is None or matched_market is None:
        return matched_market, confidence, score

    platform = reference.get("platform_mentioned", "generic")

    # Get the market's current price on the cited platform
    market_price = None
    if platform == "kalshi":
        market_price = matched_market.get("k_yes_price")
    elif platform == "polymarket":
        market_price = matched_market.get("pm_yes_price")
    else:
        # Try either
        market_price = matched_market.get("k_yes_price") or matched_market.get("pm_yes_price")

    if market_price is None:
        return matched_market, confidence, score

    try:
        market_price = float(market_price)
    except (ValueError, TypeError):
        return matched_market, confidence, score

    gap = abs(prob_cited - market_price)

    if gap > 0.20:
        if confidence == "HIGH" and score >= 95:
            confidence = "MEDIUM"
            score = 70
        else:
            score = max(0, score - 20)

    return matched_market, confidence, score


# ─── Market URL Generation ──────────────────────────────────────────────────

def generate_market_url(matched_market):
    """Generate the correct platform URL for a matched market.

    Kalshi: /markets/{k_ticker}
    Polymarket: prefer /event/{slug}, fallback /market/{pm_market_id}
    """
    if not matched_market:
        return ""

    # Polymarket
    pm_slug = matched_market.get("pm_event_slug") or matched_market.get("pm_market_slug") or ""
    if isinstance(pm_slug, str) and pm_slug and pm_slug not in ("nan", "None"):
        return f"https://polymarket.com/event/{pm_slug}"

    pm_id = matched_market.get("pm_market_id", "")
    if pm_id and not _is_missing(pm_id):
        return f"https://polymarket.com/market/{pm_id}"

    # Kalshi
    k_ticker = matched_market.get("k_ticker") or matched_market.get("market_id", "")
    if k_ticker and isinstance(k_ticker, str) and not _is_missing(k_ticker):
        return f"https://kalshi.com/markets/{k_ticker}"

    return ""


# ─── Cross-Platform Matching ────────────────────────────────────────────────

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


# ─── Syndicated Article Deduplication ───────────────────────────────────────

OUTLET_AUTHORITY = {
    "reuters.com": 5, "apnews.com": 5,
    "nytimes.com": 4, "wsj.com": 4, "washingtonpost.com": 4, "bloomberg.com": 4,
    "bbc.com": 3, "cnn.com": 3, "cnbc.com": 3, "politico.com": 3,
    "foxnews.com": 3, "nbcnews.com": 3, "cbsnews.com": 3,
    "thehill.com": 2, "axios.com": 2, "fortune.com": 2, "barrons.com": 2,
}


def normalize_title(title):
    """Normalize a title for deduplication: lowercase, strip punctuation, normalize unicode."""
    if not title:
        return ""
    text = unicode_normalize("NFKD", title.lower())
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def deduplicate_citations(citations):
    """Deduplicate syndicated articles by normalized title.

    Groups by normalized title, keeps most authoritative outlet, marks others
    with syndicated_from field. Returns (primary_citations, syndication_map)
    where syndication_map maps citation index -> primary citation index.
    """
    if not citations:
        return citations, {}

    # Group by normalized title
    groups = {}
    for i, c in enumerate(citations):
        norm = normalize_title(c.get("title", ""))
        if not norm or len(norm) < 20:
            continue
        groups.setdefault(norm, []).append(i)

    syndication_map = {}
    for norm_title, indices in groups.items():
        if len(indices) < 2:
            continue

        # Find most authoritative
        def authority(idx):
            domain = citations[idx].get("domain", "")
            return OUTLET_AUTHORITY.get(domain, 1)

        indices.sort(key=authority, reverse=True)
        primary_idx = indices[0]
        primary_domain = citations[primary_idx].get("domain", "")

        for idx in indices[1:]:
            syndication_map[idx] = primary_idx
            citations[idx]["syndicated_from"] = primary_domain

    return citations, syndication_map


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
                               slug_index, ticker_index, openai_client=None,
                               pm_id_index=None, search_index=None,
                               topic_clusters=None, cross_platform_lookup=None):
    """
    Match a citation reference to the best Bellwether market.

    Strategy:
    1. URL-based matching (highest confidence, instant)
    2. Pre-filter by topic + platform, keyword pre-filter
    3. TF-IDF / fuzzy candidate search
    4. LLM-based selection from candidates (if enabled)
    5. Probability validation (post-match)
    6. URL generation + cross-platform lookup (post-match enrichment)

    Returns (matched_market, confidence, score) or (None, "UNMATCHED", 0)
    """
    # 1. Try URL-based matching first
    url_result = match_by_url(reference, markets, slug_index, ticker_index, pm_id_index)
    if url_result:
        matched_market, confidence, score = url_result
        return matched_market, confidence, score

    subject = reference.get("subject_text", "")
    if not subject:
        return None, "UNMATCHED", 0

    # 2. Build pre-filter indices: topic ∩ platform
    platform_mentioned = reference.get("platform_mentioned", "generic")
    platform_indices = filter_markets_by_platform(markets, platform_mentioned)

    if topic_clusters:
        citation_topic = classify_citation_topic(reference)
        topic_indices = topic_clusters.get(citation_topic)
        if topic_indices and len(topic_indices) >= 10 and citation_topic not in ("Other", "Industry News"):
            if platform_indices is not None:
                # Intersect topic and platform indices
                topic_set = set(topic_indices)
                platform_indices = [i for i in platform_indices if i in topic_set]
                if not platform_indices:
                    platform_indices = None  # Fall back to all
            else:
                platform_indices = topic_indices

    # 3. Get candidates (TF-IDF or fuzzy)
    candidates = get_fuzzy_candidates(reference, markets, market_texts, market_keywords,
                                      platform_indices=platform_indices,
                                      search_index=search_index)
    if not candidates:
        return None, "UNMATCHED", 0

    # 4. Use LLM to pick the best match from candidates
    if LLM_MATCH_ENABLED and openai_client:
        market, confidence, score = match_with_llm(reference, candidates, markets, openai_client)
    else:
        # Fallback: return top fuzzy candidate if score is high enough
        best_idx, best_score = candidates[0]
        if best_score >= 65:
            market, confidence, score = markets[best_idx], "MEDIUM", best_score
        else:
            return None, "UNMATCHED", best_score

    if not market:
        return None, "UNMATCHED", 0

    # 5. Probability validation
    market, confidence, score = validate_probability_match(
        reference, market, confidence, score
    )

    return market, confidence, score


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

    # Process each citation
    matched_count = 0
    unmatched_count = 0
    no_reference_count = 0

    output_citations = []

    # Track primary citation outputs for syndication inheritance
    primary_outputs = {}

    for i, citation in enumerate(citations):
        if i % 100 == 0 and i > 0:
            logger.info(f"Progress: {i}/{len(citations)} citations processed")

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
