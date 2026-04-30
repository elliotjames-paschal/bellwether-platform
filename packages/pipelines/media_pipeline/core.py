"""
Shared pure/near-pure logic for the media pipeline.

Extracted verbatim from:
  - pipeline_media_extract_markets.py  (extraction, matching, dedup)
  - pipeline_media_calculate_fragility.py  (fragility scoring)
  - generate_media_web_data.py  (topic classification, outlet grading)

No I/O or global state — all file access stays in the calling scripts.
"""

import logging
import math
import re
from datetime import datetime, timedelta, timezone

from thefuzz import fuzz
from unicodedata import normalize as unicode_normalize

logger = logging.getLogger(__name__)

# =============================================================================
# Constants from pipeline_media_extract_markets.py
# =============================================================================

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

# ─── Syndicated Article Deduplication ───────────────────────────────────────

OUTLET_AUTHORITY = {
    "reuters.com": 5, "apnews.com": 5,
    "nytimes.com": 4, "wsj.com": 4, "washingtonpost.com": 4, "bloomberg.com": 4,
    "bbc.com": 3, "cnn.com": 3, "cnbc.com": 3, "politico.com": 3,
    "foxnews.com": 3, "nbcnews.com": 3, "cbsnews.com": 3,
    "thehill.com": 2, "axios.com": 2, "fortune.com": 2, "barrons.com": 2,
}


# =============================================================================
# Constants from pipeline_media_calculate_fragility.py
# =============================================================================

# Fragility score weights
WEIGHT_VOLUME = 0.30
WEIGHT_DEPTH = 0.30
WEIGHT_SPREAD = 0.20
WEIGHT_VOLATILITY = 0.20

# Saturation points for log scaling
VOLUME_SATURATION = 10_000_000  # $10M
DEPTH_SATURATION = 500_000      # $500K cost_to_move_5c

# Tier thresholds (same as generate_monitor_data.py)
TIER1_THRESHOLD = 100_000  # $100K = Reportable
TIER2_THRESHOLD = 10_000   # $10K  = Caution

# Volatility windows (hours)
VOLATILITY_WINDOWS = [1, 6, 24]

# Default fragility for missing data
DEFAULT_FRAGILITY_MISSING = 75


# =============================================================================
# Constants from generate_media_web_data.py
# =============================================================================

# ─── Promotional / Affiliate Detection ───────────────────────────────────────
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

# ─── Topic Patterns ──────────────────────────────────────────────────────────
# Ordered by specificity: first match wins.
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


# =============================================================================
# Helpers from pipeline_media_extract_markets.py
# =============================================================================

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


def normalize_title(title):
    """Normalize a title for deduplication: lowercase, strip punctuation, normalize unicode."""
    if not title:
        return ""
    text = unicode_normalize("NFKD", title.lower())
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# =============================================================================
# Market Reference Extraction (from pipeline_media_extract_markets.py)
# =============================================================================

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


# =============================================================================
# Matching (from pipeline_media_extract_markets.py)
# =============================================================================

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


# ─── Syndicated Article Deduplication ───────────────────────────────────────

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


# =============================================================================
# Fragility calculations (from pipeline_media_calculate_fragility.py)
# =============================================================================

def parse_price_timestamp(ts):
    """Parse a price timestamp to datetime. Handles epoch seconds and ISO strings."""
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(ts, str):
        try:
            # Try ISO format
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            pass
        try:
            # Try epoch string
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        except (ValueError, OSError):
            pass
    return None


def find_price_at_time(series, target_dt):
    """Find the closest price to target_dt. Returns (price, time_delta_hours)."""
    if not series:
        return None, None

    best_price = None
    best_delta = float("inf")

    for dt, price in series:
        delta = abs((dt - target_dt).total_seconds()) / 3600.0
        if delta < best_delta:
            best_delta = delta
            best_price = price

    return best_price, best_delta


def calculate_price_volatility(series, citation_dt, windows=VOLATILITY_WINDOWS):
    """
    Calculate price movement in windows around citation time.

    Returns dict of {window_hours: {delta_before, delta_after, max_swing, price_at_citation}}.
    """
    if not series:
        return None

    price_at_citation, _ = find_price_at_time(series, citation_dt)
    if price_at_citation is None:
        return None

    result = {}
    for window_h in windows:
        window_td = timedelta(hours=window_h)
        before_dt = citation_dt - window_td
        after_dt = citation_dt + window_td

        price_before, _ = find_price_at_time(series, before_dt)
        price_after, _ = find_price_at_time(series, after_dt)

        # Calculate deltas
        delta_before = None
        delta_after = None
        if price_before is not None:
            delta_before = round(price_at_citation - price_before, 4)
        if price_after is not None:
            delta_after = round(price_after - price_at_citation, 4)

        # Max swing: find max price range in the window
        prices_in_window = [
            p for dt, p in series
            if before_dt <= dt <= after_dt
        ]
        max_swing = None
        if prices_in_window:
            max_swing = round(max(prices_in_window) - min(prices_in_window), 4)

        result[f"{window_h}h"] = {
            "price_before": price_before,
            "price_after": price_after,
            "delta_before": delta_before,
            "delta_after": delta_after,
            "max_swing": max_swing,
        }

    result["price_at_citation"] = price_at_citation
    return result


def compute_fragility_score(volume_usd, cost_to_move_5c, spread, volatility_24h):
    """
    Compute composite fragility score (0-100).
    Higher = more fragile.

    Components (each 0-100, then weighted):
      - Volume: log-scaled, saturates at VOLUME_SATURATION
      - Depth: log-scaled cost_to_move_5c, saturates at DEPTH_SATURATION
      - Spread: linear, 0% = 0, 10%+ = 100
      - Volatility: 24h max_swing, 0% = 0, 20%+ = 100
    """
    # Volume component (inverted: low volume = high fragility)
    if volume_usd is not None and volume_usd > 0:
        vol_ratio = math.log10(volume_usd + 1) / math.log10(VOLUME_SATURATION)
        volume_score = max(0, min(100, 100 - vol_ratio * 100))
    else:
        volume_score = 100  # No volume = maximally fragile

    # Depth component (inverted: low depth = high fragility)
    if cost_to_move_5c is not None and cost_to_move_5c > 0:
        depth_ratio = math.log10(cost_to_move_5c + 1) / math.log10(DEPTH_SATURATION)
        depth_score = max(0, min(100, 100 - depth_ratio * 100))
    else:
        depth_score = 100  # No orderbook = maximally fragile

    # Spread component
    if spread is not None:
        spread_score = min(100, abs(spread) * 1000)
    else:
        spread_score = 50  # Unknown spread = moderate

    # Volatility component
    if volatility_24h is not None:
        volatility_score = min(100, abs(volatility_24h) * 500)
    else:
        volatility_score = 50  # Unknown volatility = moderate

    # Weighted composite
    composite = (
        WEIGHT_VOLUME * volume_score +
        WEIGHT_DEPTH * depth_score +
        WEIGHT_SPREAD * spread_score +
        WEIGHT_VOLATILITY * volatility_score
    )

    return {
        "fragility_score": round(composite),
        "components": {
            "volume_score": round(volume_score, 1),
            "depth_score": round(depth_score, 1),
            "spread_score": round(spread_score, 1),
            "volatility_score": round(volatility_score, 1),
        },
    }


def estimate_depth_from_volume(volume_usd):
    """Estimate orderbook depth from total volume when no orderbook data is available.

    Empirical heuristic: liquid markets typically have depth ~5-15% of total volume.
    We use a conservative 5% estimate to avoid overstating depth.
    Returns None if volume is missing/zero.
    """
    if volume_usd is None or volume_usd <= 0:
        return None
    return volume_usd * 0.05


def assign_tier(cost_to_move_5c):
    """Assign reportability tier based on orderbook depth."""
    if cost_to_move_5c is not None and cost_to_move_5c >= TIER1_THRESHOLD:
        return 1, "Reportable"
    elif cost_to_move_5c is not None and cost_to_move_5c >= TIER2_THRESHOLD:
        return 2, "Caution"
    else:
        return 3, "Fragile"


# =============================================================================
# Web data helpers (from generate_media_web_data.py)
# =============================================================================

def _safe_str(val, default=""):
    """Return val as a string, converting NaN/None to default."""
    if val is None:
        return default
    if isinstance(val, float) and math.isnan(val):
        return default
    return str(val) if not isinstance(val, str) else val


def is_promotional(citation):
    """Return True if a citation is promotional/affiliate content, not journalism."""
    text = " ".join(filter(None, [
        citation.get("title", ""),
        citation.get("sentence", ""),
        citation.get("context", ""),
    ]))
    return any(p.search(text) for p in PROMO_PATTERNS)


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
