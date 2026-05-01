#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Discover Prediction Market Citations in Media
================================================================================

Searches multiple sources for news articles that mention prediction markets:
  1. GDELT Context 2.0 / DOC 2.0 APIs (free, no auth)
  2. NewsAPI.org (requires NEWSAPI_KEY, free tier = 100 req/day)
  3. The Guardian Content API (requires GUARDIAN_API_KEY, free tier = 5,000 req/day)
  4. Internet Archive TV News Archive (free, no auth)

Outputs: data/media_citations_raw.json
State:   data/media_pipeline_state.json

GDELT APIs are free, no authentication required.
Rate limiting: minimum 5 seconds between requests (GDELT enforces this).
================================================================================
"""

import argparse
import json
import sys
import time
import logging
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from urllib.parse import urlparse

import os

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, BASE_DIR, atomic_write_json
from media_pipeline.schema import compute_url_hash

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Configuration ───────────────────────────────────────────────────────────

STATE_FILE = DATA_DIR / "media_pipeline_state.json"
OUTPUT_FILE = DATA_DIR / "media_citations_raw.json"

# GDELT API base URLs
GDELT_CONTEXT_URL = "https://api.gdeltproject.org/api/v2/context/context"
GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_TV_URL = "https://api.gdeltproject.org/api/v2/tv/tv"
GDELT_LOWERTHIRD_URL = "https://api.gdeltproject.org/api/v2/tvlowerthird/tvlowerthird"

# ─── NewsAPI.org Configuration ────────────────────────────────────────────────
# Free tier: 100 requests/day, articles up to 30 days old
# Sign up at https://newsapi.org/ and set NEWSAPI_KEY env var or add to .env
NEWSAPI_URL = "https://newsapi.org/v2/everything"
NEWSAPI_PAGE_SIZE = 100  # Max per request

# NewsAPI queries — richer syntax than GDELT, supports OR and quoted phrases.
# These are designed to catch major outlets that say "betting odds" or
# "event contracts" without naming a specific platform.
NEWSAPI_QUERIES = [
    # Platform-specific (same as GDELT Tier 1)
    "Polymarket",
    "Kalshi",
    # Broader phrases that major outlets use
    '"prediction market"',
    '"prediction markets"',
    '"betting odds" AND (election OR political OR president OR congress)',
    '"event contracts" AND (political OR election)',
    '"betting market" AND (election OR political OR trump OR president)',
]

# ─── The Guardian Content API Configuration ──────────────────────────────────
# Free tier: 12 calls/sec, 5,000 calls/day
# Register at https://open-platform.theguardian.com/access/
GUARDIAN_API_URL = "https://content.guardianapis.com/search"
GUARDIAN_PAGE_SIZE = 50

GUARDIAN_QUERIES = [
    "Polymarket",
    "Kalshi",
    '"prediction market"',
    '"prediction markets"',
    '"betting odds" AND (election OR political)',
]


# ─── Media Cloud Configuration ───────────────────────────────────────────────
# Academic search API indexing US national online news.
# API key: https://search.mediacloud.org/  (free academic registration)
# Uses collection_ids=[34412234] (US mainstream national sources).
MEDIACLOUD_COLLECTION_IDS = [34412234]

# Tier 1 — broad mentions (metadata only, no text fetch)
MEDIACLOUD_MENTION_QUERY = (
    "(Polymarket OR Kalshi)"
    " NOT (promo OR code OR bonus OR picks"
    " OR NBA OR NFL OR NHL OR MLB OR PGA"
    " OR fantasy OR DraftKings OR FanDuel"
    ' OR "betting lines")'
)

# Tier 2 — probability-language citations (text fetch + sentence extraction)
MEDIACLOUD_CITATION_QUERY = (
    "(Polymarket OR Kalshi) AND (chance OR probability OR odds)"
    " NOT (promo OR code OR bonus OR picks"
    " OR NBA OR NFL OR NHL OR MLB OR PGA"
    " OR fantasy OR DraftKings OR FanDuel"
    ' OR "betting lines")'
)

# Max articles per Media Cloud query page
MEDIACLOUD_PAGE_SIZE = 1000

# Keywords for sentence extraction from fetched article text
MEDIACLOUD_SENTENCE_KEYWORDS = [
    "Polymarket", "Kalshi", "prediction market", "prediction markets",
    "betting market", "betting odds", "event contract",
    "percent", "%", "probability", "odds", "chance",
]

def _get_newsapi_key():
    """Get NewsAPI key from environment or .env file. Returns None if not configured."""
    key = os.environ.get("NEWSAPI_KEY")
    if key:
        return key
    env_file = BASE_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("NEWSAPI_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def _get_guardian_key():
    """Get Guardian API key from environment or .env file. Returns None if not configured."""
    key = os.environ.get("GUARDIAN_API_KEY")
    if key:
        return key
    env_file = BASE_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("GUARDIAN_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def _get_mediacloud_key():
    """Get Media Cloud API key from environment or .env file. Returns None if not configured."""
    key = os.environ.get("MEDIACLOUD_API_KEY")
    if key:
        return key
    env_file = BASE_DIR / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("MEDIACLOUD_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


# Internet Archive TV News API (replacement for GDELT TV which is offline since Oct 2024)
IA_ADVANCED_SEARCH_URL = "https://archive.org/advancedsearch.php"
IA_DOWNLOAD_URL = "https://archive.org/download"
IA_METADATA_URL = "https://archive.org/metadata"
IA_MAX_RESULTS = 50  # Per keyword query

# Search keyword tiers
# Tier 1: Platform-specific (high precision)
TIER1_KEYWORDS = [
    "Polymarket",
    "Kalshi",
    "PredictIt",
]

# Tier 2: Generic phrases with qualifiers (to avoid sports betting noise)
TIER2_KEYWORDS = [
    '"prediction market" election',
    '"prediction market" political',
    '"prediction markets" election',
    '"prediction markets" political',
    '"betting market" political',
]

# TV stations to search
TV_STATIONS = [
    "CNN", "MSNBC", "FOXNEWS", "BBCNEWS",
    "CNBC", "BLOOMBERG",
]

# Max records per GDELT query (Context API limit is 200, DOC API is 250)
MAX_RECORDS_CONTEXT = 200
MAX_RECORDS_DOC = 250

# Rate limiting — GDELT enforces "one request every 5 seconds"
REQUEST_DELAY_SEC = 6.0
MAX_RETRIES = 3
BACKOFF_BASE_SEC = 10.0

# Prune citations older than this to keep file sizes manageable
RETENTION_DAYS = 90

# ─── GDELT API Helpers ──────────────────────────────────────────────────────

_last_request_time = 0.0


def gdelt_request(url, params, retries=MAX_RETRIES):
    """Make a rate-limited request to GDELT with exponential backoff."""
    global _last_request_time

    for attempt in range(retries):
        # Enforce minimum delay between requests
        elapsed = time.time() - _last_request_time
        if elapsed < REQUEST_DELAY_SEC:
            time.sleep(REQUEST_DELAY_SEC - elapsed)

        try:
            _last_request_time = time.time()
            resp = requests.get(url, params=params, timeout=45)

            if resp.status_code == 200:
                text = resp.text.strip()
                # GDELT returns error messages as plain text with 200 status
                if not text or text == "{}":
                    logger.debug(f"GDELT returned empty response for query={params.get('query','?')}")
                    return None
                if text.startswith("Invalid") or text.startswith("Please limit"):
                    logger.warning(f"GDELT error (200): {text[:150]}")
                    if "limit" in text.lower():
                        wait = BACKOFF_BASE_SEC * (2 ** attempt)
                        logger.info(f"  Rate limited, waiting {wait:.0f}s before retry...")
                        time.sleep(wait)
                        continue
                    return None
                try:
                    return resp.json()
                except json.JSONDecodeError:
                    logger.warning(f"GDELT returned non-JSON: {text[:150]}")
                    return None

            if resp.status_code == 429:
                wait = BACKOFF_BASE_SEC * (2 ** attempt)
                logger.warning(f"GDELT 429 rate limited. Waiting {wait:.0f}s...")
                time.sleep(wait)
                continue

            if resp.status_code in (500, 502, 503):
                wait = BACKOFF_BASE_SEC * (2 ** attempt)
                logger.warning(f"GDELT {resp.status_code} server error, retrying in {wait:.0f}s...")
                time.sleep(wait)
                continue

            logger.warning(f"GDELT returned {resp.status_code}: {resp.text[:200]}")
            return None

        except requests.exceptions.Timeout:
            wait = BACKOFF_BASE_SEC * (2 ** attempt)
            logger.warning(f"GDELT request timed out (attempt {attempt+1}/{retries}), waiting {wait:.0f}s...")
            time.sleep(wait)
        except requests.exceptions.ConnectionError as e:
            wait = BACKOFF_BASE_SEC * (2 ** attempt)
            logger.warning(f"GDELT connection error (attempt {attempt+1}/{retries}): {e.__class__.__name__}, waiting {wait:.0f}s...")
            time.sleep(wait)
        except requests.exceptions.RequestException as e:
            logger.error(f"GDELT request failed: {e}")
            return None

    logger.warning(f"GDELT request failed after {retries} retries for query={params.get('query','?')}")
    return None


def parse_gdelt_date(s):
    """Parse GDELT seendate string to datetime."""
    if not s:
        return None
    s = s.replace("T", "").replace("Z", "")
    try:
        if len(s) >= 14:
            return datetime.strptime(s[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        elif len(s) >= 12:
            return datetime.strptime(s[:12], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
        elif len(s) >= 8:
            return datetime.strptime(s[:8], "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    return None


def make_citation_id(url, source_type="article", discovery_source="gdelt"):
    """Generate a stable ID for a citation."""
    h = hashlib.sha256(url.encode()).hexdigest()[:12]
    prefix = discovery_source if discovery_source != "gdelt" else "gdelt"
    return f"{prefix}_{source_type}_{h}"


def timespan_for_days(days):
    """Convert days to GDELT timespan parameter string."""
    if days <= 1:
        return "24hours"
    elif days <= 7:
        return f"{days * 24}hours"
    elif days <= 31:
        return f"{days}days"
    else:
        return "3months"  # GDELT max


# ─── Search Functions ────────────────────────────────────────────────────────

def search_context_api(keyword, timespan="3months"):
    """
    Search GDELT Context 2.0 API for sentence-level matches.
    Returns list of citation dicts with sentence + context fields.

    Context API is the primary source — it returns the exact sentence
    mentioning the market, which is critical for market extraction.
    Uses `timespan` param (not STARTDATETIME which GDELT often rejects).
    """
    params = {
        "query": keyword,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": MAX_RECORDS_CONTEXT,
        "timespan": timespan,
    }

    data = gdelt_request(GDELT_CONTEXT_URL, params)
    if not data or "articles" not in data:
        return []

    articles = []
    for art in data["articles"]:
        articles.append({
            "url": art.get("url", ""),
            "title": (art.get("title") or "").strip(),
            "seendate": art.get("seendate", ""),
            "domain": art.get("domain", ""),
            "language": art.get("language", ""),
            "sourcecountry": art.get("sourcecountry", ""),
            "socialimage": art.get("socialimage", ""),
            "sentence": art.get("sentence", ""),
            "context": art.get("context", ""),
            "is_quote": art.get("isquote", 0),
            "source_type": "article",
            "search_keyword": keyword,
            "discovery_source": "gdelt_context",
        })

    return articles


def search_doc_api(keyword, timespan="3months"):
    """
    Search GDELT DOC 2.0 API for articles matching keyword.
    Returns list of article dicts (no sentence data).
    Used as a supplement to Context API for broader coverage.
    """
    params = {
        "query": keyword,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": MAX_RECORDS_DOC,
        "timespan": timespan,
    }

    data = gdelt_request(GDELT_DOC_URL, params)
    if not data or "articles" not in data:
        return []

    articles = []
    for art in data["articles"]:
        articles.append({
            "url": art.get("url", ""),
            "title": (art.get("title") or "").strip(),
            "seendate": art.get("seendate", ""),
            "domain": art.get("domain", ""),
            "language": art.get("language", ""),
            "sourcecountry": art.get("sourcecountry", ""),
            "socialimage": art.get("socialimage", ""),
            "source_type": "article",
            "search_keyword": keyword,
            "discovery_source": "gdelt_doc",
        })

    return articles


def tv_timespan_to_dates(timespan):
    """Convert a timespan string to STARTDATETIME/ENDDATETIME for TV API.
    TV API doesn't support the `timespan` param — it needs explicit dates
    in YYYYMMDDHHMMSS format.
    """
    now = datetime.now(timezone.utc)
    end = now.strftime("%Y%m%d%H%M%S")

    if "hours" in timespan:
        hours = int(timespan.replace("hours", ""))
        start_dt = now - timedelta(hours=hours)
    elif "days" in timespan:
        days = int(timespan.replace("days", ""))
        start_dt = now - timedelta(days=days)
    elif timespan == "3months":
        start_dt = now - timedelta(days=90)
    else:
        start_dt = now - timedelta(days=90)

    start = start_dt.strftime("%Y%m%d%H%M%S")
    return start, end


def search_tv_api(keyword, timespan="3months"):
    """
    Search GDELT TV 2.0 API across configured stations.
    Returns list of TV clip dicts.
    TV API uses STARTDATETIME/ENDDATETIME instead of timespan.
    """
    all_clips = []
    start_date, end_date = tv_timespan_to_dates(timespan)

    for station in TV_STATIONS:
        query = f"{keyword} station:{station}"
        params = {
            "query": query,
            "mode": "clipgallery",
            "format": "json",
            "datanorm": "perc",
            "startdatetime": start_date,
            "enddatetime": end_date,
        }

        data = gdelt_request(GDELT_TV_URL, params)
        if not data or "clips" not in data:
            continue

        for clip in data["clips"]:
            all_clips.append({
                "url": clip.get("preview_url", clip.get("url", "")),
                "title": clip.get("show", ""),
                "seendate": clip.get("date", ""),
                "station": clip.get("station", station),
                "show": clip.get("show", ""),
                "snippet": clip.get("snippet", ""),
                "preview_url": clip.get("preview_url", ""),
                "source_type": "tv",
                "search_keyword": keyword,
                "domain": station.lower(),
                "discovery_source": "gdelt_tv",
            })

    return all_clips


def search_lowerthird_api(keyword, timespan="3months"):
    """
    Search GDELT TV LowerThird (chyron/on-screen text) API.
    Covers CNN, MSNBC, FOXNEWSW, BBCNEWS only.
    No station filter needed — API covers all 4 stations.
    Returns list of TV clip dicts.
    """
    start_date, end_date = tv_timespan_to_dates(timespan)
    params = {
        "query": keyword,
        "mode": "clipgallery",
        "format": "json",
        "startdatetime": start_date,
        "enddatetime": end_date,
    }

    data = gdelt_request(GDELT_LOWERTHIRD_URL, params)
    if not data or "clips" not in data:
        return []

    clips = []
    for clip in data["clips"]:
        clips.append({
            "url": clip.get("preview_url", clip.get("url", "")),
            "title": clip.get("show", ""),
            "seendate": clip.get("date", ""),
            "station": clip.get("station", ""),
            "show": clip.get("show", ""),
            "snippet": clip.get("snippet", ""),
            "preview_url": clip.get("preview_url", ""),
            "source_type": "tv",
            "search_keyword": keyword,
            "domain": clip.get("station", "").lower(),
            "discovery_source": "gdelt_lowerthird",
        })

    return clips


# ─── NewsAPI.org Search ───────────────────────────────────────────────────────

def search_newsapi(query, from_date, to_date, api_key):
    """
    Search NewsAPI.org for articles matching query.

    Returns list of citation dicts in the same format as GDELT results.
    NewsAPI free tier: max 30 days back, 100 results per page, 100 req/day.
    """
    params = {
        "q": query,
        "from": from_date,
        "to": to_date,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": NEWSAPI_PAGE_SIZE,
        "apiKey": api_key,
    }

    try:
        resp = requests.get(NEWSAPI_URL, params=params, timeout=30)
        if resp.status_code == 401:
            logger.warning("NewsAPI: invalid API key")
            return []
        if resp.status_code == 429:
            logger.warning("NewsAPI: rate limit reached (100 req/day on free tier)")
            return []
        if resp.status_code != 200:
            logger.warning(f"NewsAPI returned {resp.status_code}: {resp.text[:200]}")
            return []

        data = resp.json()
        if data.get("status") != "ok":
            logger.warning(f"NewsAPI error: {data.get('message', 'unknown')}")
            return []

    except (requests.RequestException, ValueError) as e:
        logger.warning(f"NewsAPI request failed: {e}")
        return []

    articles = []
    for art in data.get("articles", []):
        url = art.get("url", "")
        if not url:
            continue

        # Extract domain from source or URL
        source_name = art.get("source", {}).get("name", "")
        domain = art.get("source", {}).get("id", "")
        if not domain:
            # Derive domain from URL
            try:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                domain = parsed.netloc.replace("www.", "")
            except Exception:
                domain = source_name.lower().replace(" ", "")

        # NewsAPI gives us title + description — use description as the sentence
        # since it often contains the relevant quote/context
        title = (art.get("title") or "").strip()
        description = (art.get("description") or "").strip()
        content_snippet = (art.get("content") or "").strip()

        # Use the most informative text as sentence context
        sentence = description or content_snippet or ""

        # Parse published date to GDELT-compatible seendate format
        pub_at = art.get("publishedAt", "")
        seendate = pub_at.replace("-", "").replace(":", "").replace("T", "").replace("Z", "")

        articles.append({
            "url": url,
            "title": title,
            "seendate": seendate,
            "domain": domain,
            "language": "ENGLISH",
            "sourcecountry": "",
            "socialimage": art.get("urlToImage", ""),
            "sentence": sentence[:500] if sentence else "",
            "context": content_snippet[:1000] if content_snippet else "",
            "source_type": "article",
            "search_keyword": query,
            "discovery_source": "newsapi",
        })

    return articles


# ─── The Guardian Content API Search ─────────────────────────────────────────

def search_guardian(query, from_date, to_date, api_key):
    """
    Search The Guardian Content API for articles matching query.

    Returns list of citation dicts in the same format as other sources.
    Free tier: 12 calls/sec, 5,000 calls/day.  Full article body text included.
    """
    params = {
        "q": query,
        "from-date": from_date,
        "to-date": to_date,
        "page-size": GUARDIAN_PAGE_SIZE,
        "show-fields": "trailText,bodyText,headline,byline",
        "order-by": "newest",
        "api-key": api_key,
    }

    try:
        resp = requests.get(GUARDIAN_API_URL, params=params, timeout=30)
        if resp.status_code == 401:
            logger.warning("Guardian API: invalid API key")
            return []
        if resp.status_code == 429:
            logger.warning("Guardian API: rate limit reached")
            return []
        if resp.status_code != 200:
            logger.warning(f"Guardian API returned {resp.status_code}: {resp.text[:200]}")
            return []

        data = resp.json()
        results = data.get("response", {}).get("results", [])

    except (requests.RequestException, ValueError) as e:
        logger.warning(f"Guardian API request failed: {e}")
        return []

    articles = []
    for art in results:
        url = art.get("webUrl", "")
        if not url:
            continue

        fields = art.get("fields", {})
        title = (art.get("webTitle") or "").strip()
        trail_text = (fields.get("trailText") or "").strip()
        body_text = (fields.get("bodyText") or "").strip()

        # Parse publication date to GDELT-compatible seendate format
        pub_date = art.get("webPublicationDate", "")
        seendate = pub_date.replace("-", "").replace(":", "").replace("T", "").replace("Z", "")

        articles.append({
            "url": url,
            "title": title,
            "seendate": seendate,
            "domain": "theguardian.com",
            "language": "ENGLISH",
            "sourcecountry": "",
            "socialimage": "",
            "sentence": trail_text[:500] if trail_text else "",
            "context": body_text[:1000] if body_text else "",
            "source_type": "article",
            "search_keyword": query,
            "discovery_source": "guardian",
        })

    return articles


# ─── Media Cloud Search ──────────────────────────────────────────────────────

MEDIACLOUD_API_URL = "https://search.mediacloud.org/api/search/story-list"


def _mediacloud_search(query, from_date, to_date, api_key, collection_ids=None):
    """Run a single Media Cloud search query, paginating to completion.

    Calls the Media Cloud REST API directly (no SDK dependency).
    Returns raw list of story dicts from the API.
    """
    all_stories = []
    pagination_token = None
    page_num = 0
    rate_limit_retries = 0
    MAX_RATE_LIMIT_RETRIES = 5

    while True:
        params = {
            "q": query,
            "start_date": from_date,
            "end_date": to_date,
            "page_size": MEDIACLOUD_PAGE_SIZE,
            "collection_ids[]": collection_ids or MEDIACLOUD_COLLECTION_IDS,
        }
        if pagination_token:
            params["pagination_token"] = pagination_token

        try:
            resp = requests.get(
                MEDIACLOUD_API_URL,
                params=params,
                headers={"Authorization": f"Token {api_key}"},
                timeout=60,
            )
            if resp.status_code == 401:
                logger.warning("Media Cloud API: invalid API key")
                break
            if resp.status_code == 429:
                rate_limit_retries += 1
                if rate_limit_retries > MAX_RATE_LIMIT_RETRIES:
                    logger.warning(f"Media Cloud API: rate limited {MAX_RATE_LIMIT_RETRIES} times, stopping pagination with {len(all_stories)} stories collected")
                    break
                logger.warning(f"Media Cloud API: rate limited (attempt {rate_limit_retries}/{MAX_RATE_LIMIT_RETRIES}), waiting 60s...")
                time.sleep(60)
                continue
            if resp.status_code != 200:
                logger.warning(f"Media Cloud API returned {resp.status_code}: {resp.text[:200]}")
                break

            try:
                data = resp.json()
            except ValueError:
                logger.warning("Media Cloud API returned non-JSON response")
                break
        except requests.RequestException as e:
            logger.warning(f"Media Cloud API error: {e}")
            break

        # Reset rate limit counter on successful request
        rate_limit_retries = 0

        stories = data.get("stories", [])
        if not stories:
            break

        all_stories.extend(stories)
        page_num += 1
        logger.info(f"  Media Cloud page {page_num}: +{len(stories)} stories (total: {len(all_stories)})")

        pagination_token = data.get("pagination_token")
        if not pagination_token:
            break

        # Rate limit: API allows 2 requests/minute on some endpoints
        time.sleep(31)

    return all_stories


def _fetch_article_text(url, timeout=20):
    """Fetch and extract article body text using trafilatura.

    Returns extracted text or empty string on failure.
    """
    try:
        import trafilatura
    except ImportError:
        logger.error("trafilatura package not installed — run: pip install trafilatura>=1.6.0")
        return ""

    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return ""
        text = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
        return text or ""
    except Exception as e:
        logger.debug(f"trafilatura failed for {url}: {e}")
        return ""


def search_mediacloud_mentions(query, from_date, to_date, api_key):
    """Tier 1 — broad mention search.  Metadata only, no text fetch.

    Returns list of citation dicts (same shape as other sources).
    """
    stories = _mediacloud_search(query, from_date, to_date, api_key)
    if not stories:
        return []

    articles = []
    for story in stories:
        url = story.get("url", "")
        if not url:
            continue

        title = (story.get("title") or "").strip()
        pub_date = story.get("publish_date", "")

        # Derive domain from URL
        try:
            domain = urlparse(url).netloc.replace("www.", "")
        except Exception:
            domain = ""

        # Convert publish_date to GDELT-compatible seendate format
        seendate = pub_date.replace("-", "").replace(":", "").replace("T", "").replace("Z", "").replace(" ", "")

        articles.append({
            "url": url,
            "title": title,
            "seendate": seendate[:14] if seendate else "",
            "domain": domain,
            "language": story.get("language", "en"),
            "sourcecountry": "",
            "socialimage": "",
            "sentence": "",
            "context": "",
            "source_type": "article",
            "search_keyword": "mediacloud_mention",
            "discovery_source": "mediacloud_mention",
        })

    return articles


def search_mediacloud_citations(query, from_date, to_date, api_key,
                                existing_urls=None):
    """Tier 2 — probability-language citations with text fetch.

    Fetches full article text via trafilatura and extracts keyword sentences
    for downstream market matching.

    Args:
        existing_urls: Set of URLs already discovered by other sources (for dedup).

    Returns list of citation dicts with sentence/context populated.
    """
    from media_pipeline.core import extract_keyword_sentences

    stories = _mediacloud_search(query, from_date, to_date, api_key)
    if not stories:
        return []

    existing_urls = existing_urls or set()
    articles = []
    fetch_count = 0

    for story in stories:
        url = story.get("url", "")
        if not url or url in existing_urls:
            continue

        title = (story.get("title") or "").strip()
        pub_date = story.get("publish_date", "")

        try:
            domain = urlparse(url).netloc.replace("www.", "")
        except Exception:
            domain = ""

        seendate = pub_date.replace("-", "").replace(":", "").replace("T", "").replace("Z", "").replace(" ", "")

        sentence = ""
        context = ""

        # Fetch full text and extract keyword sentences
        text = _fetch_article_text(url)
        if text:
            fetch_count += 1
            matches = extract_keyword_sentences(
                text, MEDIACLOUD_SENTENCE_KEYWORDS, context_n=2
            )
            if matches:
                # Use the first match that contains a probability keyword
                prob_match = None
                for m in matches:
                    sent_lower = m["sentence"].lower()
                    if any(k in sent_lower for k in ("%", "percent", "probability", "odds", "chance")):
                        prob_match = m
                        break
                best = prob_match or matches[0]
                sentence = best["sentence"][:500]
                context = (best["before"] + " " + best["sentence"] + " " + best["after"]).strip()[:1000]

        articles.append({
            "url": url,
            "title": title,
            "seendate": seendate[:14] if seendate else "",
            "domain": domain,
            "language": story.get("language", "en"),
            "sourcecountry": "",
            "socialimage": "",
            "sentence": sentence,
            "context": context,
            "source_type": "article",
            "search_keyword": "mediacloud_citation",
            "discovery_source": "mediacloud_citation",
        })

    logger.info(f"  Fetched text for {fetch_count}/{len(stories)} Tier 2 articles")
    return articles


# ─── Internet Archive TV News Search ─────────────────────────────────────────

# Map IA identifier prefixes to station names
_IA_STATION_MAP = {
    "CNNW": "CNN", "CNN": "CNN",
    "MSNBCW": "MSNBC", "MSNBC": "MSNBC",
    "FOXNEWSW": "Fox News", "FBC": "Fox Business",
    "BBCNEWS": "BBC News", "CNBC": "CNBC",
    "KPIX": "CBS (KPIX)", "KGO": "ABC (KGO)",
    "BLOOMBERG": "Bloomberg",
}


def _ia_identifier_to_station(identifier):
    """Extract station name from IA identifier like CNNW_20260325_030000_..."""
    prefix = identifier.split("_")[0] if "_" in identifier else identifier
    return _IA_STATION_MAP.get(prefix, prefix)


def _ia_identifier_to_show(identifier):
    """Extract show name from IA identifier like CNNW_20260325_030000_Laura_Coates_Live."""
    parts = identifier.split("_")
    if len(parts) > 3:
        return " ".join(parts[3:]).replace("_", " ")
    return ""


def _parse_srt_text(srt_content):
    """Extract plain text from SRT subtitle content, joining into sentences."""
    import re
    # Remove SRT sequence numbers, timestamps, and blank lines
    lines = []
    for line in srt_content.split("\n"):
        line = line.strip()
        if not line:
            continue
        if re.match(r"^\d+$", line):  # Sequence number
            continue
        if re.match(r"\d{2}:\d{2}:\d{2}", line):  # Timestamp line
            continue
        # Remove HTML-like tags from captions
        line = re.sub(r"<[^>]+>", "", line)
        if line:
            lines.append(line)
    return " ".join(lines)


def _extract_keyword_context(full_text, keyword, context_chars=300):
    """Extract sentence-like context around a keyword match in caption text."""
    import re
    idx = full_text.lower().find(keyword.lower())
    if idx == -1:
        return ""
    # Expand to ~context_chars around the match
    start = max(0, idx - context_chars)
    end = min(len(full_text), idx + len(keyword) + context_chars)
    snippet = full_text[start:end]
    # Try to start/end at sentence boundaries
    if start > 0:
        period = snippet.find(". ")
        if period != -1 and period < context_chars // 2:
            snippet = snippet[period + 2:]
    if end < len(full_text):
        period = snippet.rfind(". ")
        if period != -1 and period > len(snippet) // 2:
            snippet = snippet[:period + 1]
    return snippet.strip()


def search_ia_tv(keyword, days_back=90):
    """
    Search Internet Archive TV News Archive for programs mentioning keyword.

    Uses the IA advanced search API to find programs in the tvnews collection,
    then downloads their caption files to extract the context around the mention.

    Returns list of TV citation dicts compatible with the GDELT format.
    """
    # Date range for query
    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    params = {
        "q": f"{keyword} collection:tvnews date:[{start_date} TO {end_date}]",
        "fl[]": ["identifier", "title", "date", "subject", "description"],
        "sort[]": "date desc",
        "rows": IA_MAX_RESULTS,
        "output": "json",
    }

    data = None
    for attempt in range(3):
        try:
            resp = requests.get(IA_ADVANCED_SEARCH_URL, params=params, timeout=45)
            if resp.status_code != 200:
                logger.warning(f"IA search returned {resp.status_code}")
                time.sleep(10 * (attempt + 1))
                continue
            data = resp.json()
            break
        except (requests.RequestException, ValueError) as e:
            logger.warning(f"IA search attempt {attempt+1}/3 failed: {e}")
            time.sleep(10 * (attempt + 1))

    if not data:
        return []

    docs = data.get("response", {}).get("docs", [])
    if not docs:
        return []

    clips = []
    for doc in docs:
        identifier = doc.get("identifier", "")
        title = doc.get("title", "")
        date_str = doc.get("date", "")
        description = doc.get("description", "")
        subjects = doc.get("subject", [])
        if isinstance(subjects, str):
            subjects = [subjects]
        station = _ia_identifier_to_station(identifier)
        show = _ia_identifier_to_show(identifier)

        # Try to download caption file to extract context sentence
        sentence = ""
        for cc_suffix in [".cc5.srt", ".align.srt", ".cc5.txt"]:
            cc_url = f"{IA_DOWNLOAD_URL}/{identifier}/{identifier}{cc_suffix}"
            try:
                time.sleep(1)  # Be gentle with IA
                cc_resp = requests.get(cc_url, timeout=20)
                if cc_resp.status_code == 200:
                    cc_text = cc_resp.text
                    if cc_suffix.endswith(".srt"):
                        cc_text = _parse_srt_text(cc_text)
                    sentence = _extract_keyword_context(cc_text, keyword)
                    if sentence:
                        break
            except requests.RequestException:
                continue

        # Fallback: if keyword not in captions, it was likely a visual/chyron mention.
        # Use description or subject tags as context.
        if not sentence and description:
            sentence = _extract_keyword_context(description, keyword)
        if not sentence:
            # Keyword matched via subject tags (visual/chyron mention)
            other_subjects = [s for s in subjects if s.lower() != keyword.lower()][:5]
            if other_subjects:
                sentence = f"{keyword} mentioned on {station} (topics: {', '.join(other_subjects)})"

        # Build clip URL
        clip_url = f"https://archive.org/details/{identifier}"

        clips.append({
            "url": clip_url,
            "title": show or title,
            "seendate": date_str.replace("-", "").replace("T", "").replace(":", "")[:14] if date_str else "",
            "station": station,
            "show": show or title,
            "snippet": sentence[:500] if sentence else "",
            "sentence": sentence[:500] if sentence else "",
            "preview_url": clip_url,
            "source_type": "tv",
            "search_keyword": keyword,
            "domain": station.lower().replace(" ", ""),
            "discovery_source": "ia_tv",
        })

    return clips


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def load_state():
    """Load pipeline state."""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, IOError):
            pass
    return {
        "last_fetched_date": None,
        "last_run": None,
        "total_raw_fetched": 0,
        "runs": [],
    }


def save_state(state):
    """Save pipeline state."""
    atomic_write_json(STATE_FILE, state, indent=2)


def load_existing_citations():
    """Load existing raw citations for dedup."""
    if OUTPUT_FILE.exists():
        try:
            data = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
            return data.get("citations", [])
        except (json.JSONDecodeError, IOError):
            pass
    return []


def deduplicate_citations(new_citations, existing_citations):
    """
    Deduplicate citations by URL. Prefer Context API results (have sentence/context).
    Merge sentence/context from Context API into DOC API results for same URL.
    """
    by_url = {}
    for c in existing_citations:
        by_url[c["url"]] = c

    for c in new_citations:
        url = c["url"]
        if not url:
            continue

        if url in by_url:
            existing = by_url[url]
            if c.get("sentence") and not existing.get("sentence"):
                existing["sentence"] = c["sentence"]
                existing["context"] = c.get("context", "")
                existing["is_quote"] = c.get("is_quote", 0)
        else:
            by_url[url] = c

    return list(by_url.values())


def main():
    parser = argparse.ArgumentParser(description="Discover prediction market citations in media")
    parser.add_argument("--backfill", type=int, metavar="DAYS",
                        help="Force a lookback of N days (e.g. --backfill 30)")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("MEDIA CITATION DISCOVERY")
    logger.info("=" * 60)

    state = load_state()
    run_start = datetime.now(timezone.utc)

    # Determine timespan
    if args.backfill:
        backfill_days = args.backfill
        timespan = timespan_for_days(backfill_days)
        logger.info(f"Backfill mode: {backfill_days} days → timespan={timespan}")
    elif state["last_fetched_date"]:
        last = datetime.fromisoformat(state["last_fetched_date"])
        backfill_days = (run_start - last).days + 1  # +1 for overlap
        timespan = timespan_for_days(backfill_days)
    else:
        backfill_days = 90
        timespan = "3months"  # First run: max lookback

    # Context API has a 72-hour max timespan; cap it
    context_timespan = timespan if backfill_days <= 3 else "72hours"
    logger.info(f"Timespan: {timespan} (Context API capped at: {context_timespan})")

    # Load existing citations for dedup
    existing = load_existing_citations()
    logger.info(f"Existing citations: {len(existing)}")

    # Collect new citations from all APIs
    all_new = []
    api_errors = 0

    # 1. Context API (Tier 1) — primary source, has sentence + context
    # Capped to 7-day window (Context API max); DOC API covers the full range
    logger.info("--- Context API (Tier 1: platform names) ---")
    for kw in TIER1_KEYWORDS:
        results = search_context_api(kw, context_timespan)
        if results:
            logger.info(f"  '{kw}': {len(results)} articles")
            all_new.extend(results)
        else:
            logger.warning(f"  '{kw}': 0 articles (API may have returned error)")
            api_errors += 1

    # 2. Context API (Tier 2)
    logger.info("--- Context API (Tier 2: generic phrases) ---")
    for kw in TIER2_KEYWORDS:
        results = search_context_api(kw, context_timespan)
        if results:
            logger.info(f"  '{kw}': {len(results)} articles")
            all_new.extend(results)
        else:
            logger.info(f"  '{kw}': 0 articles")

    # 3. DOC API (Tier 1 only — broader coverage, no sentence data)
    # DOC API supports up to 3months, so it covers the full backfill window
    logger.info("--- DOC API (Tier 1: broader coverage) ---")
    for kw in TIER1_KEYWORDS:
        results = search_doc_api(kw, timespan)
        if results:
            logger.info(f"  '{kw}': {len(results)} articles")
            all_new.extend(results)
        else:
            logger.info(f"  '{kw}': 0 articles")

    # 4. NewsAPI.org — broader coverage including major outlets (WSJ, NYT, CNN, etc.)
    newsapi_key = _get_newsapi_key()
    if newsapi_key:
        logger.info("--- NewsAPI.org (broad keyword coverage) ---")
        # NewsAPI free tier: max 30 days back
        newsapi_from = (run_start - timedelta(days=min(backfill_days, 30))).strftime("%Y-%m-%d")
        newsapi_to = run_start.strftime("%Y-%m-%d")
        for query in NEWSAPI_QUERIES:
            results = search_newsapi(query, newsapi_from, newsapi_to, newsapi_key)
            if results:
                logger.info(f"  '{query}': {len(results)} articles")
                all_new.extend(results)
            else:
                logger.info(f"  '{query}': 0 articles")
    else:
        logger.info("--- NewsAPI.org: SKIPPED (no NEWSAPI_KEY configured) ---")

    # 5. The Guardian Content API — full article body text for free
    guardian_key = _get_guardian_key()
    if guardian_key:
        logger.info("--- The Guardian Content API ---")
        guardian_from = (run_start - timedelta(days=backfill_days)).strftime("%Y-%m-%d")
        guardian_to = run_start.strftime("%Y-%m-%d")
        for query in GUARDIAN_QUERIES:
            results = search_guardian(query, guardian_from, guardian_to, guardian_key)
            if results:
                logger.info(f"  '{query}': {len(results)} articles")
                all_new.extend(results)
            else:
                logger.info(f"  '{query}': 0 articles")
            time.sleep(1)  # Be gentle with rate limits
    else:
        logger.info("--- The Guardian: SKIPPED (no GUARDIAN_API_KEY) ---")

    # 6. TV — Internet Archive TV News Archive (replaces GDELT TV, offline since Oct 2024)
    logger.info("--- Internet Archive TV News (Tier 1: platform names) ---")
    for kw in TIER1_KEYWORDS:
        results = search_ia_tv(kw, days_back=backfill_days)
        if results:
            logger.info(f"  '{kw}': {len(results)} TV clips")
            all_new.extend(results)
        else:
            logger.info(f"  '{kw}': 0 TV clips")

    # 7. GDELT TV API (may be offline since Oct 2024, but try anyway)
    logger.info("--- GDELT TV API (Tier 1: platform names) ---")
    for kw in TIER1_KEYWORDS:
        results = search_tv_api(kw, timespan)
        if results:
            logger.info(f"  '{kw}': {len(results)} TV clips (GDELT)")
            all_new.extend(results)
        else:
            logger.info(f"  '{kw}': 0 TV clips (GDELT)")

    # 8. GDELT LowerThird / Chyron API (CNN/MSNBC/Fox/BBC only)
    logger.info("--- GDELT LowerThird API (Tier 1: on-screen text) ---")
    for kw in TIER1_KEYWORDS:
        results = search_lowerthird_api(kw, timespan)
        if results:
            logger.info(f"  '{kw}': {len(results)} chyron clips")
            all_new.extend(results)
        else:
            logger.info(f"  '{kw}': 0 chyron clips")

    # 9. Media Cloud — US national online news (academic index)
    mc_key = _get_mediacloud_key()
    if mc_key:
        mc_from = (run_start - timedelta(days=backfill_days)).strftime("%Y-%m-%d")
        mc_to = run_start.strftime("%Y-%m-%d")

        # Tier 1: broad mentions (metadata only)
        logger.info("--- Media Cloud (Tier 1: broad mentions) ---")
        mc_mentions = search_mediacloud_mentions(
            MEDIACLOUD_MENTION_QUERY, mc_from, mc_to, mc_key
        )
        if mc_mentions:
            logger.info(f"  Tier 1 mentions: {len(mc_mentions)} articles")
            all_new.extend(mc_mentions)
        else:
            logger.info("  Tier 1 mentions: 0 articles")

        # Tier 2: probability-language citations (text fetch + sentence extraction)
        # Dedupe against URLs already collected from other sources
        existing_urls = {c["url"] for c in all_new if c.get("url")}
        existing_urls.update(c["url"] for c in existing if c.get("url"))

        logger.info("--- Media Cloud (Tier 2: probability citations) ---")
        mc_citations = search_mediacloud_citations(
            MEDIACLOUD_CITATION_QUERY, mc_from, mc_to, mc_key,
            existing_urls=existing_urls,
        )
        if mc_citations:
            logger.info(f"  Tier 2 citations: {len(mc_citations)} articles")
            all_new.extend(mc_citations)
        else:
            logger.info("  Tier 2 citations: 0 articles")
    else:
        logger.info("--- Media Cloud: SKIPPED (no MEDIACLOUD_API_KEY configured) ---")

    logger.info(f"Total raw results: {len(all_new)} (API errors: {api_errors})")

    if len(all_new) == 0 and api_errors > 0:
        logger.error("No results obtained and API errors occurred. GDELT may be rate-limiting this IP.")
        logger.error("Try again later or increase REQUEST_DELAY_SEC.")

    # Deduplicate
    merged = deduplicate_citations(all_new, existing)

    # Filter: English only for articles (TV clips don't have language field)
    filtered = []
    for c in merged:
        lang = c.get("language", "").strip().upper()
        if c["source_type"] == "tv":
            filtered.append(c)
        elif lang in ("ENGLISH", "EN", "") or "english" in lang.lower():
            filtered.append(c)

    # Assign stable IDs, url_hash, and discovered_at
    discovered_at = run_start.isoformat()
    for c in filtered:
        if "id" not in c:
            c["id"] = make_citation_id(c["url"], c["source_type"], c.get("discovery_source", "gdelt"))
        if "url_hash" not in c:
            c["url_hash"] = compute_url_hash(c.get("url", ""))
        if "discovered_at" not in c:
            c["discovered_at"] = discovered_at

    # Parse and normalize dates
    for c in filtered:
        dt = parse_gdelt_date(c.get("seendate", ""))
        if dt:
            c["published_date"] = dt.isoformat()
        else:
            c["published_date"] = c.get("seendate", "")

    # Sort by date descending
    filtered.sort(key=lambda x: x.get("published_date", ""), reverse=True)

    # Prune citations older than retention window
    cutoff = (run_start - timedelta(days=RETENTION_DAYS)).isoformat()
    before_prune = len(filtered)
    filtered = [c for c in filtered if c.get("published_date", "") >= cutoff or not c.get("published_date")]
    pruned = before_prune - len(filtered)
    if pruned:
        logger.info(f"Pruned {pruned} citations older than {RETENTION_DAYS} days")

    new_count = len(filtered) - len(existing)
    logger.info(f"After dedup + filter: {len(filtered)} total ({new_count} new)")

    # Save output
    output = {
        "citations": filtered,
        "metadata": {
            "generated_at": run_start.isoformat(),
            "total_citations": len(filtered),
            "timespan": timespan,
        },
    }

    atomic_write_json(OUTPUT_FILE, output, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(filtered)} citations to {OUTPUT_FILE}")

    # Update state
    state["last_fetched_date"] = run_start.isoformat()
    state["last_run"] = run_start.isoformat()
    state["total_raw_fetched"] = len(filtered)
    state["runs"].append({
        "date": run_start.strftime("%Y-%m-%d"),
        "new_citations": max(0, new_count),
        "total_after": len(filtered),
        "duration_sec": round((datetime.now(timezone.utc) - run_start).total_seconds()),
    })
    state["runs"] = state["runs"][-30:]
    save_state(state)

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
