#!/usr/bin/env python3
"""One-off script: Re-run Media Cloud Tier 2 only.

Queries Media Cloud for probability-language articles, fetches text via
trafilatura, extracts keyword sentences, and upgrades existing Tier 1
mentions in media_citations_raw.json with sentence/context data.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR
from pipeline_media_discover_citations import (
    MEDIACLOUD_CITATION_QUERY,
    MEDIACLOUD_SENTENCE_KEYWORDS,
    _get_mediacloud_key,
    _mediacloud_search,
    _fetch_article_text,
)
from media_pipeline.core import extract_keyword_sentences

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RAW_FILE = DATA_DIR / "media_citations_raw.json"


def main():
    mc_key = _get_mediacloud_key()
    if not mc_key:
        logger.error("No MEDIACLOUD_API_KEY configured")
        return 1

    # Load existing citations
    if not RAW_FILE.exists():
        logger.error(f"Raw citations file not found: {RAW_FILE}")
        return 1

    raw_data = json.loads(RAW_FILE.read_text(encoding="utf-8"))
    citations = raw_data.get("citations", [])
    logger.info(f"Loaded {len(citations)} existing citations")

    # Build set of non-Media Cloud URLs (only dedup against GDELT, Guardian, etc.)
    non_mc_urls = {c["url"] for c in citations
                   if c.get("url") and c.get("discovery_source") not in
                   ("mediacloud_mention", "mediacloud_citation")}
    logger.info(f"Non-MC URLs for dedup: {len(non_mc_urls)}")

    # Query Tier 2
    mc_to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    mc_from = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    logger.info(f"Querying Media Cloud Tier 2: {mc_from} to {mc_to}")

    stories = _mediacloud_search(MEDIACLOUD_CITATION_QUERY, mc_from, mc_to, mc_key)
    logger.info(f"Tier 2 API returned {len(stories)} stories")

    if not stories:
        logger.info("No stories returned")
        return 0

    # Process each story: dedup against non-MC sources, fetch text, extract sentences
    tier2_by_url = {}
    fetch_success = 0
    fetch_fail = 0
    skipped_dedup = 0
    no_sentence = 0

    for i, story in enumerate(stories):
        url = story.get("url", "")
        if not url:
            continue
        if url in non_mc_urls:
            skipped_dedup += 1
            continue
        if url in tier2_by_url:
            continue  # already processed this URL

        if (i + 1) % 500 == 0:
            logger.info(f"  Progress: {i+1}/{len(stories)} stories checked, {fetch_success} texts fetched")

        title = (story.get("title") or "").strip()
        pub_date = story.get("publish_date", "")

        try:
            domain = urlparse(url).netloc.replace("www.", "")
        except Exception:
            domain = ""

        seendate = pub_date.replace("-", "").replace(":", "").replace("T", "").replace("Z", "").replace(" ", "")

        # Fetch text
        text = _fetch_article_text(url)
        if not text:
            fetch_fail += 1
            continue

        fetch_success += 1

        # Extract keyword sentences
        matches = extract_keyword_sentences(text, MEDIACLOUD_SENTENCE_KEYWORDS, context_n=2)
        if not matches:
            no_sentence += 1
            continue

        # Find best match with probability language
        prob_match = None
        for m in matches:
            sent_lower = m["sentence"].lower()
            if any(k in sent_lower for k in ("%", "percent", "probability", "odds", "chance")):
                prob_match = m
                break
        best = prob_match or matches[0]
        sentence = best["sentence"][:500]
        context = (best["before"] + " " + best["sentence"] + " " + best["after"]).strip()[:1000]

        tier2_by_url[url] = {
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
        }

    logger.info(f"Tier 2 results: {len(tier2_by_url)} with sentences, "
                f"{fetch_success} texts fetched, {fetch_fail} fetch failures, "
                f"{no_sentence} no keyword sentence, {skipped_dedup} deduped")

    if not tier2_by_url:
        logger.info("No Tier 2 citations to add")
        return 0

    # Upgrade: replace Tier 1 mentions with Tier 2 citations
    upgraded = 0
    added = 0
    new_citations = []
    for c in citations:
        url = c.get("url", "")
        if url in tier2_by_url:
            new_citations.append(tier2_by_url.pop(url))
            upgraded += 1
        else:
            new_citations.append(c)

    # Add any Tier 2 citations that weren't in Tier 1
    for c in tier2_by_url.values():
        new_citations.append(c)
        added += 1

    logger.info(f"Upgraded {upgraded} Tier 1 → Tier 2, added {added} new Tier 2 citations")

    # Save
    raw_data["citations"] = new_citations
    raw_data["metadata"]["total_citations"] = len(new_citations)
    raw_data["metadata"]["tier2_rerun_at"] = datetime.now(timezone.utc).isoformat()

    RAW_FILE.write_text(json.dumps(raw_data, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"Saved {len(new_citations)} citations to {RAW_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
