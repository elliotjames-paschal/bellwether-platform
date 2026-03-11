#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Refresh Political Tags
================================================================================

Weekly pipeline to discover new Polymarket tags and classify them as political
or non-political using GPT-4o.

PROCESS:
  1. Fetch all tags from Polymarket Gamma API (GET /tags with pagination)
  2. Load known political tags (data/polymarket_political_tags.json)
  3. Load known rejected tags (data/polymarket_rejected_tags.json)
  4. Diff to find new/unseen tags
  5. Classify new tags via GPT-4o (batches of 500)
  6. Update both JSON files

Usage:
    python pipeline_refresh_political_tags.py [--dry-run]

Options:
    --dry-run   Fetch and diff only, skip GPT classification

================================================================================
"""

import json
import time
import sys
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from config import DATA_DIR, get_openai_client

# =============================================================================
# CONFIGURATION
# =============================================================================

POLYMARKET_API_BASE = "https://gamma-api.polymarket.com"
POLITICAL_TAGS_FILE = DATA_DIR / "polymarket_political_tags.json"
REJECTED_TAGS_FILE = DATA_DIR / "polymarket_rejected_tags.json"

GPT_MODEL = "gpt-4o"
GPT_TEMPERATURE = 0
BATCH_SIZE = 500

RATE_LIMIT = 0.1  # seconds between API page fetches
MAX_RETRIES = 3
GPT_MAX_WORKERS = 4  # parallel GPT classification batches


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# =============================================================================
# TAG FETCHING
# =============================================================================

def fetch_all_tags() -> list:
    """
    Fetch all tags from Polymarket Gamma API with pagination.

    Returns:
        List of tag dicts (each has id, label, slug, etc.)
    """
    all_tags = []
    offset = 0
    page = 0

    log("Fetching all Polymarket tags...")

    while True:
        params = {"limit": 100, "offset": offset}

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(
                    f"{POLYMARKET_API_BASE}/tags",
                    params=params,
                    headers={"Accept": "application/json"},
                    timeout=30,
                )

                if response.status_code == 200:
                    tags = response.json()

                    if not tags:
                        return all_tags

                    all_tags.extend(tags)
                    offset += 100
                    page += 1

                    log(f"  Page {page}: {len(tags)} tags (total: {len(all_tags)})")

                    if len(tags) < 100:
                        return all_tags

                    time.sleep(RATE_LIMIT)
                    break

                elif response.status_code == 429:
                    wait = 10 * (2 ** attempt)
                    log(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    log(f"  Error {response.status_code}: {response.text[:200]}")
                    if attempt == MAX_RETRIES - 1:
                        return all_tags
                    time.sleep(5)

            except Exception as e:
                log(f"  Exception: {e}")
                if attempt == MAX_RETRIES - 1:
                    return all_tags
                time.sleep(5)

    return all_tags


# =============================================================================
# FILE I/O
# =============================================================================

def load_json(path) -> list:
    """Load a JSON file, returning empty list if it doesn't exist."""
    if not path.exists():
        return []
    with open(path, "r") as f:
        return json.load(f)


def save_json(path, data):
    """Save data as formatted JSON."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


# =============================================================================
# GPT CLASSIFICATION
# =============================================================================

CLASSIFICATION_PROMPT = """You are an expert political scientist. Your task is to determine whether each tag from a prediction market platform is related to politics/government/policy or not.

A tag is POLITICAL if it relates to ANY of the following:
- Elections, candidates, campaigns, voting, political parties
- Government officials, heads of state, politicians (current or former)
- Legislation, laws, regulations, government policy
- Government agencies, departments, regulatory bodies (SEC, FDA, EPA, Fed, etc.)
- International relations, diplomacy, sanctions, treaties, trade policy
- Military, defense, intelligence, national security
- Courts, judicial appointments, legal proceedings involving government
- Government operations, budgets, shutdowns, debt ceiling
- Political movements, protests, political organizations
- Geopolitics, territorial disputes, sovereignty
- Government responses to crises (pandemic policy, disaster response)
- Political polls, approval ratings
- Countries, states, or cities when the context is governance/politics

A tag is NOT POLITICAL if it relates to:
- Sports, esports, gaming
- Entertainment, celebrities, movies, music, TV shows
- Cryptocurrency prices, DeFi, NFTs, blockchain technology
- Technology products, company earnings, stock prices (unless government-related)
- Weather, natural phenomena (unless about government response)
- Science, medicine (unless about government policy/regulation)
- Social media metrics, influencer culture
- Food, lifestyle, consumer products

For each tag below, respond with EXACTLY one line in this format:
Tag N: YES or NO

Where N is the tag number and YES means political, NO means not political.

Tags to classify:
"""


def classify_tags_batch(client, tags: list) -> list[dict]:
    """
    Classify a batch of tags as political or not using GPT-4o.

    Args:
        client: OpenAI client
        tags: List of tag dicts (each with id, label, slug)

    Returns:
        Tuple of (political_tags, rejected_tags)
    """
    # Build the tag list for the prompt
    tag_lines = []
    for i, tag in enumerate(tags, 1):
        tag_lines.append(f"{i}. \"{tag['label']}\" (slug: {tag['slug']})")

    prompt = CLASSIFICATION_PROMPT + "\n".join(tag_lines)

    response = client.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {"role": "system", "content": "You are a precise classifier. Follow the output format exactly."},
            {"role": "user", "content": prompt},
        ],
        temperature=GPT_TEMPERATURE,
    )

    result_text = response.choices[0].message.content
    lines = result_text.strip().split("\n")

    political = []
    rejected = []

    for i, tag in enumerate(tags):
        tag_num = i + 1
        classified = False

        for line in lines:
            line_stripped = line.strip()
            if line_stripped.startswith(f"Tag {tag_num}:"):
                answer = line_stripped.split(":", 1)[1].strip().upper()
                if answer.startswith("YES"):
                    political.append(tag)
                else:
                    rejected.append(tag)
                classified = True
                break

        # Default to rejected if we couldn't parse the response for this tag
        if not classified:
            log(f"  Warning: Could not parse classification for tag {tag_num} ({tag['label']}), defaulting to rejected")
            rejected.append(tag)

    return political, rejected


def classify_new_tags(new_tags: list) -> tuple[list, list]:
    """
    Classify all new tags in parallel batches using ThreadPoolExecutor.

    Returns:
        Tuple of (all_political, all_rejected)
    """
    client = get_openai_client()
    all_political = []
    all_rejected = []
    results_lock = threading.Lock()

    # Split into batches
    batches = []
    for i in range(0, len(new_tags), BATCH_SIZE):
        batches.append(new_tags[i:i + BATCH_SIZE])

    total_batches = len(batches)
    completed = [0]  # mutable counter for progress logging

    def process_batch(batch_idx, batch):
        political, rejected = classify_tags_batch(client, batch)
        with results_lock:
            completed[0] += 1
            log(f"  Batch {batch_idx + 1}/{total_batches} done: "
                f"{len(political)} political, {len(rejected)} rejected "
                f"({completed[0]}/{total_batches} complete)")
        return political, rejected

    log(f"  Classifying {total_batches} batches with {GPT_MAX_WORKERS} workers...")

    with ThreadPoolExecutor(max_workers=GPT_MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_batch, idx, batch): idx
            for idx, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            try:
                political, rejected = future.result()
                all_political.extend(political)
                all_rejected.extend(rejected)
            except Exception as e:
                batch_idx = futures[future]
                log(f"  WARNING: Batch {batch_idx + 1}/{total_batches} failed: {e}")
                # Tags in this batch won't be classified — they'll be retried next run

    return all_political, all_rejected


# =============================================================================
# MAIN
# =============================================================================

def main():
    dry_run = "--dry-run" in sys.argv

    log("=" * 60)
    log("PIPELINE: Refresh Political Tags")
    if dry_run:
        log("MODE: DRY RUN (no GPT classification)")
    log("=" * 60)

    # Step 1: Fetch all tags from API
    all_api_tags = fetch_all_tags()
    log(f"Fetched {len(all_api_tags)} total tags from Polymarket API")

    if not all_api_tags:
        log("ERROR: No tags fetched from API. Aborting.")
        sys.exit(1)

    # Step 2: Load known tags
    political_tags = load_json(POLITICAL_TAGS_FILE)
    rejected_tags = load_json(REJECTED_TAGS_FILE)

    political_ids = {t["id"] for t in political_tags}
    rejected_ids = {t["id"] for t in rejected_tags}
    known_ids = political_ids | rejected_ids

    log(f"Known tags: {len(political_ids)} political, {len(rejected_ids)} rejected")

    # Step 3: Find new tags
    new_tags = []
    for tag in all_api_tags:
        tag_id = str(tag.get("id", ""))
        if tag_id and tag_id not in known_ids:
            new_tags.append({
                "id": tag_id,
                "label": tag.get("label", ""),
                "slug": tag.get("slug", ""),
            })

    log(f"New unseen tags: {len(new_tags)}")

    if not new_tags:
        log("No new tags to classify. Done!")
        return

    if dry_run:
        log("DRY RUN: Would classify the following new tags:")
        for i, tag in enumerate(new_tags[:20], 1):
            log(f"  {i}. {tag['label']} (slug: {tag['slug']}, id: {tag['id']})")
        if len(new_tags) > 20:
            log(f"  ... and {len(new_tags) - 20} more")
        return

    # Step 4: Classify new tags via GPT-4o
    log("Classifying new tags with GPT-4o...")
    new_political, new_rejected = classify_new_tags(new_tags)

    log(f"Classification results: {len(new_political)} political, {len(new_rejected)} rejected")

    # Step 5: Update files
    political_tags.extend(new_political)
    rejected_tags.extend(new_rejected)

    save_json(POLITICAL_TAGS_FILE, political_tags)
    log(f"Updated {POLITICAL_TAGS_FILE.name}: {len(political_tags)} total political tags")

    save_json(REJECTED_TAGS_FILE, rejected_tags)
    log(f"Updated {REJECTED_TAGS_FILE.name}: {len(rejected_tags)} total rejected tags")

    # Step 6: Summary
    log("=" * 60)
    log("SUMMARY")
    log(f"  API tags fetched:    {len(all_api_tags)}")
    log(f"  Previously known:    {len(known_ids)}")
    log(f"  New tags found:      {len(new_tags)}")
    log(f"  Classified political: {len(new_political)}")
    log(f"  Classified rejected:  {len(new_rejected)}")
    log(f"  Total political now:  {len(political_tags)}")
    log(f"  Total rejected now:   {len(rejected_tags)}")
    log("=" * 60)


if __name__ == "__main__":
    main()
