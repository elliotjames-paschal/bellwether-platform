#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Get Election Dates for Closed Elections
================================================================================

Part of the NEW Bellwether Pipeline (January 2026+)

This script:
1. Reads electoral markets from the master CSV
2. Filters to CLOSED markets only (is_closed == True)
3. Groups by unique elections (office, location, election_year)
4. Compares against existing election_dates_lookup.csv
5. For elections missing from the lookup, uses GPT-4o to find the date
6. Updates election_dates_lookup.csv with new entries

Only processes closed markets so we don't waste GPT calls on future
elections with unknown dates. As elections happen and markets close,
subsequent daily runs will pick them up automatically.

Usage:
    python pipeline_get_election_dates.py

Input:
    - data/combined_political_markets_with_electoral_details_UPDATED.csv
    - data/election_dates_lookup.csv (existing election dates)

Output:
    - data/election_dates_lookup.csv (updated with new elections)

================================================================================
"""

import pandas as pd
import json
import time
import os
from datetime import datetime
from pathlib import Path
from openai import OpenAI

# =============================================================================
# CONFIGURATION
# =============================================================================

from config import BASE_DIR, DATA_DIR, get_openai_client

# Input/Output files
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
DATES_LOOKUP_FILE = DATA_DIR / "election_dates_lookup.csv"
CHECKPOINT_FILE = DATA_DIR / "pipeline_election_dates_checkpoint.json"

# OpenAI Configuration
BATCH_SIZE = 50  # Increased from 10 for cost efficiency
MODEL = "gpt-4o"
TEMPERATURE = 0


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")



# =============================================================================
# STAGE 1: BATCH DATE LOOKUP (HIGH RECALL)
# =============================================================================

STAGE1_PROMPT = """You are an election date expert for elections worldwide. Determine the EXACT election date.

RULES FOR US ELECTIONS:
1. US GENERAL ELECTIONS: Always first Tuesday after first Monday in November
   - 2024: November 5, 2024
   - 2025: November 4, 2025 (off-year elections in NJ, VA)
   - 2026: November 3, 2026

2. US PRIMARIES: Vary by state - check the specific state's primary date for that year

3. US SPECIAL ELECTIONS: Have specific dates set when called

RULES FOR INTERNATIONAL ELECTIONS:
1. Look up the actual election date for the country, office, and year specified
2. For parliamentary elections, use the main election day (not runoffs unless specified)
3. For presidential elections with multiple rounds, use the first round date
4. If the election hasn't happened yet or date is unknown, return "unknown"

For each election, return the election date in YYYY-MM-DD format.
If you cannot determine the date, return "unknown" for that election.

Return JSON: {"results": [{"index": 0, "election_date": "2024-11-05"}, ...]}"""


# =============================================================================
# STAGE 2: BATCH VERIFICATION (HIGH PRECISION)
# =============================================================================

STAGE2_PROMPT = """VERIFICATION MODE - Double-check these election dates.

RULES FOR US ELECTIONS:
1. US GENERAL ELECTIONS: Always first Tuesday after first Monday in November
2. US PRIMARIES: State-specific primary dates
3. US SPECIAL ELECTIONS: Specific dates when called

RULES FOR INTERNATIONAL ELECTIONS:
1. Verify the election date for the country/office/year
2. For multi-round elections, use first round date
3. If you cannot verify, return "unknown"

Return JSON: {"results": [{"index": 0, "election_date": "2024-11-05"}, ...]}"""


# =============================================================================
# STAGE 3: BATCH TIEBREAKER
# =============================================================================

STAGE3_PROMPT = """TIEBREAKER - Final determination of election dates.

Be VERY careful. Return the EXACT election date in YYYY-MM-DD format.
If you cannot determine with confidence, return "unknown".

Return JSON: {"results": [{"index": 0, "election_date": "2024-11-05"}, ...]}"""


def stage1_batch(client, elections, batch_size=50, show_progress=True):
    """Stage 1: Batch lookup election dates."""
    results = []
    total = len(elections)

    if show_progress:
        log(f"  Stage 1: Looking up {total} elections (batch={batch_size})...")

    for start in range(0, total, batch_size):
        batch = elections[start:start + batch_size]

        # Format elections for prompt
        elections_text = "\n".join(
            f'{i}. {e["country"]} - {e["office"]} - {e["location"]} - {int(e["election_year"])} {"(PRIMARY)" if e.get("is_primary") else "(GENERAL)"}'
            for i, e in enumerate(batch)
        )

        prompt = f"Find election dates for:\n{elections_text}"

        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": STAGE1_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=TEMPERATURE,
                response_format={"type": "json_object"}
            )

            parsed = json.loads(resp.choices[0].message.content)
            for r in parsed.get("results", []):
                local_idx = r.get("index", 0)
                if local_idx < len(batch):
                    results.append({
                        "index": start + local_idx,
                        "election_date": r.get("election_date"),
                        "stage": 1
                    })

        except Exception as e:
            log(f"    Batch error: {e}")
            for i in range(len(batch)):
                results.append({
                    "index": start + i,
                    "election_date": None,
                    "stage": 1,
                    "error": str(e)
                })

        if show_progress and (start + batch_size) % 200 == 0:
            log(f"    {min(start + batch_size, total)}/{total}...")

        time.sleep(0.5)

    if show_progress:
        found = sum(1 for r in results if r.get("election_date") and r.get("election_date") != "unknown")
        log(f"  Stage 1 done: {found} dates found")

    return results


def stage2_batch(client, elections, batch_size=50, show_progress=True):
    """Stage 2: Batch verify election dates."""
    results = []
    total = len(elections)

    if show_progress:
        log(f"  Stage 2: Verifying {total} elections (batch={batch_size})...")

    for start in range(0, total, batch_size):
        batch = elections[start:start + batch_size]

        elections_text = "\n".join(
            f'{i}. {e["country"]} - {e["office"]} - {e["location"]} - {int(e["election_year"])} {"(PRIMARY)" if e.get("is_primary") else "(GENERAL)"}'
            for i, e in enumerate(batch)
        )

        prompt = f"Verify election dates for:\n{elections_text}"

        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": STAGE2_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=TEMPERATURE,
                response_format={"type": "json_object"}
            )

            parsed = json.loads(resp.choices[0].message.content)
            for r in parsed.get("results", []):
                local_idx = r.get("index", 0)
                if local_idx < len(batch):
                    results.append({
                        "index": start + local_idx,
                        "election_date": r.get("election_date"),
                        "stage": 2
                    })

        except Exception as e:
            log(f"    Batch error: {e}")
            for i in range(len(batch)):
                results.append({
                    "index": start + i,
                    "election_date": None,
                    "stage": 2,
                    "error": str(e)
                })

        if show_progress and (start + batch_size) % 200 == 0:
            log(f"    {min(start + batch_size, total)}/{total}...")

        time.sleep(0.5)

    if show_progress:
        log(f"  Stage 2 done: {len(results)} verified")

    return results


def stage3_batch(client, disagreements, elections, batch_size=50, show_progress=True):
    """Stage 3: Batch tiebreaker for disagreements."""
    total = len(disagreements)
    if total == 0:
        return []

    if show_progress:
        log(f"  Stage 3: {total} tiebreakers (batch={batch_size})...")

    results = []
    for start in range(0, total, batch_size):
        batch = disagreements[start:start + batch_size]
        batch_elections = [elections[d["index"]] for d in batch]

        elections_text = "\n".join(
            f'{i}. {e["country"]} - {e["office"]} - {e["location"]} - {int(e["election_year"])} {"(PRIMARY)" if e.get("is_primary") else "(GENERAL)"}'
            for i, e in enumerate(batch_elections)
        )

        prompt = f"Tiebreaker - determine election dates for:\n{elections_text}"

        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": STAGE3_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=TEMPERATURE,
                response_format={"type": "json_object"}
            )

            parsed = json.loads(resp.choices[0].message.content)
            for r in parsed.get("results", []):
                local_idx = r.get("index", 0)
                if local_idx < len(batch):
                    results.append({
                        "disagreement_idx": start + local_idx,
                        "election_date": r.get("election_date"),
                        "stage": 3
                    })

        except Exception as e:
            log(f"    Batch error: {e}")
            for i in range(len(batch)):
                results.append({
                    "disagreement_idx": start + i,
                    "election_date": None,
                    "stage": 3,
                    "error": str(e)
                })

        time.sleep(0.5)

    if show_progress:
        log(f"  Stage 3 done: {len(results)} tiebreakers resolved")

    return results


def run_date_pipeline(client, elections, show_progress=True):
    """Run full 3-stage date lookup pipeline."""
    from collections import Counter

    if show_progress:
        log(f"\n{'='*50}")
        log(f"LOOKING UP {len(elections)} ELECTION DATES (3-stage)")
        log(f"{'='*50}")

    # Stage 1: Batch lookup
    s1_results = stage1_batch(client, elections, BATCH_SIZE, show_progress)
    s1_lookup = {r["index"]: r for r in s1_results}

    # Stage 2: Batch verify
    s2_results = stage2_batch(client, elections, BATCH_SIZE, show_progress)
    s2_lookup = {r["index"]: r for r in s2_results}

    # Combine results
    final_results = []
    disagreements = []

    for idx in range(len(elections)):
        s1 = s1_lookup.get(idx, {})
        s2 = s2_lookup.get(idx, {})

        s1_date = s1.get("election_date")
        s2_date = s2.get("election_date")

        # Normalize "unknown" to None for comparison
        if s1_date == "unknown":
            s1_date = None
        if s2_date == "unknown":
            s2_date = None

        if s1_date and s2_date and s1_date == s2_date:
            # Agreement - accept
            final_results.append({
                "index": idx,
                "election_date": s1_date,
                "votes": 2
            })
        elif s1_date and not s2_date:
            # Only s1 has date
            final_results.append({
                "index": idx,
                "election_date": s1_date,
                "votes": 1
            })
        elif s2_date and not s1_date:
            # Only s2 has date
            final_results.append({
                "index": idx,
                "election_date": s2_date,
                "votes": 1
            })
        elif s1_date and s2_date and s1_date != s2_date:
            # Disagreement - need tiebreaker
            disagreements.append({
                "index": idx,
                "s1_date": s1_date,
                "s2_date": s2_date
            })
        else:
            # Both unknown
            final_results.append({
                "index": idx,
                "election_date": "unknown",
                "votes": 0
            })

    # Stage 3: Batch tiebreakers
    if disagreements:
        s3_results = stage3_batch(client, disagreements, elections, BATCH_SIZE, show_progress)
        s3_lookup = {r["disagreement_idx"]: r for r in s3_results}

        for i, d in enumerate(disagreements):
            s3 = s3_lookup.get(i, {})
            s3_date = s3.get("election_date")
            if s3_date == "unknown":
                s3_date = None

            # Majority vote among s1, s2, s3
            dates = [d["s1_date"], d["s2_date"], s3_date]
            valid_dates = [dt for dt in dates if dt]

            if valid_dates:
                date_counts = Counter(valid_dates)
                most_common = date_counts.most_common(1)[0]
                final_date = most_common[0]
                votes = most_common[1]
            else:
                final_date = "unknown"
                votes = 0

            final_results.append({
                "index": d["index"],
                "election_date": final_date,
                "votes": votes
            })

    final_results.sort(key=lambda x: x["index"])

    if show_progress:
        found = sum(1 for r in final_results if r.get("election_date") and r.get("election_date") != "unknown")
        log(f"\nFINAL: {found} dates found, {len(final_results) - found} unknown")
        log(f"{'='*50}")

    return final_results


def main():
    """Main function to get election dates for closed elections."""
    print("\n" + "=" * 70)
    print("PIPELINE: GET ELECTION DATES — CLOSED ELECTIONS")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    # Read from master CSV — only closed electoral markets
    if not MASTER_FILE.exists():
        log(f"Master CSV not found: {MASTER_FILE}")
        return 0
    log("Loading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"  Total markets: {len(df):,}")

    # Filter to electoral markets
    df = df[df['election_year'].notna()]
    log(f"  Electoral markets: {len(df):,}")

    # Filter to CLOSED markets only
    df = df[df['is_closed'] == True]
    log(f"  Closed electoral markets: {len(df):,}")

    # Filter for elections with complete info (any country)
    elections_with_info = df[
        (df['country'].notna()) &
        (df['office'].notna()) &
        (df['location'].notna()) &
        (df['election_year'].notna())
    ].copy()

    log(f"  Elections with complete info: {len(elections_with_info):,}")

    if len(elections_with_info) == 0:
        log("No elections to process!")
        return 0

    # Get unique elections (country, office, location, election_year)
    unique_elections = elections_with_info.groupby(
        ['country', 'office', 'location', 'election_year', 'is_primary']
    ).size().reset_index(name='market_count')

    log(f"  Unique elections: {len(unique_elections):,}")

    # Load existing election dates
    if DATES_LOOKUP_FILE.exists():
        dates_df = pd.read_csv(DATES_LOOKUP_FILE)
        log(f"  Existing dates lookup: {len(dates_df):,} entries")
    else:
        dates_df = pd.DataFrame(columns=['office', 'location', 'election_year', 'election_date'])
        log("  No existing dates lookup - starting fresh")

    # Find elections that need dates
    # Create keys for comparison (include country)
    unique_elections['key'] = (
        unique_elections['country'].astype(str) + '|' +
        unique_elections['office'].astype(str) + '|' +
        unique_elections['location'].astype(str) + '|' +
        unique_elections['election_year'].astype(str)
    )

    dates_df['key'] = (
        dates_df['country'].astype(str) + '|' +
        dates_df['office'].astype(str) + '|' +
        dates_df['location'].astype(str) + '|' +
        dates_df['election_year'].astype(str)
    )

    existing_keys = set(dates_df['key'].tolist())
    new_elections = unique_elections[~unique_elections['key'].isin(existing_keys)]

    log(f"  Elections needing dates: {len(new_elections):,}")

    if len(new_elections) == 0:
        log("No new elections need dates!")
        return len(unique_elections)

    # Load checkpoint if exists
    processed_elections = {}
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
            processed_elections = checkpoint.get('processed', {})
        log(f"  Loaded checkpoint: {len(processed_elections)} already processed")

    # Filter out already processed
    elections_to_process = []
    for _, row in new_elections.iterrows():
        key = row['key']
        if key not in processed_elections:
            elections_to_process.append({
                'country': row['country'],
                'office': row['office'],
                'location': row['location'],
                'election_year': row['election_year'],
                'is_primary': row.get('is_primary', False)
            })

    log(f"  Elections to process: {len(elections_to_process):,}")

    if elections_to_process:
        # Initialize OpenAI client
        log("\nInitializing OpenAI client...")
        client = get_openai_client()
        log("  OpenAI client ready")

        # Run 3-stage pipeline
        start_time = time.time()
        results = run_date_pipeline(client, elections_to_process, show_progress=True)

        # Add results to lookup and checkpoint
        new_entries = []
        for result in results:
            idx = result["index"]
            election_date = result.get('election_date')

            if election_date and election_date != 'unknown':
                election = elections_to_process[idx]
                key = f"{election['country']}|{election['office']}|{election['location']}|{election['election_year']}"
                processed_elections[key] = election_date

                new_entries.append({
                    'country': election['country'],
                    'office': election['office'],
                    'location': election['location'],
                    'election_year': election['election_year'],
                    'election_date': election_date
                })

        # Save checkpoint
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'processed': processed_elections,
                'last_updated': datetime.now().isoformat()
            }, f)

        elapsed = time.time() - start_time
        log(f"\nDate lookup completed in {elapsed/60:.1f} minutes")
        log(f"Found dates for {len(new_entries)} elections")

        # Append new entries to dates lookup
        if new_entries:
            new_dates_df = pd.DataFrame(new_entries)
            dates_df = pd.concat([dates_df, new_dates_df], ignore_index=True)

            # Remove the key column before saving
            if 'key' in dates_df.columns:
                dates_df = dates_df.drop(columns=['key'])

            # Sort by office, location, year
            dates_df = dates_df.sort_values(['office', 'location', 'election_year'])

            # Save updated lookup
            dates_df.to_csv(DATES_LOOKUP_FILE, index=False)
            log(f"Updated {DATES_LOOKUP_FILE.name} with {len(new_entries)} new entries")

    # Clean up checkpoint on successful completion
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    # Summary
    print("\n" + "=" * 70)
    print("ELECTION DATES LOOKUP COMPLETE")
    print("=" * 70)
    print(f"Unique elections: {len(unique_elections):,}")
    print(f"New elections processed: {len(elections_to_process):,}")
    print(f"Total dates in lookup: {len(dates_df):,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return len(unique_elections)


if __name__ == "__main__":
    main()
