#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Reclassify Incomplete Electoral Markets
================================================================================

Part of the NEW Bellwether Pipeline (January 2026+)

This script fills the gap where --full-refresh only classifies NEW markets:
it re-classifies EXISTING electoral markets that have missing metadata
(office, location, election_year).

Designed to be:
- Run one-time to backfill the 2,085 markets with missing metadata
- Called from pipeline_daily_refresh.py to catch any future gaps
- Resumable via checkpoint file if interrupted

HANDLING UNCLASSIFIABLE MARKETS:
  Some markets are tagged "1. ELECTORAL" but don't map to a specific
  election (e.g. "Will Kevin McCarthy be elected Speaker?", "Republican
  House and Democratic Senate?"). After GPT attempts classification,
  markets that still have missing fields get stamped with
  reclassify_attempted=True in the master CSV so they are skipped on
  future runs rather than being re-sent to GPT indefinitely.

Usage:
    python pipeline_reclassify_incomplete.py [--dry-run]

Options:
    --dry-run   Show which markets would be reclassified without calling GPT

Input:
    - data/combined_political_markets_with_electoral_details_UPDATED.csv (master)

Output:
    - data/combined_political_markets_with_electoral_details_UPDATED.csv (updated in-place)
    - data/reclassify_incomplete_report.csv (audit log of changes)

================================================================================
"""

import pandas as pd
import json
import time
import sys
from datetime import datetime
from pathlib import Path

# Add scripts dir to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR, get_openai_client, rotate_backups
from pipeline_classify_electoral import run_electoral_pipeline, derive_election_type
from utils.openai_classifier import extract_candidate_name, search_candidate_party

# =============================================================================
# CONFIGURATION
# =============================================================================

MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
CHECKPOINT_FILE = DATA_DIR / "pipeline_reclassify_checkpoint.json"
REPORT_FILE = DATA_DIR / "reclassify_incomplete_report.csv"
BACKUP_DIR = DATA_DIR / "backups"

# Process in chunks to allow checkpointing on large batches
CHUNK_SIZE = 500


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def get_stable_id(row):
    """
    Get a stable market identifier that survives CSV re-reads.

    Uses pm_condition_id for Polymarket, market_id for Kalshi.
    Falls back to market_id for either platform.
    """
    platform = row.get('platform', '')
    if platform == 'Polymarket':
        val = row.get('pm_condition_id')
        if pd.notna(val):
            return f"pm:{val}"
    val = row.get('market_id')
    if pd.notna(val):
        return f"mk:{val}"
    # Last resort: use question hash (very unlikely to hit this)
    q = str(row.get('question', ''))
    return f"q:{hash(q)}"


def find_incomplete_electoral(master_df):
    """
    Find electoral markets missing office, location, or election_year
    that haven't already been attempted.

    Returns:
        DataFrame of rows that need reclassification.
    """
    electoral = master_df[
        master_df['political_category'].astype(str).str.startswith('1.')
    ].copy()

    if len(electoral) == 0:
        return pd.DataFrame()

    # Missing any of the five key fields
    missing_country = electoral['country'].isna() if 'country' in electoral.columns else pd.Series(True, index=electoral.index)
    missing_office = electoral['office'].isna() if 'office' in electoral.columns else pd.Series(True, index=electoral.index)
    missing_location = electoral['location'].isna() if 'location' in electoral.columns else pd.Series(True, index=electoral.index)
    missing_year = electoral['election_year'].isna() if 'election_year' in electoral.columns else pd.Series(True, index=electoral.index)
    missing_primary = electoral['is_primary'].isna() if 'is_primary' in electoral.columns else pd.Series(True, index=electoral.index)

    incomplete = electoral[missing_country | missing_office | missing_location | missing_year | missing_primary]

    # Exclude markets already attempted (GPT couldn't classify them)
    if 'reclassify_attempted' in incomplete.columns:
        incomplete = incomplete[incomplete['reclassify_attempted'] != True]

    return incomplete


def load_checkpoint():
    """Load checkpoint of already-processed rows (keyed by stable market ID)."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"processed": {}, "last_updated": None}


def save_checkpoint(checkpoint):
    """Save checkpoint."""
    checkpoint["last_updated"] = datetime.now().isoformat()
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)


def backfill_party_affiliations():
    """
    Backfill party affiliations for US electoral markets using web search.

    Targets markets where party is null and party_search_attempted is not True.
    Uses extract_candidate_name() + search_candidate_party() with deduplication
    so each unique candidate is only searched once.
    """
    log("\n" + "=" * 50)
    log("PARTY AFFILIATION BACKFILL (Web Search)")
    log("=" * 50)

    # Reload master CSV (may have been updated by the reclassify step above)
    master_df = pd.read_csv(MASTER_FILE, low_memory=False)

    # Find US electoral markets where party is null
    if 'party' not in master_df.columns:
        log("  No 'party' column found. Nothing to do.")
        return

    electoral_mask = master_df['political_category'].astype(str).str.startswith('1.')
    us_mask = master_df['country'] == 'United States'
    null_party_mask = master_df['party'].isna()

    candidates = master_df[electoral_mask & us_mask & null_party_mask].copy()

    # Exclude already attempted
    if 'party_search_attempted' in master_df.columns:
        not_attempted = master_df.loc[candidates.index, 'party_search_attempted'] != True
        candidates = candidates[not_attempted]

    log(f"  US electoral markets with null party (not yet attempted): {len(candidates):,}")

    if len(candidates) == 0:
        log("  All parties resolved or attempted. Nothing to do.")
        return

    # Extract candidate names and build dedup cache
    rows_by_candidate = {}  # (name, location, year) -> [indices]
    no_name_indices = []

    for idx, row in candidates.iterrows():
        question = str(row.get('question', ''))
        candidate_name = extract_candidate_name(question)

        if not candidate_name:
            no_name_indices.append(idx)
            continue

        location = str(row.get('location', '')) if pd.notna(row.get('location')) else ''
        year = str(int(row['election_year'])) if pd.notna(row.get('election_year')) else ''
        cache_key = (candidate_name, location, year)

        if cache_key not in rows_by_candidate:
            rows_by_candidate[cache_key] = []
        rows_by_candidate[cache_key].append(idx)

    log(f"  Unique candidates to search: {len(rows_by_candidate):,}")
    log(f"  Rows with no extractable name: {len(no_name_indices):,}")

    # Mark no-name rows as attempted so future runs skip them
    for idx in no_name_indices:
        master_df.loc[idx, 'party_search_attempted'] = True

    # Search for each unique candidate
    resolved_count = 0
    not_found_count = 0

    for i, (cache_key, indices) in enumerate(rows_by_candidate.items()):
        candidate_name, location, year = cache_key

        party = search_candidate_party(candidate_name, location, year, show_progress=True)

        if party:
            for idx in indices:
                master_df.loc[idx, 'party'] = party
            resolved_count += len(indices)
        else:
            for idx in indices:
                master_df.loc[idx, 'party_search_attempted'] = True
            not_found_count += len(indices)

        if (i + 1) % 50 == 0:
            log(f"    Searched {i + 1}/{len(rows_by_candidate)} candidates...")

        time.sleep(0.3)

    log(f"\n  Resolved: {resolved_count:,} markets")
    log(f"  Not found: {not_found_count:,} markets")
    log(f"  No name extracted: {len(no_name_indices):,} markets")

    # Save updated master CSV
    master_df.to_csv(MASTER_FILE, index=False)
    log(f"  Saved updated master CSV: {MASTER_FILE.name}")


def main():
    """Main function to reclassify incomplete electoral markets."""
    dry_run = "--dry-run" in sys.argv

    print("\n" + "=" * 70)
    print("PIPELINE: RECLASSIFY INCOMPLETE ELECTORAL MARKETS")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE — will update master CSV'}")
    print("=" * 70 + "\n")

    # =========================================================================
    # STEP 1: Load master and find incomplete markets
    # =========================================================================

    log("Loading master CSV...")
    master_df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"  Total markets: {len(master_df):,}")

    incomplete = find_incomplete_electoral(master_df)
    log(f"  Electoral markets needing classification: {len(incomplete):,}")

    # Count previously attempted
    all_electoral = master_df[master_df['political_category'].astype(str).str.startswith('1.')]
    previously_attempted = 0
    if 'reclassify_attempted' in all_electoral.columns:
        previously_attempted = (all_electoral['reclassify_attempted'] == True).sum()
    if previously_attempted > 0:
        log(f"  Previously attempted (unclassifiable): {previously_attempted:,}")

    if len(incomplete) == 0:
        log("All electoral markets have complete metadata or were already attempted. Nothing to do.")
        return 0

    # Breakdown
    total_electoral = len(all_electoral)
    log(f"  Total electoral markets: {total_electoral:,}")
    log(f"  Complete: {total_electoral - len(incomplete) - previously_attempted:,}")
    log(f"  Incomplete (to process): {len(incomplete):,}")

    # Show what's missing
    has_country = incomplete['country'].notna().sum() if 'country' in incomplete.columns else 0
    has_office = incomplete['office'].notna().sum() if 'office' in incomplete.columns else 0
    has_location = incomplete['location'].notna().sum() if 'location' in incomplete.columns else 0
    has_year = incomplete['election_year'].notna().sum() if 'election_year' in incomplete.columns else 0
    has_primary = incomplete['is_primary'].notna().sum() if 'is_primary' in incomplete.columns else 0
    log(f"\n  Already have country: {has_country:,}")
    log(f"  Already have office: {has_office:,}")
    log(f"  Already have location: {has_location:,}")
    log(f"  Already have election_year: {has_year:,}")
    log(f"  Already have is_primary: {has_primary:,}")
    log(f"  Missing all five: {len(incomplete) - max(has_country, has_office, has_location, has_year, has_primary):,}")

    # Platform breakdown
    for platform in ['Polymarket', 'Kalshi']:
        count = len(incomplete[incomplete['platform'] == platform])
        if count > 0:
            log(f"  {platform}: {count:,}")

    if dry_run:
        log("\n--- DRY RUN: showing sample of incomplete markets ---")
        sample = incomplete.head(20)
        for _, row in sample.iterrows():
            q = row.get('question', '?')[:80]
            plat = row.get('platform', '?')
            log(f"  [{plat}] {q}")
        if len(incomplete) > 20:
            log(f"  ... and {len(incomplete) - 20:,} more")
        log("\nDry run complete. Run without --dry-run to reclassify.")
        return 0

    # =========================================================================
    # STEP 2: Backup master CSV
    # =========================================================================

    log("\nCreating backup...")
    BACKUP_DIR.mkdir(exist_ok=True)
    backup_file = BACKUP_DIR / f"master_pre_reclassify_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    master_df.to_csv(backup_file, index=False)
    log(f"  Saved backup: {backup_file.name}")
    deleted = rotate_backups("master_pre_reclassify_*.csv")
    if deleted > 0:
        log(f"  Rotated {deleted} old backup(s)")

    # =========================================================================
    # STEP 3: Build stable ID mapping
    # =========================================================================

    log("\nBuilding stable market ID mapping...")

    # Map each incomplete row: stable_id -> DataFrame index
    id_to_idx = {}
    idx_to_id = {}
    for idx, row in incomplete.iterrows():
        stable_id = get_stable_id(row)
        id_to_idx[stable_id] = idx
        idx_to_id[idx] = stable_id

    log(f"  Mapped {len(id_to_idx):,} markets to stable IDs")

    # =========================================================================
    # STEP 4: Run GPT classification on incomplete markets
    # =========================================================================

    log("\nInitializing OpenAI client...")
    client = get_openai_client()
    log("  OpenAI client ready")

    # Load checkpoint (keyed by stable market ID)
    checkpoint = load_checkpoint()
    already_processed = checkpoint["processed"]
    log(f"  Checkpoint: {len(already_processed)} already processed")

    # Filter out already-processed markets (by stable ID)
    to_process = []  # list of (stable_id, df_index, question)
    for idx, row in incomplete.iterrows():
        stable_id = idx_to_id[idx]
        if stable_id not in already_processed:
            to_process.append((stable_id, idx, row['question']))

    log(f"  Remaining to classify: {len(to_process):,}")

    if len(to_process) == 0:
        log("All incomplete markets already in checkpoint. Applying results...")
    else:
        # Process in chunks for checkpointing
        total_chunks = (len(to_process) + CHUNK_SIZE - 1) // CHUNK_SIZE
        start_time = time.time()

        for chunk_num in range(total_chunks):
            chunk_start = chunk_num * CHUNK_SIZE
            chunk_end = min(chunk_start + CHUNK_SIZE, len(to_process))
            chunk = to_process[chunk_start:chunk_end]

            chunk_questions = [q for _, _, q in chunk]
            chunk_ids = [(sid, idx) for sid, idx, _ in chunk]

            log(f"\n--- Chunk {chunk_num + 1}/{total_chunks}: "
                f"markets {chunk_start + 1}-{chunk_end} of {len(to_process)} ---")

            # Run the 3-stage pipeline (same as pipeline_classify_electoral.py)
            results = run_electoral_pipeline(client, chunk_questions, show_progress=True)

            # Save results to checkpoint (keyed by stable ID)
            for result in results:
                local_idx = result["index"]
                if local_idx < len(chunk_ids):
                    stable_id, df_idx = chunk_ids[local_idx]

                    already_processed[stable_id] = {
                        "df_index": df_idx,
                        "country": result.get("country"),
                        "office": result.get("office"),
                        "location": result.get("location"),
                        "election_year": result.get("election_year"),
                        "is_primary": result.get("is_primary"),
                        "party": result.get("party"),
                        "confidence": result.get("confidence", 0),
                        "stage": result.get("stage", 0)
                    }

            checkpoint["processed"] = already_processed
            save_checkpoint(checkpoint)
            log(f"  Checkpoint saved: {len(already_processed)} total processed")

        elapsed = time.time() - start_time
        log(f"\nClassification completed in {elapsed/60:.1f} minutes")

    # =========================================================================
    # STEP 5: Apply results to master CSV
    # =========================================================================

    log("\nApplying classification results to master CSV...")

    fields = ["country", "office", "location", "election_year", "is_primary", "party"]
    changes_made = 0
    still_incomplete_count = 0
    change_log = []

    for stable_id, data in already_processed.items():
        # Look up current DataFrame index by stable ID
        if stable_id in id_to_idx:
            idx = id_to_idx[stable_id]
        else:
            # Fallback: use stored df_index (same session, index hasn't shifted)
            idx = data.get("df_index")
            if idx is None or idx not in master_df.index:
                continue

        row_changed = False
        new_values = {}

        for field in fields:
            if field not in master_df.columns:
                master_df[field] = None

            old_val = master_df.loc[idx, field]
            new_val = data.get(field)

            # Only overwrite if the field is currently empty and we have a new value
            if pd.isna(old_val) and new_val is not None:
                master_df.loc[idx, field] = new_val
                new_values[field] = new_val
                row_changed = True

        # Check if this market is still incomplete after applying results
        still_missing = any(
            pd.isna(master_df.loc[idx, f]) if f in master_df.columns else True
            for f in ["country", "office", "location", "election_year", "is_primary"]
        )

        if still_missing:
            # Mark as attempted so future runs skip it
            master_df.loc[idx, 'reclassify_attempted'] = True
            still_incomplete_count += 1

        if row_changed:
            changes_made += 1
            change_log.append({
                "stable_id": stable_id,
                "platform": master_df.loc[idx, 'platform'],
                "question": str(master_df.loc[idx, 'question'])[:100],
                "market_id": master_df.loc[idx, 'market_id'] if 'market_id' in master_df.columns else None,
                "still_incomplete": still_missing,
                **{f"new_{f}": new_values.get(f) for f in fields},
                "confidence": data.get("confidence", 0),
                "stage": data.get("stage", 0),
            })

    log(f"  Updated {changes_made:,} markets with new metadata")
    log(f"  Marked {still_incomplete_count:,} as unclassifiable (reclassify_attempted=True)")

    # =========================================================================
    # STEP 6: Derive election_type for updated rows
    # =========================================================================

    log("\nDeriving election_type for updated rows...")

    type_set_count = 0
    for stable_id, data in already_processed.items():
        if stable_id in id_to_idx:
            idx = id_to_idx[stable_id]
        else:
            idx = data.get("df_index")
            if idx is None or idx not in master_df.index:
                continue

        row = master_df.loc[idx]
        new_type = derive_election_type(row)
        if new_type is not None:
            master_df.loc[idx, 'election_type'] = new_type
            type_set_count += 1

    log(f"  Set election_type for {type_set_count:,} markets")

    # Convert types
    if 'election_year' in master_df.columns:
        master_df['election_year'] = pd.to_numeric(master_df['election_year'], errors='coerce')

    # =========================================================================
    # STEP 7: Save updated master CSV
    # =========================================================================

    log("\nSaving updated master CSV...")
    master_df.to_csv(MASTER_FILE, index=False)
    log(f"  Saved {len(master_df):,} total markets")

    # Save audit report
    if change_log:
        report_df = pd.DataFrame(change_log)
        report_df.to_csv(REPORT_FILE, index=False)
        log(f"  Saved audit report: {REPORT_FILE.name} ({len(report_df):,} changes)")

    # =========================================================================
    # STEP 7b: Backfill party affiliations via web search
    # =========================================================================

    backfill_party_affiliations()

    # =========================================================================
    # STEP 8: Summary
    # =========================================================================

    # Re-check: markets that are incomplete AND not yet attempted
    remaining = find_incomplete_electoral(master_df)

    print("\n" + "=" * 70)
    print("RECLASSIFICATION COMPLETE")
    print("=" * 70)
    print(f"Markets updated with new metadata: {changes_made:,}")
    print(f"Markets marked unclassifiable:     {still_incomplete_count:,}")
    print(f"Remaining (not yet attempted):     {len(remaining):,}")
    print(f"Backup: {backup_file.name}")
    if change_log:
        print(f"Audit report: {REPORT_FILE.name}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    # Clean up checkpoint once all markets have been attempted
    if len(remaining) == 0 and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        log("Checkpoint cleaned up (all markets processed)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
