#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Merge New Markets to Master CSV
================================================================================

Part of the NEW Bellwether Pipeline (January 2026+)

This script:
1. Reads classified markets from BOTH:
   - pipeline_classify_electoral.py output (electoral markets with details)
   - pipeline_classify_categories.py output (all political categories)
2. Compares against existing master CSV
3. Adds NEW markets to master CSV with all fields
4. Updates market ID index for future lookups

Usage:
    python pipeline_merge_to_master.py

Input:
    - data/new_markets_electoral_details.csv (electoral with country/office/location/year)
    - data/new_markets_classified.csv (all political categories: LEGISLATIVE, REGULATORY, etc.)
    - data/combined_political_markets_with_electoral_details_UPDATED.csv (master)

Output:
    - data/combined_political_markets_with_electoral_details_UPDATED.csv (updated)
    - data/market_id_index.json (updated)

================================================================================
"""

import pandas as pd
import json
import sys
from datetime import datetime
from pathlib import Path

# Add scripts dir to path for config import
sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR, rotate_backups

# Import audit system
try:
    from audit.audit_validator import DataValidator
    from audit.audit_changelog import ChangelogTracker
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False

# Input/Output files
# Electoral markets have detailed fields (country, office, location, year, election_date)
ELECTORAL_MARKETS_FILE = DATA_DIR / "new_markets_electoral_details.csv"
# All classified markets (including non-electoral: LEGISLATIVE, REGULATORY, etc.)
CLASSIFIED_MARKETS_FILE = DATA_DIR / "new_markets_classified.csv"
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
INDEX_FILE = DATA_DIR / "market_id_index.json"
BACKUP_DIR = DATA_DIR / "backups"


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def main():
    """Main function to merge new markets to master CSV."""
    print("\n" + "=" * 70)
    print("PIPELINE: MERGE NEW MARKETS TO MASTER CSV")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    # Load new markets from BOTH files:
    # 1. Electoral markets (with detailed fields)
    # 2. Non-electoral markets (LEGISLATIVE, REGULATORY, etc.)

    electoral_df = None
    classified_df = None

    if ELECTORAL_MARKETS_FILE.exists():
        electoral_df = pd.read_csv(ELECTORAL_MARKETS_FILE)
        log(f"Loaded {len(electoral_df):,} electoral markets from {ELECTORAL_MARKETS_FILE.name}")

    if CLASSIFIED_MARKETS_FILE.exists():
        classified_df = pd.read_csv(CLASSIFIED_MARKETS_FILE)
        log(f"Loaded {len(classified_df):,} classified markets from {CLASSIFIED_MARKETS_FILE.name}")

    if electoral_df is None and classified_df is None:
        log("No new market files found. Nothing to merge.")
        return 0

    # Combine: Use electoral_df for electoral markets (has more detail),
    # and classified_df for non-electoral markets
    if electoral_df is not None and classified_df is not None:
        # Get IDs of electoral markets
        electoral_ids = set()
        if 'pm_condition_id' in electoral_df.columns:
            electoral_ids.update(electoral_df['pm_condition_id'].dropna().astype(str).tolist())
        if 'market_id' in electoral_df.columns:
            electoral_ids.update(electoral_df['market_id'].dropna().astype(str).tolist())

        # Filter classified_df to only non-electoral markets
        def is_non_electoral(row):
            pm_cid = str(row.get('pm_condition_id', '')) if pd.notna(row.get('pm_condition_id')) else ''
            mkt_id = str(row.get('market_id', '')) if pd.notna(row.get('market_id')) else ''
            # Keep if not in electoral set AND not category 1. ELECTORAL
            category = str(row.get('political_category', ''))
            return (pm_cid not in electoral_ids and mkt_id not in electoral_ids and
                    not category.startswith('1.'))

        non_electoral_df = classified_df[classified_df.apply(is_non_electoral, axis=1)]
        log(f"  Non-electoral markets to merge: {len(non_electoral_df):,}")

        # Combine: electoral + non-electoral
        new_df = pd.concat([electoral_df, non_electoral_df], ignore_index=True)
        log(f"  Combined total: {len(new_df):,}")

    elif electoral_df is not None:
        new_df = electoral_df
    else:
        new_df = classified_df

    log(f"Total new markets to merge: {len(new_df):,}")

    if len(new_df) == 0:
        log("No new markets to merge.")
        return 0

    # Load master CSV
    log("Loading master CSV...")
    if MASTER_FILE.exists():
        master_df = pd.read_csv(MASTER_FILE, low_memory=False)
        log(f"  Existing master markets: {len(master_df):,}")
    else:
        master_df = pd.DataFrame()
        log("  No existing master - creating new")

    # Create backup
    BACKUP_DIR.mkdir(exist_ok=True)
    if len(master_df) > 0:
        backup_file = BACKUP_DIR / f"master_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        master_df.to_csv(backup_file, index=False)
        log(f"  Created backup: {backup_file.name}")

        # Rotate old backups (keep last 5)
        deleted = rotate_backups("master_backup_*.csv")
        if deleted > 0:
            log(f"  Rotated {deleted} old backup(s)")

    # Get existing market IDs
    existing_pm_ids = set()
    existing_kalshi_ids = set()

    if len(master_df) > 0:
        # Polymarket: use pm_condition_id
        pm_master = master_df[master_df['platform'] == 'Polymarket']
        if 'pm_condition_id' in pm_master.columns:
            existing_pm_ids = set(pm_master['pm_condition_id'].dropna().astype(str).tolist())

        # Also check market_id
        if 'market_id' in pm_master.columns:
            existing_pm_ids.update(pm_master['market_id'].dropna().astype(str).tolist())

        # Kalshi: use market_id
        kalshi_master = master_df[master_df['platform'] == 'Kalshi']
        if 'market_id' in kalshi_master.columns:
            existing_kalshi_ids = set(kalshi_master['market_id'].dropna().astype(str).tolist())

    log(f"  Existing Polymarket IDs: {len(existing_pm_ids):,}")
    log(f"  Existing Kalshi IDs: {len(existing_kalshi_ids):,}")

    # Filter new markets to only truly new ones
    truly_new = []

    for idx, row in new_df.iterrows():
        platform = row.get('platform', '')

        if platform == 'Polymarket':
            # Check pm_condition_id or market_id
            pm_cid = str(row.get('pm_condition_id', ''))
            mkt_id = str(row.get('market_id', ''))

            if pm_cid not in existing_pm_ids and mkt_id not in existing_pm_ids:
                truly_new.append(row)

        elif platform == 'Kalshi':
            mkt_id = str(row.get('market_id', ''))

            if mkt_id not in existing_kalshi_ids:
                truly_new.append(row)

        else:
            # Unknown platform - add anyway
            truly_new.append(row)

    log(f"  Truly new markets: {len(truly_new):,}")

    if len(truly_new) == 0:
        log("All markets already exist in master - nothing to add.")
        return 0

    # Convert to DataFrame
    new_to_add = pd.DataFrame(truly_new)

    # Map 'party' column to 'party_affiliation' for master CSV consistency
    if 'party' in new_to_add.columns:
        new_to_add['party_affiliation'] = new_to_add['party']
        new_to_add = new_to_add.drop(columns=['party'])
        log(f"  Mapped 'party' → 'party_affiliation'")

    # Ensure columns match master (but preserve new columns)
    if len(master_df) > 0:
        # Add any missing columns from master to new data
        for col in master_df.columns:
            if col not in new_to_add.columns:
                new_to_add[col] = None

        # Check for new columns that don't exist in master
        new_cols = [col for col in new_to_add.columns if col not in master_df.columns]
        if new_cols:
            log(f"  New columns: {new_cols}")
            # Add these columns to master_df so they're preserved
            for col in new_cols:
                master_df[col] = None

        # Reorder to put master columns first, then new columns
        all_cols = list(master_df.columns)
        new_to_add = new_to_add[all_cols]

    # Stamp new rows with date_added for audit trail
    new_to_add['date_added'] = datetime.now().strftime('%Y-%m-%d')

    # Track new markets in changelog (if audit available)
    if AUDIT_AVAILABLE:
        try:
            changelog = ChangelogTracker()
            for _, row in new_to_add.iterrows():
                market_id = row.get('market_id', row.get('pm_condition_id', 'unknown'))
                changelog.log_event(
                    event_type='market_added',
                    market_id=str(market_id),
                    details={
                        'platform': row.get('platform', 'unknown'),
                        'category': row.get('political_category', 'unknown'),
                        'question': str(row.get('question', ''))[:100]
                    }
                )
            changelog.save()
            log(f"  Logged {len(new_to_add)} new markets to changelog")
        except Exception as e:
            log(f"  Warning: Failed to log to changelog: {e}")

    # Append to master
    updated_master = pd.concat([master_df, new_to_add], ignore_index=True)

    # Normalize winning_outcome casing (Yes/No)
    if 'winning_outcome' in updated_master.columns:
        wo = updated_master['winning_outcome'].astype(str).str.lower()
        updated_master.loc[wo == 'yes', 'winning_outcome'] = 'Yes'
        updated_master.loc[wo == 'no', 'winning_outcome'] = 'No'

    # Save updated master
    log("\nSaving updated master CSV...")
    updated_master.to_csv(MASTER_FILE, index=False)
    log(f"  Saved {len(updated_master):,} total markets")

    # Update market ID index
    log("Updating market ID index...")
    if INDEX_FILE.exists():
        with open(INDEX_FILE, 'r') as f:
            index = json.load(f)
    else:
        index = {"polymarket": [], "kalshi": []}

    # Add new IDs
    for _, row in new_to_add.iterrows():
        platform = row.get('platform', '')

        if platform == 'Polymarket':
            pm_cid = str(row.get('pm_condition_id', ''))
            if pm_cid and pm_cid != 'nan' and pm_cid not in index['polymarket']:
                index['polymarket'].append(pm_cid)

        elif platform == 'Kalshi':
            mkt_id = str(row.get('market_id', ''))
            if mkt_id and mkt_id != 'nan' and mkt_id not in index['kalshi']:
                index['kalshi'].append(mkt_id)

    index['last_updated'] = datetime.now().isoformat()

    with open(INDEX_FILE, 'w') as f:
        json.dump(index, f, indent=2)

    log(f"  Updated index: {len(index['polymarket']):,} PM, {len(index['kalshi']):,} Kalshi")

    # Summary by platform
    pm_added = len(new_to_add[new_to_add['platform'] == 'Polymarket'])
    kalshi_added = len(new_to_add[new_to_add['platform'] == 'Kalshi'])

    # Summary
    print("\n" + "=" * 70)
    print("MERGE COMPLETE")
    print("=" * 70)
    print(f"New markets added: {len(new_to_add):,}")
    print(f"  Polymarket: {pm_added:,}")
    print(f"  Kalshi: {kalshi_added:,}")
    print(f"Total in master: {len(updated_master):,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return len(new_to_add)


if __name__ == "__main__":
    main()
