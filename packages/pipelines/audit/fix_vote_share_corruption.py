#!/usr/bin/env python3
"""
================================================================================
FIX VOTE SHARE CORRUPTION
================================================================================

One-time migration script to fix vote share values that were stored as
percentages (0-100) instead of proportions (0-1).

This script:
1. Backs up the master CSV
2. Identifies all vote share values > 1.0 (indicating percentage storage)
3. Divides those values by 100 to convert to proportions
4. Saves the corrected data
5. Generates a report of all changes

Usage:
    python scripts/audit/fix_vote_share_corruption.py [--dry-run]

Options:
    --dry-run   Show what would be changed without modifying the file

================================================================================
"""

import pandas as pd
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import BASE_DIR, DATA_DIR, rotate_backups

MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
BACKUP_DIR = DATA_DIR / "backups"
REPORT_DIR = DATA_DIR / "audit" / "validation"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def main():
    dry_run = '--dry-run' in sys.argv

    print("\n" + "=" * 70)
    print("FIX VOTE SHARE CORRUPTION")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if dry_run:
        print("MODE: DRY RUN (no changes will be made)")
    print("=" * 70 + "\n")

    # Load master CSV
    log("Loading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"  Total markets: {len(df):,}")

    # Identify corrupted vote shares (values > 1.0)
    vote_share_cols = ['democrat_vote_share', 'republican_vote_share']

    changes = []
    for col in vote_share_cols:
        if col not in df.columns:
            log(f"  Column {col} not found, skipping")
            continue

        # Find rows where value > 1.0 (indicates percentage storage)
        corrupted_mask = df[col].notna() & (df[col] > 1.0)
        corrupted_count = corrupted_mask.sum()

        if corrupted_count == 0:
            log(f"  {col}: No corruption found (all values <= 1.0)")
            continue

        log(f"  {col}: Found {corrupted_count:,} corrupted values")

        # Record changes
        for idx in df[corrupted_mask].index:
            old_val = df.loc[idx, col]
            new_val = old_val / 100
            changes.append({
                'market_id': df.loc[idx, 'market_id'],
                'column': col,
                'old_value': old_val,
                'new_value': new_val,
                'question': df.loc[idx, 'question'][:100] if pd.notna(df.loc[idx, 'question']) else ''
            })

            if not dry_run:
                df.loc[idx, col] = new_val

    # Summary
    log(f"\nTotal changes: {len(changes):,}")

    if len(changes) == 0:
        log("No corruption found. Nothing to fix.")
        return 0

    # Show sample of changes
    log("\nSample changes:")
    for change in changes[:10]:
        log(f"  {change['market_id']}: {change['column']} "
            f"{change['old_value']:.2f} -> {change['new_value']:.4f}")
    if len(changes) > 10:
        log(f"  ... and {len(changes) - 10} more")

    if dry_run:
        log("\nDRY RUN: No changes made.")
        return 0

    # Create backup
    log("\nCreating backup...")
    BACKUP_DIR.mkdir(exist_ok=True)
    backup_file = BACKUP_DIR / f"master_backup_vote_share_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    original_df = pd.read_csv(MASTER_FILE, low_memory=False)
    original_df.to_csv(backup_file, index=False)
    log(f"  Backup saved: {backup_file.name}")

    # Rotate old backups
    deleted = rotate_backups("master_backup_vote_share_fix_*.csv")
    if deleted > 0:
        log(f"  Rotated {deleted} old backup(s)")

    # Save corrected data
    log("\nSaving corrected master CSV...")
    df.to_csv(MASTER_FILE, index=False)
    log("  Saved.")

    # Save change report
    log("\nSaving change report...")
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_file = REPORT_DIR / f"vote_share_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    changes_df = pd.DataFrame(changes)
    changes_df.to_csv(report_file, index=False)
    log(f"  Report saved: {report_file.name}")

    # Verify fix
    log("\nVerifying fix...")
    df_verify = pd.read_csv(MASTER_FILE, low_memory=False)
    for col in vote_share_cols:
        if col not in df_verify.columns:
            continue
        remaining = (df_verify[col].notna() & (df_verify[col] > 1.0)).sum()
        if remaining > 0:
            log(f"  WARNING: {col} still has {remaining} values > 1.0")
        else:
            max_val = df_verify[col].max()
            log(f"  {col}: OK (max value: {max_val:.4f})")

    print("\n" + "=" * 70)
    print("VOTE SHARE CORRUPTION FIX COMPLETE")
    print("=" * 70)
    print(f"Fixed: {len(changes):,} values")
    print(f"Backup: {backup_file.name}")
    print(f"Report: {report_file.name}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return len(changes)


if __name__ == "__main__":
    result = main()
    sys.exit(0 if result >= 0 else 1)
