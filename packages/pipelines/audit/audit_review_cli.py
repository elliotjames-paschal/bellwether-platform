#!/usr/bin/env python3
"""
================================================================================
AUDIT REVIEW CLI
================================================================================

Simple CLI for reviewing audit results and flagged items.
Designed for use with Claude Code - generates reports that can be
reviewed and fixed directly.

Usage:
    python -m scripts.audit.audit_review_cli [command]

Commands:
    status      Show overall audit status
    validate    Run validation and show results
    flagged     Show markets flagged for review
    duplicates  Show duplicate vote share issues
    changelog   Show recent changes
"""

import sys
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from audit.audit_config import (
    MASTER_FILE,
    VALIDATION_DIR,
    CHANGELOGS_DIR,
    REVIEW_QUEUE_DIR,
    ANOMALIES_DIR,
)
from audit.audit_validator import DataValidator


def cmd_status():
    """Show overall audit status."""
    print("\n" + "=" * 60)
    print("BELLWETHER AUDIT STATUS")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Load master data
    print("Loading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    print(f"  Total markets: {len(df):,}")
    print(f"  Platforms: {df['platform'].value_counts().to_dict()}")

    # Count by category
    print("\n  By category:")
    for cat, count in df['political_category'].value_counts().head(5).items():
        print(f"    {cat}: {count:,}")

    # Vote share coverage
    has_vote_share = df['democrat_vote_share'].notna().sum()
    print(f"\n  Vote share coverage: {has_vote_share:,} markets ({has_vote_share/len(df)*100:.1f}%)")

    # Check for corruption
    if 'democrat_vote_share' in df.columns:
        corrupted = (df['democrat_vote_share'] > 1).sum()
        if corrupted > 0:
            print(f"  ⚠️  CORRUPTION: {corrupted} vote shares > 1.0")
        else:
            print("  ✓ Vote shares in valid range (0-1)")

    # Recent validation reports
    print("\n  Recent validation reports:")
    val_files = sorted(VALIDATION_DIR.glob("*.json"), reverse=True)[:3]
    for f in val_files:
        with open(f) as fp:
            report = json.load(fp)
        status = report.get("status", "?")
        print(f"    {f.name}: {status}")

    # Recent changelogs
    print("\n  Recent changelogs:")
    log_files = sorted(CHANGELOGS_DIR.glob("*_changelog.json"), reverse=True)[:3]
    for f in log_files:
        with open(f) as fp:
            log = json.load(fp)
        summary = log.get("summary", {})
        added = summary.get("markets_added", 0)
        print(f"    {f.name}: {added} markets added")

    print("\n" + "=" * 60)


def cmd_validate():
    """Run validation and show results."""
    print("\n" + "=" * 60)
    print("RUNNING VALIDATION")
    print("=" * 60)

    validator = DataValidator()

    print("\nLoading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    print(f"  Total markets: {len(df):,}")

    print("\nRunning validation checks...")
    report = validator.validate_master_csv(df)

    report.print_summary()

    # Save report
    filepath = report.save()
    print(f"\nReport saved: {filepath}")

    print("\n" + "=" * 60)

    return report


def cmd_flagged():
    """Show markets flagged for review."""
    print("\n" + "=" * 60)
    print("FLAGGED MARKETS FOR REVIEW")
    print("=" * 60)

    # Run validation to get current issues
    validator = DataValidator()
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    report = validator.validate_master_csv(df)

    flagged_count = 0

    for issue in report.issues:
        print(f"\n[{issue.level}] {issue.rule}")
        print(f"  {issue.message}")
        if issue.sample_ids:
            print(f"  Sample IDs: {issue.sample_ids[:5]}")
            flagged_count += len(issue.sample_ids)

    if flagged_count == 0:
        print("\n  No markets flagged for review.")
    else:
        print(f"\n  Total flagged: {flagged_count}")

    print("\n" + "=" * 60)


def cmd_duplicates():
    """Show duplicate vote share issues in detail."""
    print("\n" + "=" * 60)
    print("DUPLICATE VOTE SHARE ANALYSIS")
    print("=" * 60)

    df = pd.read_csv(MASTER_FILE, low_memory=False)

    # Filter to rows with vote shares
    has_shares = df['democrat_vote_share'].notna() & df['republican_vote_share'].notna()
    vote_df = df[has_shares].copy()

    print(f"\nMarkets with vote shares: {len(vote_df):,}")

    # Create election key
    election_cols = ['country', 'office', 'location', 'election_year', 'is_primary']
    vote_df['election_key'] = vote_df[election_cols].astype(str).agg('|'.join, axis=1)

    # Get unique elections
    elections = vote_df.groupby('election_key').agg({
        'democrat_vote_share': 'first',
        'republican_vote_share': 'first',
        'market_id': 'count'
    }).reset_index()
    elections.columns = ['election_key', 'd_share', 'r_share', 'market_count']

    print(f"Unique elections with vote shares: {len(elections):,}")

    # Create share pair key
    elections['share_pair'] = (
        elections['d_share'].round(4).astype(str) + '|' +
        elections['r_share'].round(4).astype(str)
    )

    # Find duplicates
    share_counts = elections['share_pair'].value_counts()
    duplicates = share_counts[share_counts >= 3]

    print(f"\nVote share pairs appearing in 3+ elections: {len(duplicates)}")

    if len(duplicates) == 0:
        print("  No duplicate issues found.")
    else:
        print("\nDuplicate groups:")
        for share_pair, count in duplicates.items():
            d_share, r_share = share_pair.split('|')
            print(f"\n  D:{d_share} R:{r_share} - {count} elections:")

            matching = elections[elections['share_pair'] == share_pair]
            for _, row in matching.head(10).iterrows():
                parts = row['election_key'].split('|')
                desc = f"{parts[3]} {parts[1]} - {parts[2]}"  # year office - location
                print(f"    - {desc}")
            if len(matching) > 10:
                print(f"    ... and {len(matching) - 10} more")

    # Export for fixing
    if len(duplicates) > 0:
        export_path = REVIEW_QUEUE_DIR / f"{datetime.now().strftime('%Y-%m-%d')}_duplicate_vote_shares.csv"
        REVIEW_QUEUE_DIR.mkdir(parents=True, exist_ok=True)

        export_rows = []
        for share_pair in duplicates.index:
            matching = elections[elections['share_pair'] == share_pair]
            for _, row in matching.iterrows():
                export_rows.append({
                    'election_key': row['election_key'],
                    'democrat_vote_share': row['d_share'],
                    'republican_vote_share': row['r_share'],
                    'market_count': row['market_count'],
                    'share_pair': share_pair,
                    'duplicate_count': duplicates[share_pair]
                })

        export_df = pd.DataFrame(export_rows)
        export_df.to_csv(export_path, index=False)
        print(f"\nExported to: {export_path}")

    print("\n" + "=" * 60)


def cmd_changelog(days: int = 7):
    """Show recent changes."""
    print("\n" + "=" * 60)
    print(f"RECENT CHANGES (last {days} days)")
    print("=" * 60)

    log_files = sorted(CHANGELOGS_DIR.glob("*_changelog.json"), reverse=True)

    if not log_files:
        print("\n  No changelogs found.")
        print("\n" + "=" * 60)
        return

    cutoff = datetime.now() - timedelta(days=days)

    for log_file in log_files[:days]:
        try:
            with open(log_file) as f:
                log = json.load(f)

            run_date = log.get("run_date", "?")
            summary = log.get("summary", {})

            print(f"\n{run_date}:")
            for key, value in summary.items():
                if value > 0:
                    print(f"  {key}: {value:,}")

        except Exception as e:
            print(f"\n  Error reading {log_file.name}: {e}")

    print("\n" + "=" * 60)


def cmd_help():
    """Show help."""
    print(__doc__)


def main():
    commands = {
        "status": cmd_status,
        "validate": cmd_validate,
        "flagged": cmd_flagged,
        "duplicates": cmd_duplicates,
        "changelog": cmd_changelog,
        "help": cmd_help,
    }

    if len(sys.argv) < 2:
        cmd_status()
        return

    cmd = sys.argv[1].lower()

    if cmd in commands:
        commands[cmd]()
    else:
        print(f"Unknown command: {cmd}")
        print(f"Available commands: {', '.join(commands.keys())}")


if __name__ == "__main__":
    main()
