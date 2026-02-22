#!/usr/bin/env python3
"""
One-time migration script to relabel US electoral markets using OpenAI.

This replaces the old regex-based scoring with the two-stage OpenAI pipeline.
Run this once before launching the dashboard.
"""

import pandas as pd
import os
import sys
from datetime import datetime

# Add scripts to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.openai_classifier import run_pipeline

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
OUTPUT_FILE = f"{DATA_DIR}/us_electoral_markets_labeled.csv"

def main():
    print("="*60)
    print("RELABELING US ELECTORAL MARKETS WITH OPENAI")
    print("="*60)

    # Check for API key
    if not os.getenv('OPENAI_API_KEY'):
        print("\nERROR: OPENAI_API_KEY environment variable not set")
        print("Run: export OPENAI_API_KEY='your-key-here'")
        sys.exit(1)

    # Load master data
    print("\n1. Loading master data...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    print(f"   Total markets: {len(df)}")

    # Filter to US electoral markets with vote shares
    print("\n2. Filtering to US electoral markets with vote shares...")
    electoral = df[df['political_category'].str.contains('ELECTORAL', na=False)]
    us_electoral = electoral[
        (electoral['country'] == 'United States') &
        (electoral['democrat_vote_share'].notna())
    ].copy()

    print(f"   US electoral with vote shares: {len(us_electoral)}")
    print(f"     Polymarket: {len(us_electoral[us_electoral['platform']=='Polymarket'])}")
    print(f"     Kalshi: {len(us_electoral[us_electoral['platform']=='Kalshi'])}")

    # Prepare items for classification
    items = us_electoral[['market_id', 'question', 'platform', 'office', 'location', 'election_year']].to_dict('records')

    # Run classification pipeline
    print("\n3. Running OpenAI classification pipeline...")
    results = run_pipeline(
        items=items,
        question_key="question",
        batch_size=50,
        model="gpt-4o-mini",
        delay=0.05,
        show_progress=True
    )

    # Add results to dataframe
    print("\n4. Adding classification results to dataframe...")
    us_electoral['is_winner_market'] = [r['is_winner_market'] for r in results]
    us_electoral['market_party'] = [r['party'] for r in results]
    us_electoral['classification_confidence'] = [r['confidence'] for r in results]
    us_electoral['stage1_result'] = [r['s1'] for r in results]
    us_electoral['stage2_result'] = [r['s2'] for r in results]
    us_electoral['stage3_result'] = [r['s3'] for r in results]
    us_electoral['classification_votes'] = [r['votes'] for r in results]

    # Summary stats
    winner_markets = us_electoral[us_electoral['is_winner_market']]
    print(f"\n5. Results summary:")
    print(f"   Total markets classified: {len(us_electoral)}")
    print(f"   Winner markets identified: {len(winner_markets)}")
    print(f"     Polymarket: {len(winner_markets[winner_markets['platform']=='Polymarket'])}")
    print(f"     Kalshi: {len(winner_markets[winner_markets['platform']=='Kalshi'])}")
    print(f"   Party breakdown:")
    print(f"     Republican: {(winner_markets['market_party']=='Republican').sum()}")
    print(f"     Democrat: {(winner_markets['market_party']=='Democrat').sum()}")
    print(f"     Unknown: {winner_markets['market_party'].isna().sum()}")

    # Save results
    print(f"\n6. Saving to {OUTPUT_FILE}...")
    us_electoral.to_csv(OUTPUT_FILE, index=False)

    # Also create backup of original
    backup_file = f"{DATA_DIR}/us_electoral_markets_labeled_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    us_electoral.to_csv(backup_file, index=False)

    print(f"\n{'='*60}")
    print("DONE!")
    print(f"{'='*60}")
    print(f"\nOutput: {OUTPUT_FILE}")
    print(f"Backup: {backup_file}")

    # Show some examples
    print("\n\nSample winner markets:")
    print("-"*60)
    for _, row in winner_markets.head(10).iterrows():
        print(f"  [{row['platform']}] {row['question'][:70]}...")
        print(f"     Party: {row['market_party']}, Confidence: {row['classification_confidence']:.2f}")

    print("\n\nSample NON-winner markets (excluded):")
    print("-"*60)
    non_winners = us_electoral[~us_electoral['is_winner_market']]
    for _, row in non_winners.head(5).iterrows():
        print(f"  [{row['platform']}] {row['question'][:70]}...")

if __name__ == "__main__":
    main()
