#!/usr/bin/env python3
"""
Classify New Markets with OpenAI

Classifies new electoral markets that haven't been labeled yet.
Uses the two-stage OpenAI pipeline for accurate winner market detection.
"""

import pandas as pd
import os
import sys
from datetime import datetime

# Add scripts to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.openai_classifier import run_pipeline

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
LABELED_FILE = f"{DATA_DIR}/us_electoral_markets_labeled.csv"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def main():
    log("="*60)
    log("CLASSIFYING NEW ELECTORAL MARKETS")
    log("="*60)

    # Check for API key
    if not os.getenv('OPENAI_API_KEY'):
        log("⚠ OPENAI_API_KEY not set, skipping classification")
        return

    # Load master data
    log("Loading master data...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)

    # Filter to US electoral markets with vote shares
    electoral = df[df['political_category'].str.contains('ELECTORAL', na=False)]
    us_electoral = electoral[
        (electoral['country'] == 'United States') &
        (electoral['democrat_vote_share'].notna())
    ].copy()

    log(f"US electoral markets with vote shares: {len(us_electoral)}")

    # Load already labeled markets
    if os.path.exists(LABELED_FILE):
        labeled = pd.read_csv(LABELED_FILE)
        labeled_ids = set(labeled['market_id'].astype(str))
        log(f"Already labeled: {len(labeled_ids)} markets")
    else:
        labeled_ids = set()
        labeled = pd.DataFrame()
        log("No existing labeled file found")

    # Find new markets
    us_electoral['market_id'] = us_electoral['market_id'].astype(str)
    new_markets = us_electoral[~us_electoral['market_id'].isin(labeled_ids)]

    if len(new_markets) == 0:
        log("No new markets to classify")
        return

    log(f"New markets to classify: {len(new_markets)}")

    # Prepare items for classification
    items = new_markets[['market_id', 'question', 'platform', 'office', 'location', 'election_year']].to_dict('records')

    # Run classification
    results = run_pipeline(
        items=items,
        question_key="question",
        batch_size=50,
        model="gpt-4o-mini",
        delay=0.05,
        show_progress=True
    )

    # Add results to dataframe
    new_markets = new_markets.copy()
    new_markets['is_winner_market'] = [r['is_winner_market'] for r in results]
    new_markets['market_party'] = [r['party'] for r in results]
    new_markets['classification_confidence'] = [r['confidence'] for r in results]
    new_markets['stage1_result'] = [r['s1'] for r in results]
    new_markets['stage2_result'] = [r['s2'] for r in results]
    new_markets['stage3_result'] = [r['s3'] for r in results]
    new_markets['classification_votes'] = [r['votes'] for r in results]

    # Combine with existing labeled data
    if len(labeled) > 0:
        # Ensure columns match
        for col in new_markets.columns:
            if col not in labeled.columns:
                labeled[col] = None
        for col in labeled.columns:
            if col not in new_markets.columns:
                new_markets[col] = None

        combined = pd.concat([labeled, new_markets], ignore_index=True)
    else:
        combined = new_markets

    # Save
    combined.to_csv(LABELED_FILE, index=False)

    # Summary
    winner_count = new_markets['is_winner_market'].sum()
    log("="*60)
    log(f"CLASSIFICATION COMPLETE")
    log(f"  New markets classified: {len(new_markets)}")
    log(f"  Winner markets found: {winner_count}")
    log(f"  Total labeled markets: {len(combined)}")
    log(f"  Saved to: {LABELED_FILE}")
    log("="*60)

if __name__ == "__main__":
    main()
