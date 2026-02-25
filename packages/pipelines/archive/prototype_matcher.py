#!/usr/bin/env python3
"""
================================================================================
PROTOTYPE: Bellwether Market Matcher Testing
================================================================================

Tests the NLP-based market matcher on a diverse subset of ~500 markets.
Use this to validate extraction quality and match accuracy before scaling
to the full 34k dataset.

Usage:
    python prototype_matcher.py
    python prototype_matcher.py --sample-size 200
    python prototype_matcher.py --verbose

Output:
    - Console report with extraction quality metrics
    - data/prototype_matches.json with detailed results
    - Sample of extracted frames for manual review

================================================================================
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from collections import Counter

import pandas as pd

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR

from bellwether_matcher.extractor import extract_frame, load_nlp
from bellwether_matcher.matcher import match_markets, validate_match
from bellwether_matcher.taxonomy import generate_beid, get_race_beid


# Files
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
OUTPUT_FILE = DATA_DIR / "prototype_matches.json"


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def select_diverse_sample(df: pd.DataFrame, n: int = 500, min_per_category: int = 15) -> pd.DataFrame:
    """
    Select a stratified sample of markets for prototype testing.

    Strategy:
    1. Stratified sampling across political categories
    2. Proportional to category size, with floor of min_per_category
    3. Even split from both platforms within each category
    4. Prioritize categories where both platforms have coverage (matchable)

    Args:
        df: Full market DataFrame
        n: Target sample size
        min_per_category: Minimum markets per category (floor)

    Returns:
        Stratified sample DataFrame
    """
    samples = []

    # Analyze category coverage by platform
    categories = df['political_category'].dropna().unique()
    category_stats = []

    for category in categories:
        cat_df = df[df['political_category'] == category]
        kalshi_count = len(cat_df[cat_df['platform'] == 'Kalshi'])
        pm_count = len(cat_df[cat_df['platform'] == 'Polymarket'])
        total = len(cat_df)
        has_both = kalshi_count > 0 and pm_count > 0

        category_stats.append({
            'category': category,
            'total': total,
            'kalshi': kalshi_count,
            'polymarket': pm_count,
            'has_both': has_both,
        })

    # Sort: categories with both platforms first, then by size
    category_stats.sort(key=lambda x: (-x['has_both'], -x['total']))

    # Calculate proportional allocation
    total_markets = sum(c['total'] for c in category_stats)
    allocations = []

    for cat_stat in category_stats:
        # Proportional share, but with floor
        proportional = int(n * cat_stat['total'] / total_markets)
        allocated = max(min_per_category, proportional)
        allocations.append({
            **cat_stat,
            'allocated': allocated,
        })

    # Adjust if we over-allocated (due to floors)
    total_allocated = sum(a['allocated'] for a in allocations)
    if total_allocated > n:
        # Scale down proportionally, keeping floors
        excess = total_allocated - n
        # Remove from largest categories first
        allocations.sort(key=lambda x: -x['allocated'])
        for alloc in allocations:
            if alloc['allocated'] > min_per_category and excess > 0:
                reduction = min(alloc['allocated'] - min_per_category, excess)
                alloc['allocated'] -= reduction
                excess -= reduction

    # Sample from each category
    for alloc in allocations:
        category = alloc['category']
        target = alloc['allocated']
        cat_df = df[df['political_category'] == category]

        kalshi_df = cat_df[cat_df['platform'] == 'Kalshi']
        pm_df = cat_df[cat_df['platform'] == 'Polymarket']

        # Split evenly between platforms (when both have coverage)
        if len(kalshi_df) > 0 and len(pm_df) > 0:
            per_platform = target // 2
            remainder = target % 2

            k_sample = min(per_platform, len(kalshi_df))
            pm_sample = min(per_platform + remainder, len(pm_df))

            # If one platform is short, give extra to the other
            if k_sample < per_platform:
                pm_sample = min(target - k_sample, len(pm_df))
            elif pm_sample < per_platform + remainder:
                k_sample = min(target - pm_sample, len(kalshi_df))

            if k_sample > 0:
                samples.append(kalshi_df.sample(k_sample))
            if pm_sample > 0:
                samples.append(pm_df.sample(pm_sample))

        elif len(kalshi_df) > 0:
            samples.append(kalshi_df.sample(min(target, len(kalshi_df))))
        elif len(pm_df) > 0:
            samples.append(pm_df.sample(min(target, len(pm_df))))

    # Combine samples
    if not samples:
        return pd.DataFrame()

    sample_df = pd.concat(samples, ignore_index=True)

    # Final adjustment to hit target size
    if len(sample_df) > n:
        sample_df = sample_df.sample(n)

    # Shuffle and return
    return sample_df.sample(frac=1).reset_index(drop=True)


def run_prototype(sample_size: int = 500, verbose: bool = False):
    """Run the prototype matching test."""
    print("\n" + "=" * 70)
    print("BELLWETHER MATCHER PROTOTYPE TEST")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Sample size: {sample_size}")
    print("=" * 70 + "\n")

    # Step 1: Load spaCy model
    log("Loading spaCy model...")
    try:
        nlp = load_nlp()
        log(f"Loaded model: {nlp.meta['name']}")
    except RuntimeError as e:
        print(f"\nERROR: {e}")
        print("\nPlease install a spaCy model:")
        print("  python -m spacy download en_core_web_trf")
        print("  or: python -m spacy download en_core_web_sm")
        sys.exit(1)

    # Step 2: Load and sample markets
    log(f"Loading markets from {MASTER_FILE.name}...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"Loaded {len(df):,} total markets")

    log("Selecting stratified sample...")
    sample_df = select_diverse_sample(df, sample_size, min_per_category=15)
    log(f"Selected {len(sample_df):,} markets for testing")

    # Show sample distribution
    print("\nStratified sample distribution:")
    platform_counts = sample_df['platform'].value_counts()
    print(f"  Platforms: Kalshi={platform_counts.get('Kalshi', 0)}, Polymarket={platform_counts.get('Polymarket', 0)}")

    # Show category breakdown with both-platform indicator
    print("\n  Category breakdown:")
    cat_summary = sample_df.groupby('political_category').agg({
        'platform': lambda x: f"K:{sum(x=='Kalshi')}/PM:{sum(x=='Polymarket')}"
    }).reset_index()
    cat_summary.columns = ['category', 'split']
    cat_summary['count'] = sample_df.groupby('political_category').size().values
    cat_summary['has_both'] = cat_summary['split'].apply(lambda x: 'K:0' not in x and 'PM:0' not in x)
    cat_summary = cat_summary.sort_values('count', ascending=False)

    matchable_cats = cat_summary[cat_summary['has_both']]['count'].sum()
    for _, row in cat_summary.iterrows():
        marker = "*" if row['has_both'] else " "
        print(f"    {marker} {row['category']}: {row['count']} ({row['split']})")
    print(f"\n  * = both platforms present (matchable): {matchable_cats}/{len(sample_df)} markets")

    # Step 3: Extract frames
    log("\nExtracting frames...")
    frames = []
    extraction_results = {
        'success': 0,
        'failure': 0,
        'frame_types': Counter(),
        'with_country': 0,
        'with_office': 0,
        'with_year': 0,
        'with_candidate': 0,
        'with_beid': 0,
        'confidence_sum': 0.0,
    }

    for idx, row in sample_df.iterrows():
        metadata = {
            'platform': row.get('platform'),
            'market_id': row.get('market_id'),  # For Kalshi ticker-based year extraction
            'political_category': row.get('political_category'),
            # Close time fields for year fallback (note: may give wrong year for elections)
            'trading_close_time': row.get('trading_close_time'),
            'scheduled_end_time': row.get('scheduled_end_time'),
            'k_expiration_time': row.get('k_expiration_time'),
            'k_close_time': row.get('k_close_time'),
            'party': row.get('party'),
            'candidate': row.get('candidate'),
        }

        try:
            frame = extract_frame(row['question'], metadata)
            frames.append((row.to_dict(), frame))
            extraction_results['success'] += 1

            # Track statistics
            if frame.get('frame_type'):
                extraction_results['frame_types'][frame['frame_type']] += 1
            if frame.get('country'):
                extraction_results['with_country'] += 1
            if frame.get('office'):
                extraction_results['with_office'] += 1
            if frame.get('year'):
                extraction_results['with_year'] += 1
            if frame.get('candidate'):
                extraction_results['with_candidate'] += 1

            beid = generate_beid(frame)
            if beid:
                extraction_results['with_beid'] += 1

            extraction_results['confidence_sum'] += frame.get('extraction_confidence', 0)

            if verbose and idx < 10:
                print(f"\n  [{idx}] {row['question'][:80]}...")
                print(f"       Frame: {frame.get('frame_type')} | Country: {frame.get('country')} | "
                      f"Office: {frame.get('office')} | Year: {frame.get('year')}")
                print(f"       Confidence: {frame.get('extraction_confidence', 0):.2f} | BEID: {beid}")

        except Exception as e:
            extraction_results['failure'] += 1
            if verbose:
                print(f"  ERROR on row {idx}: {e}")

    # Print extraction statistics
    total = extraction_results['success'] + extraction_results['failure']
    print("\n" + "-" * 50)
    print("EXTRACTION RESULTS")
    print("-" * 50)
    print(f"Success rate: {extraction_results['success']}/{total} ({extraction_results['success']/total:.1%})")
    print(f"Failures: {extraction_results['failure']}")
    print(f"\nFrame type distribution:")
    for frame_type, count in extraction_results['frame_types'].most_common():
        print(f"  {frame_type}: {count} ({count/total:.1%})")
    print(f"\nField extraction rates:")
    print(f"  With country: {extraction_results['with_country']/total:.1%}")
    print(f"  With office: {extraction_results['with_office']/total:.1%}")
    print(f"  With year: {extraction_results['with_year']/total:.1%}")
    print(f"  With candidate: {extraction_results['with_candidate']/total:.1%}")
    print(f"  With valid BEID: {extraction_results['with_beid']/total:.1%}")
    print(f"\nAvg confidence: {extraction_results['confidence_sum']/total:.2f}")

    # Step 4: Run matching
    log("\nRunning cross-platform matching...")

    kalshi_frames = [(m, f) for m, f in frames if m.get('platform') == 'Kalshi']
    pm_frames = [(m, f) for m, f in frames if m.get('platform') == 'Polymarket']

    log(f"  Kalshi frames: {len(kalshi_frames)}")
    log(f"  Polymarket frames: {len(pm_frames)}")

    result = match_markets(kalshi_frames, pm_frames, min_confidence=0.6)

    print("\n" + "-" * 50)
    print("MATCHING RESULTS")
    print("-" * 50)
    print(f"Matches found: {len(result.matches)}")
    print(f"Common races: {result.stats['common_events']}")
    print(f"Unmatched Kalshi: {result.stats['unmatched_kalshi']}")
    print(f"Unmatched Polymarket: {result.stats['unmatched_polymarket']}")

    if result.matches:
        # Show confidence distribution
        confidences = [m.match_confidence for m in result.matches]
        print(f"\nMatch confidence distribution:")
        print(f"  High (≥0.9): {sum(1 for c in confidences if c >= 0.9)}")
        print(f"  Medium (0.7-0.9): {sum(1 for c in confidences if 0.7 <= c < 0.9)}")
        print(f"  Low (<0.7): {sum(1 for c in confidences if c < 0.7)}")

        # Show sample matches
        print(f"\nSample matches:")
        for i, match in enumerate(result.matches[:5]):
            print(f"\n  [{i+1}] BEID: {match.beid}")
            print(f"      Kalshi: {match.kalshi_market.get('question', '')[:60]}...")
            print(f"      PM: {match.polymarket_market.get('question', '')[:60]}...")
            print(f"      Confidence: {match.match_confidence:.2f}")
            print(f"      Reasons: {', '.join(match.match_reasons[:3])}")

            # Validate match
            warnings = validate_match(match)
            if warnings:
                print(f"      WARNINGS: {', '.join(warnings)}")

    # Step 5: Output results
    log(f"\nWriting results to {OUTPUT_FILE.name}...")

    output = {
        'generated_at': datetime.now().isoformat(),
        'sample_size': sample_size,
        'extraction_results': {
            'success_rate': extraction_results['success'] / total,
            'frame_type_distribution': dict(extraction_results['frame_types']),
            'field_rates': {
                'country': extraction_results['with_country'] / total,
                'office': extraction_results['with_office'] / total,
                'year': extraction_results['with_year'] / total,
                'candidate': extraction_results['with_candidate'] / total,
                'beid': extraction_results['with_beid'] / total,
            },
            'avg_confidence': extraction_results['confidence_sum'] / total,
        },
        'matching_results': {
            'matches_found': len(result.matches),
            'common_events': result.stats['common_events'],
            'unmatched_kalshi': result.stats['unmatched_kalshi'],
            'unmatched_polymarket': result.stats['unmatched_polymarket'],
        },
        'matches': [
            {
                'beid': m.beid,
                'kalshi_id': m.kalshi_market.get('market_id'),
                'kalshi_question': m.kalshi_market.get('question'),
                'pm_id': m.polymarket_market.get('market_id'),
                'pm_question': m.polymarket_market.get('question'),
                'confidence': m.match_confidence,
                'reasons': m.match_reasons,
            }
            for m in result.matches
        ],
        'sample_frames': [
            {
                'market_id': m.get('market_id'),
                'question': m.get('question'),
                'platform': m.get('platform'),
                'frame': f,
                'beid': generate_beid(f),
            }
            for m, f in frames[:20]  # First 20 for review
        ]
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    log(f"Results written to {OUTPUT_FILE}")

    # Summary
    print("\n" + "=" * 70)
    print("PROTOTYPE TEST COMPLETE")
    print("=" * 70)
    print(f"\nKey metrics:")
    print(f"  Extraction success rate: {extraction_results['success']/total:.1%}")
    print(f"  BEID generation rate: {extraction_results['with_beid']/total:.1%}")
    print(f"  Matches found: {len(result.matches)}")
    print(f"  Average match confidence: {sum(m.match_confidence for m in result.matches)/len(result.matches):.2f}"
          if result.matches else "  No matches found")

    print(f"\nReview the output at: {OUTPUT_FILE}")
    print("=" * 70 + "\n")

    return output


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Prototype test for Bellwether market matcher')
    parser.add_argument('--sample-size', type=int, default=500,
                        help='Number of markets to sample (default: 500)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print detailed extraction info')

    args = parser.parse_args()

    run_prototype(sample_size=args.sample_size, verbose=args.verbose)


if __name__ == '__main__':
    main()
