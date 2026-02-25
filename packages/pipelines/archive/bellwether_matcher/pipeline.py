#!/usr/bin/env python3
"""
Bellwether Market Matcher Pipeline.

Orchestrates the full matching pipeline:
1. Load markets from master CSV
2. Extract frames for all markets
3. Match across platforms
4. Generate BEIDs for matches
5. Output match table

Usage:
    python -m bellwether_matcher.pipeline
    # Or from scripts directory:
    python bellwether_matcher/pipeline.py
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_DIR

from .extractor import extract_frame, load_nlp
from .matcher import match_markets, MatchResult, validate_match
from .taxonomy import generate_beid


# Output file
OUTPUT_FILE = DATA_DIR / "bellwether_matches.json"


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def run_matching_pipeline(
    master_csv_path: Path | None = None,
    output_path: Path | None = None,
    limit: int | None = None,
    min_confidence: float = 0.6,
) -> dict[str, Any]:
    """
    Run the full matching pipeline.

    Args:
        master_csv_path: Path to master CSV (defaults to standard location)
        output_path: Path for output JSON (defaults to standard location)
        limit: Limit number of markets to process (for testing)
        min_confidence: Minimum match confidence threshold

    Returns:
        dict with pipeline results and statistics
    """
    master_path = master_csv_path or (DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv")
    out_path = output_path or OUTPUT_FILE

    print("\n" + "=" * 70)
    print("BELLWETHER MARKET MATCHER PIPELINE")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    # Step 1: Load NLP model
    log("Loading spaCy model...")
    nlp = load_nlp()
    log(f"Loaded model: {nlp.meta['name']}")

    # Step 2: Load markets from master CSV
    log(f"Loading markets from {master_path.name}...")
    df = pd.read_csv(master_path, low_memory=False)
    log(f"Loaded {len(df):,} total markets")

    # Apply limit if specified
    if limit:
        df = df.head(limit)
        log(f"Limited to {len(df):,} markets for testing")

    # Step 3: Separate by platform
    kalshi_df = df[df['platform'] == 'Kalshi'].copy()
    pm_df = df[df['platform'] == 'Polymarket'].copy()
    log(f"Platform split: {len(kalshi_df):,} Kalshi, {len(pm_df):,} Polymarket")

    # Step 4: Extract frames
    log("Extracting frames from market questions...")

    kalshi_frames = []
    pm_frames = []
    extraction_failures = 0

    for idx, row in kalshi_df.iterrows():
        try:
            metadata = _row_to_metadata(row)
            frame = extract_frame(row['question'], metadata)
            kalshi_frames.append((row.to_dict(), frame))
        except Exception as e:
            extraction_failures += 1
            if extraction_failures <= 5:
                log(f"  Warning: Extraction failed for {row.get('market_id', 'unknown')}: {e}")

    for idx, row in pm_df.iterrows():
        try:
            metadata = _row_to_metadata(row)
            frame = extract_frame(row['question'], metadata)
            pm_frames.append((row.to_dict(), frame))
        except Exception as e:
            extraction_failures += 1
            if extraction_failures <= 5:
                log(f"  Warning: Extraction failed for {row.get('market_id', 'unknown')}: {e}")

    log(f"Extracted {len(kalshi_frames):,} Kalshi frames, {len(pm_frames):,} PM frames")
    if extraction_failures:
        log(f"  ({extraction_failures:,} extraction failures)")

    # Step 5: Run matching
    log("Matching markets across platforms...")
    result: MatchResult = match_markets(
        kalshi_frames,
        pm_frames,
        min_confidence=min_confidence,
    )

    log(f"Found {len(result.matches):,} matches")
    log(f"Unmatched: {len(result.unmatched_kalshi):,} Kalshi, {len(result.unmatched_polymarket):,} PM")

    # Step 6: Validate matches and collect warnings
    log("Validating matches...")
    matches_with_warnings = 0
    for match in result.matches:
        warnings = validate_match(match)
        if warnings:
            matches_with_warnings += 1
            match.match_reasons.extend([f"WARNING: {w}" for w in warnings])

    if matches_with_warnings:
        log(f"  {matches_with_warnings:,} matches have warnings")

    # Step 7: Generate output
    log(f"Writing output to {out_path.name}...")
    output = _format_output(result, df)

    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    log(f"Wrote {len(output['matches']):,} matches to {out_path.name}")

    # Print summary stats
    print("\n" + "=" * 70)
    print("MATCHING COMPLETE")
    print("=" * 70)
    _print_stats(result.stats, output)

    return output


def _row_to_metadata(row: pd.Series) -> dict:
    """Convert DataFrame row to metadata dict for extraction."""
    return {
        'platform': row.get('platform'),
        'political_category': row.get('political_category'),
        # Close time fields for year fallback (when not in question text)
        'trading_close_time': row.get('trading_close_time'),
        'scheduled_end_time': row.get('scheduled_end_time'),
        'k_expiration_time': row.get('k_expiration_time'),
        'k_close_time': row.get('k_close_time'),
    }


def _format_output(result: MatchResult, df: pd.DataFrame) -> dict:
    """Format matching results for JSON output."""
    matches_list = []

    for match in result.matches:
        match_dict = {
            'beid': match.beid,
            'frame_type': match.kalshi_frame.get('frame_type'),
            'kalshi_ticker': match.kalshi_market.get('market_id'),
            'kalshi_question': match.kalshi_market.get('question'),
            'polymarket_id': match.polymarket_market.get('market_id'),
            'polymarket_question': match.polymarket_market.get('question'),
            'match_confidence': match.match_confidence,
            'match_reasons': match.match_reasons,
            'extracted_fields': {
                'country': match.kalshi_frame.get('country') or match.polymarket_frame.get('country'),
                'office': match.kalshi_frame.get('office') or match.polymarket_frame.get('office'),
                'year': match.kalshi_frame.get('year') or match.polymarket_frame.get('year'),
                'scope': match.kalshi_frame.get('scope') or match.polymarket_frame.get('scope'),
                'candidate': match.kalshi_frame.get('candidate') or match.polymarket_frame.get('candidate'),
                'party': match.kalshi_frame.get('party') or match.polymarket_frame.get('party'),
            }
        }
        matches_list.append(match_dict)

    # Sort by confidence
    matches_list.sort(key=lambda x: x['match_confidence'], reverse=True)

    # Format unmatched markets (just IDs and questions)
    unmatched_kalshi = [
        {
            'market_id': m.get('market_id'),
            'question': m.get('question'),
        }
        for m in result.unmatched_kalshi
    ]

    unmatched_pm = [
        {
            'market_id': m.get('market_id'),
            'question': m.get('question'),
        }
        for m in result.unmatched_polymarket
    ]

    return {
        'generated_at': datetime.now().isoformat(),
        'total_markets_processed': len(df),
        'total_matches': len(matches_list),
        'match_confidence_distribution': {
            'high_0.9_plus': sum(1 for m in matches_list if m['match_confidence'] >= 0.9),
            'medium_0.7_to_0.9': sum(1 for m in matches_list if 0.7 <= m['match_confidence'] < 0.9),
            'low_below_0.7': sum(1 for m in matches_list if m['match_confidence'] < 0.7),
        },
        'stats': result.stats,
        'matches': matches_list,
        'unmatched_kalshi': unmatched_kalshi[:100],  # Limit output size
        'unmatched_polymarket': unmatched_pm[:100],
    }


def _print_stats(stats: dict, output: dict):
    """Print summary statistics."""
    print(f"\nMarkets processed: {stats['total_kalshi'] + stats['total_polymarket']:,}")
    print(f"  Kalshi: {stats['total_kalshi']:,}")
    print(f"  Polymarket: {stats['total_polymarket']:,}")

    print(f"\nMatches found: {stats['matches_found']:,}")
    print(f"  Common races: {stats['common_events']:,}")
    print(f"  Match rate (Kalshi): {stats['match_rate_kalshi']:.1%}")
    print(f"  Match rate (PM): {stats['match_rate_polymarket']:.1%}")

    conf_dist = output['match_confidence_distribution']
    print(f"\nConfidence distribution:")
    print(f"  High (≥0.9): {conf_dist['high_0.9_plus']:,}")
    print(f"  Medium (0.7-0.9): {conf_dist['medium_0.7_to_0.9']:,}")
    print(f"  Low (<0.7): {conf_dist['low_below_0.7']:,}")

    print(f"\nUnmatched markets:")
    print(f"  Kalshi: {stats['unmatched_kalshi']:,}")
    print(f"  Polymarket: {stats['unmatched_polymarket']:,}")


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Run Bellwether market matching pipeline')
    parser.add_argument('--limit', type=int, help='Limit number of markets to process')
    parser.add_argument('--min-confidence', type=float, default=0.6,
                        help='Minimum match confidence (0-1)')
    parser.add_argument('--output', type=str, help='Output file path')

    args = parser.parse_args()

    output_path = Path(args.output) if args.output else None

    run_matching_pipeline(
        limit=args.limit,
        min_confidence=args.min_confidence,
        output_path=output_path,
    )


if __name__ == '__main__':
    main()
