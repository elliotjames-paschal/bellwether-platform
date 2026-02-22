"""
Find election winner markets that were miscategorized as non-electoral
"""

import pandas as pd
import re
from pathlib import Path

# Paths
DATA_DIR = Path(__file__).parent.parent / "data"
MASTER_CSV = DATA_DIR / "combined_political_markets_with_electoral_details.csv"

def load_data():
    """Load master CSV and filter to non-electoral markets"""
    df = pd.read_csv(MASTER_CSV)
    non_electoral = df[df['political_category'] != '1. ELECTORAL'].copy()
    print(f"Total markets: {len(df):,}")
    print(f"Non-electoral markets: {len(non_electoral):,}")
    return non_electoral

def check_election_winner_patterns(question):
    """
    Check if a question matches election winner market patterns.
    Returns (is_winner_market, office_type) tuple.
    """
    if pd.isna(question):
        return False, None

    question_lower = question.lower()

    # Common election winner patterns for both platforms
    patterns = {
        'President': [
            r'will.*win.*president',
            r'will.*democrat.*win.*presidency',
            r'will.*republican.*win.*presidency',
            r'win.*presidential\s+election',
        ],
        'Governor': [
            r'will.*win.*governor',
            r'will.*democrat.*win.*governorship',
            r'will.*republican.*win.*governorship',
            r'win.*gubernatorial\s+election',
        ],
        'Senate': [
            r'will.*win.*senate',
            r'will.*win.*senator',
            r'will.*win.*u\.?s\.?\s+senate',
            r'will.*win.*senate\s+race',
            r'will.*win.*senate\s+election',
        ],
        'House': [
            r'will.*win.*house',
            r'will.*win.*congressional',
            r'will.*win.*house\s+race',
            r'will.*win.*house\s+election',
            r'will.*representative.*win',
        ],
        'Mayor': [
            r'will.*win.*mayor',
            r'will.*win.*mayoral',
        ],
        'State Legislature': [
            r'will.*win.*state\s+senate',
            r'will.*win.*state\s+house',
            r'will.*win.*assembly',
            r'will.*win.*legislature',
        ],
        'Attorney General': [
            r'will.*win.*attorney\s+general',
        ],
        'Secretary of State': [
            r'will.*win.*secretary\s+of\s+state',
        ],
        'Other State Office': [
            r'will.*win.*treasurer',
            r'will.*win.*auditor',
            r'will.*win.*controller',
            r'will.*win.*comptroller',
        ],
    }

    # Check each office type
    for office, pattern_list in patterns.items():
        for pattern in pattern_list:
            if re.search(pattern, question_lower):
                return True, office

    return False, None

def main():
    print("="*80)
    print("FINDING MISCATEGORIZED ELECTION WINNER MARKETS")
    print("="*80)

    # Load data
    df = load_data()

    # Check each market
    print(f"\n{'='*80}")
    print("ANALYZING QUESTIONS...")
    print(f"{'='*80}\n")

    results = []
    for idx, row in df.iterrows():
        is_winner, office = check_election_winner_patterns(row['question'])
        if is_winner:
            results.append({
                'platform': row['platform'],
                'market_id': row.get('market_id') or row.get('ticker'),
                'question': row['question'],
                'current_category': row['political_category'],
                'detected_office': office,
                'closed': row.get('closed'),
                'volume_usd': row.get('volume_usd'),
            })

    # Create results DataFrame
    results_df = pd.DataFrame(results)

    if len(results_df) == 0:
        print("✓ No miscategorized election winner markets found!")
        return

    # Summary statistics
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total miscategorized election winner markets: {len(results_df):,}")
    print(f"\nBy platform:")
    print(results_df['platform'].value_counts())
    print(f"\nBy detected office type:")
    print(results_df['detected_office'].value_counts())
    print(f"\nBy current category:")
    print(results_df['current_category'].value_counts())

    # Show examples
    print(f"\n{'='*80}")
    print("EXAMPLES (first 20)")
    print(f"{'='*80}\n")

    for idx, row in results_df.head(20).iterrows():
        print(f"Platform: {row['platform']}")
        print(f"Market ID: {row['market_id']}")
        print(f"Question: {row['question']}")
        print(f"Current Category: {row['current_category']}")
        print(f"Detected Office: {row['detected_office']}")
        print(f"Closed: {row['closed']}")
        print(f"Volume: ${row['volume_usd']:,.0f}" if pd.notna(row['volume_usd']) else "Volume: N/A")
        print("-" * 80)

    # Save results
    output_file = DATA_DIR / "miscategorized_election_winners.csv"
    results_df.to_csv(output_file, index=False)
    print(f"\n✓ Full results saved to: {output_file}")

    # Additional analysis: High volume miscategorized markets
    high_volume = results_df[results_df['volume_usd'] > 10000].sort_values('volume_usd', ascending=False)
    if len(high_volume) > 0:
        print(f"\n{'='*80}")
        print(f"HIGH VOLUME MISCATEGORIZED MARKETS (>$10K)")
        print(f"{'='*80}")
        print(f"Found {len(high_volume)} high-volume miscategorized markets\n")
        for idx, row in high_volume.head(10).iterrows():
            print(f"${row['volume_usd']:,.0f} - {row['question']} ({row['platform']})")

if __name__ == "__main__":
    main()
