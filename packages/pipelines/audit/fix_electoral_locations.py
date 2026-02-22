#!/usr/bin/env python3
"""
================================================================================
FIX ELECTORAL LOCATION MISLABELING
================================================================================

Fixes electoral markets that have location="United States" but are actually
state-level races (e.g., "Will Trump win Iowa?" should have location="Iowa").

This script:
1. Finds electoral markets with location="United States" that mention states
2. Extracts the correct state from the question
3. Updates the location field in the master CSV

================================================================================
"""

import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

# Paths
DATA_DIR = Path(__file__).parent.parent.parent / "data"
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
AUDIT_DIR = DATA_DIR / "audit"


# US States mapping
US_STATES = {
    'Alabama': 'Alabama', 'Alaska': 'Alaska', 'Arizona': 'Arizona', 'Arkansas': 'Arkansas',
    'California': 'California', 'Colorado': 'Colorado', 'Connecticut': 'Connecticut',
    'Delaware': 'Delaware', 'Florida': 'Florida', 'Georgia': 'Georgia', 'Hawaii': 'Hawaii',
    'Idaho': 'Idaho', 'Illinois': 'Illinois', 'Indiana': 'Indiana', 'Iowa': 'Iowa',
    'Kansas': 'Kansas', 'Kentucky': 'Kentucky', 'Louisiana': 'Louisiana', 'Maine': 'Maine',
    'Maryland': 'Maryland', 'Massachusetts': 'Massachusetts', 'Michigan': 'Michigan',
    'Minnesota': 'Minnesota', 'Mississippi': 'Mississippi', 'Missouri': 'Missouri',
    'Montana': 'Montana', 'Nebraska': 'Nebraska', 'Nevada': 'Nevada',
    'New Hampshire': 'New Hampshire', 'New Jersey': 'New Jersey', 'New Mexico': 'New Mexico',
    'New York': 'New York', 'North Carolina': 'North Carolina', 'North Dakota': 'North Dakota',
    'Ohio': 'Ohio', 'Oklahoma': 'Oklahoma', 'Oregon': 'Oregon', 'Pennsylvania': 'Pennsylvania',
    'Rhode Island': 'Rhode Island', 'South Carolina': 'South Carolina',
    'South Dakota': 'South Dakota', 'Tennessee': 'Tennessee', 'Texas': 'Texas',
    'Utah': 'Utah', 'Vermont': 'Vermont', 'Virginia': 'Virginia', 'Washington': 'Washington',
    'West Virginia': 'West Virginia', 'Wisconsin': 'Wisconsin', 'Wyoming': 'Wyoming',
}

# District patterns (e.g., "NE-2", "CA-12")
DISTRICT_PATTERN = re.compile(r'\b([A-Z]{2})-(\d+)\b')

# State abbreviations
STATE_ABBREVS = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire',
    'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina',
    'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
    'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
    'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
    'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
}


def extract_state_from_question(question: str) -> Optional[str]:
    """
    Extract state name from question text.
    Returns the state name or None if not found.
    """
    if pd.isna(question):
        return None

    q = str(question)

    # Check for district pattern first (e.g., "NE-2")
    district_match = DISTRICT_PATTERN.search(q)
    if district_match:
        abbrev = district_match.group(1)
        if abbrev in STATE_ABBREVS:
            return STATE_ABBREVS[abbrev]

    # Check for full state names (case insensitive, but return proper case)
    for state in US_STATES:
        # Match whole word only
        pattern = r'\b' + re.escape(state) + r'\b'
        if re.search(pattern, q, re.IGNORECASE):
            return state

    return None


def main():
    print("\n" + "=" * 70)
    print("FIX ELECTORAL LOCATION MISLABELING")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Load master CSV
    print("\nLoading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    print(f"  Total markets: {len(df):,}")

    # Find electoral markets with location="United States"
    electoral_mask = df['political_category'] == '1. ELECTORAL'
    us_location_mask = df['location'] == 'United States'
    target_mask = electoral_mask & us_location_mask

    target_df = df[target_mask].copy()
    print(f"  Electoral markets with location='United States': {len(target_df):,}")

    # Extract states from questions
    print("\nExtracting states from questions...")
    target_df['extracted_state'] = target_df['question'].apply(extract_state_from_question)

    needs_fix = target_df[target_df['extracted_state'].notna()]
    print(f"  Markets that mention a specific state: {len(needs_fix):,}")

    if len(needs_fix) == 0:
        print("\nNo markets need fixing!")
        return

    # Show sample
    print("\nSample fixes:")
    for _, row in needs_fix.head(10).iterrows():
        print(f"  '{row['question'][:60]}...'")
        print(f"    United States -> {row['extracted_state']}")

    # Apply fixes
    print(f"\nApplying {len(needs_fix):,} location fixes...")

    fix_log = []
    for idx, row in needs_fix.iterrows():
        old_value = df.loc[idx, 'location']
        new_value = row['extracted_state']

        df.loc[idx, 'location'] = new_value
        fix_log.append({
            'market_id': row['market_id'],
            'question': row['question'],
            'old_location': old_value,
            'new_location': new_value,
        })

    # Save audit log
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    log_file = AUDIT_DIR / f"{datetime.now().strftime('%Y-%m-%d')}_electoral_location_fixes.csv"
    pd.DataFrame(fix_log).to_csv(log_file, index=False)
    print(f"  Audit log saved: {log_file}")

    # Save updated master
    print("\nSaving updated master CSV...")
    df.to_csv(MASTER_FILE, index=False)
    print(f"  Saved: {MASTER_FILE}")

    # Summary by state
    print("\nFixes by state:")
    state_counts = pd.DataFrame(fix_log)['new_location'].value_counts()
    for state, count in state_counts.head(15).items():
        print(f"  {state}: {count}")

    print("\n" + "=" * 70)
    print(f"COMPLETE: Fixed {len(needs_fix):,} electoral market locations")
    print("=" * 70 + "\n")

    return len(needs_fix)


if __name__ == "__main__":
    main()
