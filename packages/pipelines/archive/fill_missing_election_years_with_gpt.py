#!/usr/bin/env python3
"""
Fill missing election_year values using GPT-4o with election cycle logic
"""

import pandas as pd
import json
import time
from datetime import datetime
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
INPUT_FILE = f"{DATA_DIR}/polymarket_untagged_electoral_details.csv"
OUTPUT_FILE = f"{DATA_DIR}/polymarket_untagged_electoral_details.csv"
API_KEY_FILE = f"{BASE_DIR}/openai_api_key.txt"

# Configuration
BATCH_SIZE = 20
MODEL = "gpt-4o"

print("=" * 80)
print("FILLING MISSING ELECTION YEARS USING GPT-4O")
print("=" * 80)

# Load API key
with open(API_KEY_FILE, 'r') as f:
    api_key = f.read().strip()
client = OpenAI(api_key=api_key)

# Load data
df = pd.read_csv(INPUT_FILE)
print(f"\n✓ Loaded {len(df):,} markets")

# Find US elections with missing election_year
us_elections = df[df['country'] == 'United States'].copy()
missing_year = us_elections['election_year'].isna()
missing_markets = us_elections[missing_year].copy()

print(f"\n✓ US elections: {len(us_elections):,}")
print(f"✓ Missing election_year: {len(missing_markets):,}")

if len(missing_markets) == 0:
    print("\n✓ No missing election years to fill!")
    exit(0)

# Function to fill election years in batches
def fill_election_years_batch(batch_df):
    """Use GPT-4o to infer election years for markets"""

    # Prepare market data
    markets_data = []
    for idx, row in batch_df.iterrows():
        markets_data.append({
            "index": int(idx),
            "question": row['question'],
            "office": row['office'],
            "location": row['location'],
            "end_date": row['end_date'] if pd.notna(row['end_date']) else None
        })

    # Create prompt with election cycle logic
    prompt = f"""You are determining the election year for US political prediction markets.

For each market below, determine which election year it refers to based on:
1. The question text
2. The market end date (when it closed)
3. The office type and its election cycle
4. Context clues about timing

**ELECTION CYCLE RULES:**
- **President**: Every 4 years (2016, 2020, 2024, 2028...)
- **Senate**: Every 6 years per seat, but elections every 2 years (varies by state)
- **House**: Every 2 years (2020, 2022, 2024, 2026...)
- **Governor**: Usually every 4 years (varies by state)
- **Mayor**: Varies by city (typically 2-4 years)

**SPECIAL LOGIC FOR "REMAINING IN OFFICE" MARKETS:**
If the question asks "Will [person] be/remain [office] on [date]":
- Determine which election PUT that person in office
- Set election_year to that election year
- Examples:
  - "Will Joe Biden be President on April 30, 2021?" → 2020 (Biden elected in 2020)
  - "Will Donald Trump be President on July 31, 2021?" → 2020 (Trump ran in 2020)

**FOR FILING/ANNOUNCEMENT MARKETS:**
If the question is about filing to run or announcing candidacy:
- Use the election year they're filing FOR
- Example: "Will Trump file to run for president before June 2021?" → 2024 (filing for 2024 election)

**USE END_DATE AS GUIDE:**
- If end_date is 2021-04-30 and office is President, the relevant election is 2020
- If end_date is 2022-11-08 and office is House, the relevant election is 2022
- For offices with elections every 2 years (House), use the nearest even year ≤ end_date
- For President (every 4 years), use the nearest year divisible by 4 that is ≤ end_date

Markets to analyze:
{json.dumps(markets_data, indent=2)}

Return JSON with format:
{{
  "markets": [
    {{"index": 0, "election_year": 2020}},
    {{"index": 1, "election_year": 2022}},
    ...
  ]
}}

IMPORTANT:
- Return integer years only (e.g., 2020, 2022, 2024)
- If you cannot determine the year with confidence, return null
- Use election_year for the election the market is about, not the end_date year
"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a precise election year classifier. Return valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        return result.get('markets', [])

    except Exception as e:
        print(f"\n❌ Error in API call: {e}")
        return []

# Process in batches
print(f"\n{'=' * 80}")
print("PROCESSING BATCHES")
print(f"{'=' * 80}")

total_batches = (len(missing_markets) + BATCH_SIZE - 1) // BATCH_SIZE
print(f"\nTotal batches: {total_batches}")
print(f"Batch size: {BATCH_SIZE}")

filled_count = 0
batch_num = 0

for start_idx in range(0, len(missing_markets), BATCH_SIZE):
    batch_num += 1
    batch = missing_markets.iloc[start_idx:start_idx + BATCH_SIZE]

    print(f"\nBatch {batch_num}/{total_batches} | Markets {start_idx + 1}-{min(start_idx + BATCH_SIZE, len(missing_markets))} of {len(missing_markets)}")

    results = fill_election_years_batch(batch)

    # Update the dataframe
    for result in results:
        idx = result['index']
        year = result.get('election_year')
        if year is not None:
            df.at[idx, 'election_year'] = year
            filled_count += 1

    print(f"✓ Filled {len([r for r in results if r.get('election_year') is not None])}/{len(batch)} in this batch")

    time.sleep(1)  # Rate limiting

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

us_after = df[df['country'] == 'United States']
still_missing = us_after['election_year'].isna().sum()

print(f"\n✓ Successfully filled: {filled_count}/{len(missing_markets)}")
print(f"✓ Still missing: {still_missing}")
print(f"✓ US elections with election_year: {us_after['election_year'].notna().sum()}/{len(us_after)} ({us_after['election_year'].notna().sum()/len(us_after)*100:.1f}%)")

print(f"\nYear distribution after filling:")
print(us_after['election_year'].value_counts().sort_index())

# Save
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✓ Saved to: {OUTPUT_FILE}")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
