#!/usr/bin/env python3
"""
B3: Label election types with GPT verification
- US markets: Apply deterministic mapping, GPT verifies
- Non-US markets: GPT labels from scratch
"""

import pandas as pd
import json
import time
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
INPUT_FILE = f"{DATA_DIR}/polymarket_untagged_electoral_details.csv"
OUTPUT_FILE = f"{DATA_DIR}/polymarket_untagged_electoral_details.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/election_type_labeling_checkpoint.json"
API_KEY_FILE = f"{BASE_DIR}/openai_api_key.txt"

# Configuration
BATCH_SIZE = 20
MODEL = "gpt-4o"
TEMPERATURE = 0

print("=" * 80)
print("B3: ELECTION TYPE LABELING WITH GPT VERIFICATION")
print("=" * 80)

# Load API key
with open(API_KEY_FILE, 'r') as f:
    api_key = f.read().strip()
client = OpenAI(api_key=api_key)

# Load data
df = pd.read_csv(INPUT_FILE)
print(f"\n✓ Loaded {len(df):,} markets")
print(f"✓ US elections: {(df['country'] == 'United States').sum():,}")
print(f"✓ Non-US elections: {(df['country'] != 'United States').sum():,}")

# Deterministic mapping function for US markets
def deterministic_election_type(row):
    """Map office + is_primary to election_type for US markets"""
    if row['country'] != 'United States':
        return None

    office = row['office']
    is_primary = row['is_primary']

    if pd.isna(office):
        return None

    mapping = {
        ('President', False): 'Presidential',
        ('President', True): 'Presidential Primary',
        ('Senate', False): 'Senate',
        ('Senate', True): 'Senate Primary',
        ('House', False): 'House',
        ('House', True): 'House Primary',
        ('Governor', False): 'Gubernatorial',
        ('Governor', True): 'Gubernatorial Primary',
        ('Mayor', False): 'Mayoral',
        ('Mayor', True): 'Mayoral Primary',
        ('Vice President', False): 'VP Nomination',
        ('Vice President', True): 'VP Nomination',
    }

    return mapping.get((office, is_primary), None)

# Apply deterministic mapping to US markets
print(f"\nApplying deterministic mapping to US markets...")
df['deterministic_type'] = df.apply(deterministic_election_type, axis=1)

# Check checkpoint
try:
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        processed_indices = set(checkpoint['processed_indices'])
        print(f"\n✓ Loaded checkpoint: {len(processed_indices)} markets already processed")
except FileNotFoundError:
    processed_indices = set()
    print(f"\n⚠ No checkpoint found. Starting fresh.")

# Get markets to process
to_process = df[~df.index.isin(processed_indices)].copy()
print(f"\n📊 Markets remaining to process: {len(to_process):,}")

if len(to_process) == 0:
    print("\n✓ All markets already processed!")
    exit(0)

def label_election_types_batch(batch_df):
    """Use GPT to verify US types and label non-US types"""

    # Prepare market data
    markets_data = []
    for idx, row in batch_df.iterrows():
        market_info = {
            "index": int(idx),
            "question": row['question'],
            "country": row['country'],
        }

        # Add US-specific fields
        if row['country'] == 'United States':
            market_info.update({
                "office": row['office'],
                "location": row['location'],
                "is_primary": bool(row['is_primary']),
                "deterministic_type": row['deterministic_type']
            })

        markets_data.append(market_info)

    # Create prompt
    prompt = f"""You are labeling election types for prediction markets.

**AVAILABLE ELECTION TYPES (22 types):**
1. Presidential
2. Presidential Primary
3. Senate
4. Senate Primary
5. House
6. House Primary
7. Gubernatorial
8. Gubernatorial Primary
9. Mayoral
10. Mayoral Primary
11. VP Nomination
12. Parliamentary
13. Prime Minister
14. General Election
15. National Election
16. European Parliament
17. Regional Election
18. Chancellor
19. Provincial
20. Republican Primary
21. Democratic Primary
22. Multiple Elections

**YOUR TASK:**

For **US markets** (country = "United States"):
- I've provided a "deterministic_type" based on office + is_primary mapping
- VERIFY this is correct given the question text
- If correct, return the same type
- If incorrect (e.g., question is actually about a different type), return the correct type

For **NON-US markets** (all other countries):
- Determine the election type from the question and country
- Use the appropriate international types (Parliamentary, Prime Minister, General Election, etc.)
- Consider:
  * Parliamentary: Elections for legislative body members
  * Prime Minister: Head of government elections
  * General Election: Broad national elections
  * National Election: Country-wide elections
  * Regional Election: State/province level
  * Provincial: Provincial elections
  * European Parliament: EU parliament elections
  * Chancellor: For Germany and similar systems

**EXAMPLES:**

US Market:
{{"index": 0, "question": "Will Donald Trump win the 2024 presidential election?", "country": "United States", "office": "President", "is_primary": false, "deterministic_type": "Presidential"}}
→ election_type: "Presidential" (deterministic mapping is correct)

US Market:
{{"index": 1, "question": "Will Ron DeSantis win the 2024 Republican primary?", "country": "United States", "office": "President", "is_primary": true, "deterministic_type": "Presidential Primary"}}
→ election_type: "Presidential Primary" (deterministic mapping is correct)

Non-US Market:
{{"index": 2, "question": "Will Emmanuel Macron win the 2022 French presidential election?", "country": "France"}}
→ election_type: "Presidential"

Non-US Market:
{{"index": 3, "question": "Will Labour win the UK general election?", "country": "United Kingdom"}}
→ election_type: "Parliamentary"

Non-US Market:
{{"index": 4, "question": "Will Lula win the Brazilian presidential election?", "country": "Brazil"}}
→ election_type: "Presidential"

Markets to analyze:
{json.dumps(markets_data, indent=2)}

Return JSON with format:
{{
  "markets": [
    {{"index": 0, "election_type": "Presidential"}},
    {{"index": 1, "election_type": "Parliamentary"}},
    ...
  ]
}}

IMPORTANT:
- Return EXACTLY one of the 22 election types listed above
- For US markets, only change if deterministic mapping is clearly wrong
- Use appropriate international types for non-US markets
"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a precise election type classifier. Return valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=TEMPERATURE,
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

total_batches = (len(to_process) + BATCH_SIZE - 1) // BATCH_SIZE
print(f"\nTotal batches: {total_batches}")
print(f"Batch size: {BATCH_SIZE}")
print(f"Model: {MODEL}")

labeled_count = 0
corrections_count = 0
batch_num = 0

for start_idx in range(0, len(to_process), BATCH_SIZE):
    batch_num += 1
    batch = to_process.iloc[start_idx:start_idx + BATCH_SIZE]

    print(f"\n{'─' * 80}")
    print(f"Batch {batch_num}/{total_batches} | Markets {start_idx + 1}-{min(start_idx + BATCH_SIZE, len(to_process))} of {len(to_process)}")

    results = label_election_types_batch(batch)

    # Update the dataframe
    for result in results:
        idx = result['index']
        election_type = result.get('election_type')

        if election_type:
            # Check if this is a correction to deterministic mapping
            if pd.notna(df.at[idx, 'deterministic_type']) and df.at[idx, 'deterministic_type'] != election_type:
                corrections_count += 1
                print(f"  ⚠️  Correction: {df.at[idx, 'deterministic_type']} → {election_type} | {df.at[idx, 'question'][:60]}...")

            df.at[idx, 'election_type'] = election_type
            labeled_count += 1
            processed_indices.add(idx)

    print(f"✓ Labeled {len([r for r in results if r.get('election_type')])} markets in this batch")

    # Save checkpoint
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({
            'processed_indices': list(processed_indices),
            'labeled_count': labeled_count,
            'corrections_count': corrections_count
        }, f)
    print(f"💾 Checkpoint saved ({len(processed_indices)} markets processed)")

    time.sleep(1)  # Rate limiting

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

has_type = df['election_type'].notna().sum()
missing_type = df['election_type'].isna().sum()

print(f"\n✓ Markets with election_type: {has_type}/{len(df)} ({has_type/len(df)*100:.1f}%)")
print(f"✓ Corrections to deterministic mapping: {corrections_count}")
print(f"✓ Still missing: {missing_type}")

# Breakdown by country
us_elections = df[df['country'] == 'United States']
non_us_elections = df[df['country'] != 'United States']

print(f"\nUS elections with type: {us_elections['election_type'].notna().sum()}/{len(us_elections)}")
print(f"Non-US elections with type: {non_us_elections['election_type'].notna().sum()}/{len(non_us_elections)}")

print(f"\nElection type distribution:")
print(df['election_type'].value_counts().sort_values(ascending=False))

# Save
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✓ Saved to: {OUTPUT_FILE}")

print(f"\n{'=' * 80}")
print("✓ B3 COMPLETE")
print(f"{'=' * 80}")
