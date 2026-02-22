#!/usr/bin/env python3
"""
Test Electoral Classification on One Batch

Tests the GPT-4o classification on just 50 markets to validate accuracy before full run.
"""

import pandas as pd
import json
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
INPUT_FILE = f"{DATA_DIR}/combined_political_markets.csv"
API_KEY_FILE = f"{BASE_DIR}/openai_api_key.txt"

# Configuration
BATCH_SIZE = 50
MODEL = "gpt-4o"

print("=" * 80)
print("TEST: ELECTORAL MARKET CLASSIFICATION (ONE BATCH)")
print("=" * 80)

# Load API key
with open(API_KEY_FILE, 'r') as f:
    api_key = f.read().strip()

# Initialize OpenAI client
client = OpenAI(api_key=api_key)

# Load data
print(f"\nLoading: {INPUT_FILE}")
df = pd.read_csv(INPUT_FILE, low_memory=False)
print(f"✓ Loaded {len(df):,} total markets")

# Filter for electoral markets only
electoral = df[df['political_category'] == '1. ELECTORAL'].copy()
print(f"✓ Found {len(electoral):,} electoral markets")

# Get first batch
test_batch = electoral.head(BATCH_SIZE)
print(f"\n📊 Testing on first {len(test_batch)} markets")

# Prepare market data
markets_data = []
for idx, row in test_batch.iterrows():
    markets_data.append({
        "index": int(idx),
        "platform": row['platform'],
        "market_id": row['market_id'],
        "question": row['question'],
        "trading_close_time": str(row['trading_close_time']) if pd.notna(row['trading_close_time']) else None
    })

# Create the prompt
prompt = f"""You are analyzing political prediction markets to extract structured information about US elections.

For each market question below, determine these 5 fields:

1. **country**: What country is this election in?
   - If "United States" → continue to classify other fields
   - If NOT "United States" → set office, location, election_year, and is_primary to null

2. **office**: Which elected office? Must be EXACTLY one of these:
   - President
   - Vice President
   - Senate
   - House
   - Governor
   - Lt. Governor
   - Attorney General
   - Secretary of State
   - Mayor

3. **location**: Geographic location (format depends on office):
   - President / Vice President → "United States"
   - Senate/Governor/Lt. Governor/Attorney General/Secretary of State → Full state name (e.g., "Pennsylvania", "Texas", "California")
   - House → Congressional district format "XX-#" where XX is 2-letter state code, # is district number with NO LEADING ZEROS
     - Examples: "PA-1", "CA-13", "TX-18" (NOT "PA-01" or "CA-013")
     - Special case: At-large districts use "XX-AL" (e.g., "AK-AL", "WY-AL")
   - Mayor → City name (e.g., "San Francisco", "New York City", "Chicago")

4. **election_year**: Year the election takes place (integer)
   - Extract from question text first (e.g., "2024", "2022")
   - If unclear from question, infer from trading_close_time
   - If still unclear, set to null

5. **is_primary**: Type of election (boolean)
   - true if: question contains "primary", "caucus", "nomination", "nominate"
   - false if: "general election", "November election", or final election between parties

**STATE NAME RULES:**
- Always use FULL state names for location (except House districts)
- Examples: "Pennsylvania" NOT "PA", "Texas" NOT "TX", "California" NOT "CA"
- Exception: House districts use 2-letter codes (e.g., "PA-1")

**EXAMPLES:**

Example 1:
Question: "Will Donald Trump win Pennsylvania in the 2024 Presidential Election?"
→ country: "United States", office: "President", location: "United States", election_year: 2024, is_primary: false

Example 2:
Question: "Will the Democrat win PA-7 in 2022?"
→ country: "United States", office: "House", location: "PA-7", election_year: 2022, is_primary: false

Example 3:
Question: "Will Republican win Texas Senate primary in 2024?"
→ country: "United States", office: "Senate", location: "Texas", election_year: 2024, is_primary: true

Example 4:
Question: "Will a Democrat be elected Governor of Michigan in 2026?"
→ country: "United States", office: "Governor", location: "Michigan", election_year: 2026, is_primary: false

Example 5:
Question: "Will Nikki Haley win the U.S. 2024 Republican vice presidential nomination?"
→ country: "United States", office: "Vice President", location: "United States", election_year: 2024, is_primary: true

Example 6:
Question: "Will Labour win the UK general election?"
→ country: "United Kingdom", office: null, location: null, election_year: null, is_primary: null

**IMPORTANT:**
- Office names must match EXACTLY (no parentheses, no extra text)
- House districts: NO LEADING ZEROS (PA-1 not PA-01)
- If country is not "United States", all other fields must be null

Markets to classify:
{json.dumps(markets_data, indent=2)}

IMPORTANT: Return your response as a JSON object with a single key "markets" containing an array of classification objects. Each object must have these fields: index, country, office, location, election_year, is_primary.

Response format:
{{
  "markets": [
    {{"index": 0, "country": "...", "office": "...", "location": "...", "election_year": ..., "is_primary": ...}},
    {{"index": 1, "country": "...", "office": "...", "location": "...", "election_year": ..., "is_primary": ...}},
    ...
  ]
}}"""

print(f"\n{'=' * 80}")
print("CALLING GPT-4O...")
print(f"{'=' * 80}")

# Call OpenAI API
response = client.chat.completions.create(
    model=MODEL,
    messages=[
        {"role": "system", "content": "You are a precise classifier of US political election markets. Return valid JSON only."},
        {"role": "user", "content": prompt}
    ],
    temperature=0,
    response_format={"type": "json_object"}
)

# Parse response
result_text = response.choices[0].message.content
result = json.loads(result_text)

# Debug: Print the structure
print(f"\nDEBUG - Result structure type: {type(result)}")
print(f"DEBUG - Result keys (if dict): {result.keys() if isinstance(result, dict) else 'N/A'}")
print(f"DEBUG - First few items:\n{json.dumps(result if not isinstance(result, list) or len(result) < 3 else result[:3], indent=2)}\n")

# Handle both array and object with various keys
if isinstance(result, dict) and 'classifications' in result:
    classifications = result['classifications']
elif isinstance(result, dict) and 'markets' in result:
    classifications = result['markets']
elif isinstance(result, dict) and 'results' in result:
    classifications = result['results']
elif isinstance(result, list):
    classifications = result
else:
    classifications = list(result.values())

print(f"✓ Received {len(classifications)} classifications")

# Display results
print(f"\n{'=' * 80}")
print("CLASSIFICATION RESULTS")
print(f"{'=' * 80}")

for i, classification in enumerate(classifications[:10], 1):  # Show first 10
    idx = classification['index']
    original_question = test_batch.loc[idx, 'question']

    print(f"\n{i}. Market #{idx}")
    print(f"   Platform: {test_batch.loc[idx, 'platform']}")
    print(f"   Question: {original_question[:100]}{'...' if len(original_question) > 100 else ''}")
    print(f"   → Country: {classification.get('country')}")
    print(f"   → Office: {classification.get('office')}")
    print(f"   → Location: {classification.get('location')}")
    print(f"   → Year: {classification.get('election_year')}")
    print(f"   → Primary: {classification.get('is_primary')}")

if len(classifications) > 10:
    print(f"\n... and {len(classifications) - 10} more markets")

# Summary statistics
print(f"\n{'=' * 80}")
print("SUMMARY STATISTICS")
print(f"{'=' * 80}")

df_results = pd.DataFrame(classifications)

print(f"\nCountries:")
print(df_results['country'].value_counts().to_string())

us_only = df_results[df_results['country'] == 'United States']
if len(us_only) > 0:
    print(f"\nUS Elections - Offices:")
    print(us_only['office'].value_counts().to_string())

    print(f"\nUS Elections - Primaries vs General:")
    print(us_only['is_primary'].value_counts().to_string())

    print(f"\nUS Elections - Years:")
    print(us_only['election_year'].value_counts().sort_index().to_string())

# Save results to CSV for review
test_output_file = f"{DATA_DIR}/test_electoral_classifications.csv"

# Merge with original data
test_results = test_batch.copy()
for classification in classifications:
    idx = classification['index']
    if idx in test_results.index:
        test_results.at[idx, 'country'] = classification.get('country')
        test_results.at[idx, 'office'] = classification.get('office')
        test_results.at[idx, 'location'] = classification.get('location')
        test_results.at[idx, 'election_year'] = classification.get('election_year')
        test_results.at[idx, 'is_primary'] = classification.get('is_primary')

# Select relevant columns
output_cols = ['platform', 'market_id', 'question', 'trading_close_time',
               'country', 'office', 'location', 'election_year', 'is_primary']
test_results[output_cols].to_csv(test_output_file, index=False)

print(f"\n{'=' * 80}")
print("TEST COMPLETE")
print(f"{'=' * 80}")
print(f"\n✓ Test results saved to: {test_output_file}")
print(f"\nReview the CSV file to check classifications.")
print(f"If they look good, run the full script: python3 scripts/classify_electoral_details.py")
