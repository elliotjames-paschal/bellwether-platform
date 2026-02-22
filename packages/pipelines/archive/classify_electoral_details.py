#!/usr/bin/env python3
"""
Classify Electoral Market Details with GPT-4o

For markets in the '1. ELECTORAL' category, use GPT-4o to extract:
- country: Must be "United States" to proceed
- office: President, Senate, House, Governor, Lt. Governor, Attorney General, Secretary of State, Mayor
- location: Depends on office type
- election_year: Year of the election
- is_primary: True/False for primary vs general election

Only processes US elections. Non-US electoral markets are skipped.
"""

import pandas as pd
import json
import os
import time
from datetime import datetime
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
INPUT_FILE = f"{DATA_DIR}/polymarket_untagged_political_markets_categorized.csv"
OUTPUT_FILE = f"{DATA_DIR}/polymarket_untagged_electoral_details.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/polymarket_electoral_classification_checkpoint.json"
API_KEY_FILE = f"{BASE_DIR}/openai_api_key.txt"

# Configuration
BATCH_SIZE = 20
MODEL = "gpt-4o"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

print("=" * 80)
print("ELECTORAL MARKET CLASSIFICATION WITH GPT-4O")
print("=" * 80)

# Load API key
with open(API_KEY_FILE, 'r') as f:
    api_key = f.read().strip()

# Initialize OpenAI client
client = OpenAI(api_key=api_key)

# ============================================================================
# Load Data
# ============================================================================

print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

print(f"\nLoading: {INPUT_FILE}")
df = pd.read_csv(INPUT_FILE, low_memory=False)
print(f"✓ Loaded {len(df):,} total markets")

# Filter for electoral markets only
electoral = df[df['political_category'] == '1. ELECTORAL'].copy()
print(f"✓ Found {len(electoral):,} electoral markets to classify")

# ============================================================================
# Define Classification Schema
# ============================================================================

CLASSIFICATION_SCHEMA = {
    "type": "object",
    "properties": {
        "country": {
            "type": "string",
            "description": "The country this election takes place in. Use 'United States' for US elections."
        },
        "office": {
            "type": ["string", "null"],
            "enum": ["President", "Vice President", "Senate", "House", "Governor", "Lt. Governor", "Attorney General", "Secretary of State", "Mayor", None],
            "description": "The elected office. Only applicable for US elections. Must be from approved list."
        },
        "location": {
            "type": ["string", "null"],
            "description": "For President: 'United States'. For Senate/Governor/statewide: full state name. For House: district code. For Mayor: city name."
        },
        "election_year": {
            "type": ["integer", "null"],
            "description": "The year the election takes place (e.g., 2024, 2022)."
        },
        "is_primary": {
            "type": ["boolean", "null"],
            "description": "True if this is a primary/caucus election, False if general election."
        }
    },
    "required": ["country", "office", "location", "election_year", "is_primary"],
    "additionalProperties": False
}

# ============================================================================
# Load Checkpoint if Exists
# ============================================================================

processed_indices = set()
classifications = {}

if os.path.exists(CHECKPOINT_FILE):
    print(f"\n✓ Found checkpoint file: {CHECKPOINT_FILE}")
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint_data = json.load(f)
        processed_indices = set(checkpoint_data.get('processed_indices', []))
        classifications = checkpoint_data.get('classifications', {})
    print(f"✓ Resuming from checkpoint: {len(processed_indices):,} markets already processed")
else:
    print(f"\n⚠ No checkpoint found. Starting fresh.")

# Get markets that still need processing
remaining = electoral[~electoral.index.isin(processed_indices)]
print(f"\n📊 Markets remaining to process: {len(remaining):,}")

if len(remaining) == 0:
    print("\n✓ All markets already processed!")
else:
    # ============================================================================
    # Classification Function
    # ============================================================================

    def classify_batch(batch_df):
        """Classify a batch of markets using GPT-4o"""

        # Prepare market data for the prompt
        markets_data = []
        for idx, row in batch_df.iterrows():
            markets_data.append({
                "index": int(idx),
                "id": int(row['id']),
                "question": row['question']
            })

        # Create the enhanced prompt
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
   - Extract from question text if explicitly mentioned (e.g., "2024", "2022")
   - SPECIAL CASE - Markets about someone REMAINING in office:
     * If question asks "Will [person] be/remain [office] on [date]" (e.g., "Will Biden be President on April 30, 2021?")
     * Determine which election PUT that person in office
     * Set election_year to that election year
     * Examples:
       - "Will Joe Biden be President on April 30, 2021?" → election_year: 2020 (Biden elected in 2020)
       - "Will Donald Trump be President on July 31, 2021?" → election_year: 2020 (Trump ran in 2020, lost)
       - "Will Putin remain President through 2022?" → Use the most recent presidential election year for that country
   - If election year cannot be determined, set to null

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

Example 7 (REMAINING IN OFFICE):
Question: "Will Joe Biden be President of the USA on April 30, 2021?"
→ country: "United States", office: "President", location: "United States", election_year: 2020, is_primary: false
(Biden was elected in the 2020 presidential election)

Example 8 (REMAINING IN OFFICE):
Question: "Will Donald Trump be President of the USA on July 31, 2021?"
→ country: "United States", office: "President", location: "United States", election_year: 2020, is_primary: false
(Trump ran in the 2020 presidential election but lost)

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

        # Call OpenAI API with structured output
        try:
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

            # Handle both array and object with various keys
            if isinstance(result, dict) and 'classifications' in result:
                classifications_list = result['classifications']
            elif isinstance(result, dict) and 'markets' in result:
                classifications_list = result['markets']
            elif isinstance(result, dict) and 'results' in result:
                classifications_list = result['results']
            elif isinstance(result, list):
                classifications_list = result
            else:
                # If it's a dict with keys that are indices, convert to list
                classifications_list = list(result.values())

            return classifications_list

        except Exception as e:
            print(f"\n❌ Error in API call: {e}")
            raise

    # ============================================================================
    # Process Batches
    # ============================================================================

    print(f"\n{'=' * 80}")
    print("PROCESSING BATCHES")
    print(f"{'=' * 80}")

    total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nTotal batches: {total_batches}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Model: {MODEL}")

    batch_num = 0
    start_time = time.time()
    house_examples = []

    for start_idx in range(0, len(remaining), BATCH_SIZE):
        batch_num += 1
        batch = remaining.iloc[start_idx:start_idx + BATCH_SIZE]

        print(f"\n{'─' * 80}")
        print(f"Batch {batch_num}/{total_batches} | Markets {start_idx + 1}-{min(start_idx + BATCH_SIZE, len(remaining))} of {len(remaining)}")

        # Calculate progress and estimated time
        progress_pct = (batch_num / total_batches) * 100
        elapsed_time = time.time() - start_time
        if batch_num > 1:
            avg_time_per_batch = elapsed_time / (batch_num - 1)
            remaining_batches = total_batches - batch_num
            est_time_remaining = avg_time_per_batch * remaining_batches
            est_minutes = int(est_time_remaining / 60)
            est_seconds = int(est_time_remaining % 60)
            print(f"Progress: {progress_pct:.1f}% | Estimated time remaining: {est_minutes}m {est_seconds}s")

        # Attempt classification with retries
        retry_count = 0
        success = False

        while retry_count < MAX_RETRIES and not success:
            try:
                batch_results = classify_batch(batch)

                # Store results and log House districts
                for result in batch_results:
                    idx = result['index']
                    classifications[str(idx)] = {
                        'country': result.get('country'),
                        'office': result.get('office'),
                        'location': result.get('location'),
                        'election_year': result.get('election_year'),
                        'is_primary': result.get('is_primary')
                    }
                    processed_indices.add(idx)

                    # Log House district examples
                    if result.get('office') == 'House' and result.get('location'):
                        original_question = batch.loc[idx, 'question']
                        house_example = {
                            'id': batch.loc[idx, 'id'],
                            'question': original_question[:80] + ('...' if len(original_question) > 80 else ''),
                            'location': result.get('location')
                        }
                        house_examples.append(house_example)
                        print(f"  🏛️  House: {house_example['location']} | {house_example['question']}")

                success = True
                print(f"✓ Classified {len(batch_results)} markets")

            except Exception as e:
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    print(f"⚠ Retry {retry_count}/{MAX_RETRIES} after error: {e}")
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"❌ Failed after {MAX_RETRIES} retries: {e}")
                    raise

        # Save checkpoint after each batch
        checkpoint_data = {
            'processed_indices': list(processed_indices),
            'classifications': classifications,
            'last_updated': datetime.now().isoformat()
        }

        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)

        print(f"💾 Checkpoint saved ({len(processed_indices):,} markets processed)")

        # Small delay to respect rate limits
        if batch_num < total_batches:
            time.sleep(1)

    # Summary of House districts found
    if house_examples:
        print(f"\n{'=' * 80}")
        print(f"HOUSE DISTRICT SUMMARY")
        print(f"{'=' * 80}")
        print(f"\nFound {len(house_examples)} House district markets")
        print(f"Sample of House districts identified:")
        for example in house_examples[:10]:
            print(f"  {example['location']:8s} | {str(example['id']):15s} | {example['question']}")
        if len(house_examples) > 10:
            print(f"  ... and {len(house_examples) - 10} more")

# ============================================================================
# Merge Results Back to DataFrame
# ============================================================================

print(f"\n{'=' * 80}")
print("MERGING RESULTS")
print(f"{'=' * 80}")

# Initialize new columns
df['country'] = None
df['office'] = None
df['location'] = None
df['election_year'] = None
df['is_primary'] = None

# Merge classifications back
for idx_str, classification in classifications.items():
    idx = int(idx_str)
    if idx in df.index:
        df.at[idx, 'country'] = classification['country']
        df.at[idx, 'office'] = classification['office']
        df.at[idx, 'location'] = classification['location']
        df.at[idx, 'election_year'] = classification['election_year']
        df.at[idx, 'is_primary'] = classification['is_primary']

# Convert types
df['election_year'] = pd.to_numeric(df['election_year'], errors='coerce')
df['is_primary'] = df['is_primary'].astype('boolean')

print(f"\n✓ Merged {len(classifications):,} classifications")

# ============================================================================
# Summary Statistics
# ============================================================================

print(f"\n{'=' * 80}")
print("SUMMARY STATISTICS")
print(f"{'=' * 80}")

classified = df[df['country'].notna()]
us_elections = df[df['country'] == 'United States']

print(f"\nTotal markets with classifications: {len(classified):,}")
print(f"US elections: {len(us_elections):,}")
print(f"Non-US elections (skipped): {len(classified[classified['country'] != 'United States']):,}")

if len(us_elections) > 0:
    print(f"\n{'─' * 80}")
    print("US ELECTIONS BY OFFICE:")
    print(us_elections['office'].value_counts().to_string())

    print(f"\n{'─' * 80}")
    print("PRIMARIES VS GENERAL:")
    print(us_elections['is_primary'].value_counts().to_string())

    print(f"\n{'─' * 80}")
    print("TOP 10 LOCATIONS:")
    print(us_elections['location'].value_counts().head(10).to_string())

    print(f"\n{'─' * 80}")
    print("ELECTIONS BY YEAR:")
    print(us_elections['election_year'].value_counts().sort_index().to_string())

# ============================================================================
# Save Output
# ============================================================================

print(f"\n{'=' * 80}")
print("SAVING OUTPUT")
print(f"{'=' * 80}")

df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✓ Saved to: {OUTPUT_FILE}")

print(f"\n{'=' * 80}")
print("✓ CLASSIFICATION COMPLETE")
print(f"{'=' * 80}")
print(f"\nOutput: {OUTPUT_FILE}")
print(f"Total markets: {len(df):,}")
print(f"Electoral markets classified: {len(classified):,}")
print(f"US elections: {len(us_elections):,}")
