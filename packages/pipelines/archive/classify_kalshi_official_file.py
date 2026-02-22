#!/usr/bin/env python3
"""
Classify Kalshi Official Election Winner Markets using EXACT SAME logic as classify_electoral_details.py
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
INPUT_FILE = f"{DATA_DIR}/Election winner markets since Jan 2024 - Markets list.csv"
OUTPUT_FILE = f"{DATA_DIR}/kalshi_official_with_electoral_details_FULL.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/kalshi_official_classification_checkpoint_FULL.json"
API_KEY_FILE = f"{BASE_DIR}/openai_api_key.txt"

# Configuration
BATCH_SIZE = 50
MODEL = "gpt-4o"
MAX_RETRIES = 3
RETRY_DELAY = 5

print("=" * 80)
print("KALSHI OFFICIAL FILE CLASSIFICATION WITH GPT-4O")
print("=" * 80)

# Load API key
with open(API_KEY_FILE, 'r') as f:
    api_key = f.read().strip()

client = OpenAI(api_key=api_key)

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

df = pd.read_csv(INPUT_FILE, skiprows=2)
print(f"✓ Loaded {len(df):,} markets to classify")

# Load checkpoint
processed_indices = set()
classifications_dict = {}

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint_data = json.load(f)
        processed_indices = set(checkpoint_data.get('processed_indices', []))
        classifications_dict = checkpoint_data.get('classifications', {})
    print(f"✓ Loaded checkpoint: {len(processed_indices):,} markets already processed")

# Get remaining markets to process
remaining = df[~df.index.isin(processed_indices)].copy()

print(f"\nTotal markets: {len(df):,}")
print(f"Already processed: {len(processed_indices):,}")
print(f"Remaining: {len(remaining):,}")

# Initialize columns
for col in ['country', 'office', 'location', 'election_year', 'is_primary']:
    if col not in df.columns:
        df[col] = None

# Classification function - EXACT COPY from original script
def classify_batch(batch_df):
    """Classify a batch of markets using GPT-4o - EXACT SAME LOGIC as original"""

    # Prepare market data for the prompt (only title is reliable, not corrupted)
    markets_data = []
    for idx, row in batch_df.iterrows():
        markets_data.append({
            "index": int(idx),
            "question": row['title']
        })

    # Create the enhanced prompt with election cycle logic
    prompt = f"""You are analyzing political prediction markets to extract structured information about US elections.

IMPORTANT CONTEXT:
- All elections in this dataset are from 2024 or later
- Today's date context: Markets created since January 2024

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

4. **election_year**: Year the election takes place (integer) - MUST BE 2024 OR LATER
   - Extract from question text first (e.g., "2024", "2026", "2028")
   - If year not explicitly in question, use election cycle patterns to infer:
     * Presidential: Only 2024, 2028, 2032 (every 4 years)
     * House: Every even year (2024, 2026, 2028, 2030...)
     * Senate: Every even year (2024, 2026, 2028...), but depends on state's cycle
     * Governor: Most are every 4 years, varies by state (could be 2024, 2025, 2026, 2027, 2028...)
     * Mayor: Varies by city, can be any year including odd years (2024, 2025, 2026, 2027...)
   - If still unclear after using question text and cycle patterns, set to null
   - CONSTRAINT: Year must be >= 2024

5. **is_primary**: Type of election (boolean)
   - true if: question contains "primary", "caucus", "nomination", "nominate", "nominee"
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

    # Call OpenAI API - EXACT SAME as original
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

        # Parse response - EXACT SAME logic as original
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

# Process batches
if len(remaining) == 0:
    print("\n✓ All markets already classified!")
else:
    print(f"\n{'=' * 80}")
    print("PROCESSING BATCHES")
    print(f"{'=' * 80}")

    total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nTotal batches: {total_batches}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Model: {MODEL}")

    batch_num = 0
    start_time = time.time()

    for start_idx in range(0, len(remaining), BATCH_SIZE):
        batch_num += 1
        batch = remaining.iloc[start_idx:start_idx + BATCH_SIZE]

        print(f"\n{'─' * 80}")
        print(f"Batch {batch_num}/{total_batches} | Markets {start_idx + 1}-{min(start_idx + BATCH_SIZE, len(remaining))} of {len(remaining)}")

        # Calculate progress
        progress_pct = (batch_num / total_batches) * 100
        elapsed_time = time.time() - start_time
        if batch_num > 1:
            avg_time_per_batch = elapsed_time / (batch_num - 1)
            remaining_batches = total_batches - batch_num
            est_time_remaining = avg_time_per_batch * remaining_batches
            est_minutes = int(est_time_remaining / 60)
            est_seconds = int(est_time_remaining % 60)
            print(f"Progress: {progress_pct:.1f}% | Estimated time remaining: {est_minutes}m {est_seconds}s")

        # Try with retries
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                classifications_list = classify_batch(batch)

                if len(classifications_list) != len(batch):
                    print(f"⚠ Warning: Got {len(classifications_list)} classifications for {len(batch)} markets")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue

                # Store classifications
                for classification in classifications_list:
                    idx = classification.get('index')
                    if idx in batch.index:
                        classifications_dict[str(idx)] = classification
                        processed_indices.add(idx)

                # Save checkpoint
                with open(CHECKPOINT_FILE, 'w') as f:
                    json.dump({
                        'processed_indices': list(processed_indices),
                        'classifications': classifications_dict
                    }, f)

                print(f"✓ Classified {len(classifications_list)} markets")
                success = True
                break

            except Exception as e:
                print(f"❌ Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        if not success:
            print(f"⚠ Batch {batch_num} failed after {MAX_RETRIES} attempts, skipping...")

# Apply classifications to dataframe
print(f"\n{'=' * 80}")
print("APPLYING CLASSIFICATIONS")
print(f"{'=' * 80}")

for idx_str, classification in classifications_dict.items():
    idx = int(idx_str)
    df.at[idx, 'country'] = classification.get('country')
    df.at[idx, 'office'] = classification.get('office')
    df.at[idx, 'location'] = classification.get('location')
    df.at[idx, 'election_year'] = classification.get('election_year')
    df.at[idx, 'is_primary'] = classification.get('is_primary')

# Save output
print(f"\nSaving to: {OUTPUT_FILE}")
df.to_csv(OUTPUT_FILE, index=False)
print(f"✓ Saved {len(df):,} markets with electoral classifications")

# Summary
print(f"\n{'=' * 80}")
print("CLASSIFICATION SUMMARY")
print(f"{'=' * 80}")

us_elections = df[df['country'] == 'United States']
print(f"\nUS Elections: {len(us_elections):,}")
print(f"Non-US Elections: {len(df) - len(us_elections):,}")

if len(us_elections) > 0:
    print(f"\nUS Elections by office:")
    print(us_elections['office'].value_counts())

    print(f"\nPrimary vs General:")
    primaries = us_elections['is_primary'].sum()
    generals = (~us_elections['is_primary']).sum()
    print(f"  Primaries: {primaries:,}")
    print(f"  General: {generals:,}")

print(f"\n✓ COMPLETE")
