#!/usr/bin/env python3
"""
Fill missing election years using GPT-4o with web search
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
INPUT_FILE = f"{DATA_DIR}/kalshi_official_with_electoral_details_FULL.csv"
OUTPUT_FILE = f"{DATA_DIR}/kalshi_official_with_electoral_details_FULL_with_years.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/year_lookup_checkpoint.json"
API_KEY_FILE = f"{BASE_DIR}/openai_api_key.txt"

# Configuration
BATCH_SIZE = 20  # Smaller batches since we're doing web searches
MODEL = "gpt-4o"
MAX_RETRIES = 3
RETRY_DELAY = 5

print("=" * 80)
print("FILLING MISSING ELECTION YEARS WITH WEB SEARCH")
print("=" * 80)

# Load API key
with open(API_KEY_FILE, 'r') as f:
    api_key = f.read().strip()

client = OpenAI(api_key=api_key)

# Load data
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

df = pd.read_csv(INPUT_FILE)
print(f"✓ Loaded {len(df):,} total markets")

# Filter to US elections with null years
us_elections = df[df['country'] == 'United States'].copy()
missing_years = us_elections[us_elections['election_year'].isna()].copy()

print(f"✓ US elections: {len(us_elections):,}")
print(f"✓ Missing years: {len(missing_years):,}")

# Load checkpoint
processed_indices = set()
year_updates = {}

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint_data = json.load(f)
        processed_indices = set(checkpoint_data.get('processed_indices', []))
        year_updates = checkpoint_data.get('year_updates', {})
    print(f"✓ Loaded checkpoint: {len(processed_indices):,} markets already processed")

# Get remaining markets to process
remaining = missing_years[~missing_years.index.isin(processed_indices)].copy()

print(f"\nTotal missing years: {len(missing_years):,}")
print(f"Already processed: {len(processed_indices):,}")
print(f"Remaining: {len(remaining):,}")

def lookup_years_batch(batch_df):
    """Look up election years using GPT-4o with web search"""

    # Prepare market data
    markets_data = []
    for idx, row in batch_df.iterrows():
        markets_data.append({
            "index": int(idx),
            "office": row['office'],
            "location": row['location'],
            "is_primary": row['is_primary'],
            "question": row['title']
        })

    # Create prompt that instructs GPT-4o to search the web
    prompt = f"""You are helping to determine the election year for US political markets. All elections in this dataset are from January 2024 onwards.

For each market below, search the web to find when this specific election or primary is scheduled to take place.

INSTRUCTIONS:
1. For each market, search online for the election year
2. Use the office, location, and whether it's a primary to search effectively
3. Examples of good searches:
   - "Georgia Governor election 2024 2025 2026"
   - "California Governor Democratic primary 2026"
   - "Pennsylvania Senate race 2024"
   - "New York City Mayor election 2025"

4. Return the year the election/primary takes place (must be >= 2024)
5. If you cannot find the year with certainty, set it to null

Markets to look up:
{json.dumps(markets_data, indent=2)}

Return your response as a JSON object with a single key "results" containing an array of objects. Each object must have: index, election_year (integer or null).

Response format:
{{
  "results": [
    {{"index": 0, "election_year": 2024}},
    {{"index": 1, "election_year": 2026}},
    ...
  ]
}}"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that looks up US election dates. Always search the web to find accurate information. Return valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            response_format={"type": "json_object"}
        )

        result_text = response.choices[0].message.content
        result = json.loads(result_text)

        # Handle different possible response formats
        if isinstance(result, dict) and 'results' in result:
            results_list = result['results']
        elif isinstance(result, dict) and 'years' in result:
            results_list = result['years']
        elif isinstance(result, list):
            results_list = result
        else:
            results_list = list(result.values())

        return results_list

    except Exception as e:
        print(f"\n❌ Error in API call: {e}")
        raise

# Process batches
if len(remaining) == 0:
    print("\n✓ All missing years already processed!")
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
                results_list = lookup_years_batch(batch)

                if len(results_list) != len(batch):
                    print(f"⚠ Warning: Got {len(results_list)} results for {len(batch)} markets")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue

                # Store year updates
                for result in results_list:
                    idx = result.get('index')
                    year = result.get('election_year')
                    if idx in batch.index:
                        year_updates[str(idx)] = year
                        processed_indices.add(idx)

                # Save checkpoint
                with open(CHECKPOINT_FILE, 'w') as f:
                    json.dump({
                        'processed_indices': list(processed_indices),
                        'year_updates': year_updates
                    }, f)

                print(f"✓ Looked up {len(results_list)} years")
                success = True
                break

            except Exception as e:
                print(f"❌ Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        if not success:
            print(f"⚠ Batch {batch_num} failed after {MAX_RETRIES} attempts, skipping...")

# Apply year updates to dataframe
print(f"\n{'=' * 80}")
print("APPLYING YEAR UPDATES")
print(f"{'=' * 80}")

years_filled = 0
years_still_null = 0

for idx_str, year in year_updates.items():
    idx = int(idx_str)
    if year is not None:
        df.at[idx, 'election_year'] = year
        years_filled += 1
    else:
        years_still_null += 1

print(f"\nYears successfully filled: {years_filled:,}")
print(f"Years still null (could not determine): {years_still_null:,}")

# Save output
print(f"\nSaving to: {OUTPUT_FILE}")
df.to_csv(OUTPUT_FILE, index=False)
print(f"✓ Saved {len(df):,} markets with updated years")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

us_elections_updated = df[df['country'] == 'United States'].copy()
still_missing = us_elections_updated['election_year'].isna().sum()

print(f"\nUS Elections: {len(us_elections_updated):,}")
print(f"Elections with years: {len(us_elections_updated) - still_missing:,}")
print(f"Elections still missing years: {still_missing:,}")

if len(us_elections_updated[us_elections_updated['election_year'].notna()]) > 0:
    print(f"\nYear distribution:")
    print(us_elections_updated['election_year'].value_counts().sort_index())

print(f"\n✓ COMPLETE")
