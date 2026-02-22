#!/usr/bin/env python3
"""
Fix election years by searching Kalshi website with the actual question text
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
INPUT_FILE = f"{DATA_DIR}/kalshi_official_with_electoral_details_FULL_with_years.csv"
OUTPUT_FILE = f"{DATA_DIR}/kalshi_official_with_electoral_details_CORRECTED.csv"
CHECKPOINT_FILE = f"{DATA_DIR}/year_correction_checkpoint.json"
API_KEY_FILE = f"{BASE_DIR}/openai_api_key.txt"

# Configuration
BATCH_SIZE = 20
MODEL = "gpt-4o"
MAX_RETRIES = 3
RETRY_DELAY = 5

print("=" * 80)
print("CORRECTING ELECTION YEARS FROM KALSHI WEBSITE")
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

# Filter to US elections with years (we'll re-check all of them)
us_elections = df[df['country'] == 'United States'].copy()
with_years = us_elections[us_elections['election_year'].notna()].copy()

print(f"✓ US elections: {len(us_elections):,}")
print(f"✓ US elections with years to verify: {len(with_years):,}")

# Load checkpoint
processed_indices = set()
year_corrections = {}

if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint_data = json.load(f)
        processed_indices = set(checkpoint_data.get('processed_indices', []))
        year_corrections = checkpoint_data.get('year_corrections', {})
    print(f"✓ Loaded checkpoint: {len(processed_indices):,} markets already processed")

# Get remaining markets to process
remaining = with_years[~with_years.index.isin(processed_indices)].copy()

print(f"\nTotal to verify: {len(with_years):,}")
print(f"Already processed: {len(processed_indices):,}")
print(f"Remaining: {len(remaining):,}")

def lookup_years_from_kalshi(batch_df):
    """Look up election years by searching Kalshi website with question text"""

    # Prepare market data - ONLY use title (reliable field)
    markets_data = []
    for idx, row in batch_df.iterrows():
        markets_data.append({
            "index": int(idx),
            "question": row['title'],  # ONLY reliable field
            "office": row['office'],
            "location": row['location'],
            "is_primary": row['is_primary'],
            "current_year": int(row['election_year']) if pd.notna(row['election_year']) else None
        })

    # Create prompt that instructs GPT-4o to search Kalshi website by question text
    prompt = f"""You are helping to verify and correct election years for Kalshi prediction markets.

CRITICAL: For each market below, you MUST search the Kalshi website using the EXACT question text to find the market page.

INSTRUCTIONS:
1. For each market, search: "site:kalshi.com [exact question text]"
   Example: site:kalshi.com "Which party will win the House race for CA-09?"
2. Find the Kalshi market page that matches this exact question
3. Look at the market page to determine what election year it's actually about
4. Key information on Kalshi pages:
   - Market title often includes the year (e.g., "2026 California House Race")
   - Market subtitle or description specifies the election date
   - Close time indicates when the election happens
5. Return the CORRECT election year based on the actual Kalshi market page

IMPORTANT:
- The current_year shown below may be WRONG (e.g., CA-09 might be labeled 2024 but actually be 2026 or 2027)
- You MUST search Kalshi's website to verify the correct year
- Some districts have elections in different years than you might expect
- Trust the Kalshi market page over any assumptions

Markets to verify (search using the question field):
{json.dumps(markets_data, indent=2)}

Return your response as a JSON object with a single key "results" containing an array of objects. Each object must have: index, corrected_year (integer or null), confidence (high/medium/low).

Response format:
{{
  "results": [
    {{"index": 0, "corrected_year": 2026, "confidence": "high"}},
    {{"index": 1, "corrected_year": 2027, "confidence": "high"}},
    ...
  ]
}}"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that verifies election years by searching Kalshi's website. Always search using the exact question text. Return valid JSON only."},
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
    print("\n✓ All years already verified!")
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
                results_list = lookup_years_from_kalshi(batch)

                if len(results_list) != len(batch):
                    print(f"⚠ Warning: Got {len(results_list)} results for {len(batch)} markets")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue

                # Store year corrections and log changes
                changes = 0
                for result in results_list:
                    idx = result.get('index')
                    new_year = result.get('corrected_year')
                    confidence = result.get('confidence', 'unknown')

                    if idx in batch.index:
                        old_year = batch.loc[idx, 'election_year']
                        year_corrections[str(idx)] = {
                            'corrected_year': new_year,
                            'old_year': old_year,
                            'confidence': confidence
                        }
                        processed_indices.add(idx)

                        # Log if year changed
                        if new_year != old_year:
                            office = batch.loc[idx, 'office']
                            location = batch.loc[idx, 'location']
                            question = batch.loc[idx, 'title'][:60]
                            print(f"  📝 {office} {location}: {int(old_year) if pd.notna(old_year) else 'N/A'} → {int(new_year) if new_year else 'N/A'} ({confidence})")
                            print(f"     \"{question}...\"")
                            changes += 1

                # Save checkpoint
                with open(CHECKPOINT_FILE, 'w') as f:
                    json.dump({
                        'processed_indices': list(processed_indices),
                        'year_corrections': year_corrections
                    }, f)

                print(f"✓ Verified {len(results_list)} years ({changes} changes)")
                success = True
                break

            except Exception as e:
                print(f"❌ Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)

        if not success:
            print(f"⚠ Batch {batch_num} failed after {MAX_RETRIES} attempts, skipping...")

# Apply year corrections to dataframe
print(f"\n{'=' * 80}")
print("APPLYING YEAR CORRECTIONS")
print(f"{'=' * 80}")

years_changed = 0
years_unchanged = 0
years_set_to_null = 0

for idx_str, correction in year_corrections.items():
    idx = int(idx_str)
    new_year = correction['corrected_year']
    old_year = correction['old_year']

    if new_year != old_year:
        df.at[idx, 'election_year'] = new_year
        years_changed += 1
        if new_year is None:
            years_set_to_null += 1
    else:
        years_unchanged += 1

print(f"\nYears changed: {years_changed:,}")
print(f"Years unchanged: {years_unchanged:,}")
print(f"Years set to null: {years_set_to_null:,}")

# Save output
print(f"\nSaving to: {OUTPUT_FILE}")
df.to_csv(OUTPUT_FILE, index=False)
print(f"✓ Saved {len(df):,} markets with corrected years")

# Summary
print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

us_elections_updated = df[df['country'] == 'United States'].copy()
still_missing = us_elections_updated['election_year'].isna().sum()

print(f"\nUS Elections: {len(us_elections_updated):,}")
print(f"Elections with years: {len(us_elections_updated) - still_missing:,}")
print(f"Elections missing years: {still_missing:,}")

if len(us_elections_updated[us_elections_updated['election_year'].notna()]) > 0:
    print(f"\nYear distribution after correction:")
    print(us_elections_updated['election_year'].value_counts().sort_index())

print(f"\n✓ COMPLETE")
