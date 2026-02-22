#!/usr/bin/env python3
"""
Truncate US Electoral Markets via ChatGPT

Sends US elections to ChatGPT to get election dates, then truncates price data
to midnight of election day (keeping all data up to start of election day).
"""

import pandas as pd
import json
import time
from datetime import datetime
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
ELECTIONS_FILE = f"{DATA_DIR}/us_elections_for_chatgpt.csv"
MARKETS_FILE = f"{DATA_DIR}/us_electoral_markets_for_truncation.csv"
PRICE_FILE = f"{DATA_DIR}/polymarket_all_political_prices_CORRECTED_v3.json"
ELECTION_DATES_OUTPUT = f"{DATA_DIR}/us_election_dates_chatgpt_new.json"
CHECKPOINT_FILE = f"{DATA_DIR}/truncate_us_electoral_checkpoint.json"

# API Configuration
BATCH_SIZE = 20
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5
API_KEY_FILE = f"{BASE_DIR}/openai_api_key.txt"

# Initialize OpenAI client
with open(API_KEY_FILE, 'r') as f:
    api_key = f.read().strip()
client = OpenAI(api_key=api_key)

print("=" * 80)
print("TRUNCATING US ELECTORAL MARKETS WITH CHATGPT")
print("=" * 80)
print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load elections
print(f"\n{'=' * 80}")
print("LOADING DATA")
print(f"{'=' * 80}")

elections_df = pd.read_csv(ELECTIONS_FILE)
print(f"✓ Unique elections: {len(elections_df)}")

markets_df = pd.read_csv(MARKETS_FILE)
print(f"✓ Markets to truncate: {len(markets_df)}")

# Load price data
with open(PRICE_FILE, 'r') as f:
    price_data = json.load(f)
print(f"✓ Loaded price data: {len(price_data):,} tokens")

# Check for checkpoint
election_dates = {}
start_batch = 0

try:
    with open(CHECKPOINT_FILE, 'r') as f:
        checkpoint = json.load(f)
        election_dates = checkpoint.get('election_dates', {})
        start_batch = checkpoint.get('last_batch_completed', 0) + 1
        print(f"✓ Loaded checkpoint: Starting from batch {start_batch + 1}")
except FileNotFoundError:
    print(f"✓ No checkpoint found, starting fresh")

def get_election_dates_batch(elections_batch):
    """Get election dates for a batch of elections from ChatGPT"""

    # Build prompt
    elections_list = []
    for idx, row in elections_batch.iterrows():
        office = row['office'] if pd.notna(row['office']) else "Unknown"
        location = row['location'] if pd.notna(row['location']) else "Unknown"
        year = row['election_year'] if pd.notna(row['election_year']) else "Unknown"
        is_primary = row['is_primary'] if pd.notna(row['is_primary']) else False

        election_type = "Primary" if is_primary else "General Election"

        elections_list.append({
            'index': idx,
            'office': office,
            'location': location,
            'year': year,
            'election_type': election_type,
            'sample_questions': row.get('sample_questions', '')
        })

    prompt = f"""For each of the following US elections, determine the exact election date in YYYY-MM-DD format.

Elections:
{json.dumps(elections_list, indent=2)}

Return a JSON object with the format:
{{
  "dates": [
    {{
      "index": <dataframe_index>,
      "election_date": "YYYY-MM-DD",
      "confidence": "high|medium|low"
    }},
    ...
  ]
}}

Important:
- Use the actual historical election date that occurred
- For primaries, use the specific primary date for that location
- If you cannot determine the date, use "election_date": null and "confidence": "low"
"""

    for attempt in range(RETRY_ATTEMPTS):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an expert on US election dates. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            return result.get('dates', [])

        except Exception as e:
            print(f"    ✗ Error in ChatGPT API call (attempt {attempt + 1}): {str(e)[:100]}")
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_DELAY)
            else:
                return None

    return None

# Process elections in batches
print(f"\n{'=' * 80}")
print("GETTING ELECTION DATES FROM CHATGPT")
print(f"{'=' * 80}")

total_batches = (len(elections_df) + BATCH_SIZE - 1) // BATCH_SIZE
print(f"\nTotal elections: {len(elections_df)}")
print(f"Batch size: {BATCH_SIZE}")
print(f"Total batches: {total_batches}")

success_count = 0
error_count = 0

for batch_num in range(start_batch, total_batches):
    start_idx = batch_num * BATCH_SIZE
    end_idx = min(start_idx + BATCH_SIZE, len(elections_df))
    batch = elections_df.iloc[start_idx:end_idx]

    print(f"\n[Batch {batch_num + 1}/{total_batches}] Processing elections {start_idx + 1}-{end_idx}...")

    dates = get_election_dates_batch(batch)

    if dates:
        for date_info in dates:
            idx = date_info['index']
            election_date = date_info.get('election_date')
            confidence = date_info.get('confidence', 'unknown')

            if election_date:
                row = elections_df.loc[idx]
                key = f"{row['office']}|{row['location']}|{row['election_year']}|{row['is_primary']}"
                election_dates[key] = {
                    'office': row['office'],
                    'location': row['location'],
                    'election_year': row['election_year'],
                    'is_primary': row['is_primary'],
                    'election_date': election_date,
                    'confidence': confidence
                }
                success_count += 1
            else:
                error_count += 1
                print(f"    ⚠ No date returned for election {idx}")

        print(f"    ✓ Batch {batch_num + 1} completed")
    else:
        error_count += len(batch)
        print(f"    ✗ Batch {batch_num + 1} failed")

    # Save checkpoint
    checkpoint = {
        'last_batch_completed': batch_num,
        'election_dates': election_dates,
        'timestamp': datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

    time.sleep(1)  # Rate limiting

print(f"\n✓ ChatGPT labeling complete")
print(f"  Success: {success_count}")
print(f"  Errors: {error_count}")

# Save election dates
with open(ELECTION_DATES_OUTPUT, 'w') as f:
    json.dump(election_dates, f, indent=2)
print(f"\n✓ Saved election dates to: {ELECTION_DATES_OUTPUT}")

# Truncate price data
print(f"\n{'=' * 80}")
print("TRUNCATING PRICE DATA")
print(f"{'=' * 80}")

truncated_count = 0
no_date_count = 0
no_price_count = 0

# Create a lookup dict for election dates
election_date_lookup = {}
for key, info in election_dates.items():
    parts = key.split('|')
    office = parts[0]
    location = parts[1]
    year = parts[2]
    is_primary = parts[3] == 'True'

    election_date_lookup[(office, location, year, is_primary)] = info['election_date']

for idx, market in markets_df.iterrows():
    office = str(market['office']) if pd.notna(market['office']) else 'nan'
    location = str(market['location']) if pd.notna(market['location']) else 'nan'
    year = str(market['election_year']) if pd.notna(market['election_year']) else 'nan'
    is_primary = market['is_primary']

    # Look up election date
    lookup_key = (office, location, year, is_primary)
    election_date_str = election_date_lookup.get(lookup_key)

    if not election_date_str:
        no_date_count += 1
        continue

    # Parse election date
    election_dt = datetime.strptime(election_date_str, '%Y-%m-%d')
    cutoff_timestamp = int(election_dt.timestamp())

    # Truncate YES token
    token_yes = str(market['pm_token_id_yes'])
    if token_yes in price_data:
        original_len = len(price_data[token_yes])
        price_data[token_yes] = [p for p in price_data[token_yes] if p['t'] <= cutoff_timestamp]
        new_len = len(price_data[token_yes])

        if new_len < original_len:
            truncated_count += 1
    else:
        no_price_count += 1

    # Truncate NO token if exists
    token_no = str(market['pm_token_id_no'])
    if pd.notna(token_no) and token_no != '' and token_no in price_data:
        price_data[token_no] = [p for p in price_data[token_no] if p['t'] <= cutoff_timestamp]

print(f"\n✓ Truncation complete")
print(f"  Markets truncated: {truncated_count}")
print(f"  No election date: {no_date_count}")
print(f"  No price data: {no_price_count}")

# Save updated price data
print(f"\n{'=' * 80}")
print("SAVING UPDATED PRICE DATA")
print(f"{'=' * 80}")

# Backup original
backup_file = PRICE_FILE.replace('.json', f'_backup_us_electoral_truncate_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
with open(PRICE_FILE, 'r') as f:
    original_data = json.load(f)
with open(backup_file, 'w') as f:
    json.dump(original_data, f)
print(f"✓ Backed up original to: {backup_file}")

# Save updated data
with open(PRICE_FILE, 'w') as f:
    json.dump(price_data, f)
print(f"✓ Saved updated price data")

print(f"\n{'=' * 80}")
print("COMPLETE")
print(f"{'=' * 80}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
