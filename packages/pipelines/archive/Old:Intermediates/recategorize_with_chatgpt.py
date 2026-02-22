#!/usr/bin/env python3
"""
Recategorize election markets using ChatGPT API.

Processes markets with election_type in:
- Electoral
- NULL/blank
- Special Election
- Midterm

Uses OpenAI ChatGPT to categorize into standardized categories.
Processes in batches of 50 to improve efficiency.
"""

import pandas as pd
import json
import os
import sys
from datetime import datetime
from openai import OpenAI
import time

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
LOG_FILE = f"{BASE_DIR}/recategorization_progress.log"

PM_CATEGORIES_FILE = f"{DATA_DIR}/market_categories_with_outcomes.csv"
PM_ELECTION_KEYS_FILE = f"{DATA_DIR}/polymarket_with_election_keys.csv"
KALSHI_ELECTION_KEYS_FILE = f"{DATA_DIR}/kalshi_with_election_keys.json"

# Setup logging
class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, 'w')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = Logger(LOG_FILE)
sys.stderr = Logger(LOG_FILE)

print(f"Log file: {LOG_FILE}")
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# OpenAI API setup
# Try to load API key from file first, then environment variable
api_key = None

# Option 1: Load from openai_api_key.txt file
api_key_file = f"{BASE_DIR}/openai_api_key.txt"
if os.path.exists(api_key_file):
    with open(api_key_file, 'r') as f:
        api_key = f.read().strip()
    print(f"✓ Loaded API key from {api_key_file}")

# Option 2: Load from environment variable
if not api_key:
    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        print("✓ Loaded API key from OPENAI_API_KEY environment variable")

if not api_key:
    print("ERROR: No API key found!")
    print("Please either:")
    print(f"  1. Create a file: {api_key_file}")
    print("     containing your OpenAI API key")
    print("  2. Set environment variable: export OPENAI_API_KEY='your-key'")
    sys.exit(1)

client = OpenAI(api_key=api_key)

# Batch size
BATCH_SIZE = 10

# Allowed categories with descriptions
ALLOWED_CATEGORIES = {
    "Presidential": "US presidential general election",
    "Presidential Primary": "US presidential primary (Democratic or Republican)",
    "Senate": "US Senate general election",
    "Senate Primary": "US Senate primary election",
    "House": "US House of Representatives general election",
    "House Primary": "US House of Representatives primary election",
    "VP Nomination": "US Vice President nomination market",
    "Gubernatorial": "US state governor general election",
    "Gubernatorial Primary": "US state governor primary election",
    "Mayoral": "US city mayor general election",
    "Mayoral Primary": "US city mayor primary election",
    "Democratic Primary": "Democratic party primary across multiple offices",
    "Republican Primary": "Republican party primary across multiple offices",
    "Parliamentary": "Parliamentary election (non-US, legislature)",
    "Prime Minister": "Prime Minister election or selection (non-US)",
    "General Election": "General/national election (non-US, not otherwise specified)",
    "European Parliament": "European Parliament election",
    "Regional Election": "Regional/provincial/state election (non-US)",
    "Chancellor": "Chancellor election (e.g., Germany)",
    "National Election": "National-level election (non-US, general)",
    "Provincial": "Provincial election (non-US)",
}

print("="*100)
print("ELECTION TYPE RECATEGORIZATION - PHASE 2: CHATGPT API")
print("="*100)

def categorize_batch(questions_batch):
    """
    Categorize a batch of up to 10 questions using ChatGPT.

    Args:
        questions_batch: List of question texts

    Returns:
        list: List of categories (same order as input)
    """
    # Build category list for prompt
    category_list = "\n".join([f"- {cat}: {desc}" for cat, desc in ALLOWED_CATEGORIES.items()])

    # Build questions as JSON array
    questions_json = json.dumps(questions_batch)

    prompt = f"""You are categorizing prediction market questions about elections.

ALLOWED CATEGORIES:
{category_list}

MARKET QUESTIONS (as JSON array):
{questions_json}

TASK: For each question in the array, determine which election type category it BEST fits into. You MUST choose one of the allowed categories - you cannot skip a question or use any other category name.

CRITICAL: You MUST return EXACTLY {len(questions_batch)} categories in your response array, one for each question in the input. No more, no less. The number of categories must match the number of questions exactly.

RESPONSE FORMAT: Return ONLY a JSON object with a "categories" key containing an array of category names, in the same order as the input questions. Do not include any other text or explanation.

Example response format: {{"categories": ["Presidential", "Senate", "House", ...]}}

Your response (JSON object only):"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a political election categorization expert. You must respond with ONLY a valid JSON object containing a 'categories' array. No explanations, no markdown formatting, just the raw JSON object. Every question must be assigned to exactly one of the allowed categories. The 'categories' array must have EXACTLY the same number of elements as the input questions array."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            response_format={"type": "json_object"}
        )

        # Parse response
        response_text = response.choices[0].message.content.strip()

        # Try to parse as JSON
        try:
            # If response is wrapped in object, extract the array
            response_json = json.loads(response_text)
            if isinstance(response_json, dict):
                # Look for array in the dict
                categories = response_json.get('categories') or response_json.get('results') or list(response_json.values())[0]
            else:
                categories = response_json
        except json.JSONDecodeError as e:
            print(f"  ⚠ JSON parse error: {e}")
            print(f"  Response was: {response_text[:200]}...")
            return [None] * len(questions_batch)

        # Validate length
        if len(categories) != len(questions_batch):
            print(f"  ⚠ Response length mismatch: expected {len(questions_batch)}, got {len(categories)}")
            return [None] * len(questions_batch)

        # Validate each category
        validated = []
        for cat in categories:
            if cat in ALLOWED_CATEGORIES:
                validated.append(cat)
            else:
                print(f"  ⚠ Unexpected category '{cat}' - marking as None")
                validated.append(None)

        return validated

    except Exception as e:
        print(f"  ⚠ API error: {e}")
        return [None] * len(questions_batch)


def process_markets(markets_list, dataset_name):
    """
    Process a list of markets through ChatGPT in batches.

    Args:
        markets_list: List of dicts with 'id' and 'question' keys
        dataset_name: Name for logging

    Returns:
        dict: Mapping of market id to new category
    """
    results = {}
    total = len(markets_list)

    print(f"\n{'='*100}")
    print(f"{dataset_name}")
    print(f"{'='*100}")
    print(f"Processing {total:,} markets in batches of {BATCH_SIZE}...")

    # Process in batches
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = markets_list[batch_start:batch_end]

        batch_num = (batch_start // BATCH_SIZE) + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"\nBatch {batch_num}/{total_batches} (markets {batch_start+1}-{batch_end}):")

        # Extract questions
        questions = [m['question'] for m in batch]

        # Categorize
        categories = categorize_batch(questions)

        # Store results
        for market, category in zip(batch, categories):
            results[market['id']] = category

        # Show batch results
        categorized_count = sum(1 for c in categories if c is not None)
        print(f"  ✓ Categorized: {categorized_count}/{len(batch)}")

        # Rate limiting between batches
        if batch_end < total:
            time.sleep(1)

    # Overall summary
    categorized = sum(1 for v in results.values() if v is not None)
    unknown = sum(1 for v in results.values() if v is None)

    print(f"\n{'='*100}")
    print(f"SUMMARY - {dataset_name}")
    print(f"{'='*100}")
    print(f"Total markets: {total:,}")
    print(f"Categorized: {categorized:,} ({categorized/total*100:.1f}%)")
    print(f"Unknown: {unknown:,} ({unknown/total*100:.1f}%)")

    return results


# ============================================================================
# Load markets needing recategorization
# ============================================================================

print(f"\n{'='*100}")
print("LOADING MARKETS FOR RECATEGORIZATION")
print(f"{'='*100}\n")

# Load Polymarket categories file
df_pm_categories = pd.read_csv(PM_CATEGORIES_FILE)
electoral_markets = df_pm_categories[df_pm_categories['political_category'] == '1. ELECTORAL'].copy()

pm_categories_to_fix = electoral_markets[
    electoral_markets['election_type'].isin(['Electoral', 'Special Election', 'Midterm']) |
    electoral_markets['election_type'].isna()
].copy()

print(f"market_categories_with_outcomes.csv: {len(pm_categories_to_fix):,} markets")
breakdown = pm_categories_to_fix['election_type'].value_counts(dropna=False)
for cat, count in breakdown.items():
    if pd.isna(cat):
        print(f"  - (blank/null): {count:,}")
    else:
        print(f"  - {cat}: {count:,}")

# Load Polymarket election keys file
df_pm_keys = pd.read_csv(PM_ELECTION_KEYS_FILE)
pm_keys_to_fix = df_pm_keys[
    df_pm_keys['election_type'].isin(['Electoral', 'Special Election', 'Midterm']) |
    df_pm_keys['election_type'].isna()
].copy()

print(f"\npolymarket_with_election_keys.csv: {len(pm_keys_to_fix):,} markets")
breakdown = pm_keys_to_fix['election_type'].value_counts(dropna=False)
for cat, count in breakdown.items():
    if pd.isna(cat):
        print(f"  - (blank/null): {count:,}")
    else:
        print(f"  - {cat}: {count:,}")

# Load Kalshi election keys file
with open(KALSHI_ELECTION_KEYS_FILE, 'r') as f:
    kalshi_data = json.load(f)

kalshi_to_fix = [
    m for m in kalshi_data
    if m.get('election_type') in ['Electoral', 'Special Election', 'Midterm', None]
]

print(f"\nkalshi_with_election_keys.json: {len(kalshi_to_fix):,} markets")
kalshi_breakdown = {}
for m in kalshi_to_fix:
    cat = m.get('election_type')
    kalshi_breakdown[cat] = kalshi_breakdown.get(cat, 0) + 1
for cat, count in sorted(kalshi_breakdown.items(), key=lambda x: (x[0] is None, x[0])):
    if cat is None:
        print(f"  - (blank/null): {count:,}")
    else:
        print(f"  - {cat}: {count:,}")


# ============================================================================
# Ask user to confirm before spending API credits
# ============================================================================

print(f"\n{'='*100}")
print("API USAGE ESTIMATE")
print(f"{'='*100}\n")

total_markets = len(pm_categories_to_fix)
num_batches = (total_markets + BATCH_SIZE - 1) // BATCH_SIZE
estimated_cost = num_batches * 0.005  # Rough estimate with gpt-4o-mini batching
print(f"Total markets to process: {total_markets:,}")
print(f"Number of batches: {num_batches:,}")
print(f"Estimated cost (gpt-4o-mini): ~${estimated_cost:.2f}")
print(f"\nNote: This will make {num_batches:,} API calls to OpenAI")
print(f"\n✓ Proceeding automatically with API calls...")


# ============================================================================
# Process markets through ChatGPT
# ============================================================================

print(f"\n{'='*100}")
print("PROCESSING MARKETS")
print(f"{'='*100}")

# Prepare batch for market_categories_with_outcomes.csv
pm_categories_batch = [
    {'id': idx, 'question': row['question']}
    for idx, row in pm_categories_to_fix.iterrows()
]

# Process
pm_categories_results = process_markets(pm_categories_batch, "market_categories_with_outcomes.csv")

# ============================================================================
# Apply results to market_categories_with_outcomes.csv
# ============================================================================

print(f"\n{'='*100}")
print("UPDATING FILES")
print(f"{'='*100}\n")

# Create backup
backup_file = f"{DATA_DIR}/market_categories_with_outcomes_BACKUP_PHASE2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
df_pm_categories.to_csv(backup_file, index=False)
print(f"✓ Backup saved: {backup_file}")

# Apply categorizations
updates_made = 0
unknown_count = 0

for idx, new_category in pm_categories_results.items():
    if new_category is not None:
        df_pm_categories.at[idx, 'election_type'] = new_category
        updates_made += 1
    else:
        # Mark as unknown or keep as-is
        unknown_count += 1

# Save updated file
df_pm_categories.to_csv(PM_CATEGORIES_FILE, index=False)
print(f"✓ Updated {PM_CATEGORIES_FILE}")
print(f"  - {updates_made:,} markets recategorized")
print(f"  - {unknown_count:,} markets left unchanged (unable to categorize)")


# ============================================================================
# Update polymarket_with_election_keys.csv and kalshi_with_election_keys.json
# ============================================================================

print(f"\n{'='*100}")
print("SYNCING CATEGORIES TO OTHER FILES")
print(f"{'='*100}\n")

# Build mapping from market_id/token_id to new category
pm_market_id_to_category = {}
pm_token_id_to_category = {}

for idx, row in df_pm_categories.iterrows():
    if not pd.isna(row.get('election_type')):
        if not pd.isna(row.get('market_id')):
            pm_market_id_to_category[str(row['market_id'])] = row['election_type']
        if not pd.isna(row.get('token_id')):
            pm_token_id_to_category[str(row['token_id'])] = row['election_type']

# Update polymarket_with_election_keys.csv
backup_file = f"{DATA_DIR}/polymarket_with_election_keys_BACKUP_PHASE2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
df_pm_keys.to_csv(backup_file, index=False)
print(f"✓ Backup saved: {backup_file}")

pm_keys_updates = 0
for idx, row in df_pm_keys.iterrows():
    # Try to find matching category from market_categories
    market_id = str(row.get('market_id', ''))
    token_id = str(row.get('token_id', ''))

    new_category = pm_market_id_to_category.get(market_id) or pm_token_id_to_category.get(token_id)

    if new_category and row['election_type'] in ['Electoral', 'Special Election', 'Midterm', None]:
        df_pm_keys.at[idx, 'election_type'] = new_category
        pm_keys_updates += 1

df_pm_keys.to_csv(PM_ELECTION_KEYS_FILE, index=False)
print(f"✓ Updated {PM_ELECTION_KEYS_FILE}")
print(f"  - {pm_keys_updates:,} markets updated")

# Update kalshi_with_election_keys.json
# Note: Kalshi doesn't have token_id/market_id overlap with Polymarket
# so we need to recategorize based on question text
print(f"\nProcessing Kalshi markets...")

if len(kalshi_to_fix) > 0:
    # Find indices of markets that need fixing
    kalshi_batch = []
    for idx, m in enumerate(kalshi_data):
        if m in kalshi_to_fix:
            kalshi_batch.append({
                'id': idx,
                'question': m.get('title', '') or m.get('ticker', '')
            })

    kalshi_results = process_markets(kalshi_batch, "kalshi_with_election_keys.json")

    # Apply results
    backup_file = f"{DATA_DIR}/kalshi_with_election_keys_BACKUP_PHASE2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(KALSHI_ELECTION_KEYS_FILE, 'r') as f:
        original_kalshi = json.load(f)
    with open(backup_file, 'w') as f:
        json.dump(original_kalshi, f, indent=2)
    print(f"✓ Backup saved: {backup_file}")

    kalshi_updates = 0
    for market_idx, new_category in kalshi_results.items():
        if new_category is not None:
            kalshi_data[market_idx]['election_type'] = new_category
            kalshi_updates += 1

    with open(KALSHI_ELECTION_KEYS_FILE, 'w') as f:
        json.dump(kalshi_data, f, indent=2)

    print(f"✓ Updated {KALSHI_ELECTION_KEYS_FILE}")
    print(f"  - {kalshi_updates:,} markets updated")
else:
    print(f"✓ No Kalshi markets needed recategorization")


# ============================================================================
# Summary
# ============================================================================

print(f"\n{'='*100}")
print("✓ PHASE 2 RECATEGORIZATION COMPLETE")
print("="*100)
print(f"\nTotal markets processed: {total_markets:,}")
print(f"Successfully categorized: {updates_made:,}")
print(f"Unable to categorize: {unknown_count:,}")
print("\nAll files updated with backups saved.")
print("\nNext steps:")
print("  - Verify categorization results")
print("  - Check for any remaining NULL/unknown categories")
print("  - Verify consistency across all three files")
