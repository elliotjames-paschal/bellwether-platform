#!/usr/bin/env python3
"""
Recategorize Kalshi electoral markets with election_type='NA' using ChatGPT API.

Processes markets in kalshi_all_political_with_categories.json that have:
- political_category = '1. ELECTORAL'
- election_type = 'NA' (or null)

Uses OpenAI ChatGPT to categorize into standardized election types.
Processes in batches of 10 to improve efficiency.
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
LOG_FILE = f"{BASE_DIR}/kalshi_electoral_na_recategorization.log"
KALSHI_FILE = f"{DATA_DIR}/kalshi_all_political_with_categories.json"

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
    "Multiple Elections": "Combination or sweep markets involving multiple elections",
}

print("="*100)
print("KALSHI ELECTORAL NA RECATEGORIZATION - CHATGPT API")
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

For questions involving MULTIPLE elections or combinations (e.g., "Will X win AND Y win"), use "Multiple Elections".

CRITICAL: You MUST return EXACTLY {len(questions_batch)} categories in your response array, one for each question in the input. No more, no less. The number of categories must match the number of questions exactly.

RESPONSE FORMAT: Return ONLY a JSON object with a "categories" key containing an array of category names, in the same order as the input questions. Do not include any other text or explanation.

Example response format: {{"categories": ["Presidential", "Senate", "Multiple Elections", ...]}}

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
# Load Kalshi electoral markets with election_type='NA'
# ============================================================================

print(f"\n{'='*100}")
print("LOADING KALSHI ELECTORAL MARKETS WITH NA ELECTION TYPE")
print(f"{'='*100}\n")

with open(KALSHI_FILE, 'r') as f:
    kalshi_data = json.load(f)

print(f"Total markets in file: {len(kalshi_data):,}")

# Find electoral markets with NA election type
to_categorize = []
for idx, market in enumerate(kalshi_data):
    is_electoral = market.get('political_category') == '1. ELECTORAL'
    has_na_type = (market.get('election_type') == 'NA' or
                   market.get('election_type') is None or
                   market.get('election_type') == '')

    if is_electoral and has_na_type:
        to_categorize.append({
            'index': idx,
            'ticker': market.get('ticker', ''),
            'title': market.get('title', '')
        })

print(f"Electoral markets with NA election type: {len(to_categorize):,}")

# Show sample
print(f"\nSample markets to be categorized:")
for i, market in enumerate(to_categorize[:5]):
    print(f"  {i+1}. {market['title'][:90]}")

# ============================================================================
# Ask user to confirm before spending API credits
# ============================================================================

print(f"\n{'='*100}")
print("API USAGE ESTIMATE")
print(f"{'='*100}\n")

total_markets = len(to_categorize)
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

# Prepare batch
markets_batch = [
    {'id': m['index'], 'question': m['title']}
    for m in to_categorize
]

# Process
results = process_markets(markets_batch, "kalshi_all_political_with_categories.json - Electoral NA")

# ============================================================================
# Apply results
# ============================================================================

print(f"\n{'='*100}")
print("UPDATING FILE")
print(f"{'='*100}\n")

# Create backup
backup_file = f"{DATA_DIR}/kalshi_all_political_with_categories_BACKUP_ELECTORAL_NA_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(KALSHI_FILE, 'r') as f:
    backup_data = json.load(f)
with open(backup_file, 'w') as f:
    json.dump(backup_data, f, indent=2)
print(f"✓ Backup saved: {backup_file}")

# Apply categorizations
updates_made = 0
unknown_count = 0

for market_idx, new_category in results.items():
    if new_category is not None:
        kalshi_data[market_idx]['election_type'] = new_category
        updates_made += 1
    else:
        # Keep as NA
        unknown_count += 1

# Save updated file
with open(KALSHI_FILE, 'w') as f:
    json.dump(kalshi_data, f, indent=2)

print(f"✓ Updated {KALSHI_FILE}")
print(f"  - {updates_made:,} markets recategorized")
print(f"  - {unknown_count:,} markets left as NA (unable to categorize)")

# Show category distribution
print(f"\n{'='*100}")
print("NEW CATEGORY DISTRIBUTION")
print(f"{'='*100}\n")

category_counts = {}
for market_idx in results.keys():
    cat = kalshi_data[market_idx].get('election_type', 'NA')
    category_counts[cat] = category_counts.get(cat, 0) + 1

for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
    print(f"  {cat}: {count:,}")

# ============================================================================
# Summary
# ============================================================================

print(f"\n{'='*100}")
print("✓ KALSHI ELECTORAL NA RECATEGORIZATION COMPLETE")
print("="*100)
print(f"\nTotal markets processed: {total_markets:,}")
print(f"Successfully categorized: {updates_made:,}")
print(f"Unable to categorize: {unknown_count:,}")
print("\nFile updated with backup saved.")
print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
