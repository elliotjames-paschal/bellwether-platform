#!/usr/bin/env python3
"""
Fix Republican Primary and Democratic Primary Categories

These are catch-all categories that need to be properly categorized into:
- Presidential Primary
- House Primary
- Senate Primary
- Gubernatorial Primary
- Mayoral Primary
- Or other appropriate categories

Uses ChatGPT API to recategorize markets based on their titles/descriptions.
"""

import pandas as pd
import json
import os
import time
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"

FILES_TO_CHECK = {
    'polymarket_keys': f"{DATA_DIR}/polymarket_with_election_keys.csv",
    'polymarket_outcomes': f"{DATA_DIR}/market_categories_with_outcomes.csv",
    'kalshi_keys': f"{DATA_DIR}/kalshi_with_election_keys.json",
    'kalshi_all': f"{DATA_DIR}/kalshi_all_political_with_categories.json"
}

# OpenAI setup - load API key from file
with open(f"{BASE_DIR}/openai_api_key.txt", 'r') as f:
    api_key = f.read().strip()
client = OpenAI(api_key=api_key)

# Allowed categories (same as before, excluding Republican/Democratic Primary)
ALLOWED_CATEGORIES = {
    "Presidential": "US presidential general election",
    "Presidential Primary": "US presidential primary (any party)",
    "House": "US House of Representatives general election",
    "House Primary": "US House of Representatives primary (any party)",
    "Senate": "US Senate general election",
    "Senate Primary": "US Senate primary (any party)",
    "Gubernatorial": "US state governor general election",
    "Gubernatorial Primary": "US state governor primary (any party)",
    "Mayoral": "City mayor election (general or primary)",
    "Mayoral Primary": "City mayor primary (any party)",
    "Chancellor": "German Chancellor election",
    "Prime Minister": "Prime Minister election (UK, Canada, Australia, etc.)",
    "General Election": "National general election (non-US)",
    "National Election": "National election where specific type unclear",
    "Parliamentary": "Parliamentary elections (seats, coalitions, etc.)",
    "Regional Election": "State/provincial/regional elections",
    "Provincial": "Canadian provincial elections",
    "European Parliament": "European Parliament elections",
    "VP Nomination": "US Vice President nomination markets",
    "Multiple Elections": "Combination or sweep markets involving multiple elections",
}

print("="*80)
print("FIX REPUBLICAN PRIMARY AND DEMOCRATIC PRIMARY CATEGORIES")
print("="*80)

def categorize_markets_batch(markets, batch_name):
    """Use ChatGPT to categorize a batch of markets."""

    # Prepare market info for ChatGPT
    market_list = []
    for i, market in enumerate(markets, 1):
        market_list.append(f"{i}. {market['title']}")

    # Create prompt
    categories_text = "\n".join([f"- {cat}: {desc}" for cat, desc in ALLOWED_CATEGORIES.items()])
    markets_text = "\n".join(market_list)

    prompt = f"""You are categorizing prediction markets into election types. These markets were previously labeled as "Republican Primary" or "Democratic Primary" but need more specific categorization.

ALLOWED CATEGORIES:
{categories_text}

MARKETS TO CATEGORIZE:
{markets_text}

For each market, determine the MOST SPECIFIC election type category that applies. Consider:
1. What OFFICE is being contested? (Presidential, House, Senate, Governor, Mayor, etc.)
2. Is it a PRIMARY or GENERAL election?
3. For primaries, use the office-specific primary category (e.g., "Presidential Primary" not "Republican Primary")

Return ONLY a JSON array with {len(markets)} objects in this exact format:
[
  {{"market_number": 1, "category": "Presidential Primary", "reasoning": "brief explanation"}},
  {{"market_number": 2, "category": "House Primary", "reasoning": "brief explanation"}}
]

IMPORTANT:
- Use ONLY categories from the allowed list above
- Be as specific as possible (e.g., "House Primary" not just "Primary")
- Return valid JSON only, no other text"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that categorizes prediction markets. You always respond with valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )

        response_text = response.choices[0].message.content.strip()

        # Strip markdown code fences if present
        if response_text.startswith('```'):
            # Remove ```json or ``` at start
            response_text = response_text.split('\n', 1)[1] if '\n' in response_text else response_text[3:]
            # Remove ``` at end
            if response_text.endswith('```'):
                response_text = response_text.rsplit('```', 1)[0]
            response_text = response_text.strip()

        # Parse JSON response
        categorizations = json.loads(response_text)

        # Validate and apply
        results = []
        for cat in categorizations:
            market_num = cat['market_number'] - 1  # Convert to 0-indexed
            if market_num < len(markets):
                category = cat['category']
                if category in ALLOWED_CATEGORIES:
                    results.append({
                        'id': markets[market_num]['id'],
                        'title': markets[market_num]['title'],
                        'old_category': markets[market_num]['old_category'],
                        'new_category': category,
                        'reasoning': cat.get('reasoning', '')
                    })
                else:
                    print(f"  ⚠️  Invalid category '{category}' for market {market_num + 1}")
                    results.append({
                        'id': markets[market_num]['id'],
                        'title': markets[market_num]['title'],
                        'old_category': markets[market_num]['old_category'],
                        'new_category': markets[market_num]['old_category'],  # Keep old if invalid
                        'reasoning': 'Invalid category returned'
                    })

        return results

    except Exception as e:
        print(f"  ❌ Error: {e}")
        # Return unchanged
        return [{
            'id': m['id'],
            'title': m['title'],
            'old_category': m['old_category'],
            'new_category': m['old_category'],
            'reasoning': f'Error: {str(e)}'
        } for m in markets]

def process_file(file_key, file_path):
    """Process a single file to find and recategorize party primary markets."""

    print(f"\n{'='*80}")
    print(f"Processing: {file_key}")
    print(f"{'='*80}")

    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}")
        return None

    # Load file
    if file_path.endswith('.json'):
        with open(file_path, 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        is_json = True
    else:
        df = pd.read_csv(file_path)
        is_json = False

    print(f"✓ Loaded {len(df):,} total records")

    # Find party primary markets
    if 'election_type' not in df.columns:
        print("  ⚠️  No 'election_type' column found")
        return None

    party_primaries = df[df['election_type'].isin(['Republican Primary', 'Democratic Primary'])].copy()

    if len(party_primaries) == 0:
        print("  ✓ No party primary markets found in this file")
        return None

    print(f"  Found {len(party_primaries):,} party primary markets to recategorize:")
    print(f"    - Republican Primary: {(party_primaries['election_type'] == 'Republican Primary').sum()}")
    print(f"    - Democratic Primary: {(party_primaries['election_type'] == 'Democratic Primary').sum()}")

    # Prepare markets for categorization
    title_col = 'question' if 'question' in party_primaries.columns else 'title'
    id_col = 'market_id' if 'market_id' in party_primaries.columns else 'ticker'

    markets_to_categorize = []
    for _, row in party_primaries.iterrows():
        markets_to_categorize.append({
            'id': row[id_col],
            'title': row[title_col],
            'old_category': row['election_type']
        })

    # Batch process (10 at a time)
    all_results = []
    batch_size = 10

    for i in range(0, len(markets_to_categorize), batch_size):
        batch = markets_to_categorize[i:i+batch_size]
        print(f"\n  Processing batch {i//batch_size + 1}/{(len(markets_to_categorize)-1)//batch_size + 1} ({len(batch)} markets)...")

        results = categorize_markets_batch(batch, f"{file_key}_batch_{i//batch_size + 1}")
        all_results.extend(results)

        # Rate limiting
        if i + batch_size < len(markets_to_categorize):
            time.sleep(1)

    # Show results summary
    print(f"\n  {'='*76}")
    print(f"  RECATEGORIZATION RESULTS")
    print(f"  {'='*76}")

    category_changes = {}
    for result in all_results:
        old = result['old_category']
        new = result['new_category']
        key = f"{old} → {new}"
        category_changes[key] = category_changes.get(key, 0) + 1

    for change, count in sorted(category_changes.items(), key=lambda x: -x[1]):
        print(f"    {change}: {count} markets")

    # Apply changes to dataframe
    for result in all_results:
        mask = df[id_col] == result['id']
        df.loc[mask, 'election_type'] = result['new_category']

    # Save updated file
    backup_path = file_path.replace('.csv', '_backup.csv').replace('.json', '_backup.json')

    if is_json:
        # Backup original
        with open(backup_path, 'w') as f:
            json.dump(data, f, indent=2)
        # Save updated
        with open(file_path, 'w') as f:
            json.dump(df.to_dict('records'), f, indent=2)
    else:
        # Backup original
        pd.read_csv(file_path).to_csv(backup_path, index=False)
        # Save updated
        df.to_csv(file_path, index=False)

    print(f"\n  ✓ Updated file saved: {file_path}")
    print(f"  ✓ Backup saved: {backup_path}")

    return all_results

# Process all files
all_file_results = {}

for file_key, file_path in FILES_TO_CHECK.items():
    results = process_file(file_key, file_path)
    if results:
        all_file_results[file_key] = results

# Final summary
print(f"\n{'='*80}")
print("FINAL SUMMARY")
print(f"{'='*80}")

total_recategorized = sum(len(results) for results in all_file_results.values())
print(f"\nTotal markets recategorized: {total_recategorized}")

for file_key, results in all_file_results.items():
    print(f"\n{file_key}: {len(results)} markets")

print(f"\n{'='*80}")
print("✓ COMPLETE")
print(f"{'='*80}")
print("\nBackup files created for all modified files.")
print("Original files have been updated with new categories.")
