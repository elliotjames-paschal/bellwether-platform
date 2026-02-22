#!/usr/bin/env python3
"""
Test the ChatGPT categorization on 10 sample markets
"""

import pandas as pd
import json
import os
from openai import OpenAI

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
PM_CATEGORIES_FILE = f"{DATA_DIR}/market_categories_with_outcomes.csv"

# Load API key
api_key_file = f"{BASE_DIR}/openai_api_key.txt"
with open(api_key_file, 'r') as f:
    api_key = f.read().strip()

client = OpenAI(api_key=api_key)

# Allowed categories
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
print("TESTING CHATGPT CATEGORIZATION ON 10 SAMPLE MARKETS")
print("="*100)

# Load data
df = pd.read_csv(PM_CATEGORIES_FILE)
electoral_markets = df[df['political_category'] == '1. ELECTORAL'].copy()

# Get 10 markets that need categorization
to_categorize = electoral_markets[
    electoral_markets['election_type'].isin(['Electoral', 'Special Election', 'Midterm']) |
    electoral_markets['election_type'].isna()
].head(10)

print(f"\nSelected 10 markets to test:\n")
for idx, row in to_categorize.iterrows():
    print(f"{idx}. {row['question'][:80]}")
    print(f"   Current: {row['election_type']}")

# Extract questions
questions = to_categorize['question'].tolist()

# Build prompt
category_list = "\n".join([f"- {cat}: {desc}" for cat, desc in ALLOWED_CATEGORIES.items()])
questions_json = json.dumps(questions)

prompt = f"""You are categorizing prediction market questions about elections.

ALLOWED CATEGORIES:
{category_list}

MARKET QUESTIONS (as JSON array):
{questions_json}

TASK: For each question in the array, determine which election type category it BEST fits into. You MUST choose one of the allowed categories - you cannot skip a question or use any other category name.

RESPONSE FORMAT: Return ONLY a JSON object with a "categories" key containing an array of category names, in the same order as the input questions. Do not include any other text or explanation.

Example response format: {{"categories": ["Presidential", "Senate", "House", ...]}}

Your response (JSON object only):"""

print(f"\n{'='*100}")
print("SENDING TO CHATGPT...")
print(f"{'='*100}\n")

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You are a political election categorization expert. You must respond with ONLY a valid JSON object containing a 'categories' array. No explanations, no markdown formatting, just the raw JSON object. Every question must be assigned to exactly one of the allowed categories."},
        {"role": "user", "content": prompt}
    ],
    temperature=0.0,
    response_format={"type": "json_object"}
)

# Parse response
response_text = response.choices[0].message.content.strip()
print("Raw response:")
print(response_text)
print()

# Try to parse
response_json = json.loads(response_text)
if isinstance(response_json, dict):
    categories = response_json.get('categories') or response_json.get('results') or list(response_json.values())[0]
else:
    categories = response_json

print(f"\n{'='*100}")
print("RESULTS")
print(f"{'='*100}\n")

print(f"Response length: {len(categories)} (expected {len(questions)})")
print()

success_count = 0
for i, (idx, row) in enumerate(to_categorize.iterrows()):
    category = categories[i] if i < len(categories) else "MISSING"
    is_valid = category in ALLOWED_CATEGORIES

    if is_valid:
        success_count += 1
        status = "✓"
    else:
        status = "✗"

    print(f"{status} Question: {row['question'][:70]}")
    print(f"   Old: {row['election_type']}")
    print(f"   New: {category}")
    print()

print(f"{'='*100}")
print(f"SUCCESS RATE: {success_count}/{len(questions)} ({success_count/len(questions)*100:.1f}%)")
print(f"{'='*100}")
