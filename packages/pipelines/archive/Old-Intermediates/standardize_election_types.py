#!/usr/bin/env python3
"""
Standardize election_type categories across all data files.

Phase 1: Manual standardization (merges and renames)
- Merges duplicates (Senate + US Senate, House + Congressional)
- Standardizes primary naming (Primary - X → X Primary)
- Marks Electoral, NULL, Special Election, and Midterm for ChatGPT recategorization

Applies to:
- market_categories_with_outcomes.csv
- polymarket_with_election_keys.csv
- kalshi_with_election_keys.json
"""

import pandas as pd
import json
import os
from datetime import datetime

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"

# Files to process
PM_CATEGORIES_FILE = f"{DATA_DIR}/market_categories_with_outcomes.csv"
PM_ELECTION_KEYS_FILE = f"{DATA_DIR}/polymarket_with_election_keys.csv"
KALSHI_ELECTION_KEYS_FILE = f"{DATA_DIR}/kalshi_with_election_keys.json"

print("="*100)
print("ELECTION TYPE STANDARDIZATION - PHASE 1: MANUAL RULES")
print("="*100)

def standardize_election_type(election_type):
    """
    Apply manual standardization rules to election_type.

    Returns the standardized election_type.
    Leaves "Electoral", NULL, "Special Election", and "Midterm" unchanged for ChatGPT processing.
    """
    # Handle NULL/blank - leave for ChatGPT
    if pd.isna(election_type) or election_type == '':
        return election_type

    # Leave these for ChatGPT recategorization
    if election_type in ['Electoral', 'Special Election', 'Midterm']:
        return election_type

    # Standardization rules
    rules = {
        # Merge duplicates
        'US Senate': 'Senate',
        'Congressional': 'House',

        # Standardize primary naming: "Primary - X" → "X Primary"
        'Primary - Presidential': 'Presidential Primary',
        'Primary - Senate': 'Senate Primary',
        'Primary - House': 'House Primary',
        'Primary - Congressional': 'House Primary',
        'Primary - Gubernatorial': 'Gubernatorial Primary',
        'Primary - Mayoral': 'Mayoral Primary',
        'Primary - Democratic': 'Democratic Primary',
        'Primary - Republican': 'Republican Primary',

        # Keep as-is (explicitly listed for clarity)
        'Presidential': 'Presidential',
        'Senate': 'Senate',
        'House': 'House',
        'Gubernatorial': 'Gubernatorial',
        'Mayoral': 'Mayoral',
        'VP Nomination': 'VP Nomination',
        'Parliamentary': 'Parliamentary',
        'Prime Minister': 'Prime Minister',
        'European Parliament': 'European Parliament',
        'Regional Election': 'Regional Election',
        'Chancellor': 'Chancellor',
        'National Election': 'National Election',
        'Provincial': 'Provincial',
    }

    return rules.get(election_type, election_type)


# ============================================================================
# Process market_categories_with_outcomes.csv
# ============================================================================

print(f"\n{'='*100}")
print("PROCESSING: market_categories_with_outcomes.csv")
print(f"{'='*100}\n")

df_pm_categories = pd.read_csv(PM_CATEGORIES_FILE)
print(f"Loaded {len(df_pm_categories):,} total markets")

# Filter to electoral markets only
electoral_markets = df_pm_categories[df_pm_categories['political_category'] == '1. ELECTORAL'].copy()
print(f"Found {len(electoral_markets):,} electoral markets")

# Show before counts
before_counts = electoral_markets['election_type'].value_counts(dropna=False)
print(f"\nBEFORE standardization - unique categories: {len(before_counts)}")
print("\nTop 15 categories:")
for election_type, count in before_counts.head(15).items():
    if pd.isna(election_type):
        print(f"  (blank/null): {count:,}")
    else:
        print(f"  {election_type}: {count:,}")

# Apply standardization
df_pm_categories['election_type_original'] = df_pm_categories['election_type'].copy()
df_pm_categories['election_type'] = df_pm_categories['election_type'].apply(standardize_election_type)

# Show after counts
electoral_markets_after = df_pm_categories[df_pm_categories['political_category'] == '1. ELECTORAL'].copy()
after_counts = electoral_markets_after['election_type'].value_counts(dropna=False)
print(f"\nAFTER standardization - unique categories: {len(after_counts)}")
print("\nTop 15 categories:")
for election_type, count in after_counts.head(15).items():
    if pd.isna(election_type):
        print(f"  (blank/null): {count:,}")
    else:
        print(f"  {election_type}: {count:,}")

# Show what changed
print("\nCHANGES MADE:")
changes = df_pm_categories[
    (df_pm_categories['election_type_original'] != df_pm_categories['election_type']) &
    (df_pm_categories['political_category'] == '1. ELECTORAL')
]
if len(changes) > 0:
    change_summary = changes.groupby(['election_type_original', 'election_type']).size().reset_index(name='count')
    for _, row in change_summary.iterrows():
        print(f"  '{row['election_type_original']}' → '{row['election_type']}': {row['count']:,} markets")
else:
    print("  (no changes needed)")

# Show what's marked for ChatGPT
print("\nMARKED FOR CHATGPT RECATEGORIZATION:")
chatgpt_markets = electoral_markets_after[
    electoral_markets_after['election_type'].isin(['Electoral', 'Special Election', 'Midterm']) |
    electoral_markets_after['election_type'].isna()
]
chatgpt_counts = chatgpt_markets['election_type'].value_counts(dropna=False)
total_chatgpt = 0
for election_type, count in chatgpt_counts.items():
    if pd.isna(election_type):
        print(f"  (blank/null): {count:,} markets")
    else:
        print(f"  {election_type}: {count:,} markets")
    total_chatgpt += count
print(f"  TOTAL: {total_chatgpt:,} markets")

# Save backup
backup_file = f"{DATA_DIR}/market_categories_with_outcomes_BACKUP_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
df_pm_categories_original = pd.read_csv(PM_CATEGORIES_FILE)
df_pm_categories_original.to_csv(backup_file, index=False)
print(f"\n✓ Backup saved: {backup_file}")

# Save updated file (drop the original column)
df_pm_categories = df_pm_categories.drop(columns=['election_type_original'])
df_pm_categories.to_csv(PM_CATEGORIES_FILE, index=False)
print(f"✓ Updated file saved: {PM_CATEGORIES_FILE}")


# ============================================================================
# Process polymarket_with_election_keys.csv
# ============================================================================

print(f"\n{'='*100}")
print("PROCESSING: polymarket_with_election_keys.csv")
print(f"{'='*100}\n")

df_pm_keys = pd.read_csv(PM_ELECTION_KEYS_FILE)
print(f"Loaded {len(df_pm_keys):,} markets")

# Show before counts
before_counts = df_pm_keys['election_type'].value_counts(dropna=False)
print(f"\nBEFORE standardization - unique categories: {len(before_counts)}")

# Apply standardization
df_pm_keys['election_type_original'] = df_pm_keys['election_type'].copy()
df_pm_keys['election_type'] = df_pm_keys['election_type'].apply(standardize_election_type)

# Show after counts
after_counts = df_pm_keys['election_type'].value_counts(dropna=False)
print(f"AFTER standardization - unique categories: {len(after_counts)}")

# Show what changed
changes = df_pm_keys[df_pm_keys['election_type_original'] != df_pm_keys['election_type']]
if len(changes) > 0:
    change_summary = changes.groupby(['election_type_original', 'election_type']).size().reset_index(name='count')
    print("\nCHANGES MADE:")
    for _, row in change_summary.iterrows():
        print(f"  '{row['election_type_original']}' → '{row['election_type']}': {row['count']:,} markets")
else:
    print("\n(no changes needed)")

# Show what's marked for ChatGPT
chatgpt_markets = df_pm_keys[
    df_pm_keys['election_type'].isin(['Electoral', 'Special Election', 'Midterm']) |
    df_pm_keys['election_type'].isna()
]
if len(chatgpt_markets) > 0:
    print("\nMARKED FOR CHATGPT RECATEGORIZATION:")
    chatgpt_counts = chatgpt_markets['election_type'].value_counts(dropna=False)
    total_chatgpt = 0
    for election_type, count in chatgpt_counts.items():
        if pd.isna(election_type):
            print(f"  (blank/null): {count:,} markets")
        else:
            print(f"  {election_type}: {count:,} markets")
        total_chatgpt += count
    print(f"  TOTAL: {total_chatgpt:,} markets")

# Save backup
backup_file = f"{DATA_DIR}/polymarket_with_election_keys_BACKUP_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
df_pm_keys_original = pd.read_csv(PM_ELECTION_KEYS_FILE)
df_pm_keys_original.to_csv(backup_file, index=False)
print(f"\n✓ Backup saved: {backup_file}")

# Save updated file
df_pm_keys = df_pm_keys.drop(columns=['election_type_original'])
df_pm_keys.to_csv(PM_ELECTION_KEYS_FILE, index=False)
print(f"✓ Updated file saved: {PM_ELECTION_KEYS_FILE}")


# ============================================================================
# Process kalshi_with_election_keys.json
# ============================================================================

print(f"\n{'='*100}")
print("PROCESSING: kalshi_with_election_keys.json")
print(f"{'='*100}\n")

with open(KALSHI_ELECTION_KEYS_FILE, 'r') as f:
    kalshi_data = json.load(f)

print(f"Loaded {len(kalshi_data):,} markets")

# Count election types before
before_counts = {}
for market in kalshi_data:
    election_type = market.get('election_type')
    if pd.isna(election_type) or election_type is None:
        election_type = None
    before_counts[election_type] = before_counts.get(election_type, 0) + 1

print(f"\nBEFORE standardization - unique categories: {len(before_counts)}")

# Apply standardization
changes_count = {}
for market in kalshi_data:
    original = market.get('election_type')
    if original is not None:
        standardized = standardize_election_type(original)
        market['election_type'] = standardized

        if original != standardized:
            key = f"{original} → {standardized}"
            changes_count[key] = changes_count.get(key, 0) + 1

# Count election types after
after_counts = {}
chatgpt_count = {}
for market in kalshi_data:
    election_type = market.get('election_type')
    if pd.isna(election_type) or election_type is None:
        election_type = None
    after_counts[election_type] = after_counts.get(election_type, 0) + 1

    # Track ChatGPT candidates
    if election_type in ['Electoral', 'Special Election', 'Midterm', None]:
        chatgpt_count[election_type] = chatgpt_count.get(election_type, 0) + 1

print(f"AFTER standardization - unique categories: {len(after_counts)}")

if changes_count:
    print("\nCHANGES MADE:")
    for change, count in sorted(changes_count.items()):
        print(f"  {change}: {count:,} markets")
else:
    print("\n(no changes needed)")

if chatgpt_count:
    print("\nMARKED FOR CHATGPT RECATEGORIZATION:")
    total_chatgpt = 0
    for election_type, count in sorted(chatgpt_count.items(), key=lambda x: (x[0] is None, x[0])):
        if election_type is None:
            print(f"  (blank/null): {count:,} markets")
        else:
            print(f"  {election_type}: {count:,} markets")
        total_chatgpt += count
    print(f"  TOTAL: {total_chatgpt:,} markets")

# Save backup
backup_file = f"{DATA_DIR}/kalshi_with_election_keys_BACKUP_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(KALSHI_ELECTION_KEYS_FILE, 'r') as f:
    original_data = json.load(f)
with open(backup_file, 'w') as f:
    json.dump(original_data, f, indent=2)
print(f"\n✓ Backup saved: {backup_file}")

# Save updated file
with open(KALSHI_ELECTION_KEYS_FILE, 'w') as f:
    json.dump(kalshi_data, f, indent=2)
print(f"✓ Updated file saved: {KALSHI_ELECTION_KEYS_FILE}")


# ============================================================================
# Summary
# ============================================================================

print(f"\n{'='*100}")
print("✓ PHASE 1 STANDARDIZATION COMPLETE")
print("="*100)
print("\nStandardized all three files:")
print(f"  1. {PM_CATEGORIES_FILE}")
print(f"  2. {PM_ELECTION_KEYS_FILE}")
print(f"  3. {KALSHI_ELECTION_KEYS_FILE}")
print("\nBackups created for all files.")
print("\nMarked for ChatGPT recategorization:")
print("  - Electoral")
print("  - NULL/blank")
print("  - Special Election")
print("  - Midterm")
print("\nNext steps:")
print("  - Phase 2: Create ChatGPT recategorization script for marked categories")
print("  - Verify consistency across overlapping markets")
