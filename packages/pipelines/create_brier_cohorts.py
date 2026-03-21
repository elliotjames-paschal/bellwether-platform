#!/usr/bin/env python3
"""
Create Brier Score Cohort Files

Aggregates individual prediction accuracy data into cohort summary statistics.
Creates two files:
1. combined_brier_overall_cohorts.csv - Overall Brier scores by cohort
2. combined_brier_categories_cohorts.csv - Brier scores by category and cohort
"""

import pandas as pd
import numpy as np
from pathlib import Path
from config import DATA_DIR, get_latest_file
from paper_config import load_prediction_accuracy, PAPER_DATA_DIR

# Output files
OVERALL_OUTPUT = PAPER_DATA_DIR / "combined_brier_overall_cohorts.csv"
CATEGORIES_OUTPUT = PAPER_DATA_DIR / "combined_brier_categories_cohorts.csv"

# Platform-specific category outputs
PM_CATEGORIES_OUTPUT = PAPER_DATA_DIR / "polymarket_brier_categories_cohorts.csv"
KALSHI_CATEGORIES_OUTPUT = PAPER_DATA_DIR / "kalshi_brier_categories_cohorts.csv"

# Time horizons (all available points)
TIME_HORIZONS = [60, 55, 50, 45, 40, 35, 30, 25, 20, 15, 10, 5, 4, 3, 2, 1]
COHORTS = ['5d', '15d', '30d', '60d']

print("=" * 80)
print("CREATING BRIER SCORE COHORT FILES")
print("=" * 80)

# ============================================================================
# Load Data
# ============================================================================

print("\n1. Loading prediction accuracy data...")
df_pm = load_prediction_accuracy("polymarket")
df_kalshi = load_prediction_accuracy("kalshi")

if df_pm is None or df_kalshi is None:
    raise FileNotFoundError("Could not find prediction accuracy files. Run calculate_all_political_brier_scores.py first.")

print(f"   ✓ Polymarket: {len(df_pm):,} prediction records")
print(f"   ✓ Kalshi: {len(df_kalshi):,} prediction records")

# Add platform identifier
df_pm['platform'] = 'Polymarket'
df_kalshi['platform'] = 'Kalshi'

# Standardize market_id column (Kalshi uses 'ticker')
if 'ticker' in df_kalshi.columns and 'market_id' not in df_kalshi.columns:
    df_kalshi['market_id'] = df_kalshi['ticker']

# Combine
df = pd.concat([df_pm, df_kalshi], ignore_index=True)
print(f"   ✓ Combined: {len(df):,} total prediction records")

# ============================================================================
# Define Cohorts
# ============================================================================

print("\n2. Defining cohorts...")

# A cohort is defined by markets that have data at the cohort's time horizon
# For example, 7d cohort = markets with data at 7 days before resolution

cohort_definitions = {
    '5d': 5,
    '15d': 15,
    '30d': 30,
    '60d': 60
}

# Identify which markets belong to which cohorts
# A market belongs to a cohort if it has data at that time horizon
market_cohorts = {}

for cohort_name, required_days in cohort_definitions.items():
    # Get unique markets that have data at this time horizon
    markets_in_cohort = df[df['days_before_event'] == required_days]['market_id'].unique()
    market_cohorts[cohort_name] = set(markets_in_cohort)
    print(f"   ✓ {cohort_name} cohort: {len(markets_in_cohort):,} markets")

# ============================================================================
# Calculate Overall Cohort Statistics
# ============================================================================

print("\n3. Calculating overall cohort statistics...")

overall_results = []

for cohort_name, cohort_markets in market_cohorts.items():
    # Filter to only markets in this cohort
    cohort_df = df[df['market_id'].isin(cohort_markets)].copy()

    # Calculate mean Brier score at each time horizon
    row = {'Cohort': cohort_name, 'N': len(cohort_markets)}

    # Get the cohort's defining horizon
    cohort_horizon = cohort_definitions[cohort_name]

    for horizon in TIME_HORIZONS:
        # Only include horizons <= cohort horizon
        # E.g., 7d cohort should only have data at 7d, 3d, 1d (not 60d, 30d, 14d)
        if horizon <= cohort_horizon:
            horizon_data = cohort_df[cohort_df['days_before_event'] == horizon]
            if len(horizon_data) > 0:
                row[f'{horizon}d'] = horizon_data['brier_score'].mean()
            else:
                row[f'{horizon}d'] = np.nan
        else:
            row[f'{horizon}d'] = np.nan

    overall_results.append(row)
    print(f"   ✓ {cohort_name}: n={len(cohort_markets):,}")

df_overall = pd.DataFrame(overall_results)

# ============================================================================
# Calculate Category Cohort Statistics
# ============================================================================

print("\n4. Calculating category cohort statistics...")

category_results = []

# Get unique categories
categories = df['category'].unique()
categories = [c for c in categories if pd.notna(c)]

print(f"   Found {len(categories)} categories")

for cohort_name, cohort_markets in market_cohorts.items():
    # Filter to only markets in this cohort
    cohort_df = df[df['market_id'].isin(cohort_markets)].copy()

    # Get the cohort's defining horizon
    cohort_horizon = cohort_definitions[cohort_name]

    for category in sorted(categories):
        # Filter to this category
        category_df = cohort_df[cohort_df['category'] == category].copy()

        if len(category_df) == 0:
            continue

        # Count unique markets in this category
        n_markets = category_df['market_id'].nunique()

        # Calculate mean Brier score at each time horizon
        row = {'Cohort': cohort_name, 'Category': category, 'N': n_markets}

        for horizon in TIME_HORIZONS:
            # Only include horizons <= cohort horizon
            if horizon <= cohort_horizon:
                horizon_data = category_df[category_df['days_before_event'] == horizon]
                if len(horizon_data) > 0:
                    row[f'{horizon}d'] = horizon_data['brier_score'].mean()
                else:
                    row[f'{horizon}d'] = np.nan
            else:
                row[f'{horizon}d'] = np.nan

        category_results.append(row)

df_categories = pd.DataFrame(category_results)
print(f"   ✓ Generated {len(df_categories)} category-cohort combinations")

# ============================================================================
# Calculate Platform-Specific Category Statistics
# ============================================================================

print("\n5. Calculating platform-specific category statistics...")

def calculate_platform_category_stats(platform_df, platform_name, market_cohorts, cohort_definitions, time_horizons):
    """Calculate category statistics for a single platform."""
    platform_results = []

    # Get unique categories for this platform
    categories = platform_df['category'].unique()
    categories = [c for c in categories if pd.notna(c)]

    for cohort_name, cohort_markets in market_cohorts.items():
        # Filter to markets in this cohort AND this platform
        cohort_df = platform_df[platform_df['market_id'].isin(cohort_markets)].copy()

        # Get the cohort's defining horizon
        cohort_horizon = cohort_definitions[cohort_name]

        for category in sorted(categories):
            # Filter to this category
            category_df = cohort_df[cohort_df['category'] == category].copy()

            if len(category_df) == 0:
                continue

            # Count unique markets in this category
            n_markets = category_df['market_id'].nunique()

            # Calculate mean Brier score at each time horizon
            row = {'Cohort': cohort_name, 'Category': category, 'N': n_markets}

            for horizon in time_horizons:
                # Only include horizons <= cohort horizon
                if horizon <= cohort_horizon:
                    horizon_data = category_df[category_df['days_before_event'] == horizon]
                    if len(horizon_data) > 0:
                        row[f'{horizon}d'] = horizon_data['brier_score'].mean()
                    else:
                        row[f'{horizon}d'] = np.nan
                else:
                    row[f'{horizon}d'] = np.nan

            platform_results.append(row)

    return pd.DataFrame(platform_results)

# Calculate for Polymarket
df_pm_categories = calculate_platform_category_stats(
    df_pm, 'Polymarket', market_cohorts, cohort_definitions, TIME_HORIZONS
)
print(f"   ✓ Polymarket: {len(df_pm_categories)} category-cohort combinations")

# Calculate for Kalshi
df_kalshi_categories = calculate_platform_category_stats(
    df_kalshi, 'Kalshi', market_cohorts, cohort_definitions, TIME_HORIZONS
)
print(f"   ✓ Kalshi: {len(df_kalshi_categories)} category-cohort combinations")

# ============================================================================
# Save Results
# ============================================================================

print("\n6. Saving cohort files...")

df_overall.to_csv(OVERALL_OUTPUT, index=False)
print(f"   ✓ Overall cohorts: {OVERALL_OUTPUT}")

df_categories.to_csv(CATEGORIES_OUTPUT, index=False)
print(f"   ✓ Category cohorts: {CATEGORIES_OUTPUT}")

# Save platform-specific category files
df_pm_categories.to_csv(PM_CATEGORIES_OUTPUT, index=False)
print(f"   ✓ Polymarket category cohorts: {PM_CATEGORIES_OUTPUT}")

df_kalshi_categories.to_csv(KALSHI_CATEGORIES_OUTPUT, index=False)
print(f"   ✓ Kalshi category cohorts: {KALSHI_CATEGORIES_OUTPUT}")

# ============================================================================
# Summary
# ============================================================================

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Cohorts created: {', '.join(COHORTS)}")
print(f"Categories: {len(categories)}")
print(f"Time horizons: {', '.join([f'{h}d' for h in TIME_HORIZONS])}")
print("\nOverall Cohort Data:")
print(df_overall.to_string(index=False))
print("=" * 80)
