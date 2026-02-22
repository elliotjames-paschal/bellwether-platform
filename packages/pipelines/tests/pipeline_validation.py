#!/usr/bin/env python3
"""
================================================================================
PIPELINE VALIDATION TEST
================================================================================

Tests the Bellwether pipeline accuracy by:
1. Sampling existing labeled markets (ground truth)
2. Re-running them through the classification pipeline
3. Comparing outputs to original labels
4. Generating accuracy metrics and confusion matrix

Usage:
    python scripts/tests/pipeline_validation.py [--sample-size 50] [--skip-api]

Output:
    - data/tests/validation_sample.csv      - The test sample
    - data/tests/validation_results.csv     - Detailed comparison
    - data/tests/validation_report.md       - Summary report

================================================================================
"""

import sys
import os
import argparse
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from collections import defaultdict

# Add scripts directory to path for imports
SCRIPTS_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from config import DATA_DIR, get_openai_client

# Import pipeline functions
from pipeline_classify_categories import (
    run_classification_pipeline,
    expand_category,
    POLITICAL_CATEGORIES,
)
from pipeline_classify_electoral import run_electoral_pipeline

# =============================================================================
# CONFIGURATION
# =============================================================================

MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
TEST_DIR = DATA_DIR / "tests"
PANEL_A_FILE = DATA_DIR / "election_winner_panel_a_detailed.csv"

# Ensure test directory exists
TEST_DIR.mkdir(parents=True, exist_ok=True)

# Output files
SAMPLE_FILE = TEST_DIR / "validation_sample.csv"
RESULTS_FILE = TEST_DIR / "validation_results.csv"
REPORT_FILE = TEST_DIR / "validation_report.md"


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# =============================================================================
# SAMPLING
# =============================================================================

def create_stratified_sample(df: pd.DataFrame, sample_size: int = 50) -> pd.DataFrame:
    """
    Create stratified sample with equal representation from each category.

    Args:
        df: Master dataframe
        sample_size: Number of markets to sample per category

    Returns:
        Sampled dataframe with ground truth labels preserved
    """
    log(f"Creating stratified sample ({sample_size} per category)...")

    samples = []
    category_counts = {}

    for category in POLITICAL_CATEGORIES:
        if category == "16. NOT_POLITICAL":
            continue  # Skip - these are filtered out

        cat_df = df[df['political_category'] == category].copy()
        n_available = len(cat_df)
        n_sample = min(sample_size, n_available)

        if n_sample > 0:
            sample = cat_df.sample(n=n_sample, random_state=42)
            samples.append(sample)
            category_counts[category] = n_sample
            log(f"  {category}: {n_sample} sampled (of {n_available} available)")
        else:
            log(f"  {category}: 0 sampled (none available)")

    result = pd.concat(samples, ignore_index=True)
    log(f"Total sample size: {len(result)}")

    return result


def get_panel_a_markets(master_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Load Panel A markets directly for metadata testing.

    Panel A contains markets with verified ground truth metadata
    (country, office, location, election_year, is_primary).

    Args:
        master_df: Optional master dataframe to join for scheduled_end_time

    Returns:
        DataFrame of Panel A markets with ground truth metadata
    """
    log("Loading Panel A markets for metadata test...")

    if not PANEL_A_FILE.exists():
        log("  Panel A file not found - skipping metadata test")
        return pd.DataFrame()

    panel_a = pd.read_csv(PANEL_A_FILE)
    log(f"  Loaded {len(panel_a)} Panel A markets")

    # Ensure required columns exist
    required_cols = ['market_id', 'question', 'country', 'office', 'location', 'election_year', 'is_primary']
    missing = [c for c in required_cols if c not in panel_a.columns]
    if missing:
        log(f"  Warning: Panel A missing columns: {missing}")

    # Join with master to get scheduled_end_time (used for year inference)
    if master_df is not None and 'scheduled_end_time' in master_df.columns:
        master_subset = master_df[['market_id', 'scheduled_end_time']].copy()
        master_subset['market_id'] = master_subset['market_id'].astype(str)
        panel_a['market_id'] = panel_a['market_id'].astype(str)
        panel_a = panel_a.merge(master_subset, on='market_id', how='left')
        has_end_time = panel_a['scheduled_end_time'].notna().sum()
        log(f"  Joined scheduled_end_time for {has_end_time}/{len(panel_a)} markets")

    return panel_a


# =============================================================================
# CATEGORY CLASSIFICATION TEST
# =============================================================================

def test_category_classification(sample_df: pd.DataFrame, client) -> pd.DataFrame:
    """
    Test category classification accuracy.

    Args:
        sample_df: Sample with ground truth categories
        client: OpenAI client

    Returns:
        DataFrame with original and predicted categories
    """
    log("\n" + "=" * 60)
    log("TEST 1: CATEGORY CLASSIFICATION")
    log("=" * 60)

    questions = sample_df['question'].tolist()
    log(f"Classifying {len(questions)} markets...")

    # Run classification
    results = run_classification_pipeline(client, questions, show_progress=True)

    # Map results to dataframe
    predictions = []
    for i, result in enumerate(results):
        predictions.append({
            'market_id': sample_df.iloc[i]['market_id'],
            'question': questions[i],
            'original_category': sample_df.iloc[i]['political_category'],
            'predicted_category': expand_category(result['category']),
            'confidence': result.get('confidence', 0),
            'votes': result.get('votes', 0),
        })

    return pd.DataFrame(predictions)


def calculate_category_metrics(results_df: pd.DataFrame) -> dict:
    """
    Calculate accuracy metrics for category classification.
    """
    # Overall accuracy
    correct = (results_df['original_category'] == results_df['predicted_category']).sum()
    total = len(results_df)
    accuracy = correct / total if total > 0 else 0

    # Per-category metrics
    category_metrics = {}
    for category in results_df['original_category'].unique():
        cat_df = results_df[results_df['original_category'] == category]
        cat_correct = (cat_df['original_category'] == cat_df['predicted_category']).sum()
        cat_total = len(cat_df)

        # Precision: of predicted as this category, how many were correct
        predicted_as_cat = results_df[results_df['predicted_category'] == category]
        precision = (predicted_as_cat['original_category'] == category).sum() / len(predicted_as_cat) if len(predicted_as_cat) > 0 else 0

        # Recall: of actual this category, how many were predicted correctly
        recall = cat_correct / cat_total if cat_total > 0 else 0

        # F1
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        category_metrics[category] = {
            'total': cat_total,
            'correct': cat_correct,
            'accuracy': cat_correct / cat_total if cat_total > 0 else 0,
            'precision': precision,
            'recall': recall,
            'f1': f1,
        }

    # Confusion matrix
    categories = sorted(results_df['original_category'].unique())
    confusion = pd.crosstab(
        results_df['original_category'],
        results_df['predicted_category'],
        dropna=False
    )

    return {
        'overall_accuracy': accuracy,
        'correct': correct,
        'total': total,
        'category_metrics': category_metrics,
        'confusion_matrix': confusion,
    }


# =============================================================================
# ELECTORAL METADATA TEST
# =============================================================================

def test_electoral_metadata(electoral_df: pd.DataFrame, client) -> pd.DataFrame:
    """
    Test electoral metadata extraction accuracy.

    Args:
        electoral_df: Electoral markets with ground truth metadata
        client: OpenAI client

    Returns:
        DataFrame with original and predicted metadata
    """
    log("\n" + "=" * 60)
    log("TEST 2: ELECTORAL METADATA EXTRACTION")
    log("=" * 60)

    questions = electoral_df['question'].tolist()
    market_ids = electoral_df['market_id'].tolist()

    # Get scheduled_end_times if available (used to infer election year)
    if 'scheduled_end_time' in electoral_df.columns:
        scheduled_end_times = electoral_df['scheduled_end_time'].tolist()
    else:
        scheduled_end_times = None

    log(f"Extracting metadata for {len(questions)} electoral markets...")

    # Run extraction
    results = run_electoral_pipeline(client, questions, market_ids=market_ids,
                                     scheduled_end_times=scheduled_end_times, show_progress=True)

    # Map results to dataframe
    predictions = []
    metadata_fields = ['country', 'office', 'location', 'election_year', 'is_primary']

    for i, result in enumerate(results):
        row = {
            'market_id': electoral_df.iloc[i]['market_id'],
            'question': questions[i],
        }

        for field in metadata_fields:
            row[f'original_{field}'] = electoral_df.iloc[i].get(field, None)
            row[f'predicted_{field}'] = result.get(field, None)

            # Check if match
            orig = str(row[f'original_{field}']).lower().strip() if pd.notna(row[f'original_{field}']) else ''
            pred = str(row[f'predicted_{field}']).lower().strip() if pd.notna(row[f'predicted_{field}']) else ''
            row[f'match_{field}'] = orig == pred or (orig in pred) or (pred in orig)

        predictions.append(row)

    return pd.DataFrame(predictions)


def calculate_metadata_metrics(results_df: pd.DataFrame) -> dict:
    """
    Calculate accuracy metrics for metadata extraction.
    """
    metadata_fields = ['country', 'office', 'location', 'election_year', 'is_primary']

    field_metrics = {}
    for field in metadata_fields:
        match_col = f'match_{field}'
        if match_col in results_df.columns:
            matches = results_df[match_col].sum()
            total = len(results_df)
            field_metrics[field] = {
                'matches': int(matches),
                'total': total,
                'accuracy': matches / total if total > 0 else 0,
            }

    # Overall (all fields match)
    all_match_cols = [f'match_{f}' for f in metadata_fields if f'match_{f}' in results_df.columns]
    if all_match_cols:
        all_match = results_df[all_match_cols].all(axis=1).sum()
        total = len(results_df)
        overall_accuracy = all_match / total if total > 0 else 0
    else:
        overall_accuracy = 0
        all_match = 0
        total = 0

    return {
        'overall_accuracy': overall_accuracy,
        'all_fields_match': int(all_match),
        'total': total,
        'field_metrics': field_metrics,
    }


# =============================================================================
# REPORT GENERATION
# =============================================================================

def generate_report(
    category_results: pd.DataFrame,
    category_metrics: dict,
    metadata_results: pd.DataFrame,
    metadata_metrics: dict,
) -> str:
    """
    Generate markdown report for boss.
    """
    report = []
    report.append("# Bellwether Pipeline Validation Report")
    report.append(f"\n**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append(f"\n**Test Date:** {datetime.now().strftime('%Y-%m-%d')}")

    # Executive Summary
    report.append("\n## Executive Summary\n")
    report.append(f"- **Category Classification Accuracy:** {category_metrics['overall_accuracy']:.1%}")
    report.append(f"- **Electoral Metadata Accuracy (all fields):** {metadata_metrics['overall_accuracy']:.1%}")
    report.append(f"- **Markets Tested:** {category_metrics['total']:,}")

    # Category Classification
    report.append("\n## 1. Category Classification\n")
    report.append(f"Tested {category_metrics['total']} markets across {len(category_metrics['category_metrics'])} categories.\n")
    report.append(f"**Overall Accuracy: {category_metrics['overall_accuracy']:.1%}** ({category_metrics['correct']}/{category_metrics['total']} correct)\n")

    report.append("### Accuracy by Category\n")
    report.append("| Category | Accuracy | Precision | Recall | F1 | N |")
    report.append("|----------|----------|-----------|--------|-----|---|")

    for cat, metrics in sorted(category_metrics['category_metrics'].items()):
        cat_short = cat.split('. ')[1] if '. ' in cat else cat
        report.append(
            f"| {cat_short[:20]} | {metrics['accuracy']:.1%} | {metrics['precision']:.1%} | "
            f"{metrics['recall']:.1%} | {metrics['f1']:.2f} | {metrics['total']} |"
        )

    # Confusion Matrix
    report.append("\n### Confusion Matrix\n")
    report.append("```")
    report.append(category_metrics['confusion_matrix'].to_string())
    report.append("```")

    # Common Misclassifications
    report.append("\n### Common Misclassifications\n")
    misclassified = category_results[category_results['original_category'] != category_results['predicted_category']]
    if len(misclassified) > 0:
        confusion_pairs = misclassified.groupby(['original_category', 'predicted_category']).size().sort_values(ascending=False)
        report.append("| Original | Predicted As | Count |")
        report.append("|----------|--------------|-------|")
        for (orig, pred), count in confusion_pairs.head(10).items():
            orig_short = orig.split('. ')[1][:15] if '. ' in orig else orig[:15]
            pred_short = pred.split('. ')[1][:15] if '. ' in pred else pred[:15]
            report.append(f"| {orig_short} | {pred_short} | {count} |")
    else:
        report.append("No misclassifications found!")

    # Electoral Metadata
    report.append("\n## 2. Electoral Metadata Extraction\n")
    report.append(f"Tested {metadata_metrics['total']} electoral markets.\n")
    report.append(f"**All Fields Match: {metadata_metrics['overall_accuracy']:.1%}** ({metadata_metrics['all_fields_match']}/{metadata_metrics['total']})\n")

    report.append("### Accuracy by Field\n")
    report.append("| Field | Accuracy | Matches | Total |")
    report.append("|-------|----------|---------|-------|")

    for field, metrics in metadata_metrics['field_metrics'].items():
        report.append(f"| {field} | {metrics['accuracy']:.1%} | {metrics['matches']} | {metrics['total']} |")

    # Recommendations
    report.append("\n## 3. Recommendations\n")

    if category_metrics['overall_accuracy'] >= 0.90:
        report.append("- ✅ Category classification accuracy is excellent (≥90%)")
    elif category_metrics['overall_accuracy'] >= 0.80:
        report.append("- ⚠️ Category classification accuracy is good but could be improved (80-90%)")
    else:
        report.append("- ❌ Category classification accuracy needs improvement (<80%)")

    if metadata_metrics['overall_accuracy'] >= 0.85:
        report.append("- ✅ Electoral metadata extraction is reliable (≥85%)")
    elif metadata_metrics['overall_accuracy'] >= 0.70:
        report.append("- ⚠️ Electoral metadata extraction is adequate but could be improved (70-85%)")
    else:
        report.append("- ❌ Electoral metadata extraction needs improvement (<70%)")

    # Worst performing categories
    worst_cats = sorted(
        category_metrics['category_metrics'].items(),
        key=lambda x: x[1]['accuracy']
    )[:3]
    if worst_cats and worst_cats[0][1]['accuracy'] < 0.80:
        report.append("\n**Categories needing attention:**")
        for cat, metrics in worst_cats:
            if metrics['accuracy'] < 0.80:
                cat_short = cat.split('. ')[1] if '. ' in cat else cat
                report.append(f"- {cat_short}: {metrics['accuracy']:.1%} accuracy")

    return "\n".join(report)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Validate Bellwether pipeline accuracy")
    parser.add_argument('--sample-size', type=int, default=50, help="Markets per category (default: 50)")
    parser.add_argument('--skip-api', action='store_true', help="Skip API calls (use existing results)")
    parser.add_argument('--categories-only', action='store_true', help="Only test category classification")
    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("BELLWETHER PIPELINE VALIDATION TEST")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Sample size: {args.sample_size} per category")
    print("=" * 70 + "\n")

    # Load master data
    log("Loading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"  Loaded {len(df):,} markets")

    # Create sample
    sample_df = create_stratified_sample(df, args.sample_size)

    # Get Panel A markets for metadata test (ground truth metadata)
    if not args.categories_only:
        panel_a_df = get_panel_a_markets(master_df=df)
        electoral_sample = panel_a_df.sample(n=min(100, len(panel_a_df)), random_state=42) if len(panel_a_df) > 0 else pd.DataFrame()
    else:
        electoral_sample = pd.DataFrame()

    # Save sample
    sample_df.to_csv(SAMPLE_FILE, index=False)
    log(f"Saved sample to: {SAMPLE_FILE}")

    if args.skip_api:
        log("\n--skip-api flag set, loading existing results...")
        if RESULTS_FILE.exists():
            category_results = pd.read_csv(RESULTS_FILE)
        else:
            log("No existing results found. Run without --skip-api first.")
            return
        metadata_results = pd.DataFrame()
    else:
        # Initialize OpenAI client
        log("\nInitializing OpenAI client...")
        client = get_openai_client()

        # Test 1: Category Classification
        category_results = test_category_classification(sample_df, client)

        # Test 2: Electoral Metadata (if we have Panel B data)
        if len(electoral_sample) > 0 and not args.categories_only:
            metadata_results = test_electoral_metadata(electoral_sample, client)
        else:
            metadata_results = pd.DataFrame()

    # Calculate metrics
    log("\n" + "=" * 60)
    log("CALCULATING METRICS")
    log("=" * 60)

    category_metrics = calculate_category_metrics(category_results)
    log(f"Category Classification Accuracy: {category_metrics['overall_accuracy']:.1%}")

    if len(metadata_results) > 0:
        metadata_metrics = calculate_metadata_metrics(metadata_results)
        log(f"Electoral Metadata Accuracy: {metadata_metrics['overall_accuracy']:.1%}")
    else:
        metadata_metrics = {'overall_accuracy': 0, 'all_fields_match': 0, 'total': 0, 'field_metrics': {}}

    # Save results
    category_results.to_csv(RESULTS_FILE, index=False)
    log(f"\nSaved results to: {RESULTS_FILE}")

    if len(metadata_results) > 0:
        metadata_results.to_csv(TEST_DIR / "validation_metadata_results.csv", index=False)

    # Generate report
    report = generate_report(category_results, category_metrics, metadata_results, metadata_metrics)
    with open(REPORT_FILE, 'w') as f:
        f.write(report)
    log(f"Saved report to: {REPORT_FILE}")

    # Print summary
    print("\n" + "=" * 70)
    print("VALIDATION COMPLETE")
    print("=" * 70)
    print(f"\nCategory Classification: {category_metrics['overall_accuracy']:.1%} accuracy")
    print(f"  - {category_metrics['correct']}/{category_metrics['total']} correct")

    if metadata_metrics['total'] > 0:
        print(f"\nElectoral Metadata: {metadata_metrics['overall_accuracy']:.1%} all fields match")
        print(f"  - {metadata_metrics['all_fields_match']}/{metadata_metrics['total']} exact matches")

    print(f"\nReport saved to: {REPORT_FILE}")
    print("=" * 70 + "\n")

    return category_metrics, metadata_metrics


if __name__ == "__main__":
    main()
