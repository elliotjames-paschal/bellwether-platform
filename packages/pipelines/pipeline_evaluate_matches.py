#!/usr/bin/env python3
"""
Pipeline Step: Evaluate Match Accuracy Against Human Labels

Compares human labels against current pipeline output to compute precision,
recall, and F1 for cross-platform matching. Also evaluates category accuracy.

This is a READ-ONLY script — it does NOT modify any data files.

Reads: data/human_labels.json, data/tickers_postprocessed.json,
       data/combined_political_markets_with_electoral_details_UPDATED.csv
Writes: data/match_accuracy_report.json
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

# --- Paths ---
HUMAN_LABELS_FILE = DATA_DIR / "human_labels.json"
TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"
MASTER_CSV_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
REPORT_FILE = DATA_DIR / "match_accuracy_report.json"

# Valid political categories (from pipeline_classify_categories.py)
VALID_CATEGORIES = {
    "1. ELECTORAL", "2. MONETARY_POLICY", "3. LEGISLATIVE",
    "4. APPOINTMENTS", "5. REGULATORY", "6. INTERNATIONAL",
    "7. JUDICIAL", "8. MILITARY_SECURITY", "9. CRISIS_EMERGENCY",
    "10. GOVERNMENT_OPERATIONS", "11. PARTY_POLITICS",
    "12. STATE_LOCAL", "13. TIMING_EVENTS", "14. POLLING_APPROVAL",
    "15. POLITICAL_SPEECH", "16. NOT_POLITICAL",
}


def load_human_labels() -> dict:
    """Load human_labels.json."""
    if not HUMAN_LABELS_FILE.exists():
        return {"labels": []}
    with open(HUMAN_LABELS_FILE) as f:
        return json.load(f)


def load_tickers_data() -> dict:
    """Load tickers_postprocessed.json."""
    if not TICKERS_FILE.exists():
        return {"tickers": []}
    with open(TICKERS_FILE) as f:
        return json.load(f)


def build_market_id_to_ticker(tickers_data: dict) -> dict:
    """Build market_id -> ticker_dict lookup."""
    lookup = {}
    for t in tickers_data.get("tickers", []):
        mid = str(t.get("market_id", ""))
        if mid:
            lookup[mid] = t
    return lookup


def load_master_csv_categories() -> dict:
    """Load market_id -> political_category from master CSV.

    Returns dict mapping market_id to category string.
    """
    import csv
    if not MASTER_CSV_FILE.exists():
        return {}
    lookup = {}
    with open(MASTER_CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mid = row.get("market_id", "").strip()
            cat = row.get("political_category", "").strip()
            if mid and cat:
                lookup[mid] = cat
    return lookup


def evaluate_same_event_labels(labels: list, ticker_lookup: dict) -> dict:
    """Evaluate same-event labels against pipeline ticker assignments.

    For each same_event_same_rules label with 2+ markets:
    - True Positive: pipeline assigned the same ticker to all markets
    - False Negative: pipeline assigned different tickers (missed match)

    For each different_event label:
    - True Negative: pipeline assigned different tickers (correct separation)
    - False Positive: pipeline assigned the same ticker (incorrect match)

    Returns evaluation dict with counts and disagreement details.
    """
    true_positives = []
    false_negatives = []
    false_positives = []
    true_negatives = []
    skipped = 0

    for label in labels:
        label_type = label.get("label_type", "")
        market_ids = label.get("market_ids", [])

        if label_type not in ("same_event_same_rules", "same_event_different_rules", "different_event"):
            continue

        if len(market_ids) < 2:
            skipped += 1
            continue

        # Look up tickers for all markets
        tickers_found = []
        for mid in market_ids:
            ticker_obj = ticker_lookup.get(mid)
            if ticker_obj:
                tickers_found.append(ticker_obj.get("ticker", "UNKNOWN"))
            else:
                tickers_found.append(None)

        # Skip if any market not found in tickers
        if None in tickers_found:
            skipped += 1
            continue

        # Check if all tickers are the same
        unique_tickers = set(tickers_found)
        all_same = len(unique_tickers) == 1

        entry = {
            "label_id": label.get("label_id", ""),
            "label_type": label_type,
            "market_ids": market_ids,
            "pipeline_tickers": tickers_found,
            "description": label.get("description", ""),
        }

        if label_type in ("same_event_same_rules", "same_event_different_rules"):
            if all_same:
                true_positives.append(entry)
            else:
                entry["action_needed"] = "unify_tickers"
                false_negatives.append(entry)
        elif label_type == "different_event":
            if all_same:
                entry["action_needed"] = "break_match"
                false_positives.append(entry)
            else:
                true_negatives.append(entry)

    return {
        "true_positives": true_positives,
        "false_negatives": false_negatives,
        "false_positives": false_positives,
        "true_negatives": true_negatives,
        "skipped": skipped,
    }


def evaluate_category_labels(labels: list, category_lookup: dict) -> dict:
    """Evaluate category labels against pipeline category assignments.

    For each not_political label:
    - Correct: pipeline already has NOT_POLITICAL
    - Incorrect: pipeline has a different category

    For each wrong_category label:
    - Logged for review (we can't auto-evaluate without knowing the correct category)

    Returns evaluation dict.
    """
    correct = []
    incorrect = []
    skipped = 0

    for label in labels:
        label_type = label.get("label_type", "")
        market_ids = label.get("market_ids", [])

        if label_type == "not_political":
            for mid in market_ids:
                current_cat = category_lookup.get(mid, "")
                entry = {
                    "label_id": label.get("label_id", ""),
                    "market_id": mid,
                    "current_category": current_cat,
                    "expected_category": "16. NOT_POLITICAL",
                }
                if not current_cat:
                    skipped += 1
                    continue
                if current_cat == "16. NOT_POLITICAL":
                    correct.append(entry)
                else:
                    entry["action_needed"] = "mark_not_political"
                    incorrect.append(entry)

        elif label_type == "wrong_category":
            for mid in market_ids:
                current_cat = category_lookup.get(mid, "")
                entry = {
                    "label_id": label.get("label_id", ""),
                    "market_id": mid,
                    "current_category": current_cat,
                    "description": label.get("description", ""),
                    "action_needed": "review_category",
                }
                if not current_cat:
                    skipped += 1
                    continue
                incorrect.append(entry)

    return {
        "correct": correct,
        "incorrect": incorrect,
        "skipped": skipped,
    }


def compute_precision_recall(tp: int, fp: int, fn: int) -> dict:
    """Compute precision, recall, and F1 score.

    Returns dict with precision, recall, f1. Returns 0.0 for metrics
    that would involve division by zero.
    """
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


def generate_report(same_event_eval: dict, category_eval: dict) -> dict:
    """Generate the accuracy report combining all evaluations."""
    tp = len(same_event_eval["true_positives"])
    fp = len(same_event_eval["false_positives"])
    fn = len(same_event_eval["false_negatives"])
    tn = len(same_event_eval["true_negatives"])
    metrics = compute_precision_recall(tp, fp, fn)

    # Combine all disagreements for easy review
    disagreements = []
    for entry in same_event_eval["false_negatives"]:
        disagreements.append({**entry, "human_judgment": "same event"})
    for entry in same_event_eval["false_positives"]:
        disagreements.append({**entry, "human_judgment": "different event"})
    for entry in category_eval["incorrect"]:
        disagreements.append({**entry, "human_judgment": "category mismatch"})

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "sample_size": tp + fp + fn + tn,
        "matching": {
            "true_positives": tp,
            "false_positives": fp,
            "false_negatives": fn,
            "true_negatives": tn,
            **metrics,
        },
        "categories": {
            "correct": len(category_eval["correct"]),
            "incorrect": len(category_eval["incorrect"]),
            "skipped": category_eval["skipped"],
        },
        "disagreements": disagreements,
        "matching_skipped": same_event_eval["skipped"],
    }


def main():
    parser = argparse.ArgumentParser(description="Evaluate match accuracy against human labels")
    parser.add_argument("--verbose", action="store_true", help="Print detailed disagreements")
    args = parser.parse_args()

    now = datetime.now(timezone.utc).isoformat()
    print(f"[{now}] Evaluating match accuracy...")

    # Load data
    human_data = load_human_labels()
    labels = human_data.get("labels", [])
    if not labels:
        print("  No human labels found. Nothing to evaluate.")
        return

    print(f"  Human labels: {len(labels)}")

    tickers_data = load_tickers_data()
    ticker_lookup = build_market_id_to_ticker(tickers_data)
    print(f"  Tickers loaded: {len(ticker_lookup)}")

    category_lookup = load_master_csv_categories()
    print(f"  Category lookup: {len(category_lookup)} markets")

    # Evaluate
    same_event_eval = evaluate_same_event_labels(labels, ticker_lookup)
    category_eval = evaluate_category_labels(labels, category_lookup)

    # Generate report
    report = generate_report(same_event_eval, category_eval)

    # Print summary
    m = report["matching"]
    print(f"\n  === Matching Accuracy ===")
    print(f"  True Positives:  {m['true_positives']}")
    print(f"  False Negatives: {m['false_negatives']}")
    print(f"  False Positives: {m['false_positives']}")
    print(f"  True Negatives:  {m['true_negatives']}")
    print(f"  Precision: {m['precision']:.2%}")
    print(f"  Recall:    {m['recall']:.2%}")
    print(f"  F1:        {m['f1']:.2%}")
    print(f"  Skipped:   {report['matching_skipped']}")

    c = report["categories"]
    print(f"\n  === Category Accuracy ===")
    print(f"  Correct:   {c['correct']}")
    print(f"  Incorrect: {c['incorrect']}")
    print(f"  Skipped:   {c['skipped']}")

    if args.verbose and report["disagreements"]:
        print(f"\n  === Disagreements ({len(report['disagreements'])}) ===")
        for d in report["disagreements"]:
            print(f"    [{d.get('label_id', '?')}] {d.get('human_judgment', '?')}: "
                  f"{d.get('market_ids', d.get('market_id', '?'))}")
            if d.get("pipeline_tickers"):
                print(f"      Tickers: {d['pipeline_tickers']}")
            if d.get("current_category"):
                print(f"      Category: {d['current_category']}")
            if d.get("action_needed"):
                print(f"      Action: {d['action_needed']}")

    # Write report
    atomic_write_json(REPORT_FILE, report, indent=2)
    print(f"\n  Report written to {REPORT_FILE.name}")


if __name__ == "__main__":
    main()
