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
import math
import argparse
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

# --- Paths ---
HUMAN_LABELS_FILE = DATA_DIR / "human_labels.json"
TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"
MASTER_CSV_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
REPORT_FILE = DATA_DIR / "match_accuracy_report.json"
CANDIDATES_FILE = DATA_DIR / "cross_platform_candidates.json"
VERDICTS_FILE = DATA_DIR / "cross_platform_resolution_verdicts.json"

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


def generate_suggested_labels(
    ticker_lookup: dict,
    existing_labels: list,
    max_suggestions: int = 25,
) -> list:
    """Generate a ranked list of market pairs most valuable to label next.

    Scoring uses a composite formula that balances:
    - uncertainty_score (0.4): ticker component similarity — pairs that almost
      match are most informative (1-2 field diffs)
    - cosine_similarity (0.3): embedding agreement from discovery pipeline
    - novelty_score (0.2): whether this error pattern already has labels;
      saturated patterns get deprioritized
    - log(volume) (0.1): tiebreaker, log-scaled to prevent volume dominance

    Reads cross_platform_candidates.json and cross_platform_resolution_verdicts.json
    if available. Falls back gracefully if files don't exist.
    """
    # Load candidates (embedding-discovered pairs)
    candidates = []
    if CANDIDATES_FILE.exists():
        with open(CANDIDATES_FILE) as f:
            cdata = json.load(f)
        # Collect from all buckets — B and C are the interesting ones
        for bucket_key in ("bucket_b", "bucket_c"):
            candidates.extend(cdata.get(bucket_key, []))

    if not candidates:
        return []

    # Load verdicts if available (adds OVERLAPPING/DIFFERENT/IDENTICAL classification)
    verdict_lookup = {}
    if VERDICTS_FILE.exists():
        with open(VERDICTS_FILE) as f:
            vdata = json.load(f)
        for v in vdata.get("verdicts", []):
            verdict_lookup[v.get("pair_key")] = v

    # Build set of already-labeled market pairs to exclude
    labeled_pairs = set()
    for label in existing_labels:
        mids = label.get("market_ids", [])
        if len(mids) >= 2:
            # Store both orderings
            for i in range(len(mids)):
                for j in range(i + 1, len(mids)):
                    labeled_pairs.add((mids[i], mids[j]))
                    labeled_pairs.add((mids[j], mids[i]))

    # Count existing correction patterns to compute novelty
    # (which ticker-component diffs already have labels)
    existing_diff_counts = Counter()
    for label in existing_labels:
        lt = label.get("label_type", "")
        if lt in ("same_event_same_rules", "same_event_different_rules", "different_event"):
            mids = label.get("market_ids", [])
            tickers = [ticker_lookup.get(m, {}).get("ticker", "") for m in mids]
            if len(tickers) >= 2 and all(tickers):
                diff_key = _diff_signature(tickers[0], tickers[1])
                if diff_key:
                    existing_diff_counts[diff_key] += 1

    # Score each candidate
    scored = []
    for pair in candidates:
        k_mid = pair.get("kalshi_market_id", "")
        p_mid = pair.get("poly_market_id", "")

        # Skip already-labeled pairs
        if (k_mid, p_mid) in labeled_pairs:
            continue

        cosine_sim = pair.get("cosine_similarity", 0.0)

        # Uncertainty score: how close are the tickers?
        k_ticker = pair.get("kalshi_ticker", "")
        p_ticker = pair.get("poly_ticker", "")
        uncertainty = _ticker_uncertainty(k_ticker, p_ticker)

        # Novelty: does this diff pattern already have labels?
        diff_key = _diff_signature(k_ticker, p_ticker)
        existing_count = existing_diff_counts.get(diff_key, 0) if diff_key else 0
        novelty = 1.0 / (1.0 + existing_count)  # 1.0 if new, 0.5 if 1 existing, 0.33 if 2, etc.

        # Volume (log-scaled, from ticker lookup)
        k_vol = ticker_lookup.get(k_mid, {}).get("volume", 0) or 0
        p_vol = ticker_lookup.get(p_mid, {}).get("volume", 0) or 0
        combined_volume = k_vol + p_vol
        # Normalize: log10(volume) / log10(100M) gives ~0-1 range
        log_vol = math.log10(max(combined_volume, 1)) / 8.0
        log_vol = min(log_vol, 1.0)

        # Composite score
        score = (
            0.4 * uncertainty
            + 0.3 * cosine_sim
            + 0.2 * novelty
            + 0.1 * log_vol
        )

        # Build reason string
        verdict = verdict_lookup.get(pair.get("pair_key"), {})
        verdict_str = verdict.get("verdict", "")
        reason_parts = []
        if verdict_str == "OVERLAPPING":
            reason_parts.append("Ambiguous verdict — human review resolves uncertainty")
        elif uncertainty > 0.5:
            reason_parts.append("Tickers nearly match — likely same event with different resolution")
        if novelty > 0.9:
            reason_parts.append("Novel error pattern (no existing labels)")
        if log_vol > 0.6:
            reason_parts.append(f"High volume (${combined_volume:,.0f})")

        scored.append({
            "kalshi_market_id": k_mid,
            "poly_market_id": p_mid,
            "ticker_a": k_ticker,
            "ticker_b": p_ticker,
            "kalshi_question": pair.get("kalshi_question", ""),
            "poly_question": pair.get("poly_question", ""),
            "combined_volume": combined_volume,
            "cosine_similarity": round(cosine_sim, 4),
            "score": round(score, 4),
            "score_components": {
                "uncertainty": round(uncertainty, 4),
                "cosine_similarity": round(cosine_sim, 4),
                "novelty": round(novelty, 4),
                "log_volume": round(log_vol, 4),
            },
            "verdict": verdict_str or None,
            "bucket": pair.get("bucket", ""),
            "reason": "; ".join(reason_parts) if reason_parts else "Candidate for review",
        })

    # Sort by composite score descending
    scored.sort(key=lambda x: x["score"], reverse=True)

    # Add rank
    for i, s in enumerate(scored[:max_suggestions]):
        s["rank"] = i + 1

    return scored[:max_suggestions]


def _parse_ticker_fields(ticker: str) -> dict:
    """Parse BWR ticker into component dict."""
    parts = ticker.split("-")
    if len(parts) < 7:
        return {"agent": "", "action": "", "target": "", "mechanism": "", "threshold": "", "timeframe": ""}
    return {
        "agent": parts[1],
        "action": parts[2],
        "target": "-".join(parts[3:-3]),
        "mechanism": parts[-3],
        "threshold": parts[-2],
        "timeframe": parts[-1],
    }


def _ticker_uncertainty(ticker_a: str, ticker_b: str) -> float:
    """Score how 'close' two tickers are. Higher = more uncertain (almost matching).

    Returns 0.0 for identical tickers (no uncertainty — already matched).
    Returns highest scores for 1-2 field differences (pipeline is uncertain).
    Returns lower scores for 3+ differences (clearly different).
    """
    if not ticker_a or not ticker_b:
        return 0.3  # Unknown — moderate uncertainty
    if ticker_a == ticker_b:
        return 0.0  # Already matched

    a = _parse_ticker_fields(ticker_a)
    b = _parse_ticker_fields(ticker_b)

    # Count field differences, weighting core identity fields higher
    core_fields = ("agent", "action", "target", "timeframe")
    resolution_fields = ("mechanism", "threshold")

    core_diffs = sum(1 for f in core_fields if a.get(f) != b.get(f))
    res_diffs = sum(1 for f in resolution_fields if a.get(f) != b.get(f))

    if core_diffs == 0:
        # Only resolution differences — very likely same event, high uncertainty
        return 0.9 if res_diffs > 0 else 0.0
    elif core_diffs == 1:
        # One core field differs — could be alias issue, high value
        return 0.7
    elif core_diffs == 2:
        # Two core diffs — moderate uncertainty
        return 0.4
    else:
        # 3+ core diffs — probably genuinely different
        return 0.15


def _diff_signature(ticker_a: str, ticker_b: str) -> str:
    """Create a hashable signature of which fields differ between two tickers.

    Used to track which error patterns already have labels (novelty scoring).
    """
    if not ticker_a or not ticker_b:
        return ""
    a = _parse_ticker_fields(ticker_a)
    b = _parse_ticker_fields(ticker_b)
    diffs = sorted(f for f in ("agent", "action", "target", "mechanism", "threshold", "timeframe")
                   if a.get(f) != b.get(f))
    return "|".join(diffs) if diffs else ""


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


def generate_report(
    same_event_eval: dict,
    category_eval: dict,
    suggested_labels: list = None,
    batch_id: str = None,
) -> dict:
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

    report = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "batch_id": batch_id,
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

    if suggested_labels is not None:
        report["suggested_labels"] = suggested_labels
        report["suggested_labels_scoring"] = {
            "formula": "0.4*uncertainty + 0.3*cosine_similarity + 0.2*novelty + 0.1*log_volume",
            "weights": {
                "uncertainty": 0.4,
                "cosine_similarity": 0.3,
                "novelty": 0.2,
                "log_volume": 0.1,
            },
            "notes": (
                "uncertainty: ticker component similarity (1-2 field diffs score highest). "
                "novelty: 1/(1+existing_labels_for_pattern) — new patterns prioritized. "
                "log_volume: log10(volume)/8, capped at 1.0 — tiebreaker only."
            ),
        }

    return report


def main():
    parser = argparse.ArgumentParser(description="Evaluate match accuracy against human labels")
    parser.add_argument("--verbose", action="store_true", help="Print detailed disagreements")
    parser.add_argument("--max-suggestions", type=int, default=25,
                        help="Max number of suggested labels to generate (default: 25)")
    parser.add_argument("--batch-id", type=str, default=None,
                        help="Batch ID for traceability (auto-generated if not provided)")
    args = parser.parse_args()

    batch_id = args.batch_id or datetime.now(timezone.utc).strftime("batch_%Y%m%d_%H%M%S")
    now = datetime.now(timezone.utc).isoformat()
    print(f"[{now}] Evaluating match accuracy (batch: {batch_id})...")

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

    # Generate suggested labels
    suggested = generate_suggested_labels(
        ticker_lookup, labels, max_suggestions=args.max_suggestions,
    )
    if suggested:
        print(f"\n  Suggested labels generated: {len(suggested)}")
    else:
        print(f"\n  No suggested labels (candidates file may not exist yet)")

    # Generate report
    report = generate_report(
        same_event_eval, category_eval,
        suggested_labels=suggested, batch_id=batch_id,
    )

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

    if suggested:
        top_n = suggested[:10]
        print(f"\n  === Top {len(top_n)} Suggested Labels ===")
        for s in top_n:
            print(f"    #{s['rank']} (score: {s['score']:.3f}) {s['reason']}")
            print(f"      K: {s['ticker_a']}")
            print(f"      P: {s['ticker_b']}")
            if args.verbose:
                sc = s['score_components']
                print(f"      Components: unc={sc['uncertainty']:.2f} cos={sc['cosine_similarity']:.2f} "
                      f"nov={sc['novelty']:.2f} vol={sc['log_volume']:.2f}")

    # Write report
    atomic_write_json(REPORT_FILE, report, indent=2)
    print(f"\n  Report written to {REPORT_FILE.name}")


if __name__ == "__main__":
    main()
