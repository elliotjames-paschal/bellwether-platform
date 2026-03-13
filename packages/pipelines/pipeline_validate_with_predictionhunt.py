#!/usr/bin/env python3
"""
Validate cross-platform match candidates against PredictionHunt's Smart Matching.

Used as a gate before auto-applying IDENTICAL verdicts from the embedding+GPT pipeline.
PredictionHunt serves as an independent second opinion — disagreements go to
matches_pending_review.json for human review instead of being auto-applied.

Key principle: PH no-data does NOT block a match (they may have less coverage).
Only an active disagreement (PH says a different Polymarket market) triggers review.

Usage:
    # Validate IDENTICAL verdicts from embedding pipeline
    python pipeline_validate_with_predictionhunt.py

    # Dry run (no API calls)
    python pipeline_validate_with_predictionhunt.py --dry-run

    # Limit number of API calls
    python pipeline_validate_with_predictionhunt.py --limit 10

Input:
    data/cross_platform_candidates.json
    data/cross_platform_resolution_verdicts.json

Output:
    data/predictionhunt_validation.json  (full results)
    data/matches_pending_review.json     (disagreements for human review)
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

CANDIDATES_FILE = DATA_DIR / "cross_platform_candidates.json"
VERDICTS_FILE = DATA_DIR / "cross_platform_resolution_verdicts.json"
VALIDATION_FILE = DATA_DIR / "predictionhunt_validation.json"
PENDING_REVIEW_FILE = DATA_DIR / "matches_pending_review.json"
CHECKPOINT_FILE = DATA_DIR / "predictionhunt_checked.json"

PIPELINE_NAME = "validate_embedding"


def load_identical_verdicts():
    """Load Bucket B candidates that received IDENTICAL GPT verdicts."""
    if not CANDIDATES_FILE.exists():
        return []
    if not VERDICTS_FILE.exists():
        return []

    with open(CANDIDATES_FILE) as f:
        candidates = json.load(f)
    with open(VERDICTS_FILE) as f:
        verdicts_data = json.load(f)

    verdicts_by_key = {v["pair_key"]: v for v in verdicts_data.get("verdicts", [])}

    identical = []
    for pair in candidates.get("bucket_b", []):
        v = verdicts_by_key.get(pair["pair_key"])
        if v and v.get("verdict") == "IDENTICAL":
            identical.append({
                "pair_key": pair["pair_key"],
                "kalshi_market_id": pair["kalshi_market_id"],
                "poly_market_id": pair["poly_market_id"],
                "kalshi_ticker": pair.get("kalshi_ticker", ""),
                "poly_ticker": pair.get("poly_ticker", ""),
                "kalshi_question": pair.get("kalshi_question", ""),
                "poly_question": pair.get("poly_question", ""),
                "cosine_similarity": pair.get("cosine_similarity", 0),
                "correct_ticker": v.get("correct_ticker", ""),
            })

    return identical


def load_checkpoint():
    """Load already-checked pair keys."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"checked": {}}


def save_checkpoint(checkpoint):
    checkpoint["updated_at"] = datetime.now(timezone.utc).isoformat()
    atomic_write_json(CHECKPOINT_FILE, checkpoint, indent=2, ensure_ascii=False)


def _matches_poly_id(our_poly_id, ph_poly_markets):
    """Check if our Polymarket ID matches any PH Polymarket market.

    Handles the format mismatch: we may store slugs (e.g.,
    'will-kathleen-riebe-be-the-democratic-nominee-for-ut-01')
    while PH returns numeric IDs (e.g., '704282') with source_url
    containing the slug.

    Args:
        our_poly_id: Our Polymarket identifier (slug or numeric)
        ph_poly_markets: List of dicts with 'id' and 'source_url' from PH
    """
    our_id_norm = str(our_poly_id).strip().lower()

    for pm in ph_poly_markets:
        ph_id = str(pm["id"] if isinstance(pm, dict) else pm).strip().lower()
        # Direct ID match
        if ph_id == our_id_norm:
            return True
        # Slug match: check if our slug appears in PH's source_url
        if isinstance(pm, dict):
            source_url = pm.get("source_url", "").lower()
            if our_id_norm in source_url:
                return True
            # Reverse: check if PH's ID appears in our ID (if ours is a URL/slug)
            if ph_id and ph_id in our_id_norm:
                return True

    return False


def classify_ph_response(ph_result, our_poly_id, client, our_kalshi_id=None):
    """Classify PredictionHunt response relative to our expected Polymarket match.

    Uses group-level matching when a Kalshi market ID is provided: finds the
    specific group containing our Kalshi market and checks if our Polymarket
    market is in the same group.

    Handles slug-vs-numeric ID mismatch by also checking source_url fields.

    Returns:
        (status, ph_poly_ids)
        status: "confirmed" | "disagreed" | "no_match" | "error"
    """
    if not ph_result.get("success"):
        # PH returns success=false with count=0 when it has no data for that ticker
        # — that's "no_match", not an error. Only treat as error if there's an HTTP/network issue.
        error = ph_result.get("error", "")
        if ph_result.get("count", 0) == 0 and not error:
            return "no_match", []
        return "error", []

    # If we have the Kalshi market ID, do precise group-level matching
    if our_kalshi_id:
        group_pm_markets = client.find_group_for_kalshi_market(ph_result, our_kalshi_id)
        if not group_pm_markets:
            return "no_match", []

        ph_ids = [m["id"] if isinstance(m, dict) else m for m in group_pm_markets]
        if _matches_poly_id(our_poly_id, group_pm_markets):
            return "confirmed", ph_ids
        else:
            return "disagreed", ph_ids

    # Fallback: check all Polymarket IDs across all groups
    ids = client.extract_platform_ids(ph_result)
    ph_poly_markets = ids["polymarket_ids"]  # list of dicts with id, source_url

    if not ph_poly_markets:
        return "no_match", []

    ph_ids = [m["id"] for m in ph_poly_markets]
    if _matches_poly_id(our_poly_id, ph_poly_markets):
        return "confirmed", ph_ids

    return "disagreed", ph_ids


def load_pending_review():
    """Load existing pending review items."""
    if PENDING_REVIEW_FILE.exists():
        with open(PENDING_REVIEW_FILE) as f:
            data = json.load(f)
        return data.get("items", [])
    return []


def main():
    parser = argparse.ArgumentParser(description="Validate matches against PredictionHunt")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be queried without calling API")
    parser.add_argument("--limit", type=int, default=0, help="Max API calls (0 = all unchecked)")
    parser.add_argument("--delay", type=float, default=1.0, help="Seconds between API calls")
    args = parser.parse_args()

    print(f"[{datetime.now().isoformat()}] PredictionHunt validation...")

    # Load IDENTICAL verdicts to validate
    identical = load_identical_verdicts()
    if not identical:
        print("  No IDENTICAL verdicts to validate.")
        sys.exit(0)

    print(f"  Found {len(identical)} IDENTICAL verdicts")

    # Load checkpoint — skip already-checked pairs
    checkpoint = load_checkpoint()
    checked_keys = set(checkpoint["checked"].keys())
    to_check = [p for p in identical if p["pair_key"] not in checked_keys]

    if not to_check:
        print("  All pairs already checked.")
        sys.exit(0)

    if args.limit > 0:
        to_check = to_check[:args.limit]

    print(f"  Will check {len(to_check)} pairs (skipping {len(identical) - len(to_check)} already checked)")

    if args.dry_run:
        print("\n  DRY RUN — would query these Kalshi tickers:")
        for p in to_check:
            print(f"    {p['kalshi_market_id']} (poly: {p['poly_market_id']}, sim: {p['cosine_similarity']:.3f})")
        sys.exit(0)

    # Initialize client
    from predictionhunt_client import PredictionHuntClient, BudgetExhaustedError
    client = PredictionHuntClient(delay=args.delay)

    remaining, used, limit = client.check_budget()
    print(f"  PH budget: {used}/{limit} used, {remaining} remaining")

    if remaining < len(to_check):
        print(f"  WARNING: Only {remaining} requests remaining, but {len(to_check)} to check. "
              f"Will stop at budget limit.")

    # Process pairs
    results = {"confirmed": [], "disagreed": [], "no_match": [], "error": []}
    pending_review_new = []

    for i, pair in enumerate(to_check):
        try:
            client.check_budget()
        except BudgetExhaustedError:
            print(f"\n  Budget exhausted after {i} requests. Saving progress.")
            break

        k_ticker = pair["kalshi_market_id"]
        print(f"  [{i+1}/{len(to_check)}] Querying: {k_ticker}", end=" ")

        ph_result = client.query_by_kalshi_ticker(k_ticker, pipeline=PIPELINE_NAME)
        status, ph_poly_ids = classify_ph_response(ph_result, pair["poly_market_id"], client, our_kalshi_id=k_ticker)

        print(f"-> {status}" + (f" (PH poly: {ph_poly_ids})" if ph_poly_ids else ""))

        # Record result
        result_entry = {
            "pair_key": pair["pair_key"],
            "kalshi_market_id": pair["kalshi_market_id"],
            "poly_market_id": pair["poly_market_id"],
            "kalshi_ticker": pair.get("kalshi_ticker", ""),
            "poly_ticker": pair.get("poly_ticker", ""),
            "cosine_similarity": pair.get("cosine_similarity", 0),
            "ph_status": status,
            "ph_poly_ids": ph_poly_ids,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

        if not ph_result.get("success"):
            result_entry["ph_error"] = ph_result.get("error", "unknown")

        results[status].append(result_entry)

        # Save to checkpoint
        checkpoint["checked"][pair["pair_key"]] = {
            "status": status,
            "checked_at": result_entry["checked_at"],
        }

        # If disagreed, add to pending review
        if status == "disagreed":
            pending_review_new.append({
                "source": "embedding_gpt",
                "pair_key": pair["pair_key"],
                "kalshi_market_id": pair["kalshi_market_id"],
                "poly_market_id": pair["poly_market_id"],
                "our_ticker": pair.get("kalshi_ticker", ""),
                "kalshi_question": pair.get("kalshi_question", ""),
                "poly_question": pair.get("poly_question", ""),
                "cosine_similarity": pair.get("cosine_similarity", 0),
                "gpt_verdict": "IDENTICAL",
                "ph_status": "disagreed",
                "ph_matched_pm": ph_poly_ids,
                "created_at": datetime.now(timezone.utc).isoformat(),
            })

        # Checkpoint every 10 requests
        if (i + 1) % 10 == 0:
            save_checkpoint(checkpoint)

    # Final checkpoint save
    save_checkpoint(checkpoint)

    # Write validation report
    total_checked = sum(len(v) for v in results.values())
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_checked": total_checked,
        "confirmed": len(results["confirmed"]),
        "disagreed": len(results["disagreed"]),
        "no_match": len(results["no_match"]),
        "error": len(results["error"]),
        "agreement_rate": (len(results["confirmed"]) / total_checked) if total_checked > 0 else 0,
        "results": results,
    }
    atomic_write_json(VALIDATION_FILE, report, indent=2, ensure_ascii=False)
    print(f"\n  Saved validation report to {VALIDATION_FILE}")

    # Update pending review file (merge with existing)
    if pending_review_new:
        existing_review = load_pending_review()
        existing_keys = {item["pair_key"] for item in existing_review}
        for item in pending_review_new:
            if item["pair_key"] not in existing_keys:
                existing_review.append(item)

        review_output = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "items": existing_review,
        }
        atomic_write_json(PENDING_REVIEW_FILE, review_output, indent=2, ensure_ascii=False)
        print(f"  Added {len(pending_review_new)} items to pending review ({PENDING_REVIEW_FILE})")

    # Summary
    print(f"\n=== PREDICTIONHUNT VALIDATION COMPLETE ===")
    print(f"  Checked:    {total_checked}")
    print(f"  Confirmed:  {len(results['confirmed'])} (PH agrees)")
    print(f"  No match:   {len(results['no_match'])} (PH has no data — will still apply)")
    print(f"  Disagreed:  {len(results['disagreed'])} (flagged for review)")
    print(f"  Errors:     {len(results['error'])} (will retry next run)")

    usage = client.get_usage_summary()
    print(f"\n  PH budget: {usage['used']}/{usage['limit']} used this month")

    return 0


if __name__ == "__main__":
    sys.exit(main())
