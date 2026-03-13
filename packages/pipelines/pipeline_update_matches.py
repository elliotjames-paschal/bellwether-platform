#!/usr/bin/env python3
"""
Phase 2.5e: Update Match Files

Applies cross-platform discovery results:
- Bucket A (identical tickers found via embeddings): log as data issue
- Bucket B + IDENTICAL verdict: unify tickers in tickers_postprocessed.json
- Bucket B + OVERLAPPING/DIFFERENT: write to near_matches.json
- Update reviewed pairs tracking file

Reads: cross_platform_candidates.json, cross_platform_resolution_verdicts.json,
       tickers_postprocessed.json
Writes: tickers_postprocessed.json (updated), near_matches.json,
        cross_platform_reviewed_pairs.json
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

# --- Paths ---
CANDIDATES_FILE = DATA_DIR / "cross_platform_candidates.json"
VERDICTS_FILE = DATA_DIR / "cross_platform_resolution_verdicts.json"
TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"
NEAR_MATCHES_FILE = DATA_DIR / "near_matches.json"
REVIEWED_PAIRS_FILE = DATA_DIR / "cross_platform_reviewed_pairs.json"
PENDING_REVIEW_FILE = DATA_DIR / "matches_pending_review.json"


def reassemble_ticker(ticker):
    """Reassemble ticker string from components."""
    agent = ticker.get("agent", "UNKNOWN")
    action = ticker.get("action", "UNKNOWN")
    target = ticker.get("target", "UNKNOWN")
    mechanism = ticker.get("mechanism", "STD")
    threshold = ticker.get("threshold", "ANY")
    timeframe = ticker.get("timeframe", "UNKNOWN")
    return f"BWR-{agent}-{action}-{target}-{mechanism}-{threshold}-{timeframe}"


def parse_ticker_components(ticker_str):
    """Parse ticker string into components dict."""
    parts = ticker_str.split("-")
    if len(parts) < 7:
        return {
            "agent": parts[1] if len(parts) > 1 else "",
            "action": parts[2] if len(parts) > 2 else "",
            "target": "-".join(parts[3:]) if len(parts) > 3 else "",
            "mechanism": "", "threshold": "", "timeframe": "",
        }
    return {
        "agent": parts[1],
        "action": parts[2],
        "target": "-".join(parts[3:-3]),
        "mechanism": parts[-3],
        "threshold": parts[-2],
        "timeframe": parts[-1],
    }


def load_candidates():
    """Load triaged candidates."""
    if not CANDIDATES_FILE.exists():
        return {"bucket_a": [], "bucket_b": [], "bucket_c": []}
    with open(CANDIDATES_FILE) as f:
        return json.load(f)


def load_verdicts():
    """Load GPT verdicts keyed by pair_key."""
    if not VERDICTS_FILE.exists():
        return {}
    with open(VERDICTS_FILE) as f:
        data = json.load(f)
    return {v["pair_key"]: v for v in data.get("verdicts", [])}


def load_tickers_data():
    """Load full tickers data structure."""
    with open(TICKERS_FILE) as f:
        return json.load(f)


def load_reviewed_pairs():
    """Load existing reviewed pairs."""
    if REVIEWED_PAIRS_FILE.exists():
        with open(REVIEWED_PAIRS_FILE) as f:
            return json.load(f)
    return {"updated_at": None, "pairs": {}}


def handle_bucket_a(bucket_a, tickers_by_id):
    """
    Bucket A: Identical tickers found via embeddings but not matched.
    This is a bug — log it. The markets should already be grouped by
    generate_market_map since they have the same ticker.
    """
    if not bucket_a:
        return 0

    print(f"\n  Bucket A: {len(bucket_a)} identical-ticker pairs (logging as data issues)")
    for pair in bucket_a:
        k_mid = pair["kalshi_market_id"]
        p_mid = pair["poly_market_id"]
        k_t = tickers_by_id.get(k_mid, {})
        p_t = tickers_by_id.get(p_mid, {})
        print(f"    BUG? {pair['kalshi_ticker']}")
        print(f"         Kalshi: {k_mid} | Poly: {p_mid}")
        if k_t.get("ticker") != p_t.get("ticker"):
            print(f"         Actual tickers differ: K={k_t.get('ticker')} vs PM={p_t.get('ticker')}")

    return len(bucket_a)


def unify_identical_tickers(identical_verdicts, tickers_by_id, all_tickers):
    """
    For IDENTICAL verdicts: unify tickers so both platforms match.

    Strategy: If GPT provided a correct_ticker, use that. Otherwise,
    use the Kalshi market's ticker (convention: Kalshi has more explicit rules).

    Returns count of tickers modified.
    """
    modified = 0

    for verdict in identical_verdicts:
        k_mid = verdict["kalshi_market_id"]
        p_mid = verdict["poly_market_id"]

        k_ticker_obj = tickers_by_id.get(k_mid)
        p_ticker_obj = tickers_by_id.get(p_mid)

        if not k_ticker_obj or not p_ticker_obj:
            print(f"    WARN: Missing ticker for {k_mid} or {p_mid}")
            continue

        # Determine target ticker
        correct_ticker_str = verdict.get("correct_ticker")
        if correct_ticker_str and correct_ticker_str.startswith("BWR-"):
            target_components = parse_ticker_components(correct_ticker_str)
        else:
            # Default: use Kalshi's components
            target_components = {
                "mechanism": k_ticker_obj.get("mechanism", "STD"),
                "threshold": k_ticker_obj.get("threshold", "ANY"),
            }
            correct_ticker_str = k_ticker_obj["ticker"]

        # Find which market needs updating
        if k_ticker_obj["ticker"] == correct_ticker_str:
            # Kalshi is correct, update Polymarket
            to_update = p_ticker_obj
        elif p_ticker_obj["ticker"] == correct_ticker_str:
            # Polymarket is correct, update Kalshi
            to_update = k_ticker_obj
        else:
            # Neither matches the correct ticker exactly — update both to match
            # Update the Polymarket market's components to match Kalshi
            to_update = p_ticker_obj

        old_ticker = to_update["ticker"]
        to_update["mechanism"] = target_components.get("mechanism", to_update.get("mechanism"))
        to_update["threshold"] = target_components.get("threshold", to_update.get("threshold"))
        to_update["ticker"] = reassemble_ticker(to_update)

        if to_update["ticker"] != old_ticker:
            modified += 1
            to_update["match_source"] = "auto_embedding_gpt"
            to_update["match_confidence"] = verdict.get("cosine_similarity", 0)
            print(f"    Fixed: {old_ticker} -> {to_update['ticker']} ({to_update['market_id']})")

    return modified


def write_near_matches(overlapping, different, existing_near_matches=None):
    """Write OVERLAPPING and DIFFERENT pairs to near_matches.json."""
    entries = existing_near_matches or []
    existing_keys = {e["pair_key"] for e in entries}

    for verdict in overlapping + different:
        if verdict["pair_key"] in existing_keys:
            continue
        entries.append({
            "pair_key": verdict["pair_key"],
            "kalshi_market_id": verdict["kalshi_market_id"],
            "poly_market_id": verdict["poly_market_id"],
            "kalshi_ticker": verdict.get("kalshi_ticker", ""),
            "poly_ticker": verdict.get("poly_ticker", ""),
            "kalshi_question": verdict.get("kalshi_question", ""),
            "poly_question": verdict.get("poly_question", ""),
            "cosine_similarity": verdict.get("cosine_similarity", 0),
            "verdict": verdict["verdict"],
            "explanation": verdict.get("explanation", ""),
            "match_source": verdict.get("match_source", "auto_embedding_gpt"),
            "reviewed_at": verdict.get("reviewed_at", datetime.now().isoformat()),
        })

    return entries


def update_reviewed_pairs(reviewed_data, all_processed_pairs):
    """Add all processed pair keys to the reviewed pairs tracking."""
    pairs = reviewed_data.get("pairs", {})

    for pair in all_processed_pairs:
        pair_key = pair.get("pair_key", "")
        if not pair_key:
            continue
        pairs[pair_key] = {
            "reviewed_at": datetime.now().isoformat(),
            "bucket": pair.get("bucket", ""),
            "verdict": pair.get("verdict", ""),
            "action_taken": pair.get("action_taken", "none"),
            "match_source": pair.get("match_source", "auto"),
        }

    reviewed_data["pairs"] = pairs
    reviewed_data["updated_at"] = datetime.now().isoformat()
    return reviewed_data


def gate_with_predictionhunt(identical_verdicts, candidates_bucket_b, dry_run=False):
    """Run PredictionHunt validation on IDENTICAL verdicts before auto-applying.

    Returns:
        (approved, flagged)
        approved: list of verdicts that PH confirmed or had no data for (safe to apply)
        flagged: list of verdicts where PH disagreed (sent to pending review)
    """
    try:
        from predictionhunt_client import PredictionHuntClient, BudgetExhaustedError
    except ImportError:
        print("  PredictionHunt: Client not available, skipping validation gate")
        return identical_verdicts, []

    try:
        client = PredictionHuntClient()
    except ValueError:
        print("  PredictionHunt: No API key set, skipping validation gate")
        return identical_verdicts, []
    try:
        remaining, used, limit = client.check_budget()
    except BudgetExhaustedError:
        print(f"  PredictionHunt: Monthly budget exhausted, skipping validation gate")
        return identical_verdicts, []

    print(f"\n  PredictionHunt gate: {remaining} requests remaining ({used}/{limit} used)")

    if dry_run:
        print(f"  DRY RUN: Would query PH for {len(identical_verdicts)} IDENTICAL pairs")
        return identical_verdicts, []

    # Build lookup for candidate details
    candidate_lookup = {}
    for pair in candidates_bucket_b:
        candidate_lookup[pair["pair_key"]] = pair

    approved = []
    flagged = []
    skipped_budget = 0

    # Import validation helper
    from pipeline_validate_with_predictionhunt import classify_ph_response, load_pending_review

    for i, verdict in enumerate(identical_verdicts):
        try:
            client.check_budget()
        except BudgetExhaustedError:
            # Budget hit — approve remaining without PH check
            print(f"    Budget exhausted, approving remaining {len(identical_verdicts) - i} without PH check")
            approved.extend(identical_verdicts[i:])
            skipped_budget = len(identical_verdicts) - i
            break

        k_ticker = verdict["kalshi_market_id"]
        p_id = verdict["poly_market_id"]
        pair_info = candidate_lookup.get(verdict.get("pair_key", ""), {})

        ph_result = client.query_by_kalshi_ticker(k_ticker, pipeline="validate_embedding")
        status, ph_poly_ids = classify_ph_response(ph_result, p_id, client, our_kalshi_id=k_ticker)

        if status == "confirmed" or status == "no_match":
            approved.append(verdict)
            label = "confirmed" if status == "confirmed" else "no_data"
            print(f"    [{i+1}] {k_ticker} -> {label}, approved")
        elif status == "disagreed":
            flagged.append(verdict)
            print(f"    [{i+1}] {k_ticker} -> DISAGREED (PH poly: {ph_poly_ids}), flagged for review")

            # Add to pending review file
            existing_review = load_pending_review()
            existing_keys = {item.get("pair_key") for item in existing_review}
            pair_key = verdict.get("pair_key", f"{k_ticker}|{p_id}")
            if pair_key not in existing_keys:
                existing_review.append({
                    "source": "embedding_gpt",
                    "pair_key": pair_key,
                    "kalshi_market_id": k_ticker,
                    "poly_market_id": p_id,
                    "our_ticker": pair_info.get("kalshi_ticker", ""),
                    "kalshi_question": pair_info.get("kalshi_question", ""),
                    "poly_question": pair_info.get("poly_question", ""),
                    "cosine_similarity": verdict.get("cosine_similarity", 0),
                    "gpt_verdict": "IDENTICAL",
                    "ph_status": "disagreed",
                    "ph_matched_pm": ph_poly_ids,
                    "created_at": datetime.now().isoformat(),
                })
                atomic_write_json(PENDING_REVIEW_FILE,
                                  {"updated_at": datetime.now().isoformat(), "items": existing_review},
                                  indent=2, ensure_ascii=False)
        elif status == "error":
            # Real API error — approve anyway (don't block on PH errors)
            approved.append(verdict)
            error_detail = ph_result.get("error", "unknown")
            print(f"    [{i+1}] {k_ticker} -> PH error ({error_detail}), approved anyway")

    print(f"  PredictionHunt gate: {len(approved)} approved, {len(flagged)} flagged"
          + (f", {skipped_budget} skipped (budget)" if skipped_budget else ""))

    return approved, flagged


def main():
    parser = argparse.ArgumentParser(description="Apply cross-platform match fixes")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    parser.add_argument("--skip-ph", action="store_true", help="Skip PredictionHunt validation gate")
    args = parser.parse_args()

    print(f"[{datetime.now().isoformat()}] Updating cross-platform matches...")

    # Load data
    candidates = load_candidates()
    verdicts = load_verdicts()

    bucket_a = candidates.get("bucket_a", [])
    bucket_b = candidates.get("bucket_b", [])
    bucket_c = candidates.get("bucket_c", [])

    total = len(bucket_a) + len(bucket_b) + len(bucket_c)
    if total == 0:
        print("  No candidates to process.")
        sys.exit(0)

    print(f"  Candidates: A={len(bucket_a)}, B={len(bucket_b)}, C={len(bucket_c)}")
    print(f"  Verdicts: {len(verdicts)}")

    # Load tickers
    if not TICKERS_FILE.exists():
        print(f"  Tickers file not found: {TICKERS_FILE}")
        sys.exit(0)

    tickers_data = load_tickers_data()
    all_tickers = tickers_data["tickers"]
    tickers_by_id = {str(t["market_id"]): t for t in all_tickers}

    # Track all processed pairs for the reviewed log
    all_processed = []

    # 1. Handle Bucket A
    bucket_a_count = handle_bucket_a(bucket_a, tickers_by_id)
    for p in bucket_a:
        p["action_taken"] = "logged_as_bug"
    all_processed.extend(bucket_a)

    # 2. Handle Bucket B with verdicts
    identical_verdicts = []
    overlapping_verdicts = []
    different_verdicts = []
    error_verdicts = []

    for pair in bucket_b:
        v = verdicts.get(pair["pair_key"])
        if not v:
            continue  # Not yet compared — skip

        verdict_type = v.get("verdict", "")
        # Merge pair info into verdict
        v.update({
            "kalshi_ticker": pair.get("kalshi_ticker", v.get("kalshi_ticker", "")),
            "poly_ticker": pair.get("poly_ticker", v.get("poly_ticker", "")),
            "kalshi_question": pair.get("kalshi_question", v.get("kalshi_question", "")),
            "poly_question": pair.get("poly_question", v.get("poly_question", "")),
            "bucket": "B",
        })

        if verdict_type == "IDENTICAL":
            identical_verdicts.append(v)
        elif verdict_type == "OVERLAPPING":
            overlapping_verdicts.append(v)
        elif verdict_type == "DIFFERENT":
            different_verdicts.append(v)
        elif verdict_type == "ERROR":
            error_verdicts.append(v)

    print(f"\n  Bucket B verdicts: IDENTICAL={len(identical_verdicts)}, "
          f"OVERLAPPING={len(overlapping_verdicts)}, DIFFERENT={len(different_verdicts)}, "
          f"ERROR={len(error_verdicts)}")

    # 2b. PredictionHunt validation gate (unless --skip-ph)
    ph_flagged = []
    if identical_verdicts and not args.skip_ph:
        identical_verdicts, ph_flagged = gate_with_predictionhunt(
            identical_verdicts, bucket_b, dry_run=args.dry_run)

    # 3. Unify IDENTICAL tickers (only PH-approved ones)
    tickers_modified = 0
    if identical_verdicts:
        print(f"\n  Unifying {len(identical_verdicts)} IDENTICAL pairs...")
        tickers_modified = unify_identical_tickers(identical_verdicts, tickers_by_id, all_tickers)
        print(f"  Modified {tickers_modified} tickers")

    for v in identical_verdicts:
        v["action_taken"] = "unified_ticker" if tickers_modified > 0 else "no_change_needed"
    for v in overlapping_verdicts:
        v["action_taken"] = "near_match"
    for v in different_verdicts:
        v["action_taken"] = "different_event"
    for v in error_verdicts:
        v["action_taken"] = "error_skipped"

    all_processed.extend(identical_verdicts)
    all_processed.extend(overlapping_verdicts)
    all_processed.extend(different_verdicts)
    # Don't add error verdicts to reviewed — they'll be retried

    # Add Bucket C to reviewed (no action needed)
    for p in bucket_c:
        p["action_taken"] = "different_event"
    all_processed.extend(bucket_c)

    # 4. Write near_matches.json
    existing_near = []
    if NEAR_MATCHES_FILE.exists():
        with open(NEAR_MATCHES_FILE) as f:
            existing_data = json.load(f)
            existing_near = existing_data.get("matches", existing_data) if isinstance(existing_data, dict) else existing_data

    near_entries = write_near_matches(overlapping_verdicts, different_verdicts, existing_near)

    if args.dry_run:
        print(f"\n  DRY RUN: Would modify {tickers_modified} tickers")
        print(f"  DRY RUN: Would write {len(near_entries)} near matches")
        print(f"  DRY RUN: Would mark {len(all_processed)} pairs as reviewed")
        sys.exit(0)

    # 5. Save updated tickers
    if tickers_modified > 0:
        tickers_data["cross_platform_discovery_at"] = datetime.now().isoformat()
        tickers_data["cross_platform_fixes"] = tickers_modified
        atomic_write_json(TICKERS_FILE, tickers_data, indent=2)
        print(f"  Saved updated tickers to {TICKERS_FILE}")

    # 6. Save near matches
    near_output = {
        "updated_at": datetime.now().isoformat(),
        "matches": near_entries,
        "stats": {
            "overlapping": len([e for e in near_entries if e["verdict"] == "OVERLAPPING"]),
            "different": len([e for e in near_entries if e["verdict"] == "DIFFERENT"]),
        },
    }
    atomic_write_json(NEAR_MATCHES_FILE, near_output, indent=2)
    print(f"  Saved {len(near_entries)} near matches to {NEAR_MATCHES_FILE}")

    # PH-flagged verdicts don't get action_taken = unified, they stay unprocessed
    for v in ph_flagged:
        v["action_taken"] = "ph_flagged_for_review"
    all_processed.extend(ph_flagged)

    # 7. Update reviewed pairs
    reviewed_data = load_reviewed_pairs()
    reviewed_data = update_reviewed_pairs(reviewed_data, all_processed)
    atomic_write_json(REVIEWED_PAIRS_FILE, reviewed_data, indent=2)
    print(f"  Updated reviewed pairs: {len(reviewed_data['pairs'])} total")

    # Summary
    print(f"\n=== CROSS-PLATFORM UPDATE COMPLETE ===")
    print(f"  Bucket A (identical ticker bugs): {bucket_a_count}")
    print(f"  New matches fixed (IDENTICAL): {tickers_modified}")
    if ph_flagged:
        print(f"  PH flagged for review: {len(ph_flagged)}")
    print(f"  Near matches logged (OVERLAPPING): {len(overlapping_verdicts)}")
    print(f"  Different events (DIFFERENT): {len(different_verdicts)}")
    print(f"  Errors to retry: {len(error_verdicts)}")

    print(f"\n[{datetime.now().isoformat()}] Update complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
