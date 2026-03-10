#!/usr/bin/env python3
"""
Pipeline Step: Apply Human Labels as Final Overrides

Reads human_labels.json and applies pending labels to pipeline data files.
Runs AFTER postprocess_tickers.py so human labels are the final word.

Validation before application:
- same_event_same_rules: only unify if tickers differ in mechanism/threshold only
  (if agent/action/target differ, mark as needs_review)
- wrong_category: only apply if description contains a valid category name
- not_political: always safe to apply

Reads: data/human_labels.json, data/tickers_postprocessed.json,
       data/cross_platform_reviewed_pairs.json, data/near_matches.json,
       data/combined_political_markets_with_electoral_details_UPDATED.csv
Writes: Same files (modified), data/human_labels.json (status updates)
"""

import sys
import csv
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

# --- Paths ---
HUMAN_LABELS_FILE = DATA_DIR / "human_labels.json"
TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"
REVIEWED_PAIRS_FILE = DATA_DIR / "cross_platform_reviewed_pairs.json"
NEAR_MATCHES_FILE = DATA_DIR / "near_matches.json"
MASTER_CSV_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"

# Valid political categories
VALID_CATEGORIES = {
    "1. ELECTORAL", "2. MONETARY_POLICY", "3. LEGISLATIVE",
    "4. APPOINTMENTS", "5. REGULATORY", "6. INTERNATIONAL",
    "7. JUDICIAL", "8. MILITARY_SECURITY", "9. CRISIS_EMERGENCY",
    "10. GOVERNMENT_OPERATIONS", "11. PARTY_POLITICS",
    "12. STATE_LOCAL", "13. TIMING_EVENTS", "14. POLLING_APPROVAL",
    "15. POLITICAL_SPEECH",
}

# Category name aliases for matching user descriptions
CATEGORY_ALIASES = {}
for cat in VALID_CATEGORIES:
    # "1. ELECTORAL" -> aliases: "electoral", "1. electoral"
    name = cat.split(". ", 1)[1] if ". " in cat else cat
    CATEGORY_ALIASES[name.lower()] = cat
    CATEGORY_ALIASES[cat.lower()] = cat
    # Also allow without underscores: "monetary policy"
    CATEGORY_ALIASES[name.lower().replace("_", " ")] = cat


def parse_ticker_components(ticker_str: str) -> dict:
    """Parse 'BWR-AGENT-ACTION-TARGET-MECHANISM-THRESHOLD-TIMEFRAME' into dict."""
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


def reassemble_ticker(ticker: dict) -> str:
    """Reassemble ticker string from components."""
    agent = ticker.get("agent", "UNKNOWN")
    action = ticker.get("action", "UNKNOWN")
    target = ticker.get("target", "UNKNOWN")
    mechanism = ticker.get("mechanism", "STD")
    threshold = ticker.get("threshold", "ANY")
    timeframe = ticker.get("timeframe", "UNKNOWN")
    return f"BWR-{agent}-{action}-{target}-{mechanism}-{threshold}-{timeframe}"


def make_pair_key(id_a: str, id_b: str) -> str:
    """Canonical pair key: sorted alphabetically, pipe-delimited."""
    ids = sorted([str(id_a), str(id_b)])
    return f"{ids[0]}|{ids[1]}"


def load_json(path: Path, default=None):
    """Load JSON file or return default."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return default if default is not None else {}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def validate_same_event_pair(ticker_a: dict, ticker_b: dict) -> tuple:
    """Validate whether two tickers can be safely unified.

    Returns (can_unify: bool, reason: str).

    Safe to unify if tickers differ ONLY in mechanism and/or threshold.
    NOT safe if agent, action, target, or timeframe differ —
    this means GPT assigned fundamentally different tickers and
    automatic unification could be wrong.
    """
    comp_a = parse_ticker_components(ticker_a.get("ticker", ""))
    comp_b = parse_ticker_components(ticker_b.get("ticker", ""))

    # Already the same ticker — nothing to do
    if ticker_a.get("ticker") == ticker_b.get("ticker"):
        return True, "already_matched"

    # Check core identity
    agent_match = comp_a["agent"] == comp_b["agent"]
    action_match = comp_a["action"] == comp_b["action"]
    target_match = comp_a["target"] == comp_b["target"]
    timeframe_match = comp_a["timeframe"] == comp_b["timeframe"]

    if agent_match and action_match and target_match and timeframe_match:
        diffs = []
        if comp_a["mechanism"] != comp_b["mechanism"]:
            diffs.append(f"mechanism: {comp_a['mechanism']} vs {comp_b['mechanism']}")
        if comp_a["threshold"] != comp_b["threshold"]:
            diffs.append(f"threshold: {comp_a['threshold']} vs {comp_b['threshold']}")
        return True, f"safe_unify ({', '.join(diffs)})"

    # Core components differ — not safe to auto-unify
    diffs = []
    if not agent_match:
        diffs.append(f"agent: {comp_a['agent']} vs {comp_b['agent']}")
    if not action_match:
        diffs.append(f"action: {comp_a['action']} vs {comp_b['action']}")
    if not target_match:
        diffs.append(f"target: {comp_a['target']} vs {comp_b['target']}")
    if not timeframe_match:
        diffs.append(f"timeframe: {comp_a['timeframe']} vs {comp_b['timeframe']}")

    return False, f"core_differs ({', '.join(diffs)})"


def choose_canonical_ticker(ticker_a: dict, ticker_b: dict) -> dict:
    """Choose which ticker to use as canonical when unifying.

    Prefers Kalshi's ticker (convention: Kalshi has more explicit resolution rules).
    If neither is Kalshi, picks the first alphabetically for determinism.
    """
    if ticker_a.get("platform") == "Kalshi":
        return ticker_a
    if ticker_b.get("platform") == "Kalshi":
        return ticker_b
    # Neither is Kalshi — pick first alphabetically
    if ticker_a.get("ticker", "") <= ticker_b.get("ticker", ""):
        return ticker_a
    return ticker_b


def apply_same_event_same_rules(labels: list, tickers_by_id: dict) -> tuple:
    """Apply same-event-same-rules labels by unifying tickers.

    Returns (applied_count, results_list).
    """
    applied = 0
    results = []

    for label in labels:
        if label.get("label_type") != "same_event_same_rules":
            continue
        if label.get("status") != "pending" or label.get("applied_at"):
            continue

        market_ids = label.get("market_ids", [])
        if len(market_ids) < 2:
            label["status"] = "needs_review"
            label["applied_action"] = "skipped_single_market"
            results.append(("skip", label["label_id"], "single market"))
            continue

        # Look up ticker objects
        ticker_objs = []
        for mid in market_ids:
            obj = tickers_by_id.get(mid)
            if obj:
                ticker_objs.append(obj)

        if len(ticker_objs) < 2:
            label["status"] = "needs_review"
            label["applied_action"] = "skipped_missing_tickers"
            results.append(("skip", label["label_id"], "missing tickers"))
            continue

        # Validate the first pair (for N>2, validate pairwise from first)
        can_unify, reason = validate_same_event_pair(ticker_objs[0], ticker_objs[1])

        if reason == "already_matched":
            label["status"] = "applied"
            label["applied_at"] = now_iso()
            label["applied_action"] = "already_matched"
            results.append(("already", label["label_id"], "already matched"))
            continue

        if not can_unify:
            label["status"] = "needs_review"
            label["applied_action"] = f"validation_failed: {reason}"
            results.append(("needs_review", label["label_id"], reason))
            continue

        # Choose canonical ticker and unify
        canonical = choose_canonical_ticker(ticker_objs[0], ticker_objs[1])
        canonical_ticker_str = canonical["ticker"]
        canonical_components = parse_ticker_components(canonical_ticker_str)

        for obj in ticker_objs:
            if obj["ticker"] != canonical_ticker_str:
                old_ticker = obj["ticker"]
                obj["mechanism"] = canonical_components["mechanism"]
                obj["threshold"] = canonical_components["threshold"]
                obj["ticker"] = reassemble_ticker(obj)
                obj["match_source"] = "human"
                obj["human_label_id"] = label["label_id"]
                results.append(("unified", label["label_id"],
                                f"{old_ticker} -> {obj['ticker']}"))

        label["status"] = "applied"
        label["applied_at"] = now_iso()
        label["applied_action"] = "unified_tickers"
        applied += 1

    return applied, results


def apply_same_event_different_rules(labels: list, tickers_by_id: dict) -> tuple:
    """Apply same-event-different-rules labels by adding to near_matches.

    Returns (entries_to_add, results).
    """
    entries = []
    results = []

    for label in labels:
        if label.get("label_type") != "same_event_different_rules":
            continue
        if label.get("status") != "pending" or label.get("applied_at"):
            continue

        market_ids = label.get("market_ids", [])
        if len(market_ids) < 2:
            label["status"] = "needs_review"
            results.append(("skip", label["label_id"], "single market"))
            continue

        # Create near-match entries for cross-platform pairs
        for i in range(len(market_ids)):
            for j in range(i + 1, len(market_ids)):
                mid_a, mid_b = market_ids[i], market_ids[j]
                t_a = tickers_by_id.get(mid_a, {})
                t_b = tickers_by_id.get(mid_b, {})

                # Only create near-match for cross-platform pairs
                if t_a.get("platform") == t_b.get("platform"):
                    continue

                pair_key = make_pair_key(mid_a, mid_b)
                entries.append({
                    "pair_key": pair_key,
                    "kalshi_market_id": mid_a if t_a.get("platform") == "Kalshi" else mid_b,
                    "poly_market_id": mid_b if t_b.get("platform") != "Kalshi" else mid_a,
                    "kalshi_ticker": t_a.get("ticker", "") if t_a.get("platform") == "Kalshi" else t_b.get("ticker", ""),
                    "poly_ticker": t_b.get("ticker", "") if t_b.get("platform") != "Kalshi" else t_a.get("ticker", ""),
                    "kalshi_question": t_a.get("original_question", "") if t_a.get("platform") == "Kalshi" else t_b.get("original_question", ""),
                    "poly_question": t_b.get("original_question", "") if t_b.get("platform") != "Kalshi" else t_a.get("original_question", ""),
                    "verdict": "OVERLAPPING",
                    "match_source": "human",
                    "human_label_id": label["label_id"],
                    "explanation": label.get("description", ""),
                    "reviewed_at": now_iso(),
                })

        label["status"] = "applied"
        label["applied_at"] = now_iso()
        label["applied_action"] = "added_near_match"
        results.append(("near_match", label["label_id"], f"{len(market_ids)} markets"))

    return entries, results


def apply_different_event(labels: list, tickers_by_id: dict) -> tuple:
    """Apply different-event labels by breaking incorrect matches.

    If two markets have the same ticker but human says different event,
    append '_SPLIT' suffix to the second market's ticker to break the match.

    Returns (modified_count, pair_entries, results).
    """
    modified = 0
    pair_entries = []
    results = []

    for label in labels:
        if label.get("label_type") != "different_event":
            continue
        if label.get("status") != "pending" or label.get("applied_at"):
            continue

        market_ids = label.get("market_ids", [])
        if len(market_ids) < 2:
            label["status"] = "needs_review"
            results.append(("skip", label["label_id"], "single market"))
            continue

        # Check if markets actually share a ticker
        tickers_found = [tickers_by_id.get(mid, {}).get("ticker") for mid in market_ids]
        unique = set(t for t in tickers_found if t)

        if len(unique) > 1:
            # Already different tickers — no action needed
            label["status"] = "applied"
            label["applied_at"] = now_iso()
            label["applied_action"] = "already_different"
            results.append(("already", label["label_id"], "already different tickers"))
        elif len(unique) == 1:
            # Same ticker — break the match on the second market
            second_mid = market_ids[1]
            obj = tickers_by_id.get(second_mid)
            if obj:
                old_ticker = obj["ticker"]
                obj["ticker"] = old_ticker + "_SPLIT"
                obj["match_source"] = "human"
                obj["human_label_id"] = label["label_id"]
                modified += 1
                results.append(("split", label["label_id"],
                                f"{old_ticker} -> {obj['ticker']}"))

            label["status"] = "applied"
            label["applied_at"] = now_iso()
            label["applied_action"] = "split_ticker"
        else:
            label["status"] = "needs_review"
            label["applied_action"] = "no_tickers_found"
            results.append(("skip", label["label_id"], "no tickers found"))
            continue

        # Add to reviewed pairs for all cross-platform combinations
        for i in range(len(market_ids)):
            for j in range(i + 1, len(market_ids)):
                pair_entries.append({
                    "pair_key": make_pair_key(market_ids[i], market_ids[j]),
                    "verdict": "DIFFERENT",
                    "match_source": "human",
                })

    return modified, pair_entries, results


def apply_not_political(labels: list, master_csv_path: Path) -> tuple:
    """Mark markets as NOT_POLITICAL in the master CSV.

    Returns (modified_count, results).
    """
    # Collect market IDs to mark
    market_ids_to_mark = set()
    labels_to_update = []

    for label in labels:
        if label.get("label_type") != "not_political":
            continue
        if label.get("status") != "pending" or label.get("applied_at"):
            continue
        for mid in label.get("market_ids", []):
            market_ids_to_mark.add(mid)
        labels_to_update.append(label)

    if not market_ids_to_mark or not master_csv_path.exists():
        return 0, []

    # Read and modify CSV
    rows = []
    modified = 0
    with open(master_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row.get("market_id", "").strip() in market_ids_to_mark:
                if row.get("political_category") != "16. NOT_POLITICAL":
                    row["political_category"] = "16. NOT_POLITICAL"
                    modified += 1
            rows.append(row)

    if modified > 0:
        with open(master_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    results = []
    for label in labels_to_update:
        label["status"] = "applied"
        label["applied_at"] = now_iso()
        label["applied_action"] = "marked_not_political"
        results.append(("not_political", label["label_id"],
                        f"{len(label.get('market_ids', []))} markets"))

    return modified, results


def detect_category_in_description(description: str) -> str:
    """Try to extract a valid category from a label description.

    Returns canonical category string or empty string if not found.
    """
    desc_lower = description.lower()
    for alias, canonical in CATEGORY_ALIASES.items():
        if alias in desc_lower:
            return canonical
    return ""


def apply_wrong_category(labels: list, master_csv_path: Path) -> tuple:
    """Recategorize markets based on human labels.

    Only applies if the description contains a recognizable category name.
    Otherwise marks as needs_review.

    Returns (modified_count, results).
    """
    changes = {}  # market_id -> new_category
    labels_applied = []
    labels_review = []
    results = []

    for label in labels:
        if label.get("label_type") != "wrong_category":
            continue
        if label.get("status") != "pending" or label.get("applied_at"):
            continue

        new_cat = detect_category_in_description(label.get("description", ""))
        if not new_cat:
            label["status"] = "needs_review"
            label["applied_action"] = "no_valid_category_in_description"
            results.append(("needs_review", label["label_id"],
                            "no valid category in description"))
            labels_review.append(label)
            continue

        for mid in label.get("market_ids", []):
            changes[mid] = new_cat
        labels_applied.append(label)

    if not changes or not master_csv_path.exists():
        return 0, results

    # Read and modify CSV
    rows = []
    modified = 0
    with open(master_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            mid = row.get("market_id", "").strip()
            if mid in changes and row.get("political_category") != changes[mid]:
                row["political_category"] = changes[mid]
                modified += 1
            rows.append(row)

    if modified > 0:
        with open(master_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    for label in labels_applied:
        label["status"] = "applied"
        label["applied_at"] = now_iso()
        label["applied_action"] = "recategorized"
        results.append(("recategorized", label["label_id"],
                        f"{len(label.get('market_ids', []))} markets"))

    return modified, results


def add_to_reviewed_pairs(labels: list, reviewed_data: dict) -> int:
    """Add human-labeled pairs to cross_platform_reviewed_pairs.json.

    This prevents the embedding-based discovery from re-surfacing
    pairs that humans have already judged.
    """
    pairs = reviewed_data.get("pairs", {})
    added = 0

    for label in labels:
        if label.get("status") not in ("applied", "needs_review"):
            continue

        market_ids = label.get("market_ids", [])
        if len(market_ids) < 2:
            continue

        for i in range(len(market_ids)):
            for j in range(i + 1, len(market_ids)):
                pair_key = make_pair_key(market_ids[i], market_ids[j])
                if pair_key not in pairs:
                    pairs[pair_key] = {
                        "reviewed_at": now_iso(),
                        "bucket": "human",
                        "verdict": label.get("label_type", "other"),
                        "action_taken": label.get("applied_action", "human_reviewed"),
                        "match_source": "human",
                    }
                    added += 1

    reviewed_data["pairs"] = pairs
    reviewed_data["updated_at"] = now_iso()
    return added


def main():
    parser = argparse.ArgumentParser(description="Apply human labels as pipeline overrides")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    args = parser.parse_args()

    print(f"[{now_iso()}] Applying human labels...")

    # Load data
    human_data = load_json(HUMAN_LABELS_FILE, {"labels": []})
    labels = human_data.get("labels", [])
    pending = [l for l in labels if l.get("status") == "pending" and not l.get("applied_at")]

    if not pending:
        print("  No pending labels to apply.")
        return

    print(f"  Total labels: {len(labels)}, pending: {len(pending)}")

    # Load tickers
    tickers_data = load_json(TICKERS_FILE, {"tickers": []})
    tickers_by_id = {}
    for t in tickers_data.get("tickers", []):
        tickers_by_id[str(t.get("market_id", ""))] = t

    print(f"  Tickers loaded: {len(tickers_by_id)}")

    all_results = []

    # 1. Apply same-event-same-rules
    unified, results = apply_same_event_same_rules(labels, tickers_by_id)
    all_results.extend(results)
    print(f"  Same-event-same-rules: {unified} unified")

    # 2. Apply same-event-different-rules
    near_entries, results = apply_same_event_different_rules(labels, tickers_by_id)
    all_results.extend(results)
    print(f"  Same-event-different-rules: {len(near_entries)} near-match entries")

    # 3. Apply different-event
    split_count, diff_pair_entries, results = apply_different_event(labels, tickers_by_id)
    all_results.extend(results)
    print(f"  Different-event: {split_count} tickers split")

    # 4. Apply not-political
    np_count, results = apply_not_political(labels, MASTER_CSV_FILE)
    all_results.extend(results)
    print(f"  Not-political: {np_count} markets updated")

    # 5. Apply wrong-category
    wc_count, results = apply_wrong_category(labels, MASTER_CSV_FILE)
    all_results.extend(results)
    print(f"  Wrong-category: {wc_count} markets recategorized")

    # 6. Add to reviewed pairs
    reviewed_data = load_json(REVIEWED_PAIRS_FILE, {"updated_at": None, "pairs": {}})
    pairs_added = add_to_reviewed_pairs(labels, reviewed_data)
    print(f"  Reviewed pairs added: {pairs_added}")

    # Print all results
    if all_results:
        print(f"\n  Actions taken:")
        for action, label_id, detail in all_results:
            print(f"    [{action}] {label_id}: {detail}")

    if args.dry_run:
        print("\n  [DRY RUN] Not writing changes.")
        return

    # Write updated files
    # Tickers
    atomic_write_json(TICKERS_FILE, tickers_data, indent=2)
    print(f"  Wrote {TICKERS_FILE.name}")

    # Near matches
    existing_near = load_json(NEAR_MATCHES_FILE, [])
    if isinstance(existing_near, dict):
        existing_near = existing_near.get("entries", [])
    existing_keys = {e.get("pair_key") for e in existing_near}
    for entry in near_entries:
        if entry["pair_key"] not in existing_keys:
            existing_near.append(entry)
    if near_entries:
        atomic_write_json(NEAR_MATCHES_FILE, existing_near, indent=2)
        print(f"  Wrote {NEAR_MATCHES_FILE.name}")

    # Reviewed pairs
    if pairs_added > 0:
        atomic_write_json(REVIEWED_PAIRS_FILE, reviewed_data, indent=2)
        print(f"  Wrote {REVIEWED_PAIRS_FILE.name}")

    # Human labels (status updates)
    human_data["updated_at"] = now_iso()
    atomic_write_json(HUMAN_LABELS_FILE, human_data, indent=2, ensure_ascii=False)
    print(f"  Wrote {HUMAN_LABELS_FILE.name}")


if __name__ == "__main__":
    main()
