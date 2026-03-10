#!/usr/bin/env python3
"""
Pipeline Step: Generate Ticker Corrections from Human Feedback Errors

Analyzes the match_accuracy_report.json to identify systematic error patterns
in GPT ticker generation, and produces ticker_corrections.json with
deterministic alias rules that postprocess_tickers.py applies on the NEXT run.

This creates a feedback loop:
  human labels → accuracy report → error patterns → correction rules → better tickers

Reads: data/match_accuracy_report.json, data/human_labels.json,
       data/tickers_postprocessed.json
Writes: data/ticker_corrections.json
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

# --- Paths ---
REPORT_FILE = DATA_DIR / "match_accuracy_report.json"
HUMAN_LABELS_FILE = DATA_DIR / "human_labels.json"
TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"
CORRECTIONS_FILE = DATA_DIR / "ticker_corrections.json"

# Minimum frequency to generate a correction rule, per type.
# Higher thresholds for riskier corrections (renaming people/offices).
MIN_FREQUENCY_DEFAULT = 2
MIN_FREQUENCY_BY_TYPE = {
    "mechanism_alias": 2,   # Low risk: resolution method (PROJECTED→CERTIFIED)
    "timeframe_alias": 2,   # Low risk: date formatting (2026 vs 2026_Q3)
    "target_alias":    4,   # Medium-high risk: could merge different offices
    "agent_alias":     5,   # High risk: could merge different people
}


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


def classify_disagreement(tickers: list) -> dict:
    """Classify what differs between two tickers.

    Returns dict with error_type and diff details.
    """
    if len(tickers) < 2:
        return {"error_type": "unknown", "details": {}}

    comp_a = parse_ticker_components(tickers[0])
    comp_b = parse_ticker_components(tickers[1])

    diffs = {}
    for field in ("agent", "action", "target", "mechanism", "threshold", "timeframe"):
        if comp_a.get(field) != comp_b.get(field):
            diffs[field] = (comp_a.get(field, ""), comp_b.get(field, ""))

    if not diffs:
        return {"error_type": "identical", "details": {}}

    diff_fields = set(diffs.keys())

    # Classify by which fields differ
    if diff_fields == {"mechanism"}:
        return {"error_type": "mechanism_mismatch", "details": diffs}
    elif diff_fields == {"threshold"}:
        return {"error_type": "threshold_mismatch", "details": diffs}
    elif diff_fields == {"agent"}:
        return {"error_type": "agent_variant", "details": diffs}
    elif diff_fields == {"target"}:
        return {"error_type": "target_variant", "details": diffs}
    elif diff_fields == {"timeframe"}:
        return {"error_type": "timeframe_mismatch", "details": diffs}
    elif diff_fields <= {"mechanism", "threshold"}:
        return {"error_type": "resolution_mismatch", "details": diffs}
    else:
        return {"error_type": "multi_field", "details": diffs}


def analyze_error_patterns(report: dict, min_frequency: int | dict = MIN_FREQUENCY_BY_TYPE) -> list:
    """Group false negatives by error type and extract correction rules.

    Returns list of correction dicts sorted by frequency.
    """
    disagreements = report.get("disagreements", [])
    if not disagreements:
        return []

    # Only analyze matching disagreements (not category ones)
    matching_errors = [
        d for d in disagreements
        if d.get("human_judgment") == "same event" and d.get("pipeline_tickers")
    ]

    if not matching_errors:
        return []

    # Classify each error
    error_classifications = []
    for error in matching_errors:
        tickers = error.get("pipeline_tickers", [])
        classification = classify_disagreement(tickers)
        classification["label_id"] = error.get("label_id", "")
        classification["tickers"] = tickers
        error_classifications.append(classification)

    # Resolve min_frequency per correction type
    def _min_freq(correction_type: str) -> int:
        if isinstance(min_frequency, dict):
            return min_frequency.get(correction_type, MIN_FREQUENCY_DEFAULT)
        return min_frequency

    # Count patterns
    corrections = []

    # Group by error type
    by_type = {}
    for ec in error_classifications:
        et = ec["error_type"]
        by_type.setdefault(et, []).append(ec)

    # Generate correction rules for each type
    for error_type, errors in by_type.items():
        if error_type == "mechanism_mismatch":
            # Count specific mechanism pairs
            mech_pairs = Counter()
            for e in errors:
                vals = e["details"].get("mechanism", ("", ""))
                # Sort the pair for consistency
                pair = tuple(sorted(vals))
                mech_pairs[pair] += 1

            for (from_val, to_val), count in mech_pairs.items():
                if count >= _min_freq("mechanism_alias"):
                    corrections.append({
                        "type": "mechanism_alias",
                        "from": from_val,
                        "to": to_val,
                        "frequency": count,
                        "source_labels": [e["label_id"] for e in errors
                                          if tuple(sorted(e["details"].get("mechanism", ()))) == (from_val, to_val)],
                    })

        elif error_type == "agent_variant":
            agent_pairs = Counter()
            for e in errors:
                vals = e["details"].get("agent", ("", ""))
                pair = tuple(sorted(vals))
                agent_pairs[pair] += 1

            for (from_val, to_val), count in agent_pairs.items():
                if count >= _min_freq("agent_alias"):
                    corrections.append({
                        "type": "agent_alias",
                        "from": from_val,
                        "to": to_val,
                        "frequency": count,
                        "source_labels": [e["label_id"] for e in errors
                                          if tuple(sorted(e["details"].get("agent", ()))) == (from_val, to_val)],
                    })

        elif error_type == "target_variant":
            target_pairs = Counter()
            for e in errors:
                vals = e["details"].get("target", ("", ""))
                pair = tuple(sorted(vals))
                target_pairs[pair] += 1

            for (from_val, to_val), count in target_pairs.items():
                if count >= _min_freq("target_alias"):
                    corrections.append({
                        "type": "target_alias",
                        "from": from_val,
                        "to": to_val,
                        "frequency": count,
                        "source_labels": [e["label_id"] for e in errors
                                          if tuple(sorted(e["details"].get("target", ()))) == (from_val, to_val)],
                    })

        elif error_type == "timeframe_mismatch":
            tf_pairs = Counter()
            for e in errors:
                vals = e["details"].get("timeframe", ("", ""))
                pair = tuple(sorted(vals))
                tf_pairs[pair] += 1

            for (from_val, to_val), count in tf_pairs.items():
                if count >= _min_freq("timeframe_alias"):
                    corrections.append({
                        "type": "timeframe_alias",
                        "from": from_val,
                        "to": to_val,
                        "frequency": count,
                        "source_labels": [e["label_id"] for e in errors
                                          if tuple(sorted(e["details"].get("timeframe", ()))) == (from_val, to_val)],
                    })

    # Sort by frequency descending
    corrections.sort(key=lambda x: x["frequency"], reverse=True)
    return corrections


def generate_corrections(report: dict, min_frequency: int | dict = MIN_FREQUENCY_BY_TYPE, batch_id: str = None) -> dict:
    """Generate the corrections file from the accuracy report.

    Returns the full corrections dict to write.
    """
    corrections = analyze_error_patterns(report, min_frequency=min_frequency)
    label_count = report.get("sample_size", 0)

    # Record the effective thresholds used
    if isinstance(min_frequency, dict):
        freq_record = min_frequency
    else:
        freq_record = {k: min_frequency for k in MIN_FREQUENCY_BY_TYPE}

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "batch_id": batch_id,
        "source_label_count": label_count,
        "correction_count": len(corrections),
        "min_frequency": freq_record,
        "corrections": corrections,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate ticker corrections from error patterns")
    parser.add_argument("--min-frequency", type=int, default=None,
                        help="Uniform minimum frequency (overrides per-type defaults)")
    parser.add_argument("--min-freq-mechanism", type=int, default=None,
                        help=f"Min frequency for mechanism_alias (default: {MIN_FREQUENCY_BY_TYPE['mechanism_alias']})")
    parser.add_argument("--min-freq-agent", type=int, default=None,
                        help=f"Min frequency for agent_alias (default: {MIN_FREQUENCY_BY_TYPE['agent_alias']})")
    parser.add_argument("--min-freq-target", type=int, default=None,
                        help=f"Min frequency for target_alias (default: {MIN_FREQUENCY_BY_TYPE['target_alias']})")
    parser.add_argument("--min-freq-timeframe", type=int, default=None,
                        help=f"Min frequency for timeframe_alias (default: {MIN_FREQUENCY_BY_TYPE['timeframe_alias']})")
    parser.add_argument("--dry-run", action="store_true", help="Print corrections without writing")
    parser.add_argument("--batch-id", type=str, default=None,
                        help="Batch ID for traceability (auto-generated if not provided)")
    args = parser.parse_args()

    batch_id = args.batch_id or datetime.now(timezone.utc).strftime("batch_%Y%m%d_%H%M%S")

    # Build per-type frequency config
    if args.min_frequency is not None:
        # Uniform override: apply same value to all types
        min_freq = {k: args.min_frequency for k in MIN_FREQUENCY_BY_TYPE}
    else:
        min_freq = dict(MIN_FREQUENCY_BY_TYPE)

    # Per-type CLI overrides (take precedence)
    if args.min_freq_mechanism is not None:
        min_freq["mechanism_alias"] = args.min_freq_mechanism
    if args.min_freq_agent is not None:
        min_freq["agent_alias"] = args.min_freq_agent
    if args.min_freq_target is not None:
        min_freq["target_alias"] = args.min_freq_target
    if args.min_freq_timeframe is not None:
        min_freq["timeframe_alias"] = args.min_freq_timeframe

    now = datetime.now(timezone.utc).isoformat()
    print(f"[{now}] Generating ticker corrections (batch: {batch_id})...")
    print(f"  Min frequency thresholds: {min_freq}")

    if not REPORT_FILE.exists():
        print("  No accuracy report found. Run pipeline_evaluate_matches.py first.")
        return

    with open(REPORT_FILE) as f:
        report = json.load(f)

    fn_count = report.get("matching", {}).get("false_negatives", 0)
    print(f"  False negatives in report: {fn_count}")

    result = generate_corrections(report, min_frequency=min_freq, batch_id=batch_id)

    print(f"  Corrections generated: {result['correction_count']}")

    if result["corrections"]:
        for corr in result["corrections"]:
            print(f"    {corr['type']}: {corr['from']} → {corr['to']} "
                  f"(freq: {corr['frequency']}, labels: {len(corr['source_labels'])})")

    if args.dry_run:
        print("\n  [DRY RUN] Not writing corrections.")
        return

    atomic_write_json(CORRECTIONS_FILE, result, indent=2)
    print(f"  Wrote {CORRECTIONS_FILE.name}")
    print(f"  These corrections will take effect on the NEXT pipeline run "
          f"when postprocess_tickers.py reads them.")


if __name__ == "__main__":
    main()
