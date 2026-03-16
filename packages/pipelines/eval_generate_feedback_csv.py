#!/usr/bin/env python3
"""
Convert mapped equivalents into a feedback CSV for pipeline testing.

Input:  data/equivalents_eval/uuid_to_market_id.json
        data/equivalents_eval/equivalents_polymarket_kalshi/equivalents_shared_data.bson
Output: data/equivalents_eval/equivalents_feedback.csv
"""

import sys
import json
import csv
import bson
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR

BSON_FILE = DATA_DIR / "equivalents_eval" / "equivalents_polymarket_kalshi" / "equivalents_shared_data.bson"
MAPPING_FILE = DATA_DIR / "equivalents_eval" / "uuid_to_market_id.json"
OUTPUT_FILE = DATA_DIR / "equivalents_eval" / "equivalents_feedback.csv"
HUMAN_LABELS_FILE = DATA_DIR / "human_labels.json"

# Distance threshold: pairs above this get labeled same-event:different-rules
DIFFERENT_RULES_DISTANCE = 0.6


def main():
    print("Generating feedback CSV from equivalents...")

    if not MAPPING_FILE.exists():
        print("  ERROR: Run eval_map_equivalents.py first to create uuid_to_market_id.json")
        return

    if not BSON_FILE.exists():
        print("  ERROR: BSON file not found")
        return

    # Load mapping
    with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
        mapping_data = json.load(f)
    mapping = mapping_data.get("mapping", {})
    print(f"  Loaded {len(mapping)} UUID mappings")

    # Load equivalents
    with open(BSON_FILE, 'rb') as f:
        docs = bson.decode_all(f.read())
    print(f"  Loaded {len(docs)} equivalent groups")

    # Find the last ingested timestamp to generate timestamps after it
    base_timestamp = datetime(2026, 3, 16, tzinfo=timezone.utc)
    if HUMAN_LABELS_FILE.exists():
        with open(HUMAN_LABELS_FILE, 'r', encoding='utf-8') as f:
            labels_data = json.load(f)
        last_ts = labels_data.get("last_ingested_timestamp", "")
        if last_ts:
            try:
                base_timestamp = datetime.fromisoformat(last_ts.replace('Z', '+00:00')) + timedelta(seconds=1)
            except ValueError:
                pass
    print(f"  Timestamps will start after: {base_timestamp.isoformat()}")

    rows = []
    skipped_no_mapping = 0
    skipped_same_platform = 0
    row_idx = 0

    for doc in docs:
        markets = doc.get("markets", [])
        relations = doc.get("relations", [])

        # Collect mapped markets by platform
        kalshi_markets = []
        poly_markets = []

        for m in markets:
            uuid = m.get("uuid", "")
            if uuid not in mapping:
                continue
            entry = mapping[uuid]
            market_info = {
                "uuid": uuid,
                "market_id": entry["market_id"],
                "platform": entry["platform"],
                "document": m.get("document", ""),
            }
            if entry["platform"] == "kalshi":
                kalshi_markets.append(market_info)
            else:
                poly_markets.append(market_info)

        # Generate cross-platform pairs
        if not kalshi_markets or not poly_markets:
            if len(kalshi_markets) + len(poly_markets) >= 2:
                skipped_same_platform += 1
            else:
                skipped_no_mapping += 1
            continue

        # Find the distance for this group (use min distance across relations)
        min_distance = 1.0
        for r in relations:
            d = r.get("distance", 1.0)
            if isinstance(d, str):
                d = float(d)
            min_distance = min(min_distance, d)

        # Determine feedback type based on distance
        if min_distance > DIFFERENT_RULES_DISTANCE:
            feedback_type = "same-event:different-rules"
        else:
            feedback_type = "same-event:same-rules"

        # Generate one row per cross-platform pair
        for km in kalshi_markets:
            for pm in poly_markets:
                row_idx += 1
                timestamp = base_timestamp + timedelta(seconds=row_idx)

                # Build description from both documents
                k_title = km["document"].split('. ')[0] if km["document"] else km["market_id"]
                p_title = pm["document"].split('. ')[0] if pm["document"] else pm["market_id"]
                description = f"Equivalent: [{k_title}] ≈ [{p_title}] (dist={min_distance:.3f})"

                # Markets JSON
                markets_json = json.dumps([
                    {
                        "key": km["market_id"],
                        "label": k_title[:200],
                        "platform": "Kalshi",
                        "category": "Electoral",
                    },
                    {
                        "key": pm["market_id"],
                        "label": p_title[:200],
                        "platform": "Polymarket",
                        "category": "Electoral",
                    },
                ])

                rows.append({
                    "Timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "Feedback Type": feedback_type,
                    "Description": description,
                    "Market Count": 2,
                    "Markets (JSON)": markets_json,
                })

    print(f"\nResults:")
    print(f"  Feedback rows generated: {len(rows)}")
    print(f"  Skipped (no mapping): {skipped_no_mapping}")
    print(f"  Skipped (same platform only): {skipped_same_platform}")

    same_rules = sum(1 for r in rows if "same-rules" in r["Feedback Type"])
    diff_rules = sum(1 for r in rows if "different-rules" in r["Feedback Type"])
    print(f"  same-event:same-rules: {same_rules}")
    print(f"  same-event:different-rules: {diff_rules}")

    if not rows:
        print("  No rows to write!")
        return

    # Write CSV
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Timestamp", "Feedback Type", "Description", "Market Count", "Markets (JSON)"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  Wrote {OUTPUT_FILE}")
    print(f"  Next: python pipeline_ingest_feedback.py --csv-file {OUTPUT_FILE} --dry-run")


if __name__ == "__main__":
    main()
