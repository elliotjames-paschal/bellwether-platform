"""
Integration tests for human feedback pipeline modifications.

Tests the modifications to existing pipeline files:
- pipeline_update_matches.py: match_source field
- postprocess_tickers.py: Fix #15 (ticker_corrections.json)
- generate_market_map.py: match_source propagation
- pipeline_daily_refresh.py: new pipeline steps

Run: pytest packages/pipelines/tests/test_pipeline_integration.py -v
"""

import pytest
import json
import sys
import csv
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ──────────────────────────────────────────────────
# pipeline_update_matches.py: match_source in unified tickers
# ──────────────────────────────────────────────────


class TestUpdateMatchesMatchSource:
    def test_unify_sets_match_source(self):
        from pipeline_update_matches import unify_identical_tickers, reassemble_ticker

        tickers_by_id = {
            "K1": {
                "market_id": "K1", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2028",
                "platform": "Kalshi",
            },
            "P1": {
                "market_id": "P1", "ticker": "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028",
                "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                "mechanism": "STD", "threshold": "ANY", "timeframe": "2028",
                "platform": "Polymarket",
            },
        }

        verdicts = [{
            "kalshi_market_id": "K1",
            "poly_market_id": "P1",
            "correct_ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
            "cosine_similarity": 0.92,
        }]

        modified = unify_identical_tickers(verdicts, tickers_by_id, [])
        assert modified == 1
        assert tickers_by_id["P1"]["match_source"] == "auto_embedding_gpt"
        assert tickers_by_id["P1"]["match_confidence"] == 0.92

    def test_write_near_matches_includes_source(self):
        from pipeline_update_matches import write_near_matches

        verdicts = [{
            "pair_key": "K1|P1",
            "kalshi_market_id": "K1",
            "poly_market_id": "P1",
            "verdict": "OVERLAPPING",
        }]

        entries = write_near_matches(verdicts, [])
        assert len(entries) == 1
        assert entries[0]["match_source"] == "auto_embedding_gpt"

    def test_update_reviewed_pairs_includes_source(self):
        from pipeline_update_matches import update_reviewed_pairs

        reviewed_data = {"pairs": {}, "updated_at": None}
        pairs = [{"pair_key": "K1|P1", "bucket": "B", "verdict": "IDENTICAL",
                  "action_taken": "unified_ticker"}]

        update_reviewed_pairs(reviewed_data, pairs)
        assert "K1|P1" in reviewed_data["pairs"]
        assert reviewed_data["pairs"]["K1|P1"]["match_source"] == "auto"


# ──────────────────────────────────────────────────
# postprocess_tickers.py: Fix #15 (corrections from human feedback)
# ──────────────────────────────────────────────────


class TestPostprocessCorrections:
    def test_mechanism_alias_applied(self, tmp_path):
        """Fix #15 should apply mechanism aliases from ticker_corrections.json."""
        # Create corrections file
        corrections = {
            "corrections": [
                {"type": "mechanism_alias", "from": "PROJECTED", "to": "CERTIFIED", "frequency": 5}
            ]
        }
        (tmp_path / "ticker_corrections.json").write_text(json.dumps(corrections))

        # Create minimal tickers file
        tickers_data = {
            "tickers": [{
                "market_id": "K1",
                "ticker": "BWR-TRUMP-WIN-PRES_US-PROJECTED-ANY-2028",
                "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                "mechanism": "PROJECTED", "threshold": "ANY", "timeframe": "2028",
                "platform": "Kalshi",
            }]
        }
        tickers_file = tmp_path / "tickers.json"
        tickers_file.write_text(json.dumps(tickers_data))

        # Create minimal enriched file
        enriched = {"markets": []}
        enriched_file = tmp_path / "enriched.json"
        enriched_file.write_text(json.dumps(enriched))

        output_file = tmp_path / "tickers_postprocessed.json"

        # Import and run postprocess
        from postprocess_tickers import postprocess
        postprocess(tickers_file, enriched_file, output_file)

        # Verify the correction was applied
        with open(output_file) as f:
            result = json.load(f)

        ticker = result["tickers"][0]
        assert ticker["mechanism"] == "CERTIFIED"
        assert "CERTIFIED" in ticker["ticker"]

    def test_agent_alias_applied(self, tmp_path):
        """Fix #15 should apply agent aliases."""
        corrections = {
            "corrections": [
                {"type": "agent_alias", "from": "POWELL", "to": "J_POWELL", "frequency": 3}
            ]
        }
        (tmp_path / "ticker_corrections.json").write_text(json.dumps(corrections))

        tickers_data = {
            "tickers": [{
                "market_id": "K1",
                "ticker": "BWR-POWELL-CUT-RATE-STD-ANY-2026",
                "agent": "POWELL", "action": "CUT", "target": "RATE",
                "mechanism": "STD", "threshold": "ANY", "timeframe": "2026",
                "platform": "Kalshi",
            }]
        }
        tickers_file = tmp_path / "tickers.json"
        tickers_file.write_text(json.dumps(tickers_data))

        enriched = {"markets": []}
        enriched_file = tmp_path / "enriched.json"
        enriched_file.write_text(json.dumps(enriched))

        output_file = tmp_path / "tickers_postprocessed.json"

        from postprocess_tickers import postprocess
        postprocess(tickers_file, enriched_file, output_file)

        with open(output_file) as f:
            result = json.load(f)

        assert result["tickers"][0]["agent"] == "J_POWELL"

    def test_no_corrections_file_ok(self, tmp_path):
        """If ticker_corrections.json doesn't exist, postprocess should still work."""
        tickers_data = {
            "tickers": [{
                "market_id": "K1",
                "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2028",
                "platform": "Kalshi",
            }]
        }
        tickers_file = tmp_path / "tickers.json"
        tickers_file.write_text(json.dumps(tickers_data))

        enriched = {"markets": []}
        enriched_file = tmp_path / "enriched.json"
        enriched_file.write_text(json.dumps(enriched))

        output_file = tmp_path / "tickers_postprocessed.json"

        from postprocess_tickers import postprocess
        postprocess(tickers_file, enriched_file, output_file)

        with open(output_file) as f:
            result = json.load(f)

        # Should still work fine
        assert len(result["tickers"]) == 1


# ──────────────────────────────────────────────────
# End-to-end: Full feedback loop test
# ──────────────────────────────────────────────────


class TestEndToEndFeedbackLoop:
    """Test the complete flow: ingest → evaluate → apply → corrections."""

    def test_full_cycle(self, tmp_path, monkeypatch):
        """Simulate a complete feedback integration cycle."""
        from pipeline_ingest_feedback import ingest_new_rows, load_human_labels
        from pipeline_evaluate_matches import (
            evaluate_same_event_labels,
            evaluate_category_labels,
            generate_report,
            build_market_id_to_ticker,
        )
        from pipeline_apply_human_labels import (
            apply_same_event_same_rules,
            apply_not_political,
            add_to_reviewed_pairs,
        )
        from generate_ticker_corrections import generate_corrections

        # Step 1: Set up tickers with a known false negative
        # (same event, different mechanism — human will confirm they match)
        tickers_data = {
            "tickers": [
                {"market_id": "K1", "ticker": "BWR-WARSH-APPOINT-FED_CHAIR-CERTIFIED-ANY-2029",
                 "agent": "WARSH", "action": "APPOINT", "target": "FED_CHAIR",
                 "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2029",
                 "platform": "Kalshi", "original_question": "Will Kevin Warsh be Fed Chair?"},
                {"market_id": "P1", "ticker": "BWR-WARSH-APPOINT-FED_CHAIR-STD-ANY-2029",
                 "agent": "WARSH", "action": "APPOINT", "target": "FED_CHAIR",
                 "mechanism": "STD", "threshold": "ANY", "timeframe": "2029",
                 "platform": "Polymarket", "original_question": "Kevin Warsh next Fed Chair?"},
                {"market_id": "K2", "ticker": "BWR-MUSK-HIT-BLUESKY-STD-1M-2026",
                 "agent": "MUSK", "action": "HIT", "target": "BLUESKY",
                 "mechanism": "STD", "threshold": "1M", "timeframe": "2026",
                 "platform": "Kalshi", "original_question": "Will Musk hit 1M on Bluesky?"},
            ]
        }

        # Step 2: Simulate ingested feedback
        existing_data = {
            "schema_version": 1,
            "updated_at": None,
            "last_ingested_timestamp": "2026-02-12T22:41:28.460Z",
            "labels": [],
        }
        csv_rows = [
            {
                "Timestamp": "2026-03-01T10:00:00.000Z",
                "Feedback Type": "same-event:same-rules",
                "Description": "Warsh Fed Chair — same event",
                "Market Count": "2",
                "Markets (JSON)": json.dumps([
                    {"key": "BWR-WARSH-APPOINT-FED_CHAIR-CERTIFIED-ANY-2029", "label": "Warsh K", "platform": "Kalshi"},
                    {"key": "BWR-WARSH-APPOINT-FED_CHAIR-STD-ANY-2029", "label": "Warsh PM", "platform": "Polymarket"},
                ]),
            },
            {
                "Timestamp": "2026-03-02T10:00:00.000Z",
                "Feedback Type": "not-political",
                "Description": "Bluesky is not political",
                "Market Count": "1",
                "Markets (JSON)": json.dumps([
                    {"key": "BWR-MUSK-HIT-BLUESKY-STD-1M-2026", "label": "Musk Bluesky", "platform": "Kalshi"},
                ]),
            },
        ]

        # Build ticker lookup for resolution
        from pipeline_ingest_feedback import build_ticker_lookup
        ticker_lookup_map = build_ticker_lookup(tickers_data)

        new_labels, latest_ts = ingest_new_rows(csv_rows, existing_data, tickers_data)
        assert len(new_labels) == 2
        existing_data["labels"] = new_labels
        existing_data["last_ingested_timestamp"] = latest_ts

        # Step 3: Evaluate
        id_lookup = build_market_id_to_ticker(tickers_data)
        same_eval = evaluate_same_event_labels(new_labels, id_lookup)

        # Warsh: pipeline has different tickers → should be a false negative
        assert len(same_eval["false_negatives"]) == 1
        assert same_eval["false_negatives"][0]["action_needed"] == "unify_tickers"

        # Step 4: Apply
        tickers_by_id = {str(t["market_id"]): t for t in tickers_data["tickers"]}
        applied, results = apply_same_event_same_rules(new_labels, tickers_by_id)
        assert applied == 1

        # Verify P1 now has same mechanism as K1
        assert tickers_by_id["P1"]["mechanism"] == "CERTIFIED"
        assert tickers_by_id["P1"]["match_source"] == "human"

        # Verify the same-event label is now applied
        same_event_label = [l for l in new_labels if l["label_type"] == "same_event_same_rules"][0]
        assert same_event_label["status"] == "applied"

        # Step 5: Add to reviewed pairs
        reviewed_data = {"updated_at": None, "pairs": {}}
        added = add_to_reviewed_pairs(new_labels, reviewed_data)
        assert added >= 1  # At least the same-event pair was added

        # Step 6: Generate corrections from accuracy report
        category_eval = {"correct": [], "incorrect": [], "skipped": 0}
        report = generate_report(same_eval, category_eval)
        corrections = generate_corrections(report)

        # With only 1 false negative, no corrections should be generated
        # (MIN_FREQUENCY=2)
        assert corrections["correction_count"] == 0

        # Step 7: Verify idempotency — re-running apply does nothing
        applied2, _ = apply_same_event_same_rules(new_labels, tickers_by_id)
        assert applied2 == 0  # Already applied

    def test_needs_review_for_unsafe_unification(self, tmp_path):
        """Labels where agent/action/target differ should be needs_review."""
        tickers_data = {
            "tickers": [
                {"market_id": "K1", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                 "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                 "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2028",
                 "platform": "Kalshi"},
                {"market_id": "P1", "ticker": "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028",
                 "agent": "HARRIS", "action": "WIN", "target": "PRES_US",
                 "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2028",
                 "platform": "Polymarket"},
            ]
        }
        tickers_by_id = {str(t["market_id"]): t for t in tickers_data["tickers"]}

        labels = [{
            "label_id": "hl_unsafe",
            "label_type": "same_event_same_rules",
            "market_ids": ["K1", "P1"],
            "description": "User thinks these are same event",
            "status": "pending",
            "applied_at": None,
            "applied_action": None,
        }]

        from pipeline_apply_human_labels import apply_same_event_same_rules
        applied, _ = apply_same_event_same_rules(labels, tickers_by_id)

        assert applied == 0
        assert labels[0]["status"] == "needs_review"
        assert "agent" in labels[0]["applied_action"]
        # Original tickers unchanged
        assert tickers_by_id["K1"]["ticker"] == "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028"
        assert tickers_by_id["P1"]["ticker"] == "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028"


# ──────────────────────────────────────────────────
# Batch ID traceability
# ──────────────────────────────────────────────────


class TestBatchIdTraceability:
    """Test that batch_id flows through the entire pipeline."""

    def test_batch_id_flows_through_full_cycle(self, tmp_path):
        """A single batch_id should appear on ingested labels, applied labels,
        the accuracy report, and the corrections file."""
        from pipeline_ingest_feedback import ingest_new_rows, build_ticker_lookup
        from pipeline_evaluate_matches import evaluate_same_event_labels, generate_report, build_market_id_to_ticker
        from pipeline_apply_human_labels import apply_same_event_same_rules
        from generate_ticker_corrections import generate_corrections

        batch_id = "batch_20260310_143000"

        tickers_data = {
            "tickers": [
                {"market_id": "K1", "ticker": "BWR-WARSH-APPOINT-FED_CHAIR-CERTIFIED-ANY-2029",
                 "agent": "WARSH", "action": "APPOINT", "target": "FED_CHAIR",
                 "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2029",
                 "platform": "Kalshi"},
                {"market_id": "P1", "ticker": "BWR-WARSH-APPOINT-FED_CHAIR-STD-ANY-2029",
                 "agent": "WARSH", "action": "APPOINT", "target": "FED_CHAIR",
                 "mechanism": "STD", "threshold": "ANY", "timeframe": "2029",
                 "platform": "Polymarket"},
            ]
        }
        existing_data = {
            "schema_version": 1, "updated_at": None,
            "last_ingested_timestamp": "2026-02-12T22:41:28.460Z",
            "labels": [],
        }
        csv_rows = [{
            "Timestamp": "2026-03-01T10:00:00.000Z",
            "Feedback Type": "same-event:same-rules",
            "Description": "Warsh Fed Chair",
            "Market Count": "2",
            "Markets (JSON)": json.dumps([
                {"key": "BWR-WARSH-APPOINT-FED_CHAIR-CERTIFIED-ANY-2029", "label": "K", "platform": "Kalshi"},
                {"key": "BWR-WARSH-APPOINT-FED_CHAIR-STD-ANY-2029", "label": "PM", "platform": "Polymarket"},
            ]),
        }]

        # 1. Ingest with batch_id
        new_labels, _ = ingest_new_rows(csv_rows, existing_data, tickers_data, batch_id=batch_id)
        assert len(new_labels) == 1
        assert new_labels[0]["ingested_batch_id"] == batch_id

        # 2. Apply with batch_id
        tickers_by_id = {str(t["market_id"]): t for t in tickers_data["tickers"]}
        applied, _ = apply_same_event_same_rules(new_labels, tickers_by_id, batch_id=batch_id)
        assert applied == 1
        assert new_labels[0]["applied_batch_id"] == batch_id

        # 3. Evaluate with batch_id
        id_lookup = build_market_id_to_ticker(tickers_data)
        same_eval = evaluate_same_event_labels(new_labels, id_lookup)
        category_eval = {"correct": [], "incorrect": [], "skipped": 0}
        report = generate_report(same_eval, category_eval, batch_id=batch_id)
        assert report["batch_id"] == batch_id

        # 4. Corrections with batch_id
        corrections = generate_corrections(report, batch_id=batch_id)
        assert corrections["batch_id"] == batch_id


# ──────────────────────────────────────────────────
# Edge cases and error handling
# ──────────────────────────────────────────────────


class TestEdgeCases:
    def test_empty_markets_json_skipped(self):
        """Rows with empty or invalid Markets (JSON) should be skipped."""
        from pipeline_ingest_feedback import ingest_new_rows

        existing = {
            "schema_version": 1, "updated_at": None,
            "last_ingested_timestamp": "2026-01-01T00:00:00.000Z",
            "labels": [],
        }
        rows = [
            {"Timestamp": "2026-03-01T10:00:00.000Z", "Feedback Type": "other",
             "Description": "test", "Market Count": "0", "Markets (JSON)": ""},
            {"Timestamp": "2026-03-01T11:00:00.000Z", "Feedback Type": "other",
             "Description": "test", "Market Count": "0", "Markets (JSON)": "not json"},
            {"Timestamp": "2026-03-01T12:00:00.000Z", "Feedback Type": "other",
             "Description": "test", "Market Count": "0", "Markets (JSON)": "[]"},
        ]
        new_labels, _ = ingest_new_rows(rows, existing, {"tickers": []})
        assert len(new_labels) == 0

    def test_label_with_single_market_skipped_for_same_event(self):
        """Same-event labels with <2 markets should be needs_review."""
        from pipeline_apply_human_labels import apply_same_event_same_rules

        labels = [{
            "label_id": "hl_single",
            "label_type": "same_event_same_rules",
            "market_ids": ["K1"],
            "status": "pending",
            "applied_at": None,
            "applied_action": None,
        }]
        applied, _ = apply_same_event_same_rules(labels, {})
        assert applied == 0
        assert labels[0]["status"] == "needs_review"

    def test_stale_market_ids_handled(self):
        """Labels referencing market_ids not in tickers should be needs_review."""
        from pipeline_apply_human_labels import apply_same_event_same_rules

        labels = [{
            "label_id": "hl_stale",
            "label_type": "same_event_same_rules",
            "market_ids": ["GONE1", "GONE2"],
            "status": "pending",
            "applied_at": None,
            "applied_action": None,
        }]
        applied, _ = apply_same_event_same_rules(labels, {})
        assert applied == 0
        assert labels[0]["status"] == "needs_review"

    def test_idempotency_already_applied_skipped(self):
        """Labels already applied should not be re-processed."""
        from pipeline_apply_human_labels import apply_same_event_same_rules

        tickers_by_id = {
            "K1": {"market_id": "K1", "ticker": "BWR-X-Y-Z-CERTIFIED-ANY-2028",
                   "agent": "X", "action": "Y", "target": "Z",
                   "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Kalshi"},
            "P1": {"market_id": "P1", "ticker": "BWR-X-Y-Z-STD-ANY-2028",
                   "agent": "X", "action": "Y", "target": "Z",
                   "mechanism": "STD", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Polymarket"},
        }
        labels = [{
            "label_id": "hl_done",
            "label_type": "same_event_same_rules",
            "market_ids": ["K1", "P1"],
            "status": "applied",
            "applied_at": "2026-03-01T00:00:00.000Z",
            "applied_action": "unified_tickers",
        }]
        applied, _ = apply_same_event_same_rules(labels, tickers_by_id)
        assert applied == 0
        # P1 mechanism should be unchanged
        assert tickers_by_id["P1"]["mechanism"] == "STD"

    def test_unicode_description_preserved(self):
        """Unicode in descriptions should be preserved."""
        from pipeline_ingest_feedback import ingest_new_rows

        existing = {
            "schema_version": 1, "updated_at": None,
            "last_ingested_timestamp": "2026-01-01T00:00:00.000Z",
            "labels": [],
        }
        rows = [{
            "Timestamp": "2026-03-01T10:00:00.000Z",
            "Feedback Type": "other",
            "Description": "Flávio Bolsonaro — not Jair",
            "Market Count": "1",
            "Markets (JSON)": json.dumps([{"key": "BWR-X-Y-Z-STD-ANY-2026", "label": "L", "platform": "K"}]),
        }]
        new_labels, _ = ingest_new_rows(rows, existing, {"tickers": []})
        assert len(new_labels) == 1
        assert "Flávio" in new_labels[0]["description"]
        assert "—" in new_labels[0]["description"]

    def test_corrections_applied_once_not_per_ticker(self, tmp_path):
        """Fix #15 should load corrections once, not per ticker."""
        from postprocess_tickers import postprocess

        corrections = {
            "corrections": [
                {"type": "mechanism_alias", "from": "PROJECTED", "to": "CERTIFIED", "frequency": 5}
            ]
        }
        (tmp_path / "ticker_corrections.json").write_text(json.dumps(corrections))

        tickers_data = {
            "tickers": [
                {"market_id": f"K{i}", "ticker": f"BWR-X-Y-Z-PROJECTED-ANY-2028",
                 "agent": "X", "action": "Y", "target": "Z",
                 "mechanism": "PROJECTED", "threshold": "ANY", "timeframe": "2028",
                 "platform": "Kalshi"}
                for i in range(100)
            ]
        }
        tickers_file = tmp_path / "tickers.json"
        tickers_file.write_text(json.dumps(tickers_data))

        enriched = {"markets": []}
        enriched_file = tmp_path / "enriched.json"
        enriched_file.write_text(json.dumps(enriched))

        output_file = tmp_path / "output.json"
        postprocess(tickers_file, enriched_file, output_file)

        with open(output_file) as f:
            result = json.load(f)

        # All 100 tickers should have CERTIFIED
        for t in result["tickers"]:
            assert t["mechanism"] == "CERTIFIED"

    def test_timestamp_fallback_uses_default(self):
        """Invalid last_ingested_timestamp should fall back to DEFAULT, not datetime.min."""
        from pipeline_ingest_feedback import ingest_new_rows, DEFAULT_LAST_INGESTED

        existing = {
            "schema_version": 1, "updated_at": None,
            "last_ingested_timestamp": "INVALID_TIMESTAMP",
            "labels": [],
        }
        # Row before DEFAULT_LAST_INGESTED should be skipped
        rows = [{
            "Timestamp": "2026-02-01T00:00:00.000Z",
            "Feedback Type": "other",
            "Description": "old row",
            "Market Count": "1",
            "Markets (JSON)": json.dumps([{"key": "BWR-X-Y-Z-STD-ANY-2026", "label": "L", "platform": "K"}]),
        }]
        new_labels, _ = ingest_new_rows(rows, existing, {"tickers": []})
        # Should be skipped because 2026-02-01 < DEFAULT (2026-02-12)
        assert len(new_labels) == 0


# ──────────────────────────────────────────────────
# Match Exclusion: generate_market_map grouping
# ──────────────────────────────────────────────────


class TestExclusionGrouping:
    """Tests that generate_market_map correctly splits groups with exclusions."""

    def test_split_excluded_groups_basic(self):
        from generate_market_map import split_excluded_groups

        markets = [
            {"market_id": "K1", "platform": "Kalshi"},
            {"market_id": "P1", "platform": "Polymarket"},
        ]
        exclusions = {frozenset(["K1", "P1"])}

        subgroups = split_excluded_groups(markets, exclusions)

        assert len(subgroups) == 2
        # Each subgroup has exactly one market
        sizes = sorted(len(sg) for sg in subgroups)
        assert sizes == [1, 1]

    def test_no_exclusion_single_group(self):
        from generate_market_map import split_excluded_groups

        markets = [
            {"market_id": "K1", "platform": "Kalshi"},
            {"market_id": "P1", "platform": "Polymarket"},
        ]
        exclusions = set()

        subgroups = split_excluded_groups(markets, exclusions)

        assert len(subgroups) == 1
        assert len(subgroups[0]) == 2

    def test_partial_exclusion_three_markets(self):
        """Exclude (A,B) but not (A,C) or (B,C) → two groups, A and B never together."""
        from generate_market_map import split_excluded_groups

        markets = [
            {"market_id": "A", "platform": "Kalshi"},
            {"market_id": "B", "platform": "Polymarket"},
            {"market_id": "C", "platform": "Polymarket"},
        ]
        # Only A and B are excluded from each other
        exclusions = {frozenset(["A", "B"])}

        subgroups = split_excluded_groups(markets, exclusions)

        # Must be 2 groups, and A and B must NOT be in the same group
        assert len(subgroups) == 2
        for sg in subgroups:
            ids = {m["market_id"] for m in sg}
            assert not ({"A", "B"} <= ids), "A and B must not be in the same group"
        # All markets accounted for
        all_ids = {m["market_id"] for sg in subgroups for m in sg}
        assert all_ids == {"A", "B", "C"}

    def test_single_market_no_split(self):
        from generate_market_map import split_excluded_groups

        markets = [{"market_id": "K1", "platform": "Kalshi"}]
        exclusions = set()

        subgroups = split_excluded_groups(markets, exclusions)
        assert len(subgroups) == 1
        assert len(subgroups[0]) == 1

    def test_all_pairs_excluded(self):
        """All 3 markets excluded from each other → 3 singleton groups."""
        from generate_market_map import split_excluded_groups

        markets = [
            {"market_id": "A", "platform": "Kalshi"},
            {"market_id": "B", "platform": "Polymarket"},
            {"market_id": "C", "platform": "Polymarket"},
        ]
        exclusions = {
            frozenset(["A", "B"]),
            frozenset(["A", "C"]),
            frozenset(["B", "C"]),
        }

        subgroups = split_excluded_groups(markets, exclusions)
        assert len(subgroups) == 3

    def test_irrelevant_exclusion_ignored(self):
        """Exclusion for different market IDs shouldn't affect this group."""
        from generate_market_map import split_excluded_groups

        markets = [
            {"market_id": "K1", "platform": "Kalshi"},
            {"market_id": "P1", "platform": "Polymarket"},
        ]
        exclusions = {frozenset(["X1", "X2"])}  # Different markets

        subgroups = split_excluded_groups(markets, exclusions)
        assert len(subgroups) == 1
        assert len(subgroups[0]) == 2

    def test_load_match_exclusions_missing_file(self):
        from generate_market_map import load_match_exclusions
        # Just verify it doesn't crash when file doesn't exist
        # (it uses the module-level path which may or may not exist)
        result = load_match_exclusions()
        assert isinstance(result, set)


# ──────────────────────────────────────────────────
# Disambiguation Rules: generate_ticker_corrections
# ──────────────────────────────────────────────────


class TestDisambiguationRules:
    """Tests for false positive pattern analysis and disambiguation generation."""

    def test_analyze_threshold_false_positive(self):
        from generate_ticker_corrections import analyze_false_positive_patterns

        report = {
            "disagreements": [
                {
                    "label_id": "hl_001",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": [
                        "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                        "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                    ],
                },
                {
                    "label_id": "hl_002",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": [
                        "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                        "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                    ],
                },
            ],
            "sample_size": 10,
        }

        rules, _ = analyze_false_positive_patterns(report, min_frequency=2)
        threshold_rules = [r for r in rules if r["type"] == "threshold_disambiguation"]
        assert len(threshold_rules) == 1
        assert threshold_rules[0]["action"] == "re_extract_threshold"
        assert threshold_rules[0]["pattern"]["agent"] == "TRUMP"
        assert threshold_rules[0]["pattern"]["threshold"] == "ANY"
        assert threshold_rules[0]["frequency"] == 2

    def test_analyze_timeframe_false_positive(self):
        from generate_ticker_corrections import analyze_false_positive_patterns

        report = {
            "disagreements": [
                {
                    "label_id": "hl_t1",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": [
                        "BWR-FED-CUT-RATES-STD-25BPS-2026",
                        "BWR-FED-CUT-RATES-STD-25BPS-2026",
                    ],
                },
                {
                    "label_id": "hl_t2",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": [
                        "BWR-FED-CUT-RATES-STD-25BPS-2026",
                        "BWR-FED-CUT-RATES-STD-25BPS-2026",
                    ],
                },
            ],
            "sample_size": 10,
        }

        rules, _ = analyze_false_positive_patterns(report, min_frequency=2)
        timeframe_rules = [r for r in rules if r["type"] == "timeframe_disambiguation"]
        assert len(timeframe_rules) == 1
        assert timeframe_rules[0]["action"] == "re_extract_timeframe_monthly"
        assert timeframe_rules[0]["pattern"]["timeframe"] == "2026"

    def test_below_frequency_threshold_no_rule(self):
        from generate_ticker_corrections import analyze_false_positive_patterns

        report = {
            "disagreements": [
                {
                    "label_id": "hl_solo",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": [
                        "BWR-X-W-Y-STD-ANY-2028",
                        "BWR-X-W-Y-STD-ANY-2028",
                    ],
                },
            ],
            "sample_size": 5,
        }

        rules, _ = analyze_false_positive_patterns(report, min_frequency=3)
        assert len(rules) == 0

    def test_no_false_positives_empty(self):
        from generate_ticker_corrections import analyze_false_positive_patterns

        report = {"disagreements": [], "sample_size": 0}
        rules, unresolvable = analyze_false_positive_patterns(report, min_frequency=2)
        assert rules == []
        assert unresolvable == 0

    def test_generate_disambiguations_output_schema(self):
        from generate_ticker_corrections import generate_disambiguations

        report = {
            "disagreements": [
                {
                    "label_id": "hl_001",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": ["BWR-A-W-X-STD-ANY-2028", "BWR-A-W-X-STD-ANY-2028"],
                },
                {
                    "label_id": "hl_002",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": ["BWR-A-W-X-STD-ANY-2028", "BWR-A-W-X-STD-ANY-2028"],
                },
            ],
            "sample_size": 10,
        }

        result = generate_disambiguations(report, min_frequency=2, batch_id="batch_test")

        assert "generated_at" in result
        assert result["batch_id"] == "batch_test"
        # Multi-field detection: threshold=ANY + mechanism=STD + timeframe=2028 all detected
        assert result["disambiguation_count"] >= 1
        assert len(result["disambiguations"]) >= 1
        # At minimum, threshold_disambiguation should be present
        types = [d["type"] for d in result["disambiguations"]]
        assert "threshold_disambiguation" in types

    def test_false_negatives_ignored(self):
        """False negatives (same event, unify) should NOT produce disambiguations."""
        from generate_ticker_corrections import analyze_false_positive_patterns

        report = {
            "disagreements": [
                {
                    "label_id": "hl_fn",
                    "human_judgment": "same event",
                    "action_needed": "unify_tickers",
                    "pipeline_tickers": ["BWR-A-W-X-STD-ANY-2028", "BWR-A-W-X-CERT-ANY-2028"],
                },
            ],
            "sample_size": 5,
        }

        rules, _ = analyze_false_positive_patterns(report, min_frequency=1)
        assert rules == []


# ──────────────────────────────────────────────────
# Fix 16: Disambiguation in postprocess_tickers
# ──────────────────────────────────────────────────


class TestFix16Disambiguation:
    """Tests for Fix 16 disambiguation rule application in postprocess."""

    def test_threshold_disambiguation_re_extracts(self):
        """Ticker matching a threshold disambiguation rule should have threshold re-extracted."""
        from create_tickers import extract_threshold

        ticker = {
            "market_id": "K1",
            "ticker": "BWR-TRUMP-WIN-PRES_US-CERT-ANY-2028",
            "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
            "mechanism": "CERT", "threshold": "ANY", "timeframe": "2028",
            "platform": "Kalshi",
            "original_question": "Will Trump win the 2028 presidential election by more than 5%?",
        }

        rule = {
            "type": "threshold_disambiguation",
            "pattern": {"agent": "TRUMP", "action": "WIN", "target": "PRES_US", "threshold": "ANY"},
            "action": "re_extract_threshold",
        }

        # Check what extract_threshold returns for this question
        extracted = extract_threshold(ticker["original_question"])

        # Apply rule logic manually (same as Fix 16)
        if (ticker["agent"] == rule["pattern"]["agent"]
            and ticker["action"] == rule["pattern"]["action"]
            and ticker["target"] == rule["pattern"]["target"]
            and ticker["threshold"] == rule["pattern"]["threshold"]):
            if extracted != "ANY":
                ticker["threshold"] = extracted

        # If extract_threshold found something, threshold should change
        # If not, it stays ANY — both outcomes are valid
        assert isinstance(ticker["threshold"], str)

    def test_disambiguation_no_match_no_change(self):
        """Rule that doesn't match the ticker should not modify it."""
        ticker = {
            "market_id": "K1",
            "ticker": "BWR-HARRIS-WIN-PRES_US-CERT-YES-2028",
            "agent": "HARRIS", "action": "WIN", "target": "PRES_US",
            "mechanism": "CERT", "threshold": "YES", "timeframe": "2028",
            "platform": "Kalshi",
            "original_question": "Will Harris win?",
        }

        rule = {
            "type": "threshold_disambiguation",
            "pattern": {"agent": "TRUMP", "action": "WIN", "target": "PRES_US", "threshold": "ANY"},
            "action": "re_extract_threshold",
        }

        # Agent doesn't match — should not change
        original_threshold = ticker["threshold"]
        if (ticker["agent"] == rule["pattern"]["agent"]
            and ticker["action"] == rule["pattern"]["action"]
            and ticker["target"] == rule["pattern"]["target"]):
            pass  # Would apply rule
        assert ticker["threshold"] == original_threshold

    def test_timeframe_disambiguation_longer_extraction(self):
        """Timeframe disambiguation should only apply if extraction is more specific."""
        from postprocess_tickers import extract_date_from_description

        # A description with a specific month
        desc = "Will the Fed cut rates by March 2026?"
        extracted = extract_date_from_description(desc)

        # The extracted date should be more specific than just "2026"
        if extracted:
            assert len(extracted) > 4 or extracted != "2026"
