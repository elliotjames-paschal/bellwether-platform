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
