"""
Tests for pipeline_apply_human_labels.py

Run: pytest packages/pipelines/tests/test_apply_human_labels.py -v
"""

import pytest
import json
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline_apply_human_labels import (
    validate_same_event_pair,
    choose_canonical_ticker,
    apply_same_event_same_rules,
    apply_same_event_different_rules,
    apply_different_event,
    apply_not_political,
    apply_wrong_category,
    detect_category_in_description,
    add_to_reviewed_pairs,
    make_pair_key,
    parse_ticker_components,
    reassemble_ticker,
)


# ──────────────────────────────────────────────────
# Helper to create ticker dicts
# ──────────────────────────────────────────────────


def make_ticker(market_id, ticker, platform="Kalshi", question=""):
    components = parse_ticker_components(ticker)
    return {
        "market_id": market_id,
        "ticker": ticker,
        "platform": platform,
        "original_question": question,
        **components,
    }


def make_label(label_id, label_type, market_ids, description="", status="pending"):
    return {
        "label_id": label_id,
        "label_type": label_type,
        "market_ids": market_ids,
        "description": description,
        "status": status,
        "applied_at": None,
        "applied_action": None,
    }


# ──────────────────────────────────────────────────
# validate_same_event_pair
# ──────────────────────────────────────────────────


class TestValidateSameEventPair:
    def test_same_ticker_already_matched(self):
        t = make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        can, reason = validate_same_event_pair(t, t)
        assert can is True
        assert reason == "already_matched"

    def test_mechanism_only_diff_safe(self):
        t_a = make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        t_b = make_ticker("P1", "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028")
        can, reason = validate_same_event_pair(t_a, t_b)
        assert can is True
        assert "safe_unify" in reason
        assert "mechanism" in reason

    def test_threshold_only_diff_safe(self):
        t_a = make_ticker("K1", "BWR-FED-CUT-RATE-STD-GT_25BPS-2026")
        t_b = make_ticker("P1", "BWR-FED-CUT-RATE-STD-ANY-2026")
        can, reason = validate_same_event_pair(t_a, t_b)
        assert can is True

    def test_agent_differs_unsafe(self):
        t_a = make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        t_b = make_ticker("P1", "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028")
        can, reason = validate_same_event_pair(t_a, t_b)
        assert can is False
        assert "agent" in reason

    def test_action_differs_unsafe(self):
        t_a = make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        t_b = make_ticker("P1", "BWR-TRUMP-LOSE-PRES_US-CERTIFIED-ANY-2028")
        can, reason = validate_same_event_pair(t_a, t_b)
        assert can is False
        assert "action" in reason

    def test_target_differs_unsafe(self):
        t_a = make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        t_b = make_ticker("P1", "BWR-TRUMP-WIN-GOV_FL-CERTIFIED-ANY-2028")
        can, reason = validate_same_event_pair(t_a, t_b)
        assert can is False
        assert "target" in reason

    def test_timeframe_differs_unsafe(self):
        t_a = make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        t_b = make_ticker("P1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2032")
        can, reason = validate_same_event_pair(t_a, t_b)
        assert can is False
        assert "timeframe" in reason

    def test_multiple_core_diffs(self):
        t_a = make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        t_b = make_ticker("P1", "BWR-HARRIS-LOSE-GOV_FL-STD-ANY-2032")
        can, reason = validate_same_event_pair(t_a, t_b)
        assert can is False


# ──────────────────────────────────────────────────
# choose_canonical_ticker
# ──────────────────────────────────────────────────


class TestChooseCanonicalTicker:
    def test_prefers_kalshi(self):
        k = make_ticker("K1", "BWR-T-W-P-CERTIFIED-ANY-2028", "Kalshi")
        p = make_ticker("P1", "BWR-T-W-P-STD-ANY-2028", "Polymarket")
        assert choose_canonical_ticker(k, p) is k
        assert choose_canonical_ticker(p, k) is k

    def test_alphabetical_fallback(self):
        a = make_ticker("A1", "BWR-A-W-P-STD-ANY-2028", "Polymarket")
        b = make_ticker("B1", "BWR-B-W-P-STD-ANY-2028", "Polymarket")
        assert choose_canonical_ticker(a, b) is a
        assert choose_canonical_ticker(b, a) is a


# ──────────────────────────────────────────────────
# apply_same_event_same_rules
# ──────────────────────────────────────────────────


class TestApplySameEventSameRules:
    def test_unifies_mechanism_diff(self):
        """Safe unification when only mechanism differs."""
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-WARSH-APPOINT-FED_CHAIR-CERTIFIED-ANY-2029", "Kalshi"),
            "P1": make_ticker("P1", "BWR-WARSH-APPOINT-FED_CHAIR-STD-ANY-2029", "Polymarket"),
        }
        labels = [make_label("hl_001", "same_event_same_rules", ["K1", "P1"])]

        applied, results = apply_same_event_same_rules(labels, tickers_by_id)

        assert applied == 1
        assert labels[0]["status"] == "applied"
        assert labels[0]["applied_action"] == "unified_tickers"
        # Polymarket should now have Kalshi's mechanism
        assert tickers_by_id["P1"]["mechanism"] == "CERTIFIED"
        assert tickers_by_id["P1"]["match_source"] == "human"

    def test_already_matched_noop(self):
        """If tickers already match, mark as applied but don't modify."""
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028", "Kalshi"),
            "P1": make_ticker("P1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028", "Polymarket"),
        }
        labels = [make_label("hl_002", "same_event_same_rules", ["K1", "P1"])]

        applied, results = apply_same_event_same_rules(labels, tickers_by_id)

        assert applied == 0  # No modification needed
        assert labels[0]["status"] == "applied"
        assert labels[0]["applied_action"] == "already_matched"

    def test_core_differs_needs_review(self):
        """If agent/action/target/timeframe differ, mark needs_review."""
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028", "Kalshi"),
            "P1": make_ticker("P1", "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028", "Polymarket"),
        }
        labels = [make_label("hl_003", "same_event_same_rules", ["K1", "P1"])]

        applied, results = apply_same_event_same_rules(labels, tickers_by_id)

        assert applied == 0
        assert labels[0]["status"] == "needs_review"
        assert "validation_failed" in labels[0]["applied_action"]

    def test_missing_ticker_needs_review(self):
        """If a market isn't in the ticker data, mark needs_review."""
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028", "Kalshi"),
        }
        labels = [make_label("hl_004", "same_event_same_rules", ["K1", "UNKNOWN"])]

        applied, results = apply_same_event_same_rules(labels, tickers_by_id)

        assert applied == 0
        assert labels[0]["status"] == "needs_review"

    def test_single_market_needs_review(self):
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028"),
        }
        labels = [make_label("hl_005", "same_event_same_rules", ["K1"])]

        applied, _ = apply_same_event_same_rules(labels, tickers_by_id)
        assert applied == 0
        assert labels[0]["status"] == "needs_review"

    def test_skips_already_applied(self):
        """Labels with applied_at set should be skipped."""
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-T-W-P-CERT-ANY-2028", "Kalshi"),
            "P1": make_ticker("P1", "BWR-T-W-P-STD-ANY-2028", "Polymarket"),
        }
        labels = [make_label("hl_006", "same_event_same_rules", ["K1", "P1"],
                             status="applied")]
        labels[0]["applied_at"] = "2026-01-01"

        applied, _ = apply_same_event_same_rules(labels, tickers_by_id)
        assert applied == 0

    def test_skips_wrong_label_type(self):
        """Only processes same_event_same_rules labels."""
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-T-W-P-CERT-ANY-2028"),
        }
        labels = [make_label("hl_007", "not_political", ["K1"])]

        applied, _ = apply_same_event_same_rules(labels, tickers_by_id)
        assert applied == 0


# ──────────────────────────────────────────────────
# apply_same_event_different_rules
# ──────────────────────────────────────────────────


class TestApplySameEventDifferentRules:
    def test_creates_near_match_entry(self):
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-WARSH-APPOINT-FED-CERT-ANY-2029", "Kalshi"),
            "P1": make_ticker("P1", "BWR-WARSH-APPOINT-FED-STD-ANY-2029", "Polymarket"),
        }
        labels = [make_label("hl_010", "same_event_different_rules", ["K1", "P1"],
                             "Different resolution criteria")]

        entries, results = apply_same_event_different_rules(labels, tickers_by_id)

        assert len(entries) == 1
        assert entries[0]["verdict"] == "OVERLAPPING"
        assert entries[0]["match_source"] == "human"
        assert labels[0]["status"] == "applied"

    def test_skips_same_platform(self):
        """Same-platform pairs shouldn't create near-match entries."""
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-A-W-P-STD-ANY-2028", "Kalshi"),
            "K2": make_ticker("K2", "BWR-B-W-P-STD-ANY-2028", "Kalshi"),
        }
        labels = [make_label("hl_011", "same_event_different_rules", ["K1", "K2"])]

        entries, _ = apply_same_event_different_rules(labels, tickers_by_id)
        assert len(entries) == 0


# ──────────────────────────────────────────────────
# apply_different_event
# ──────────────────────────────────────────────────


class TestApplyDifferentEvent:
    def test_splits_matching_tickers(self):
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-TRUMP-WIN-PRES_US-CERT-ANY-2028", "Kalshi"),
            "P1": make_ticker("P1", "BWR-TRUMP-WIN-PRES_US-CERT-ANY-2028", "Polymarket"),
        }
        labels = [make_label("hl_020", "different_event", ["K1", "P1"])]

        modified, pairs, results = apply_different_event(labels, tickers_by_id)

        assert modified == 1
        assert tickers_by_id["P1"]["ticker"].endswith("_SPLIT")
        assert tickers_by_id["P1"]["match_source"] == "human"
        assert labels[0]["status"] == "applied"
        assert len(pairs) == 1
        assert pairs[0]["verdict"] == "DIFFERENT"

    def test_already_different_noop(self):
        tickers_by_id = {
            "K1": make_ticker("K1", "BWR-A-W-X-STD-ANY-2028", "Kalshi"),
            "P1": make_ticker("P1", "BWR-B-W-Y-STD-ANY-2028", "Polymarket"),
        }
        labels = [make_label("hl_021", "different_event", ["K1", "P1"])]

        modified, _, _ = apply_different_event(labels, tickers_by_id)
        assert modified == 0
        assert labels[0]["status"] == "applied"
        assert labels[0]["applied_action"] == "already_different"


# ──────────────────────────────────────────────────
# apply_not_political
# ──────────────────────────────────────────────────


class TestApplyNotPolitical:
    def test_marks_market_not_political(self, tmp_path):
        csv_path = tmp_path / "master.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["market_id", "political_category", "question"])
            writer.writeheader()
            writer.writerow({"market_id": "K001", "political_category": "1. ELECTORAL", "question": "Q1"})
            writer.writerow({"market_id": "K002", "political_category": "3. LEGISLATIVE", "question": "Q2"})

        labels = [make_label("hl_030", "not_political", ["K001"])]
        modified, results = apply_not_political(labels, csv_path)

        assert modified == 1
        assert labels[0]["status"] == "applied"

        # Verify CSV was updated
        with open(csv_path, newline="") as f:
            rows = list(csv.DictReader(f))
        assert rows[0]["political_category"] == "16. NOT_POLITICAL"
        assert rows[1]["political_category"] == "3. LEGISLATIVE"  # Unchanged

    def test_already_not_political(self, tmp_path):
        csv_path = tmp_path / "master.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["market_id", "political_category"])
            writer.writeheader()
            writer.writerow({"market_id": "K001", "political_category": "16. NOT_POLITICAL"})

        labels = [make_label("hl_031", "not_political", ["K001"])]
        modified, _ = apply_not_political(labels, csv_path)

        assert modified == 0  # Already correct
        assert labels[0]["status"] == "applied"  # Still marked applied


# ──────────────────────────────────────────────────
# detect_category_in_description
# ──────────────────────────────────────────────────


class TestDetectCategoryInDescription:
    def test_detects_electoral(self):
        assert detect_category_in_description("Should be ELECTORAL") == "1. ELECTORAL"

    def test_detects_appointments(self):
        assert detect_category_in_description("This is an APPOINTMENTS market") == "4. APPOINTMENTS"

    def test_detects_with_spaces(self):
        assert detect_category_in_description("Should be monetary policy") == "2. MONETARY_POLICY"

    def test_case_insensitive(self):
        assert detect_category_in_description("should be legislative") == "3. LEGISLATIVE"

    def test_no_match(self):
        assert detect_category_in_description("This is wrong somehow") == ""

    def test_empty(self):
        assert detect_category_in_description("") == ""


# ──────────────────────────────────────────────────
# apply_wrong_category
# ──────────────────────────────────────────────────


class TestApplyWrongCategory:
    def test_recategorizes_with_valid_category(self, tmp_path):
        csv_path = tmp_path / "master.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["market_id", "political_category"])
            writer.writeheader()
            writer.writerow({"market_id": "K001", "political_category": "1. ELECTORAL"})

        labels = [make_label("hl_040", "wrong_category", ["K001"],
                             "Should be APPOINTMENTS")]
        modified, results = apply_wrong_category(labels, csv_path)

        assert modified == 1
        assert labels[0]["status"] == "applied"

        with open(csv_path, newline="") as f:
            rows = list(csv.DictReader(f))
        assert rows[0]["political_category"] == "4. APPOINTMENTS"

    def test_needs_review_without_category(self, tmp_path):
        csv_path = tmp_path / "master.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["market_id", "political_category"])
            writer.writeheader()
            writer.writerow({"market_id": "K001", "political_category": "1. ELECTORAL"})

        labels = [make_label("hl_041", "wrong_category", ["K001"],
                             "This is in the wrong category")]  # No valid category mentioned
        modified, results = apply_wrong_category(labels, csv_path)

        assert modified == 0
        assert labels[0]["status"] == "needs_review"


# ──────────────────────────────────────────────────
# add_to_reviewed_pairs
# ──────────────────────────────────────────────────


class TestAddToReviewedPairs:
    def test_adds_pair_for_applied_labels(self):
        reviewed_data = {"updated_at": None, "pairs": {}}
        labels = [
            make_label("hl_050", "same_event_same_rules", ["K1", "P1"], status="applied"),
        ]
        labels[0]["applied_action"] = "unified_tickers"

        added = add_to_reviewed_pairs(labels, reviewed_data)

        assert added == 1
        pair_key = make_pair_key("K1", "P1")
        assert pair_key in reviewed_data["pairs"]
        assert reviewed_data["pairs"][pair_key]["match_source"] == "human"

    def test_skips_pending_labels(self):
        reviewed_data = {"updated_at": None, "pairs": {}}
        labels = [make_label("hl_051", "same_event_same_rules", ["K1", "P1"])]

        added = add_to_reviewed_pairs(labels, reviewed_data)
        assert added == 0

    def test_no_duplicate_pairs(self):
        pair_key = make_pair_key("K1", "P1")
        reviewed_data = {
            "updated_at": None,
            "pairs": {pair_key: {"match_source": "human"}},
        }
        labels = [
            make_label("hl_052", "same_event_same_rules", ["K1", "P1"], status="applied"),
        ]

        added = add_to_reviewed_pairs(labels, reviewed_data)
        assert added == 0  # Already exists

    def test_creates_pairwise_for_3_markets(self):
        reviewed_data = {"updated_at": None, "pairs": {}}
        labels = [
            make_label("hl_053", "same_event_same_rules", ["A", "B", "C"], status="applied"),
        ]
        labels[0]["applied_action"] = "unified_tickers"

        added = add_to_reviewed_pairs(labels, reviewed_data)
        assert added == 3  # A|B, A|C, B|C


# ──────────────────────────────────────────────────
# make_pair_key
# ──────────────────────────────────────────────────


class TestMakePairKey:
    def test_sorted_order(self):
        assert make_pair_key("Z", "A") == "A|Z"
        assert make_pair_key("A", "Z") == "A|Z"

    def test_deterministic(self):
        assert make_pair_key("X", "Y") == make_pair_key("Y", "X")


# ──────────────────────────────────────────────────
# parse_ticker_components / reassemble_ticker
# ──────────────────────────────────────────────────


class TestTickerParsing:
    def test_roundtrip(self):
        ticker = "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028"
        components = parse_ticker_components(ticker)
        assert components["agent"] == "TRUMP"
        assert components["mechanism"] == "CERTIFIED"
        reassembled = reassemble_ticker(components)
        assert reassembled == ticker

    def test_hyphenated_target(self):
        ticker = "BWR-DEM-CONTROL-HOUSE-SENATE-CERTIFIED-ANY-2026"
        components = parse_ticker_components(ticker)
        assert components["agent"] == "DEM"
        assert components["target"] == "HOUSE-SENATE"
        assert components["mechanism"] == "CERTIFIED"


# ──────────────────────────────────────────────────
# Batch ID tracking
# ──────────────────────────────────────────────────


class TestBatchIdTracking:
    def test_batch_id_stamped_on_unified_labels(self):
        """Applied labels should carry the batch_id."""
        tickers_by_id = {
            "K1": {"market_id": "K1", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                   "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                   "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Kalshi"},
            "P1": {"market_id": "P1", "ticker": "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028",
                   "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                   "mechanism": "STD", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Polymarket"},
        }
        labels = [{
            "label_id": "hl_batch_test",
            "label_type": "same_event_same_rules",
            "market_ids": ["K1", "P1"],
            "status": "pending",
            "applied_at": None,
            "applied_action": None,
        }]
        applied, _ = apply_same_event_same_rules(labels, tickers_by_id, batch_id="batch_20260310_120000")
        assert applied == 1
        assert labels[0]["applied_batch_id"] == "batch_20260310_120000"

    def test_batch_id_none_when_not_provided(self):
        """Without batch_id, field should be None."""
        tickers_by_id = {
            "K1": {"market_id": "K1", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                   "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                   "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Kalshi"},
            "P1": {"market_id": "P1", "ticker": "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028",
                   "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                   "mechanism": "STD", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Polymarket"},
        }
        labels = [{
            "label_id": "hl_no_batch",
            "label_type": "same_event_same_rules",
            "market_ids": ["K1", "P1"],
            "status": "pending",
            "applied_at": None,
            "applied_action": None,
        }]
        applied, _ = apply_same_event_same_rules(labels, tickers_by_id)
        assert applied == 1
        assert labels[0]["applied_batch_id"] is None

    def test_batch_id_on_not_political(self, tmp_path):
        """Not-political labels should carry batch_id."""
        csv_file = tmp_path / "markets.csv"
        csv_file.write_text("market_id,political_category\nM1,1. ELECTORAL\n")

        labels = [{
            "label_id": "hl_np_batch",
            "label_type": "not_political",
            "market_ids": ["M1"],
            "status": "pending",
            "applied_at": None,
            "applied_action": None,
        }]
        modified, _ = apply_not_political(labels, csv_file, batch_id="batch_test_001")
        assert modified == 1
        assert labels[0]["applied_batch_id"] == "batch_test_001"


# ──────────────────────────────────────────────────
# N>2 market pairwise validation
# ──────────────────────────────────────────────────


class TestNGreaterThan2Validation:
    def test_three_markets_all_safe(self):
        """3 markets differing only in mechanism should all unify."""
        tickers_by_id = {
            "K1": {"market_id": "K1", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                   "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                   "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Kalshi"},
            "P1": {"market_id": "P1", "ticker": "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028",
                   "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                   "mechanism": "STD", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Polymarket"},
            "P2": {"market_id": "P2", "ticker": "BWR-TRUMP-WIN-PRES_US-PROJECTED-ANY-2028",
                   "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                   "mechanism": "PROJECTED", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Polymarket"},
        }
        labels = [{
            "label_id": "hl_3way",
            "label_type": "same_event_same_rules",
            "market_ids": ["K1", "P1", "P2"],
            "status": "pending",
            "applied_at": None,
            "applied_action": None,
        }]
        applied, _ = apply_same_event_same_rules(labels, tickers_by_id)
        assert applied == 1
        # All should now have Kalshi's mechanism
        assert tickers_by_id["P1"]["mechanism"] == "CERTIFIED"
        assert tickers_by_id["P2"]["mechanism"] == "CERTIFIED"

    def test_three_markets_one_unsafe_pair_blocks_all(self):
        """If any pair has differing agent, the entire group is needs_review."""
        tickers_by_id = {
            "K1": {"market_id": "K1", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                   "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                   "mechanism": "CERTIFIED", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Kalshi"},
            "P1": {"market_id": "P1", "ticker": "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028",
                   "agent": "TRUMP", "action": "WIN", "target": "PRES_US",
                   "mechanism": "STD", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Polymarket"},
            "P2": {"market_id": "P2", "ticker": "BWR-HARRIS-WIN-PRES_US-STD-ANY-2028",
                   "agent": "HARRIS", "action": "WIN", "target": "PRES_US",
                   "mechanism": "STD", "threshold": "ANY", "timeframe": "2028",
                   "platform": "Polymarket"},
        }
        labels = [{
            "label_id": "hl_3way_unsafe",
            "label_type": "same_event_same_rules",
            "market_ids": ["K1", "P1", "P2"],
            "status": "pending",
            "applied_at": None,
            "applied_action": None,
        }]
        applied, _ = apply_same_event_same_rules(labels, tickers_by_id)
        assert applied == 0
        assert labels[0]["status"] == "needs_review"
        assert "agent" in labels[0]["applied_action"]
        # No tickers should have been modified
        assert tickers_by_id["P1"]["mechanism"] == "STD"
        assert tickers_by_id["P2"]["agent"] == "HARRIS"
