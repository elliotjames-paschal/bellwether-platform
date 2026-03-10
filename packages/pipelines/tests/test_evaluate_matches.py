"""
Tests for pipeline_evaluate_matches.py

Run: pytest packages/pipelines/tests/test_evaluate_matches.py -v
"""

import pytest
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline_evaluate_matches import (
    evaluate_same_event_labels,
    evaluate_category_labels,
    compute_precision_recall,
    generate_report,
    build_market_id_to_ticker,
)


# ──────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────


@pytest.fixture
def ticker_lookup():
    """Simulated ticker lookup with known matches and mismatches."""
    tickers = {
        "tickers": [
            # Matched pair — same ticker on both platforms
            {"market_id": "K001", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028", "platform": "Kalshi"},
            {"market_id": "P001", "ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028", "platform": "Polymarket"},
            # Unmatched pair — different tickers (false negative scenario)
            {"market_id": "K002", "ticker": "BWR-WARSH-APPOINT-FED_CHAIR-CERTIFIED-ANY-2029", "platform": "Kalshi"},
            {"market_id": "P002", "ticker": "BWR-WARSH-APPOINT-FED_CHAIR-STD-ANY-2029", "platform": "Polymarket"},
            # Another matched pair for different_event testing
            {"market_id": "K003", "ticker": "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028", "platform": "Kalshi"},
            {"market_id": "P003", "ticker": "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028", "platform": "Polymarket"},
            # Single market (no match)
            {"market_id": "K004", "ticker": "BWR-DESANTIS-WIN-GOV_FL-CERTIFIED-ANY-2026", "platform": "Kalshi"},
        ]
    }
    return build_market_id_to_ticker(tickers)


# ──────────────────────────────────────────────────
# compute_precision_recall
# ──────────────────────────────────────────────────


class TestComputePrecisionRecall:
    def test_perfect_precision_recall(self):
        result = compute_precision_recall(tp=10, fp=0, fn=0)
        assert result["precision"] == 1.0
        assert result["recall"] == 1.0
        assert result["f1"] == 1.0

    def test_no_predictions(self):
        result = compute_precision_recall(tp=0, fp=0, fn=5)
        assert result["precision"] == 0.0
        assert result["recall"] == 0.0
        assert result["f1"] == 0.0

    def test_all_zeros(self):
        result = compute_precision_recall(tp=0, fp=0, fn=0)
        assert result["precision"] == 0.0
        assert result["recall"] == 0.0
        assert result["f1"] == 0.0

    def test_precision_only(self):
        """All predictions correct but many missed."""
        result = compute_precision_recall(tp=5, fp=0, fn=15)
        assert result["precision"] == 1.0
        assert result["recall"] == 0.25
        assert 0 < result["f1"] < 1

    def test_recall_only(self):
        """All positives found but many false positives."""
        result = compute_precision_recall(tp=10, fp=10, fn=0)
        assert result["precision"] == 0.5
        assert result["recall"] == 1.0

    def test_realistic_values(self):
        result = compute_precision_recall(tp=30, fp=2, fn=8)
        assert result["precision"] == pytest.approx(0.9375, abs=0.001)
        assert result["recall"] == pytest.approx(0.7895, abs=0.001)
        assert 0 < result["f1"] < 1


# ──────────────────────────────────────────────────
# evaluate_same_event_labels
# ──────────────────────────────────────────────────


class TestEvaluateSameEventLabels:
    def test_true_positive_same_ticker(self, ticker_lookup):
        """Human says same-event AND pipeline has same ticker → TP."""
        labels = [{
            "label_id": "hl_001",
            "label_type": "same_event_same_rules",
            "market_ids": ["K001", "P001"],
            "description": "Trump win",
        }]
        result = evaluate_same_event_labels(labels, ticker_lookup)
        assert len(result["true_positives"]) == 1
        assert len(result["false_negatives"]) == 0

    def test_false_negative_different_ticker(self, ticker_lookup):
        """Human says same-event BUT pipeline has different tickers → FN."""
        labels = [{
            "label_id": "hl_002",
            "label_type": "same_event_same_rules",
            "market_ids": ["K002", "P002"],
            "description": "Warsh Fed Chair",
        }]
        result = evaluate_same_event_labels(labels, ticker_lookup)
        assert len(result["true_positives"]) == 0
        assert len(result["false_negatives"]) == 1
        assert result["false_negatives"][0]["action_needed"] == "unify_tickers"

    def test_false_positive_different_event_same_ticker(self, ticker_lookup):
        """Human says different-event BUT pipeline has same ticker → FP."""
        labels = [{
            "label_id": "hl_003",
            "label_type": "different_event",
            "market_ids": ["K003", "P003"],
            "description": "Not same event",
        }]
        result = evaluate_same_event_labels(labels, ticker_lookup)
        assert len(result["false_positives"]) == 1
        assert result["false_positives"][0]["action_needed"] == "break_match"

    def test_true_negative_different_event_different_ticker(self, ticker_lookup):
        """Human says different-event AND pipeline has different tickers → TN."""
        labels = [{
            "label_id": "hl_004",
            "label_type": "different_event",
            "market_ids": ["K002", "P002"],
            "description": "Not same",
        }]
        result = evaluate_same_event_labels(labels, ticker_lookup)
        assert len(result["true_negatives"]) == 1

    def test_skips_non_matching_label_types(self, ticker_lookup):
        """Labels like not_political should be skipped."""
        labels = [{
            "label_id": "hl_005",
            "label_type": "not_political",
            "market_ids": ["K001"],
            "description": "Not political",
        }]
        result = evaluate_same_event_labels(labels, ticker_lookup)
        assert len(result["true_positives"]) == 0
        assert len(result["false_negatives"]) == 0

    def test_skips_single_market_labels(self, ticker_lookup):
        """Labels with only 1 market can't be evaluated for matching."""
        labels = [{
            "label_id": "hl_006",
            "label_type": "same_event_same_rules",
            "market_ids": ["K001"],
        }]
        result = evaluate_same_event_labels(labels, ticker_lookup)
        assert result["skipped"] == 1

    def test_skips_unknown_market_ids(self, ticker_lookup):
        """Markets not in ticker lookup should be skipped."""
        labels = [{
            "label_id": "hl_007",
            "label_type": "same_event_same_rules",
            "market_ids": ["UNKNOWN1", "UNKNOWN2"],
        }]
        result = evaluate_same_event_labels(labels, ticker_lookup)
        assert result["skipped"] == 1

    def test_same_event_different_rules_as_tp(self, ticker_lookup):
        """same_event_different_rules where pipeline matched → still counts as TP."""
        labels = [{
            "label_id": "hl_008",
            "label_type": "same_event_different_rules",
            "market_ids": ["K001", "P001"],
        }]
        result = evaluate_same_event_labels(labels, ticker_lookup)
        assert len(result["true_positives"]) == 1

    def test_multiple_labels(self, ticker_lookup):
        """Test evaluation with mix of TP, FN, FP."""
        labels = [
            {"label_id": "hl_a", "label_type": "same_event_same_rules",
             "market_ids": ["K001", "P001"]},  # TP
            {"label_id": "hl_b", "label_type": "same_event_same_rules",
             "market_ids": ["K002", "P002"]},  # FN
            {"label_id": "hl_c", "label_type": "different_event",
             "market_ids": ["K003", "P003"]},  # FP
        ]
        result = evaluate_same_event_labels(labels, ticker_lookup)
        assert len(result["true_positives"]) == 1
        assert len(result["false_negatives"]) == 1
        assert len(result["false_positives"]) == 1

    def test_empty_labels(self, ticker_lookup):
        result = evaluate_same_event_labels([], ticker_lookup)
        assert len(result["true_positives"]) == 0
        assert result["skipped"] == 0


# ──────────────────────────────────────────────────
# evaluate_category_labels
# ──────────────────────────────────────────────────


class TestEvaluateCategoryLabels:
    @pytest.fixture
    def category_lookup(self):
        return {
            "K001": "1. ELECTORAL",
            "K002": "16. NOT_POLITICAL",
            "K003": "3. LEGISLATIVE",
            "K004": "1. ELECTORAL",
        }

    def test_not_political_correct(self, category_lookup):
        """Pipeline already has NOT_POLITICAL → correct."""
        labels = [{
            "label_id": "hl_010",
            "label_type": "not_political",
            "market_ids": ["K002"],
        }]
        result = evaluate_category_labels(labels, category_lookup)
        assert len(result["correct"]) == 1
        assert len(result["incorrect"]) == 0

    def test_not_political_incorrect(self, category_lookup):
        """Pipeline has ELECTORAL but human says not political → incorrect."""
        labels = [{
            "label_id": "hl_011",
            "label_type": "not_political",
            "market_ids": ["K001"],
        }]
        result = evaluate_category_labels(labels, category_lookup)
        assert len(result["correct"]) == 0
        assert len(result["incorrect"]) == 1
        assert result["incorrect"][0]["action_needed"] == "mark_not_political"

    def test_wrong_category_always_incorrect(self, category_lookup):
        """Wrong-category labels are always logged as incorrect for review."""
        labels = [{
            "label_id": "hl_012",
            "label_type": "wrong_category",
            "market_ids": ["K003"],
            "description": "Should be APPOINTMENTS not LEGISLATIVE",
        }]
        result = evaluate_category_labels(labels, category_lookup)
        assert len(result["incorrect"]) == 1
        assert result["incorrect"][0]["action_needed"] == "review_category"

    def test_skips_unknown_market(self, category_lookup):
        labels = [{
            "label_id": "hl_013",
            "label_type": "not_political",
            "market_ids": ["UNKNOWN"],
        }]
        result = evaluate_category_labels(labels, category_lookup)
        assert result["skipped"] == 1

    def test_multiple_markets_in_label(self, category_lookup):
        """Not-political label with multiple markets."""
        labels = [{
            "label_id": "hl_014",
            "label_type": "not_political",
            "market_ids": ["K001", "K002"],
        }]
        result = evaluate_category_labels(labels, category_lookup)
        # K001 is ELECTORAL (incorrect), K002 is NOT_POLITICAL (correct)
        assert len(result["correct"]) == 1
        assert len(result["incorrect"]) == 1

    def test_ignores_same_event_labels(self, category_lookup):
        labels = [{
            "label_id": "hl_015",
            "label_type": "same_event_same_rules",
            "market_ids": ["K001", "K002"],
        }]
        result = evaluate_category_labels(labels, category_lookup)
        assert len(result["correct"]) == 0
        assert len(result["incorrect"]) == 0


# ──────────────────────────────────────────────────
# generate_report
# ──────────────────────────────────────────────────


class TestGenerateReport:
    def test_report_structure(self):
        same_event_eval = {
            "true_positives": [{"label_id": "a"}],
            "false_negatives": [{"label_id": "b", "market_ids": ["K1", "P1"],
                                 "pipeline_tickers": ["T1", "T2"], "action_needed": "unify"}],
            "false_positives": [],
            "true_negatives": [],
            "skipped": 2,
        }
        category_eval = {
            "correct": [{"label_id": "c"}],
            "incorrect": [{"label_id": "d", "action_needed": "review"}],
            "skipped": 0,
        }

        report = generate_report(same_event_eval, category_eval)

        assert "generated_at" in report
        # sample_size = tp + fp + fn + tn = 1 + 0 + 1 + 0 = 2
        assert report["sample_size"] == 2
        assert report["matching"]["true_positives"] == 1
        assert report["matching"]["false_negatives"] == 1
        assert report["matching"]["precision"] == 1.0  # 1/(1+0)
        assert report["matching"]["recall"] == 0.5  # 1/(1+1)
        assert report["matching_skipped"] == 2
        assert report["categories"]["correct"] == 1
        assert report["categories"]["incorrect"] == 1
        assert len(report["disagreements"]) == 2  # 1 FN + 1 category incorrect

    def test_empty_report(self):
        empty_eval = {"true_positives": [], "false_negatives": [],
                      "false_positives": [], "true_negatives": [], "skipped": 0}
        empty_cat = {"correct": [], "incorrect": [], "skipped": 0}

        report = generate_report(empty_eval, empty_cat)
        assert report["sample_size"] == 0
        assert report["matching"]["precision"] == 0.0
        assert len(report["disagreements"]) == 0

    def test_disagreements_include_human_judgment(self):
        same_event_eval = {
            "true_positives": [],
            "false_negatives": [{"label_id": "fn1"}],
            "false_positives": [{"label_id": "fp1"}],
            "true_negatives": [],
            "skipped": 0,
        }
        category_eval = {"correct": [], "incorrect": [], "skipped": 0}

        report = generate_report(same_event_eval, category_eval)
        judgments = {d["human_judgment"] for d in report["disagreements"]}
        assert "same event" in judgments
        assert "different event" in judgments


# ──────────────────────────────────────────────────
# build_market_id_to_ticker
# ──────────────────────────────────────────────────


class TestBuildMarketIdToTicker:
    def test_basic_lookup(self):
        data = {"tickers": [
            {"market_id": "K001", "ticker": "BWR-TEST"},
            {"market_id": "P001", "ticker": "BWR-TEST2"},
        ]}
        lookup = build_market_id_to_ticker(data)
        assert "K001" in lookup
        assert lookup["K001"]["ticker"] == "BWR-TEST"

    def test_empty(self):
        assert build_market_id_to_ticker({"tickers": []}) == {}

    def test_missing_market_id_skipped(self):
        data = {"tickers": [{"ticker": "BWR-TEST"}]}
        lookup = build_market_id_to_ticker(data)
        assert len(lookup) == 0

    def test_string_coercion(self):
        """market_id should be coerced to string."""
        data = {"tickers": [{"market_id": 12345, "ticker": "BWR-TEST"}]}
        lookup = build_market_id_to_ticker(data)
        assert "12345" in lookup
