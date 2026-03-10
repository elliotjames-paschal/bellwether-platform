"""
Tests for generate_ticker_corrections.py

Run: pytest packages/pipelines/tests/test_ticker_corrections.py -v
"""

import pytest
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generate_ticker_corrections import (
    classify_disagreement,
    analyze_error_patterns,
    generate_corrections,
    parse_ticker_components,
    MIN_FREQUENCY_DEFAULT,
    MIN_FREQUENCY_BY_TYPE,
)


# ──────────────────────────────────────────────────
# classify_disagreement
# ──────────────────────────────────────────────────


class TestClassifyDisagreement:
    def test_mechanism_mismatch(self):
        result = classify_disagreement([
            "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
            "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028",
        ])
        assert result["error_type"] == "mechanism_mismatch"
        assert "mechanism" in result["details"]
        assert result["details"]["mechanism"] == ("CERTIFIED", "STD")

    def test_agent_variant(self):
        result = classify_disagreement([
            "BWR-POWELL-CUT-RATE-STD-ANY-2026",
            "BWR-J_POWELL-CUT-RATE-STD-ANY-2026",
        ])
        assert result["error_type"] == "agent_variant"
        assert result["details"]["agent"] == ("POWELL", "J_POWELL")

    def test_target_variant(self):
        result = classify_disagreement([
            "BWR-DEM-CONTROL-SENATE_US-STD-ANY-2026",
            "BWR-DEM-CONTROL-SENATE-STD-ANY-2026",
        ])
        assert result["error_type"] == "target_variant"

    def test_threshold_mismatch(self):
        result = classify_disagreement([
            "BWR-FED-CUT-RATE-STD-GT_25BPS-2026",
            "BWR-FED-CUT-RATE-STD-ANY-2026",
        ])
        assert result["error_type"] == "threshold_mismatch"

    def test_timeframe_mismatch(self):
        result = classify_disagreement([
            "BWR-TRUMP-WIN-PRES_US-CERT-ANY-2028",
            "BWR-TRUMP-WIN-PRES_US-CERT-ANY-2032",
        ])
        assert result["error_type"] == "timeframe_mismatch"

    def test_resolution_mismatch(self):
        """Both mechanism AND threshold differ."""
        result = classify_disagreement([
            "BWR-FED-CUT-RATE-CERTIFIED-GT_25BPS-2026",
            "BWR-FED-CUT-RATE-STD-ANY-2026",
        ])
        assert result["error_type"] == "resolution_mismatch"

    def test_multi_field(self):
        """Agent + target + mechanism all differ."""
        result = classify_disagreement([
            "BWR-TRUMP-WIN-PRES_US-CERT-ANY-2028",
            "BWR-HARRIS-WIN-GOV_FL-STD-ANY-2028",
        ])
        assert result["error_type"] == "multi_field"

    def test_identical_tickers(self):
        result = classify_disagreement([
            "BWR-TRUMP-WIN-PRES_US-CERT-ANY-2028",
            "BWR-TRUMP-WIN-PRES_US-CERT-ANY-2028",
        ])
        assert result["error_type"] == "identical"

    def test_single_ticker(self):
        result = classify_disagreement(["BWR-TRUMP-WIN-PRES_US-CERT-ANY-2028"])
        assert result["error_type"] == "unknown"

    def test_empty(self):
        result = classify_disagreement([])
        assert result["error_type"] == "unknown"


# ──────────────────────────────────────────────────
# analyze_error_patterns
# ──────────────────────────────────────────────────


class TestAnalyzeErrorPatterns:
    def test_generates_mechanism_correction(self):
        """Two mechanism mismatches of the same pair should generate a rule."""
        report = {
            "disagreements": [
                {
                    "label_id": "hl_001",
                    "human_judgment": "same event",
                    "pipeline_tickers": [
                        "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
                        "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028",
                    ],
                },
                {
                    "label_id": "hl_002",
                    "human_judgment": "same event",
                    "pipeline_tickers": [
                        "BWR-HARRIS-WIN-PRES_US-CERTIFIED-ANY-2028",
                        "BWR-HARRIS-WIN-PRES_US-STD-ANY-2028",
                    ],
                },
            ]
        }
        corrections = analyze_error_patterns(report)
        assert len(corrections) == 1
        assert corrections[0]["type"] == "mechanism_alias"
        assert corrections[0]["frequency"] == 2
        assert set(corrections[0]["source_labels"]) == {"hl_001", "hl_002"}

    def test_below_min_frequency_excluded(self):
        """Single occurrence should not generate a rule (MIN_FREQUENCY=2)."""
        report = {
            "disagreements": [
                {
                    "label_id": "hl_003",
                    "human_judgment": "same event",
                    "pipeline_tickers": [
                        "BWR-WARSH-APPOINT-FED-CERT-ANY-2029",
                        "BWR-WARSH-APPOINT-FED-STD-ANY-2029",
                    ],
                },
            ]
        }
        corrections = analyze_error_patterns(report)
        assert len(corrections) == 0

    def test_generates_agent_correction(self):
        report = {
            "disagreements": [
                {
                    "label_id": "hl_a",
                    "human_judgment": "same event",
                    "pipeline_tickers": [
                        "BWR-POWELL-CUT-RATE-STD-ANY-2026",
                        "BWR-J_POWELL-CUT-RATE-STD-ANY-2026",
                    ],
                },
                {
                    "label_id": "hl_b",
                    "human_judgment": "same event",
                    "pipeline_tickers": [
                        "BWR-POWELL-RAISE-RATE-STD-ANY-2026",
                        "BWR-J_POWELL-RAISE-RATE-STD-ANY-2026",
                    ],
                },
            ]
        }
        corrections = analyze_error_patterns(report, min_frequency=2)
        assert len(corrections) == 1
        assert corrections[0]["type"] == "agent_alias"
        assert set([corrections[0]["from"], corrections[0]["to"]]) == {"POWELL", "J_POWELL"}

    def test_ignores_category_disagreements(self):
        """Category disagreements should not generate ticker corrections."""
        report = {
            "disagreements": [
                {
                    "label_id": "hl_c",
                    "human_judgment": "category mismatch",
                    "current_category": "1. ELECTORAL",
                },
                {
                    "label_id": "hl_d",
                    "human_judgment": "category mismatch",
                    "current_category": "3. LEGISLATIVE",
                },
            ]
        }
        corrections = analyze_error_patterns(report)
        assert len(corrections) == 0

    def test_empty_report(self):
        corrections = analyze_error_patterns({})
        assert corrections == []

    def test_no_false_negatives(self):
        report = {"disagreements": []}
        corrections = analyze_error_patterns(report)
        assert corrections == []

    def test_multi_field_errors_not_correctable(self):
        """Multi-field errors can't produce simple alias rules."""
        report = {
            "disagreements": [
                {
                    "label_id": "hl_e",
                    "human_judgment": "same event",
                    "pipeline_tickers": [
                        "BWR-TRUMP-WIN-PRES_US-CERT-ANY-2028",
                        "BWR-HARRIS-LOSE-GOV_FL-STD-ANY-2032",
                    ],
                },
                {
                    "label_id": "hl_f",
                    "human_judgment": "same event",
                    "pipeline_tickers": [
                        "BWR-A-WIN-X-CERT-ANY-2028",
                        "BWR-B-LOSE-Y-STD-ANY-2032",
                    ],
                },
            ]
        }
        corrections = analyze_error_patterns(report)
        assert len(corrections) == 0  # multi_field errors don't produce rules

    def test_sorted_by_frequency(self):
        """Corrections should be sorted by frequency descending."""
        report = {
            "disagreements": [
                # 3x mechanism mismatch
                {"label_id": "h1", "human_judgment": "same event",
                 "pipeline_tickers": ["BWR-A-W-P-CERT-ANY-28", "BWR-A-W-P-STD-ANY-28"]},
                {"label_id": "h2", "human_judgment": "same event",
                 "pipeline_tickers": ["BWR-B-W-P-CERT-ANY-28", "BWR-B-W-P-STD-ANY-28"]},
                {"label_id": "h3", "human_judgment": "same event",
                 "pipeline_tickers": ["BWR-C-W-P-CERT-ANY-28", "BWR-C-W-P-STD-ANY-28"]},
                # 2x agent variant
                {"label_id": "h4", "human_judgment": "same event",
                 "pipeline_tickers": ["BWR-X-W-P-STD-ANY-28", "BWR-Y-W-P-STD-ANY-28"]},
                {"label_id": "h5", "human_judgment": "same event",
                 "pipeline_tickers": ["BWR-X-L-Q-STD-ANY-28", "BWR-Y-L-Q-STD-ANY-28"]},
            ]
        }
        corrections = analyze_error_patterns(report, min_frequency=2)
        assert len(corrections) == 2
        assert corrections[0]["frequency"] >= corrections[1]["frequency"]


# ──────────────────────────────────────────────────
# generate_corrections
# ──────────────────────────────────────────────────


class TestGenerateCorrections:
    def test_output_structure(self):
        report = {"sample_size": 10, "disagreements": []}
        result = generate_corrections(report)
        assert "generated_at" in result
        assert result["source_label_count"] == 10
        assert result["correction_count"] == 0
        assert result["corrections"] == []

    def test_with_corrections(self):
        report = {
            "sample_size": 5,
            "disagreements": [
                {"label_id": "h1", "human_judgment": "same event",
                 "pipeline_tickers": ["BWR-A-W-P-CERT-ANY-28", "BWR-A-W-P-STD-ANY-28"]},
                {"label_id": "h2", "human_judgment": "same event",
                 "pipeline_tickers": ["BWR-B-W-P-CERT-ANY-28", "BWR-B-W-P-STD-ANY-28"]},
            ]
        }
        result = generate_corrections(report)
        assert result["correction_count"] == 1
        assert result["corrections"][0]["type"] == "mechanism_alias"
