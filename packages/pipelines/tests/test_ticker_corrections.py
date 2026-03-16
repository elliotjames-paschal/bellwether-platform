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
    analyze_false_positive_patterns,
    generate_disambiguations,
    identify_collapsed_field,
    identify_collapsed_fields,
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


# ──────────────────────────────────────────────────
# identify_collapsed_field
# ──────────────────────────────────────────────────


class TestIdentifyCollapsedField:
    def test_threshold_any(self):
        result = identify_collapsed_field("BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        assert result == "threshold"

    def test_timeframe_year_only(self):
        result = identify_collapsed_field("BWR-FED-CUT-RATES-STD-25BPS-2026")
        assert result == "timeframe"

    def test_specific_threshold_and_timeframe(self):
        """Threshold and timeframe are specific, but mechanism STD is still a collapse."""
        result = identify_collapsed_field("BWR-FED-CUT-RATES-STD-25BPS-2026_MAR")
        # threshold is 25BPS (not ANY), timeframe has month → those are fine
        # but mechanism=STD is a collapse indicator
        assert result == "mechanism"

    def test_threshold_takes_priority(self):
        """If threshold is ANY, that's the collapsed field even if timeframe is a year."""
        result = identify_collapsed_field("BWR-X-W-Y-STD-ANY-2026")
        assert result == "threshold"


# ──────────────────────────────────────────────────
# identify_collapsed_fields (plural — multi-field version)
# ──────────────────────────────────────────────────


class TestIdentifyCollapsedFields:
    def test_threshold_any(self):
        result = identify_collapsed_fields("BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        assert "threshold" in result

    def test_timeframe_year_only(self):
        result = identify_collapsed_fields("BWR-FED-CUT-RATES-CERTIFIED-25BPS-2026")
        assert "timeframe" in result

    def test_mechanism_std(self):
        result = identify_collapsed_fields("BWR-FED-CUT-RATES-STD-25BPS-2026")
        assert "mechanism" in result

    def test_agent_bare_lastname(self):
        """POWELL is in NAME_COLLISIONS, so it should be flagged."""
        result = identify_collapsed_fields("BWR-POWELL-CUT-BONDS-CERTIFIED-25BPS-2028_Q1")
        assert "agent" in result

    def test_target_ambiguous(self):
        """RATE is in _AMBIGUOUS_TARGETS."""
        result = identify_collapsed_fields("BWR-FED-CUT-RATE-CERTIFIED-25BPS-2028_Q1")
        assert "target" in result

    def test_multi_field_collapse(self):
        """Ticker with multiple generic fields returns all of them."""
        result = identify_collapsed_fields("BWR-POWELL-CUT-RATE-STD-ANY-2026")
        assert "threshold" in result
        assert "timeframe" in result
        assert "mechanism" in result
        assert "agent" in result
        assert "target" in result

    def test_no_collapse_specific_ticker(self):
        """Fully specific ticker returns empty list."""
        result = identify_collapsed_fields("BWR-J_POWELL-CUT-FED_FUNDS-ANY_MEETING-25BPS-2026_Q1")
        assert result == []

    def test_backward_compat_wrapper(self):
        """Singular version returns first collapsed field."""
        result = identify_collapsed_field("BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028")
        assert result == "threshold"

    def test_backward_compat_empty(self):
        """Singular version returns '' when nothing collapsed."""
        result = identify_collapsed_field("BWR-J_POWELL-CUT-FED_FUNDS-ANY_MEETING-25BPS-2026_Q1")
        assert result == ""


# ──────────────────────────────────────────────────
# analyze_false_positive_patterns (unit tests)
# ──────────────────────────────────────────────────


class TestAnalyzeFalsePositivePatterns:
    def test_groups_by_agent_action_target(self):
        """Two false positives with same agent/action/target/threshold should group."""
        report = {
            "disagreements": [
                {
                    "label_id": "fp1",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": ["BWR-X-W-Y-STD-ANY-2028", "BWR-X-W-Y-STD-ANY-2028"],
                },
                {
                    "label_id": "fp2",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": ["BWR-X-W-Y-STD-ANY-2028", "BWR-X-W-Y-STD-ANY-2028"],
                },
            ],
        }
        rules, unresolvable = analyze_false_positive_patterns(report, min_frequency=2)
        # Should produce threshold_disambiguation (ANY) + mechanism (STD) + timeframe (2028)
        threshold_rules = [r for r in rules if r["type"] == "threshold_disambiguation"]
        assert len(threshold_rules) == 1
        assert threshold_rules[0]["frequency"] == 2
        assert set(threshold_rules[0]["source_labels"]) == {"fp1", "fp2"}

    def test_different_patterns_separate_rules(self):
        """False positives with different agent/action/target should be separate rules."""
        report = {
            "disagreements": [
                {
                    "label_id": "fp1",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": ["BWR-A-W-X-STD-ANY-2028"] * 2,
                },
                {
                    "label_id": "fp2",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": ["BWR-A-W-X-STD-ANY-2028"] * 2,
                },
                {
                    "label_id": "fp3",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": ["BWR-B-W-Z-STD-ANY-2028"] * 2,
                },
                {
                    "label_id": "fp4",
                    "human_judgment": "different event",
                    "action_needed": "break_match",
                    "pipeline_tickers": ["BWR-B-W-Z-STD-ANY-2028"] * 2,
                },
            ],
        }
        rules, _ = analyze_false_positive_patterns(report, min_frequency=2)
        # Each pattern (A/W/X and B/W/Z) should produce at least threshold + mechanism rules
        threshold_rules = [r for r in rules if r["type"] == "threshold_disambiguation"]
        assert len(threshold_rules) == 2

    def test_ignores_non_break_match(self):
        """Disagreements without action_needed='break_match' should be ignored."""
        report = {
            "disagreements": [
                {
                    "label_id": "fp1",
                    "human_judgment": "different event",
                    "action_needed": "something_else",
                    "pipeline_tickers": ["BWR-X-W-Y-STD-ANY-2028"] * 2,
                },
            ],
        }
        rules, unresolvable = analyze_false_positive_patterns(report, min_frequency=1)
        assert rules == []
        assert unresolvable == 0

    def test_mechanism_disambiguation_generated(self):
        """FPs with mechanism=STD should generate mechanism_disambiguation."""
        # Use specific threshold and non-bare-year timeframe so only mechanism triggers
        report = {
            "disagreements": [
                {"label_id": f"fp{i}", "human_judgment": "different event",
                 "action_needed": "break_match",
                 "pipeline_tickers": ["BWR-SMITH-W-Y-STD-25BPS-2028_Q1"] * 2}
                for i in range(2)
            ]
        }
        rules, _ = analyze_false_positive_patterns(report, min_frequency=2)
        types = [r["type"] for r in rules]
        assert "mechanism_disambiguation" in types
        mech_rules = [r for r in rules if r["type"] == "mechanism_disambiguation"]
        assert mech_rules[0]["pattern"]["mechanism"] == "STD"

    def test_agent_disambiguation_high_threshold(self):
        """agent_disambiguation needs 5 observations (same as agent_alias)."""
        report = {
            "disagreements": [
                {"label_id": f"fp{i}", "human_judgment": "different event",
                 "action_needed": "break_match",
                 "pipeline_tickers": ["BWR-POWELL-CUT-BONDS-CERTIFIED-25BPS-2028_Q1"] * 2}
                for i in range(4)  # only 4, below threshold of 5
            ]
        }
        rules, _ = analyze_false_positive_patterns(report)
        agent_rules = [r for r in rules if r["type"] == "agent_disambiguation"]
        assert len(agent_rules) == 0

    def test_agent_disambiguation_at_threshold(self):
        """agent_disambiguation fires at exactly 5 observations."""
        report = {
            "disagreements": [
                {"label_id": f"fp{i}", "human_judgment": "different event",
                 "action_needed": "break_match",
                 "pipeline_tickers": ["BWR-POWELL-CUT-BONDS-CERTIFIED-25BPS-2028_Q1"] * 2}
                for i in range(5)
            ]
        }
        rules, _ = analyze_false_positive_patterns(report)
        agent_rules = [r for r in rules if r["type"] == "agent_disambiguation"]
        assert len(agent_rules) == 1

    def test_multi_field_produces_multiple_rules(self):
        """A ticker with threshold=ANY and bare-year timeframe produces both rules."""
        report = {
            "disagreements": [
                {"label_id": f"fp{i}", "human_judgment": "different event",
                 "action_needed": "break_match",
                 "pipeline_tickers": ["BWR-SMITH-W-BONDS-CERTIFIED-ANY-2026"] * 2}
                for i in range(2)
            ]
        }
        rules, _ = analyze_false_positive_patterns(report, min_frequency=2)
        types = {r["type"] for r in rules}
        assert "threshold_disambiguation" in types
        assert "timeframe_disambiguation" in types

    def test_unresolvable_fp_counted(self):
        """Fully specific tickers produce no rules but count as unresolvable."""
        report = {
            "disagreements": [
                {"label_id": "fp1", "human_judgment": "different event",
                 "action_needed": "break_match",
                 "pipeline_tickers": ["BWR-J_POWELL-CUT-FED_FUNDS-ANY_MEETING-25BPS-2026_Q1"] * 2}
                for _ in range(5)
            ]
        }
        rules, unresolvable = analyze_false_positive_patterns(report, min_frequency=1)
        assert len(rules) == 0
        assert unresolvable == 5


# ──────────────────────────────────────────────────
# generate_disambiguations
# ──────────────────────────────────────────────────


class TestGenerateDisambiguations:
    def test_empty_report(self):
        result = generate_disambiguations({"disagreements": [], "sample_size": 0})
        assert result["disambiguation_count"] == 0
        assert result["disambiguations"] == []

    def test_includes_batch_id(self):
        result = generate_disambiguations(
            {"disagreements": [], "sample_size": 0},
            batch_id="batch_test"
        )
        assert result["batch_id"] == "batch_test"
