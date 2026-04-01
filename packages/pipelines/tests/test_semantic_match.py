"""
Tests for semantic_match package (ported from event-standardization).

Run: pytest packages/pipelines/tests/test_semantic_match.py -v
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from semantic_match.semantic_features import build_semantic_features
from semantic_match.verify import verify_pair, VerificationResult
from semantic_match.temporal import classify_temporal_relation
from semantic_match.entity_keys import extract_entity_keys
from semantic_match.profile_adapter import build_profile_from_market


# ──────────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────────

def _make_profile(title, rules="", entities=None, category="politics",
                  close_time=None, market_type=None):
    """Build a profile dict with semantic features attached."""
    profile = {
        "title_norm": title,
        "title": title,
        "rules_primary": rules,
        "rules": rules,
        "entity_keys": entities or [],
        "canonical_category_lvl1": category,
        "market_type": market_type,
        "start_ts": None,
        "end_ts": close_time,
        "event_ts": None,
    }
    profile["sem_features"] = build_semantic_features(profile)
    return profile


# ──────────────────────────────────────────────────
# Entity Key Extraction
# ──────────────────────────────────────────────────

class TestExtractEntityKeys:
    def test_known_tickers(self):
        keys = extract_entity_keys("Will the Fed raise rates?")
        assert "fed" in keys

    def test_capitalized_names(self):
        keys = extract_entity_keys("Will Donald Trump win?")
        assert "donald_trump" in keys or "trump" in keys

    def test_blacklisted_words_excluded(self):
        keys = extract_entity_keys("Will the March deadline pass?")
        # "March" is blacklisted
        assert "march" not in keys

    def test_empty_input(self):
        assert extract_entity_keys("") == []
        assert extract_entity_keys(None) == []


# ──────────────────────────────────────────────────
# Semantic Feature Extraction
# ──────────────────────────────────────────────────

class TestBuildSemanticFeatures:
    def test_winner_detection(self):
        profile = _make_profile("Will Trump win the 2024 presidential election?",
                                entities=["trump", "presidential_election"])
        sem = profile["sem_features"]
        assert sem["proposition_type"] == "winner"

    def test_numeric_threshold(self):
        profile = _make_profile("Will CPI be above 3.5% in March?",
                                entities=["cpi"])
        sem = profile["sem_features"]
        assert 3.5 in sem["numeric_thresholds"]

    def test_crypto_asset(self):
        profile = _make_profile("Will Bitcoin reach $60,000?",
                                category="crypto")
        sem = profile["sem_features"]
        assert sem["asset_ticker"] == "BTC"

    def test_resolution_source(self):
        profile = _make_profile("What will CPI be?",
                                rules="Resolves based on BLS data release")
        sem = profile["sem_features"]
        assert "bls" in sem["resolution_source"]

    def test_reach_level_detection(self):
        profile = _make_profile("Will BTC reach $100k?", category="crypto")
        sem = profile["sem_features"]
        assert sem["proposition_type"] == "reach_level"

    def test_empty_profile(self):
        profile = _make_profile("")
        sem = profile["sem_features"]
        assert sem["proposition_type"] == "unknown"
        assert sem["entities"] == []


# ──────────────────────────────────────────────────
# Pair Verification
# ──────────────────────────────────────────────────

class TestVerifyPair:
    def test_equivalent_same_event(self):
        a = _make_profile("Will Trump win 2024 election?",
                          entities=["trump", "2024", "election"])
        b = _make_profile("Trump to win 2024 presidential election",
                          entities=["trump", "2024", "election", "presidential"])
        result = verify_pair(a, b)
        assert result.relation == "equivalent"
        assert result.confidence > 0.7

    def test_independent_different_events(self):
        a = _make_profile("Will CPI exceed 3%?", entities=["cpi"])
        b = _make_profile("Will Lakers win NBA finals?", entities=["lakers", "nba"])
        result = verify_pair(a, b)
        assert result.relation == "independent"

    def test_disjoint_entities_trump_vs_biden(self):
        a = _make_profile("Will Trump win 2024?",
                          entities=["trump", "2024", "election"],
                          market_type="winner")
        b = _make_profile("Will Biden win 2024?",
                          entities=["biden", "2024", "election"],
                          market_type="winner")
        result = verify_pair(a, b)
        assert result.relation in ("mutually_exclusive", "independent")

    def test_numeric_threshold_mismatch(self):
        a = _make_profile("Will CPI be above 3.0%?", entities=["cpi", "3.0"])
        b = _make_profile("Will CPI be above 3.5%?", entities=["cpi", "3.5"])
        result = verify_pair(a, b)
        # Different thresholds should not be equivalent
        assert result.relation != "equivalent" or result.confidence < 0.85

    def test_rule_mismatch_core_vs_headline(self):
        a = _make_profile("What will CPI be?",
                          rules="Resolves based on core CPI",
                          entities=["cpi"])
        b = _make_profile("What will CPI be?",
                          rules="Resolves based on headline CPI",
                          entities=["cpi"])
        result = verify_pair(a, b)
        assert result.relation in ("aligned_rule_mismatch", "independent", "overlap")

    def test_returns_verification_result(self):
        a = _make_profile("Test market A", entities=["test"])
        b = _make_profile("Test market B", entities=["test"])
        result = verify_pair(a, b)
        assert isinstance(result, VerificationResult)
        assert 0.0 <= result.confidence <= 1.0
        assert isinstance(result.evidence, dict)


# ──────────────────────────────────────────────────
# Profile Adapter
# ──────────────────────────────────────────────────

class TestProfileAdapter:
    def test_build_from_question(self):
        profile = build_profile_from_market(
            question="Will Trump win the 2028 Presidential Election?",
            rules="Resolves Yes if Trump wins the certified Electoral College vote.",
            ticker="BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
        )
        assert "title_norm" in profile
        assert "sem_features" in profile
        assert profile["sem_features"]["proposition_type"] == "winner"

    def test_build_with_empty_rules(self):
        profile = build_profile_from_market(
            question="Will Bitcoin reach $100k?",
            rules="",
            category="crypto",
        )
        assert profile["sem_features"]["asset_ticker"] == "BTC"

    def test_entities_extracted_from_question(self):
        profile = build_profile_from_market(
            question="Will the Fed raise interest rates in June?",
        )
        assert len(profile["entity_keys"]) > 0

    def test_category_default(self):
        profile = build_profile_from_market(question="Test")
        assert profile["canonical_category_lvl1"] == "politics"


# ──────────────────────────────────────────────────
# Temporal Classification
# ──────────────────────────────────────────────────

class TestTemporalRelation:
    def test_same_close_time(self):
        a = {"start_ts": "2026-01-01T00:00:00Z", "end_ts": "2026-11-03T00:00:00Z"}
        b = {"start_ts": "2026-01-01T00:00:00Z", "end_ts": "2026-11-03T00:00:00Z"}
        rel = classify_temporal_relation(a, b)
        assert rel == "time_equivalent"

    def test_unknown_when_missing(self):
        a = {}
        b = {}
        rel = classify_temporal_relation(a, b)
        assert rel == "time_unknown"

    def test_disjoint_far_apart(self):
        a = {"start_ts": "2024-01-01", "end_ts": "2024-01-31"}
        b = {"start_ts": "2026-06-01", "end_ts": "2026-06-30"}
        rel = classify_temporal_relation(a, b)
        assert rel == "time_disjoint"


# ──────────────────────────────────────────────────
# Import Isolation
# ──────────────────────────────────────────────────

class TestImportIsolation:
    def test_no_heavy_deps(self):
        """Verify semantic_match doesn't pull in torch/sentence_transformers."""
        import importlib
        # pandas is expected (used by temporal.py), but torch should not be
        assert "torch" not in sys.modules
        assert "sentence_transformers" not in sys.modules


# ──────────────────────────────────────────────────
# Golden Fixture Tests
# ──────────────────────────────────────────────────

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

class TestGoldenPairs:
    @pytest.fixture(scope="class")
    def golden_pairs(self):
        fixture_path = FIXTURES_DIR / "prefilter_golden_pairs.json"
        if not fixture_path.exists():
            pytest.skip("Golden fixture not found")
        import json
        with open(fixture_path) as f:
            return json.load(f)

    def test_golden_pairs_produce_expected_relations(self, golden_pairs):
        """Each golden pair should produce the expected relation type."""
        failures = []
        for i, pair in enumerate(golden_pairs):
            profile_a = build_profile_from_market(
                question=pair["kalshi_question"],
                rules=pair.get("kalshi_rules", ""),
            )
            profile_b = build_profile_from_market(
                question=pair["poly_question"],
                rules=pair.get("poly_rules", ""),
            )
            result = verify_pair(profile_a, profile_b)
            expected = pair["expected_relation"]

            # Allow some flexibility for related relation types
            acceptable = {expected}
            if expected == "mutually_exclusive":
                acceptable.add("independent")
            if expected == "independent":
                acceptable.add("mutually_exclusive")
                acceptable.add("uncertain")  # uncertain with low confidence is safe (defers to GPT)

            if result.relation not in acceptable:
                failures.append(
                    f"  Pair {i} ({pair.get('description', '')}): "
                    f"expected {expected}, got {result.relation} "
                    f"(confidence={result.confidence:.2f})"
                )

            # Check minimum confidence if specified
            min_conf = pair.get("min_confidence")
            if min_conf and result.relation == expected and result.confidence < min_conf:
                failures.append(
                    f"  Pair {i} ({pair.get('description', '')}): "
                    f"correct relation but confidence {result.confidence:.2f} < {min_conf}"
                )

        if failures:
            pytest.fail(f"Golden pair failures:\n" + "\n".join(failures))
