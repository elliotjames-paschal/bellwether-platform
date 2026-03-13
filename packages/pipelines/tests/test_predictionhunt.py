"""
Tests for PredictionHunt integration.

Covers:
- predictionhunt_client.py: ticker derivation, response parsing, budget tracking
- pipeline_validate_with_predictionhunt.py: classification logic
- pipeline_update_matches.py: gate_with_predictionhunt() integration

Run: pytest packages/pipelines/tests/test_predictionhunt.py -v
"""

import pytest
import json
import sys
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ──────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────


def make_ph_response(groups, success=True):
    """Build a PredictionHunt API response."""
    return {
        "success": success,
        "count": 1 if success and groups else 0,
        "events": [{
            "title": "Test Event",
            "event_date": "2028-11-05",
            "event_type": "election",
            "groups": groups,
        }] if groups else [],
    }


def make_group(title, markets):
    """Build a PH group with markets."""
    return {"title": title, "markets": markets}


def make_market(mid, source):
    """Build a PH market entry."""
    return {"id": mid, "source": source, "source_url": f"https://{source}.com/{mid}"}


def make_ticker(market_id, platform, ticker_str, mechanism="CERTIFIED", threshold="ANY"):
    """Build a ticker entry for tickers_postprocessed.json."""
    parts = ticker_str.replace("BWR-", "").split("-")
    return {
        "market_id": market_id,
        "platform": platform,
        "ticker": ticker_str,
        "agent": parts[0] if len(parts) > 0 else "UNKNOWN",
        "action": parts[1] if len(parts) > 1 else "UNKNOWN",
        "target": parts[2] if len(parts) > 2 else "UNKNOWN",
        "mechanism": mechanism,
        "threshold": threshold,
        "timeframe": parts[-1] if len(parts) > 3 else "UNKNOWN",
        "original_question": f"Question for {market_id}",
    }


CANDIDATES_FIXTURE = {
    "generated_at": "2026-03-12T00:00:00",
    "threshold": 0.7,
    "bucket_a": [],
    "bucket_b": [
        {
            "pair_key": "K1|P1",
            "kalshi_market_id": "K1",
            "poly_market_id": "P1",
            "kalshi_ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
            "poly_ticker": "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028",
            "kalshi_question": "Will Trump win the 2028 presidential election?",
            "poly_question": "Trump wins 2028 presidency?",
            "cosine_similarity": 0.93,
            "bucket": "B",
            "reason": "same_event_different_resolution",
            "diffs": ["mechanism: CERTIFIED vs STD"],
        },
        {
            "pair_key": "K2|P2",
            "kalshi_market_id": "K2",
            "poly_market_id": "P2",
            "kalshi_ticker": "BWR-FED-CUT-FFR-ANY_MEETING-25BPS-2026",
            "poly_ticker": "BWR-FED-CUT-FFR-STD-25BPS-2026",
            "kalshi_question": "Will the Fed cut rates by 25bps?",
            "poly_question": "Federal Reserve 25bps rate cut in 2026?",
            "cosine_similarity": 0.88,
            "bucket": "B",
            "reason": "same_event_different_resolution",
            "diffs": ["mechanism: ANY_MEETING vs STD"],
        },
        {
            "pair_key": "K3|P3",
            "kalshi_market_id": "K3",
            "poly_market_id": "P3",
            "kalshi_ticker": "BWR-DEM-WIN-GOV_CA-CERTIFIED-ANY-2026",
            "poly_ticker": "BWR-DEM-WIN-GOV_CA-STD-ANY-2026",
            "kalshi_question": "Will a Democrat win CA governor?",
            "poly_question": "Democratic CA governor 2026?",
            "cosine_similarity": 0.85,
            "bucket": "B",
            "reason": "same_event_different_resolution",
            "diffs": ["mechanism: CERTIFIED vs STD"],
        },
    ],
    "bucket_c": [],
    "stats": {},
}

VERDICTS_FIXTURE = {
    "verdicts": [
        {
            "pair_key": "K1|P1",
            "kalshi_market_id": "K1",
            "poly_market_id": "P1",
            "verdict": "IDENTICAL",
            "correct_ticker": "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028",
            "cosine_similarity": 0.93,
        },
        {
            "pair_key": "K2|P2",
            "kalshi_market_id": "K2",
            "poly_market_id": "P2",
            "verdict": "IDENTICAL",
            "correct_ticker": "BWR-FED-CUT-FFR-ANY_MEETING-25BPS-2026",
            "cosine_similarity": 0.88,
        },
        {
            "pair_key": "K3|P3",
            "kalshi_market_id": "K3",
            "poly_market_id": "P3",
            "verdict": "OVERLAPPING",
            "cosine_similarity": 0.85,
        },
    ],
}

TICKERS_FIXTURE = {
    "generated_at": "2026-03-12T00:00:00",
    "model": "gpt-4o",
    "total_markets": 4,
    "tickers": [
        make_ticker("K1", "Kalshi", "BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028"),
        make_ticker("P1", "Polymarket", "BWR-TRUMP-WIN-PRES_US-STD-ANY-2028", mechanism="STD"),
        make_ticker("K2", "Kalshi", "BWR-FED-CUT-FFR-ANY_MEETING-25BPS-2026", mechanism="ANY_MEETING", threshold="25BPS"),
        make_ticker("P2", "Polymarket", "BWR-FED-CUT-FFR-STD-25BPS-2026", mechanism="STD", threshold="25BPS"),
        make_ticker("K3", "Kalshi", "BWR-DEM-WIN-GOV_CA-CERTIFIED-ANY-2026"),
        make_ticker("P3", "Polymarket", "BWR-DEM-WIN-GOV_CA-STD-ANY-2026", mechanism="STD"),
    ],
}


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Create a temp data directory with fixture files."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    (data_dir / "cross_platform_candidates.json").write_text(
        json.dumps(CANDIDATES_FIXTURE, indent=2))
    (data_dir / "cross_platform_resolution_verdicts.json").write_text(
        json.dumps(VERDICTS_FIXTURE, indent=2))
    (data_dir / "tickers_postprocessed.json").write_text(
        json.dumps(TICKERS_FIXTURE, indent=2))

    return data_dir


# ──────────────────────────────────────────────────
# predictionhunt_client.py: kalshi_market_id_to_event_ticker
# ──────────────────────────────────────────────────


class TestKalshiTickerDerivation:
    def test_three_part_strips_last(self):
        from predictionhunt_client import kalshi_market_id_to_event_ticker
        assert kalshi_market_id_to_event_ticker("KXPRESPERSON-28-JVAN") == "KXPRESPERSON-28"

    def test_three_part_nomination(self):
        from predictionhunt_client import kalshi_market_id_to_event_ticker
        assert kalshi_market_id_to_event_ticker("KXPRESNOMD-28-TW") == "KXPRESNOMD-28"

    def test_three_part_control(self):
        from predictionhunt_client import kalshi_market_id_to_event_ticker
        assert kalshi_market_id_to_event_ticker("CONTROLH-2026-D") == "CONTROLH-2026"

    def test_four_part_strips_last(self):
        from predictionhunt_client import kalshi_market_id_to_event_ticker
        assert kalshi_market_id_to_event_ticker("KXHONDURASPRESIDENTMOV-25NOV30-7") == "KXHONDURASPRESIDENTMOV-25NOV30"

    def test_two_parts_unchanged(self):
        from predictionhunt_client import kalshi_market_id_to_event_ticker
        assert kalshi_market_id_to_event_ticker("KXPRESPERSON-28") == "KXPRESPERSON-28"

    def test_single_part_unchanged(self):
        from predictionhunt_client import kalshi_market_id_to_event_ticker
        assert kalshi_market_id_to_event_ticker("KXPRESPERSON") == "KXPRESPERSON"

    def test_short_ticker_unchanged(self):
        from predictionhunt_client import kalshi_market_id_to_event_ticker
        assert kalshi_market_id_to_event_ticker("AB") == "AB"


# ──────────────────────────────────────────────────
# predictionhunt_client.py: extract_platform_ids
# ──────────────────────────────────────────────────


class TestExtractPlatformIds:
    def setup_method(self):
        from predictionhunt_client import PredictionHuntClient
        with patch.object(PredictionHuntClient, '__init__', lambda self, **kw: None):
            self.client = PredictionHuntClient()

    def test_extracts_kalshi_and_polymarket(self):
        response = make_ph_response([
            make_group("Candidate A", [
                make_market("K1", "kalshi"),
                make_market("P1", "polymarket"),
            ]),
        ])
        ids = self.client.extract_platform_ids(response)
        assert len(ids["kalshi_ids"]) == 1
        assert len(ids["polymarket_ids"]) == 1
        assert ids["kalshi_ids"][0]["id"] == "K1"
        assert ids["polymarket_ids"][0]["id"] == "P1"

    def test_multiple_groups(self):
        response = make_ph_response([
            make_group("Candidate A", [make_market("K1", "kalshi"), make_market("P1", "polymarket")]),
            make_group("Candidate B", [make_market("K2", "kalshi"), make_market("P2", "polymarket")]),
        ])
        ids = self.client.extract_platform_ids(response)
        assert len(ids["kalshi_ids"]) == 2
        assert len(ids["polymarket_ids"]) == 2

    def test_includes_predictit(self):
        """PredictIt markets are neither kalshi nor polymarket."""
        response = make_ph_response([
            make_group("Test", [
                make_market("K1", "kalshi"),
                make_market("PI1", "predictit"),
                make_market("P1", "polymarket"),
            ]),
        ])
        ids = self.client.extract_platform_ids(response)
        assert len(ids["kalshi_ids"]) == 1
        assert len(ids["polymarket_ids"]) == 1

    def test_empty_on_failure(self):
        response = {"success": False, "error": "http_500"}
        ids = self.client.extract_platform_ids(response)
        assert ids == {"kalshi_ids": [], "polymarket_ids": []}

    def test_empty_events(self):
        response = make_ph_response([], success=True)
        ids = self.client.extract_platform_ids(response)
        assert ids == {"kalshi_ids": [], "polymarket_ids": []}


# ──────────────────────────────────────────────────
# predictionhunt_client.py: find_group_for_kalshi_market
# ──────────────────────────────────────────────────


class TestFindGroupForKalshiMarket:
    def setup_method(self):
        from predictionhunt_client import PredictionHuntClient
        with patch.object(PredictionHuntClient, '__init__', lambda self, **kw: None):
            self.client = PredictionHuntClient()

    def test_finds_correct_group(self):
        response = make_ph_response([
            make_group("Candidate A", [make_market("K1", "kalshi"), make_market("P1", "polymarket")]),
            make_group("Candidate B", [make_market("K2", "kalshi"), make_market("P2", "polymarket")]),
        ])
        result = self.client.find_group_for_kalshi_market(response, "K2")
        assert len(result) == 1
        assert result[0]["id"] == "P2"

    def test_multiple_polymarket_in_group(self):
        response = make_ph_response([
            make_group("Test", [
                make_market("K1", "kalshi"),
                make_market("P1", "polymarket"),
                make_market("P1b", "polymarket"),
            ]),
        ])
        result = self.client.find_group_for_kalshi_market(response, "K1")
        ids = {m["id"] for m in result}
        assert ids == {"P1", "P1b"}

    def test_kalshi_not_found(self):
        response = make_ph_response([
            make_group("Test", [make_market("K1", "kalshi"), make_market("P1", "polymarket")]),
        ])
        result = self.client.find_group_for_kalshi_market(response, "K999")
        assert result == []

    def test_case_insensitive(self):
        response = make_ph_response([
            make_group("Test", [make_market("KXPRESPERSON-28-JVAN", "kalshi"), make_market("P1", "polymarket")]),
        ])
        result = self.client.find_group_for_kalshi_market(response, "kxpresperson-28-jvan")
        assert len(result) == 1
        assert result[0]["id"] == "P1"

    def test_includes_source_url(self):
        response = make_ph_response([
            make_group("Test", [make_market("K1", "kalshi"), make_market("P1", "polymarket")]),
        ])
        result = self.client.find_group_for_kalshi_market(response, "K1")
        assert "source_url" in result[0]

    def test_failed_response(self):
        response = {"success": False}
        result = self.client.find_group_for_kalshi_market(response, "K1")
        assert result == []

    def test_empty_events(self):
        response = make_ph_response([], success=True)
        result = self.client.find_group_for_kalshi_market(response, "K1")
        assert result == []


# ──────────────────────────────────────────────────
# predictionhunt_client.py: budget tracking
# ──────────────────────────────────────────────────


class TestBudgetTracking:
    def test_fresh_month_resets(self, tmp_path):
        from predictionhunt_client import PredictionHuntClient, MONTHLY_LIMIT

        usage_file = tmp_path / "predictionhunt_usage.json"
        old_usage = {
            "monthly_limit": MONTHLY_LIMIT,
            "current_month": "2025-01",
            "requests_used": 999,
            "requests_log": [],
        }
        usage_file.write_text(json.dumps(old_usage))

        with patch.object(PredictionHuntClient, '__init__', lambda self, **kw: None):
            client = PredictionHuntClient()

        with patch("predictionhunt_client.USAGE_FILE", usage_file):
            usage = client._load_usage()
            assert usage["requests_used"] == 0
            assert usage["current_month"] != "2025-01"

    def test_budget_exhausted_raises(self, tmp_path):
        from predictionhunt_client import PredictionHuntClient, BudgetExhaustedError

        usage_file = tmp_path / "predictionhunt_usage.json"
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        usage = {
            "monthly_limit": 1000,
            "current_month": current_month,
            "requests_used": 1000,
            "requests_log": [],
        }
        usage_file.write_text(json.dumps(usage))

        with patch.object(PredictionHuntClient, '__init__', lambda self, **kw: None):
            client = PredictionHuntClient()

        with patch("predictionhunt_client.USAGE_FILE", usage_file):
            with pytest.raises(BudgetExhaustedError):
                client.check_budget()

    def test_increment_persists(self, tmp_path):
        from predictionhunt_client import PredictionHuntClient

        usage_file = tmp_path / "predictionhunt_usage.json"
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        usage = {
            "monthly_limit": 1000,
            "current_month": current_month,
            "requests_used": 5,
            "requests_log": [],
        }
        usage_file.write_text(json.dumps(usage))

        with patch.object(PredictionHuntClient, '__init__', lambda self, **kw: None):
            client = PredictionHuntClient()

        with patch("predictionhunt_client.USAGE_FILE", usage_file):
            client._increment_usage("test_pipeline")
            reloaded = json.loads(usage_file.read_text())
            assert reloaded["requests_used"] == 6
            assert reloaded["requests_log"][0]["pipeline"] == "test_pipeline"
            assert reloaded["requests_log"][0]["count"] == 1

    def test_increment_groups_by_date_and_pipeline(self, tmp_path):
        from predictionhunt_client import PredictionHuntClient

        usage_file = tmp_path / "predictionhunt_usage.json"
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        usage = {
            "monthly_limit": 1000,
            "current_month": current_month,
            "requests_used": 3,
            "requests_log": [{"date": today, "count": 3, "pipeline": "validate_embedding"}],
        }
        usage_file.write_text(json.dumps(usage))

        with patch.object(PredictionHuntClient, '__init__', lambda self, **kw: None):
            client = PredictionHuntClient()

        with patch("predictionhunt_client.USAGE_FILE", usage_file):
            # Same pipeline same day — should increment existing entry
            client._increment_usage("validate_embedding")
            reloaded = json.loads(usage_file.read_text())
            assert len(reloaded["requests_log"]) == 1
            assert reloaded["requests_log"][0]["count"] == 4

            # Different pipeline same day — new entry
            client._increment_usage("manual")
            reloaded = json.loads(usage_file.read_text())
            assert len(reloaded["requests_log"]) == 2


# ──────────────────────────────────────────────────
# pipeline_validate_with_predictionhunt.py: classify_ph_response
# ──────────────────────────────────────────────────


class TestClassifyPhResponse:
    def setup_method(self):
        from predictionhunt_client import PredictionHuntClient
        with patch.object(PredictionHuntClient, '__init__', lambda self, **kw: None):
            self.client = PredictionHuntClient()
            # Attach real methods (they don't use self.api_key etc.)
            from predictionhunt_client import PredictionHuntClient as RealClass
            self.client.find_group_for_kalshi_market = RealClass.find_group_for_kalshi_market.__get__(self.client)
            self.client.extract_platform_ids = RealClass.extract_platform_ids.__get__(self.client)

    def test_confirmed_group_level(self):
        from pipeline_validate_with_predictionhunt import classify_ph_response
        response = make_ph_response([
            make_group("Trump", [make_market("K1", "kalshi"), make_market("P1", "polymarket")]),
        ])
        status, ids = classify_ph_response(response, "P1", self.client, our_kalshi_id="K1")
        assert status == "confirmed"
        assert "P1" in ids

    def test_disagreed_different_polymarket(self):
        from pipeline_validate_with_predictionhunt import classify_ph_response
        response = make_ph_response([
            make_group("Trump", [make_market("K1", "kalshi"), make_market("P99", "polymarket")]),
        ])
        status, ids = classify_ph_response(response, "P1", self.client, our_kalshi_id="K1")
        assert status == "disagreed"
        assert "P99" in ids

    def test_no_match_kalshi_not_in_any_group(self):
        from pipeline_validate_with_predictionhunt import classify_ph_response
        response = make_ph_response([
            make_group("Other", [make_market("K999", "kalshi"), make_market("P1", "polymarket")]),
        ])
        status, ids = classify_ph_response(response, "P1", self.client, our_kalshi_id="K1")
        assert status == "no_match"
        assert ids == []

    def test_no_match_empty_events(self):
        from pipeline_validate_with_predictionhunt import classify_ph_response
        response = {"success": True, "count": 0, "events": []}
        status, ids = classify_ph_response(response, "P1", self.client, our_kalshi_id="K1")
        assert status == "no_match"

    def test_error_on_failed_response(self):
        from pipeline_validate_with_predictionhunt import classify_ph_response
        response = {"success": False, "error": "http_500"}
        status, ids = classify_ph_response(response, "P1", self.client, our_kalshi_id="K1")
        assert status == "error"
        assert ids == []

    def test_slug_vs_numeric_id_confirmed_via_source_url(self):
        """Our slug ID should match PH's numeric ID when slug is in source_url."""
        from pipeline_validate_with_predictionhunt import classify_ph_response
        our_slug = "will-kathleen-riebe-be-the-democratic-nominee-for-ut-01"
        response = make_ph_response([
            make_group("Riebe", [
                {"id": "KXUT1D-26-KRIE", "source": "kalshi", "source_url": "https://kalshi.com/markets/KXUT1D"},
                {"id": "704282", "source": "polymarket",
                 "source_url": f"https://polymarket.com/market/{our_slug}?via=predictionhunt"},
            ]),
        ])
        status, ids = classify_ph_response(response, our_slug, self.client, our_kalshi_id="KXUT1D-26-KRIE")
        assert status == "confirmed"

    def test_slug_no_match_in_source_url(self):
        """Slug not in source_url should be a real disagreement."""
        from pipeline_validate_with_predictionhunt import classify_ph_response
        # Override make_market to have a different slug in source_url
        response = make_ph_response([
            make_group("Other", [
                {"id": "KXUT1D-26-KRIE", "source": "kalshi", "source_url": "https://kalshi.com/x"},
                {"id": "704282", "source": "polymarket", "source_url": "https://polymarket.com/market/completely-different-market"},
            ]),
        ])
        our_slug = "will-kathleen-riebe-be-the-democratic-nominee-for-ut-01"
        status, ids = classify_ph_response(response, our_slug, self.client, our_kalshi_id="KXUT1D-26-KRIE")
        assert status == "disagreed"

    def test_fallback_without_kalshi_id_confirmed(self):
        """When no kalshi_id provided, falls back to flat list scan."""
        from pipeline_validate_with_predictionhunt import classify_ph_response
        response = make_ph_response([
            make_group("A", [make_market("K1", "kalshi"), make_market("P1", "polymarket")]),
            make_group("B", [make_market("K2", "kalshi"), make_market("P2", "polymarket")]),
        ])
        status, ids = classify_ph_response(response, "P2", self.client, our_kalshi_id=None)
        assert status == "confirmed"

    def test_fallback_without_kalshi_id_disagreed(self):
        from pipeline_validate_with_predictionhunt import classify_ph_response
        response = make_ph_response([
            make_group("A", [make_market("K1", "kalshi"), make_market("P99", "polymarket")]),
        ])
        status, ids = classify_ph_response(response, "P1", self.client, our_kalshi_id=None)
        assert status == "disagreed"


# ──────────────────────────────────────────────────
# pipeline_update_matches.py: gate_with_predictionhunt
# ──────────────────────────────────────────────────


class TestGateWithPredictionhunt:
    """Tests the PH gate function in pipeline_update_matches.py."""

    def _make_verdict(self, k_id, p_id, similarity=0.90):
        return {
            "pair_key": f"{k_id}|{p_id}",
            "kalshi_market_id": k_id,
            "poly_market_id": p_id,
            "cosine_similarity": similarity,
            "verdict": "IDENTICAL",
        }

    def _make_bucket_b_pair(self, k_id, p_id, similarity=0.90):
        return {
            "pair_key": f"{k_id}|{p_id}",
            "kalshi_market_id": k_id,
            "poly_market_id": p_id,
            "kalshi_ticker": f"BWR-TEST-WIN-X-CERTIFIED-ANY-2028",
            "poly_ticker": f"BWR-TEST-WIN-X-STD-ANY-2028",
            "kalshi_question": f"Question for {k_id}",
            "poly_question": f"Question for {p_id}",
            "cosine_similarity": similarity,
        }

    @patch.dict(os.environ, {}, clear=False)
    def test_no_api_key_approves_all(self):
        """Without PREDICTIONHUNT_API_KEY, gate is skipped."""
        from pipeline_update_matches import gate_with_predictionhunt

        # Remove the key if it exists
        env = os.environ.copy()
        env.pop("PREDICTIONHUNT_API_KEY", None)
        with patch.dict(os.environ, env, clear=True):
            verdicts = [self._make_verdict("K1", "P1"), self._make_verdict("K2", "P2")]
            approved, flagged = gate_with_predictionhunt(verdicts, [], dry_run=False)
            assert len(approved) == 2
            assert len(flagged) == 0

    def test_dry_run_approves_all(self):
        from pipeline_update_matches import gate_with_predictionhunt

        with patch.dict(os.environ, {"PREDICTIONHUNT_API_KEY": "test_key"}):
            with patch("predictionhunt_client.PredictionHuntClient") as MockClient:
                client = MockClient.return_value
                client.check_budget.return_value = (100, 0, 1000)

                verdicts = [self._make_verdict("K1", "P1")]
                approved, flagged = gate_with_predictionhunt(verdicts, [], dry_run=True)
                assert len(approved) == 1
                assert len(flagged) == 0
                client.query_by_kalshi_ticker.assert_not_called()

    def test_all_confirmed(self, tmp_path):
        from pipeline_update_matches import gate_with_predictionhunt

        with patch.dict(os.environ, {"PREDICTIONHUNT_API_KEY": "test_key"}):
            with patch("predictionhunt_client.PredictionHuntClient") as MockClient:
                client = MockClient.return_value
                client.check_budget.return_value = (100, 0, 1000)
                client.query_by_kalshi_ticker.return_value = {"success": True}

                with patch("pipeline_validate_with_predictionhunt.classify_ph_response", return_value=("confirmed", ["P1"])):
                    verdicts = [self._make_verdict("K1", "P1")]
                    bucket_b = [self._make_bucket_b_pair("K1", "P1")]
                    approved, flagged = gate_with_predictionhunt(verdicts, bucket_b)
                    assert len(approved) == 1
                    assert len(flagged) == 0

    def test_mix_confirmed_and_disagreed(self, tmp_path):
        from pipeline_update_matches import gate_with_predictionhunt

        def mock_classify(ph_result, p_id, client, our_kalshi_id=None):
            if p_id == "P1":
                return "confirmed", ["P1"]
            else:
                return "disagreed", ["P99"]

        with patch.dict(os.environ, {"PREDICTIONHUNT_API_KEY": "test_key"}):
            with patch("predictionhunt_client.PredictionHuntClient") as MockClient:
                client = MockClient.return_value
                client.check_budget.return_value = (100, 0, 1000)
                client.query_by_kalshi_ticker.return_value = {"success": True}

                with patch("pipeline_validate_with_predictionhunt.classify_ph_response", side_effect=mock_classify):
                    with patch("pipeline_validate_with_predictionhunt.load_pending_review", return_value=[]):
                        with patch("pipeline_update_matches.atomic_write_json"):
                            verdicts = [
                                self._make_verdict("K1", "P1"),
                                self._make_verdict("K2", "P2"),
                            ]
                            bucket_b = [
                                self._make_bucket_b_pair("K1", "P1"),
                                self._make_bucket_b_pair("K2", "P2"),
                            ]
                            approved, flagged = gate_with_predictionhunt(verdicts, bucket_b)
                            assert len(approved) == 1
                            assert approved[0]["poly_market_id"] == "P1"
                            assert len(flagged) == 1
                            assert flagged[0]["poly_market_id"] == "P2"

    def test_no_match_still_approved(self):
        """PH having no data should NOT block a match."""
        from pipeline_update_matches import gate_with_predictionhunt

        with patch.dict(os.environ, {"PREDICTIONHUNT_API_KEY": "test_key"}):
            with patch("predictionhunt_client.PredictionHuntClient") as MockClient:
                client = MockClient.return_value
                client.check_budget.return_value = (100, 0, 1000)
                client.query_by_kalshi_ticker.return_value = {"success": True}

                with patch("pipeline_validate_with_predictionhunt.classify_ph_response", return_value=("no_match", [])):
                    verdicts = [self._make_verdict("K1", "P1")]
                    approved, flagged = gate_with_predictionhunt(verdicts, [])
                    assert len(approved) == 1
                    assert len(flagged) == 0

    def test_error_still_approved(self):
        """API errors should NOT block a match."""
        from pipeline_update_matches import gate_with_predictionhunt

        with patch.dict(os.environ, {"PREDICTIONHUNT_API_KEY": "test_key"}):
            with patch("predictionhunt_client.PredictionHuntClient") as MockClient:
                client = MockClient.return_value
                client.check_budget.return_value = (100, 0, 1000)
                client.query_by_kalshi_ticker.return_value = {"success": False, "error": "http_500"}

                with patch("pipeline_validate_with_predictionhunt.classify_ph_response", return_value=("error", [])):
                    verdicts = [self._make_verdict("K1", "P1")]
                    approved, flagged = gate_with_predictionhunt(verdicts, [])
                    assert len(approved) == 1
                    assert len(flagged) == 0

    def test_budget_exhaustion_mid_check(self):
        """When budget runs out, remaining verdicts are approved without query."""
        from predictionhunt_client import BudgetExhaustedError
        from pipeline_update_matches import gate_with_predictionhunt

        call_count = 0

        def mock_check_budget():
            nonlocal call_count
            call_count += 1
            if call_count > 2:  # Allow first verdict, block second
                raise BudgetExhaustedError("exhausted")
            return (1, 999, 1000)

        with patch.dict(os.environ, {"PREDICTIONHUNT_API_KEY": "test_key"}):
            with patch("predictionhunt_client.PredictionHuntClient") as MockClient:
                client = MockClient.return_value
                client.check_budget.side_effect = mock_check_budget
                client.query_by_kalshi_ticker.return_value = {"success": True}

                with patch("pipeline_validate_with_predictionhunt.classify_ph_response", return_value=("confirmed", ["P1"])):
                    verdicts = [
                        self._make_verdict("K1", "P1"),
                        self._make_verdict("K2", "P2"),
                        self._make_verdict("K3", "P3"),
                    ]
                    approved, flagged = gate_with_predictionhunt(verdicts, [])
                    # First was checked and confirmed, remaining 2 auto-approved
                    assert len(approved) == 3
                    assert len(flagged) == 0


# ──────────────────────────────────────────────────
# Integration: full pipeline_update_matches flow with PH gate
# ──────────────────────────────────────────────────


class TestPHGateFullIntegration:
    """End-to-end: synthetic data files, mocked PH API, verify all outputs."""

    def test_confirmed_pair_unified_disagreed_pair_flagged(self, tmp_data_dir):
        """K1|P1 confirmed by PH → unified. K2|P2 disagreed → flagged."""
        import pipeline_update_matches as pum

        # Patch file paths to use tmp dir
        with patch.object(pum, "CANDIDATES_FILE", tmp_data_dir / "cross_platform_candidates.json"), \
             patch.object(pum, "VERDICTS_FILE", tmp_data_dir / "cross_platform_resolution_verdicts.json"), \
             patch.object(pum, "TICKERS_FILE", tmp_data_dir / "tickers_postprocessed.json"), \
             patch.object(pum, "NEAR_MATCHES_FILE", tmp_data_dir / "near_matches.json"), \
             patch.object(pum, "REVIEWED_PAIRS_FILE", tmp_data_dir / "cross_platform_reviewed_pairs.json"), \
             patch.object(pum, "PENDING_REVIEW_FILE", tmp_data_dir / "matches_pending_review.json"):

            # Mock the PH gate: K1 confirmed, K2 disagreed
            def mock_gate(identical_verdicts, bucket_b, dry_run=False):
                approved = [v for v in identical_verdicts if v["kalshi_market_id"] == "K1"]
                flagged = [v for v in identical_verdicts if v["kalshi_market_id"] == "K2"]
                # Write pending review for flagged
                if flagged:
                    items = [{
                        "source": "embedding_gpt",
                        "pair_key": f["pair_key"],
                        "kalshi_market_id": f["kalshi_market_id"],
                        "poly_market_id": f["poly_market_id"],
                        "ph_status": "disagreed",
                        "ph_matched_pm": ["P99"],
                        "created_at": datetime.now().isoformat(),
                    } for f in flagged]
                    from config import atomic_write_json
                    atomic_write_json(
                        tmp_data_dir / "matches_pending_review.json",
                        {"updated_at": datetime.now().isoformat(), "items": items},
                        indent=2,
                    )
                return approved, flagged

            with patch.object(pum, "gate_with_predictionhunt", side_effect=mock_gate):
                with patch("sys.argv", ["pipeline_update_matches.py"]):
                    pum.main()

            # Verify tickers: P1 should be unified to CERTIFIED, P2 should be unchanged
            tickers = json.loads((tmp_data_dir / "tickers_postprocessed.json").read_text())
            tickers_by_id = {t["market_id"]: t for t in tickers["tickers"]}

            # P1 unified to match K1's mechanism
            assert tickers_by_id["P1"]["mechanism"] == "CERTIFIED"
            assert tickers_by_id["P1"]["match_source"] == "auto_embedding_gpt"

            # P2 NOT unified (PH disagreed)
            assert tickers_by_id["P2"]["mechanism"] == "STD"
            assert "match_source" not in tickers_by_id["P2"]

            # Verify pending review file
            pending = json.loads((tmp_data_dir / "matches_pending_review.json").read_text())
            assert len(pending["items"]) == 1
            assert pending["items"][0]["pair_key"] == "K2|P2"
            assert pending["items"][0]["ph_status"] == "disagreed"

            # Verify reviewed pairs includes PH-flagged action
            reviewed = json.loads((tmp_data_dir / "cross_platform_reviewed_pairs.json").read_text())
            assert "K2|P2" in reviewed["pairs"]
            assert reviewed["pairs"]["K2|P2"]["action_taken"] == "ph_flagged_for_review"

    def test_skip_ph_unifies_all(self, tmp_data_dir):
        """With --skip-ph, all IDENTICAL verdicts are unified without PH check."""
        import pipeline_update_matches as pum

        with patch.object(pum, "CANDIDATES_FILE", tmp_data_dir / "cross_platform_candidates.json"), \
             patch.object(pum, "VERDICTS_FILE", tmp_data_dir / "cross_platform_resolution_verdicts.json"), \
             patch.object(pum, "TICKERS_FILE", tmp_data_dir / "tickers_postprocessed.json"), \
             patch.object(pum, "NEAR_MATCHES_FILE", tmp_data_dir / "near_matches.json"), \
             patch.object(pum, "REVIEWED_PAIRS_FILE", tmp_data_dir / "cross_platform_reviewed_pairs.json"), \
             patch.object(pum, "PENDING_REVIEW_FILE", tmp_data_dir / "matches_pending_review.json"):

            with patch("sys.argv", ["pipeline_update_matches.py", "--skip-ph"]):
                pum.main()

            tickers = json.loads((tmp_data_dir / "tickers_postprocessed.json").read_text())
            tickers_by_id = {t["market_id"]: t for t in tickers["tickers"]}

            # Both P1 and P2 should be unified
            assert tickers_by_id["P1"]["mechanism"] == "CERTIFIED"
            assert tickers_by_id["P2"]["mechanism"] == "ANY_MEETING"

            # No pending review file should exist
            assert not (tmp_data_dir / "matches_pending_review.json").exists()

    def test_overlapping_verdict_goes_to_near_matches(self, tmp_data_dir):
        """K3|P3 with OVERLAPPING verdict should go to near_matches, not unified."""
        import pipeline_update_matches as pum

        with patch.object(pum, "CANDIDATES_FILE", tmp_data_dir / "cross_platform_candidates.json"), \
             patch.object(pum, "VERDICTS_FILE", tmp_data_dir / "cross_platform_resolution_verdicts.json"), \
             patch.object(pum, "TICKERS_FILE", tmp_data_dir / "tickers_postprocessed.json"), \
             patch.object(pum, "NEAR_MATCHES_FILE", tmp_data_dir / "near_matches.json"), \
             patch.object(pum, "REVIEWED_PAIRS_FILE", tmp_data_dir / "cross_platform_reviewed_pairs.json"), \
             patch.object(pum, "PENDING_REVIEW_FILE", tmp_data_dir / "matches_pending_review.json"):

            with patch.object(pum, "gate_with_predictionhunt", return_value=([], [])):
                with patch("sys.argv", ["pipeline_update_matches.py"]):
                    pum.main()

            near = json.loads((tmp_data_dir / "near_matches.json").read_text())
            pair_keys = [m["pair_key"] for m in near["matches"]]
            assert "K3|P3" in pair_keys

            # P3 should NOT be unified
            tickers = json.loads((tmp_data_dir / "tickers_postprocessed.json").read_text())
            tickers_by_id = {t["market_id"]: t for t in tickers["tickers"]}
            assert tickers_by_id["P3"]["mechanism"] == "STD"
