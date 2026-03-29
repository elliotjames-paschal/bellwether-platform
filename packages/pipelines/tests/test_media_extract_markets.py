"""
Tests for pipeline_media_extract_markets.py — market reference extraction and matching.

Run: pytest packages/pipelines/tests/test_media_extract_markets.py -v
"""

import json
import math
import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline_media_extract_markets import (
    _is_missing,
    _to_float,
    _safe_set,
    flatten_market,
    extract_keywords,
    build_market_search_text,
    build_market_indices,
    extract_market_references,
    get_fuzzy_candidates,
    match_by_url,
    match_with_llm,
    match_reference_to_market,
    load_bwr_ticker_map,
    resolve_bwr_ticker,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────

class TestIsMissing:
    def test_none(self):
        assert _is_missing(None) is True

    def test_nan_float(self):
        assert _is_missing(float("nan")) is True

    def test_nan_string(self):
        assert _is_missing("nan") is True

    def test_empty_string(self):
        assert _is_missing("") is True

    def test_whitespace_nan(self):
        assert _is_missing("  NaN  ") is True

    def test_valid_string(self):
        assert _is_missing("hello") is False

    def test_valid_float(self):
        assert _is_missing(0.5) is False

    def test_zero(self):
        assert _is_missing(0) is False


class TestToFloat:
    def test_none(self):
        assert _to_float(None) is None

    def test_valid_float(self):
        assert _to_float(0.81) == 0.81

    def test_string_float(self):
        assert _to_float("0.5") == 0.5

    def test_invalid_string(self):
        assert _to_float("abc") is None

    def test_nan_returns_none(self):
        assert _to_float(float("nan")) is None

    def test_int(self):
        assert _to_float(42) == 42.0


class TestSafeSet:
    def test_sets_when_missing(self):
        d = {"key": None}
        _safe_set(d, "key", "value")
        assert d["key"] == "value"

    def test_sets_when_nan(self):
        d = {"key": float("nan")}
        _safe_set(d, "key", "value")
        assert d["key"] == "value"

    def test_preserves_existing(self):
        d = {"key": "existing"}
        _safe_set(d, "key", "new")
        assert d["key"] == "existing"

    def test_sets_when_absent(self):
        d = {}
        _safe_set(d, "key", "value")
        assert d["key"] == "value"

    def test_sets_when_nan_string(self):
        d = {"key": "nan"}
        _safe_set(d, "key", "value")
        assert d["key"] == "value"


# ─── Flatten Market ──────────────────────────────────────────────────────────

class TestFlattenMarket:
    def test_kalshi_basic(self):
        raw = {
            "original_csv": {
                "question": "Will Dems win House?",
                "platform": "Kalshi",
                "market_id": "CONTROLH-2026-D",
            },
            "api_data": {
                "market": {
                    "ticker": "CONTROLH-2026-D",
                    "title": "Will Democrats win the House in 2026?",
                    "rules_primary": "Resolves Yes if Dems win",
                    "last_price_dollars": 0.81,
                    "status": "active",
                    "yes_sub_title": "Democratic Party",
                },
                "event": {"title": "Which party will win the U.S. House?"},
            },
            "fetch_errors": [],
        }
        flat = flatten_market(raw)

        assert flat["question"] == "Will Dems win House?"
        assert flat["k_ticker"] == "CONTROLH-2026-D"
        assert flat["title"] == "Will Democrats win the House in 2026?"
        assert flat["event_title"] == "Which party will win the U.S. House?"
        assert flat["k_rules_primary"] == "Resolves Yes if Dems win"
        assert flat["k_yes_price"] == 0.81
        assert flat["platform"] == "Kalshi"
        assert flat["k_yes_sub_title"] == "Democratic Party"

    def test_polymarket_basic(self):
        raw = {
            "original_csv": {
                "question": "Fed rate hike in 2025?",
                "platform": "Polymarket",
                "market_id": "516706",
                "pm_market_slug": float("nan"),
            },
            "api_data": {
                "market": {
                    "id": 516706,
                    "question": "Fed rate hike in 2025?",
                    "slug": "fed-rate-hike-in-2025",
                    "description": "Resolves Yes if rates go up",
                    "lastTradePrice": 0.05,
                    "active": True,
                    "volumeNum": 1000000,
                },
                "event": {"title": "Fed Decisions", "slug": "fed-decisions"},
            },
            "fetch_errors": [],
        }
        flat = flatten_market(raw)

        assert flat["question"] == "Fed rate hike in 2025?"
        assert flat["pm_market_slug"] == "fed-rate-hike-in-2025"  # NaN replaced
        assert flat["pm_market_id"] == "516706"
        assert flat["pm_yes_price"] == 0.05
        assert flat["description"] == "Resolves Yes if rates go up"
        assert flat["pm_event_slug"] == "fed-decisions"
        assert flat["event_title"] == "Fed Decisions"

    def test_already_flat_passthrough(self):
        flat_input = {"question": "Will X happen?", "platform": "Kalshi"}
        result = flatten_market(flat_input)
        assert result["question"] == "Will X happen?"

    def test_nan_in_csv_overridden_by_api(self):
        raw = {
            "original_csv": {
                "question": "Some question",
                "platform": "Polymarket",
                "pm_market_slug": float("nan"),
            },
            "api_data": {
                "market": {"slug": "real-slug", "id": 123},
                "event": {},
            },
            "fetch_errors": [],
        }
        flat = flatten_market(raw)
        assert flat["pm_market_slug"] == "real-slug"

    def test_missing_api_data(self):
        raw = {
            "original_csv": {"question": "Test?", "platform": "Kalshi"},
            "api_data": None,
            "fetch_errors": ["timeout"],
        }
        flat = flatten_market(raw)
        assert flat["question"] == "Test?"

    def test_question_fallback_from_api(self):
        raw = {
            "original_csv": {"platform": "Polymarket"},
            "api_data": {
                "market": {"question": "From API?", "id": 1},
                "event": {},
            },
            "fetch_errors": [],
        }
        flat = flatten_market(raw)
        assert flat["question"] == "From API?"


# ─── Extract Keywords ────────────────────────────────────────────────────────

class TestExtractKeywords:
    def test_removes_stopwords(self):
        kw = extract_keywords("Will the Federal Reserve raise interest rates?")
        assert "federal" in kw
        assert "reserve" in kw
        assert "raise" in kw
        assert "the" not in kw
        assert "will" not in kw

    def test_removes_platform_words(self):
        kw = extract_keywords("On Polymarket, traders bet on prediction markets")
        assert "polymarket" not in kw
        assert "prediction" not in kw
        assert "markets" not in kw
        assert "traders" not in kw

    def test_removes_short_words(self):
        kw = extract_keywords("Is it OK to go?")
        # "is", "it", "OK" (2 chars), "to", "go" (2 chars) — all filtered
        assert kw == ""

    def test_preserves_names(self):
        kw = extract_keywords("Will Trump win the 2028 election?")
        assert "trump" in kw
        assert "win" in kw
        assert "election" in kw


# ─── Build Market Search Text ────────────────────────────────────────────────

class TestBuildMarketSearchText:
    def test_includes_all_fields(self):
        market = {
            "question": "Will X happen?",
            "title": "X happening",
            "description": "Resolves Yes if X",
            "event_title": "X Events",
            "k_rules_primary": "If X then Yes",
            "pm_market_slug": "x-happening",
        }
        text = build_market_search_text(market)
        assert "Will X happen?" in text
        assert "X happening" in text
        assert "Resolves Yes if X" in text
        assert "X Events" in text
        assert "If X then Yes" in text
        assert "x happening" in text  # slug hyphens -> spaces

    def test_skips_nan_values(self):
        market = {"question": "Valid?", "title": "nan", "description": None}
        text = build_market_search_text(market)
        assert text == "Valid?"

    def test_empty_market(self):
        assert build_market_search_text({}) == ""


# ─── Build Market Indices ────────────────────────────────────────────────────

class TestBuildMarketIndices:
    def test_slug_index(self):
        markets = [
            {"pm_market_slug": "fed-rate-hike", "pm_event_slug": "fed-stuff"},
            {"pm_market_slug": float("nan")},
        ]
        slug_idx, ticker_idx = build_market_indices(markets)
        assert "fed-rate-hike" in slug_idx
        assert "fed-stuff" in slug_idx
        assert slug_idx["fed-rate-hike"] == 0

    def test_ticker_index(self):
        markets = [
            {"k_ticker": "CONTROLH-2026-D", "market_id": "CONTROLH-2026-D"},
        ]
        slug_idx, ticker_idx = build_market_indices(markets)
        assert "CONTROLH-2026-D" in ticker_idx

    def test_nan_excluded(self):
        markets = [{"pm_market_slug": "nan", "k_ticker": float("nan")}]
        slug_idx, ticker_idx = build_market_indices(markets)
        assert len(slug_idx) == 0
        assert len(ticker_idx) == 0


# ─── Extract Market References ───────────────────────────────────────────────

class TestExtractMarketReferences:
    def test_platform_with_probability(self):
        citation = {"sentence": "Polymarket traders give Trump a 62% chance of winning"}
        refs = extract_market_references(citation)
        assert len(refs) >= 1
        ref = refs[0]
        assert ref["platform_mentioned"] == "polymarket"
        assert ref["probability_cited"] == 0.62

    def test_percent_on_platform(self):
        citation = {"sentence": "The odds are 75% on Kalshi for this outcome"}
        refs = extract_market_references(citation)
        assert len(refs) >= 1
        assert refs[0]["platform_mentioned"] == "kalshi"
        assert refs[0]["probability_cited"] == 0.75

    def test_platform_mention_only(self):
        citation = {"sentence": "Polymarket opened a pop-up bar in DC"}
        refs = extract_market_references(citation)
        assert len(refs) == 1
        assert refs[0]["platform_mentioned"] == "polymarket"
        assert refs[0]["probability_cited"] is None

    def test_no_reference(self):
        citation = {"sentence": "The weather will be sunny tomorrow"}
        refs = extract_market_references(citation)
        assert refs == []

    def test_generic_prediction_market(self):
        citation = {"sentence": "Prediction market odds show a 55% chance of recession"}
        refs = extract_market_references(citation)
        assert len(refs) >= 1
        assert refs[0]["platform_mentioned"] == "generic"
        assert refs[0]["probability_cited"] == 0.55


# ─── URL-based Matching ─────────────────────────────────────────────────────

class TestMatchByUrl:
    def setup_method(self):
        self.markets = [
            {"pm_market_slug": "fed-rate-hike", "question": "Fed rate hike?"},
            {"k_ticker": "CONTROLH-2026-D", "question": "Dems win House?"},
        ]
        self.slug_idx = {"fed-rate-hike": 0}
        self.ticker_idx = {"CONTROLH-2026-D": 1}

    def test_polymarket_url(self):
        ref = {"subject_text": "Check polymarket.com/event/fed-rate-hike for details"}
        result = match_by_url(ref, self.markets, self.slug_idx, self.ticker_idx)
        assert result is not None
        market, conf, score = result
        assert market["question"] == "Fed rate hike?"
        assert conf == "HIGH"
        assert score == 100

    def test_kalshi_url(self):
        ref = {"subject_text": "See kalshi.com/markets/CONTROLH-2026-D"}
        result = match_by_url(ref, self.markets, self.slug_idx, self.ticker_idx)
        assert result is not None
        market, conf, score = result
        assert market["question"] == "Dems win House?"

    def test_no_url(self):
        ref = {"subject_text": "Polymarket shows 62% odds"}
        result = match_by_url(ref, self.markets, self.slug_idx, self.ticker_idx)
        assert result is None

    def test_empty_subject(self):
        ref = {"subject_text": ""}
        result = match_by_url(ref, self.markets, self.slug_idx, self.ticker_idx)
        assert result is None


# ─── Fuzzy Candidate Generation ──────────────────────────────────────────────

class TestGetFuzzyCandidates:
    def test_returns_candidates_sorted_by_score(self):
        markets = [
            {"question": "Will there be a recession?"},
            {"question": "Will Trump win 2028?"},
            {"question": "Fed rate hike in 2025?"},
        ]
        texts = [build_market_search_text(m) for m in markets]
        keywords = [extract_keywords(t) for t in texts]
        ref = {
            "subject_text": "Federal Reserve expected to hike rates this year",
            "platform_mentioned": "generic",
            "probability_cited": None,
        }
        cands = get_fuzzy_candidates(ref, markets, texts, keywords)
        # Fed rate hike should be among candidates
        assert len(cands) > 0
        # First candidate should have highest score
        if len(cands) > 1:
            assert cands[0][1] >= cands[1][1]

    def test_empty_subject(self):
        ref = {"subject_text": "", "platform_mentioned": "generic", "probability_cited": None}
        cands = get_fuzzy_candidates(ref, [], [], [])
        assert cands == []


# ─── LLM Matching ────────────────────────────────────────────────────────────

class TestMatchWithLlm:
    def test_llm_returns_valid_index(self):
        markets = [
            {"question": "Will there be a recession?", "platform": "Polymarket"},
            {"question": "Fed rate hike?", "platform": "Polymarket"},
        ]
        candidates = [(0, 50), (1, 45)]
        ref = {
            "subject_text": "Fed expected to raise rates",
            "platform_mentioned": "polymarket",
            "probability_cited": 0.25,
        }

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "1"
        mock_client.chat.completions.create.return_value = mock_response

        market, conf, score = match_with_llm(ref, candidates, markets, mock_client)
        assert market["question"] == "Fed rate hike?"
        assert conf == "HIGH"
        assert score == 95

    def test_llm_returns_none(self):
        markets = [{"question": "Unrelated market", "platform": "Kalshi"}]
        candidates = [(0, 50)]
        ref = {
            "subject_text": "Pop-up bar opening in DC",
            "platform_mentioned": "polymarket",
            "probability_cited": None,
        }

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "NONE"
        mock_client.chat.completions.create.return_value = mock_response

        market, conf, score = match_with_llm(ref, candidates, markets, mock_client)
        assert market is None
        assert conf == "UNMATCHED"

    def test_llm_api_failure_returns_unmatched(self):
        candidates = [(0, 50)]
        markets = [{"question": "Test", "platform": "Kalshi"}]
        ref = {"subject_text": "test", "platform_mentioned": "generic", "probability_cited": None}

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = Exception("API error")

        market, conf, score = match_with_llm(ref, candidates, markets, mock_client)
        assert market is None
        assert conf == "UNMATCHED"

    def test_llm_out_of_range_index(self):
        candidates = [(0, 50)]
        markets = [{"question": "Test", "platform": "Kalshi"}]
        ref = {"subject_text": "test", "platform_mentioned": "generic", "probability_cited": None}

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "99"
        mock_client.chat.completions.create.return_value = mock_response

        market, conf, score = match_with_llm(ref, candidates, markets, mock_client)
        assert market is None
        assert conf == "UNMATCHED"


# ─── Full Match Pipeline ────────────────────────────────────────────────────

class TestMatchReferenceToMarket:
    def setup_method(self):
        self.markets = [
            {"question": "Will Dems win House?", "platform": "Kalshi",
             "k_ticker": "CONTROLH-2026-D", "pm_market_slug": "nan"},
            {"question": "Fed rate hike?", "platform": "Polymarket",
             "pm_market_slug": "fed-rate-hike", "pm_market_id": "123"},
        ]
        self.texts = [build_market_search_text(m) for m in self.markets]
        self.keywords = [extract_keywords(t) for t in self.texts]
        self.slug_idx, self.ticker_idx = build_market_indices(self.markets)

    def test_url_match_takes_priority(self):
        ref = {
            "subject_text": "Check polymarket.com/event/fed-rate-hike for the latest",
            "platform_mentioned": "polymarket",
            "probability_cited": None,
        }
        market, conf, score = match_reference_to_market(
            ref, self.markets, self.texts, self.keywords,
            self.slug_idx, self.ticker_idx, openai_client=None
        )
        assert market["question"] == "Fed rate hike?"
        assert conf == "HIGH"
        assert score == 100

    def test_empty_subject_returns_unmatched(self):
        ref = {"subject_text": "", "platform_mentioned": "generic", "probability_cited": None}
        market, conf, score = match_reference_to_market(
            ref, self.markets, self.texts, self.keywords,
            self.slug_idx, self.ticker_idx
        )
        assert market is None
        assert conf == "UNMATCHED"

    def test_llm_called_when_enabled(self):
        ref = {
            "subject_text": "Democrats expected to win midterm House race",
            "platform_mentioned": "kalshi",
            "probability_cited": 0.81,
        }

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "0"
        mock_client.chat.completions.create.return_value = mock_response

        with patch("pipeline_media_extract_markets.LLM_MATCH_ENABLED", True):
            market, conf, score = match_reference_to_market(
                ref, self.markets, self.texts, self.keywords,
                self.slug_idx, self.ticker_idx, mock_client
            )

        # LLM should have been called
        mock_client.chat.completions.create.assert_called_once()

# ─── BWR Ticker Bridge ──────────────────────────────────────────────────────

class TestResolveBwrTicker:
    def test_resolves_by_k_ticker(self):
        lookup = {("k", "CONTROLH-2026-D"): "BWR-DEM-WIN-HOUSE-2026"}
        market = {"k_ticker": "CONTROLH-2026-D", "pm_token_id": ""}
        assert resolve_bwr_ticker(market, lookup) == "BWR-DEM-WIN-HOUSE-2026"

    def test_resolves_by_pm_token_id(self):
        lookup = {("pm", "1234567890"): "BWR-FED-CUT-FFR-JAN2026"}
        market = {"k_ticker": "", "pm_token_id": "1234567890"}
        assert resolve_bwr_ticker(market, lookup) == "BWR-FED-CUT-FFR-JAN2026"

    def test_k_ticker_takes_priority(self):
        lookup = {
            ("k", "TICKER-1"): "BWR-FROM-KALSHI",
            ("pm", "9999"): "BWR-FROM-PM",
        }
        market = {"k_ticker": "TICKER-1", "pm_token_id": "9999"}
        assert resolve_bwr_ticker(market, lookup) == "BWR-FROM-KALSHI"

    def test_returns_empty_when_no_match(self):
        lookup = {("k", "OTHER"): "BWR-OTHER"}
        market = {"k_ticker": "MISSING", "pm_token_id": ""}
        assert resolve_bwr_ticker(market, lookup) == ""

    def test_returns_empty_for_empty_market(self):
        assert resolve_bwr_ticker({}, {}) == ""

    def test_load_bwr_ticker_map_missing_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "pipeline_media_extract_markets.MARKET_MAP_FILE",
            tmp_path / "nonexistent.json",
        )
        result = load_bwr_ticker_map()
        assert result == {}

    def test_load_bwr_ticker_map_valid(self, tmp_path, monkeypatch):
        map_file = tmp_path / "market_map.json"
        map_file.write_text(json.dumps({
            "markets": [
                {"ticker": "BWR-TEST-1", "k_ticker": "K1", "pm_token_id": None},
                {"ticker": "BWR-TEST-2", "k_ticker": None, "pm_token_id": "PM2"},
                {"ticker": "", "k_ticker": "K3", "pm_token_id": "PM3"},
            ]
        }))
        monkeypatch.setattr(
            "pipeline_media_extract_markets.MARKET_MAP_FILE", map_file,
        )
        lookup = load_bwr_ticker_map()
        assert lookup[("k", "K1")] == "BWR-TEST-1"
        assert lookup[("pm", "PM2")] == "BWR-TEST-2"
        assert ("k", "K3") not in lookup  # empty ticker skipped


# ─── Full Match Pipeline (continued) ────────────────────────────────────────

class TestMatchReferenceToMarketContinued(TestMatchReferenceToMarket):
    def test_fuzzy_fallback_when_llm_disabled(self):
        ref = {
            "subject_text": "Democrats expected to win midterm House race",
            "platform_mentioned": "kalshi",
            "probability_cited": None,
        }

        with patch("pipeline_media_extract_markets.LLM_MATCH_ENABLED", False):
            market, conf, score = match_reference_to_market(
                ref, self.markets, self.texts, self.keywords,
                self.slug_idx, self.ticker_idx, openai_client=None
            )
        # Should still attempt fuzzy fallback (result depends on scores)
        assert conf in ("MEDIUM", "UNMATCHED")
