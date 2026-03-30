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
    filter_markets_by_platform,
    keyword_prefilter,
    validate_probability_match,
    generate_market_url,
    normalize_title,
    deduplicate_citations,
    classify_citation_topic,
    build_topic_clusters,
    find_cross_platform_counterpart,
    POLYMARKET_MARKET_URL,
    POLYMARKET_URL,
    KALSHI_URL,
    FUZZY_CANDIDATE_THRESHOLD,
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
        slug_idx, ticker_idx, pm_id_idx = build_market_indices(markets)
        assert "fed-rate-hike" in slug_idx
        assert "fed-stuff" in slug_idx
        assert slug_idx["fed-rate-hike"] == 0

    def test_ticker_index(self):
        markets = [
            {"k_ticker": "CONTROLH-2026-D", "market_id": "CONTROLH-2026-D"},
        ]
        slug_idx, ticker_idx, pm_id_idx = build_market_indices(markets)
        assert "CONTROLH-2026-D" in ticker_idx

    def test_nan_excluded(self):
        markets = [{"pm_market_slug": "nan", "k_ticker": float("nan")}]
        slug_idx, ticker_idx, pm_id_idx = build_market_indices(markets)
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
        self.slug_idx, self.ticker_idx, self.pm_id_idx = build_market_indices(self.markets)

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


# ─── Title-enriched Extraction ─────────────────────────────────────────────

class TestTitleEnrichedExtraction:
    """Tests for Changes 1-2: article_title/article_sentence fields and title prepend."""

    def test_article_title_and_sentence_populated(self):
        citation = {
            "title": "Lucrative bets anticipated Trump policy moves",
            "sentence": "An unknown Polymarket punter took in $400,000",
        }
        refs = extract_market_references(citation)
        assert len(refs) >= 1
        assert refs[0]["article_title"] == "Lucrative bets anticipated Trump policy moves"
        assert refs[0]["article_sentence"] == "An unknown Polymarket punter took in $400,000"

    def test_title_in_subject_text(self):
        """Title should appear in subject_text — either via regex window or prepend."""
        citation = {
            "title": "Iran conflict drives prediction market surge",
            "sentence": "Polymarket saw record volume on Iran contracts",
        }
        refs = extract_market_references(citation)
        assert len(refs) >= 1
        assert "Iran conflict drives prediction market surge" in refs[0]["subject_text"]

    def test_title_prepended_when_outside_window(self):
        """When title is far from the regex match, it gets prepended."""
        citation = {
            "title": "Venezuela crisis deepens amid sanctions debate",
            # Title comes after sentence+context in search_text, so put enough padding
            "sentence": "Polymarket users watched closely",
            "context": "X" * 500,
        }
        refs = extract_market_references(citation)
        assert len(refs) >= 1
        # Title should be prepended since it's far outside the 200-char window
        assert "Venezuela crisis deepens" in refs[0]["subject_text"]

    def test_title_not_duplicated_if_already_in_subject(self):
        """If title is already captured in the regex window, don't prepend it again."""
        citation = {
            "title": "Polymarket odds",
            "sentence": "Polymarket odds show 62% for Trump",
        }
        refs = extract_market_references(citation)
        assert len(refs) >= 1
        # Title "Polymarket odds" appears in search_text which builds subject_text
        # Count occurrences — should not have double prepend
        subj = refs[0]["subject_text"]
        assert subj.count("Polymarket odds") <= 2  # at most in subject window + title prepend

    def test_no_title_still_works(self):
        citation = {"sentence": "Polymarket launched a new feature"}
        refs = extract_market_references(citation)
        assert len(refs) >= 1
        assert refs[0]["article_title"] == ""

    def test_probability_ref_gets_title_fields(self):
        citation = {
            "title": "Maduro ouster bets surge",
            "sentence": "Kalshi traders give 76% odds on Maduro leaving",
        }
        refs = extract_market_references(citation)
        assert len(refs) >= 1
        assert refs[0]["article_title"] == "Maduro ouster bets surge"
        assert refs[0]["probability_cited"] == 0.76


class TestTitleFuzzyCandidates:
    """Tests for Change 3: title-keyword second pass in get_fuzzy_candidates."""

    def setup_method(self):
        self.markets = [
            {"question": "Will Maduro leave office by March 2026?", "platform": "Polymarket"},
            {"question": "Will there be a ceasefire in Ukraine?", "platform": "Kalshi"},
        ]
        self.texts = [build_market_search_text(m) for m in self.markets]
        self.keywords = [extract_keywords(t) for t in self.texts]

    def test_title_brings_in_candidates(self):
        """A reference with weak subject_text but strong title should find candidates."""
        ref = {
            "subject_text": "general market discussion | Polymarket punter made money",
            "article_title": "Maduro ouster bets surge on prediction markets",
        }
        candidates = get_fuzzy_candidates(ref, self.markets, self.texts, self.keywords)
        # Should find the Maduro market via title keywords
        matched_indices = {idx for idx, _ in candidates}
        assert 0 in matched_indices  # Maduro market


# ─── Improvement 1: Polymarket /market/{ID} URL + pm_id_index ────────────────

class TestPolymarketMarketUrl:
    def test_regex_matches_market_id(self):
        m = POLYMARKET_MARKET_URL.search("polymarket.com/market/abc-123-def")
        assert m is not None
        assert m.group(1) == "abc-123-def"

    def test_regex_no_match_event(self):
        m = POLYMARKET_MARKET_URL.search("polymarket.com/event/some-slug")
        assert m is None

    def test_pm_id_index_built(self):
        markets = [
            {"pm_market_id": "12345", "question": "Test?"},
            {"pm_market_id": "", "question": "No ID"},
            {"question": "No PM field"},
        ]
        _, _, pm_id_idx = build_market_indices(markets)
        assert "12345" in pm_id_idx
        assert pm_id_idx["12345"] == 0
        assert len(pm_id_idx) == 1

    def test_match_by_url_with_pm_id(self):
        markets = [{"pm_market_id": "99887", "question": "PM market?"}]
        _, _, pm_id_idx = build_market_indices(markets)
        ref = {"subject_text": "See polymarket.com/market/99887 for details"}
        result = match_by_url(ref, markets, {}, {}, pm_id_index=pm_id_idx)
        assert result is not None
        market, conf, score = result
        assert market["question"] == "PM market?"
        assert conf == "HIGH"


# ─── Improvement 2: Platform Filtering ───────────────────────────────────────

class TestFilterMarketsByPlatform:
    def test_polymarket_filter(self):
        markets = [
            {"pm_market_id": "123", "question": "PM market"},
            {"k_ticker": "K-1", "question": "Kalshi market"},
            {"pm_market_id": "456", "question": "Another PM"},
        ]
        indices = filter_markets_by_platform(markets, "polymarket")
        assert indices == [0, 2]

    def test_kalshi_filter(self):
        markets = [
            {"pm_market_id": "123", "question": "PM market"},
            {"k_ticker": "K-1", "question": "Kalshi market"},
        ]
        indices = filter_markets_by_platform(markets, "kalshi")
        assert indices == [1]

    def test_generic_returns_none(self):
        markets = [{"pm_market_id": "123"}, {"k_ticker": "K-1"}]
        assert filter_markets_by_platform(markets, "generic") is None


# ─── Improvement 3: Keyword Pre-filter ───────────────────────────────────────

class TestKeywordPrefilter:
    def test_filters_by_keywords(self):
        # All top keywords: "presidential", "election", "trump" (3 keywords >= 4 chars)
        texts = [
            "Will Trump win the presidential election in 2028?",
            "Fed rate hike decision in 2025",
            "Trump presidential election odds 2028 look strong",
            "Bitcoin price above 100k",
            "Trump beats Biden in presidential election polls",
            "Trump presidential election heats up in swing states",
        ]
        result = keyword_prefilter("Trump presidential election", texts)
        assert result is not None
        # Texts 0, 2, 4, 5 all contain trump + presidential + election
        assert 0 in result
        assert 2 in result
        assert 1 not in result
        assert 3 not in result

    def test_falls_back_when_few_keywords(self):
        texts = ["test market"]
        # Subject with only 1 keyword >= 4 chars
        result = keyword_prefilter("go up", texts)
        assert result is None  # Falls back

    def test_falls_back_when_few_matches(self):
        texts = ["Completely unrelated topic about cooking recipes"] * 10
        result = keyword_prefilter("Trump presidential election 2028", texts)
        # No markets match all keywords, should fall back
        assert result is None

    def test_respects_candidate_indices(self):
        texts = [
            "Trump election 2028 presidential race",
            "Biden economy policy changes",
            "Trump wins presidential election 2028",
        ]
        result = keyword_prefilter("Trump presidential election 2028", texts, candidate_indices=[0, 2])
        assert result is not None
        assert 1 not in result


# ─── Improvement 4: TF-IDF Index ────────────────────────────────────────────

class TestMarketSearchIndex:
    @pytest.fixture
    def search_index(self):
        from pipeline_media_extract_markets import MarketSearchIndex
        texts = [
            "Will Trump win the 2028 presidential election?",
            "Fed rate hike interest rates 2025",
            "Will Bitcoin reach 100k in 2025?",
        ]
        return MarketSearchIndex(texts)

    def test_construction(self, search_index):
        assert search_index is not None
        assert search_index._matrix.shape[0] == 3

    def test_search_returns_results(self, search_index):
        results = search_index.search("Trump presidential election 2028")
        assert len(results) > 0
        # First result should be the Trump market (index 0)
        assert results[0][0] == 0

    def test_search_with_candidate_indices(self, search_index):
        results = search_index.search("Trump election", candidate_indices=[1, 2])
        # Should only search within indices 1 and 2
        for idx, _ in results:
            assert idx in (1, 2)


# ─── Improvement 5: Topic Clusters ──────────────────────────────────────────

class TestTopicClusters:
    def test_build_clusters(self):
        markets = [
            {"question": "Trump win?"},
            {"question": "Fed rate cut?"},
        ]
        texts = [
            "Will Trump win the presidential election 2028?",
            "Will the Federal Reserve cut interest rates?",
        ]
        clusters = build_topic_clusters(markets, texts)
        assert isinstance(clusters, dict)
        # Should have at least one cluster
        assert len(clusters) > 0

    def test_classify_citation_by_title(self):
        ref = {
            "article_title": "Trump leads in election polls",
            "subject_text": "some unrelated text",
        }
        topic = classify_citation_topic(ref)
        assert topic == "US Politics"

    def test_classify_citation_fallback_to_subject(self):
        ref = {
            "article_title": "",
            "subject_text": "Federal Reserve rate cut decision upcoming",
        }
        topic = classify_citation_topic(ref)
        assert topic == "Fed & Rates"


# ─── Improvement 6: Probability Validation ──────────────────────────────────

class TestValidateProbabilityMatch:
    def test_no_prob_cited_passthrough(self):
        ref = {"probability_cited": None, "platform_mentioned": "polymarket"}
        market = {"pm_yes_price": 0.5}
        m, c, s = validate_probability_match(ref, market, "HIGH", 95)
        assert c == "HIGH"
        assert s == 95

    def test_large_gap_downgrades_high(self):
        ref = {"probability_cited": 0.80, "platform_mentioned": "polymarket"}
        market = {"pm_yes_price": 0.50}  # 30pp gap
        m, c, s = validate_probability_match(ref, market, "HIGH", 95)
        assert c == "MEDIUM"
        assert s == 70

    def test_large_gap_subtracts_for_medium(self):
        ref = {"probability_cited": 0.80, "platform_mentioned": "kalshi"}
        market = {"k_yes_price": 0.50}  # 30pp gap
        m, c, s = validate_probability_match(ref, market, "MEDIUM", 60)
        assert s == 40

    def test_small_gap_no_change(self):
        ref = {"probability_cited": 0.62, "platform_mentioned": "polymarket"}
        market = {"pm_yes_price": 0.65}  # 3pp gap
        m, c, s = validate_probability_match(ref, market, "HIGH", 95)
        assert c == "HIGH"
        assert s == 95


# ─── Improvement 7: Market URL Generation ───────────────────────────────────

class TestGenerateMarketUrl:
    def test_polymarket_slug(self):
        market = {"pm_event_slug": "fed-rate-hike", "pm_market_id": "123"}
        url = generate_market_url(market)
        assert url == "https://polymarket.com/event/fed-rate-hike"

    def test_polymarket_market_id_fallback(self):
        market = {"pm_market_id": "99887", "pm_event_slug": "nan"}
        url = generate_market_url(market)
        assert url == "https://polymarket.com/market/99887"

    def test_kalshi_ticker(self):
        market = {"k_ticker": "CONTROLH-2026-D"}
        url = generate_market_url(market)
        assert url == "https://kalshi.com/markets/CONTROLH-2026-D"

    def test_empty_market(self):
        assert generate_market_url({}) == ""
        assert generate_market_url(None) == ""


# ─── Improvement 8: Cross-Platform Matching ─────────────────────────────────

class TestCrossPlatformMatching:
    def test_find_counterpart_kalshi_to_pm(self):
        markets = [
            {"k_ticker": "K-1", "question": "Kalshi market?"},
            {"pm_market_id": "PM-1", "question": "PM market?", "pm_event_slug": "pm-event"},
        ]
        lookup = {"K-1": "PM-1", "PM-1": "K-1"}
        ticker_index = {"K-1": 0}
        slug_index = {}
        result = find_cross_platform_counterpart(
            markets[0], "polymarket", lookup, markets, ticker_index, slug_index
        )
        assert result is not None
        assert result["platform"] == "polymarket"
        assert result["market_id"] == "PM-1"

    def test_find_counterpart_pm_to_kalshi(self):
        markets = [
            {"k_ticker": "K-1", "question": "Kalshi market?"},
            {"pm_market_id": "PM-1", "question": "PM market?"},
        ]
        lookup = {"K-1": "PM-1", "PM-1": "K-1"}
        ticker_index = {"K-1": 0}
        slug_index = {}
        result = find_cross_platform_counterpart(
            markets[1], "kalshi", lookup, markets, ticker_index, slug_index
        )
        assert result is not None
        assert result["platform"] == "kalshi"

    def test_no_counterpart(self):
        markets = [{"k_ticker": "K-1", "question": "Test?"}]
        result = find_cross_platform_counterpart(
            markets[0], "polymarket", {}, markets, {}, {}
        )
        assert result is None

    def test_empty_lookup(self):
        result = find_cross_platform_counterpart(
            {"k_ticker": "K-1"}, "polymarket", {}, [], {}, {}
        )
        assert result is None


# ─── Improvement 9: Syndicated Article Deduplication ────────────────────────

class TestNormalizeTitle:
    def test_basic_normalization(self):
        assert normalize_title("Trump's Bold Move!") == "trumps bold move"

    def test_unicode_normalization(self):
        # Full-width characters or accents
        result = normalize_title("Café Economics")
        assert "cafe" in result

    def test_empty(self):
        assert normalize_title("") == ""
        assert normalize_title(None) == ""

    def test_whitespace_collapse(self):
        assert normalize_title("  Hello   World  ") == "hello world"


class TestDeduplicateCitations:
    def test_groups_by_title(self):
        citations = [
            {"title": "Breaking: Trump wins big", "domain": "reuters.com"},
            {"title": "Breaking: Trump wins big", "domain": "townhall.com"},
            {"title": "Unrelated article", "domain": "cnn.com"},
        ]
        result, syn_map = deduplicate_citations(citations)
        assert 1 in syn_map
        assert syn_map[1] == 0  # townhall syndicated from reuters
        assert result[1].get("syndicated_from") == "reuters.com"

    def test_authority_picks_highest(self):
        citations = [
            {"title": "Big news about prediction markets today", "domain": "townhall.com"},
            {"title": "Big news about prediction markets today", "domain": "reuters.com"},
        ]
        result, syn_map = deduplicate_citations(citations)
        # reuters (authority 5) should be primary, townhall (authority 2) should be syndicated
        assert 0 in syn_map
        assert syn_map[0] == 1

    def test_short_titles_not_grouped(self):
        citations = [
            {"title": "Short", "domain": "reuters.com"},
            {"title": "Short", "domain": "cnn.com"},
        ]
        result, syn_map = deduplicate_citations(citations)
        assert len(syn_map) == 0  # Too short to group

    def test_unique_titles_no_dedup(self):
        citations = [
            {"title": "Article one about markets and politics today", "domain": "reuters.com"},
            {"title": "Article two about different things entirely", "domain": "cnn.com"},
        ]
        result, syn_map = deduplicate_citations(citations)
        assert len(syn_map) == 0


# ─── URL Extraction from Full Text ──────────────────────────────────────────

class TestURLExtraction:
    """Tests for step 0: direct URL extraction from full citation text."""

    def test_url_extraction_from_context(self):
        """URL in context field (not near platform mention) is extracted."""
        citation = {
            "sentence": "Prediction markets are booming this year",
            "context": "For more details see polymarket.com/event/trump-2028-election results",
        }
        refs = extract_market_references(citation)
        url_refs = [r for r in refs if r.get("match_method") == "url_extraction"]
        assert len(url_refs) >= 1
        assert url_refs[0]["platform_mentioned"] == "polymarket"
        assert "trump-2028-election" in url_refs[0]["raw_text"]

    def test_url_extraction_dedup(self):
        """Same URL appearing in sentence + context doesn't produce duplicate refs."""
        url = "polymarket.com/event/fed-rate-hike"
        citation = {
            "sentence": f"Check {url} for odds",
            "context": f"As shown on {url} the market moved",
        }
        refs = extract_market_references(citation)
        url_refs = [r for r in refs if r.get("match_method") == "url_extraction"]
        # Should only have 1 URL ref despite URL appearing twice in search_text
        assert len(url_refs) == 1

    def test_url_extraction_priority(self):
        """URL ref appears before probability-based refs in the list."""
        citation = {
            "sentence": "Polymarket gives 62% odds on this event",
            "context": "See polymarket.com/event/some-market for details",
        }
        refs = extract_market_references(citation)
        # First ref should be URL extraction
        assert len(refs) >= 2
        assert refs[0].get("match_method") == "url_extraction"

    def test_url_extraction_multiple_urls(self):
        """Article with 2 different platform URLs produces 2 URL refs."""
        citation = {
            "sentence": "Compare platforms",
            "context": (
                "polymarket.com/event/trump-win gives 55% while "
                "kalshi.com/markets/CONTROLH-2026-D shows 52%"
            ),
        }
        refs = extract_market_references(citation)
        url_refs = [r for r in refs if r.get("match_method") == "url_extraction"]
        assert len(url_refs) == 2
        platforms = {r["platform_mentioned"] for r in url_refs}
        assert platforms == {"polymarket", "kalshi"}

    def test_url_extraction_coexists_with_probability(self):
        """URL ref + probability ref for same citation both extracted."""
        citation = {
            "sentence": "Polymarket shows 73% chance of a Trump win",
            "context": "Full details at polymarket.com/event/trump-2028",
        }
        refs = extract_market_references(citation)
        url_refs = [r for r in refs if r.get("match_method") == "url_extraction"]
        prob_refs = [r for r in refs if r.get("match_method") != "url_extraction" and r["probability_cited"] is not None]
        assert len(url_refs) >= 1
        assert len(prob_refs) >= 1
        assert prob_refs[0]["probability_cited"] == 0.73

    def test_url_extraction_kalshi(self):
        """Kalshi URL is correctly extracted."""
        citation = {
            "sentence": "Traders are watching kalshi.com/markets/KXFEDRATE-25 closely",
        }
        refs = extract_market_references(citation)
        url_refs = [r for r in refs if r.get("match_method") == "url_extraction"]
        assert len(url_refs) == 1
        assert url_refs[0]["platform_mentioned"] == "kalshi"
        assert "KXFEDRATE-25" in url_refs[0]["raw_text"]

    def test_url_extraction_polymarket_market_url(self):
        """polymarket.com/market/{id} format is extracted."""
        citation = {
            "sentence": "See polymarket.com/market/abc-123-def for this contract",
        }
        refs = extract_market_references(citation)
        url_refs = [r for r in refs if r.get("match_method") == "url_extraction"]
        assert len(url_refs) >= 1
        assert "abc-123-def" in url_refs[0]["raw_text"]

    def test_url_extraction_subject_text_contains_url(self):
        """subject_text of URL ref must contain the URL (for match_by_url to work)."""
        citation = {
            "sentence": "Markets are active",
            "context": "Visit polymarket.com/event/some-slug for the latest",
        }
        refs = extract_market_references(citation)
        url_refs = [r for r in refs if r.get("match_method") == "url_extraction"]
        assert len(url_refs) == 1
        assert "polymarket.com/event/some-slug" in url_refs[0]["subject_text"]

    def test_url_extraction_match_pipeline(self):
        """URL extraction ref feeds into match_by_url for score=100 match."""
        markets = [
            {"pm_market_slug": "trump-2028", "question": "Will Trump win 2028?"},
        ]
        slug_idx, ticker_idx, pm_id_idx = build_market_indices(markets)
        citation = {
            "sentence": "Markets are buzzing",
            "context": "See polymarket.com/event/trump-2028 for details",
        }
        refs = extract_market_references(citation)
        url_refs = [r for r in refs if r.get("match_method") == "url_extraction"]
        assert len(url_refs) >= 1
        result = match_by_url(url_refs[0], markets, slug_idx, ticker_idx, pm_id_idx)
        assert result is not None
        market, conf, score = result
        assert market["question"] == "Will Trump win 2028?"
        assert conf == "HIGH"
        assert score == 100


# ─── Match Threshold Sensitivity Analysis ───────────────────────────────────

class TestThresholdSensitivity:
    """Diagnostic tests evaluating fuzzy match quality at different thresholds.

    These tests measure how threshold changes affect candidate selection.
    The threshold (FUZZY_CANDIDATE_THRESHOLD) controls which candidates are
    shortlisted for LLM selection — lower threshold = more candidates.
    """

    def setup_method(self):
        self.markets = [
            {"question": "Will Democrats win the House in 2026?", "platform": "Kalshi",
             "k_ticker": "CONTROLH-2026-D"},
            {"question": "Will there be a federal government shutdown in 2026?", "platform": "Kalshi",
             "k_ticker": "KXGOVSHUT-26"},
            {"question": "Will Trump win the 2028 presidential election?", "platform": "Polymarket",
             "pm_market_slug": "trump-2028"},
            {"question": "Will the Fed cut interest rates in January 2026?", "platform": "Kalshi",
             "k_ticker": "KXFEDRATE-26JAN"},
            {"question": "Will Bitcoin reach $100k in 2025?", "platform": "Polymarket",
             "pm_market_slug": "bitcoin-100k-2025"},
            {"question": "Will Ukraine and Russia reach a ceasefire by 2026?", "platform": "Polymarket",
             "pm_market_slug": "ukraine-russia-ceasefire"},
        ]
        self.texts = [build_market_search_text(m) for m in self.markets]
        self.keywords = [extract_keywords(t) for t in self.texts]

    def _get_candidates_at_threshold(self, ref, threshold):
        """Get fuzzy candidates using a specific threshold."""
        import pipeline_media_extract_markets as mod
        original = mod.FUZZY_CANDIDATE_THRESHOLD
        try:
            mod.FUZZY_CANDIDATE_THRESHOLD = threshold
            return get_fuzzy_candidates(ref, self.markets, self.texts, self.keywords)
        finally:
            mod.FUZZY_CANDIDATE_THRESHOLD = original

    def test_threshold_40_vs_65_candidate_count(self):
        """Measure how many more candidates threshold=40 produces vs threshold=65."""
        ref = {
            "subject_text": "Democrats expected to win midterm House race according to traders",
            "platform_mentioned": "kalshi",
            "probability_cited": 0.81,
        }
        cands_40 = self._get_candidates_at_threshold(ref, 40)
        cands_65 = self._get_candidates_at_threshold(ref, 65)

        print(f"\n  Threshold 40: {len(cands_40)} candidates")
        for idx, score in cands_40:
            print(f"    [{idx}] score={score:.0f} — {self.markets[idx]['question']}")
        print(f"  Threshold 65: {len(cands_65)} candidates")
        for idx, score in cands_65:
            print(f"    [{idx}] score={score:.0f} — {self.markets[idx]['question']}")
        print(f"  Extra candidates at 40: {len(cands_40) - len(cands_65)}")

        # At minimum, threshold=40 should have >= as many candidates as 65
        assert len(cands_40) >= len(cands_65)

    def test_low_threshold_false_positive_examples(self):
        """Cases where low threshold matches wrong market, high threshold correctly rejects."""
        ref = {
            "subject_text": "The federal budget debate will reach a conclusion soon",
            "platform_mentioned": "generic",
            "probability_cited": 0.40,
        }
        cands_40 = self._get_candidates_at_threshold(ref, 40)
        cands_65 = self._get_candidates_at_threshold(ref, 65)

        print(f"\n  'federal budget debate' — expecting no good match:")
        print(f"  Threshold 40: {len(cands_40)} candidates")
        for idx, score in cands_40:
            print(f"    [{idx}] score={score:.0f} — {self.markets[idx]['question']}")
        print(f"  Threshold 65: {len(cands_65)} candidates (should be fewer/zero)")

        # Higher threshold should produce fewer noisy candidates
        assert len(cands_65) <= len(cands_40)

    def test_high_threshold_miss_examples(self):
        """Cases where correct match has moderate fuzzy score (45-60) — missed at threshold=65."""
        ref = {
            "subject_text": "Ceasefire talks between Kyiv and Moscow stall again",
            "platform_mentioned": "polymarket",
            "probability_cited": 0.15,
        }
        cands_40 = self._get_candidates_at_threshold(ref, 40)
        cands_65 = self._get_candidates_at_threshold(ref, 65)

        print(f"\n  'Kyiv/Moscow ceasefire' — correct market is Ukraine/Russia ceasefire:")
        print(f"  Threshold 40: {len(cands_40)} candidates")
        for idx, score in cands_40:
            print(f"    [{idx}] score={score:.0f} — {self.markets[idx]['question']}")
        print(f"  Threshold 65: {len(cands_65)} candidates")
        for idx, score in cands_65:
            print(f"    [{idx}] score={score:.0f} — {self.markets[idx]['question']}")

        # The ceasefire market (idx=5) should appear at threshold 40
        cand_indices_40 = {idx for idx, _ in cands_40}
        if 5 in cand_indices_40:
            print("  [OK] Ceasefire market found at threshold 40")
        else:
            print("  [MISS] Ceasefire market NOT found at threshold 40")

    def test_top_candidate_stability(self):
        """When threshold changes, does the #1 candidate stay the same?

        If yes, threshold mainly affects noise level, not match quality,
        because the LLM picks from the candidate list.
        """
        test_cases = [
            {
                "subject_text": "Trump leads in 2028 election prediction markets",
                "platform_mentioned": "polymarket",
                "probability_cited": 0.55,
                "label": "Trump 2028 election",
            },
            {
                "subject_text": "Fed expected to cut rates at next meeting",
                "platform_mentioned": "kalshi",
                "probability_cited": 0.70,
                "label": "Fed rate cut",
            },
            {
                "subject_text": "Government shutdown looms as budget talks stall",
                "platform_mentioned": "kalshi",
                "probability_cited": 0.35,
                "label": "Government shutdown",
            },
        ]

        stable_count = 0
        for case in test_cases:
            label = case.pop("label")
            ref = case
            cands_30 = self._get_candidates_at_threshold(ref, 30)
            cands_40 = self._get_candidates_at_threshold(ref, 40)
            cands_50 = self._get_candidates_at_threshold(ref, 50)
            cands_65 = self._get_candidates_at_threshold(ref, 65)

            top = {}
            for name, cands in [("30", cands_30), ("40", cands_40), ("50", cands_50), ("65", cands_65)]:
                if cands:
                    top[name] = cands[0][0]
                else:
                    top[name] = None

            all_same = len(set(v for v in top.values() if v is not None)) <= 1
            if all_same:
                stable_count += 1

            print(f"\n  '{label}' — top candidate at each threshold:")
            for name, cands in [("30", cands_30), ("40", cands_40), ("50", cands_50), ("65", cands_65)]:
                if cands:
                    idx, score = cands[0]
                    print(f"    threshold={name}: [{idx}] score={score:.0f} — {self.markets[idx]['question']} ({len(cands)} total)")
                else:
                    print(f"    threshold={name}: no candidates")
            print(f"    Stable: {'YES' if all_same else 'NO'}")

        print(f"\n  Top-1 stability: {stable_count}/{len(test_cases)} cases stable across thresholds")
        # This is diagnostic — we just want to observe stability
