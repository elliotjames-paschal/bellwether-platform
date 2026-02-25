"""Tests for cross-platform market matching."""

import pytest
import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from bellwether_matcher.matcher import (
    match_markets,
    MarketMatch,
    MatchResult,
    _fuzzy_name_match,
    _compare_frames,
    _compare_threshold_frames,
    _compare_policy_frames,
    _compare_appointment_frames,
    validate_match,
)


class TestMatchMarkets:
    """Tests for match_markets function."""

    def test_exact_match(self):
        """Test matching identical market questions."""
        kalshi_market = {
            'market_id': 'KXPRES-24-TRUMP',
            'question': 'Will Donald Trump win the 2024 US Presidential Election?',
            'platform': 'Kalshi',
        }
        kalshi_frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Donald Trump',
            'year': 2024,
            'outcome_type': 'WIN',
        }

        pm_market = {
            'market_id': '123456',
            'question': 'Will Donald Trump win the 2024 presidential election?',
            'platform': 'Polymarket',
        }
        pm_frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Donald Trump',
            'year': 2024,
            'outcome_type': 'WIN',
        }

        result = match_markets(
            [(kalshi_market, kalshi_frame)],
            [(pm_market, pm_frame)],
        )

        assert len(result.matches) == 1
        assert result.matches[0].match_confidence >= 0.8
        assert result.stats['unmatched_kalshi'] == 0
        assert result.stats['unmatched_polymarket'] == 0

    def test_no_match_different_year(self):
        """Test that different years don't match."""
        kalshi_frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Donald Trump',
            'year': 2024,
        }
        pm_frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Donald Trump',
            'year': 2028,  # Different year
        }

        result = match_markets(
            [({'market_id': '1'}, kalshi_frame)],
            [({'market_id': '2'}, pm_frame)],
        )

        assert len(result.matches) == 0
        assert result.stats['unmatched_kalshi'] == 1
        assert result.stats['unmatched_polymarket'] == 1

    def test_no_match_different_candidate(self):
        """Test that different candidates in same race don't match."""
        kalshi_frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Donald Trump',
            'year': 2024,
        }
        pm_frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Joe Biden',  # Different candidate
            'year': 2024,
        }

        result = match_markets(
            [({'market_id': '1'}, kalshi_frame)],
            [({'market_id': '2'}, pm_frame)],
        )

        # Should not match - different candidates
        assert len(result.matches) == 0

    def test_multiple_matches(self):
        """Test matching multiple markets."""
        kalshi_frames = [
            ({'market_id': 'K1'}, {
                'frame_type': 'contest',
                'country': 'US',
                'office': 'PRES',
                'candidate': 'Donald Trump',
                'year': 2024,
            }),
            ({'market_id': 'K2'}, {
                'frame_type': 'contest',
                'country': 'US',
                'office': 'SEN',
                'scope': 'GA',
                'party': 'DEM',
                'year': 2026,
            }),
        ]

        pm_frames = [
            ({'market_id': 'P1'}, {
                'frame_type': 'contest',
                'country': 'US',
                'office': 'PRES',
                'candidate': 'Donald Trump',
                'year': 2024,
            }),
            ({'market_id': 'P2'}, {
                'frame_type': 'contest',
                'country': 'US',
                'office': 'SEN',
                'scope': 'GA',
                'party': 'DEM',
                'year': 2026,
            }),
        ]

        result = match_markets(kalshi_frames, pm_frames)

        assert len(result.matches) == 2

    def test_unmatched_markets(self):
        """Test handling of markets without matches."""
        kalshi_frames = [
            ({'market_id': 'K1'}, {
                'frame_type': 'contest',
                'country': 'US',
                'office': 'PRES',
                'year': 2024,
            }),
            ({'market_id': 'K2'}, {
                'frame_type': 'threshold',  # Different frame type
                'metric': 'SPX',
                'year': 2024,
            }),
        ]

        pm_frames = [
            ({'market_id': 'P1'}, {
                'frame_type': 'contest',
                'country': 'US',
                'office': 'PRES',
                'year': 2024,
            }),
        ]

        result = match_markets(kalshi_frames, pm_frames)

        # Only one match possible
        assert len(result.matches) == 1
        assert result.stats['unmatched_kalshi'] == 1  # The threshold market
        assert result.stats['unmatched_polymarket'] == 0

    def test_min_confidence_filter(self):
        """Test that low confidence matches are filtered."""
        # Create frames that will have low match confidence
        kalshi_frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'year': 2024,
            # Missing candidate, party, etc.
        }
        pm_frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'year': 2024,
        }

        # With high threshold, might not match
        result_high = match_markets(
            [({'market_id': '1'}, kalshi_frame)],
            [({'market_id': '2'}, pm_frame)],
            min_confidence=0.95,
        )

        # With low threshold, should match
        result_low = match_markets(
            [({'market_id': '1'}, kalshi_frame)],
            [({'market_id': '2'}, pm_frame)],
            min_confidence=0.3,
        )

        # At least the low threshold should find a match
        assert len(result_low.matches) >= len(result_high.matches)


class TestFuzzyNameMatch:
    """Tests for fuzzy name matching."""

    def test_exact_match(self):
        """Test exact name match."""
        assert _fuzzy_name_match("Donald Trump", "Donald Trump") == 100

    def test_case_insensitive(self):
        """Test case insensitivity."""
        score = _fuzzy_name_match("donald trump", "DONALD TRUMP")
        assert score == 100

    def test_close_match(self):
        """Test close name match."""
        score = _fuzzy_name_match("Donald Trump", "Donald J Trump")
        assert score >= 80

    def test_partial_match(self):
        """Test partial name match."""
        score = _fuzzy_name_match("Trump", "Donald Trump")
        assert score >= 60

    def test_different_names(self):
        """Test different names."""
        score = _fuzzy_name_match("Donald Trump", "Joe Biden")
        assert score < 50

    def test_unicode_names(self):
        """Test Unicode name handling."""
        score = _fuzzy_name_match("José García", "Jose Garcia")
        assert score >= 90


class TestCompareFrames:
    """Tests for frame comparison."""

    def test_identical_frames(self):
        """Test comparison of identical frames."""
        frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Donald Trump',
            'year': 2024,
            'party': 'GOP',
            'outcome_type': 'WIN',
        }

        confidence, reasons = _compare_frames(frame, frame)
        assert confidence >= 0.9
        assert len(reasons) > 0

    def test_different_frame_type(self):
        """Test that different frame types return 0."""
        frame1 = {'frame_type': 'contest'}
        frame2 = {'frame_type': 'threshold'}

        confidence, reasons = _compare_frames(frame1, frame2)
        assert confidence == 0.0
        assert len(reasons) == 0

    def test_year_mismatch(self):
        """Test that year mismatch returns 0."""
        frame1 = {'frame_type': 'contest', 'year': 2024}
        frame2 = {'frame_type': 'contest', 'year': 2028}

        confidence, reasons = _compare_frames(frame1, frame2)
        assert confidence == 0.0

    def test_scope_mismatch(self):
        """Test that scope mismatch returns 0."""
        frame1 = {'frame_type': 'contest', 'scope': 'GA', 'year': 2024}
        frame2 = {'frame_type': 'contest', 'scope': 'OH', 'year': 2024}

        confidence, reasons = _compare_frames(frame1, frame2)
        assert confidence == 0.0

    def test_primary_vs_general(self):
        """Test that primary vs general don't match."""
        frame1 = {'frame_type': 'contest', 'year': 2024, 'is_primary': True}
        frame2 = {'frame_type': 'contest', 'year': 2024, 'is_primary': False}

        confidence, reasons = _compare_frames(frame1, frame2)
        assert confidence == 0.0


class TestValidateMatch:
    """Tests for match validation."""

    def test_valid_match_no_warnings(self):
        """Test that valid match has no warnings."""
        match = MarketMatch(
            beid="BWR-ELEC-US-PRES-TRUMP-WIN-2024",
            kalshi_market={'market_id': 'K1'},
            polymarket_market={'market_id': 'P1'},
            match_confidence=0.95,
            match_reasons=['frame_type=contest'],
            kalshi_frame={'extraction_confidence': 0.9},
            polymarket_frame={'extraction_confidence': 0.85},
        )

        warnings = validate_match(match)
        assert len(warnings) == 0

    def test_low_confidence_warning(self):
        """Test warning for low extraction confidence."""
        match = MarketMatch(
            beid="BWR-ELEC-US-PRES-TRUMP-WIN-2024",
            kalshi_market={'market_id': 'K1'},
            polymarket_market={'market_id': 'P1'},
            match_confidence=0.95,
            match_reasons=['frame_type=contest'],
            kalshi_frame={'extraction_confidence': 0.3},  # Low
            polymarket_frame={'extraction_confidence': 0.9},
        )

        warnings = validate_match(match)
        assert any('confidence' in w.lower() for w in warnings)


class TestCompareThresholdFrames:
    """Tests for threshold frame comparison."""

    def test_identical_threshold_frames(self):
        """Test matching identical threshold frames."""
        frame = {
            'frame_type': 'threshold',
            'metric': 'SPX',
            'threshold_value': 5000,
            'threshold_direction': 'above',
            'year': 2024,
        }

        confidence, reasons = _compare_threshold_frames(frame, frame)
        assert confidence >= 0.9
        assert 'metric=SPX' in reasons

    def test_different_metric(self):
        """Test that different metrics don't match."""
        frame1 = {'frame_type': 'threshold', 'metric': 'SPX', 'year': 2024}
        frame2 = {'frame_type': 'threshold', 'metric': 'BTC', 'year': 2024}

        confidence, reasons = _compare_threshold_frames(frame1, frame2)
        assert confidence == 0.0

    def test_similar_threshold_values(self):
        """Test that similar threshold values match."""
        frame1 = {'frame_type': 'threshold', 'metric': 'SPX', 'threshold_value': 5000, 'year': 2024}
        frame2 = {'frame_type': 'threshold', 'metric': 'SPX', 'threshold_value': 5000, 'year': 2024}

        confidence, reasons = _compare_threshold_frames(frame1, frame2)
        assert confidence >= 0.8
        assert any('threshold' in r for r in reasons)

    def test_different_threshold_values(self):
        """Test that very different thresholds don't match."""
        frame1 = {'frame_type': 'threshold', 'metric': 'SPX', 'threshold_value': 5000, 'year': 2024}
        frame2 = {'frame_type': 'threshold', 'metric': 'SPX', 'threshold_value': 6000, 'year': 2024}

        confidence, reasons = _compare_threshold_frames(frame1, frame2)
        assert confidence == 0.0


class TestComparePolicyFrames:
    """Tests for policy change frame comparison."""

    def test_identical_policy_frames(self):
        """Test matching identical policy frames."""
        frame = {
            'frame_type': 'policy_change',
            'actor': 'FED',
            'metric': 'RATE',
            'threshold_direction': 'decrease',
            'year': 2024,
        }

        confidence, reasons = _compare_policy_frames(frame, frame)
        assert confidence >= 0.9
        assert 'actor=FED' in reasons

    def test_different_actor(self):
        """Test that different actors don't match."""
        frame1 = {'frame_type': 'policy_change', 'actor': 'FED', 'year': 2024}
        frame2 = {'frame_type': 'policy_change', 'actor': 'ECB', 'year': 2024}

        confidence, reasons = _compare_policy_frames(frame1, frame2)
        assert confidence == 0.0

    def test_same_actor_different_direction(self):
        """Test same actor but different direction still matches (lower confidence)."""
        frame1 = {'frame_type': 'policy_change', 'actor': 'FED', 'threshold_direction': 'increase', 'year': 2024}
        frame2 = {'frame_type': 'policy_change', 'actor': 'FED', 'threshold_direction': 'decrease', 'year': 2024}

        confidence, reasons = _compare_policy_frames(frame1, frame2)
        # Should have some match since same actor, but lower than perfect
        assert confidence > 0.0
        assert confidence < 0.9


class TestCompareAppointmentFrames:
    """Tests for appointment frame comparison."""

    def test_identical_appointment_frames(self):
        """Test matching identical appointment frames."""
        frame = {
            'frame_type': 'appointment',
            'office': 'FED_CHAIR',
            'candidate': 'Kevin Warsh',
            'year': 2025,
            'country': 'US',
        }

        confidence, reasons = _compare_appointment_frames(frame, frame)
        assert confidence >= 0.9
        assert 'office=FED_CHAIR' in reasons

    def test_different_office(self):
        """Test that different offices don't match."""
        frame1 = {'frame_type': 'appointment', 'office': 'FED_CHAIR', 'year': 2025}
        frame2 = {'frame_type': 'appointment', 'office': 'SCOTUS', 'year': 2025}

        confidence, reasons = _compare_appointment_frames(frame1, frame2)
        assert confidence == 0.0

    def test_different_candidate(self):
        """Test that different candidates don't match."""
        frame1 = {'frame_type': 'appointment', 'office': 'FED_CHAIR', 'candidate': 'Kevin Warsh', 'year': 2025}
        frame2 = {'frame_type': 'appointment', 'office': 'FED_CHAIR', 'candidate': 'Jerome Powell', 'year': 2025}

        confidence, reasons = _compare_appointment_frames(frame1, frame2)
        assert confidence == 0.0


class TestNonElectoralMatching:
    """Tests for matching non-electoral markets."""

    def test_match_threshold_markets(self):
        """Test matching threshold markets across platforms."""
        kalshi_frame = {
            'frame_type': 'threshold',
            'metric': 'SPX',
            'threshold_value': 5000,
            'threshold_direction': 'above',
            'year': 2024,
        }
        pm_frame = {
            'frame_type': 'threshold',
            'metric': 'SPX',
            'threshold_value': 5000,
            'threshold_direction': 'above',
            'year': 2024,
        }

        result = match_markets(
            [({'market_id': 'K1'}, kalshi_frame)],
            [({'market_id': 'P1'}, pm_frame)],
        )

        assert len(result.matches) == 1
        assert result.matches[0].match_confidence >= 0.8

    def test_match_policy_markets(self):
        """Test matching policy change markets across platforms."""
        kalshi_frame = {
            'frame_type': 'policy_change',
            'actor': 'FED',
            'metric': 'RATE',
            'threshold_direction': 'decrease',
            'year': 2024,
        }
        pm_frame = {
            'frame_type': 'policy_change',
            'actor': 'FED',
            'metric': 'RATE',
            'threshold_direction': 'decrease',
            'year': 2024,
        }

        result = match_markets(
            [({'market_id': 'K1'}, kalshi_frame)],
            [({'market_id': 'P1'}, pm_frame)],
        )

        assert len(result.matches) == 1

    def test_no_match_electoral_vs_threshold(self):
        """Test that electoral and threshold frames don't match."""
        kalshi_frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'year': 2024,
        }
        pm_frame = {
            'frame_type': 'threshold',
            'metric': 'SPX',
            'year': 2024,
        }

        result = match_markets(
            [({'market_id': 'K1'}, kalshi_frame)],
            [({'market_id': 'P1'}, pm_frame)],
        )

        # Should not match - different frame types
        assert len(result.matches) == 0
