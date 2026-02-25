"""Tests for frame extraction."""

import pytest
import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from bellwether_matcher.extractor import (
    extract_frame,
    extract_candidate_name,
    _clean_question,
)


# Skip all tests if spaCy not available
try:
    from bellwether_matcher.extractor import load_nlp
    load_nlp()
    SPACY_AVAILABLE = True
except (ImportError, RuntimeError):
    SPACY_AVAILABLE = False


@pytest.mark.skipif(not SPACY_AVAILABLE, reason="spaCy model not installed")
class TestExtractFrame:
    """Tests for extract_frame function."""

    def test_basic_presidential_election(self):
        """Test extraction from a basic presidential election question."""
        question = "Will Donald Trump win the 2024 US Presidential Election?"
        frame = extract_frame(question)

        assert frame['frame_type'] == 'contest'
        assert frame['country'] == 'US'
        assert frame['office'] == 'PRES'
        assert frame['year'] == 2024
        assert frame['outcome_type'] == 'WIN'
        assert 'Trump' in frame.get('candidate', '') or 'TRUMP' in str(frame.get('extracted_names', []))

    def test_senate_race(self):
        """Test extraction from a Senate race question."""
        question = "Will the Republican Party win the Georgia Senate seat in 2026?"
        frame = extract_frame(question)

        assert frame['frame_type'] == 'contest'
        assert frame['country'] == 'US'
        assert frame['office'] == 'SEN'
        assert frame['year'] == 2026
        assert frame['party'] == 'GOP'
        assert 'GA' in str(frame.get('scope', ''))

    def test_foreign_election(self):
        """Test extraction from a non-US election."""
        question = "Will Gustavo Petro win the 2022 Colombian Presidential Election?"
        frame = extract_frame(question)

        assert frame['frame_type'] == 'contest'
        assert frame['country'] == 'CO'
        assert frame['office'] == 'PRES'
        assert frame['year'] == 2022

    def test_primary_detection(self):
        """Test detection of primary elections."""
        question = "Will Ron DeSantis win the 2024 Republican Presidential Primary?"
        frame = extract_frame(question)

        assert frame['frame_type'] == 'contest'
        assert frame['is_primary'] == True
        assert frame['party'] == 'GOP'

    def test_house_district(self):
        """Test extraction of House district races."""
        question = "Will the Republican candidate win OH-11 in the 2024 election?"
        frame = extract_frame(question)

        assert frame['frame_type'] == 'contest'
        assert frame['office'] == 'HOUSE'
        assert frame['scope'] == 'OH-11'
        assert frame['scope_type'] == 'district'

    def test_threshold_frame(self):
        """Test extraction of threshold/metric frame."""
        question = "Will the S&P 500 reach 5000 by the end of 2024?"
        frame = extract_frame(question)

        assert frame['frame_type'] == 'threshold'
        assert frame['metric'] == 'SPX'
        assert frame['threshold_value'] == 5000.0

    def test_fed_policy_frame(self):
        """Test extraction of Fed policy frame."""
        question = "Will the Federal Reserve cut interest rates in 2024?"
        frame = extract_frame(question)

        assert frame['frame_type'] in ('policy_change', 'threshold')
        assert frame['actor'] == 'FED'
        assert frame['year'] == 2024

    def test_appointment_frame(self):
        """Test extraction of appointment frame."""
        question = "Will Kevin Warsh be nominated as Fed Chair in 2025?"
        frame = extract_frame(question)

        assert frame['frame_type'] in ('appointment', 'contest')
        assert frame['office'] == 'FED_CHAIR' or frame['actor'] == 'FED'
        assert frame['year'] == 2025

    def test_metadata_override(self):
        """Test that metadata can override/supplement extraction."""
        question = "Will X win the election?"
        metadata = {
            'country': 'United States',
            'office': 'Governor',
            'election_year': 2024,
            'location': 'California',
        }
        frame = extract_frame(question, metadata)

        assert frame['country'] == 'US'
        assert frame['office'] == 'GOV'
        assert frame['year'] == 2024
        assert frame['scope'] == 'California'

    def test_extraction_confidence(self):
        """Test that confidence scores are reasonable."""
        # Well-formed question should have high confidence
        good_question = "Will Joe Biden win the 2024 US Presidential Election?"
        good_frame = extract_frame(good_question)
        assert good_frame['extraction_confidence'] >= 0.5

        # Vague question should have lower confidence
        vague_question = "Will it happen?"
        vague_frame = extract_frame(vague_question)
        assert vague_frame['extraction_confidence'] < good_frame['extraction_confidence']

    def test_empty_question(self):
        """Test handling of empty question."""
        frame = extract_frame("")
        assert frame['frame_type'] is None
        assert frame['extraction_confidence'] == 0.0

    def test_non_political_question(self):
        """Test handling of non-political question."""
        question = "Will Bitcoin reach $100,000 by 2025?"
        frame = extract_frame(question)

        # Should still extract threshold info
        assert frame['frame_type'] == 'threshold'
        assert frame['metric'] == 'BTC'


class TestExtractCandidateName:
    """Tests for candidate name parsing."""

    def test_simple_name(self):
        """Test simple two-part name."""
        assert extract_candidate_name("Donald Trump") == ("Donald", "Trump")
        assert extract_candidate_name("Joe Biden") == ("Joe", "Biden")

    def test_single_name(self):
        """Test single name."""
        assert extract_candidate_name("Macron") == ("", "Macron")

    def test_compound_name(self):
        """Test compound surname."""
        result = extract_candidate_name("Gustavo Petro")
        assert result[1] == "Petro"

    def test_title_removal(self):
        """Test removal of titles."""
        result = extract_candidate_name("Sen. Mitch McConnell")
        assert result[1] == "McConnell"

    def test_empty_name(self):
        """Test empty name."""
        assert extract_candidate_name("") is None
        assert extract_candidate_name(None) is None


class TestCleanQuestion:
    """Tests for question cleaning."""

    def test_unicode_normalization(self):
        """Test Unicode normalization."""
        assert _clean_question("José García") == "Jose Garcia"

    def test_whitespace_normalization(self):
        """Test whitespace normalization."""
        assert _clean_question("Will   Trump  win?") == "Will Trump win?"

    def test_preserves_content(self):
        """Test that content is preserved."""
        original = "Will Donald Trump win the 2024 election?"
        cleaned = _clean_question(original)
        assert "Donald Trump" in cleaned
        assert "2024" in cleaned
