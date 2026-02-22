"""Tests for BEID taxonomy and generation."""

import pytest
import sys
from pathlib import Path

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from bellwether_matcher.taxonomy import (
    generate_beid,
    slugify,
    slugify_name,
    slugify_number,
    parse_beid,
    beid_matches,
    get_race_beid,
    get_event_beid,
)


class TestGenerateBEID:
    """Tests for BEID generation."""

    def test_presidential_race(self):
        """Test BEID generation for presidential race."""
        frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Donald Trump',
            'outcome_type': 'WIN',
            'year': 2024,
        }
        beid = generate_beid(frame)

        assert beid is not None
        assert beid.startswith('BWR-ELEC-')
        assert 'US' in beid
        assert 'PRES' in beid
        assert 'TRUMP' in beid
        assert '2024' in beid

    def test_senate_race_with_state(self):
        """Test BEID for Senate race with state scope."""
        frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'SEN',
            'scope': 'GA',
            'party': 'DEM',
            'outcome_type': 'WIN',
            'year': 2026,
        }
        beid = generate_beid(frame)

        assert beid is not None
        assert 'SEN' in beid
        assert 'GA' in beid
        assert 'DEM' in beid
        assert '2026' in beid

    def test_primary_election(self):
        """Test BEID for primary election."""
        frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Ron DeSantis',
            'outcome_type': 'NOMINATION',
            'year': 2024,
            'is_primary': True,
        }
        beid = generate_beid(frame)

        assert beid is not None
        assert 'PRI' in beid
        assert 'DESANTIS' in beid

    def test_foreign_election(self):
        """Test BEID for non-US election."""
        frame = {
            'frame_type': 'contest',
            'country': 'CO',
            'office': 'PRES',
            'candidate': 'Gustavo Petro',
            'outcome_type': 'WIN',
            'year': 2022,
        }
        beid = generate_beid(frame)

        assert beid is not None
        assert 'CO' in beid
        assert 'PETRO' in beid

    def test_threshold_beid(self):
        """Test BEID for threshold frame."""
        frame = {
            'frame_type': 'threshold',
            'metric': 'SPX',
            'threshold_value': 5000,
            'threshold_direction': 'above',
            'year': 2024,
        }
        beid = generate_beid(frame)

        assert beid is not None
        assert beid.startswith('BWR-THRESH-')
        assert 'SPX' in beid

    def test_policy_beid(self):
        """Test BEID for policy change frame."""
        frame = {
            'frame_type': 'policy_change',
            'actor': 'FED',
            'metric': 'RATE',
            'threshold_direction': 'decrease',
            'year': 2024,
        }
        beid = generate_beid(frame)

        assert beid is not None
        assert beid.startswith('BWR-POLICY-')
        assert 'FED' in beid

    def test_appointment_beid(self):
        """Test BEID for appointment frame."""
        frame = {
            'frame_type': 'appointment',
            'country': 'US',
            'office': 'FED_CHAIR',
            'candidate': 'Kevin Warsh',
            'outcome_type': 'CONFIRM',
            'year': 2025,
        }
        beid = generate_beid(frame)

        assert beid is not None
        assert beid.startswith('BWR-APPT-')
        assert 'WARSH' in beid

    def test_missing_required_field(self):
        """Test that BEID returns None when required fields missing."""
        # Contest without year
        frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
        }
        assert generate_beid(frame) is None

        # Contest without office
        frame = {
            'frame_type': 'contest',
            'country': 'US',
            'year': 2024,
        }
        assert generate_beid(frame) is None

    def test_empty_frame(self):
        """Test BEID with empty/minimal frame."""
        assert generate_beid({}) is None
        assert generate_beid({'frame_type': 'binary_outcome'}) is None


class TestSlugify:
    """Tests for slug generation."""

    def test_basic_slugify(self):
        """Test basic text slugification."""
        assert slugify("New York") == "NEW_YORK"
        assert slugify("CA-52") == "CA_52"

    def test_unicode_slugify(self):
        """Test Unicode handling."""
        assert slugify("São Paulo") == "SAO_PAULO"

    def test_special_chars(self):
        """Test special character handling."""
        assert slugify("test@#$%123") == "TEST_123"


class TestSlugifyName:
    """Tests for name slugification."""

    def test_simple_name(self):
        """Test simple name."""
        assert slugify_name("Donald Trump") == "TRUMP"
        assert slugify_name("Joe Biden") == "BIDEN"

    def test_single_name(self):
        """Test single name."""
        assert slugify_name("Madonna") == "MADONNA"

    def test_unicode_name(self):
        """Test Unicode name."""
        assert slugify_name("José García") == "GARCIA"

    def test_empty_name(self):
        """Test empty name."""
        assert slugify_name("") == ""
        assert slugify_name(None) == ""


class TestSlugifyNumber:
    """Tests for number slugification."""

    def test_integer(self):
        """Test integer formatting."""
        assert slugify_number(100) == "100"

    def test_large_number(self):
        """Test large number formatting."""
        assert slugify_number(5000000) == "5M"
        assert slugify_number(1500000000) == "1B"

    def test_decimal(self):
        """Test decimal formatting."""
        assert slugify_number(4.5) == "4P5"


class TestParseBEID:
    """Tests for BEID parsing."""

    def test_parse_contest(self):
        """Test parsing contest BEID."""
        beid = "BWR-ELEC-US-PRES-TRUMP-WIN-2024"
        parsed = parse_beid(beid)

        assert parsed['frame_type'] == 'contest'
        assert parsed['country'] == 'US'
        assert parsed['office'] == 'PRES'
        assert parsed['year'] == 2024
        assert parsed['outcome_type'] == 'WIN'
        assert 'TRUMP' in parsed.get('subject', '')

    def test_parse_primary(self):
        """Test parsing primary election BEID."""
        beid = "BWR-ELEC-US-PRES-DESANTIS-NOMINATION-2024-PRI"
        parsed = parse_beid(beid)

        assert parsed['is_primary'] == True
        assert parsed['outcome_type'] == 'NOMINATION'

    def test_parse_invalid(self):
        """Test parsing invalid BEID."""
        assert parse_beid("") == {}
        assert parse_beid("INVALID") == {}
        assert parse_beid("BWR-") == {}


class TestBEIDMatches:
    """Tests for BEID matching."""

    def test_exact_match(self):
        """Test exact BEID match."""
        beid = "BWR-ELEC-US-PRES-TRUMP-WIN-2024"
        assert beid_matches(beid, beid, strict=True) == True

    def test_different_beids(self):
        """Test different BEIDs don't match."""
        beid1 = "BWR-ELEC-US-PRES-TRUMP-WIN-2024"
        beid2 = "BWR-ELEC-US-PRES-BIDEN-WIN-2024"
        assert beid_matches(beid1, beid2, strict=True) == False

    def test_non_strict_race_match(self):
        """Test non-strict matching for same race, different candidates."""
        beid1 = "BWR-ELEC-US-PRES-TRUMP-WIN-2024"
        beid2 = "BWR-ELEC-US-PRES-BIDEN-WIN-2024"
        # These are same race but different candidates
        assert beid_matches(beid1, beid2, strict=False) == True


class TestGetRaceBEID:
    """Tests for race-level BEID generation."""

    def test_presidential_race(self):
        """Test race BEID for presidential race."""
        frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'candidate': 'Donald Trump',
            'year': 2024,
        }
        race_beid = get_race_beid(frame)

        assert race_beid is not None
        assert 'TRUMP' not in race_beid  # No candidate in race BEID
        assert 'US' in race_beid
        assert 'PRES' in race_beid
        assert '2024' in race_beid

    def test_senate_race_with_state(self):
        """Test race BEID includes state scope."""
        frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'SEN',
            'scope': 'GA',
            'year': 2026,
        }
        race_beid = get_race_beid(frame)

        assert 'GA' in race_beid

    def test_missing_required(self):
        """Test race BEID returns None without required fields."""
        assert get_race_beid({'frame_type': 'contest'}) is None
        assert get_race_beid({'frame_type': 'contest', 'country': 'US'}) is None


class TestGetEventBEID:
    """Tests for event-level BEID generation (all frame types)."""

    def test_contest_uses_race_beid(self):
        """Test that contest frames use get_race_beid."""
        frame = {
            'frame_type': 'contest',
            'country': 'US',
            'office': 'PRES',
            'year': 2024,
        }
        event_beid = get_event_beid(frame)
        race_beid = get_race_beid(frame)

        assert event_beid == race_beid
        assert 'ELEC' in event_beid

    def test_threshold_event(self):
        """Test event BEID for threshold frame."""
        frame = {
            'frame_type': 'threshold',
            'metric': 'SPX',
            'threshold_value': 5000,
            'threshold_direction': 'above',
            'year': 2024,
        }
        event_beid = get_event_beid(frame)

        assert event_beid is not None
        assert 'THRESH' in event_beid
        assert 'SPX' in event_beid
        assert '2024' in event_beid

    def test_policy_event(self):
        """Test event BEID for policy change frame."""
        frame = {
            'frame_type': 'policy_change',
            'actor': 'FED',
            'metric': 'RATE',
            'threshold_direction': 'decrease',
            'year': 2024,
        }
        event_beid = get_event_beid(frame)

        assert event_beid is not None
        assert 'POLICY' in event_beid
        assert 'FED' in event_beid
        assert 'RATE' in event_beid

    def test_appointment_event(self):
        """Test event BEID for appointment frame."""
        frame = {
            'frame_type': 'appointment',
            'country': 'US',
            'office': 'FED_CHAIR',
            'candidate': 'Kevin Warsh',
            'year': 2025,
        }
        event_beid = get_event_beid(frame)

        assert event_beid is not None
        assert 'APPT' in event_beid
        assert 'FED_CHAIR' in event_beid
        # Candidate name should NOT be in event BEID (for grouping)
        assert 'WARSH' not in event_beid

    def test_binary_outcome_returns_none(self):
        """Test that binary_outcome frames return None."""
        frame = {
            'frame_type': 'binary_outcome',
            'year': 2024,
        }
        assert get_event_beid(frame) is None

    def test_threshold_without_metric_returns_none(self):
        """Test that threshold without metric returns None."""
        frame = {
            'frame_type': 'threshold',
            'year': 2024,
        }
        assert get_event_beid(frame) is None

    def test_appointment_without_office_returns_none(self):
        """Test that appointment without office/actor returns None."""
        frame = {
            'frame_type': 'appointment',
            'year': 2025,
        }
        assert get_event_beid(frame) is None
