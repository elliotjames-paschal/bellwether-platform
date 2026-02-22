"""
Bellwether Event ID (BEID) taxonomy and generation.

BEIDs are deterministic identifiers for events that can be matched
across platforms. They encode the key semantic fields of an event
in a human-readable format.

Format: BWR-{FRAME}-{COUNTRY}-{OFFICE}-{SUBJECT}-{OUTCOME}-{YEAR}

Examples:
- BWR-ELEC-US-PRES-TRUMP-WIN-2024
- BWR-ELEC-US-SEN-GA-DEM-WIN-2026
- BWR-RATE-US-FED-CUT-2025
- BWR-APPT-US-SCOTUS-CONFIRM-2025
"""
from __future__ import annotations

import re
from typing import Any

from unidecode import unidecode

# Valid city scope codes (from extractor)
_VALID_CITY_SCOPES = frozenset({
    'NYC', 'LA', 'CHICAGO', 'SF', 'BOSTON', 'ATLANTA', 'MIAMI',
    'DETROIT', 'OAKLAND', 'SEATTLE', 'DENVER', 'HOUSTON', 'DALLAS',
    'PHILA', 'PHOENIX', 'SAN_ANTONIO', 'SAN_DIEGO', 'AUSTIN',
    'MINNEAPOLIS', 'PITTSBURGH', 'CLEVELAND', 'NEW_ORLEANS',
    'CHARLOTTE', 'ALBUQUERQUE', 'JERSEY_CITY',
})

# District pattern: 2-letter state + dash + number (e.g., PA-10, CA-52)
_DISTRICT_PATTERN = re.compile(r'^[A-Z]{2}-\d+$')


def _is_valid_scope(scope: str) -> bool:
    """
    Validate scope against whitelist.

    Valid scopes:
    - US state abbreviations (2 letters, validated via pattern)
    - District patterns (e.g., PA-10, CA-52)
    - City codes from _VALID_CITY_SCOPES

    Invalid: "IN", "OR", "THE", random words from spaCy
    """
    if not scope:
        return False

    scope_upper = scope.upper()

    # Check city codes
    if scope_upper in _VALID_CITY_SCOPES:
        return True

    # Check district pattern (e.g., PA-10)
    if _DISTRICT_PATTERN.match(scope_upper):
        return True

    # Check US state abbreviation (2 letters only)
    # Use us package for validation
    try:
        import us
        if len(scope_upper) == 2 and us.states.lookup(scope_upper):
            return True
    except ImportError:
        # Fallback: accept 2-letter codes that aren't common words
        _INVALID_2LETTER = {'IN', 'OR', 'AN', 'AS', 'AT', 'BE', 'BY', 'DO',
                            'GO', 'IF', 'IS', 'IT', 'MY', 'NO', 'OF', 'ON',
                            'SO', 'TO', 'UP', 'US', 'WE'}
        if len(scope_upper) == 2 and scope_upper not in _INVALID_2LETTER:
            return True

    return False


def generate_beid(frame: dict[str, Any]) -> str | None:
    """
    Generate a deterministic Bellwether Event ID from an extracted frame.

    Args:
        frame: Extracted frame dictionary from extractor.extract_frame()

    Returns:
        BEID string or None if frame is insufficient for ID generation
    """
    frame_type = frame.get('frame_type')
    if not frame_type:
        return None

    # Route to frame-specific generator
    if frame_type == 'contest':
        return _generate_contest_beid(frame)
    elif frame_type == 'threshold':
        return _generate_threshold_beid(frame)
    elif frame_type == 'appointment':
        return _generate_appointment_beid(frame)
    elif frame_type == 'policy_change':
        return _generate_policy_beid(frame)
    elif frame_type == 'legislation':
        return _generate_legislation_beid(frame)
    elif frame_type == 'agreement':
        return _generate_agreement_beid(frame)
    else:
        return _generate_generic_beid(frame)


def _generate_contest_beid(frame: dict) -> str | None:
    """Generate BEID for electoral contest frame."""
    parts = ['BWR', 'ELEC']

    # Country (required for contest)
    country = frame.get('country')
    if not country:
        country = 'XX'  # Unknown country
    parts.append(country)

    # Office (required for contest)
    office = frame.get('office')
    if not office:
        return None  # Can't generate without office
    parts.append(office)

    # Scope (state, district, city) - optional, validated
    scope = frame.get('scope')
    if scope and _is_valid_scope(scope):
        parts.append(slugify(scope))

    # Subject: candidate name or party
    candidate = frame.get('candidate')
    party = frame.get('party')

    if candidate:
        parts.append(slugify_name(candidate))
    elif party:
        parts.append(party)
    else:
        # No subject - this is a generic race BEID
        pass

    # Outcome type
    outcome = frame.get('outcome_type', 'WIN')
    parts.append(outcome)

    # Year (required)
    year = frame.get('year')
    if year:
        parts.append(str(year))
    else:
        return None  # Can't generate without year

    # Check for primary
    if frame.get('is_primary'):
        parts.append('PRI')

    return '-'.join(parts)


def _generate_threshold_beid(frame: dict) -> str | None:
    """Generate BEID for threshold/metric frame."""
    parts = ['BWR', 'THRESH']

    # Actor (institution setting threshold)
    actor = frame.get('actor')
    if actor:
        parts.append(actor)

    # Metric
    metric = frame.get('metric')
    if metric:
        parts.append(metric)
    else:
        return None  # Need a metric

    # Threshold value
    threshold = frame.get('threshold_value')
    if threshold is not None:
        parts.append(slugify_number(threshold))

    # Direction
    direction = frame.get('threshold_direction')
    if direction:
        parts.append(direction.upper())

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    return '-'.join(parts) if len(parts) > 3 else None


def _generate_appointment_beid(frame: dict) -> str | None:
    """Generate BEID for appointment frame."""
    parts = ['BWR', 'APPT']

    # Country (use 'US' if None or missing)
    country = frame.get('country')
    if not country:
        country = 'US'
    parts.append(country)

    # Office/position
    office = frame.get('office')
    actor = frame.get('actor')

    if office:
        parts.append(office)
    elif actor:
        parts.append(actor)
    else:
        return None

    # Candidate name
    candidate = frame.get('candidate')
    if candidate:
        parts.append(slugify_name(candidate))

    # Outcome (use 'CONFIRM' if None or missing)
    outcome = frame.get('outcome_type')
    if not outcome:
        outcome = 'CONFIRM'
    parts.append(outcome)

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    return '-'.join(parts)


def _generate_policy_beid(frame: dict) -> str | None:
    """Generate BEID for policy change frame."""
    parts = ['BWR', 'POLICY']

    # Actor
    actor = frame.get('actor')
    if actor:
        parts.append(actor)

    # Metric/subject
    metric = frame.get('metric')
    if metric:
        parts.append(metric)

    # Direction
    direction = frame.get('threshold_direction')
    if direction:
        parts.append(direction.upper())

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    return '-'.join(parts) if len(parts) > 2 else None


def _generate_legislation_beid(frame: dict) -> str | None:
    """Generate BEID for legislation frame."""
    parts = ['BWR', 'LEG']

    # Actor (legislative body)
    actor = frame.get('actor')
    if actor:
        parts.append(actor)

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    # This needs more work - legislation is hard to uniquely identify
    return '-'.join(parts) if len(parts) > 2 else None


def _generate_agreement_beid(frame: dict) -> str | None:
    """Generate BEID for agreement frame."""
    parts = ['BWR', 'AGREE']

    # Parties involved
    actor = frame.get('actor')
    if actor:
        parts.append(actor)

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    return '-'.join(parts) if len(parts) > 2 else None


def _generate_generic_beid(frame: dict) -> str | None:
    """Generate BEID for generic/binary outcome frame."""
    # Generic frames are hard to match, return None
    return None


def slugify(text: str) -> str:
    """Convert text to uppercase slug format."""
    if not text:
        return ''

    # Convert to ASCII
    text = unidecode(text)

    # Uppercase and replace non-alphanumeric with underscore
    text = text.upper()
    text = re.sub(r'[^A-Z0-9]+', '_', text)

    # Remove leading/trailing underscores
    text = text.strip('_')

    return text


def slugify_name(name: str) -> str:
    """
    Convert a person name to a BEID-friendly slug.

    Extracts last name and normalizes.
    Examples:
        "Donald Trump" -> "TRUMP"
        "Joe Biden" -> "BIDEN"
        "Gustavo Petro" -> "PETRO"
        "Iván Cepeda Castro" -> "CEPEDA_CASTRO"
    """
    if not name:
        return ''

    # Convert to ASCII
    name = unidecode(name)

    # Split into parts
    parts = name.split()

    if len(parts) == 0:
        return ''
    elif len(parts) == 1:
        return slugify(parts[0])
    else:
        # Take last 1-2 parts as surname
        # Handle compound surnames (common in Spanish/Portuguese)
        # If last two parts are both short, might be compound
        if len(parts) >= 2 and len(parts[-2]) <= 4 and parts[-2].lower() not in ('de', 'del', 'la', 'van', 'von'):
            # Single surname
            return slugify(parts[-1])
        elif len(parts) >= 2 and parts[-2].lower() in ('de', 'del', 'la', 'van', 'von', 'da', 'dos', 'das'):
            # Compound with connector
            return slugify('_'.join(parts[-2:]))
        else:
            # Take last name
            return slugify(parts[-1])


def slugify_number(value: float) -> str:
    """
    Convert a numeric value to a BEID-friendly slug.

    Examples:
        4.5 -> "4P5" (4 point 5)
        100000 -> "100K"
        1000000 -> "1M"
    """
    if value == int(value):
        value = int(value)

    if isinstance(value, float):
        # Format with 'P' for decimal point
        return str(value).replace('.', 'P').replace('-', 'N')

    # Handle large integers
    if value >= 1_000_000_000:
        return f"{value // 1_000_000_000}B"
    elif value >= 1_000_000:
        return f"{value // 1_000_000}M"
    elif value >= 1_000:
        return f"{value // 1_000}K"
    else:
        return str(value)


def parse_beid(beid: str) -> dict[str, Any]:
    """
    Parse a BEID back into its component fields.

    Args:
        beid: BEID string

    Returns:
        dict with extracted fields
    """
    if not beid or not beid.startswith('BWR-'):
        return {}

    parts = beid.split('-')
    result = {'raw_beid': beid}

    if len(parts) < 3:
        return result

    # Frame type
    frame_prefix = parts[1]
    frame_map = {
        'ELEC': 'contest',
        'THRESH': 'threshold',
        'APPT': 'appointment',
        'POLICY': 'policy_change',
        'LEG': 'legislation',
        'AGREE': 'agreement',
    }
    result['frame_type'] = frame_map.get(frame_prefix, 'unknown')

    # Parse frame-specific fields
    if frame_prefix == 'ELEC' and len(parts) >= 5:
        result['country'] = parts[2]
        result['office'] = parts[3]
        # Remaining parts vary
        remaining = parts[4:]
        # Look for year at end
        if remaining and remaining[-1].isdigit() and len(remaining[-1]) == 4:
            result['year'] = int(remaining.pop())
        # Look for PRI flag
        if remaining and remaining[-1] == 'PRI':
            result['is_primary'] = True
            remaining.pop()
        # Look for outcome type
        if remaining and remaining[-1] in ('WIN', 'NOMINATION', 'CANDIDACY', 'CONTROL', 'MAJORITY'):
            result['outcome_type'] = remaining.pop()
        # Remaining is subject (candidate/party/scope)
        if remaining:
            result['subject'] = '_'.join(remaining)

    return result


def beid_matches(beid1: str, beid2: str, strict: bool = True) -> bool:
    """
    Check if two BEIDs refer to the same event.

    Args:
        beid1: First BEID
        beid2: Second BEID
        strict: If True, require exact match. If False, allow partial matches.

    Returns:
        True if BEIDs match
    """
    if strict:
        return beid1 == beid2

    # Parse both BEIDs
    parsed1 = parse_beid(beid1)
    parsed2 = parse_beid(beid2)

    # Must have same frame type
    if parsed1.get('frame_type') != parsed2.get('frame_type'):
        return False

    # Must have same year
    if parsed1.get('year') != parsed2.get('year'):
        return False

    # Frame-specific matching
    if parsed1.get('frame_type') == 'contest':
        # Must match on country, office
        if parsed1.get('country') != parsed2.get('country'):
            return False
        if parsed1.get('office') != parsed2.get('office'):
            return False
        # Subject can differ (different candidates for same race)
        return True

    return False


def get_race_beid(frame: dict) -> str | None:
    """
    Generate a race-level BEID (without candidate specificity).

    Includes outcome_type so that WIN vs NOM markets don't match.
    Example: BWR-ELEC-US-SEN-MN-WIN-2026 (different from NOM-2026)
    """
    parts = ['BWR', 'ELEC']

    country = frame.get('country')
    if not country:
        return None
    parts.append(country)

    office = frame.get('office')
    if not office:
        return None
    parts.append(office)

    # Include scope for sub-national races (state, district)
    scope = frame.get('scope')
    if scope:
        parts.append(slugify(scope))

    # Include outcome_type - WIN vs NOM vs CANDIDACY are different events
    outcome = frame.get('outcome_type')
    if outcome:
        parts.append(outcome)
    else:
        parts.append('WIN')  # Default to WIN if not specified

    year = frame.get('year')
    if not year:
        return None
    parts.append(str(year))

    # Include primary flag
    if frame.get('is_primary'):
        parts.append('PRI')

    return '-'.join(parts)


def get_event_beid(frame: dict) -> str | None:
    """
    Generate an event-level BEID for grouping (without subject specificity).

    Works for all frame types, not just electoral contests.
    Used to group markets about the same underlying event for matching.

    Examples:
        contest: BWR-ELEC-US-PRES-2024
        threshold: BWR-THRESH-SPX-5K-ABOVE-2024
        policy_change: BWR-POLICY-FED-RATE-2024
        appointment: BWR-APPT-US-FED_CHAIR-2025
    """
    frame_type = frame.get('frame_type')
    if not frame_type:
        return None

    if frame_type == 'contest':
        return get_race_beid(frame)

    elif frame_type == 'threshold':
        return _get_threshold_event_beid(frame)

    elif frame_type == 'policy_change':
        return _get_policy_event_beid(frame)

    elif frame_type == 'appointment':
        return _get_appointment_event_beid(frame)

    elif frame_type == 'legislation':
        return _get_legislation_event_beid(frame)

    elif frame_type == 'agreement':
        return _get_agreement_event_beid(frame)

    else:
        return None  # binary_outcome and unknown types can't be grouped


def _get_threshold_event_beid(frame: dict) -> str | None:
    """Generate event BEID for threshold frame (without exact value)."""
    parts = ['BWR', 'THRESH']

    # Metric is required for grouping
    metric = frame.get('metric')
    if not metric:
        return None
    parts.append(metric)

    # Include threshold value bucket for grouping similar thresholds
    threshold = frame.get('threshold_value')
    if threshold is not None:
        # Round to nearest bucket for grouping
        parts.append(slugify_number(threshold))

    # Direction
    direction = frame.get('threshold_direction')
    if direction:
        parts.append(direction.upper())

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    return '-'.join(parts) if len(parts) >= 3 else None


def _get_policy_event_beid(frame: dict) -> str | None:
    """Generate event BEID for policy change frame."""
    parts = ['BWR', 'POLICY']

    # Actor (e.g., FED, ECB)
    actor = frame.get('actor')
    if actor:
        parts.append(actor)

    # Metric/subject (e.g., RATE)
    metric = frame.get('metric')
    if metric:
        parts.append(metric)

    # Need at least actor or metric
    if len(parts) < 3:
        return None

    # Direction (optional but helps distinguish)
    direction = frame.get('threshold_direction')
    if direction:
        parts.append(direction.upper())

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    return '-'.join(parts)


def _get_appointment_event_beid(frame: dict) -> str | None:
    """Generate event BEID for appointment frame (without nominee name)."""
    parts = ['BWR', 'APPT']

    # Country
    country = frame.get('country')
    if country:
        parts.append(country)

    # Office/position being filled
    office = frame.get('office')
    actor = frame.get('actor')

    if office:
        parts.append(office)
    elif actor:
        parts.append(actor)
    else:
        return None  # Need position for grouping

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    return '-'.join(parts)


def _get_legislation_event_beid(frame: dict) -> str | None:
    """Generate event BEID for legislation frame."""
    parts = ['BWR', 'LEG']

    # Actor (legislative body)
    actor = frame.get('actor')
    if actor:
        parts.append(actor)

    # Country
    country = frame.get('country')
    if country:
        parts.append(country)

    # Need at least one identifier
    if len(parts) < 3:
        return None

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    return '-'.join(parts)


def _get_agreement_event_beid(frame: dict) -> str | None:
    """Generate event BEID for agreement frame."""
    parts = ['BWR', 'AGREE']

    # Actor/parties
    actor = frame.get('actor')
    if actor:
        parts.append(actor)

    # Need at least one identifier
    if len(parts) < 3:
        return None

    # Year
    year = frame.get('year')
    if year:
        parts.append(str(year))

    return '-'.join(parts)
