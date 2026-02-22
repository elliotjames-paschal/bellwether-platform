"""
Cross-platform market matching using extracted frames.

Matches equivalent markets across Kalshi and Polymarket using
semantic frame comparison and fuzzy name matching.
"""
from __future__ import annotations

from typing import Any
from dataclasses import dataclass, field

from thefuzz import fuzz
from unidecode import unidecode

from .taxonomy import generate_beid, get_event_beid, get_race_beid, slugify_name


@dataclass
class MarketMatch:
    """Represents a matched pair of markets."""
    beid: str
    kalshi_market: dict
    polymarket_market: dict
    match_confidence: float
    match_reasons: list[str] = field(default_factory=list)
    kalshi_frame: dict = field(default_factory=dict)
    polymarket_frame: dict = field(default_factory=dict)


@dataclass
class MatchResult:
    """Result of matching attempt."""
    matches: list[MarketMatch]
    unmatched_kalshi: list[dict]
    unmatched_polymarket: list[dict]
    stats: dict = field(default_factory=dict)


def match_markets(
    kalshi_frames: list[dict],
    polymarket_frames: list[dict],
    min_confidence: float = 0.6,
    fuzzy_name_threshold: int = 85,
) -> MatchResult:
    """
    Match markets across platforms using extracted frames.

    Args:
        kalshi_frames: List of (market_dict, frame) tuples for Kalshi
        polymarket_frames: List of (market_dict, frame) tuples for Polymarket
        min_confidence: Minimum confidence score for a match (0-1)
        fuzzy_name_threshold: Minimum fuzz ratio for name matches (0-100)

    Returns:
        MatchResult with matches, unmatched markets, and stats
    """
    matches = []
    matched_kalshi_ids = set()
    matched_pm_ids = set()
    category_mismatches = 0
    year_mismatches = 0

    # Group markets by event BEID for efficient matching (works for all frame types)
    kalshi_by_event = _group_by_event(kalshi_frames)
    pm_by_event = _group_by_event(polymarket_frames)

    # Find events that exist on both platforms
    common_events = set(kalshi_by_event.keys()) & set(pm_by_event.keys())

    # Match markets within each common event
    for event_beid in common_events:
        kalshi_in_event = kalshi_by_event[event_beid]
        pm_in_event = pm_by_event[event_beid]

        # For each Kalshi market, find best PM match
        for k_market, k_frame in kalshi_in_event:
            best_match = None
            best_confidence = 0.0
            best_reasons = []

            for pm_market, pm_frame in pm_in_event:
                # Skip if already matched
                pm_id = _get_market_id(pm_market, 'polymarket')
                if pm_id in matched_pm_ids:
                    continue

                # PRE-FILTER 1: Categories must match exactly
                # Categories are consistently applied by our classification pipeline,
                # so disagreement means they're not the same event
                k_category = _normalize_category(k_frame.get('political_category'))
                pm_category = _normalize_category(pm_frame.get('political_category'))
                if k_category and pm_category and k_category != pm_category:
                    category_mismatches += 1
                    continue

                # PRE-FILTER 2: Years must match
                # Year is extracted from question text or market close time
                k_year = k_frame.get('year')
                pm_year = pm_frame.get('year')
                if k_year and pm_year and k_year != pm_year:
                    year_mismatches += 1
                    continue

                # Compare frames (BEID comparison happens here)
                confidence, reasons = _compare_frames(
                    k_frame, pm_frame, fuzzy_name_threshold
                )

                if confidence > best_confidence and confidence >= min_confidence:
                    best_confidence = confidence
                    best_match = (pm_market, pm_frame)
                    best_reasons = reasons

            if best_match:
                pm_market, pm_frame = best_match

                # Generate BEID (prefer the more complete frame)
                beid = generate_beid(k_frame) or generate_beid(pm_frame) or event_beid

                match = MarketMatch(
                    beid=beid,
                    kalshi_market=k_market,
                    polymarket_market=pm_market,
                    match_confidence=best_confidence,
                    match_reasons=best_reasons,
                    kalshi_frame=k_frame,
                    polymarket_frame=pm_frame,
                )
                matches.append(match)

                # Mark as matched
                k_id = _get_market_id(k_market, 'kalshi')
                pm_id = _get_market_id(pm_market, 'polymarket')
                matched_kalshi_ids.add(k_id)
                matched_pm_ids.add(pm_id)

    # Collect unmatched markets
    unmatched_kalshi = [
        m for m, f in kalshi_frames
        if _get_market_id(m, 'kalshi') not in matched_kalshi_ids
    ]
    unmatched_pm = [
        m for m, f in polymarket_frames
        if _get_market_id(m, 'polymarket') not in matched_pm_ids
    ]

    # Calculate stats
    stats = {
        'total_kalshi': len(kalshi_frames),
        'total_polymarket': len(polymarket_frames),
        'matches_found': len(matches),
        'common_events': len(common_events),
        'unmatched_kalshi': len(unmatched_kalshi),
        'unmatched_polymarket': len(unmatched_pm),
        'category_mismatches': category_mismatches,
        'year_mismatches': year_mismatches,
        'match_rate_kalshi': len(matches) / len(kalshi_frames) if kalshi_frames else 0,
        'match_rate_polymarket': len(matches) / len(polymarket_frames) if polymarket_frames else 0,
    }

    return MatchResult(
        matches=matches,
        unmatched_kalshi=unmatched_kalshi,
        unmatched_polymarket=unmatched_pm,
        stats=stats,
    )


def _group_by_event(frames: list[tuple[dict, dict]]) -> dict[str, list[tuple[dict, dict]]]:
    """Group markets by their event-level BEID (works for all frame types)."""
    grouped = {}
    for market, frame in frames:
        event_beid = get_event_beid(frame)
        if event_beid:
            if event_beid not in grouped:
                grouped[event_beid] = []
            grouped[event_beid].append((market, frame))
    return grouped


def _get_market_id(market: dict, platform: str) -> str:
    """Extract unique market identifier."""
    if platform == 'kalshi':
        return market.get('market_id') or market.get('ticker', '')
    else:
        return market.get('market_id') or market.get('pm_condition_id', '')


def _normalize_category(category: str | None) -> str | None:
    """
    Normalize political category for comparison.

    Categories like "1. ELECTORAL" -> "ELECTORAL"
    Returns None if category is empty/None.
    """
    if not category:
        return None

    category = str(category).strip()
    if not category:
        return None

    # Remove numeric prefix like "1. " or "10. "
    import re
    category = re.sub(r'^\d+\.\s*', '', category)

    # Uppercase for consistent comparison
    return category.upper()


def _compare_frames(
    frame1: dict,
    frame2: dict,
    fuzzy_name_threshold: int = 85,
) -> tuple[float, list[str]]:
    """
    Compare two frames and return confidence score and reasons.

    Returns:
        Tuple of (confidence score 0-1, list of match reasons)
    """
    frame_type = frame1.get('frame_type')

    # Frame type must match
    if frame_type != frame2.get('frame_type'):
        return (0.0, [])

    # Route to frame-specific comparison
    if frame_type == 'contest':
        return _compare_contest_frames(frame1, frame2, fuzzy_name_threshold)
    elif frame_type == 'threshold':
        return _compare_threshold_frames(frame1, frame2)
    elif frame_type == 'policy_change':
        return _compare_policy_frames(frame1, frame2)
    elif frame_type == 'appointment':
        return _compare_appointment_frames(frame1, frame2, fuzzy_name_threshold)
    else:
        # Generic comparison for other types
        return _compare_generic_frames(frame1, frame2)


def _compare_contest_frames(
    frame1: dict,
    frame2: dict,
    fuzzy_name_threshold: int = 85,
) -> tuple[float, list[str]]:
    """Compare electoral contest frames."""
    score = 0.0
    max_score = 0.0
    reasons = []

    # Frame type match (already verified)
    max_score += 2.0
    score += 2.0
    reasons.append(f"frame_type={frame1['frame_type']}")

    # Country match
    max_score += 1.0
    if frame1.get('country') and frame1.get('country') == frame2.get('country'):
        score += 1.0
        reasons.append(f"country={frame1['country']}")

    # Office match
    max_score += 1.5
    if frame1.get('office') and frame1.get('office') == frame2.get('office'):
        score += 1.5
        reasons.append(f"office={frame1['office']}")

    # Year match
    max_score += 2.0
    if frame1.get('year') and frame1.get('year') == frame2.get('year'):
        score += 2.0
        reasons.append(f"year={frame1['year']}")
    elif frame1.get('year') and frame2.get('year'):
        return (0.0, [])  # Year mismatch is fatal

    # Scope match
    max_score += 1.0
    scope1 = frame1.get('scope')
    scope2 = frame2.get('scope')
    if scope1 and scope2:
        if scope1 == scope2:
            score += 1.0
            reasons.append(f"scope={scope1}")
        else:
            return (0.0, [])  # Scope mismatch is fatal

    # Candidate name match (fuzzy)
    max_score += 2.0
    cand1 = frame1.get('candidate')
    cand2 = frame2.get('candidate')
    if cand1 and cand2:
        name_score = _fuzzy_name_match(cand1, cand2)
        if name_score >= fuzzy_name_threshold:
            score += 2.0 * (name_score / 100)
            reasons.append(f"candidate_match={name_score}%")
        else:
            return (0.0, [])  # Name mismatch is fatal
    elif cand1 or cand2:
        score += 0.5  # Partial credit

    # Party match
    max_score += 0.5
    if frame1.get('party') and frame1.get('party') == frame2.get('party'):
        score += 0.5
        reasons.append(f"party={frame1['party']}")

    # Outcome type match
    max_score += 0.5
    if frame1.get('outcome_type') and frame1.get('outcome_type') == frame2.get('outcome_type'):
        score += 0.5
        reasons.append(f"outcome={frame1['outcome_type']}")

    # Primary flag match
    max_score += 0.5
    is_primary1 = frame1.get('is_primary', False)
    is_primary2 = frame2.get('is_primary', False)
    if is_primary1 == is_primary2:
        score += 0.5
        if is_primary1:
            reasons.append("is_primary=True")
    else:
        return (0.0, [])  # Primary vs general is fatal

    confidence = score / max_score if max_score > 0 else 0.0
    return (round(confidence, 3), reasons)


def _compare_threshold_frames(frame1: dict, frame2: dict) -> tuple[float, list[str]]:
    """Compare threshold/metric frames."""
    score = 0.0
    max_score = 0.0
    reasons = []

    # Frame type match
    max_score += 2.0
    score += 2.0
    reasons.append("frame_type=threshold")

    # Metric match (required)
    max_score += 2.0
    if frame1.get('metric') and frame1.get('metric') == frame2.get('metric'):
        score += 2.0
        reasons.append(f"metric={frame1['metric']}")
    elif frame1.get('metric') and frame2.get('metric'):
        return (0.0, [])  # Different metrics is fatal

    # Threshold value match (with tolerance)
    max_score += 2.0
    val1 = frame1.get('threshold_value')
    val2 = frame2.get('threshold_value')
    if val1 is not None and val2 is not None:
        # Allow 5% tolerance for threshold values
        if val1 == val2:
            score += 2.0
            reasons.append(f"threshold={val1}")
        elif abs(val1 - val2) / max(abs(val1), abs(val2), 1) < 0.05:
            score += 1.5
            reasons.append(f"threshold~={val1}")
        else:
            return (0.0, [])  # Significantly different thresholds

    # Direction match
    max_score += 1.0
    if frame1.get('threshold_direction') == frame2.get('threshold_direction'):
        score += 1.0
        if frame1.get('threshold_direction'):
            reasons.append(f"direction={frame1['threshold_direction']}")

    # Year match
    max_score += 1.5
    if frame1.get('year') and frame1.get('year') == frame2.get('year'):
        score += 1.5
        reasons.append(f"year={frame1['year']}")
    elif frame1.get('year') and frame2.get('year'):
        return (0.0, [])

    # Actor match
    max_score += 1.0
    if frame1.get('actor') and frame1.get('actor') == frame2.get('actor'):
        score += 1.0
        reasons.append(f"actor={frame1['actor']}")

    confidence = score / max_score if max_score > 0 else 0.0
    return (round(confidence, 3), reasons)


def _compare_policy_frames(frame1: dict, frame2: dict) -> tuple[float, list[str]]:
    """Compare policy change frames."""
    score = 0.0
    max_score = 0.0
    reasons = []

    # Frame type match
    max_score += 2.0
    score += 2.0
    reasons.append("frame_type=policy_change")

    # Actor match (e.g., FED, ECB)
    max_score += 2.0
    if frame1.get('actor') and frame1.get('actor') == frame2.get('actor'):
        score += 2.0
        reasons.append(f"actor={frame1['actor']}")
    elif frame1.get('actor') and frame2.get('actor'):
        return (0.0, [])  # Different actors is fatal

    # Metric match (e.g., RATE)
    max_score += 1.5
    if frame1.get('metric') and frame1.get('metric') == frame2.get('metric'):
        score += 1.5
        reasons.append(f"metric={frame1['metric']}")

    # Direction match
    max_score += 1.0
    if frame1.get('threshold_direction') == frame2.get('threshold_direction'):
        score += 1.0
        if frame1.get('threshold_direction'):
            reasons.append(f"direction={frame1['threshold_direction']}")

    # Year match
    max_score += 1.5
    if frame1.get('year') and frame1.get('year') == frame2.get('year'):
        score += 1.5
        reasons.append(f"year={frame1['year']}")
    elif frame1.get('year') and frame2.get('year'):
        return (0.0, [])

    confidence = score / max_score if max_score > 0 else 0.0
    return (round(confidence, 3), reasons)


def _compare_appointment_frames(
    frame1: dict,
    frame2: dict,
    fuzzy_name_threshold: int = 85,
) -> tuple[float, list[str]]:
    """Compare appointment frames."""
    score = 0.0
    max_score = 0.0
    reasons = []

    # Frame type match
    max_score += 2.0
    score += 2.0
    reasons.append("frame_type=appointment")

    # Office/position match
    max_score += 2.0
    office1 = frame1.get('office') or frame1.get('actor')
    office2 = frame2.get('office') or frame2.get('actor')
    if office1 and office1 == office2:
        score += 2.0
        reasons.append(f"office={office1}")
    elif office1 and office2:
        return (0.0, [])  # Different positions is fatal

    # Candidate name match (fuzzy)
    max_score += 2.0
    cand1 = frame1.get('candidate')
    cand2 = frame2.get('candidate')
    if cand1 and cand2:
        name_score = _fuzzy_name_match(cand1, cand2)
        if name_score >= fuzzy_name_threshold:
            score += 2.0 * (name_score / 100)
            reasons.append(f"candidate_match={name_score}%")
        else:
            return (0.0, [])  # Name mismatch is fatal
    elif cand1 or cand2:
        score += 0.5

    # Year match
    max_score += 1.5
    if frame1.get('year') and frame1.get('year') == frame2.get('year'):
        score += 1.5
        reasons.append(f"year={frame1['year']}")
    elif frame1.get('year') and frame2.get('year'):
        return (0.0, [])

    # Country match
    max_score += 1.0
    if frame1.get('country') and frame1.get('country') == frame2.get('country'):
        score += 1.0
        reasons.append(f"country={frame1['country']}")

    confidence = score / max_score if max_score > 0 else 0.0
    return (round(confidence, 3), reasons)


def _compare_generic_frames(frame1: dict, frame2: dict) -> tuple[float, list[str]]:
    """Compare generic/unknown frame types."""
    score = 0.0
    max_score = 0.0
    reasons = []

    # Frame type match
    max_score += 2.0
    score += 2.0
    reasons.append(f"frame_type={frame1.get('frame_type')}")

    # Year match
    max_score += 2.0
    if frame1.get('year') and frame1.get('year') == frame2.get('year'):
        score += 2.0
        reasons.append(f"year={frame1['year']}")
    elif frame1.get('year') and frame2.get('year'):
        return (0.0, [])

    # Actor match
    max_score += 1.0
    if frame1.get('actor') and frame1.get('actor') == frame2.get('actor'):
        score += 1.0
        reasons.append(f"actor={frame1['actor']}")

    confidence = score / max_score if max_score > 0 else 0.0
    return (round(confidence, 3), reasons)


def _fuzzy_name_match(name1: str, name2: str) -> int:
    """
    Calculate fuzzy match score between two names.

    Returns score 0-100.
    """
    # Normalize names
    name1 = unidecode(name1.lower().strip())
    name2 = unidecode(name2.lower().strip())

    # Try exact match first
    if name1 == name2:
        return 100

    # Try last name match
    slug1 = slugify_name(name1)
    slug2 = slugify_name(name2)
    if slug1 and slug2 and slug1 == slug2:
        return 95

    # Fuzzy match full names
    ratio = fuzz.ratio(name1, name2)

    # Also try token sort (handles word order differences)
    token_ratio = fuzz.token_sort_ratio(name1, name2)

    # Also try partial ratio (handles partial matches)
    partial_ratio = fuzz.partial_ratio(name1, name2)

    # Return best score
    return max(ratio, token_ratio, partial_ratio)


def validate_match(match: MarketMatch) -> list[str]:
    """
    Validate a match and return list of warnings.

    Checks for issues like price discrepancies.
    """
    warnings = []

    # Check for large price discrepancy (if price data available)
    k_price = match.kalshi_market.get('k_last_price')
    pm_prices = match.polymarket_market.get('pm_outcome_prices')

    if k_price and pm_prices:
        try:
            # Kalshi price is in cents, convert to decimal
            k_prob = float(k_price) / 100 if k_price > 1 else float(k_price)

            # PM prices are typically YES/NO probabilities
            if isinstance(pm_prices, str):
                import json
                pm_prices = json.loads(pm_prices)

            if isinstance(pm_prices, list) and len(pm_prices) >= 2:
                pm_prob = float(pm_prices[1])  # YES probability

                # Check for >25¢ spread
                spread = abs(k_prob - pm_prob)
                if spread > 0.25:
                    warnings.append(f"Large price spread: {spread:.1%} (K={k_prob:.1%}, PM={pm_prob:.1%})")
        except (ValueError, TypeError, KeyError):
            pass

    # Check for low extraction confidence
    k_conf = match.kalshi_frame.get('extraction_confidence', 0)
    pm_conf = match.polymarket_frame.get('extraction_confidence', 0)
    if k_conf < 0.5 or pm_conf < 0.5:
        warnings.append(f"Low extraction confidence: K={k_conf:.2f}, PM={pm_conf:.2f}")

    return warnings


def find_potential_matches(
    unmatched: list[dict],
    all_frames: list[tuple[dict, dict]],
    platform: str,
    top_k: int = 3,
) -> list[tuple[dict, list[tuple[dict, float, list[str]]]]]:
    """
    Find potential matches for unmatched markets.

    Returns list of (unmatched_market, [(candidate, score, reasons), ...])
    """
    results = []

    for unmatched_market in unmatched:
        # Get frame for unmatched market (should extract if needed)
        # For now, assume we have it
        candidates = []

        for market, frame in all_frames:
            # Skip same platform
            if platform == 'kalshi' and 'k_' in str(market.get('market_id', '')):
                continue
            if platform == 'polymarket' and 'pm_' in str(market.get('market_id', '')):
                continue

            # This would need the unmatched frame
            # For now, skip - this is a helper for manual review
            pass

        results.append((unmatched_market, candidates[:top_k]))

    return results


def merge_matches_with_existing(
    new_matches: list[MarketMatch],
    existing_matches: list[dict],
) -> list[dict]:
    """
    Merge newly found matches with existing match data.

    Handles conflicts by preferring higher confidence matches.
    """
    # Index existing by BEID
    by_beid = {}
    for match in existing_matches:
        beid = match.get('beid')
        if beid:
            existing_conf = match.get('match_confidence', 0)
            if beid not in by_beid or existing_conf > by_beid[beid].get('match_confidence', 0):
                by_beid[beid] = match

    # Add new matches
    for match in new_matches:
        existing = by_beid.get(match.beid)
        if not existing or match.match_confidence > existing.get('match_confidence', 0):
            by_beid[match.beid] = {
                'beid': match.beid,
                'kalshi_ticker': match.kalshi_market.get('market_id'),
                'polymarket_id': match.polymarket_market.get('market_id'),
                'match_confidence': match.match_confidence,
                'match_reasons': match.match_reasons,
                'kalshi_frame': match.kalshi_frame,
                'polymarket_frame': match.polymarket_frame,
            }

    return list(by_beid.values())
