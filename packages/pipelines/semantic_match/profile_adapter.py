"""Adapter to convert bellwether market data into profile dicts for semantic verification.

Bridges the bellwether pipeline's market format (enriched JSON, candidate pairs)
to the profile dict format expected by build_semantic_features() and verify_pair().
"""

from __future__ import annotations
import re
from typing import Any, Dict, Optional, Tuple

from semantic_match.entity_keys import extract_entity_keys
from semantic_match.semantic_features import build_semantic_features


def _infer_market_type_from_text(question: str) -> Optional[str]:
    """Infer market_type from question text using regex heuristics."""
    text = question.lower()
    if re.search(r"who will win|will .* win|\bwinner\b", text):
        return "winner"
    if re.search(r"\bover\b|\bunder\b|more than|less than|above|below", text):
        return "total_ou"
    if re.search(r"\breach\b|\bhit\b|\btouch\b", text):
        return "reach_level"
    if re.search(r"\bbetween\b|\brange\b|\bbucket\b", text):
        return "range_bucket"
    return None


def build_profile_from_market(
    question: str,
    rules: str = "",
    ticker: str = "",
    category: str = "politics",
    close_time: Optional[str] = None,
    expiration_time: Optional[str] = None,
    market_id: str = "",
) -> Dict[str, Any]:
    """Convert bellwether market fields into a profile dict for semantic verification.

    Args:
        question: The market question text.
        rules: Resolution rules text (from enriched data).
        ticker: BWR ticker string (e.g. BWR-TRUMP-WIN-PRES_US-CERTIFIED-ANY-2028).
        category: Market category (default: politics).
        close_time: ISO timestamp for market close/expiration.
        expiration_time: Alternative expiration timestamp.
        market_id: Market identifier for reference.
    """
    # Extract entity keys from combined text
    combined_text = f"{question} {rules}" if rules else question
    entity_keys = extract_entity_keys(combined_text)

    # Infer market_type from ticker or question
    market_type = _infer_market_type_from_text(question)

    # Build the profile dict in the shape expected by build_semantic_features
    profile = {
        "id": market_id,
        "title_norm": question,
        "title": question,
        "rules_primary": rules,
        "rules": rules,
        "entity_keys": entity_keys,
        "canonical_category_lvl1": category.lower() if category else "politics",
        "market_type": market_type,
        "start_ts": None,
        "end_ts": close_time or expiration_time,
        "event_ts": None,
    }

    # Run semantic feature extraction
    profile["sem_features"] = build_semantic_features(profile)

    return profile


def build_profile_pair_from_candidate(
    pair: Dict[str, Any],
    resolution_lookup: Dict[str, str],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Build two profiles from a bellwether candidate pair dict + resolution lookup.

    Args:
        pair: Candidate pair dict with kalshi_market_id, poly_market_id,
              kalshi_question, poly_question, kalshi_ticker, poly_ticker.
        resolution_lookup: Dict mapping market_id -> resolution rules text.

    Returns:
        Tuple of (kalshi_profile, poly_profile).
    """
    k_mid = pair["kalshi_market_id"]
    p_mid = pair["poly_market_id"]

    k_rules = resolution_lookup.get(k_mid, "")
    p_rules = resolution_lookup.get(p_mid, "")

    profile_a = build_profile_from_market(
        question=pair["kalshi_question"],
        rules=k_rules,
        ticker=pair.get("kalshi_ticker", ""),
        market_id=k_mid,
    )
    profile_b = build_profile_from_market(
        question=pair["poly_question"],
        rules=p_rules,
        ticker=pair.get("poly_ticker", ""),
        market_id=p_mid,
    )

    return profile_a, profile_b
