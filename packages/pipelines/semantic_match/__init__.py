"""Semantic feature extraction and heuristic pair verification.

Ported from event-standardization for use as a pre-filter in the
bellwether cross-platform resolution comparison pipeline.
"""

from semantic_match.semantic_features import build_semantic_features
from semantic_match.verify import verify_pair, VerificationResult
from semantic_match.temporal import classify_temporal_relation
from semantic_match.relation_features import build_pair_features
from semantic_match.blocking import get_block_keys
from semantic_match.profile_adapter import build_profile_from_market, build_profile_pair_from_candidate
from semantic_match.entity_keys import extract_entity_keys

__all__ = [
    "build_semantic_features",
    "verify_pair",
    "VerificationResult",
    "classify_temporal_relation",
    "build_pair_features",
    "get_block_keys",
    "build_profile_from_market",
    "build_profile_pair_from_candidate",
    "extract_entity_keys",
]
