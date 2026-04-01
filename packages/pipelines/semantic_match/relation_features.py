"""Pair-wise relation features for event verification.

Ported from event-standardization/app/standardize/relation_features.py
"""

from __future__ import annotations
from typing import Any, Dict, List, Set, Optional, Tuple, Union

from semantic_match.temporal import classify_temporal_relation


def _normalize_token_set(text: str) -> Set[str]:
    """Simple token set normalization."""
    if not text:
        return set()
    return set(text.lower().split())

def _jaccard(s1: Set[str], s2: Set[str]) -> float:
    if not s1 and not s2:
        return 0.0
    u = len(s1.union(s2))
    if u == 0:
        return 0.0
    return len(s1.intersection(s2)) / u

def _detect_rule_mismatch_local(a: Dict[str, Any], b: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Detect rule mismatches (duplicated from verify.py to avoid circular imports)."""
    reasons = []
    sem_a = a.get("sem_features") or {}
    sem_b = b.get("sem_features") or {}

    val_a = sem_a.get("resolution_source")
    src_a = (" ".join(str(x) for x in val_a) if isinstance(val_a, list) else str(val_a or "")).lower()
    val_b = sem_b.get("resolution_source")
    src_b = (" ".join(str(x) for x in val_b) if isinstance(val_b, list) else str(val_b or "")).lower()

    if src_a and src_b and src_a != src_b:
        reasons.append(f"resolution_source_mismatch:{src_a}_vs_{src_b}")

    mismatch_pairs = [
        ({"close", "closing", "close price", "at close", "eod", "end of day"}, {"at any time", "touch", "hit", "reach", "intraday", "high", "low"}),
        ({"final"}, {"preliminary", "initial"}),
        ({"core"}, {"headline"}),
        ({"yoy", "year over year"}, {"qoq", "quarter over quarter", "mom"}),
        ({"saar"}, {"nsa", "unadjusted"}),
    ]

    text_a = (str(a.get("title_norm") or "") + " " + str(a.get("rules_primary") or "")).lower()
    text_b = (str(b.get("title_norm") or "") + " " + str(b.get("rules_primary") or "")).lower()

    for set_x, set_y in mismatch_pairs:
        has_a_x = any(w in text_a for w in set_x)
        has_a_y = any(w in text_a for w in set_y)
        has_b_x = any(w in text_b for w in set_x)
        has_b_y = any(w in text_b for w in set_y)

        if (has_a_x and has_b_y) or (has_a_y and has_b_x):
            reasons.append(f"keyword_mismatch:{list(set_x)[0]}_vs_{list(set_y)[0]}")

    return bool(reasons), reasons

def _numeric_relation(sem_a: Dict[str, Any], sem_b: Dict[str, Any]) -> str:
    """Determine numeric relation: equivalent, subset, overlap, disjoint, unknown."""
    nums_a = sem_a.get("numeric") or []
    nums_b = sem_b.get("numeric") or []

    if not nums_a and not nums_b:
        t_a = sem_a.get("numeric_thresholds") or []
        t_b = sem_b.get("numeric_thresholds") or []
        if not t_a and not t_b:
            return "unknown"
        if set(t_a) == set(t_b):
            return "equivalent"
        if set(t_a).intersection(t_b):
            return "overlap"
        return "disjoint"

    def get_repr(nums):
        out = set()
        for x in nums:
            if "interval" in x and x["interval"]:
                out.add(tuple(x["interval"]))
            elif "value" in x:
                out.add(x["value"])
        return out

    set_a = get_repr(nums_a)
    set_b = get_repr(nums_b)

    if not set_a and not set_b:
        return "unknown"
    if set_a == set_b:
        return "equivalent"
    if set_a.issubset(set_b):
        return "a_subset_b"
    if set_b.issubset(set_a):
        return "b_subset_a"
    if set_a.intersection(set_b):
        return "overlap"
    return "disjoint"

def build_pair_features(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Extract pair-wise features for verification evidence."""
    sem_a = a.get("sem_features") or {}
    sem_b = b.get("sem_features") or {}

    ents_a = set(sem_a.get("entities") or [])
    ents_b = set(sem_b.get("entities") or [])
    entity_jaccard = _jaccard(ents_a, ents_b)

    out_a = set(sem_a.get("outcome_entities") or [])
    out_b = set(sem_b.get("outcome_entities") or [])
    if not out_a: out_a = ents_a
    if not out_b: out_b = ents_b
    outcome_entity_jaccard = _jaccard(out_a, out_b)

    ctx_a = set(sem_a.get("context_entities") or [])
    ctx_b = set(sem_b.get("context_entities") or [])
    if not ctx_a: ctx_a = ents_a
    if not ctx_b: ctx_b = ents_b
    context_entity_jaccard = _jaccard(ctx_a, ctx_b)

    title_toks_a = _normalize_token_set(str(a.get("title_norm") or ""))
    title_toks_b = _normalize_token_set(str(b.get("title_norm") or ""))
    title_token_similarity = _jaccard(title_toks_a, title_toks_b)

    rule_toks_a = _normalize_token_set(str(a.get("rules_primary") or ""))
    rule_toks_b = _normalize_token_set(str(b.get("rules_primary") or ""))
    rule_token_similarity = _jaccard(rule_toks_a, rule_toks_b)

    temporal_relation = classify_temporal_relation(a, b)

    prop_a = sem_a.get("proposition_type") or "unknown"
    prop_b = sem_b.get("proposition_type") or "unknown"
    prop_compat = (prop_a == prop_b) or (prop_a == "unknown") or (prop_b == "unknown")

    rule_mismatch, mismatch_reasons = _detect_rule_mismatch_local(a, b)
    num_rel = _numeric_relation(sem_a, sem_b)

    return {
        "entity_jaccard": entity_jaccard,
        "outcome_entity_jaccard": outcome_entity_jaccard,
        "context_entity_jaccard": context_entity_jaccard,
        "title_token_similarity": title_token_similarity,
        "rule_token_similarity": rule_token_similarity,
        "temporal_relation": temporal_relation,
        "prop_type_a": prop_a,
        "prop_type_b": prop_b,
        "prop_compat": prop_compat,
        "numeric_relation": num_rel,
        "rule_mismatch": rule_mismatch,
        "rule_mismatch_reasons": mismatch_reasons
    }
