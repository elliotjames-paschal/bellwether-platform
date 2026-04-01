"""Heuristic pair verification using semantic features.

Ported from event-standardization/app/standardize/verify.py
"""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Literal, Set, Tuple

from semantic_match.temporal import classify_temporal_relation
from semantic_match.relation_features import build_pair_features

RelationType = Literal[
    "equivalent", "subset", "independent", "uncertain",
    "overlap", "aligned_rule_mismatch", "mutually_exclusive"
]

@dataclass
class VerificationResult:
    relation: RelationType
    confidence: float
    evidence: Dict[str, Any] = field(default_factory=dict)
    reasons: List[str] = field(default_factory=list)


# Compatibility map for proposition types.
PROP_COMPATIBILITY: Dict[str, List[str]] = {
    "binary_yesno": ["binary_yesno", "categorical", "unknown"],
    "winner": ["winner", "categorical", "unknown"],
    "total_ou": ["total_ou", "categorical", "unknown"],
    "spread": ["spread", "categorical", "unknown"],
    "range_bucket": ["range_bucket", "categorical", "unknown"],
    "price_at_time": ["price_at_time", "reach_level", "categorical", "unknown"],
    "reach_level": ["reach_level", "price_at_time", "categorical", "unknown"],
    "categorical": ["binary_yesno", "winner", "total_ou", "spread", "range_bucket", "price_at_time", "reach_level", "categorical", "unknown"],
    "unknown": ["binary_yesno", "winner", "total_ou", "spread", "range_bucket", "price_at_time", "reach_level", "categorical", "unknown"],
}

# Entity synonyms for alignment
ENTITY_SYNONYMS: Dict[str, str] = {
    "fomc": "fed",
    "federal_reserve": "fed",
    "donald_trump": "trump",
    "will_trump": "trump",
    "joe_biden": "biden",
    "gop": "republican",
    "dem": "democrat",
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "us": "united_states",
    "u.s.": "united_states",
    "usa": "united_states",
    "united_states_of_america": "united_states",
}

# Entities that are mutually exclusive within the same context
DISJOINT_GROUPS: List[Set[str]] = [
    {"trump", "biden", "desantis", "haley", "kennedy"},
    {"republican", "democrat", "libertarian", "green"},
    {"lakers", "celtics", "warriors", "suns", "knicks", "heat", "bulls", "bucks", "nuggets", "clippers"},
    {"chiefs", "49ers", "ravens", "lions", "eagles", "cowboys"},
    {"michigan", "georgia", "florida", "ohio", "pennsylvania", "wisconsin", "arizona", "nevada", "north_carolina", "national"},
    {"market_cap", "tvl", "volume", "price"},
]

_RE_NON_ALNUM = re.compile(r"[^a-z0-9\s_]")
_RE_WS = re.compile(r"\s+")

def _canon_entity(e: str) -> str:
    """Canonicalize entity: lowercase, strip punctuation, normalize spaces."""
    e = e.strip().lower()
    e = _RE_NON_ALNUM.sub("", e)
    e = _RE_WS.sub("_", e)
    return e

def _normalize_entities(entities: Set[str]) -> Set[str]:
    """Apply canonicalization and synonyms."""
    norm = set()
    for e in entities:
        ce = _canon_entity(e)
        ce = ENTITY_SYNONYMS.get(ce, ce)
        norm.add(ce)
    return norm


def _check_disjoint_mismatch(ent_a: Set[str], ent_b: Set[str]) -> bool:
    """Return True if any disjoint group has elements from both sides that are DIFFERENT."""
    for i, group in enumerate(DISJOINT_GROUPS):
        if i == 4:  # Skip regions; handled separately
            continue
        in_a = ent_a.intersection(group)
        in_b = ent_b.intersection(group)
        if in_a and in_b and in_a != in_b:
            return True
    return False


def _get_region_entities(entities: Set[str]) -> Set[str]:
    """Extract regions from entities set using DISJOINT_GROUPS[4]."""
    regions_group = DISJOINT_GROUPS[4]
    return entities.intersection(regions_group)


def _detect_rule_mismatch(a: Dict[str, Any], b: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Detect if two events are aligned but have conflicting resolution rules."""
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

    text_a = (a.get("title_norm") or "") + " " + (a.get("rules_primary") or "")
    text_b = (b.get("title_norm") or "") + " " + (b.get("rules_primary") or "")
    text_a = text_a.lower()
    text_b = text_b.lower()

    for set_x, set_y in mismatch_pairs:
        has_a_x = any(w in text_a for w in set_x)
        has_a_y = any(w in text_a for w in set_y)
        has_b_x = any(w in text_b for w in set_x)
        has_b_y = any(w in text_b for w in set_y)

        if (has_a_x and has_b_y) or (has_a_y and has_b_x):
            reasons.append(f"keyword_mismatch:{set_x}_vs_{set_y}")

    return bool(reasons), reasons


def _detect_overlap(a: Dict[str, Any], b: Dict[str, Any], t_rel: str) -> bool:
    """Detect if two events overlap but neither is a subset."""
    if t_rel == "overlap":
        return True
    return False


def _detect_mutually_exclusive(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Detect if events are mutually exclusive (e.g. Trump wins vs Biden wins)."""
    sem_a = a.get("sem_features") or {}
    sem_b = b.get("sem_features") or {}

    raw_ent_a = set(sem_a.get("entities") or [])
    raw_ent_b = set(sem_b.get("entities") or [])
    ent_a = _normalize_entities(raw_ent_a)
    ent_b = _normalize_entities(raw_ent_b)

    if _check_disjoint_mismatch(ent_a, ent_b):
        return True

    return False


def _title_similarity(a: Dict[str, Any], b: Dict[str, Any]) -> float:
    """Compute token-level cosine similarity between title_norm fields."""
    title_a = (a.get("title_norm") or "").lower()
    title_b = (b.get("title_norm") or "").lower()
    if not title_a or not title_b:
        return 0.0
    tokens_a = _RE_NON_ALNUM.sub(" ", title_a).split()
    tokens_b = _RE_NON_ALNUM.sub(" ", title_b).split()
    if not tokens_a or not tokens_b:
        return 0.0
    vocab: set[str] = set(tokens_a) | set(tokens_b)
    ca = Counter(tokens_a)
    cb = Counter(tokens_b)
    dot = sum(ca[w] * cb[w] for w in vocab)
    norm_a = sum(v * v for v in ca.values()) ** 0.5
    norm_b = sum(v * v for v in cb.values()) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _is_point_in_time(sem: Dict[str, Any]) -> bool:
    """Check if a semantic profile represents a point-in-time event."""
    tw = sem.get("time_window") or {}
    if tw.get("event_ts"):
        return True
    s, e = tw.get("start_ts"), tw.get("end_ts")
    return bool(s and e and s == e)

def verify_pair(a: Dict[str, Any], b: Dict[str, Any], use_llm: bool = False) -> VerificationResult:
    """Heuristic-based verification of the relationship between two profiles.

    Optional LLM stage for deep semantic verification.
    """
    sem_a = a.get("sem_features") or {}
    sem_b = b.get("sem_features") or {}

    reasons = []
    evidence = {}

    # 1. Entity Overlap (with normalization)
    raw_ent_a = set(sem_a.get("entities") or [])
    raw_ent_b = set(sem_b.get("entities") or [])

    ent_a = _normalize_entities(raw_ent_a)
    ent_b = _normalize_entities(raw_ent_b)

    ent_inter = ent_a.intersection(ent_b)
    ent_union = ent_a.union(ent_b)
    ent_jaccard = len(ent_inter) / len(ent_union) if ent_union else 0.0

    evidence["entity_overlap"] = ent_jaccard

    title_sim = _title_similarity(a, b)
    evidence["title_similarity"] = round(title_sim, 4)

    if not ent_a or not ent_b:
        reasons.append("missing_entities")
    elif ent_jaccard == 0:
        if title_sim < 0.50:
            return VerificationResult("independent", 0.9, evidence, ["zero_entity_overlap"])
        reasons.append("zero_entity_overlap_but_high_title_sim")

    # 2. Proposition Type
    prop_a = sem_a.get("proposition_type", "unknown")
    prop_b = sem_b.get("proposition_type", "unknown")
    evidence["proposition_types"] = [prop_a, prop_b]

    if prop_b not in PROP_COMPATIBILITY.get(prop_a, []):
        return VerificationResult("independent", 0.8, evidence, [f"prop_mismatch:{prop_a}_vs_{prop_b}"])

    # 3. Time Relation
    t_rel = classify_temporal_relation(a, b)
    evidence["temporal_relation"] = t_rel
    if t_rel == "time_disjoint":
        return VerificationResult("independent", 0.95, evidence, ["disjoint_time"])

    # 4. Disjoint entities
    disjoint = _check_disjoint_mismatch(ent_a, ent_b)
    if disjoint:
        if t_rel != "time_disjoint" and prop_a in ("winner", "categorical") and prop_b in ("winner", "categorical") and ent_jaccard >= 0.2:
            return VerificationResult("mutually_exclusive", 0.9, evidence, ["disjoint_entities_same_context"])
        return VerificationResult("independent", 0.9, evidence, ["disjoint_entities_mismatch"])

    # 5. Rule Mismatch Detection
    rule_mismatch, mismatch_reasons = _detect_rule_mismatch(a, b)
    evidence["rule_mismatch"] = rule_mismatch

    # 6. Numeric Thresholds
    nums_a = sem_a.get("numeric_thresholds") or []
    nums_b = sem_b.get("numeric_thresholds") or []
    evidence["numeric_thresholds"] = [nums_a, nums_b]

    num_rel = "equivalent"
    if nums_a and nums_b:
        if len(nums_a) == 1 and len(nums_b) == 1:
            va, vb = nums_a[0], nums_b[0]
            if abs(va - vb) < 1e-7:
                 num_rel = "equivalent"
            elif va > vb:
                 num_rel = "a_subset_b"
            else:
                 num_rel = "b_subset_a"
        elif len(nums_a) == 2 and len(nums_b) == 2:
             min_a, max_a = min(nums_a), max(nums_a)
             min_b, max_b = min(nums_b), max(nums_b)
             if min_b <= min_a and max_a <= max_b:
                 num_rel = "a_subset_b"
             elif min_a <= min_b and max_b <= max_a:
                 num_rel = "b_subset_a"
             elif max_a < min_b or max_b < min_a:
                 num_rel = "disjoint"
             else:
                 num_rel = "overlap"
        elif nums_a != nums_b:
            num_rel = "uncertain"
    elif nums_a or nums_b:
        num_rel = "uncertain"
        reasons.append("numeric_missing_on_one_side")

    if num_rel == "disjoint" and ent_jaccard >= 0.3 and t_rel != "time_disjoint":
        return VerificationResult("independent", 0.85, evidence, ["numeric_disjoint"])

    reg_a = _get_region_entities(ent_a)
    reg_b = _get_region_entities(ent_b)

    final_rel: RelationType = "equivalent"
    confidence = 0.5

    is_regional_mismatch = (reg_a != reg_b) and (bool(reg_a) or bool(reg_b))

    is_time_equiv = (t_rel in ("time_equivalent", "overlap", "time_adjacent")) or (
        t_rel in ("a_subset_b", "b_subset_a") and (_is_point_in_time(sem_a) or _is_point_in_time(sem_b))
    )
    is_time_unknown = t_rel == "time_unknown"
    is_time_compatible = is_time_equiv or is_time_unknown
    ent_threshold = 0.2 if is_time_equiv else 0.4

    if is_time_compatible and ent_jaccard >= ent_threshold:
        if rule_mismatch and ent_jaccard >= ent_threshold:
            return VerificationResult("aligned_rule_mismatch", 0.9, evidence, mismatch_reasons)

        if (num_rel == "equivalent" or (num_rel == "uncertain" and ent_jaccard >= 0.5)) and is_regional_mismatch:
            final_rel = "subset"
            confidence = 0.7

            if not reg_b:
                evidence["subset_direction"] = "a_subset_b"
            elif not reg_a:
                evidence["subset_direction"] = "b_subset_a"
            elif "national" in reg_b:
                evidence["subset_direction"] = "a_subset_b"
            elif "national" in reg_a:
                evidence["subset_direction"] = "b_subset_a"
            else:
                final_rel = "independent"
                reasons.append("regional_disjoint")
                confidence = 0.9

        elif num_rel == "equivalent" and not is_regional_mismatch:
            final_rel = "equivalent"
            base = 0.75 + (0.25 * ent_jaccard)
            confidence = base * (0.85 if is_time_unknown else 1.0)
        elif num_rel == "uncertain" and ent_jaccard >= 0.5:
            final_rel = "equivalent"
            base = 0.7 + (0.2 * ent_jaccard)
            confidence = base * (0.85 if is_time_unknown else 1.0)
        elif num_rel == "a_subset_b":
            final_rel = "subset"
            evidence["subset_direction"] = "a_subset_b"
            confidence = 0.8
        elif num_rel == "b_subset_a":
            final_rel = "subset"
            evidence["subset_direction"] = "b_subset_a"
            confidence = 0.8
        elif num_rel == "overlap" and ent_jaccard >= 0.4:
            final_rel = "overlap"
            confidence = max(0.7, 0.8)
        else:
            final_rel = "uncertain"
            confidence = 0.4
    elif t_rel in ["a_subset_b", "b_subset_a"] and ent_jaccard > 0.4:
        if num_rel == "equivalent" or num_rel == "uncertain":
             final_rel = "subset"
             evidence["subset_direction"] = t_rel
             confidence = 0.7
        else:
             final_rel = "uncertain"
             confidence = 0.4
    else:
        if ent_jaccard >= 0.8 and not is_regional_mismatch:
             final_rel = "equivalent"
             confidence = 0.8

        elif ent_jaccard >= 0.6 and not is_regional_mismatch:
             if _detect_mutually_exclusive(a, b):
                 final_rel = "mutually_exclusive"
                 confidence = 0.9
             else:
                 final_rel = "equivalent"
                 confidence = 0.7
        else:
             final_rel = "uncertain"
             confidence = 0.3

             if _detect_overlap(a, b, t_rel):
                 final_rel = "overlap"
                 confidence = 0.6

    if use_llm:
        return _verify_with_llm(a, b, VerificationResult(final_rel, confidence, evidence, reasons))

    # Add enriched pair features
    try:
        evidence["pair_features"] = build_pair_features(a, b)
    except Exception:
        pass

    return VerificationResult(final_rel, confidence, evidence, reasons)


def _build_llm_dossier(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Build a compact dossier for a single profile for the LLM prompt."""
    sem = profile.get("sem_features") or {}
    return {
        "title": profile.get("title_norm") or "",
        "rules": (profile.get("rules_primary") or "")[:500],
        "entities": sem.get("entities") or [],
        "proposition_type": sem.get("proposition_type") or "unknown",
        "numeric_thresholds": sem.get("numeric_thresholds") or [],
        "time_window": sem.get("time_window") or {},
    }


_LLM_SYSTEM_PROMPT = """You are an expert at comparing prediction market events across platforms.
Given two events (A and B), determine their relationship.

Respond with a JSON object containing exactly these fields:
- "relation": one of "equivalent", "subset", "overlap", "independent", "uncertain", "aligned_rule_mismatch", "mutually_exclusive"
- "confidence": a float between 0.0 and 1.0
- "reasoning": a brief explanation (1-2 sentences)

Rules:
- "equivalent": Both events resolve the same way under the same conditions.
- "subset": One event's resolution conditions are strictly contained within the other's.
- "overlap": Events share some but not all resolution conditions.
- "independent": Events are about completely different things.
- "aligned_rule_mismatch": Same topic but different resolution rules/sources.
- "mutually_exclusive": If one resolves YES, the other must resolve NO.
- "uncertain": Not enough information to determine."""


def _verify_with_llm(a: Dict[str, Any], b: Dict[str, Any], current: VerificationResult) -> VerificationResult:
    """LLM verification using GPT-4o-mini. Falls back to heuristic on error."""
    dossier_a = _build_llm_dossier(a)
    dossier_b = _build_llm_dossier(b)

    user_msg = (
        f"Event A:\n{json.dumps(dossier_a, indent=2, default=str)}\n\n"
        f"Event B:\n{json.dumps(dossier_b, indent=2, default=str)}\n\n"
        "What is the relationship between these two events?"
    )

    try:
        import openai

        client = openai.OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )

        raw = json.loads(response.choices[0].message.content)
        relation = raw.get("relation", "uncertain")
        confidence = float(raw.get("confidence", 0.5))
        reasoning = raw.get("reasoning", "")

        valid_relations = {
            "equivalent", "subset", "independent", "uncertain",
            "overlap", "aligned_rule_mismatch", "mutually_exclusive",
        }
        if relation not in valid_relations:
            relation = "uncertain"

        confidence = max(0.0, min(1.0, confidence))

        evidence = dict(current.evidence)
        evidence["llm_relation"] = relation
        evidence["llm_confidence"] = confidence
        evidence["llm_reasoning"] = reasoning

        return VerificationResult(
            relation=relation,
            confidence=confidence,
            evidence=evidence,
            reasons=current.reasons + [f"llm:{reasoning}"],
        )

    except Exception as e:
        evidence = dict(current.evidence)
        evidence["llm_error"] = str(e)
        return VerificationResult(
            relation=current.relation,
            confidence=current.confidence,
            evidence=evidence,
            reasons=current.reasons + ["llm_fallback"],
        )
