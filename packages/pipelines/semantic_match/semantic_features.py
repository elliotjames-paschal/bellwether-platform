"""Extraction of semantic features from contract profiles (V2).

Ported from event-standardization/app/standardize/semantic_features.py
"""

from __future__ import annotations
import re
from typing import Any, Dict, List, Optional, Set, Union, Tuple

# --- Helper Functions ---

def _get_text(profile: Dict[str, Any]) -> str:
    """Combine title and rules into a single lowercased text blob."""
    title = str(profile.get("title_norm") or profile.get("title") or "").lower()
    rules = str(profile.get("rules_primary") or profile.get("rules") or "").lower()
    return f"{title} {rules}"

def _extract_entities(profile: Dict[str, Any], text: str) -> Tuple[List[str], Optional[str], List[str], List[str]]:
    """Extract entities, asset ticker, context entities, and outcomes."""
    entity_keys = profile.get("entity_keys") or []
    if not isinstance(entity_keys, (list, set)):
        entity_keys = []

    entities = set(entity_keys)
    asset_ticker = None

    # Context and Outcome placeholders (extensible)
    context_entities = []
    outcome_entities = []

    # Map for robust ticker extraction
    TICKER_MAP = {
        "btc": "BTC", "bitcoin": "BTC",
        "eth": "ETH", "ethereum": "ETH",
        "sol": "SOL", "solana": "SOL",
        "doge": "DOGE", "pepe": "PEPE"
    }

    # Simple crypto asset ticker extraction if in crypto category
    cat = str(profile.get("canonical_category_lvl1") or "").lower()
    if cat == "crypto":
        for variant, ticker in TICKER_MAP.items():
            if re.search(rf"\b{re.escape(variant)}\b", text):
                asset_ticker = ticker
                entities.add(ticker)
                break

    # Extract structural metrics
    METRICS = {
        "market cap": "market_cap",
        "marketcap": "market_cap",
        "tvl": "tvl",
        "total value locked": "tvl",
        "volume": "volume",
        "transaction count": "volume",
        "hashrate": "hashrate",
        "difficulty": "difficulty",
    }
    for variant, e_key in METRICS.items():
        if variant in text:
            entities.add(e_key)

    return sorted(list(entities)), asset_ticker, context_entities, outcome_entities

def _extract_prop_type(profile: Dict[str, Any], text: str) -> Tuple[str, List[str], Optional[Dict[str, str]]]:
    """Determine proposition type and return type, flags, and rationale."""
    prop_type = "unknown"
    flags = []
    rationale = None

    market_type = profile.get("market_type")
    if not market_type:
        flags.append("missing_market_type")

    market_type_str = str(market_type or "").lower()

    # Regex patterns with boundaries where appropriate
    patterns = {
        "up_down_intraday": [r"up or down", r"close higher", r"price will be above"],
        "price_at_time": [r"price of .* at .* pm", r"price of .* at .* am", r"settlement price"],
        "reach_level": [r"\breach\b", r"\bhit\b", r"\btouch\b"],
        "range_bucket": [r"\bbetween\b", r"\brange\b", r"\bbucket\b"],
        "winner": [r"who will win", r"will .* win", r"\bwinner\b", r"\bcandidate\b"],
        # Use word boundaries for over/under to avoid "overall", "understand"
        "total_ou": [r"\bover\b", r"\bunder\b", r"more than", r"less than"],
    }

    for kind, regexes in patterns.items():
        for r in regexes:
            match = re.search(r, text)
            if match:
                prop_type = kind
                rationale = {
                    "source": "regex_match",
                    "matched_pattern": r,
                    "matched_text": match.group(0)
                }
                break
        if prop_type != "unknown":
            break

    # Fallback to market_type
    if prop_type == "unknown" and market_type_str in ["winner", "total_ou", "spread", "categorical"]:
        prop_type = market_type_str
        rationale = {"source": "market_type_fallback", "matched_pattern": market_type_str, "matched_text": market_type_str}

    return prop_type, flags, rationale

def _extract_numeric_struct(text: str) -> Tuple[List[float], List[Dict[str, Any]]]:
    """Extract numeric thresholds and structured numeric data."""
    thresholds = []
    numeric_structs = []

    regex = r"(>|<|>=|<=|above|below|more than|less than)?\s*[\$]?\s*(\d+(?:,\d+)*(?:\.\d+)?)\s*(million\b|billion\b|trillion\b|k\b|m\b|%)?"

    matches = re.findall(regex, text)

    seen_values = set()

    for comp, n_str, unit in matches:
        try:
            raw_val = float(n_str.replace(",", ""))

            # Scale value
            scaled_val = raw_val
            unit_norm = unit.lower().strip()

            if unit_norm in ("million", "m"):
                scaled_val *= 1_000_000
            elif unit_norm == "billion":
                scaled_val *= 1_000_000_000
            elif unit_norm == "trillion":
                scaled_val *= 1_000_000_000_000
            elif unit_norm == "k":
                scaled_val *= 1_000
            elif unit_norm == "%":
                pass

            # Map comparator
            comp_norm = comp.lower().strip()
            comparator = "="
            if comp_norm in (">", "above", "more than"):
                comparator = ">"
            elif comp_norm in ("<", "below", "less than"):
                comparator = "<"
            elif comp_norm == ">=":
                comparator = ">="
            elif comp_norm == "<=":
                comparator = "<="

            if raw_val not in seen_values:
                thresholds.append(raw_val)
                seen_values.add(raw_val)

            if scaled_val != raw_val and scaled_val not in seen_values:
                thresholds.append(scaled_val)
                seen_values.add(scaled_val)

            numeric_structs.append({
                "value": scaled_val,
                "raw_value": raw_val,
                "unit": unit_norm if unit_norm else None,
                "comparator": comparator
            })

        except ValueError:
            continue

    # Interval detection (simple "between X and Y")
    interval_regex = r"between\s*[\$]?(\d+(?:,\d+)*(?:\.\d+)?)\s*(?:and|to)\s*[\$]?(\d+(?:,\d+)*(?:\.\d+)?)"
    interval_matches = re.findall(interval_regex, text)
    for start_str, end_str in interval_matches:
        try:
            s_val = float(start_str.replace(",", ""))
            e_val = float(end_str.replace(",", ""))
            if s_val not in seen_values:
                thresholds.append(s_val)
                seen_values.add(s_val)
            if e_val not in seen_values:
                thresholds.append(e_val)
                seen_values.add(e_val)

            numeric_structs.append({
                "interval": [s_val, e_val],
                "unit": None,
                "comparator": "range"
            })
        except ValueError:
            continue

    return sorted(list(thresholds))[:8], numeric_structs

def _extract_resolution_sources(text: str) -> List[str]:
    """Extract resolution sources."""
    source_cues = []
    sources = ["bls", "noaa", "associated press", "ap ", "federal reserve", "fed ", "coinbase", "kraken", "binance"]

    for s in sources:
        if s in text:
            clean_s = s.strip()
            if clean_s not in source_cues:
                source_cues.append(clean_s)

    return source_cues

# --- Main Function ---

def build_semantic_features(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Extract semantic features from a contract profile (V2)."""
    text = _get_text(profile)

    entities, asset_ticker, context_entities, outcome_entities = _extract_entities(profile, text)
    prop_type, flags, rationale = _extract_prop_type(profile, text)
    thresholds, numeric_structs = _extract_numeric_struct(text)
    sources = _extract_resolution_sources(text)

    return {
        "entities": entities,
        "asset_ticker": asset_ticker,
        "proposition_type": prop_type,
        "numeric_thresholds": thresholds,
        "time_window": {
            "start_ts": profile.get("start_ts"),
            "end_ts": profile.get("end_ts"),
            "event_ts": profile.get("event_ts"),
        },
        "resolution_source": sources,
        "flags": flags,
        "numeric": numeric_structs,
        "outcome_entities": outcome_entities,
        "context_entities": context_entities,
        "proposition_rationale": rationale,
    }
