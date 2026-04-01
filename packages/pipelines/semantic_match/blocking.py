"""Symmetric blocking for candidate generation.

Ported from event-standardization/app/standardize/blocking_v2.py
"""

from __future__ import annotations
import hashlib
import json
import math
from datetime import datetime
from typing import Any, Dict, List, Set, Tuple


def _stable_hash(val: Any) -> str:
    """Create a stable 8-char hash for blocking keys."""
    if isinstance(val, list):
        val = sorted(val)
    s = json.dumps(val, sort_keys=True)
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:8]


def canonicalize_entities(entities: List[str]) -> List[str]:
    """Lowercase, strip, sort, and resolve synonyms for entity keys."""
    from semantic_match.verify import ENTITY_SYNONYMS

    if not entities:
        return []
    cleaned = set()
    for e in entities:
        if not e:
            continue
        key = str(e).strip().lower()
        key = ENTITY_SYNONYMS.get(key, key)
        cleaned.add(key)
    return sorted(list(cleaned))


def bucket_numeric(val: float) -> str:
    """Discretize numeric values to 2 significant figures for stable blocking."""
    if val == 0:
        return "0"
    try:
        power = math.floor(math.log10(abs(val)))
        factor = 10**(power - 1)
        bucketed = round(val / factor) * factor
        return f"{bucketed:g}"
    except (ValueError, OverflowError):
        return f"{val:g}"


def bucket_time_month(ts: Any) -> str | None:
    """Bucket timestamps into calendar months (YYYY_MM)."""
    if not ts:
        return None
    try:
        dt = None
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        elif hasattr(ts, "to_pydatetime"):
            dt = ts.to_pydatetime()
        elif isinstance(ts, datetime):
            dt = ts

        if dt:
            return f"{dt.year}_{dt.month:02d}"
    except Exception:
        pass
    return None


def bucket_time(ts: Any) -> str | None:
    """Bucket timestamps into ISO weeks (Year_Wxx)."""
    if not ts:
        return None
    try:
        dt = None
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        elif hasattr(ts, "to_pydatetime"):
            dt = ts.to_pydatetime()
        elif isinstance(ts, datetime):
            dt = ts

        if dt:
            year, week, _ = dt.isocalendar()
            return f"{year}_w{week}"
    except Exception:
        pass
    return None


def get_block_keys(profile: Dict[str, Any]) -> Dict[str, List[str]]:
    """Generate symmetric block keys for a profile."""
    lvl1 = str(profile.get("canonical_category_lvl1") or "unknown").strip().lower()
    sem = profile.get("sem_features") or {}

    blocks: Dict[str, List[str]] = {
        "primary": []
    }

    if lvl1 != "unknown":
        blocks["primary"].append(f"primary:{lvl1}")

    conf = str(profile.get("canonical_confidence") or "unknown").lower()
    if conf != "high":
        xcat_entities = canonicalize_entities(
            (profile.get("sem_features") or {}).get("entities") or []
        )
        for ent in xcat_entities[:3]:
            blocks.setdefault("xcat_ent", []).append(f"xcat_ent:{ent}")

    raw_entities = sem.get("entities") or []
    entities = canonicalize_entities(raw_entities)

    if entities:
        sig = _stable_hash(entities)
        blocks.setdefault("entity_sig", []).append(f"ent:{sig}")
        for ent in entities[:3]:
            blocks.setdefault("entity_top", []).append(f"ent_top:{ent}")

    thresholds = sem.get("numeric_thresholds") or []
    unique_thresholds = sorted(list(set(thresholds)))
    for t in unique_thresholds:
        bucket = bucket_numeric(t)
        blocks.setdefault("numeric", []).append(f"num:{bucket}")

    event_ts = sem.get("time_window", {}).get("event_ts")
    week_key = bucket_time(event_ts)
    if week_key:
        blocks.setdefault("time", []).append(f"time:{week_key}")

    end_ts = profile.get("end_ts") or (sem.get("time_window", {}).get("event_ts"))
    month_key = bucket_time_month(end_ts)
    if month_key:
        blocks.setdefault("time_month", []).append(f"month:{month_key}")

    return blocks
