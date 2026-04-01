"""Deterministic temporal alignment: classify or reject candidate pairs by time windows.

Ported from event-standardization/app/standardize/temporal.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Tuple

import pandas as pd

# Category-specific max gap (days) to treat as disjoint.
MAX_GAP_DAYS_BY_CATEGORY = {
    "sports": 7.0,
    "crypto": 14.0,
    "macro": 60.0,
    "politics": 60.0,
    "other": 60.0,
}

# Slack (days) for subset containment.
SUBSET_SLACK_DAYS = 1.0

# IoU threshold above which we classify as time_equivalent.
IOU_EQUIVALENT_THRESHOLD = 0.7


def _get(profile: Any, key: str, default=None):
    if hasattr(profile, "get"):
        return profile.get(key, default)
    if hasattr(profile, "index") and key in getattr(profile, "index", []):
        return profile[key]
    return getattr(profile, key, default)


def _to_ts(x: Any) -> Optional[pd.Timestamp]:
    if x is None:
        return None
    if isinstance(x, float) and pd.isna(x):
        return None
    try:
        t = pd.Timestamp(x)
        return None if pd.isna(t) else t
    except Exception:
        return None


def parse_time_window(profile: Any) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp], str]:
    """Return (start_ts, end_ts, certainty_flag)."""
    event_ts = _to_ts(_get(profile, "event_ts"))
    start_ts = _to_ts(_get(profile, "start_ts"))
    end_ts = _to_ts(_get(profile, "end_ts"))

    if event_ts is not None:
        return (event_ts, event_ts, "high")

    if start_ts is not None and end_ts is not None and not pd.isna(start_ts) and not pd.isna(end_ts):
        if start_ts <= end_ts:
            return (start_ts, end_ts, "high")
        return (end_ts, start_ts, "high")

    open_t = _to_ts(_get(profile, "open_time"))
    close_t = _to_ts(_get(profile, "close_time"))
    exp_t = _to_ts(_get(profile, "expiration_time"))
    exp_exp_t = _to_ts(_get(profile, "expected_expiration_time"))

    start = start_ts or open_t
    end = end_ts or close_t or exp_t or exp_exp_t
    if start is not None and end is not None:
        if start <= end:
            return (start, end, "medium")
        return (end, start, "medium")
    times = [t for t in [open_t, close_t, exp_t, exp_exp_t] if t is not None]
    if len(times) >= 2:
        return (min(times), max(times), "medium")
    if len(times) == 1:
        return (times[0], times[0], "low")
    return (None, None, "low")


def _category(profile: Any) -> str:
    cat = _get(profile, "coarse_category") or _get(profile, "canonical_category_lvl1")
    return (cat or "").strip().lower() or "other"


def _gap_seconds(start_a, end_a, start_b, end_b) -> float:
    if end_a < start_b:
        return (start_b - end_a).total_seconds()
    if end_b < start_a:
        return (start_a - end_b).total_seconds()
    return 0.0


def _overlap_seconds(start_a, end_a, start_b, end_b) -> float:
    overlap_start = max(start_a, start_b)
    overlap_end = min(end_a, end_b)
    if overlap_start <= overlap_end:
        return (overlap_end - overlap_start).total_seconds()
    return 0.0


def _union_seconds(start_a, end_a, start_b, end_b) -> float:
    union_start = min(start_a, start_b)
    union_end = max(end_a, end_b)
    return (union_end - union_start).total_seconds()


def _iou(start_a, end_a, start_b, end_b) -> float:
    overlap = _overlap_seconds(start_a, end_a, start_b, end_b)
    union = _union_seconds(start_a, end_a, start_b, end_b)
    if union <= 0:
        return 1.0
    return overlap / union


def classify_temporal_relation(
    a: Any,
    b: Any,
    max_gap_days: Optional[float] = None,
    subset_slack_days: float = SUBSET_SLACK_DAYS,
    iou_equivalent: float = IOU_EQUIVALENT_THRESHOLD,
) -> str:
    """Classify relation between two profiles' time windows.

    Returns one of: time_equivalent, a_subset_b, b_subset_a, overlap,
    time_adjacent, time_disjoint, time_unknown.
    """
    start_a, end_a, cert_a = parse_time_window(a)
    start_b, end_b, cert_b = parse_time_window(b)

    if max_gap_days is None:
        cat = _category(a)
        max_gap_days = MAX_GAP_DAYS_BY_CATEGORY.get(cat, 60.0)

    max_gap_seconds = max_gap_days * 86400.0
    slack_seconds = subset_slack_days * 86400.0

    if (start_a is None or end_a is None) and (start_b is None or end_b is None):
        return "time_unknown"

    if start_a is None or end_a is None:
        return "time_unknown"
    if start_b is None or end_b is None:
        return "time_unknown"

    gap = _gap_seconds(start_a, end_a, start_b, end_b)
    if gap > max_gap_seconds:
        return "time_disjoint"

    overlap = _overlap_seconds(start_a, end_a, start_b, end_b)
    iou = _iou(start_a, end_a, start_b, end_b)
    if iou >= iou_equivalent:
        return "time_equivalent"

    if start_a >= start_b - pd.Timedelta(seconds=slack_seconds) and end_a <= end_b + pd.Timedelta(seconds=slack_seconds):
        return "a_subset_b"
    if start_b >= start_a - pd.Timedelta(seconds=slack_seconds) and end_b <= end_a + pd.Timedelta(seconds=slack_seconds):
        return "b_subset_a"

    if overlap > 0:
        return "overlap"

    if gap <= 86400.0:
        return "time_equivalent"
    return "time_adjacent"


@dataclass
class TemporalEvidence:
    start_a: Optional[pd.Timestamp]
    end_a: Optional[pd.Timestamp]
    start_b: Optional[pd.Timestamp]
    end_b: Optional[pd.Timestamp]
    certainty_a: str
    certainty_b: str


def temporal_evidence(a: Any, b: Any) -> TemporalEvidence:
    """Build window evidence for a pair (a, b)."""
    start_a, end_a, cert_a = parse_time_window(a)
    start_b, end_b, cert_b = parse_time_window(b)
    return TemporalEvidence(
        start_a=start_a, end_a=end_a,
        start_b=start_b, end_b=end_b,
        certainty_a=cert_a, certainty_b=cert_b,
    )
