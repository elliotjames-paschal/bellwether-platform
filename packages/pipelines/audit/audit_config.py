"""
================================================================================
AUDIT CONFIGURATION
================================================================================

Configuration for the Bellwether audit system including validation rules,
thresholds, and anomaly detection parameters.
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DATA_DIR

# =============================================================================
# PATHS
# =============================================================================

AUDIT_DIR = DATA_DIR / "audit"
GPT_LOGS_DIR = AUDIT_DIR / "gpt_logs"
CHANGELOGS_DIR = AUDIT_DIR / "changelogs"
VALIDATION_DIR = AUDIT_DIR / "validation"
REVIEW_QUEUE_DIR = AUDIT_DIR / "review_queue"
ANOMALIES_DIR = AUDIT_DIR / "anomalies"

# Master data file
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"

# =============================================================================
# VALIDATION RULES
# =============================================================================

VALIDATION_RULES = {
    # Schema validation - required columns that must exist
    "required_columns": [
        "market_id",
        "platform",
        "question",
        "political_category"
    ],

    # Electoral markets should have these fields (warning if missing)
    "electoral_required": [
        "country",
        "office",
        "location",
        "election_year",
        "is_primary"
    ],

    # Value constraints
    "vote_share_range": (0.0, 1.0),  # CRITICAL: catch 0-100 corruption
    "valid_platforms": ["Polymarket", "Kalshi"],
    "valid_categories": [
        "1. ELECTORAL",
        "2. MONETARY_POLICY",
        "3. LEGISLATIVE",
        "4. APPOINTMENTS",
        "5. REGULATORY",
        "6. INTERNATIONAL",
        "7. JUDICIAL",
        "8. MILITARY_SECURITY",
        "9. CRISIS_EMERGENCY",
        "10. GOVERNMENT_OPERATIONS",
        "11. PARTY_POLITICS",
        "12. STATE_LOCAL",
        "13. TIMING_EVENTS",
        "14. POLLING_APPROVAL",
        "15. POLITICAL_SPEECH",
        "NEEDS_REVIEW"
    ],

    # Completeness thresholds (warn if below)
    "electoral_metadata_min_pct": 0.85,  # 85% of electoral markets should have metadata
    "resolution_rate_min_pct": 0.70,     # 70% of closed markets should have outcomes
}

# =============================================================================
# VALIDATION LEVELS
# =============================================================================

# CRITICAL: Send alert, log prominently, but continue pipeline
# ERROR: Log and include in report
# WARNING: Log only

VALIDATION_LEVELS = {
    "CRITICAL": [
        "vote_share_range",      # Values must be 0-1, not 0-100
        "required_columns",      # Basic schema must be valid
    ],
    "ERROR": [
        "valid_platforms",       # Platform must be Polymarket or Kalshi
        "valid_categories",      # Category must be in allowed list
        "duplicate_vote_shares", # Different elections with identical vote shares
    ],
    "WARNING": [
        "electoral_metadata",    # Coverage of electoral fields
        "resolution_completeness", # Closed markets without outcomes
    ],
}

# =============================================================================
# ANOMALY DETECTION
# =============================================================================

ANOMALY_RULES = {
    # Vote share issues
    "vote_share_corruption": {
        "description": "Vote shares outside 0-1 range (likely stored as percentage)",
        "severity": "CRITICAL",
        "check": "any_value_above_1",
    },
    "duplicate_vote_shares": {
        "description": "Different elections have identical vote shares (likely copy/paste bug)",
        "severity": "ERROR",
        "check": "duplicate_vote_share_pairs",
        "min_duplicates": 3,  # Flag if 3+ elections have exact same D/R shares
    },

    # Daily run anomalies
    "spike_new_markets": {
        "description": "Unusual spike in new markets discovered",
        "severity": "WARNING",
        "threshold": 500,  # Flag if more than 500 new markets/day
    },
    "category_skew": {
        "description": "Single category dominates new classifications",
        "severity": "WARNING",
        "threshold": 0.50,  # Flag if one category > 50% of new markets
    },
    "gpt_failure_rate": {
        "description": "High GPT API failure rate",
        "severity": "ERROR",
        "threshold": 0.10,  # Flag if > 10% API failures
    },
    "electoral_metadata_gap": {
        "description": "Many electoral markets missing metadata",
        "severity": "WARNING",
        "threshold": 0.15,  # Flag if > 15% missing
    },
}

# =============================================================================
# REVIEW QUEUE
# =============================================================================

REVIEW_TRIGGERS = {
    # GPT classification issues
    "low_confidence": {
        "description": "GPT confidence below threshold",
        "threshold": 0.7,
    },
    "stage_disagreement": {
        "description": "No majority vote across classification stages",
    },

    # Validation triggers
    "validation_warning": {
        "description": "Validation rule triggered a warning",
    },
    "anomaly_detected": {
        "description": "Anomaly detection flagged this market",
    },
}

# =============================================================================
# RETENTION POLICY
# =============================================================================

RETENTION_DAYS = {
    "gpt_logs": 30,        # Detailed GPT call logs
    "changelogs": 90,      # Daily change records
    "validation": 30,      # Validation reports
    "anomalies": 90,       # Anomaly detection reports
    "review_history": 365, # Completed reviews
}
