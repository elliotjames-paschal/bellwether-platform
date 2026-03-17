"""
Shared category constants and utilities for the ticker-based classification system.

Used by: postprocess_tickers.py, generate_web_data.py, generate_market_map.py
"""

# The 16 valid category codes
VALID_CATEGORIES = {
    "ELEC", "MONETARY", "ECON", "LEGIS", "APPOINT", "REGULATE",
    "INTL", "JUDICIAL", "MILITARY", "GOVOPS", "LEADER", "CANDIDACY",
    "POLLING", "SPEECH", "CRISIS", "MISC",
}

# Display names for the research tab
CATEGORY_DISPLAY_NAMES = {
    "ELEC": "Electoral",
    "MONETARY": "Monetary Policy",
    "ECON": "Economic Data",
    "LEGIS": "Legislative",
    "APPOINT": "Appointments",
    "REGULATE": "Regulatory",
    "INTL": "International",
    "JUDICIAL": "Judicial",
    "MILITARY": "Military & Security",
    "GOVOPS": "Government Operations",
    "LEADER": "Leadership Changes",
    "CANDIDACY": "Candidacy",
    "POLLING": "Polling & Approval",
    "SPEECH": "Political Speech",
    "CRISIS": "Crisis & Emergency",
    "MISC": "Other",
}

# Colors for charts and globe visualization
CATEGORY_COLORS = {
    "ELEC": "#3b82f6",
    "MONETARY": "#10b981",
    "ECON": "#22c55e",
    "LEGIS": "#8b5cf6",
    "APPOINT": "#f59e0b",
    "REGULATE": "#ef4444",
    "INTL": "#06b6d4",
    "JUDICIAL": "#ec4899",
    "MILITARY": "#64748b",
    "GOVOPS": "#0ea5e9",
    "LEADER": "#a855f7",
    "CANDIDACY": "#84cc16",
    "POLLING": "#14b8a6",
    "SPEECH": "#f97316",
    "CRISIS": "#dc2626",
    "MISC": "#6b7280",
}

# Map old numbered categories to new codes
OLD_TO_NEW_CATEGORY = {
    "1. ELECTORAL": "ELEC",
    "2. MONETARY_POLICY": "MONETARY",
    "3. LEGISLATIVE": "LEGIS",
    "4. APPOINTMENTS": "APPOINT",
    "5. REGULATORY": "REGULATE",
    "6. INTERNATIONAL": "INTL",
    "7. JUDICIAL": "JUDICIAL",
    "8. MILITARY_SECURITY": "MILITARY",
    "9. CRISIS_EMERGENCY": "CRISIS",
    "10. GOVERNMENT_OPERATIONS": "GOVOPS",
    "11. PARTY_POLITICS": "LEADER",
    "12. STATE_LOCAL": "ELEC",
    "13. TIMING_EVENTS": "MISC",
    "14. POLLING_APPROVAL": "POLLING",
    "15. POLITICAL_SPEECH": "SPEECH",
    "16. NOT_POLITICAL": "MISC",
    "CANDIDACY_ANNOUNCEMENT": "CANDIDACY",
    "PARTISAN_CONTROL": "ELEC",
    "NOT_POLITICAL": "MISC",
}


def format_category_name(code: str) -> str:
    """Format a category code for display.

    Handles both new codes ("ELEC") and old numbered format ("1. ELECTORAL").
    """
    if code in CATEGORY_DISPLAY_NAMES:
        return CATEGORY_DISPLAY_NAMES[code]
    # Handle old numbered format
    if ". " in code:
        code = code.split(". ", 1)[-1]
    return code.replace("_", " ").title()


def old_to_new_category(old_cat: str) -> str:
    """Convert an old numbered category to a new category code.

    If the input is already a valid new category code, returns it unchanged.
    """
    if old_cat in VALID_CATEGORIES:
        return old_cat
    return OLD_TO_NEW_CATEGORY.get(old_cat, "MISC")
