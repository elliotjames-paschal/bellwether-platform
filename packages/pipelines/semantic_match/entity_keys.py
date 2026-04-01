"""Entity key extraction from text.

Extracted from event-standardization/app/standardize/features.py
"""

from __future__ import annotations
import re
from typing import List

_CAPITALIZED_WORDS = re.compile(r"\b[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*\b")
_ENTITY_BLACKLIST = {
    "Will", "The", "When", "Does", "What", "How", "Which", "Winner",
    "Los", "San", "New", "United", "States", "March", "April", "June", "July",
    "January", "February", "August", "September", "October", "November", "December",
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
}

_KNOWN_TICKERS = re.compile(
    r"\b(?:BTC|ETH|SOL|XRP|LINK|DOGE|ADA|LTC|XLM|AVAX|DOT|MATIC|ATOM|"
    r"CPI|GDP|Fed|SEC|FDA|NBA|NFL|MLB|NHL|IMF|ECB|FOMC|OPEC|USDA)\b",
    re.IGNORECASE,
)
_ALPHANUMERIC_ENTITIES = re.compile(r"\b[0-9]+[a-z]{1,4}\b", re.IGNORECASE)
_NUMERIC_ENTITIES = re.compile(r"\b[0-9]{2,}(?:\.[0-9]+)?\b")

ENTITY_KEYS_TOP_N = 10


def extract_entity_keys(text: str) -> List[str]:
    """High-signal canonical entity keys (lowercase, snake_case for phrases)."""
    if not text:
        return []
    keys: list[str] = []
    for m in _KNOWN_TICKERS.finditer(text):
        t = m.group(0).strip().lower()
        if t and t not in keys:
            keys.append(t)
    for m in _ALPHANUMERIC_ENTITIES.finditer(text):
        t = m.group(0).strip().lower()
        if t and t not in keys:
            keys.append(t)
    for m in _NUMERIC_ENTITIES.finditer(text):
        t = m.group(0).strip()
        if t and t not in keys:
            keys.append(t)
    for m in _CAPITALIZED_WORDS.finditer(text):
        phrase = m.group(0).strip()
        words = phrase.split()

        if phrase not in _ENTITY_BLACKLIST:
            canonical = phrase.lower().replace(" ", "_")
            if canonical not in keys:
                keys.append(canonical)

        if len(words) > 1:
            for w in words:
                if len(w) >= 3 and w not in _ENTITY_BLACKLIST:
                    w_low = w.lower()
                    if w_low not in keys:
                        keys.append(w_low)

    return keys[:ENTITY_KEYS_TOP_N]
