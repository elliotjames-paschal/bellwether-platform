"""
Bellwether Market Matcher - NLP-based cross-platform market matching.

This module uses spaCy to parse prediction market questions, extract
structured semantic frames, and match equivalent markets across platforms.

Main components:
- extractor: Parse questions and extract semantic frames
- matcher: Match markets across Kalshi and Polymarket
- taxonomy: Generate deterministic Bellwether Event IDs (BEIDs)
- dictionaries: Normalization maps for countries, offices, parties, etc.
- pipeline: Orchestration for daily matching runs
"""

from .extractor import extract_frame, load_nlp
from .matcher import match_markets
from .taxonomy import generate_beid, get_event_beid, get_race_beid

__all__ = [
    'extract_frame',
    'load_nlp',
    'match_markets',
    'generate_beid',
    'get_event_beid',
    'get_race_beid',
]
