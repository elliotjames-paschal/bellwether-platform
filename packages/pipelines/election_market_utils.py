#!/usr/bin/env python3
"""
Election Market Utilities

Shared functions for identifying winner markets and grouping elections.
Used by generate_web_data.py (globe) and generate_monitor_data.py (monitor).
"""

import re
import pandas as pd
from collections import defaultdict

# === Exclusion patterns: filter OUT these markets ===
NON_WINNER_PATTERNS = [
    # Vote share / margin / threshold patterns
    r'vote\s+share', r'popular\s+vote\s*%', r'more\s+than\s+\d+',
    r'at\s+least\s+\d+', r'fewer\s+than\s+\d+', r'less\s+than\s+\d+',
    r'\d+\s*%\s+of\s+(the\s+)?votes?', r'between\s+\d+\s*%?\s+and\s+\d+',
    r'by\s+\d+\s+point', r'margin\s+of', r'margin\s+in', r'win\s+by\s+more',
    r'percentage', r'total\s+votes', r'turnout', r'electoral\s+vote',
    r'\d+\s+swing\s+state', r'swing\s+state.*\d+',
    # Dropout / withdrawal
    r'drop\s*out', r'withdraw', r'resign', r'suspend\s+(his|her|their)?\s*campaign',
    # Count questions
    r'how\s+many', r'number\s+of',
    # Endorsements
    r'\bendorse\b', r'endorsement',
    # Debates / speeches
    r'\bdebate\b', r'\bspeech\b', r'state\s+of\s+the\s+union',
    # Approval / ratings
    r'\bapproval\b', r'favorability', r'\brating\b', r'polling\s+average',
    # Announcements
    r'\bannounce\b', r'file\s+to\s+run', r'enter\s+the\s+race',
    # Temporal questions
    r'before\s+(january|february|march|april|may|june|july|august|september|october|november|december)',
    r'by\s+(january|february|march|april|may|june|july|august|september|october|november|december)',
    r'by\s+end\s+of', r'by\s+the\s+end\s+of', r'still\s+in\s+office', r'leave\s+office',
    # Legal
    r'\bconvicted\b', r'\bindicted\b', r'\bsentenced\b', r'prison\s+time',
    # Runoff
    r'runoff', r'second\s+round',
]
NON_WINNER_REGEX = re.compile('|'.join(NON_WINNER_PATTERNS), re.IGNORECASE)

# === Inclusion patterns: markets MUST match one of these ===
WINNER_KEYWORDS = [
    # Direct win language
    r'\bwin\b', r'\bwins\b', r'\bwinner\b', r'\bwinning\b',
    r'\bvictory\b', r'\bvictorious\b', r'\belected\b', r'\belect\b',
    r'\bcontrol\b', r'\bflip\b', r'\bmajority\b',
    # Head-to-head
    r'\bvs\.?\b',
    # Office holder patterns
    r'\bbe\s+(the\s+)?(governor|senator|president|mayor|representative|rep\.|congressman|congresswoman|prime\s+minister|chancellor|premier)\b',
    r'\bbe\s+(the\s+)?next\s+(governor|senator|president|mayor|prime\s+minister)\b',
    r'\bstill\s+be\s+(a\s+)?(governor|senator|president|mayor)\b',
    # Nomination
    r'\bnominee\b', r'\bnomination\b', r'\bnom\b', r'\bprimary\b', r'\bfirst\s+place\b',
    # Party winner
    r'\b(democrat|republican|dem|rep|D|R)\s+(win|wins|victory|control)\b',
    r'\b(democratic|republican)\s+(party|candidate)\b',
]
WINNER_KEYWORD_REGEX = re.compile('|'.join(WINNER_KEYWORDS), re.IGNORECASE)


def is_likely_winner_market(question):
    """Check if a market question is likely a winner market."""
    if pd.isna(question):
        return False
    q = str(question)
    # Exclude non-winner patterns
    if NON_WINNER_REGEX.search(q):
        return False
    # Must match a winner keyword
    if WINNER_KEYWORD_REGEX.search(q):
        return True
    return False


def make_election_key(row):
    """
    Create election key from market row: country|office|location|year|is_primary

    Returns None if required fields (country, office, year) are missing.
    Location is optional (empty string for national elections).
    """
    # Required fields
    country = str(row.get('country', '')).strip() if pd.notna(row.get('country')) else ''
    office = str(row.get('office', '')).strip() if pd.notna(row.get('office')) else ''

    year = row.get('election_year')
    if pd.notna(year):
        try:
            year = str(int(float(year)))
        except (ValueError, TypeError):
            year = ''
    else:
        year = ''

    # Drop if required fields are missing
    if not country or not office or not year:
        return None

    # Optional fields
    location = str(row.get('location', '')).strip() if pd.notna(row.get('location')) else ''
    is_primary = str(row.get('is_primary', False)).lower() == 'true'

    return f"{country}|{office}|{location}|{year}|{is_primary}"


def get_electoral_markets(df):
    """Filter dataframe to electoral markets only."""
    return df[
        (df['political_category'].str.startswith('1.', na=False)) |
        (df['political_category'].str.contains('ELECTORAL', case=False, na=False))
    ].copy()


def get_winner_markets_by_election(df):
    """
    Group electoral markets by election, filter to winner markets, pick highest volume.

    Returns dict: election_key -> {
        'markets': list of market rows,
        'winner_market': highest volume winner market row,
        'info': election info dict
    }
    """
    # Filter to electoral
    electoral = get_electoral_markets(df)

    # Filter to likely winner markets
    electoral['is_winner_market'] = electoral['question'].apply(is_likely_winner_market)
    winner_markets = electoral[electoral['is_winner_market']].copy()

    # Group by election (skip markets with missing metadata)
    elections = defaultdict(lambda: {'markets': [], 'winner_market': None})

    for _, row in winner_markets.iterrows():
        key = make_election_key(row)
        if key is None:
            continue  # Skip markets missing required metadata (country, office, year)
        elections[key]['markets'].append(row)

    # Pick highest volume per election
    for key, data in elections.items():
        markets = data['markets']
        if markets:
            # Sort by volume descending
            sorted_markets = sorted(
                markets,
                key=lambda m: float(m.get('volume_usd', 0)) if pd.notna(m.get('volume_usd')) else 0,
                reverse=True
            )
            data['winner_market'] = sorted_markets[0]

    return dict(elections)
