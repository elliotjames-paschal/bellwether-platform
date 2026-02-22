#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Classify Electoral Market Details
================================================================================

Part of the NEW Bellwether Pipeline (January 2026+)

This script:
1. Reads markets from pipeline_classify_categories.py output
2. Filters for markets classified as "1. ELECTORAL"
3. Uses 3-STAGE GPT-4o classification to extract:
   - country: Country of the election
   - office: President, Senate, House, Governor, etc.
   - location: State/district/city
   - election_year: Year of the election
   - is_primary: True/False for primary vs general election
4. Outputs classified markets with electoral details

Usage:
    python pipeline_classify_electoral.py

Input:
    - data/new_markets_classified.csv (from pipeline_classify_categories.py)

Output:
    - data/new_markets_electoral_details.csv (with electoral fields filled in)

================================================================================
"""

import pandas as pd
import json
import time
import os
import re
from datetime import datetime
from pathlib import Path
from openai import OpenAI
from utils.openai_classifier import extract_candidate_name, search_candidate_party

# =============================================================================
# PRE-EXTRACTION: Extract location and year from question text
# =============================================================================
# This reduces GPT token usage by extracting obvious information beforehand.

US_STATES = {
    'Alabama': 'Alabama', 'Alaska': 'Alaska', 'Arizona': 'Arizona', 'Arkansas': 'Arkansas',
    'California': 'California', 'Colorado': 'Colorado', 'Connecticut': 'Connecticut',
    'Delaware': 'Delaware', 'Florida': 'Florida', 'Georgia': 'Georgia', 'Hawaii': 'Hawaii',
    'Idaho': 'Idaho', 'Illinois': 'Illinois', 'Indiana': 'Indiana', 'Iowa': 'Iowa',
    'Kansas': 'Kansas', 'Kentucky': 'Kentucky', 'Louisiana': 'Louisiana', 'Maine': 'Maine',
    'Maryland': 'Maryland', 'Massachusetts': 'Massachusetts', 'Michigan': 'Michigan',
    'Minnesota': 'Minnesota', 'Mississippi': 'Mississippi', 'Missouri': 'Missouri',
    'Montana': 'Montana', 'Nebraska': 'Nebraska', 'Nevada': 'Nevada',
    'New Hampshire': 'New Hampshire', 'New Jersey': 'New Jersey', 'New Mexico': 'New Mexico',
    'New York': 'New York', 'North Carolina': 'North Carolina', 'North Dakota': 'North Dakota',
    'Ohio': 'Ohio', 'Oklahoma': 'Oklahoma', 'Oregon': 'Oregon', 'Pennsylvania': 'Pennsylvania',
    'Rhode Island': 'Rhode Island', 'South Carolina': 'South Carolina',
    'South Dakota': 'South Dakota', 'Tennessee': 'Tennessee', 'Texas': 'Texas',
    'Utah': 'Utah', 'Vermont': 'Vermont', 'Virginia': 'Virginia', 'Washington': 'Washington',
    'West Virginia': 'West Virginia', 'Wisconsin': 'Wisconsin', 'Wyoming': 'Wyoming',
}

STATE_ABBREVS = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire',
    'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina',
    'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
    'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
    'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
    'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
}

# District pattern (e.g., "NE-2", "CA-12", "PA-7")
DISTRICT_PATTERN = re.compile(r'\b([A-Z]{2})-(\d{1,2})\b')

# Congressional district pattern (e.g., "Maine's 1st congressional district")
# Uses word boundary before state name to avoid matching "win Maine"
CONGRESSIONAL_DISTRICT_PATTERN = re.compile(
    r"\b(\w+)'s\s+(\d+)(?:st|nd|rd|th)\s+congressional\s+district",
    re.IGNORECASE
)

# Reverse mapping: state name to abbreviation
STATE_TO_ABBREV = {v: k for k, v in STATE_ABBREVS.items()}

# Year pattern (4-digit years 2020-2030)
YEAR_PATTERN = re.compile(r'\b(202\d)\b')


def pre_extract_location(question: str) -> dict:
    """
    Extract location from question text before GPT call.

    Returns dict with:
        - state: Full state name if found (e.g., "Iowa")
        - district: District code if found (e.g., "NE-2")
        - confidence: "high" if clear match, "low" if ambiguous
    """
    if pd.isna(question):
        return {"state": None, "district": None, "confidence": None}

    q = str(question)

    # Check for district pattern first (e.g., "NE-2", "CA-12", "NJ-07")
    district_match = DISTRICT_PATTERN.search(q)
    if district_match:
        abbrev = district_match.group(1)
        district_num = district_match.group(2).lstrip('0') or '0'  # Strip leading zeros, keep "0" if all zeros
        if abbrev in STATE_ABBREVS:
            return {
                "state": STATE_ABBREVS[abbrev],
                "district": f"{abbrev}-{district_num}",
                "confidence": "high"
            }

    # Check for congressional district pattern (e.g., "Maine's 1st congressional district")
    cong_match = CONGRESSIONAL_DISTRICT_PATTERN.search(q)
    if cong_match:
        state_name = cong_match.group(1).title()  # Normalize case
        district_num = cong_match.group(2)
        if state_name in STATE_TO_ABBREV:
            abbrev = STATE_TO_ABBREV[state_name]
            return {
                "state": state_name,
                "district": f"{abbrev}-{district_num}",
                "confidence": "high"
            }

    # Check for full state names
    found_states = []
    for state in US_STATES:
        pattern = r'\b' + re.escape(state) + r'\b'
        if re.search(pattern, q, re.IGNORECASE):
            found_states.append(state)

    if len(found_states) == 1:
        # Single state found - high confidence
        return {"state": found_states[0], "district": None, "confidence": "high"}
    elif len(found_states) > 1:
        # Multiple states - let GPT decide, but provide hints
        return {"state": found_states[0], "district": None, "confidence": "low"}

    return {"state": None, "district": None, "confidence": None}


def pre_extract_year(question: str) -> dict:
    """
    Extract election year from question text before GPT call.

    Returns dict with:
        - year: Integer year if found
        - confidence: "high" if single year, "low" if multiple/ambiguous

    Note: Uses election year (when votes cast), not inauguration year.
    """
    if pd.isna(question):
        return {"year": None, "confidence": None}

    q = str(question)

    # Find all 4-digit years
    years = [int(y) for y in YEAR_PATTERN.findall(q)]

    if len(years) == 0:
        return {"year": None, "confidence": None}
    elif len(years) == 1:
        return {"year": years[0], "confidence": "high"}
    else:
        # Multiple years - use the earliest (likely election year, not inauguration)
        # E.g., "2024 election" and "2025 inauguration" -> use 2024
        return {"year": min(years), "confidence": "low"}


def pre_extract_metadata(question: str, scheduled_end_time: str = None) -> dict:
    """
    Pre-extract location and year from question text.
    Combines location and year extraction.

    Args:
        question: Market question text
        scheduled_end_time: Optional market end time (ISO format or datetime string)
                           Used to infer election year if not found in question text
    """
    location_info = pre_extract_location(question)
    year_info = pre_extract_year(question)

    # If no year found in question text, try to infer from scheduled_end_time
    # Note: scheduled_end_time is when the MARKET closes, not when the election happens.
    # For elections, markets often close AFTER the election (e.g., Jan 2025 for Nov 2024 election).
    # So we only use scheduled_end_time if it's in Oct/Nov/Dec (likely same year as election)
    # or if it's early in the year (Jan-Mar), use the previous year.
    if year_info["year"] is None and scheduled_end_time:
        try:
            end_time_str = str(scheduled_end_time)
            if len(end_time_str) >= 10:  # Need at least YYYY-MM-DD
                inferred_year = int(end_time_str[:4])
                month = int(end_time_str[5:7])
                if 2020 <= inferred_year <= 2030:
                    # If market closes Jan-Mar, election was likely previous year
                    if month <= 3:
                        inferred_year -= 1
                    # Only use as hint, not high confidence (market timing != election timing)
                    year_info = {"year": inferred_year, "confidence": "medium"}
        except (ValueError, TypeError, IndexError):
            pass

    return {
        "location_hint": location_info["state"],
        "district_hint": location_info["district"],
        "location_confidence": location_info["confidence"],
        "year_hint": year_info["year"],
        "year_confidence": year_info["confidence"],
    }


def apply_pre_extracted_metadata(gpt_result: dict, pre_extracted: dict) -> dict:
    """
    Override GPT results with pre-extracted values when confidence is high.

    Rules:
    - If we found a state/district with high confidence, use it for location
    - If we found a year with high confidence, use it for election_year
    - GPT results are used for: country, office, is_primary (and low-confidence fields)
    """
    result = gpt_result.copy()

    # Override location if we have high-confidence extraction
    if pre_extracted.get("location_confidence") == "high":
        if pre_extracted.get("district_hint"):
            # House race - use district code
            result["location"] = pre_extracted["district_hint"]
        elif pre_extracted.get("location_hint"):
            # State-level race
            result["location"] = pre_extracted["location_hint"]

    # Override year if we have high-confidence extraction
    if pre_extracted.get("year_confidence") == "high" and pre_extracted.get("year_hint"):
        result["election_year"] = pre_extracted["year_hint"]

    return result


# =============================================================================
# CONFIGURATION
# =============================================================================

from config import DATA_DIR, get_openai_client

# Input/Output files
INPUT_FILE = DATA_DIR / "new_markets_classified.csv"
OUTPUT_FILE = DATA_DIR / "new_markets_electoral_details.csv"
CHECKPOINT_FILE = DATA_DIR / "pipeline_classify_electoral_checkpoint.json"

# OpenAI Configuration
BATCH_SIZE = 20
MODEL = "gpt-4o"
TEMPERATURE = 0


def derive_election_type(row):
    """
    Derive election_type from office, is_primary, and country.
    Maps to the categories used in Table 4 analysis.
    """
    country = row.get('country')
    office = row.get('office')
    is_primary = row.get('is_primary')

    if pd.isna(office):
        return None

    # US election type mapping
    if country == 'United States':
        us_mapping = {
            ('President', False): 'Presidential',
            ('President', True): 'Presidential Primary',
            ('Vice President', False): 'VP Nomination',
            ('Vice President', True): 'VP Nomination',
            ('Senate', False): 'Senate',
            ('Senate', True): 'Senate Primary',
            ('House', False): 'House',
            ('House', True): 'House Primary',
            ('Governor', False): 'Gubernatorial',
            ('Governor', True): 'Gubernatorial Primary',
            ('Mayor', False): 'Mayoral',
            ('Mayor', True): 'Mayoral Primary',
            ('Lt. Governor', False): 'Gubernatorial',
            ('Lt. Governor', True): 'Gubernatorial Primary',
            ('Attorney General', False): 'Gubernatorial',
            ('Attorney General', True): 'Gubernatorial Primary',
            ('Secretary of State', False): 'Gubernatorial',
            ('Secretary of State', True): 'Gubernatorial Primary',
        }
        return us_mapping.get((office, bool(is_primary)), None)

    # Non-US election type mapping
    intl_mapping = {
        'President': 'Presidential' if not is_primary else 'Presidential Primary',
        'Prime Minister': 'Prime Minister',
        'Chancellor': 'Chancellor',
        'Parliament': 'Parliamentary',
        'European Parliament': 'European Parliament',
        'Regional': 'Regional Election',
        'Mayor': 'Mayoral' if not is_primary else 'Mayoral Primary',
        'Other': 'OTHER_NEEDS_REVIEW',
    }
    return intl_mapping.get(office, 'OTHER_NEEDS_REVIEW')


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# =============================================================================
# STAGE 1: BATCH CLASSIFICATION (HIGH RECALL)
# =============================================================================

STAGE1_SYSTEM_PROMPT = """You are analyzing political prediction markets to extract election details.

For each market, determine:
1. **country**: What country is this election in? (e.g., "United States", "United Kingdom", "France", "Germany")

2. **office**: Which elected office? Use these categories:
   **US offices:** President, Vice President, Senate, House, Governor, Lt. Governor, Attorney General, Secretary of State, Mayor
   **Non-US offices:** President, Prime Minister, Chancellor, Parliament, European Parliament, Regional, Mayor, Other
   - Use "President" for presidential systems (France, Brazil, Mexico, South Korea, etc.)
   - Use "Prime Minister" for PM elections (UK, Canada, Australia, Japan, India, etc.)
   - Use "Chancellor" for Germany, Austria
   - Use "Parliament" for legislative/parliamentary elections (MPs, senators, congress)
   - Use "European Parliament" for EU Parliament elections
   - Use "Regional" for state/provincial/regional/local council elections outside US
   - Use "Mayor" for mayoral elections anywhere
   - Use "Other" if none of the above fit (referendums, party leadership, judicial, etc.)

3. **location**: Geographic location
   **US locations:**
   - President/VP: "United States"
   - Senate/Governor: Full state name (e.g., "Pennsylvania")
   - House: District code "XX-#" (e.g., "PA-7", "CA-13")
   - Mayor: City name
   **Non-US locations:**
   - National elections: Country name (e.g., "France", "United Kingdom")
   - Regional elections: Region/province name (e.g., "Bavaria", "Quebec")
   - City elections: City name

4. **election_year**: Year the election takes place (integer)
   - IMPORTANT: For Kalshi markets, the event_ticker often contains the year as a suffix (e.g., "-26" means 2026, "-24" means 2024)
   - For Polymarket, the market_id/slug may contain the year
   - Use this information to determine the correct year when the question text doesn't specify

5. **is_primary**: true if primary/caucus/nomination/party leadership contest, false if general election

6. **party**: Which party is this market about?
   - "Republican" or "Democrat" for US elections about a specific party
   - For non-US: use the party name if the market is about a specific party winning (e.g., "Conservative", "Labour", "CDU")
   - null if about multiple parties, "which party wins", or cannot determine

Be INCLUSIVE in Stage 1. Extract what you can determine.

Return JSON: {"results": [{"index": 0, "country": "...", "office": "...", "location": "...", "election_year": 2024, "is_primary": false, "party": "Republican"}, ...]}"""


def stage1_batch(client, questions, market_ids=None, batch_size=20, show_progress=True):
    """Stage 1: Batch classify electoral details for high recall."""
    results = []
    total = len(questions)

    # Default to empty identifiers if not provided
    if market_ids is None:
        market_ids = [""] * total

    if show_progress:
        log(f"  Stage 1: Classifying {total} electoral markets (batch={batch_size})...")

    for start in range(0, total, batch_size):
        batch_q = questions[start:start + batch_size]
        batch_ids = market_ids[start:start + batch_size]
        # Include market identifier if available (helps GPT determine year from ticker)
        prompt = "Extract election details:\n" + "\n".join(
            f'{i}. [{mid}] "{q}"' if mid else f'{i}. "{q}"'
            for i, (q, mid) in enumerate(zip(batch_q, batch_ids))
        )

        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": STAGE1_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=TEMPERATURE,
                response_format={"type": "json_object"}
            )

            parsed = json.loads(resp.choices[0].message.content)
            for r in parsed.get("results", []):
                results.append({
                    "index": start + r.get("index", 0),
                    "country": r.get("country"),
                    "office": r.get("office"),
                    "location": r.get("location"),
                    "election_year": r.get("election_year"),
                    "is_primary": r.get("is_primary"),
                    "party": r.get("party"),
                    "confidence": float(r.get("confidence", 0.7)),
                    "stage": 1
                })

        except Exception as e:
            log(f"    Batch error: {e}")
            for i in range(len(batch)):
                results.append({
                    "index": start + i,
                    "country": None,
                    "office": None,
                    "location": None,
                    "election_year": None,
                    "is_primary": None,
                    "party": None,
                    "confidence": 0,
                    "stage": 1,
                    "error": str(e)
                })

        if show_progress and (start + batch_size) % 100 == 0:
            log(f"    {min(start + batch_size, total)}/{total}...")

        time.sleep(0.5)

    results.sort(key=lambda x: x["index"])

    if show_progress:
        us_count = sum(1 for r in results if r.get("country") == "United States")
        log(f"  Stage 1 done: {us_count} US elections, {total - us_count} non-US")

    return results


# =============================================================================
# STAGE 2: BATCH VERIFICATION (HIGH PRECISION)
# =============================================================================

STAGE2_SYSTEM_PROMPT = """VERIFICATION MODE - Confirm election details for these US markets.

Verify and correct if needed:
1. **office**: Must be EXACTLY one of: President, Vice President, Senate, House, Governor, Lt. Governor, Attorney General, Secretary of State, Mayor
2. **location**:
   - President/VP: "United States"
   - Senate/Governor/statewide: Full state name (e.g., "Pennsylvania" not "PA")
   - House: District "XX-#" format (e.g., "PA-7" - NO leading zeros)
   - Mayor: City name
3. **election_year**: Correct year
   - IMPORTANT: The market identifier in brackets (e.g., [KXGOVVTNOMD-26]) often contains the year as a suffix ("-26" = 2026)
4. **is_primary**: true for primary/caucus/nomination, false for general
5. **party**: Which party is this market about?
   - "Republican" if about a Republican candidate winning (Trump, Vance, any R candidate)
   - "Democrat" if about a Democratic candidate winning (Harris, Biden, any D candidate)
   - null if "which party wins", third party, or generic

SPECIAL CASE - "Will X be/remain Y on [date]":
- This asks about someone remaining in office
- election_year = the year they were elected (e.g., Biden on 4/30/2021 → 2020)

Return JSON: {"results": [{"index": 0, "office": "...", "location": "...", "election_year": 2024, "is_primary": false, "party": "Republican", "confidence": 0.9}, ...]}"""


def stage2_verify(client, questions, stage1_results, market_ids=None, batch_size=20, show_progress=True):
    """Stage 2: Batch verify US election classifications."""
    # Only verify US elections
    us_idx = [r["index"] for r in stage1_results if r.get("country") == "United States"]
    total = len(us_idx)

    # Default to empty identifiers if not provided
    if market_ids is None:
        market_ids = [""] * len(questions)

    if show_progress:
        log(f"  Stage 2: Verifying {total} US elections (batch={batch_size})...")

    results = []
    for start in range(0, total, batch_size):
        batch_indices = us_idx[start:start + batch_size]
        batch_questions = [questions[idx] for idx in batch_indices]
        batch_ids = [market_ids[idx] for idx in batch_indices]

        # Include market identifier if available
        prompt = "Verify these US elections:\n" + "\n".join(
            f'{i}. [{mid}] "{q}"' if mid else f'{i}. "{q}"'
            for i, (q, mid) in enumerate(zip(batch_questions, batch_ids))
        )

        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": STAGE2_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=TEMPERATURE,
                response_format={"type": "json_object"}
            )

            parsed = json.loads(resp.choices[0].message.content)
            for r in parsed.get("results", []):
                local_idx = r.get("index", 0)
                if local_idx < len(batch_indices):
                    results.append({
                        "index": batch_indices[local_idx],
                        "country": "United States",
                        "office": r.get("office"),
                        "location": r.get("location"),
                        "election_year": r.get("election_year"),
                        "is_primary": r.get("is_primary"),
                        "party": r.get("party"),
                        "confidence": float(r.get("confidence", 0.8)),
                        "stage": 2
                    })

        except Exception as e:
            log(f"    Batch error: {e}")
            for idx in batch_indices:
                results.append({
                    "index": idx,
                    "stage": 2,
                    "error": str(e)
                })

        if show_progress and (start + batch_size) % 100 == 0:
            log(f"    {min(start + batch_size, total)}/{total}...")

        time.sleep(0.5)

    if show_progress:
        log(f"  Stage 2 done: {len(results)} verified")

    return results


# =============================================================================
# STAGE 3: BATCH TIEBREAKER
# =============================================================================

STAGE3_SYSTEM_PROMPT = """TIEBREAKER - Final verification of election details for these US markets.

Determine the EXACT details for each:

1. **office**: President, Vice President, Senate, House, Governor, Lt. Governor, Attorney General, Secretary of State, or Mayor
2. **location**:
   - President/VP: "United States"
   - Senate/statewide: Full state name
   - House: "XX-#" format (no leading zeros)
   - Mayor: City name
3. **election_year**: Year of the election (integer)
   - IMPORTANT: The market identifier in brackets often contains the year as a suffix ("-26" = 2026, "-24" = 2024)
4. **is_primary**: true=primary/caucus, false=general
5. **party**: "Republican", "Democrat", or null

Return JSON: {"results": [{"index": 0, "office": "...", "location": "...", "election_year": 2024, "is_primary": false, "party": "Republican", "confidence": 0.9}, ...]}"""


def stage3_batch_tiebreak(client, disagreements, batch_size=20, show_progress=True):
    """Stage 3: Batch tiebreaker for disagreements."""
    total = len(disagreements)
    if total == 0:
        return []

    if show_progress:
        log(f"  Stage 3: {total} tiebreakers (batch={batch_size})...")

    results = []
    for start in range(0, total, batch_size):
        batch = disagreements[start:start + batch_size]
        # Include market_id if available
        prompt = "Tiebreaker for these US elections:\n" + "\n".join(
            f'{i}. [{d.get("market_id", "")}] "{d["question"]}"' if d.get("market_id") else f'{i}. "{d["question"]}"'
            for i, d in enumerate(batch)
        )

        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": STAGE3_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=TEMPERATURE,
                response_format={"type": "json_object"}
            )

            parsed = json.loads(resp.choices[0].message.content)
            for r in parsed.get("results", []):
                local_idx = r.get("index", 0)
                if local_idx < len(batch):
                    results.append({
                        "disagreement_idx": start + local_idx,
                        "country": "United States",
                        "office": r.get("office"),
                        "location": r.get("location"),
                        "election_year": r.get("election_year"),
                        "is_primary": r.get("is_primary"),
                        "party": r.get("party"),
                        "confidence": float(r.get("confidence", 0.8)),
                        "stage": 3
                    })

        except Exception as e:
            log(f"    Batch error: {e}")
            for i in range(len(batch)):
                results.append({
                    "disagreement_idx": start + i,
                    "stage": 3,
                    "error": str(e)
                })

        time.sleep(0.5)

    if show_progress:
        log(f"  Stage 3 done: {len(results)} tiebreakers resolved")

    return results


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def run_electoral_pipeline(client, questions, market_ids=None, scheduled_end_times=None, show_progress=True):
    """Run full 3-stage electoral classification pipeline (all stages batched).

    Args:
        client: OpenAI client
        questions: List of market question texts
        market_ids: Optional list of market IDs for logging
        scheduled_end_times: Optional list of scheduled end times (used to infer election year)
        show_progress: Whether to log progress
    """
    if show_progress:
        log(f"\n{'='*50}")
        log(f"CLASSIFYING {len(questions)} ELECTORAL MARKETS (batched)")
        log(f"{'='*50}")

    # Default to empty identifiers if not provided
    if market_ids is None:
        market_ids = [""] * len(questions)
    if scheduled_end_times is None:
        scheduled_end_times = [None] * len(questions)

    # Pre-extract location and year from question text (reduces GPT token usage)
    # Also uses scheduled_end_time to infer year when not in question text
    pre_extracted = [pre_extract_metadata(q, t) for q, t in zip(questions, scheduled_end_times)]
    high_conf_locations = sum(1 for p in pre_extracted if p.get("location_confidence") == "high")
    high_conf_years = sum(1 for p in pre_extracted if p.get("year_confidence") == "high")
    if show_progress:
        log(f"  Pre-extracted: {high_conf_locations} locations, {high_conf_years} years (high confidence)")

    # Stage 1: Batch classify
    s1_results = stage1_batch(client, questions, market_ids, BATCH_SIZE, show_progress)
    s1_lookup = {r["index"]: r for r in s1_results}

    # Stage 2: Batch verify US elections
    s2_results = stage2_verify(client, questions, s1_results, market_ids, BATCH_SIZE, show_progress)
    s2_lookup = {r["index"]: r for r in s2_results}

    # Combine results
    final_results = []
    disagreements = []

    for idx in range(len(questions)):
        s1 = s1_lookup.get(idx, {})
        s2 = s2_lookup.get(idx)

        if s1.get("country") != "United States":
            # Non-US election - keep Stage 1 result (including office and location)
            final_results.append({
                "index": idx,
                "country": s1.get("country"),
                "office": s1.get("office"),
                "location": s1.get("location"),
                "election_year": s1.get("election_year"),
                "is_primary": s1.get("is_primary"),
                "party": s1.get("party"),
                "confidence": s1.get("confidence", 0.5),
                "stage": 1
            })
        elif s2 and "error" not in s2:
            # Check for disagreement on key fields
            if (s1.get("office") == s2.get("office") and
                s1.get("location") == s2.get("location")):
                # Agreement - use Stage 2
                final_results.append({
                    "index": idx,
                    "country": "United States",
                    "office": s2.get("office"),
                    "location": s2.get("location"),
                    "election_year": s2.get("election_year"),
                    "is_primary": s2.get("is_primary"),
                    "party": s2.get("party"),
                    "confidence": s2.get("confidence", 0.8),
                    "stage": 2
                })
            else:
                # Disagreement - need tiebreaker
                disagreements.append({
                    "index": idx,
                    "question": questions[idx],
                    "market_id": market_ids[idx] if market_ids else "",
                    "s1": s1,
                    "s2": s2
                })
        else:
            # Use Stage 1 if Stage 2 failed
            final_results.append({
                "index": idx,
                "country": "United States",
                "office": s1.get("office"),
                "location": s1.get("location"),
                "election_year": s1.get("election_year"),
                "is_primary": s1.get("is_primary"),
                "party": s1.get("party"),
                "confidence": s1.get("confidence", 0.5),
                "stage": 1
            })

    # Stage 3: Batch tiebreakers
    if disagreements:
        s3_results = stage3_batch_tiebreak(client, disagreements, BATCH_SIZE, show_progress)
        s3_lookup = {r["disagreement_idx"]: r for r in s3_results}

        for i, d in enumerate(disagreements):
            s3 = s3_lookup.get(i, {})

            if "error" not in s3:
                final_results.append({
                    "index": d["index"],
                    "country": "United States",
                    "office": s3.get("office"),
                    "location": s3.get("location"),
                    "election_year": s3.get("election_year"),
                    "is_primary": s3.get("is_primary"),
                    "party": s3.get("party"),
                    "confidence": s3.get("confidence", 0.7),
                    "stage": 3
                })
            else:
                # Fallback to Stage 2 if available
                s2 = d.get("s2", d.get("s1", {}))
                final_results.append({
                    "index": d["index"],
                    "country": "United States",
                    "office": s2.get("office"),
                    "location": s2.get("location"),
                    "election_year": s2.get("election_year"),
                    "is_primary": s2.get("is_primary"),
                    "party": s2.get("party"),
                    "confidence": 0.5,
                    "stage": 2
                })

    final_results.sort(key=lambda x: x["index"])

    # Stage 4: Web search for US candidate-name markets with unknown party
    us_null_party = [r for r in final_results
                     if r.get("country") == "United States" and r.get("party") is None]

    if us_null_party:
        if show_progress:
            log(f"\n  Stage 4: Web search for {len(us_null_party)} US markets with unknown party...")

        # Deduplicate by (candidate_name, location, year)
        candidate_cache = {}  # (name, location, year) -> party
        resolved_count = 0

        for result in us_null_party:
            question = questions[result["index"]]
            candidate_name = extract_candidate_name(question)
            if not candidate_name:
                continue

            location = result.get("location", "") or ""
            year = str(int(result["election_year"])) if result.get("election_year") else ""
            cache_key = (candidate_name, location, year)

            if cache_key not in candidate_cache:
                party = search_candidate_party(candidate_name, location, year, show_progress=show_progress)
                candidate_cache[cache_key] = party
                time.sleep(0.3)

            found_party = candidate_cache[cache_key]
            if found_party:
                result["party"] = found_party
                resolved_count += 1

        if show_progress:
            log(f"  Stage 4 done: {resolved_count} parties resolved via web search "
                f"({len(candidate_cache)} unique candidates searched)")

    # Apply pre-extracted metadata overrides (high-confidence extractions)
    override_count = 0
    for i, result in enumerate(final_results):
        idx = result["index"]
        pre = pre_extracted[idx]

        # Only override for US elections (non-US locations need GPT judgment)
        if result.get("country") == "United States":
            original_location = result.get("location")
            original_year = result.get("election_year")

            updated_result = apply_pre_extracted_metadata(result, pre)
            final_results[i] = updated_result

            if updated_result.get("location") != original_location or updated_result.get("election_year") != original_year:
                override_count += 1

    if show_progress:
        us_count = sum(1 for r in final_results if r.get("country") == "United States")
        log(f"\nFINAL: {us_count} US, {len(final_results) - us_count} non-US")
        if override_count > 0:
            log(f"  Pre-extraction overrides applied: {override_count}")
        log(f"{'='*50}")

    return final_results


def main():
    """Main function to classify electoral market details."""
    print("\n" + "=" * 70)
    print("PIPELINE: CLASSIFY ELECTORAL MARKET DETAILS")
    print("Using 3-Stage GPT Classification Pipeline")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    # Check if input file exists
    if not INPUT_FILE.exists():
        log(f"Input file not found: {INPUT_FILE}")
        log("Run pipeline_classify_categories.py first!")
        log("⚠ NO DATA PROCESSED - skipping electoral classification")
        return 0  # Graceful exit - nothing to process

    # Load classified markets
    log("Loading classified markets...")
    df = pd.read_csv(INPUT_FILE)
    log(f"  Total markets: {len(df):,}")

    # Filter for electoral markets
    electoral = df[df['political_category'] == '1. ELECTORAL'].copy()
    non_electoral = df[df['political_category'] != '1. ELECTORAL'].copy()

    log(f"  Electoral markets: {len(electoral):,}")
    log(f"  Non-electoral markets: {len(non_electoral):,}")

    if len(electoral) == 0:
        log("No electoral markets to classify!")
        # Save non-electoral markets as-is
        df.to_csv(OUTPUT_FILE, index=False)
        return 0

    # Initialize OpenAI client
    log("\nInitializing OpenAI client...")
    client = get_openai_client()
    log("  OpenAI client ready")

    # Load checkpoint if exists
    processed_indices = {}
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
            processed_indices = {int(k): v for k, v in checkpoint.get('results', {}).items()}
        log(f"  Loaded checkpoint: {len(processed_indices)} already processed")

    # Get questions and market identifiers to classify
    # Use k_event_ticker for Kalshi, market_id for Polymarket
    questions = electoral['question'].tolist()
    original_indices = electoral.index.tolist()

    # Build market identifier list (event ticker for Kalshi, slug for PM)
    # For Kalshi: k_event_ticker contains year suffix like "-26" for 2026
    # For PM: prefer pm_market_slug (has year in name), fall back to market_id
    market_ids = []
    for _, row in electoral.iterrows():
        if pd.notna(row.get('k_event_ticker')):
            market_ids.append(row['k_event_ticker'])
        elif pd.notna(row.get('pm_market_slug')):
            market_ids.append(row['pm_market_slug'])
        elif pd.notna(row.get('market_id')):
            market_ids.append(row['market_id'])
        else:
            market_ids.append("")

    log(f"  Market identifiers available: {sum(1 for m in market_ids if m):,}")

    # Filter out already processed
    questions_to_process = []
    market_ids_to_process = []
    indices_to_process = []
    for q, mid, idx in zip(questions, market_ids, original_indices):
        if idx not in processed_indices:
            questions_to_process.append(q)
            market_ids_to_process.append(mid)
            indices_to_process.append(idx)

    log(f"  Markets to classify: {len(questions_to_process):,}")

    if questions_to_process:
        # Run 3-stage classification pipeline
        start_time = time.time()
        results = run_electoral_pipeline(client, questions_to_process, market_ids_to_process, show_progress=True)

        # Map results back to original indices
        for result in results:
            local_idx = result["index"]
            original_idx = indices_to_process[local_idx]

            # Update dataframe
            for field in ["country", "office", "location", "election_year", "is_primary", "party"]:
                df.loc[original_idx, field] = result.get(field)

            # Save to checkpoint
            processed_indices[original_idx] = {
                "country": result.get("country"),
                "office": result.get("office"),
                "location": result.get("location"),
                "election_year": result.get("election_year"),
                "is_primary": result.get("is_primary"),
                "party": result.get("party"),
                "confidence": result.get("confidence", 0)
            }

        # Save checkpoint
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'results': {str(k): v for k, v in processed_indices.items()},
                'last_updated': datetime.now().isoformat()
            }, f)

        elapsed = time.time() - start_time
        log(f"\nClassification completed in {elapsed/60:.1f} minutes")

    # Also apply any previously processed results
    for idx_str, data in processed_indices.items():
        idx = int(idx_str)
        if idx in df.index:
            for field in ["country", "office", "location", "election_year", "is_primary", "party"]:
                if pd.isna(df.loc[idx, field]) if field in df.columns else True:
                    df.loc[idx, field] = data.get(field)

    # Convert types
    if 'election_year' in df.columns:
        df['election_year'] = pd.to_numeric(df['election_year'], errors='coerce')

    # Derive election_type from office + is_primary + country
    log("\nDeriving election_type from office/is_primary...")
    df['election_type'] = df.apply(derive_election_type, axis=1)

    set_count = df['election_type'].notna().sum()
    log(f"  Set election_type for {set_count:,} markets")

    # Flag markets that need review
    needs_review = df[df['election_type'] == 'OTHER_NEEDS_REVIEW']
    if len(needs_review) > 0:
        log(f"  ⚠ {len(needs_review)} markets flagged as OTHER_NEEDS_REVIEW")

    # Save results
    log("\n" + "=" * 50)
    log("SAVING RESULTS")
    log("=" * 50)

    df.to_csv(OUTPUT_FILE, index=False)
    log(f"Saved {len(df):,} markets to: {OUTPUT_FILE}")

    # Summary statistics
    us_elections = df[df['country'] == 'United States']
    non_us = df[(df['political_category'] == '1. ELECTORAL') & (df['country'] != 'United States')]

    log("\nUS Elections by Office:")
    if len(us_elections) > 0 and 'office' in us_elections.columns:
        for office, count in us_elections['office'].value_counts().items():
            if pd.notna(office):
                log(f"  {office}: {count:,}")

    log("\nUS Elections by Party:")
    if len(us_elections) > 0 and 'party' in us_elections.columns:
        party_counts = us_elections['party'].value_counts(dropna=False)
        for party, count in party_counts.items():
            party_label = party if pd.notna(party) else "Unknown/Both"
            log(f"  {party_label}: {count:,}")

    log("\nNon-US Elections by Office:")
    if len(non_us) > 0 and 'office' in non_us.columns:
        for office, count in non_us['office'].value_counts(dropna=False).items():
            office_label = office if pd.notna(office) else "Unknown"
            log(f"  {office_label}: {count:,}")

    log("\nNon-US Elections by Country (top 10):")
    if len(non_us) > 0 and 'country' in non_us.columns:
        for country, count in non_us['country'].value_counts().head(10).items():
            if pd.notna(country):
                log(f"  {country}: {count:,}")

    # Clean up checkpoint on successful completion
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    # Summary
    print("\n" + "=" * 70)
    print("ELECTORAL CLASSIFICATION COMPLETE")
    print("=" * 70)
    print(f"Electoral markets processed: {len(electoral):,}")
    print(f"US elections: {len(us_elections):,}")
    print(f"Non-US elections: {len(non_us):,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return len(df)


if __name__ == "__main__":
    main()
