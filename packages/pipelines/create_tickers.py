#!/usr/bin/env python3
"""
Unified Ticker Creation Pipeline

Single-phase pipeline that:
1. Pre-extracts timeframe, threshold, and round via regex
2. Makes one GPT-4o call to get canonical agent, action, target, mechanism
3. Post-processes for normalization and disambiguation
4. Assembles final ticker: AGENT-ACTION-TARGET-MECHANISM-THRESHOLD-TIMEFRAME
"""

import json
import gzip
import re
import sys
import argparse
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from collections import defaultdict

from config import DATA_DIR, get_openai_client

try:
    from openai import AsyncOpenAI
    HAS_ASYNC = True
except ImportError:
    HAS_ASYNC = False

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROMPTS_DIR = SCRIPT_DIR / "prompts"

# Load ticker prompt
TICKER_PROMPT = (PROMPTS_DIR / "ticker_prompt.md").read_text()

# ============================================================
# REGEX PRE-EXTRACTION
# ============================================================

MONTH_MAP = {
    'january': 'JAN', 'jan': 'JAN',
    'february': 'FEB', 'feb': 'FEB',
    'march': 'MAR', 'mar': 'MAR',
    'april': 'APR', 'apr': 'APR',
    'may': 'MAY',
    'june': 'JUN', 'jun': 'JUN',
    'july': 'JUL', 'jul': 'JUL',
    'august': 'AUG', 'aug': 'AUG',
    'september': 'SEP', 'sep': 'SEP', 'sept': 'SEP',
    'october': 'OCT', 'oct': 'OCT',
    'november': 'NOV', 'nov': 'NOV',
    'december': 'DEC', 'dec': 'DEC',
}


def extract_timeframe(text: str) -> Optional[str]:
    """
    Extract timeframe from text. Returns one of:
    - Quarter: 2025_Q3
    - Month: MAR2026
    - Year: 2026
    """
    if not text:
        return None
    text = str(text)

    # Quarter patterns
    match = re.search(r'\bQ([1-4])\s*\'?(202[4-9]|2030)\b', text, re.IGNORECASE)
    if match:
        return f"{match.group(2)}_Q{match.group(1)}"
    match = re.search(r'\b(202[4-9]|2030)[\s\-]?Q([1-4])\b', text, re.IGNORECASE)
    if match:
        return f"{match.group(1)}_Q{match.group(2)}"
    match = re.search(r'\bQ([1-4])\s*\'?(2[4-9]|30)\b', text, re.IGNORECASE)
    if match:
        return f"20{match.group(2)}_Q{match.group(1)}"

    # Year boundary: "before January 1, 2027" → 2026
    boundary_match = re.search(
        r'(?:before|by|until)\s+(?:Jan(?:uary)?)\s+[1-9],?\s+(202[5-9]|2030)',
        text, re.IGNORECASE
    )
    if boundary_match:
        return str(int(boundary_match.group(1)) - 1)

    # Full month + year
    match = re.search(
        r'\b(January|February|March|April|May|June|July|August|'
        r'September|October|November|December)\s+(?:\d{1,2},?\s+)?'
        r'(202[4-9]|2030)\b',
        text, re.IGNORECASE
    )
    if match:
        month_abbr = MONTH_MAP.get(match.group(1).lower(), match.group(1)[:3].upper())
        return f"{month_abbr}{match.group(2)}"

    # Short month + year
    match = re.search(
        r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+'
        r'(?:\d{1,2},?\s+)?(202[4-9]|2030)\b',
        text, re.IGNORECASE
    )
    if match:
        month_abbr = MONTH_MAP.get(match.group(1).lower(), match.group(1)[:3].upper())
        return f"{month_abbr}{match.group(2)}"

    # Year + month
    match = re.search(
        r'\b(202[4-9]|2030)\s+'
        r'(January|February|March|April|May|June|July|August|'
        r'September|October|November|December|'
        r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\b',
        text, re.IGNORECASE
    )
    if match:
        month_abbr = MONTH_MAP.get(match.group(2).lower(), match.group(2)[:3].upper())
        return f"{month_abbr}{match.group(1)}"

    # Month + 2-digit year
    match = re.search(
        r'\b(January|February|March|April|May|June|July|August|'
        r'September|October|November|December|'
        r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+'
        r"(?:\d{1,2},?\s+)?'?(2[4-9]|30)\b",
        text, re.IGNORECASE
    )
    if match:
        month_abbr = MONTH_MAP.get(match.group(1).lower(), match.group(1)[:3].upper())
        return f"{month_abbr}20{match.group(2)}"

    # Year only
    match = re.search(r'\b(202[4-9]|2030)\b', text)
    if match:
        return match.group(1)

    # Abbreviated year
    match = re.search(r"'(2[4-9]|30)\b", text)
    if match:
        return f"20{match.group(1)}"

    # Ticker format: -28-
    match = re.search(r'-(2[4-9]|30)-', text)
    if match:
        return f"20{match.group(1)}"

    return None


def extract_threshold(text: str) -> str:
    """
    Extract threshold from text. Returns canonical form:
    - 25BPS, 50BPS, 75BPS, 100BPS
    - GT_1.0PCT, GTE_2.0PCT, LT_3.0PCT, EQ_4.2PCT
    - 1X, 2X, 3X (for "1 time", "2 times", etc.)
    - ANY if nothing found
    """
    if not text:
        return "ANY"
    text = str(text)
    text_lower = text.lower()

    # Basis points: "25 bps", "25 basis points", ">25bps"
    match = re.search(r'[>≥]?\s*(\d+)\s*(?:bps|basis\s*points?)', text_lower)
    if match:
        bps = match.group(1)
        if '>' in text or '≥' in text or 'more than' in text_lower or 'at least' in text_lower:
            return f"GT_{bps}BPS"
        return f"{bps}BPS"

    # Percentage with comparison: "above 2.0%", "more than 1.5%", "at least 3%"
    match = re.search(
        r'(?:above|more than|greater than|over|>\s*|≥\s*|at least|exceed)'
        r'\s*(\d+\.?\d*)\s*%',
        text_lower
    )
    if match:
        pct = match.group(1)
        return f"GT_{pct}PCT"

    match = re.search(
        r'(?:below|less than|under|<\s*|≤\s*|at most)'
        r'\s*(\d+\.?\d*)\s*%',
        text_lower
    )
    if match:
        pct = match.group(1)
        return f"LT_{pct}PCT"

    # Percentage equality or range
    match = re.search(r'(\d+\.?\d*)\s*%', text_lower)
    if match:
        pct = match.group(1)
        # Check context for comparison
        before = text_lower[:match.start()]
        if 'above' in before or 'more than' in before or 'exceed' in before:
            return f"GT_{pct}PCT"
        if 'below' in before or 'less than' in before:
            return f"LT_{pct}PCT"
        return f"EQ_{pct}PCT"

    # Times/occurrences: "3 times", "at least 2 times", "1 time"
    match = re.search(r'(?:at least\s+)?(\d+)\s*times?', text_lower)
    if match:
        num = match.group(1)
        if 'at least' in text_lower[:match.start()+15]:
            return f"GTE_{num}X"
        return f"{num}X"

    # "X or more" pattern
    match = re.search(r'(\d+)\s*(?:or more|plus|\+)', text_lower)
    if match:
        return f"GTE_{match.group(1)}X"

    return "ANY"


def extract_round(text: str) -> Optional[str]:
    """
    Detect election round. Returns:
    - "_R1" for first round
    - "_R2" for runoff/second round
    - None if not detected
    """
    if not text:
        return None
    text_lower = text.lower()

    # First round patterns
    if re.search(r'(?:first|1st)\s*round', text_lower):
        return "_R1"
    if re.search(r'round\s*(?:one|1)\b', text_lower):
        return "_R1"

    # Runoff/second round patterns
    if re.search(r'(?:runoff|run-off|second|2nd)\s*round', text_lower):
        return "_R2"
    if re.search(r'round\s*(?:two|2)\b', text_lower):
        return "_R2"
    if 'runoff' in text_lower:
        return "_R2"

    return None


def pre_extract_fields(market: Dict[str, Any]) -> Dict[str, str]:
    """Run regex pre-extraction on market data."""
    csv = market.get("original_csv", market)
    api = market.get("api_data", {})
    mkt = api.get("market", {}) if isinstance(api.get("market"), dict) else {}

    question = str(csv.get("question", ""))
    market_id = str(csv.get("market_id", ""))
    platform = str(csv.get("platform", ""))

    # Get rules
    rules = ""
    if mkt:
        rules = mkt.get("rules_primary") or mkt.get("description") or ""
    if not rules:
        rules = csv.get("k_rules_primary", "") or csv.get("pm_description", "")
    rules = str(rules)[:2000]

    # Combine text for extraction
    combined_text = f"{question} {rules}"

    # Extract timeframe (question → market_id → rules)
    timeframe = extract_timeframe(question)
    if not timeframe:
        timeframe = extract_timeframe(market_id)
    if not timeframe:
        timeframe = extract_timeframe(rules)
    if not timeframe:
        timeframe = str(datetime.now().year)  # Fallback to current year

    # Extract threshold
    threshold = extract_threshold(combined_text)

    # Extract round suffix
    round_suffix = extract_round(combined_text)

    return {
        "market_id": market_id,
        "platform": platform,
        "question": question,
        "rules": rules,
        "timeframe": timeframe,
        "threshold": threshold,
        "round_suffix": round_suffix or "",
    }


# ============================================================
# GPT CALL (with web search)
# ============================================================

def extract_json_from_response(text: str) -> str:
    """Extract JSON from GPT response that may include markdown fences."""
    if "```json" in text:
        start = text.find("```json") + 7
        end = text.find("```", start)
        if end > start:
            return text[start:end].strip()

    if "```" in text:
        start = text.find("```") + 3
        if text[start:start + 1] == "\n":
            start += 1
        end = text.find("```", start)
        if end > start:
            return text[start:end].strip()

    brace_start = text.find("{")
    if brace_start >= 0:
        depth = 0
        for i, c in enumerate(text[brace_start:]):
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return text[brace_start:brace_start + i + 1]

    return text


def create_batch_prompt(markets: List[Dict[str, str]]) -> str:
    """Create prompt for GPT-4o batch."""
    lines = ["Assign canonical ticker fields for the following markets:\n"]

    for i, m in enumerate(markets):
        lines.append(f"## Market {i}")
        lines.append(f"**question:** {m['question']}")
        lines.append(f"**rules:** {m['rules']}")
        lines.append(f"**timeframe (pre-filled):** {m['timeframe']}")
        lines.append(f"**threshold (pre-filled):** {m['threshold']}")
        lines.append("")

    lines.append(
        f'Return JSON: {{"tickers": ['
        f'{{"agent": "...", "action": "...", "target": "...", "mechanism": "..."}}, '
        f'... {len(markets)} objects]}}'
    )
    return "\n".join(lines)


def parse_response(text: str, markets: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Parse GPT response (may include markdown fences from search model)."""
    json_str = extract_json_from_response(text)
    parsed = json.loads(json_str)

    if isinstance(parsed, list):
        results = parsed
    elif isinstance(parsed, dict):
        results = parsed.get("tickers") or parsed.get("extractions") or parsed.get("results") or [parsed]
    else:
        results = [parsed]

    # Merge with pre-extracted fields
    for i, result in enumerate(results):
        if i < len(markets):
            result["market_id"] = markets[i]["market_id"]
            result["platform"] = markets[i]["platform"]
            result["original_question"] = markets[i]["question"]
            result["timeframe"] = markets[i]["timeframe"]
            result["threshold"] = markets[i]["threshold"]
            result["round_suffix"] = markets[i]["round_suffix"]

    return results


MAX_RETRIES = 5
RETRY_BASE_DELAY = 2.0  # seconds, doubles each retry
INTER_BATCH_DELAY = 1.0  # seconds between batches to avoid rate limits


async def process_batch_async(
    client,
    markets: List[Dict[str, str]],
    batch_idx: int,
    total_batches: int,
    model: str,
    semaphore: asyncio.Semaphore,
    pbar=None
) -> tuple:
    """Process a batch asynchronously with retry on rate limits."""
    user_prompt = create_batch_prompt(markets)

    async with semaphore:
        # Stagger requests to avoid bursting the rate limit
        await asyncio.sleep(INTER_BATCH_DELAY * (batch_idx % 3))
        last_error = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                response = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": TICKER_PROMPT},
                        {"role": "user", "content": user_prompt}
                    ],
                )

                if pbar:
                    pbar.update(1)

                text = response.choices[0].message.content
                results = parse_response(text, markets)
                return (batch_idx, results)

            except Exception as e:
                last_error = e
                error_str = str(e)
                is_rate_limit = "429" in error_str or "rate limit" in error_str.lower()
                error_lower = error_str.lower()
                is_retryable = is_rate_limit or "timeout" in error_lower or "timed out" in error_lower or "500" in error_str or "502" in error_str or "503" in error_str or "connection" in error_lower

                if is_retryable and attempt < MAX_RETRIES:
                    delay = RETRY_BASE_DELAY * (2 ** attempt)
                    print(f"    {'Rate limited' if is_rate_limit else 'Error'} (batch {batch_idx + 1}), retrying in {delay:.0f}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                    await asyncio.sleep(delay)
                    continue

                if pbar:
                    pbar.update(1)
                print(f"    ERROR (batch {batch_idx + 1}): {e}")
                return (batch_idx, [
                    {
                        "market_id": m["market_id"],
                        "platform": m["platform"],
                        "original_question": m["question"],
                        "timeframe": m["timeframe"],
                        "threshold": m["threshold"],
                        "round_suffix": m["round_suffix"],
                        "error": str(last_error)
                    }
                    for m in markets
                ])


# ============================================================
# POST-PROCESSING
# ============================================================

ELECTED_OFFICES = {
    'PRES', 'VP', 'GOV', 'SENATE', 'HOUSE', 'MAYOR', 'PM',
    'PRES_US', 'PRES_BR', 'PRES_MX', 'PRES_AR', 'PRES_CO', 'PRES_FR',
    'DEM_NOMINATION', 'GOP_NOMINATION', 'DEM_NOM', 'GOP_NOM',
}

US_STATE_CODES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
}

COUNTRY_CONFLICTS = {
    'VA': (['vatican', 'holy see'], 'VATICAN'),
    'GA': (['georgia country', 'tbilisi', 'georgian'], 'GE'),
    'CO': (['colombia', 'colombian', 'bogota'], 'COLOMBIA'),
}

# Known name collisions - agents that need FIRST_LAST disambiguation
# This list is auto-generated from collision detection + manually curated
NAME_COLLISIONS = {
    # === HIGH PRIORITY (cross-platform match risk) ===
    'MURPHY': {
        'phil': 'P_MURPHY', 'chris': 'C_MURPHY', 'mark': 'M_MURPHY',
        'morgan': 'MO_MURPHY', 'thomas': 'T_MURPHY',
    },
    'POWELL': {
        'jerome': 'J_POWELL', 'denise': 'D_POWELL', 'lucy': 'L_POWELL',
    },
    'SANDERS': {
        'bernie': 'B_SANDERS', 'huckabee': 'S_HUCKABEE_SANDERS', 'sarah': 'S_HUCKABEE_SANDERS',
    },
    'JOHNSON': {
        'boris': 'B_JOHNSON', 'mike': 'M_JOHNSON', 'ron': 'R_JOHNSON',
        'dusty': 'D_JOHNSON', 'joe': 'JO_JOHNSON', 'jeff': 'JE_JOHNSON',
        'dwayne': 'DW_JOHNSON', 'brandon': 'BR_JOHNSON', 'nathan': 'N_JOHNSON',
        'perry': 'P_JOHNSON', 'julie': 'JU_JOHNSON', 'tanille': 'T_JOHNSON',
    },
    'HARRIS': {
        'kamala': 'K_HARRIS', 'shawn': 'S_HARRIS', 'wood': 'W_HARRIS',
    },
    'OBAMA': {
        'barack': 'B_OBAMA', 'michelle': 'M_OBAMA',
    },
    'BIDEN': {
        'joe': 'J_BIDEN', 'hunter': 'H_BIDEN',
    },
    'NEWSOM': {
        'gavin': 'G_NEWSOM', 'kevin': 'K_NEWSOM',
    },
    'DESANTIS': {
        'ron': 'R_DESANTIS', 'casey': 'C_DESANTIS',
    },
    'BROWN': {
        'sherrod': 'S_BROWN', 'brandon': 'BR_BROWN', 'sean': 'SE_BROWN',
        'dan': 'D_BROWN', 'daniel': 'DA_BROWN', 'toni': 'T_BROWN',
        'scott': 'SC_BROWN', 'mel': 'M_BROWN', 'yumeka': 'Y_BROWN',
        'deirdre': 'DE_BROWN', 'don': 'DO_BROWN', 'olujimi': 'O_BROWN',
    },
    'JONES': {
        'doug': 'D_JONES', 'shevrin': 'S_JONES', 'brandon': 'BR_JONES',
        'david': 'DA_JONES', 'lloyd': 'L_JONES', 'burt': 'BU_JONES',
        'gian': 'G_JONES', 'jolanda': 'JO_JONES', 'dathan': 'DT_JONES',
        'darren': 'DR_JONES',
    },
    'SMITH': {
        'joshua': 'JO_SMITH', 'braden': 'BR_SMITH', 'bernadette': 'BE_SMITH',
        'robin': 'R_SMITH', 'stephen': 'ST_SMITH', 'jack': 'JA_SMITH',
        'mark': 'M_SMITH', 'paul': 'P_SMITH',
    },
    'WILLIAMS': {
        'jumaane': 'JU_WILLIAMS', 'josh': 'JO_WILLIAMS', 'lee': 'L_WILLIAMS',
        'david': 'D_WILLIAMS', 'anthony': 'A_WILLIAMS', 'marcus': 'MA_WILLIAMS',
        'mikel': 'MI_WILLIAMS', 'jeffery': 'JE_WILLIAMS',
    },
    'MOORE': {
        'wes': 'W_MOORE', 'barry': 'B_MOORE', 'colton': 'C_MOORE',
        'robert': 'R_MOORE', 'gregg': 'G_MOORE', 'sidney': 'S_MOORE',
    },
    'COLLINS': {
        'susan': 'S_COLLINS', 'mike': 'M_COLLINS', 'doug': 'D_COLLINS',
        'jay': 'J_COLLINS', 'kina': 'K_COLLINS',
    },
    'ADAMS': {
        'eric': 'E_ADAMS', 'adrienne': 'A_ADAMS',
    },
    'COOK': {
        'tim': 'T_COOK', 'lisa': 'L_COOK', 'denell': 'D_COOK',
    },
    'KELLY': {
        'laura': 'L_KELLY', 'mark': 'M_KELLY', 'robin': 'R_KELLY',
        'scott': 'S_KELLY',
    },
    'CRUZ': {
        'ted': 'T_CRUZ', 'orlando': 'O_CRUZ',
    },
    'JAMES': {
        'letitia': 'L_JAMES', 'lebron': 'LB_JAMES', 'john': 'J_JAMES',
    },
    'BOOKER': {
        'cory': 'C_BOOKER', 'corey': 'C_BOOKER', 'charles': 'CH_BOOKER',
    },
    'SANTOS': {
        'george': 'G_SANTOS', 'renan': 'R_SANTOS',
    },
    'MAXWELL': {
        'ghislaine': 'G_MAXWELL', 'bryan': 'B_MAXWELL',
    },
    'WILLIS': {
        'fani': 'F_WILLIS', 'tom': 'T_WILLIS',
    },

    # === MANUALLY CURATED (from previous code) ===
    'BOLSONARO': {
        'jair': 'J_BOLSONARO', 'flavio': 'F_BOLSONARO', 'flávio': 'F_BOLSONARO',
        'eduardo': 'E_BOLSONARO', 'michelle': 'M_BOLSONARO',
    },
    'CLINTON': {
        'hillary': 'H_CLINTON', 'bill': 'B_CLINTON', 'chelsea': 'C_CLINTON',
    },
    'TRUMP': {
        'ivanka': 'I_TRUMP', 'eric': 'E_TRUMP', 'don jr': 'DJ_TRUMP',
        'donald jr': 'DJ_TRUMP', 'barron': 'B_TRUMP', 'lara': 'L_TRUMP',
        'melania': 'M_TRUMP',
    },
    'KENNEDY': {
        'robert': 'RFK', 'rfk': 'RFK', 'john': 'JFK',
    },
}


def is_elected_office(target: str) -> bool:
    """Check if target is an elected office."""
    if not target:
        return False
    target_upper = target.upper()
    if target_upper in ELECTED_OFFICES:
        return True
    if any(target_upper.startswith(p) for p in ['PRES', 'GOV', 'SENATE', 'HOUSE']):
        return True
    if target_upper.endswith('_NOM') or 'NOMINATION' in target_upper:
        return True
    return False


def is_election_target(target: str) -> bool:
    """Check if target is an election-related target (for timeframe normalization)."""
    if not target:
        return False
    target_upper = target.upper()
    # Standard elected offices
    if is_elected_office(target):
        return True
    # State-specific targets: GOV_CA, SEN_TX, HOUSE_CA_22, etc.
    if any(target_upper.startswith(p) for p in ['GOV_', 'SEN_', 'SENATE_', 'HOUSE_', 'MAYOR_']):
        return True
    # Nomination/primary targets
    if 'NOM' in target_upper or 'PRIMARY' in target_upper:
        return True
    # District patterns: CA_22, TX_18, etc.
    if re.match(r'^[A-Z]{2}[-_]\d+', target_upper):
        return True
    return False


def collapse_mechanism(action: str, target: str, mechanism: str) -> str:
    """
    Collapse equivalent mechanisms to canonical values for cross-platform matching.

    Rule 1: Mechanism collapse based on action type.
    """
    target_upper = target.upper() if target else ""

    # WIN on nomination/election targets: PRIMARY → CERTIFIED, PROJECTED → CERTIFIED
    if action == "WIN":
        if is_election_target(target):
            if mechanism == "PRIMARY":
                # Only collapse if it's a general election, not an actual primary
                if 'PRIMARY' not in target_upper and '_NOM' not in target_upper:
                    return "CERTIFIED"
            if mechanism == "PROJECTED":
                return "CERTIFIED"
        # International parliament elections: OFFICIAL_SOURCE → CERTIFIED
        if mechanism == "OFFICIAL_SOURCE" and is_elected_office(target):
            return "CERTIFIED"

    # LEAVE/RESIGN/FIRST_OUT: ANNOUNCED → ANY_MEANS, VOLUNTARY → ANY_MEANS
    if action in ["LEAVE", "RESIGN", "FIRST_OUT"]:
        if mechanism in ["ANNOUNCED", "VOLUNTARY"]:
            return "ANY_MEANS"

    # REPORT: MONTHLY_REPORT → OFFICIAL_SOURCE
    if action == "REPORT":
        if mechanism == "MONTHLY_REPORT":
            return "OFFICIAL_SOURCE"

    # RECEIVE, HIT (awards/outcomes): STD → OFFICIAL_SOURCE
    if action in ["RECEIVE", "HIT"]:
        if mechanism == "STD":
            return "OFFICIAL_SOURCE"

    return mechanism


def normalize_election_timeframe(action: str, target: str, timeframe: str) -> str:
    """
    Normalize month-level timeframes to year-only for election markets.

    Rule 2: Election timeframe normalization.
    NOV2026 → 2026, AUG2026 → 2026, etc. for election markets only.
    """
    if not timeframe:
        return timeframe

    # Only apply to WIN actions on election targets
    if action != "WIN":
        return timeframe

    if not is_election_target(target):
        return timeframe

    # Check if timeframe is month+year format (e.g., NOV2026, AUG2026)
    match = re.match(r'^[A-Z]{3}(20\d{2})$', timeframe)
    if match:
        return match.group(1)  # Return just the year

    return timeframe


def postprocess_ticker(ticker: Dict[str, Any]) -> Dict[str, Any]:
    """Apply all post-processing fixes."""
    question = ticker.get("original_question", "").lower()
    agent = ticker.get("agent", "")
    action = ticker.get("action", "")
    target = ticker.get("target", "")
    mechanism = ticker.get("mechanism", "")
    timeframe = ticker.get("timeframe", "")

    # Fix 1: Election mechanism → CERTIFIED (original rule)
    if action == "WIN" and is_elected_office(target):
        if mechanism in ["STD", "OFFICIAL_SOURCE", "ANNOUNCED", "PROJECTED"]:
            ticker["mechanism"] = "CERTIFIED"

    # Fix 2: Geographic disambiguation
    if target in US_STATE_CODES and target in COUNTRY_CONFLICTS:
        keywords, replacement = COUNTRY_CONFLICTS[target]
        if any(kw in question for kw in keywords):
            ticker["target"] = replacement

    # Fix 3: Name collision fixes (FIRST_LAST)
    if agent in NAME_COLLISIONS:
        for first_name, canonical in NAME_COLLISIONS[agent].items():
            if first_name in question:
                ticker["agent"] = canonical
                break

    # Fix 4: Append round suffix to target
    if ticker.get("round_suffix"):
        ticker["target"] = ticker["target"] + ticker["round_suffix"]

    # Fix 5: Mechanism collapse for cross-platform matching
    ticker["mechanism"] = collapse_mechanism(action, ticker["target"], ticker["mechanism"])

    # Fix 6: Election timeframe normalization (month → year for elections)
    ticker["timeframe"] = normalize_election_timeframe(action, ticker["target"], ticker["timeframe"])

    # Fix 7: Core vs headline economic indicator distinction
    # CPI, PCE, PPI - if "core" in question, append _CORE to target
    target_upper = ticker["target"].upper() if ticker["target"] else ""
    if "core" in question:
        if target_upper == "CPI":
            ticker["target"] = "CPI_CORE"
        elif target_upper == "PCE":
            ticker["target"] = "PCE_CORE"
        elif target_upper == "PPI":
            ticker["target"] = "PPI_CORE"
        elif target_upper == "INFLATION":
            ticker["target"] = "INFLATION_CORE"

    return ticker


def assemble_ticker(t: Dict[str, Any]) -> str:
    """Assemble final ticker string with BWR prefix."""
    agent = t.get("agent", "UNKNOWN")
    action = t.get("action", "UNKNOWN")
    target = t.get("target", "UNKNOWN")
    mechanism = t.get("mechanism", "STD")
    threshold = t.get("threshold", "ANY")
    timeframe = t.get("timeframe", "UNKNOWN")

    return f"BWR-{agent}-{action}-{target}-{mechanism}-{threshold}-{timeframe}"


# ============================================================
# REGEX FALLBACK FOR GPT FAILURES
# ============================================================

US_STATE_NAMES = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
    'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE',
    'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ',
    'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC',
    'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR',
    'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
    'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
}


def _extract_state(text: str) -> str:
    """Extract US state code from text. Returns '' if not found."""
    text_lower = text.lower()
    # Check full state names (longest first to match "new hampshire" before "new")
    for name, code in sorted(US_STATE_NAMES.items(), key=lambda x: -len(x[0])):
        if name in text_lower:
            return code
    return ''


# Patterns: "Will [party] win the [office] race in [state]"
# Also matches: "Will [party] win the [state] [office] race"
_PARTY_WIN_RE = re.compile(
    r'will\s+(?:the\s+)?'
    r'(republicans?|democratics?|democrats?|republican\s+party|democratic\s+party|gop|dem)\s+'
    r'win\s+(?:the\s+)?'
    r'(governor(?:ship)?|senate|house)\s+'
    r'(?:race\s+)?(?:in|for)\s+'
    r'(.+?)(?:\?|$)',
    re.IGNORECASE
)

# Patterns: "Will [party] win the [state] [office] race in [year]"
_PARTY_WIN_STATE_FIRST_RE = re.compile(
    r'will\s+(?:the\s+)?'
    r'(republicans?|democratics?|democrats?|republican\s+party|democratic\s+party|gop|dem)\s+'
    r'win\s+(?:the\s+)?'
    r'(.+?)\s+'
    r'(governor|senate|house)\s+'
    r'race',
    re.IGNORECASE
)

# Patterns: "Will [name] be the [party] nominee for [office] in [state]"
_NOMINEE_RE = re.compile(
    r'wil[l]?\s+'
    r'(.+?)\s+'
    r'be\s+the\s+'
    r'(republican|democratic|dem|gop)\s+'
    r'nominee\s+for\s+'
    r'(?:the\s+)?'
    r'(governor|senate|house|senator)\s+'
    r'(?:race\s+)?(?:in|of)\s+'
    r'(.+?)(?:\?|$)',
    re.IGNORECASE
)

# Patterns: "Will [party] win the governorship in [state]"
_GOV_WIN_RE = re.compile(
    r'will\s+(?:the\s+)?'
    r'(republicans?|democratics?|democrats?|republican\s+party|democratic\s+party)\s+'
    r'(?:party\s+)?win\s+(?:the\s+)?governorship\s+in\s+'
    r'(.+?)(?:\?|$)',
    re.IGNORECASE
)


def _party_to_agent(party_str: str) -> str:
    """Normalize party string to agent code."""
    p = party_str.strip().lower()
    if p in ('republican', 'republicans', 'republican party', 'gop'):
        return 'GOP'
    if p in ('democratic', 'democratics', 'democrats', 'democratic party', 'dem'):
        return 'DEM'
    return p.upper()


def _office_to_target(office_str: str, state_code: str) -> str:
    """Convert office + state to target code."""
    o = office_str.strip().lower()
    if o in ('governor', 'governorship'):
        return f'GOV_{state_code}' if state_code else 'GOV'
    if o in ('senate', 'senator'):
        return f'SEN_{state_code}' if state_code else 'SENATE'
    if o == 'house':
        return f'HOUSE_{state_code}' if state_code else 'HOUSE'
    return o.upper()


def regex_fallback_classify(ticker_entry: dict) -> bool:
    """Attempt to classify an errored ticker using regex patterns.

    Modifies ticker_entry in-place if successful.
    Returns True if classification succeeded, False otherwise.
    """
    q = ticker_entry.get('original_question', '')
    if not q:
        return False

    # Pattern 1: "Will [party] win the governorship in [state]"
    m = _GOV_WIN_RE.search(q)
    if m:
        agent = _party_to_agent(m.group(1))
        state = _extract_state(m.group(2))
        if agent and state:
            ticker_entry['agent'] = agent
            ticker_entry['action'] = 'WIN'
            ticker_entry['target'] = f'GOV_{state}'
            ticker_entry['mechanism'] = 'CERTIFIED'
            ticker_entry.pop('error', None)
            ticker_entry['classified_by'] = 'regex_fallback'
            return True

    # Pattern 2: "Will [party] win the [office] race in [state]"
    m = _PARTY_WIN_RE.search(q)
    if m:
        agent = _party_to_agent(m.group(1))
        office = m.group(2).strip()
        state = _extract_state(m.group(3))
        if agent and state:
            ticker_entry['agent'] = agent
            ticker_entry['action'] = 'WIN'
            ticker_entry['target'] = _office_to_target(office, state)
            ticker_entry['mechanism'] = 'CERTIFIED'
            ticker_entry.pop('error', None)
            ticker_entry['classified_by'] = 'regex_fallback'
            return True

    # Pattern 2b: "Will [party] win the [state] [office] race"
    m = _PARTY_WIN_STATE_FIRST_RE.search(q)
    if m:
        agent = _party_to_agent(m.group(1))
        state = _extract_state(m.group(2))
        office = m.group(3).strip()
        if agent and state:
            ticker_entry['agent'] = agent
            ticker_entry['action'] = 'WIN'
            ticker_entry['target'] = _office_to_target(office, state)
            ticker_entry['mechanism'] = 'CERTIFIED'
            ticker_entry.pop('error', None)
            ticker_entry['classified_by'] = 'regex_fallback'
            return True

    # Pattern 3: "Will [name] be the [party] nominee for [office] in [state]"
    m = _NOMINEE_RE.search(q)
    if m:
        name = m.group(1).strip().split()[-1].upper()  # Last name
        office = m.group(3).strip()
        state = _extract_state(m.group(4))
        if name and state:
            ticker_entry['agent'] = name
            ticker_entry['action'] = 'WIN_NOM'
            ticker_entry['target'] = _office_to_target(office, state)
            ticker_entry['mechanism'] = 'CERTIFIED'
            ticker_entry.pop('error', None)
            ticker_entry['classified_by'] = 'regex_fallback'
            return True

    return False


# ============================================================
# POST-CLASSIFICATION STATE VALIDATION
# ============================================================

def validate_state_consistency(ticker_entry: dict) -> bool:
    """Check that states mentioned in question match the target field.

    Returns True if consistent (or no state to check). Returns False if
    the question mentions a different state than the target, indicating
    a GPT misclassification. Sets error field on the entry if invalid.
    """
    q = ticker_entry.get('original_question', '')
    target = ticker_entry.get('target', '')
    if not q or not target or 'error' in ticker_entry:
        return True  # Nothing to validate

    # Extract state from question
    q_state = _extract_state(q)
    if not q_state:
        return True  # No state in question — can't validate

    # Extract state from target (e.g., GOV_AR -> AR, SEN_MN -> MN, HOUSE_PA10 -> PA)
    target_state = ''
    for code in US_STATE_CODES:
        # Match state code at end of target or followed by district number
        if re.search(rf'_{code}(\d*)$', target):
            target_state = code
            break

    if not target_state:
        return True  # Target doesn't contain a state — can't validate

    if q_state != target_state:
        ticker_entry['error'] = (
            f'state_mismatch: question mentions {q_state} '
            f'but target is {target} ({target_state})'
        )
        ticker_entry['classified_by'] = 'state_validation_rejected'
        # Remove the ticker so it gets UNKNOWN instead of a wrong match
        ticker_entry.pop('ticker', None)
        ticker_entry.pop('agent', None)
        ticker_entry.pop('action', None)
        ticker_entry.pop('target', None)
        return False

    return True


# ============================================================
# MAIN PIPELINE
# ============================================================

def load_markets(input_file: Path) -> List[Dict[str, Any]]:
    """Load markets from file."""
    if str(input_file).endswith(".gz"):
        with gzip.open(input_file, "rt", encoding="utf-8") as f:
            data = json.load(f)
    else:
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    return data.get("markets", data)


def get_market_id(market: Dict[str, Any]) -> str:
    """Extract market_id from a market dict (handles enriched format)."""
    csv = market.get("original_csv", market)
    return str(csv.get("market_id", ""))


def filter_to_untickered(
    markets: List[Dict[str, Any]],
    output_file: Path
) -> tuple:
    """
    Filter markets to only those without existing tickers.

    Returns (filtered_markets, existing_tickers_list).
    """
    if not output_file.exists():
        print("No existing tickers file found, all markets need tickers")
        return markets, []

    with open(output_file, "r", encoding="utf-8") as f:
        existing_data = json.load(f)

    existing_tickers = existing_data.get("tickers", [])
    existing_ids = {t["market_id"] for t in existing_tickers if "market_id" in t and "error" not in t}

    errored_ids = {t["market_id"] for t in existing_tickers if "market_id" in t and "error" in t}

    filtered = []
    skipped = 0
    for m in markets:
        market_id = get_market_id(m)
        if market_id and market_id in existing_ids:
            skipped += 1
        else:
            filtered.append(m)

    retry_count = sum(1 for m in filtered if get_market_id(m) in errored_ids)
    new_count = len(filtered) - retry_count
    print(f"{len(filtered)} markets need tickers ({new_count} new, {retry_count} retrying errors), {skipped} already tickered (skipped)")
    return filtered, existing_tickers


async def run_pipeline_async(
    markets: List[Dict[str, Any]],
    model: str,
    workers: int,
    batch_size: int
) -> List[Dict[str, Any]]:
    """Run the full pipeline with async processing. Returns list of ticker results."""
    print(f"Pre-extracting fields for {len(markets)} markets...")
    prepared = [pre_extract_fields(m) for m in markets]

    # Filter invalid
    valid = [m for m in prepared if m["question"] and m["rules"]]
    print(f"Valid markets: {len(valid)}")

    # Batch
    batches = [valid[i:i + batch_size] for i in range(0, len(valid), batch_size)]
    print(f"Split into {len(batches)} batches of up to {batch_size}")

    # Process
    client = AsyncOpenAI()
    semaphore = asyncio.Semaphore(workers)
    results_by_batch = {}

    pbar = tqdm(total=len(batches), desc="Processing", unit="batch") if HAS_TQDM else None

    tasks = [
        process_batch_async(client, batch, i, len(batches), model, semaphore, pbar)
        for i, batch in enumerate(batches)
    ]

    for coro in asyncio.as_completed(tasks):
        try:
            batch_idx, results = await coro
            results_by_batch[batch_idx] = results
        except Exception as e:
            print(f"    Batch failed with unhandled error: {e}")
            # Continue processing other batches instead of crashing

    if pbar:
        pbar.close()

    # Merge results in order
    all_results = []
    for i in sorted(results_by_batch.keys()):
        all_results.extend(results_by_batch[i])

    # Post-process and assemble tickers
    print("Post-processing and assembling tickers...")
    regex_rescued = 0
    state_rejected = 0
    for t in all_results:
        if "error" in t:
            # Attempt regex fallback for errored (rate-limited) tickers
            if regex_fallback_classify(t):
                regex_rescued += 1
                postprocess_ticker(t)
                t["ticker"] = assemble_ticker(t)
        else:
            postprocess_ticker(t)
            t["ticker"] = assemble_ticker(t)
            # Validate state consistency on GPT-classified tickers
            if not validate_state_consistency(t):
                state_rejected += 1

    if regex_rescued:
        print(f"  Regex fallback rescued {regex_rescued} errored tickers")
    if state_rejected:
        print(f"  State validation rejected {state_rejected} GPT-classified tickers")

    return all_results


def save_tickers(tickers: List[Dict[str, Any]], output_file: Path, model: str):
    """Save tickers to output file and print stats."""
    output = {
        "generated_at": datetime.now().isoformat(),
        "model": model,
        "total_markets": len(tickers),
        "tickers": tickers
    }

    DATA_DIR.mkdir(exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    errors = sum(1 for t in tickers if "error" in t)
    unique = len(set(t.get("ticker", "") for t in tickers if "ticker" in t))
    print(f"\nComplete!")
    print(f"  Total: {len(tickers)}")
    print(f"  Errors: {errors}")
    print(f"  Unique tickers: {unique}")
    print(f"  Output: {output_file}")



# ============================================================
# OPENAI BATCH API MODE (50% cost reduction)
# ============================================================

BATCH_JSONL_FILE = DATA_DIR / "batch_ticker_requests.jsonl"
BATCH_STATE_FILE = DATA_DIR / "batch_ticker_state.json"


def prepare_batch_jsonl(markets: List[Dict[str, Any]], model: str, batch_size: int) -> int:
    """Prepare JSONL file for OpenAI Batch API. Returns number of requests."""
    prepared = [pre_extract_fields(m) for m in markets]
    valid = [m for m in prepared if m["question"] and m["rules"]]
    batches = [valid[i:i + batch_size] for i in range(0, len(valid), batch_size)]

    with open(BATCH_JSONL_FILE, "w") as f:
        for i, batch in enumerate(batches):
            request = {
                "custom_id": f"batch-{i}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": TICKER_PROMPT},
                        {"role": "user", "content": create_batch_prompt(batch)}
                    ]
                }
            }
            f.write(json.dumps(request) + "\n")

    # Save batch metadata for download phase
    batch_meta = {
        "model": model,
        "batch_size": batch_size,
        "total_markets": len(valid),
        "total_requests": len(batches),
        "created_at": datetime.now().isoformat(),
        "markets": valid  # Save for parsing responses later
    }
    with open(BATCH_STATE_FILE, "w") as f:
        json.dump(batch_meta, f)

    print(f"Prepared {len(batches)} requests for {len(valid)} markets -> {BATCH_JSONL_FILE}")
    return len(batches)


def handle_batch_api(args):
    """Handle --batch-api submit/check/download."""
    from openai import OpenAI
    client = OpenAI()

    if args.batch_api == "submit":
        markets = load_markets(args.input)
        if args.limit:
            markets = markets[:args.limit]

        if not args.full_refresh:
            markets, existing = filter_to_untickered(markets, args.output)
            print(f"Filtered to {len(markets)} untickered markets")

        if not markets:
            print("No markets to process.")
            return

        prepare_batch_jsonl(markets, args.model, args.batch_size)

        # Upload file
        with open(BATCH_JSONL_FILE, "rb") as f:
            uploaded = client.files.create(file=f, purpose="batch")
        print(f"Uploaded file: {uploaded.id}")

        # Create batch
        batch = client.batches.create(
            input_file_id=uploaded.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        print(f"Batch created: {batch.id}")
        print(f"Status: {batch.status}")
        print(f"\nTo check: python create_tickers.py --batch-api check --batch-id {batch.id}")

        # Save batch ID to state
        with open(BATCH_STATE_FILE) as f:
            state = json.load(f)
        state["batch_id"] = batch.id
        state["file_id"] = uploaded.id
        with open(BATCH_STATE_FILE, "w") as f:
            json.dump(state, f)

    elif args.batch_api == "check":
        batch_id = args.batch_id
        if not batch_id and BATCH_STATE_FILE.exists():
            with open(BATCH_STATE_FILE) as f:
                state = json.load(f)
            batch_id = state.get("batch_id")
        if not batch_id:
            print("No batch ID. Use --batch-id or submit a batch first.")
            return

        batch = client.batches.retrieve(batch_id)
        print(f"Batch {batch_id}:")
        print(f"  Status: {batch.status}")
        print(f"  Total requests: {batch.request_counts.total}")
        print(f"  Completed: {batch.request_counts.completed}")
        print(f"  Failed: {batch.request_counts.failed}")
        if batch.status == "completed":
            print(f"\nReady! Run: python create_tickers.py --batch-api download --batch-id {batch_id}")

    elif args.batch_api == "download":
        batch_id = args.batch_id
        if not batch_id and BATCH_STATE_FILE.exists():
            with open(BATCH_STATE_FILE) as f:
                state = json.load(f)
            batch_id = state.get("batch_id")
        if not batch_id:
            print("No batch ID.")
            return

        batch = client.batches.retrieve(batch_id)
        if batch.status != "completed":
            print(f"Batch not ready. Status: {batch.status}")
            return

        # Download results
        result_file = client.files.content(batch.output_file_id)
        lines = result_file.text.strip().split("\n")
        print(f"Downloaded {len(lines)} results")

        # Load state for market data
        with open(BATCH_STATE_FILE) as f:
            state = json.load(f)
        batch_size = state["batch_size"]
        all_markets = state["markets"]

        # Parse results
        all_results = []
        for line in lines:
            resp = json.loads(line)
            custom_id = resp["custom_id"]
            batch_idx = int(custom_id.split("-")[1])

            # Get the markets for this batch
            start = batch_idx * batch_size
            end = min(start + batch_size, len(all_markets))
            batch_markets = all_markets[start:end]

            if resp["response"]["status_code"] == 200:
                text = resp["response"]["body"]["choices"][0]["message"]["content"]
                try:
                    parsed = parse_response(text, batch_markets)
                    for t in parsed:
                        postprocess_ticker(t)
                        t["ticker"] = assemble_ticker(t)
                    all_results.extend(parsed)
                except Exception as e:
                    print(f"  Parse error batch {batch_idx}: {e}")
                    for m in batch_markets:
                        all_results.append({"market_id": m["market_id"], "error": str(e)})
            else:
                print(f"  API error batch {batch_idx}: {resp['response']['status_code']}")
                for m in batch_markets:
                    all_results.append({"market_id": m["market_id"], "error": "API error"})

        # Merge with existing tickers
        existing_tickers = []
        if args.output.exists():
            with open(args.output) as f:
                existing_tickers = json.load(f).get("tickers", [])
        existing_by_id = {t["market_id"]: t for t in existing_tickers if "market_id" in t}

        for t in all_results:
            mid = t.get("market_id")
            if mid:
                existing_by_id[mid] = t

        save_tickers(list(existing_by_id.values()), args.output, state["model"])
        print(f"Saved {len(existing_by_id)} tickers (including {len(all_results)} new from batch)")


def main():
    parser = argparse.ArgumentParser(description="Create tickers from markets")
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=DATA_DIR / "enriched_political_markets.json.gz",
        help="Input enriched markets file"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=DATA_DIR / "tickers.json",
        help="Output file"
    )
    parser.add_argument(
        "--model", "-m",
        type=str,
        default="gpt-4o",
        help="OpenAI model for ticker classification"
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=3,
        help="Parallel workers (keep low to avoid rate limits)"
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=50,
        help="Markets per batch"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Limit markets to process"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Reprocess all markets (ignore existing tickers)"
    )
    parser.add_argument(
        "--max-retry",
        type=int,
        default=500,
        help="Max errored markets to retry per run (default 500)"
    )
    parser.add_argument(
        "--batch-api",
        choices=["submit", "check", "download"],
        default=None,
        help="Use OpenAI Batch API (50%% cost). submit=create batch, check=poll status, download=fetch results"
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default=None,
        help="Batch ID for --batch-api check/download"
    )

    args = parser.parse_args()

    # Handle Batch API modes
    if args.batch_api:
        handle_batch_api(args)
        return

    markets = load_markets(args.input)
    if args.limit:
        markets = markets[:args.limit]

    print(f"Loaded {len(markets)} markets")

    # Build set of all enriched market IDs for pruning
    all_enriched_ids = {get_market_id(m) for m in markets}
    all_enriched_ids.discard("")

    # Incremental mode: skip markets that already have tickers
    if args.full_refresh:
        filtered, existing_tickers = markets, []
    else:
        filtered, existing_tickers = filter_to_untickered(markets, args.output)

    # Cap retried errors to avoid re-hitting rate limits all at once
    errored_ids = {t["market_id"] for t in existing_tickers if "market_id" in t and "error" in t}
    new_markets = [m for m in filtered if get_market_id(m) not in errored_ids]
    retry_markets = [m for m in filtered if get_market_id(m) in errored_ids]
    if len(retry_markets) > args.max_retry:
        print(f"Capping retries: {len(retry_markets)} errored markets, retrying {args.max_retry} this run")
        retry_markets = retry_markets[:args.max_retry]
    filtered = new_markets + retry_markets

    # Index existing tickers by market_id for merging
    existing_by_id = {t["market_id"]: t for t in existing_tickers if "market_id" in t}

    if not filtered:
        print("All markets already have tickers, pruning stale entries...")
        before = len(existing_by_id)
        pruned = {mid: t for mid, t in existing_by_id.items() if mid in all_enriched_ids}
        removed = before - len(pruned)
        if removed:
            print(f"Pruned {removed} stale tickers")
        save_tickers(list(pruned.values()), args.output, args.model)
        return

    # Run GPT pipeline on new/filtered markets only
    try:
        new_results = asyncio.run(run_pipeline_async(
            filtered,
            args.model,
            args.workers,
            args.batch_size
        ))
    except Exception as e:
        print(f"\nERROR in async pipeline: {e}")
        import traceback
        traceback.print_exc()
        print("Saving existing tickers (no new results)...")
        save_tickers(list(existing_by_id.values()), args.output, args.model)
        # Exit 0 so postprocess_tickers.py still runs on existing data
        return

    # Merge: new results overwrite/add to existing
    for t in new_results:
        mid = t.get("market_id")
        if mid:
            existing_by_id[mid] = t

    # Run regex fallback on any remaining errored tickers from prior runs
    prior_rescued = 0
    for t in existing_by_id.values():
        if "error" in t and regex_fallback_classify(t):
            postprocess_ticker(t)
            t["ticker"] = assemble_ticker(t)
            prior_rescued += 1
    if prior_rescued:
        print(f"Regex fallback rescued {prior_rescued} previously-errored tickers")

    # Prune tickers whose market_id is no longer in enriched input
    before = len(existing_by_id)
    merged = {mid: t for mid, t in existing_by_id.items() if mid in all_enriched_ids}
    removed = before - len(merged)
    if removed:
        print(f"Pruned {removed} stale tickers")

    save_tickers(list(merged.values()), args.output, args.model)


if __name__ == "__main__":
    main()
