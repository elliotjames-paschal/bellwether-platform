#!/usr/bin/env python3
"""
Post-process ticker assignments to fix common issues:

1. UNKNOWN timeframes - extract from description text
2. Election mechanism normalization - WIN + elected office = CERTIFIED
3. ANNOUNCED vs CERTIFIED heuristics - ballot qualification = CERTIFIED
"""

import json
import gzip
import re
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

from config import DATA_DIR
from create_tickers import extract_threshold

# Elected offices that should use CERTIFIED mechanism
ELECTED_OFFICES = {
    'PRES', 'VP', 'GOV', 'SENATE', 'HOUSE', 'MAYOR',
    'PRES_US', 'PRES_BR', 'PRES_PE', 'PRES_PT', 'PRES_CL', 'PRES_MX',
    'PRES_AR', 'PRES_CO', 'PRES_FR', 'PRES_DE', 'PRES_UA', 'PRES_RU',
    'GOV_AZ', 'GOV_CA', 'GOV_FL', 'GOV_TX', 'GOV_NY', 'GOV_PA', 'GOV_IA',
    'SENATE_NH', 'SENATE_OH', 'SENATE_PA', 'SENATE_GA', 'SENATE_AZ',
    'DEM_NOMINATION', 'GOP_NOMINATION', 'DEM_NOM', 'GOP_NOM',
}

# Month name to number mapping
MONTHS = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}

MONTH_NAMES = ['', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
               'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


def extract_date_from_description(description: str) -> str:
    """Extract date from description text and return timeframe string."""
    if not description:
        return None

    desc = str(description)

    # Pattern: "before/by/on/until Month DD, YYYY" or "Month DD, YYYY"
    patterns = [
        r'(?:before|by|on|until|through)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})',
        r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})',
        r'(?:in|during|by end of)\s+(\d{4})',
        r'(\d{4})\s+(?:election|presidential|gubernatorial)',
    ]

    # Try full date patterns first
    for pattern in patterns[:2]:
        match = re.search(pattern, desc, re.IGNORECASE)
        if match:
            month_name = match.group(1).lower()
            day = int(match.group(2))
            year = int(match.group(3))
            month = MONTHS.get(month_name, 0)

            if month == 0:
                continue

            # Apply same logic as extract_timeframe
            if month == 1 and day <= 20:
                return str(year - 1)
            if month == 12 and day >= 28:
                return str(year)
            if month == 3 and day >= 28:
                return f"{year}_Q1"
            elif month == 6 and day >= 28:
                return f"{year}_Q2"
            elif month == 9 and day >= 28:
                return f"{year}_Q3"

            return f"{MONTH_NAMES[month]}{year}"

    # Try year-only patterns
    for pattern in patterns[2:]:
        match = re.search(pattern, desc, re.IGNORECASE)
        if match:
            year = match.group(1)
            if year.isdigit() and 2024 <= int(year) <= 2030:
                return year

    return None


def is_elected_office(target: str) -> bool:
    """Check if target is an elected office."""
    if not target:
        return False

    # Direct match
    if target.upper() in ELECTED_OFFICES:
        return True

    # Pattern matches
    patterns = [
        r'^PRES',  # Any presidency
        r'^GOV',   # Any governorship
        r'^SENATE', # Any senate
        r'^HOUSE', # Any house
        r'^MAYOR', # Any mayoral race
        r'_NOM$',  # Nominations
        r'NOMINATION',
    ]

    for pattern in patterns:
        if re.search(pattern, target.upper()):
            return True

    return False


def fix_election_mechanism(ticker: dict) -> bool:
    """Fix 2: If action=WIN and target is elected office, mechanism=CERTIFIED."""
    action = ticker.get('action', '')
    target = ticker.get('target', '')
    mechanism = ticker.get('mechanism', '')

    if action == 'WIN' and is_elected_office(target):
        if mechanism in ['STD', 'OFFICIAL_SOURCE', 'ANNOUNCED']:
            ticker['mechanism'] = 'CERTIFIED'
            return True

    return False


def fix_ballot_qualification(ticker: dict) -> bool:
    """Fix 3: Ballot qualification questions use CERTIFIED, not ANNOUNCED."""
    question = ticker.get('original_question', '').lower()
    mechanism = ticker.get('mechanism', '')

    # Keywords indicating official ballot qualification (CERTIFIED)
    certified_keywords = [
        'qualif', 'on the ballot', 'eligible to run', 'allowed to run',
        'cleared to run', 'approved to run', 'certified'
    ]

    # Check for certified keywords
    for kw in certified_keywords:
        if kw in question:
            if mechanism in ['ANNOUNCED', 'STD']:
                ticker['mechanism'] = 'CERTIFIED'
                return True
            break

    return False


def fix_economic_mechanism(ticker: dict) -> bool:
    """Fix 4: Normalize economic data mechanisms."""
    action = ticker.get('action', '')
    target = ticker.get('target', '')
    mechanism = ticker.get('mechanism', '')

    # Economic data targets
    econ_targets = ['GDP', 'CPI', 'UNEMPLOYMENT', 'INFLATION', 'PCE', 'PPI',
                    'JOBS', 'NFP', 'PAYROLLS']

    # If it's economic data reporting
    if action == 'REPORT' and any(t in target.upper() for t in econ_targets):
        if mechanism in ['STD', 'OFFICIAL_SOURCE']:
            ticker['mechanism'] = 'MONTHLY_REPORT'
            return True

    return False


def fix_fed_mechanism(ticker: dict) -> bool:
    """Fix 5: Normalize Fed rate decision mechanisms."""
    agent = ticker.get('agent', '')
    action = ticker.get('action', '')
    target = ticker.get('target', '')
    mechanism = ticker.get('mechanism', '')

    # Fed rate decisions
    if agent == 'FED' and action in ['CUT', 'HIKE', 'HOLD'] and 'RATE' in target.upper():
        # If it's about any meeting in a time period
        if mechanism in ['STD', 'OFFICIAL_SOURCE']:
            ticker['mechanism'] = 'ANY_MEETING'
            return True

    return False


def fix_departure_mechanism(ticker: dict) -> bool:
    """Fix 6: Normalize departure/leaving mechanisms."""
    action = ticker.get('action', '')
    mechanism = ticker.get('mechanism', '')
    question = ticker.get('original_question', '').lower()

    # LEAVE actions should typically be ANY_MEANS unless rules specify otherwise
    if action == 'LEAVE':
        # Check for specific departure types in question
        if any(kw in question for kw in ['resign', 'step down', 'voluntar']):
            if mechanism != 'VOLUNTARY':
                ticker['mechanism'] = 'VOLUNTARY'
                return True
        elif mechanism in ['ANNOUNCED', 'STD']:
            ticker['mechanism'] = 'ANY_MEANS'
            return True

    return False


def fix_pardon_mechanism(ticker: dict) -> bool:
    """Fix 7: Pardons use STD (presidential action, no official source needed)."""
    action = ticker.get('action', '')
    mechanism = ticker.get('mechanism', '')

    if action == 'PARDON':
        if mechanism == 'OFFICIAL_SOURCE':
            ticker['mechanism'] = 'STD'
            return True

    return False


def extract_election_year(question: str) -> str:
    """Extract election year from question text."""
    if not question:
        return None

    q = str(question)

    # Pattern: "2026 election", "2028 presidential", "in 2026", etc.
    patterns = [
        r'(\d{4})\s+(?:election|presidential|gubernatorial|senate|house|primary|general)',
        r'(?:election|presidential|gubernatorial|senate|house|primary|general)\s+(?:in\s+)?(\d{4})',
        r'(?:in|for|during)\s+(\d{4})',
        r'(\d{4})\s+(?:race|contest|vote)',
    ]

    for pattern in patterns:
        match = re.search(pattern, q, re.IGNORECASE)
        if match:
            year = match.group(1)
            if 2024 <= int(year) <= 2032:
                return year

    return None


def fix_election_timeframe(ticker: dict) -> bool:
    """Fix 8: For elections, use election year from question instead of end_date."""
    action = ticker.get('action', '')
    target = ticker.get('target', '')
    timeframe = ticker.get('timeframe', '')
    question = ticker.get('original_question', '')

    # Only for WIN actions on elected offices
    if action != 'WIN' or not is_elected_office(target):
        return False

    # Extract election year from question
    election_year = extract_election_year(question)
    if not election_year:
        return False

    # If current timeframe is a specific month/quarter, normalize to year
    # e.g., NOV2028, JAN2029, 2028_Q4 -> 2028
    current_year = None
    if timeframe and timeframe != 'UNKNOWN':
        # Extract year from timeframe
        year_match = re.search(r'(\d{4})', timeframe)
        if year_match:
            current_year = year_match.group(1)

    # Use election year if different from current or if UNKNOWN
    if election_year != timeframe:
        ticker['timeframe'] = election_year
        return True

    return False


def fix_projected_mechanism(ticker: dict) -> bool:
    """Fix 9: PROJECTED -> CERTIFIED for elections."""
    action = ticker.get('action', '')
    target = ticker.get('target', '')
    mechanism = ticker.get('mechanism', '')

    if action == 'WIN' and is_elected_office(target):
        if mechanism == 'PROJECTED':
            ticker['mechanism'] = 'CERTIFIED'
            return True

    return False


# US state codes that could conflict with country codes
US_STATE_CODES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
}

# Country names that conflict with US state codes
COUNTRY_CONFLICTS = {
    'VA': ['vatican', 'holy see'],
    'GA': ['georgia country', 'tbilisi', 'georgian'],  # vs Georgia state
    'CO': ['colombia', 'colombian', 'bogota'],  # vs Colorado
}


def fix_state_country_collision(ticker: dict) -> bool:
    """Fix 10: Distinguish US state codes from country codes."""
    target = ticker.get('target', '')
    question = ticker.get('original_question', '').lower()

    if not target or len(target) != 2:
        return False

    if target not in US_STATE_CODES:
        return False

    # Check if question refers to a country, not US state
    conflicts = COUNTRY_CONFLICTS.get(target, [])
    for keyword in conflicts:
        if keyword in question:
            # It's a country - use full country name
            if target == 'VA' and ('vatican' in question or 'holy see' in question):
                ticker['target'] = 'VATICAN'
                return True
            elif target == 'GA' and any(kw in question for kw in ['georgia country', 'tbilisi', 'georgian']):
                ticker['target'] = 'GE'  # ISO code for Georgia country
                return True
            elif target == 'CO' and any(kw in question for kw in ['colombia', 'colombian', 'bogota']):
                ticker['target'] = 'COLOMBIA'
                return True

    return False


# Known name collisions - agents that need FIRST_LAST disambiguation
# Copied from create_tickers.py to apply retroactively to old tickers
NAME_COLLISIONS = {
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


def fix_name_collisions(ticker: dict) -> bool:
    """Fix 13: Disambiguate bare last-name agents using NAME_COLLISIONS."""
    agent = ticker.get('agent', '')
    if agent not in NAME_COLLISIONS:
        return False

    question = ticker.get('original_question', '').lower()
    first_names = NAME_COLLISIONS[agent]

    for first_name, canonical in first_names.items():
        if first_name in question:
            ticker['agent'] = canonical
            return True

    return False


def fix_missing_threshold(ticker: dict) -> bool:
    """Fix 14: Re-extract threshold for HIT markets that have ANY."""
    if ticker.get('action') != 'HIT' or ticker.get('threshold', '') != 'ANY':
        return False

    question = ticker.get('original_question', '')
    extracted = extract_threshold(question)
    if extracted != 'ANY':
        ticker['threshold'] = extracted
        return True

    return False


def reassemble_ticker(ticker: dict) -> str:
    """Reassemble ticker string from components."""
    agent = ticker.get('agent', 'UNKNOWN')
    action = ticker.get('action', 'UNKNOWN')
    target = ticker.get('target', 'UNKNOWN')
    mechanism = ticker.get('mechanism', 'STD')
    threshold = ticker.get('threshold', 'ANY')
    timeframe = ticker.get('timeframe', 'UNKNOWN')

    return f"BWR-{agent}-{action}-{target}-{mechanism}-{threshold}-{timeframe}"


def postprocess(
    tickers_file: Path,
    enriched_file: Path,
    output_file: Path
):
    """Run all post-processing fixes."""

    print(f"Loading tickers from {tickers_file}...")
    with open(tickers_file) as f:
        data = json.load(f)

    tickers = data['tickers']
    print(f"Loaded {len(tickers)} tickers")

    # Load enriched data for description access
    print(f"Loading enriched data from {enriched_file}...")
    if str(enriched_file).endswith('.gz'):
        with gzip.open(enriched_file, 'rt') as f:
            enriched = json.load(f)
    else:
        with open(enriched_file) as f:
            enriched = json.load(f)

    markets = enriched.get('markets', enriched)

    # Build market_id -> description lookup
    descriptions = {}
    for m in markets:
        csv = m.get('original_csv', m)
        api = m.get('api_data', {})
        mkt = api.get('market', {}) if isinstance(api.get('market'), dict) else {}

        market_id = csv.get('market_id', '')
        desc = mkt.get('description', '') or csv.get('pm_description', '')
        descriptions[str(market_id)] = desc

    print(f"Built description lookup for {len(descriptions)} markets")

    # Build AAT -> timeframe lookup from Kalshi (for UNKNOWN backfill)
    # Uses agent-action-target tuple to match across platforms
    kalshi_aat_timeframes = defaultdict(set)
    for ticker in tickers:
        if ticker.get('platform') == 'Kalshi':
            agent = ticker.get('agent', '')
            action = ticker.get('action', '')
            target = ticker.get('target', '')
            tf = ticker.get('timeframe', '')
            if agent and action and target and tf and tf != 'UNKNOWN':
                kalshi_aat_timeframes[(agent, action, target)].add(tf)

    # Apply fixes
    stats = defaultdict(int)

    for ticker in tickers:
        # Fix 1: UNKNOWN timeframes
        if ticker.get('timeframe') == 'UNKNOWN':
            market_id = str(ticker.get('market_id', ''))
            desc = descriptions.get(market_id, '')

            extracted = extract_date_from_description(desc)
            if extracted:
                ticker['timeframe'] = extracted
                stats['timeframe_fixed'] += 1
            else:
                stats['timeframe_still_unknown'] += 1

        # Fix 2: Election mechanism
        if fix_election_mechanism(ticker):
            stats['election_mechanism_fixed'] += 1

        # Fix 3: Ballot qualification
        if fix_ballot_qualification(ticker):
            stats['ballot_qualification_fixed'] += 1

        # Fix 4: Economic data mechanism
        if fix_economic_mechanism(ticker):
            stats['economic_mechanism_fixed'] += 1

        # Fix 5: Fed rate mechanism
        if fix_fed_mechanism(ticker):
            stats['fed_mechanism_fixed'] += 1

        # Fix 6: Departure mechanism
        if fix_departure_mechanism(ticker):
            stats['departure_mechanism_fixed'] += 1

        # Fix 7: Pardon mechanism
        if fix_pardon_mechanism(ticker):
            stats['pardon_mechanism_fixed'] += 1

        # Fix 8: Election timeframe normalization
        if fix_election_timeframe(ticker):
            stats['election_timeframe_fixed'] += 1

        # Fix 9: PROJECTED -> CERTIFIED for elections
        if fix_projected_mechanism(ticker):
            stats['projected_mechanism_fixed'] += 1

        # Fix 10: State/country collision
        if fix_state_country_collision(ticker):
            stats['state_country_fixed'] += 1

        # Fix 11: UNKNOWN timeframe backfill from Kalshi
        if ticker.get('timeframe') == 'UNKNOWN' and ticker.get('platform') == 'Polymarket':
            aat = (ticker.get('agent', ''), ticker.get('action', ''), ticker.get('target', ''))
            kalshi_tfs = kalshi_aat_timeframes.get(aat, set())
            if len(kalshi_tfs) == 1:
                # Exactly one Kalshi family - inherit timeframe
                ticker['timeframe'] = list(kalshi_tfs)[0]
                stats['timeframe_backfilled'] += 1

        # Fix 12: Final fallback - UNKNOWN -> current year
        if ticker.get('timeframe') == 'UNKNOWN':
            current_year = str(datetime.now().year)
            ticker['timeframe'] = current_year
            stats['timeframe_current_year_fallback'] += 1

        # Fix 13: Name collision disambiguation
        if fix_name_collisions(ticker):
            stats['name_collision_fixed'] += 1

        # Fix 14: Re-extract missing thresholds
        if fix_missing_threshold(ticker):
            stats['threshold_reextracted'] += 1

        # Fix 15: Apply corrections derived from human feedback
        corrections_file = tickers_file.parent / "ticker_corrections.json"
        if corrections_file.exists():
            try:
                with open(corrections_file) as cf:
                    corr_data = json.load(cf)
                for corr in corr_data.get("corrections", []):
                    corr_type = corr.get("type", "")
                    from_val = corr.get("from", "")
                    to_val = corr.get("to", "")
                    if corr_type == "mechanism_alias" and ticker.get("mechanism") == from_val:
                        ticker["mechanism"] = to_val
                        stats["correction_mechanism_alias"] += 1
                    elif corr_type == "agent_alias" and ticker.get("agent") == from_val:
                        ticker["agent"] = to_val
                        stats["correction_agent_alias"] += 1
                    elif corr_type == "target_alias" and ticker.get("target") == from_val:
                        ticker["target"] = to_val
                        stats["correction_target_alias"] += 1
                    elif corr_type == "timeframe_alias" and ticker.get("timeframe") == from_val:
                        ticker["timeframe"] = to_val
                        stats["correction_timeframe_alias"] += 1
            except (json.JSONDecodeError, OSError):
                pass  # Skip if corrections file is invalid

        # Reassemble ticker string
        ticker['ticker'] = reassemble_ticker(ticker)

    # Save output
    data['postprocessed_at'] = datetime.now().isoformat()
    data['postprocess_stats'] = dict(stats)

    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    # Summary
    print(f"\n=== POST-PROCESSING COMPLETE ===")
    print(f"Timeframes fixed from description: {stats['timeframe_fixed']}")
    print(f"Timeframes still unknown: {stats['timeframe_still_unknown']}")
    print(f"Election mechanisms fixed: {stats['election_mechanism_fixed']}")
    print(f"Ballot qualification fixed: {stats['ballot_qualification_fixed']}")
    print(f"Economic mechanism fixed: {stats['economic_mechanism_fixed']}")
    print(f"Fed mechanism fixed: {stats['fed_mechanism_fixed']}")
    print(f"Departure mechanism fixed: {stats['departure_mechanism_fixed']}")
    print(f"Pardon mechanism fixed: {stats['pardon_mechanism_fixed']}")
    print(f"Election timeframe normalized: {stats['election_timeframe_fixed']}")
    print(f"PROJECTED -> CERTIFIED: {stats['projected_mechanism_fixed']}")
    print(f"State/country collision fixed: {stats['state_country_fixed']}")
    print(f"Timeframe backfilled from Kalshi: {stats['timeframe_backfilled']}")
    print(f"Timeframe fallback to current year: {stats['timeframe_current_year_fallback']}")
    print(f"Name collisions disambiguated: {stats['name_collision_fixed']}")
    print(f"Thresholds re-extracted: {stats['threshold_reextracted']}")

    # Recalculate stats
    unique_tickers = len(set(t['ticker'] for t in tickers))
    print(f"\nNew unique tickers: {unique_tickers}")
    print(f"Output: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Post-process ticker assignments")
    parser.add_argument(
        "--tickers", "-t",
        type=Path,
        default=DATA_DIR / "tickers_all.json",
        help="Input tickers file"
    )
    parser.add_argument(
        "--enriched", "-e",
        type=Path,
        default=DATA_DIR / "enriched_political_markets.json.gz",
        help="Enriched markets file (for descriptions)"
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=DATA_DIR / "tickers_postprocessed.json",
        help="Output file"
    )

    args = parser.parse_args()

    postprocess(args.tickers, args.enriched, args.output)


if __name__ == "__main__":
    main()
