#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Select Election Winner Markets (Combined Steps 1+2+3)
================================================================================

Part of the Bellwether Pipeline

This script combines the full election comparison pipeline into ONE step:

For each unique election (country, office, location, year, is_primary):
  1. Pre-filter markets: exclude vote share, margin, threshold, dropout questions
  2. Sort remaining by volume (highest first), cap at top 15 per platform
  3. Send to GPT-4o with web search:
     - Who won this election? (vote shares)
     - Which market on each platform is about the winner?
  4. Save: vote shares to master CSV + winner market selections to lookup file

Output:
  - Updates master CSV with vote share data
  - Creates data/election_winner_selections.json (GPT-selected winner market IDs)
  - election_winner_markets_comparison.py reads this file directly

Usage:
    python pipeline_select_election_winners.py [--dry-run] [--limit N] [--force] [--enrich-only]

Options:
    --dry-run      Show elections that would be processed without API calls
    --limit N      Only process N elections (for testing)
    --force        Re-process all elections (ignore cached results)
    --enrich-only  Only run enrichment on existing selections (no GPT calls)

================================================================================
"""

import pandas as pd
import json
import os
import sys
import re
import time
import asyncio
import smtplib
from datetime import datetime
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

sys.path.insert(0, str(Path(__file__).parent))
from config import BASE_DIR, DATA_DIR, rotate_backups

# Import coordinate lookup from generate_web_data
from generate_web_data import LOCATION_COORDS, US_STATE_ABBREVS

# =============================================================================
# CONFIGURATION
# =============================================================================

MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
SELECTIONS_FILE = DATA_DIR / "election_winner_selections.json"
BACKUP_DIR = DATA_DIR / "backups"
LOGS_DIR = BASE_DIR / "logs"
EMAIL_CONFIG_FILE = LOGS_DIR / "email_config.json"
GPT_OVERRIDE_LOG = LOGS_DIR / "gpt_overrides.json"

# GPT model with built-in web search for election results
SEARCH_MODEL = "gpt-4o-search-preview"
MAX_RETRIES = 3
DELAY_BETWEEN_CALLS = 2  # seconds

# Pre-filter: regex patterns for NON-winner markets (excluded before GPT)
# These patterns identify markets that are NOT simple "will X win?" questions
NON_WINNER_PATTERNS = [
    # === Vote share / margin / threshold patterns ===
    r'vote\s+share',
    r'popular\s+vote\s*%',
    r'more\s+than\s+\d+',
    r'at\s+least\s+\d+',
    r'fewer\s+than\s+\d+',
    r'less\s+than\s+\d+',
    r'\d+\s*%\s+of\s+(the\s+)?votes?',  # "X% of the vote", "X% of votes"
    r'between\s+\d+\s*%?\s+and\s+\d+',  # "between 50% and 100%" margin buckets
    r'by\s+\d+\s+point',
    r'margin\s+of',
    r'margin\s+in',
    r'win\s+by\s+more',
    r'percentage',
    r'total\s+votes',
    r'turnout',
    r'electoral\s+vote',
    r'\d+\s+swing\s+state',
    r'swing\s+state.*\d+',

    # === Dropout / withdrawal / suspension ===
    r'drop\s*out',
    r'withdraw',
    r'resign',
    r'suspend\s+(his|her|their)?\s*campaign',

    # === Count / quantity questions ===
    r'how\s+many',
    r'number\s+of',

    # === Endorsements ===
    r'\bendorse\b',
    r'endorsement',

    # === Debates / speeches / events ===
    r'\bdebate\b',
    r'\bspeech\b',
    r'state\s+of\s+the\s+union',

    # === Approval / ratings / polling ===
    r'\bapproval\b',
    r'favorability',
    r'\brating\b',
    r'polling\s+average',

    # === Announcements / filings ===
    r'\bannounce\b',
    r'file\s+to\s+run',
    r'enter\s+the\s+race',

    # === Temporal / tenure questions ===
    r'before\s+(january|february|march|april|may|june|july|august|september|october|november|december)',
    r'by\s+(january|february|march|april|may|june|july|august|september|october|november|december)',
    r'by\s+end\s+of',
    r'by\s+the\s+end\s+of',
    r'still\s+in\s+office',
    r'leave\s+office',

    # === Conviction / legal outcomes ===
    r'\bconvicted\b',
    r'\bindicted\b',
    r'\bsentenced\b',
    r'prison\s+time',

    # === Runoff / second round (separate markets) ===
    r'runoff',
    r'second\s+round',
]
NON_WINNER_REGEX = re.compile('|'.join(NON_WINNER_PATTERNS), re.IGNORECASE)

# Inclusion patterns: markets MUST match one of these to be a winner market
WINNER_KEYWORDS = [
    # Direct win language
    r'\bwin\b',
    r'\bwins\b',
    r'\bwinner\b',
    r'\bwinning\b',
    r'\bvictory\b',
    r'\bvictorious\b',
    r'\belected\b',
    r'\belect\b',
    r'\bcontrol\b',      # "Democrats control Senate"
    r'\bflip\b',         # "Will X flip to Democrat"
    r'\bmajority\b',     # "Republican majority"

    # Head-to-head matchups ("X vs Y")
    r'\bvs\.?\b',

    # Office holder patterns ("Will X be Governor/Senator/President")
    r'\bbe\s+(the\s+)?(governor|senator|president|mayor|representative|rep\.|congressman|congresswoman)\b',
    r'\bbe\s+(the\s+)?next\s+(governor|senator|president|mayor)\b',
    r'\bstill\s+be\s+(a\s+)?(governor|senator|president|mayor)\b',

    # Nomination patterns
    r'\bnominee\b',
    r'\bnomination\b',
    r'\bnom\b',          # "D-nom", "R-nom"
    r'\bprimary\b',      # Primary election context
    r'\bfirst\s+place\b',

    # Party winner patterns
    r'\b(democrat|republican|dem|rep|D|R)\s+(win|wins|victory|control)\b',
    r'\b(democratic|republican)\s+(party|candidate)\b',
]
WINNER_KEYWORD_REGEX = re.compile('|'.join(WINNER_KEYWORDS), re.IGNORECASE)

# Max markets per platform to send to GPT (sorted by volume)
MAX_MARKETS_PER_PLATFORM = 15

# Election grouping columns
ELECTION_COLS = ['country', 'office', 'location', 'election_year', 'is_primary']


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def load_email_config():
    """Load email configuration."""
    if not EMAIL_CONFIG_FILE.exists():
        return None
    try:
        with open(EMAIL_CONFIG_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return None


def send_gpt_override_email(overrides):
    """Send email notification when GPT finds markets via web search."""
    config = load_email_config()
    if not config:
        log("No email config - logging GPT overrides to console only")
        return False

    try:
        # Build email body
        lines = [
            "⚠️ GPT MARKET OVERRIDE ALERT",
            "=" * 50,
            "",
            f"GPT found {len(overrides)} market(s) via web search that were NOT in the provided election groups.",
            "This may indicate labeling errors in the master CSV that need review.",
            "",
            "OVERRIDES:",
            "-" * 50,
        ]

        for override in overrides:
            lines.append(f"\nElection: {override['election_key']}")
            lines.append(f"  Platform: {override['platform']}")
            lines.append(f"  Market ID: {override['market_id']}")
            lines.append(f"  Reason: {override['reason']}")

        lines.append("")
        lines.append("-" * 50)
        lines.append("Please review these markets and fix any labeling issues in the master CSV.")

        body = "\n".join(lines)

        # Send email
        msg = MIMEMultipart()
        msg.attach(MIMEText(body, 'plain'))
        msg['From'] = config.get('from_email', config.get('smtp_user'))
        msg['To'] = ', '.join(config.get('recipients', []))
        msg['Subject'] = f"⚠️ Bellwether: GPT found {len(overrides)} market(s) via web search"

        with smtplib.SMTP(config['smtp_server'], config.get('smtp_port', 587)) as server:
            server.starttls()
            server.login(config['smtp_user'], config['smtp_password'])
            server.send_message(msg)

        log(f"  📧 GPT override email sent to {len(config.get('recipients', []))} recipient(s)")
        return True

    except Exception as e:
        log(f"  Failed to send GPT override email: {e}")
        return False


def log_gpt_overrides(overrides):
    """Log GPT overrides to a JSON file for tracking."""
    LOGS_DIR.mkdir(exist_ok=True)

    # Load existing log
    existing = []
    if GPT_OVERRIDE_LOG.exists():
        try:
            with open(GPT_OVERRIDE_LOG, 'r') as f:
                existing = json.load(f)
        except Exception:
            existing = []

    # Add new overrides with timestamp
    for override in overrides:
        override['logged_at'] = datetime.now().isoformat()
        existing.append(override)

    # Save
    with open(GPT_OVERRIDE_LOG, 'w') as f:
        json.dump(existing, f, indent=2)

    log(f"  📝 Logged {len(overrides)} GPT override(s) to {GPT_OVERRIDE_LOG.name}")


def make_election_key(row):
    """Create a unique string key for an election."""
    parts = []
    for col in ELECTION_COLS:
        val = row.get(col)
        if pd.notna(val):
            parts.append(str(val))
        else:
            parts.append("")
    return "|".join(parts)


def load_selections():
    """Load existing winner selections."""
    if SELECTIONS_FILE.exists():
        with open(SELECTIONS_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_selections(selections):
    """Save winner selections."""
    with open(SELECTIONS_FILE, 'w') as f:
        json.dump(selections, f, indent=2)



def pre_filter_markets(markets_df):
    """
    Remove obvious non-winner markets using regex patterns.
    Returns filtered dataframe.
    """
    mask = markets_df['question'].str.contains(NON_WINNER_REGEX, na=False)
    return markets_df[~mask].copy()


def build_prompt(election_info, pm_markets, kalshi_markets):
    """
    Build GPT prompt for a single election.

    Args:
        election_info: dict with country, office, location, year, is_primary
        pm_markets: list of dicts with market_id, question, volume_usd
        kalshi_markets: list of dicts with market_id, question, volume_usd
    """
    # Build election description
    parts = []
    year_val = election_info.get('election_year')
    if year_val and pd.notna(year_val):
        parts.append(str(int(float(year_val))))
    loc_val = election_info.get('location')
    if loc_val and pd.notna(loc_val) and str(loc_val) != election_info.get('country'):
        parts.append(str(loc_val))
    country_val = election_info.get('country')
    if country_val and pd.notna(country_val):
        parts.append(str(country_val))
    office_val = election_info.get('office')
    if office_val and pd.notna(office_val):
        parts.append(str(office_val))

    is_primary = election_info.get('is_primary')
    if is_primary and str(is_primary).lower() == 'true':
        parts.append("Primary")

    election_desc = " ".join(parts) + " Election"

    # Build market lists
    pm_section = "POLYMARKET MARKETS:\n"
    if pm_markets:
        for i, m in enumerate(pm_markets, 1):
            vol = f"${m['volume_usd']:,.0f}" if m.get('volume_usd') else "N/A"
            slug = m.get('pm_event_slug', '')
            slug_str = f" (Event: {slug})" if slug else ""
            pm_section += f"  {i}. [ID: {m['market_id']}] \"{m['question']}\"{slug_str} (Volume: {vol})\n"
    else:
        pm_section += "  (No markets on this platform)\n"

    kalshi_section = "KALSHI MARKETS:\n"
    if kalshi_markets:
        for i, m in enumerate(kalshi_markets, 1):
            vol = f"${m['volume_usd']:,.0f}" if m.get('volume_usd') else "N/A"
            kalshi_section += f"  {i}. [ID: {m['market_id']}] \"{m['question']}\" (Volume: {vol})\n"
    else:
        kalshi_section += "  (No markets on this platform)\n"

    prompt = f"""Search the web for the official results of the {election_desc}.

Then, from the prediction markets listed below, identify which market on each platform
is the direct "will the winner win?" market.

A "winner market" asks: "Will [candidate/party] win [this election]?" with a binary yes/no outcome.
It is NOT a vote share question, margin question, threshold question, or dropout question.

IMPORTANT: When multiple winner markets exist for the same election on a platform
(e.g., both a candidate-specific market and a party market), select the one with
the HIGHEST VOLUME. Volume is shown in parentheses next to each market.

IMPORTANT: For Polymarket markets, the "Event" slug (if shown) indicates what the market
actually covers. Use this to disambiguate — e.g., a market titled "Will a Democrat win
Maine's 2nd congressional district?" with Event slug containing "presidential" is about
the presidential race in that district, NOT the House race. Only select markets that
match the specific election being analyzed.

{pm_section}
{kalshi_section}

Return a JSON object:
{{
    "election_found": true,
    "democrat_vote_share": <number or null if non-partisan/primary>,
    "republican_vote_share": <number or null if non-partisan/primary>,
    "winning_candidate": "<name>",
    "winning_party": "Democrat" | "Republican" | null,
    "polymarket_winner_market_id": "<market_id from list above or null if no winner market>",
    "kalshi_winner_market_id": "<market_id from list above or null if no winner market>",
    "notes": "<brief explanation of selection>"
}}

For PRIMARY elections:
- democrat_vote_share / republican_vote_share may not apply (all same party)
- Set winning_party to the party of the primary
- Pick the market about the candidate who actually won the primary

If the election hasn't happened yet or results can't be found:
{{
    "election_found": false,
    "notes": "reason"
}}

If no market on a platform is a direct winner market, set that platform's ID to null.

Return ONLY the JSON object, no other text."""

    return prompt


def extract_json_from_response(text):
    """Extract JSON from GPT response."""
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


# Regex for plausible winner market questions (binary "will X win Y?" style)
WINNER_MARKET_REGEX = re.compile(
    r'\b(win|wins|winner)\b', re.IGNORECASE
)


def is_plausible_winner_market(question):
    """Check if a market question looks like a binary winner market."""
    q = str(question)
    # Must match winner pattern and NOT match non-winner patterns
    return (WINNER_MARKET_REGEX.search(q) is not None
            and not NON_WINNER_REGEX.search(q))


def is_winner_side_market(question, winning_candidate, winning_party):
    """Check if a market question is about the winning candidate or party."""
    q = question.lower()

    # Check winning candidate (last name match)
    if winning_candidate and winning_candidate.lower() not in ('none', '?', ''):
        parts = winning_candidate.strip().split()
        if parts:
            last_name = parts[-1].lower()
            if len(last_name) > 2 and last_name in q:
                return True

    # Check winning party
    if winning_party and winning_party.lower() not in ('none', 'unknown', '?', ''):
        if winning_party.lower() in q:
            return True

    # Head-to-head markets (mention both candidates) are always valid
    # These are neutral — they resolve based on who wins
    if re.search(r'\bvs\.?\b', q, re.IGNORECASE):
        return True

    return False


def validate_selection_volume(result, pm_markets, kalshi_markets):
    """
    Validate GPT's winner market selection against volume.

    If GPT selected a winner market but a higher-volume plausible winner market
    exists on the same platform, swap to the higher-volume market.

    Returns (result, swapped) where swapped is True if any selection was changed.
    """
    swapped = False

    # Extract winner info from GPT result
    winning_candidate = result.get('winning_candidate', '')
    winning_party = result.get('winning_party', '')

    for platform_key, markets in [('polymarket_winner_market_id', pm_markets),
                                  ('kalshi_winner_market_id', kalshi_markets)]:
        selected_id = result.get(platform_key)
        if not selected_id or not markets:
            continue

        # Find the selected market's volume
        selected_vol = None
        for m in markets:
            if str(m['market_id']) == str(selected_id):
                selected_vol = m.get('volume_usd', 0)
                break

        # GPT found a market via web search that wasn't in the provided list
        # This can happen when our labeling is wrong - GPT may have found the correct market
        # Allow it but log and flag for review
        if selected_vol is None:
            platform_name = 'PM' if 'polymarket' in platform_key else 'Kalshi'
            log(f"    ⚠️ GPT OVERRIDE ({platform_name}): {selected_id} found via search (not in provided list)")
            # Track this for email notification
            if 'gpt_overrides' not in result:
                result['gpt_overrides'] = []
            result['gpt_overrides'].append({
                'platform': platform_name,
                'market_id': selected_id,
                'reason': 'Market found via GPT web search, not in election group'
            })
            swapped = True
            continue

        # Find the highest-volume plausible winner market on this platform
        # Must be both a winner-style market AND about the winning side
        best_id = selected_id
        best_vol = selected_vol
        for m in markets:
            if (is_plausible_winner_market(m['question'])
                    and is_winner_side_market(m['question'], winning_candidate, winning_party)
                    and m.get('volume_usd', 0) > best_vol):
                best_id = m['market_id']
                best_vol = m['volume_usd']

        if str(best_id) != str(selected_id):
            platform_name = 'PM' if 'polymarket' in platform_key else 'Kalshi'
            log(f"    VOLUME OVERRIDE ({platform_name}): "
                f"{selected_id} (${selected_vol:,.0f}) -> {best_id} (${best_vol:,.0f})")
            result[platform_key] = str(best_id)
            swapped = True

    return result, swapped


def query_gpt(client, prompt):
    """Send prompt to GPT-4o-search-preview and parse JSON response."""
    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=SEARCH_MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=600
            )
            text = response.choices[0].message.content.strip()
            json_str = extract_json_from_response(text)
            return json.loads(json_str)

        except json.JSONDecodeError as e:
            log(f"    JSON parse error (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(DELAY_BETWEEN_CALLS)

        except Exception as e:
            log(f"    API error (attempt {attempt + 1}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(DELAY_BETWEEN_CALLS)

    return None


# Concurrency for parallel GPT calls
MAX_CONCURRENT_GPT = 5


async def query_gpt_async(client, prompt, semaphore):
    """Send prompt to GPT-4o-search-preview asynchronously with rate limiting."""
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                response = await client.chat.completions.create(
                    model=SEARCH_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=600
                )
                text = response.choices[0].message.content.strip()
                json_str = extract_json_from_response(text)
                return json.loads(json_str)

            except json.JSONDecodeError as e:
                log(f"    JSON parse error (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(DELAY_BETWEEN_CALLS)

            except Exception as e:
                log(f"    API error (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    wait = DELAY_BETWEEN_CALLS * (2 ** attempt)
                    await asyncio.sleep(wait)

        return None


async def run_elections_async(prepared):
    """Run GPT calls for all prepared elections in parallel.

    Args:
        prepared: list of (key, prompt, pm_markets, kalshi_markets, desc)

    Returns:
        list of (key, result, pm_markets, kalshi_markets, desc)
    """
    from openai import AsyncOpenAI
    client = AsyncOpenAI()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_GPT)

    async def process_one(item):
        key, prompt, pm_markets, kalshi_markets, desc = item
        result = await query_gpt_async(client, prompt, semaphore)
        return (key, result, pm_markets, kalshi_markets, desc)

    tasks = [process_one(item) for item in prepared]
    results = []
    completed = 0

    for coro in asyncio.as_completed(tasks):
        result = await coro
        results.append(result)
        completed += 1
        if completed % 10 == 0 or completed == len(tasks):
            log(f"  GPT calls completed: {completed}/{len(tasks)}")

    return results


def _resolve_coords(country, location):
    """Resolve (country, location) to (lat, lng) using LOCATION_COORDS.

    Falls back to US state abbreviation lookup for House districts,
    then to country-level coordinates.  Returns None if no match.
    """
    key = (str(country), str(location))
    if key in LOCATION_COORDS:
        return LOCATION_COORDS[key]

    # US House district fallback: "AZ-1" -> "Arizona"
    if country == 'United States':
        m = re.match(r'^([A-Z]{2})-', str(location))
        if m:
            state_name = US_STATE_ABBREVS.get(m.group(1))
            if state_name:
                state_key = ('United States', state_name)
                if state_key in LOCATION_COORDS:
                    return LOCATION_COORDS[state_key]

    # Country-level fallback
    country_key = (str(country), str(country))
    if country_key in LOCATION_COORDS:
        return LOCATION_COORDS[country_key]

    return None


def enrich_selections(df, selections):
    """Enrich selections with additional fields needed for globe and monitor.

    Adds:
      - lat, lng: Geographic coordinates
      - pm_event_slug, k_event_ticker: Platform event identifiers
      - pm_markets_count, k_markets_count: Market counts per platform
      - pm_volume_usd, k_volume_usd: Volume per platform
      - is_completed: Whether election has concluded
    """
    current_year = datetime.now().year
    enriched_count = 0

    for election_key, selection in selections.items():
        # Parse election key
        parts = election_key.split('|')
        if len(parts) < 5:
            continue

        country = parts[0] or None
        office = parts[1] or None
        location = parts[2] or None
        year_str = parts[3] or None
        is_primary = parts[4] or None

        # Skip if already enriched
        if selection.get('lat') is not None:
            continue

        # Resolve coordinates
        if country and location:
            coords = _resolve_coords(country, location)
            if coords:
                selection['lat'] = coords[0]
                selection['lng'] = coords[1]
            else:
                selection['lat'] = None
                selection['lng'] = None
        else:
            selection['lat'] = None
            selection['lng'] = None

        # Find matching markets in master CSV
        mask = pd.Series([True] * len(df))
        if country:
            mask &= (df['country'] == country)
        else:
            mask &= df['country'].isna()
        if location:
            mask &= (df['location'] == location)
        else:
            mask &= df['location'].isna()
        if office:
            mask &= (df['office'] == office)
        else:
            mask &= df['office'].isna()
        if year_str:
            try:
                year_val = float(year_str)
                mask &= (df['election_year'] == year_val)
            except ValueError:
                mask &= df['election_year'].isna()
        else:
            mask &= df['election_year'].isna()
        if is_primary:
            mask &= (df['is_primary'].astype(str) == str(is_primary))
        else:
            mask &= df['is_primary'].isna()

        election_markets = df[mask]

        # Calculate per-platform stats
        pm_markets = election_markets[election_markets['platform'] == 'Polymarket']
        k_markets = election_markets[election_markets['platform'] == 'Kalshi']

        selection['pm_markets_count'] = int(len(pm_markets))
        selection['k_markets_count'] = int(len(k_markets))
        selection['pm_volume_usd'] = float(pm_markets['volume_usd'].sum()) if len(pm_markets) > 0 else 0.0
        selection['k_volume_usd'] = float(k_markets['volume_usd'].sum()) if len(k_markets) > 0 else 0.0

        # Get event identifiers from winner markets or highest-volume market
        pm_winner_id = selection.get('polymarket_winner_market_id')
        k_winner_id = selection.get('kalshi_winner_market_id')

        # Helper to safely get top market by volume
        def get_top_market(markets_df):
            """Safely get the highest-volume market row, or None if empty/all-NaN."""
            if len(markets_df) == 0:
                return None
            vol = markets_df['volume_usd']
            if vol.isna().all():
                return markets_df.iloc[0]  # Fallback to first row
            return markets_df.loc[vol.idxmax()]

        # For LIVE elections (no winner market yet), identify likely winner market
        # by filtering out non-winner markets and picking highest volume
        if not selection.get('election_found', False):
            # Filter to likely winner markets using exclusion patterns
            def filter_to_winner_candidates(markets_df):
                if len(markets_df) == 0:
                    return markets_df
                # Exclude markets matching non-winner patterns
                mask = ~markets_df['question'].str.contains(NON_WINNER_REGEX, na=False)
                return markets_df[mask]

            # Polymarket: identify likely winner market
            if not pm_winner_id and len(pm_markets) > 0:
                pm_candidates = filter_to_winner_candidates(pm_markets)
                if len(pm_candidates) > 0:
                    top_pm = get_top_market(pm_candidates)
                    if top_pm is not None:
                        selection['polymarket_winner_market_id'] = str(top_pm['market_id'])
                        pm_winner_id = selection['polymarket_winner_market_id']

            # Kalshi: identify likely winner market
            if not k_winner_id and len(k_markets) > 0:
                k_candidates = filter_to_winner_candidates(k_markets)
                if len(k_candidates) > 0:
                    top_k = get_top_market(k_candidates)
                    if top_k is not None:
                        selection['kalshi_winner_market_id'] = str(top_k['market_id'])
                        k_winner_id = selection['kalshi_winner_market_id']

        # Polymarket event slug
        selection['pm_event_slug'] = None
        if len(pm_markets) > 0:
            slug_found = False
            if pm_winner_id:
                winner_row = pm_markets[pm_markets['market_id'].astype(str) == str(pm_winner_id)]
                if len(winner_row) > 0:
                    slug = winner_row.iloc[0].get('pm_event_slug')
                    selection['pm_event_slug'] = str(slug) if pd.notna(slug) else None
                    slug_found = True
            if not slug_found:
                top_pm = get_top_market(pm_markets)
                if top_pm is not None:
                    slug = top_pm.get('pm_event_slug')
                    selection['pm_event_slug'] = str(slug) if pd.notna(slug) else None

        # Kalshi event ticker
        selection['k_event_ticker'] = None
        if len(k_markets) > 0:
            ticker_found = False
            if k_winner_id:
                winner_row = k_markets[k_markets['market_id'].astype(str) == str(k_winner_id)]
                if len(winner_row) > 0:
                    ticker = winner_row.iloc[0].get('k_event_ticker')
                    selection['k_event_ticker'] = str(ticker) if pd.notna(ticker) else None
                    ticker_found = True
            if not ticker_found:
                top_k = get_top_market(k_markets)
                if top_k is not None:
                    ticker = top_k.get('k_event_ticker')
                    selection['k_event_ticker'] = str(ticker) if pd.notna(ticker) else None

        # Determine if completed
        if year_str:
            try:
                year_val = float(year_str)
                selection['is_completed'] = year_val < current_year
            except ValueError:
                selection['is_completed'] = False
        else:
            selection['is_completed'] = False

        enriched_count += 1

    return enriched_count


def update_master_vote_shares(df, election_key, result):
    """Update vote shares on master CSV for all markets in this election."""
    parts = election_key.split("|")
    # Key format: country|office|location|election_year|is_primary (from ELECTION_COLS)
    election = {
        "country": parts[0] or None,
        "office": parts[1] or None,
        "location": parts[2] or None,
        "year": float(parts[3]) if parts[3] else None,
        "is_primary": parts[4] or None
    }

    mask = pd.Series([True] * len(df))

    # FIX: Match fields exactly, including NaN values
    # When field is None/empty, only match rows where that field is also NaN
    # This prevents vote shares from leaking across unrelated elections

    # Country
    if election['country']:
        mask &= (df['country'] == election['country'])
    else:
        mask &= df['country'].isna()

    # Location
    if election['location']:
        mask &= (df['location'] == election['location'])
    else:
        mask &= df['location'].isna()

    # Office
    if election['office']:
        mask &= (df['office'] == election['office'])
    else:
        mask &= df['office'].isna()

    # Year
    if election['year']:
        mask &= (df['election_year'] == election['year'])
    else:
        mask &= df['election_year'].isna()

    # Is Primary
    if election['is_primary']:
        mask &= (df['is_primary'].astype(str) == str(election['is_primary']))
    else:
        mask &= df['is_primary'].isna()

    # Only update markets missing vote share
    d_share = result.get('democrat_vote_share')
    r_share = result.get('republican_vote_share')

    if d_share is not None and r_share is not None:
        missing = mask & df['democrat_vote_share'].isna()
        count = missing.sum()
        if count > 0:
            # Convert from percentage (0-100) to proportion (0-1)
            df.loc[missing, 'democrat_vote_share'] = d_share / 100
            df.loc[missing, 'republican_vote_share'] = r_share / 100
            df.loc[missing, 'vote_share_source'] = 'GPT-4o web search (pipeline)'
            return count

    return 0


def main():
    # Parse arguments
    dry_run = '--dry-run' in sys.argv
    force = '--force' in sys.argv
    enrich_only = '--enrich-only' in sys.argv
    limit = None
    for i, arg in enumerate(sys.argv):
        if arg == '--limit' and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])

    print("\n" + "=" * 70)
    print("PIPELINE: SELECT ELECTION WINNER MARKETS (Combined Steps 1+2+3)")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if dry_run:
        print("MODE: DRY RUN")
    if force:
        print("MODE: FORCE (re-process all)")
    if enrich_only:
        print("MODE: ENRICH ONLY (no GPT calls)")
    if limit:
        print(f"LIMIT: {limit} elections")
    print("=" * 70 + "\n")

    # Enrich-only mode: just enrich existing selections and exit
    if enrich_only:
        log("Loading master CSV for enrichment...")
        df = pd.read_csv(MASTER_FILE, low_memory=False)
        log(f"  Total markets: {len(df):,}")

        # Ensure vote share columns exist
        for col in ['democrat_vote_share', 'republican_vote_share', 'vote_share_source']:
            if col not in df.columns:
                df[col] = pd.NA

        selections = load_selections()
        log(f"  Existing selections: {len(selections):,}")

        log("\nEnriching selections with globe/monitor data...")
        enriched = enrich_selections(df, selections)
        save_selections(selections)

        print("\n" + "=" * 70)
        print("ENRICHMENT COMPLETE")
        print("=" * 70)
        print(f"Elections enriched: {enriched}")
        print(f"Selections saved to: {SELECTIONS_FILE}")
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70 + "\n")
        return enriched

    # Load data
    log("Loading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    log(f"  Total markets: {len(df):,}")

    # Ensure vote share columns exist
    for col in ['democrat_vote_share', 'republican_vote_share', 'vote_share_source']:
        if col not in df.columns:
            df[col] = pd.NA

    # Filter to US electoral markets
    electoral = df[
        ((df['political_category'].str.startswith('1.', na=False)) |
         (df['political_category'].str.contains('ELECTORAL', case=False, na=False))) &
        (df['country'] == 'United States')
    ].copy()
    log(f"  US electoral markets: {len(electoral):,}")

    # Group by election
    elections = {}
    for _, row in electoral.iterrows():
        key = make_election_key(row)
        if key not in elections:
            elections[key] = {
                'info': {col: row[col] for col in ELECTION_COLS},
                'markets': []
            }
        elections[key]['markets'].append({
            'market_id': str(row['market_id']),
            'question': str(row.get('question', '')),
            'volume_usd': float(row['volume_usd']) if pd.notna(row.get('volume_usd')) else 0,
            'platform': row.get('platform', ''),
            'pm_event_slug': str(row.get('pm_event_slug', '')) if pd.notna(row.get('pm_event_slug')) else '',
        })

    log(f"  Unique elections: {len(elections):,}")

    # Exclude state-level presidential elections (they're nested under national presidential)
    # These are not standalone elections - they're slices of the national race
    us_states = set(US_STATE_ABBREVS.values())
    excluded_state_pres = []
    for key in list(elections.keys()):
        info = elections[key]['info']
        if info.get('office') == 'President' and info.get('location') in us_states:
            excluded_state_pres.append(key)
            del elections[key]
    if excluded_state_pres:
        log(f"  Excluded state-level presidential: {len(excluded_state_pres):,}")

    # Load existing selections
    selections = load_selections()

    # Determine which elections need processing
    if force:
        elections_to_process = list(elections.keys())
    else:
        elections_to_process = [k for k in elections if k not in selections]

    log(f"  Already processed: {len(selections):,}")
    log(f"  Need processing: {len(elections_to_process):,}")

    if limit:
        elections_to_process = elections_to_process[:limit]
        log(f"  Limited to: {len(elections_to_process):,}")

    if len(elections_to_process) == 0:
        log("\nNo elections need processing!")
        return 0

    # Show what would be processed
    log(f"\nElections to process:")
    for key in elections_to_process[:10]:
        info = elections[key]['info']
        n_markets = len(elections[key]['markets'])
        year = int(float(info['election_year'])) if pd.notna(info.get('election_year')) else '?'
        is_primary = " (Primary)" if str(info.get('is_primary', '')).lower() == 'true' else ""
        log(f"  {year} {info.get('office', '?')} - {info.get('location', '?')}{is_primary} ({n_markets} markets)")
    if len(elections_to_process) > 10:
        log(f"  ... and {len(elections_to_process) - 10} more")

    if dry_run:
        # Show pre-filter stats
        total_markets = sum(len(elections[k]['markets']) for k in elections_to_process)
        log(f"\nDRY RUN: Would process {len(elections_to_process)} elections ({total_markets} markets)")
        return 0

    # Process elections
    log(f"\nProcessing {len(elections_to_process)} elections...")

    processed = 0
    found = 0
    vote_shares_updated = 0
    pm_selected = 0
    kalshi_selected = 0
    both_selected = 0
    all_gpt_overrides = []  # Track markets found via GPT web search

    # Phase 1: Prepare all elections (validate, pre-filter, build prompts)
    prepared = []  # list of (key, prompt, pm_markets, kalshi_markets, desc)

    for key in elections_to_process:
        election = elections[key]
        info = election['info']
        all_markets = election['markets']

        year = int(float(info['election_year'])) if pd.notna(info.get('election_year')) else '?'
        office = info.get('office') if pd.notna(info.get('office')) else '?'
        location = info.get('location') if pd.notna(info.get('location')) else '?'
        is_primary = " (Primary)" if str(info.get('is_primary', '')).lower() == 'true' else ""
        desc = f"{year} {office} - {location}{is_primary}"

        # Skip elections with incomplete metadata - require ALL of: country, office, location, year
        # This prevents vote shares from being assigned to elections we can't uniquely identify
        missing_fields = []
        if not info.get('country') or pd.isna(info.get('country')):
            missing_fields.append('country')
        if not info.get('office') or pd.isna(info.get('office')) or office == '?':
            missing_fields.append('office')
        if not info.get('location') or pd.isna(info.get('location')) or location == '?':
            missing_fields.append('location')
        if not info.get('election_year') or pd.isna(info.get('election_year')) or year == '?':
            missing_fields.append('year')

        if missing_fields:
            log(f"  SKIP: {desc} - Incomplete metadata (missing: {', '.join(missing_fields)})")
            selections[key] = {
                "election_found": False,
                "error": f"Incomplete metadata: missing {', '.join(missing_fields)}",
                "skipped_markets": len(all_markets)
            }
            processed += 1
            continue

        # Pre-filter non-winner markets
        filtered = [m for m in all_markets if not NON_WINNER_REGEX.search(m['question'])]

        # Split by platform and sort by volume
        pm_markets = sorted(
            [m for m in filtered if m['platform'] == 'Polymarket'],
            key=lambda x: x['volume_usd'], reverse=True
        )[:MAX_MARKETS_PER_PLATFORM]

        kalshi_markets = sorted(
            [m for m in filtered if m['platform'] == 'Kalshi'],
            key=lambda x: x['volume_usd'], reverse=True
        )[:MAX_MARKETS_PER_PLATFORM]

        prompt = build_prompt(info, pm_markets, kalshi_markets)
        prepared.append((key, prompt, pm_markets, kalshi_markets, desc))

    # Save skipped selections
    save_selections(selections)

    if not prepared:
        log("No elections need GPT processing (all skipped or validated)")
    else:
        # Phase 2: Send all GPT calls in parallel (5 concurrent, rate-limited)
        log(f"\nSending {len(prepared)} GPT calls ({MAX_CONCURRENT_GPT} concurrent)...")
        gpt_results = asyncio.run(run_elections_async(prepared))

        # Phase 3: Process results sequentially (update shared state)
        for key, result, pm_markets, kalshi_markets, desc in gpt_results:
            log(f"\n  {desc}")

            if result is None:
                log(f"    FAILED: No response from GPT")
                selections[key] = {"election_found": False, "error": "API failure"}
                processed += 1
                continue

            if not result.get('election_found', False):
                log(f"    Not found: {result.get('notes', 'no details')}")
                selections[key] = result
                selections[key]['processed_at'] = datetime.now().isoformat()
                processed += 1
                continue

            # Validate GPT selection against volume
            result, was_swapped = validate_selection_volume(result, pm_markets, kalshi_markets)

            # Track any GPT overrides (markets found via web search)
            if 'gpt_overrides' in result:
                for override in result['gpt_overrides']:
                    override['election_key'] = key
                    all_gpt_overrides.append(override)

            # Log results
            found += 1
            winner = result.get('winning_candidate', '?')
            party = result.get('winning_party', '?')
            d_share = result.get('democrat_vote_share')
            r_share = result.get('republican_vote_share')

            if d_share and r_share:
                log(f"    Winner: {winner} ({party}) - D:{d_share}% R:{r_share}%")
            else:
                log(f"    Winner: {winner} ({party})")

            pm_id = result.get('polymarket_winner_market_id')
            k_id = result.get('kalshi_winner_market_id')

            if pm_id:
                pm_selected += 1
            if k_id:
                kalshi_selected += 1
            if pm_id and k_id:
                both_selected += 1

            # Save selection
            result['processed_at'] = datetime.now().isoformat()
            result['election_info'] = {k: str(v) if pd.notna(v) else None for k, v in elections[key]['info'].items()}
            selections[key] = result

            # Update master CSV vote shares
            count = update_master_vote_shares(df, key, result)
            vote_shares_updated += count

            processed += 1

        # Save all selections after processing
        save_selections(selections)

    # Save updated master CSV
    log("\nSaving updated master CSV...")
    BACKUP_DIR.mkdir(exist_ok=True)
    backup_file = BACKUP_DIR / f"master_backup_winner_select_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    original_df = pd.read_csv(MASTER_FILE, low_memory=False)
    original_df.to_csv(backup_file, index=False)
    log(f"  Created backup: {backup_file.name}")

    deleted = rotate_backups("master_backup_winner_select_*.csv")
    if deleted > 0:
        log(f"  Rotated {deleted} old backup(s)")

    df.to_csv(MASTER_FILE, index=False)
    log("  Saved updated master CSV")

    # Enrich selections with additional fields (lat/lng, event slugs, counts)
    log("\nEnriching selections with globe/monitor data...")
    enriched = enrich_selections(df, selections)
    save_selections(selections)
    log(f"  Enriched {enriched} elections with coordinates and market stats")

    # Summary
    print("\n" + "=" * 70)
    print("ELECTION WINNER SELECTION COMPLETE")
    print("=" * 70)
    print(f"Elections processed: {processed}")
    print(f"  Results found: {found}")
    print(f"  Not found / future: {processed - found}")
    print(f"Vote shares updated: {vote_shares_updated:,} markets")
    print(f"Winner markets selected:")
    print(f"  Polymarket: {pm_selected}")
    print(f"  Kalshi: {kalshi_selected}")
    print(f"  Both platforms: {both_selected}")
    print(f"Selections saved to: {SELECTIONS_FILE}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    # Send email notification if GPT found any markets via web search
    if all_gpt_overrides:
        print(f"\n⚠️  GPT OVERRIDES: {len(all_gpt_overrides)} market(s) found via web search")
        log_gpt_overrides(all_gpt_overrides)
        send_gpt_override_email(all_gpt_overrides)
        print("These markets may indicate labeling errors - please review.\n")

    return found


if __name__ == "__main__":
    result = main()
    sys.exit(0 if result >= 0 else 1)
