#!/usr/bin/env python3
"""
Fix Name Collisions

Re-runs markets with ambiguous agent names (MURPHY, POWELL, JOHNSON, etc.)
using a modified prompt that requests FIRST_LAST format disambiguation.
"""

import json
import gzip
import re
import asyncio
from pathlib import Path
from datetime import datetime
from collections import defaultdict

try:
    from openai import AsyncOpenAI
except ImportError:
    print("Error: openai package required. Run: pip install openai")
    exit(1)

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# Paths
from config import DATA_DIR, atomic_write_json

SCRIPT_DIR = Path(__file__).resolve().parent
PROMPTS_DIR = SCRIPT_DIR / "prompts"

# Load base prompt and modify for FIRST_LAST
BASE_PROMPT = (PROMPTS_DIR / "ticker_prompt.md").read_text()

DISAMBIGUATION_ADDENDUM = """

## CRITICAL: Name Disambiguation Required

The markets in this batch have AMBIGUOUS AGENT NAMES that require FIRST_LAST format.

For example:
- "Phil Murphy" → P_MURPHY (not MURPHY)
- "Chris Murphy" → C_MURPHY (not MURPHY)
- "Jerome Powell" → J_POWELL (not POWELL)
- "Joe Biden" → J_BIDEN (not BIDEN)
- "Hunter Biden" → H_BIDEN (not BIDEN)

You MUST use FIRST_LAST format (first initial + underscore + last name) for ALL agents in this batch.
This is required to distinguish between different people with the same last name.
"""

TICKER_PROMPT = BASE_PROMPT + DISAMBIGUATION_ADDENDUM

# Common words that aren't first names (for collision detection)
NOT_NAMES = {'WILL', 'THE', 'OF', 'IN', 'FOR', 'TO', 'BE', 'AND', 'OR', 'NO', 'ANY', 'ONE',
             'NEXT', 'JOIN', 'LEAVE', 'DURING', 'EVIDENCE', 'ODDS', 'INTERVIEW', 'PRESIDENT',
             'SENATE', 'HOUSE', 'LARGEST', 'TEXAS', 'TARGET', 'JANUARY', 'OCTOBER', 'MARCH',
             'MORE', 'YEAR', 'US', 'THAN', 'ELECTED', 'ANOTHER', 'ON', 'NYC', 'FROM', 'OVER',
             'STATES', 'WELSH', 'SCOTTISH', 'PREMIER', 'POLITICIAN', 'CIO'}


def extract_first_name(question: str, agent: str) -> str:
    """Extract first name from question given agent last name."""
    pattern = rf'([A-Z][a-z]+)\s+{agent}'
    match = re.search(pattern, question, re.IGNORECASE)
    if match:
        fn = match.group(1).upper()
        if fn not in NOT_NAMES:
            return fn
    return None


def find_collisions(tickers_data: dict) -> dict:
    """Find all agents with name collisions."""
    by_agent = defaultdict(list)
    for t in tickers_data['tickers']:
        by_agent[t['agent']].append(t)

    collisions = {}
    for agent, markets in by_agent.items():
        first_names = set()
        for m in markets:
            fn = extract_first_name(m['original_question'], agent)
            if fn:
                first_names.add(fn)
        if len(first_names) > 1:
            collisions[agent] = {
                'first_names': sorted(list(first_names)),
                'market_ids': [m['market_id'] for m in markets]
            }

    return collisions


def load_enriched_markets(input_file: Path) -> dict:
    """Load enriched markets indexed by market_id."""
    if str(input_file).endswith(".gz"):
        with gzip.open(input_file, "rt", encoding="utf-8") as f:
            data = json.load(f)
    else:
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f)

    markets = data.get("markets", data)
    indexed = {}
    for m in markets:
        csv = m.get("original_csv", m)
        market_id = csv.get("market_id")
        if market_id:
            indexed[market_id] = m
    return indexed


def pre_extract_fields(market: dict) -> dict:
    """Extract fields from market (same as create_tickers.py)."""
    from create_tickers import extract_timeframe, extract_threshold, extract_round

    csv = market.get("original_csv", market)
    api = market.get("api_data", {})
    mkt = api.get("market", {}) if isinstance(api.get("market"), dict) else {}

    question = str(csv.get("question", ""))
    market_id = str(csv.get("market_id", ""))
    platform = str(csv.get("platform", ""))

    rules = ""
    if mkt:
        rules = mkt.get("rules_primary") or mkt.get("description") or ""
    if not rules:
        rules = csv.get("k_rules_primary", "") or csv.get("pm_description", "")
    rules = str(rules)[:2000]

    combined_text = f"{question} {rules}"

    timeframe = extract_timeframe(question) or extract_timeframe(market_id) or extract_timeframe(rules)
    if not timeframe:
        timeframe = str(datetime.now().year)

    threshold = extract_threshold(combined_text)
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


def create_batch_prompt(markets: list) -> str:
    """Create prompt for GPT-4o batch."""
    lines = ["Assign canonical ticker fields for the following markets:\n"]
    lines.append("**IMPORTANT: Use FIRST_LAST format for ALL agents (e.g., P_MURPHY, J_POWELL)**\n")

    for i, m in enumerate(markets):
        lines.append(f"## Market {i}")
        lines.append(f"**question:** {m['question']}")
        lines.append(f"**rules:** {m['rules']}")
        lines.append(f"**timeframe (pre-filled):** {m['timeframe']}")
        lines.append(f"**threshold (pre-filled):** {m['threshold']}")
        lines.append("")

    lines.append(
        f'{{"tickers": ['
        f'{{"agent": "...", "action": "...", "target": "...", "mechanism": "..."}}, '
        f'... {len(markets)} objects]}}'
    )
    return "\n".join(lines)


def parse_response(text: str, markets: list) -> list:
    """Parse GPT-4o response."""
    parsed = json.loads(text)

    if isinstance(parsed, list):
        results = parsed
    elif isinstance(parsed, dict):
        results = parsed.get("tickers") or parsed.get("extractions") or parsed.get("results") or [parsed]
    else:
        results = [parsed]

    for i, result in enumerate(results):
        if i < len(markets):
            result["market_id"] = markets[i]["market_id"]
            result["platform"] = markets[i]["platform"]
            result["original_question"] = markets[i]["question"]
            result["timeframe"] = markets[i]["timeframe"]
            result["threshold"] = markets[i]["threshold"]
            result["round_suffix"] = markets[i]["round_suffix"]

    return results


async def process_batch_async(
    client,
    markets: list,
    batch_idx: int,
    semaphore: asyncio.Semaphore,
    pbar=None
) -> tuple:
    """Process a batch asynchronously."""
    user_prompt = create_batch_prompt(markets)

    async with semaphore:
        try:
            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": TICKER_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                response_format={"type": "json_object"}
            )

            if pbar:
                pbar.update(1)

            text = response.choices[0].message.content
            results = parse_response(text, markets)
            return (batch_idx, results)

        except Exception as e:
            if pbar:
                pbar.update(1)
            print(f"    ERROR (batch {batch_idx + 1}): {e}")
            return (batch_idx, [])


def postprocess_ticker(ticker: dict) -> dict:
    """Apply post-processing fixes."""
    from create_tickers import is_elected_office, US_STATE_CODES, COUNTRY_CONFLICTS

    question = ticker.get("original_question", "").lower()
    action = ticker.get("action", "")
    target = ticker.get("target", "")
    mechanism = ticker.get("mechanism", "")

    # Fix 1: Election mechanism → CERTIFIED
    if action == "WIN" and is_elected_office(target):
        if mechanism in ["STD", "OFFICIAL_SOURCE", "ANNOUNCED", "PROJECTED"]:
            ticker["mechanism"] = "CERTIFIED"

    # Fix 2: Geographic disambiguation
    if target in US_STATE_CODES and target in COUNTRY_CONFLICTS:
        keywords, replacement = COUNTRY_CONFLICTS[target]
        if any(kw in question for kw in keywords):
            ticker["target"] = replacement

    # Fix 3: Append round suffix to target
    if ticker.get("round_suffix"):
        ticker["target"] = ticker["target"] + ticker["round_suffix"]

    return ticker


def assemble_ticker(t: dict) -> str:
    """Assemble final ticker string."""
    agent = t.get("agent", "UNKNOWN")
    action = t.get("action", "UNKNOWN")
    target = t.get("target", "UNKNOWN")
    mechanism = t.get("mechanism", "STD")
    threshold = t.get("threshold", "ANY")
    timeframe = t.get("timeframe", "UNKNOWN")

    return f"{agent}-{action}-{target}-{mechanism}-{threshold}-{timeframe}"


async def main():
    print("Loading current tickers...")
    with open(DATA_DIR / "tickers.json") as f:
        tickers_data = json.load(f)

    print("Finding name collisions...")
    collisions = find_collisions(tickers_data)
    print(f"Found {len(collisions)} agents with collisions")

    # Get all market IDs that need re-running
    collision_market_ids = set()
    for agent, info in collisions.items():
        collision_market_ids.update(info['market_ids'])

    print(f"Total markets to re-run: {len(collision_market_ids)}")

    # Load enriched markets
    print("Loading enriched markets...")
    enriched = load_enriched_markets(DATA_DIR / "enriched_political_markets.json.gz")

    # Prepare markets for re-run
    print("Preparing markets...")
    prepared = []
    for market_id in collision_market_ids:
        if market_id in enriched:
            fields = pre_extract_fields(enriched[market_id])
            prepared.append(fields)

    print(f"Prepared {len(prepared)} markets for re-run")

    # Batch and process
    batch_size = 25
    batches = [prepared[i:i + batch_size] for i in range(0, len(prepared), batch_size)]
    print(f"Split into {len(batches)} batches")

    client = AsyncOpenAI()
    semaphore = asyncio.Semaphore(10)
    results_by_batch = {}

    pbar = tqdm(total=len(batches), desc="Re-processing", unit="batch") if HAS_TQDM else None

    tasks = [
        process_batch_async(client, batch, i, semaphore, pbar)
        for i, batch in enumerate(batches)
    ]

    for coro in asyncio.as_completed(tasks):
        batch_idx, results = await coro
        results_by_batch[batch_idx] = results

    if pbar:
        pbar.close()

    # Merge results in order
    all_results = []
    for i in sorted(results_by_batch.keys()):
        all_results.extend(results_by_batch[i])

    # Post-process and assemble tickers
    print("Post-processing...")
    fixed_by_id = {}
    for t in all_results:
        if "error" not in t and t.get("agent"):
            postprocess_ticker(t)
            t["ticker"] = assemble_ticker(t)
            fixed_by_id[t["market_id"]] = t

    print(f"Fixed {len(fixed_by_id)} markets")

    # Merge back into original tickers
    print("Merging into tickers.json...")
    updated_count = 0
    for i, t in enumerate(tickers_data['tickers']):
        if t['market_id'] in fixed_by_id:
            fixed = fixed_by_id[t['market_id']]
            tickers_data['tickers'][i] = fixed
            updated_count += 1

    print(f"Updated {updated_count} tickers")

    # Save
    tickers_data['collision_fix_applied'] = datetime.now().isoformat()
    atomic_write_json(DATA_DIR / "tickers.json", tickers_data, indent=2)

    print(f"Saved to {DATA_DIR / 'tickers.json'}")

    # Show sample fixes
    print("\n=== Sample Fixes ===")
    samples = list(fixed_by_id.values())[:10]
    for s in samples:
        print(f"{s['ticker']}")
        print(f"  Q: {s['original_question'][:70]}...")
        print()


if __name__ == "__main__":
    asyncio.run(main())
