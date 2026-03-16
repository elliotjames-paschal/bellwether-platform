#!/usr/bin/env python3
"""
Generate synthetic tickers for equivalents evaluation markets.

Reads the equivalents feedback CSV and creates ticker entries so that
pipeline_apply_human_labels.py can resolve them.

Input:  data/equivalents_eval/equivalents_feedback.csv
        data/enriched_political_markets.json.gz
Output: data/tickers_postprocessed.json (merged with existing)
"""

import sys
import json
import gzip
import csv
import re
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR

FEEDBACK_FILE = DATA_DIR / "equivalents_eval" / "equivalents_feedback.csv"
ENRICHED_FILE = DATA_DIR / "enriched_political_markets.json.gz"
TICKERS_FILE = DATA_DIR / "tickers_postprocessed.json"


def question_to_ticker(market_id: str, question: str, platform: str) -> dict:
    """Generate a plausible BWR ticker from a market question.

    This is a simplified heuristic — not production quality, just enough
    for testing the feedback pipeline.
    """
    q = question.lower().strip()

    # Default fields
    agent = "UNKNOWN"
    action = "OCCUR"
    target = "EVENT"
    mechanism = "STD"
    threshold = "ANY"
    timeframe = "2025"

    # Extract timeframe
    year_match = re.search(r'\b(202[3-9])\b', q)
    if year_match:
        timeframe = year_match.group(1)

    quarter_match = re.search(r'q([1-4])\s*(202[3-9])', q)
    if quarter_match:
        timeframe = f"Q{quarter_match.group(1)}{quarter_match.group(2)}"

    # Presidential election
    if 'president' in q or 'presidential' in q:
        action = "WIN"
        target = "PRES_US"
        mechanism = "CERTIFIED"
        if 'trump' in q:
            agent = "TRUMP"
        elif 'pritzker' in q:
            agent = "PRITZKER"
        elif 'harris' in q:
            agent = "HARRIS"
        elif 'haley' in q:
            agent = "HALEY"
        elif 'which party' in q or 'party will win' in q:
            agent = "PARTY"
        else:
            agent = "CANDIDATE"
    # Deportation
    elif 'deport' in q:
        agent = "TRUMP"
        action = "DEPORT"
        target = "IMMIGRANTS"
        threshold_match = re.search(r'([\d,]+)', q.replace(',', ''))
        if threshold_match:
            threshold = threshold_match.group(1)
    # Cabinet
    elif 'cabinet' in q or 'leave' in q:
        action = "LEAVE"
        target = "CABINET"
        if 'wiles' in q:
            agent = "WILES"
        elif 'vance' in q:
            agent = "VANCE"
        else:
            agent = "OFFICIAL"
    # Putin/Trump meeting
    elif 'putin' in q and ('meet' in q or 'trump' in q):
        agent = "TRUMP_PUTIN"
        action = "MEET"
        target = "SUMMIT"
        if 'china' in q:
            target = "SUMMIT_CHINA"
    # Treasury blockchain
    elif 'treasury' in q and 'blockchain' in q:
        agent = "TREASURY"
        action = "TRANSACT"
        target = "BLOCKCHAIN"
    # Tax
    elif 'tax' in q or 'income tax' in q:
        agent = "TRUMP"
        action = "WAIVE"
        target = "INCOME_TAX"
        if '150k' in q or '150,000' in q:
            threshold = "150K"
    # Brazil unemployment
    elif 'brazil' in q and 'unemployment' in q:
        agent = "BRAZIL"
        action = "BELOW"
        target = "UNEMPLOYMENT"
        pct_match = re.search(r'([\d.]+)%', q)
        if pct_match:
            threshold = pct_match.group(1).replace('.', 'PT')
    # Brazil inflation
    elif 'brazil' in q and 'inflation' in q:
        agent = "BRAZIL"
        action = "BELOW"
        target = "INFLATION"
        pct_match = re.search(r'([\d.]+)%', q)
        if pct_match:
            threshold = pct_match.group(1).replace('.', 'PT')
    # GDP
    elif 'gdp' in q:
        agent = "US"
        action = "GROW"
        target = "GDP"
        if 'negative' in q:
            action = "CONTRACT"
    # Tariff refund
    elif 'tariff' in q and 'refund' in q:
        agent = "COURT"
        action = "ORDER"
        target = "TARIFF_REFUND"
    # Recession
    elif 'recession' in q:
        agent = "US"
        action = "ENTER"
        target = "RECESSION"
        mechanism = "NBER"
    # Fed rates
    elif 'fed' in q and ('rate' in q or 'hike' in q or 'cut' in q):
        agent = "FED"
        if 'hike' in q:
            action = "HIKE"
        elif 'cut' in q:
            action = "CUT"
        else:
            action = "CHANGE"
        target = "RATES"
    # Israel Syria
    elif 'israel' in q and 'syria' in q:
        agent = "ISRAEL_SYRIA"
        action = "NORMALIZE"
        target = "RELATIONS"

    ticker = f"BWR-{agent}-{action}-{target}-{mechanism}-{threshold}-{timeframe}"

    return {
        "market_id": market_id,
        "ticker": ticker,
        "agent": agent,
        "action": action,
        "target": target,
        "mechanism": mechanism,
        "threshold": threshold,
        "timeframe": timeframe,
        "platform": platform,
        "synthetic": True,
    }


def main():
    print("Generating synthetic tickers for equivalents evaluation...")

    if not FEEDBACK_FILE.exists():
        print("  ERROR: Run eval_generate_feedback_csv.py first")
        return

    # Collect all market IDs we need tickers for
    needed_ids = set()
    with open(FEEDBACK_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            markets = json.loads(row["Markets (JSON)"])
            for m in markets:
                needed_ids.add(m["key"])

    print(f"  Need tickers for {len(needed_ids)} market IDs")

    # Load enriched data to get questions
    print("  Loading enriched data...")
    with gzip.open(ENRICHED_FILE, 'rt', encoding='utf-8') as f:
        enriched = json.load(f)

    questions = {}
    platforms = {}
    for m in enriched.get("markets", []):
        csv_data = m.get("original_csv", {})
        mid = csv_data.get("market_id", "")
        if mid in needed_ids:
            questions[mid] = str(csv_data.get("question", ""))
            platforms[mid] = csv_data.get("platform", "Unknown")

    print(f"  Found questions for {len(questions)} / {len(needed_ids)} markets")

    # Load existing tickers
    if TICKERS_FILE.exists():
        with open(TICKERS_FILE, 'r', encoding='utf-8') as f:
            tickers_data = json.load(f)
    else:
        tickers_data = {"schema_version": 1, "tickers": []}

    existing_ids = {t["market_id"] for t in tickers_data.get("tickers", [])}
    print(f"  Existing tickers: {len(existing_ids)}")

    # Generate tickers for each needed market
    new_tickers = []
    for mid in sorted(needed_ids):
        if mid in existing_ids:
            continue
        question = questions.get(mid, mid)
        platform = platforms.get(mid, "Unknown")
        ticker_obj = question_to_ticker(mid, question, platform)
        new_tickers.append(ticker_obj)
        print(f"    {mid:50} -> {ticker_obj['ticker']}")

    print(f"\n  Generated {len(new_tickers)} new tickers")

    # Merge
    tickers_data["tickers"].extend(new_tickers)
    tickers_data["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    with open(TICKERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(tickers_data, f, indent=2)

    print(f"  Wrote {TICKERS_FILE} ({len(tickers_data['tickers'])} total tickers)")


if __name__ == "__main__":
    main()
