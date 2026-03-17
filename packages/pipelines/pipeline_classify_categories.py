#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Classify New Markets into 15 Political Categories
================================================================================

Part of the NEW Bellwether Pipeline (January 2026+)

This script:
1. Reads new markets from pipeline_discover_markets.py output
2. Skips markets already labeled as "1. ELECTORAL" (auto-labeled from tags)
3. Uses 3-STAGE GPT-4o classification (same as winner market classifier):
   - Stage 1: Batch classification (50/call) for high recall
   - Stage 2: Verification of political markets for high precision
   - Stage 3: Tiebreaker for disagreements (majority vote)
4. Marks uncertain classifications as NEEDS_REVIEW
5. Updates the new_markets file with classifications

Usage:
    python pipeline_classify_categories.py

Input:
    - data/new_markets_discovered.csv (from pipeline_discover_markets.py)

Output:
    - data/new_markets_classified.csv (with political_category filled in)

================================================================================
"""

import pandas as pd
import json
import re
import sys
import time
import os
from datetime import datetime
from pathlib import Path
# =============================================================================
# CONFIGURATION
# =============================================================================

from config import DATA_DIR, get_openai_client

# Input/Output files
INPUT_FILE = DATA_DIR / "new_markets_discovered.csv"
OUTPUT_FILE = DATA_DIR / "new_markets_classified.csv"
CHECKPOINT_FILE = DATA_DIR / "pipeline_classify_categories_checkpoint.json"

# OpenAI Configuration
BATCH_SIZE = 50
DEFAULT_MODEL = "gpt-4o-mini"
TEMPERATURE = 0

# Allow --model override: python pipeline_classify_categories.py --model gpt-4o
MODEL = DEFAULT_MODEL
if "--model" in sys.argv:
    idx = sys.argv.index("--model")
    if idx + 1 < len(sys.argv):
        MODEL = sys.argv[idx + 1]

# Political categories (16 types - includes NOT_POLITICAL for filtering)
POLITICAL_CATEGORIES = [
    "1. ELECTORAL",
    "2. MONETARY_POLICY",
    "3. LEGISLATIVE",
    "4. APPOINTMENTS",
    "5. REGULATORY",
    "6. INTERNATIONAL",
    "7. JUDICIAL",
    "8. MILITARY_SECURITY",
    "9. CRISIS_EMERGENCY",
    "10. GOVERNMENT_OPERATIONS",
    "11. PARTY_POLITICS",
    "12. STATE_LOCAL",
    "13. TIMING_EVENTS",
    "14. POLLING_APPROVAL",
    "15. POLITICAL_SPEECH",
    "16. NOT_POLITICAL"
]

# Categories as short codes for validation (includes NOT_POLITICAL)
VALID_CATEGORIES = {f"{i}." for i in range(1, 17)}


def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# =============================================================================
# PRE-FILTER: Remove obvious non-political markets BEFORE GPT classification
# =============================================================================
# This saves API costs and prevents contamination in the dataset

# Patterns that indicate NON-POLITICAL content (will be filtered out)
NON_POLITICAL_PATTERNS = [
    # === SPORTS ===
    # Major leagues and championships
    (r'\b(NFL|NBA|NHL|MLB|MLS|UFC|WWE|F1|NASCAR|PGA|ATP|WTA)\b', 'Sports league'),
    (r'\b(Super Bowl|World Series|Stanley Cup|NBA Finals|World Cup|Champions League)\b', 'Sports championship'),
    (r'\b(Premier League|La Liga|Serie A|Bundesliga|Ligue 1|Eredivisie|MLS Cup)\b', 'Soccer league'),
    (r'\b(win|won)\s+the\s+\d{4}\s+(World Series|Super Bowl|Stanley Cup|NBA Finals)\b', 'Sports championship'),
    (r'\bWorld Baseball Classic\b', 'Sports championship'),
    (r'\b(EPL|Premier League)\b.*\b(standings|finish|relegated)\b', 'Soccer standings'),
    (r'\b(Masters tournament|PGA Championship|US Open golf|Open Championship|Ryder Cup)\b', 'Golf tournament'),
    (r'\bAll[ -]Star\b.*\b(selected|game|selection)\b', 'Sports All-Star'),

    # Sports betting terms
    (r'\bpoint spread\b', 'Sports betting'),
    (r'\bover/under\b', 'Sports betting'),
    (r'\bmoneyline\b', 'Sports betting'),
    (r'\btop\s+(\w+\s+)?goal\s*scorer\b', 'Sports stats'),
    (r'\btop\s+scorer\b', 'Sports stats'),
    (r'\b(touchdown|home run|goal|assist)s?\s+record\b', 'Sports stats'),
    (r'\bMVP\s+(award|winner|vote)\b', 'Sports award'),
    (r'\bHall of Fame\b', 'Sports'),
    (r'\bNaismith\b', 'College basketball award'),
    (r'\bHeisman\b', 'College football award'),
    (r'\b(Freshman|Player) of the Year\b', 'Sports award'),
    (r'\bAll-American\b', 'Sports award'),
    (r'\b(be traded|traded by).*season\b', 'Sports trade'),
    (r'\b(not be traded|be traded)\?', 'Sports trade'),  # Catches "Will X not be traded?" or "Will X be traded?"
    (r'\bvs\b.*Winner\?', 'Sports matchup'),
    (r'\bnext\b.*\bHead Coach\b', 'Sports coaching'),
    (r'\b(Raiders|Chiefs|Cowboys|Eagles|Packers|Bears|Lions|49ers|Patriots|Jets|Dolphins|Bills|Broncos|Chargers|Bengals|Browns|Steelers|Ravens|Titans|Colts|Texans|Jaguars|Saints|Buccaneers|Falcons|Panthers|Seahawks|Rams|Cardinals|Giants|Commanders|Vikings)\b.*\b(coach|trade|sign|draft)\b', 'NFL team'),
    (r'\btraded to the (Jets|Raiders|Chiefs|Cowboys|Eagles|Packers|Bears|Lions|49ers|Patriots)\b', 'Sports trade'),

    # Olympics (medal predictions, not policy)
    (r'\bwin the (most|second|third)\s+(most\s+)?(gold\s+)?medals?\b', 'Olympics sports'),
    (r'\b(gold|silver|bronze)\s+medal\s+at\s+the\b', 'Olympics sports'),
    (r'\bmedal points\b', 'Olympics sports'),
    (r'\bIce Hockey\s+(gold|silver|bronze)\b', 'Olympics sports'),

    # Academic/Other awards
    (r'\bFields Medal\b', 'Academic award'),
    (r'\bFIDE\b', 'Chess'),

    # === ESPORTS ===
    (r'\b(LCS|LPL|LEC|VCT|CDL|PGL|ESL|IEM|Worlds|Major)\b.*\b(win|qualify|champion)\b', 'Esports'),
    (r'\b(esports|e-sports)\b', 'Esports'),
    (r'\b(CS2|CSGO|CS:GO|Valorant|League of Legends|Dota|Dota2|Fortnite|Apex Legends)\b', 'Esports game'),
    (r'\b(Fnatic|Cloud9|T1|G2|Team Liquid|NRG|Sentinels|100 Thieves|OpTic|FaZe|Evil Geniuses)\b', 'Esports team'),
    (r'\bwin\s+(the\s+)?(LCS|LPL|LEC|VCT|CDL|PGL|ESL|IEM)\b', 'Esports league'),
    (r'\bqualify for (Worlds|playoffs|the Major)\b', 'Esports'),

    # === CRYPTO/STOCK PRICES ===
    (r'\b(Bitcoin|BTC|Ethereum|ETH|Solana|SOL|XRP|Dogecoin|DOGE)\b.*\b(price|above|below|hit|reach)\s*\$', 'Crypto price'),
    (r'\$\d+k\b.*\b(Bitcoin|BTC|ETH)\b', 'Crypto price'),
    (r'\b(Bitcoin|BTC)\s*(dominance|volatility)\b', 'Crypto market'),
    (r'\b(Bitcoin|Ethereum|BTC|ETH)\b.*\b(daily candle|candle change)\b', 'Crypto price'),
    (r'\bETF Flows\b', 'Crypto ETF'),
    (r'\bGwei\b', 'Crypto technical'),
    (r'\b(AAPL|TSLA|NVDA|GOOGL|MSFT|AMZN|META|NFLX|AMD)\b.*\b(stock|price|above|below)\b', 'Stock price'),
    (r'\$[A-Z]{1,5}\s+(stock|share|price)\s+(above|below|hit)', 'Stock price'),
    (r'\bS&P 500\b.*\b(hit \$|LOW|HIGH|percentage change)\b', 'Stock price'),
    (r'\b(Magnificent 7|top performing)\b.*\bcompany\b', 'Stock performance'),
    (r'\btrillionaire\b', 'Wealth ranking'),

    # === COMMODITIES ===
    (r'\b(Gold|Silver|Copper|Crude Oil|Natural Gas|Platinum|Palladium)\s*\([A-Z]{1,2}\)\s*settle', 'Commodity price'),
    (r'\b(WTI|Brent)\s*(crude|oil)?\s*(price|above|below|settle)', 'Commodity price'),

    # === ENTERTAINMENT/AWARDS ===
    (r'\b(Grammy|Oscar|Emmy|BAFTA|Golden Globe|Tony Award|SAG Award)\b', 'Entertainment award'),
    (r'\b(Best Picture|Best Actor|Best Actress|Best Director|Best Film)\b.*\b(award|win|nomination)\b', 'Entertainment award'),
    (r'\bbox office\b', 'Movies'),
    (r'\btop grossing (movie|film)\b', 'Movies'),
    (r'\bBillboard\s+(Hot|100|200|chart)\b', 'Music charts'),
    (r'\brelease an? (new\s+)?(album|single)\b', 'Music'),
    (r'\b(tour|concert)\s+(announce|dates|start)\b', 'Music'),

    # === REALITY TV ===
    (r'\bBig Brother\b(?!.*\b(China|surveillance|Orwell)\b)', 'Reality TV'),
    (r'\bLove Island\b', 'Reality TV'),
    (r'\bBachelor(ette)?\b', 'Reality TV'),
    (r'\bSurvivor\b.*\b(winner|win|voted|eliminated|immunity)\b', 'Reality TV'),

    # === TECH PRODUCTS/VALUATIONS (not policy) ===
    (r'\$\d+[BT]\+?\s*valuation', 'Tech valuation'),
    (r'\b(OpenAI|Anthropic|xAI)\b.*\$\d+[BT]', 'Tech valuation'),
    (r'\b(iPhone|iPad|Galaxy|Pixel)\s*\d+\s*(release|launch|announce)', 'Tech product'),
    (r'\bbest AI model\b', 'AI ranking'),
    (r'\b(second|third)-?best AI\b', 'AI ranking'),
    (r'\b(FrontierMath|Humanity\'s Last Exam)\b', 'AI benchmark'),
    (r'\b(GPT|Gemini|Claude)\b.*\b(score|benchmark)\b', 'AI benchmark'),

    # === SOCIAL MEDIA METRICS ===
    (r'\b(hit|reach|pass)\s*\d+\s*(Million|M|K)?\s*(subscribers|followers)\b', 'Social media metrics'),
    (r'\bsubscriber count\b', 'Social media metrics'),
    (r'\bmillion (subscribers|followers)\b', 'Social media metrics'),

    # === CELEBRITY GOSSIP ===
    (r'\b(pregnant|pregnancy|engaged|married|divorce|dating|baby)\b.*\b(202[4-9]|203\d)\b', 'Celebrity gossip'),
    (r'\bconfirmed pregnant\b', 'Celebrity gossip'),

    # === WEATHER (not climate policy) ===
    (r'\binches of (rain|snow)\b', 'Weather'),
    (r'\b(temperature|rainfall|snowfall)\s+(in|on|for|at)\s+[A-Z]', 'Weather'),

    # === VIDEO GAMES (not esports) ===
    (r'\bGTA\s*(VI|6|VII|7)\b(?!.*\b(before|ceasefire|invasion|war|president|Trump)\b)', 'Video game'),
    (r'\bvideo game\s+(release|sales|launch)\b', 'Video game'),

    # === COLLECTIBLES/TRADING ===
    (r'\b(Pokemon|Pokémon)\s+card\b', 'Collectibles'),
    (r'\bCharizard\b', 'Collectibles'),
    (r'\btrading card\b.*\b(price|sale|value)\b', 'Collectibles'),
    (r'\b(Pop Mart|Labubu)\b', 'Collectibles'),

    # === TWEET COUNTS (non-political social media metrics) ===
    (r'\b(Elon|Musk)\b.*\btweet', 'Tweet counts'),
    (r'\btweet\s+\d+.*times\b', 'Tweet counts'),
    (r'\b\d+.*times.*tweet\b', 'Tweet counts'),

    # === BUSINESS METRICS (not regulatory) ===
    (r'\bbeat quarterly earnings\b', 'Business metrics'),
    (r'\b(revenue|earnings)\s+(beat|miss|exceed)\b', 'Business metrics'),
    (r'\bquarterly (revenue|earnings|results)\b', 'Business metrics'),
    (r'\bTSA\s*(passengers?|checkpoint)\b', 'TSA metrics'),

    # === ENTERTAINMENT EVENTS ===
    (r'\bEurovision\b', 'Eurovision'),
    (r'\bBallon d.Or\b', 'Sports award'),
    (r'\bTiny Desk\b', 'Entertainment'),
    (r'\bNPR Music\b', 'Entertainment'),
    (r'\bGoogle.*Year in Search\b', 'Entertainment'),
    (r'\bYear in Search.*Google\b', 'Entertainment'),
    (r'\bDebate Bingo\b', 'Entertainment'),

    # === REAL ESTATE ===
    (r'\b(median home value|rent grow|housing price)\b', 'Real estate'),

    # === FASHION ===
    (r'\bcreative director\b.*\b(Versace|Gucci|Prada|Chanel|Dior)\b', 'Fashion'),

    # === APP RANKINGS ===
    (r'\b#1 (Free )?App\b', 'App ranking'),

    # === YOUTUBE/INFLUENCER ===
    (r'\bMrBeast\b', 'Influencer'),
    (r'\b(views|subscribers)\s+on\s+(a\s+)?(YouTube|MrBeast)\b', 'Social media metrics'),
    (r'\b\d+m\s+views\b', 'Social media metrics'),

    # === CELEBRITY GOSSIP (Epstein files celebrity mentions) ===
    (r'\b(named|mentioned)\s+in\s+(the\s+)?(newly\s+)?released\s+Epstein\s+files?\b', 'Celebrity gossip'),
    (r'\bEpstein\s+files?\b(?!.*(Trump|Clinton|DOJ|FBI|judge|court|congress))', 'Celebrity gossip'),
    (r'\b(unfollow|dating|relationship)\b.*\b(Grimes|Brady|Belichick)\b', 'Celebrity gossip'),

    # === NATURAL DISASTERS (not policy response) ===
    (r'\bearthquakes?\b.*\bmagnitude\b', 'Natural disaster'),
    (r'\b(volcanic|VEI)\b.*\beruption', 'Natural disaster'),
    (r'\btornadoes?\b.*\boccur\b', 'Natural disaster'),

    # === SOCIAL/DEMOGRAPHIC TRENDS ===
    (r'\brank\b.*\bnames\b.*\bSSA\b', 'Baby names'),
    (r'\bCanada.s?\b.*\bpopulation\b', 'Demographics'),
    (r'\begg prices\b', 'Commodity prices'),

    # === TECH INCIDENTS ===
    (r'\b(Discord|Cloudflare)\b.*\b(incident|outage)\b', 'Tech incident'),

    # === CRYPTO (non-regulatory) ===
    (r'\b(Bitcoin|ETH|Ethereum)\b.*\b(ATH|all.time.high)\b', 'Crypto price'),
    (r'\bcrypto\b.*\bliquidation\b', 'Crypto market'),

    # === BUSINESS (non-regulatory) ===
    (r'\bOpenAI\b.*\bacquired\b', 'Tech acquisition'),
    (r'\bbuy\b.*\bsports team\b', 'Sports business'),
    (r'\bS&P 500\b.*\bgain\b.*\bday\b', 'Stock market'),
    (r'\bsay\b.*\bduring\b.*\bearnings call\b', 'Corporate earnings call'),

    # === EDUCATION (non-political) ===
    (r'\b(university|commencement|graduation)\b.*\bcancel', 'Education'),

    # === MUSIC/ENTERTAINMENT ===
    (r'\brelease\b.*\b(song|album)\b.*\b202[5-9]\b', 'Music release'),

    # === SPORTS MATCHUPS ===
    (r'\bat\b.*\bWinner\?\s*$', 'Sports matchup'),

    # === FANTASY SPORTS ===
    (r'^yes [A-Z][a-z]+ [A-Z][a-z]+,yes', 'Fantasy sports lineup'),

    # === MUSIC INDUSTRY ===
    (r'\bDJ Mag\b.*\bTop\s*\d+\b', 'DJ rankings'),
    (r'\b(Coachella|Lollapalooza|Glastonbury|Bonnaroo)\b.*\bHeadliner\b', 'Music festival'),
    (r'\b#1\s+(album|song|hit|single)\b', 'Music charts'),
    (r'\btop\s+\d+\s+(song|album|hit)\b', 'Music charts'),
    (r'\bBillboard\b.*\b(Hot 100|chart|#1)\b', 'Music charts'),

    # === REALITY TV ===
    (r'\bSingle.s Inferno\b', 'Reality TV'),
    (r'\bLove Island\b.*\b(winner|couple|finale)\b', 'Reality TV'),
    (r'\b(Bachelor|Bachelorette)\b.*\b(winner|finale|rose)\b', 'Reality TV'),

    # === CELEBRITY PERSONAL ===
    (r'\b(separate|divorce|split)\b.*\b202[5-9]\b', 'Celebrity gossip'),
    (r'\bbar exam\b', 'Personal achievement'),
]

# Keywords that indicate POLITICAL content (protect from filtering)
# Note: "bill" removed to avoid matching "Bill Gates" - legislation context covered by "legislation"
POLITICAL_PROTECT_KEYWORDS = re.compile(
    r'\b(Trump|Biden|Obama|Clinton|election|congress|senate|parliament|legislation|'
    r'law|regulation|tariff|sanction|treaty|vote|governor|mayor|president|'
    r'minister|policy|government|federal|DOJ|FBI|CIA|NATO|UN|EU|White House|'
    r'Capitol|Supreme Court|executive order|cabinet|nomination|impeach|'
    r'Republican|Democrat|GOP|DNC|RNC|primary|caucus|ballot|referendum|'
    r'attend|speech|address|testimony|hearing|committee|investigation|'
    r'ban|restriction|mandate|stimulus|deficit|debt ceiling|shutdown|'
    r'ambassador|diplomat|summit|ceasefire|invasion|military|defense|'
    r'immigration|border|asylum|deportation|visa|citizenship)\b',
    re.IGNORECASE
)


def pre_filter_non_political(df, show_progress=True):
    """
    Filter out obvious non-political markets BEFORE sending to GPT.

    This catches sports, esports, crypto prices, entertainment, etc.
    that would be classified as NOT_POLITICAL anyway - saving API costs.

    Returns:
        (df_political, df_filtered) - markets to classify, markets removed
    """
    if show_progress:
        log("\n" + "=" * 50)
        log("PRE-FILTER: Removing obvious non-political markets")
        log("=" * 50)

    filtered_indices = set()
    filter_reasons = {}

    for idx, row in df.iterrows():
        question = row.get('question', '')
        if pd.isna(question):
            continue

        # Check if protected by political keywords
        if POLITICAL_PROTECT_KEYWORDS.search(question):
            continue

        # Check against non-political patterns
        for pattern, reason in NON_POLITICAL_PATTERNS:
            try:
                if re.search(pattern, question, re.IGNORECASE):
                    filtered_indices.add(idx)
                    filter_reasons[idx] = reason
                    break  # One match is enough
            except re.error:
                continue

    # Split dataframe
    df_filtered = df.loc[list(filtered_indices)].copy()
    df_filtered['filter_reason'] = df_filtered.index.map(filter_reasons)
    df_political = df.drop(list(filtered_indices))

    if show_progress:
        log(f"  Markets checked: {len(df):,}")
        log(f"  Pre-filtered (non-political): {len(df_filtered):,}")
        log(f"  Remaining for GPT classification: {len(df_political):,}")

        if len(df_filtered) > 0:
            # Show breakdown by reason
            reason_counts = df_filtered['filter_reason'].value_counts()
            log("\n  Pre-filtered by category:")
            for reason, count in reason_counts.head(10).items():
                log(f"    {reason}: {count}")

            # Show sample of filtered markets
            log("\n  Sample filtered markets:")
            for _, row in df_filtered.head(5).iterrows():
                log(f"    [{row['filter_reason']}] {row['question'][:70]}...")

    return df_political, df_filtered


# =============================================================================
# STAGE 1: BATCH CLASSIFICATION (HIGH RECALL)
# =============================================================================

STAGE1_SYSTEM_PROMPT = """You are an expert political scientist categorizing prediction markets.
Your task is to classify each market into one of 16 categories.

⚠️ CRITICAL: ELECTORAL (1.) is the MOST IMPORTANT category! ⚠️
Any market about WHO WINS an election, election outcomes, candidate performance,
vote shares, or electoral results should be ELECTORAL - regardless of office level.
This includes: Presidential, Congressional, Senate, House, Governor, Lt. Governor,
Attorney General, Secretary of State, Mayor, City Council, State Legislature,
School Board, Judge elections, DA elections, ANY elected office at ANY level.

DO NOT use STATE_LOCAL (12.) for elections! STATE_LOCAL is for non-election
state/local governance matters (e.g., state laws, local ordinances, municipal decisions).

⚠️ Use NOT_POLITICAL (16.) for non-political markets like sports, esports,
entertainment, crypto prices, weather, or celebrity gossip.

Categories:
1. ELECTORAL - Elections at ALL levels (federal, state, local, international).
   WHO WINS elections, vote shares, candidate performance, election outcomes.
   Examples: "Will X win governor?", "Who wins mayor of Y?", "Will Z win state senate?"
2. MONETARY_POLICY - Fed decisions, interest rates, inflation, central bank
3. LEGISLATIVE - Congressional actions, bills, votes, legislation
4. APPOINTMENTS - Government nominations, confirmations, cabinet picks
5. REGULATORY - Agency decisions (SEC, FDA, EPA), regulatory approvals
6. INTERNATIONAL - Foreign policy, sanctions, trade, diplomacy, treaties
7. JUDICIAL - Court decisions, legal rulings, Supreme Court cases
8. MILITARY_SECURITY - Military actions, defense, conflicts, cybersecurity
9. CRISIS_EMERGENCY - Disaster response, emergencies, pandemic response
10. GOVERNMENT_OPERATIONS - Budget, shutdowns, debt ceiling, contracts
11. PARTY_POLITICS - Internal party decisions, leadership, scandals (not elections)
12. STATE_LOCAL - State/local NON-ELECTION matters only (laws, ordinances, policies)
13. TIMING_EVENTS - Political timing, announcement scheduling
14. POLLING_APPROVAL - Opinion polls, approval ratings, public opinion
15. POLITICAL_SPEECH - What politicians will say, speech content
16. NOT_POLITICAL - Sports, esports, entertainment, crypto prices, non-political topics

Return JSON: {"results": [{"index": 0, "category": "5.", "confidence": 0.9}, ...]}
Use category numbers only (e.g., "1.", "2.", "16.", etc.)."""


def _parallel_gpt_batches(client, batches_with_args, max_workers=5):
    """Run GPT batch calls in parallel. Each item: (callable, args) -> list of results."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    all_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fn, *args): i for i, (fn, args) in enumerate(batches_with_args)}
        for future in as_completed(futures):
            all_results.extend(future.result())
    return all_results


def stage1_batch(client, questions, batch_size=50, show_progress=True):
    """Stage 1: Batch classify for high recall."""
    total = len(questions)

    if show_progress:
        log(f"  Stage 1: Classifying {total} markets (batch={batch_size}, parallel)...")

    def classify_one_batch(start, batch):
        prompt = "Classify these markets:\n" + "\n".join(
            f"{i}. \"{q}\"" for i, q in enumerate(batch)
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
            batch_results = []
            for r in parsed.get("results", []):
                cat = r.get("category", "NEEDS_REVIEW")
                if not any(cat.startswith(v) for v in VALID_CATEGORIES):
                    cat = "NEEDS_REVIEW"
                batch_results.append({
                    "index": start + r.get("index", 0),
                    "category": cat,
                    "confidence": float(r.get("confidence", 0.5)),
                    "stage": 1
                })
            return batch_results
        except Exception as e:
            log(f"    Batch error: {e}")
            return [{
                "index": start + i,
                "category": "NEEDS_REVIEW",
                "confidence": 0,
                "stage": 1,
                "error": str(e)
            } for i in range(len(batch))]

    batches = []
    for start in range(0, total, batch_size):
        batch = questions[start:start + batch_size]
        batches.append((classify_one_batch, (start, batch)))

    results = _parallel_gpt_batches(client, batches, max_workers=5)
    results.sort(key=lambda x: x["index"])

    if show_progress:
        log(f"  Stage 1 done: {total} markets classified")

    return results


# =============================================================================
# STAGE 2: BATCH VERIFICATION (HIGH PRECISION)
# =============================================================================

STAGE2_SYSTEM_PROMPT = """VERIFICATION MODE - Confirm the best category for each market.

⚠️ CRITICAL: ELECTORAL (1.) is the MOST IMPORTANT category! ⚠️
Any market about WHO WINS an election at ANY level should be ELECTORAL.
This includes mayoral, gubernatorial, state legislature, city council, judge elections, etc.
DO NOT use STATE_LOCAL (12.) for elections - that's for non-election state/local matters.

⚠️ Use NOT_POLITICAL (16.) for sports, esports, entertainment, crypto prices, etc.

Categories (use number only):
1. ELECTORAL - Elections at ALL levels (who wins, vote shares, election outcomes)
2. MONETARY_POLICY - Fed, interest rates, inflation
3. LEGISLATIVE - Congress, bills, legislation
4. APPOINTMENTS - Nominations, confirmations
5. REGULATORY - SEC, FDA, EPA decisions
6. INTERNATIONAL - Foreign policy, trade, diplomacy
7. JUDICIAL - Courts, legal rulings
8. MILITARY_SECURITY - Military, defense, conflicts
9. CRISIS_EMERGENCY - Disasters, emergencies
10. GOVERNMENT_OPERATIONS - Budget, shutdowns
11. PARTY_POLITICS - Party leadership, scandals (not elections)
12. STATE_LOCAL - State/local NON-ELECTION matters only
13. TIMING_EVENTS - Political timing
14. POLLING_APPROVAL - Polls, approval ratings
15. POLITICAL_SPEECH - What politicians say
16. NOT_POLITICAL - Sports, esports, entertainment, crypto, non-political

Return JSON: {"results": [{"index": 0, "category": "5.", "confidence": 0.9}, ...]}
Use category numbers only (e.g., "1.", "2.", "16.", etc.)."""


def stage2_verify(client, questions, stage1_results, batch_size=50, show_progress=True):
    """Stage 2: Batch verify classifications (same batch size as Stage 1)."""
    all_idx = [r["index"] for r in stage1_results]
    total = len(all_idx)

    if show_progress:
        log(f"  Stage 2: Verifying {total} markets (batch={batch_size}, parallel)...")

    def verify_one_batch(start, batch_indices):
        batch_questions = [questions[idx] for idx in batch_indices]
        prompt = "Verify categories for these markets:\n" + "\n".join(
            f"{i}. \"{q}\"" for i, q in enumerate(batch_questions)
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
            batch_results = []
            for r in parsed.get("results", []):
                local_idx = r.get("index", 0)
                cat = r.get("category", "NEEDS_REVIEW")
                if not any(cat.startswith(v) for v in VALID_CATEGORIES):
                    cat = "NEEDS_REVIEW"
                if local_idx < len(batch_indices):
                    batch_results.append({
                        "index": batch_indices[local_idx],
                        "category": cat,
                        "confidence": float(r.get("confidence", 0.5)),
                        "stage": 2
                    })
            return batch_results
        except Exception as e:
            log(f"    Batch error: {e}")
            return [{
                "index": idx,
                "category": "NEEDS_REVIEW",
                "confidence": 0,
                "stage": 2,
                "error": str(e)
            } for idx in batch_indices]

    batches = []
    for start in range(0, total, batch_size):
        batch_indices = all_idx[start:start + batch_size]
        batches.append((verify_one_batch, (start, batch_indices)))

    results = _parallel_gpt_batches(client, batches, max_workers=5)

    if show_progress:
        log(f"  Stage 2 done: {len(results)} markets verified")

    return results


# =============================================================================
# STAGE 3: BATCH TIEBREAKER
# =============================================================================

STAGE3_SYSTEM_PROMPT = """TIEBREAKER - Final classification decision for each market.

⚠️ CRITICAL: ELECTORAL (1.) is the MOST IMPORTANT category! ⚠️
Any market about WHO WINS an election at ANY level should be ELECTORAL.
Mayor, Governor, State Senate, City Council, Judge, DA - ALL are ELECTORAL.
STATE_LOCAL (12.) is ONLY for non-election state/local matters.

⚠️ Use NOT_POLITICAL (16.) for sports, esports, entertainment, crypto prices, etc.

Categories: 1=Elections (ALL levels), 2=Fed/Monetary, 3=Congress/Laws, 4=Appointments,
5=Regulatory, 6=Foreign Policy, 7=Courts, 8=Military, 9=Crisis,
10=Government Ops, 11=Party Politics, 12=State/Local (non-elections only), 13=Timing,
14=Polls, 15=Political Speech, 16=NOT_POLITICAL (sports/esports/entertainment/crypto)

Return JSON: {"results": [{"index": 0, "category": "5.", "confidence": 0.9}, ...]}"""


def stage3_batch_tiebreak(client, disagreements, batch_size=50, show_progress=True):
    """Stage 3: Batch tiebreaker for disagreements."""
    total = len(disagreements)
    if total == 0:
        return []

    if show_progress:
        log(f"  Stage 3: {total} tiebreakers (batch={batch_size}, parallel)...")

    def tiebreak_one_batch(start, batch):
        prompt = "Tiebreaker for these markets:\n" + "\n".join(
            f'{i}. "{d["question"]}"' for i, d in enumerate(batch)
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
            batch_results = []
            for r in parsed.get("results", []):
                local_idx = r.get("index", 0)
                cat = r.get("category", "NEEDS_REVIEW")
                if not any(cat.startswith(v) for v in VALID_CATEGORIES):
                    cat = "NEEDS_REVIEW"
                if local_idx < len(batch):
                    batch_results.append({
                        "disagreement_idx": start + local_idx,
                        "category": cat,
                        "confidence": float(r.get("confidence", 0.5)),
                        "stage": 3
                    })
            return batch_results
        except Exception as e:
            log(f"    Batch error: {e}")
            return [{
                "disagreement_idx": start + i,
                "category": "NEEDS_REVIEW",
                "confidence": 0,
                "stage": 3,
                "error": str(e)
            } for i in range(len(batch))]

    batches = []
    for start in range(0, total, batch_size):
        batch = disagreements[start:start + batch_size]
        batches.append((tiebreak_one_batch, (start, batch)))

    results = _parallel_gpt_batches(client, batches, max_workers=5)

    if show_progress:
        log(f"  Stage 3 done: {len(results)} tiebreakers resolved")

    return results


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def expand_category(short_cat):
    """Expand short category (e.g., '5.') to full name (e.g., '5. REGULATORY')."""
    if short_cat == "NEEDS_REVIEW":
        return "NEEDS_REVIEW"

    for full_cat in POLITICAL_CATEGORIES:
        if full_cat.startswith(short_cat):
            return full_cat

    return "NEEDS_REVIEW"  # If can't expand, flag for review


# Regex patterns for detecting electoral markets misclassified as STATE_LOCAL
_ELECTION_KEYWORDS = re.compile(
    r'\b(win|winner|wins|elect|election)\b', re.IGNORECASE
)
_OFFICE_KEYWORDS = re.compile(
    r'\b(governor|mayor|senate|senator|house|congress|congressional|'
    r'attorney\s+general|secretary\s+of\s+state|council|legislature|'
    r'judge|district\s+attorney|DA|alderman|comptroller|'
    r'lt\.?\s*governor|lieutenant\s+governor)\b', re.IGNORECASE
)


def validate_electoral_classification(results, questions, show_progress=True):
    """
    Post-classification safety net: catch electoral markets misclassified as STATE_LOCAL.

    If a market classified as 12. STATE_LOCAL has both election keywords (win/elect)
    and office keywords (governor/mayor/senate/etc.), reclassify as 1. ELECTORAL.
    """
    reclassified = 0
    for r in results:
        if r["category"] != "12.":
            continue
        q = questions[r["index"]]
        if _ELECTION_KEYWORDS.search(q) and _OFFICE_KEYWORDS.search(q):
            if show_progress:
                log(f"    RECLASSIFY STATE_LOCAL -> ELECTORAL: \"{q[:80]}...\"")
            r["category"] = "1."
            reclassified += 1

    if show_progress and reclassified > 0:
        log(f"  Reclassified {reclassified} STATE_LOCAL -> ELECTORAL")

    return results


def run_classification_pipeline(client, questions, show_progress=True):
    """Run full 3-stage classification pipeline (all stages batched)."""
    from collections import Counter

    if show_progress:
        log(f"\n{'='*50}")
        log(f"CLASSIFYING {len(questions)} MARKETS (3-stage batched)")
        log(f"{'='*50}")

    # Stage 1: Batch classify
    s1_results = stage1_batch(client, questions, BATCH_SIZE, show_progress)
    s1_lookup = {r["index"]: r for r in s1_results}

    # Stage 2: Batch verify
    s2_results = stage2_verify(client, questions, s1_results, BATCH_SIZE, show_progress)
    s2_lookup = {r["index"]: r for r in s2_results}

    # Combine results
    final_results = []
    disagreements = []

    for idx in range(len(questions)):
        s1 = s1_lookup.get(idx, {})
        s2 = s2_lookup.get(idx)

        s1_cat = s1.get("category", "NEEDS_REVIEW")
        s2_cat = s2.get("category", "NEEDS_REVIEW") if s2 else s1_cat

        # Filter out NEEDS_REVIEW when comparing
        valid_s1 = s1_cat if s1_cat != "NEEDS_REVIEW" else None
        valid_s2 = s2_cat if s2_cat != "NEEDS_REVIEW" else None

        if valid_s1 and valid_s2 and valid_s1 == valid_s2:
            # Agreement on valid category -> accept
            final_results.append({
                "index": idx,
                "category": valid_s2,
                "confidence": s2.get("confidence", 0.5) if s2 else s1.get("confidence", 0.5),
                "s1": s1_cat,
                "s2": s2_cat,
                "s3": None,
                "votes": 2
            })
        elif valid_s1 and not valid_s2:
            # Only s1 has valid category
            final_results.append({
                "index": idx,
                "category": valid_s1,
                "confidence": s1.get("confidence", 0.5),
                "s1": s1_cat,
                "s2": s2_cat,
                "s3": None,
                "votes": 1
            })
        elif valid_s2 and not valid_s1:
            # Only s2 has valid category
            final_results.append({
                "index": idx,
                "category": valid_s2,
                "confidence": s2.get("confidence", 0.5),
                "s1": s1_cat,
                "s2": s2_cat,
                "s3": None,
                "votes": 1
            })
        elif not valid_s1 and not valid_s2:
            # Both NEEDS_REVIEW -> send to tiebreaker
            disagreements.append({
                "index": idx,
                "question": questions[idx],
                "s1": s1_cat,
                "s2": s2_cat
            })
        else:
            # Disagreement on different valid categories -> need tiebreaker
            disagreements.append({
                "index": idx,
                "question": questions[idx],
                "s1": s1_cat,
                "s2": s2_cat
            })

    # Stage 3: Batch tiebreakers for disagreements
    if disagreements:
        s3_results = stage3_batch_tiebreak(client, disagreements, BATCH_SIZE, show_progress)
        s3_lookup = {r["disagreement_idx"]: r for r in s3_results}

        for i, d in enumerate(disagreements):
            s3 = s3_lookup.get(i, {})
            s3_cat = s3.get("category", "NEEDS_REVIEW")

            cats = [d["s1"], d["s2"], s3_cat]
            # Filter to only valid categories for voting
            valid_cats = [c for c in cats if c != "NEEDS_REVIEW" and any(c.startswith(v) for v in VALID_CATEGORIES)]

            if valid_cats:
                cat_counts = Counter(valid_cats)
                most_common = cat_counts.most_common(1)[0]
                final_cat = most_common[0]
                votes = most_common[1]
            else:
                # All 3 stages failed to classify - flag for review
                final_cat = "NEEDS_REVIEW"
                votes = 0

            final_results.append({
                "index": d["index"],
                "category": final_cat,
                "confidence": s3.get("confidence", 0.5),
                "s1": d["s1"],
                "s2": d["s2"],
                "s3": s3_cat,
                "votes": votes
            })

    final_results.sort(key=lambda x: x["index"])

    # Post-classification validation: catch electoral markets misclassified as STATE_LOCAL
    final_results = validate_electoral_classification(final_results, questions, show_progress)

    if show_progress:
        needs_review = sum(1 for r in final_results if r["category"] == "NEEDS_REVIEW")
        classified = len(final_results) - needs_review
        log(f"\nFINAL: {classified} classified, {needs_review} need review")
        log(f"{'='*50}")

    return final_results


def main():
    """Main function to classify new markets into categories."""
    print("\n" + "=" * 70)
    print("PIPELINE: CLASSIFY MARKETS INTO POLITICAL CATEGORIES")
    print("Using 3-Stage GPT Classification Pipeline (15 political + NOT_POLITICAL)")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    # Check if input file exists
    if not INPUT_FILE.exists():
        log(f"Input file not found: {INPUT_FILE}")
        log("Run pipeline_discover_markets.py first!")
        log("⚠ NO DATA PROCESSED - skipping classification")
        return 0  # Graceful exit - nothing to process

    # Load new markets
    log("Loading new markets...")
    df = pd.read_csv(INPUT_FILE)
    # Ensure political_category is string type (avoids LossySetitemError on NaN-only columns)
    if 'political_category' in df.columns:
        df['political_category'] = df['political_category'].astype(object)
    else:
        df['political_category'] = pd.Series([None] * len(df), dtype=object)
    log(f"  Total new markets: {len(df):,}")

    # Split into already classified and needs classification
    force_reclassify = "--force-reclassify" in sys.argv
    if force_reclassify:
        already_classified = df[df['political_category'] == '1. ELECTORAL'].copy()
        needs_classification = df[df['political_category'] != '1. ELECTORAL'].copy()
        needs_classification.loc[:, 'political_category'] = None
        log(f"  --force-reclassify: keeping {len(already_classified):,} ELECTORAL, reclassifying {len(needs_classification):,}")
    else:
        already_classified = df[df['political_category'].notna()].copy()
        needs_classification = df[df['political_category'].isna()].copy()
        log(f"  Already classified (skipped): {len(already_classified):,}")

    log(f"  Needs GPT classification: {len(needs_classification):,}")

    if len(needs_classification) == 0:
        log("No markets need classification!")
        df.to_csv(OUTPUT_FILE, index=False)
        return len(already_classified)

    # PRE-FILTER: Remove obvious non-political markets before GPT
    needs_classification, pre_filtered = pre_filter_non_political(needs_classification, show_progress=True)

    # Save pre-filtered markets to audit file
    if len(pre_filtered) > 0:
        pre_filtered_file = DATA_DIR / "new_markets_pre_filtered.csv"
        pre_filtered.to_csv(pre_filtered_file, index=False)
        log(f"\n  Saved {len(pre_filtered):,} pre-filtered markets to: {pre_filtered_file}")

        # Update main df to mark these as NOT_POLITICAL
        for idx in pre_filtered.index:
            df.loc[idx, 'political_category'] = '16. NOT_POLITICAL'

    if len(needs_classification) == 0:
        log("\nAll markets pre-filtered! No GPT classification needed.")
        # Still need to save - excluding NOT_POLITICAL
        df_classified = df[df['political_category'] != '16. NOT_POLITICAL'].copy()
        df_classified.to_csv(OUTPUT_FILE, index=False)
        return len(df_classified)

    # Initialize OpenAI client
    log("\nInitializing OpenAI client...")
    client = get_openai_client()
    log("  OpenAI client ready")

    # Load checkpoint if exists (keyed by stable market_id, not DataFrame index)
    processed_markets = {}
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
            processed_markets = checkpoint.get('results', {})
        log(f"  Loaded checkpoint: {len(processed_markets)} already processed")

    # Build market_id -> df_index mapping for needs_classification
    mid_to_idx = {}
    for idx, row in needs_classification.iterrows():
        mid_to_idx[str(row['market_id'])] = idx

    # Filter out already processed (by market_id)
    questions_to_process = []
    ids_to_process = []  # list of (market_id, df_index)
    for idx, row in needs_classification.iterrows():
        mid = str(row['market_id'])
        if mid not in processed_markets:
            questions_to_process.append(row['question'])
            ids_to_process.append((mid, idx))

    log(f"  Markets to classify: {len(questions_to_process):,}")

    if questions_to_process:
        # Run 3-stage classification pipeline
        start_time = time.time()
        results = run_classification_pipeline(client, questions_to_process, show_progress=True)

        # Map results back to market IDs and update dataframe
        for result in results:
            local_idx = result["index"]
            market_id, df_idx = ids_to_process[local_idx]
            full_category = expand_category(result["category"])
            df.loc[df_idx, 'political_category'] = full_category

            # Save to checkpoint (keyed by market_id)
            processed_markets[market_id] = {
                "category": full_category,
                "confidence": result.get("confidence", 0),
                "votes": result.get("votes", 1)
            }

        # Save checkpoint
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({
                'results': processed_markets,
                'last_updated': datetime.now().isoformat()
            }, f)

        elapsed = time.time() - start_time
        log(f"\nClassification completed in {elapsed/60:.1f} minutes")

    # Also apply any previously processed results
    for market_id, data in processed_markets.items():
        idx = mid_to_idx.get(market_id)
        if idx is not None and idx in df.index and pd.isna(df.loc[idx, 'political_category']):
            df.loc[idx, 'political_category'] = data.get("category", "NEEDS_REVIEW")

    # Separate classified markets from those needing review
    df_classified = df[df['political_category'] != 'NEEDS_REVIEW'].copy()
    df_needs_review = df[df['political_category'] == 'NEEDS_REVIEW'].copy()

    # Filter out NOT_POLITICAL markets - these should not be in the pipeline
    df_not_political = df_classified[df_classified['political_category'] == '16. NOT_POLITICAL'].copy()
    df_classified = df_classified[df_classified['political_category'] != '16. NOT_POLITICAL'].copy()

    # Save results
    log("\n" + "=" * 50)
    log("SAVING RESULTS")
    log("=" * 50)

    if len(df_not_political) > 0:
        log(f"Removed {len(df_not_political):,} NOT_POLITICAL markets (sports/esports/entertainment/etc)")
        # Save NOT_POLITICAL to separate file for reference
        not_political_file = DATA_DIR / "new_markets_not_political.csv"
        df_not_political.to_csv(not_political_file, index=False)
        log(f"  Saved to: {not_political_file}")

    df_classified.to_csv(OUTPUT_FILE, index=False)
    log(f"Saved {len(df_classified):,} classified markets to: {OUTPUT_FILE}")

    # Save markets needing review to separate file
    if len(df_needs_review) > 0:
        review_file = DATA_DIR / "new_markets_needs_review.csv"
        df_needs_review.to_csv(review_file, index=False)
        log(f"Saved {len(df_needs_review):,} markets needing review to: {review_file}")

    # Category distribution
    log("\nCategory distribution:")
    cat_counts = df_classified['political_category'].value_counts()
    for cat in POLITICAL_CATEGORIES:
        count = cat_counts.get(cat, 0)
        if count > 0:
            log(f"  {cat}: {count:,}")

    # Clean up checkpoint on successful completion
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    # Summary
    print("\n" + "=" * 70)
    print("CLASSIFICATION COMPLETE")
    print("=" * 70)
    print(f"Markets processed: {len(needs_classification):,}")
    print(f"Successfully classified: {len(df_classified):,}")
    print(f"Needs manual review: {len(df_needs_review):,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return len(df_classified)


if __name__ == "__main__":
    main()
