#!/usr/bin/env python3
"""
================================================================================
FIX CATEGORY CONTAMINATION
================================================================================

Identifies and removes non-political markets that were incorrectly classified.
Uses keyword patterns to find obvious contamination (sports, crypto, entertainment).

Target categories with highest contamination rates:
- TIMING_EVENTS: 48%
- GOVERNMENT_OPERATIONS: 36%
- REGULATORY: 24%
- STATE_LOCAL: 22%
- PARTY_POLITICS: 18%

================================================================================
"""

import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

# Paths
DATA_DIR = Path(__file__).parent.parent.parent / "data"
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
AUDIT_DIR = DATA_DIR / "audit"


# =============================================================================
# NON-POLITICAL PATTERNS (from pipeline_classify_categories.py)
# =============================================================================

NON_POLITICAL_PATTERNS = {
    # Sports
    'sports_leagues': r'\b(NFL|NBA|MLB|NHL|MLS|UFC|WWE|PGA|ATP|WTA|F1|NASCAR|Premier League|La Liga|Serie A|Bundesliga|Ligue 1|Champions League|UCL|Europa League|World Series|Super Bowl|Stanley Cup|World Cup)\b',
    'sports_terms': r'\b(touchdown|home run|goal scorer|striker|goalkeeper|quarterback|playoffs|championship game|grand slam|hole-in-one|birdie|eagle|triple crown|batting average|ERA|rushing yards|three-pointer)\b',
    'sports_teams': r'\b(Yankees|Dodgers|Lakers|Celtics|Patriots|Cowboys|Steelers|Warriors|Chiefs|Eagles|49ers|Bears|Packers|Red Sox|Cubs|Mets|Giants|Cardinals|Braves|Astros|Phillies|Padres|Mariners|Orioles|Rays|Guardians|Twins|Tigers|Royals|Rangers|White Sox|Reds|Brewers|Pirates|Rockies|Diamondbacks|Marlins|Nationals|Athletics|Angels|Blue Jays|Knicks|Nets|Bulls|Heat|Bucks|Suns|Clippers|Mavericks|Nuggets|76ers|Raptors|Cavaliers|Hawks|Wizards|Hornets|Magic|Pacers|Pistons|Timberwolves|Thunder|Pelicans|Grizzlies|Spurs|Jazz|Trail Blazers|Kings|Rockets|Manchester United|Manchester City|Liverpool|Chelsea|Arsenal|Real Madrid|Barcelona|Bayern Munich|Juventus|PSG|Inter Milan|AC Milan)\b',

    # Esports
    'esports': r'\b(esports|e-sports|League of Legends|LoL|Dota|CSGO|CS2|Counter-Strike|Valorant|Fortnite|PUBG|Overwatch|Call of Duty|CoD|Worlds Championship|LCS|LEC|LCK|LPL|The International|Major tournament)\b',

    # Crypto prices
    'crypto_prices': r'\b(Bitcoin|BTC|Ethereum|ETH|Solana|SOL|Dogecoin|DOGE|XRP|Cardano|ADA|Polkadot|Avalanche|Chainlink|Polygon|MATIC|Litecoin|LTC|Shiba|PEPE|memecoin|meme coin)\s*(price|to|above|below|reach|hit|at|over|under|\$|USD)',
    'crypto_terms': r'\b(cryptocurrency|crypto|token price|coin price|market cap|trading volume|DeFi|NFT floor|mint price|gas fees|halving|staking rewards|yield farming|liquidity pool|DEX|CEX|airdrop value)\b',

    # Entertainment
    'awards': r'\b(Oscar|Academy Award|Grammy|Emmy|Tony Award|Golden Globe|BAFTA|MTV Award|BET Award|American Music Award|Billboard Music Award|Brit Award|ESPY|Ballon d\'Or|FIFA Best|Laureus)\b',
    'entertainment': r'\b(box office|movie premiere|album release|concert|tour dates|streaming numbers|Spotify|Netflix|Disney\+|HBO Max|viewership|ratings|episode|season finale|series finale|soundtrack|Billboard Hot 100|chart position|record sales|platinum|diamond certification|TikTok viral|YouTube views|subscriber count|follower count)\b',
    'reality_tv': r'\b(Bachelor|Bachelorette|Survivor winner|Big Brother|American Idol|The Voice|America\'s Got Talent|Dancing with the Stars|Love Island|Real Housewives)\b',
    'celebrity': r'\b(dating|engaged|married|divorced|pregnant|baby|affair|scandal|rehab|overdose|death hoax|feud|beef|diss track|celebrity couple|red carpet|paparazzi|tabloid)\b',

    # Video games
    'video_games': r'\b(GTA|Grand Theft Auto|Call of Duty|Zelda|Mario|Pokemon|Elden Ring|Starfield|Cyberpunk|Baldur\'s Gate|Diablo|World of Warcraft|WoW|Final Fantasy|Minecraft|Roblox|Among Us|Apex Legends|game release|DLC|expansion pack|patch notes|speedrun|game sales|console sales|PlayStation|Xbox|Nintendo|Steam)\b',

    # Weather
    'weather': r'\b(hurricane category|tornado|earthquake magnitude|tsunami|wildfire acres|flood|drought|temperature record|snowfall|rainfall|heat wave|cold snap|weather forecast|storm surge|wind speed mph|Richter scale)\b',

    # Stock/Business (non-regulatory)
    'stock_prices': r'\b(stock price|share price|market cap|IPO price|earnings per share|EPS|P/E ratio|dividend|stock split|buyback|trading at|closes at|opens at|52-week high|52-week low|all-time high|ATH)\b',
    'business_metrics': r'\b(revenue|quarterly earnings|annual report|profit margin|EBITDA|cash flow|debt ratio|balance sheet|income statement|guidance|forecast|analyst rating|price target|deliveries|units sold|subscribers|MAU|DAU|user growth)\b',

    # Misc non-political
    'space_tourism': r'\b(SpaceX Starship|Blue Origin|Virgin Galactic|rocket launch|Mars landing|Moon landing|asteroid mining|space tourism|orbital flight)\b',
    'collectibles': r'\b(Pokemon card|baseball card|trading card|sports memorabilia|auction price|Sotheby|Christie|rare coin|stamp collection|vintage car|art auction|NFT sale|Bored Ape|CryptoPunk)\b',
    'social_media': r'\b(tweet count|tweets per|follower count|followers|following|likes|retweets|views|impressions|engagement rate|viral post|trending topic|hashtag)\b',
}

# Keywords that indicate political content (protect from removal)
POLITICAL_PROTECT_KEYWORDS = [
    'trump', 'biden', 'harris', 'obama', 'clinton', 'congress', 'senate', 'house', 'powell',
    'representative', 'senator', 'governor', 'mayor', 'president', 'administration',
    'white house', 'capitol', 'supreme court', 'federal', 'legislation', 'bill',
    'policy', 'regulation', 'election', 'vote', 'ballot', 'campaign', 'democrat',
    'republican', 'gop', 'dnc', 'rnc', 'primary', 'caucus', 'electoral', 'impeach',
    'cabinet', 'secretary', 'attorney general', 'fbi', 'cia', 'doj', 'dhs', 'epa',
    'fed', 'federal reserve', 'tariff', 'sanction', 'treaty', 'nato', 'un',
    'ukraine', 'russia', 'china', 'israel', 'gaza', 'iran', 'north korea',
    'military', 'troops', 'deployment', 'war', 'conflict', 'ceasefire', 'peace',
    'immigration', 'border', 'asylum', 'deportation', 'refugee', 'visa',
    'abortion', 'roe', 'gun', 'amendment', 'constitutional', 'scotus',
    'pardon', 'clemency', 'indictment', 'conviction', 'trial', 'lawsuit',
    'poll', 'approval rating', 'favorability', 'approval', 'disapproval',
    'executive order', 'veto', 'filibuster', 'shutdown', 'debt ceiling',
    'state of the union', 'inaugural', 'cabinet', 'nomination', 'confirmation',
]


def is_non_political(question: str) -> Tuple[bool, str]:
    """
    Check if a question is non-political based on patterns.
    Returns (is_non_political, reason).
    """
    if pd.isna(question):
        return False, ""

    q = str(question).lower()

    # First check if it has strong political keywords
    for keyword in POLITICAL_PROTECT_KEYWORDS:
        if keyword in q:
            return False, f"protected:{keyword}"

    # Check non-political patterns
    for category, pattern in NON_POLITICAL_PATTERNS.items():
        if re.search(pattern, question, re.IGNORECASE):
            return True, category

    return False, ""


def main():
    print("\n" + "=" * 70)
    print("FIX CATEGORY CONTAMINATION")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Load master CSV
    print("\nLoading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    print(f"  Total markets: {len(df):,}")

    # Target categories with high contamination
    target_categories = [
        '13. TIMING_EVENTS',
        '10. GOVERNMENT_OPERATIONS',
        '5. REGULATORY',
        '12. STATE_LOCAL',
        '11. PARTY_POLITICS',
        '15. POLITICAL_SPEECH',
        '4. APPOINTMENTS',
        '6. INTERNATIONAL',
    ]

    print(f"\nScanning {len(target_categories)} high-contamination categories...")

    all_contaminated = []

    for cat in target_categories:
        cat_df = df[df['political_category'] == cat].copy()

        # Check each market
        contaminated = []
        for idx, row in cat_df.iterrows():
            is_contam, reason = is_non_political(row['question'])
            if is_contam:
                contaminated.append({
                    'index': idx,
                    'market_id': row['market_id'],
                    'question': row['question'],
                    'platform': row['platform'],
                    'original_category': cat,
                    'contamination_type': reason,
                })

        if contaminated:
            print(f"  {cat}: {len(contaminated)}/{len(cat_df)} flagged ({len(contaminated)/len(cat_df):.1%})")
            all_contaminated.extend(contaminated)
        else:
            print(f"  {cat}: 0/{len(cat_df)} flagged")

    print(f"\n  TOTAL FLAGGED: {len(all_contaminated)} markets")

    if len(all_contaminated) == 0:
        print("\nNo contamination found!")
        return

    # Show samples by type
    contam_df = pd.DataFrame(all_contaminated)
    print("\nContamination by type:")
    for ctype, count in contam_df['contamination_type'].value_counts().items():
        print(f"  {ctype}: {count}")
        samples = contam_df[contam_df['contamination_type'] == ctype]['question'].head(3).tolist()
        for s in samples:
            print(f"    • {s[:70]}..." if len(s) > 70 else f"    • {s}")

    # Save flagged markets for review
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    review_file = AUDIT_DIR / f"{datetime.now().strftime('%Y-%m-%d')}_contamination_flagged.csv"
    contam_df.to_csv(review_file, index=False)
    print(f"\nFlagged markets saved for review: {review_file}")

    # Ask for confirmation
    print("\n" + "-" * 70)
    response = input("Remove these markets from master CSV? [y/N]: ").strip().lower()

    if response != 'y':
        print("Aborted. Review the flagged markets and run again if needed.")
        return

    # Remove contaminated markets
    indices_to_remove = [c['index'] for c in all_contaminated]
    df_cleaned = df.drop(indices_to_remove)

    # Save audit log
    removed_file = AUDIT_DIR / f"{datetime.now().strftime('%Y-%m-%d')}_contamination_removed.csv"
    contam_df.to_csv(removed_file, index=False)
    print(f"\nRemoved markets logged: {removed_file}")

    # Save cleaned master
    print("\nSaving cleaned master CSV...")
    df_cleaned.to_csv(MASTER_FILE, index=False)
    print(f"  Saved: {MASTER_FILE}")
    print(f"  Removed: {len(all_contaminated):,} markets")
    print(f"  New total: {len(df_cleaned):,} markets")

    print("\n" + "=" * 70)
    print(f"COMPLETE: Removed {len(all_contaminated):,} contaminated markets")
    print("=" * 70 + "\n")

    return len(all_contaminated)


if __name__ == "__main__":
    main()
