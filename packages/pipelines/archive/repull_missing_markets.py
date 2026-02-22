#!/usr/bin/env python3
"""
Repull Missing Market Price Data from Dome API

This script pulls price data for specific missing Polymarket markets:
- Wisconsin Supreme Court (527848)
- CA-27 (505157)
- CO-8 (512628)

The data is added to the CORRECTED file, and ONLY these specific tokens are truncated.
"""

import json
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

# Paths
BASE_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi")
DATA_DIR = BASE_DIR / "data"
PRICES_FILE = DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json"
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
ELECTION_DATES = DATA_DIR / "election_dates_lookup.csv"

# Dome API endpoint
DOME_API_BASE = "https://data-api.polymarket.com/candlesticks"

# Markets to repull
MARKETS_TO_PULL = [
    {
        "name": "Wisconsin Supreme Court",
        "market_id": 527848,
        "condition_id": "0x354ebe41d75446cd43eb2dbebacd344cfb9a8abe9b26ce39c5074e0841e5a885",
        "token_yes": "8482778105598978702482860615499306660276940779107944683038963310770224106864",
        "token_no": "88996768914755589030273472569780758869780921699118294118437910728260537946424",
        "election_date": "2025-04-01",  # Wisconsin SC election
    },
    {
        "name": "CA-27",
        "market_id": 505157,
        "condition_id": "0xc25e59d89a54d4cf156eec9a8fb485b831d927c28702f24b67665089534c7e21",
        "token_yes": "91185558637433545319929408005961748855542339385322666155845562934483186143095",
        "token_no": "80930953820477036464086345222563860546984367300984297592638987850341018599905",
        "election_date": "2024-11-05",  # 2024 General Election
    },
    {
        "name": "CO-8",
        "market_id": 512628,
        "condition_id": "0x565b93f16cdeb61ba3d477320e708dc8524b05993cbdc86a3a02914b1ff935ef",
        "token_yes": "9093215244912254816377663646197356805579066249602841657643717096794150687701",
        "token_no": "52414671915074173872802781156113296565179095591024656487856599039926764471896",
        "election_date": "2024-11-05",  # 2024 General Election
    },
]

# Global cutoff: November 10, 2025 23:59:59 UTC
GLOBAL_CUTOFF = datetime(2025, 11, 10, 23, 59, 59, tzinfo=timezone.utc)
GLOBAL_CUTOFF_TS = int(GLOBAL_CUTOFF.timestamp())


def fetch_candlesticks(condition_id: str, start_ts: int, end_ts: int):
    """Fetch candlesticks from Dome API"""
    url = f"{DOME_API_BASE}/{condition_id}"
    params = {
        "interval": "1h",
        "start_ts": start_ts,
        "end_ts": end_ts,
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"   Error {response.status_code}: {response.text}")
        return None

    return response.json()


def parse_candlesticks_response(response_data):
    """Parse Dome API candlesticks response into token price histories

    Response structure:
    {
        "candlesticks": [
            [candle1, candle2, ..., {token_id: "...", side: "..."}],
            [candle1, candle2, ..., {token_id: "...", side: "..."}]
        ]
    }
    """
    result = {}

    if not response_data or "candlesticks" not in response_data:
        return result

    candlesticks_list = response_data["candlesticks"]

    for token_data in candlesticks_list:
        if not token_data or len(token_data) == 0:
            continue

        # The last element contains token metadata
        token_info = token_data[-1]
        if not isinstance(token_info, dict) or "token_id" not in token_info:
            continue

        token_id = token_info["token_id"]

        # Process candlesticks (all elements except the last)
        prices = []
        for candle in token_data[:-1]:
            if isinstance(candle, dict) and "c" in candle and "t" in candle:
                prices.append({
                    "p": candle["c"],  # close price
                    "t": candle["t"],  # timestamp
                })

        result[token_id] = prices

    return result


def truncate_at_election(prices, election_date_str):
    """Truncate prices at end of election day"""
    election_date = datetime.strptime(election_date_str, "%Y-%m-%d").date()
    election_end = datetime(
        election_date.year,
        election_date.month,
        election_date.day,
        23, 59, 59,
        tzinfo=timezone.utc
    )
    election_cutoff_ts = int(election_end.timestamp())

    # Use the earlier of election cutoff and global cutoff
    final_cutoff_ts = min(election_cutoff_ts, GLOBAL_CUTOFF_TS)

    return [p for p in prices if p["t"] <= final_cutoff_ts]


def main():
    print("=" * 80)
    print("REPULLING MISSING MARKET PRICE DATA")
    print("=" * 80)

    # Load existing prices
    print("\n1. Loading existing price data...")
    with open(PRICES_FILE, 'r') as f:
        existing_prices = json.load(f)
    print(f"   Loaded {len(existing_prices):,} existing token histories")

    # Count existing prices for our target tokens
    for market in MARKETS_TO_PULL:
        yes_count = len(existing_prices.get(market["token_yes"], []))
        no_count = len(existing_prices.get(market["token_no"], []))
        print(f"   {market['name']}: YES={yes_count}, NO={no_count} price points")

    # Pull data from Dome API
    print("\n2. Pulling data from Dome API...")

    # Time range: from market start to election day
    # Use a wide range - start from 2020
    start_ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
    end_ts = GLOBAL_CUTOFF_TS

    new_data = {}

    for market in MARKETS_TO_PULL:
        print(f"\n   Fetching {market['name']}...")
        print(f"      condition_id: {market['condition_id']}")

        response = fetch_candlesticks(market["condition_id"], start_ts, end_ts)

        if response is None:
            print(f"      FAILED to fetch data")
            continue

        token_prices = parse_candlesticks_response(response)

        if not token_prices:
            print(f"      No candlestick data found in response")
            # Print response for debugging
            print(f"      Response keys: {response.keys() if isinstance(response, dict) else 'not a dict'}")
            if isinstance(response, dict) and "candlesticks" in response:
                print(f"      Candlesticks count: {len(response['candlesticks'])}")
                if response['candlesticks']:
                    print(f"      First candlestick type: {type(response['candlesticks'][0])}")
                    if isinstance(response['candlesticks'][0], list) and response['candlesticks'][0]:
                        print(f"      First item sample: {response['candlesticks'][0][:2]}...")
                        print(f"      Last item: {response['candlesticks'][0][-1]}")
            continue

        # Store prices for each token
        for token_id, prices in token_prices.items():
            # Truncate at election date
            truncated_prices = truncate_at_election(prices, market["election_date"])
            new_data[token_id] = truncated_prices

            # Identify if YES or NO token
            if token_id == market["token_yes"]:
                token_type = "YES"
            elif token_id == market["token_no"]:
                token_type = "NO"
            else:
                token_type = "UNKNOWN"

            print(f"      {token_type} token: {len(truncated_prices)} price points (after truncation)")

        time.sleep(0.5)  # Rate limiting

    # Update existing prices with new data
    print("\n3. Updating price file...")

    tokens_added = 0
    tokens_updated = 0
    total_prices_added = 0

    for token_id, prices in new_data.items():
        if token_id in existing_prices:
            old_count = len(existing_prices[token_id])
            if old_count == 0 and len(prices) > 0:
                tokens_updated += 1
            existing_prices[token_id] = prices
        else:
            tokens_added += 1
            existing_prices[token_id] = prices
        total_prices_added += len(prices)

    print(f"   Tokens added: {tokens_added}")
    print(f"   Tokens updated: {tokens_updated}")
    print(f"   Total price points added: {total_prices_added}")

    # Verify counts
    print("\n4. Verifying updated counts...")
    for market in MARKETS_TO_PULL:
        yes_count = len(existing_prices.get(market["token_yes"], []))
        no_count = len(existing_prices.get(market["token_no"], []))
        print(f"   {market['name']}: YES={yes_count}, NO={no_count} price points")

    # Save updated prices
    print("\n5. Saving updated price file...")
    with open(PRICES_FILE, 'w') as f:
        json.dump(existing_prices, f)
    print(f"   Saved to: {PRICES_FILE}")

    # Final verification - no prices after global cutoff
    print("\n6. Verifying no prices after November 10, 2025...")
    violations = 0
    for token_id in new_data.keys():
        for p in existing_prices.get(token_id, []):
            if p['t'] > GLOBAL_CUTOFF_TS:
                violations += 1

    if violations == 0:
        print("   PASS: No prices after November 10, 2025")
    else:
        print(f"   FAIL: Found {violations} prices after cutoff!")

    print("\n" + "=" * 80)
    print("DONE")
    print("=" * 80)


if __name__ == "__main__":
    main()
