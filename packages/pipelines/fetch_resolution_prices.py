#!/usr/bin/env python3
"""
Fetch Resolution Prices via Native APIs

This script fetches the TRUE resolution prices (last traded price before market closure)
for all resolved markets by calling native platform APIs directly:
- Polymarket: CLOB API prices-history endpoint
- Kalshi: Native series candlestick endpoint (with trades API fallback)

This allows comparison between:
1. Current truncated prices (at event date for elections, trading_close - 24h for others)
2. True resolution prices (at actual trading_close_time)

Output: data/resolution_prices.json
"""

import pandas as pd
import requests
import json
import time
import os
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR

MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
OUTPUT_FILE = DATA_DIR / "resolution_prices.json"
CHECKPOINT_FILE = DATA_DIR / "resolution_prices_checkpoint.json"

# Native API endpoints
CLOB_API_BASE = "https://clob.polymarket.com"
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Parallel processing
MAX_WORKERS = 10
MAX_RETRIES = 3
CHECKPOINT_INTERVAL = 500  # Save checkpoint every N markets
RATE_LIMIT_DELAY = 0.1  # 100ms between requests

# Thread-safe counters
results_lock = threading.Lock()


class RateLimiter:
    """Thread-safe rate limiter."""
    def __init__(self, delay):
        self._delay = delay
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            if elapsed < self._delay:
                time.sleep(self._delay - elapsed)
            self._last = time.monotonic()


_rate_limiter = RateLimiter(RATE_LIMIT_DELAY)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def parse_close_time(close_time_val):
    """Parse trading_close_time to Unix timestamp."""
    if pd.isna(close_time_val):
        return None
    try:
        dt = pd.to_datetime(str(close_time_val), utc=True)
        return int(dt.timestamp())
    except:
        return None


def derive_series_ticker(ticker):
    """Derive series ticker from market ticker (everything before last hyphen).

    e.g. PRES-2024-DT -> PRES-2024, KXSENATE-26-GA-R -> KXSENATE-26-GA
    """
    parts = ticker.split('-')
    if len(parts) > 1:
        return '-'.join(parts[:-1])
    return ticker


def fetch_polymarket_resolution_price(token_id, trading_close_ts):
    """Fetch the resolution price for a Polymarket market via CLOB API."""
    for attempt in range(MAX_RETRIES):
        try:
            _rate_limiter.wait()
            response = requests.get(
                f"{CLOB_API_BASE}/prices-history",
                params={
                    'market': token_id,
                    'interval': 'max',
                    'fidelity': 1440  # Daily candles
                },
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                history = data.get('history', [])

                if not history:
                    return None

                # Find the last price point before trading_close_ts
                best = None
                for point in history:
                    ts = point.get('t', 0)
                    if ts <= trading_close_ts:
                        best = point

                if best:
                    price = float(best.get('p', 0))
                    ts = best.get('t', 0)
                    return {
                        'price': price,
                        'timestamp': ts,
                        'date': datetime.fromtimestamp(ts).strftime('%Y-%m-%d') if ts else None
                    }

                return None

            elif response.status_code == 429:
                wait_time = 10 * (2 ** attempt)
                log(f"  Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            elif response.status_code == 400:
                return None  # Invalid token_id
            else:
                return None

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return None

    return None


def fetch_kalshi_resolution_price_from_trades(ticker, trading_close_ts):
    """Fallback: Fetch resolution price from Kalshi trades API."""
    # Get trades up to trading close time
    end_dt = datetime.fromtimestamp(trading_close_ts)

    for attempt in range(MAX_RETRIES):
        try:
            _rate_limiter.wait()
            response = requests.get(
                f"{KALSHI_API_BASE}/markets/trades",
                params={
                    'ticker': ticker,
                    'limit': 100
                },
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                trades = data.get('trades', [])

                if trades:
                    # Trades are sorted by created_time desc, so first is most recent
                    # Find the last trade before trading_close_ts
                    for trade in trades:
                        trade_time_str = trade.get('created_time', '')
                        if trade_time_str:
                            try:
                                trade_dt = datetime.fromisoformat(trade_time_str.replace('Z', '+00:00'))
                                trade_ts = trade_dt.timestamp()
                                if trade_ts <= trading_close_ts:
                                    # yes_price is in cents (0-100)
                                    price = trade.get('yes_price', 0)
                                    return {
                                        'price': price / 100.0,
                                        'timestamp': trade_ts,
                                        'date': trade_dt.strftime('%Y-%m-%d'),
                                        'source': 'kalshi_trades'
                                    }
                            except:
                                continue

                    # If no trades before close time, use the most recent one
                    last_trade = trades[0]
                    price = last_trade.get('yes_price', 0)
                    return {
                        'price': price / 100.0,
                        'timestamp': trading_close_ts,
                        'date': end_dt.strftime('%Y-%m-%d'),
                        'source': 'kalshi_trades'
                    }
                return None

            elif response.status_code == 429:
                time.sleep(5 * (2 ** attempt))
                continue
            else:
                return None

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            return None

    return None


def fetch_kalshi_resolution_price(ticker, trading_close_ts):
    """Fetch the resolution price for a Kalshi market via native series candlestick API."""
    series_ticker = derive_series_ticker(ticker)
    start_ts = trading_close_ts - (30 * 86400)

    for attempt in range(MAX_RETRIES):
        try:
            _rate_limiter.wait()
            response = requests.get(
                f"{KALSHI_API_BASE}/series/{series_ticker}/markets/{ticker}/candlesticks",
                params={
                    'period_interval': 1440,  # Daily candles
                    'start_ts': start_ts,
                    'end_ts': trading_close_ts
                },
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                candlesticks = data.get('candlesticks', [])

                if candlesticks:
                    # Get the last candle's close price
                    last_candle = candlesticks[-1]
                    close_price = last_candle.get('price', {}).get('close')
                    if close_price is not None:
                        last_ts = last_candle.get('end_period_ts', 0)
                        return {
                            'price': close_price / 100.0,
                            'timestamp': last_ts,
                            'date': datetime.fromtimestamp(last_ts).strftime('%Y-%m-%d') if last_ts else None,
                            'source': 'kalshi_candlestick'
                        }

                # No valid candlestick data - try trades API fallback
                return fetch_kalshi_resolution_price_from_trades(ticker, trading_close_ts)

            elif response.status_code == 429:
                wait_time = 10 * (2 ** attempt)
                time.sleep(wait_time)
                continue
            elif response.status_code == 404:
                # Series not found - try trades API fallback
                return fetch_kalshi_resolution_price_from_trades(ticker, trading_close_ts)
            else:
                # Other API error - try fallback
                return fetch_kalshi_resolution_price_from_trades(ticker, trading_close_ts)

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
                continue
            # All retries failed - try fallback
            return fetch_kalshi_resolution_price_from_trades(ticker, trading_close_ts)

    return None


def save_checkpoint(results, processed_ids):
    """Save checkpoint to allow resuming."""
    checkpoint = {
        'results': results,
        'processed_ids': list(processed_ids),
        'timestamp': datetime.now().isoformat()
    }
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f)

def load_checkpoint():
    """Load checkpoint if exists."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE) as f:
                checkpoint = json.load(f)
            return checkpoint.get('results', {}), set(checkpoint.get('processed_ids', []))
        except:
            pass
    return {}, set()

def process_polymarket_market(row, pm_token_ids):
    """Process a single Polymarket market - returns results for both YES and NO tokens."""
    market_id = str(row['market_id'])
    token_id_yes = str(row['pm_token_id_yes'])
    token_id_no = str(row['pm_token_id_no']) if pd.notna(row.get('pm_token_id_no')) else None
    trading_close_ts = parse_close_time(row['trading_close_time'])

    if not trading_close_ts:
        return market_id, []

    results = []

    # Fetch price history for YES token via CLOB API
    result = fetch_polymarket_resolution_price(token_id_yes, trading_close_ts)

    # Add YES token result if in prediction_accuracy
    if result and token_id_yes in pm_token_ids:
        results.append({
            'key': f'pm_{token_id_yes}',
            'data': {
                'platform': 'polymarket',
                'market_id': market_id,
                'token_id': token_id_yes,
                'resolution_price': result['price'],
                'resolution_date': result['date'],
                'trading_close_time': row['trading_close_time'],
                'winning_outcome': row['winning_outcome']
            }
        })

    # Add NO token result (price = 1 - yes_price) if in prediction_accuracy
    if result and token_id_no and token_id_no in pm_token_ids:
        results.append({
            'key': f'pm_{token_id_no}',
            'data': {
                'platform': 'polymarket',
                'market_id': market_id,
                'token_id': token_id_no,
                'resolution_price': 1.0 - result['price'],  # NO price = 1 - YES price
                'resolution_date': result['date'],
                'trading_close_time': row['trading_close_time'],
                'winning_outcome': 'No' if row['winning_outcome'] == 'Yes' else 'Yes'
            }
        })

    return market_id, results

def process_kalshi_market(row):
    """Process a single Kalshi market - for parallel execution."""
    market_id = str(row['market_id'])
    ticker = market_id
    trading_close_ts = parse_close_time(row['trading_close_time'])

    if not trading_close_ts or not ticker:
        return market_id, None

    result = fetch_kalshi_resolution_price(ticker, trading_close_ts)

    if result:
        return market_id, {
            'platform': 'kalshi',
            'market_id': market_id,
            'ticker': ticker,
            'resolution_price': result['price'],
            'resolution_date': result['date'],
            'trading_close_time': row['trading_close_time'],
            'winning_outcome': row['winning_outcome'],
            'source': result.get('source', 'kalshi_candlestick')
        }
    return market_id, None

def main():
    log("="*60)
    log("FETCHING RESOLUTION PRICES (NATIVE APIs)")
    log(f"Using {MAX_WORKERS} parallel workers")
    log("="*60)

    # Load prediction accuracy files to get list of markets in calibration graph
    log("\n1. Loading prediction accuracy files (calibration graph markets)...")
    pm_accuracy = pd.read_csv(DATA_DIR / "polymarket_prediction_accuracy_all_political.csv",
                               dtype={'token_id': str, 'market_id': str})
    kalshi_accuracy = pd.read_csv(DATA_DIR / "kalshi_prediction_accuracy_all_political.csv",
                                   dtype={'ticker': str})

    # Get unique market/token IDs from 1-day-before predictions
    pm_1d = pm_accuracy[pm_accuracy['days_before_event'] == 1]
    kalshi_1d = kalshi_accuracy[kalshi_accuracy['days_before_event'] == 1]

    pm_token_ids = set(pm_1d['token_id'].astype(str).unique())
    kalshi_tickers = set(kalshi_1d['ticker'].astype(str).unique())

    log(f"   Polymarket tokens in calibration: {len(pm_token_ids):,}")
    log(f"   Kalshi tickers in calibration: {len(kalshi_tickers):,}")

    # Load master data
    log("\n2. Loading master data...")
    df = pd.read_csv(MASTER_FILE, low_memory=False, dtype={'pm_token_id_yes': str, 'pm_token_id_no': str})

    # Filter to resolved markets with trading_close_time
    resolved = df[
        (df['winning_outcome'].isin(['Yes', 'No'])) &
        (df['trading_close_time'].notna())
    ].copy()

    # Further filter to only markets in calibration graph (match YES or NO tokens)
    pm_resolved = resolved[
        (resolved['platform'].str.lower() == 'polymarket') &
        (resolved['pm_condition_id'].notna())
    ].copy()
    pm_resolved['token_yes_str'] = pm_resolved['pm_token_id_yes'].astype(str)
    pm_resolved['token_no_str'] = pm_resolved['pm_token_id_no'].fillna('').astype(str)
    pm_markets = pm_resolved[
        pm_resolved['token_yes_str'].isin(pm_token_ids) |
        pm_resolved['token_no_str'].isin(pm_token_ids)
    ]
    kalshi_resolved = resolved[resolved['platform'].str.lower() == 'kalshi'].copy()
    kalshi_markets = kalshi_resolved[kalshi_resolved['market_id'].astype(str).isin(kalshi_tickers)]

    log(f"   Polymarket: {len(pm_markets):,} markets (filtered to calibration)")
    log(f"   Kalshi: {len(kalshi_markets):,} markets (filtered to calibration)")

    # Load checkpoint
    results, processed_ids = load_checkpoint()
    if results:
        log(f"\n   Resuming from checkpoint ({len(processed_ids):,} already processed)")

    # Filter to unprocessed markets
    pm_to_process = [row for _, row in pm_markets.iterrows() if str(row['market_id']) not in processed_ids]
    kalshi_to_process = [row for _, row in kalshi_markets.iterrows() if str(row['market_id']) not in processed_ids]

    log(f"\n   To process: {len(pm_to_process):,} Polymarket, {len(kalshi_to_process):,} Kalshi")

    # Process Polymarket markets in parallel
    log("\n3. Fetching Polymarket resolution prices (CLOB API, parallel)...")
    pm_success = 0
    pm_errors = 0
    processed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_polymarket_market, row, pm_token_ids): row for row in pm_to_process}

        for future in as_completed(futures):
            market_id, result_list = future.result()
            processed_count += 1

            with results_lock:
                processed_ids.add(market_id)
                if result_list:
                    for item in result_list:
                        results[item['key']] = item['data']
                    pm_success += len(result_list)
                else:
                    pm_errors += 1

            if processed_count % 500 == 0:
                log(f"   Processed {processed_count}/{len(pm_to_process)} PM markets ({pm_success} tokens, {pm_errors} errors)")
                save_checkpoint(results, processed_ids)

    log(f"   Polymarket complete: {pm_success} tokens from {processed_count} markets, {pm_errors} errors")
    save_checkpoint(results, processed_ids)

    # Process Kalshi markets in parallel
    log("\n4. Fetching Kalshi resolution prices (native candlestick + trades fallback)...")
    k_success = 0
    k_errors = 0
    k_fallback = 0
    processed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_kalshi_market, row): row for row in kalshi_to_process}

        for future in as_completed(futures):
            market_id, result_data = future.result()
            processed_count += 1

            with results_lock:
                processed_ids.add(market_id)
                if result_data:
                    results[f"k_{market_id}"] = result_data
                    k_success += 1
                    if result_data.get('source') == 'kalshi_trades':
                        k_fallback += 1
                else:
                    k_errors += 1

            if processed_count % 500 == 0:
                log(f"   Processed {processed_count}/{len(kalshi_to_process)} Kalshi markets (success: {k_success}, fallback: {k_fallback}, errors: {k_errors})")
                save_checkpoint(results, processed_ids)

    log(f"   Kalshi complete: {k_success} success ({k_fallback} via trades API fallback), {k_errors} errors")

    # Save final results
    log("\n5. Saving results...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)

    log(f"   Saved {len(results):,} resolution prices to {OUTPUT_FILE.name}")

    # Clean up checkpoint
    if CHECKPOINT_FILE.exists():
        os.remove(CHECKPOINT_FILE)

    # Summary
    log("\n" + "="*60)
    log("SUMMARY")
    log("="*60)
    log(f"Total resolution prices fetched: {len(results):,}")
    log(f"  Polymarket: {pm_success:,}")
    log(f"  Kalshi: {k_success:,}")
    log(f"\nOutput: {OUTPUT_FILE}")
    log("\nDone!")

if __name__ == "__main__":
    main()
