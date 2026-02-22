#!/usr/bin/env python3
"""
Export Bellwether distribution dataset files.

Creates:
- political_markets_master.csv
- price_histories.parquet
- liquidity_metrics.csv
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# Paths
DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = Path(__file__).parent.parent / "distribution"

def export_master():
    """Export political_markets_master.csv"""
    print("Exporting political_markets_master.csv...")

    # Load source
    df = pd.read_csv(DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv",
                     low_memory=False)

    # Select and rename columns to match data page spec
    columns = [
        # Identifiers
        'market_id', 'platform', 'pm_condition_id', 'pm_token_id_yes', 'pm_token_id_no', 'k_event_ticker',
        # Market Details
        'question', 'political_category', 'volume_usd', 'tags', 'date_added',
        # Electoral
        'election_type', 'country', 'location', 'office', 'election_year', 'is_primary', 'party', 'candidate', 'election_eve_price',
        # Resolution
        'is_closed', 'resolution_outcome', 'winning_outcome', 'trading_close_time', 'scheduled_end_time',
        # Kalshi-specific
        'k_last_price', 'k_yes_bid', 'k_yes_ask', 'k_volume_contracts', 'k_open_interest', 'k_liquidity', 'k_status', 'k_result'
    ]

    # Only keep columns that exist
    existing_cols = [c for c in columns if c in df.columns]
    master = df[existing_cols].copy()

    output_path = OUTPUT_DIR / "political_markets_master.csv"
    master.to_csv(output_path, index=False)
    print(f"  -> {output_path} ({len(master):,} markets, {len(existing_cols)} columns)")
    return master

def export_prices():
    """Export price_histories.csv using daily-updated price files."""
    print("Exporting price_histories.csv...")

    # Load master for metadata lookup
    master = pd.read_csv(DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv", low_memory=False)

    # Build lookup tables
    pm_master = master[master['platform'] == 'Polymarket'].copy()
    pm_token_to_market = dict(zip(pm_master['pm_token_id_yes'].astype(str), pm_master['market_id'].astype(str)))
    pm_token_to_category = dict(zip(pm_master['pm_token_id_yes'].astype(str), pm_master['political_category']))

    k_master = master[master['platform'] == 'Kalshi'].copy()
    k_ticker_to_category = dict(zip(k_master['market_id'].astype(str), k_master['political_category']))

    rows = []

    # Load PM prices (daily-updated DOMEAPI file)
    print("  Loading Polymarket prices...")
    with open(DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json", 'r') as f:
        pm_prices = json.load(f)

    for token_id, price_list in pm_prices.items():
        market_id = pm_token_to_market.get(token_id)
        category = pm_token_to_category.get(token_id)

        for entry in price_list:
            ts = entry.get('t')
            price = entry.get('p')
            if ts and price is not None:
                rows.append({
                    'market_id': market_id,
                    'token_id': token_id,
                    'platform': 'polymarket',
                    'timestamp': pd.to_datetime(ts, unit='s', utc=True),
                    'price': price,
                    'category': category
                })

    print(f"    {len(rows):,} Polymarket price observations")

    # Load Kalshi prices (daily-updated) - different format: end_period_ts, price.close (in cents)
    print("  Loading Kalshi prices...")
    with open(DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json", 'r') as f:
        k_prices = json.load(f)

    kalshi_count = 0
    for ticker, price_list in k_prices.items():
        category = k_ticker_to_category.get(ticker)

        for entry in price_list:
            ts = entry.get('end_period_ts')
            price_data = entry.get('price', {})
            price_cents = price_data.get('close')
            if ts and price_cents is not None:
                price = price_cents / 100.0  # Convert cents to decimal
                rows.append({
                    'market_id': ticker,
                    'token_id': ticker,
                    'platform': 'kalshi',
                    'timestamp': pd.to_datetime(ts, unit='s', utc=True),
                    'price': price,
                    'category': category
                })
                kalshi_count += 1

    print(f"    {kalshi_count:,} Kalshi price observations")

    # Create DataFrame
    df = pd.DataFrame(rows)

    # Sort by market and timestamp
    df = df.sort_values(['market_id', 'timestamp']).reset_index(drop=True)

    output_path = OUTPUT_DIR / "price_histories.csv"
    df.to_csv(output_path, index=False)
    print(f"  -> {output_path} ({len(df):,} rows, {df['market_id'].nunique():,} unique markets)")

    return df

def export_liquidity():
    """Export liquidity_metrics.csv"""
    print("Exporting liquidity_metrics.csv...")

    results = []

    # Process Polymarket orderbook
    pm_path = DATA_DIR / "orderbook_history_polymarket.json"
    if pm_path.exists():
        with open(pm_path) as f:
            pm_data = json.load(f)

        for market_id, market in pm_data.items():
            metrics_list = market.get('metrics', [])
            if not metrics_list:
                continue

            # Compute aggregate metrics
            spreads = [m['spread'] for m in metrics_list if m.get('spread') is not None]
            rel_spreads = [m['relative_spread'] for m in metrics_list if m.get('relative_spread') is not None]
            depths = [m['total_depth'] for m in metrics_list if m.get('total_depth') is not None]
            bid_depths = [m['bid_depth'] for m in metrics_list if m.get('bid_depth') is not None]
            ask_depths = [m['ask_depth'] for m in metrics_list if m.get('ask_depth') is not None]
            midpoints = [m['midpoint'] for m in metrics_list if m.get('midpoint') is not None]

            if not spreads:
                continue

            # Compute imbalance
            imbalances = []
            for m in metrics_list:
                bd, ad = m.get('bid_depth', 0), m.get('ask_depth', 0)
                if bd + ad > 0:
                    imbalances.append((bd - ad) / (bd + ad))

            # Time span
            timestamps = [m['timestamp'] for m in metrics_list if m.get('timestamp')]
            time_span_hours = (max(timestamps) - min(timestamps)) / 3600000 if len(timestamps) > 1 else 0

            results.append({
                'market_id': market_id,
                'token_id': market.get('token_id'),
                'platform': 'polymarket',
                'question': market.get('question'),
                'category': market.get('category'),
                # Spread metrics
                'spread_mean': sum(spreads) / len(spreads),
                'spread_median': sorted(spreads)[len(spreads)//2],
                'spread_std': pd.Series(spreads).std(),
                'spread_min': min(spreads),
                'spread_max': max(spreads),
                'rel_spread_mean': sum(rel_spreads) / len(rel_spreads) if rel_spreads else None,
                'rel_spread_median': sorted(rel_spreads)[len(rel_spreads)//2] if rel_spreads else None,
                # Depth metrics
                'depth_mean': sum(depths) / len(depths) if depths else None,
                'depth_median': sorted(depths)[len(depths)//2] if depths else None,
                'depth_max': max(depths) if depths else None,
                'bid_depth_mean': sum(bid_depths) / len(bid_depths) if bid_depths else None,
                'ask_depth_mean': sum(ask_depths) / len(ask_depths) if ask_depths else None,
                'depth_imbalance_mean': sum(imbalances) / len(imbalances) if imbalances else None,
                # Metadata
                'n_snapshots': market.get('n_snapshots', len(metrics_list)),
                'time_span_hours': time_span_hours,
                'volume_usd': market.get('volume_usd'),
                'price_mean': sum(midpoints) / len(midpoints) if midpoints else None,
                'price_std': pd.Series(midpoints).std() if midpoints else None,
            })

    # Process Kalshi orderbook
    k_path = DATA_DIR / "orderbook_history_kalshi.json"
    if k_path.exists():
        with open(k_path) as f:
            k_data = json.load(f)

        for market_id, market in k_data.items():
            metrics_list = market.get('metrics', [])
            if not metrics_list:
                continue

            spreads = [m['spread'] for m in metrics_list if m.get('spread') is not None]
            rel_spreads = [m['relative_spread'] for m in metrics_list if m.get('relative_spread') is not None]
            depths = [m['total_depth'] for m in metrics_list if m.get('total_depth') is not None]
            bid_depths = [m['bid_depth'] for m in metrics_list if m.get('bid_depth') is not None]
            ask_depths = [m['ask_depth'] for m in metrics_list if m.get('ask_depth') is not None]
            midpoints = [m['midpoint'] for m in metrics_list if m.get('midpoint') is not None]

            if not spreads:
                continue

            imbalances = []
            for m in metrics_list:
                bd, ad = m.get('bid_depth', 0), m.get('ask_depth', 0)
                if bd + ad > 0:
                    imbalances.append((bd - ad) / (bd + ad))

            timestamps = [m['timestamp'] for m in metrics_list if m.get('timestamp')]
            time_span_hours = (max(timestamps) - min(timestamps)) / 3600000 if len(timestamps) > 1 else 0

            results.append({
                'market_id': market_id,
                'token_id': market.get('ticker'),
                'platform': 'kalshi',
                'question': market.get('question'),
                'category': market.get('category'),
                'spread_mean': sum(spreads) / len(spreads),
                'spread_median': sorted(spreads)[len(spreads)//2],
                'spread_std': pd.Series(spreads).std(),
                'spread_min': min(spreads),
                'spread_max': max(spreads),
                'rel_spread_mean': sum(rel_spreads) / len(rel_spreads) if rel_spreads else None,
                'rel_spread_median': sorted(rel_spreads)[len(rel_spreads)//2] if rel_spreads else None,
                'depth_mean': sum(depths) / len(depths) if depths else None,
                'depth_median': sorted(depths)[len(depths)//2] if depths else None,
                'depth_max': max(depths) if depths else None,
                'bid_depth_mean': sum(bid_depths) / len(bid_depths) if bid_depths else None,
                'ask_depth_mean': sum(ask_depths) / len(ask_depths) if ask_depths else None,
                'depth_imbalance_mean': sum(imbalances) / len(imbalances) if imbalances else None,
                'n_snapshots': market.get('n_snapshots', len(metrics_list)),
                'time_span_hours': time_span_hours,
                'volume_usd': market.get('volume_usd'),
                'price_mean': sum(midpoints) / len(midpoints) if midpoints else None,
                'price_std': pd.Series(midpoints).std() if midpoints else None,
            })

    df = pd.DataFrame(results)
    output_path = OUTPUT_DIR / "liquidity_metrics.csv"
    df.to_csv(output_path, index=False)
    print(f"  -> {output_path} ({len(df):,} markets)")
    return df

def main():
    print("=" * 60)
    print("BELLWETHER DISTRIBUTION DATASET EXPORT")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Export each file
    master = export_master()
    prices = export_prices()
    liquidity = export_liquidity()

    print("\n" + "=" * 60)
    print("EXPORT COMPLETE")
    print("=" * 60)
    print(f"\nFiles saved to: {OUTPUT_DIR}")
    print(f"\nSummary:")
    print(f"  - political_markets_master.csv: {len(master):,} markets")
    print(f"  - price_histories.csv: {len(prices):,} price observations")
    print(f"  - liquidity_metrics.csv: {len(liquidity):,} markets with liquidity data")

    # Print file sizes
    print(f"\nFile sizes:")
    for f in OUTPUT_DIR.glob("*"):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  - {f.name}: {size_mb:.1f} MB")

if __name__ == "__main__":
    main()
