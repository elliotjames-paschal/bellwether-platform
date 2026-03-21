#!/usr/bin/env python3
"""
Bootstrap Orderbook Summary from Historical Data (One-Time)

Streams the large orderbook history files using ijson to avoid loading
the entire 3.7 GB into memory. Computes:
  - Per-market running stats (sum, sum_sq, min, max, count for each metric)
  - Per-date daily aggregates (median spread, median depth per platform)

Output: data/orderbook_summary.json (~1 MB)

This is a one-time script. After bootstrapping, fetch_orderbooks.py
updates the summary incrementally without loading the big files.

Requirements: pip install ijson numpy
"""

import json
import io
import os
import sys
import math
import ijson
import numpy as np
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import BASE_DIR, DATA_DIR

PM_FILE = DATA_DIR / "orderbook_history_polymarket.json"
KALSHI_FILE = DATA_DIR / "orderbook_history_kalshi.json"
SUMMARY_FILE = DATA_DIR / "orderbook_summary.json"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def safe_val(v):
    """Return value if finite number, else None."""
    if v is None:
        return None
    try:
        v = float(v)
        if math.isfinite(v):
            return v
    except (TypeError, ValueError):
        pass
    return None


def init_running_stat():
    return {"n": 0, "sum": 0.0, "sum_sq": 0.0, "min": None, "max": None}


def update_running_stat(stat, value):
    """Update running statistics with a new value."""
    if value is None:
        return
    stat["n"] += 1
    stat["sum"] += value
    stat["sum_sq"] += value * value
    if stat["min"] is None or value < stat["min"]:
        stat["min"] = value
    if stat["max"] is None or value > stat["max"]:
        stat["max"] = value


def process_history_file_streaming(filepath, platform, summary_markets, daily_data):
    """Stream one history file using ijson, processing one market at a time."""
    if not filepath.exists():
        log(f"  {filepath.name} not found, skipping")
        return 0

    log(f"  Streaming {filepath.name}...")
    id_field = 'token_id' if platform == 'polymarket' else 'ticker'
    count = 0
    total_snapshots = 0

    # ijson streams the top-level dict: each key is a market_id,
    # each value is the market object with metadata + metrics array.
    # We use ijson.items to get each top-level key-value pair.
    class NaNFilter(io.RawIOBase):
        """Wraps a binary file, replacing NaN with null on the fly."""
        def __init__(self, raw):
            self._raw = raw
        def readable(self):
            return True
        def readinto(self, b):
            data = self._raw.read(len(b))
            if not data:
                return 0
            # Replace NaN (not inside quotes) with null
            data = data.replace(b'NaN', b'null')
            n = len(data)
            b[:n] = data
            return n

    with open(filepath, 'rb') as raw_f:
        f = io.BufferedReader(NaNFilter(raw_f), buffer_size=1024*1024)
        for market_id, market in ijson.kvitems(f, ''):
            metrics_list = market.get('metrics', [])
            if not metrics_list:
                continue

            entry = {
                "platform": platform,
                "id_value": market.get(id_field, market_id),
                "question": str(market.get('question', ''))[:100],
                "category": str(market.get('category', '')),
                "volume_usd": market.get('volume_usd', 0) or 0,
                "n_snapshots": len(metrics_list),
                "first_timestamp": None,
                "last_timestamp": None,
                "spread": init_running_stat(),
                "rel_spread": init_running_stat(),
                "depth": init_running_stat(),
                "bid_depth": init_running_stat(),
                "ask_depth": init_running_stat(),
                "imbalance": init_running_stat(),
                "midpoint": init_running_stat(),
            }

            for m in metrics_list:
                ts = m.get('timestamp', 0)

                if ts:
                    if entry["first_timestamp"] is None or ts < entry["first_timestamp"]:
                        entry["first_timestamp"] = ts
                    if entry["last_timestamp"] is None or ts > entry["last_timestamp"]:
                        entry["last_timestamp"] = ts

                spread = safe_val(m.get('spread'))
                update_running_stat(entry["spread"], spread)

                rel_spread = safe_val(m.get('relative_spread'))
                update_running_stat(entry["rel_spread"], rel_spread)

                total_depth = safe_val(m.get('total_depth'))
                update_running_stat(entry["depth"], total_depth)

                bid_depth = safe_val(m.get('bid_depth'))
                update_running_stat(entry["bid_depth"], bid_depth)

                ask_depth = safe_val(m.get('ask_depth'))
                update_running_stat(entry["ask_depth"], ask_depth)

                midpoint = safe_val(m.get('midpoint'))
                update_running_stat(entry["midpoint"], midpoint)

                if bid_depth is not None and ask_depth is not None and (bid_depth + ask_depth) > 0:
                    imb = (bid_depth - ask_depth) / (bid_depth + ask_depth)
                    update_running_stat(entry["imbalance"], imb)

                # Daily aggregates
                if ts:
                    date_str = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
                    if date_str not in daily_data:
                        daily_data[date_str] = {}
                    if platform not in daily_data[date_str]:
                        daily_data[date_str][platform] = {"spreads": [], "depths": [], "n_snapshots": 0}

                    day = daily_data[date_str][platform]
                    day["n_snapshots"] += 1
                    if rel_spread is not None and rel_spread > 0:
                        day["spreads"].append(rel_spread * 100)
                    if total_depth is not None and total_depth > 0:
                        day["depths"].append(total_depth)

            summary_markets[market_id] = entry
            total_snapshots += len(metrics_list)
            count += 1

            if count % 500 == 0:
                log(f"    Progress: {count:,} markets, {total_snapshots:,} snapshots")

    log(f"  Done: {count:,} markets, {total_snapshots:,} snapshots")
    return count


def finalize_daily(daily_data):
    """Convert daily raw lists to median stats, then free the raw lists."""
    result = {}
    for date_str, platforms in sorted(daily_data.items()):
        result[date_str] = {}
        for platform, day in platforms.items():
            entry = {"n_snapshots": day["n_snapshots"]}
            if day["spreads"]:
                entry["spread_median"] = round(float(np.median(day["spreads"])), 2)
            if day["depths"]:
                entry["depth_median"] = round(float(np.median(day["depths"])), 0)
            result[date_str][platform] = entry
    return result


def main():
    log("=" * 60)
    log("BOOTSTRAPPING ORDERBOOK SUMMARY (streaming)")
    log("=" * 60)

    summary_markets = {}
    daily_data = {}

    log("\n1. Processing Polymarket history...")
    pm_count = process_history_file_streaming(PM_FILE, "polymarket", summary_markets, daily_data)

    log("\n2. Processing Kalshi history...")
    k_count = process_history_file_streaming(KALSHI_FILE, "kalshi", summary_markets, daily_data)

    log("\n3. Finalizing daily aggregates...")
    daily_final = finalize_daily(daily_data)
    # Free raw daily data
    del daily_data
    log(f"  {len(daily_final)} days of data")

    log("\n4. Saving summary...")
    summary = {
        "version": 1,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "markets": summary_markets,
        "daily": daily_final,
    }

    with open(SUMMARY_FILE, 'w') as f:
        json.dump(summary, f)

    size_mb = SUMMARY_FILE.stat().st_size / 1024 / 1024
    log(f"  Saved {SUMMARY_FILE.name} ({size_mb:.1f} MB)")

    log("\n" + "=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log(f"Markets: {pm_count:,} PM + {k_count:,} Kalshi = {pm_count + k_count:,} total")
    log(f"Days: {len(daily_final)}")
    total_snapshots = sum(m['n_snapshots'] for m in summary_markets.values())
    log(f"Total snapshots: {total_snapshots:,}")
    log("\nDone! fetch_orderbooks.py will now use this summary incrementally.")


if __name__ == "__main__":
    main()
