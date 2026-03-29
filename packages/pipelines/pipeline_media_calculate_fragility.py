#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Calculate Citation Fragility Metrics
================================================================================

For each matched citation, calculate fragility metrics at the time of citation:
  1. Thin market detection (volume, orderbook depth, spread)
  2. Price volatility around citation (±1h, ±6h, ±24h windows)

Produces a composite fragility score (0-100) and tier assignment.

Input:  data/media_citations_matched.json
Input:  data/kalshi_all_political_prices_CORRECTED_v3.json
Input:  data/polymarket_all_political_prices_CORRECTED.json
Input:  data/orderbook_summary.json
Output: data/media_citations_with_fragility.json
================================================================================
"""

import json
import logging
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Configuration ───────────────────────────────────────────────────────────

MATCHED_FILE = DATA_DIR / "media_citations_matched.json"
OUTPUT_FILE = DATA_DIR / "media_citations_with_fragility.json"

# Price history files
KALSHI_PRICES_FILE = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"
PM_PRICES_FILE = DATA_DIR / "polymarket_all_political_prices_CORRECTED.json"

# Orderbook summary
ORDERBOOK_FILE = DATA_DIR / "orderbook_summary.json"

# Fragility score weights
WEIGHT_VOLUME = 0.30
WEIGHT_DEPTH = 0.30
WEIGHT_SPREAD = 0.20
WEIGHT_VOLATILITY = 0.20

# Saturation points for log scaling
VOLUME_SATURATION = 10_000_000  # $10M
DEPTH_SATURATION = 500_000      # $500K cost_to_move_5c

# Tier thresholds (same as generate_monitor_data.py)
TIER1_THRESHOLD = 100_000  # $100K = Reportable
TIER2_THRESHOLD = 10_000   # $10K  = Caution

# Volatility windows (hours)
VOLATILITY_WINDOWS = [1, 6, 24]

# Default fragility for missing data
DEFAULT_FRAGILITY_MISSING = 75


# ─── Price History Helpers ────────────────────────────────────────────────────

def load_price_history():
    """Load price history files. Returns (kalshi_prices, pm_prices)."""
    kalshi_prices = {}
    pm_prices = {}

    if KALSHI_PRICES_FILE.exists():
        logger.info(f"Loading Kalshi prices from {KALSHI_PRICES_FILE.name}...")
        try:
            with open(KALSHI_PRICES_FILE, "r") as f:
                kalshi_prices = json.load(f)
            logger.info(f"  Loaded prices for {len(kalshi_prices)} Kalshi markets")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load Kalshi prices: {e}")
    else:
        logger.warning(f"Kalshi prices not found: {KALSHI_PRICES_FILE}")

    if PM_PRICES_FILE.exists():
        logger.info(f"Loading Polymarket prices from {PM_PRICES_FILE.name}...")
        try:
            with open(PM_PRICES_FILE, "r") as f:
                pm_prices = json.load(f)
            logger.info(f"  Loaded prices for {len(pm_prices)} Polymarket markets")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load Polymarket prices: {e}")
    else:
        logger.warning(f"Polymarket prices not found: {PM_PRICES_FILE}")

    return kalshi_prices, pm_prices


def load_orderbook_summary():
    """Load orderbook summary for liquidity metrics."""
    if not ORDERBOOK_FILE.exists():
        logger.warning(f"Orderbook summary not found: {ORDERBOOK_FILE}")
        return {}

    try:
        with open(ORDERBOOK_FILE, "r") as f:
            data = json.load(f)
        logger.info(f"Loaded orderbook summary: {len(data)} entries")
        return data
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load orderbook summary: {e}")
        return {}


def parse_price_timestamp(ts):
    """Parse a price timestamp to datetime. Handles epoch seconds and ISO strings."""
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(ts, str):
        try:
            # Try ISO format
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            pass
        try:
            # Try epoch string
            return datetime.fromtimestamp(float(ts), tz=timezone.utc)
        except (ValueError, OSError):
            pass
    return None


def get_price_series(market_id, platform, kalshi_prices, pm_prices):
    """
    Get the price time series for a market.
    Returns list of (datetime, price) tuples sorted by time.
    """
    raw_series = None

    if platform == "kalshi" and market_id in kalshi_prices:
        raw_series = kalshi_prices[market_id]
    elif platform == "polymarket" and market_id in pm_prices:
        raw_series = pm_prices[market_id]

    if not raw_series:
        return []

    # Parse into (datetime, price) pairs
    parsed = []
    for point in raw_series:
        # Handle different formats: {t, p} or {timestamp, price} or [t, p]
        if isinstance(point, dict):
            ts = point.get("t") or point.get("timestamp") or point.get("ts")
            price = point.get("p") or point.get("price") or point.get("yes_price")
        elif isinstance(point, (list, tuple)) and len(point) >= 2:
            ts, price = point[0], point[1]
        else:
            continue

        dt = parse_price_timestamp(ts)
        if dt and price is not None:
            try:
                p = float(price)
                # Normalize: if price > 1, assume cents
                if p > 1:
                    p = p / 100.0
                parsed.append((dt, p))
            except (ValueError, TypeError):
                continue

    parsed.sort(key=lambda x: x[0])
    return parsed


def find_price_at_time(series, target_dt):
    """Find the closest price to target_dt. Returns (price, time_delta_hours)."""
    if not series:
        return None, None

    best_price = None
    best_delta = float("inf")

    for dt, price in series:
        delta = abs((dt - target_dt).total_seconds()) / 3600.0
        if delta < best_delta:
            best_delta = delta
            best_price = price

    return best_price, best_delta


# ─── Fragility Calculations ──────────────────────────────────────────────────

def calculate_price_volatility(series, citation_dt, windows=VOLATILITY_WINDOWS):
    """
    Calculate price movement in windows around citation time.

    Returns dict of {window_hours: {delta_before, delta_after, max_swing, price_at_citation}}.
    """
    if not series:
        return None

    price_at_citation, _ = find_price_at_time(series, citation_dt)
    if price_at_citation is None:
        return None

    result = {}
    for window_h in windows:
        window_td = timedelta(hours=window_h)
        before_dt = citation_dt - window_td
        after_dt = citation_dt + window_td

        price_before, _ = find_price_at_time(series, before_dt)
        price_after, _ = find_price_at_time(series, after_dt)

        # Calculate deltas
        delta_before = None
        delta_after = None
        if price_before is not None:
            delta_before = round(price_at_citation - price_before, 4)
        if price_after is not None:
            delta_after = round(price_after - price_at_citation, 4)

        # Max swing: find max price range in the window
        prices_in_window = [
            p for dt, p in series
            if before_dt <= dt <= after_dt
        ]
        max_swing = None
        if prices_in_window:
            max_swing = round(max(prices_in_window) - min(prices_in_window), 4)

        result[f"{window_h}h"] = {
            "price_before": price_before,
            "price_after": price_after,
            "delta_before": delta_before,
            "delta_after": delta_after,
            "max_swing": max_swing,
        }

    result["price_at_citation"] = price_at_citation
    return result


def compute_fragility_score(volume_usd, cost_to_move_5c, spread, volatility_24h):
    """
    Compute composite fragility score (0-100).
    Higher = more fragile.

    Components (each 0-100, then weighted):
      - Volume: log-scaled, saturates at VOLUME_SATURATION
      - Depth: log-scaled cost_to_move_5c, saturates at DEPTH_SATURATION
      - Spread: linear, 0% = 0, 10%+ = 100
      - Volatility: 24h max_swing, 0% = 0, 20%+ = 100
    """
    # Volume component (inverted: low volume = high fragility)
    if volume_usd is not None and volume_usd > 0:
        vol_ratio = math.log10(volume_usd + 1) / math.log10(VOLUME_SATURATION)
        volume_score = max(0, min(100, 100 - vol_ratio * 100))
    else:
        volume_score = 100  # No volume = maximally fragile

    # Depth component (inverted: low depth = high fragility)
    if cost_to_move_5c is not None and cost_to_move_5c > 0:
        depth_ratio = math.log10(cost_to_move_5c + 1) / math.log10(DEPTH_SATURATION)
        depth_score = max(0, min(100, 100 - depth_ratio * 100))
    else:
        depth_score = 100  # No orderbook = maximally fragile

    # Spread component
    if spread is not None:
        spread_score = min(100, abs(spread) * 1000)
    else:
        spread_score = 50  # Unknown spread = moderate

    # Volatility component
    if volatility_24h is not None:
        volatility_score = min(100, abs(volatility_24h) * 500)
    else:
        volatility_score = 50  # Unknown volatility = moderate

    # Weighted composite
    composite = (
        WEIGHT_VOLUME * volume_score +
        WEIGHT_DEPTH * depth_score +
        WEIGHT_SPREAD * spread_score +
        WEIGHT_VOLATILITY * volatility_score
    )

    return {
        "fragility_score": round(composite),
        "components": {
            "volume_score": round(volume_score, 1),
            "depth_score": round(depth_score, 1),
            "spread_score": round(spread_score, 1),
            "volatility_score": round(volatility_score, 1),
        },
    }


def assign_tier(cost_to_move_5c):
    """Assign reportability tier based on orderbook depth."""
    if cost_to_move_5c is not None and cost_to_move_5c >= TIER1_THRESHOLD:
        return 1, "Reportable"
    elif cost_to_move_5c is not None and cost_to_move_5c >= TIER2_THRESHOLD:
        return 2, "Caution"
    else:
        return 3, "Fragile"


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("MEDIA CITATION: CALCULATE FRAGILITY")
    logger.info("=" * 60)

    # Load matched citations
    if not MATCHED_FILE.exists():
        logger.error(f"Matched citations not found: {MATCHED_FILE}")
        return 1

    matched_data = json.loads(MATCHED_FILE.read_text(encoding="utf-8"))
    citations = matched_data.get("citations", [])
    logger.info(f"Loaded {len(citations)} citations")

    # Load price history
    kalshi_prices, pm_prices = load_price_history()

    # Load orderbook summary
    orderbook = load_orderbook_summary()

    # Process each citation
    scored_count = 0
    skipped_count = 0

    for i, citation in enumerate(citations):
        if i % 100 == 0 and i > 0:
            logger.info(f"Progress: {i}/{len(citations)} citations processed")

        refs = citation.get("market_references", [])
        if not refs:
            continue

        # Parse citation date
        pub_date_str = citation.get("published_date", "")
        try:
            citation_dt = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            citation_dt = None

        for ref in refs:
            matched = ref.get("matched_market")
            if not matched:
                continue

            # Get market identifiers
            k_ticker = matched.get("k_ticker", "")
            pm_token = matched.get("pm_token_id", "")
            platform = matched.get("platform", "")
            volume = matched.get("total_volume", 0) or 0

            # Look up orderbook metrics
            ob_key = k_ticker or pm_token
            ob_data = orderbook.get(ob_key, {})
            cost_to_move = ob_data.get("cost_to_move_5c")
            spread = ob_data.get("rel_spread_mean") or ob_data.get("spread_mean")

            # Calculate price volatility
            volatility = None
            if citation_dt:
                # Get price series for this market
                price_id = k_ticker if platform == "kalshi" else pm_token
                series = get_price_series(price_id, platform, kalshi_prices, pm_prices)

                if series:
                    volatility = calculate_price_volatility(series, citation_dt)

            # Get 24h max swing for fragility scoring
            max_swing_24h = None
            if volatility and "24h" in volatility:
                max_swing_24h = volatility["24h"].get("max_swing")

            # Compute fragility score
            fragility = compute_fragility_score(volume, cost_to_move, spread, max_swing_24h)

            # Assign tier
            tier, tier_label = assign_tier(cost_to_move)

            # Attach to reference
            matched["fragility"] = {
                **fragility,
                "price_tier": tier,
                "tier_label": tier_label,
                "volume_usd": volume,
                "cost_to_move_5c": cost_to_move,
                "spread_mean": spread,
            }

            if volatility:
                matched["volatility"] = volatility

            # Set price at citation time
            if volatility and "price_at_citation" in volatility:
                matched["price_at_citation"] = volatility["price_at_citation"]

            scored_count += 1

    logger.info(f"Scored {scored_count} market references, {skipped_count} skipped")

    # Save output
    output = {
        "citations": citations,
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_citations": len(citations),
            "scored_references": scored_count,
            "fragility_weights": {
                "volume": WEIGHT_VOLUME,
                "depth": WEIGHT_DEPTH,
                "spread": WEIGHT_SPREAD,
                "volatility": WEIGHT_VOLATILITY,
            },
        },
    }

    atomic_write_json(OUTPUT_FILE, output, indent=2, ensure_ascii=False)
    logger.info(f"Saved to {OUTPUT_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
