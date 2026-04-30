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
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from config import DATA_DIR, atomic_write_json

# ─── Import shared logic from media_pipeline.core ───────────────────────────
from media_pipeline.core import (
    # Fragility constants
    WEIGHT_VOLUME,
    WEIGHT_DEPTH,
    WEIGHT_SPREAD,
    WEIGHT_VOLATILITY,
    VOLUME_SATURATION,
    DEPTH_SATURATION,
    TIER1_THRESHOLD,
    TIER2_THRESHOLD,
    VOLATILITY_WINDOWS,
    DEFAULT_FRAGILITY_MISSING,
    # Fragility functions
    parse_price_timestamp,
    find_price_at_time,
    calculate_price_volatility,
    compute_fragility_score,
    estimate_depth_from_volume,
    assign_tier,
)

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


# ─── I/O Functions (stay in this script) ─────────────────────────────────────

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
    """Load orderbook summary for liquidity metrics.

    The summary file uses a nested format: {"version": ..., "markets": {market_id: {...}}, ...}.
    We extract the per-market data and flatten running stats into simple values.
    """
    if not ORDERBOOK_FILE.exists():
        logger.warning(f"Orderbook summary not found: {ORDERBOOK_FILE} — will use inline market liquidity data")
        return {}

    try:
        with open(ORDERBOOK_FILE, "r") as f:
            raw = json.load(f)

        # Handle nested format from bootstrap_orderbook_summary.py
        markets = raw.get("markets", raw) if isinstance(raw, dict) else {}
        if not markets or (isinstance(raw, dict) and "version" in raw and not raw.get("markets")):
            logger.warning("Orderbook summary is empty or has no market data")
            return {}

        # Flatten running stats into simple mean values
        result = {}
        for market_id, entry in markets.items():
            flat = {}
            # Extract mean depth as cost_to_move_5c proxy
            depth_stat = entry.get("depth", {})
            if isinstance(depth_stat, dict) and depth_stat.get("n", 0) > 0:
                flat["cost_to_move_5c"] = depth_stat["sum"] / depth_stat["n"]
            elif isinstance(depth_stat, (int, float)):
                flat["cost_to_move_5c"] = depth_stat

            # Extract mean relative spread
            spread_stat = entry.get("rel_spread", {})
            if isinstance(spread_stat, dict) and spread_stat.get("n", 0) > 0:
                flat["rel_spread_mean"] = spread_stat["sum"] / spread_stat["n"]
            elif isinstance(spread_stat, (int, float)):
                flat["rel_spread_mean"] = spread_stat

            # Also check for direct fields (in case summary format changes)
            if "cost_to_move_5c" not in flat:
                if "cost_to_move_5c" in entry:
                    flat["cost_to_move_5c"] = entry["cost_to_move_5c"]
            if "rel_spread_mean" not in flat:
                for key in ("rel_spread_mean", "spread_mean"):
                    if key in entry:
                        flat["rel_spread_mean"] = entry[key]
                        break

            if flat:
                result[market_id] = flat

        logger.info(f"Loaded orderbook summary: {len(result)} markets with liquidity data")
        return result
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load orderbook summary: {e}")
        return {}


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

            # Fallback 1: use inline liquidity data from enriched market (Kalshi)
            if cost_to_move is None:
                liq = matched.get("k_liquidity_dollars")
                if liq is not None:
                    try:
                        liq = float(liq)
                        if liq > 0 and not math.isnan(liq):
                            cost_to_move = liq
                    except (ValueError, TypeError):
                        pass

            # Fallback 2: estimate depth from volume (when no orderbook data)
            if cost_to_move is None and volume > 0:
                cost_to_move = estimate_depth_from_volume(volume)

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
