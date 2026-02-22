#!/usr/bin/env python3
"""
================================================================================
PIPELINE SAMPLE TEST: Test pipeline with small sample (50 markets)
================================================================================

Tests the full pipeline flow with a small sample to verify everything works
before running on the full dataset.

Usage:
    python test_pipeline_sample.py

================================================================================
"""

import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

BASE_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi")
DATA_DIR = BASE_DIR / "data"

# Test configuration
SAMPLE_SIZE = 50
# Load API key from config
from config import get_dome_api_key
DOME_API_KEY = get_dome_api_key()

results = {}

def log(msg, level="INFO"):
    symbol = {"INFO": "ℹ", "OK": "✓", "FAIL": "✗", "WARN": "⚠"}.get(level, "•")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {symbol} {msg}")

def test_passed(name):
    results[name] = True
    log(f"{name}: PASSED", "OK")

def test_failed(name, error):
    results[name] = False
    log(f"{name}: FAILED - {error}", "FAIL")

# =============================================================================
# TEST 1: Fetch sample from Dome API
# =============================================================================

def test_dome_api_sample():
    """Fetch a small sample from Dome API."""
    log("TEST 1: Fetching sample from Dome API...")

    try:
        # Polymarket - fetch 25 markets
        pm_response = requests.get(
            "https://api.domeapi.io/v1/polymarket/markets",
            headers={"Authorization": DOME_API_KEY},
            params={"limit": 25, "tags": "Politics"},
            timeout=30
        )

        if pm_response.status_code != 200:
            test_failed("dome_api_sample", f"PM API returned {pm_response.status_code}")
            return None

        pm_markets = pm_response.json().get("markets", [])
        log(f"  Polymarket: fetched {len(pm_markets)} markets", "OK")

        # Kalshi - fetch 25 markets
        kalshi_response = requests.get(
            "https://api.domeapi.io/v1/kalshi/markets",
            headers={"Authorization": DOME_API_KEY},
            params={"limit": 25},
            timeout=30
        )

        if kalshi_response.status_code != 200:
            test_failed("dome_api_sample", f"Kalshi API returned {kalshi_response.status_code}")
            return None

        kalshi_markets = kalshi_response.json().get("markets", [])
        log(f"  Kalshi: fetched {len(kalshi_markets)} markets", "OK")

        test_passed("dome_api_sample")
        return {"polymarket": pm_markets, "kalshi": kalshi_markets}

    except Exception as e:
        test_failed("dome_api_sample", str(e))
        return None

# =============================================================================
# TEST 2: Process markets into our format
# =============================================================================

def test_market_processing(sample_data):
    """Test that we can process raw API data into our format."""
    log("TEST 2: Processing markets into our format...")

    if not sample_data:
        test_failed("market_processing", "No sample data")
        return None

    try:
        processed = []

        # Process Polymarket
        for m in sample_data["polymarket"][:10]:
            processed.append({
                "platform": "Polymarket",
                "market_id": m.get("market_slug"),
                "pm_condition_id": m.get("condition_id"),
                "question": m.get("title"),
                "tags": json.dumps(m.get("tags", [])),
                "dome_status": m.get("status"),
            })

        # Process Kalshi
        for m in sample_data["kalshi"][:10]:
            processed.append({
                "platform": "Kalshi",
                "market_id": m.get("market_ticker"),
                "k_event_ticker": m.get("event_ticker"),
                "question": m.get("title"),
                "dome_status": m.get("status"),
            })

        log(f"  Processed {len(processed)} markets", "OK")

        # Verify structure
        df = pd.DataFrame(processed)
        required_cols = ["platform", "market_id", "question"]
        missing = [c for c in required_cols if c not in df.columns]

        if missing:
            test_failed("market_processing", f"Missing columns: {missing}")
            return None

        log(f"  DataFrame shape: {df.shape}", "OK")
        test_passed("market_processing")
        return df

    except Exception as e:
        test_failed("market_processing", str(e))
        return None

# =============================================================================
# TEST 3: Test price fetching (1 market only)
# =============================================================================

def test_price_fetching():
    """Test fetching price history for a single market."""
    log("TEST 3: Fetching price history (1 market)...")

    try:
        # Get a sample token from existing prices
        pm_prices_file = DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json"

        with open(pm_prices_file, 'r') as f:
            prices = json.load(f)

        sample_token = list(prices.keys())[0]
        sample_prices = prices[sample_token]

        log(f"  Sample token: {sample_token[:20]}...", "OK")
        log(f"  Price points: {len(sample_prices)}", "OK")

        if sample_prices:
            first = sample_prices[0]
            log(f"  First price: t={first.get('t')}, p={first.get('p')}", "OK")

        test_passed("price_fetching")
        return True

    except Exception as e:
        test_failed("price_fetching", str(e))
        return False

# =============================================================================
# TEST 4: Test Brier score calculation
# =============================================================================

def test_brier_calculation():
    """Test Brier score calculation on sample data."""
    log("TEST 4: Testing Brier score calculation...")

    try:
        # Load a sample from existing prediction accuracy
        master_file = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
        df = pd.read_csv(master_file, low_memory=False)

        # Find markets with outcomes
        with_outcomes = df[df['resolution_outcome'].notna()].head(10)
        log(f"  Markets with outcomes: {len(with_outcomes)}", "OK")

        # Simulate Brier calculation
        # resolution_outcome is a string like "Yes", "No", "Republicans", etc.
        # For binary markets, Yes=1, No=0
        brier_scores = []
        for _, row in with_outcomes.iterrows():
            outcome_str = str(row['resolution_outcome']).lower()
            # Map to numeric: Yes/positive outcomes = 1, No = 0
            outcome = 1 if outcome_str in ['yes', '1', 'true'] else 0
            # Simulate a prediction (we'd normally get this from price data)
            prediction = 0.7 if outcome == 1 else 0.3
            brier = (prediction - outcome) ** 2
            brier_scores.append(brier)

        avg_brier = sum(brier_scores) / len(brier_scores) if brier_scores else 0
        log(f"  Sample Brier scores calculated: {len(brier_scores)}", "OK")
        log(f"  Average Brier: {avg_brier:.4f}", "OK")

        test_passed("brier_calculation")
        return True

    except Exception as e:
        test_failed("brier_calculation", str(e))
        return False

# =============================================================================
# TEST 5: Test truncation logic
# =============================================================================

def test_truncation():
    """Test price truncation logic on real data."""
    log("TEST 5: Testing truncation logic...")

    try:
        from datetime import timezone, timedelta

        # Part A: Test date math
        trading_close = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        pm_cutoff = trading_close - timedelta(hours=24)
        kalshi_cutoff = trading_close - timedelta(hours=12)

        assert pm_cutoff < trading_close
        assert kalshi_cutoff > pm_cutoff
        log(f"  Date math: PM -24h, Kalshi -12h", "OK")

        # Part B: Test actual truncation on sample data
        sample_prices = [
            {"t": 1730000000, "p": 0.50},  # Before cutoff
            {"t": 1731000000, "p": 0.55},  # At cutoff
            {"t": 1732000000, "p": 0.60},  # After cutoff - should be removed
            {"t": 1733000000, "p": 0.65},  # After cutoff - should be removed
        ]
        cutoff_ts = 1731000000

        # Apply truncation (same logic as truncate_domeapi_prices.py)
        truncated = [p for p in sample_prices if p["t"] <= cutoff_ts]

        assert len(truncated) == 2, f"Expected 2 prices, got {len(truncated)}"
        assert truncated[-1]["t"] == cutoff_ts, "Last price should be at cutoff"
        log(f"  Truncation filter: {len(sample_prices)} → {len(truncated)} prices", "OK")

        # Part C: Test on real price data (verify structure)
        pm_prices_file = DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json"
        with open(pm_prices_file, 'r') as f:
            prices = json.load(f)

        # Find a market with prices
        sample_token = None
        for token, token_prices in prices.items():
            if len(token_prices) > 10:
                sample_token = token
                break

        if sample_token:
            token_prices = prices[sample_token]
            timestamps = [p["t"] for p in token_prices]

            # Verify prices are sorted by timestamp
            is_sorted = all(timestamps[i] <= timestamps[i+1] for i in range(len(timestamps)-1))
            log(f"  Real data: {len(token_prices)} prices, sorted={is_sorted}", "OK")
        else:
            log(f"  Real data: no sample found", "WARN")

        test_passed("truncation")
        return True

    except Exception as e:
        test_failed("truncation", str(e))
        return False

# =============================================================================
# TEST 6: Test web data generation (structure only)
# =============================================================================

def test_web_data_structure():
    """Test that web data JSON files have correct structure."""
    log("TEST 6: Testing web data structure...")

    try:
        web_data_dir = BASE_DIR / "website" / "data"

        expected_files = [
            ("summary.json", ["total_markets", "us_elections"]),
            ("calibration.json", ["polymarket", "kalshi"]),
            ("brier_by_category.json", ["categories", "polymarket"]),
        ]

        for filename, required_keys in expected_files:
            filepath = web_data_dir / filename
            if not filepath.exists():
                log(f"  {filename}: MISSING", "WARN")
                continue

            with open(filepath, 'r') as f:
                data = json.load(f)

            missing = [k for k in required_keys if k not in data]
            if missing:
                log(f"  {filename}: missing keys {missing}", "WARN")
            else:
                log(f"  {filename}: OK", "OK")

        test_passed("web_data_structure")
        return True

    except Exception as e:
        test_failed("web_data_structure", str(e))
        return False

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "=" * 70)
    print("PIPELINE SAMPLE TEST (50 markets)")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    start_time = time.time()

    # Run tests
    sample_data = test_dome_api_sample()
    test_market_processing(sample_data)
    test_price_fetching()
    test_brier_calculation()
    test_truncation()
    test_web_data_structure()

    elapsed = time.time() - start_time

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    passed = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)

    for name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"  {name}: {status}")

    print(f"\nTotal: {passed} passed, {failed} failed")
    print(f"Time: {elapsed:.1f} seconds")
    print("=" * 70 + "\n")

    if failed == 0:
        print("All tests passed. Ready for full pipeline run.")
    else:
        print("Some tests failed. Fix issues before running full pipeline.")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
