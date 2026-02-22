#!/usr/bin/env python3
"""
================================================================================
PIPELINE TEST SCRIPT: Validate All Components Before Full Deployment
================================================================================

This script tests each pipeline component individually to ensure everything
works before running the full pipeline.

Usage:
    python test_pipeline_components.py [--test-gpt] [--test-api] [--test-all]

Options:
    --test-gpt   Test OpenAI API calls (costs money, but minimal)
    --test-api   Test Dome API calls
    --test-all   Run all tests including GPT

================================================================================
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime

# Add scripts directory to path
BASE_DIR = Path("/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi")
DATA_DIR = BASE_DIR / "data"
SCRIPTS_DIR = BASE_DIR / "scripts"

# Test results
results = {}

def log(msg, level="INFO"):
    """Print timestamped log message."""
    symbol = {"INFO": "ℹ", "OK": "✓", "FAIL": "✗", "WARN": "⚠"}.get(level, "•")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {symbol} {msg}")

def test_passed(name):
    results[name] = True
    log(f"{name}: PASSED", "OK")

def test_failed(name, error):
    results[name] = False
    log(f"{name}: FAILED - {error}", "FAIL")

# =============================================================================
# TEST 1: Check all required files exist
# =============================================================================

def test_required_files():
    """Test that all required input files exist."""
    log("Testing required files...")

    required_files = [
        ("Master CSV", DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"),
        ("Election dates", DATA_DIR / "election_dates_lookup.csv"),
        ("PM prices", DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json"),
        ("Kalshi prices", DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"),
    ]

    all_exist = True
    for name, path in required_files:
        if path.exists():
            size = path.stat().st_size / (1024*1024)
            log(f"  {name}: {size:.1f} MB", "OK")
        else:
            log(f"  {name}: MISSING", "FAIL")
            all_exist = False

    if all_exist:
        test_passed("required_files")
    else:
        test_failed("required_files", "Some files missing")

# =============================================================================
# TEST 2: Check all pipeline scripts exist and are valid Python
# =============================================================================

def test_pipeline_scripts():
    """Test that all pipeline scripts exist and have valid syntax."""
    log("Testing pipeline scripts...")

    scripts = [
        "pipeline_classify_kalshi_events.py",
        "pipeline_discover_markets.py",
        "pipeline_check_resolutions.py",
        "pipeline_classify_categories.py",
        "pipeline_classify_electoral.py",
        "pipeline_get_election_dates.py",
        "pipeline_merge_to_master.py",
        "pull_domeapi_prices_incremental.py",
        "pull_domeapi_prices_kalshi.py",
        "truncate_domeapi_prices.py",
        "truncate_kalshi_prices.py",
        "calculate_all_political_brier_scores.py",
        "generate_web_data.py",
    ]

    all_valid = True
    for script in scripts:
        path = SCRIPTS_DIR / script
        if not path.exists():
            log(f"  {script}: MISSING", "FAIL")
            all_valid = False
            continue

        # Check syntax by compiling
        try:
            with open(path, 'r') as f:
                code = f.read()
            compile(code, script, 'exec')
            log(f"  {script}: OK", "OK")
        except SyntaxError as e:
            log(f"  {script}: SYNTAX ERROR line {e.lineno}", "FAIL")
            all_valid = False

    if all_valid:
        test_passed("pipeline_scripts")
    else:
        test_failed("pipeline_scripts", "Some scripts invalid")

# =============================================================================
# TEST 3: Test Dome API connection
# =============================================================================

def test_dome_api():
    """Test Dome API connection and authentication."""
    log("Testing Dome API connection...")

    try:
        import requests

        from config import get_dome_api_key
        api_key = get_dome_api_key().replace('Bearer ', '')
        if not api_key.startswith('Bearer '):
            api_key = f"Bearer {api_key}"

        # Test Polymarket endpoint
        response = requests.get(
            "https://api.domeapi.io/v1/polymarket/markets",
            headers={"Authorization": api_key},
            params={"limit": 1},
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            markets = data.get("markets", [])
            log(f"  Polymarket API: OK (got {len(markets)} market)", "OK")
        else:
            log(f"  Polymarket API: HTTP {response.status_code}", "FAIL")
            test_failed("dome_api", f"HTTP {response.status_code}")
            return

        # Test Kalshi endpoint
        response = requests.get(
            "https://api.domeapi.io/v1/kalshi/markets",
            headers={"Authorization": api_key},
            params={"limit": 1},
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            markets = data.get("markets", [])
            log(f"  Kalshi API: OK (got {len(markets)} market)", "OK")
            test_passed("dome_api")
        else:
            log(f"  Kalshi API: HTTP {response.status_code}", "FAIL")
            test_failed("dome_api", f"HTTP {response.status_code}")

    except Exception as e:
        test_failed("dome_api", str(e))

# =============================================================================
# TEST 4: Test OpenAI API connection
# =============================================================================

def test_openai_api():
    """Test OpenAI API connection with a minimal call."""
    log("Testing OpenAI API connection...")

    try:
        from openai import OpenAI

        # Get API key
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            key_file = BASE_DIR / "openai_api_key.txt"
            if key_file.exists():
                with open(key_file, 'r') as f:
                    api_key = f.read().strip()

        if not api_key:
            log("  No OPENAI_API_KEY found", "WARN")
            test_failed("openai_api", "No API key")
            return

        client = OpenAI(api_key=api_key)

        # Minimal test call
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Reply with just 'OK'"}],
            max_tokens=5,
            temperature=0
        )

        reply = response.choices[0].message.content.strip()
        log(f"  OpenAI API: OK (response: {reply})", "OK")
        test_passed("openai_api")

    except Exception as e:
        test_failed("openai_api", str(e))

# =============================================================================
# TEST 5: Test GPT classification prompt (actual classification)
# =============================================================================

def test_gpt_classification():
    """Test GPT classification with sample data."""
    log("Testing GPT classification logic...")

    try:
        from openai import OpenAI

        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            key_file = BASE_DIR / "openai_api_key.txt"
            if key_file.exists():
                with open(key_file, 'r') as f:
                    api_key = f.read().strip()

        if not api_key:
            log("  No OPENAI_API_KEY found", "WARN")
            test_failed("gpt_classification", "No API key")
            return

        client = OpenAI(api_key=api_key)

        # Test Kalshi event_ticker classification
        test_prompt = """You are classifying Kalshi prediction market event tickers.
Determine if each event_ticker represents a POLITICAL market.

POLITICAL markets include elections, government policy, legislative actions, etc.
NOT POLITICAL: sports, entertainment, crypto, weather, etc.

Return JSON: {"results": [{"index": 0, "is_political": true}, ...]}

Classify these:
0. event_ticker="KXSENATE-OH-24" sample_title="Which party wins Ohio Senate?"
1. event_ticker="INXBTC-25JAN" sample_title="Bitcoin price above $100k?"
2. event_ticker="KXFED-25JAN" sample_title="Will the Fed cut rates?"
"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": test_prompt}],
            max_tokens=200,
            temperature=0,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        classifications = result.get("results", [])

        # Verify expected results
        expected = {0: True, 1: False, 2: True}  # Senate=political, BTC=not, Fed=political
        correct = 0
        for c in classifications:
            idx = c.get("index")
            is_pol = c.get("is_political")
            exp = expected.get(idx)
            if is_pol == exp:
                correct += 1
                log(f"  Sample {idx}: {is_pol} (expected {exp}) ✓", "OK")
            else:
                log(f"  Sample {idx}: {is_pol} (expected {exp}) ✗", "FAIL")

        if correct == 3:
            test_passed("gpt_classification")
        else:
            test_failed("gpt_classification", f"Only {correct}/3 correct")

    except Exception as e:
        test_failed("gpt_classification", str(e))

# =============================================================================
# TEST 6: Test data loading and parsing
# =============================================================================

def test_data_loading():
    """Test that data files can be loaded and parsed correctly."""
    log("Testing data loading...")

    try:
        import pandas as pd

        # Test master CSV
        master_file = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
        df = pd.read_csv(master_file, low_memory=False)
        log(f"  Master CSV: {len(df):,} rows, {len(df.columns)} columns", "OK")

        # Check required columns
        required_cols = ['platform', 'market_id', 'question', 'political_category']
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            log(f"  Missing columns: {missing}", "FAIL")
            test_failed("data_loading", f"Missing columns: {missing}")
            return

        # Test price JSON
        pm_prices_file = DATA_DIR / "polymarket_all_political_prices_DOMEAPI_CORRECTED.json"
        with open(pm_prices_file, 'r') as f:
            pm_prices = json.load(f)
        log(f"  PM prices: {len(pm_prices):,} tokens", "OK")

        kalshi_prices_file = DATA_DIR / "kalshi_all_political_prices_CORRECTED_v3.json"
        with open(kalshi_prices_file, 'r') as f:
            kalshi_prices = json.load(f)
        log(f"  Kalshi prices: {len(kalshi_prices):,} tickers", "OK")

        # Verify price data structure
        sample_token = list(pm_prices.keys())[0] if pm_prices else None
        if sample_token:
            sample_prices = pm_prices[sample_token]
            if sample_prices and isinstance(sample_prices, list):
                sample = sample_prices[0]
                if 't' in sample and 'p' in sample:
                    log(f"  PM price format: OK (t={sample['t']}, p={sample['p']})", "OK")
                else:
                    log(f"  PM price format: unexpected keys {sample.keys()}", "WARN")

        test_passed("data_loading")

    except Exception as e:
        test_failed("data_loading", str(e))

# =============================================================================
# TEST 7: Test truncation logic
# =============================================================================

def test_truncation_logic():
    """Test that truncation logic works correctly."""
    log("Testing truncation logic...")

    try:
        import pandas as pd
        from datetime import datetime, timezone, timedelta

        # Simulate truncation logic
        test_cases = [
            {
                "name": "Election market",
                "trading_close": "2024-11-06T12:00:00Z",
                "election_date": "2024-11-05",
                "expected_cutoff": "2024-11-05T23:59:59",  # End of election day
            },
            {
                "name": "Non-election PM",
                "trading_close": "2025-01-15T12:00:00Z",
                "election_date": None,
                "expected_cutoff": "2025-01-14T12:00:00",  # close - 24h
            },
            {
                "name": "Non-election Kalshi",
                "trading_close": "2025-01-15T12:00:00Z",
                "election_date": None,
                "expected_cutoff": "2025-01-15T00:00:00",  # close - 12h
                "platform": "Kalshi"
            },
        ]

        all_pass = True
        for tc in test_cases:
            close_dt = pd.to_datetime(tc["trading_close"])
            if tc["election_date"]:
                # Election: end of election day
                ed = pd.to_datetime(tc["election_date"])
                cutoff = datetime(ed.year, ed.month, ed.day, 23, 59, 59, tzinfo=timezone.utc)
            else:
                # Non-election: close - 24h (PM) or close - 12h (Kalshi)
                hours = 12 if tc.get("platform") == "Kalshi" else 24
                cutoff = close_dt - timedelta(hours=hours)

            expected = pd.to_datetime(tc["expected_cutoff"])

            # Compare (ignore timezone for simplicity)
            if cutoff.replace(tzinfo=None) == expected.replace(tzinfo=None):
                log(f"  {tc['name']}: OK", "OK")
            else:
                log(f"  {tc['name']}: FAIL (got {cutoff}, expected {expected})", "FAIL")
                all_pass = False

        if all_pass:
            test_passed("truncation_logic")
        else:
            test_failed("truncation_logic", "Some cases failed")

    except Exception as e:
        test_failed("truncation_logic", str(e))

# =============================================================================
# TEST 8: Test Brier score calculation
# =============================================================================

def test_brier_calculation():
    """Test Brier score calculation logic."""
    log("Testing Brier score calculation...")

    try:
        # Brier score = (prediction - outcome)^2
        test_cases = [
            {"prediction": 0.9, "outcome": 1, "expected": 0.01},   # Confident correct
            {"prediction": 0.1, "outcome": 0, "expected": 0.01},   # Confident correct
            {"prediction": 0.9, "outcome": 0, "expected": 0.81},   # Confident wrong
            {"prediction": 0.5, "outcome": 1, "expected": 0.25},   # Uncertain
            {"prediction": 0.5, "outcome": 0, "expected": 0.25},   # Uncertain
        ]

        all_pass = True
        for tc in test_cases:
            brier = (tc["prediction"] - tc["outcome"]) ** 2
            if abs(brier - tc["expected"]) < 0.001:
                log(f"  p={tc['prediction']}, o={tc['outcome']}: {brier:.3f} ✓", "OK")
            else:
                log(f"  p={tc['prediction']}, o={tc['outcome']}: {brier:.3f} (expected {tc['expected']}) ✗", "FAIL")
                all_pass = False

        if all_pass:
            test_passed("brier_calculation")
        else:
            test_failed("brier_calculation", "Some cases failed")

    except Exception as e:
        test_failed("brier_calculation", str(e))

# =============================================================================
# MAIN
# =============================================================================

def main():
    test_gpt = "--test-gpt" in sys.argv or "--test-all" in sys.argv
    test_api = "--test-api" in sys.argv or "--test-all" in sys.argv

    print("\n" + "=" * 70)
    print("PIPELINE COMPONENT TESTS")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Options: test_gpt={test_gpt}, test_api={test_api}")
    print("=" * 70 + "\n")

    # Always run these tests
    test_required_files()
    test_pipeline_scripts()
    test_data_loading()
    test_truncation_logic()
    test_brier_calculation()

    # Optional API tests
    if test_api:
        test_dome_api()
    else:
        log("Skipping Dome API test (use --test-api to enable)", "WARN")

    if test_gpt:
        test_openai_api()
        test_gpt_classification()
    else:
        log("Skipping GPT tests (use --test-gpt to enable)", "WARN")

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    passed = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)

    for name, passed_test in results.items():
        status = "PASS" if passed_test else "FAIL"
        print(f"  {name}: {status}")

    print(f"\nTotal: {passed} passed, {failed} failed")
    print("=" * 70 + "\n")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
