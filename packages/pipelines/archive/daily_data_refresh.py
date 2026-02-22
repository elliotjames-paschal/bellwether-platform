#!/usr/bin/env python3
"""
Daily Data Refresh Orchestrator

This script runs daily (via GitHub Actions cron) to:
1. Fetch new prices from Dome API (Polymarket + Kalshi)
2. Classify any new markets with OpenAI
3. Update master CSV
4. Run all analysis scripts
5. Generate JSON outputs for the dashboard
"""

import pandas as pd
import subprocess
import sys
import os
from datetime import datetime, timedelta
import json

# Paths
BASE_DIR = "/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
DATA_DIR = f"{BASE_DIR}/data"
SCRIPTS_DIR = f"{BASE_DIR}/scripts"
WEBSITE_DIR = f"{BASE_DIR}/website"

# Data files
MASTER_FILE = f"{DATA_DIR}/combined_political_markets_with_electoral_details_UPDATED.csv"
PM_PRICES_FILE = f"{DATA_DIR}/polymarket_all_political_prices_DOMEAPI_v1.json"
KALSHI_PRICES_FILE = f"{DATA_DIR}/kalshi_all_political_prices_DOMEAPI_v1.json"
LAST_UPDATE_FILE = f"{DATA_DIR}/last_update.json"

# Global cutoff - last known data date
DEFAULT_LAST_UPDATE = "2024-11-10"

def log(msg):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_script(script_name, description):
    """Run a Python script and return success status."""
    script_path = f"{SCRIPTS_DIR}/{script_name}"
    log(f"Running: {description}...")

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        if result.returncode == 0:
            log(f"  ✓ {description} complete")
            return True
        else:
            log(f"  ✗ {description} failed: {result.stderr[:200]}")
            return False
    except subprocess.TimeoutExpired:
        log(f"  ✗ {description} timed out")
        return False
    except Exception as e:
        log(f"  ✗ {description} error: {e}")
        return False

def get_last_update_date():
    """Get the date of the last successful update."""
    if os.path.exists(LAST_UPDATE_FILE):
        try:
            with open(LAST_UPDATE_FILE, 'r') as f:
                data = json.load(f)
                return data.get('last_update', DEFAULT_LAST_UPDATE)
        except:
            pass
    return DEFAULT_LAST_UPDATE

def save_last_update_date():
    """Save today's date as the last update."""
    with open(LAST_UPDATE_FILE, 'w') as f:
        json.dump({
            'last_update': datetime.now().strftime('%Y-%m-%d'),
            'updated_at': datetime.now().isoformat()
        }, f, indent=2)

def step1_fetch_prices():
    """Step 1: Fetch new price data from Dome API."""
    log("="*60)
    log("STEP 1: FETCHING PRICE DATA")
    log("="*60)

    last_update = get_last_update_date()
    today = datetime.now().strftime('%Y-%m-%d')

    log(f"Last update: {last_update}")
    log(f"Today: {today}")
    log(f"Fetching data from {last_update} to {today}")

    # Set environment variables for the fetch scripts
    os.environ['FETCH_START_DATE'] = last_update
    os.environ['FETCH_END_DATE'] = today
    os.environ['INCREMENTAL_MODE'] = 'true'

    # Fetch Polymarket prices
    success_pm = run_script(
        "pull_domeapi_prices_incremental.py",
        "Fetch Polymarket prices from Dome API"
    )

    # Fetch Kalshi prices
    success_kalshi = run_script(
        "pull_domeapi_prices_kalshi.py",
        "Fetch Kalshi prices from Dome API"
    )

    return success_pm or success_kalshi  # Continue if at least one succeeded

def step2_classify_new_markets():
    """Step 2: Classify any new markets with OpenAI."""
    log("="*60)
    log("STEP 2: CLASSIFYING NEW MARKETS")
    log("="*60)

    # Check if OPENAI_API_KEY is set
    if not os.getenv('OPENAI_API_KEY'):
        log("  ⚠ OPENAI_API_KEY not set, skipping classification")
        return True

    # This will only classify markets not already in the labeled file
    return run_script(
        "classify_new_markets.py",
        "Classify new electoral markets with OpenAI"
    )

def step3_calculate_predictions():
    """Step 3: Calculate prediction accuracy metrics."""
    log("="*60)
    log("STEP 3: CALCULATING PREDICTIONS")
    log("="*60)

    return run_script(
        "calculate_all_political_brier_scores.py",
        "Calculate Brier scores for all markets"
    )

def step4_run_analyses():
    """Step 4: Run all analysis scripts."""
    log("="*60)
    log("STEP 4: RUNNING ANALYSES")
    log("="*60)

    analyses = [
        ("create_brier_cohorts.py", "Create Brier cohorts"),
        ("brier_score_analysis.py", "Brier score analysis"),
        ("table_4_brier_by_election_type.py", "Brier by election type"),
        ("calibration_density_plots.py", "Calibration density plots"),
        ("calibration_density_plots_elections.py", "Calibration (elections)"),
        ("election_winner_markets_comparison.py", "Election winner comparison"),
        ("partisan_bias_calibration.py", "Partisan bias calibration"),
    ]

    success_count = 0
    for script, description in analyses:
        if run_script(script, description):
            success_count += 1

    log(f"  Completed {success_count}/{len(analyses)} analyses")
    return success_count > 0

def step5_generate_web_data():
    """Step 5: Generate JSON data for the website."""
    log("="*60)
    log("STEP 5: GENERATING WEB DATA")
    log("="*60)

    return run_script(
        "generate_web_data.py",
        "Generate JSON data for dashboard"
    )

def main():
    """Main orchestration function."""
    print("\n" + "="*60)
    print("DAILY DATA REFRESH")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")

    # Track overall success
    all_success = True

    # Step 1: Fetch prices
    if not step1_fetch_prices():
        log("⚠ Price fetch had issues, continuing anyway...")

    # Step 2: Classify new markets
    if not step2_classify_new_markets():
        log("⚠ Classification had issues, continuing anyway...")

    # Step 3: Calculate predictions
    if not step3_calculate_predictions():
        log("✗ Prediction calculation failed")
        all_success = False

    # Step 4: Run analyses
    if not step4_run_analyses():
        log("✗ Analysis scripts failed")
        all_success = False

    # Step 5: Generate web data
    if not step5_generate_web_data():
        log("✗ Web data generation failed")
        all_success = False

    # Save last update date if successful
    if all_success:
        save_last_update_date()

    # Summary
    print("\n" + "="*60)
    if all_success:
        print("✓ DAILY REFRESH COMPLETE")
    else:
        print("⚠ DAILY REFRESH COMPLETED WITH ERRORS")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")

    return 0 if all_success else 1

if __name__ == "__main__":
    sys.exit(main())
