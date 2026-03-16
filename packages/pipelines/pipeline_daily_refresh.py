#!/usr/bin/env python3
"""
================================================================================
PIPELINE SCRIPT: Daily Refresh Orchestrator
================================================================================

Part of the NEW Bellwether Pipeline (January 2026+)

This is the main orchestrator that runs all pipeline scripts in order.
Designed for daily GitHub Actions runs but can also be run locally.

PIPELINE STEPS:
  Phase 1: Discovery & Classification
    1. CLASSIFY KALSHI EVENTS - GPT classify Kalshi event_tickers
    2. DISCOVER - Find new political markets (native Kalshi + Polymarket APIs)
    3. CHECK RESOLUTIONS - Update outcomes for closed markets
    4. CLASSIFY CATEGORIES - GPT classify into 15 political categories
    5. CLASSIFY ELECTORAL - Extract electoral details for electoral markets
  Phase 2: Merge & Enrich
    6. MERGE TO MASTER - Add new markets to master CSV
    7. RECLASSIFY INCOMPLETE - Re-classify existing markets with missing metadata
    8. GET ELECTION DATES - Lookup dates (after merge+reclassify sees all markets)
    9. SELECT ELECTION WINNERS - GPT web search: vote shares + winner selection
  Phase 3: Price Data
    10. FETCH PRICES - Get price history from native APIs
    11. TRUNCATE PRICES - Apply election date truncation
    12. ELECTION EVE PRICES - Fetch election eve prices
  Phase 4: Analysis
    13. CALCULATE BRIER - Calculate Brier scores
    14. RUN ANALYSES - Run analysis scripts (incl. election winner comparison)
  Phase 5: Web
    15. GENERATE WEB DATA - Generate JSON for dashboard
    16. GENERATE MARKET MAP - Extract cross-platform markets for commercial API

Usage:
    python pipeline_daily_refresh.py [--full-refresh] [--start-phase N]

Options:
    --full-refresh     Run discovery with all markets (first run / catch-up)
    --start-phase N    Start from phase N (1-6), skipping earlier phases

Environment Variables:
    OPENAI_API_KEY  - OpenAI API key (required for classification)

================================================================================
"""

import subprocess
import sys
import os
import json
import time
from datetime import datetime
from pathlib import Path

# Add scripts dir to path for logging import
sys.path.insert(0, str(Path(__file__).parent))
from logging_config import (
    setup_logging, get_logger, log_header, log_phase,
    log_step_start, log_step_done, log_summary, flush_email, get_error_count
)

# Import audit system
try:
    from audit.audit_changelog import ChangelogTracker
    from audit.audit_validator import DataValidator
    from audit.audit_anomaly import AnomalyDetector
    from audit.audit_daily_summary import generate_and_send_summary
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================

from config import BASE_DIR, DATA_DIR, WEBSITE_DIR, SCRIPTS_DIR


def run_script(script_name, description, args=None, required=True, script_dir=None):
    """
    Run a Python script and return success status.

    Captures stdout/stderr and logs them appropriately.

    Args:
        script_name: Name of the script file
        description: Human-readable description for logging
        args: Optional list of command-line arguments
        required: If True, missing script is an error; if False, it's skipped
        script_dir: Optional directory to look for script (defaults to SCRIPTS_DIR)
    """
    logger = get_logger("orchestrator")
    base_dir = script_dir if script_dir else SCRIPTS_DIR
    script_path = base_dir / script_name

    if not script_path.exists():
        logger.warning(f"Script not found: {script_name}")
        return not required  # Fail only if required

    log_step_start(description)
    start_time = time.time()

    cmd = [sys.executable, str(script_path)]
    if args:
        cmd.extend(args)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
            # No timeout - let scripts run to completion
        )

        duration = time.time() - start_time

        # Log stdout lines at DEBUG level
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    logger.debug(f"  {line.strip()}")

        if result.returncode == 0:
            log_step_done(description, duration, success=True)
            return True
        else:
            logger.error(f"{description} failed with exit code {result.returncode}")
            if result.stderr:
                for line in result.stderr.strip().split('\n')[:10]:  # First 10 lines
                    if line.strip():
                        logger.error(f"  {line.strip()}")
            log_step_done(description, duration, success=False)
            return False

    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"{description} raised exception: {e}")
        log_step_done(description, duration, success=False)
        return False


def load_state():
    """Load pipeline state from file."""
    state_file = DATA_DIR / "pipeline_state.json"
    if state_file.exists():
        try:
            with open(state_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError, OSError):
            pass
    return {"last_run": None, "last_successful_run": None}


def save_state(state):
    """Save pipeline state to file."""
    state_file = DATA_DIR / "pipeline_state.json"
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)


def main():
    """Main orchestration function."""
    full_refresh = "--full-refresh" in sys.argv
    run_start_time = time.time()

    # Parse --start-phase argument
    start_phase = 1  # Default: run all phases
    for arg in sys.argv[1:]:
        if arg.startswith("--start-phase="):
            try:
                start_phase = int(arg.split("=")[1])
            except ValueError:
                print(f"Invalid --start-phase value: {arg}")
                return 1
        elif arg == "--start-phase":
            # Handle --start-phase N format
            idx = sys.argv.index(arg)
            if idx + 1 < len(sys.argv):
                try:
                    start_phase = int(sys.argv[idx + 1])
                except ValueError:
                    print(f"Invalid --start-phase value: {sys.argv[idx + 1]}")
                    return 1

    # Initialize logging
    run_name = "full_refresh" if full_refresh else "daily"
    setup_logging(run_name=run_name)
    logger = get_logger("orchestrator")

    # Log header
    log_header("BELLWETHER PIPELINE: DAILY REFRESH")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Mode: {'FULL REFRESH' if full_refresh else 'INCREMENTAL'}")
    if start_phase > 1:
        logger.info(f"Starting from Phase {start_phase} (skipping Phases 1-{start_phase-1})")

    # Load state
    state = load_state()
    state["last_run"] = datetime.now().isoformat()
    save_state(state)

    # Check for OpenAI API key
    has_openai = bool(os.environ.get('OPENAI_API_KEY')) or (BASE_DIR / "openai_api_key.txt").exists()
    if not has_openai:
        logger.warning("No OPENAI_API_KEY found - classification steps will be skipped")

    # Initialize audit system
    changelog = None
    if AUDIT_AVAILABLE:
        try:
            changelog = ChangelogTracker()
            logger.info("Audit system initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize audit system: {e}")

    # Track results
    results = {}
    step_results = {}

    # =========================================================================
    # PHASE 1: MARKET DISCOVERY & CLASSIFICATION
    # =========================================================================

    if start_phase <= 1:
        log_phase(1, "MARKET DISCOVERY & CLASSIFICATION")

        # Step 0a: Refresh Polymarket political tags (GPT classifies new tags)
        if has_openai:
            success = run_script(
                "pipeline_refresh_political_tags.py",
                "Refresh Polymarket political tags (GPT)",
                required=False
            )
            results["refresh_political_tags"] = success
            step_results["refresh_political_tags"] = "OK" if success else ("FAIL" if success is False else "SKIP")
        else:
            logger.info("Skipping political tag refresh (no OpenAI API key)")
            results["refresh_political_tags"] = None
            step_results["refresh_political_tags"] = "SKIP"

        # Step 0b: Classify Kalshi political event tickers (native API + keywords)
        kalshi_classify_args = ["--full-refresh"] if full_refresh else []
        success = run_script(
            "pipeline_classify_kalshi_events.py",
            "Classify Kalshi political event tickers (native API)",
            args=kalshi_classify_args,
            required=True
        )
        results["classify_kalshi_events"] = success
        step_results["classify_kalshi_events"] = "OK" if success else "FAIL"

        # Step 1: Discover new political markets (native APIs)
        discover_args = ["--active-only"] if not full_refresh else []
        success = run_script(
            "pipeline_discover_markets_v2.py",
            "Discover new political markets (native APIs)",
            args=discover_args,
            required=True
        )
        results["discover"] = success
        step_results["discover_markets"] = "OK" if success else "FAIL"

        # Step 2: Check resolutions
        success = run_script(
            "pipeline_check_resolutions.py",
            "Check market resolutions",
            required=False
        )
        results["resolutions"] = success
        step_results["check_resolutions"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        if has_openai:
            # Step 3: Classify into 15 categories
            success = run_script(
                "pipeline_classify_categories.py",
                "Classify into 15 political categories (GPT)",
                required=True
            )
            results["classify_categories"] = success
            step_results["classify_categories"] = "OK" if success else "FAIL"

            # Step 4: Extract electoral details
            success = run_script(
                "pipeline_classify_electoral.py",
                "Extract electoral details (GPT)",
                required=True
            )
            results["classify_electoral"] = success
            step_results["classify_electoral"] = "OK" if success else "FAIL"
        else:
            logger.info("Skipping classification steps (no OpenAI API key)")
            results["classify_categories"] = None
            results["classify_electoral"] = None
            step_results["classify_categories"] = "SKIP"
            step_results["classify_electoral"] = "SKIP"
    else:
        logger.info(f"Skipping Phase 1 (starting from Phase {start_phase})")

    # =========================================================================
    # PHASE 2: MERGE NEW MARKETS TO MASTER
    # =========================================================================

    if start_phase <= 2:
        log_phase(2, "MERGE NEW MARKETS TO MASTER")

        success = run_script(
            "pipeline_merge_to_master.py",
            "Merge new markets to master CSV",
            required=False
        )
        results["merge_to_master"] = success
        step_results["merge_to_master"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Reclassify incomplete electoral markets (fills gaps in old data)
        if has_openai:
            success = run_script(
                "pipeline_reclassify_incomplete.py",
                "Reclassify incomplete electoral markets (GPT)",
                required=False
            )
            results["reclassify_incomplete"] = success
            step_results["reclassify_incomplete"] = "OK" if success else ("FAIL" if success is False else "SKIP")
        else:
            logger.info("Skipping reclassification (no OpenAI API key)")
            results["reclassify_incomplete"] = None
            step_results["reclassify_incomplete"] = "SKIP"

        # Get election dates (runs after merge + reclassify so it sees all markets)
        success = run_script(
            "pipeline_get_election_dates.py",
            "Lookup election dates for new elections",
            required=False
        )
        results["election_dates"] = success
        step_results["get_election_dates"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Clean election dates CSV (fix partial dates, NaN, float years)
        try:
            from config import clean_election_dates_csv
            clean_election_dates_csv()
        except Exception as e:
            logger.warning(f"Election dates cleanup failed: {e}")

        # Select election winner markets (combined: vote shares + winner selection per election)
        if has_openai:
            success = run_script(
                "pipeline_select_election_winners.py",
                "Select election winner markets (GPT web search)",
                required=False
            )
            results["select_election_winners"] = success
            step_results["select_election_winners"] = "OK" if success else ("FAIL" if success is False else "SKIP")
        else:
            logger.info("Skipping election winner selection (no OpenAI API key)")
            results["select_election_winners"] = None
            step_results["select_election_winners"] = "SKIP"
    else:
        logger.info(f"Skipping Phase 2 (starting from Phase {start_phase})")

    # =========================================================================
    # PHASE 3: PRICE DATA
    # =========================================================================

    if start_phase <= 3:
        log_phase(3, "PRICE DATA")

        # Fetch prices from native APIs (run in parallel — different APIs, different output files)
        price_args = ["--full-refresh"] if full_refresh else []

        logger.info("Starting Polymarket + Kalshi price fetches in parallel...")
        python_exe = sys.executable
        scripts_dir = SCRIPTS_DIR

        pm_cmd = [python_exe, str(scripts_dir / "pull_polymarket_prices.py")] + price_args
        kalshi_cmd = [python_exe, str(scripts_dir / "pull_kalshi_prices.py")] + price_args

        pm_proc = subprocess.Popen(pm_cmd, cwd=str(scripts_dir), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        kalshi_proc = subprocess.Popen(kalshi_cmd, cwd=str(scripts_dir), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        pm_output, _ = pm_proc.communicate()
        kalshi_output, _ = kalshi_proc.communicate()

        # Log output
        if pm_output:
            for line in pm_output.decode('utf-8', errors='replace').strip().split('\n'):
                logger.info(f"  [PM prices] {line}")
        if kalshi_output:
            for line in kalshi_output.decode('utf-8', errors='replace').strip().split('\n'):
                logger.info(f"  [Kalshi prices] {line}")

        pm_success = pm_proc.returncode == 0
        kalshi_success = kalshi_proc.returncode == 0

        results["fetch_pm_prices"] = pm_success
        step_results["fetch_pm_prices"] = "OK" if pm_success else "FAIL"
        results["fetch_kalshi_prices"] = kalshi_success
        step_results["fetch_kalshi_prices"] = "OK" if kalshi_success else "FAIL"

        if pm_success and kalshi_success:
            logger.info("Both price fetches completed successfully")
        else:
            if not pm_success:
                logger.warning(f"Polymarket price fetch failed (exit code {pm_proc.returncode})")
            if not kalshi_success:
                logger.warning(f"Kalshi price fetch failed (exit code {kalshi_proc.returncode})")

        # Truncate prices at election dates / trading_close_time
        success = run_script(
            "truncate_polymarket_prices.py",
            "Truncate Polymarket prices at election dates",
            required=False
        )
        results["truncate_pm"] = success
        step_results["truncate_pm_prices"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        success = run_script(
            "truncate_kalshi_prices.py",
            "Truncate Kalshi prices at election dates",
            required=False
        )
        results["truncate_kalshi"] = success
        step_results["truncate_kalshi_prices"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Fetch election eve prices (UTC midnight on election day)
        success = run_script(
            "pipeline_election_eve_prices.py",
            "Fetch election eve prices from local price data",
            required=False
        )
        results["election_eve_prices"] = success
        step_results["election_eve_prices"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Fetch orderbook snapshots for liquidity analysis
        success = run_script(
            "fetch_orderbooks.py",
            "Fetch orderbook snapshots (native APIs)",
            required=False
        )
        results["fetch_orderbooks"] = success
        step_results["fetch_orderbooks"] = "OK" if success else ("FAIL" if success is False else "SKIP")
    else:
        logger.info(f"Skipping Phase 3 (starting from Phase {start_phase})")

    # =========================================================================
    # PHASE 4: ANALYSIS
    # =========================================================================

    if start_phase <= 4:
        log_phase(4, "ANALYSIS")

        # Calculate Brier scores
        success = run_script(
            "calculate_all_political_brier_scores.py",
            "Calculate Brier scores for all markets",
            required=False
        )
        results["brier_scores"] = success
        step_results["calculate_brier"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Analysis scripts that require Brier scores to have succeeded
        brier_dependent_scripts = [
            ("create_brier_cohorts.py", "Create Brier cohorts"),
            ("brier_score_analysis.py", "Brier score analysis"),
            ("table_4_brier_by_election_type.py", "Brier by election type"),
            ("calibration_density_plots.py", "Calibration density plots"),
            ("calibration_density_plots_elections.py", "Calibration (elections)"),
            ("election_winner_markets_comparison.py", "Election winner comparison"),
            ("partisan_bias_calibration.py", "Partisan bias calibration"),
            ("calibration_by_race_closeness.py", "Calibration by race margin"),
            ("table_partisan_bias_regression.py", "Partisan bias regression"),
        ]

        # Analysis scripts that run independently of Brier scores
        independent_scripts = [
            ("table_1_aggregate.py", "Market counts by category"),
            ("table_2_election_types.py", "Election types breakdown"),
            ("table_3_platform_comparison.py", "Platform comparison"),
            ("volume_timeseries_by_category.py", "Volume timeseries"),
            ("prediction_vs_volume.py", "Prediction vs volume plots"),
            ("calculate_liquidity_metrics.py", "Calculate liquidity metrics"),
            ("generate_liquidity_analysis.py", "Generate liquidity analysis"),
            ("fetch_panel_a_trades.py", "Fetch Panel A trades (Data API)"),
            ("aggregate_trader_partisanship.py", "Aggregate trader partisanship"),
        ]

        analysis_success = 0
        total_scripts = len(independent_scripts)

        # Always run independent scripts
        for script, desc in independent_scripts:
            if run_script(script, desc, required=False):
                analysis_success += 1

        # Only run Brier-dependent scripts if Brier calculation succeeded
        if results.get("brier_scores"):
            total_scripts += len(brier_dependent_scripts)
            for script, desc in brier_dependent_scripts:
                if run_script(script, desc, required=False):
                    analysis_success += 1
        else:
            logger.info(f"Skipping {len(brier_dependent_scripts)} Brier-dependent scripts (Brier calculation failed)")
            step_results["brier_dependent"] = "SKIP"

        results["analysis"] = analysis_success > 0
        step_results["analysis_scripts"] = f"OK ({analysis_success}/{total_scripts})"
        logger.info(f"Completed {analysis_success}/{total_scripts} analysis scripts")
    else:
        logger.info(f"Skipping Phase 4 (starting from Phase {start_phase})")

    # =========================================================================
    # PHASE 5: WEB DATA GENERATION
    # =========================================================================

    if start_phase <= 5:
        log_phase(5, "WEB DATA GENERATION")

        # Fetch PM event slugs for URL building (needed by generate_monitor_data)
        # Only run on full refresh (Sunday) — takes ~8 hours, mapping persists between runs
        slug_file = DATA_DIR / "pm_event_slug_mapping.json"
        if full_refresh or not slug_file.exists():
            slug_args = ["--incremental"] if slug_file.exists() else []
            success = run_script(
                "fetch_pm_event_slugs.py",
                "Fetch Polymarket event slugs for URLs",
                args=slug_args if slug_args else None,
                required=False
            )
            results["fetch_pm_slugs"] = success
            step_results["fetch_pm_slugs"] = "OK" if success else ("FAIL" if success is False else "SKIP")
        else:
            logger.info("Skipping slug fetch (daily run, mapping file exists)")
            results["fetch_pm_slugs"] = None
            step_results["fetch_pm_slugs"] = "SKIP"

        # === V2 TICKER-BASED MATCHING ===
        # Step 1: Enrich markets with full API data
        success = run_script(
            "enrich_markets_with_api_data.py",
            "Enrich markets with API data",
            required=False
        )
        results["enrich_markets"] = success
        step_results["enrich_markets"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 2: Generate canonical tickers using GPT-4o
        if has_openai:
            success = run_script(
                "create_tickers.py",
                "Generate canonical BWR tickers (GPT-4o)",
                required=False
            )
            results["create_tickers"] = success
            step_results["create_tickers"] = "OK" if success else ("FAIL" if success is False else "SKIP")
        else:
            logger.info("Skipping ticker generation (no OpenAI API key)")
            results["create_tickers"] = None
            step_results["create_tickers"] = "SKIP"

        # Step 2b: Post-process tickers (deterministic fixes)
        success = run_script(
            "postprocess_tickers.py",
            "Post-process ticker assignments",
            required=False
        )
        results["postprocess_tickers"] = success
        step_results["postprocess_tickers"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Generate a shared batch ID for all feedback pipeline steps
        from datetime import datetime as _dt, timezone as _tz
        feedback_batch_id = _dt.now(_tz.utc).strftime("batch_%Y%m%d_%H%M%S")
        logger.info(f"Feedback pipeline batch: {feedback_batch_id}")

        # Step 2b.1: Ingest human feedback from Google Sheet
        success = run_script(
            "pipeline_ingest_feedback.py",
            "Ingest human feedback from Google Sheet",
            args=["--batch-id", feedback_batch_id],
            required=False
        )
        results["ingest_feedback"] = success
        step_results["ingest_feedback"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 2b.2: Apply human labels as final overrides
        success = run_script(
            "pipeline_apply_human_labels.py",
            "Apply human labels (ground truth overrides)",
            args=["--batch-id", feedback_batch_id],
            required=False
        )
        results["apply_human_labels"] = success
        step_results["apply_human_labels"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 2b.3: Evaluate match accuracy against human labels (report only)
        success = run_script(
            "pipeline_evaluate_matches.py",
            "Evaluate match accuracy vs human labels",
            args=["--batch-id", feedback_batch_id],
            required=False
        )
        results["evaluate_matches"] = success
        step_results["evaluate_matches"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 2c: Cross-platform discovery (embedding-based, no GPT needed)
        success = run_script(
            "pipeline_discover_cross_platform.py",
            "Discover cross-platform market candidates (embeddings)",
            required=False
        )
        results["discover_cross_platform"] = success
        step_results["discover_cross_platform"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 2d: Compare resolutions for Bucket B candidates (GPT)
        if has_openai and results.get("discover_cross_platform"):
            success = run_script(
                "pipeline_compare_resolutions.py",
                "Compare resolution criteria (GPT-4o)",
                required=False
            )
            results["compare_resolutions"] = success
            step_results["compare_resolutions"] = "OK" if success else ("FAIL" if success is False else "SKIP")
        else:
            if not has_openai:
                logger.info("Skipping resolution comparison (no OpenAI API key)")
            results["compare_resolutions"] = None
            step_results["compare_resolutions"] = "SKIP"

        # Step 2e: Apply cross-platform match fixes
        success = run_script(
            "pipeline_update_matches.py",
            "Update cross-platform match files",
            required=False
        )
        results["update_matches"] = success
        step_results["update_matches"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 2f: Generate ticker corrections for NEXT run's postprocessing
        success = run_script(
            "generate_ticker_corrections.py",
            "Generate ticker corrections from human feedback errors",
            args=["--batch-id", feedback_batch_id],
            required=False
        )
        results["ticker_corrections"] = success
        step_results["ticker_corrections"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 3: Generate market map using ticker-based matching
        success = run_script(
            "generate_market_map.py",
            "Generate market map for commercial API",
            required=False
        )
        results["market_map"] = success
        step_results["generate_market_map"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 4: Generate full worker index (all active markets) and upload to KV
        worker_args = []
        if not os.environ.get("CLOUDFLARE_API_TOKEN"):
            worker_args.append("--skip-kv-upload")
            logger.info("No CLOUDFLARE_API_TOKEN — passing --skip-kv-upload to generate_worker_index.py")
        success = run_script(
            "generate_worker_index.py",
            "Generate worker index for V2 workers (KV upload)",
            args=worker_args if worker_args else None,
            required=False
        )
        results["worker_index"] = success
        step_results["generate_worker_index"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 5: Generate web data and monitor (AFTER tickers so active_markets.json has BWR IDs)
        success = run_script(
            "generate_web_data.py",
            "Generate JSON data for dashboard",
            required=False
        )
        results["web_data"] = success
        step_results["generate_web_data"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 5b: Extract contract rules for Market Monitor
        success = run_script(
            "generate_market_rules.py",
            "Extract contract rules for monitor",
            required=False
        )
        results["market_rules"] = success
        step_results["generate_market_rules"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Step 6: Upload active_markets.json to KV (for /api/markets/search and /top)
        if os.environ.get("CLOUDFLARE_API_TOKEN"):
            success = run_script(
                "upload_active_markets_kv.py",
                "Upload active_markets.json to Cloudflare KV",
                required=False
            )
            results["upload_active_markets_kv"] = success
            step_results["upload_active_markets_kv"] = "OK" if success else ("FAIL" if success is False else "SKIP")
        else:
            logger.info("Skipping KV upload (no CLOUDFLARE_API_TOKEN)")
            results["upload_active_markets_kv"] = None
            step_results["upload_active_markets_kv"] = "SKIP"

        # Export liquidity data for website
        success = run_script(
            "export_liquidity_for_website.py",
            "Export liquidity data for website",
            required=False
        )
        results["liquidity_export"] = success
        step_results["export_liquidity"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Export liquidity timeseries for website
        success = run_script(
            "export_liquidity_timeseries.py",
            "Export liquidity timeseries for website",
            required=False
        )
        results["liquidity_timeseries"] = success
        step_results["export_liquidity_ts"] = "OK" if success else ("FAIL" if success is False else "SKIP")

        # Generate civic elections data for US Election Calendar
        success = run_script(
            "generate_civic_elections.py",
            "Generate US Election Calendar data",
            required=False
        )
        results["civic_elections"] = success
        step_results["civic_elections"] = "OK" if success else ("FAIL" if success is False else "SKIP")
    else:
        logger.info(f"Skipping Phase 5 (starting from Phase {start_phase})")

    # =========================================================================
    # AUDIT: Data Validation & Anomaly Detection
    # =========================================================================

    if AUDIT_AVAILABLE:
        log_step_start("Run data validation and anomaly detection")
        audit_start = time.time()

        try:
            # Pre-publish validation
            validator = DataValidator()
            validation_result = validator.run_all_checks(source="pre_publish")

            if validation_result['status'] == 'OK':
                logger.info("Data validation passed - no issues found")
            else:
                logger.warning(f"Data validation: {validation_result['summary']['critical']} critical, "
                             f"{validation_result['summary']['error']} errors, "
                             f"{validation_result['summary']['warning']} warnings")
                # Log individual issues at appropriate level to trigger email
                for issue in validation_result['issues']:
                    if issue['level'] == 'CRITICAL':
                        logger.critical(f"VALIDATION: [{issue['rule']}] {issue['message']}")
                    elif issue['level'] == 'ERROR':
                        logger.error(f"VALIDATION: [{issue['rule']}] {issue['message']}")

            # Anomaly detection
            detector = AnomalyDetector()
            anomaly_result = detector.run_all_checks().to_dict()

            if anomaly_result['anomalies_detected'] == 0:
                logger.info("Anomaly detection passed - no anomalies found")
            else:
                logger.warning(f"Anomaly detection: {anomaly_result['anomalies_detected']} anomalies found")
                for anomaly in anomaly_result['anomalies']:
                    # Log at appropriate level to trigger email for serious issues
                    if anomaly['severity'] == 'CRITICAL':
                        logger.critical(f"AUDIT: [{anomaly['id']}] {anomaly['description']}")
                    elif anomaly['severity'] == 'ERROR':
                        logger.error(f"AUDIT: [{anomaly['id']}] {anomaly['description']}")
                    else:
                        logger.warning(f"AUDIT: [{anomaly['id']}] {anomaly['description']}")

            # Save changelog
            if changelog:
                changelog.save()
                logger.info(f"Changelog saved: {len(changelog.changes)} changes tracked")

            # Send daily summary email (always, not just on errors)
            try:
                logger.info("Generating and sending daily audit summary email...")
                generate_and_send_summary()
            except Exception as e:
                logger.warning(f"Failed to send daily summary email: {e}")

            results["audit"] = True
            step_results["audit"] = "OK"

        except Exception as e:
            logger.error(f"Audit system error: {e}")
            results["audit"] = False
            step_results["audit"] = "FAIL"

        audit_duration = time.time() - audit_start
        log_step_done("Run data validation and anomaly detection", audit_duration, success=results.get("audit", False))
    else:
        logger.info("Audit system not available - skipping validation")
        results["audit"] = None
        step_results["audit"] = "SKIP"

    # Note: Git deploy (commit + push docs/data/) is handled by run_pipeline.sh
    # to avoid duplicate pushes. The shell script has PAT-based auth.

    # =========================================================================
    # SUMMARY
    # =========================================================================

    # Count successes
    success_count = sum(1 for v in results.values() if v is True)
    fail_count = sum(1 for v in results.values() if v is False)
    skip_count = sum(1 for v in results.values() if v is None)

    all_success = fail_count == 0
    total_duration = time.time() - run_start_time

    if all_success:
        state["last_successful_run"] = datetime.now().isoformat()
        save_state(state)

    # Log summary
    log_summary(
        {'success': success_count, 'failed': fail_count, 'skipped': skip_count},
        total_duration
    )

    # Print step results
    logger.info("")
    logger.info("Step Results:")
    for step, status in step_results.items():
        logger.info(f"  {step}: {status}")

    # Send error email if any errors occurred
    if get_error_count() > 0:
        flush_email()

    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
