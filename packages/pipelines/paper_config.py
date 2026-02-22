"""
Paper Generation Config — Shared context for cutoff-filtered paper output.

When BELLWETHER_CUTOFF_DATE and BELLWETHER_OUTPUT_DIR env vars are set
(by generate_paper.py), this module provides:
  - Filtered data loaders (master CSV, prediction accuracy)
  - Paper-specific output directories (graphs, tables, data)

When env vars are NOT set (standalone script execution), everything
falls back to the default behavior — no filtering, default output dirs.
"""

import os
import pandas as pd
from pathlib import Path
from config import BASE_DIR, DATA_DIR, get_latest_file


# ---------------------------------------------------------------------------
# Cutoff date (None = no filtering)
# ---------------------------------------------------------------------------
_cutoff_str = os.environ.get("BELLWETHER_CUTOFF_DATE")
CUTOFF_DATE = pd.Timestamp(_cutoff_str, tz='UTC') if _cutoff_str else None

# ---------------------------------------------------------------------------
# Output directories
# ---------------------------------------------------------------------------
_output_dir = os.environ.get("BELLWETHER_OUTPUT_DIR")

if _output_dir:
    _paper_dir = Path(_output_dir)
    PAPER_GRAPHS_DIR = _paper_dir / "graphs" / "combined"
    PAPER_TABLES_DIR = _paper_dir / "tables"
    PAPER_DATA_DIR = _paper_dir / "data"
    for d in (PAPER_GRAPHS_DIR, PAPER_TABLES_DIR, PAPER_DATA_DIR):
        d.mkdir(parents=True, exist_ok=True)
else:
    # Defaults — same dirs the scripts already use
    PAPER_GRAPHS_DIR = BASE_DIR / "graphs" / "combined"
    PAPER_TABLES_DIR = BASE_DIR / "tables"
    PAPER_DATA_DIR = DATA_DIR


# ---------------------------------------------------------------------------
# Filtered data loaders
# ---------------------------------------------------------------------------

def _compare_with_cutoff(series, cutoff):
    """Compare datetime series with cutoff, handling tz-aware/naive mismatch."""
    if series.dt.tz is not None:
        # Series is tz-aware, use tz-aware cutoff
        return series <= cutoff
    else:
        # Series is tz-naive, use tz-naive cutoff
        return series <= cutoff.tz_localize(None)


def load_master_csv():
    """Load master CSV, optionally filtered to trading_close_time <= cutoff."""
    master_file = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
    df = pd.read_csv(master_file, low_memory=False)
    if CUTOFF_DATE is not None:
        # Use format='mixed' and utc=True to handle different datetime formats
        # (Polymarket: 'YYYY-MM-DD HH:MM:SS+00', Kalshi: 'YYYY-MM-DDTHH:MM:SSZ')
        df['trading_close_time'] = pd.to_datetime(df['trading_close_time'], format='mixed', utc=True, errors='coerce')
        before = len(df)
        df = df[_compare_with_cutoff(df['trading_close_time'], CUTOFF_DATE)]
        print(f"  [paper_config] Filtered master CSV: {before:,} → {len(df):,} (cutoff {CUTOFF_DATE.date()})")
    return df


def load_prediction_accuracy(platform):
    """
    Load prediction accuracy CSV for a platform, optionally filtered.

    Args:
        platform: "polymarket" or "kalshi"

    Returns:
        DataFrame, or None if file not found
    """
    pattern = f"{platform}_prediction_accuracy_all_political*.csv"
    pred_file = get_latest_file(pattern)
    if pred_file is None:
        print(f"  [paper_config] WARNING: No file found for pattern {pattern}")
        return None
    df = pd.read_csv(pred_file)
    if CUTOFF_DATE is not None and 'reference_date' in df.columns:
        # Use format='mixed' and utc=True to handle different datetime formats
        df['reference_date'] = pd.to_datetime(df['reference_date'], format='mixed', utc=True, errors='coerce')
        before = len(df)
        df = df[_compare_with_cutoff(df['reference_date'], CUTOFF_DATE)]
        print(f"  [paper_config] Filtered {platform} predictions: {before:,} → {len(df):,} (cutoff {CUTOFF_DATE.date()})")
    return df
