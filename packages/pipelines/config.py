"""
Configuration for Bellwether Pipeline

API keys and other sensitive configuration.
This file should NOT be committed to version control.
"""

import os
from datetime import datetime, timezone
from pathlib import Path

# Base directory (bellwether-platform root)
# config.py is at packages/pipelines/config.py, so we go up 2 levels
PIPELINES_DIR = Path(__file__).resolve().parent  # packages/pipelines/
PACKAGES_DIR = PIPELINES_DIR.parent              # packages/
BASE_DIR = PACKAGES_DIR.parent                   # bellwether-platform/

DATA_DIR = BASE_DIR / "data"
SCRIPTS_DIR = PIPELINES_DIR  # Pipeline scripts are in packages/pipelines/
WEBSITE_DIR = BASE_DIR / "docs"

# API Keys - loaded from environment or local file
def get_openai_api_key():
    """Get OpenAI API key from environment or local file."""
    # First try environment
    key = os.environ.get('OPENAI_API_KEY')
    if key:
        return key

    # Then try local file
    key_file = BASE_DIR / "openai_api_key.txt"
    if key_file.exists():
        return key_file.read_text().strip()

    raise ValueError(
        "OPENAI_API_KEY not found. Set OPENAI_API_KEY environment variable "
        "or create openai_api_key.txt in the project root."
    )


def get_openai_client():
    """Get OpenAI client using API key from environment or local file."""
    from openai import OpenAI
    return OpenAI(api_key=get_openai_api_key())


def get_latest_file(pattern, directory=None):
    """
    Find the latest file matching a glob pattern.

    Args:
        pattern: Glob pattern like "polymarket_prediction_accuracy_all_political_*.csv"
        directory: Directory to search in (defaults to DATA_DIR)

    Returns:
        Path to the most recent matching file, or None if no matches
    """
    import glob

    search_dir = directory or DATA_DIR
    full_pattern = str(search_dir / pattern)

    matches = glob.glob(full_pattern)
    if not matches:
        return None

    # Sort by modification time, most recent first
    matches.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    return Path(matches[0])


def rotate_backups(pattern, directory=None, keep=5):
    """
    Keep only the N most recent backup files matching a pattern.

    Args:
        pattern: Glob pattern like "master_backup_*.csv" or "pm_prices_backup_*.json"
        directory: Directory to search in (defaults to DATA_DIR / "backups")
        keep: Number of recent backups to keep (default 5)

    Returns:
        Number of files deleted
    """
    import glob

    backup_dir = directory or (DATA_DIR / "backups")
    full_pattern = str(backup_dir / pattern)

    matches = glob.glob(full_pattern)
    if len(matches) <= keep:
        return 0

    # Sort by modification time, oldest first
    matches.sort(key=lambda x: os.path.getmtime(x))

    # Delete oldest files, keeping only 'keep' most recent
    to_delete = matches[:-keep]
    deleted = 0
    for filepath in to_delete:
        try:
            os.remove(filepath)
            deleted += 1
        except OSError:
            pass

    return deleted


def get_market_anchor_time(market_row, is_election, election_date_lookup_fn=None):
    """
    Determine the anchor time for truncation.

    - Electoral markets: midnight UTC on election day (via lookup)
    - All others: trading_close_time

    Returns datetime or None.
    """
    import pandas as pd

    if is_election and election_date_lookup_fn is not None:
        election_date = election_date_lookup_fn(market_row)
        if election_date is not None and pd.notna(election_date):
            return election_date

    # Non-electoral (or electoral fallback): use trading_close_time directly
    trading_close_time = market_row.get('trading_close_time')
    if pd.notna(trading_close_time):
        try:
            result = pd.to_datetime(trading_close_time, utc=True)
            if pd.notna(result):
                return result
        except Exception:
            pass

    return None


def clean_election_dates_csv(path=None):
    """
    Clean election_dates_lookup.csv in place.

    Fixes float election_year (2026.0 → 2026). Does not drop rows.
    """
    import pandas as pd

    path = Path(path) if path else (DATA_DIR / "election_dates_lookup.csv")
    if not path.exists():
        return 0

    df = pd.read_csv(path)

    # Cast election_year to int so downstream int() calls don't hit float issues
    df["election_year"] = pd.to_numeric(df["election_year"], errors="coerce").fillna(0).astype(int)

    df.to_csv(path, index=False)
    return original_len - len(df)


def atomic_write_json(path, data, **json_kwargs):
    """Write JSON atomically via temp file + os.replace()."""
    import json
    import tempfile
    path = Path(path)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f, **json_kwargs)
        os.replace(tmp, path)
    except:
        os.unlink(tmp)
        raise
