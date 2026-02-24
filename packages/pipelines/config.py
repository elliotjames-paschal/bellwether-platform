"""
Configuration for Bellwether Pipeline

API keys and other sensitive configuration.
This file should NOT be committed to version control.
"""

import os
from pathlib import Path

# Base directory (bellwether-platform root)
# config.py is at packages/pipelines/config.py, so we go up 2 levels
PIPELINES_DIR = Path(__file__).resolve().parent  # packages/pipelines/
PACKAGES_DIR = PIPELINES_DIR.parent              # packages/
BASE_DIR = PACKAGES_DIR.parent                   # bellwether-platform/

DATA_DIR = BASE_DIR / "data"
SCRIPTS_DIR = PIPELINES_DIR  # Pipeline scripts are in packages/pipelines/
WEBSITE_DIR = PACKAGES_DIR / "website"

# API Keys - loaded from environment or local file
def get_dome_api_key():
    """Get Dome API key from environment or local file."""
    # First try environment
    key = os.environ.get('DOME_API_KEY')
    if key:
        return f"Bearer {key}" if not key.startswith('Bearer ') else key

    # Then try local file
    key_file = BASE_DIR / "dome_api_key.txt"
    if key_file.exists():
        key = key_file.read_text().strip()
        return f"Bearer {key}" if not key.startswith('Bearer ') else key

    raise ValueError(
        "DOME_API_KEY not found. Set DOME_API_KEY environment variable "
        "or create dome_api_key.txt in the project root."
    )


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
