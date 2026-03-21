#!/usr/bin/env python3
"""
Fix duplicate vote shares caused by GPT copy-paste bugs.

Nulls out democrat_vote_share and republican_vote_share for:
1. AZ-9, CA-1, FL-18 House 2024 — identical D:0.347 R:0.653 across different districts
2. TN-7 2023/2025/2026 — shares copied to non-existent election years
3. NYC Mayor 2026/2027 — 2025 results copied to future years
"""

import os
import sys
import shutil
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_DIR

MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
BACKUP_DIR = DATA_DIR / "backups"


def main():
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    original_count = len(df)
    total_fixed = 0

    # 1. AZ-9, CA-1, FL-18 with D:0.347 R:0.653
    mask1 = (
        (df["democrat_vote_share"].round(3) == 0.347) &
        (df["republican_vote_share"].round(3) == 0.653) &
        (df["location"].isin(["AZ-9", "CA-1", "FL-18"]))
    )
    n1 = mask1.sum()
    df.loc[mask1, ["democrat_vote_share", "republican_vote_share"]] = np.nan
    print(f"Fix 1: Nulled {n1} rows — AZ-9/CA-1/FL-18 identical D:0.347 R:0.653")
    total_fixed += n1

    # 2. TN-7 for years 2023, 2025, 2026 (non-existent elections)
    mask2 = (
        (df["location"] == "TN-7") &
        (df["election_year"].isin([2023.0, 2025.0, 2026.0])) &
        (df["democrat_vote_share"].round(4) == 0.4506) &
        (df["republican_vote_share"].round(3) == 0.539)
    )
    n2 = mask2.sum()
    df.loc[mask2, ["democrat_vote_share", "republican_vote_share"]] = np.nan
    print(f"Fix 2: Nulled {n2} rows — TN-7 2023/2025/2026 non-existent elections")
    total_fixed += n2

    # 3. NYC Mayor 2026, 2027 with 2025 shares copied
    mask3 = (
        (df["office"] == "Mayor") &
        (df["location"].str.contains("New York", na=False)) &
        (df["election_year"].isin([2026.0, 2027.0])) &
        (df["democrat_vote_share"].round(4) == 0.5078) &
        (df["republican_vote_share"].round(4) == 0.0701)
    )
    n3 = mask3.sum()
    df.loc[mask3, ["democrat_vote_share", "republican_vote_share"]] = np.nan
    print(f"Fix 3: Nulled {n3} rows — NYC Mayor 2026/2027 copied from 2025")
    total_fixed += n3

    print(f"\nTotal rows fixed: {total_fixed}")
    assert len(df) == original_count, "Row count changed!"

    # Backup
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = BACKUP_DIR / f"master_backup_vote_share_fix_{ts}.csv"
    shutil.copy2(MASTER_FILE, backup)
    print(f"Backup: {backup.name}")

    # Save
    df.to_csv(MASTER_FILE, index=False)
    print(f"Saved: {MASTER_FILE.name}")


if __name__ == "__main__":
    main()
