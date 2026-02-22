"""
================================================================================
CHANGELOG TRACKER
================================================================================

Tracks changes during pipeline runs:
- New markets added
- Categories assigned
- Electoral details filled
- Vote shares added
- Resolutions set

Generates daily changelog JSON and human-readable CSV diff.
"""

import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional
import os

from .audit_config import CHANGELOGS_DIR


class ChangelogTracker:
    """
    Tracks changes during a pipeline run.

    Usage:
        tracker = ChangelogTracker()
        tracker.start_run("daily")

        # In each pipeline step
        tracker.record_market_added(market_id, platform, category, source_step)
        tracker.record_category_assigned(market_id, category, confidence, source_step)

        # At end of pipeline
        tracker.end_run()
        tracker.save()
    """

    def __init__(self):
        self.changes: List[Dict] = []
        self.summary: Dict[str, int] = defaultdict(int)
        self.run_name: str = ""
        self.run_start: Optional[str] = None
        self.run_end: Optional[str] = None
        self.gpt_stats: Dict[str, int] = defaultdict(int)

    def start_run(self, run_name: str = "daily"):
        """Initialize a new changelog run."""
        self.run_name = run_name
        self.run_start = datetime.now().isoformat()
        self.changes = []
        self.summary = defaultdict(int)
        self.gpt_stats = defaultdict(int)

    def end_run(self):
        """Mark the run as complete."""
        self.run_end = datetime.now().isoformat()

    # =========================================================================
    # CHANGE RECORDING METHODS
    # =========================================================================

    def record_market_added(self, market_id: str, platform: str,
                           category: str, source_step: str):
        """Record a new market being added to master CSV."""
        self.changes.append({
            "type": "market_added",
            "timestamp": datetime.now().isoformat(),
            "market_id": str(market_id),
            "platform": platform,
            "political_category": category,
            "source_step": source_step
        })
        self.summary["markets_added"] += 1

    def record_category_assigned(self, market_id: str, category: str,
                                  confidence: float = None, source_step: str = None,
                                  gpt_log_ref: str = None):
        """Record a category being assigned to a market."""
        change = {
            "type": "category_assigned",
            "timestamp": datetime.now().isoformat(),
            "market_id": str(market_id),
            "category": category,
            "source_step": source_step or "pipeline_classify_categories"
        }
        if confidence is not None:
            change["confidence"] = confidence
        if gpt_log_ref:
            change["gpt_log_ref"] = gpt_log_ref

        self.changes.append(change)
        self.summary["categories_assigned"] += 1

    def record_electoral_filled(self, market_id: str, fields_updated: Dict[str, Any],
                                source_step: str = None, gpt_log_ref: str = None):
        """Record electoral metadata being filled in."""
        change = {
            "type": "electoral_filled",
            "timestamp": datetime.now().isoformat(),
            "market_id": str(market_id),
            "fields_updated": fields_updated,
            "source_step": source_step or "pipeline_classify_electoral"
        }
        if gpt_log_ref:
            change["gpt_log_ref"] = gpt_log_ref

        self.changes.append(change)
        self.summary["electoral_filled"] += 1

    def record_resolution_set(self, market_id: str, outcome: str,
                              source_step: str = None):
        """Record a market resolution being determined."""
        self.changes.append({
            "type": "resolution_set",
            "timestamp": datetime.now().isoformat(),
            "market_id": str(market_id),
            "outcome": outcome,
            "source_step": source_step or "pipeline_check_resolutions"
        })
        self.summary["resolutions_set"] += 1

    def record_vote_share_added(self, election_key: str, d_share: float,
                                 r_share: float, source_step: str = None,
                                 gpt_log_ref: str = None):
        """Record vote shares being added for an election."""
        change = {
            "type": "vote_share_added",
            "timestamp": datetime.now().isoformat(),
            "election_key": election_key,
            "democrat_vote_share": d_share,
            "republican_vote_share": r_share,
            "source_step": source_step or "pipeline_select_election_winners"
        }
        if gpt_log_ref:
            change["gpt_log_ref"] = gpt_log_ref

        self.changes.append(change)
        self.summary["vote_shares_added"] += 1

    def record_winner_selected(self, election_key: str, pm_market_id: str = None,
                               kalshi_market_id: str = None, source_step: str = None,
                               gpt_log_ref: str = None):
        """Record winner markets being selected for an election."""
        change = {
            "type": "winner_selected",
            "timestamp": datetime.now().isoformat(),
            "election_key": election_key,
            "polymarket_winner_id": pm_market_id,
            "kalshi_winner_id": kalshi_market_id,
            "source_step": source_step or "pipeline_select_election_winners"
        }
        if gpt_log_ref:
            change["gpt_log_ref"] = gpt_log_ref

        self.changes.append(change)
        self.summary["winners_selected"] += 1

    def record_gpt_call(self, script: str, tokens_used: int = 0):
        """Record a GPT API call for statistics."""
        self.gpt_stats["gpt_calls"] += 1
        self.gpt_stats["gpt_tokens_used"] += tokens_used

    def record_gpt_error(self, script: str, error: str):
        """Record a GPT API error."""
        self.gpt_stats["gpt_errors"] += 1
        self.changes.append({
            "type": "gpt_error",
            "timestamp": datetime.now().isoformat(),
            "script": script,
            "error": error
        })

    # =========================================================================
    # OUTPUT METHODS
    # =========================================================================

    def to_dict(self) -> Dict:
        """Convert changelog to dictionary format."""
        return {
            "run_date": datetime.now().strftime("%Y-%m-%d"),
            "run_name": self.run_name,
            "run_start": self.run_start,
            "run_end": self.run_end,
            "summary": {
                **dict(self.summary),
                **dict(self.gpt_stats)
            },
            "changes": self.changes
        }

    def generate_diff_csv(self) -> pd.DataFrame:
        """
        Generate a human-readable CSV diff.
        Easy to scan and review with Claude Code.
        """
        rows = []
        for change in self.changes:
            row = {
                "time": change["timestamp"],
                "type": change["type"],
                "market_id": change.get("market_id", change.get("election_key", "")),
                "details": "",
                "source": change.get("source_step", "")
            }

            # Format details based on change type
            if change["type"] == "market_added":
                row["details"] = f"{change.get('platform', '')} | {change.get('political_category', '')}"
            elif change["type"] == "category_assigned":
                row["details"] = change.get("category", "")
            elif change["type"] == "electoral_filled":
                fields = change.get("fields_updated", {})
                row["details"] = " | ".join(f"{k}={v}" for k, v in fields.items())
            elif change["type"] == "resolution_set":
                row["details"] = change.get("outcome", "")
            elif change["type"] == "vote_share_added":
                d = change.get("democrat_vote_share", "")
                r = change.get("republican_vote_share", "")
                row["details"] = f"D:{d:.4f} R:{r:.4f}" if d and r else ""
            elif change["type"] == "winner_selected":
                pm = change.get("polymarket_winner_id", "")
                k = change.get("kalshi_winner_id", "")
                row["details"] = f"PM:{pm} K:{k}"
            elif change["type"] == "gpt_error":
                row["details"] = change.get("error", "")[:100]

            rows.append(row)

        return pd.DataFrame(rows)

    def save(self, directory: Path = None) -> tuple:
        """
        Save changelog to JSON and CSV files.
        Returns (json_path, csv_path).
        """
        save_dir = directory or CHANGELOGS_DIR
        save_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.now().strftime("%Y-%m-%d")

        # Save JSON changelog
        json_path = save_dir / f"{date_str}_changelog.json"
        with open(json_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)

        # Save CSV diff
        csv_path = save_dir / f"{date_str}_diff.csv"
        diff_df = self.generate_diff_csv()
        diff_df.to_csv(csv_path, index=False)

        return json_path, csv_path

    def print_summary(self):
        """Print a summary to console."""
        print(f"\nChangelog Summary ({self.run_name})")
        print("=" * 50)
        print(f"Run: {self.run_start} to {self.run_end}")
        print(f"\nChanges:")
        for key, value in sorted(self.summary.items()):
            print(f"  {key}: {value:,}")
        print(f"\nGPT Stats:")
        for key, value in sorted(self.gpt_stats.items()):
            print(f"  {key}: {value:,}")
        print(f"\nTotal change records: {len(self.changes):,}")


# Global tracker instance (can be used across pipeline scripts)
_global_tracker: Optional[ChangelogTracker] = None


def get_tracker() -> ChangelogTracker:
    """Get or create the global changelog tracker."""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = ChangelogTracker()
    return _global_tracker


def init_tracker(run_name: str = "daily") -> ChangelogTracker:
    """Initialize a fresh global tracker."""
    global _global_tracker
    _global_tracker = ChangelogTracker()
    _global_tracker.start_run(run_name)
    return _global_tracker


def save_tracker(directory: Path = None) -> tuple:
    """Save the global tracker and return file paths."""
    tracker = get_tracker()
    tracker.end_run()
    return tracker.save(directory)
