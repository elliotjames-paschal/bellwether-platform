"""
================================================================================
ANOMALY DETECTION
================================================================================

Detects anomalies in pipeline runs:
- Vote share corruption
- Duplicate vote shares across elections
- Unusual spikes in new markets
- Category skew
- GPT failure rates

Generates anomaly reports for review.
"""

import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from .audit_config import (
    ANOMALY_RULES,
    ANOMALIES_DIR,
    MASTER_FILE,
)


@dataclass
class Anomaly:
    """A detected anomaly."""
    rule_id: str
    severity: str  # CRITICAL, ERROR, WARNING
    description: str
    details: Dict[str, Any] = field(default_factory=dict)
    recommendation: str = ""


@dataclass
class AnomalyReport:
    """Collection of detected anomalies."""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    run_date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))
    anomalies: List[Anomaly] = field(default_factory=list)

    def add(self, anomaly: Optional[Anomaly]):
        if anomaly:
            self.anomalies.append(anomaly)

    def has_critical(self) -> bool:
        return any(a.severity == "CRITICAL" for a in self.anomalies)

    def has_errors(self) -> bool:
        return any(a.severity in ("CRITICAL", "ERROR") for a in self.anomalies)

    @property
    def critical_count(self) -> int:
        return sum(1 for a in self.anomalies if a.severity == "CRITICAL")

    @property
    def error_count(self) -> int:
        return sum(1 for a in self.anomalies if a.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for a in self.anomalies if a.severity == "WARNING")

    def to_dict(self) -> Dict:
        return {
            "timestamp": self.timestamp,
            "run_date": self.run_date,
            "anomalies_detected": len(self.anomalies),
            "summary": {
                "critical": self.critical_count,
                "error": self.error_count,
                "warning": self.warning_count,
            },
            "anomalies": [
                {
                    "id": a.rule_id,
                    "severity": a.severity,
                    "description": a.description,
                    "details": a.details,
                    "recommendation": a.recommendation,
                }
                for a in self.anomalies
            ]
        }

    def save(self, directory: Path = None) -> Path:
        save_dir = directory or ANOMALIES_DIR
        save_dir.mkdir(parents=True, exist_ok=True)
        filepath = save_dir / f"{self.run_date}_anomalies.json"
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        return filepath

    def print_summary(self):
        print(f"\nAnomaly Report")
        print("=" * 50)
        print(f"Detected: {len(self.anomalies)} anomalies")
        print(f"  Critical: {self.critical_count}")
        print(f"  Error: {self.error_count}")
        print(f"  Warning: {self.warning_count}")

        if self.anomalies:
            print("\nAnomalies:")
            for a in self.anomalies:
                print(f"  [{a.severity}] {a.rule_id}: {a.description}")


class AnomalyDetector:
    """Detects anomalies in data and pipeline runs."""

    def __init__(self):
        self.rules = ANOMALY_RULES

    def check_vote_share_corruption(self, df: pd.DataFrame) -> Optional[Anomaly]:
        """Check for vote shares stored as 0-100 instead of 0-1."""
        corrupted_count = 0
        sample_values = []

        for col in ['democrat_vote_share', 'republican_vote_share']:
            if col not in df.columns:
                continue
            corrupted = df[col][df[col] > 1.0]
            corrupted_count += len(corrupted)
            sample_values.extend(corrupted.head(5).tolist())

        if corrupted_count > 0:
            return Anomaly(
                rule_id="vote_share_corruption",
                severity="CRITICAL",
                description=f"Found {corrupted_count} vote shares > 1.0 (likely stored as percentage)",
                details={
                    "count": corrupted_count,
                    "sample_values": sample_values[:10]
                },
                recommendation="Run scripts/audit/fix_vote_share_corruption.py to convert to proportions"
            )
        return None

    def check_duplicate_vote_shares(self, df: pd.DataFrame) -> Optional[Anomaly]:
        """Check for different elections with identical vote shares."""
        if 'democrat_vote_share' not in df.columns:
            return None

        # Filter to rows with vote shares
        has_shares = df['democrat_vote_share'].notna() & df['republican_vote_share'].notna()
        vote_df = df[has_shares].copy()

        if len(vote_df) == 0:
            return None

        # Create election key
        election_cols = ['country', 'office', 'location', 'election_year', 'is_primary']
        available_cols = [c for c in election_cols if c in df.columns]
        vote_df['election_key'] = vote_df[available_cols].fillna('').astype(str).agg('|'.join, axis=1)

        # Get unique elections
        elections = vote_df.groupby('election_key').agg({
            'democrat_vote_share': 'first',
            'republican_vote_share': 'first',
        }).reset_index()

        # Create share pair
        elections['share_pair'] = (
            elections['democrat_vote_share'].round(4).astype(str) + '|' +
            elections['republican_vote_share'].round(4).astype(str)
        )

        # Find duplicates
        share_counts = elections['share_pair'].value_counts()
        min_dups = self.rules.get("duplicate_vote_shares", {}).get("min_duplicates", 3)
        duplicates = share_counts[share_counts >= min_dups]

        if len(duplicates) > 0:
            duplicate_details = []
            for share_pair, count in duplicates.head(5).items():
                matching = elections[elections['share_pair'] == share_pair]
                duplicate_details.append({
                    'vote_shares': share_pair,
                    'count': int(count),
                    'elections': matching['election_key'].head(5).tolist()
                })

            return Anomaly(
                rule_id="duplicate_vote_shares",
                severity="ERROR",
                description=f"Found {len(duplicates)} vote share pairs in {min_dups}+ different elections",
                details={"duplicates": duplicate_details},
                recommendation="Review duplicate vote shares - likely GPT returned same results for different elections"
            )
        return None

    def check_new_market_spike(self, changelog_summary: Dict) -> Optional[Anomaly]:
        """Check for unusual spike in new markets."""
        threshold = self.rules.get("spike_new_markets", {}).get("threshold", 500)
        markets_added = changelog_summary.get("markets_added", 0)

        if markets_added > threshold:
            return Anomaly(
                rule_id="spike_new_markets",
                severity="WARNING",
                description=f"Unusual spike: {markets_added} new markets (threshold: {threshold})",
                details={"count": markets_added, "threshold": threshold},
                recommendation="Review new markets for bulk import or API issue"
            )
        return None

    def check_category_skew(self, changelog_summary: Dict) -> Optional[Anomaly]:
        """Check if single category dominates new classifications."""
        category_counts = changelog_summary.get("category_counts", {})
        if not category_counts:
            return None

        total = sum(category_counts.values())
        if total == 0:
            return None

        threshold = self.rules.get("category_skew", {}).get("threshold", 0.5)

        for category, count in category_counts.items():
            ratio = count / total
            if ratio > threshold:
                return Anomaly(
                    rule_id="category_skew",
                    severity="WARNING",
                    description=f"Category skew: {category} is {ratio:.1%} of new markets",
                    details={
                        "category": category,
                        "count": count,
                        "total": total,
                        "ratio": ratio,
                        "threshold": threshold
                    },
                    recommendation="Review category classifications for systematic error"
                )
        return None

    def check_gpt_failure_rate(self, changelog_summary: Dict) -> Optional[Anomaly]:
        """Check GPT API failure rate."""
        gpt_calls = changelog_summary.get("gpt_calls", 0)
        gpt_errors = changelog_summary.get("gpt_errors", 0)

        if gpt_calls == 0:
            return None

        threshold = self.rules.get("gpt_failure_rate", {}).get("threshold", 0.1)
        failure_rate = gpt_errors / gpt_calls

        if failure_rate > threshold:
            return Anomaly(
                rule_id="gpt_failure_rate",
                severity="ERROR",
                description=f"High GPT failure rate: {failure_rate:.1%} ({gpt_errors}/{gpt_calls})",
                details={
                    "calls": gpt_calls,
                    "errors": gpt_errors,
                    "rate": failure_rate,
                    "threshold": threshold
                },
                recommendation="Check API key, rate limits, and prompt formatting"
            )
        return None

    def check_electoral_metadata_gap(self, df: pd.DataFrame) -> Optional[Anomaly]:
        """Check for many electoral markets missing metadata."""
        electoral_mask = (
            df['political_category'].str.startswith('1.', na=False) |
            df['political_category'].str.contains('ELECTORAL', case=False, na=False)
        )
        electoral_df = df[electoral_mask]

        if len(electoral_df) == 0:
            return None

        # Check each field
        fields = ['country', 'office', 'location', 'election_year', 'is_primary']
        missing_pcts = {}
        for field in fields:
            if field in df.columns:
                missing_pcts[field] = electoral_df[field].isna().mean()

        # Overall missing rate
        avg_missing = sum(missing_pcts.values()) / len(missing_pcts) if missing_pcts else 0
        threshold = self.rules.get("electoral_metadata_gap", {}).get("threshold", 0.15)

        if avg_missing > threshold:
            return Anomaly(
                rule_id="electoral_metadata_gap",
                severity="WARNING",
                description=f"Electoral metadata gap: {avg_missing:.1%} missing (threshold: {threshold:.1%})",
                details={
                    "missing_by_field": {k: f"{v:.1%}" for k, v in missing_pcts.items()},
                    "electoral_markets": len(electoral_df),
                    "threshold": threshold
                },
                recommendation="Run reclassification on markets with missing metadata"
            )
        return None

    def run_all_checks(self, df: pd.DataFrame = None,
                       changelog_summary: Dict = None) -> AnomalyReport:
        """Alias for analyze() method."""
        return self.analyze(df, changelog_summary)

    def analyze(self, df: pd.DataFrame = None,
                changelog_summary: Dict = None) -> AnomalyReport:
        """
        Run all anomaly checks.

        Args:
            df: Master dataframe (loads from file if not provided)
            changelog_summary: Summary from ChangelogTracker (optional)

        Returns:
            AnomalyReport with all detected anomalies
        """
        if df is None:
            df = pd.read_csv(MASTER_FILE, low_memory=False)

        changelog_summary = changelog_summary or {}

        report = AnomalyReport()

        # Data anomalies
        report.add(self.check_vote_share_corruption(df))
        report.add(self.check_duplicate_vote_shares(df))
        report.add(self.check_electoral_metadata_gap(df))

        # Pipeline anomalies (if changelog provided)
        if changelog_summary:
            report.add(self.check_new_market_spike(changelog_summary))
            report.add(self.check_category_skew(changelog_summary))
            report.add(self.check_gpt_failure_rate(changelog_summary))

        return report


def main():
    """Run anomaly detection and print report."""
    print("\n" + "=" * 70)
    print("BELLWETHER ANOMALY DETECTION")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    detector = AnomalyDetector()

    print("\nLoading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    print(f"  Total markets: {len(df):,}")

    print("\nRunning anomaly detection...")
    report = detector.analyze(df)

    report.print_summary()

    # Save report
    filepath = report.save()
    print(f"\nReport saved: {filepath}")

    print("\n" + "=" * 70)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")

    return report


if __name__ == "__main__":
    report = main()
    exit_code = 1 if report.has_critical() else 0
    exit(exit_code)
