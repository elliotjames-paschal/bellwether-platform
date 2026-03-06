"""
================================================================================
DATA VALIDATOR
================================================================================

Validates data quality at key pipeline checkpoints:
- Pre-merge: Before new markets are added to master CSV
- Pre-publish: Before website JSON is generated

Validation levels:
- CRITICAL: Send alert, log prominently, but continue pipeline
- ERROR: Log and include in report
- WARNING: Log only
"""

import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from .audit_config import (
    VALIDATION_RULES,
    VALIDATION_LEVELS,
    VALIDATION_DIR,
    MASTER_FILE,
)


@dataclass
class ValidationIssue:
    """A single validation issue."""
    rule: str
    level: str  # CRITICAL, ERROR, WARNING
    message: str
    count: int = 1
    sample_ids: List[str] = field(default_factory=list)
    sample_values: List[Any] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Collection of validation results."""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    source: str = ""  # "pre_merge" or "pre_publish"
    issues: List[ValidationIssue] = field(default_factory=list)

    def add(self, issue: Optional[ValidationIssue]):
        """Add an issue if not None."""
        if issue:
            self.issues.append(issue)

    def has_critical_errors(self) -> bool:
        return any(i.level == "CRITICAL" for i in self.issues)

    def has_errors(self) -> bool:
        return any(i.level in ("CRITICAL", "ERROR") for i in self.issues)

    @property
    def critical_count(self) -> int:
        return sum(1 for i in self.issues if i.level == "CRITICAL")

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.level == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.level == "WARNING")

    @property
    def status(self) -> str:
        if self.has_critical_errors():
            return "CRITICAL"
        elif self.has_errors():
            return "ERROR"
        elif self.warning_count > 0:
            return "WARNING"
        return "OK"

    def to_dict(self) -> Dict:
        return {
            "timestamp": self.timestamp,
            "source": self.source,
            "status": self.status,
            "summary": {
                "critical": self.critical_count,
                "error": self.error_count,
                "warning": self.warning_count,
            },
            "issues": [
                {
                    "rule": i.rule,
                    "level": i.level,
                    "message": i.message,
                    "count": i.count,
                    "sample_ids": i.sample_ids[:10],
                    "sample_values": [str(v) for v in i.sample_values[:10]],
                    "details": i.details,
                }
                for i in self.issues
            ],
        }

    def save(self, directory: Path = None):
        """Save report to JSON file."""
        save_dir = directory or VALIDATION_DIR
        save_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{datetime.now().strftime('%Y-%m-%d_%H%M%S')}_{self.source}_validation.json"
        filepath = save_dir / filename
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        return filepath

    def print_summary(self):
        """Print a summary to console."""
        print(f"\nValidation Report ({self.source})")
        print("=" * 50)
        print(f"Status: {self.status}")
        print(f"Critical: {self.critical_count}, Errors: {self.error_count}, Warnings: {self.warning_count}")

        if self.issues:
            print("\nIssues:")
            for issue in self.issues:
                print(f"  [{issue.level}] {issue.rule}: {issue.message}")
                if issue.sample_ids:
                    print(f"    Sample IDs: {issue.sample_ids[:5]}")


class DataValidator:
    """Validates data quality at key pipeline checkpoints."""

    def __init__(self):
        self.rules = VALIDATION_RULES
        self.levels = VALIDATION_LEVELS

    def _get_level(self, rule: str) -> str:
        """Get the severity level for a rule."""
        for level, rules in self.levels.items():
            if rule in rules:
                return level
        return "WARNING"

    # =========================================================================
    # INDIVIDUAL VALIDATION CHECKS
    # =========================================================================

    def _check_required_columns(self, df: pd.DataFrame) -> Optional[ValidationIssue]:
        """Check that required columns exist."""
        required = self.rules["required_columns"]
        missing = [col for col in required if col not in df.columns]

        if missing:
            return ValidationIssue(
                rule="required_columns",
                level=self._get_level("required_columns"),
                message=f"Missing required columns: {missing}",
                details={"missing_columns": missing}
            )
        return None

    def _check_vote_share_range(self, df: pd.DataFrame) -> Optional[ValidationIssue]:
        """
        CRITICAL: Detect vote shares stored as 0-100 instead of 0-1.
        """
        issues = []
        min_val, max_val = self.rules["vote_share_range"]

        for col in ['democrat_vote_share', 'republican_vote_share']:
            if col not in df.columns:
                continue

            values = df[col].dropna()
            corrupted = values[(values < min_val) | (values > max_val)]

            if len(corrupted) > 0:
                issues.append({
                    'column': col,
                    'count': len(corrupted),
                    'sample_ids': df.loc[corrupted.index, 'market_id'].head(10).tolist(),
                    'sample_values': corrupted.head(10).tolist(),
                    'max_value': float(corrupted.max()),
                    'min_value': float(corrupted.min()),
                })

        if issues:
            total_count = sum(i['count'] for i in issues)
            return ValidationIssue(
                rule="vote_share_range",
                level=self._get_level("vote_share_range"),
                message=f"Found {total_count} vote shares outside 0-1 range (likely stored as percentage)",
                count=total_count,
                sample_ids=issues[0]['sample_ids'] if issues else [],
                sample_values=issues[0]['sample_values'] if issues else [],
                details={"columns": issues}
            )
        return None

    def _check_duplicate_vote_shares(self, df: pd.DataFrame) -> Optional[ValidationIssue]:
        """
        Detect different elections with identical vote share pairs.
        This indicates a copy/paste bug where GPT returned the same results
        for multiple elections.
        """
        if 'democrat_vote_share' not in df.columns or 'republican_vote_share' not in df.columns:
            return None

        # Get unique elections with vote shares
        election_cols = ['country', 'office', 'location', 'election_year', 'is_primary']
        available_cols = [c for c in election_cols if c in df.columns]

        if not available_cols:
            return None

        # Filter to rows with vote shares
        has_shares = df['democrat_vote_share'].notna() & df['republican_vote_share'].notna()
        vote_df = df[has_shares].copy()

        if len(vote_df) == 0:
            return None

        # Create election key
        vote_df['election_key'] = vote_df[available_cols].fillna('').astype(str).agg('|'.join, axis=1)

        # Get unique elections with their vote shares
        elections = vote_df.groupby('election_key').agg({
            'democrat_vote_share': 'first',
            'republican_vote_share': 'first',
            'market_id': 'first'
        }).reset_index()

        # Create vote share pair key
        elections['share_pair'] = (
            elections['democrat_vote_share'].round(4).astype(str) + '|' +
            elections['republican_vote_share'].round(4).astype(str)
        )

        # Find duplicates
        share_counts = elections['share_pair'].value_counts()
        duplicates = share_counts[share_counts >= 3]  # 3+ elections with same shares

        if len(duplicates) == 0:
            return None

        # Get details of duplicate groups
        duplicate_details = []
        for share_pair, count in duplicates.head(5).items():
            matching = elections[elections['share_pair'] == share_pair]
            duplicate_details.append({
                'vote_shares': share_pair,
                'count': int(count),
                'elections': matching['election_key'].head(5).tolist()
            })

        return ValidationIssue(
            rule="duplicate_vote_shares",
            level=self._get_level("duplicate_vote_shares"),
            message=f"Found {len(duplicates)} vote share pairs appearing in 3+ different elections",
            count=len(duplicates),
            details={"duplicates": duplicate_details}
        )

    def _check_platform_values(self, df: pd.DataFrame) -> Optional[ValidationIssue]:
        """Check that platform values are valid."""
        if 'platform' not in df.columns:
            return None

        valid = self.rules["valid_platforms"]
        invalid_mask = ~df['platform'].isin(valid) & df['platform'].notna()
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            invalid_values = df.loc[invalid_mask, 'platform'].unique().tolist()
            return ValidationIssue(
                rule="valid_platforms",
                level=self._get_level("valid_platforms"),
                message=f"Found {invalid_count} markets with invalid platform values",
                count=invalid_count,
                sample_ids=df.loc[invalid_mask, 'market_id'].head(10).tolist(),
                sample_values=invalid_values[:10],
            )
        return None

    def _check_electoral_metadata_completeness(self, df: pd.DataFrame) -> Optional[ValidationIssue]:
        """Check that electoral markets have required metadata."""
        # Filter to electoral markets
        electoral_mask = (
            df['political_category'].str.startswith('1.', na=False) |
            df['political_category'].str.contains('ELECTORAL', case=False, na=False)
        )
        electoral_df = df[electoral_mask]

        if len(electoral_df) == 0:
            return None

        # Check each required field including is_primary
        missing_counts = {}
        for col in self.rules["electoral_required"]:
            if col not in df.columns:
                missing_counts[col] = len(electoral_df)
            else:
                missing_counts[col] = electoral_df[col].isna().sum()

        # Calculate overall coverage
        total_fields = len(self.rules["electoral_required"]) * len(electoral_df)
        total_missing = sum(missing_counts.values())
        coverage = 1 - (total_missing / total_fields) if total_fields > 0 else 1

        min_coverage = self.rules["electoral_metadata_min_pct"]
        if coverage < min_coverage:
            return ValidationIssue(
                rule="electoral_metadata",
                level=self._get_level("electoral_metadata"),
                message=f"Electoral metadata coverage {coverage:.1%} below threshold {min_coverage:.1%}",
                details={
                    "coverage": coverage,
                    "threshold": min_coverage,
                    "missing_by_field": missing_counts,
                    "total_electoral_markets": len(electoral_df),
                }
            )
        return None

    def _check_resolution_completeness(self, df: pd.DataFrame) -> Optional[ValidationIssue]:
        """Check that closed markets have resolution outcomes."""
        if 'is_closed' not in df.columns:
            return None

        closed_mask = df['is_closed'] == True
        closed_df = df[closed_mask]

        if len(closed_df) == 0:
            return None

        # Check for missing winning_outcome
        if 'winning_outcome' in df.columns:
            missing_outcome = closed_df['winning_outcome'].isna().sum()
        else:
            missing_outcome = len(closed_df)

        resolution_rate = 1 - (missing_outcome / len(closed_df))
        min_rate = self.rules["resolution_rate_min_pct"]

        if resolution_rate < min_rate:
            return ValidationIssue(
                rule="resolution_completeness",
                level=self._get_level("resolution_completeness"),
                message=f"Resolution rate {resolution_rate:.1%} below threshold {min_rate:.1%}",
                count=missing_outcome,
                details={
                    "resolution_rate": resolution_rate,
                    "threshold": min_rate,
                    "closed_markets": len(closed_df),
                    "missing_outcomes": missing_outcome,
                }
            )
        return None

    # =========================================================================
    # MAIN VALIDATION METHODS
    # =========================================================================

    def validate_new_markets(self, new_df: pd.DataFrame) -> ValidationReport:
        """
        Validate new markets before merging to master.
        Run before pipeline_merge_to_master.py.
        """
        report = ValidationReport(source="pre_merge")

        # Schema checks
        report.add(self._check_required_columns(new_df))

        # Value range checks
        report.add(self._check_vote_share_range(new_df))
        report.add(self._check_platform_values(new_df))

        return report

    def run_all_checks(self, source: str = "pre_publish", df: pd.DataFrame = None) -> Dict:
        """
        Run all validation checks and return results as dict.

        Args:
            source: "pre_publish" or "pre_merge"
            df: DataFrame to validate (loads master CSV if None)

        Returns:
            Dict with keys: status, summary, issues, timestamp, source
        """
        if source == "pre_merge" and df is not None:
            report = self.validate_new_markets(df)
        else:
            report = self.validate_master_csv(df)
        return report.to_dict()

    def validate_master_csv(self, master_df: pd.DataFrame = None) -> ValidationReport:
        """
        Validate master CSV before publishing to website.
        Run before generate_web_data.py.
        """
        if master_df is None:
            master_df = pd.read_csv(MASTER_FILE, low_memory=False)

        report = ValidationReport(source="pre_publish")

        # Schema checks
        report.add(self._check_required_columns(master_df))

        # Value checks
        report.add(self._check_vote_share_range(master_df))
        report.add(self._check_platform_values(master_df))

        # Completeness checks
        report.add(self._check_electoral_metadata_completeness(master_df))
        report.add(self._check_resolution_completeness(master_df))

        # Anomaly checks
        report.add(self._check_duplicate_vote_shares(master_df))

        return report


def main():
    """Run validation on master CSV and print report."""
    print("\n" + "=" * 70)
    print("BELLWETHER DATA VALIDATION")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    validator = DataValidator()

    print("\nLoading master CSV...")
    df = pd.read_csv(MASTER_FILE, low_memory=False)
    print(f"  Total markets: {len(df):,}")

    print("\nRunning validation...")
    report = validator.validate_master_csv(df)

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
    exit_code = 1 if report.has_critical_errors() else 0
    exit(exit_code)
