#!/usr/bin/env python3
"""
Daily Audit Summary Email

Generates and sends a daily summary email with:
1. Pipeline health status
2. New markets summary with samples per category
3. Panel A/B winner market audit
4. Validation/anomaly results

This runs at the end of the daily pipeline and ALWAYS sends an email
(not just on errors) so you can review daily operations.
"""

import pandas as pd
import json
import smtplib
import random
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import BASE_DIR, DATA_DIR

# Use same email config as logging_config.py
LOGS_DIR = BASE_DIR / "logs"
EMAIL_CONFIG_FILE = LOGS_DIR / "email_config.json"
MASTER_FILE = DATA_DIR / "combined_political_markets_with_electoral_details_UPDATED.csv"
SELECTIONS_FILE = DATA_DIR / "election_winner_selections.json"
PANEL_A_FILE = DATA_DIR / "election_winner_panel_a_detailed.csv"


def load_email_config():
    """Load email configuration (same format as logging_config.py)."""
    if not EMAIL_CONFIG_FILE.exists():
        return None
    try:
        with open(EMAIL_CONFIG_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def get_new_markets_summary(df, days=1):
    """Get summary of new markets added in last N days."""
    if 'date_added' not in df.columns:
        return None, None

    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
    cutoff = datetime.now() - timedelta(days=days)
    recent = df[df['date_added'] >= cutoff].copy()

    if len(recent) == 0:
        return None, None

    # Summary by category
    by_category = recent.groupby('political_category').agg({
        'market_id': 'count',
        'question': list
    }).rename(columns={'market_id': 'count'})

    # Sample 5 random markets per category
    samples = {}
    for cat in by_category.index:
        questions = by_category.loc[cat, 'question']
        sample_size = min(5, len(questions))
        samples[cat] = random.sample(questions, sample_size)

    # Summary stats
    summary = {
        'total': len(recent),
        'by_platform': recent['platform'].value_counts().to_dict(),
        'by_category': by_category['count'].to_dict(),
    }

    return summary, samples


def get_panel_a_audit(df):
    """Audit Panel A - winner markets selected for each platform."""
    if not SELECTIONS_FILE.exists():
        return None

    with open(SELECTIONS_FILE, 'r') as f:
        selections = json.load(f)

    # Get elections processed today
    today = datetime.now().strftime('%Y-%m-%d')

    audit = {
        'total_elections': len(selections),
        'with_pm_winner': 0,
        'with_kalshi_winner': 0,
        'with_both': 0,
        'sample_selections': []
    }

    for key, data in selections.items():
        if not data.get('election_found', False):
            continue

        has_pm = data.get('polymarket_winner_id') is not None
        has_k = data.get('kalshi_winner_id') is not None

        if has_pm:
            audit['with_pm_winner'] += 1
        if has_k:
            audit['with_kalshi_winner'] += 1
        if has_pm and has_k:
            audit['with_both'] += 1

    # Sample 10 random elections with both platforms
    both_platform_elections = [
        (k, v) for k, v in selections.items()
        if v.get('election_found') and v.get('polymarket_winner_id') and v.get('kalshi_winner_id')
    ]

    sample_size = min(10, len(both_platform_elections))
    if sample_size > 0:
        sample = random.sample(both_platform_elections, sample_size)
        for key, data in sample:
            # Get market questions
            pm_id = data.get('polymarket_winner_id')
            k_id = data.get('kalshi_winner_id')

            pm_q = df[df['market_id'].astype(str) == str(pm_id)]['question'].values
            k_q = df[df['market_id'].astype(str) == str(k_id)]['question'].values

            audit['sample_selections'].append({
                'election': key,
                'pm_market': pm_q[0][:80] if len(pm_q) > 0 else f"ID: {pm_id}",
                'kalshi_market': k_q[0][:80] if len(k_q) > 0 else f"ID: {k_id}",
                'd_share': data.get('democrat_vote_share'),
                'r_share': data.get('republican_vote_share'),
            })

    return audit


def get_panel_b_audit(df):
    """Audit Panel B - overlapping elections between platforms.

    Returns ALL overlapping elections with full market details for manual review.
    """
    if not PANEL_A_FILE.exists():
        return None

    panel_a = pd.read_csv(PANEL_A_FILE)

    # Find elections on both platforms
    election_cols = ['country', 'office', 'location', 'election_year', 'is_primary']

    # Group by election and check platforms
    elections = panel_a.groupby(election_cols).agg({
        'platform': lambda x: set(x),
        'winner_prediction': list,
        'market_id': list
    }).reset_index()

    # Filter to elections with both platforms
    both = elections[elections['platform'].apply(lambda x: 'Polymarket' in x and 'Kalshi' in x)]

    audit = {
        'total_elections_panel_a': len(elections),
        'overlapping_elections': len(both),
        'pm_only': len(elections[elections['platform'].apply(lambda x: x == {'Polymarket'})]),
        'kalshi_only': len(elections[elections['platform'].apply(lambda x: x == {'Kalshi'})]),
        'all_overlaps': []  # ALL overlapping elections, not just a sample
    }

    # Get ALL overlapping elections with full details
    for _, row in both.iterrows():
        # Get market details for each platform
        election_markets = panel_a[
            (panel_a['country'] == row['country']) &
            (panel_a['office'] == row['office']) &
            (panel_a['location'] == row['location']) &
            (panel_a['election_year'] == row['election_year'])
        ]

        pm_row = election_markets[election_markets['platform'] == 'Polymarket']
        k_row = election_markets[election_markets['platform'] == 'Kalshi']

        pm_pred = None
        pm_market_id = None
        pm_question = None
        k_pred = None
        k_market_id = None
        k_question = None

        if len(pm_row) > 0:
            pm_pred = pm_row.iloc[0]['winner_prediction']
            pm_market_id = pm_row.iloc[0]['market_id']
            # Get question from master CSV
            pm_match = df[df['market_id'].astype(str) == str(pm_market_id)]
            if len(pm_match) > 0:
                pm_question = pm_match.iloc[0]['question']

        if len(k_row) > 0:
            k_pred = k_row.iloc[0]['winner_prediction']
            k_market_id = k_row.iloc[0]['market_id']
            # Get question from master CSV
            k_match = df[df['market_id'].astype(str) == str(k_market_id)]
            if len(k_match) > 0:
                k_question = k_match.iloc[0]['question']

        election_desc = f"{int(row['election_year'])} {row['office']} - {row['location']}"

        difference = None
        if pm_pred is not None and k_pred is not None:
            difference = abs(pm_pred - k_pred)

        audit['all_overlaps'].append({
            'election': election_desc,
            'pm_market_id': pm_market_id,
            'pm_question': pm_question,
            'pm_prediction': round(pm_pred, 3) if pm_pred is not None else None,
            'kalshi_market_id': k_market_id,
            'kalshi_question': k_question,
            'kalshi_prediction': round(k_pred, 3) if k_pred is not None else None,
            'difference': round(difference, 3) if difference is not None else None
        })

    # Sort by difference (largest first) to highlight potential issues
    audit['all_overlaps'].sort(key=lambda x: x['difference'] if x['difference'] is not None else 0, reverse=True)

    return audit


def get_validation_summary():
    """Get latest validation results."""
    validation_dir = DATA_DIR / "audit" / "validation"
    if not validation_dir.exists():
        return None

    # Find most recent validation file
    files = sorted(validation_dir.glob("*_pre_publish_validation.json"), reverse=True)
    if not files:
        return None

    with open(files[0], 'r') as f:
        return json.load(f)


def get_anomaly_summary():
    """Get latest anomaly detection results."""
    anomaly_dir = DATA_DIR / "audit" / "anomalies"
    if not anomaly_dir.exists():
        return None

    # Find most recent anomaly file
    files = sorted(anomaly_dir.glob("*_anomalies.json"), reverse=True)
    if not files:
        return None

    with open(files[0], 'r') as f:
        return json.load(f)


def format_email_body(df, new_markets_summary, new_markets_samples,
                      panel_a_audit, panel_b_audit, validation, anomalies):
    """Format the daily summary email body."""

    lines = []
    lines.append("=" * 60)
    lines.append("BELLWETHER DAILY AUDIT SUMMARY")
    lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 60)
    lines.append("")

    # Overall health
    lines.append("## OVERALL HEALTH")
    lines.append("")

    if validation:
        status = validation.get('status', 'UNKNOWN')
        if status == 'OK':
            lines.append(f"✓ Validation: PASSED")
        else:
            lines.append(f"✗ Validation: {status}")
            lines.append(f"  Critical: {validation['summary']['critical']}, Errors: {validation['summary']['error']}, Warnings: {validation['summary']['warning']}")

    if anomalies:
        if anomalies.get('anomalies_detected', 0) == 0:
            lines.append(f"✓ Anomaly Detection: PASSED")
        else:
            lines.append(f"⚠ Anomaly Detection: {anomalies['anomalies_detected']} issues found")
            for a in anomalies.get('anomalies', []):
                lines.append(f"  - [{a['severity']}] {a['id']}: {a['description']}")

    lines.append("")
    lines.append(f"Total markets in database: {len(df):,}")
    lines.append(f"  Polymarket: {len(df[df['platform'] == 'Polymarket']):,}")
    lines.append(f"  Kalshi: {len(df[df['platform'] == 'Kalshi']):,}")
    lines.append("")

    # New markets
    lines.append("-" * 60)
    lines.append("## NEW MARKETS (Last 24 Hours)")
    lines.append("")

    if new_markets_summary:
        lines.append(f"Total added: {new_markets_summary['total']:,}")
        lines.append(f"  By platform: {new_markets_summary['by_platform']}")
        lines.append("")

        lines.append("### Sample Markets by Category (5 random each)")
        lines.append("")

        for cat, samples in sorted(new_markets_samples.items()):
            count = new_markets_summary['by_category'].get(cat, 0)
            lines.append(f"**{cat}** ({count} new)")
            for q in samples:
                lines.append(f"  • {q[:100]}{'...' if len(q) > 100 else ''}")
            lines.append("")
    else:
        lines.append("No new markets added in last 24 hours")
        lines.append("")

    # Panel A audit
    lines.append("-" * 60)
    lines.append("## PANEL A - WINNER MARKET SELECTIONS")
    lines.append("")

    if panel_a_audit:
        lines.append(f"Total elections with results: {panel_a_audit['total_elections']}")
        lines.append(f"  With Polymarket winner: {panel_a_audit['with_pm_winner']}")
        lines.append(f"  With Kalshi winner: {panel_a_audit['with_kalshi_winner']}")
        lines.append(f"  With BOTH platforms: {panel_a_audit['with_both']}")
        lines.append("")

        if panel_a_audit['sample_selections']:
            lines.append("### Sample Winner Market Selections")
            lines.append("")
            for s in panel_a_audit['sample_selections'][:5]:
                lines.append(f"**{s['election']}**")
                lines.append(f"  PM: {s['pm_market']}")
                lines.append(f"  Kalshi: {s['kalshi_market']}")
                if s['d_share'] and s['r_share']:
                    lines.append(f"  Vote shares: D={s['d_share']:.1%}, R={s['r_share']:.1%}")
                lines.append("")
    else:
        lines.append("No Panel A data available")
        lines.append("")

    # Panel B audit
    lines.append("-" * 60)
    lines.append("## PANEL B - PLATFORM OVERLAP (MANUAL REVIEW)")
    lines.append("")

    if panel_b_audit:
        lines.append(f"Total elections in Panel A: {panel_b_audit['total_elections_panel_a']}")
        lines.append(f"  Overlapping (both platforms): {panel_b_audit['overlapping_elections']}")
        lines.append(f"  Polymarket only: {panel_b_audit['pm_only']}")
        lines.append(f"  Kalshi only: {panel_b_audit['kalshi_only']}")
        lines.append("")

        if panel_b_audit['all_overlaps']:
            lines.append("### ALL Overlapping Elections - Winner Markets Selected")
            lines.append("(Sorted by prediction difference - largest first)")
            lines.append("")

            for i, s in enumerate(panel_b_audit['all_overlaps'], 1):
                lines.append(f"--- [{i}/{len(panel_b_audit['all_overlaps'])}] {s['election']} ---")

                # Polymarket market
                lines.append(f"  POLYMARKET:")
                if s['pm_question']:
                    lines.append(f"    Market: {s['pm_question']}")
                else:
                    lines.append(f"    Market ID: {s['pm_market_id']}")
                lines.append(f"    Prediction: {s['pm_prediction']}")

                # Kalshi market
                lines.append(f"  KALSHI:")
                if s['kalshi_question']:
                    lines.append(f"    Market: {s['kalshi_question']}")
                else:
                    lines.append(f"    Market ID: {s['kalshi_market_id']}")
                lines.append(f"    Prediction: {s['kalshi_prediction']}")

                # Difference
                if s['difference'] is not None:
                    diff_pct = s['difference'] * 100
                    flag = " ⚠️ LARGE DIFF" if diff_pct > 10 else ""
                    lines.append(f"  DIFFERENCE: {diff_pct:.1f}%{flag}")

                lines.append("")
    else:
        lines.append("No Panel B data available")
        lines.append("")

    lines.append("=" * 60)
    lines.append("End of Daily Summary")
    lines.append("=" * 60)

    return "\n".join(lines)


def send_daily_summary_email(body):
    """Send the daily summary email using same config as pipeline error emails."""
    config = load_email_config()
    if not config:
        print("No email config found - printing to console instead")
        print(body)
        return False

    try:
        # Use same format as logging_config.py
        msg = MIMEMultipart()
        msg['From'] = config.get('from_email', config.get('smtp_user'))
        msg['To'] = ', '.join(config.get('recipients', []))
        msg['Subject'] = f"✅ Bellwether Daily Audit - {datetime.now().strftime('%Y-%m-%d')}"

        msg.attach(MIMEText(body, 'plain'))

        # Use same SMTP settings as logging_config.py
        smtp_host = config.get('smtp_host', 'smtp.stanford.edu')
        smtp_port = config.get('smtp_port', 25)
        smtp_user = config.get('smtp_user')
        smtp_password = config.get('smtp_password')

        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            # Try STARTTLS (required by many servers)
            try:
                server.starttls()
            except smtplib.SMTPNotSupportedError:
                pass  # Server doesn't support TLS, continue anyway
            # Only authenticate if credentials provided
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)

        print(f"Daily summary email sent to {len(config.get('recipients', []))} recipient(s)")
        return True

    except Exception as e:
        print(f"Failed to send email: {e}")
        print("Email body:")
        print(body)
        return False


def generate_and_send_summary():
    """Main function to generate and send daily summary."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Generating daily audit summary...")

    # Load data
    df = pd.read_csv(MASTER_FILE, low_memory=False)

    # Gather all audit data
    new_markets_summary, new_markets_samples = get_new_markets_summary(df, days=1)
    panel_a_audit = get_panel_a_audit(df)
    panel_b_audit = get_panel_b_audit(df)
    validation = get_validation_summary()
    anomalies = get_anomaly_summary()

    # Format email
    body = format_email_body(
        df, new_markets_summary, new_markets_samples,
        panel_a_audit, panel_b_audit, validation, anomalies
    )

    # Send email
    success = send_daily_summary_email(body)

    # Also save to file for reference
    summary_dir = DATA_DIR / "audit" / "daily_summaries"
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_file = summary_dir / f"{datetime.now().strftime('%Y-%m-%d')}_summary.txt"
    with open(summary_file, 'w') as f:
        f.write(body)
    print(f"  Summary saved to {summary_file}")

    return success


if __name__ == "__main__":
    generate_and_send_summary()
