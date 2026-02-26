#!/bin/bash
# ==============================================================================
# Bellwether Daily Pipeline Runner
#
# Runs the full pipeline, then commits and pushes updated website data
# to GitHub so GitHub Pages auto-deploys.
#
# Usage:
#   bash scripts/run_daily_pipeline.sh          # normal daily run
#   bash scripts/run_daily_pipeline.sh --full-refresh  # full refresh
# ==============================================================================

set -euo pipefail

PROJECT_DIR="/Users/paschal/Hall Research Dropbox/Elliot Paschal/Polymarket:Kalshi"
SCRIPTS_DIR="$PROJECT_DIR/scripts"
WEBSITE_DIR="$PROJECT_DIR/website"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/daily_cron.log"
ERROR_FILE="$LOG_DIR/pipeline_error.tmp"

# Email config (matches email_config.json)
EMAIL_FROM="paschal@stanford.edu"
EMAIL_TO="paschal@stanford.edu"
SMTP_HOST="smtp.stanford.edu"
SMTP_PORT="25"

mkdir -p "$LOG_DIR"

# Timestamp helper
ts() { date "+%Y-%m-%d %H:%M:%S"; }

# Log to both file and stdout
log() { echo "[$(ts)] $1" | tee -a "$LOG_FILE"; }

# Send error email via SMTP
send_error_email() {
    local subject="$1"
    local body="$2"

    # Try to send email using Python (more reliable than sendmail on macOS)
    /usr/bin/python3 << PYEOF 2>/dev/null || true
import smtplib
from email.mime.text import MIMEText

msg = MIMEText("""$body""")
msg['Subject'] = "$subject"
msg['From'] = "$EMAIL_FROM"
msg['To'] = "$EMAIL_TO"

try:
    with smtplib.SMTP("$SMTP_HOST", $SMTP_PORT, timeout=30) as server:
        server.sendmail("$EMAIL_FROM", ["$EMAIL_TO"], msg.as_string())
    print("Email sent successfully")
except Exception as e:
    print(f"Failed to send email: {e}")
PYEOF
}

# Rotate log if over 1MB
if [ -f "$LOG_FILE" ] && [ "$(stat -f%z "$LOG_FILE" 2>/dev/null || echo 0)" -gt 1048576 ]; then
    mv "$LOG_FILE" "$LOG_FILE.prev"
fi

log "=========================================="
log "BELLWETHER DAILY PIPELINE START"
log "=========================================="

# --- Step 1: Run the pipeline ---
log "Running pipeline_daily_refresh.py $*"

# Capture both stdout and stderr, keep exit code
EXIT_CODE=0
/usr/bin/python3 "$SCRIPTS_DIR/pipeline_daily_refresh.py" "$@" >> "$LOG_FILE" 2> >(tee "$ERROR_FILE" >> "$LOG_FILE") || EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    log "Pipeline completed successfully"
    rm -f "$ERROR_FILE"
else
    log "ERROR: Pipeline exited with code $EXIT_CODE"
    log "Check logs for details. Continuing to push any partial web data updates."

    # Send email notification for pipeline failure
    ERROR_CONTENT=""
    if [ -f "$ERROR_FILE" ]; then
        ERROR_CONTENT=$(tail -50 "$ERROR_FILE")
    fi

    send_error_email \
        "[Bellwether] Pipeline FAILED - $(date +'%Y-%m-%d %H:%M')" \
        "The Bellwether daily pipeline failed to run.

Exit Code: $EXIT_CODE
Time: $(date)
Host: $(hostname)

Error Output:
$ERROR_CONTENT

Check the full log at:
$LOG_FILE"

    log "Error notification email sent"
    rm -f "$ERROR_FILE"
fi

# --- Step 2: Commit and push website data ---
log "Checking for website data changes..."

cd "$WEBSITE_DIR"

# Stage only the data directory (the JSON files generate_web_data.py writes)
git add data/

if git diff --staged --quiet; then
    log "No website data changes to commit"
else
    COMMIT_MSG="Daily data update $(date +'%Y-%m-%d')"
    git commit -m "$COMMIT_MSG"
    log "Committed: $COMMIT_MSG"

    if git push origin main >> "$LOG_FILE" 2>&1; then
        log "Pushed to GitHub - Pages will auto-deploy"
    else
        log "ERROR: git push failed"
    fi
fi

log "=========================================="
log "BELLWETHER DAILY PIPELINE DONE"
log "=========================================="
