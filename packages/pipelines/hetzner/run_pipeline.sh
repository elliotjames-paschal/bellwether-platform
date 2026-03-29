#!/bin/bash
# ============================================================================
# Bellwether Pipeline - Hetzner Daily Runner
# ============================================================================
# Called by cron. Runs the pipeline, then commits and pushes web data.
#
# Usage (manual):
#   sudo -u bellwether /opt/bellwether/packages/pipelines/hetzner/run_pipeline.sh
#   sudo -u bellwether /opt/bellwether/packages/pipelines/hetzner/run_pipeline.sh --full-refresh  # needs 4GB+ RAM
# ============================================================================

set -euo pipefail

INSTALL_DIR="/opt/bellwether"
VENV_DIR="/opt/bellwether/venv"
ENV_FILE="/opt/bellwether/.env"
LOG_DIR="/opt/bellwether/logs"
LOCK_FILE="/opt/bellwether/locks/pipeline.lock"
STALENESS_FILE="$INSTALL_DIR/docs/data/active_markets.json"
SUMMARY_FILE="$LOG_DIR/last_run_summary.txt"
MAX_STALENESS_HOURS=36

# Git push retry settings
GIT_PUSH_MAX_RETRIES=3
GIT_PUSH_RETRY_DELAY=15  # seconds

# Timestamp helper
ts() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $1"; }

# --------------------------------------------------------------------------
# Alert helper - sends email via Python logging system
# --------------------------------------------------------------------------
send_alert() {
    local subject="$1"
    local message="$2"
    # Append pipeline step results if available
    local summary=""
    if [[ -f "$SUMMARY_FILE" ]]; then
        summary=$(cat "$SUMMARY_FILE")
    fi
    ALERT_SUBJECT="$subject" ALERT_MESSAGE="$message" ALERT_SUMMARY="$summary" python3 -c "
import os, sys
sys.path.insert(0, '$INSTALL_DIR/packages/pipelines')
from logging_config import setup_logging, get_logger, flush_email
setup_logging(run_name='git_push_alert')
logger = get_logger('run_pipeline')
msg = os.environ['ALERT_SUBJECT'] + ': ' + os.environ['ALERT_MESSAGE']
summary = os.environ.get('ALERT_SUMMARY', '')
if summary:
    msg += '\n\nPipeline step results:\n' + summary
logger.error(msg)
flush_email()
" 2>/dev/null || log "WARNING: Failed to send alert email"
}

# --------------------------------------------------------------------------
# Prevent overlapping runs
# --------------------------------------------------------------------------
mkdir -p "$(dirname "$LOCK_FILE")"

if [[ -f "$LOCK_FILE" ]]; then
    LOCK_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [[ -n "$LOCK_PID" ]] && kill -0 "$LOCK_PID" 2>/dev/null; then
        log "ERROR: Pipeline already running (PID $LOCK_PID). Exiting."
        exit 1
    else
        log "WARNING: Stale lock file found (PID $LOCK_PID not running). Removing."
        rm -f "$LOCK_FILE"
    fi
fi

echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

# --------------------------------------------------------------------------
# Load secrets
# --------------------------------------------------------------------------
if [[ ! -f "$ENV_FILE" ]]; then
    log "ERROR: $ENV_FILE not found"
    exit 1
fi

PERMS=$(stat -c '%a' "$ENV_FILE")
if [[ "$PERMS" != "600" ]]; then
    log "WARNING: $ENV_FILE has permissions $PERMS (expected 600). Fixing..."
    chmod 600 "$ENV_FILE"
fi

set -a
source "$ENV_FILE"
set +a

if [[ -z "${OPENAI_API_KEY:-}" || "$OPENAI_API_KEY" == "sk-REPLACE_ME" ]]; then
    log "ERROR: OPENAI_API_KEY not set. Edit $ENV_FILE"
    exit 1
fi

# --------------------------------------------------------------------------
# Activate venv
# --------------------------------------------------------------------------
source "$VENV_DIR/bin/activate"

# --------------------------------------------------------------------------
# Pull latest code
# --------------------------------------------------------------------------
cd "$INSTALL_DIR"
if ! git pull --ff-only origin v2/hetzner 2>&1; then
    log "WARNING: git pull failed - attempting reset to remote"
    git fetch origin v2/hetzner
    git reset --hard origin/v2/hetzner
    log "Reset to origin/v2/hetzner"
fi

# --------------------------------------------------------------------------
# Check for OOM kills from previous run
# --------------------------------------------------------------------------
# If the previous run was OOM-killed, the process dies instantly (SIGKILL)
# and never reaches the alert code. Detect it here at the start of the next run.
check_previous_oom() {
    # Check dmesg for OOM kills of python3 in the last 48 hours
    local oom_lines
    oom_lines=$(dmesg --time-format iso 2>/dev/null | grep -i "oom.*python3\|killed process.*python3" | tail -5)
    if [[ -n "$oom_lines" ]]; then
        log "WARNING: OOM kill detected from previous run:"
        log "$oom_lines"
        send_alert "OOM kill detected" "Python3 was OOM-killed on $(hostname). Recent dmesg entries:\n$oom_lines\n\nThis likely means the previous pipeline run (possibly --full-refresh or --full-refresh) exceeded the 2GB RAM limit."
    fi
}
check_previous_oom

# --------------------------------------------------------------------------
# Run pipeline
# --------------------------------------------------------------------------
log "======================================================================"
log "  Bellwether Pipeline - Hetzner VPS"
log "  Host: $(hostname)"
log "  Python: $(python3 --version 2>&1)"
log "  Args: $*"
log "  API key: loaded (${#OPENAI_API_KEY} chars)"
log "======================================================================"

EXIT_CODE=0
python3 -u packages/pipelines/pipeline_daily_refresh.py "$@" || EXIT_CODE=$?

log "Pipeline finished with exit code: $EXIT_CODE"

# --------------------------------------------------------------------------
# Check if pipeline was OOM-killed (exit code 137 = SIGKILL)
# --------------------------------------------------------------------------
if [[ $EXIT_CODE -eq 137 ]]; then
    log "ERROR: Pipeline killed with SIGKILL (exit code 137) — likely OOM"
    send_alert "Pipeline OOM-killed" "Pipeline exited with code 137 (SIGKILL) on $(hostname). Args: $*. This typically means the process exceeded available RAM."
fi

# --------------------------------------------------------------------------
# Push website data to GitHub (triggers GitHub Pages deploy)
# --------------------------------------------------------------------------
log "Checking for website data changes..."

git add docs/data/
if git diff --staged --quiet; then
    log "No website data changes to commit"
else
    COMMIT_MSG="Daily data update $(date +'%Y-%m-%d')"
    git commit -m "$COMMIT_MSG"
    log "Committed: $COMMIT_MSG"

    # Retry loop for git push
    PUSH_SUCCESS=false
    for attempt in $(seq 1 $GIT_PUSH_MAX_RETRIES); do
        log "Git push attempt $attempt/$GIT_PUSH_MAX_RETRIES..."
        if git push origin v2/hetzner 2>&1; then
            log "Pushed to GitHub - Pages will auto-deploy"
            PUSH_SUCCESS=true
            break
        else
            log "WARNING: git push attempt $attempt failed"
            if [[ $attempt -lt $GIT_PUSH_MAX_RETRIES ]]; then
                log "Retrying in ${GIT_PUSH_RETRY_DELAY}s..."
                sleep $GIT_PUSH_RETRY_DELAY
                # Double the delay for next retry
                GIT_PUSH_RETRY_DELAY=$((GIT_PUSH_RETRY_DELAY * 2))
            fi
        fi
    done

    if [[ "$PUSH_SUCCESS" != "true" ]]; then
        log "ERROR: git push failed after $GIT_PUSH_MAX_RETRIES attempts"
        send_alert "Git push failed" "Failed after $GIT_PUSH_MAX_RETRIES attempts. Data committed locally but not pushed to GitHub."
        EXIT_CODE=1
    fi
fi

# --------------------------------------------------------------------------
# Data staleness check
# --------------------------------------------------------------------------
if [[ -f "$STALENESS_FILE" ]]; then
    FILE_AGE_SECONDS=$(( $(date +%s) - $(stat -c '%Y' "$STALENESS_FILE") ))
    FILE_AGE_HOURS=$(( FILE_AGE_SECONDS / 3600 ))

    if [[ $FILE_AGE_HOURS -gt $MAX_STALENESS_HOURS ]]; then
        log "WARNING: $STALENESS_FILE is ${FILE_AGE_HOURS}h old (threshold: ${MAX_STALENESS_HOURS}h)"
        send_alert "Data staleness warning" "active_markets.json is ${FILE_AGE_HOURS} hours old (threshold: ${MAX_STALENESS_HOURS}h). Pipeline may not be updating data correctly."
    else
        log "Data freshness OK: active_markets.json is ${FILE_AGE_HOURS}h old"
    fi
else
    log "WARNING: Staleness check file not found: $STALENESS_FILE"
    send_alert "Data staleness warning" "active_markets.json not found - pipeline may not have generated output."
fi

# --------------------------------------------------------------------------
# Cleanup secrets from environment
# --------------------------------------------------------------------------
unset OPENAI_API_KEY DOME_API_KEY GOOGLE_CIVIC_API_KEY PREDICTIONHUNT_API_KEY CLOUDFLARE_API_TOKEN

log "======================================================================"
log "  Pipeline complete (exit code: $EXIT_CODE)"
log "======================================================================"

exit $EXIT_CODE
