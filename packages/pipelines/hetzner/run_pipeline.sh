#!/bin/bash
# ============================================================================
# Bellwether Pipeline - Hetzner Daily Runner
# ============================================================================
# Called by cron. Runs the pipeline, then commits and pushes web data.
#
# Usage (manual):
#   sudo -u bellwether /opt/bellwether/packages/pipelines/hetzner/run_pipeline.sh
#   sudo -u bellwether /opt/bellwether/packages/pipelines/hetzner/run_pipeline.sh --full-refresh
# ============================================================================

set -euo pipefail

INSTALL_DIR="/opt/bellwether"
VENV_DIR="/opt/bellwether/venv"
ENV_FILE="/opt/bellwether/.env"
LOG_DIR="/opt/bellwether/logs"
LOCK_FILE="/tmp/bellwether-pipeline.lock"

# Timestamp helper
ts() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $1"; }

# --------------------------------------------------------------------------
# Prevent overlapping runs
# --------------------------------------------------------------------------
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
git pull --ff-only origin v2/hetzner 2>/dev/null || log "WARNING: git pull failed (continuing with current code)"

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

    if git push origin v2/hetzner 2>&1; then
        log "Pushed to GitHub - Pages will auto-deploy"
    else
        log "ERROR: git push failed"
    fi
fi

# --------------------------------------------------------------------------
# Cleanup secrets from environment
# --------------------------------------------------------------------------
unset OPENAI_API_KEY DOME_API_KEY GOOGLE_CIVIC_API_KEY PREDICTIONHUNT_API_KEY CLOUDFLARE_API_TOKEN

log "======================================================================"
log "  Pipeline complete"
log "======================================================================"

exit $EXIT_CODE
