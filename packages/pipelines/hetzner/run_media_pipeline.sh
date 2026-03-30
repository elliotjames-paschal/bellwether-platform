#!/bin/bash
# ============================================================================
# Bellwether Media Pipeline - Hetzner Daily Runner
# ============================================================================
# Runs the media citation pipeline from a SEPARATE clone on the 'media' branch.
# Completely isolated from the production pipeline (v2/hetzner).
#
# Layout:
#   /opt/bellwether/          <- production (v2/hetzner branch)
#   /opt/bellwether-media/    <- media pipeline (media branch)
#   /opt/bellwether/.env      <- shared secrets (read-only from here)
#   /opt/bellwether/venv/     <- shared venv
#
# Usage (manual):
#   sudo -u bellwether /opt/bellwether-media/packages/pipelines/hetzner/run_media_pipeline.sh
#   sudo -u bellwether /opt/bellwether-media/packages/pipelines/hetzner/run_media_pipeline.sh --backfill 30
#
# Setup (one-time, as root):
#   git clone --branch media https://github.com/elliotjames-paschal/bellwether-platform.git /opt/bellwether-media
#   chown -R bellwether:bellwether /opt/bellwether-media
#   # Install any new deps:
#   sudo -u bellwether /opt/bellwether/venv/bin/pip install -r /opt/bellwether-media/packages/pipelines/requirements.txt
#   # Add cron (as bellwether user):
#   # 30 7 * * * /opt/bellwether-media/packages/pipelines/hetzner/run_media_pipeline.sh >> /opt/bellwether/logs/media_cron.log 2>&1
# ============================================================================

set -euo pipefail

MEDIA_DIR="/opt/bellwether-media"
PROD_DIR="/opt/bellwether"
VENV_DIR="$PROD_DIR/venv"
ENV_FILE="$PROD_DIR/.env"
LOG_DIR="$PROD_DIR/logs"
LOCK_FILE="$PROD_DIR/locks/media_pipeline.lock"

BRANCH="media"

# Git push retry settings
GIT_PUSH_MAX_RETRIES=3
GIT_PUSH_RETRY_DELAY=15

ts() { date "+%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(ts)] $1"; }

# --------------------------------------------------------------------------
# Prevent overlapping media pipeline runs
# --------------------------------------------------------------------------
mkdir -p "$(dirname "$LOCK_FILE")"

if [[ -f "$LOCK_FILE" ]]; then
    LOCK_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [[ -n "$LOCK_PID" ]] && kill -0 "$LOCK_PID" 2>/dev/null; then
        log "ERROR: Media pipeline already running (PID $LOCK_PID). Exiting."
        exit 1
    else
        log "WARNING: Stale lock file found (PID $LOCK_PID not running). Removing."
        rm -f "$LOCK_FILE"
    fi
fi

echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

# --------------------------------------------------------------------------
# Load secrets (shared with production)
# --------------------------------------------------------------------------
if [[ ! -f "$ENV_FILE" ]]; then
    log "ERROR: $ENV_FILE not found"
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

# NewsAPI key is optional but recommended
if [[ -z "${NEWSAPI_KEY:-}" ]]; then
    log "WARNING: NEWSAPI_KEY not set — NewsAPI source will be skipped"
fi

# OpenAI is required for market extraction step
if [[ -z "${OPENAI_API_KEY:-}" || "$OPENAI_API_KEY" == "sk-REPLACE_ME" ]]; then
    log "ERROR: OPENAI_API_KEY not set. Edit $ENV_FILE"
    exit 1
fi

# --------------------------------------------------------------------------
# Activate shared venv
# --------------------------------------------------------------------------
source "$VENV_DIR/bin/activate"

# --------------------------------------------------------------------------
# Pull latest media branch (never touches v2/hetzner)
# --------------------------------------------------------------------------
cd "$MEDIA_DIR"
if ! git pull --ff-only origin "$BRANCH" 2>&1; then
    log "WARNING: git pull failed - attempting reset to remote"
    git fetch origin "$BRANCH"
    git reset --hard "origin/$BRANCH"
    log "Reset to origin/$BRANCH"
fi

# --------------------------------------------------------------------------
# Run media pipeline
# --------------------------------------------------------------------------
log "======================================================================"
log "  Bellwether MEDIA Pipeline"
log "  Host: $(hostname)"
log "  Branch: $BRANCH"
log "  Dir: $MEDIA_DIR"
log "  Args: $*"
log "======================================================================"

EXIT_CODE=0
python3 -u packages/pipelines/run_media_pipeline.py "$@" || EXIT_CODE=$?

log "Media pipeline finished with exit code: $EXIT_CODE"

# --------------------------------------------------------------------------
# Push media web data to GitHub (media branch only, NOT v2/hetzner)
# --------------------------------------------------------------------------
log "Checking for media data changes..."

git add docs/data/media_summary.json docs/data/media_outlets.json docs/data/media_citations.json 2>/dev/null || true

if git diff --staged --quiet; then
    log "No media data changes to commit"
else
    COMMIT_MSG="Media data update $(date +'%Y-%m-%d')"
    git commit -m "$COMMIT_MSG"
    log "Committed: $COMMIT_MSG"

    # Push to origin (main repo, media branch)
    PUSH_SUCCESS=false
    for attempt in $(seq 1 $GIT_PUSH_MAX_RETRIES); do
        log "Git push attempt $attempt/$GIT_PUSH_MAX_RETRIES (origin)..."
        if git push origin "$BRANCH" 2>&1; then
            log "Pushed to origin (branch: $BRANCH)"
            PUSH_SUCCESS=true
            break
        else
            log "WARNING: git push attempt $attempt failed"
            if [[ $attempt -lt $GIT_PUSH_MAX_RETRIES ]]; then
                log "Retrying in ${GIT_PUSH_RETRY_DELAY}s..."
                sleep $GIT_PUSH_RETRY_DELAY
                GIT_PUSH_RETRY_DELAY=$((GIT_PUSH_RETRY_DELAY * 2))
            fi
        fi
    done

    if [[ "$PUSH_SUCCESS" != "true" ]]; then
        log "ERROR: git push to origin failed after $GIT_PUSH_MAX_RETRIES attempts"
        EXIT_CODE=1
    fi

    # Push to pages fork (triggers GitHub Pages deploy)
    if git push pages "$BRANCH" 2>&1; then
        log "Pushed to pages fork (GitHub Pages will auto-deploy)"
    else
        log "WARNING: git push to pages fork failed (non-fatal)"
    fi
fi

# --------------------------------------------------------------------------
# Cleanup
# --------------------------------------------------------------------------
unset OPENAI_API_KEY NEWSAPI_KEY DOME_API_KEY GOOGLE_CIVIC_API_KEY PREDICTIONHUNT_API_KEY CLOUDFLARE_API_TOKEN

log "======================================================================"
log "  Media pipeline complete (exit code: $EXIT_CODE)"
log "  Branch: $BRANCH (v2/hetzner NOT touched)"
log "======================================================================"

exit $EXIT_CODE
