#!/bin/bash
# ============================================================================
# Bellwether Pipeline - Hetzner VPS Setup
# ============================================================================
# Run this ONCE on a fresh Hetzner CX22 (Ubuntu 24.04 LTS).
#
# Prerequisites:
#   - SSH into your VPS as root
#   - Have your API keys ready (OPENAI_API_KEY, DOME_API_KEY, etc.)
#
# Usage:
#   ssh root@<your-vps-ip>
#   bash bellwether-platform/packages/pipelines/hetzner/setup.sh
#
# What this does:
#   1. Creates a non-root 'bellwether' user
#   2. Installs Python 3.12, git, system deps
#   3. Clones the repo & creates a venv
#   4. Installs Python dependencies
#   5. Creates secrets file (you fill in API keys)
#   6. Sets up cron for daily pipeline + git push
#   7. Configures email alerts via logging system
#   8. Sets up unattended security upgrades
#   9. Configures UFW firewall (SSH only)
# ============================================================================

set -euo pipefail

# --------------------------------------------------------------------------
# Configuration — edit these before running
# --------------------------------------------------------------------------
REPO_URL="https://github.com/vcbee/bellwether-platform.git"
REPO_BRANCH="v2/data-full"
SERVICE_USER="bellwether"
INSTALL_DIR="/opt/bellwether"
DATA_DIR="/opt/bellwether/data"
VENV_DIR="/opt/bellwether/venv"
LOG_DIR="/opt/bellwether/logs"

# --------------------------------------------------------------------------
# 0. Preflight checks
# --------------------------------------------------------------------------
echo "============================================"
echo "  Bellwether Pipeline - Hetzner VPS Setup"
echo "============================================"
echo ""

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: Run this as root (or with sudo)"
    exit 1
fi

# Detect OS
if ! grep -qi "ubuntu" /etc/os-release 2>/dev/null; then
    echo "WARNING: This script targets Ubuntu 24.04. Your OS may differ."
    read -rp "Continue anyway? [y/N] " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || exit 1
fi

# --------------------------------------------------------------------------
# 1. System packages
# --------------------------------------------------------------------------
echo ""
echo "[1/9] Installing system packages..."

export DEBIAN_FRONTEND=noninteractive

apt-get update -qq
apt-get install -y -qq \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    python3-pip \
    git \
    curl \
    jq \
    build-essential \
    unattended-upgrades \
    ufw \
    msmtp \
    msmtp-mta

echo "  Installed: python3.12, git, curl, jq, build-essential, ufw, msmtp"

# --------------------------------------------------------------------------
# 2. Create service user
# --------------------------------------------------------------------------
echo ""
echo "[2/9] Creating service user '$SERVICE_USER'..."

if id "$SERVICE_USER" &>/dev/null; then
    echo "  User '$SERVICE_USER' already exists"
else
    useradd --system --create-home --shell /bin/bash "$SERVICE_USER"
    echo "  Created user '$SERVICE_USER'"
fi

# --------------------------------------------------------------------------
# 3. Clone repository
# --------------------------------------------------------------------------
echo ""
echo "[3/9] Setting up repository..."

if [[ -d "$INSTALL_DIR/.git" ]]; then
    echo "  Repo already cloned at $INSTALL_DIR"
    cd "$INSTALL_DIR"
    sudo -u "$SERVICE_USER" git fetch origin
    sudo -u "$SERVICE_USER" git checkout "$REPO_BRANCH"
    sudo -u "$SERVICE_USER" git pull --ff-only origin "$REPO_BRANCH"
else
    mkdir -p "$(dirname "$INSTALL_DIR")"
    git clone --branch "$REPO_BRANCH" "$REPO_URL" "$INSTALL_DIR"
    chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"
    echo "  Cloned $REPO_URL -> $INSTALL_DIR"
fi

# --------------------------------------------------------------------------
# 4. Create directories
# --------------------------------------------------------------------------
echo ""
echo "[4/9] Creating directories..."

mkdir -p "$DATA_DIR" "$LOG_DIR" "$LOG_DIR/pipeline_runs"
chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"

echo "  $DATA_DIR"
echo "  $LOG_DIR"

# --------------------------------------------------------------------------
# 5. Python virtual environment + dependencies
# --------------------------------------------------------------------------
echo ""
echo "[5/9] Setting up Python environment..."

if [[ ! -d "$VENV_DIR" ]]; then
    sudo -u "$SERVICE_USER" python3.12 -m venv "$VENV_DIR"
    echo "  Created venv at $VENV_DIR"
else
    echo "  Venv already exists at $VENV_DIR"
fi

# Install deps as the service user
sudo -u "$SERVICE_USER" "$VENV_DIR/bin/pip" install --upgrade pip wheel setuptools -q
sudo -u "$SERVICE_USER" "$VENV_DIR/bin/pip" install -q \
    -r "$INSTALL_DIR/packages/pipelines/requirements.txt"

# Install sentence-transformers (used for market matching, not in requirements.txt)
sudo -u "$SERVICE_USER" "$VENV_DIR/bin/pip" install -q \
    'sentence-transformers>=2.2.0' \
    'python-Levenshtein>=0.25.0' \
    'tqdm>=4.65.0'

PKG_COUNT=$("$VENV_DIR/bin/pip" list --format=columns 2>/dev/null | wc -l)
echo "  Installed $PKG_COUNT packages"
echo "  Python: $("$VENV_DIR/bin/python3" --version)"

# --------------------------------------------------------------------------
# 6. Secrets file
# --------------------------------------------------------------------------
echo ""
echo "[6/9] Setting up secrets..."

ENV_FILE="/opt/bellwether/.env"
if [[ ! -f "$ENV_FILE" ]]; then
    (
        umask 077
        cat > "$ENV_FILE" << 'ENVEOF'
# Bellwether Pipeline Secrets
# This file is chmod 600 (owner-only). Do NOT relax permissions.

# Required
OPENAI_API_KEY=sk-REPLACE_ME

# Required for market discovery
DOME_API_KEY=REPLACE_ME

# Required for election calendar
GOOGLE_CIVIC_API_KEY=REPLACE_ME

# Optional
PREDICTIONHUNT_API_KEY=
CLOUDFLARE_API_TOKEN=
ENVEOF
    )
    chown "$SERVICE_USER:$SERVICE_USER" "$ENV_FILE"
    echo "  Created $ENV_FILE"
    echo "  >>> EDIT THIS FILE WITH YOUR API KEYS AFTER SETUP <<<"
else
    echo "  $ENV_FILE already exists"
fi

chmod 600 "$ENV_FILE"

# --------------------------------------------------------------------------
# 7. Create the runner script
# --------------------------------------------------------------------------
echo ""
echo "[7/9] Creating pipeline runner..."

RUNNER="$INSTALL_DIR/packages/pipelines/hetzner/run_pipeline.sh"
cat > "$RUNNER" << 'RUNEOF'
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
git pull --ff-only origin v2/data-full 2>/dev/null || log "WARNING: git pull failed (continuing with current code)"

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

    if git push origin v2/data-full 2>&1; then
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
RUNEOF

chmod +x "$RUNNER"
chown "$SERVICE_USER:$SERVICE_USER" "$RUNNER"
echo "  Created $RUNNER"

# --------------------------------------------------------------------------
# 8. Install crontab
# --------------------------------------------------------------------------
echo ""
echo "[8/9] Setting up cron schedule..."

CRON_LOG="$LOG_DIR/cron.log"

# Write crontab for the service user
CRONTAB_CONTENT=$(cat << CRONEOF
# Bellwether Pipeline - Hetzner VPS
# Runs daily at 06:00 UTC (1am EST / 10pm PST)
# Output goes to cron.log, pipeline also writes its own logs to logs/pipeline_runs/

SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

0 6 * * * /opt/bellwether/packages/pipelines/hetzner/run_pipeline.sh >> $CRON_LOG 2>&1
CRONEOF
)

echo "$CRONTAB_CONTENT" | crontab -u "$SERVICE_USER" -
echo "  Installed crontab for $SERVICE_USER:"
echo "    - Daily at 06:00 UTC (incremental)"

# --------------------------------------------------------------------------
# 9. Firewall + unattended upgrades
# --------------------------------------------------------------------------
echo ""
echo "[9/9] Hardening server..."

# UFW firewall - SSH only
ufw --force reset > /dev/null 2>&1
ufw default deny incoming > /dev/null 2>&1
ufw default allow outgoing > /dev/null 2>&1
ufw allow ssh > /dev/null 2>&1
ufw --force enable > /dev/null 2>&1
echo "  Firewall: SSH only (all other inbound blocked)"

# Unattended security upgrades
cat > /etc/apt/apt.conf.d/20auto-upgrades << 'UEOF'
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
APT::Periodic::AutocleanInterval "7";
UEOF
echo "  Unattended security upgrades: enabled"

# --------------------------------------------------------------------------
# Git config for the service user (needed for auto-commits)
# --------------------------------------------------------------------------
sudo -u "$SERVICE_USER" git -C "$INSTALL_DIR" config user.name "Bellwether Bot"
sudo -u "$SERVICE_USER" git -C "$INSTALL_DIR" config user.email "bellwether-bot@noreply.github.com"

echo "  Git config: set for auto-commits"

# --------------------------------------------------------------------------
# Done
# --------------------------------------------------------------------------
echo ""
echo "============================================"
echo "  Setup complete!"
echo "============================================"
echo ""
echo "NEXT STEPS:"
echo ""
echo "  1. Add your API keys:"
echo "     nano $ENV_FILE"
echo ""
echo "  2. Transfer data from Sherlock:"
echo "     # From your LOCAL machine:"
echo "     bash packages/pipelines/hetzner/transfer_data.sh <sherlock-user> <vps-ip>"
echo ""
echo "  3. Set up GitHub deploy key (for git push):"
echo "     sudo -u $SERVICE_USER ssh-keygen -t ed25519 -C 'bellwether-hetzner' -f /home/$SERVICE_USER/.ssh/id_ed25519 -N ''"
echo "     cat /home/$SERVICE_USER/.ssh/id_ed25519.pub"
echo "     # Add this as a deploy key at: https://github.com/vcbee/bellwether-platform/settings/keys"
echo "     # Check 'Allow write access'"
echo ""
echo "  4. Switch remote to SSH (needed for deploy key auth):"
echo "     sudo -u $SERVICE_USER git -C $INSTALL_DIR remote set-url origin git@github.com:vcbee/bellwether-platform.git"
echo ""
echo "  5. Test the pipeline:"
echo "     sudo -u $SERVICE_USER $INSTALL_DIR/packages/pipelines/hetzner/run_pipeline.sh"
echo ""
echo "  6. Verify cron is set:"
echo "     crontab -u $SERVICE_USER -l"
echo ""
echo "  7. (Optional) Set up email alerts:"
echo "     See: $INSTALL_DIR/packages/pipelines/hetzner/README.md"
echo ""
