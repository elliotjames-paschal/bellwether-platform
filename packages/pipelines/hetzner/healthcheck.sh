#!/bin/bash
# ============================================================================
# Bellwether Pipeline - Health Check
# ============================================================================
# Quick diagnostic to verify the VPS is correctly set up.
# Run after setup or whenever things seem off.
#
# Usage:
#   sudo -u bellwether bash /opt/bellwether/packages/pipelines/hetzner/healthcheck.sh
# ============================================================================

set -uo pipefail

INSTALL_DIR="/opt/bellwether"
VENV_DIR="/opt/bellwether/venv"
ENV_FILE="/opt/bellwether/.env"
DATA_DIR="/opt/bellwether/data"
LOG_DIR="/opt/bellwether/logs"

PASS=0
FAIL=0
WARN=0

check() {
    local label="$1"
    local result="$2"
    if [[ "$result" == "PASS" ]]; then
        echo "  [OK]   $label"
        ((PASS++))
    elif [[ "$result" == "WARN" ]]; then
        echo "  [WARN] $label"
        ((WARN++))
    else
        echo "  [FAIL] $label"
        ((FAIL++))
    fi
}

echo "============================================"
echo "  Bellwether Health Check"
echo "  $(date -Iseconds)"
echo "============================================"
echo ""

# --------------------------------------------------------------------------
# 1. Python
# --------------------------------------------------------------------------
echo "Python:"
if "$VENV_DIR/bin/python3" --version &>/dev/null; then
    VER=$("$VENV_DIR/bin/python3" --version 2>&1)
    check "Python installed ($VER)" "PASS"
else
    check "Python not found at $VENV_DIR/bin/python3" "FAIL"
fi

# Check key packages
for pkg in pandas openai requests scipy spacy; do
    if "$VENV_DIR/bin/python3" -c "import $pkg" 2>/dev/null; then
        check "Package: $pkg" "PASS"
    else
        check "Package: $pkg (not installed)" "FAIL"
    fi
done
echo ""

# --------------------------------------------------------------------------
# 2. Secrets
# --------------------------------------------------------------------------
echo "Secrets:"
if [[ -f "$ENV_FILE" ]]; then
    PERMS=$(stat -c '%a' "$ENV_FILE")
    if [[ "$PERMS" == "600" ]]; then
        check ".env exists with correct permissions (600)" "PASS"
    else
        check ".env permissions are $PERMS (should be 600)" "WARN"
    fi

    set -a; source "$ENV_FILE" 2>/dev/null; set +a
    for key in OPENAI_API_KEY DOME_API_KEY GOOGLE_CIVIC_API_KEY; do
        val="${!key:-}"
        if [[ -z "$val" || "$val" == *"REPLACE_ME"* ]]; then
            check "$key: not set" "FAIL"
        else
            check "$key: set (${#val} chars)" "PASS"
        fi
    done
else
    check ".env file missing" "FAIL"
fi
echo ""

# --------------------------------------------------------------------------
# 3. Data
# --------------------------------------------------------------------------
echo "Data:"
if [[ -d "$DATA_DIR" ]]; then
    DATA_SIZE=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)
    check "Data directory exists ($DATA_SIZE)" "PASS"

    # Check for key files
    for f in combined_political_markets_with_electoral_details_UPDATED.csv enriched_political_markets.json; do
        if [[ -f "$DATA_DIR/$f" ]]; then
            SIZE=$(du -sh "$DATA_DIR/$f" 2>/dev/null | cut -f1)
            check "$f ($SIZE)" "PASS"
        else
            check "$f (missing)" "FAIL"
        fi
    done
else
    check "Data directory missing" "FAIL"
fi
echo ""

# --------------------------------------------------------------------------
# 4. Git
# --------------------------------------------------------------------------
echo "Git:"
cd "$INSTALL_DIR"
BRANCH=$(git branch --show-current 2>/dev/null || echo "unknown")
check "Branch: $BRANCH" "PASS"

if git remote get-url origin 2>/dev/null | grep -q "git@github.com"; then
    check "Remote: SSH (deploy key auth)" "PASS"
else
    REMOTE=$(git remote get-url origin 2>/dev/null || echo "none")
    check "Remote: $REMOTE (switch to SSH for push)" "WARN"
fi

if ssh -T git@github.com 2>&1 | grep -qi "success\|authenticated"; then
    check "GitHub SSH auth: working" "PASS"
else
    check "GitHub SSH auth: not working (deploy key needed)" "WARN"
fi
echo ""

# --------------------------------------------------------------------------
# 5. Cron
# --------------------------------------------------------------------------
echo "Cron:"
CRON=$(crontab -l 2>/dev/null || echo "")
if echo "$CRON" | grep -q "run_pipeline"; then
    check "Cron job installed" "PASS"
    DAILY=$(echo "$CRON" | grep -v "^#" | grep "run_pipeline" | head -1)
    check "Schedule: $DAILY" "PASS"
else
    check "No cron job found" "FAIL"
fi
echo ""

# --------------------------------------------------------------------------
# 6. Recent runs
# --------------------------------------------------------------------------
echo "Recent pipeline runs:"
RUN_DIR="$LOG_DIR/pipeline_runs"
if [[ -d "$RUN_DIR" ]]; then
    LATEST=$(ls -t "$RUN_DIR"/*_daily.log 2>/dev/null | head -1)
    if [[ -n "$LATEST" ]]; then
        LATEST_TIME=$(stat -c '%y' "$LATEST" | cut -d. -f1)
        check "Last run: $LATEST_TIME" "PASS"

        # Check if last run had errors
        ERROR_LOG="${LATEST%.log}_errors.log"
        if [[ -f "$ERROR_LOG" && -s "$ERROR_LOG" ]]; then
            NERRORS=$(wc -l < "$ERROR_LOG")
            check "Last run had $NERRORS error lines (check $ERROR_LOG)" "WARN"
        else
            check "Last run: no errors" "PASS"
        fi
    else
        check "No pipeline runs found yet" "WARN"
    fi
else
    check "No log directory" "WARN"
fi
echo ""

# --------------------------------------------------------------------------
# 7. Disk space
# --------------------------------------------------------------------------
echo "Disk:"
DISK_USAGE=$(df -h /opt 2>/dev/null | tail -1 | awk '{print $5}')
DISK_AVAIL=$(df -h /opt 2>/dev/null | tail -1 | awk '{print $4}')
USAGE_NUM=${DISK_USAGE%\%}
if [[ "$USAGE_NUM" -lt 80 ]]; then
    check "Disk usage: $DISK_USAGE used, $DISK_AVAIL available" "PASS"
elif [[ "$USAGE_NUM" -lt 90 ]]; then
    check "Disk usage: $DISK_USAGE used, $DISK_AVAIL available" "WARN"
else
    check "Disk usage: $DISK_USAGE used, $DISK_AVAIL available (critical!)" "FAIL"
fi
echo ""

# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------
echo "============================================"
echo "  Results: $PASS passed, $WARN warnings, $FAIL failed"
echo "============================================"

if [[ $FAIL -gt 0 ]]; then
    exit 1
elif [[ $WARN -gt 0 ]]; then
    exit 0
else
    exit 0
fi
