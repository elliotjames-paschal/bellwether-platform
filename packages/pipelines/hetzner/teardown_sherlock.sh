#!/bin/bash
# ============================================================================
# Bellwether Pipeline - Sherlock Teardown
# ============================================================================
# Run this AFTER confirming the Hetzner VPS is working correctly.
# Stops the self-resubmitting job cycle and cleans up Sherlock resources.
#
# Usage:
#   ssh <sunetid>@login.sherlock.stanford.edu
#   bash bellwether-platform/packages/pipelines/hetzner/teardown_sherlock.sh
# ============================================================================

set -uo pipefail

echo "============================================"
echo "  Bellwether - Sherlock Teardown"
echo "============================================"
echo ""

# --------------------------------------------------------------------------
# 1. Check for running/pending jobs
# --------------------------------------------------------------------------
echo "[1/4] Checking for Bellwether SLURM jobs..."

JOBS=$(squeue --me --name=bellwether --format="%.10i %.12j %.8T %.10M %.9l" 2>/dev/null || echo "")

if [[ -n "$JOBS" ]]; then
    echo "$JOBS"
    echo ""
    read -rp "Cancel all bellwether jobs? [y/N] " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        scancel --name=bellwether
        echo "  Cancelled all bellwether jobs"
    else
        echo "  Skipped — jobs still running"
    fi
else
    echo "  No bellwether jobs found"
fi

# --------------------------------------------------------------------------
# 2. Remove scrontab entries (if any)
# --------------------------------------------------------------------------
echo ""
echo "[2/4] Checking scrontab..."

SCRONTAB=$(scrontab -l 2>/dev/null || echo "")
if echo "$SCRONTAB" | grep -qi "bellwether"; then
    echo "  Found bellwether entries in scrontab"
    echo "  Run 'scrontab -e' to remove them manually"
else
    echo "  No bellwether entries in scrontab"
fi

# --------------------------------------------------------------------------
# 3. Verify data was transferred
# --------------------------------------------------------------------------
echo ""
echo "[3/4] Current data on Sherlock..."

if [[ -n "${GROUP_HOME:-}" && -d "$GROUP_HOME/bellwether-data" ]]; then
    DATA_LOC="$GROUP_HOME/bellwether-data"
elif [[ -d "$HOME/bellwether-platform/data" ]]; then
    DATA_LOC="$HOME/bellwether-platform/data"
else
    DATA_LOC="(not found)"
fi

if [[ "$DATA_LOC" != "(not found)" ]]; then
    echo "  Location: $DATA_LOC"
    du -sh "$DATA_LOC" 2>/dev/null || true
    echo ""
    echo "  IMPORTANT: Verify this data exists on your Hetzner VPS before deleting."
    echo "  Keep this data for at least 1 week after migration as a safety net."
else
    echo "  Data directory not found (may already be cleaned up)"
fi

# --------------------------------------------------------------------------
# 4. Optional cleanup
# --------------------------------------------------------------------------
echo ""
echo "[4/4] Cleanup options..."
echo ""
echo "  Once you've confirmed the VPS is running correctly for 1+ week:"
echo ""
echo "  # Remove venv (saves ~2 GB)"
if [[ -n "${GROUP_HOME:-}" ]]; then
    echo "  rm -rf $GROUP_HOME/envs/bellwether"
else
    echo "  rm -rf ~/envs/bellwether"
fi
echo ""
echo "  # Remove data (saves ~2.6 GB) — ONLY after confirming VPS has it"
if [[ "$DATA_LOC" != "(not found)" ]]; then
    echo "  rm -rf $DATA_LOC"
fi
echo ""
echo "  # Remove secrets"
echo "  rm -f ~/.bellwether_env"
echo ""
echo "  # Remove logs"
echo "  rm -rf ~/logs/bellwether_*"
echo ""
echo "============================================"
echo "  Teardown checklist complete"
echo "  Keep Sherlock data for 1 week as safety net"
echo "============================================"
