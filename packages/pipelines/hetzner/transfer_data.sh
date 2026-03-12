#!/bin/bash
# ============================================================================
# Bellwether Pipeline - Transfer data from Sherlock to Hetzner VPS
# ============================================================================
# Run this FROM YOUR LOCAL MACHINE. It pulls data from Sherlock and pushes
# it to the Hetzner VPS in one step (via local staging).
#
# Usage:
#   bash packages/pipelines/hetzner/transfer_data.sh <sunetid> <vps-ip>
#
# Example:
#   bash packages/pipelines/hetzner/transfer_data.sh paschal 65.108.xx.xx
#
# If you already have data locally:
#   bash packages/pipelines/hetzner/transfer_data.sh --local <vps-ip>
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BELLWETHER_HOME="$(cd "$SCRIPT_DIR/../../.." && pwd)"
LOCAL_DATA="$BELLWETHER_HOME/data"
REMOTE_DATA="/opt/bellwether/data"
REMOTE_USER="bellwether"

# Common rsync excludes
RSYNC_EXCLUDES=(
    --exclude='backups/'
    --exclude='__pycache__/'
    --exclude='*.pyc'
    --exclude='.DS_Store'
)

echo "============================================"
echo "  Transfer data to Hetzner VPS"
echo "============================================"

# --------------------------------------------------------------------------
# Parse args
# --------------------------------------------------------------------------
if [[ "${1:-}" == "--local" ]]; then
    # Local data -> VPS
    VPS_IP="${2:?Usage: $0 --local <vps-ip>}"

    if [[ ! -d "$LOCAL_DATA" ]]; then
        echo "ERROR: Local data dir not found at $LOCAL_DATA"
        exit 1
    fi

    echo "  Mode:   local -> VPS"
    echo "  Source: $LOCAL_DATA"
    echo "  Dest:   $REMOTE_USER@$VPS_IP:$REMOTE_DATA"
    echo ""

    echo "Transferring data..."
    rsync -avz --progress \
        "${RSYNC_EXCLUDES[@]}" \
        "$LOCAL_DATA/" \
        "$REMOTE_USER@$VPS_IP:$REMOTE_DATA/"

elif [[ $# -ge 2 ]]; then
    # Sherlock -> VPS (two-hop via local)
    SUNETID="${1}"
    VPS_IP="${2}"
    SHERLOCK="$SUNETID@login.sherlock.stanford.edu"
    STAGING_DIR="/tmp/bellwether-data-transfer"

    echo "  Mode:   Sherlock -> local staging -> VPS"
    echo "  Sherlock: $SHERLOCK"
    echo "  VPS:      $REMOTE_USER@$VPS_IP"
    echo ""

    # Step 1: Find data location on Sherlock
    echo "[1/3] Finding data on Sherlock..."
    SHERLOCK_DATA=$(ssh "$SHERLOCK" 'if [[ -L $HOME/bellwether-platform/data ]]; then readlink $HOME/bellwether-platform/data; else echo $HOME/bellwether-platform/data; fi')
    echo "  Sherlock data: $SHERLOCK_DATA"

    # Step 2: Pull from Sherlock to local staging
    echo ""
    echo "[2/3] Pulling from Sherlock to local staging..."
    mkdir -p "$STAGING_DIR"
    rsync -avz --progress \
        "${RSYNC_EXCLUDES[@]}" \
        "$SHERLOCK:$SHERLOCK_DATA/" \
        "$STAGING_DIR/"

    echo ""
    echo "  Staging size: $(du -sh "$STAGING_DIR" | cut -f1)"

    # Step 3: Push from local staging to VPS
    echo ""
    echo "[3/3] Pushing to Hetzner VPS..."
    rsync -avz --progress \
        "${RSYNC_EXCLUDES[@]}" \
        "$STAGING_DIR/" \
        "$REMOTE_USER@$VPS_IP:$REMOTE_DATA/"

    # Cleanup staging
    rm -rf "$STAGING_DIR"
    echo "  Cleaned up staging dir"

else
    echo "Usage:"
    echo "  $0 <sunetid> <vps-ip>     # Sherlock -> VPS (via local staging)"
    echo "  $0 --local <vps-ip>       # Local data -> VPS"
    exit 1
fi

echo ""
echo "============================================"
echo "  Transfer complete!"
echo "============================================"
echo ""
echo "Data size on VPS:"
ssh "$REMOTE_USER@$VPS_IP" "du -sh $REMOTE_DATA"
echo ""
echo "Verify with:"
echo "  ssh $REMOTE_USER@$VPS_IP 'ls -la $REMOTE_DATA/'"
