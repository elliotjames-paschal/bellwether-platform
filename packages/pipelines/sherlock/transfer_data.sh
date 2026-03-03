#!/bin/bash
# ============================================================================
# Bellwether Pipeline - Transfer data to Sherlock
# ============================================================================
# Run this FROM YOUR LOCAL MACHINE to push data to Sherlock.
#
# Usage:
#   bash packages/pipelines/sherlock/transfer_data.sh <sunetid>
#
# Example:
#   bash packages/pipelines/sherlock/transfer_data.sh paschal
# ============================================================================

set -euo pipefail

SUNETID="${1:?Usage: $0 <sunetid>}"
SHERLOCK="$SUNETID@login.sherlock.stanford.edu"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BELLWETHER_HOME="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DATA_DIR="$BELLWETHER_HOME/data"

if [[ ! -d "$DATA_DIR" ]]; then
    echo "ERROR: $DATA_DIR not found"
    exit 1
fi

echo "============================================"
echo "  Transferring data to Sherlock"
echo "============================================"
echo "  From: $DATA_DIR"
echo "  To:   $SHERLOCK"
echo ""

# Check where data should go on Sherlock
echo "Checking Sherlock data location..."
DATA_DEST=$(ssh "$SHERLOCK" 'if [[ -L $HOME/bellwether-platform/data ]]; then readlink $HOME/bellwether-platform/data; else echo $HOME/bellwether-platform/data; fi')
echo "  Remote data dir: $DATA_DEST"
echo ""

# Transfer with rsync (compressed, progress, skip unchanged)
echo "Starting rsync..."
rsync -avz --progress \
    --exclude='backups/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='.DS_Store' \
    "$DATA_DIR/" \
    "$SHERLOCK:$DATA_DEST/"

echo ""
echo "============================================"
echo "  Transfer complete!"
echo "============================================"
echo ""
echo "Data size on Sherlock:"
ssh "$SHERLOCK" "du -sh $DATA_DEST"
