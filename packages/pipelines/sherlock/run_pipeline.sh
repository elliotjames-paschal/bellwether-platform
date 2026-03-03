#!/bin/bash
# ============================================================================
# Bellwether Pipeline - Sherlock Runner
# ============================================================================
# Wrapper script that loads the environment and runs the daily pipeline.
# After completion, resubmits itself to run again in 6 hours.
#
# Security: API keys are sourced at runtime from a chmod-600 file in $HOME,
# NOT passed via SLURM --export (which leaks to job metadata). Keys are
# unset after the pipeline finishes.
#
# Usage:
#   First run (manual):   sbatch packages/pipelines/sherlock/run_pipeline.sh
#   It will then self-resubmit every 6 hours after completion.
#
#   Stop the cycle:       scancel <jobid>  (cancel the pending resubmission)
# ============================================================================

#SBATCH --job-name=bellwether
#SBATCH --partition=normal
#SBATCH --time=03:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=2
#SBATCH --output=/home/users/%u/logs/bellwether_%j.out
#SBATCH --error=/home/users/%u/logs/bellwether_%j.err
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=paschal@stanford.edu
#SBATCH --export=NONE

set -euo pipefail

# SLURM copies scripts to /var/spool, so we can't use BASH_SOURCE for paths.
# Use GROUP_HOME or fall back to HOME to find the repo.
if [[ -d "${GROUP_HOME:-}/bellwether-platform" ]]; then
    BELLWETHER_HOME="$GROUP_HOME/bellwether-platform"
elif [[ -d "$HOME/bellwether-platform" ]]; then
    BELLWETHER_HOME="$HOME/bellwether-platform"
else
    echo "ERROR: Cannot find bellwether-platform in \$GROUP_HOME or \$HOME"
    exit 1
fi

SCRIPT_DIR="$BELLWETHER_HOME/packages/pipelines/sherlock"

# --------------------------------------------------------------------------
# Load Sherlock config
# --------------------------------------------------------------------------
CONF="$SCRIPT_DIR/sherlock.conf"
if [[ ! -f "$CONF" ]]; then
    echo "ERROR: $CONF not found. Run setup.sh first."
    exit 1
fi
source "$CONF"

# --------------------------------------------------------------------------
# Load secrets (sourced at runtime, not via SLURM --export)
# --------------------------------------------------------------------------
if [[ -f "$ENV_FILE" ]]; then
    PERMS=$(stat -c '%a' "$ENV_FILE" 2>/dev/null || stat -f '%Lp' "$ENV_FILE")
    if [[ "$PERMS" != "600" ]]; then
        echo "WARNING: $ENV_FILE has permissions $PERMS (expected 600). Fixing..."
        chmod 600 "$ENV_FILE"
    fi
    source "$ENV_FILE"
else
    echo "ERROR: $ENV_FILE not found. Run setup.sh first."
    exit 1
fi

if [[ -z "${OPENAI_API_KEY:-}" || "$OPENAI_API_KEY" == "sk-REPLACE_ME" ]]; then
    echo "ERROR: OPENAI_API_KEY not set. Edit $ENV_FILE"
    exit 1
fi

# --------------------------------------------------------------------------
# Load Python
# --------------------------------------------------------------------------
if [[ -n "${PYTHON_MODULE:-}" ]]; then
    module load "$PYTHON_MODULE" 2>/dev/null || true
fi

source "$VENV_DIR/bin/activate"

# --------------------------------------------------------------------------
# Run pipeline
# --------------------------------------------------------------------------
echo "======================================================================"
echo "  Bellwether Pipeline - Sherlock"
echo "  $(date -Iseconds)"
echo "  Host: $(hostname)"
echo "  SLURM Job ID: ${SLURM_JOB_ID:-manual}"
echo "  Python: $(python3 --version 2>&1)"
echo "  Args: $*"
echo "  API key: loaded (${#OPENAI_API_KEY} chars)"
echo "======================================================================"

cd "$BELLWETHER_HOME"

# Pull latest code
git pull --ff-only origin v2/data-full 2>/dev/null || echo "WARNING: git pull failed (offline or no remote)"

# Run the pipeline with unbuffered output
python3 -u packages/pipelines/pipeline_daily_refresh.py "$@"
EXIT_CODE=$?

# --------------------------------------------------------------------------
# Cleanup: unset secrets from environment
# --------------------------------------------------------------------------
unset OPENAI_API_KEY

echo ""
echo "======================================================================"
echo "  Pipeline finished with exit code: $EXIT_CODE"
echo "  $(date -Iseconds)"
echo "======================================================================"

# --------------------------------------------------------------------------
# Self-resubmit: run again 6 hours from now
# --------------------------------------------------------------------------
RESUBMIT="${BELLWETHER_RESUBMIT:-true}"
if [[ "$RESUBMIT" == "true" ]]; then
    NEXT_JOB=$(sbatch --begin="now+6hours" "$SCRIPT_DIR/run_pipeline.sh" 2>&1)
    echo "  Resubmitted: $NEXT_JOB"
    echo "  Next run: $(date -d '+6 hours' -Iseconds 2>/dev/null || date -v+6H -Iseconds 2>/dev/null || echo '~6 hours from now')"
else
    echo "  BELLWETHER_RESUBMIT=false — not resubmitting"
fi

exit $EXIT_CODE
