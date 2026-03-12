#!/bin/bash
# ============================================================================
# Bellwether Feedback Pipeline - Farmshare Batch Job
# ============================================================================
# Runs the full feedback pipeline test:
#   1. Generate tickers (GPT-4o) — the slow/expensive step
#   2. Post-process tickers
#   3. Ingest feedback from Google Sheet
#   4. Apply human labels
#   5. Evaluate match accuracy
#   6. Generate correction rules
#
# All output is saved to ~/bellwether-platform/feedback_test_results/
#
# Usage:
#   sbatch packages/pipelines/farmshare/run_feedback_test.sh
#
# Monitor:
#   squeue -u $USER
#   tail -f ~/bellwether_feedback_JOBID.out
# ============================================================================

#SBATCH --job-name=bwr-feedback
#SBATCH --partition=normal
#SBATCH --qos=normal
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=12:00:00
#SBATCH --output=/home/users/%u/bellwether_feedback_%j.out
#SBATCH --error=/home/users/%u/bellwether_feedback_%j.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=vaniac@stanford.edu

set -euo pipefail

# ── Paths ──────────────────────────────────────────────────────────────────
BELLWETHER_HOME="$HOME/bellwether-platform"
PIPELINES_DIR="$BELLWETHER_HOME/packages/pipelines"
DATA_DIR="$BELLWETHER_HOME/data"
RESULTS_DIR="$BELLWETHER_HOME/feedback_test_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RUN_DIR="$RESULTS_DIR/$TIMESTAMP"

mkdir -p "$RUN_DIR"

# ── Environment ────────────────────────────────────────────────────────────
echo "======================================================================"
echo "  Bellwether Feedback Pipeline Test"
echo "  $(date -Iseconds)"
echo "  Host: $(hostname)"
echo "  SLURM Job ID: ${SLURM_JOB_ID:-manual}"
echo "  Results dir: $RUN_DIR"
echo "======================================================================"

# Load Python venv
if [[ -f "$HOME/envs/bellwether/bin/activate" ]]; then
    source "$HOME/envs/bellwether/bin/activate"
else
    echo "ERROR: venv not found at ~/envs/bellwether"
    exit 1
fi

echo "Python: $(python3 --version 2>&1)"

# Load API keys
ENV_FILE="$BELLWETHER_HOME/.env"
if [[ -f "$ENV_FILE" ]]; then
    sed -i 's/\r$//' "$ENV_FILE"
    set -a && source "$ENV_FILE" && set +a
else
    echo "ERROR: .env not found at $ENV_FILE"
    exit 1
fi

# Verify OpenAI key
if [[ -z "${OPENAI_API_KEY:-}" ]]; then
    # Fallback to txt file
    if [[ -f "$BELLWETHER_HOME/openai_api_key.txt" ]]; then
        export OPENAI_API_KEY=$(cat "$BELLWETHER_HOME/openai_api_key.txt" | tr -d '[:space:]')
    else
        echo "ERROR: No OpenAI API key found"
        exit 1
    fi
fi

echo "API key: loaded (${#OPENAI_API_KEY} chars)"
echo ""

cd "$PIPELINES_DIR"

# ── Helper function ────────────────────────────────────────────────────────
run_step() {
    local step_name="$1"
    local log_file="$RUN_DIR/${step_name}.log"
    shift
    echo "----------------------------------------------------------------------"
    echo "[$(date +%H:%M:%S)] STARTING: $step_name"
    echo "  Command: $@"
    echo "  Log: $log_file"

    if "$@" > "$log_file" 2>&1; then
        echo "[$(date +%H:%M:%S)] DONE: $step_name"
    else
        local exit_code=$?
        echo "[$(date +%H:%M:%S)] FAILED: $step_name (exit code $exit_code)"
        echo "  Last 10 lines:"
        tail -10 "$log_file" | sed 's/^/    /'
    fi
    echo ""
}

# ── Step 1: Generate tickers (GPT-4o) ─────────────────────────────────────
echo "======================================================================"
echo "  PHASE 1: TICKER GENERATION"
echo "======================================================================"

run_step "01_create_tickers" python3 -u create_tickers.py
run_step "02_postprocess_tickers" python3 -u postprocess_tickers.py

# Verify tickers
TICKER_COUNT=$(python3 -c "import json; d=json.load(open('$DATA_DIR/tickers_postprocessed.json')); print(len(d.get('tickers',[])))" 2>/dev/null || echo "0")
echo "Tickers generated: $TICKER_COUNT"
if [[ "$TICKER_COUNT" == "0" ]]; then
    echo "WARNING: No tickers generated. Feedback steps 2-4 will show empty results."
fi

# Save a copy of tickers to results
cp "$DATA_DIR/tickers_postprocessed.json" "$RUN_DIR/" 2>/dev/null || true

echo ""
echo "======================================================================"
echo "  PHASE 2: FEEDBACK PIPELINE"
echo "======================================================================"

# ── Step 2: Ingest feedback ───────────────────────────────────────────────
run_step "03_ingest_dryrun" python3 -u pipeline_ingest_feedback.py --dry-run
run_step "04_ingest" python3 -u pipeline_ingest_feedback.py

# ── Step 3: Apply labels ─────────────────────────────────────────────────
run_step "05_apply_dryrun" python3 -u pipeline_apply_human_labels.py --dry-run
run_step "06_apply" python3 -u pipeline_apply_human_labels.py

# ── Step 4: Evaluate ─────────────────────────────────────────────────────
run_step "07_evaluate" python3 -u pipeline_evaluate_matches.py --verbose

# ── Step 5: Generate corrections ─────────────────────────────────────────
run_step "08_corrections_dryrun" python3 -u generate_ticker_corrections.py --dry-run --min-frequency 1

# ── Copy all result files ────────────────────────────────────────────────
echo "======================================================================"
echo "  SAVING RESULTS"
echo "======================================================================"

for f in human_labels.json match_accuracy_report.json ticker_corrections.json ticker_disambiguations.json near_matches.json match_exclusions.json cross_platform_reviewed_pairs.json; do
    if [[ -f "$DATA_DIR/$f" ]]; then
        cp "$DATA_DIR/$f" "$RUN_DIR/"
        echo "  Saved: $f"
    fi
done

# ── Print summary ────────────────────────────────────────────────────────
echo ""
echo "======================================================================"
echo "  SUMMARY"
echo "======================================================================"
echo ""

echo "--- Tickers ---"
python3 -c "
import json
d = json.load(open('$DATA_DIR/tickers_postprocessed.json'))
print(f'  Total tickers: {len(d.get(\"tickers\",[]))}')
" 2>/dev/null || echo "  Failed to read tickers"

echo ""
echo "--- Human Labels ---"
python3 -c "
import json
d = json.load(open('$DATA_DIR/human_labels.json'))
labels = d.get('labels', [])
by_status = {}
for l in labels:
    s = l.get('status', 'unknown')
    by_status[s] = by_status.get(s, 0) + 1
print(f'  Total labels: {len(labels)}')
for s, c in sorted(by_status.items()):
    print(f'    {s}: {c}')
" 2>/dev/null || echo "  Failed to read labels"

echo ""
echo "--- Match Accuracy ---"
python3 -c "
import json
d = json.load(open('$DATA_DIR/match_accuracy_report.json'))
m = d['matching']
print(f'  TP={m[\"true_positives\"]} FN={m[\"false_negatives\"]} FP={m[\"false_positives\"]} TN={m[\"true_negatives\"]}')
print(f'  Precision={m[\"precision\"]:.1%} Recall={m[\"recall\"]:.1%} F1={m[\"f1\"]:.1%}')
print(f'  Skipped: {d.get(\"matching_skipped\", 0)}')
print(f'  Disagreements: {len(d.get(\"disagreements\", []))}')
" 2>/dev/null || echo "  Failed to read report"

echo ""
echo "--- Corrections ---"
python3 -c "
import json
d = json.load(open('$DATA_DIR/ticker_corrections.json'))
print(f'  Correction rules: {len(d.get(\"corrections\",[]))}')
" 2>/dev/null || echo "  No corrections file"

python3 -c "
import json
d = json.load(open('$DATA_DIR/ticker_disambiguations.json'))
print(f'  Disambiguation rules: {len(d.get(\"disambiguations\",[]))}')
" 2>/dev/null || echo "  No disambiguations file"

echo ""
echo "======================================================================"
echo "  All results saved to: $RUN_DIR"
echo "  Job finished: $(date -Iseconds)"
echo "======================================================================"
