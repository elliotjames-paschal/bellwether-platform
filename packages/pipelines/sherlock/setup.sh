#!/bin/bash
# ============================================================================
# Bellwether Pipeline - Sherlock Setup
# ============================================================================
# Run this ONCE on Sherlock to set up the environment.
#
# Usage:
#   ssh <sunetid>@login.sherlock.stanford.edu
#   bash bellwether-platform/packages/pipelines/sherlock/setup.sh
# ============================================================================

set -euo pipefail

echo "============================================"
echo "  Bellwether Pipeline - Sherlock Setup"
echo "============================================"

# --------------------------------------------------------------------------
# 1. Verify we're on Sherlock
# --------------------------------------------------------------------------
if [[ -z "${SHERLOCK:-}" && ! -d /share/software ]]; then
    echo "WARNING: This doesn't look like Sherlock. Continuing anyway..."
fi

# --------------------------------------------------------------------------
# 2. Create directory structure
# --------------------------------------------------------------------------
echo ""
echo "[1/6] Creating directories..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BELLWETHER_HOME="$(cd "$SCRIPT_DIR/../../.." && pwd)"
LOG_DIR="$HOME/logs"
mkdir -p "$LOG_DIR"

# Use $GROUP_HOME for large files (venv, data) if available
if [[ -n "${GROUP_HOME:-}" ]]; then
    VENV_DIR="$GROUP_HOME/envs/bellwether"
    DATA_STORE="$GROUP_HOME/bellwether-data"
    echo "  Using GROUP_HOME for venv: $VENV_DIR"
    echo "  Using GROUP_HOME for data: $DATA_STORE"
else
    VENV_DIR="$HOME/envs/bellwether"
    DATA_STORE="$BELLWETHER_HOME/data"
    echo "  WARNING: \$GROUP_HOME not set. Using \$HOME (15 GB limit!)"
    echo "  Venv: $VENV_DIR"
fi

# --------------------------------------------------------------------------
# 3. Load Python module and create venv
# --------------------------------------------------------------------------
echo ""
echo "[2/6] Setting up Python environment..."

# Find best available Python
PYTHON_MODULE=""
for mod in python/3.12 python/3.11 python/3.10 python/3.9; do
    if module avail "$mod" 2>&1 | grep -q "$mod"; then
        PYTHON_MODULE="$mod"
        break
    fi
done

if [[ -z "$PYTHON_MODULE" ]]; then
    echo "  No Python 3.9+ module found. Trying system python3..."
    PYTHON_CMD="python3"
else
    echo "  Loading module: $PYTHON_MODULE"
    module load "$PYTHON_MODULE"
    PYTHON_CMD="python3"
fi

$PYTHON_CMD --version

if [[ ! -d "$VENV_DIR" ]]; then
    echo "  Creating virtual environment at $VENV_DIR..."
    $PYTHON_CMD -m venv "$VENV_DIR"
else
    echo "  Virtual environment already exists at $VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

# --------------------------------------------------------------------------
# 4. Install dependencies
# --------------------------------------------------------------------------
echo ""
echo "[3/6] Installing Python dependencies..."

pip install --upgrade pip wheel setuptools -q

# Core pipeline dependencies
# Use --only-binary for scipy/numpy to avoid compiling from source (needs OpenBLAS)
pip install -q --only-binary=scipy,numpy \
    'pandas>=2.0.0' \
    'numpy>=1.24.0,<2.5' \
    'scipy>=1.10.0,<1.15' \
    'requests>=2.28.0' \
    'openai>=1.0.0' \
    'matplotlib>=3.7.0' \
    'seaborn>=0.12.0' \
    'jinja2>=3.1.0' \
    'spacy>=3.7.0' \
    'thefuzz>=0.20.0' \
    'unidecode>=1.3.0' \
    'sentence-transformers>=2.2.0' \
    'python-dotenv>=1.0.0' \
    'tqdm>=4.65.0' \
    'statsmodels>=0.14.0' \
    'aiohttp>=3.9.0' \
    'reportlab>=4.0.0' \
    'python-Levenshtein>=0.25.0'

echo "  Installed $(pip list --format=columns | wc -l) packages"

# --------------------------------------------------------------------------
# 5. Set up data directory symlink
# --------------------------------------------------------------------------
echo ""
echo "[4/6] Setting up data directory..."

if [[ -n "${GROUP_HOME:-}" ]]; then
    mkdir -p "$DATA_STORE"

    # If data/ exists in repo and isn't a symlink, move it
    if [[ -d "$BELLWETHER_HOME/data" && ! -L "$BELLWETHER_HOME/data" ]]; then
        echo "  Moving existing data to $DATA_STORE..."
        cp -r "$BELLWETHER_HOME/data/"* "$DATA_STORE/" 2>/dev/null || true
        rm -rf "$BELLWETHER_HOME/data"
    fi

    # Create symlink
    if [[ ! -L "$BELLWETHER_HOME/data" ]]; then
        ln -s "$DATA_STORE" "$BELLWETHER_HOME/data"
        echo "  Symlinked data/ -> $DATA_STORE"
    else
        echo "  Symlink already exists"
    fi
else
    mkdir -p "$BELLWETHER_HOME/data"
    echo "  Using $BELLWETHER_HOME/data directly"
fi

# --------------------------------------------------------------------------
# 6. Create secrets file + verify permissions
# --------------------------------------------------------------------------
echo ""
echo "[5/6] Setting up secrets..."

ENV_FILE="$HOME/.bellwether_env"
if [[ ! -f "$ENV_FILE" ]]; then
    # Create with restrictive umask so file is NEVER world/group readable
    (
        umask 077
        cat > "$ENV_FILE" << 'ENVEOF'
# Bellwether Pipeline Secrets
# This file is chmod 600 (owner-only). Do NOT relax permissions.
# Do NOT commit this file to git.
export OPENAI_API_KEY="sk-REPLACE_ME"
ENVEOF
    )
    echo "  Created $ENV_FILE"
    echo "  >>> nano ~/.bellwether_env  (add your OpenAI key) <<<"
else
    echo "  $ENV_FILE already exists"
fi

# Always enforce correct permissions
chmod 600 "$ENV_FILE"
echo "  Permissions: $(ls -l "$ENV_FILE" | awk '{print $1}')"

# Verify $HOME is not group/world readable
HOME_PERMS=$(stat -c '%a' "$HOME" 2>/dev/null || stat -f '%Lp' "$HOME")
if [[ "${HOME_PERMS: -1}" != "0" || "${HOME_PERMS: -2:1}" != "0" ]]; then
    echo "  WARNING: \$HOME has loose permissions ($HOME_PERMS). Tightening..."
    chmod 700 "$HOME"
fi

# Make sure secrets are in .gitignore
GITIGNORE="$BELLWETHER_HOME/.gitignore"
if [[ -f "$GITIGNORE" ]]; then
    if ! grep -q "bellwether_env" "$GITIGNORE"; then
        echo ".bellwether_env" >> "$GITIGNORE"
    fi
    if ! grep -q "openai_api_key" "$GITIGNORE"; then
        echo "openai_api_key.txt" >> "$GITIGNORE"
    fi
fi

# --------------------------------------------------------------------------
# 7. Write config file for the wrapper script
# --------------------------------------------------------------------------
echo ""
echo "[6/6] Writing Sherlock config..."

CONFIG_FILE="$BELLWETHER_HOME/packages/pipelines/sherlock/sherlock.conf"
cat > "$CONFIG_FILE" << CONFEOF
# Auto-generated by setup.sh on $(date -Iseconds)
PYTHON_MODULE="$PYTHON_MODULE"
VENV_DIR="$VENV_DIR"
LOG_DIR="$LOG_DIR"
ENV_FILE="$ENV_FILE"
CONFEOF

echo "  Saved: $CONFIG_FILE"

# --------------------------------------------------------------------------
# Done
# --------------------------------------------------------------------------
echo ""
echo "============================================"
echo "  Setup complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Edit your API key:  nano ~/.bellwether_env"
echo "  2. Transfer data:      rsync -avz data/ sherlock:$DATA_STORE/"
echo "  3. Test the pipeline:  bash packages/pipelines/sherlock/run_pipeline.sh"
echo "  4. Set up daily cron:  scrontab -e  (paste from scrontab.txt)"
echo ""
