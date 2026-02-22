#!/bin/bash
#
# Run commands in an isolated Docker sandbox
# Usage:
#   ./sandbox-test.sh command "your command here"   - Run a single command
#   ./sandbox-test.sh script /path/to/script.py     - Copy and run a script
#   ./sandbox-test.sh shell                         - Start interactive shell
#
set -e

MODE="${1:-help}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE_NAME="claude-sandbox"

# Build sandbox image if needed
build_image() {
    if ! docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
        echo "Building sandbox environment..."
        docker build -q -t "$IMAGE_NAME" -f "$PROJECT_DIR/.claude/Dockerfile.sandbox" "$PROJECT_DIR" > /dev/null
    fi
}

# Run command in sandbox with project files copied in
run_in_sandbox() {
    # Create a tarball of essential files (exclude large data dirs)
    # This avoids Docker volume mount issues with special characters in paths
    (
        cd "$PROJECT_DIR"
        # Include scripts, website, .claude, and specific data files
        tar -cf - \
            --exclude='node_modules' \
            --exclude='.git' \
            --exclude='__pycache__' \
            --exclude='*.pyc' \
            --exclude='data/*.csv' \
            --exclude='data/*.parquet' \
            --exclude='data/domeapi_prices' \
            --exclude='data/kalshi_prices' \
            --exclude='data/kalshi_all_political_prices*' \
            --exclude='data/kalshi_all_political_with*' \
            --exclude='data/kalshi_political_event*' \
            --exclude='data/kalshi_political_markets*' \
            --exclude='data/cities.json' \
            scripts website .claude data 2>/dev/null
    ) | docker run --rm -i "$IMAGE_NAME" \
        bash -c "
            tar -xf - -C /workspace 2>/dev/null
            cd /workspace
            $1
        "
}

case "$MODE" in
    command|cmd|c)
        build_image
        shift
        run_in_sandbox "$*"
        ;;
    script|s)
        build_image
        shift
        SCRIPT="$1"
        shift
        run_in_sandbox "python3 $SCRIPT $*"
        ;;
    shell|sh)
        build_image
        echo "Starting sandbox shell. Type 'exit' to quit."
        echo "Project files are in /workspace (changes won't affect originals)"
        echo "Copying files..."
        # Create tarball and pipe to container, then start interactive shell
        tar -C "$PROJECT_DIR" -cf - . 2>/dev/null | docker run --rm -i "$IMAGE_NAME" \
            bash -c "tar -xf - -C /workspace 2>/dev/null && cd /workspace && bash"
        ;;
    *)
        echo "Sandbox Test Runner"
        echo ""
        echo "Usage:"
        echo "  $0 command \"your bash command\"  - Run command in sandbox"
        echo "  $0 script path/to/script.py     - Run Python script in sandbox"
        echo "  $0 shell                        - Interactive sandbox shell"
        echo ""
        echo "Project files are copied to /workspace inside the container."
        echo "Any changes stay inside the container - originals are never modified."
        ;;
esac
