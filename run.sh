#!/bin/bash
# =============================================================================
# Illumio Monitoring Tool - Run Script
# =============================================================================
# This script sets up the environment and runs the monitoring tool.
# 
# Usage:
#   ./run.sh                    # Run with default config
#   ./run.sh /path/to/config    # Run with custom config
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONFIG_FILE="${1:-config/config.yaml}"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

check_env_var() {
    if [ -z "${!1}" ]; then
        echo "WARNING: Environment variable $1 is not set"
        return 1
    fi
    return 0
}

echo "Checking required environment variables..."
check_env_var "ILLUMIO_API_USER" || true
check_env_var "ILLUMIO_API_SECRET" || true
check_env_var "SNOW_API_USER" || true
check_env_var "SNOW_API_KEY" || true

if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

echo "=============================================="
echo "Illumio Monitoring Tool"
echo "=============================================="
echo "Config: $CONFIG_FILE"
echo "Date: $(date '+%d-%m-%Y %H:%M:%S')"
echo "=============================================="

python src/main.py --config "$CONFIG_FILE"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "=============================================="
    echo "Execution completed successfully!"
    echo "=============================================="
else
    echo ""
    echo "=============================================="
    echo "Execution completed with errors (exit code: $EXIT_CODE)"
    echo "=============================================="
fi

exit $EXIT_CODE
