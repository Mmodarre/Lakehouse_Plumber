#!/usr/bin/env bash
set -euo pipefail

# -------------------------------------------------------------------
# LHP Docker entrypoint
# Validates the mounted project and logs startup configuration
# before handing off to the CMD (uvicorn).
# -------------------------------------------------------------------

PROJECT_ROOT="${LHP_PROJECT_ROOT:-/project}"

# 1. Validate that a valid LHP project is mounted
if [ ! -f "${PROJECT_ROOT}/lhp.yaml" ]; then
    echo "ERROR: No lhp.yaml found at ${PROJECT_ROOT}/lhp.yaml"
    echo ""
    echo "Make sure LHP_PROJECT_PATH in your .env points to a valid LHP project directory."
    echo "The directory must contain an lhp.yaml configuration file."
    echo ""
    echo "Example:"
    echo "  LHP_PROJECT_PATH=/path/to/your/lhp-project"
    exit 1
fi

# 2. Warn (don't exit) if AI is enabled but credentials look incomplete
if [ "${LHP_AI_ENABLED:-false}" = "true" ]; then
    if [ -z "${ANTHROPIC_BASE_URL:-}" ]; then
        echo "WARNING: LHP_AI_ENABLED=true but ANTHROPIC_BASE_URL is not set."
        echo "  AI chat will not work without a valid Anthropic-compatible endpoint."
        echo "  Set ANTHROPIC_BASE_URL in your .env file to enable AI features."
        echo ""
    fi
fi

# 3. Log startup configuration
echo "=== LHP Docker Container ==="
echo "  Project root : ${PROJECT_ROOT}"
echo "  Dev mode     : ${LHP_DEV_MODE:-true}"
echo "  AI enabled   : ${LHP_AI_ENABLED:-false}"
echo "  Log level    : ${LHP_LOG_LEVEL:-INFO}"
echo "  Static dir   : ${LHP_STATIC_DIR:-not set}"
echo "============================="
echo ""

# 4. Hand off to CMD (uvicorn) — exec replaces this shell for proper signal handling
exec "$@"
