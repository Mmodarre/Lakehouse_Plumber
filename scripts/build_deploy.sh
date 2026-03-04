#!/usr/bin/env bash
# Build and package LHP for Databricks Apps deployment.
#
# Usage:
#   ./scripts/build_deploy.sh              # full build
#   ./scripts/build_deploy.sh --skip-frontend  # skip React build
#   ./scripts/build_deploy.sh --skip-wheel     # skip Python wheel
#
# Outputs (all under deploy/):
#   static/           — pre-built React SPA
#   *.whl             — Python wheel
#   requirements.txt  — pip install spec pointing to the wheel

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEPLOY_DIR="$REPO_ROOT/deploy"
WEB_APP_DIR="$REPO_ROOT/web_app"

SKIP_FRONTEND=false
SKIP_WHEEL=false

for arg in "$@"; do
    case "$arg" in
        --skip-frontend) SKIP_FRONTEND=true ;;
        --skip-wheel)    SKIP_WHEEL=true ;;
        *)               echo "Unknown flag: $arg"; exit 1 ;;
    esac
done

echo "=== LHP Deploy Build ==="
echo "Repo root:  $REPO_ROOT"
echo "Deploy dir: $DEPLOY_DIR"

# ── 1. Build React frontend ──────────────────────────────────
if [ "$SKIP_FRONTEND" = false ]; then
    echo ""
    echo "--- Building React frontend ---"
    cd "$WEB_APP_DIR"
    npm ci
    npm run build
    echo "Frontend built: $WEB_APP_DIR/dist/"
else
    echo ""
    echo "--- Skipping frontend build ---"
fi

# ── 2. Build Python wheel ────────────────────────────────────
if [ "$SKIP_WHEEL" = false ]; then
    echo ""
    echo "--- Building Python wheel ---"
    cd "$REPO_ROOT"
    # Clean old wheels from deploy/
    rm -f "$DEPLOY_DIR"/*.whl
    python -m build --wheel --outdir "$DEPLOY_DIR"
    echo "Wheel built in $DEPLOY_DIR/"
else
    echo ""
    echo "--- Skipping wheel build ---"
fi

# ── 3. Copy static files ─────────────────────────────────────
echo ""
echo "--- Copying static files ---"
rm -rf "$DEPLOY_DIR/static"
if [ -d "$WEB_APP_DIR/dist" ]; then
    cp -r "$WEB_APP_DIR/dist" "$DEPLOY_DIR/static"
    echo "Copied web_app/dist/ -> deploy/static/"
else
    echo "WARNING: web_app/dist/ not found — run without --skip-frontend"
fi

# ── 4. Generate requirements.txt ──────────────────────────────
echo ""
echo "--- Generating requirements.txt ---"
WHEEL_FILE=$(ls "$DEPLOY_DIR"/*.whl 2>/dev/null | head -1)
if [ -n "$WHEEL_FILE" ]; then
    WHEEL_NAME=$(basename "$WHEEL_FILE")
    cat > "$DEPLOY_DIR/requirements.txt" <<EOF
./${WHEEL_NAME}[api,deploy]
EOF
    echo "Generated deploy/requirements.txt -> ./${WHEEL_NAME}[api,deploy]"
else
    echo "WARNING: No wheel found in $DEPLOY_DIR — run without --skip-wheel"
fi

echo ""
echo "=== Build complete ==="
echo "Next steps:"
echo "  cd deploy && databricks bundle validate -t dev"
echo "  cd deploy && databricks bundle deploy -t dev"
