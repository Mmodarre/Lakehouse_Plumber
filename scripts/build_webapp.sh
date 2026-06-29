#!/usr/bin/env bash
# LHP Web IDE — build the React SPA and stage it into the wheel.
#
# Builds the Vite SPA at repo-root web_app/ and syncs the produced
# web_app/dist/ into src/lhp/webapp/static/ (gitignored) so that a
# subsequent `python -m build` ships the assets via the
# `"lhp.webapp" = ["static/**/*", ...]` package-data glob in
# pyproject.toml. Users never need Node — CI / release run this script
# before building the wheel.
#
# Usage:  ./scripts/build_webapp.sh
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WEB_APP_DIR="$PROJECT_ROOT/web_app"
DIST_DIR="$WEB_APP_DIR/dist"
STATIC_DIR="$PROJECT_ROOT/src/lhp/webapp/static"

echo "[build_webapp] repo root: $PROJECT_ROOT"
echo "[build_webapp] web app:   $WEB_APP_DIR"

# Build the SPA
cd "$WEB_APP_DIR"

echo "[build_webapp] installing frontend dependencies (npm ci)..."
npm ci

echo "[build_webapp] building SPA (npm run build)..."
npm run build

if [ ! -d "$DIST_DIR" ]; then
  echo "[build_webapp] ERROR: expected build output not found at $DIST_DIR" >&2
  exit 1
fi

# Sync into the wheel's package-data static dir
echo "[build_webapp] syncing $DIST_DIR -> $STATIC_DIR ..."
rm -rf "$STATIC_DIR"
mkdir -p "$STATIC_DIR"
cp -R "$DIST_DIR"/. "$STATIC_DIR"/

echo "[build_webapp] done. Staged SPA assets are ready for 'python -m build'."
