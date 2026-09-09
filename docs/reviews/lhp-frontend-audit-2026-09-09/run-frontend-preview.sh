#!/bin/bash
# Launch the reviewed frontend branch against a local testing-project copy.
# Usage: bash run-frontend-preview.sh [project-directory] [port]
set -euo pipefail
lhp_preview_code=/private/tmp/lhp-frontend-guidance-templates-092
lhp_preview_python=/private/tmp/lhp-ux-backend-venv/bin/python
lhp_preview_project="${1:-/private/tmp/lhp-guidance-testing-project-092}"
lhp_preview_port="${2:-8137}"
if [[ ! -x "$lhp_preview_python" || ! -f "$lhp_preview_code/src/lhp/webapp/static/index.html" ]]; then
  echo "The prepared preview runtime or built frontend is missing." >&2
  exit 1
fi
if [[ ! -f "$lhp_preview_project/lhp.yaml" ]]; then
  echo "No lhp.yaml found in: $lhp_preview_project" >&2
  exit 1
fi
cd "$lhp_preview_project"
exec env PATH="/private/tmp/lhp-ux-backend-venv/bin:$PATH" PYTHONPATH="$lhp_preview_code/src" "$lhp_preview_python" -m lhp.cli.main web --port "$lhp_preview_port"
