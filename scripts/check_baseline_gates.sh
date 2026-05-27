#!/usr/bin/env bash
# Regression-only constitution gates for the Week-7 refactor window.
#
# Phase 7 carries two transitional violation pools per plan §7:
#   * 5 file-size violations  (§3.3 / §9.3) — owned by Phase 9.
#   * 15 placement violations (§2 / §9.2)   — owned by Phase 8.
#
# This script blocks a commit only on a REGRESSION above those Week-6
# baselines; the absolute pool is allowed to persist until Phase 8/9
# closes it. The remaining gates (stability-drift, ruff B904/TRY400/
# RUF013, import-linter, tests/api/ contract tests) stay strict
# (zero tolerance).
#
# Once Phase 8 + Phase 9 land, set both baselines to 0 and the gate
# behaves identically to the original strict configuration.

set -u

WK6_FILE_SIZE_BASELINE=5
WK6_PLACEMENT_BASELINE=15

echo '[lhp] running pre-commit constitution gates...' >&2

fs_out=$(python scripts/check_file_sizes.py --all 2>&1)
fs_count=$(printf '%s\n' "$fs_out" | sed -n 's/^\[check_file_sizes\] \([0-9][0-9]*\) violation.*/\1/p')
[ -z "$fs_count" ] && fs_count=0
if [ "$fs_count" -gt "$WK6_FILE_SIZE_BASELINE" ]; then
  printf '%s\n' "$fs_out" >&2
  echo "[lhp] file-size REGRESSION: $fs_count > $WK6_FILE_SIZE_BASELINE (Week-6 baseline, plan §7)" >&2
  exit 2
fi

pl_out=$(python scripts/check_placement.py --all 2>&1)
pl_count=$(printf '%s\n' "$pl_out" | sed -n 's/^\[check_placement\] \([0-9][0-9]*\) violation.*/\1/p')
[ -z "$pl_count" ] && pl_count=0
if [ "$pl_count" -gt "$WK6_PLACEMENT_BASELINE" ]; then
  printf '%s\n' "$pl_out" >&2
  echo "[lhp] placement REGRESSION: $pl_count > $WK6_PLACEMENT_BASELINE (Week-6 baseline, plan §7)" >&2
  exit 2
fi

python scripts/check_stability_drift.py --all || exit 2

if command -v ruff >/dev/null 2>&1; then
  ruff check --select B904,TRY400,RUF013 src/ tests/ || exit 2
fi

pre-commit run import-linter --all-files >/dev/null 2>&1 || {
  echo '[lhp] import-linter failed; run: pre-commit run import-linter --all-files' >&2
  exit 2
}

if [ -d tests/api ]; then
  pytest tests/api/ -q --no-header --tb=line >/dev/null 2>&1 || {
    echo '[lhp] tests/api/ contract tests failed; run: pytest tests/api/ -v' >&2
    exit 2
  }
fi

exit 0
