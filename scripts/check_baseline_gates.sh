#!/usr/bin/env bash
# Regression-only constitution gates for the Week-8 refactor window.
#
# Week 8 end-state per plan §7:
#   * 3 file-size violations (§3.3 / §9.3) — Phase-9 deferrals
#     (CLI presenter extraction + generator-template extraction).
#   * 0 placement violations (§2 / §9.2) — Phase 8 closed the pool.
#
# This script blocks a commit only on a REGRESSION above those Week-8
# baselines; the file-size pool is allowed to persist at 3 until the
# Phase-9 deferrals land. The remaining gates (stability-drift, ruff
# B904/TRY400/RUF013, import-linter, tests/api/ contract tests) stay
# strict (zero tolerance).
#
# Once the Phase-9 deferrals land, set the file-size baseline to 0 and
# the gate behaves identically to the original strict configuration.

set -u

WK8_FILE_SIZE_BASELINE=0
WK8_PLACEMENT_BASELINE=0

echo '[lhp] running pre-commit constitution gates...' >&2

fs_out=$(python scripts/check_file_sizes.py --all 2>&1)
fs_count=$(printf '%s\n' "$fs_out" | sed -n 's/^\[check_file_sizes\] \([0-9][0-9]*\) violation.*/\1/p')
[ -z "$fs_count" ] && fs_count=0
if [ "$fs_count" -gt "$WK8_FILE_SIZE_BASELINE" ]; then
  printf '%s\n' "$fs_out" >&2
  echo "[lhp] file-size REGRESSION: $fs_count > $WK8_FILE_SIZE_BASELINE (Week-8 baseline, plan §7)" >&2
  exit 2
fi

pl_out=$(python scripts/check_placement.py --all 2>&1)
pl_count=$(printf '%s\n' "$pl_out" | sed -n 's/^\[check_placement\] \([0-9][0-9]*\) violation.*/\1/p')
[ -z "$pl_count" ] && pl_count=0
if [ "$pl_count" -gt "$WK8_PLACEMENT_BASELINE" ]; then
  printf '%s\n' "$pl_out" >&2
  echo "[lhp] placement REGRESSION: $pl_count > $WK8_PLACEMENT_BASELINE (Week-8 baseline, plan §7)" >&2
  exit 2
fi

python scripts/check_stability_drift.py --all || exit 2

# §9.14 — no inline f-string codegen >20 lines in generators/. Strict
# (no baseline tolerance) because this gate landed alongside Phase 9.3
# which already eliminates every pre-existing violation.
python scripts/check_codegen_inline.py --all || exit 2

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
