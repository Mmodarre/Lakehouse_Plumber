"""KNOWN-FAILING (owner-authorized).

Documents finding #2: src/lhp/presets/preset_manager.py ~L122 merges
operational_metadata via list(set(...)) (and ~L67 joins a cycle path over a
set), producing non-deterministic order across PYTHONHASHSEED ->
non-reproducible codegen / spurious E2E hash diffs. The CODE is wrong, not
this test -- make the merge deterministic (e.g. preserve insertion order or
sort); do not weaken this assertion.

Why subprocesses?
-----------------
CPython randomizes str/bytes hashing per process (PYTHONHASHSEED). The
iteration order of `set` therefore only varies *across* processes, not within
one. So we spawn several child interpreters with different PYTHONHASHSEED
values, run the real PresetManager merge in each, capture the resulting column
ORDER, and assert every child produced the identical order.

Against `list(set(...))` the orders diverge -> these tests FAIL (intended).
After a deterministic fix (insertion-order-preserving dedup or a sort) every
child yields the same order -> they pass.
"""

import os
import subprocess
import sys

import pytest

# Enough distinct string columns that randomized set ordering reliably
# diverges across the seeds below. With 8 elements the probability of two
# independent random hash seeds yielding the same set-iteration order is
# vanishingly small, so the failure is reliably red (never flaky).
SEEDS = ["0", "1", "2", "42", "7", "99"]


_MERGE_CHILD = r"""
import json, tempfile
from pathlib import Path
from lhp.presets.preset_manager import PresetManager

with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    (d / "base.yaml").write_text(
        "name: base\n"
        "version: \"1.0\"\n"
        "defaults:\n"
        "  operational_metadata:\n"
        "    - _ingestion_timestamp\n"
        "    - _source_file_path\n"
        "    - _pipeline_run_id\n"
        "    - _record_hash\n"
    )
    (d / "ext.yaml").write_text(
        "name: ext\n"
        "version: \"1.0\"\n"
        "extends: base\n"
        "defaults:\n"
        "  operational_metadata:\n"
        "    - _processing_timestamp\n"
        "    - _batch_id\n"
        "    - _environment\n"
        "    - _record_version\n"
    )
    mgr = PresetManager(d)
    resolved = mgr.resolve_preset_chain(["ext"])
    print(json.dumps(resolved["operational_metadata"]))
"""


_CYCLE_CHILD = r"""
import tempfile
from pathlib import Path
from lhp.presets.preset_manager import PresetManager

with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    (d / "a.yaml").write_text("name: a\nversion: \"1.0\"\nextends: b\ndefaults: {}\n")
    (d / "b.yaml").write_text("name: b\nversion: \"1.0\"\nextends: c\ndefaults: {}\n")
    (d / "c.yaml").write_text("name: c\nversion: \"1.0\"\nextends: dd\ndefaults: {}\n")
    (d / "dd.yaml").write_text("name: dd\nversion: \"1.0\"\nextends: a\ndefaults: {}\n")
    mgr = PresetManager(d)
    try:
        mgr.resolve_preset_chain(["a"])
    except Exception as e:
        ctx = getattr(e, "context", {}) or {}
        print(ctx.get("Chain", ""))
    else:
        raise SystemExit("expected circular-inheritance error was not raised")
"""


def _run_with_seed(child_source: str, seed: str) -> str:
    """Fails loudly if the child errors, so a real product/import regression cannot masquerade as a green test."""
    env = {**os.environ, "PYTHONHASHSEED": seed}
    proc = subprocess.run(
        [sys.executable, "-c", child_source],
        capture_output=True,
        text=True,
        env=env,
    )
    assert proc.returncode == 0, (
        f"child (PYTHONHASHSEED={seed}) exited {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    return proc.stdout.strip()


@pytest.mark.unit
def test_operational_metadata_merge_order_is_deterministic_known_bug():
    """Merged operational_metadata column order must be identical across
    PYTHONHASHSEED values.

    KNOWN-FAILING: preset_manager.py ~L122 dedups via list(set(...)), whose
    iteration order is randomized per process -> the merged column order
    differs across seeds. CORRECT behavior is a single deterministic order
    (insertion-order-preserving dedup or a sort). Fix the production code; do
    NOT weaken this assertion.
    """
    outputs = {seed: _run_with_seed(_MERGE_CHILD, seed) for seed in SEEDS}

    distinct = set(outputs.values())
    assert len(distinct) == 1, (
        "operational_metadata merge order is NON-DETERMINISTIC across "
        "PYTHONHASHSEED (finding #2, preset_manager.py ~L122 "
        "`list(set(...))`). Per-seed outputs:\n"
        + "\n".join(f"  PYTHONHASHSEED={s}: {outputs[s]}" for s in SEEDS)
    )


@pytest.mark.unit
def test_circular_inheritance_cycle_path_is_deterministic_known_bug():
    """The LHP-CFG-022 circular-inheritance cycle-path string must be identical
    across PYTHONHASHSEED values.

    KNOWN-FAILING: preset_manager.py ~L67 builds the cycle path by joining over
    the `visited` set, whose iteration order is randomized per process -> the
    reported cycle path differs across seeds. CORRECT behavior is a single
    deterministic, reproducible path string. Fix the production code; do NOT
    weaken this assertion.
    """
    outputs = {seed: _run_with_seed(_CYCLE_CHILD, seed) for seed in SEEDS}

    distinct = set(outputs.values())
    assert len(distinct) == 1, (
        "circular-inheritance cycle-path string is NON-DETERMINISTIC across "
        "PYTHONHASHSEED (finding #2, preset_manager.py ~L67 join over a "
        "set). Per-seed outputs:\n"
        + "\n".join(f"  PYTHONHASHSEED={s}: {outputs[s]}" for s in SEEDS)
    )
