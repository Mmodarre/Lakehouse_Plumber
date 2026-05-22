"""Worker-log leak regression test for ``lhp generate``.

The defining defect: spawn-pool workers used to attach ``logging.basicConfig``
with a ``[worker %(process)d]`` stderr stream handler. The parent's
``Live(... redirect_stderr=True)`` only intercepts the parent's own
``sys.stderr``, not the worker's, so up to N copies of every worker-side
warning leaked verbatim to the user's terminal — including multi-line
``===``-bordered LHPError blocks.

Phase 3 of the LHP error-UX hardening replaced ``basicConfig`` in
``_init_worker_logger`` with a ``NullHandler``-only setup. This test
exercises the failing reproducer end-to-end via a real subprocess and
asserts that zero ``[worker `` lines escape to either output stream.
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


def _build_leak_repro_fixture(project_root: Path) -> None:
    """Build a minimal LHP project with one intentionally-broken flowgroup.

    Two pipelines × one flowgroup each, so the spawn pool dispatches at
    least one worker. The ``broken`` flowgroup uses a ``cloudfilesd``
    source type (intentional typo) which surfaces as ``LHP-VAL-007``
    during validation, deterministically failing the generate run with a
    rendered LHPError panel.
    """
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "pipelines" / "p_ok").mkdir(parents=True, exist_ok=True)
    (project_root / "pipelines" / "p_broken").mkdir(parents=True, exist_ok=True)
    (project_root / "substitutions").mkdir(exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        'name: worker_log_leak_repro\nversion: "1.0"\n'
    )

    (project_root / "substitutions" / "dev.yaml").write_text(
        textwrap.dedent("""\
            dev:
              env: dev
              catalog: test_catalog
              raw_schema: raw
              landing_volume: /Volumes/test/landing
            """)
    )

    (project_root / "pipelines" / "p_ok" / "ok.yaml").write_text(
        textwrap.dedent("""\
            pipeline: p_ok
            flowgroup: ok
            actions:
              - name: load_ok
                type: load
                source:
                  type: sql
                  sql: "SELECT 1 as id"
                target: v_ok
              - name: write_ok
                type: write
                source: v_ok
                write_target:
                  type: streaming_table
                  database: ${catalog}.${raw_schema}
                  table: ok
                  create_table: true
            """)
    )

    (project_root / "pipelines" / "p_broken" / "broken.yaml").write_text(
        textwrap.dedent("""\
            pipeline: p_broken
            flowgroup: broken
            actions:
              - name: load_broken
                type: load
                source:
                  type: cloudfilesd
                  path: "${landing_volume}/broken/*.parquet"
                  format: parquet
                target: v_broken
              - name: write_broken
                type: write
                source: v_broken
                write_target:
                  type: streaming_table
                  database: ${catalog}.${raw_schema}
                  table: broken
                  create_table: true
            """)
    )


@pytest.mark.e2e
def test_no_worker_log_leak_on_failure(tmp_path: Path) -> None:
    """On a failing generate run, no `[worker NNNN]`-prefixed lines may
    reach the user's terminal on either stdout or stderr.

    The fixture is built fresh under ``tmp_path`` so the test is fully
    self-contained — no dependency on Example_Projects fixture state.
    The CLI is invoked via ``python -m lhp.cli.main`` so the test runs
    whenever the package is importable, regardless of whether the
    ``lhp`` console script is on PATH.
    """
    project_root = tmp_path / "lhp_proj_worker_log_leak"
    _build_leak_repro_fixture(project_root)

    env = {**os.environ, "NO_COLOR": "1"}
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "lhp.cli.main",
            "generate",
            "-e",
            "dev",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )

    # The reproducer fixture has an intentional ``cloudfilesd`` typo,
    # so a clean failing run exits non-zero with an LHPError panel.
    assert result.returncode != 0, (
        f"expected non-zero exit, got {result.returncode}\n"
        f"stdout tail:\n{result.stdout[-500:]}\n"
        f"stderr tail:\n{result.stderr[-500:]}"
    )

    combined = result.stdout + result.stderr
    # Sanity: ensure we actually ran the LHP CLI (the failure panel
    # references the LHP-VAL-007 code surfaced for the cloudfilesd typo).
    assert "LHP-VAL-" in combined, (
        f"lhp generate did not produce an LHPError panel — subprocess "
        f"may have failed before reaching the generate path.\n"
        f"stdout tail:\n{result.stdout[-500:]}\n"
        f"stderr tail:\n{result.stderr[-500:]}"
    )

    leak_count = combined.count("[worker ")
    assert leak_count == 0, (
        f"Found {leak_count} `[worker ` leaked log lines.\n"
        f"First 1000 chars of combined output:\n{combined[:1000]}"
    )
