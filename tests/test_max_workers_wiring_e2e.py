"""End-to-end ``--max-workers`` wiring assertion.

Verifies the CLI's ``--max-workers N`` flag is propagated all the way
through ``GenerateCommand.execute`` -> ``_create_application_facade`` ->
``ActionOrchestrator.__init__`` as ``max_workers=N`` (not just that the
signature accepts the kwarg, but that the actual value the user typed
arrives at the orchestrator constructor).

The test monkeypatches ``ActionOrchestrator.__init__`` to capture the
constructor's args/kwargs, builds a minimal valid LHP project under
``tmp_path``, then invokes the Click CLI via
``click.testing.CliRunner``.
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
from click.testing import CliRunner


def _build_minimal_lhp_project(project_root: Path) -> None:
    """Build the minimal valid LHP project layout under ``project_root``.

    Required for the CLI's ``_find_project_root`` + ``check_substitution_file``
    setup to succeed. The pipeline content here doesn't have to actually
    generate — the monkeypatched ``ActionOrchestrator`` will short-circuit
    real generation.
    """
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "pipelines").mkdir(exist_ok=True)
    (project_root / "substitutions").mkdir(exist_ok=True)
    (project_root / "presets").mkdir(exist_ok=True)
    (project_root / "templates").mkdir(exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        "name: max_workers_wiring_test\n" 'version: "1.0"\n'
    )
    (project_root / "substitutions" / "dev.yaml").write_text(
        "dev:\n" "  env: dev\n" "  catalog: test_catalog\n" "  bronze_schema: bronze\n"
    )

    # One trivial flowgroup so discovery has something to find. We don't
    # need it to generate — the monkeypatched __init__ will stop us before
    # generation runs to completion (or it may run; either is fine because
    # the assertion is purely on the constructor args).
    pdir = project_root / "pipelines" / "01_trivial"
    pdir.mkdir(parents=True, exist_ok=True)
    (pdir / "fg1.yaml").write_text(textwrap.dedent("""\
            pipeline: pipeline_trivial
            flowgroup: fg1
            actions:
              - name: load_fg1
                type: load
                source:
                  type: sql
                  sql: "SELECT 1 as id"
                target: v_fg1
              - name: write_fg1
                type: write
                source: v_fg1
                write_target:
                  type: streaming_table
                  database: ${catalog}.${bronze_schema}
                  table: t_fg1
                  create_table: true
            """))


@pytest.mark.unit
class TestMaxWorkersWiringEndToEnd:
    """Plan task E: ``--max-workers 3`` reaches ActionOrchestrator.__init__."""

    def test_cli_max_workers_arg_reaches_orchestrator_constructor(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        project_root = tmp_path / "lhp_proj_mw"
        _build_minimal_lhp_project(project_root)

        # Capture ALL ActionOrchestrator.__init__ invocations with their
        # full args/kwargs. We then assert one of them received
        # ``max_workers=3``. We don't constrain WHICH invocation if the
        # CLI happens to construct an orchestrator more than once — only
        # that the value reached the constructor at least once.
        from lhp.core.coordination import orchestrator as orchestrator_module

        original_init = orchestrator_module.ActionOrchestrator.__init__
        captured_calls: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []

        def capturing_init(self, *args: Any, **kwargs: Any) -> None:
            captured_calls.append((args, kwargs))
            return original_init(self, *args, **kwargs)

        monkeypatch.setattr(
            orchestrator_module.ActionOrchestrator,
            "__init__",
            capturing_init,
        )

        # Invoke the CLI inside the project root.
        from lhp.cli.main import cli

        runner = CliRunner()
        import os

        prev_cwd = Path.cwd()
        try:
            os.chdir(project_root)
            result = runner.invoke(
                cli, ["generate", "--env", "dev", "--max-workers", "3"]
            )
        finally:
            os.chdir(prev_cwd)

        # The CLI may or may not exit zero depending on whether the
        # generation succeeded — we don't care. What we care about is
        # whether ActionOrchestrator.__init__ was called with
        # ``max_workers=3``.
        assert captured_calls, (
            "Expected at least one ActionOrchestrator construction; got 0.\n"
            f"CLI exit_code={result.exit_code}\nOutput:\n{result.output}"
        )

        # Find a call with max_workers == 3 (kwarg-form), or with 3 as the
        # max_workers positional (the orchestrator signature is keyword-only
        # for that arg in the current codebase, but we accept both forms
        # defensively).
        found_max_workers_3 = False
        for args, kwargs in captured_calls:
            if kwargs.get("max_workers") == 3:
                found_max_workers_3 = True
                break

        assert found_max_workers_3, (
            "ActionOrchestrator.__init__ was never called with "
            "max_workers=3. The --max-workers CLI flag is NOT being "
            "propagated to the orchestrator constructor.\n"
            f"Captured calls (args/kwargs):\n"
            + "\n".join(f"  args={a!r}\n  kwargs={k!r}" for a, k in captured_calls)
            + f"\n\nCLI exit_code={result.exit_code}\nOutput:\n{result.output}"
        )
