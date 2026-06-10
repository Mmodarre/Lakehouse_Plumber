"""End-to-end parallel-batch failure-mode tests for ``lhp generate``.

The fixture in these tests is **3 pipelines × 4 flowgroups** so we can
exercise the flat-pool engine with enough concurrency to catch
cross-pipeline races and partial-failure state isolation.

These tests build a fresh LHP project under ``tmp_path`` from scratch
(no shared helpers with the legacy parallel-test file). The Click CLI
is invoked via ``click.testing.CliRunner.invoke`` so we exercise the
whole stack: ``lhp generate`` -> ApplicationFacade -> orchestrator
-> flat-pool engine.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner


def _build_lhp_project(project_root: Path) -> None:
    """Build a minimal valid LHP project skeleton at ``project_root``.

    Layout:
        lhp.yaml
        substitutions/dev.yaml
        pipelines/
            <to be filled in per test>
    """
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "pipelines").mkdir(exist_ok=True)
    (project_root / "substitutions").mkdir(exist_ok=True)
    (project_root / "presets").mkdir(exist_ok=True)
    (project_root / "templates").mkdir(exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        'name: parallel_failure_modes_test\nversion: "1.0"\n'
    )

    (project_root / "substitutions" / "dev.yaml").write_text(
        "dev:\n  env: dev\n  catalog: test_catalog\n  bronze_schema: bronze\n"
    )


def _valid_flowgroup_yaml(pipeline: str, flowgroup: str, table: str) -> str:
    """Build a self-contained valid flowgroup YAML.

    Uses ``type: sql`` load with a one-liner SELECT, plus a streaming_table
    write target. Identical structure across the 3×4 fixture so behavior is
    apples-to-apples.
    """
    return textwrap.dedent(f"""\
        pipeline: {pipeline}
        flowgroup: {flowgroup}
        actions:
          - name: load_{flowgroup}
            type: load
            source:
              type: sql
              sql: "SELECT 1 as id"
            target: v_{flowgroup}
          - name: write_{flowgroup}
            type: write
            source: v_{flowgroup}
            write_target:
              type: streaming_table
              database: ${{catalog}}.${{bronze_schema}}
              table: {table}
              create_table: true
        """)


def _broken_flowgroup_yaml(pipeline: str, flowgroup: str) -> str:
    """Build a broken flowgroup: references a template that doesn't exist."""
    return textwrap.dedent(f"""\
        pipeline: {pipeline}
        flowgroup: {flowgroup}
        use_template: this_template_does_not_exist
        template_parameters: {{}}
        actions: []
        """)


def _write_3x4_fixture(
    project_root: Path,
    *,
    broken_pipeline: str | None = None,
    broken_flowgroup_index: int | None = None,
) -> None:
    """Write a 3 pipelines × 4 flowgroups fixture.

    If ``broken_pipeline`` is provided, ONE flowgroup in that pipeline (the
    one at ``broken_flowgroup_index``) is broken.
    """
    pipelines = [
        ("pipeline_alpha", "01_alpha"),
        ("pipeline_beta", "02_beta"),
        ("pipeline_gamma", "03_gamma"),
    ]
    for pipeline_name, pipeline_dir in pipelines:
        pdir = project_root / "pipelines" / pipeline_dir
        pdir.mkdir(parents=True, exist_ok=True)
        for i in range(4):
            fg_name = f"{pipeline_name}_fg{i}"
            table = f"t_{pipeline_name}_{i}"
            yaml_path = pdir / f"{fg_name}.yaml"
            if (
                broken_pipeline == pipeline_name
                and broken_flowgroup_index is not None
                and i == broken_flowgroup_index
            ):
                yaml_path.write_text(_broken_flowgroup_yaml(pipeline_name, fg_name))
            else:
                yaml_path.write_text(
                    _valid_flowgroup_yaml(pipeline_name, fg_name, table)
                )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParallelGenerateFailureModes:
    """3x4 fixture failure-mode tests for the flat-pool engine."""

    def _invoke_cli(self, project_root: Path, *extra_args: str):
        from lhp.cli.main import cli

        runner = CliRunner()
        # CWD must be inside the project so ``_find_project_root`` locates lhp.yaml.
        # Save/restore instead of ``isolated_filesystem`` to keep operating on the project
        # built under tmp_path.
        import os

        prev_cwd = Path.cwd()
        try:
            os.chdir(project_root)
            result = runner.invoke(cli, ["generate", "--env", "dev", *extra_args])
        finally:
            os.chdir(prev_cwd)
        return result

    def test_per_pipeline_completion_marker_emits_once_per_pipeline(
        self, tmp_path: Path
    ) -> None:
        """Each pipeline appears at least once in the run summary table.

        ``--show-details`` expands per-pipeline detail; the per-pipeline rows
        render on an all-success run regardless, so each name surfaces in output.
        """
        project_root = tmp_path / "lhp_proj_per_pipeline_marker"
        _build_lhp_project(project_root)
        _write_3x4_fixture(project_root)  # All valid

        result = self._invoke_cli(project_root, "--show-details")
        # All pipelines succeed.
        assert result.exit_code == 0, (
            f"Expected exit 0 with all-valid fixture; got "
            f"exit_code={result.exit_code}\nOutput:\n{result.output}"
        )

        output = result.output
        for pipeline_name in (
            "pipeline_alpha",
            "pipeline_beta",
            "pipeline_gamma",
        ):
            # ``== 1`` is brittle: any unrelated mention (panel border, banner) would flip it.
            count = output.count(pipeline_name)
            assert count >= 1, (
                f"Expected pipeline {pipeline_name} to surface in CLI "
                f"output at least once; got 0.\nFull output:\n{output}"
            )

    def test_any_failure_writes_no_files_all_or_nothing(self, tmp_path: Path) -> None:
        """One broken flowgroup aborts the WHOLE batch — no pipeline writes.

        Generate is all-or-nothing: the gate raises on ANY per-flowgroup failure BEFORE
        any file is written, so even all-valid siblings emit zero ``.py`` files.
        """
        project_root = tmp_path / "lhp_proj_all_or_nothing"
        _build_lhp_project(project_root)
        _write_3x4_fixture(
            project_root,
            broken_pipeline="pipeline_beta",
            broken_flowgroup_index=2,
        )

        result = self._invoke_cli(project_root, "--show-details")

        assert result.exit_code != 0, (
            f"Expected non-zero exit when a flowgroup is broken; got "
            f"exit_code={result.exit_code}\nOutput:\n{result.output}"
        )

        generated_dev = project_root / "generated" / "dev"

        for pipeline_name in (
            "pipeline_alpha",
            "pipeline_beta",
            "pipeline_gamma",
        ):
            pdir = generated_dev / pipeline_name
            written = list(pdir.glob("*.py")) if pdir.exists() else []
            assert not written, (
                f"All-or-nothing violated: pipeline {pipeline_name} wrote "
                f"{[p.name for p in written]} despite a failing flowgroup in "
                f"the batch.\nOutput:\n{result.output}"
            )
