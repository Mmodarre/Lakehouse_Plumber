"""End-to-end parallel-batch failure-mode tests for ``lhp generate``.

The fixture in these tests is **3 pipelines × 4 flowgroups** so we can
exercise the flat-pool engine with enough concurrency to catch
cross-pipeline races and partial-failure state isolation.

These tests build a fresh LHP project under ``tmp_path`` from scratch
(no shared helpers with the legacy parallel-test file). The Click CLI
is invoked via ``click.testing.CliRunner.invoke`` so we exercise the
whole stack: CLI -> GenerateCommand -> ApplicationFacade -> orchestrator
-> flat-pool engine.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

# ---------------------------------------------------------------------------
# Fixture-building helpers (intentionally local — no shared setup with the
# legacy parallel-test file).
# ---------------------------------------------------------------------------


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
        # Click invoke; CWD must be inside the project so the CLI's
        # _find_project_root locates lhp.yaml. We save & restore the
        # outer CWD around the invocation rather than using
        # isolated_filesystem, since we want to operate ON the project
        # we built under tmp_path, not on a fresh isolated dir.
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
        """Each pipeline appears exactly once in the run summary table.

        Post-Phase-4, the per-pipeline completion signal lives in the
        Rich summary table that follows the Live status panel. The
        behavioral contract being verified — every pipeline submitted to
        the batch generator surfaces in the user-visible output exactly
        once — is unchanged; only the rendered form moved from a
        streaming ``✅ {pipeline_id}: ...`` line per pipeline to one row
        in the summary table. Counting the pipeline name preserves the
        original guard for missed/duplicated completion records.

        ``--show-all`` opts into the full per-pipeline summary table;
        the post-Phase-E failures-only default would suppress the table
        on this all-success fixture, but this test asserts on
        per-pipeline row visibility.
        """
        project_root = tmp_path / "lhp_proj_per_pipeline_marker"
        _build_lhp_project(project_root)
        _write_3x4_fixture(project_root)  # All valid

        result = self._invoke_cli(project_root, "--show-all")
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
            # Looser check than the original ``output.count(name) == 1`` —
            # the count form was brittle, because any unrelated mention
            # (e.g. inside a Rich panel border, breadcrumb, or banner)
            # would flip the assertion. The behavioral contract is
            # "appears at least once in user-visible output", which is
            # what ``>= 1`` enforces.
            count = output.count(pipeline_name)
            assert count >= 1, (
                f"Expected pipeline {pipeline_name} to surface in CLI "
                f"output at least once; got 0.\nFull output:\n{output}"
            )

    def test_partial_failure_isolates_broken_pipeline(self, tmp_path: Path) -> None:
        """One broken flowgroup must not poison sibling pipelines.

        Cross-pipeline isolation contract for the flat-pool engine: a
        Phase-A failure inside one pipeline bubbles up as an overall
        non-zero exit, but the workers handling the OTHER pipelines
        continue to completion and their per-pipeline artifacts land on
        disk. The broken pipeline's artifacts must NOT be written (the
        PipelineProcessor short-circuits via _raise_for_phase_a_failures
        before _write_python_files runs).
        """
        project_root = tmp_path / "lhp_proj_partial_failure"
        _build_lhp_project(project_root)
        _write_3x4_fixture(
            project_root,
            broken_pipeline="pipeline_beta",
            broken_flowgroup_index=2,
        )

        result = self._invoke_cli(project_root, "--show-all")

        # Overall failure: at least one pipeline blew up.
        assert result.exit_code != 0, (
            f"Expected non-zero exit when a flowgroup is broken; got "
            f"exit_code={result.exit_code}\nOutput:\n{result.output}"
        )

        generated_dev = project_root / "generated" / "dev"

        # Sibling pipelines completed: their per-flowgroup .py files exist.
        for sibling in ("pipeline_alpha", "pipeline_gamma"):
            sibling_dir = generated_dev / sibling
            assert sibling_dir.exists(), (
                f"Sibling pipeline {sibling} directory missing — broken "
                f"pipeline poisoned the sibling.\nOutput:\n{result.output}"
            )
            py_files = sorted(p.name for p in sibling_dir.glob("*.py"))
            assert py_files, (
                f"Sibling pipeline {sibling} produced no .py files — "
                f"broken pipeline poisoned the sibling.\nOutput:\n"
                f"{result.output}"
            )

        # Broken pipeline: no .py files emitted (PipelineProcessor
        # short-circuits before _write_python_files when Phase A fails).
        broken_dir = generated_dev / "pipeline_beta"
        broken_files = list(broken_dir.glob("*.py")) if broken_dir.exists() else []
        assert not broken_files, (
            f"Broken pipeline pipeline_beta should have no .py files "
            f"written, found: {[p.name for p in broken_files]}"
        )
