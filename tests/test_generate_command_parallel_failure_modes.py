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

import json
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
        "name: parallel_failure_modes_test\n" 'version: "1.0"\n'
    )

    (project_root / "substitutions" / "dev.yaml").write_text(
        "dev:\n" "  env: dev\n" "  catalog: test_catalog\n" "  bronze_schema: bronze\n"
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

    def test_bad_flowgroup_in_middle_pipeline_leaves_others_intact(
        self, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "lhp_proj_mid_fail"
        _build_lhp_project(project_root)
        # Middle pipeline is pipeline_beta; break its 2nd flowgroup.
        _write_3x4_fixture(
            project_root,
            broken_pipeline="pipeline_beta",
            broken_flowgroup_index=1,
        )

        result = self._invoke_cli(project_root)
        # The overall command must exit non-zero (one pipeline failed).
        assert result.exit_code != 0, (
            f"Expected non-zero exit code on partial failure, got "
            f"exit_code={result.exit_code}\nOutput:\n{result.output}"
        )

        # Other pipelines' generated files should be written.
        generated_dir = project_root / "generated" / "dev"
        alpha_files = list((generated_dir / "pipeline_alpha").glob("*.py"))
        gamma_files = list((generated_dir / "pipeline_gamma").glob("*.py"))
        assert alpha_files, (
            "Successful pipeline_alpha must have written .py files. "
            f"Found: {alpha_files}"
        )
        assert gamma_files, (
            "Successful pipeline_gamma must have written .py files. "
            f"Found: {gamma_files}"
        )

        # State file contains the successful pipelines' entries.
        state_file = project_root / ".lhp_state.json"
        assert state_file.exists(), (
            "State file must exist after partial failure (successful "
            "pipelines should have persisted state)."
        )
        # Read via StateManager to decouple from raw JSON schema details.
        from lhp.core.state_manager import StateManager

        sm = StateManager(project_root=project_root)
        env_files = sm.get_generated_files("dev")
        persisted_pipelines = {fs.pipeline for fs in env_files.values()}
        assert "pipeline_alpha" in persisted_pipelines, (
            "pipeline_alpha entries must be in state. "
            f"Persisted: {persisted_pipelines}"
        )
        assert "pipeline_gamma" in persisted_pipelines, (
            "pipeline_gamma entries must be in state. "
            f"Persisted: {persisted_pipelines}"
        )

        # Failing pipeline's flowgroups must NOT be in the state file.
        assert "pipeline_beta" not in persisted_pipelines, (
            "pipeline_beta's entries must NOT be in state on failure. "
            f"Persisted: {persisted_pipelines}"
        )

    def test_state_persists_through_simulated_kill(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Monkeypatch ``_assemble_pipeline_outputs`` to raise on 2nd call.

        Simulates a kill mid-batch after pipeline 1 has fully persisted
        its state. Asserts the first pipeline's entries survive.
        """
        project_root = tmp_path / "lhp_proj_sim_kill"
        _build_lhp_project(project_root)
        _write_3x4_fixture(project_root)  # All valid

        from lhp.core.orchestrator import ActionOrchestrator

        original = ActionOrchestrator._assemble_pipeline_outputs
        call_count = {"n": 0}

        def kill_on_second_call(self, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("simulated kill mid-batch (2nd assemble call)")
            return original(self, *args, **kwargs)

        monkeypatch.setattr(
            ActionOrchestrator,
            "_assemble_pipeline_outputs",
            kill_on_second_call,
        )

        result = self._invoke_cli(project_root)
        # Should fail (simulated kill).
        assert result.exit_code != 0, (
            f"Expected non-zero exit code on simulated kill; got "
            f"exit_code={result.exit_code}\nOutput:\n{result.output}"
        )

        # Pipeline 1's state must persist regardless of pipeline 2's kill.
        state_file = project_root / ".lhp_state.json"
        assert state_file.exists(), (
            "State file must exist; pipeline 1's atomic save should have "
            "completed before pipeline 2's simulated kill."
        )

        from lhp.core.state_manager import StateManager

        sm = StateManager(project_root=project_root)
        env_files = sm.get_generated_files("dev")
        persisted_pipelines = {fs.pipeline for fs in env_files.values()}

        # At least one pipeline (the first completed) must be persisted.
        # We don't constrain WHICH pipeline completes first (depends on
        # thread-pool scheduling order), only that at least one did.
        assert persisted_pipelines, (
            "At least one pipeline's entries must have survived the "
            "simulated kill. Persisted: (none)"
        )

    def test_per_pipeline_completion_marker_emits_once_per_pipeline(
        self, tmp_path: Path
    ) -> None:
        """Assert the CLI emits one ✅ {pipeline}: ... marker per pipeline.

        ``_display_generation_response`` emits a line of the form
        ``✅ {pipeline_id}: Generated N file(s)``  (or ``Up-to-date``).
        We count occurrences of ``✅ {pipeline_name}:`` to confirm each
        pipeline gets exactly one such line.
        """
        project_root = tmp_path / "lhp_proj_per_pipeline_marker"
        _build_lhp_project(project_root)
        _write_3x4_fixture(project_root)  # All valid

        result = self._invoke_cli(project_root)
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
            # ``_display_generation_response`` emits exactly one
            # ``✅ {pipeline_id}:`` per pipeline. The summary footer line
            # is ``✅ Code generation completed successfully`` (no
            # pipeline_id colon), so this substring search is unique per
            # pipeline.
            marker = f"✅ {pipeline_name}:"
            count = output.count(marker)
            assert count == 1, (
                f"Expected exactly 1 ✅ marker for {pipeline_name}, "
                f"got {count}.\nFull output:\n{output}"
            )
