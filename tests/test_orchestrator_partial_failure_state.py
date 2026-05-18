"""Partial-failure state persistence semantics for batch pipeline generation.

When the orchestrator processes multiple pipelines in a single flat-pool
batch and one pipeline fails, the successful pipelines' state entries
must still be written to ``.lhp_state.json`` (per the architecture's
incremental-ROI guarantee). The failed pipeline's entries must NOT be
in the state file.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.state_manager import StateManager
from lhp.utils.error_formatter import LHPError


def _write_lhp_project_skeleton(project_root: Path) -> None:
    """Build a minimal LHP project at ``project_root``."""
    (project_root / "pipelines").mkdir(parents=True, exist_ok=True)
    (project_root / "substitutions").mkdir(parents=True, exist_ok=True)
    (project_root / "presets").mkdir(parents=True, exist_ok=True)
    (project_root / "templates").mkdir(parents=True, exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        'name: partial_failure_test\nversion: "1.0"\n'
    )
    (project_root / "substitutions" / "dev.yaml").write_text(
        "dev:\n" "  env: dev\n" "  catalog: test_catalog\n" "  bronze_schema: bronze\n"
    )


def _write_simple_flowgroup(
    project_root: Path,
    pipeline_dir: str,
    pipeline_name: str,
    flowgroup_name: str,
    target_table: str,
) -> Path:
    """Write a self-contained valid flowgroup with a load + write action."""
    pdir = project_root / "pipelines" / pipeline_dir
    pdir.mkdir(parents=True, exist_ok=True)
    yaml_path = pdir / f"{flowgroup_name}.yaml"
    yaml_path.write_text(
        f"pipeline: {pipeline_name}\n"
        f"flowgroup: {flowgroup_name}\n"
        "actions:\n"
        f"  - name: load_{flowgroup_name}\n"
        "    type: load\n"
        "    source:\n"
        "      type: sql\n"
        f'      sql: "SELECT 1 as id"\n'
        f"    target: v_{flowgroup_name}\n"
        f"  - name: write_{flowgroup_name}\n"
        "    type: write\n"
        f"    source: v_{flowgroup_name}\n"
        "    write_target:\n"
        "      type: streaming_table\n"
        "      database: ${catalog}.${bronze_schema}\n"
        f"      table: {target_table}\n"
        f"      create_table: true\n"
    )
    return yaml_path


def _write_broken_flowgroup(
    project_root: Path,
    pipeline_dir: str,
    pipeline_name: str,
    flowgroup_name: str,
) -> Path:
    """Write a flowgroup that references a template that does not exist.

    Failure surfaces inside ``FlowgroupProcessor.process_flowgroup`` ->
    ``TemplateEngine.render_template`` -> ``ErrorFormatter.template_not_found``
    (an :class:`LHPError`).
    """
    pdir = project_root / "pipelines" / pipeline_dir
    pdir.mkdir(parents=True, exist_ok=True)
    yaml_path = pdir / f"{flowgroup_name}.yaml"
    yaml_path.write_text(
        f"pipeline: {pipeline_name}\n"
        f"flowgroup: {flowgroup_name}\n"
        "use_template: this_template_does_not_exist\n"
        "template_parameters: {}\n"
        "actions: []\n"
    )
    return yaml_path


@pytest.mark.unit
class TestGeneratePipelinesByFieldsPartialFailure:
    """Plan task B: aggregate-error + per-pipeline atomic save."""

    def test_partial_failure_persists_successful_pipelines_state(
        self, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "lhp_proj"
        project_root.mkdir()
        _write_lhp_project_skeleton(project_root)

        # 3 pipelines: A (ok), B (broken — middle), C (ok).
        _write_simple_flowgroup(
            project_root,
            pipeline_dir="01_a",
            pipeline_name="pipeline_a",
            flowgroup_name="a_fg",
            target_table="t_a",
        )
        _write_broken_flowgroup(
            project_root,
            pipeline_dir="02_b",
            pipeline_name="pipeline_b",
            flowgroup_name="b_fg",
        )
        _write_simple_flowgroup(
            project_root,
            pipeline_dir="03_c",
            pipeline_name="pipeline_c",
            flowgroup_name="c_fg",
            target_table="t_c",
        )

        output_dir = project_root / "generated" / "dev"

        orchestrator = ActionOrchestrator(
            project_root=project_root,
            enforce_version=False,
            max_workers=1,  # determinism; matters for clarity only
        )
        state_manager = StateManager(project_root=project_root)

        # (a) Aggregate LHPValidationError raised.
        with pytest.raises(LHPError):
            orchestrator.generate_pipelines_by_fields(
                pipeline_fields=["pipeline_a", "pipeline_b", "pipeline_c"],
                env="dev",
                output_dir=output_dir,
                state_manager=state_manager,
            )

        # The orchestrator's per-pipeline atomic save in
        # _assemble_pipeline_outputs should have already flushed
        # successful pipelines' state entries to .lhp_state.json. Reload
        # the state file from disk to verify.
        state_file = project_root / ".lhp_state.json"
        assert state_file.exists(), (
            "State file must exist after partial-failure batch run "
            "(successful pipelines should have persisted their state)."
        )

        with state_file.open() as fh:
            state_data = json.load(fh)

        # The state schema places generated files under environments → env →
        # pipeline keying via FileState records. We inspect the persisted
        # state by reading it via a fresh StateManager (decoupling from
        # raw JSON schema details).
        reloaded = StateManager(project_root=project_root)
        env_files = reloaded.get_generated_files("dev")

        # Collect pipeline names from the persisted file states.
        persisted_pipelines = {fs.pipeline for fs in env_files.values()}

        # (b) Successful pipelines' entries exist in .lhp_state.json.
        assert "pipeline_a" in persisted_pipelines, (
            "Successful pipeline_a's entries must be persisted in "
            f".lhp_state.json. Persisted: {persisted_pipelines}"
        )
        assert "pipeline_c" in persisted_pipelines, (
            "Successful pipeline_c's entries must be persisted in "
            f".lhp_state.json. Persisted: {persisted_pipelines}"
        )

        # (c) Failed pipeline's entries are NOT in the state file.
        assert "pipeline_b" not in persisted_pipelines, (
            "Failed pipeline_b's entries must NOT be in .lhp_state.json. "
            f"Persisted: {persisted_pipelines}"
        )
