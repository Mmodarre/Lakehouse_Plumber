"""§5.7 stream + plan-isolation invariants for ``GenerationFacade.plan_generation``.

``plan_generation`` is a streaming facade method that mirrors
``generate_pipelines`` event-for-event but produces a ``GenerationPlan``
(generate-to-temp, write nothing real). This module pins the behaviour on a
REAL multi-pipeline project (no mocks — the engine, gate and commit are the
production drivers driven against a throwaway temp dir):

1. ``OperationStarted`` is first; every ``PhaseStarted`` is paired with a
   ``PhaseCompleted`` of the same phase, in discover → preflight → generate
   order; each pipeline emits ``PipelineStarted`` + exactly one terminal (no
   dangling Starts); the terminal ``GenerationPlanCompleted`` is LAST.
2. The terminal carries a complete ``GenerationPlan``: a non-empty ``files``
   tuple, the right ``pipeline_count`` / ``file_count``, and an
   ``output_location`` equal to the REAL ``generated/<env>`` dir a normal
   generate would write to.
3. The real ``generated/<env>`` tree is UNTOUCHED — ``plan_generation`` writes
   nothing to disk (it does NOT commit).
4. ``pipeline_filter`` narrows the plan to one pipeline; ``include_tests=True``
   adds the per-pipeline test-reporting hook (a ``test_hook`` file).

Tests import strictly from :mod:`lhp.api` — no internal modules.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lhp.api import (
    GenerationPlan,
    GenerationPlanCompleted,
    LakehousePlumberApplicationFacade,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    collect_response,
)


def _write_pipeline(project_root: Path, pipeline: str, *, with_test: bool) -> None:
    """Write one valid load+write flowgroup; optionally add a ``test`` action."""
    pdir = project_root / "pipelines" / pipeline
    pdir.mkdir(parents=True, exist_ok=True)
    actions = [
        {
            "name": f"load_{pipeline}",
            "type": "load",
            "target": f"v_{pipeline}_raw",
            "source": {
                "type": "cloudfiles",
                "path": "${landing_path}/" + pipeline,
                "format": "json",
            },
        },
        {
            "name": f"write_{pipeline}",
            "type": "write",
            "source": f"v_{pipeline}_raw",
            "write_target": {
                "type": "streaming_table",
                "catalog": "${catalog}",
                "schema": "${bronze_schema}",
                "table": f"{pipeline}_table",
                "create_table": True,
            },
        },
    ]
    if with_test:
        actions.append(
            {
                "name": f"tst_{pipeline}",
                "type": "test",
                "test_type": "uniqueness",
                "source": f"v_{pipeline}_raw",
                "target": f"tst_{pipeline}",
                "columns": ["id"],
                "on_violation": "warn",
                "test_id": "SIT-01",
            }
        )
    with open(pdir / f"{pipeline}_fg.yaml", "w") as f:
        yaml.dump(
            {"pipeline": pipeline, "flowgroup": f"{pipeline}_fg", "actions": actions}, f
        )


def _project(tmp_path: Path) -> Path:
    """Write a minimal two-pipeline project; ``p_two`` carries a test action.

    A ``test_reporting`` block + publisher module are present so that
    ``include_tests=True`` genuinely produces the per-pipeline test-reporting
    hook (hook generation requires BOTH ``include_tests`` and a configured
    ``test_reporting``)."""
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions", "py_functions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text(
        "name: plan_stream\n"
        "version: '1.0'\n"
        "test_reporting:\n"
        "  module_path: py_functions/publisher.py\n"
        "  function_name: publish_results\n"
    )
    (project_root / "py_functions" / "publisher.py").write_text(
        "def publish_results(results, config, context, spark):\n"
        "    return {'published': len(results), 'failed': 0, 'errors': []}\n"
    )
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(
            {
                "dev": {
                    "catalog": "dev_catalog",
                    "bronze_schema": "bronze",
                    "landing_path": "/mnt/dev/landing",
                }
            },
            f,
        )
    _write_pipeline(project_root, "p_one", with_test=False)
    _write_pipeline(project_root, "p_two", with_test=True)
    return project_root


@pytest.fixture
def facade_in(tmp_path: Path):
    """Write the project, chdir in, yield (facade, project_root)."""
    project_root = _project(tmp_path)
    original_cwd = os.getcwd()
    os.chdir(project_root)
    try:
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        yield facade, project_root
    finally:
        os.chdir(original_cwd)


def _assert_phase_pairs(events: list) -> None:
    """Every PhaseStarted is matched by a PhaseCompleted of the same phase, in
    discover → preflight → generate order."""
    started = [e.phase for e in events if isinstance(e, PhaseStarted)]
    completed = [e.phase for e in events if isinstance(e, PhaseCompleted)]
    assert started == completed, (
        f"PhaseStarted sequence {started} != PhaseCompleted sequence {completed}"
    )
    assert started == ["discover", "preflight", "generate"]


def _assert_no_dangling_pipeline_starts(events: list) -> None:
    """Each PipelineStarted is IMMEDIATELY followed by exactly one terminal for
    the same pipeline, and the global count invariant holds."""
    starts = [e for e in events if isinstance(e, PipelineStarted)]
    completed = [e for e in events if isinstance(e, PipelineCompleted)]
    failed = [e for e in events if isinstance(e, PipelineFailed)]
    assert len(starts) == len(completed) + len(failed)
    i = 0
    while i < len(events):
        ev = events[i]
        if isinstance(ev, PipelineStarted):
            assert i + 1 < len(events), "PipelineStarted with no following event"
            terminal = events[i + 1]
            assert isinstance(terminal, (PipelineCompleted, PipelineFailed)), (
                f"PipelineStarted({ev.pipeline}) not immediately followed by a "
                f"terminal; got {type(terminal).__name__}"
            )
            assert terminal.pipeline == ev.pipeline
            i += 2
            continue
        i += 1


@pytest.mark.integration
class TestPlanGenerationStream:
    def test_unfiltered_call_yields_empty_plan_with_full_frame(self, facade_in):
        """``plan_generation``'s only narrowing knob is ``pipeline_filter``; a
        ``None`` filter plans nothing (the primitive yields no pipeline), but
        the full §5.7 phase frame + an EMPTY terminal plan are still emitted."""
        facade, project_root = facade_in
        events = list(facade.generation.plan_generation("dev", pipeline_filter=None))

        assert isinstance(events[0], OperationStarted)
        _assert_phase_pairs(events)
        # No pipelines planned → no per-pipeline events.
        assert not any(isinstance(e, PipelineStarted) for e in events)
        # Terminal still present, carrying an empty-but-valid plan.
        assert isinstance(events[-1], GenerationPlanCompleted)
        plan = events[-1].response
        assert isinstance(plan, GenerationPlan)
        assert plan.files == ()
        assert plan.file_count == 0
        assert plan.pipeline_count == 0
        assert plan.output_location == project_root / "generated" / "dev"

    def test_filtered_stream_shape_and_terminal_plan(self, facade_in):
        facade, project_root = facade_in
        events = list(facade.generation.plan_generation("dev", pipeline_filter="p_one"))

        assert isinstance(events[0], OperationStarted)
        assert events[0].operation_name == "plan_generation"
        assert events[0].env == "dev"

        _assert_phase_pairs(events)

        _assert_no_dangling_pipeline_starts(events)
        completed = [e for e in events if isinstance(e, PipelineCompleted)]
        assert [e.pipeline for e in completed] == ["p_one"]
        assert not any(isinstance(e, PipelineFailed) for e in events)

        assert isinstance(events[-1], GenerationPlanCompleted)
        plan = events[-1].response
        assert isinstance(plan, GenerationPlan)

        assert len(plan.files) >= 1
        assert plan.file_count == len(plan.files)
        assert plan.pipeline_count == 1
        assert plan.output_location == project_root / "generated" / "dev"
        # The flowgroup module is planned with byte content + correct pipeline.
        flowgroup_files = [f for f in plan.files if f.kind == "flowgroup"]
        assert any(f.path.name == "p_one_fg.py" for f in flowgroup_files)
        assert all(f.pipeline == "p_one" for f in plan.files)
        assert all(isinstance(f.content, str) and f.content for f in plan.files)

    def test_real_output_dir_untouched(self, facade_in):
        """``plan_generation`` writes NOTHING to generated/<env>."""
        facade, project_root = facade_in
        generated = project_root / "generated" / "dev"

        plan = collect_response(
            facade.generation.plan_generation("dev", pipeline_filter="p_one")
        )
        assert isinstance(plan, GenerationPlan)
        assert len(plan.files) >= 1  # the plan is non-empty

        # Nothing was written to the real target: the directory does not exist
        # (or, defensively, contains no files).
        if generated.exists():
            assert sorted(generated.rglob("*")) == []
        else:
            assert not generated.exists()
        # And the plan still REPORTS that real directory as output_location.
        assert plan.output_location == generated

    def test_pipeline_filter_narrows_plan(self, facade_in):
        """``pipeline_filter`` restricts the plan to the named pipeline only."""
        facade, _ = facade_in
        plan_one = collect_response(
            facade.generation.plan_generation("dev", pipeline_filter="p_one")
        )
        plan_two = collect_response(
            facade.generation.plan_generation("dev", pipeline_filter="p_two")
        )
        assert {f.pipeline for f in plan_one.files} == {"p_one"}
        assert {f.pipeline for f in plan_two.files} == {"p_two"}
        assert plan_one.pipeline_count == 1
        assert plan_two.pipeline_count == 1

    def test_include_tests_adds_test_hook(self, facade_in):
        """``include_tests=True`` plans the per-pipeline test-reporting hook for
        the pipeline carrying a ``test`` action (a ``test_hook`` file that the
        ``include_tests=False`` plan does not produce)."""
        facade, _ = facade_in

        plan_no_tests = collect_response(
            facade.generation.plan_generation(
                "dev", pipeline_filter="p_two", include_tests=False
            )
        )
        plan_with_tests = collect_response(
            facade.generation.plan_generation(
                "dev", pipeline_filter="p_two", include_tests=True
            )
        )

        kinds_without = {f.kind for f in plan_no_tests.files}
        assert "test_hook" not in kinds_without

        test_hook_files = [f for f in plan_with_tests.files if f.kind == "test_hook"]
        assert test_hook_files, "include_tests=True should plan a test_hook file"
        assert all(f.pipeline == "p_two" for f in test_hook_files)
        # Honouring include_tests adds files, never drops them.
        assert plan_with_tests.file_count > plan_no_tests.file_count
