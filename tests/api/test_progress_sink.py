"""Contract tests for :class:`lhp.api.ProgressSink` wired through the facade.

``ProgressSink`` is a concrete, mutable counter (NOT a ``Protocol`` / ABC —
§3.6, §13.8) the long-running generate / validate streams advance as flowgroups
complete. The core pool fires ``on_total`` ONCE with the flat worklist length
(the FLOWGROUP count, not the pipeline count) and ``on_flowgroup_done`` once per
flowgroup, which the facade adapts to ``ProgressSink.on_total`` /
``ProgressSink.on_advance``.

These tests run on a REAL project over the production pool (no mocks), mirroring
``tests/api/test_generate_progress_stream.py``. They assert:

1. A passed sink ends with ``done == <flowgroup count>`` and ``total`` set
   exactly once to that same count, on BOTH generate and validate.
2. The run completes normally when a sink is supplied (a terminal response DTO
   is produced).
3. A sink whose methods are overridden to no-ops does not break the run — the
   stream survives regardless of what the sink's hooks do.

Tests import strictly from :mod:`lhp.api` — no internal modules.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest
import yaml

from lhp.api import (
    BatchGenerationResponse,
    BatchValidationResponse,
    GenerationCompleted,
    GenerationPlanCompleted,
    LakehousePlumberApplicationFacade,
    ProgressSink,
    ValidationCompleted,
    collect_response,
)

# Real, many-flowgroup project copied per-test (never mutated in place) so the
# plan path drives the production pool over a realistic worklist (§8.5).
_E2E_FIXTURE = (
    Path(__file__).resolve().parents[1] / "e2e" / "fixtures" / "testing_project"
)


def _write_pipeline(project_root: Path, pipeline: str, table: str) -> None:
    """Write one single-flowgroup pipeline (one flowgroup => one pool unit)."""
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
                "table": table,
                "create_table": True,
            },
        },
    ]
    with open(pdir / f"{pipeline}_fg.yaml", "w") as f:
        yaml.dump(
            {"pipeline": pipeline, "flowgroup": f"{pipeline}_fg", "actions": actions}, f
        )


def _project(tmp_path: Path, pipelines: list[str]) -> Path:
    """Write a minimal project with one single-flowgroup pipeline per name."""
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text("name: progress_sink\nversion: '1.0'\n")
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
    for name in pipelines:
        _write_pipeline(project_root, name, f"{name}_table")
    return project_root


@pytest.fixture
def facade_in(tmp_path: Path):
    """Return a builder that writes a project and yields (facade, output_dir)."""
    cwd: list[str] = []

    def _build(pipelines: list[str]):
        project_root = _project(tmp_path, pipelines)
        output_dir = project_root / "generated" / "dev"
        cwd.append(os.getcwd())
        os.chdir(project_root)
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        return facade, output_dir

    yield _build
    if cwd:
        os.chdir(cwd[0])


class _CountingSink(ProgressSink):
    """A ProgressSink that records how many times ``on_total`` was called.

    Lets the test assert ``total`` is set EXACTLY ONCE (not merely "ends at the
    right value") while still being a real ``ProgressSink`` for the facade.
    """

    def __init__(self) -> None:
        super().__init__()
        self.total_calls = 0

    def on_total(self, n: int) -> None:
        self.total_calls += 1
        super().on_total(n)


class _NoOpSink(ProgressSink):
    """A sink whose hooks are overridden to do NOTHING.

    Proves the run survives an arbitrary sink implementation: the stream must
    not depend on the sink mutating any state.
    """

    def on_total(self, n: int) -> None:  # intentional no-op
        pass

    def on_advance(self) -> None:
        pass


@pytest.mark.integration
class TestProgressSinkUnit:
    """Pure-counter behaviour, no facade — the load-bearing scalar contract."""

    def test_counters_start_at_zero(self) -> None:
        sink = ProgressSink()
        assert sink.total == 0
        assert sink.done == 0

    def test_on_total_sets_and_on_advance_increments(self) -> None:
        sink = ProgressSink()
        sink.on_total(3)
        sink.on_advance()
        sink.on_advance()
        assert sink.total == 3
        assert sink.done == 2

    def test_hooks_never_raise_on_edge_inputs(self) -> None:
        """The hooks run inside the coordinator loop and MUST NOT raise."""
        sink = ProgressSink()
        # Negative / zero must not raise (trivial assignment, no validation).
        sink.on_total(-1)
        sink.on_total(0)
        for _ in range(5):
            sink.on_advance()
        assert sink.total == 0
        assert sink.done == 5


@pytest.mark.integration
class TestProgressSinkGenerate:
    def test_sink_counts_flowgroups_on_generate(self, facade_in) -> None:
        """Two single-flowgroup pipelines => two pool units => done == 2,
        total set exactly once to 2; the run completes normally."""
        facade, output_dir = facade_in(["p_one", "p_two"])
        sink = _CountingSink()

        response = collect_response(
            facade.generate_pipelines(
                pipeline_fields=["p_one", "p_two"],
                env="dev",
                output_dir=output_dir,
                progress=sink,
            )
        )

        assert sink.total == 2
        assert sink.done == 2
        # Set EXACTLY once (not re-set per pipeline / per flowgroup).
        assert sink.total_calls == 1
        # Run completed normally with a terminal response DTO.
        assert isinstance(response, BatchGenerationResponse)
        assert response.success is True

    def test_run_survives_noop_sink_on_generate(self, facade_in) -> None:
        """A sink whose hooks do nothing must not break the stream."""
        facade, output_dir = facade_in(["solo"])
        sink = _NoOpSink()

        events = list(
            facade.generate_pipelines(
                pipeline_fields=["solo"],
                env="dev",
                output_dir=output_dir,
                progress=sink,
            )
        )

        # The no-op sink never mutated its counters, yet the run reached its
        # terminal cleanly.
        assert sink.total == 0
        assert sink.done == 0
        assert isinstance(events[-1], GenerationCompleted)
        assert events[-1].response.success is True

    def test_generate_without_sink_is_unchanged(self, facade_in) -> None:
        """``progress=None`` (the default) wires no callbacks and still runs."""
        facade, output_dir = facade_in(["only"])
        response = collect_response(
            facade.generate_pipelines(
                pipeline_fields=["only"], env="dev", output_dir=output_dir
            )
        )
        assert isinstance(response, BatchGenerationResponse)
        assert response.success is True


@pytest.mark.integration
class TestProgressSinkValidate:
    def test_sink_counts_flowgroups_on_validate(self, facade_in) -> None:
        """Validate threads the same flowgroup-grained progress as generate."""
        facade, _ = facade_in(["v_one", "v_two", "v_three"])
        sink = _CountingSink()

        response = collect_response(
            facade.validate_pipelines(
                pipeline_fields=["v_one", "v_two", "v_three"],
                env="dev",
                progress=sink,
            )
        )

        assert sink.total == 3
        assert sink.done == 3
        assert sink.total_calls == 1
        assert isinstance(response, BatchValidationResponse)

    def test_run_survives_noop_sink_on_validate(self, facade_in) -> None:
        facade, _ = facade_in(["v_solo"])
        sink = _NoOpSink()

        events = list(
            facade.validate_pipelines(
                pipeline_fields=["v_solo"], env="dev", progress=sink
            )
        )

        assert sink.total == 0
        assert sink.done == 0
        assert isinstance(events[-1], ValidationCompleted)


@pytest.mark.integration
@pytest.mark.slow
class TestProgressSinkPlan:
    """The plan/diff path drives the SAME per-flowgroup ProgressSink as generate.

    ``plan_generation`` forwards ``ProgressSink.on_total`` / ``.on_advance`` as
    plain callables into ``build_generation_plan`` →
    ``orchestrator.generate_pipelines`` (the identical flat per-flowgroup pool
    the real generate drives, just against a throwaway temp dir). So a plan over
    the whole project counts flowgroups exactly as a generate does. Run over an
    isolated temp COPY of the e2e fixture (never mutate the fixture); the plan
    writes only to its own discarded temp tree, so the project's
    ``generated/<env>`` is untouched.
    """

    def _project_copy(self, tmp_path: Path) -> Path:
        dest = tmp_path / "plan_project"
        shutil.copytree(_E2E_FIXTURE, dest)
        return dest

    def test_plan_drives_sink_over_whole_project(self, tmp_path: Path) -> None:
        """No filter → W1 auto-derives the full project worklist; the sink ends
        with ``done == total`` (every discovered flowgroup advanced), ``total``
        set EXACTLY once, and a terminal ``GenerationPlanCompleted``."""
        project_root = self._project_copy(tmp_path)
        cwd = os.getcwd()
        os.chdir(project_root)
        try:
            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )
            sink = _CountingSink()
            events = list(facade.generation.plan_generation(env="dev", progress=sink))
        finally:
            os.chdir(cwd)

        # Set EXACTLY once (the §5.7 on_total contract: once with the flat
        # worklist length, the FLOWGROUP count, not the pipeline count).
        assert sink.total_calls == 1
        # The whole-project fixture has many flowgroups; the sink advanced once
        # per flowgroup, ending exactly at the announced total.
        assert sink.total > 1
        assert sink.done == sink.total
        # A plan is byte-faithful but read-only: it terminates with the plan DTO,
        # and the project's real generated/<env> tree was never written.
        assert isinstance(events[-1], GenerationPlanCompleted)
        assert not (project_root / "generated").exists()

    def test_plan_without_sink_is_unchanged(self, tmp_path: Path) -> None:
        """``progress=None`` (the default) wires no callbacks and still plans."""
        project_root = self._project_copy(tmp_path)
        cwd = os.getcwd()
        os.chdir(project_root)
        try:
            facade = LakehousePlumberApplicationFacade.for_project(
                project_root, enforce_version=False
            )
            events = list(facade.generation.plan_generation(env="dev"))
        finally:
            os.chdir(cwd)

        assert isinstance(events[-1], GenerationPlanCompleted)
        assert not (project_root / "generated").exists()
