"""§5.7 progress-stream invariants for ``GenerationFacade.generate_pipelines``.

Pins the ordering / pairing invariants of the §5.7 event stream on a REAL
multi-pipeline project (no mocks — the engine, gate and commit are the
production drivers; the spawn pool is the real ``ProcessPoolExecutor``):

1. :class:`OperationStarted` is the FIRST event (§5.7).
2. Every :class:`PhaseStarted` is paired with a :class:`PhaseCompleted` of the
   same ``phase`` (``discover`` / ``preflight`` / ``generate`` / ``monitoring``,
   plus ``bundle_sync`` when bundle is enabled), in that order.
3. Each pipeline emits a :class:`PipelineStarted` IMMEDIATELY followed by
   exactly ONE terminal — :class:`PipelineCompleted` (success) or
   :class:`PipelineFailed` (failure). No dangling Starts.
4. The load-bearing invariant
   ``count(PipelineStarted) == count(PipelineCompleted) + count(PipelineFailed)``
   holds on BOTH the success path and the gate-abort path.
5. On success the terminal :class:`GenerationCompleted` is LAST and the files
   are actually written to disk.

Tests import strictly from :mod:`lhp.api` and :mod:`lhp.errors` — no internal
modules.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lhp.api import (
    BatchGenerationResponse,
    GenerationCompleted,
    LakehousePlumberApplicationFacade,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    collect_response,
)
from lhp.errors import LHPError


def _write_pipeline(
    project_root: Path, pipeline: str, table: str, *, dup: bool
) -> None:
    """Write one flowgroup. When ``dup`` it declares two ``create_table`` writes
    to the SAME table — a per-flowgroup ``LHP-CFG-004`` the gate aggregates."""
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
            "name": f"write_{pipeline}_a",
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
    if dup:
        actions.append(
            {
                "name": f"write_{pipeline}_b",
                "type": "write",
                "source": f"v_{pipeline}_raw",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": table,
                    "create_table": True,
                },
            }
        )
    with open(pdir / f"{pipeline}_fg.yaml", "w") as f:
        yaml.dump(
            {"pipeline": pipeline, "flowgroup": f"{pipeline}_fg", "actions": actions}, f
        )


def _project(tmp_path: Path, pipelines: dict[str, bool]) -> Path:
    """Write a minimal project. ``pipelines`` maps pipeline name → ``dup`` flag."""
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text("name: progress_stream\nversion: '1.0'\n")
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
    for name, dup in pipelines.items():
        _write_pipeline(project_root, name, f"{name}_table", dup=dup)
    return project_root


@pytest.fixture
def facade_in(tmp_path: Path):
    """Return a builder that writes a project and yields (facade, output_dir)."""
    created: list[str] = []

    def _build(pipelines: dict[str, bool]):
        project_root = _project(tmp_path, pipelines)
        output_dir = project_root / "generated" / "dev"
        created.append(os.getcwd())
        os.chdir(project_root)
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        return facade, output_dir

    yield _build
    if created:
        os.chdir(created[0])


def _assert_phase_pairs(events: list) -> None:
    """Every PhaseStarted is matched by a PhaseCompleted of the same phase, in
    discover → preflight → generate order (only phases that started)."""
    started = [e.phase for e in events if isinstance(e, PhaseStarted)]
    completed = [e.phase for e in events if isinstance(e, PhaseCompleted)]
    assert started == completed, (
        f"PhaseStarted sequence {started} != PhaseCompleted sequence {completed}"
    )
    # Discover + preflight always run; on success generate also completes.
    assert started[:2] == ["discover", "preflight"]


def _assert_no_dangling_pipeline_starts(events: list) -> None:
    """Each PipelineStarted is IMMEDIATELY followed by exactly one terminal for
    the same pipeline, and the global count invariant holds."""
    starts = [e for e in events if isinstance(e, PipelineStarted)]
    completed = [e for e in events if isinstance(e, PipelineCompleted)]
    failed = [e for e in events if isinstance(e, PipelineFailed)]
    # Load-bearing invariant.
    assert len(starts) == len(completed) + len(failed)
    # Walk: every Start is immediately paired with a terminal for that pipeline.
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
class TestGenerateProgressStreamSuccess:
    def test_full_stream_ordering_and_pairing_on_success(self, facade_in):
        facade, output_dir = facade_in({"p_one": False, "p_two": False})

        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=["p_one", "p_two"],
                env="dev",
                output_dir=output_dir,
            )
        )

        assert isinstance(events[0], OperationStarted)
        assert events[0].operation_name == "generate_pipelines"
        assert events[0].env == "dev"

        # Paired phases. The full generate orchestration consolidates
        # discover → preflight → generate → format → monitoring into the one
        # stream. This is a DEFAULT run (``apply_formatting`` unset → resolves to
        # the project's ``lhp.yaml`` setting, ``True`` when absent), so the
        # ``format`` phase runs between generate and monitoring. The monitoring
        # phase always runs on a successful non-dry-run generate (no-op when
        # monitoring is not configured, as here); ``bundle_sync`` is absent
        # because this project is not bundle-enabled.
        _assert_phase_pairs(events)
        assert [e.phase for e in events if isinstance(e, PhaseStarted)] == [
            "discover",
            "preflight",
            "generate",
            "format",
            "monitoring",
        ]

        _assert_no_dangling_pipeline_starts(events)
        completed = [e for e in events if isinstance(e, PipelineCompleted)]
        assert sorted(e.pipeline for e in completed) == ["p_one", "p_two"]
        assert all(isinstance(e.files_written, int) for e in completed)
        assert not any(isinstance(e, PipelineFailed) for e in events)

        assert isinstance(events[-1], GenerationCompleted)
        assert isinstance(events[-1].response, BatchGenerationResponse)
        assert events[-1].response.success is True

        # Files actually written to disk.
        py_files = sorted(output_dir.rglob("*.py"))
        names = {p.name for p in py_files}
        assert "p_one_fg.py" in names
        assert "p_two_fg.py" in names

    def test_per_pipeline_started_precedes_its_own_terminal(self, facade_in):
        """A PipelineStarted is emitted only once its terminal is in hand, so
        Started always immediately precedes the SAME pipeline's terminal."""
        facade, output_dir = facade_in({"a_p": False, "b_p": False})
        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=["a_p", "b_p"], env="dev", output_dir=output_dir
            )
        )
        per_pipeline = [
            e
            for e in events
            if isinstance(e, (PipelineStarted, PipelineCompleted, PipelineFailed))
        ]
        assert len(per_pipeline) == 4  # 2 starts + 2 terminals
        assert isinstance(per_pipeline[0], PipelineStarted)
        assert isinstance(per_pipeline[1], PipelineCompleted)
        assert per_pipeline[0].pipeline == per_pipeline[1].pipeline
        assert isinstance(per_pipeline[2], PipelineStarted)
        assert isinstance(per_pipeline[3], PipelineCompleted)
        assert per_pipeline[2].pipeline == per_pipeline[3].pipeline

    def test_default_run_emits_format_phase(self, facade_in):
        """A DEFAULT run — ``apply_formatting`` UNSET (the tri-state ``None``) —
        EMITS a paired ``format`` phase, positioned AFTER ``generate`` and BEFORE
        ``monitoring``.

        This is the load-bearing tri-state regression: the format phase guards on
        the RESOLVED ``apply_formatting`` bool, not the raw ``None`` the caller
        threads. A project with no ``lhp.yaml`` ``apply_formatting`` key resolves
        to ``True``, so omitting the override MUST still surface the phase — if the
        guard ever regressed to truth-testing the raw ``None`` (falsy), the phase
        would silently vanish on every default generate.
        """
        facade, output_dir = facade_in({"p_one": False, "p_two": False})

        # No apply_formatting argument: the tri-state stays None all the way down.
        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=["p_one", "p_two"],
                env="dev",
                output_dir=output_dir,
            )
        )

        # Every PhaseStarted is still paired with its PhaseCompleted, and the
        # format phase now sits between generate and monitoring.
        _assert_phase_pairs(events)
        assert [e.phase for e in events if isinstance(e, PhaseStarted)] == [
            "discover",
            "preflight",
            "generate",
            "format",
            "monitoring",
        ]

        # The format phase completed successfully (a real ruff pass ran clean).
        format_completed = [
            e for e in events if isinstance(e, PhaseCompleted) and e.phase == "format"
        ]
        assert len(format_completed) == 1
        assert format_completed[0].success is True

        # Sanity: still a clean terminal and files on disk.
        assert isinstance(events[-1], GenerationCompleted)
        assert events[-1].response.success is True

    def test_no_format_override_skips_format_phase(self, facade_in):
        """``apply_formatting=False`` (the CLI ``--no-format`` path) SKIPS the
        format phase entirely — no ``PhaseStarted('format')`` / ``PhaseCompleted(
        'format')`` is emitted — while every other phase is unchanged. Pins the
        other arm of the resolved-bool guard.
        """
        facade, output_dir = facade_in({"p_one": False, "p_two": False})

        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=["p_one", "p_two"],
                env="dev",
                output_dir=output_dir,
                apply_formatting=False,
            )
        )

        _assert_phase_pairs(events)
        assert [e.phase for e in events if isinstance(e, PhaseStarted)] == [
            "discover",
            "preflight",
            "generate",
            "monitoring",
        ]
        assert not any(
            isinstance(e, (PhaseStarted, PhaseCompleted)) and e.phase == "format"
            for e in events
        )
        assert isinstance(events[-1], GenerationCompleted)
        assert events[-1].response.success is True


@pytest.mark.integration
class TestGenerateProgressStreamAbort:
    def test_gate_abort_pairs_failed_and_no_dangling_starts(self, facade_in):
        """On a gate failure the failing pipeline emits PipelineStarted +
        PipelineFailed (paired), the non-failing pipeline emits NO per-pipeline
        events, and the stream raises after a single trailing ErrorEmitted —
        the count invariant (Started == Completed + Failed) still holds."""
        facade, output_dir = facade_in({"ok_p": False, "bad_p": True})

        collected: list = []
        gen = facade.generation.generate_pipelines(
            pipeline_fields=["ok_p", "bad_p"],
            env="dev",
            output_dir=output_dir,
        )
        with pytest.raises(LHPError) as exc_info:
            for event in gen:
                collected.append(event)
        assert exc_info.value.code == "LHP-CFG-004"

        # First event is still OperationStarted.
        assert isinstance(collected[0], OperationStarted)

        # No-dangling-Starts holds on the abort path: the only per-pipeline
        # events are bad_p's Started + Failed (ok_p emitted nothing — it never
        # crystallised a delta because the gate raised before commit).
        _assert_no_dangling_pipeline_starts(collected)
        failed = [e for e in collected if isinstance(e, PipelineFailed)]
        assert [e.pipeline for e in failed] == ["bad_p"]
        assert failed[0].code == "LHP-CFG-004"
        starts = [e for e in collected if isinstance(e, PipelineStarted)]
        assert [e.pipeline for e in starts] == ["bad_p"]
        assert not any(isinstance(e, PipelineCompleted) for e in collected)

        # No generate-phase COMPLETION on the abort path (the raise closes the
        # stream per §1.4) and no terminal GenerationCompleted.
        completed_phases = [e.phase for e in collected if isinstance(e, PhaseCompleted)]
        assert "generate" not in completed_phases
        assert not any(isinstance(e, GenerationCompleted) for e in collected)

        # All-or-nothing: zero files written (gate precedes any commit/wipe).
        assert sorted(output_dir.rglob("*.py")) == []

    def test_collect_response_reraises_on_gate_abort(self, facade_in):
        facade, output_dir = facade_in({"ok_p": False, "bad_p": True})
        with pytest.raises(LHPError) as exc_info:
            collect_response(
                facade.generation.generate_pipelines(
                    pipeline_fields=["ok_p", "bad_p"],
                    env="dev",
                    output_dir=output_dir,
                )
            )
        assert exc_info.value.code == "LHP-CFG-004"
