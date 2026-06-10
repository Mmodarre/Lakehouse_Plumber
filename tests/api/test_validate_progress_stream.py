"""§5.7 progress-stream invariants for ``ValidationFacade.validate_pipelines``.

Pins the ordering / pairing invariants on a REAL multi-pipeline project (no
mocks — the engine and the report-only fold are the production drivers):

1. :class:`OperationStarted` is the FIRST event (§5.7).
2. Every :class:`PhaseStarted` is paired with a :class:`PhaseCompleted` of the
   same ``phase`` (``discover`` / ``preflight`` / ``validate``), in that order.
3. Each pipeline emits a :class:`PipelineStarted` IMMEDIATELY followed by
   exactly ONE terminal — :class:`PipelineCompleted` (validated clean) or
   :class:`PipelineFailed` (validation errors). No dangling Starts.
4. The load-bearing invariant
   ``count(PipelineStarted) == count(PipelineCompleted) + count(PipelineFailed)``
   holds with EQUALITY across the whole batch — validate REPORTS (there is no
   all-or-nothing gate), so EVERY pipeline yields a Started+terminal pair, even
   when a sibling pipeline has errors (unlike generate, whose abort path
   suppresses the non-failing pipelines' events).
5. The terminal :class:`ValidationCompleted` is LAST and carries a
   :class:`BatchValidationResponse` (success ``False`` when any pipeline had
   findings — REPORTED, not raised).

Tests import strictly from :mod:`lhp.api` — no internal modules.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lhp.api import (
    BatchValidationResponse,
    LakehousePlumberApplicationFacade,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    ValidationCompleted,
    collect_response,
)


def _write_pipeline(
    project_root: Path, pipeline: str, table: str, *, dup: bool
) -> None:
    """Write one flowgroup. When ``dup`` it declares two ``create_table`` writes
    to the SAME table — a per-flowgroup ``LHP-CFG-004`` validate REPORTS as a
    per-pipeline finding (no raise)."""
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
    project_root = tmp_path / "proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text("name: validate_progress\nversion: '1.0'\n")
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
    created: list[str] = []

    def _build(pipelines: dict[str, bool]):
        project_root = _project(tmp_path, pipelines)
        created.append(os.getcwd())
        os.chdir(project_root)
        return LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )

    yield _build
    if created:
        os.chdir(created[0])


def _assert_phase_pairs(events: list) -> None:
    """Every PhaseStarted is matched by a PhaseCompleted of the same phase, in
    discover → preflight → validate order."""
    started = [e.phase for e in events if isinstance(e, PhaseStarted)]
    completed = [e.phase for e in events if isinstance(e, PhaseCompleted)]
    assert started == completed, (
        f"PhaseStarted sequence {started} != PhaseCompleted sequence {completed}"
    )
    assert started[:2] == ["discover", "preflight"]


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
class TestValidateProgressStream:
    def test_full_stream_ordering_and_pairing_all_clean(self, facade_in):
        facade = facade_in({"p_one": False, "p_two": False})

        events = list(
            facade.validation.validate_pipelines(
                pipeline_fields=["p_one", "p_two"],
                env="dev",
            )
        )

        # 1. OperationStarted is first.
        assert isinstance(events[0], OperationStarted)
        assert events[0].operation_name == "validate_pipelines"
        assert events[0].env == "dev"

        # 2. Paired phases, discover → preflight → validate.
        _assert_phase_pairs(events)
        assert [e.phase for e in events if isinstance(e, PhaseStarted)] == [
            "discover",
            "preflight",
            "validate",
        ]

        # 3 + 4. Per-pipeline pairing + count invariant, one Completed each.
        _assert_no_dangling_pipeline_starts(events)
        completed = [e for e in events if isinstance(e, PipelineCompleted)]
        assert sorted(e.pipeline for e in completed) == ["p_one", "p_two"]
        assert all(e.files_written == 0 for e in completed)  # validate writes nothing
        assert not any(isinstance(e, PipelineFailed) for e in events)

        # 5. Terminal ValidationCompleted is LAST and carries a success batch.
        assert isinstance(events[-1], ValidationCompleted)
        assert isinstance(events[-1].response, BatchValidationResponse)
        assert events[-1].response.success is True

    def test_failing_pipeline_pairs_failed_and_still_reports(self, facade_in):
        """A pipeline with validation errors emits PipelineStarted +
        PipelineFailed (paired), the clean sibling still emits Started +
        Completed, and the run REPORTS — the terminal ValidationCompleted
        carries a non-zero-exit batch, NOT a raise. The count invariant holds
        with equality (every pipeline pairs)."""
        facade = facade_in({"ok_p": False, "bad_p": True})

        events = list(
            facade.validation.validate_pipelines(
                pipeline_fields=["ok_p", "bad_p"],
                env="dev",
            )
        )

        assert isinstance(events[0], OperationStarted)

        # Both phases + the validate phase all completed (no abort/raise).
        _assert_phase_pairs(events)
        assert [e.phase for e in events if isinstance(e, PhaseStarted)] == [
            "discover",
            "preflight",
            "validate",
        ]

        # No dangling Starts; EVERY pipeline pairs (validate REPORTS), so the
        # count invariant holds with equality across the whole batch.
        _assert_no_dangling_pipeline_starts(events)
        starts = [e for e in events if isinstance(e, PipelineStarted)]
        assert sorted(e.pipeline for e in starts) == ["bad_p", "ok_p"]

        completed = [e for e in events if isinstance(e, PipelineCompleted)]
        failed = [e for e in events if isinstance(e, PipelineFailed)]
        assert [e.pipeline for e in completed] == ["ok_p"]
        assert [e.pipeline for e in failed] == ["bad_p"]
        # The failure event carries an LHP code + message (no live exception).
        assert failed[0].code.startswith("LHP-")
        assert failed[0].message

        # Terminal still emits (validate REPORTS): ValidationCompleted LAST,
        # carrying a non-zero-exit batch. No raise.
        assert isinstance(events[-1], ValidationCompleted)
        assert isinstance(events[-1].response, BatchValidationResponse)
        assert events[-1].response.success is False

    def test_collect_response_returns_batch_on_findings(self, facade_in):
        """``collect_response`` walks the report-only stream and returns the
        terminal failed batch DTO (validate never raises on findings)."""
        facade = facade_in({"ok_p": False, "bad_p": True})
        response = collect_response(
            facade.validation.validate_pipelines(
                pipeline_fields=["ok_p", "bad_p"],
                env="dev",
            )
        )
        assert isinstance(response, BatchValidationResponse)
        assert response.success is False

    def test_discover_phase_performs_discovery_in_facade(self, facade_in):
        """With NO pre-discovered set, the discover phase performs project-wide
        discovery via ``bootstrap.discover_all_flowgroups``. The discover phase
        pair is emitted and discovery is actually invoked."""
        facade = facade_in({"p_one": False, "p_two": False})

        # Spy on the existing project-wide discovery surface the facade now
        # drives inside the discover phase (no orchestrator edit — we wrap the
        # method it already calls).
        bootstrap = facade._orchestrator.bootstrap
        original = bootstrap.discover_all_flowgroups
        calls: list[int] = []

        def _spy():
            calls.append(1)
            return original()

        bootstrap.discover_all_flowgroups = _spy  # type: ignore[method-assign]
        try:
            events = list(
                facade.validation.validate_pipelines(
                    pipeline_fields=["p_one", "p_two"],
                    env="dev",
                )
            )
        finally:
            bootstrap.discover_all_flowgroups = original  # type: ignore[method-assign]

        # The facade drove discovery in-stream (the discover phase is no longer
        # a no-op placeholder). Memoization means the FS is scanned once even
        # though preflight + the worklist builder also consume the set.
        assert calls, "facade did not perform discovery in the discover phase"
        # Discover phase is still a paired Start/Complete, first in order.
        _assert_phase_pairs(events)
        assert [e.phase for e in events if isinstance(e, PhaseStarted)] == [
            "discover",
            "preflight",
            "validate",
        ]
        assert isinstance(events[-1], ValidationCompleted)
        assert events[-1].response.success is True

    def test_pre_discovered_set_adopted_verbatim_no_double_discovery(self, facade_in):
        """A caller-supplied ``pre_discovered_all_flowgroups`` is adopted VERBATIM
        — the facade does NOT re-discover (no double scan) — and the stream is
        identical (discover/preflight/validate phases, paired per-pipeline
        terminals, terminal ``ValidationCompleted``)."""
        facade = facade_in({"p_one": False, "p_two": False})

        # Resolve the set once up front, then hand it back in as the
        # pre-discovered path.
        pre = list(facade._orchestrator.bootstrap.discover_all_flowgroups())

        # Spy AFTER the up-front resolve: any further call would be the facade
        # re-discovering, which the pre-discovered path must avoid.
        bootstrap = facade._orchestrator.bootstrap
        original = bootstrap.discover_all_flowgroups
        calls: list[int] = []

        def _spy():
            calls.append(1)
            return original()

        bootstrap.discover_all_flowgroups = _spy  # type: ignore[method-assign]
        try:
            events = list(
                facade.validation.validate_pipelines(
                    pipeline_fields=["p_one", "p_two"],
                    env="dev",
                    pre_discovered_all_flowgroups=pre,
                )
            )
        finally:
            bootstrap.discover_all_flowgroups = original  # type: ignore[method-assign]

        # The facade adopted the supplied set verbatim — it did NOT call the
        # discovery surface again (memoization aside, the pre-discovered branch
        # must not even reach it).
        assert calls == [], "facade re-discovered despite a supplied pre_discovered set"
        _assert_phase_pairs(events)
        assert [e.phase for e in events if isinstance(e, PhaseStarted)] == [
            "discover",
            "preflight",
            "validate",
        ]
        _assert_no_dangling_pipeline_starts(events)
        completed = [e for e in events if isinstance(e, PipelineCompleted)]
        assert sorted(e.pipeline for e in completed) == ["p_one", "p_two"]
        assert isinstance(events[-1], ValidationCompleted)
        assert events[-1].response.success is True
