"""Unit tests for the validate fold in :mod:`lhp.core.coordination.executor`.

:meth:`PipelineExecutionService.run_validate` drives the consolidated flat
engine (:func:`._pool._run_flowgroup_pool_core`, ``mode="validate"``)
and folds the per-pipeline results via
:func:`._pool.assemble_validate_outcomes`. The cross-flowgroup
barrier — table-creation conflicts (``LHP-VAL-009`` / ``LHP-CFG-004``) and CDC
fan-in (``LHP-VAL-010``) — runs INSIDE the engine on the RESOLVED flowgroups
the workers returned (§9.24 closure), and its structured
:class:`~lhp.errors.LHPError`s land on the outcome's ``issues`` tuple as
:class:`~lhp.models.processing.ValidationIssueRecord`s (each carrying the live
LHPError, not a stringified projection) via
:func:`._cross_flowgroup_issues.build_cross_flowgroup_issues` (§9.24:
single-sourced construction; the engine only surfaces). Carrying the live
error lets the validate CLI render the structured panel (with ``code`` /
``suggestions``) instead of the legacy ``=====`` border.

The spawn pool is removed the same way
``test_flowgroup_pool`` does it — a synchronous in-process executor + a fake
worker injected on the engine module (``_pool``) — so the system under test
stays the real ``assemble_validate_outcomes`` fold plus
the real cross-fg barrier, not the subprocess mechanics.
"""

from __future__ import annotations

from concurrent.futures import Future
from typing import List, Sequence
from unittest.mock import MagicMock

import pytest

from lhp.core._interfaces import CrossFlowgroupCheckResult
from lhp.core.coordination import _pool as fe
from lhp.core.coordination._flowgroup_pool import _FlowgroupWorkerState
from lhp.core.coordination.executor import PipelineExecutionService
from lhp.errors import LHPError
from lhp.models import FlowGroupContext
from lhp.models.processing import FlowgroupOutcome


class _FakeFlowGroup:
    """Minimal FlowGroup stand-in for the validate fold unit test.

    The fold only needs the resolved flowgroups to be forwarded to the
    (mocked) ``validate_cross_flowgroup`` boundary, so their internal
    shape is irrelevant — only object identity matters (the barrier-spy
    assertions key on it).
    """

    def __init__(self, pipeline: str, flowgroup: str) -> None:
        self.pipeline = pipeline
        self.flowgroup = flowgroup


def _ctx(pipeline: str, flowgroup: str) -> FlowGroupContext:
    return FlowGroupContext(
        flowgroup=_FakeFlowGroup(pipeline, flowgroup), source_yaml=None
    )


class _SyncExecutor:
    """Synchronous stand-in for ``ProcessPoolExecutor`` (in-process).

    Runs each submitted callable eagerly on ``submit`` and returns a real,
    already-completed :class:`concurrent.futures.Future`, keeping the
    engine's ``executor.submit(...)`` / ``as_completed(...)`` / ``fut.result()``
    call shape intact while removing the spawn boundary. Accepts and ignores
    the ``max_workers`` / ``mp_context`` / ``initializer`` / ``initargs``
    kwargs the engine passes — the faked worker fn reads no module global.
    """

    def __init__(self, *args, **kwargs) -> None:
        self.submitted: List[tuple] = []

    def __enter__(self) -> "_SyncExecutor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def submit(self, fn, /, *args, **kwargs) -> Future:
        self.submitted.append((args, kwargs))
        fut: Future = Future()
        try:
            fut.set_result(fn(*args, **kwargs))
        except Exception as exc:  # pragma: no cover - fake worker never raises
            fut.set_exception(exc)
        return fut


@pytest.mark.unit
def test_assemble_folds_table_creation_errors_into_issue_records(monkeypatch):
    """Table-creation errors fold into ``issues`` as a structured ``LHP-VAL-009``.

    Migrated from the deleted pipeline-batched validate-pool path. Drives the real
    :meth:`PipelineExecutionService.run_validate` (typed kwargs signature)
    through the consolidated engine, with two seams swapped to drop the
    spawn boundary (the ``test_flowgroup_pool`` pattern):

      * ``ProcessPoolExecutor`` -> a synchronous in-process executor;
      * ``_process_one_flowgroup`` -> a fake validate-mode worker returning
        an ``ok`` :class:`FlowgroupOutcome` carrying a resolved flowgroup.

    Everything else is production code: the engine's per-pipeline bucketing,
    the cross-flowgroup barrier on the RESOLVED set, and the
    :func:`assemble_validate_outcomes` fold.

    Arrange: ``ValidationService.validate_cross_flowgroup`` (the only external
    boundary, per §8.8) returns a :class:`CrossFlowgroupCheckResult` with a
    non-empty ``table_creation_errors`` list — the system under test is the
    fold + barrier surfacing, not the subprocess pool mechanics (separately
    covered by ``test_flowgroup_pool``).

    Assert: the per-pipeline outcome is unsuccessful AND carries a structured
    ``LHP-VAL-009`` on ``issues`` (a ValidationIssueRecord whose ``issue`` is
    the live LHPError, not a string), with no owning-flowgroup attribution
    (cross-fg), and the boundary was invoked once with the resolved flowgroups
    + ``pipeline_filter``.
    """
    pipeline_name = "bronze"
    resolved = _FakeFlowGroup(pipeline_name, "bronze_fg")

    # The single external boundary we mock (§8.8): the real barrier consumes
    # this result through ``build_cross_flowgroup_issues`` -> ``LHP-VAL-009``.
    cross_result = CrossFlowgroupCheckResult(
        table_creation_errors=[
            "Table 'cat.sch.tbl' has 2 actions with create_table: true",
        ],
        cdc_fanin_errors=[],
    )
    validation_service = MagicMock()
    validation_service.validate_cross_flowgroup = MagicMock(return_value=cross_result)

    def _fake_worker(fg_ctx: FlowGroupContext, *, mode: str) -> FlowgroupOutcome:
        # Mirror the real validate-mode worker: ok outcome carrying the
        # RESOLVED flowgroup (the barrier runs on it), no formatted code.
        assert mode == "validate"
        return FlowgroupOutcome.ok(
            fg_ctx.flowgroup.pipeline,
            fg_ctx.flowgroup.flowgroup,
            resolved_flowgroup=resolved,
        )

    # Swap the two engine seams: spawn pool -> synchronous executor, worker
    # entry -> the in-process fake above (both referenced by module-level name).
    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(fe, "_process_one_flowgroup", _fake_worker)

    # Worker-pool collaborators are never exercised (the fake worker reads no
    # state); a minimal real state keeps the dataclass type honest.
    worker_state = _FlowgroupWorkerState(
        processor=MagicMock(),
        substitution_managers={},
        include_tests=False,
        code_generator=MagicMock(),
        pipeline_output_dirs={},
        environment="dev",
    )

    service = PipelineExecutionService(
        validate_worker_state=worker_state,
        validation_service=validation_service,
    )

    # ``run_validate`` is itself the outcome generator (E4 removed the
    # callback-firing drain adapter); drain it to a list.
    outcomes = list(
        service.run_validate(
            flowgroups_by_pipeline={pipeline_name: [_ctx(pipeline_name, "bronze_fg")]},
            substitution_managers={pipeline_name: object()},
            output_dirs={pipeline_name: None},
            discovery_errors={},
        )
    )

    # Exactly one outcome yielded for the single pipeline.
    assert len(outcomes) == 1
    outcome = outcomes[0]

    # The outcome fails and the structured VAL-009 lands on ``issues`` as a
    # ValidationIssueRecord carrying the LIVE LHPError (not a stringified
    # projection) at error severity.
    assert outcome.pipeline == pipeline_name
    assert outcome.success is False
    val_009 = [
        r
        for r in outcome.issues
        if isinstance(r.issue, LHPError) and r.issue.code == "LHP-VAL-009"
    ]
    assert len(val_009) == 1, outcome.issues
    assert val_009[0].severity == "error"
    # No finding degraded to a plain string — the cross-fg issue is structured.
    assert not [r for r in outcome.issues if isinstance(r.issue, str)]
    # A cross-flowgroup fan-in finding has no single owning flowgroup, so it
    # carries no flowgroup attribution / source file.
    assert val_009[0].flowgroup_name is None
    assert val_009[0].source_file is None

    # The boundary was actually invoked with the RESOLVED flowgroups,
    # pipeline-filtered (the §9.24 closure runs on the resolved set).
    validation_service.validate_cross_flowgroup.assert_called_once()
    call = validation_service.validate_cross_flowgroup.call_args
    assert call.kwargs.get("pipeline_filter") == pipeline_name
    passed_flowgroups: Sequence[object] = call.args[0]
    assert [id(fg) for fg in passed_flowgroups] == [id(resolved)]
