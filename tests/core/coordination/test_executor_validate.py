"""Unit tests for the validate fold in :mod:`lhp.core.coordination.executor`.

The ``_assemble`` closure inside
:meth:`PipelineExecutionService.run_validate` folds BOTH cross-flowgroup
error families (table-creation -> ``LHP-VAL-009`` and CDC fan-in ->
``LHP-VAL-010``) into the structured ``lhp_errors`` tuple via
:func:`lhp.core.coordination._cross_flowgroup_issues.build_cross_flowgroup_issues`
(§9.24: single-sourced construction; this site only surfaces). Both
families flow into ``lhp_errors`` rather than the stringified ``errors``
projection, so the validate CLI renders the structured panel (with
``code`` / ``suggestions``) instead of the legacy ``=====`` border.
"""

from __future__ import annotations

from typing import List, Tuple
from unittest.mock import MagicMock

import pytest

from lhp.core._interfaces import CrossFlowgroupCheckResult
from lhp.core.coordination import executor as pe
from lhp.core.coordination.executor import (
    FlowgroupValidationResult,
    PipelineExecutionService,
    PipelineValidationOutcome,
)
from lhp.models import FlowGroupContext
from lhp.models.processing import PipelineWorkUnit


class _FakeFlowGroup:
    """Minimal FlowGroup stand-in for the validate fold unit test.

    The fold only needs ``flowgroups_by_pipeline`` to be non-empty so
    ``_assemble`` skips the empty-discovery short-circuit and reaches the
    cross-flowgroup fold. The flowgroup objects are forwarded to the
    (mocked) ``validate_cross_flowgroup`` boundary, so their internal
    shape is irrelevant.
    """

    def __init__(self, pipeline: str, flowgroup: str) -> None:
        self.pipeline = pipeline
        self.flowgroup = flowgroup


def _ctx(pipeline: str, flowgroup: str) -> FlowGroupContext:
    return FlowGroupContext(
        flowgroup=_FakeFlowGroup(pipeline, flowgroup), source_yaml=None
    )


@pytest.mark.unit
def test_assemble_folds_table_creation_errors_into_lhp_errors(monkeypatch):
    """Table-creation errors fold into ``lhp_errors`` as ``LHP-VAL-009``.

    Arrange: ``ValidationService.validate_cross_flowgroup`` (the only
    external boundary, per §8.8) returns a
    :class:`CrossFlowgroupCheckResult` with non-empty
    ``table_creation_errors``. The validate worker pool is replaced with a
    synchronous stand-in that invokes the REAL ``_assemble`` closure with a
    clean (no per-flowgroup error) result, so the system under test is the
    fold itself — not the subprocess pool mechanics (separately covered by
    ``test_run_validate_pool_guards_executor_submit_raises``).

    Assert: the per-pipeline outcome is unsuccessful AND carries a
    structured ``LHP-VAL-009`` in ``lhp_errors``.
    """
    pipeline_name = "bronze"

    # The single external boundary we mock (§8.8): the real ``_assemble``
    # fold consumes this result through ``build_cross_flowgroup_issues``.
    cross_result = CrossFlowgroupCheckResult(
        table_creation_errors=[
            "Table 'cat.sch.tbl' has 2 actions with create_table: true",
        ],
        cdc_fanin_errors=[],
    )
    validation_service = MagicMock()
    validation_service.validate_cross_flowgroup = MagicMock(
        return_value=cross_result
    )

    # Worker-pool collaborators are never exercised: the fake pool below
    # short-circuits the spawn boundary and drives ``_assemble`` directly.
    worker_state = pe._ValidateWorkerState(
        processor=MagicMock(),
        substitution_managers={},
        include_tests=False,
    )

    service = PipelineExecutionService(
        validate_worker_state=worker_state,
        validation_service=validation_service,
    )

    captured: List[Tuple[str, List[FlowgroupValidationResult]]] = []

    def _fake_run_validate_pool(
        *,
        flowgroups_by_pipeline,
        worker_state,
        assemble_pipeline,
        max_workers,
        on_pipeline_complete=None,
    ):
        """Synchronously drive the REAL ``_assemble`` closure.

        Mirrors what the real pool delivers post-barrier: a clean (no
        per-flowgroup error) result list per pipeline. No subprocess
        spawn, no pickling — only the fold under test runs.
        """
        outcomes: List[PipelineValidationOutcome] = []
        for pipeline in flowgroups_by_pipeline:
            results = [
                FlowgroupValidationResult(
                    pipeline=pipeline,
                    flowgroup_name="bronze_fg",
                    errors=(),
                    lhp_error=None,
                )
            ]
            captured.append((pipeline, results))
            outcome = assemble_pipeline(pipeline, results)
            outcomes.append(outcome)
            if on_pipeline_complete is not None:
                on_pipeline_complete(outcome)
        return outcomes

    monkeypatch.setattr(pe, "run_validate_pool", _fake_run_validate_pool)

    work_units = [
        PipelineWorkUnit(
            pipeline_name=pipeline_name,
            flowgroups=(_ctx(pipeline_name, "bronze_fg"),),
        )
    ]

    outcomes = service.run_validate(work_units)

    # The fake pool drove the real fold exactly once.
    assert [p for p, _ in captured] == [pipeline_name]
    assert len(outcomes) == 1
    outcome = outcomes[0]

    # The outcome fails and the structured VAL-009 lands in ``lhp_errors``.
    assert outcome.pipeline == pipeline_name
    assert outcome.success is False
    val_009 = [e for e in outcome.lhp_errors if e.code == "LHP-VAL-009"]
    assert len(val_009) == 1, outcome.lhp_errors
    # The error is structured, not stringified into ``errors``.
    assert outcome.errors == ()

    # The boundary was actually invoked with the resolved flowgroups.
    validation_service.validate_cross_flowgroup.assert_called_once()
    call = validation_service.validate_cross_flowgroup.call_args
    assert call.kwargs.get("pipeline_filter") == pipeline_name
