"""The project stage of the flat engine: VAL-072 across every pipeline (#346).

``apply_project_checks`` joins the facts every flowgroup outcome carries, runs
the project-wide check once, and attaches each issue to the reading
pipeline's ``cross_fg_issues``: validate reports it and the generate gate
stops on it before any file is written.
"""

from __future__ import annotations

from concurrent.futures import Future
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, Tuple

import pytest

from lhp.core.coordination import _pool as fe
from lhp.core.coordination._flowgroup_pool import _FlowgroupWorkerState
from lhp.core.coordination._pool import _PipelinePoolResult, assemble_validate_outcomes
from lhp.core.coordination._project_cross_checks import apply_project_checks
from lhp.core.coordination.executor import PipelineExecutionService
from lhp.core.coordination.validation_service import ValidationService
from lhp.errors import ErrorFactory, LHPError, codes
from lhp.models import FlowGroupContext
from lhp.models.processing import FlowgroupOutcome, StreamReadFact, TableProducerFact

pytestmark = pytest.mark.unit

_RW = TableProducerFact(
    pipeline="p_silver",
    flowgroup="fg_rw",
    action="write_rw",
    table="c.s.rw",
    is_replace_where=True,
)
_READER = StreamReadFact(
    pipeline="p_gold",
    flowgroup="fg_gold",
    action="load_rw",
    table_ref="c.s.rw",
    via="delta",
)


def _outcome(pipeline: str, flowgroup: str, *, producers=(), readers=(), code=None):
    return FlowgroupOutcome.ok(
        pipeline,
        flowgroup,
        formatted_code=code,
        producer_facts=producers,
        stream_read_facts=readers,
    )


def _result(pipeline: str, *outcomes: FlowgroupOutcome) -> _PipelinePoolResult:
    return _PipelinePoolResult(
        pipeline=pipeline,
        outcomes_in_order=tuple(outcomes),
        cross_fg_issues=(),
        cross_fg_errors=(),
    )


def _pool_results() -> List[_PipelinePoolResult]:
    return [
        _result("p_silver", _outcome("p_silver", "fg_rw", producers=(_RW,))),
        _result("p_gold", _outcome("p_gold", "fg_gold", readers=(_READER,))),
    ]


def test_issue_is_attached_to_the_reading_pipeline(tmp_path: Path):
    silver, gold = apply_project_checks(_pool_results(), ValidationService(tmp_path))

    assert silver.cross_fg_issues == ()
    assert [issue.code for issue in gold.cross_fg_issues] == ["LHP-VAL-072"]
    assert "p_gold.fg_gold.load_rw streams from 'c.s.rw'" in (
        gold.cross_fg_issues[0].details
    )
    assert gold.cross_fg_issues[0].context["Pipeline"] == "p_gold"


def test_validate_reports_the_issue_without_a_flowgroup(tmp_path: Path):
    results = apply_project_checks(_pool_results(), ValidationService(tmp_path))

    silver, gold = assemble_validate_outcomes(results, discovery_errors={})

    assert silver.success
    assert not gold.success
    (record,) = gold.issues
    assert record.flowgroup_name is None
    assert record.source_file is None
    assert record.issue.code == "LHP-VAL-072"


def test_no_replace_where_producer_skips_the_check():
    """Without a REPLACE WHERE producer the service is never consulted."""
    plain = TableProducerFact("p_silver", "fg", "write_st", "c.s.rw", False)
    results = [
        _result("p_silver", _outcome("p_silver", "fg", producers=(plain,))),
        _result("p_gold", _outcome("p_gold", "fg_gold", readers=(_READER,))),
    ]

    assert apply_project_checks(results, object()) == results


_TWO_PART_READER = StreamReadFact(
    pipeline="p_gold",
    flowgroup="fg_gold",
    action="transform_two_part",
    table_ref="s.rw",
    via="sql",
)


def test_partial_run_leaves_two_part_reads_unmatched(tmp_path: Path):
    """Under ``-p`` / a sandbox scope the selected facts cannot show that
    ``s.rw`` is ambiguous project-wide, so a 2-part read is not resolved."""
    results = [
        _result("p_silver", _outcome("p_silver", "fg_rw", producers=(_RW,))),
        _result("p_gold", _outcome("p_gold", "fg_gold", readers=(_TWO_PART_READER,))),
    ]
    service = ValidationService(tmp_path)

    _, partial_gold = apply_project_checks(results, service, partial_run=True)
    _, full_gold = apply_project_checks(results, service)

    assert partial_gold.cross_fg_issues == ()
    assert [issue.code for issue in full_gold.cross_fg_issues] == ["LHP-VAL-072"]


def test_partial_run_still_matches_a_three_part_cross_pipeline_read(tmp_path: Path):
    _, gold = apply_project_checks(
        _pool_results(), ValidationService(tmp_path), partial_run=True
    )

    assert [issue.code for issue in gold.cross_fg_issues] == ["LHP-VAL-072"]


class _RaisingValidationService(ValidationService):
    def __init__(self, project_root: Path, exc: Exception) -> None:
        super().__init__(project_root)
        self._exc = exc

    def validate_project_stream_reads(self, producers, readers, **kwargs):
        raise self._exc


def test_exception_guard_turns_an_error_into_an_issue(tmp_path: Path):
    service = _RaisingValidationService(tmp_path, RuntimeError("index exploded"))

    silver, gold = apply_project_checks(_pool_results(), service)

    assert silver.cross_fg_errors == () and silver.cross_fg_issues == ()
    assert gold.cross_fg_issues == ()
    assert gold.cross_fg_errors == (
        "REPLACE WHERE stream-read check failed: index exploded",
    )


def test_exception_guard_keeps_a_raised_lhp_error_structured(tmp_path: Path):
    error = ErrorFactory.validation_error(
        codes.VAL_072, title="Pre-built", details="raised by the check"
    )
    service = _RaisingValidationService(tmp_path, error)

    _, gold = apply_project_checks(_pool_results(), service)

    assert gold.cross_fg_issues == (error,)
    assert gold.cross_fg_errors == ()


class _SyncExecutor:
    """In-process stand-in for ``ProcessPoolExecutor``."""

    def __init__(self, *args, **kwargs) -> None:
        pass

    def __enter__(self) -> "_SyncExecutor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def submit(self, fn, /, *args, **kwargs) -> Future:
        fut: Future = Future()
        fut.set_result(fn(*args, **kwargs))
        return fut


class _FakeFlowGroup:
    def __init__(self, pipeline: str, flowgroup: str) -> None:
        self.pipeline = pipeline
        self.flowgroup = flowgroup


def _ctx(pipeline: str, flowgroup: str) -> FlowGroupContext:
    return FlowGroupContext(
        flowgroup=_FakeFlowGroup(pipeline, flowgroup), source_yaml=None
    )


def test_generate_gate_raises_and_no_files_are_written(monkeypatch, tmp_path: Path):
    flowgroups_by_pipeline: Mapping[str, Sequence[FlowGroupContext]] = {
        "p_silver": [_ctx("p_silver", "fg_rw")],
        "p_gold": [_ctx("p_gold", "fg_gold")],
    }
    outcomes: Dict[Tuple[str, str], FlowgroupOutcome] = {
        ("p_silver", "fg_rw"): _outcome(
            "p_silver", "fg_rw", producers=(_RW,), code="a = 1\n"
        ),
        ("p_gold", "fg_gold"): _outcome(
            "p_gold", "fg_gold", readers=(_READER,), code="b = 1\n"
        ),
    }

    def _fake_worker(fg_ctx: FlowGroupContext, *, mode: str) -> FlowgroupOutcome:
        return outcomes[(fg_ctx.flowgroup.pipeline, fg_ctx.flowgroup.flowgroup)]

    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(fe, "_process_one_flowgroup", _fake_worker)
    env_dir = tmp_path / "generated"
    worker_state = _FlowgroupWorkerState(
        processor=None,
        substitution_managers=dict.fromkeys(flowgroups_by_pipeline, object()),
        include_tests=False,
        code_generator=None,
        pipeline_output_dirs={p: env_dir / p for p in flowgroups_by_pipeline},
        environment="dev",
    )

    stream = PipelineExecutionService(max_workers=2)._iter_generate_deltas(
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        worker_state=worker_state,
        validation_service=ValidationService(tmp_path),
        output_dir=env_dir,
        project_config=None,
        project_root=tmp_path,
        max_workers=2,
    )
    with pytest.raises(LHPError) as excinfo:
        list(stream)

    assert excinfo.value.code == "LHP-VAL-072"
    assert not env_dir.exists()
