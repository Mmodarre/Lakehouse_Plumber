"""Unit tests for the single-wave flowgroup worker.

Exercises :func:`lhp.core.coordination._flowgroup_pool._process_one_flowgroup`
directly, in-process (NOT through a ``ProcessPoolExecutor``), to prove the
core contract: the worker resolves + validates (and, in generate mode,
codegens + ``ast.parse``-validates) one flowgroup and converts EVERY failure into a
:class:`~lhp.models.processing.FlowgroupOutcome` via the total
:meth:`FlowgroupOutcome.failure` constructor — it NEVER lets an exception
escape (constitution §5.6).

Real heavyweight services would drag the whole resolution/codegen graph
into a unit test, so we inject minimal fakes
that raise the specific exceptions; the point under test is the worker's
own ``try/except → failure`` conversion path, which is real, not faked.
The worker reads its collaborators from the module-global
``_flowgroup_state``, so each test sets that global (via ``monkeypatch``)
to a :class:`_FlowgroupWorkerState` wrapping the fakes — exactly how a
spawned worker reads state populated by ``_init_flowgroup_worker``.
"""

from __future__ import annotations

from concurrent.futures import Future
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence

import pytest

from lhp.core._interfaces import CrossFlowgroupCheckResult
from lhp.core.coordination import _flowgroup_pool as fp
from lhp.core.coordination import _pool as fe
from lhp.core.coordination._flowgroup_pool import (
    _FlowgroupWorkerState,
    _process_one_flowgroup,
)
from lhp.core.coordination._pool import _PipelinePoolResult, _run_flowgroup_pool_core
from lhp.core.coordination.executor import PipelineExecutionService
from lhp.errors import (
    ErrorCategory,
    LHPConfigError,
    LHPError,
    LHPValidationError,
    PythonFunctionConflictError,
    codes,
)
from lhp.models import FlowGroupContext
from lhp.models.deprecations import record_deprecation
from lhp.models.processing import (
    CopiedModuleRecord,
    DeprecationWarningRecord,
    FlowgroupOutcome,
    PipelineDelta,
)

PIPELINE = "pipe_a"
FLOWGROUP = "fg_one"


class _FakeFlowGroup:
    """Minimal FlowGroup stand-in.

    The worker only reads ``.pipeline`` / ``.flowgroup`` off the raw and
    resolved flowgroups; the resolved instance is otherwise forwarded
    opaquely onto the outcome, so no real fields are needed.
    """

    def __init__(self, pipeline: str = PIPELINE, flowgroup: str = FLOWGROUP) -> None:
        self.pipeline = pipeline
        self.flowgroup = flowgroup


def _ctx(
    *,
    pipeline: str = PIPELINE,
    flowgroup: str = FLOWGROUP,
    source_yaml: Optional[Path] = None,
    auxiliary_files: Optional[Mapping[str, str]] = None,
) -> FlowGroupContext:
    return FlowGroupContext(
        flowgroup=_FakeFlowGroup(pipeline, flowgroup),
        source_yaml=source_yaml,
        auxiliary_files=auxiliary_files or {},
    )


class _FakeResolver:
    """Stands in for FlowgroupResolutionService.

    ``process_flowgroup`` either raises ``raise_exc`` (simulating a
    per-flowgroup validation failure) or returns a NEW FlowGroupContext
    carrying a resolved flowgroup and the supplied ``aux`` mapping —
    mirroring the real method's ``dataclasses.replace(ctx, ...)`` return.
    """

    def __init__(
        self,
        *,
        raise_exc: Optional[Exception] = None,
        resolved_flowgroup: Optional[object] = None,
        aux: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._raise = raise_exc
        self._resolved = resolved_flowgroup or _FakeFlowGroup()
        self._aux = aux or {}
        self.calls: List[tuple] = []

    def process_flowgroup(self, ctx, substitution_mgr, include_tests=True):
        self.calls.append((ctx, substitution_mgr, include_tests))
        if self._raise is not None:
            raise self._raise
        return FlowGroupContext(
            flowgroup=self._resolved,
            source_yaml=ctx.source_yaml,
            auxiliary_files=self._aux,
        )


class _FakeCodeGen:
    """Stands in for CodeGenerationService.

    ``generate_flowgroup_code`` either raises ``raise_exc`` or returns
    ``code``. When ``append_record`` is given it is appended to the
    in/out ``phase_a_records`` list, exercising the worker's
    ``tuple(records)`` capture (records are mutated in place by the real
    method).
    """

    def __init__(
        self,
        *,
        code: str = "x = 1\n",
        raise_exc: Optional[Exception] = None,
        append_record: Optional[object] = None,
    ) -> None:
        self._code = code
        self._raise = raise_exc
        self._append_record = append_record
        self.calls: List[dict] = []

    def generate_flowgroup_code(
        self,
        flowgroup,
        substitution_mgr,
        output_dir=None,
        source_yaml=None,
        env=None,
        include_tests=False,
        phase_a_records=None,
        auxiliary_files=None,
    ):
        self.calls.append(
            {
                "flowgroup": flowgroup,
                "output_dir": output_dir,
                "source_yaml": source_yaml,
                "env": env,
                "include_tests": include_tests,
                "phase_a_records": phase_a_records,
                "auxiliary_files": auxiliary_files,
            }
        )
        if self._raise is not None:
            raise self._raise
        if self._append_record is not None and phase_a_records is not None:
            phase_a_records.append(self._append_record)
        return self._code


def _install_state(
    monkeypatch: pytest.MonkeyPatch,
    *,
    resolver: _FakeResolver,
    code_generator: Optional[_FakeCodeGen] = None,
    output_dir: Optional[Path] = None,
    include_tests: bool = False,
) -> _FlowgroupWorkerState:
    """Build a worker state over the fakes and set the module global.

    This is the in-process analogue of ``_init_flowgroup_worker`` running
    in a spawned worker: it populates ``_flowgroup_pool._flowgroup_state``
    so ``_process_one_flowgroup`` reads the fakes. The worker no longer
    formats (it only ``ast.parse``-validates the generated code), so the
    state carries no formatter.
    """
    state = _FlowgroupWorkerState(
        processor=resolver,
        substitution_managers={PIPELINE: object()},
        include_tests=include_tests,
        code_generator=code_generator or _FakeCodeGen(),
        pipeline_output_dirs={PIPELINE: output_dir},
        environment="dev",
    )
    monkeypatch.setattr(fp, "_flowgroup_state", state)
    return state


def _val_error() -> LHPValidationError:
    return LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="007",
        title="FlowGroup validation failed",
        details="bad flowgroup",
    )


def _parse_031_error() -> LHPConfigError:
    return LHPConfigError(
        category=ErrorCategory.CONFIG,
        code_number="031",
        title="Generated source failed to parse",
        details="generated source could not be parsed",
    )


@pytest.mark.unit
def test_generate_mode_ok_outcome(monkeypatch):
    """Generate mode returns an ok outcome with code, records, aux files."""
    resolved = _FakeFlowGroup()
    record = object()  # opaque CopiedModuleRecord stand-in
    resolver = _FakeResolver(
        resolved_flowgroup=resolved,
        aux={"monitor/extra.py": "print('x')\n"},
    )
    code_gen = _FakeCodeGen(code="y = 2\n", append_record=record)
    _install_state(
        monkeypatch,
        resolver=resolver,
        code_generator=code_gen,
        output_dir=Path("/out/pipe_a"),
        include_tests=True,
    )

    outcome = _process_one_flowgroup(_ctx(), mode="generate")

    assert isinstance(outcome, FlowgroupOutcome)
    assert outcome.success is True
    assert outcome.pipeline == PIPELINE
    assert outcome.flowgroup_name == FLOWGROUP
    assert outcome.resolved_flowgroup is resolved
    # The worker no longer formats: ``formatted_code`` carries the UNFORMATTED
    # source verbatim (the terminal ruff format pass on the coordinator formats
    # the committed tree). ``y = 2\n`` is valid Python, so the syntax guard passes.
    assert outcome.formatted_code == "y = 2\n"
    assert outcome.copy_records == (record,)
    assert outcome.auxiliary_files == (("monitor/extra.py", "print('x')\n"),)
    assert outcome.lhp_error is None
    assert outcome.errors == ()
    # Codegen got the run-wide wiring from state + per-flowgroup source_yaml.
    assert code_gen.calls[0]["env"] == "dev"
    assert code_gen.calls[0]["output_dir"] == Path("/out/pipe_a")
    assert code_gen.calls[0]["include_tests"] is True


@pytest.mark.unit
def test_validate_mode_ok_outcome_carries_resolved_no_code(monkeypatch):
    """Validate mode returns ok with the resolved FG and NO codegen."""
    resolved = _FakeFlowGroup()
    resolver = _FakeResolver(resolved_flowgroup=resolved)
    code_gen = _FakeCodeGen()
    _install_state(
        monkeypatch,
        resolver=resolver,
        code_generator=code_gen,
    )

    outcome = _process_one_flowgroup(_ctx(), mode="validate")

    assert outcome.success is True
    assert outcome.resolved_flowgroup is resolved
    assert outcome.formatted_code is None
    assert outcome.copy_records == ()
    assert outcome.lhp_error is None
    assert code_gen.calls == []


# Worker → main deprecation-warning routing (C3)
#
# The worker wrapper opens a ``collect_deprecations`` scope around the impl,
# stamped with this flowgroup's source_yaml + name. Soft-deprecation sites
# nested under resolution / validation / codegen call ``record_deprecation``;
# the wrapper drains them onto ``FlowgroupOutcome.warnings`` (deduped by
# ``(code, file)``) so C2's per-pipeline merge picks them up. Workers run under
# a NullHandler, so without this the warnings would be lost. These tests use a
# resolver that records a deprecation during ``process_flowgroup`` (standing in
# for the real ``database`` / ``database_suffix`` / ``enforcement`` sites).
class _RecordingResolver(_FakeResolver):
    """A resolver that records a deprecation while resolving the flowgroup."""

    def __init__(self, *, code, n_calls: int = 1, **kwargs) -> None:
        super().__init__(**kwargs)
        self._code = code
        self._n_calls = n_calls

    def process_flowgroup(self, ctx, substitution_mgr, include_tests=True):
        # Fire the same deprecation ``n_calls`` times (e.g. the inline schema
        # path warns twice) to prove the wrapper dedups by (code, file).
        for _ in range(self._n_calls):
            record_deprecation(
                self._code,
                title="A field is deprecated.",
                details="Use the replacement instead.",
            )
        return super().process_flowgroup(ctx, substitution_mgr, include_tests)


@pytest.mark.unit
def test_resolution_deprecation_rides_onto_validate_outcome(monkeypatch):
    """A deprecation recorded during resolve lands on the validate outcome.

    Proves the worker wrapper's collect/drain plumbing: the record is stamped
    with the ctx's ``source_yaml`` (file) and flowgroup name, and rides on
    ``FlowgroupOutcome.warnings`` even in validate mode (no codegen).
    """
    resolver = _RecordingResolver(code=codes.DEPR_002, n_calls=2)
    _install_state(monkeypatch, resolver=resolver)

    src = Path("flowgroups/fg_one.yaml")
    outcome = _process_one_flowgroup(_ctx(source_yaml=src), mode="validate")

    assert outcome.success is True
    # Two identical (code, file) records deduped to one on the outcome.
    assert len(outcome.warnings) == 1
    (warning,) = outcome.warnings
    assert isinstance(warning, DeprecationWarningRecord)
    assert warning.code == codes.DEPR_002.code
    assert warning.file == src
    assert warning.flowgroup == FLOWGROUP


@pytest.mark.unit
def test_resolution_deprecation_rides_onto_generate_outcome(monkeypatch):
    """The warning also rides on a generate-mode ok outcome (alongside code)."""
    resolver = _RecordingResolver(code=codes.DEPR_004)
    _install_state(
        monkeypatch,
        resolver=resolver,
        code_generator=_FakeCodeGen(code="z = 3\n"),
    )

    outcome = _process_one_flowgroup(
        _ctx(source_yaml=Path("flowgroups/fg_one.yaml")), mode="generate"
    )

    assert outcome.success is True
    assert outcome.formatted_code == "z = 3\n"
    assert [w.code for w in outcome.warnings] == [codes.DEPR_004.code]


@pytest.mark.unit
def test_resolution_deprecation_rides_onto_failure_outcome(monkeypatch):
    """A deprecation recorded before a resolve failure still rides back.

    The site records onto the scope, then resolution raises; the wrapper drains
    the scope regardless of the impl's failure path, so the warning is NOT lost
    when the flowgroup ultimately fails.
    """
    resolver = _RecordingResolver(code=codes.DEPR_002, raise_exc=_val_error())
    _install_state(monkeypatch, resolver=resolver)

    outcome = _process_one_flowgroup(
        _ctx(source_yaml=Path("flowgroups/fg_one.yaml")), mode="validate"
    )

    assert outcome.success is False
    assert isinstance(outcome.lhp_error, LHPValidationError)
    assert [w.code for w in outcome.warnings] == [codes.DEPR_002.code]


@pytest.mark.unit
@pytest.mark.parametrize("mode", ["validate", "generate"])
def test_per_flowgroup_validation_failure_returns_failure_dto(monkeypatch, mode):
    """resolve/validate raising LHPValidationError -> failure DTO, no raise.

    In generate mode the early failure return also proves codegen is
    short-circuited (the worker skips codegen when validation failed).
    """
    err = _val_error()
    resolver = _FakeResolver(raise_exc=err)
    code_gen = _FakeCodeGen()
    _install_state(
        monkeypatch,
        resolver=resolver,
        code_generator=code_gen,
    )

    outcome = _process_one_flowgroup(_ctx(), mode=mode)

    assert isinstance(outcome, FlowgroupOutcome)
    assert outcome.success is False
    assert outcome.pipeline == PIPELINE
    assert outcome.flowgroup_name == FLOWGROUP
    # LHPError travels live on the structured channel for verbatim re-raise.
    assert outcome.lhp_error is err
    assert isinstance(outcome.lhp_error, LHPValidationError)
    assert outcome.errors == ()
    assert outcome.resolved_flowgroup is None
    assert outcome.formatted_code is None
    assert code_gen.calls == []


@pytest.mark.unit
def test_codegen_failure_returns_failure_dto(monkeypatch):
    """A non-LHP exception in codegen -> failure DTO on the string channel."""
    boom = RuntimeError("codegen blew up")
    resolver = _FakeResolver()
    code_gen = _FakeCodeGen(raise_exc=boom)
    _install_state(
        monkeypatch,
        resolver=resolver,
        code_generator=code_gen,
    )

    outcome = _process_one_flowgroup(_ctx(), mode="generate")

    assert outcome.success is False
    assert outcome.lhp_error is None
    assert len(outcome.errors) == 1
    assert "RuntimeError" in outcome.errors[0]
    assert "codegen blew up" in outcome.errors[0]
    assert outcome.flowgroup_name == FLOWGROUP


@pytest.mark.unit
def test_syntax_guard_031_failure_returns_failure_dto(monkeypatch):
    """Codegen emitting un-parseable Python -> failure DTO with a live CFG-031.

    Drives the REAL ``ast.parse`` syntax guard
    (:func:`assert_generated_python_valid`) the worker now runs in place of
    the relocated formatting pass: the fake code generator returns
    syntactically-invalid source (``def (:``), so the guard raises the same
    ``LHP-CFG-031`` the user sees when the generated source cannot be parsed.
    The worker catches it (§5.6) and surfaces it on the structured channel.
    The guard is NOT mocked — this asserts the real behavior end-to-end.
    """
    resolver = _FakeResolver()
    code_gen = _FakeCodeGen(code="def (:\n")  # invalid Python -> real ast.parse fails
    _install_state(
        monkeypatch,
        resolver=resolver,
        code_generator=code_gen,
    )

    outcome = _process_one_flowgroup(_ctx(), mode="generate")

    assert outcome.success is False
    assert isinstance(outcome.lhp_error, LHPConfigError)
    assert outcome.lhp_error.code == "LHP-CFG-031"
    assert outcome.lhp_error.context.get("Flowgroup") == FLOWGROUP
    assert outcome.errors == ()


@pytest.mark.unit
@pytest.mark.parametrize(
    "stage, mode",
    [
        ("resolve", "validate"),
        ("resolve", "generate"),
        ("codegen", "generate"),
    ],
)
def test_worker_never_propagates_exception(monkeypatch, stage, mode):
    """Patch each collaborator to raise; assert the call RETURNS, never raises.

    This is the constitution-§5.6 contract under test: whatever blows up
    inside the worker, the call site sees a FlowgroupOutcome, not an
    exception crossing the (would-be) process boundary. The former
    ``format`` stage is gone — the worker no longer formats; the
    generate-mode syntax-guard failure path is covered by
    :func:`test_syntax_guard_031_failure_returns_failure_dto`.
    """
    boom = RuntimeError(f"{stage} exploded")
    resolver = _FakeResolver(raise_exc=boom if stage == "resolve" else None)
    code_gen = _FakeCodeGen(raise_exc=boom if stage == "codegen" else None)
    _install_state(
        monkeypatch,
        resolver=resolver,
        code_generator=code_gen,
    )

    outcome = _process_one_flowgroup(_ctx(), mode=mode)

    assert isinstance(outcome, FlowgroupOutcome)
    assert outcome.success is False


@pytest.mark.unit
def test_worker_never_propagates_even_when_substitution_lookup_fails(monkeypatch):
    """A KeyError from the substitution-manager lookup is also converted.

    The per-pipeline substitution lookup happens inside the first
    ``try`` block, so even a wiring bug (missing pipeline key) degrades
    to a failure DTO rather than escaping the worker.
    """
    resolver = _FakeResolver()
    state = _FlowgroupWorkerState(
        processor=resolver,
        substitution_managers={},  # missing PIPELINE key -> KeyError
        include_tests=False,
        code_generator=_FakeCodeGen(),
        pipeline_output_dirs={PIPELINE: None},
        environment="dev",
    )
    monkeypatch.setattr(fp, "_flowgroup_state", state)

    outcome = _process_one_flowgroup(_ctx(), mode="generate")

    assert isinstance(outcome, FlowgroupOutcome)
    assert outcome.success is False
    assert outcome.lhp_error is None
    assert "KeyError" in outcome.errors[0]
    # Resolver never ran (lookup failed first).
    assert resolver.calls == []


# Flat fan-out engine: _run_flowgroup_pool_core (validate mode)
#
# Locally-defined fakes are not picklable across a spawn boundary, so the
# ProcessPoolExecutor is swapped for a synchronous in-process executor and the
# worker entry for an in-process fake. The engine's real coordinator logic —
# flatten, submit, as_completed bucket, per-pipeline sort, cross-fg barrier,
# input-order assembly — is unmodified.


class _SyncExecutor:
    """Synchronous stand-in for ``ProcessPoolExecutor`` (in-process).

    Runs each submitted callable eagerly on ``submit`` and returns a real,
    already-completed :class:`concurrent.futures.Future`. This keeps the
    engine's ``executor.submit(...)`` / ``as_completed(...)`` / ``fut.result()``
    call shape intact while removing the spawn boundary. Accepts (and ignores)
    the ``max_workers`` / ``mp_context`` / ``initializer`` / ``initargs`` kwargs
    the engine passes — the faked worker fn reads no module global, so the
    initializer is intentionally not invoked.
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


class _BarrierSpy:
    """Records every ``validate_cross_flowgroup`` call (the §9.24 barrier).

    Captures the exact ``flowgroups`` sequence and ``pipeline_filter`` the
    engine passes per pipeline so the test can assert the barrier ran on the
    RESOLVED flowgroups (by identity), and how many times it fired (once per
    pipeline — including the single-flowgroup one). Returns an EMPTY
    :class:`CrossFlowgroupCheckResult` so ``build_cross_flowgroup_issues``
    yields no issues (this test isolates the fan-out/ordering/barrier-wiring,
    not the issue-building, which ``test_cross_flowgroup_issues`` covers).
    """

    def __init__(self) -> None:
        # (pipeline_filter, tuple-of-flowgroups) per invocation, in call order.
        self.calls: List[tuple] = []

    def validate_cross_flowgroup(
        self,
        flowgroups: Sequence[object],
        *,
        pipeline_filter: Optional[str] = None,
    ) -> CrossFlowgroupCheckResult:
        self.calls.append((pipeline_filter, tuple(flowgroups)))
        return CrossFlowgroupCheckResult()


@pytest.mark.unit
def test_flowgroup_pool_validate_barrier_on_resolved_and_pipeline_order(monkeypatch):
    """validate-mode engine: barrier runs on RESOLVED FGs, 1 result/pipeline.

    Worklist spans TWO pipelines in a deliberate input order — a multi-
    flowgroup pipeline first, then a SINGLE-flowgroup pipeline. The engine
    must:

    * return exactly one ``_PipelinePoolResult`` per pipeline, in INPUT
      pipeline order;
    * within the multi-fg pipeline, present outcomes SORTED by
      ``flowgroup_name`` — the worklist is supplied out of order to
      prove the sort;
    * fire the cross-flowgroup barrier on the RESOLVED flowgroups the workers
      returned, NOT the raw inputs (§9.24); and
    * fire that barrier even for the SINGLE-flowgroup pipeline (the §9.24
      closure — the trivial case is NOT skipped).
    """
    multi = "pipe_multi"
    single = "pipe_single"
    raw_ctxs = [
        _ctx(pipeline=multi, flowgroup="fg_b"),
        _ctx(pipeline=multi, flowgroup="fg_a"),
        _ctx(pipeline=single, flowgroup="fg_solo"),
    ]
    flowgroups_by_pipeline: Dict[str, List[FlowGroupContext]] = {
        multi: [raw_ctxs[0], raw_ctxs[1]],
        single: [raw_ctxs[2]],
    }

    resolved_by_name: Dict[tuple, _FakeFlowGroup] = {
        (multi, "fg_b"): _FakeFlowGroup(multi, "fg_b"),
        (multi, "fg_a"): _FakeFlowGroup(multi, "fg_a"),
        (single, "fg_solo"): _FakeFlowGroup(single, "fg_solo"),
    }

    def _fake_worker(fg_ctx: FlowGroupContext, *, mode: str) -> FlowgroupOutcome:
        assert mode == "validate"
        pipeline = fg_ctx.flowgroup.pipeline
        fg_name = fg_ctx.flowgroup.flowgroup
        resolved = resolved_by_name[(pipeline, fg_name)]
        return FlowgroupOutcome.ok(pipeline, fg_name, resolved_flowgroup=resolved)

    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(fe, "_process_one_flowgroup", _fake_worker)

    barrier = _BarrierSpy()
    worker_state = _FlowgroupWorkerState(
        processor=_FakeResolver(),
        substitution_managers={multi: object(), single: object()},
        include_tests=False,
        code_generator=_FakeCodeGen(),
        pipeline_output_dirs={multi: None, single: None},
        environment="dev",
    )

    results = _run_flowgroup_pool_core(
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        worker_state=worker_state,
        validation_service=barrier,
        max_workers=4,
        mode="validate",
    )

    assert [r.pipeline for r in results] == [multi, single]
    assert all(isinstance(r, _PipelinePoolResult) for r in results)

    multi_result, single_result = results

    # ---- multi-fg pipeline: outcomes SORTED by flowgroup_name (D8) ----
    assert [o.flowgroup_name for o in multi_result.outcomes_in_order] == [
        "fg_a",
        "fg_b",
    ]
    assert all(o.success for o in multi_result.outcomes_in_order)
    # No issues because the spy returns an empty CrossFlowgroupCheckResult.
    assert multi_result.cross_fg_issues == ()
    assert multi_result.cross_fg_errors == ()

    # ---- single-fg pipeline still produces a result ----
    assert [o.flowgroup_name for o in single_result.outcomes_in_order] == ["fg_solo"]

    called_pipelines = [pf for (pf, _fgs) in barrier.calls]
    assert sorted(called_pipelines) == [multi, single]
    assert single in called_pipelines, "1-flowgroup pipeline must trigger the barrier"

    # ---- barrier received the RESOLVED flowgroups, NOT the raw inputs ----
    by_pipeline = dict(barrier.calls)
    assert set(map(id, by_pipeline[multi])) == {
        id(resolved_by_name[(multi, "fg_a")]),
        id(resolved_by_name[(multi, "fg_b")]),
    }
    raw_multi_ids = {id(raw_ctxs[0].flowgroup), id(raw_ctxs[1].flowgroup)}
    assert raw_multi_ids.isdisjoint(set(map(id, by_pipeline[multi])))
    assert [id(fg) for fg in by_pipeline[single]] == [
        id(resolved_by_name[(single, "fg_solo")])
    ]


def _gate_worker_factory(outcomes_by_key):
    def _fake_worker(fg_ctx: FlowGroupContext, *, mode: str) -> FlowgroupOutcome:
        assert mode == "generate"
        key = (fg_ctx.flowgroup.pipeline, fg_ctx.flowgroup.flowgroup)
        return outcomes_by_key[key]

    return _fake_worker


def _drive_gate(monkeypatch, tmp_path, *, flowgroups_by_pipeline, outcomes_by_key):
    """Output dirs point at ``tmp_path`` so callers can assert the tree stays empty after a gate raise."""
    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(
        fe, "_process_one_flowgroup", _gate_worker_factory(outcomes_by_key)
    )

    pipelines = list(flowgroups_by_pipeline.keys())
    worker_state = _FlowgroupWorkerState(
        processor=_FakeResolver(),
        substitution_managers={p: object() for p in pipelines},
        include_tests=False,
        code_generator=_FakeCodeGen(),
        pipeline_output_dirs=dict.fromkeys(pipelines, tmp_path),
        environment="dev",
    )
    service = PipelineExecutionService(max_workers=4)
    return list(
        service._iter_generate_deltas(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=_BarrierSpy(),  # empty CrossFlowgroupCheckResult → no cross-fg issues
            max_workers=4,
        )
    )


def _drive_commit(monkeypatch, tmp_path, *, flowgroups_by_pipeline, outcomes_by_key):
    """Output dirs are NOT pre-created — the commit step's own ``mkdir`` must create them."""
    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(
        fe, "_process_one_flowgroup", _gate_worker_factory(outcomes_by_key)
    )

    pipelines = list(flowgroups_by_pipeline.keys())
    env_dir = tmp_path / "generated"
    pipeline_dirs = {p: env_dir / p for p in pipelines}
    worker_state = _FlowgroupWorkerState(
        processor=_FakeResolver(),
        substitution_managers={p: object() for p in pipelines},
        include_tests=False,
        code_generator=_FakeCodeGen(),
        pipeline_output_dirs=dict(pipeline_dirs),
        environment="dev",
    )
    service = PipelineExecutionService(max_workers=4)
    deltas = list(
        service._iter_generate_deltas(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            worker_state=worker_state,
            validation_service=_BarrierSpy(),
            output_dir=env_dir,
            project_config=None,
            project_root=tmp_path,
            max_workers=4,
        )
    )
    return deltas, deltas, env_dir, pipeline_dirs


@pytest.mark.unit
def test_flowgroup_pool_generate_gate_single_failure_raises_no_writes(
    monkeypatch, tmp_path
):
    """One failing fg + one valid → gate raises THAT failure's code, no writes.

    The failing flowgroup carries a live LHPValidationError (007); the valid
    one carries formatted_code. Exactly one failing unit → the gate re-raises
    the single error verbatim (LHP-VAL-007), NOT a 902. No files are written.
    """
    err = _val_error()  # LHP-VAL-007
    flowgroups_by_pipeline = {
        "pipe_bad": [_ctx(pipeline="pipe_bad", flowgroup="fg_bad")],
        "pipe_ok": [_ctx(pipeline="pipe_ok", flowgroup="fg_ok")],
    }
    outcomes_by_key = {
        ("pipe_bad", "fg_bad"): FlowgroupOutcome.failure(
            "pipe_bad", "fg_bad", lhp_error=err
        ),
        ("pipe_ok", "fg_ok"): FlowgroupOutcome.ok(
            "pipe_ok",
            "fg_ok",
            resolved_flowgroup=_FakeFlowGroup("pipe_ok", "fg_ok"),
            formatted_code="x = 1\n",
        ),
    }

    with pytest.raises(LHPError) as excinfo:
        _drive_gate(
            monkeypatch,
            tmp_path,
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            outcomes_by_key=outcomes_by_key,
        )

    # Single failure → the sole failure's code re-raised verbatim (not 902).
    assert excinfo.value is err
    assert excinfo.value.code == "LHP-VAL-007"
    # All-or-nothing: NOTHING was written.
    assert list(tmp_path.iterdir()) == []


@pytest.mark.unit
def test_flowgroup_pool_generate_gate_multi_failure_902_no_writes(
    monkeypatch, tmp_path
):
    """Two failing flowgroups (two pipelines) → gate raises LHP-VAL-902, no writes."""
    flowgroups_by_pipeline = {
        "pipe_a": [_ctx(pipeline="pipe_a", flowgroup="fg_a")],
        "pipe_b": [_ctx(pipeline="pipe_b", flowgroup="fg_b")],
    }
    outcomes_by_key = {
        ("pipe_a", "fg_a"): FlowgroupOutcome.failure(
            "pipe_a", "fg_a", lhp_error=_val_error()
        ),
        ("pipe_b", "fg_b"): FlowgroupOutcome.failure(
            "pipe_b", "fg_b", lhp_error=_parse_031_error()
        ),
    }

    with pytest.raises(LHPValidationError) as excinfo:
        _drive_gate(
            monkeypatch,
            tmp_path,
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            outcomes_by_key=outcomes_by_key,
        )

    assert excinfo.value.code == "LHP-VAL-902"
    assert excinfo.value.context["failure_count"] == 2
    assert "pipe_a" in excinfo.value.context
    assert "pipe_b" in excinfo.value.context
    assert list(tmp_path.iterdir()) == []


@pytest.mark.unit
def test_flowgroup_pool_generate_gate_copy_conflict_019_no_writes(
    monkeypatch, tmp_path
):
    """Two SUCCESSFUL outcomes whose copy_records collide on dest from different
    sources → gate's dry pass raises LHP-VAL-019, no writes.

    Both flowgroups validate + codegen fine (ok outcomes carrying code), but
    their CopiedModuleRecords target the SAME destination file from DIFFERENT
    source paths. The gate's PythonFileCopier.plan dry pass detects this BEFORE
    any write and the gate surfaces the single 019.
    """
    dest = tmp_path / "custom" / "shared.py"
    funcs_dir = tmp_path / "custom"
    record_a = CopiedModuleRecord(
        source_path="/src/a/shared.py",
        dest_path=dest,
        content="def a():\n    pass\n",
        module_path="custom.shared",
        custom_functions_dir=funcs_dir,
    )
    record_b = CopiedModuleRecord(
        source_path="/src/b/shared.py",  # DIFFERENT source, SAME dest → 019
        dest_path=dest,
        content="def b():\n    pass\n",
        module_path="custom.shared",
        custom_functions_dir=funcs_dir,
    )
    flowgroups_by_pipeline = {
        "pipe_x": [_ctx(pipeline="pipe_x", flowgroup="fg_x")],
        "pipe_y": [_ctx(pipeline="pipe_y", flowgroup="fg_y")],
    }
    outcomes_by_key = {
        ("pipe_x", "fg_x"): FlowgroupOutcome.ok(
            "pipe_x",
            "fg_x",
            resolved_flowgroup=_FakeFlowGroup("pipe_x", "fg_x"),
            formatted_code="x = 1\n",
            copy_records=(record_a,),
        ),
        ("pipe_y", "fg_y"): FlowgroupOutcome.ok(
            "pipe_y",
            "fg_y",
            resolved_flowgroup=_FakeFlowGroup("pipe_y", "fg_y"),
            formatted_code="y = 1\n",
            copy_records=(record_b,),
        ),
    }

    with pytest.raises(PythonFunctionConflictError) as excinfo:
        _drive_gate(
            monkeypatch,
            tmp_path,
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            outcomes_by_key=outcomes_by_key,
        )

    assert excinfo.value.code == "LHP-VAL-019"
    assert not dest.exists()


@pytest.mark.unit
def test_flowgroup_pool_generate_commit_writes_files_and_fires_deltas(
    monkeypatch, tmp_path
):
    """All-valid 2-pipeline run → COMMIT writes ``{flowgroup}.py`` per pipeline.

    The gate-passed path WRITES each pipeline's files
    (content == the outcome's ``formatted_code``), synthesizes a success
    :class:`PipelineDelta` per pipeline, and fires
    ``on_generate_pipeline_complete`` once per committed pipeline (spied here).
    Per-pipeline output dirs are distinct subdirs of the env dir so the file
    set under EACH is asserted independently.
    """
    flowgroups_by_pipeline = {
        "pipe_a": [_ctx(pipeline="pipe_a", flowgroup="fg_a")],
        "pipe_b": [_ctx(pipeline="pipe_b", flowgroup="fg_b")],
    }
    outcomes_by_key = {
        ("pipe_a", "fg_a"): FlowgroupOutcome.ok(
            "pipe_a",
            "fg_a",
            resolved_flowgroup=_FakeFlowGroup("pipe_a", "fg_a"),
            formatted_code="a = 1\n",
        ),
        ("pipe_b", "fg_b"): FlowgroupOutcome.ok(
            "pipe_b",
            "fg_b",
            resolved_flowgroup=_FakeFlowGroup("pipe_b", "fg_b"),
            formatted_code="b = 1\n",
        ),
    }

    deltas, completed, env_dir, pipeline_dirs = _drive_commit(
        monkeypatch,
        tmp_path,
        flowgroups_by_pipeline=flowgroups_by_pipeline,
        outcomes_by_key=outcomes_by_key,
    )

    assert [d.pipeline_name for d in deltas] == ["pipe_a", "pipe_b"]
    assert all(d.success for d in deltas)
    assert [d.files_written for d in deltas] == [1, 1]
    assert deltas[0].generated_filenames == ("fg_a.py",)
    assert deltas[1].generated_filenames == ("fg_b.py",)

    assert [d.pipeline_name for d in completed] == ["pipe_a", "pipe_b"]
    assert all(d.success for d in completed)

    assert sorted(p.name for p in pipeline_dirs["pipe_a"].iterdir()) == ["fg_a.py"]
    assert sorted(p.name for p in pipeline_dirs["pipe_b"].iterdir()) == ["fg_b.py"]
    assert (pipeline_dirs["pipe_a"] / "fg_a.py").read_text(
        encoding="utf-8"
    ) == "a = 1\n"
    assert (pipeline_dirs["pipe_b"] / "fg_b.py").read_text(
        encoding="utf-8"
    ) == "b = 1\n"


@pytest.mark.unit
def test_flowgroup_pool_generate_commit_skipped_and_no_wipe_on_gate_failure(
    monkeypatch, tmp_path
):
    """Gate FAILURE → no commit, and the env tree is NOT wiped (writes-only-if-passed).

    Reuses the single-failure gate setup (one failing fg + one valid). The gate
    raises BEFORE the commit phase, so the whole-env wipe — which lives in the
    commit phase — must NOT run: a stale file pre-seeded under the env
    dir survives, and no per-pipeline ``{flowgroup}.py`` is written. This is the
    byte-identity guarantee that a failed run leaves prior output untouched.
    """
    err = _val_error()  # LHP-VAL-007
    flowgroups_by_pipeline = {
        "pipe_bad": [_ctx(pipeline="pipe_bad", flowgroup="fg_bad")],
        "pipe_ok": [_ctx(pipeline="pipe_ok", flowgroup="fg_ok")],
    }
    outcomes_by_key = {
        ("pipe_bad", "fg_bad"): FlowgroupOutcome.failure(
            "pipe_bad", "fg_bad", lhp_error=err
        ),
        ("pipe_ok", "fg_ok"): FlowgroupOutcome.ok(
            "pipe_ok",
            "fg_ok",
            resolved_flowgroup=_FakeFlowGroup("pipe_ok", "fg_ok"),
            formatted_code="x = 1\n",
        ),
    }

    # Pre-seed a stale artifact in the env dir from a hypothetical prior run.
    env_dir = tmp_path / "generated"
    env_dir.mkdir(parents=True)
    stale = env_dir / "stale_pipeline" / "old.py"
    stale.parent.mkdir(parents=True)
    stale.write_text("stale = True\n", encoding="utf-8")

    completed: List[PipelineDelta] = []
    with pytest.raises(LHPError) as excinfo:
        _drive_commit(
            monkeypatch,
            tmp_path,
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            outcomes_by_key=outcomes_by_key,
        )

    assert excinfo.value is err
    assert stale.exists()
    assert stale.read_text(encoding="utf-8") == "stale = True\n"
    assert not (env_dir / "pipe_ok").exists()


@pytest.mark.unit
def test_flowgroup_pool_worker_warning_rides_back_into_pipeline_result(monkeypatch):
    """A worker-attached DeprecationWarningRecord rides back to the result.

    The single-flowgroup happy path: the worker returns an ok outcome carrying
    one warning on ``FlowgroupOutcome.warnings``; the engine must surface it on
    the per-pipeline ``_PipelinePoolResult.warnings`` (the worker→main hop
    preserves the field, then ``_finalize`` collects it).
    """
    pipeline = "pipe_w"
    warning = DeprecationWarningRecord(
        code="LHP-DEPR-001",
        message="`foo` is deprecated; use `bar`",
        file=Path("flowgroups/fg_solo.yaml"),
        flowgroup="fg_solo",
    )

    def _fake_worker(fg_ctx: FlowGroupContext, *, mode: str) -> FlowgroupOutcome:
        fg_name = fg_ctx.flowgroup.flowgroup
        return FlowgroupOutcome.ok(
            pipeline,
            fg_name,
            resolved_flowgroup=_FakeFlowGroup(pipeline, fg_name),
            warnings=(warning,),
        )

    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(fe, "_process_one_flowgroup", _fake_worker)

    worker_state = _FlowgroupWorkerState(
        processor=_FakeResolver(),
        substitution_managers={pipeline: object()},
        include_tests=False,
        code_generator=_FakeCodeGen(),
        pipeline_output_dirs={pipeline: None},
        environment="dev",
    )

    results = _run_flowgroup_pool_core(
        flowgroups_by_pipeline={
            pipeline: [_ctx(pipeline=pipeline, flowgroup="fg_solo")]
        },
        worker_state=worker_state,
        validation_service=_BarrierSpy(),
        max_workers=2,
        mode="validate",
    )

    assert len(results) == 1
    (result,) = results
    assert result.warnings == (warning,)
    assert result.warnings[0] is warning


@pytest.mark.unit
def test_flowgroup_pool_merges_and_dedups_warnings_by_code_file(monkeypatch):
    """Per-pipeline warnings are MERGED + DEDUPED by (code, file), order-stable.

    One pipeline, THREE flowgroups, with warnings arranged to exercise every
    dedup branch:

    * ``fg_a`` → W1 ``(DEPR-001, a.yaml)``.
    * ``fg_b`` → W1_dup ``(DEPR-001, a.yaml)`` — SAME ``(code, file)`` as W1 but
      a DIFFERENT message → must COLLAPSE into W1 (first-seen wins; W1's full
      payload, incl. message/flowgroup, is kept verbatim) — and W2
      ``(DEPR-002, a.yaml)`` — different ``code``, same file → SURVIVES.
    * ``fg_c`` → W3 ``(DEPR-001, b.yaml)`` — same ``code``, different file →
      SURVIVES.

    Expected merge: exactly ``(W1, W2, W3)`` — three records — in deterministic
    first-seen order. The worklist is supplied OUT of flowgroup-name order to
    prove the order is driven by ``_finalize``'s ``flowgroup_name`` sort, NOT by
    pool completion order.
    """
    pipeline = "pipe_w"
    a_yaml = Path("flowgroups/a.yaml")
    b_yaml = Path("flowgroups/b.yaml")
    w1 = DeprecationWarningRecord(
        code="LHP-DEPR-001", message="first-seen", file=a_yaml, flowgroup="fg_a"
    )
    w1_dup = DeprecationWarningRecord(
        code="LHP-DEPR-001",
        message="DUP — must be dropped",
        file=a_yaml,
        flowgroup="fg_b",
    )
    w2 = DeprecationWarningRecord(
        code="LHP-DEPR-002",
        message="same file, other code",
        file=a_yaml,
        flowgroup="fg_b",
    )
    w3 = DeprecationWarningRecord(
        code="LHP-DEPR-001",
        message="same code, other file",
        file=b_yaml,
        flowgroup="fg_c",
    )

    warnings_by_fg: Dict[str, tuple] = {
        "fg_a": (w1,),
        "fg_b": (w1_dup, w2),
        "fg_c": (w3,),
    }

    def _fake_worker(fg_ctx: FlowGroupContext, *, mode: str) -> FlowgroupOutcome:
        fg_name = fg_ctx.flowgroup.flowgroup
        return FlowgroupOutcome.ok(
            pipeline,
            fg_name,
            resolved_flowgroup=_FakeFlowGroup(pipeline, fg_name),
            warnings=warnings_by_fg[fg_name],
        )

    monkeypatch.setattr(fe, "ProcessPoolExecutor", _SyncExecutor)
    monkeypatch.setattr(fe, "_process_one_flowgroup", _fake_worker)

    worker_state = _FlowgroupWorkerState(
        processor=_FakeResolver(),
        substitution_managers={pipeline: object()},
        include_tests=False,
        code_generator=_FakeCodeGen(),
        pipeline_output_dirs={pipeline: None},
        environment="dev",
    )

    results = _run_flowgroup_pool_core(
        flowgroups_by_pipeline={
            pipeline: [
                # OUT of name order: c, a, b — to prove the dedup order is driven
                # by the per-pipeline flowgroup_name sort, not completion order.
                _ctx(pipeline=pipeline, flowgroup="fg_c"),
                _ctx(pipeline=pipeline, flowgroup="fg_a"),
                _ctx(pipeline=pipeline, flowgroup="fg_b"),
            ]
        },
        worker_state=worker_state,
        validation_service=_BarrierSpy(),
        max_workers=4,
        mode="validate",
    )

    assert len(results) == 1
    (result,) = results

    assert result.warnings == (w1, w2, w3)
    assert result.warnings[0] is w1
    assert result.warnings[0].message == "first-seen"
    # The dedup is keyed on (code, file) ONLY — proven by W2 (same file, other
    # code) and W3 (same code, other file) both surviving alongside W1.
    assert [(w.code, w.file) for w in result.warnings] == [
        ("LHP-DEPR-001", a_yaml),
        ("LHP-DEPR-002", a_yaml),
        ("LHP-DEPR-001", b_yaml),
    ]
