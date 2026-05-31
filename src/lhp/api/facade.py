"""Application facade — single entry point for LHP runtime operations.

Composes four sub-facades (generation, validation, inspection, bundle)
that group related operations. Constructed exclusively via
:meth:`LakehousePlumberApplicationFacade.for_project`; the composition
root lives in :mod:`lhp.core.coordination.layers`.

:stability: provisional
"""

# JUSTIFIED: This module defines the public facade composition surface
# as a single cohesive entry point. Two sub-facade classes
# (Generation, Validation) plus the top-level
# ``LakehousePlumberApplicationFacade`` co-locate here to preserve the
# single-import-path invariant of §1.10. ``BundleFacade`` has been
# extracted to ``lhp.api._bundle_facade`` (private module, re-exported
# below) to absorb C5's three methods plus their helper converters and
# scaffolding logic. ``InspectionFacade`` has likewise been extracted
# to ``lhp.api._inspection_facade`` (private module, re-exported
# below) so the twelve read-only inspection methods plus their §3.2
# method-enumeration docstring don't push this composition module over
# §3.3's 800-line hard cap. C6 added the stream-protocol generator
# wrappers plus their ``_do_*`` private helpers (the OLD bodies,
# extracted) for ``generate_pipelines`` and ``validate_pipelines`` per
# §5.7. Heavy DTO-conversion bodies live in
# :mod:`lhp.api._converters`; what remains is the per-method
# delegation surface for Generation and Validation.
# TODO(Phase 9.5): extract sub-facade composition once the BundleFacade / InspectionFacade pattern absorbs the per-method delegation surface; see LOCAL/REMAINING_WORK.md §9.5.
from __future__ import annotations

import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
)

from lhp.api._bundle_facade import BundleFacade as BundleFacade  # re-export (§1.10)
from lhp.api._converters import (
    _build_generation_batch_failure,
    _build_generation_batch_success,
    _build_validation_batch,
    _build_validation_batch_from_issues,
    _delta_to_generation_response,
    _finalize_monitoring_to_result,
    _issue_view_to_lhp_error,
    _outcome_to_validation_response,
)
from lhp.api._inspection_facade import (
    InspectionFacade as InspectionFacade,  # re-export (§1.10)
)
from lhp.api._preflight import _run_project_preflight
from lhp.api.events import (
    ErrorEmitted,
    GenerationCompleted,
    LHPEvent,
    OperationStarted,
    ValidationCompleted,
)
from lhp.api.responses import (
    BatchGenerationResponse,
    BatchValidationResponse,
    FinalizeMonitoringResult,
    GenerationResponse,
    ValidationResponse,
)
from lhp.errors import LHPError
from lhp.utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from lhp.models import FlowGroup

    from .callbacks import WarningCollector

    # Internal orchestrator type, referenced only as a quoted annotation
    # below; never named directly in the public API surface (§1.10,
    # §9.13). Composition is delegated to ``core/coordination/layers``.
    _Orchestrator = Any


class GenerationFacade:
    """Generation operations on a constructed project.

    :stability: provisional
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self._logger = logging.getLogger(__name__)

    def generate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        output_dir: Optional[Path],
        specific_flowgroups: Optional[List[str]] = None,
        include_tests: bool = False,
        bundle_enabled: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "GenerationResponse"], None]
        ] = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> Iterator[LHPEvent]:
        """Stream-protocol wrapper around batch pipeline generation.

        Yields :class:`OperationStarted` first, then on success exactly
        one :class:`GenerationCompleted` carrying the
        :class:`BatchGenerationResponse`. If the underlying coordinator
        raises an :class:`LHPError`, an :class:`ErrorEmitted` is yielded
        and the error is re-raised (constitution §1.4, §5.7).

        Callers that don't need event-level visibility use
        :func:`lhp.api.collect_response` to walk the stream and return
        the terminal response DTO.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-VAL-*`` (config/action/schema
            validation), ``LHP-CFG-*`` (project config + substitution),
            ``LHP-FILE-*`` (missing files), ``LHP-MULT-*`` (multi-document
            YAML), and ``LHP-TPL-*`` (template expansion) propagated from
            the per-pipeline workers. An :class:`ErrorEmitted` event is
            yielded before the exception escapes (§1.4 stream protocol).
        """
        yield OperationStarted(operation_name="generate_pipelines", env=env)

        # §9.24: the preflight LOGIC is single-sourced in
        # ``_run_project_preflight`` (shared with the validate path). This
        # call site only SURFACES failures: generate raises (validate folds
        # them into a failed batch instead). It MUST sit in this OUTER
        # generator — BEFORE the ``_do_generate_pipelines`` delegation
        # below — because that helper's ``except Exception → return failure``
        # body SWALLOWS exceptions into a DTO, which would eat the
        # ErrorEmitted/raise rendezvous.
        #
        # ``bundle_enabled`` is threaded from the CLI (``--no-bundle`` +
        # databricks.yml auto-detect): when ``True`` this preflight also runs
        # the bundle catalog/schema check (→ ``LHP-CFG-026``), single-sourced
        # with validate. It only fires if the orchestrator was constructed
        # with a ``pipeline_config_path`` (generate requires ``-pc`` when
        # bundle is on). Covers DUPLICATE + TEST-REPORTING + bundle checks.
        preflight_issues = _run_project_preflight(
            self._orchestrator,
            env=env,
            bundle_enabled=bundle_enabled,
            include_tests=include_tests,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
        )
        if preflight_issues:
            preflight_error = _issue_view_to_lhp_error(preflight_issues[0])
            yield ErrorEmitted(lhp_error=preflight_error)
            raise preflight_error

        try:
            response = self._do_generate_pipelines(
                pipeline_filter=pipeline_filter,
                pipeline_fields=pipeline_fields,
                env=env,
                output_dir=output_dir,
                specific_flowgroups=specific_flowgroups,
                include_tests=include_tests,
                pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                max_workers=max_workers,
                on_pipeline_complete=on_pipeline_complete,
                warning_collector=warning_collector,
            )
        except LHPError as exc:
            yield ErrorEmitted(lhp_error=exc)
            raise
        yield GenerationCompleted(response=response)

    def _do_generate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        output_dir: Optional[Path],
        specific_flowgroups: Optional[List[str]] = None,
        include_tests: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "GenerationResponse"], None]
        ] = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> "BatchGenerationResponse":
        """Coordinate batch generation and return the terminal batch DTO.

        Internal: invoked by the public generator wrapper
        :meth:`generate_pipelines`.

        Failure handling is split deliberately:

        * **Structured (:class:`LHPError`) failures** — most importantly
          the all-or-nothing gate aggregate raised by ``run_generate``
          (spec §3 / §3bis) — are re-raised UNCHANGED so they propagate
          out to :meth:`generate_pipelines`, whose ``except LHPError``
          performs the §1.4 stream-protocol rendezvous (yield
          :class:`ErrorEmitted`, then re-raise). This body no longer
          swallows ``LHPError`` into a DTO.
        * **Non-LHP infra failures** (e.g. an :class:`OSError` from a
          commit-time disk write, AFTER the gate has passed) degrade to a
          batch-failure response via :func:`_build_generation_batch_failure`
          so the CLI gets a terminal DTO with an ``error_code`` rather than
          a live exception. See the comment on the residual ``except``
          below: this is intentional, not a §1.4 gap.

        The ``generated/<env>`` wipe is NOT performed here: the commit step
        (:func:`~lhp.core.coordination.executor.PipelineExecutionService`)
        owns the single whole-env wipe, which now runs AFTER the
        all-or-nothing gate so a gate failure leaves prior output
        untouched. A dry run threads ``output_dir=None`` from the CLI, so
        ``output_dir`` is forwarded as-is and the engine writes nothing.
        """
        from lhp.models.processing import PipelineDelta

        pipeline_responses: Dict[str, "GenerationResponse"] = {}

        def _on_delta(delta: "PipelineDelta") -> None:
            response = _delta_to_generation_response(delta, output_dir=output_dir)
            pipeline_responses[delta.pipeline_name] = response
            if on_pipeline_complete is not None:
                try:
                    on_pipeline_complete(delta.pipeline_name, response)
                except Exception as cb_exc:
                    self._logger.warning(
                        f"on_pipeline_complete callback raised "
                        f"for {delta.pipeline_name}: {cb_exc}"
                    )

        try:
            with perf_timer(f"facade.generate_pipelines [{len(pipeline_fields)}]"):
                self._orchestrator.generate_pipelines(
                    pipeline_filter=pipeline_filter,
                    pipeline_fields=(
                        list(pipeline_fields) if pipeline_filter is None else None
                    ),
                    env=env,
                    output_dir=output_dir,
                    specific_flowgroups=specific_flowgroups,
                    include_tests=include_tests,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                    max_workers=max_workers,
                    on_pipeline_complete=_on_delta,
                    warning_collector=warning_collector,
                )
            return _build_generation_batch_success(
                pipeline_responses, output_dir=output_dir
            )
        except LHPError:
            # §1.4 rendezvous: let the structured failure (most importantly
            # the all-or-nothing gate aggregate from ``run_generate``)
            # propagate to ``generate_pipelines``, which yields
            # ``ErrorEmitted`` then re-raises. Bare ``raise`` keeps the
            # original cause chain (B904) — do NOT wrap in a DTO here.
            raise
        except Exception as exc:
            # A NON-LHP infra failure at/after the gate — e.g. an ``OSError``
            # from a commit-time disk write once the gate has already passed —
            # is reported as a batch-failure DTO, NOT routed through the
            # ``ErrorEmitted``+raise rendezvous (which is reserved for
            # ``LHPError``). Such a failure may leave a partially written
            # output tree: commit is a non-transactional multi-file write
            # and making it atomic is explicitly out of scope. This is an
            # intentional, documented degradation, not a silent §1.4 gap.
            self._logger.exception("Batch pipeline generation failed")
            return _build_generation_batch_failure(pipeline_responses, exc)

    def finalize_monitoring_artifacts(
        self, env: str, output_dir: Path
    ) -> FinalizeMonitoringResult:
        """Finalize monitoring-pipeline artifacts after a successful generation.

        Wraps the post-generation monitoring-pipeline assembly step.
        Returns a frozen result describing what was produced (or why
        finalization skipped/failed). Failures are caught and surfaced
        on the DTO rather than raised so callers can map ``error_code``
        to an exit code without handling a live exception (§4.8).

        :stability: provisional
        :raises: None — failures are surfaced on the result's
            ``error_code`` field (§4.8).
        """
        try:
            with perf_timer("facade.finalize_monitoring_artifacts"):
                self._orchestrator.finalize_monitoring_artifacts(env, output_dir)
            build_result = self._orchestrator.monitoring.last_build_result
            if build_result is None:
                # Monitoring not configured / no synthetic flowgroup built —
                # legitimate no-op.
                return _finalize_monitoring_to_result(
                    monitoring_pipeline_path=None,
                    event_log_table_created=False,
                )
            monitoring_dir = self._orchestrator.project_root / "monitoring" / env
            notebook_path: Optional[Path] = (
                monitoring_dir / "union_event_logs.py"
                if monitoring_dir.exists()
                else None
            )
            return _finalize_monitoring_to_result(
                monitoring_pipeline_path=notebook_path,
                event_log_table_created=build_result.flowgroup is not None,
            )
        except Exception as exc:
            from lhp.errors import LHPError

            if isinstance(exc, LHPError):
                self._logger.debug(f"Monitoring finalization failed: {exc.code}")
            else:
                self._logger.exception("Monitoring finalization failed")
            return _finalize_monitoring_to_result(
                monitoring_pipeline_path=None,
                event_log_table_created=False,
                exc=exc,
            )


class ValidationFacade:
    """Validation operations on a constructed project.

    :stability: provisional
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self._logger = logging.getLogger(__name__)

    def validate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "ValidationResponse"], None]
        ] = None,
        include_tests: bool = True,
        bundle_enabled: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> Iterator[LHPEvent]:
        """Stream-protocol wrapper around batch pipeline validation.

        Yields :class:`OperationStarted` first, then on success exactly
        one :class:`ValidationCompleted` carrying the
        :class:`BatchValidationResponse`. If the underlying coordinator
        raises an :class:`LHPError`, an :class:`ErrorEmitted` is yielded
        and the error is re-raised (constitution §1.4, §5.7).

        Callers that don't need event-level visibility use
        :func:`lhp.api.collect_response` to walk the stream and return
        the terminal response DTO.

        ``bundle_enabled`` mirrors the generate path: when ``True`` the
        shared preflight also runs the bundle catalog/schema check
        (→ ``LHP-CFG-026``), but only if the orchestrator was constructed
        with a ``pipeline_config_path``. Defaults to ``False`` so the
        non-bundle behavior is unchanged (§9.24).

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-VAL-*`` (config/action/schema
            validation), ``LHP-CFG-*`` (project config + substitution),
            ``LHP-FILE-*`` (missing files), ``LHP-MULT-*`` (multi-document
            YAML), and ``LHP-TPL-*`` (template expansion) propagated from
            the per-pipeline workers. An :class:`ErrorEmitted` event is
            yielded before the exception escapes (§1.4 stream protocol).
        """
        yield OperationStarted(operation_name="validate_pipelines", env=env)
        # §9.24: project-level preflight is single-sourced in
        # ``_run_project_preflight`` (shared with the generate path); this
        # site only SURFACES its issues. validate FOLDS them into a failed
        # batch (non-zero exit) — it never raises — whereas generate raises.
        # ``bundle_enabled`` is threaded from the CLI (``--no-bundle`` +
        # databricks.yml auto-detect) so the bundle catalog/schema preflight
        # runs symmetrically with generate.
        preflight_issues = _run_project_preflight(
            self._orchestrator,
            env=env,
            bundle_enabled=bundle_enabled,
            include_tests=include_tests,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
        )
        if preflight_issues:
            yield ValidationCompleted(
                response=_build_validation_batch_from_issues(
                    preflight_issues, tuple(pipeline_fields)
                )
            )
            return
        try:
            response = self._do_validate_pipelines(
                pipeline_filter=pipeline_filter,
                pipeline_fields=pipeline_fields,
                env=env,
                max_workers=max_workers,
                on_pipeline_complete=on_pipeline_complete,
                include_tests=include_tests,
                pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                warning_collector=warning_collector,
            )
        except LHPError as exc:
            yield ErrorEmitted(lhp_error=exc)
            raise
        yield ValidationCompleted(response=response)

    def _do_validate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "ValidationResponse"], None]
        ] = None,
        include_tests: bool = True,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> "BatchValidationResponse":
        """Coordinate batch validation; aggregate failure preserves the
        LHP error code (§4.8) on the DTO so the CLI can map it to an
        exit code without handling a live exception.

        Internal: invoked by the public generator wrapper
        :meth:`validate_pipelines`, which has already run the shared
        §9.24 project preflight (duplicate-(pipeline, flowgroup) +
        test-reporting + optional bundle) and short-circuited on any
        issues. This body therefore only runs the per-pipeline
        validation pool. The graceful-DTO failure path (catch
        ``Exception`` → return DTO with ``error_code``) is intentional
        and orthogonal to the stream-protocol ``ErrorEmitted``
        rendezvous, which is reserved for genuinely catastrophic
        structured failures that this body does not choose to swallow.
        """
        from lhp.core.coordination.executor import PipelineValidationOutcome

        pipeline_responses: Dict[str, "ValidationResponse"] = {}

        def _on_outcome(outcome: "PipelineValidationOutcome") -> None:
            response = _outcome_to_validation_response(outcome)
            pipeline_responses[outcome.pipeline] = response
            if on_pipeline_complete is not None:
                try:
                    on_pipeline_complete(outcome.pipeline, response)
                except Exception as cb_exc:
                    self._logger.warning(
                        f"on_pipeline_complete callback raised "
                        f"for {outcome.pipeline}: {cb_exc}"
                    )

        try:
            with perf_timer(f"facade.validate_pipelines [{len(pipeline_fields)}]"):
                self._orchestrator.validate_pipelines(
                    pipeline_filter=pipeline_filter,
                    pipeline_fields=(
                        list(pipeline_fields) if pipeline_filter is None else None
                    ),
                    env=env,
                    include_tests=include_tests,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                    max_workers=max_workers,
                    on_pipeline_complete=_on_outcome,
                    warning_collector=warning_collector,
                )
            return _build_validation_batch(pipeline_responses, tuple(pipeline_fields))
        except Exception as exc:
            from lhp.errors import LHPError

            if isinstance(exc, LHPError):
                self._logger.debug(
                    f"Batch pipeline validation failed: "
                    f"{len(pipeline_responses)} pipeline(s) had outcomes captured"
                )
            else:
                self._logger.exception("Batch pipeline validation failed")
            return _build_validation_batch(
                pipeline_responses, tuple(pipeline_fields), exc=exc
            )


class LakehousePlumberApplicationFacade:
    """Top-level application facade.

    Composes four sub-facades that group related operations:

    - ``generation`` — batch generation runs.
    - ``validation`` — config validation.
    - ``inspection`` — read-only project introspection.
    - ``bundle`` — Asset Bundle operations.

    Constructed exclusively via :meth:`for_project`. The bare
    ``__init__(...)`` form is internal — external callers must route
    through the classmethod so the composition-root logic (single
    ``ConfigValidator``, threaded collaborators) is enforced (§4.5).

    The public ``orchestrator`` attribute is intentionally absent
    (§1.10, §9.23); callers must use the sub-facade methods.

    :stability: stable
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator
        self.generation = GenerationFacade(orchestrator)
        self.validation = ValidationFacade(orchestrator)
        self.inspection = InspectionFacade(orchestrator)
        self.bundle = BundleFacade(orchestrator)

    @classmethod
    def for_project(
        cls,
        project_root: Path,
        *,
        pipeline_config_path: Optional[str] = None,
        enforce_version: bool = True,
        max_workers: Optional[int] = None,
    ) -> "LakehousePlumberApplicationFacade":
        """Construct a fully-wired facade from a project root.

        Centralises service-graph construction so no service reaches
        into another's private attributes; builds the single
        ``ConfigValidator`` that threads through validation and
        flowgroup-resolution (closes the §9.24 leak).

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` if ``lhp.yaml`` is
            absent, malformed, or fails project-config validation;
            ``LHP-VAL-*`` if ``enforce_version`` is set and the
            ``lhp_version`` constraint rejects the installed package;
            ``LHP-FILE-*`` for missing-path conditions discovered
            during composition.
        """
        # Composition is delegated to an internal helper so the
        # orchestrator class name never appears in :mod:`lhp.api`
        # source (§1.10, §9.13). Lazy import.
        from lhp.core.coordination.layers import build_facade_orchestrator

        orchestrator = build_facade_orchestrator(
            project_root,
            pipeline_config_path=pipeline_config_path,
            enforce_version=enforce_version,
            max_workers=max_workers,
        )
        return cls(orchestrator)

    # Shortcut delegations — §1.11.
    def generate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        output_dir: Optional[Path],
        specific_flowgroups: Optional[List[str]] = None,
        include_tests: bool = False,
        bundle_enabled: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "GenerationResponse"], None]
        ] = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> Iterator[LHPEvent]:
        """Shortcut for ``self.generation.generate_pipelines(...)``.

        Restates the canonical signature (§4.2) and forwards every
        parameter unchanged via ``yield from`` (§1.4, §5.7).

        :stability: provisional
        :raises lhp.errors.LHPError: same families as
            :meth:`GenerationFacade.generate_pipelines` — ``LHP-VAL-*``,
            ``LHP-CFG-*``, ``LHP-FILE-*``, ``LHP-MULT-*``, ``LHP-TPL-*``.
        """
        yield from self.generation.generate_pipelines(
            pipeline_filter=pipeline_filter,
            pipeline_fields=pipeline_fields,
            env=env,
            output_dir=output_dir,
            specific_flowgroups=specific_flowgroups,
            include_tests=include_tests,
            bundle_enabled=bundle_enabled,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            max_workers=max_workers,
            on_pipeline_complete=on_pipeline_complete,
            warning_collector=warning_collector,
        )

    def validate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Sequence[str] = (),
        env: str,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "ValidationResponse"], None]
        ] = None,
        include_tests: bool = True,
        bundle_enabled: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence["FlowGroup"]] = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> Iterator[LHPEvent]:
        """Shortcut for ``self.validation.validate_pipelines(...)``.

        Restates the canonical signature (§4.2) and forwards every
        parameter unchanged via ``yield from`` (§1.4, §5.7).

        :stability: provisional
        :raises lhp.errors.LHPError: same families as
            :meth:`ValidationFacade.validate_pipelines` — ``LHP-VAL-*``,
            ``LHP-CFG-*``, ``LHP-FILE-*``, ``LHP-MULT-*``, ``LHP-TPL-*``.
        """
        yield from self.validation.validate_pipelines(
            pipeline_filter=pipeline_filter,
            pipeline_fields=pipeline_fields,
            env=env,
            max_workers=max_workers,
            on_pipeline_complete=on_pipeline_complete,
            include_tests=include_tests,
            bundle_enabled=bundle_enabled,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            warning_collector=warning_collector,
        )
