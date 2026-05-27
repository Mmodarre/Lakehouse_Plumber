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
from lhp.api._inspection_facade import InspectionFacade as InspectionFacade  # re-export (§1.10)
from lhp.api.events import (
    ErrorEmitted,
    GenerationCompleted,
    LHPEvent,
    OperationStarted,
    ValidationCompleted,
)
from lhp.api._converters import (
    _build_generation_batch_failure,
    _build_generation_batch_success,
    _build_validation_batch,
    _delta_to_generation_response,
    _finalize_monitoring_to_result,
    _outcome_to_validation_response,
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
    from lhp.cli.warning_collector import WarningCollector
    from lhp.models.config import FlowGroup

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
        pre_discovered_all_flowgroups: Any = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "GenerationResponse"], None]
        ] = None,
        on_pipeline_start: Optional[Callable[[str], None]] = None,
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
        """
        yield OperationStarted(operation_name="generate_pipelines", env=env)
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
                on_pipeline_start=on_pipeline_start,
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
        pre_discovered_all_flowgroups: Any = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "GenerationResponse"], None]
        ] = None,
        on_pipeline_start: Optional[Callable[[str], None]] = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> "BatchGenerationResponse":
        """Coordinate batch generation; aggregate failure preserves the
        LHP error code (§4.8) so the CLI can map it to an exit code
        without handling a live exception.

        Internal: invoked by the public generator wrapper
        :meth:`generate_pipelines`. The graceful-DTO failure path
        (catch ``Exception`` → return DTO with ``error_code``) is
        intentional and orthogonal to the stream-protocol
        ``ErrorEmitted`` rendezvous, which is reserved for genuinely
        catastrophic structured failures that this body does not
        choose to swallow.
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
                    pipeline_fields=list(pipeline_fields) if pipeline_filter is None else None,
                    env=env,
                    output_dir=output_dir,
                    specific_flowgroups=specific_flowgroups,
                    include_tests=include_tests,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                    max_workers=max_workers,
                    on_pipeline_complete=_on_delta,
                    on_pipeline_start=on_pipeline_start,
                    warning_collector=warning_collector,
                )
            return _build_generation_batch_success(
                pipeline_responses, output_dir=output_dir
            )
        except Exception as exc:
            from lhp.errors import LHPError

            if isinstance(exc, LHPError):
                self._logger.debug(
                    f"Batch pipeline generation failed: "
                    f"{len(pipeline_responses)} pipeline(s) had outcomes captured"
                )
            else:
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
            monitoring_dir = (
                self._orchestrator.project_root / "monitoring" / env
            )
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
                self._logger.debug(
                    f"Monitoring finalization failed: {exc.code}"
                )
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
        pre_discovered_all_flowgroups: Any = None,
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

        :stability: provisional
        """
        yield OperationStarted(operation_name="validate_pipelines", env=env)
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
        pre_discovered_all_flowgroups: Any = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> "BatchValidationResponse":
        """Coordinate batch validation; aggregate failure preserves the
        LHP error code (§4.8) on the DTO so the CLI can map it to an
        exit code without handling a live exception.

        Internal: invoked by the public generator wrapper
        :meth:`validate_pipelines`. The graceful-DTO failure path
        (catch ``Exception`` → return DTO with ``error_code``) is
        intentional and orthogonal to the stream-protocol
        ``ErrorEmitted`` rendezvous, which is reserved for genuinely
        catastrophic structured failures that this body does not
        choose to swallow.
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
                    pipeline_fields=list(pipeline_fields) if pipeline_filter is None else None,
                    env=env,
                    include_tests=include_tests,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                    max_workers=max_workers,
                    on_pipeline_complete=_on_outcome,
                    warning_collector=warning_collector,
                )
            return _build_validation_batch(
                pipeline_responses, tuple(pipeline_fields)
            )
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


# ``InspectionFacade`` is implemented in
# :mod:`lhp.api._inspection_facade` and ``BundleFacade`` in
# :mod:`lhp.api._bundle_facade` — both private modules — to keep this
# composition module under the §3.3 size budget. The re-exports near
# the top of this file preserve the single public import path
# (``from lhp.api import InspectionFacade, BundleFacade``).


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

    :stability: provisional
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

    # ----- shortcut delegations (§1.11) -----
    def generate_pipelines(self, **kwargs: Any) -> Iterator[LHPEvent]:
        """Shortcut for ``self.generation.generate_pipelines(...)``.

        Returns the event stream from the sub-facade unchanged via
        ``yield from`` (constitution §1.4, §5.7).

        :stability: provisional
        """
        yield from self.generation.generate_pipelines(**kwargs)

    def validate_pipelines(self, **kwargs: Any) -> Iterator[LHPEvent]:
        """Shortcut for ``self.validation.validate_pipelines(...)``.

        Returns the event stream from the sub-facade unchanged via
        ``yield from`` (constitution §1.4, §5.7).

        :stability: provisional
        """
        yield from self.validation.validate_pipelines(**kwargs)
