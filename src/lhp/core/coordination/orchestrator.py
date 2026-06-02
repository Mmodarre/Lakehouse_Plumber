"""Main orchestration for LakehousePlumber pipeline generation.

:class:`ActionOrchestrator` is the composition root wiring eight
ABC-typed collaborator services (discovery, flowgroup resolution,
validation, code generation, dependency analysis, monitoring,
execution, bootstrap) and exposing four public methods to callers
(CLI commands and :class:`LakehousePlumberApplicationFacade`).

Per Target Architecture §4 (thin coordination layer), callers use the
public services directly (``.bootstrap``, ``.discovery``,
``.processing``, ``.codegen``); the orchestrator exposes only four
methods, each the narrowest surface for its responsibility and none
composing another:
``discover_flowgroups`` (single-pipeline directory read);
``finalize_monitoring_artifacts`` (end-of-run notebook/job write);
``generate_pipelines`` (batch generate; hands to
:class:`PipelineExecutionService`); ``validate_pipelines`` (batch
validate; hands to :class:`PipelineExecutionService`).
"""

# JUSTIFIED: Constructor wires eight ABC-typed collaborator services
# inline (~170L) per §4.10/§4.12, including the §9.24 injection branch
# and back-compat construction for direct `ActionOrchestrator(project_root)`
# test callers at tests/test_orchestrator.py:607.
# Under §9.3's 800-line hard cap.

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence

from lhp.models import FlowGroup

if TYPE_CHECKING:
    from ...models.processing import PipelineDelta

from ...parsers.blueprint_parser import BlueprintParser
from ...parsers.yaml_parser import CachingYAMLParser, YAMLParser
from ...presets.preset_manager import PresetManager
from ...utils.version import (
    get_version,
)
from .._interfaces import (
    BaseCodeGenerationService,
    BaseDependencyAnalysisService,
    BaseFlowgroupBootstrapService,
    BaseFlowgroupDiscoveryService,
    BaseFlowgroupResolutionService,
    BaseMonitoringFinalizerService,
    BasePipelineExecutionService,
    BaseValidationService,
    BaseWarningCollector,
)
from ..codegen.coordinator import CodeGenerationService
from ..dependencies import DependencyAnalysisService, DependencyResolver
from ..discovery.blueprint_discoverer import BlueprintDiscoverer
from ..discovery.flowgroup_discoverer import FlowgroupDiscoveryService
from ..loaders import ProjectConfigLoader
from ..loaders.version_enforcement import enforce_version_requirements
from ..processing import TemplateEngine
from ..processing.blueprint_expander import BlueprintExpander
from ..registry import ActionRegistry, OrchestrationDependencies
from ..validators import ConfigValidator, SecretValidator
from ._flowgroup_pool import _FlowgroupWorkerState
from .bootstrap_service import FlowgroupBootstrapService
from .executor import (
    OnValidationComplete,
    PipelineExecutionService,
    PipelineValidationOutcome,
)
from .flowgroup_worklist_builder import build_flowgroup_worklist
from .monitoring_service import MonitoringFinalizerService


def _auto_max_workers() -> int:
    """Resolve a worker count when no explicit override is supplied.

    Detection chain (3.11+ compatible):
      1. ``os.process_cpu_count()`` — Python 3.13+, respects CPU affinity natively.
      2. ``os.sched_getaffinity(0)`` — Linux, reflects cgroup CPU quotas
         (e.g. Docker ``--cpus=2`` on a large host returns 2).
      3. ``os.cpu_count()`` — macOS / Windows fallback.

    Applies a 20% headroom (``floor(detected * 0.8)``) so the main thread
    and OS have room to schedule alongside the spawn'd worker pool. The
    workload cap (don't spawn more workers than independent submissions)
    is intentionally NOT applied here — callers know their own workload
    shape and apply it at the submission site.
    """
    if hasattr(os, "process_cpu_count"):
        detected = os.process_cpu_count() or 1  # type: ignore[attr-defined]
    elif hasattr(os, "sched_getaffinity"):
        detected = len(os.sched_getaffinity(0))
    else:
        detected = os.cpu_count() or 1
    return max(1, int(detected * 0.8))


class ActionOrchestrator:
    """Coordinates discovery, processing, generation, and validation services."""

    def __init__(
        self,
        project_root: Path,
        enforce_version: bool = True,
        dependencies: OrchestrationDependencies = None,
        pipeline_config_path: Optional[str] = None,
        max_workers: Optional[int] = None,
        *,
        flowgroup_resolver: Optional[BaseFlowgroupResolutionService] = None,
        validation_service: Optional[BaseValidationService] = None,
        config_validator: Optional[ConfigValidator] = None,
        bootstrap_service: Optional[BaseFlowgroupBootstrapService] = None,
    ):
        """Initialize orchestrator.

        Args:
            max_workers: Worker count for the parallel pool (generate
                parallelizes per pipeline, validate per flowgroup). If
                ``None``, resolves to ``LHP_MAX_WORKERS`` env var, else
                :func:`_auto_max_workers` (~80% of OS-visible CPU count,
                honoring cgroup CPU limits on Linux). ``1`` is sequential.
            flowgroup_resolver: Pre-built
                :class:`FlowgroupResolutionService` injected by
                :meth:`LakehousePlumberApplicationFacade.for_project`.
                Required.
            validation_service: Pre-built :class:`ValidationService`
                injected by
                :meth:`LakehousePlumberApplicationFacade.for_project`.
                Required.
            config_validator: Pre-built :class:`ConfigValidator`
                injected by the composition root. Forwarded to the
                :class:`DependencyAnalysisService` so the single
                ``ConfigValidator`` instance is shared across the
                process. When ``None``, ``DependencyAnalysisService``
                falls back to constructing its own.
            bootstrap_service: Pre-built :class:`FlowgroupBootstrapService`
                injected by the composition root. Owns the
                discovery → blueprint expansion → monitoring chain and
                the synthetic-context provenance table. When ``None``,
                the orchestrator constructs one inline from its
                ``discovery`` / ``blueprint_discoverer`` /
                ``blueprint_expander`` / ``monitoring`` collaborators.

        Raises:
            ValueError: If ``flowgroup_resolver`` or ``validation_service``
                is ``None``. Direct construction without these services is
                no longer supported; route through
                :meth:`LakehousePlumberApplicationFacade.for_project`.
        """
        if flowgroup_resolver is None or validation_service is None:
            raise ValueError(
                "ActionOrchestrator must be constructed via "
                "LakehousePlumberApplicationFacade.for_project(...); direct "
                "construction without flowgroup_resolver + validation_service "
                "is no longer supported."
            )
        self.project_root = project_root
        self.enforce_version = enforce_version
        self._orchestration_dependencies = dependencies or OrchestrationDependencies()
        self.pipeline_config_path = pipeline_config_path
        self.logger = logging.getLogger(__name__)

        self.yaml_parser = YAMLParser()
        self._cached_yaml_parser = CachingYAMLParser(self.yaml_parser)
        self.preset_manager = PresetManager(project_root / "presets")
        self.template_engine = TemplateEngine(project_root / "templates")
        self.project_config_loader = ProjectConfigLoader(project_root)
        self.action_registry = ActionRegistry()
        self.secret_validator = SecretValidator()
        self.dependency_resolver = DependencyResolver()

        self.project_config = self.project_config_loader.load_project_config()

        # Typed service attributes, all typed by their ABC
        # (constitution §4.10 + §4.12).
        self.discovery: BaseFlowgroupDiscoveryService = FlowgroupDiscoveryService(
            project_root,
            self.project_config_loader,
            yaml_parser=self._cached_yaml_parser,
        )
        self.blueprint_parser = BlueprintParser(
            caching_yaml_parser=self._cached_yaml_parser
        )
        self.blueprint_discoverer = BlueprintDiscoverer(
            project_root,
            project_config=self.project_config,
            blueprint_parser=self.blueprint_parser,
            caching_yaml_parser=self._cached_yaml_parser,
        )
        self.blueprint_expander = BlueprintExpander()

        self.validation: BaseValidationService = validation_service
        self.processing: BaseFlowgroupResolutionService = flowgroup_resolver
        self.codegen: BaseCodeGenerationService = CodeGenerationService(
            self.action_registry,
            self.dependency_resolver,
            self.preset_manager,
            self.project_config,
            project_root,
        )
        self.dependencies: BaseDependencyAnalysisService = DependencyAnalysisService(
            project_root=project_root,
            project_config=self.project_config,
            validation_service=self.validation,
            config_validator=config_validator,
        )
        self.monitoring: BaseMonitoringFinalizerService = MonitoringFinalizerService(
            project_config=self.project_config,
            project_root=self.project_root,
            dependencies=self._orchestration_dependencies,
            pipeline_config_path=self.pipeline_config_path,
            logger=self.logger,
        )
        if max_workers is not None:
            self.max_workers: int = max(1, max_workers)
        else:
            env_override = os.environ.get("LHP_MAX_WORKERS")
            if env_override:
                try:
                    self.max_workers = max(1, int(env_override))
                except ValueError:
                    self.logger.warning(
                        f"LHP_MAX_WORKERS={env_override!r} is not an integer; "
                        f"falling back to auto-detect."
                    )
                    self.max_workers = _auto_max_workers()
            else:
                self.max_workers = _auto_max_workers()
        self.execution: BasePipelineExecutionService = PipelineExecutionService(
            max_workers=self.max_workers,
        )
        self.bootstrap: BaseFlowgroupBootstrapService = (
            bootstrap_service
            or FlowgroupBootstrapService(
                discovery=self.discovery,
                blueprint_discoverer=self.blueprint_discoverer,
                blueprint_expander=self.blueprint_expander,
                monitoring=self.monitoring,
                logger=self.logger,
            )
        )

        # Legacy aliases retained for test compatibility: orchestrator method
        # bodies use the canonical typed attributes; `self.processor` /
        # `self.generator` remain pointing at `self.processing` / `self.codegen`
        # because tests (test_cdc_fanin, test_append_flow, test_local_variables_e2e)
        # still reach in by the old names.
        self.processor = self.processing
        self.generator = self.codegen

        if self.enforce_version:
            self._enforce_version_requirements()

        self.logger.info(
            f"Initialized ActionOrchestrator with service-based architecture: {project_root}"
        )
        if self.project_config:
            self.logger.info(
                f"Loaded project configuration: {self.project_config.name} v{self.project_config.version}"
            )
        else:
            self.logger.info("No project configuration found, using defaults")

    def _enforce_version_requirements(self) -> None:
        """Enforce ``required_lhp_version`` via :mod:`core.loaders.version_enforcement`.

        ``get_version()`` is looked up via this module's namespace so tests
        that ``patch('lhp.core.coordination.orchestrator.get_version', ...)`` still take
        effect after the D8b extraction of the enforcement body.
        """
        enforce_version_requirements(
            self.project_config,
            actual_version=get_version(),
        )

    def discover_flowgroups(self, pipeline_dir: Path) -> List[FlowGroup]:
        """Discover all flowgroups in a specific pipeline directory.

        Directory-based discovery is not on the service ABC (which keys on
        pipeline field, not directory). Delegate to the discovery service's
        directory helper directly until the legacy directory path retires.
        """
        return self.discovery._legacy_discover_flowgroups_by_dir(pipeline_dir)  # type: ignore[attr-defined]

    def finalize_monitoring_artifacts(self, env: str, output_dir: Path) -> None:
        """Reconcile monitoring artifacts: clean stale, write current.

        Pure pass-through to :meth:`MonitoringFinalizerService.finalize_artifacts`.
        Called AFTER the pipeline generation loop; the service handles
        notebook + job resource generation, cleanup of stale artifacts, and
        the add/remove/rename transitions.
        """
        self.monitoring.finalize_artifacts(env, output_dir)

    def generate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Optional[Sequence[str]] = None,
        env: str,
        output_dir: Optional[Path] = None,
        specific_flowgroups: Optional[List[str]] = None,
        include_tests: bool = False,
        apply_formatting: bool | None = None,
        pre_discovered_all_flowgroups: Optional[Sequence[FlowGroup]] = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[Callable[["PipelineDelta"], None]] = None,
        warning_collector: Optional[BaseWarningCollector] = None,
    ) -> Dict[str, tuple[str, ...]]:
        """Build the flat worklist, hand to PipelineExecutionService.run_generate.

        Exactly one of ``pipeline_filter`` (single pipeline by field) or
        ``pipeline_fields`` (batch by field list) may be supplied. When
        both are ``None`` no pipelines are generated and an empty mapping
        is returned (the caller is expected to discover the pipeline
        list first; see :class:`GenerationFacade`).

        ``apply_formatting`` is a tri-state override for the terminal
        ruff pass: ``None`` (the default) resolves to the loaded
        project config's ``apply_formatting`` value (``True`` when there
        is no project config); ``True`` / ``False`` override it. This
        method resolves the override to a concrete bool and passes that
        plain value into :meth:`PipelineExecutionService.run_generate`.

        Routes through the consolidated flat per-flowgroup engine,
        mirroring :meth:`validate_pipelines`:
        :func:`flowgroup_worklist_builder.build_flowgroup_worklist` produces the
        flat four-map shape — here with a REAL ``output_dir`` (generate writes;
        validate passes ``None``), so each pipeline's ``output_dirs`` entry is
        ``output_dir / <pipeline>`` (and is created on disk by the builder). The
        per-pipeline cross-flowgroup validation now runs on the RESOLVED
        flowgroups INSIDE the engine (§9.24).

        :meth:`PipelineExecutionService.run_generate` drives the engine in
        ``mode="generate"``, applies the all-or-nothing GATE (RAISES on any
        failure, before any write — so this method no longer wraps the call in
        an aggregate-and-raise step), then commits each clean pipeline and
        returns ``{pipeline -> generated filenames}`` (the per-pipeline
        :class:`PipelineDelta`s flow through ``on_pipeline_complete``). A
        per-pipeline discovery failure (carried in the worklist's
        ``discovery_errors`` map) aborts the whole batch inside ``run_generate``
        — generate is all-or-nothing.
        """
        if pipeline_filter is not None and pipeline_fields is not None:
            raise ValueError(
                "generate_pipelines: pass either pipeline_filter or "
                "pipeline_fields, not both"
            )
        if pipeline_filter is not None:
            effective_fields: Sequence[str] = [pipeline_filter]
        elif pipeline_fields is not None:
            effective_fields = pipeline_fields
        else:
            return {}

        self.logger.info(
            f"Starting batch pipeline generation: {len(effective_fields)} pipeline(s) for env: {env}"
        )
        (
            flowgroups_by_pipeline,
            substitution_managers,
            output_dirs,
            discovery_errors,
        ) = build_flowgroup_worklist(
            self,
            pipeline_fields=effective_fields,
            env=env,
            output_dir=output_dir,
            pre_discovered_all_flowgroups=(
                list(pre_discovered_all_flowgroups)
                if pre_discovered_all_flowgroups is not None
                else None
            ),
            warning_collector=warning_collector,
        )
        self.execution.configure_generate(
            max_workers=max_workers if max_workers is not None else self.max_workers,
            on_pipeline_complete=on_pipeline_complete,
            environment=env,
            include_tests=include_tests,
            validation_service=self.validation,
            worker_state=self._build_generate_worker_state(env, include_tests),
        )
        # Resolve the tri-state formatting override against the loaded project
        # config: ``None`` means "use the project's ``lhp.yaml``
        # ``apply_formatting`` setting" (default ``True`` when there is no
        # project config); ``True`` / ``False`` override it. ``run_generate``
        # and everything inward receive the already-resolved plain bool.
        if apply_formatting is None:
            effective_apply_formatting = (
                self.project_config.apply_formatting
                if self.project_config is not None
                else True
            )
        else:
            effective_apply_formatting = apply_formatting
        return self.execution.run_generate(
            flowgroups_by_pipeline=flowgroups_by_pipeline,
            substitution_managers=substitution_managers,
            output_dirs=output_dirs,
            discovery_errors=discovery_errors,
            output_dir=output_dir,
            project_config=self.project_config,
            project_root=self.project_root,
            max_workers=max_workers,
            apply_formatting=effective_apply_formatting,
        )

    def _build_generate_worker_state(
        self,
        env: str,
        include_tests: bool,
    ) -> _FlowgroupWorkerState:
        """Build the unified worker state for the flat-engine generate path.

        Generate uses the same :class:`_FlowgroupWorkerState` carrier as
        validate — identical to
        :meth:`_build_validate_worker_state` (the generate-only collaborators
        ``code_generator`` / ``environment`` are genuinely
        consumed here, unlike in validate mode). The per-pipeline
        ``substitution_managers`` / ``pipeline_output_dirs`` are placeholders;
        :meth:`PipelineExecutionService.run_generate` replaces them per batch
        from the worklist builder's maps. ``project_config`` / ``project_root``
        are deliberately NOT on this carrier (they are commit-step inputs the
        coordinator passes to ``run_generate`` separately, never crossing the
        spawn boundary). No formatter rides along: the worker only
        ``ast.parse``-validates generated code; the single terminal ruff pass
        runs on the coordinator (:class:`PipelineExecutionService`).
        """
        return _FlowgroupWorkerState(
            processor=self.processing,
            substitution_managers={},
            include_tests=include_tests,
            code_generator=self.codegen,
            pipeline_output_dirs={},
            environment=env,
        )

    def validate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Optional[Sequence[str]] = None,
        env: str,
        include_tests: bool = True,
        pre_discovered_all_flowgroups: Optional[Sequence[FlowGroup]] = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[OnValidationComplete] = None,
        warning_collector: Optional[BaseWarningCollector] = None,
    ) -> List[PipelineValidationOutcome]:
        """Build the flat worklist, hand to PipelineExecutionService.run_validate.

        Exactly one of ``pipeline_filter`` (single pipeline by field) or
        ``pipeline_fields`` (batch by field list) may be supplied. When
        both are ``None`` no pipelines are validated and an empty list
        is returned (the caller is expected to discover the pipeline
        list first; see :class:`ValidationFacade`).

        Routes through the consolidated flat per-flowgroup engine:
        :func:`flowgroup_worklist_builder.build_flowgroup_worklist` produces
        the flat four-map shape (``output_dir=None`` — validate writes
        nothing), which :meth:`PipelineExecutionService.run_validate` drives
        in ``mode="validate"``. Cross-flowgroup validation now runs on the
        RESOLVED flowgroups inside the engine (§9.24).
        """
        if pipeline_filter is not None and pipeline_fields is not None:
            raise ValueError(
                "validate_pipelines: pass either pipeline_filter or "
                "pipeline_fields, not both"
            )
        if pipeline_filter is not None:
            effective_fields: Sequence[str] = [pipeline_filter]
        elif pipeline_fields is not None:
            effective_fields = pipeline_fields
        else:
            return []

        (
            flowgroups_by_pipeline,
            substitution_managers,
            output_dirs,
            discovery_errors,
        ) = build_flowgroup_worklist(
            self,
            pipeline_fields=effective_fields,
            env=env,
            output_dir=None,
            pre_discovered_all_flowgroups=(
                list(pre_discovered_all_flowgroups)
                if pre_discovered_all_flowgroups is not None
                else None
            ),
            warning_collector=warning_collector,
        )
        self.execution.configure_validate(
            max_workers=max_workers if max_workers is not None else self.max_workers,
            include_tests=include_tests,
            validation_service=self.validation,
            on_pipeline_complete=on_pipeline_complete,
            worker_state=self._build_validate_worker_state(env, include_tests),
        )
        return list(
            self.execution.run_validate(
                flowgroups_by_pipeline=flowgroups_by_pipeline,
                substitution_managers=substitution_managers,
                output_dirs=output_dirs,
                discovery_errors=discovery_errors,
            )
        )

    def _build_validate_worker_state(
        self, env: str, include_tests: bool
    ) -> _FlowgroupWorkerState:
        """Build the unified worker state for the flat-engine validate path.

        The consolidated engine takes one
        :class:`_FlowgroupWorkerState` for both modes. In validate mode the
        worker only reads ``processor`` / ``substitution_managers`` /
        ``include_tests`` (it resolves + per-flowgroup-validates and stops);
        the generate-only collaborators (``code_generator`` /
        ``pipeline_output_dirs`` / ``environment``) are required by the
        dataclass but unused on this path. They are populated with the real
        collaborators anyway — harmless for validate, and the exact shape
        generate reuses. The per-pipeline ``substitution_managers``
        / ``pipeline_output_dirs`` are placeholders here; ``run_validate``
        replaces them per batch from the worklist builder's maps.
        """
        return _FlowgroupWorkerState(
            processor=self.processing,
            substitution_managers={},
            include_tests=include_tests,
            code_generator=self.codegen,
            pipeline_output_dirs={},
            environment=env,
        )
