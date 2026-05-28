"""Main orchestration for LakehousePlumber pipeline generation."""

# JUSTIFIED: Orchestrator sits at ~770 lines because it carries three
# irreducible responsibilities that resist further extraction:
# (1) constructor wiring of seven ABC-typed collaborator services (~170L,
# including the §9.24 injection branch + 30-line back-compat construction
# for direct `ActionOrchestrator(project_root)` test callers) — inlining is
# intentional, the alternative is a separate `BootstrapService` that exists
# only to call seven constructors;
# (2) the work-unit builder helpers `_build_generate_work_units` and
# `_build_validate_work_units` — now thin delegators (~40L combined) to
# `coordination/work_unit_builder.py`, but the orchestrator must still
# thread main-process-only `_synthetic_contexts` state through
# `_make_context` calls into the spawn-boundary envelope;
# (3) `_aggregate_generate_outcomes` (~50L) — the failure-bucketing
# single-failure/multi-failure/re-raise branches are load-bearing for the
# CLI's structured-Panel display contract and don't isolate cleanly.
# Under the §9.3 800-line hard cap.
# TODO(Phase 9.1): extract BootstrapService (discovery + monitoring wiring), split __init__ into substitution-manager + output-dir sub-builders, fold _aggregate_generate_outcomes into executor — target ≤300L per LOCAL/REMAINING_WORK.md §9.1.

import logging
import os
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Sequence, Tuple

from ...models.config import FlowGroup, FlowGroupContext

if TYPE_CHECKING:
    from ...api.callbacks import WarningCollector
    from ...generators.python_file_copier import CopiedModuleRecord
    from ...models.processing import PipelineDelta

from ...errors import (
    ErrorCategory,
    LHPError,
    LHPValidationError,
    lhp_error_from_worker_failure,
)
from ...models.processing import PipelineWorkUnit
from ...parsers.blueprint_parser import BlueprintParser
from ...parsers.yaml_parser import CachingYAMLParser, YAMLParser
from ...presets.preset_manager import PresetManager
from ...utils.file_header import write_normalized
from ...utils.formatter import CodeFormatter
from ...utils.performance_timer import perf_timer
from ...utils.version import (  # noqa: F401 — re-export for tests that monkeypatch `orchestrator.get_version`
    get_version,
)
from ..codegen.coordinator import CodeGenerationService
from ..dependencies import DependencyAnalysisService, DependencyResolver
from ..discovery.blueprint_discoverer import BlueprintDiscoverer
from ..discovery.flowgroup_discoverer import FlowgroupDiscoveryService
from ..loaders import ProjectConfigLoader
from ..loaders.version_enforcement import enforce_version_requirements
from ..processing import TemplateEngine
from ..processing.blueprint_expander import BlueprintExpander, BlueprintProvenance
from ..processing.substitution import EnhancedSubstitutionManager
from ..registry import ActionRegistry, OrchestrationDependencies
from ..validators import ConfigValidator
from ..validators.secret_validator import SecretValidator
from ._interfaces import (
    BaseCodeGenerationService,
    BaseDependencyAnalysisService,
    BaseFlowgroupDiscoveryService,
    BaseFlowgroupResolutionService,
    BaseMonitoringFinalizerService,
    BasePipelineExecutionService,
    BaseValidationService,
)
from .executor import (  # noqa: F401 — kept for tests that monkeypatch the symbol
    OnValidationComplete,
    PipelineExecutionService,
    PipelineValidationOutcome,
    _GenerateWorkerState,
    _ValidateWorkerState,
    run_generate_pool,
    run_validate_pool,
)
from .monitoring_service import MonitoringFinalizerService
from .work_unit_builder import build_generate_work_units, build_validate_work_units


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

        # Seven typed service attributes, all typed by their ABC
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
        self._synthetic_contexts: Dict[Tuple[str, str], FlowGroupContext] = {}

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

        # Legacy aliases retained for test compatibility: orchestrator method
        # bodies use the canonical typed attributes; `self.processor` /
        # `self.generator` remain pointing at `self.processing` / `self.codegen`
        # because tests (test_cdc_fanin, test_append_flow, test_local_variables_e2e)
        # still reach in by the old names.
        self.processor = self.processing
        self.generator = self.codegen
        self._formatter = CodeFormatter()

        self._monitoring_result = None

        self._pipeline_slice_cache: Dict[str, List[FlowGroup]] = {}
        self._pipeline_slice_cache_id: Optional[int] = None

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

    def get_include_patterns(self) -> List[str]:
        return list(self.discovery.get_include_patterns())

    def discover_flowgroups(self, pipeline_dir: Path) -> List[FlowGroup]:
        """Discover all flowgroups in a specific pipeline directory.

        Directory-based discovery is not on the service ABC (which keys on
        pipeline field, not directory). Delegate to the discovery service's
        directory helper directly until the legacy directory path retires.
        """
        return self.discovery._legacy_discover_flowgroups_by_dir(pipeline_dir)  # type: ignore[attr-defined]

    def discover_all_flowgroups(self) -> List[FlowGroup]:
        """Discover disk-sourced flowgroups, then expand blueprints and monitoring.

        Side effects: populates ``self._synthetic_contexts`` and refreshes
        ``self._monitoring_result`` from the monitoring service's last build.
        """
        with perf_timer("discover_all_flowgroups [orchestrator]"):
            flowgroups = list(self.discovery.discover_flowgroups(pipeline_filter=None))

        with perf_timer(
            "Blueprint expansion",
            phase=True,
            parent_phase="Pipeline discovery",
        ):
            blueprint_ctxs, provenance = self._expand_blueprints()
        flowgroups.extend(ctx.flowgroup for ctx in blueprint_ctxs)
        self._synthetic_contexts = {
            (ctx.flowgroup.pipeline, ctx.flowgroup.flowgroup): ctx
            for ctx in blueprint_ctxs
        }
        if provenance:
            self.discovery.register_synthetic_sources(  # type: ignore[attr-defined]
                {key: prov.blueprint_path for key, prov in provenance.items()}
            )

        self._build_monitoring(flowgroups)
        if self._monitoring_result and self._monitoring_result.context is not None:
            monitoring_ctx = self._monitoring_result.context
            flowgroups.append(monitoring_ctx.flowgroup)
            self._synthetic_contexts[
                (monitoring_ctx.flowgroup.pipeline, monitoring_ctx.flowgroup.flowgroup)
            ] = monitoring_ctx
        return flowgroups

    def _expand_blueprints(
        self,
    ) -> Tuple[List[FlowGroupContext], Dict[Tuple[str, str], BlueprintProvenance]]:
        """Discover and expand blueprints + instances into synthetic FlowGroupContexts.

        Returns an empty result when no blueprint or instance files are present
        in the project (the entire feature is fully opt-in via file presence).
        """
        blueprints = self.blueprint_discoverer.discover_blueprints()
        if not blueprints:
            return [], {}

        instances = self.blueprint_discoverer.discover_instances(blueprints)
        if not instances:
            self.logger.info(
                f"Found {len(blueprints)} blueprint(s) but no instance files; "
                "blueprint expansion produces no flowgroups."
            )
            return [], {}

        return self.blueprint_expander.expand(blueprints, instances)

    def _build_monitoring(self, discovered_flowgroups: List[FlowGroup]):
        """Build monitoring artifacts via the monitoring service.

        Delegates to ``self.monitoring.build_flowgroup``; the service stashes
        the full :class:`MonitoringBuildResult` on its own ``last_build_result``
        property, which the orchestrator mirrors onto ``self._monitoring_result``
        for callers that read it directly.
        """
        self.monitoring.build_flowgroup(discovered_flowgroups)
        self._monitoring_result = self.monitoring.last_build_result  # type: ignore[attr-defined]
        return self._monitoring_result

    def finalize_monitoring_artifacts(self, env: str, output_dir: Path) -> None:
        """Reconcile monitoring artifacts: clean stale, write current.

        Pure pass-through to :meth:`MonitoringFinalizerService.finalize_artifacts`.
        Called AFTER the pipeline generation loop; the service handles
        notebook + job resource generation, cleanup of stale artifacts, and
        the add/remove/rename transitions.
        """
        self.monitoring.finalize_artifacts(env, output_dir)

    def _cleanup_monitoring_artifacts(self, env: str, output_dir: Path) -> None:
        self.monitoring.cleanup_artifacts(env, output_dir)

    def discover_flowgroups_by_pipeline_field(
        self,
        pipeline_field: str,
        pre_discovered_all_flowgroups: Optional[Sequence[FlowGroup]] = None,
    ) -> List[FlowGroup]:
        """Discover flowgroups matching a pipeline field across all directories.

        When ``pre_discovered_all_flowgroups`` is provided, filters from
        that list instead of re-discovering. Otherwise delegates to the
        discovery service's pipeline-filtered read.
        """
        if pre_discovered_all_flowgroups is not None:
            return [
                fg
                for fg in pre_discovered_all_flowgroups
                if fg.pipeline == pipeline_field
            ]
        with perf_timer(f"discover_by_pipeline_field [{pipeline_field}]"):
            return list(
                self.discovery.discover_flowgroups(pipeline_filter=pipeline_field)
            )

    def validate_duplicate_pipeline_flowgroup_combinations(
        self, flowgroups: List[FlowGroup]
    ) -> None:
        """Validate no duplicate pipeline+flowgroup combinations exist.

        Pure pass-through to :meth:`ValidationService.validate_duplicates`,
        which raises :class:`LHPValidationError` on duplicates with the same
        error code, title, suggestions, and context as the legacy inline path.
        """
        self.validation.validate_duplicates(flowgroups)

    def _lookup_pipeline_slice(
        self,
        all_flowgroups: List[FlowGroup],
        pipeline_field: str,
    ) -> List[FlowGroup]:
        """Return the per-pipeline slice with a memoized by-pipeline grouping.

        The by-pipeline dict is keyed by `id(all_flowgroups)`; on a fresh
        `discover_all_flowgroups` result, the dict is rebuilt once and reused
        for every subsequent pipeline call. At 32k-flowgroup scale this turns
        80×32k iterations into one full scan amortized across all pipelines.
        """
        if self._pipeline_slice_cache_id != id(all_flowgroups):
            grouping: Dict[str, List[FlowGroup]] = defaultdict(list)
            for fg in all_flowgroups:
                grouping[fg.pipeline].append(fg)
            self._pipeline_slice_cache = dict(grouping)
            self._pipeline_slice_cache_id = id(all_flowgroups)
        return self._pipeline_slice_cache.get(pipeline_field, [])

    def _invalidate_pipeline_slice_cache(self) -> None:
        """Reset the by-pipeline grouping cache.

        The cache keys on ``id(all_flowgroups)``; Python may reuse that id
        after the list is GC'd, so the plural entry points clear the cache
        on each invocation.
        """
        self._pipeline_slice_cache.clear()
        self._pipeline_slice_cache_id = None

    def generate_pipelines(
        self,
        *,
        pipeline_filter: Optional[str] = None,
        pipeline_fields: Optional[Sequence[str]] = None,
        env: str,
        output_dir: Optional[Path] = None,
        specific_flowgroups: Optional[List[str]] = None,
        include_tests: bool = False,
        pre_discovered_all_flowgroups: Optional[Sequence[FlowGroup]] = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[Callable[["PipelineDelta"], None]] = None,
        on_pipeline_start: Optional[Callable[[str], None]] = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> Dict[str, tuple[str, ...]]:
        """Build work units, hand to PipelineExecutionService.run_generate, aggregate.

        Exactly one of ``pipeline_filter`` (single pipeline by field) or
        ``pipeline_fields`` (batch by field list) may be supplied. When
        both are ``None`` no pipelines are generated and an empty mapping
        is returned (the caller is expected to discover the pipeline
        list first; see :class:`GenerationFacade`).
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

        self._invalidate_pipeline_slice_cache()
        self.logger.info(
            f"Starting batch pipeline generation: {len(effective_fields)} pipeline(s) for env: {env}"
        )
        work_units = self._build_generate_work_units(
            pipeline_fields=effective_fields,
            env=env,
            output_dir=output_dir,
            specific_flowgroups=specific_flowgroups,
            include_tests=include_tests,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            warning_collector=warning_collector,
        )
        self.execution.configure_generate(
            max_workers=max_workers if max_workers is not None else self.max_workers,
            on_pipeline_start=on_pipeline_start,
            on_pipeline_complete=on_pipeline_complete,
            environment=env,
            include_tests=include_tests,
            worker_state=self._build_generate_worker_state(env, include_tests),
        )
        return self._aggregate_generate_outcomes(
            self.execution.run_generate(work_units)
        )

    def _build_generate_worker_state(
        self,
        env: str,
        include_tests: bool,
    ) -> _GenerateWorkerState:
        return _GenerateWorkerState(
            processor=self.processing,
            code_generator=self.codegen,
            formatter=self._formatter,
            substitution_managers={},
            pipeline_output_dirs={},
            environment=env,
            project_root=self.project_root,
            project_config=self.project_config,
            include_tests=include_tests,
        )

    def _build_generate_work_units(
        self,
        *,
        pipeline_fields: Sequence[str],
        env: str,
        output_dir: Optional[Path],
        specific_flowgroups: Optional[List[str]],
        include_tests: bool,
        pre_discovered_all_flowgroups: Optional[Sequence[FlowGroup]],
        warning_collector: Optional["WarningCollector"],
    ) -> Tuple[PipelineWorkUnit, ...]:
        """Thin delegator to :func:`work_unit_builder.build_generate_work_units`."""
        return build_generate_work_units(
            self,
            pipeline_fields=pipeline_fields,
            env=env,
            output_dir=output_dir,
            specific_flowgroups=specific_flowgroups,
            include_tests=include_tests,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            warning_collector=warning_collector,
        )

    def _aggregate_generate_outcomes(
        self,
        deltas: Sequence["PipelineDelta"],
    ) -> Dict[str, tuple[str, ...]]:
        """Bucket deltas; raise the appropriate aggregate error on any failure.

        Single-failure: re-raise the live :class:`LHPError` from the worker
        (preserves structured display) or rebuild one via
        :func:`lhp_error_from_worker_failure` when the worker raised a
        non-LHP exception. Multi-failure: synthesize :class:`LHPValidationError`
        ``902`` listing each failing pipeline's error code/title.
        """
        successful = [d for d in deltas if d.success]
        failed = [d for d in deltas if not d.success]

        if failed:
            if len(failed) == 1:
                d = failed[0]
                if d.lhp_error is not None:
                    raise d.lhp_error
                raise lhp_error_from_worker_failure(
                    pipeline_name=d.pipeline_name,
                    error_type=d.error_type or "UnknownError",
                    error_message=d.error_message or "(no message)",
                    error_traceback=d.error_traceback or "",
                )
            by_pipeline: Dict[str, str] = {}
            for d in failed:
                if d.lhp_error is not None:
                    by_pipeline[d.pipeline_name] = (
                        f"{d.lhp_error.code} ({d.lhp_error.title})"
                    )
                else:
                    by_pipeline[d.pipeline_name] = (
                        f"{d.error_type or 'UnknownError'} (non-LHP exception)"
                    )
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="902",
                title=f"{len(failed)} pipeline(s) failed",
                details=(
                    f"{len(failed)} of {len(failed) + len(successful)} pipelines "
                    f"failed during generation. See per-pipeline rows in the "
                    f"summary table for full diagnostics."
                ),
                context={"failure_count": len(failed), **by_pipeline},
                suggestions=[
                    "Inspect the summary table above for per-pipeline status",
                    "Run 'lhp validate' for detailed per-flowgroup diagnostics",
                ],
            )

        return {delta.pipeline_name: delta.generated_filenames for delta in successful}

    def _find_source_yaml_for_flowgroup(self, flowgroup: FlowGroup) -> Optional[Path]:
        """Find the source YAML for a flowgroup (multi-doc / array supported)."""
        return self.discovery.find_source_yaml_for_flowgroup(flowgroup)  # type: ignore[attr-defined]

    def _make_context(self, fg: FlowGroup) -> FlowGroupContext:
        """Wrap a FlowGroup in its FlowGroupContext for the worker boundary.

        Looks up synthetic provenance (synthetic flag, auxiliary_files) from
        `self._synthetic_contexts`; disk-sourced flowgroups get default values.
        Source YAML is resolved via the FlowgroupDiscoveryService (threading.Lock'd
        index — must run on the main process before spawn).
        """
        source_yaml = self._find_source_yaml_for_flowgroup(fg)
        if self._synthetic_contexts:
            existing = self._synthetic_contexts.get((fg.pipeline, fg.flowgroup))
            if existing is not None:
                return replace(existing, flowgroup=fg, source_yaml=source_yaml)
        return FlowGroupContext(flowgroup=fg, source_yaml=source_yaml)

    def process_flowgroup(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        include_tests: bool = True,
    ) -> FlowGroup:
        """Expand templates, apply presets and substitutions.

        Back-compat shim around :meth:`FlowgroupResolutionService.resolve`
        for callers that pass/expect a FlowGroup rather than a
        FlowGroupContext.
        """
        ctx_in = self._make_context(flowgroup)
        ctx_out = self.processing.resolve(
            ctx_in, substitution_mgr, include_tests=include_tests
        )
        return ctx_out.flowgroup

    def generate_flowgroup_code(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        output_dir: Optional[Path] = None,
        source_yaml: Optional[Path] = None,
        env: Optional[str] = None,
        include_tests: bool = False,
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
    ) -> str:
        """Generate complete Python code for a flowgroup.

        Args:
            phase_a_records: Optional list passed by Phase A workers in the
                cross-pipeline flat pool; when supplied, the file copier
                appends :class:`CopiedModuleRecord` entries to it instead
                of writing to disk. Phase B replays those records.
        """
        return self.codegen.generate(
            flowgroup,
            substitution_mgr,
            output_dir=output_dir,
            source_yaml=source_yaml,
            env=env,
            include_tests=include_tests,
            phase_a_records=(
                tuple(phase_a_records) if phase_a_records is not None else None
            ),
        )

    def _discover_and_filter_flowgroups(
        self,
        env: str,
        pipeline_identifier: str,
        include_tests: bool,
        specific_flowgroups: List[str] | None = None,
        use_directory_discovery: bool = False,
        pre_discovered_flowgroups: Optional[List[FlowGroup]] = None,
    ) -> List[FlowGroup]:
        """Delegator — see FlowgroupDiscoveryService.discover_and_filter_for_pipeline.

        Preserved for test pin: ``tests/unit/test_source_path_index.py``
        patches/calls this name. Production paths go through
        :meth:`FlowgroupDiscoveryService.discover_and_filter_for_pipeline`
        directly.
        """
        return self.discovery.discover_and_filter_for_pipeline(  # type: ignore[attr-defined]
            env=env,
            pipeline_identifier=pipeline_identifier,
            include_tests=include_tests,
            specific_flowgroups=specific_flowgroups,
            use_directory_discovery=use_directory_discovery,
            pre_discovered_flowgroups=pre_discovered_flowgroups,
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
        warning_collector: Optional["WarningCollector"] = None,
    ) -> List[PipelineValidationOutcome]:
        """Build work units, hand to PipelineExecutionService.run_validate.

        Exactly one of ``pipeline_filter`` (single pipeline by field) or
        ``pipeline_fields`` (batch by field list) may be supplied. When
        both are ``None`` no pipelines are validated and an empty list
        is returned (the caller is expected to discover the pipeline
        list first; see :class:`ValidationFacade`).
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

        self._invalidate_pipeline_slice_cache()
        work_units = self._build_validate_work_units(
            pipeline_fields=effective_fields,
            env=env,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            warning_collector=warning_collector,
        )
        self.execution.configure_validate(
            max_workers=max_workers if max_workers is not None else self.max_workers,
            include_tests=include_tests,
            validation_service=self.validation,
            on_pipeline_complete=on_pipeline_complete,
            worker_state=self._build_validate_worker_state(include_tests),
        )
        return list(self.execution.run_validate(work_units))

    def _build_validate_worker_state(self, include_tests: bool) -> _ValidateWorkerState:
        return _ValidateWorkerState(
            processor=self.processing,
            substitution_managers={},
            include_tests=include_tests,
        )

    def _build_validate_work_units(
        self,
        *,
        pipeline_fields: Sequence[str],
        env: str,
        pre_discovered_all_flowgroups: Optional[Sequence[FlowGroup]],
        warning_collector: Optional["WarningCollector"],
    ) -> Tuple[PipelineWorkUnit, ...]:
        """Thin delegator to :func:`work_unit_builder.build_validate_work_units`."""
        return build_validate_work_units(
            self,
            pipeline_fields=pipeline_fields,
            env=env,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            warning_collector=warning_collector,
        )
