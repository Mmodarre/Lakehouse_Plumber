"""Main orchestration for LakehousePlumber pipeline generation.

:class:`ActionOrchestrator` is the composition root wiring eight
ABC-typed collaborator services (discovery, flowgroup resolution,
validation, code generation, dependency analysis, monitoring,
execution, bootstrap) and exposing seven public methods to callers
(CLI commands and :class:`LakehousePlumberApplicationFacade`).

Per Target Architecture §4 (thin coordination layer), callers use the
public services directly (``.bootstrap``, ``.discovery``,
``.processing``, ``.codegen``); the orchestrator exposes only these seven
methods, each the narrowest surface for its responsibility:
``discover_flowgroups`` (single-pipeline directory read);
``finalize_monitoring_artifacts`` (end-of-run notebook/job write);
``generate_pipelines`` (pure delta-generator — builds the batch worklist,
hands to :class:`PipelineExecutionService`, does NOT format);
``resolve_apply_formatting`` (single seam resolving the
``apply_formatting`` tri-state to a bool);
``format_output_tree`` (single terminal ``ruff format`` pass over the
output tree — perf-wrapped delegate to ``codegen.formatter``);
``validate_pipelines`` (batch validate; hands to
:class:`PipelineExecutionService`);
``build_sandbox_rewrite_plan`` (``--sandbox`` pre-pass — thin
delegate to ``coordination.sandbox_prepass``). The format primitive
(``resolve_apply_formatting`` + ``format_output_tree``) lives here but is
INVOKED by each caller AFTER it drains ``generate_pipelines``.
"""

# JUSTIFIED: Constructor wires eight ABC-typed collaborator services
# inline (~170L) per §4.10/§4.12, including the §9.24 injection branch
# and back-compat construction for direct `ActionOrchestrator(project_root)`
# test callers at tests/test_orchestrator.py:607.
# Under §9.3's 800-line hard cap.

import logging
import os
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Generator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from lhp.models import FlowGroup

if TYPE_CHECKING:
    from ...models.processing import (
        PipelineDelta,
        RunWarningRecord,
        SandboxRunConfig,
    )
    from ..sandbox import SandboxRewritePlan, SandboxTableRenames

from ...parsers.blueprint_parser import BlueprintParser
from ...parsers.parse_cache import PersistentParseCache
from ...parsers.yaml_parser import CachingYAMLParser, YAMLParser
from ...presets.preset_manager import PresetManager
from ...utils.performance_timer import perf_timer
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
)
from ..codegen.coordinator import CodeGenerationService
from ..codegen.formatter import format_generated_tree
from ..dependencies import (
    DependencyAnalysisService,
    DependencyResolver,
    PersistentGraphCache,
)
from ..discovery.blueprint_discoverer import BlueprintDiscoverer
from ..discovery.flowgroup_discoverer import FlowgroupDiscoveryService
from ..loaders import PipelineConfigLoader, ProjectConfigLoader
from ..loaders.version_enforcement import enforce_version_requirements
from ..processing import TemplateEngine
from ..processing.blueprint_expander import BlueprintExpander
from ..registry import ActionRegistry, OrchestrationDependencies
from ..validators import ConfigValidator, SecretValidator
from ._flowgroup_pool import _FlowgroupWorkerState
from .bootstrap_service import FlowgroupBootstrapService
from .executor import (
    PipelineExecutionService,
    PipelineValidationOutcome,
)
from .flowgroup_worklist_builder import build_flowgroup_worklist
from .monitoring_service import MonitoringFinalizerService
from .sandbox_prepass import build_sandbox_rewrite_plan as _build_sandbox_rewrite_plan


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
        no_cache: bool = False,
    ):
        """Initialize orchestrator.

        Args:
            max_workers: Worker count for the parallel pool (generate
                parallelizes per pipeline, validate per flowgroup). If
                ``None``, resolves to ``LHP_MAX_WORKERS`` env var, else
                :func:`_auto_max_workers` (~80% of OS-visible CPU count,
                honoring cgroup CPU limits on Linux). ``1`` is sequential.
            no_cache: Disable the persistent on-disk parse cache for this
                run. Also disabled when the ``LHP_NO_CACHE`` env var is
                truthy (``"1"``, ``"true"``, ``"yes"``). When enabled
                (the default), one :class:`PersistentParseCache` under
                ``<project_root>/.lhp/cache/parse`` is shared by this
                orchestrator's ``CachingYAMLParser`` and the
                :class:`DependencyAnalysisService`'s.
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

        # Resolved BEFORE service construction so the worker budget can be
        # threaded into the services built below (discovery's cold parse
        # pool, DependencyAnalysisService's own discoverer).
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

        self.yaml_parser = YAMLParser()
        # Persistent parse cache: default ON; disabled by the ``no_cache``
        # param or a truthy LHP_NO_CACHE (same idiom as LHP_IGNORE_VERSION).
        disable_parse_cache = no_cache or os.environ.get(
            "LHP_NO_CACHE", ""
        ).lower() in ("1", "true", "yes")
        self._persistent_parse_cache: Optional[PersistentParseCache] = None
        if disable_parse_cache:
            self.logger.debug(
                "Persistent parse cache disabled (no_cache flag or LHP_NO_CACHE)"
            )
        else:
            self._persistent_parse_cache = PersistentParseCache(
                project_root / ".lhp" / "cache" / "parse"
            )
        # Persistent graph build-cache: same on/off gate as the parse cache.
        # Survives restarts so a fresh ``lhp dag`` / restarted ``lhp web``
        # serves the assembled dependency graph on a HIT, skipping the
        # discovery + parse + build the parse cache alone cannot avoid.
        self._persistent_graph_cache: Optional[PersistentGraphCache] = None
        if disable_parse_cache:
            self.logger.debug(
                "Persistent graph cache disabled (no_cache flag or LHP_NO_CACHE)"
            )
        else:
            self._persistent_graph_cache = PersistentGraphCache(
                project_root / ".lhp" / "cache" / "graph",
                project_root,
            )
            self._persistent_graph_cache.sweep()
        self._cached_yaml_parser = CachingYAMLParser(
            self.yaml_parser, persistent_cache=self._persistent_parse_cache
        )
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
            max_workers=self.max_workers,
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
            persistent_parse_cache=self._persistent_parse_cache,
            persistent_graph_cache=self._persistent_graph_cache,
            max_workers=self.max_workers,
        )
        self.monitoring: BaseMonitoringFinalizerService = MonitoringFinalizerService(
            project_config=self.project_config,
            project_root=self.project_root,
            dependencies=self._orchestration_dependencies,
            pipeline_config_path=self.pipeline_config_path,
            logger=self.logger,
        )
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

        Called AFTER the pipeline generation loop.
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
        pre_discovered_all_flowgroups: Optional[Sequence[FlowGroup]] = None,
        max_workers: Optional[int] = None,
        packaging_modes: Optional[Mapping[str, str]] = None,
        on_total: Optional[Callable[[int], None]] = None,
        on_flowgroup_done: Optional[Callable[[str], None]] = None,
        sandbox_plan: Optional["SandboxRewritePlan"] = None,
    ) -> Generator["PipelineDelta", None, Tuple["RunWarningRecord", ...]]:
        """Build the flat worklist, hand to PipelineExecutionService.run_generate.

        Exactly one of ``pipeline_filter`` (single pipeline by field) or
        ``pipeline_fields`` (batch by field list) may be supplied. When
        both are ``None`` no pipelines are generated and the stream is
        empty (the caller is expected to discover the pipeline list first;
        see :class:`GenerationFacade`).

        This is a PURE delta-generator: it does NOT format. There is no
        ``apply_formatting`` parameter — formatting is the caller's
        responsibility AFTER draining this generator. The two callers (the
        generate event stream and the read-only plan path) each resolve the
        ``apply_formatting`` tri-state via :meth:`resolve_apply_formatting` and
        run the single terminal ruff pass themselves (:meth:`format_output_tree`);
        the execution service never formats either.

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
        an aggregate-and-raise step), then commits each clean pipeline (writing
        UNFORMATTED source). This is a GENERATOR: it ``yield from``
        ``run_generate``, surfacing the per-pipeline :class:`PipelineDelta`s in
        input order (every failure delta first, then the gate raise per §1.4,
        then a success delta per committed pipeline); once that stream drains
        cleanly it runs the terminal format here. It RETURNS (via
        ``StopIteration.value``, captured by the facade's ``yield from``) the
        batch's merged + deduped worker deprecation warnings for the facade to
        re-emit as :class:`~lhp.api.WarningEmitted` events.
        A per-pipeline discovery failure (carried in the worklist's
        ``discovery_errors`` map) aborts the whole batch inside ``run_generate``
        — generate is all-or-nothing.

        ``sandbox_plan`` is the ``--sandbox`` rewrite plan from
        :meth:`build_sandbox_rewrite_plan`; its ``renames`` ride the worker
        state across the spawn boundary so every worker applies the
        table-rename hooks (structured pass on the resolved flowgroup +
        text pass over the generated Python). ``None`` (the default) is the
        legacy no-rewrite path, byte-identical to a run without the kwarg.
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
            return ()

        self.logger.info(
            f"Starting batch pipeline generation: {len(effective_fields)} pipeline(s) for env: {env}"
        )
        if packaging_modes is None:
            packaging_modes = self._resolve_packaging_modes(list(effective_fields))
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
        )
        self.execution.configure_generate(
            max_workers=max_workers if max_workers is not None else self.max_workers,
            environment=env,
            include_tests=include_tests,
            validation_service=self.validation,
            worker_state=self._build_generate_worker_state(
                env,
                include_tests,
                table_renames=(
                    sandbox_plan.renames if sandbox_plan is not None else None
                ),
            ),
        )
        # The execution service writes UNFORMATTED source and does not format:
        # drain its delta-stream and RETURN. The single terminal ruff pass is NOT
        # run here — this method is a pure delta-generator. Its two callers each
        # drive the format primitive themselves AFTER draining: the generate
        # event stream (:mod:`lhp.api._generate_stream`) surfaces it as a §5.7
        # ``format`` phase, and the read-only plan path
        # (:func:`lhp.core.codegen.plan_builder.build_generation_plan`) runs it
        # over its temp tree for byte-for-byte plan/generate parity. Both resolve
        # the tri-state via :meth:`resolve_apply_formatting` and call
        # :meth:`format_output_tree`.
        return (
            yield from self.execution.run_generate(
                flowgroups_by_pipeline=flowgroups_by_pipeline,
                substitution_managers=substitution_managers,
                output_dirs=output_dirs,
                discovery_errors=discovery_errors,
                output_dir=output_dir,
                project_config=self.project_config,
                project_root=self.project_root,
                env=env,
                packaging_modes=packaging_modes,
                max_workers=max_workers,
                on_total=on_total,
                on_flowgroup_done=on_flowgroup_done,
            )
        )

    def _resolve_packaging_modes(self, pipeline_names: List[str]) -> Mapping[str, str]:
        """Resolve each pipeline's packaging mode from ``pipeline_config``.

        Thin delegation to
        :meth:`PipelineConfigLoader.resolve_packaging_modes`; with no
        per-pipeline ``packaging`` set every entry is ``"source"`` (today's
        behavior). The coordinator passes the map to ``run_generate`` for the
        commit-step wheel branch — it never crosses the worker/spawn boundary.
        """
        return PipelineConfigLoader(
            self.project_root, self.pipeline_config_path
        ).resolve_packaging_modes(pipeline_names)

    def resolve_apply_formatting(self, apply_formatting: bool | None) -> bool:
        """Resolve the tri-state terminal-format override to a plain bool.

        The single seam for the ``apply_formatting`` tri-state (constitution
        §4.1): ``None`` means "use the loaded project's ``lhp.yaml``
        ``apply_formatting`` setting" — ``True`` when there is no project config
        — while ``True`` / ``False`` override it. Held here, on the orchestrator
        (which owns ``project_config``), so neither caller of the format
        primitive duplicates the rule.

        PUBLIC because the format invocation lives OUTSIDE this class — the
        generate event stream (``lhp.api._generate_stream``) and the plan path
        (``core.codegen.plan_builder``) both resolve the override here, then
        guard their :meth:`format_output_tree` call on the result (§9.23 forbids
        the ``api`` layer reaching a private orchestrator method).
        """
        if apply_formatting is None:
            return (
                self.project_config.apply_formatting
                if self.project_config is not None
                else True
            )
        return apply_formatting

    def format_output_tree(self, output_dir: Path) -> None:
        """Run the single terminal ``ruff format`` pass over ``output_dir``.

        Thin delegate to :func:`lhp.core.codegen.formatter.format_generated_tree`,
        wrapped in the ``format_tree`` perf span. Held on the orchestrator (the
        composition root, permitted to call ``codegen`` functions directly per
        §5) so callers ABOVE ``core`` invoke the format through a single legal
        ``api → core`` seam rather than importing ``codegen`` directly. The
        PRIMITIVE lives here; the INVOCATION lives with each caller AFTER it
        drains :meth:`generate_pipelines`: the generate event stream
        (:mod:`lhp.api._generate_stream`) surfaces it as a §5.7 ``format`` phase,
        and the read-only plan path
        (:func:`lhp.core.codegen.plan_builder.build_generation_plan`) runs it over
        its temp tree for byte parity. The commit step writes UNFORMATTED source
        verbatim; this formats it ONCE, over the whole tree.

        :raises lhp.errors.LHPError: ``LHP-CFG-033`` if ruff exits non-zero, or
            ``LHP-CFG-034`` if the ruff executable cannot be located.
        """
        with perf_timer("format_tree", category="format_tree"):
            format_generated_tree(output_dir)

    def build_sandbox_rewrite_plan(
        self,
        env: str,
        run: "SandboxRunConfig",
        flowgroups: List[FlowGroup],
    ) -> "SandboxRewritePlan":
        """Build the ``--sandbox`` table-rewrite plan (pre-pass).

        Thin delegation seam to
        :func:`.sandbox_prepass.build_sandbox_rewrite_plan`, mirroring the
        :meth:`format_output_tree` precedent: the PRIMITIVE lives in its own
        coordination module; the orchestrator (composition root) exposes the
        single legal ``api → core`` seam and supplies the collaborators it
        already owns (resolution service, bootstrap service, substitution
        factory, project root). Main-thread only, after the discover phase:
        ``flowgroups`` is the discovered (blueprint-expanded) set the caller
        already holds; ``run.pipelines`` is the already-resolved concrete
        scope.
        """
        return _build_sandbox_rewrite_plan(
            processing=self.processing,
            bootstrap=self.bootstrap,
            orchestration_dependencies=self._orchestration_dependencies,
            project_root=self.project_root,
            env=env,
            run=run,
            flowgroups=flowgroups,
        )

    def _build_generate_worker_state(
        self,
        env: str,
        include_tests: bool,
        table_renames: Optional["SandboxTableRenames"] = None,
    ) -> _FlowgroupWorkerState:
        """Build the unified worker state for the flat engine (both modes).

        The single canonical builder — :meth:`_build_validate_worker_state`
        delegates here, since the ``_FlowgroupWorkerState`` shape is identical
        for validate and generate (validate ignores the generate-only fields).
        ``substitution_managers`` / ``pipeline_output_dirs`` are placeholders;
        ``run_generate`` replaces them per batch from the worklist builder's maps.
        ``project_config`` / ``project_root`` are deliberately NOT on this carrier
        (commit-step inputs passed to ``run_generate`` separately, never crossing
        the spawn boundary). No formatter on the worker: the worker only
        ``ast.parse``-validates; the single terminal ruff pass
        (:meth:`format_output_tree`) is driven by the caller — the generate event
        stream or the plan path — after :meth:`generate_pipelines` drains.
        ``table_renames`` is the ``--sandbox`` rename set (``None`` outside
        sandbox runs); it rides the state across the spawn boundary so each
        worker can apply the D7 rewrite hooks.
        """
        return _FlowgroupWorkerState(
            processor=self.processing,
            substitution_managers={},
            include_tests=include_tests,
            code_generator=self.codegen,
            pipeline_output_dirs={},
            environment=env,
            table_renames=table_renames,
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
        on_total: Optional[Callable[[int], None]] = None,
        on_flowgroup_done: Optional[Callable[[str], None]] = None,
        sandbox_plan: Optional["SandboxRewritePlan"] = None,
    ) -> Generator[PipelineValidationOutcome, None, Tuple["RunWarningRecord", ...]]:
        """Build the flat worklist, hand to PipelineExecutionService.run_validate.

        Exactly one of ``pipeline_filter`` (single pipeline by field) or
        ``pipeline_fields`` (batch by field list) may be supplied. When
        both are ``None`` no pipelines are validated and the stream is
        empty (the caller is expected to discover the pipeline list first;
        see :class:`ValidationFacade`).

        Routes through the consolidated flat per-flowgroup engine,
        mirroring :meth:`generate_pipelines`:
        :func:`flowgroup_worklist_builder.build_flowgroup_worklist` produces
        the flat four-map shape (``output_dir=None`` — validate writes
        nothing), which :meth:`PipelineExecutionService.run_validate` drives
        in ``mode=\"validate\"``. Cross-flowgroup validation now runs on the
        RESOLVED flowgroups inside the engine (§9.24).

        This is a GENERATOR: it ``yield from`` ``run_validate``, surfacing the
        per-pipeline :class:`PipelineValidationOutcome`s in DETERMINISTIC INPUT
        PIPELINE ORDER, and RETURNS (via ``StopIteration.value``, captured by
        the facade's ``yield from``) the batch's merged + deduped worker
        deprecation warnings for the facade to re-emit as
        :class:`~lhp.api.WarningEmitted` events. The facade consumes this stream
        and renders the §5.7 progress events (a paired ``PipelineStarted`` +
        terminal per outcome). Validate REPORTS findings — there is no
        gate/raise, so the stream simply runs to completion (a discovery failure
        folds to an unsuccessful outcome, not a batch abort — unlike generate).

        ``sandbox_plan`` is the ``--sandbox`` rewrite plan from
        :meth:`build_sandbox_rewrite_plan`; validate applies ONLY the
        structured rewrite on each resolved flowgroup (no generated text to
        rewrite, no ``LHP-VAL-066`` — a documented v1 limitation), so the
        cross-flowgroup barrier and validation see the sandbox names.
        ``None`` (the default) is the legacy no-rewrite path.
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
            return ()

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
        )
        self.execution.configure_validate(
            max_workers=max_workers if max_workers is not None else self.max_workers,
            include_tests=include_tests,
            validation_service=self.validation,
            worker_state=self._build_validate_worker_state(
                env,
                include_tests,
                table_renames=(
                    sandbox_plan.renames if sandbox_plan is not None else None
                ),
            ),
        )
        return (
            yield from self.execution.run_validate(
                flowgroups_by_pipeline=flowgroups_by_pipeline,
                substitution_managers=substitution_managers,
                output_dirs=output_dirs,
                discovery_errors=discovery_errors,
                on_total=on_total,
                on_flowgroup_done=on_flowgroup_done,
            )
        )

    def _build_validate_worker_state(
        self,
        env: str,
        include_tests: bool,
        table_renames: Optional["SandboxTableRenames"] = None,
    ) -> _FlowgroupWorkerState:
        """Build the unified worker state for the flat-engine validate path.

        The ``_FlowgroupWorkerState`` shape is identical for both modes (one
        state serves validate and generate); validate simply ignores the
        generate-only collaborators. Delegates to the single canonical builder
        rather than duplicate the constructor (constitution §4.1).
        """
        return self._build_generate_worker_state(
            env, include_tests, table_renames=table_renames
        )
