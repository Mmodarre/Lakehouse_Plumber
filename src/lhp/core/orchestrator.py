"""Main orchestration for LakehousePlumber pipeline generation."""

import logging
import os
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

from ..models.config import Action, FlowGroup, FlowGroupContext

if TYPE_CHECKING:
    from ..generators.python_file_copier import CopiedModuleRecord
    from ..models.processing import PipelineDelta

from ..parsers.blueprint_parser import BlueprintParser

# Component imports (for service initialization)
from ..parsers.yaml_parser import CachingYAMLParser, YAMLParser
from ..presets.preset_manager import PresetManager
from ..utils.error_formatter import (
    ErrorCategory,
    LHPConfigError,
    LHPError,
    LHPFileError,
    LHPValidationError,
    lhp_error_from_worker_failure,
)
from ..utils.file_header import write_normalized
from ..utils.formatter import CodeFormatter
from ..utils.performance_timer import perf_timer
from ..utils.source_extractor import (
    extract_single_source_view,
    extract_source_views_from_action,
)
from ..utils.substitution import EnhancedSubstitutionManager
from ..utils.version import get_version
from .action_registry import ActionRegistry
from .dependency_resolver import DependencyResolver
from .factories import OrchestrationDependencies
from .pipeline_executor import (
    FlowgroupValidationResult,
    OnValidationComplete,
    PipelineValidationOutcome,
    _GenerateWorkerState,
    _ValidateWorkerState,
    run_generate_pool,
    run_validate_pool,
)
from .project_config_loader import ProjectConfigLoader
from .secret_validator import SecretValidator
from .services.blueprint_discoverer import BlueprintDiscoverer
from .services.blueprint_expander import BlueprintExpander, BlueprintProvenance
from .services.code_generator import CodeGenerator

# Service imports
from .services.flowgroup_discoverer import FlowgroupDiscoverer
from .services.flowgroup_processor import FlowgroupProcessor
from .services.pipeline_validator import PipelineValidator
from .template_engine import TemplateEngine
from .validator import ConfigValidator


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
    """
    Main orchestration for pipeline generation (Service-based architecture).

    Implements the business layer interface and coordinates specialized services
    for discovery, processing, generation, and validation while maintaining
    the same public API for backward compatibility.
    """

    def __init__(
        self,
        project_root: Path,
        enforce_version: bool = True,
        dependencies: OrchestrationDependencies = None,
        pipeline_config_path: Optional[str] = None,
        max_workers: Optional[int] = None,
    ):
        """
        Initialize orchestrator with service composition and dependency injection.

        Args:
            project_root: Root directory of the LakehousePlumber project
            enforce_version: Whether to enforce version requirements (default: True)
            dependencies: Optional dependency container for injection (uses defaults if None)
            pipeline_config_path: Optional path to custom pipeline config file (relative to project_root)
            max_workers: Maximum worker processes for the parallel pool
                (generate parallelizes per pipeline, validate per flowgroup).
                If None, resolves in priority order: ``LHP_MAX_WORKERS`` env
                var, else :func:`_auto_max_workers` (~80% of OS-visible CPU
                count, honoring cgroup CPU limits on Linux). ``1`` is
                sequential.
        """
        self.project_root = project_root
        self.enforce_version = enforce_version
        self.dependencies = dependencies or OrchestrationDependencies()
        self.pipeline_config_path = pipeline_config_path
        self.logger = logging.getLogger(__name__)

        # Initialize core components (still needed for services)
        self.yaml_parser = YAMLParser()
        self._cached_yaml_parser = CachingYAMLParser(self.yaml_parser)
        self.preset_manager = PresetManager(project_root / "presets")
        self.template_engine = TemplateEngine(project_root / "templates")
        self.project_config_loader = ProjectConfigLoader(project_root)
        self.action_registry = ActionRegistry()
        self.secret_validator = SecretValidator()
        self.dependency_resolver = DependencyResolver()

        # Load project configuration (needed for validator)
        self.project_config = self.project_config_loader.load_project_config()

        # Initialize config validator with project config for metadata validation
        self.config_validator = ConfigValidator(project_root, self.project_config)

        # Initialize services with component dependencies
        self.discoverer = FlowgroupDiscoverer(
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
        self._blueprint_provenance: Dict[Tuple[str, str], BlueprintProvenance] = {}
        self._synthetic_contexts: Dict[Tuple[str, str], FlowGroupContext] = {}
        self.processor = FlowgroupProcessor(
            self.template_engine,
            self.preset_manager,
            self.config_validator,
            self.secret_validator,
        )
        self.generator = CodeGenerator(
            self.action_registry,
            self.dependency_resolver,
            self.preset_manager,
            self.project_config,
            project_root,
        )
        self.validator = PipelineValidator(
            project_root, self.config_validator, self.secret_validator
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

    @property
    def cached_yaml_parser(self) -> CachingYAMLParser:
        """Public accessor for the shared CachingYAMLParser instance."""
        return self._cached_yaml_parser

    def _enforce_version_requirements(self) -> None:
        """Enforce version requirements if specified in project config."""
        # Skip if no project config or no version requirement
        if not self.project_config or not self.project_config.required_lhp_version:
            return

        # Check for bypass environment variable
        if os.environ.get("LHP_IGNORE_VERSION", "").lower() in ("1", "true", "yes"):
            self.logger.warning(
                f"Version requirement bypass enabled via LHP_IGNORE_VERSION. "
                f"Required: {self.project_config.required_lhp_version}"
            )
            return

        try:
            from packaging.specifiers import SpecifierSet
            from packaging.version import Version
        except ImportError:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="006",
                title="Missing packaging dependency",
                details="The 'packaging' library is required for version range checking but is not installed.",
                suggestions=[
                    "Install packaging: pip install packaging>=23.2",
                    "Or set LHP_IGNORE_VERSION=1 to bypass version checking",
                ],
            )

        required_spec = self.project_config.required_lhp_version
        actual_version = get_version()

        try:
            spec_set = SpecifierSet(required_spec)
            actual_ver = Version(actual_version)

            if actual_ver not in spec_set:
                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="007",
                    title="LakehousePlumber version requirement not satisfied",
                    details=f"Project requires LakehousePlumber version '{required_spec}', but version '{actual_version}' is installed.",
                    suggestions=[
                        f"Install a compatible version: pip install 'lakehouse-plumber{required_spec}'",
                        f"Or update the project's version requirement in lhp.yaml if you intend to upgrade",
                        "Or set LHP_IGNORE_VERSION=1 to bypass version checking (not recommended for production)",
                    ],
                    context={
                        "Required Version": required_spec,
                        "Installed Version": actual_version,
                        "Project Name": self.project_config.name,
                    },
                )
        except Exception as e:
            if isinstance(e, LHPError):
                raise
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="008",
                title="Invalid version requirement specification",
                details=f"Could not parse version requirement '{required_spec}': {e}",
                suggestions=[
                    "Use valid PEP 440 version specifiers (e.g., '>=0.4.1,<0.5.0')",
                    "Check the required_lhp_version field in lhp.yaml",
                    "Examples: '==0.4.1', '~=0.4.1', '>=0.4.1,<0.5.0'",
                ],
            )

    def get_include_patterns(self) -> List[str]:
        """
        Get include patterns from project configuration.

        Returns:
            List of include patterns, or empty list if none specified
        """
        return self.discoverer.get_include_patterns()

    # ============================================================================
    # BUSINESS LAYER INTERFACE IMPLEMENTATION
    # ============================================================================

    def validate_configuration(self, pipeline_identifier: str, env: str) -> tuple:
        """Validate configuration based on business rules."""
        return self.validate_pipeline_by_field(pipeline_identifier, env)

    def discover_flowgroups(self, pipeline_dir: Path) -> List[FlowGroup]:
        """
        Discover all flowgroups in a specific pipeline directory.

        Args:
            pipeline_dir: Directory containing flowgroup YAML files

        Returns:
            List of discovered flowgroups
        """
        return self.discoverer.discover_flowgroups(pipeline_dir)

    def discover_all_flowgroups(self) -> List[FlowGroup]:
        """
        Discover all flowgroups across all directories in the project.

        Combines three sources, in order:
          1. Disk-sourced flowgroups via FlowgroupDiscoverer (existing).
          2. Synthetic flowgroups expanded from blueprints x instances. Once
             expanded, these are indistinguishable from disk-sourced flowgroups
             for downstream code paths (Step 0.5 -> Step 5).
          3. Synthetic monitoring flowgroup (existing) when monitoring is
             configured.

        Side effects:
          - Populates `self._blueprint_provenance` with the expansion's
            (pipeline, flowgroup) -> BlueprintProvenance mapping. Used by the
            state dependency resolver, dependency tracker, and source-path
            index to handle instance-file changes correctly.
          - Populates `self._synthetic_contexts` with FlowGroupContext envelopes
            for synthetic flowgroups (blueprint-expanded + monitoring).
          - Stores the full MonitoringBuildResult in `self._monitoring_result`.

        Returns:
            List of all discovered flowgroups (regular + synthetic + monitoring).
        """
        with perf_timer("discover_all_flowgroups [orchestrator]"):
            flowgroups = self.discoverer.discover_all_flowgroups()

        # Expand blueprints into synthetic flowgroups
        with perf_timer(
            "Blueprint expansion",
            phase=True,
            parent_phase="Pipeline discovery",
        ):
            blueprint_ctxs, provenance = self._expand_blueprints()
        flowgroups.extend(ctx.flowgroup for ctx in blueprint_ctxs)
        self._blueprint_provenance = provenance
        self._synthetic_contexts = {
            (ctx.flowgroup.pipeline, ctx.flowgroup.flowgroup): ctx
            for ctx in blueprint_ctxs
        }

        # Wire synthetic flowgroups into the FlowgroupDiscoverer source-path
        # index so `find_source_yaml_for_flowgroup` resolves them to their
        # blueprint path (Phase 7).
        if provenance:
            self.discoverer.register_synthetic_sources(
                {key: prov.blueprint_path for key, prov in provenance.items()}
            )

        # Build monitoring artifacts if configured
        self._monitoring_result = self._build_monitoring(flowgroups)
        if self._monitoring_result and self._monitoring_result.context is not None:
            monitoring_ctx = self._monitoring_result.context
            flowgroups.append(monitoring_ctx.flowgroup)
            key = (
                monitoring_ctx.flowgroup.pipeline,
                monitoring_ctx.flowgroup.flowgroup,
            )
            self._synthetic_contexts[key] = monitoring_ctx

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
        """Build monitoring pipeline artifacts if configured.

        Returns the full MonitoringBuildResult (FlowGroup + notebook + eligible pipelines)
        or None if monitoring is not applicable.

        Args:
            discovered_flowgroups: Already-discovered flowgroups (for pipeline names)

        Returns:
            MonitoringBuildResult or None
        """
        if not self.project_config or not self.project_config.monitoring:
            return None

        from .services.monitoring_pipeline_builder import MonitoringPipelineBuilder
        from .services.pipeline_config_loader import PipelineConfigLoader

        # Resolve monitoring pipeline name for alias support in pipeline config
        monitoring_pipeline_name = None
        if self.project_config and self.project_config.monitoring:
            m = self.project_config.monitoring
            if m.enabled:
                monitoring_pipeline_name = (
                    m.pipeline_name
                    or f"{self.project_config.name}_event_log_monitoring"
                )

        pipeline_config_loader = PipelineConfigLoader(
            self.project_root,
            self.pipeline_config_path,
            monitoring_pipeline_name=monitoring_pipeline_name,
        )

        builder = MonitoringPipelineBuilder(
            project_config=self.project_config,
            pipeline_config_loader=pipeline_config_loader,
            project_root=self.project_root,
        )

        # Extract unique pipeline names from discovered flowgroups
        pipeline_names = list(
            dict.fromkeys(fg.pipeline for fg in discovered_flowgroups)
        )

        return builder.build(pipeline_names)

    def finalize_monitoring_artifacts(self, env: str, output_dir: Path) -> None:
        """Reconcile monitoring artifacts: clean stale, write current.

        Called AFTER the pipeline generation loop. Handles all transitions:
        - Monitoring added: write notebook + job
        - Monitoring removed: clean up notebook + job
        - Pipeline renamed: old artifacts removed, new ones written
        - MVs added/removed: job updated (notebook-only vs full)

        Args:
            env: Environment name
            output_dir: Base output directory (e.g. generated/dev)
        """
        # 1. Clean up existing monitoring artifacts (handles renames and removal)
        self._cleanup_monitoring_artifacts(env, output_dir)

        if not self._monitoring_result:
            return

        # 2. Create substitution manager to resolve tokens in template context
        substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
        substitution_mgr = self.dependencies.create_substitution_manager(
            substitution_file, env
        )

        # 3. Apply substitution to template context values
        resolved_context = substitution_mgr.substitute_yaml(
            self._monitoring_result.template_context
        )

        # 4. Render notebook with resolved context
        from ..utils.template_renderer import TemplateRenderer

        renderer = TemplateRenderer.from_package()
        notebook_content = renderer.render_template(
            "monitoring/union_event_logs.py.j2", resolved_context
        )

        # 5. Write notebook to monitoring/{env}/
        monitoring_pipeline_name = self._monitoring_result.pipeline_name

        monitoring_dir = self.project_root / "monitoring" / env
        monitoring_dir.mkdir(parents=True, exist_ok=True)
        notebook_path = monitoring_dir / "union_event_logs.py"
        write_normalized(notebook_path, notebook_content)
        self.logger.info(f"Generated monitoring notebook: {notebook_path}")

        # 6. Load + substitute + merge monitoring job config from the dedicated file
        import yaml

        from .services.job_generator import JobGenerator

        raw_job_config_rel_path = self.project_config.monitoring.job_config_path
        # Substitute tokens (e.g. ${env}) in the path. The loader only checks
        # file existence for static paths; tokenized paths are resolved here
        # once the environment is known.
        job_config_rel_path = (
            substitution_mgr.substitute_yaml(raw_job_config_rel_path)
            if raw_job_config_rel_path
            else raw_job_config_rel_path
        )
        # Presence + (static-path) existence are guaranteed by ProjectConfigLoader
        # validation, but we keep a defensive check here because orchestration
        # also runs through code paths that bypass the loader's validation (tests)
        # and tokenized paths only resolve at this stage.
        if not job_config_rel_path:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="008",
                title="Monitoring job_config_path is required",
                details=(
                    "monitoring.job_config_path must be set when monitoring is enabled."
                ),
                suggestions=[
                    "Add job_config_path to your monitoring config in lhp.yaml",
                    "Example: job_config_path: config/monitoring_job_config.yaml",
                ],
            )

        job_config_file = self.project_root / job_config_rel_path
        if not job_config_file.is_file():
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="001",
                title="Monitoring job_config file not found",
                details=(
                    f"monitoring.job_config_path points to '{job_config_rel_path}', "
                    f"but no file exists at {job_config_file}."
                ),
                suggestions=[
                    "Create the file at the configured path",
                    "Or update monitoring.job_config_path to a valid location",
                ],
                context={
                    "File Path": str(job_config_rel_path),
                    "Project Root": str(self.project_root),
                },
            )

        try:
            with open(job_config_file, "r", encoding="utf-8") as f:
                raw_job_config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="002",
                title="Invalid monitoring job_config YAML",
                details=f"Failed to parse {job_config_file}: {e}",
                suggestions=["Fix the YAML syntax in the monitoring job config"],
                context={"File Path": str(job_config_file)},
            )

        if not isinstance(raw_job_config, dict):
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="008",
                title="Invalid monitoring job_config structure",
                details=(
                    f"{job_config_file} must contain a YAML mapping at the top level, "
                    f"got {type(raw_job_config).__name__}."
                ),
                suggestions=[
                    "Use a flat single-document mapping (no 'project_defaults' wrapper, "
                    "no 'job_name' key)",
                ],
            )

        substituted_job_config = substitution_mgr.substitute_yaml(raw_job_config)
        resolved_job_config = JobGenerator.resolve_monitoring_job_config(
            substituted_job_config
        )

        # 7. Generate and write monitoring job resource
        job_name = f"{monitoring_pipeline_name}_job"
        job_gen = JobGenerator(project_root=self.project_root)
        notebook_workspace_path = (
            "${workspace.file_path}/monitoring/${bundle.target}/union_event_logs"
        )
        has_pipeline = self._monitoring_result.flowgroup is not None
        job_resource_content = job_gen.generate_monitoring_job(
            pipeline_name=monitoring_pipeline_name,
            notebook_path=notebook_workspace_path,
            job_name=job_name,
            job_config=resolved_job_config,
            has_pipeline=has_pipeline,
        )
        resources_dir = self.project_root / "resources"
        resources_dir.mkdir(parents=True, exist_ok=True)
        job_resource_path = resources_dir / f"{monitoring_pipeline_name}.job.yml"
        write_normalized(job_resource_path, job_resource_content)
        self.logger.info(f"Generated monitoring job resource: {job_resource_path}")

    _MONITORING_JOB_HEADER = "# Generated by LakehousePlumber - Monitoring Job"

    def _cleanup_monitoring_artifacts(self, env: str, output_dir: Path) -> None:
        """Remove existing monitoring artifacts before writing new ones.

        Identifies monitoring artifacts by:
        - Notebook: monitoring/{env}/ directory contents
        - Job resource: resources/*.job.yml files with monitoring header comment
        - Generated DLT code: generated/{env}/<pipeline>/ dirs with FLOWGROUP_ID = "monitoring"
        """
        # 1. Clean monitoring notebook directory
        monitoring_dir = self.project_root / "monitoring" / env
        if monitoring_dir.exists():
            for f in monitoring_dir.iterdir():
                if f.is_file():
                    f.unlink()
                    self.logger.info(f"Removed monitoring artifact: {f}")
            # Remove empty directory
            if not any(monitoring_dir.iterdir()):
                monitoring_dir.rmdir()
                self.logger.debug(f"Removed empty directory: {monitoring_dir}")
            # Remove parent monitoring/ if also empty
            monitoring_parent = monitoring_dir.parent
            if monitoring_parent.exists() and not any(monitoring_parent.iterdir()):
                monitoring_parent.rmdir()
                self.logger.debug(f"Removed empty directory: {monitoring_parent}")

        # 2. Clean monitoring job resources (identified by header comment)
        resources_dir = self.project_root / "resources"
        if resources_dir.exists():
            for f in resources_dir.iterdir():
                if f.is_file() and f.suffix == ".yml" and f.name.endswith(".job.yml"):
                    try:
                        first_line = f.read_text().split("\n", 1)[0]
                        if first_line.startswith(self._MONITORING_JOB_HEADER):
                            f.unlink()
                            self.logger.info(f"Removed monitoring job: {f}")
                    except OSError:
                        pass

        # 3. Clean generated DLT monitoring pipeline directories.
        #    Only when monitoring is removed/disabled — when monitoring IS configured,
        #    the pipeline generation loop manages the generated/ directory.
        #    Synthetic monitoring flowgroups aren't tracked in state, so orphan
        #    detection misses them. Identify by FLOWGROUP_ID = "monitoring" marker.
        if not self._monitoring_result and output_dir and output_dir.exists():
            import shutil

            for pipeline_dir in output_dir.iterdir():
                if not pipeline_dir.is_dir():
                    continue
                monitoring_py = pipeline_dir / "monitoring.py"
                if not monitoring_py.exists():
                    continue
                try:
                    content = monitoring_py.read_text(encoding="utf-8")
                    if 'FLOWGROUP_ID = "monitoring"' in content:
                        shutil.rmtree(pipeline_dir)
                        self.logger.info(
                            f"Removed monitoring pipeline directory: {pipeline_dir}"
                        )
                except OSError:
                    pass

    def discover_flowgroups_by_pipeline_field(
        self,
        pipeline_field: str,
        pre_discovered_all_flowgroups: Optional[List[FlowGroup]] = None,
    ) -> List[FlowGroup]:
        """Discover all flowgroups with a specific pipeline field across all directories.

        Args:
            pipeline_field: The pipeline field value to search for
            pre_discovered_all_flowgroups: If provided, filter from this list
                instead of running a new discovery scan.

        Returns:
            List of flowgroups with the specified pipeline field
        """
        if pre_discovered_all_flowgroups is not None:
            all_flowgroups = pre_discovered_all_flowgroups
        else:
            with perf_timer(f"discover_by_pipeline_field [{pipeline_field}]"):
                all_flowgroups = self.discover_all_flowgroups()

        matching_flowgroups = []

        for flowgroup in all_flowgroups:
            if flowgroup.pipeline == pipeline_field:
                matching_flowgroups.append(flowgroup)
                self.logger.debug(
                    f"Found flowgroup '{flowgroup.flowgroup}' for pipeline '{pipeline_field}'"
                )

        return matching_flowgroups

    def validate_duplicate_pipeline_flowgroup_combinations(
        self, flowgroups: List[FlowGroup]
    ) -> None:
        """Validate that there are no duplicate pipeline+flowgroup combinations.

        Args:
            flowgroups: List of flowgroups to validate

        Raises:
            ValueError: If duplicate combinations are found
        """
        errors = self.config_validator.validate_duplicate_pipeline_flowgroup(flowgroups)
        if errors:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="009",
                title="Duplicate pipeline+flowgroup combinations found",
                details=f"Duplicate pipeline+flowgroup combinations found:\n"
                + "\n".join(f"  - {e}" for e in errors),
                suggestions=[
                    "Ensure each pipeline+flowgroup combination is unique",
                    "Check for duplicate flowgroup names within the same pipeline",
                    "Rename one of the duplicate flowgroups",
                ],
                context={"Duplicates": len(errors)},
            )

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

    def generate_pipeline_by_field(
        self,
        pipeline_field: str,
        env: str,
        output_dir: Path = None,
        specific_flowgroups: List[str] = None,
        include_tests: bool = False,
        pre_discovered_all_flowgroups: Optional[List[FlowGroup]] = None,
    ) -> tuple[str, ...]:
        """Thin shim — delegate to the plural :meth:`generate_pipelines_by_fields`.

        On failure the plural method re-raises the original exception
        unchanged for the single-pipeline case, so callers that catch
        specific :class:`LHPError` subclasses continue to work.
        """
        results = self.generate_pipelines_by_fields(
            pipeline_fields=[pipeline_field],
            env=env,
            output_dir=output_dir,
            specific_flowgroups=specific_flowgroups,
            include_tests=include_tests,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            max_workers=self.max_workers,
        )
        return results.get(pipeline_field, ())

    def generate_pipelines_by_fields(
        self,
        *,
        pipeline_fields: Sequence[str],
        env: str,
        output_dir: Optional[Path],
        specific_flowgroups: Optional[List[str]] = None,
        include_tests: bool = False,
        pre_discovered_all_flowgroups: Optional[List[FlowGroup]] = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[Callable[["PipelineDelta"], None]] = None,
    ) -> Dict[str, tuple[str, ...]]:
        """Run one worker per pipeline; aggregate results on the main thread.

        Each worker owns the full pipeline pass (Phase A discovery + Phase B
        validation/file writes). The main thread is a pure aggregator.

        Args:
            pipeline_fields: Pipeline names to generate. Order is preserved
                in the returned mapping; ``on_pipeline_complete`` fires in
                completion order (not input order).
            env: Environment name (e.g. ``"dev"``).
            output_dir: Root output directory (``output_dir / <pipeline>``)
                or ``None`` for dry-run.
            specific_flowgroups: Optional whitelist of flowgroup names.
            include_tests: Include test actions in the generated code.
            pre_discovered_all_flowgroups: Re-use the caller's one-shot
                discovery instead of re-discovering.
            max_workers: Pool size override. ``None`` falls back to
                ``self.max_workers``.
            on_pipeline_complete: Optional main-thread callback fired once
                per pipeline with the :class:`PipelineDelta`. Callback
                exceptions are logged but do not abort the batch.

        Returns:
            Mapping of ``{pipeline_name: {generated_path: code}}`` for
            **successful** pipelines. Failed pipelines are absent from the
            dict — the aggregate :class:`LHPError` raised at the end
            captures their errors.

        Raises:
            LHPError: When a single pipeline failed; reconstructed via
                :meth:`LHPError.from_worker_exception` so the worker's
                error type and full traceback survive.
            LHPValidationError: When multiple pipelines failed; one
                aggregate error listing every failure.
        """
        self._invalidate_pipeline_slice_cache()

        self.logger.info(
            f"Starting batch pipeline generation: {len(pipeline_fields)} pipeline(s) "
            f"for env: {env}"
        )

        if pre_discovered_all_flowgroups is not None:
            all_flowgroups = pre_discovered_all_flowgroups
        else:
            with perf_timer("discover_all_flowgroups"):
                all_flowgroups = self.discover_all_flowgroups()
            with perf_timer("validate_duplicates"):
                self.validate_duplicate_pipeline_flowgroup_combinations(all_flowgroups)

        contexts_by_pipeline: Dict[str, List[FlowGroupContext]] = {}
        substitution_managers: Dict[str, EnhancedSubstitutionManager] = {}
        pipeline_output_dirs: Dict[str, Optional[Path]] = {}

        substitution_file = self.project_root / "substitutions" / f"{env}.yaml"

        for pipeline_field in pipeline_fields:
            slice_for_pipeline = self._lookup_pipeline_slice(
                all_flowgroups, pipeline_field
            )
            with perf_timer(
                f"discover_and_filter_flowgroups [{pipeline_field}]",
                category="discover_and_filter_flowgroups",
            ):
                flowgroups = self._discover_and_filter_flowgroups(
                    env=env,
                    pipeline_identifier=pipeline_field,
                    include_tests=include_tests,
                    specific_flowgroups=specific_flowgroups,
                    use_directory_discovery=False,
                    pre_discovered_flowgroups=slice_for_pipeline,
                )
            contexts_by_pipeline[pipeline_field] = [
                self._make_context(fg) for fg in flowgroups
            ]
            pipeline_output_dirs[pipeline_field] = None

            if not flowgroups:
                continue

            pipeline_out_dir = output_dir / pipeline_field if output_dir else None
            if pipeline_out_dir is not None:
                pipeline_out_dir.mkdir(parents=True, exist_ok=True)
            pipeline_output_dirs[pipeline_field] = pipeline_out_dir

            with perf_timer(
                f"create_substitution_manager [{pipeline_field}]",
                category="create_substitution_manager",
            ):
                substitution_managers[pipeline_field] = (
                    self.dependencies.create_substitution_manager(
                        substitution_file, env
                    )
                )

        worker_state = _GenerateWorkerState(
            processor=self.processor,
            code_generator=self.generator,
            formatter=self._formatter,
            substitution_managers=substitution_managers,
            pipeline_output_dirs=pipeline_output_dirs,
            environment=env,
            project_root=self.project_root,
            project_config=self.project_config,
            include_tests=include_tests,
        )

        resolved_workers = max(
            1, max_workers if max_workers is not None else self.max_workers
        )

        successful, failed = run_generate_pool(
            flowgroups_by_pipeline=contexts_by_pipeline,
            worker_state=worker_state,
            max_workers=resolved_workers,
            on_pipeline_complete=on_pipeline_complete,
        )

        if failed:
            if len(failed) == 1:
                d = failed[0]
                raise lhp_error_from_worker_failure(
                    pipeline_name=d.pipeline_name,
                    error_type=d.error_type or "UnknownError",
                    error_message=d.error_message or "(no message)",
                    error_traceback=d.error_traceback or "",
                )
            failure_lines = [
                f"  - {d.pipeline_name}: {d.error_type}: {d.error_message}"
                for d in failed
            ]
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="011",
                title=(f"{len(failed)} pipeline(s) failed during batch generation"),
                details=("Multiple pipelines failed:\n" + "\n".join(failure_lines)),
                suggestions=[
                    "Inspect each failing pipeline's error individually",
                    "Run with --pipeline <name> to isolate failures",
                    "Run 'lhp validate' for upfront diagnostics",
                ],
                context={
                    "Failed Pipelines": ", ".join(d.pipeline_name for d in failed),
                    "Successful Pipelines": str(len(successful)),
                    "Failure Count": str(len(failed)),
                },
            )

        return {delta.pipeline_name: delta.generated_filenames for delta in successful}

    def _find_source_yaml_for_flowgroup(self, flowgroup: FlowGroup) -> Optional[Path]:
        """Find the source YAML file for a given flowgroup.

        Delegates to FlowgroupDiscoverer service for consistency.

        Supports multi-document (---) and flowgroups array syntax.

        Args:
            flowgroup: The flowgroup to find the source YAML for

        Returns:
            Path to the source YAML file, or None if not found
        """
        return self.discoverer.find_source_yaml_for_flowgroup(flowgroup)

    def _make_context(self, fg: FlowGroup) -> FlowGroupContext:
        """Wrap a FlowGroup in its FlowGroupContext for the worker boundary.

        Looks up synthetic provenance (synthetic flag, auxiliary_files) from
        `self._synthetic_contexts`; disk-sourced flowgroups get default values.
        Source YAML is resolved via the FlowgroupDiscoverer (threading.Lock'd
        index — must run on the main process before spawn). Tests may construct
        the orchestrator without a discoverer, in which case source_yaml is
        left None.
        """
        source_yaml = (
            self._find_source_yaml_for_flowgroup(fg)
            if self.discoverer is not None
            else None
        )
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
        """
        Process flowgroup: expand templates, apply presets, apply substitutions.

        Backward-compatible shim around :meth:`FlowgroupProcessor.process_flowgroup`,
        which now takes/returns :class:`FlowGroupContext`. This shim wraps the
        FlowGroup in a default-empty context and returns just the processed
        FlowGroup for callers that don't care about provenance.

        Args:
            flowgroup: FlowGroup to process
            substitution_mgr: Substitution manager for the environment
            include_tests: If False, filter out test actions before processing.
                Defaults to True for backward compatibility.

        Returns:
            Processed flowgroup
        """
        ctx_in = self._make_context(flowgroup)
        ctx_out = self.processor.process_flowgroup(
            ctx_in, substitution_mgr, include_tests=include_tests
        )
        return ctx_out.flowgroup

    # _apply_preset_config and _deep_merge methods moved to FlowgroupProcessor service

    def generate_flowgroup_code(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: EnhancedSubstitutionManager,
        output_dir: Optional[Path] = None,
        source_yaml: Optional[Path] = None,
        env: Optional[str] = None,
        include_tests: bool = False,
        python_file_copier=None,
        phase_a_records: Optional[List["CopiedModuleRecord"]] = None,
    ) -> str:
        """
        Generate complete Python code for a flowgroup.

        Args:
            flowgroup: FlowGroup to generate code for
            substitution_mgr: Substitution manager for the environment
            output_dir: Output directory for generated files
            source_yaml: Source YAML path for file tracking
            env: Environment name for file tracking
            include_tests: Whether to include test actions
            python_file_copier: Thread-safe Python file copier (for parallel mode)
            phase_a_records: Optional list passed by Phase A workers in the
                cross-pipeline flat pool; when supplied, the file copier
                appends :class:`CopiedModuleRecord` entries to it instead
                of writing to disk. Phase B replays those records.

        Returns:
            Complete Python code for the flowgroup
        """
        return self.generator.generate_flowgroup_code(
            flowgroup,
            substitution_mgr,
            output_dir,
            source_yaml,
            env,
            include_tests,
            python_file_copier,
            phase_a_records=phase_a_records,
        )

    def determine_action_subtype(self, action: Action) -> str:
        """
        Determine the sub-type of an action for generator selection.

        Args:
            action: Action to determine sub-type for

        Returns:
            Sub-type string for generator selection
        """
        return self.generator.determine_action_subtype(action)

    def _discover_and_filter_flowgroups(
        self,
        env: str,
        pipeline_identifier: str,
        include_tests: bool,
        specific_flowgroups: List[str] = None,
        use_directory_discovery: bool = False,
        pre_discovered_flowgroups: Optional[List[FlowGroup]] = None,
    ) -> List[FlowGroup]:
        """
        Discover and filter flowgroups based on generation requirements.

        Args:
            env: Environment name
            pipeline_identifier: Pipeline name or field value
            include_tests: Include test actions parameter
            specific_flowgroups: Optional list of specific flowgroups
            use_directory_discovery: Use directory-based discovery vs field-based

        Returns:
            List of flowgroups that should be generated
        """
        if use_directory_discovery:
            pipeline_dir = self.project_root / "pipelines" / pipeline_identifier
            if not pipeline_dir.exists():
                raise LHPFileError(
                    category=ErrorCategory.IO,
                    code_number="001",
                    title="Pipeline directory not found",
                    details=f"Pipeline directory not found: {pipeline_dir}",
                    suggestions=[
                        f"Check that the directory '{pipeline_dir}' exists",
                        "Verify the pipeline name is correct",
                        "Run 'lhp info' to see available pipelines",
                    ],
                    context={
                        "Pipeline": pipeline_identifier,
                        "Directory": str(pipeline_dir),
                    },
                )
            all_flowgroups = self.discoverer.discover_flowgroups(pipeline_dir)
        else:
            if pre_discovered_flowgroups is not None:
                all_flowgroups = [
                    fg
                    for fg in pre_discovered_flowgroups
                    if fg.pipeline == pipeline_identifier
                ]
                if all_flowgroups:
                    self.logger.info(
                        f"Found {len(all_flowgroups)} flowgroup(s) for pipeline: "
                        f"{pipeline_identifier}"
                    )
                else:
                    self.logger.warning(
                        f"No flowgroups found for pipeline: {pipeline_identifier}"
                    )
            else:
                all_flowgroups = self.discover_flowgroups_by_pipeline_field(
                    pipeline_identifier
                )

        if not all_flowgroups:
            if use_directory_discovery:
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="014",
                    title="No flowgroups found",
                    details=f"No flowgroups found in pipeline: {pipeline_identifier}",
                    suggestions=[
                        "Check that the pipeline directory contains YAML flowgroup files",
                        "Verify the pipeline name is correct",
                        "Run 'lhp info' to see project configuration",
                    ],
                    context={"Pipeline": pipeline_identifier},
                )
            else:
                self.logger.warning(
                    f"No flowgroups found for pipeline field: {pipeline_identifier}"
                )
                return []

        if specific_flowgroups:
            filtered_flowgroups = [
                fg for fg in all_flowgroups if fg.flowgroup in specific_flowgroups
            ]
            self.logger.info(
                f"Generating specific flowgroups: {len(filtered_flowgroups)}/{len(all_flowgroups)}"
            )
            return filtered_flowgroups

        return all_flowgroups

    def _process_flowgroups_batch(
        self,
        flowgroups: List[FlowGroup],
        substitution_mgr: EnhancedSubstitutionManager,
        include_tests: bool = True,
    ) -> List[FlowGroup]:
        """Process all flowgroups in a batch.

        Handles template expansion, preset application, and substitution
        for a list of flowgroups. Returns the processed FlowGroups (callers
        of this method don't need provenance — `FlowGroupContext` envelopes
        are constructed at the worker boundary).

        Args:
            flowgroups: List of flowgroups to process
            substitution_mgr: Substitution manager for the environment
            include_tests: If False, filter out test actions before processing.

        Returns:
            List of processed flowgroups

        Raises:
            Exception: If processing fails for any flowgroup
        """
        processed = []
        for flowgroup in flowgroups:
            self.logger.info(f"Processing flowgroup: {flowgroup.flowgroup}")
            try:
                with perf_timer(
                    f"process_flowgroup [{flowgroup.flowgroup}]",
                    category="process_flowgroup",
                ):
                    ctx_in = self._make_context(flowgroup)
                    ctx_out = self.processor.process_flowgroup(
                        ctx_in, substitution_mgr, include_tests=include_tests
                    )
                processed.append(ctx_out.flowgroup)
            except Exception as e:
                self.logger.debug(
                    f"Error processing flowgroup {flowgroup.flowgroup}: {e}"
                )
                raise
        return processed

    def group_write_actions_by_target(
        self, write_actions: List[Action]
    ) -> Dict[str, List[Action]]:
        """
        Group write actions by their target table.

        Args:
            write_actions: List of write actions

        Returns:
            Dictionary mapping target table names to lists of actions
        """
        return self.generator.group_write_actions_by_target(write_actions)

    def create_combined_write_action(
        self, actions: List[Action], target_table: str
    ) -> Action:
        """
        Create a combined write action with individual action metadata preserved.

        Args:
            actions: List of write actions targeting the same table
            target_table: Full target table name

        Returns:
            Combined action with individual action metadata
        """
        return self.generator.create_combined_write_action(actions, target_table)

    def _extract_single_source_view(self, source) -> str:
        """Extract a single source view from various source formats.

        Delegates to utility function for consistency across codebase.

        Args:
            source: Source configuration (string, list, or dict)

        Returns:
            Source view name as string
        """
        return extract_single_source_view(source)

    def _extract_source_views_from_action(self, source) -> List[str]:
        """Extract all source views from an action source configuration.

        Delegates to utility function for consistency across codebase.

        Args:
            source: Source configuration (string, list, or dict)

        Returns:
            List of source view names
        """
        return extract_source_views_from_action(source)

    def validate_pipeline_by_field(
        self,
        pipeline_field: str,
        env: str,
        include_tests: bool = True,
        pre_discovered_all_flowgroups: Optional[List[FlowGroup]] = None,
    ) -> Tuple[List[str], List[str]]:
        """Thin shim — delegate to the plural :meth:`validate_pipelines_by_fields`.

        Preserves the legacy ``(errors, warnings)`` tuple return so the
        existing :class:`ValidateCommand` per-pipeline loop and
        :class:`LakehousePlumberApplicationFacade.validate_pipeline` keep
        working unchanged.
        """
        outcomes = self.validate_pipelines_by_fields(
            pipeline_fields=[pipeline_field],
            env=env,
            include_tests=include_tests,
            pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
            max_workers=self.max_workers,
        )
        if not outcomes:
            return [], []
        outcome = outcomes[0]
        return list(outcome.errors), list(outcome.warnings)

    def validate_pipelines_by_fields(
        self,
        *,
        pipeline_fields: Sequence[str],
        env: str,
        include_tests: bool = True,
        pre_discovered_all_flowgroups: Optional[List[FlowGroup]] = None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[OnValidationComplete] = None,
    ) -> List[PipelineValidationOutcome]:
        """Flat-pool validate across multiple pipelines.

        Mirrors :meth:`generate_pipelines_by_fields` but simpler: no state
        save, no Phase B replay, no file writes. Phase A workers call
        :meth:`process_flowgroup` (which runs schema + reference + action
        validation); Phase B per pipeline runs
        :meth:`ConfigValidator.validate_cdc_fanin_compatibility` as the
        post-barrier cross-flowgroup check.

        Args:
            pipeline_fields: Pipeline names to validate. Outcomes are
                returned in input order (stable for display).
            env: Environment name.
            include_tests: When False, test actions are filtered before
                processing (matches single-pipeline shim default of True).
            pre_discovered_all_flowgroups: Re-use caller's one-shot
                discovery; if None, runs ``discover_all_flowgroups()``.
            max_workers: Process-pool size; falls back through
                ``self.max_workers`` → :func:`_auto_max_workers`
                (~80% of detected CPU count, honoring cgroup limits on Linux).
            on_pipeline_complete: Optional callback fired once per
                pipeline (main thread, completion order).

        Returns:
            List of :class:`PipelineValidationOutcome`, one per input
            pipeline, in input order.
        """
        self._invalidate_pipeline_slice_cache()

        if pre_discovered_all_flowgroups is not None:
            all_flowgroups = pre_discovered_all_flowgroups
        else:
            with perf_timer("discover_all_flowgroups"):
                all_flowgroups = self.discover_all_flowgroups()

        flowgroups_by_pipeline: Dict[str, List[FlowGroup]] = {}
        contexts_by_pipeline: Dict[str, List[FlowGroupContext]] = {}
        substitution_managers: Dict[str, EnhancedSubstitutionManager] = {}
        discovery_errors: Dict[str, str] = {}
        substitution_file = self.project_root / "substitutions" / f"{env}.yaml"

        for pipeline_field in pipeline_fields:
            try:
                flowgroups = self.discover_flowgroups_by_pipeline_field(
                    pipeline_field,
                    pre_discovered_all_flowgroups=all_flowgroups,
                )
            except Exception as e:
                # Discovery itself failed — surface as a single
                # "Pipeline validation failed: ..." error.
                self.logger.debug(
                    f"Pipeline '{pipeline_field}' discovery failed",
                    exc_info=True,
                )
                flowgroups_by_pipeline[pipeline_field] = []
                contexts_by_pipeline[pipeline_field] = []
                discovery_errors[pipeline_field] = f"Pipeline validation failed: {e}"
                continue

            flowgroups_by_pipeline[pipeline_field] = flowgroups
            # Wrap each FlowGroup in its FlowGroupContext envelope for the
            # worker boundary. Source-path resolution holds a threading.Lock
            # so it must happen on the main process before spawn.
            contexts_by_pipeline[pipeline_field] = [
                self._make_context(fg) for fg in flowgroups
            ]
            if not flowgroups:
                continue
            substitution_managers[pipeline_field] = (
                self.dependencies.create_substitution_manager(substitution_file, env)
            )

        # Captured once and shipped to each worker via the pool's initializer=
        # seam. Replaces the per-task functools.partial capture that re-pickled
        # FlowgroupProcessor on every submit (the big win on the validate path:
        # one capture per pool vs. one per flowgroup).
        worker_state = _ValidateWorkerState(
            processor=self.processor,
            substitution_managers=substitution_managers,
            include_tests=include_tests,
        )

        def _assemble(
            pipeline_name: str,
            results: List[FlowgroupValidationResult],
        ) -> PipelineValidationOutcome:
            flowgroups = flowgroups_by_pipeline.get(pipeline_name, [])

            # Discovery failure — report and stop.
            if pipeline_name in discovery_errors:
                return PipelineValidationOutcome(
                    pipeline=pipeline_name,
                    errors=(discovery_errors[pipeline_name],),
                    warnings=(),
                    success=False,
                )

            # Empty discovery — surface as a validation error.
            if not flowgroups:
                return PipelineValidationOutcome(
                    pipeline=pipeline_name,
                    errors=(
                        f"No flowgroups found for pipeline field: {pipeline_name}",
                    ),
                    warnings=(),
                    success=False,
                )

            errors: List[str] = []
            for result in results:
                errors.extend(result.errors)

            # Cross-flowgroup CDC fan-in compatibility — runs even when
            # per-flowgroup errors exist (mismatches only surface when
            # flowgroups are considered as a set).
            try:
                with perf_timer(
                    f"validate_cdc_fanin_compatibility [{pipeline_name}]",
                    category="validate_cdc_fanin_compatibility",
                ):
                    cdc_errors = self.config_validator.validate_cdc_fanin_compatibility(
                        flowgroups
                    )
                errors.extend(cdc_errors)
            except LHPError as e:
                errors.append(f"CDC fan-in validation: {e}")
            except Exception as e:
                errors.append(f"CDC fan-in validation failed: {e}")

            return PipelineValidationOutcome(
                pipeline=pipeline_name,
                errors=tuple(errors),
                warnings=(),
                success=len(errors) == 0,
            )

        # Resolve worker count — constructor already turned None into
        # _auto_max_workers(); method-level override takes precedence.
        resolved_workers = max(
            1, max_workers if max_workers is not None else self.max_workers
        )

        return run_validate_pool(
            pipelines=list(pipeline_fields),
            flowgroups_by_pipeline=contexts_by_pipeline,
            worker_state=worker_state,
            assemble_pipeline=_assemble,
            max_workers=resolved_workers,
            on_pipeline_complete=on_pipeline_complete,
        )
