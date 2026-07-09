"""Dependency analysis composition root.

:stability: provisional
"""

import json
import logging
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
)

from lhp.models import FlowGroup, FlowGroupContext, ProjectConfig

from ...models.dependencies import (
    DependencyAnalysisResult,
    DependencyGraphs,
)
from ...parsers.blueprint_parser import BlueprintParser
from ...parsers.parse_cache import PersistentParseCache
from ...parsers.yaml_parser import CachingYAMLParser, YAMLParser
from ...presets.preset_manager import PresetManager
from .._interfaces import BaseDependencyAnalysisService
from ..coordination import ValidationService
from ..discovery.blueprint_discoverer import BlueprintDiscoverer
from ..discovery.flowgroup_discoverer import FlowgroupDiscoveryService
from ..loaders import ProjectConfigLoader
from ..processing import TemplateEngine
from ..processing.blueprint_expander import BlueprintExpander, BlueprintProvenance
from ..processing.flowgroup_resolver import FlowgroupResolutionService
from ..validators import ConfigValidator, SecretValidator
from . import output
from .analyzer import DependencyAnalyzer
from .builder import DependencyGraphBuilder

if TYPE_CHECKING:
    from ..processing.substitution import EnhancedSubstitutionManager


class DependencyAnalysisService(BaseDependencyAnalysisService):
    """Composition root for dependency analysis.

    :stability: provisional
    """

    # This class exposes 9 public methods (within the constitution §3.2 cap of
    # 10). It is the single composition root for the whole dependency subsystem
    # and owns four distinct concerns that share the cached `_flowgroups` /
    # `_flowgroup_file_paths` / `_blueprint_provenance` state:
    #   - discovery + processing (`get_flowgroups`, with optional pipeline /
    #     blueprint filters) — owned here so the builder can be a pure
    #     `(flowgroups, file_paths) -> graphs` function. Blueprint expansion
    #     is ALWAYS full: one flowgroup per (blueprint x instance x spec);
    #     there is no dedup view (a blueprint that parameterizes `pipeline:`
    #     per instance would silently lose pipelines from the graph);
    #   - the ABC-required graph verbs (`build_graphs`, `analyze`, `export`)
    #     plus the memoized `analyze_project` entry point (one build+analyze
    #     per `(pipeline_filter, blueprint_filter)` per service instance —
    #     the `dag` command's analyze and save paths share one analysis);
    #   - graph-derived queries (`get_execution_order`,
    #     `detect_circular_dependencies`);
    #   - job-level orchestration (`analyze_dependencies_by_job`) plus
    #     `get_project_name`.
    # `analyze_dependencies_by_job` is the single validated entry point for job
    # orchestration: it validates job_name usage, reuses the memoized global
    # analyze pass, and partitions per job_name (delegating to the analyzer's
    # internal `partition_result_by_job`). The output writer drives the live
    # `dag --format job` path through it.

    def __init__(
        self,
        project_root: Path,
        project_config: ProjectConfig,
        validation_service: ValidationService,
        *,
        config_validator: Optional[ConfigValidator] = None,
        persistent_parse_cache: Optional[PersistentParseCache] = None,
        max_workers: Optional[int] = None,
    ) -> None:
        # Public attribute — output.py:587 / 616 / 636 reads it directly.
        self.project_root = project_root
        self.project_config = project_config
        self.validation_service = validation_service
        self.logger = logging.getLogger(__name__)

        # Local ProjectConfigLoader: the downstream FlowgroupDiscoveryService
        # still takes a loader (manifest §7.4 option (a)).
        self._project_config_loader = ProjectConfigLoader(project_root)

        # Shared YAML parser (caching wrapper feeds both discoverers).
        # ``persistent_parse_cache`` is the orchestrator's store instance so
        # the dag/deps path shares the same on-disk shards as generation.
        self.yaml_parser = YAMLParser()
        self._cached_yaml_parser = CachingYAMLParser(
            self.yaml_parser, persistent_cache=persistent_parse_cache
        )

        # ``max_workers`` sizes the discoverer's cold-discovery parse pool;
        # ``None`` (legacy direct construction) stays fully serial.
        self._flowgroup_discoverer = FlowgroupDiscoveryService(
            project_root,
            self._project_config_loader,
            yaml_parser=self._cached_yaml_parser,
            max_workers=1 if max_workers is None else max(1, max_workers),
        )
        blueprint_parser = BlueprintParser(caching_yaml_parser=self._cached_yaml_parser)
        self._blueprint_discoverer = BlueprintDiscoverer(
            project_root,
            project_config=project_config,
            blueprint_parser=blueprint_parser,
            caching_yaml_parser=self._cached_yaml_parser,
        )
        self._blueprint_expander = BlueprintExpander()

        template_engine = TemplateEngine(project_root / "templates")
        preset_manager = PresetManager(project_root / "presets")
        secret_validator = SecretValidator()

        # Config validator: prefer the one injected by the composition
        # root (single shared instance across ValidationService and the
        # FlowgroupResolutionService below). Fall back to a local
        # construction only for legacy callers that bypass the facade.
        cfg_validator = config_validator or ConfigValidator(
            project_root, project_config
        )

        self._flowgroup_resolver = FlowgroupResolutionService(
            template_engine,
            preset_manager,
            cfg_validator,
            secret_validator,
        )

        self._flowgroups: Optional[List[FlowGroup]] = None
        self._analysis_memo: Dict[
            Tuple[Optional[str], Optional[str]], DependencyAnalysisResult
        ] = {}
        self._flowgroup_file_paths: Dict[str, Path] = {}
        self._blueprint_provenance: Dict[Tuple[str, str], BlueprintProvenance] = {}

        # The builder performs graph construction only; discovery /
        # processing / view-mode are owned here and threaded in as
        # ``(flowgroups, file_paths)``.
        self._builder = DependencyGraphBuilder(project_root=project_root)
        self._analyzer = DependencyAnalyzer()

    def build_graphs(self, flowgroups: Sequence[FlowGroup]) -> DependencyGraphs:
        """Build the action/flowgroup/pipeline graph triple from a given set.

        Threads the file-path index populated by the prior ``get_flowgroups``
        call into the builder so the :class:`SourceParser` can resolve
        relative SQL/Python file references. The ABC-mandated signature takes
        only the flowgroup sequence; the index is service state.
        """
        return self._builder.build_from_flowgroups(
            list(flowgroups), self._flowgroup_file_paths
        )

    def analyze(self, graphs: DependencyGraphs) -> DependencyAnalysisResult:
        return self._analyzer.analyze(graphs)

    def export(
        self,
        result: DependencyAnalysisResult,
        format: Literal["dot", "json", "text"],
    ) -> str:
        if format == "dot":
            return output.export_to_dot(result.graphs, level="pipeline")
        if format == "json":
            return json.dumps(
                output.export_to_json(result), indent=2, ensure_ascii=False
            )
        if format == "text":
            return output.export_to_text(result)
        raise ValueError(f"Unknown format: {format!r}")

    def get_project_name(self) -> str:
        if self.project_config and self.project_config.name:
            return self.project_config.name
        return self.project_root.name if self.project_root else "lhp_project"

    def analyze_dependencies_by_job(
        self,
    ) -> Tuple[Dict[str, DependencyAnalysisResult], DependencyAnalysisResult]:
        """Perform dependency analysis grouped by ``job_name``.

        Reuses the memoized global analysis (``analyze_project``), then
        partitions the global result by ``job_name``. Returns the tuple
        ``(job_results, global_result)``.
        """
        from ..validators import validate_job_names

        self.logger.info("Starting multi-job dependency analysis...")

        flowgroups = self.get_flowgroups()

        if not flowgroups:
            self.logger.warning("No flowgroups found for analysis")
            empty_graphs = self.build_graphs([])
            empty_result = DependencyAnalysisResult(
                graphs=empty_graphs,
                pipeline_dependencies={},
                execution_stages=[],
                circular_dependencies=[],
                external_sources=[],
            )
            return {}, empty_result

        # Validate job_name usage (all-or-nothing rule)
        validate_job_names(flowgroups)

        has_job_name = any(fg.job_name for fg in flowgroups)

        if not has_job_name:
            self.logger.info("No job_name defined - performing single-job analysis")
            result = self.analyze_project()
            project_name = self.get_project_name()
            job_results = {f"{project_name}_orchestration": result}
            return job_results, result

        # Group flowgroups by job_name (for logging only — partition uses flowgroups).
        job_groups: Dict[str, List[FlowGroup]] = {}
        for fg in flowgroups:
            if fg.job_name not in job_groups:
                job_groups[fg.job_name] = []
            job_groups[fg.job_name].append(fg)

        self.logger.info(
            f"Found {len(job_groups)} job group(s): "
            f"{', '.join(sorted(job_groups.keys()))}"
        )

        self.logger.info("Step 1: Analyzing all flowgroups together (global view)")
        global_result = self.analyze_project()

        self.logger.info(
            f"Step 2: Partitioning global result by {len(job_groups)} job group(s)"
        )
        job_results = self._analyzer.partition_result_by_job(global_result, flowgroups)

        self.logger.info(
            f"Multi-job analysis complete: {len(job_results)} job(s), "
            f"{len(flowgroups)} total flowgroups"
        )

        return job_results, global_result

    def get_flowgroups(
        self,
        pipeline_filter: Optional[str] = None,
        blueprint_filter: Optional[str] = None,
    ) -> List[FlowGroup]:
        """Get flowgroups, optionally filtered by pipeline and/or blueprint.

        Synthetic flowgroups expanded from blueprints are ALWAYS fully
        included — one per (blueprint x instance x spec). There is no dedup
        view: collapsing synthetics to one representative per spec silently
        dropped every other instance's pipeline from the graph whenever a
        blueprint parameterizes ``pipeline:`` per instance. With
        ``blueprint_filter``, only synthetics from that blueprint are
        emitted (and on-disk flowgroups are excluded).

        Populates ``self._flowgroup_file_paths`` as a side effect (mapping each
        flowgroup to its source YAML); ``build_graphs`` threads that index into
        the builder so the :class:`SourceParser` can resolve relative file
        references.
        """
        if self._flowgroups is None:
            self._flowgroups = self._discover_and_process_all_flowgroups()

        flowgroups = self._flowgroups

        if blueprint_filter is not None:
            allowed_keys = {
                key
                for key, prov in self._blueprint_provenance.items()
                if prov.blueprint_name == blueprint_filter
            }
            flowgroups = [
                fg for fg in flowgroups if (fg.pipeline, fg.flowgroup) in allowed_keys
            ]

        if pipeline_filter:
            return [fg for fg in flowgroups if fg.pipeline == pipeline_filter]
        return flowgroups

    def analyze_project(
        self,
        pipeline_filter: Optional[str] = None,
        blueprint_filter: Optional[str] = None,
    ) -> DependencyAnalysisResult:
        """Discover, build and analyze — once per filter pair, memoized.

        The single entry point the facade routes through: the ``dag``
        command's analyze and save paths (and the job-orchestration global
        pass) all share ONE discovery + graph build + analysis per service
        instance and filter combination.
        """
        key = (pipeline_filter, blueprint_filter)
        if key not in self._analysis_memo:
            flowgroups = self.get_flowgroups(
                pipeline_filter=pipeline_filter, blueprint_filter=blueprint_filter
            )
            self._analysis_memo[key] = self._analyzer.analyze(
                self.build_graphs(flowgroups)
            )
        return self._analysis_memo[key]

    def _discover_and_process_all_flowgroups(self) -> List[FlowGroup]:
        """Discover flowgroups (on-disk + synthetic) and process them once.

        The substitution manager is constructed once and reused for every
        flowgroup; per-flowgroup processing errors fall back to the raw
        flowgroup so the dependency graph never crashes on a single bad file.
        """
        from ..processing.substitution import EnhancedSubstitutionManager

        substitution_mgr = EnhancedSubstitutionManager(
            substitution_file=None,
            env="dev",
            skip_validation=True,
        )

        processed: List[FlowGroup] = []
        for (
            fg,
            yaml_file_path,
        ) in self._flowgroup_discoverer.discover_all_flowgroups_with_paths():
            processed.append(self._process_one(fg, yaml_file_path, substitution_mgr))

        try:
            blueprints = self._blueprint_discoverer.discover_blueprints()
            if not blueprints:
                return processed
            instances = self._blueprint_discoverer.discover_instances(blueprints)
            if not instances:
                return processed
            synthetic_ctxs, provenance = self._blueprint_expander.expand(
                blueprints, instances
            )
            self._blueprint_provenance = provenance
            for ctx in synthetic_ctxs:
                fg = ctx.flowgroup
                bp_path = provenance[(fg.pipeline, fg.flowgroup)].blueprint_path
                processed.append(self._process_one(fg, bp_path, substitution_mgr))
        except Exception as e:
            # Blueprint expansion errors surface via `lhp validate` / `generate`;
            # `deps` is read-only and degrades gracefully rather than blocking.
            self.logger.warning(
                f"Blueprint expansion skipped during dependency analysis: {e}"
            )

        return processed

    def _process_one(
        self,
        fg: FlowGroup,
        file_path: Path,
        substitution_mgr: "EnhancedSubstitutionManager",
    ) -> FlowGroup:
        """Process one flowgroup; fall back to raw fg on template/preset error.

        The dag path resolves fully (templates, presets, substitutions) but
        skips per-flowgroup config validation (``validate_config=False``) —
        structural analysis should not degrade to the raw flowgroup just
        because its config is invalid; ``lhp validate`` owns that diagnosis.
        """
        try:
            ctx_in = FlowGroupContext(flowgroup=fg, source_yaml=file_path)
            ctx_out = self._flowgroup_resolver.process_flowgroup(
                ctx_in, substitution_mgr, validate_config=False
            )
            processed_fg = ctx_out.flowgroup
            self._flowgroup_file_paths[processed_fg.flowgroup] = file_path
            return processed_fg
        except Exception as e:
            self.logger.warning(f"Could not process flowgroup {fg.flowgroup}: {e}")
            self._flowgroup_file_paths[fg.flowgroup] = file_path
            return fg

    def get_execution_order(self, graphs: DependencyGraphs) -> List[List[str]]:
        return self._analyzer.get_execution_order(graphs)

    def detect_circular_dependencies(self, graphs: DependencyGraphs) -> List[List[str]]:
        return self._analyzer.detect_circular_dependencies(graphs)
