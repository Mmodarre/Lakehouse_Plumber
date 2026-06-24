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
    Set,
    Tuple,
)

from lhp.models import FlowGroup, FlowGroupContext, ProjectConfig

from ...models.dependencies import (
    DependencyAnalysisResult,
    DependencyGraphs,
)
from ...parsers.blueprint_parser import BlueprintParser
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
    # `_flowgroup_file_paths` / `_blueprint_*` state:
    #   - discovery + processing + blueprint view-mode (`get_flowgroups`,
    #     `set_blueprint_view_mode`) — owned here so the builder can be a pure
    #     `(flowgroups, file_paths) -> graphs` function;
    #   - the ABC-required graph verbs (`build_graphs`, `analyze`, `export`);
    #   - graph-derived queries (`get_execution_order`,
    #     `detect_circular_dependencies`);
    #   - job-level orchestration (`analyze_dependencies_by_job`) plus
    #     `get_project_name`.
    # `analyze_dependencies_by_job` is the single validated entry point for job
    # orchestration: it validates job_name usage, runs one global analyze pass,
    # and partitions per job_name (delegating to the analyzer's internal
    # `partition_result_by_job`). The output writer drives the live
    # `dag --format job` path through it.

    def __init__(
        self,
        project_root: Path,
        project_config: ProjectConfig,
        validation_service: ValidationService,
        *,
        config_validator: Optional[ConfigValidator] = None,
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
        self.yaml_parser = YAMLParser()
        self._cached_yaml_parser = CachingYAMLParser(self.yaml_parser)

        self._flowgroup_discoverer = FlowgroupDiscoveryService(
            project_root,
            self._project_config_loader,
            yaml_parser=self._cached_yaml_parser,
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
            project_root=project_root,
        )

        # View-mode controls: set via `set_blueprint_view_mode` and consumed
        # in get_flowgroups. Defaults to dedupe ON (one representative per
        # spec + count annotation) to keep 32k-flowgroup output usable.
        self._expand_blueprints_view: bool = False
        self._blueprint_filter: Optional[str] = None

        self._flowgroups: Optional[List[FlowGroup]] = None
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

        First analyzes all flowgroups together (global view), then
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
            graphs = self.build_graphs(flowgroups)
            result = self._analyzer.analyze(graphs)
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
        global_graphs = self.build_graphs(flowgroups)
        global_result = self._analyzer.analyze(global_graphs)

        self.logger.info(
            f"Step 2: Partitioning global result by {len(job_groups)} job group(s)"
        )
        job_results = self._analyzer.partition_result_by_job(global_result, flowgroups)

        self.logger.info(
            f"Multi-job analysis complete: {len(job_results)} job(s), "
            f"{len(flowgroups)} total flowgroups"
        )

        return job_results, global_result

    def get_flowgroups(self, pipeline_filter: Optional[str] = None) -> List[FlowGroup]:
        """Get flowgroups, optionally filtered by pipeline.

        Includes synthetic flowgroups expanded from blueprints, deduplicated
        by `(blueprint_name, spec_index)` unless `set_blueprint_view_mode(
        expand=True)` was called. With a blueprint filter, only synthetics
        from that blueprint are emitted (and on-disk flowgroups are excluded).

        Populates ``self._flowgroup_file_paths`` as a side effect (mapping each
        flowgroup to its source YAML); ``build_graphs`` threads that index into
        the builder so the :class:`SourceParser` can resolve relative file
        references.
        """
        if self._flowgroups is None:
            self._flowgroups = self._discover_and_process_all_flowgroups()

        flowgroups = self._flowgroups

        if self._blueprint_filter is not None:
            allowed_keys = {
                key
                for key, prov in self._blueprint_provenance.items()
                if prov.blueprint_name == self._blueprint_filter
            }
            flowgroups = [
                fg for fg in flowgroups if (fg.pipeline, fg.flowgroup) in allowed_keys
            ]
        elif not self._expand_blueprints_view and self._blueprint_provenance:
            # Dedupe synthetics by (blueprint_name, spec_index); first wins.
            # Non-synthetic flowgroups pass through.
            seen_specs: Set[Tuple[str, int]] = set()
            deduped: List[FlowGroup] = []
            for fg in flowgroups:
                prov = self._blueprint_provenance.get((fg.pipeline, fg.flowgroup))
                if prov is None:
                    deduped.append(fg)
                    continue
                spec_key = (prov.blueprint_name, prov.spec_index)
                if spec_key in seen_specs:
                    continue
                seen_specs.add(spec_key)
                deduped.append(fg)
            flowgroups = deduped

        if pipeline_filter:
            return [fg for fg in flowgroups if fg.pipeline == pipeline_filter]
        return flowgroups

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
        """Process one flowgroup; fall back to raw fg on template/preset error."""
        try:
            ctx_in = FlowGroupContext(flowgroup=fg, source_yaml=file_path)
            ctx_out = self._flowgroup_resolver.process_flowgroup(
                ctx_in, substitution_mgr
            )
            processed_fg = ctx_out.flowgroup
            self._flowgroup_file_paths[processed_fg.flowgroup] = file_path
            return processed_fg
        except Exception as e:
            self.logger.warning(f"Could not process flowgroup {fg.flowgroup}: {e}")
            self._flowgroup_file_paths[fg.flowgroup] = file_path
            return fg

    def set_blueprint_view_mode(
        self,
        expand_blueprints: bool = False,
        blueprint: Optional[str] = None,
    ) -> None:
        """Configure how synthetic flowgroups are surfaced in the deps graph.

        Args:
            expand_blueprints: When True, emit one flowgroup per (blueprint x
                instance x spec) — the literal expansion. Default False
                dedupes by (blueprint_name, spec_index) for readable graphs at
                32k-flowgroup scale.
            blueprint: When set, restrict the graph to the named blueprint's
                flowgroups (still deduped unless expand_blueprints=True).
        """
        self._expand_blueprints_view = expand_blueprints
        self._blueprint_filter = blueprint
        # Invalidate cached flowgroup list so the new view is rebuilt next call
        # (cheap because the underlying discovery/expansion is also cached on
        # the discoverer's side — the cache invalidation here only affects the
        # processed/filtered list assembled in get_flowgroups).
        self._flowgroups = None
        self._flowgroup_file_paths = {}

    def get_execution_order(self, graphs: DependencyGraphs) -> List[List[str]]:
        return self._analyzer.get_execution_order(graphs)

    def detect_circular_dependencies(self, graphs: DependencyGraphs) -> List[List[str]]:
        return self._analyzer.detect_circular_dependencies(graphs)
