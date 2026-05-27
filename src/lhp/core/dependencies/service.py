"""Dependency analysis composition root.

Implements :class:`BaseDependencyAnalysisService` (the canonical ABC under
``coordination/_interfaces.py``) by composing two internal collaborators:

- :class:`DependencyGraphBuilder` — discovery, source extraction, graph construction.
- :class:`DependencyAnalyzer` — pure topological / cycle / external-source analysis.

The constructor takes an already-loaded :class:`ProjectConfig` and a
:class:`ValidationService` (replacing the legacy 2-arg form that took a
``ProjectConfigLoader``). This matches the pattern established at
``orchestrator.py:170-175`` where the orchestrator already loads the config
and constructs the validation service before any downstream consumer.

This module is the new public re-export target. ``dependencies/__init__.py``
points :class:`DependencyAnalysisService` at this class.

:stability: provisional
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Literal, Optional, Sequence, Tuple

from ...models.config import FlowGroup, ProjectConfig
from ...models.dependencies import (
    DependencyAnalysisResult,
    DependencyGraphs,
    PipelineDependency,
)
from ...parsers.blueprint_parser import BlueprintParser
from ...parsers.yaml_parser import CachingYAMLParser, YAMLParser
from ...presets.preset_manager import PresetManager
from ..coordination import ValidationService
from ..coordination._interfaces import BaseDependencyAnalysisService
from ..discovery.blueprint_discoverer import BlueprintDiscoverer
from ..discovery.flowgroup_discoverer import FlowgroupDiscoveryService
from ..processing.blueprint_expander import BlueprintExpander
from ..processing.flowgroup_resolver import FlowgroupResolutionService
from ..loaders import ProjectConfigLoader
from ..processing import TemplateEngine
from ..validators import ConfigValidator
from ..validators.secret_validator import SecretValidator
from . import output
from .analyzer import DependencyAnalyzer
from .builder import DependencyGraphBuilder


class DependencyAnalysisService(BaseDependencyAnalysisService):
    """Composition root for dependency analysis.

    Inherits the :class:`BaseDependencyAnalysisService` ABC. Composes a
    builder (graph + discovery + source extraction), an analyzer
    (topological / cycle analysis), a metrics service (placeholders), and
    uses ``output.export_to_*`` for serialization.

    The constructor signature is the new 3-arg form (``project_root``,
    ``project_config``, ``validation_service``). External callers
    (orchestrator, CLI) construct the validation service first and pass
    it in; the legacy 2-arg ``(project_root, config_loader)`` form is
    removed by Phase B6's hard cut.

    :stability: provisional
    """

    def __init__(
        self,
        project_root: Path,
        project_config: ProjectConfig,
        validation_service: ValidationService,
        *,
        config_validator: Optional[ConfigValidator] = None,
    ) -> None:
        """Wire the composition root.

        Args:
            project_root: Root directory of the LakehousePlumber project.
            project_config: Already-loaded :class:`ProjectConfig` (the
                orchestrator/CLI loads this before constructing the
                service; loader-vs-config translation is confined to a
                single ``ProjectConfigLoader`` built here for the
                discovery service only).
            validation_service: Live :class:`ValidationService`. Kept
                as a typed collaborator for callers that still pass it
                positionally; ``config_validator`` is the preferred
                injection point.
            config_validator: Pre-built :class:`ConfigValidator`
                injected by
                :func:`lhp.core.coordination.layers.build_facade_orchestrator`.
                When ``None``, a fresh ``ConfigValidator`` is
                constructed here (legacy fallback for callers not yet
                migrated through the application facade).
        """
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

        # Discovery / expansion collaborators.
        flowgroup_discoverer = FlowgroupDiscoveryService(
            project_root,
            self._project_config_loader,
            yaml_parser=self._cached_yaml_parser,
        )
        blueprint_parser = BlueprintParser(
            caching_yaml_parser=self._cached_yaml_parser
        )
        blueprint_discoverer = BlueprintDiscoverer(
            project_root,
            project_config=project_config,
            blueprint_parser=blueprint_parser,
            caching_yaml_parser=self._cached_yaml_parser,
        )
        blueprint_expander = BlueprintExpander()

        # Template + preset engines.
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

        flowgroup_resolver = FlowgroupResolutionService(
            template_engine,
            preset_manager,
            cfg_validator,
            secret_validator,
        )

        # Compose the three internal services.
        self._builder = DependencyGraphBuilder(
            project_root=project_root,
            flowgroup_discoverer=flowgroup_discoverer,
            blueprint_discoverer=blueprint_discoverer,
            blueprint_expander=blueprint_expander,
            flowgroup_resolver=flowgroup_resolver,
        )
        self._analyzer = DependencyAnalyzer()

    # --- BaseDependencyAnalysisService ABC implementations ------------------

    def build_graphs(self, flowgroups: Sequence[FlowGroup]) -> DependencyGraphs:
        """Build action/flowgroup/pipeline dependency graphs from a flowgroup set.

        The ABC takes the flowgroup sequence directly; the builder's
        ``build_from_flowgroups`` skips discovery and proceeds straight to
        graph construction.
        """
        return self._builder.build_from_flowgroups(list(flowgroups))

    def analyze(self, graphs: DependencyGraphs) -> DependencyAnalysisResult:
        """Run topological / cycle / external-source analysis on the given graphs."""
        return self._analyzer.analyze(graphs)

    def export(
        self,
        result: DependencyAnalysisResult,
        format: Literal["dot", "json", "text"],
    ) -> str:
        """Serialize the analysis result in the requested format."""
        if format == "dot":
            return output.export_to_dot(result.graphs, level="pipeline")
        if format == "json":
            return json.dumps(
                output.export_to_json(result), indent=2, ensure_ascii=False
            )
        if format == "text":
            return output.export_to_text(result)
        raise ValueError(f"Unknown format: {format!r}")

    # --- Ancillary methods (called by CLI / output.py) ----------------------

    def get_project_name(self) -> str:
        """Get the project name from lhp.yaml configuration.

        Returns:
            Project name from ``project_config.name``, or falls back to the
            project root directory name if the config has no name field.
        """
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

        Raises:
            LHPError: If ``job_name`` validation fails.
        """
        from ..validators.job_name_validator import validate_job_names

        self.logger.info("Starting multi-job dependency analysis...")

        # Get all flowgroups
        flowgroups = self._builder.get_flowgroups()

        if not flowgroups:
            self.logger.warning("No flowgroups found for analysis")
            empty_graphs = self._builder._create_empty_graphs()
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

        # Check if any flowgroup has job_name
        has_job_name = any(fg.job_name for fg in flowgroups)

        if not has_job_name:
            # No job_name defined - single-job analysis path.
            self.logger.info("No job_name defined - performing single-job analysis")
            graphs = self._builder.build_from_flowgroups(flowgroups)
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

        # First: analyze all flowgroups together for global view.
        self.logger.info("Step 1: Analyzing all flowgroups together (global view)")
        global_graphs = self._builder.build_from_flowgroups(flowgroups)
        global_result = self._analyzer.analyze(global_graphs)

        # Second: partition the global result by job_name.
        self.logger.info(
            f"Step 2: Partitioning global result by {len(job_groups)} job group(s)"
        )
        job_results = self._analyzer.partition_result_by_job(
            global_result, flowgroups
        )

        self.logger.info(
            f"Multi-job analysis complete: {len(job_results)} job(s), "
            f"{len(flowgroups)} total flowgroups"
        )

        return job_results, global_result

    def partition_result_by_job(
        self,
        global_result: DependencyAnalysisResult,
        flowgroups: List[FlowGroup],
    ) -> Dict[str, DependencyAnalysisResult]:
        """Partition a global dependency analysis result by ``job_name``."""
        return self._analyzer.partition_result_by_job(global_result, flowgroups)

    def get_flowgroups(
        self, pipeline_filter: Optional[str] = None
    ) -> List[FlowGroup]:
        """Get flowgroups, optionally filtered by pipeline."""
        return self._builder.get_flowgroups(pipeline_filter)

    def set_blueprint_view_mode(
        self,
        expand_blueprints: bool = False,
        blueprint: Optional[str] = None,
    ) -> None:
        """Configure how synthetic flowgroups are surfaced in the deps graph."""
        self._builder.set_blueprint_view_mode(expand_blueprints, blueprint)

    def get_execution_order(self, graphs: DependencyGraphs) -> List[List[str]]:
        """Get pipeline execution order using topological sorting."""
        return self._analyzer.get_execution_order(graphs)

    def detect_circular_dependencies(
        self, graphs: DependencyGraphs
    ) -> List[List[str]]:
        """Detect circular dependencies at all graph levels."""
        return self._analyzer.detect_circular_dependencies(graphs)

    # --- Metrics forwards ---------------------------------------------------

    

    

    
