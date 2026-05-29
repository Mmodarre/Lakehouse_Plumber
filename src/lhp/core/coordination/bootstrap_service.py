"""Bootstrap service: 8th typed coordination service.

Owns the discovery → blueprint expansion → monitoring chain and the
synthetic-context provenance table consulted when wrapping a discovered
:class:`FlowGroup` in its :class:`FlowGroupContext` envelope. Consults
:class:`FlowgroupDiscoveryService`, :class:`BlueprintDiscoverer`,
:class:`BlueprintExpander`, and :class:`MonitoringFinalizerService`.

:stability: provisional
"""

import logging
from dataclasses import replace
from typing import Dict, List, Optional, Tuple

from lhp.models import FlowGroup, FlowGroupContext
from ...utils.performance_timer import perf_timer
from ..discovery.blueprint_discoverer import BlueprintDiscoverer
from ..processing.blueprint_expander import BlueprintExpander, BlueprintProvenance
from .._interfaces import (
    BaseFlowgroupBootstrapService,
    BaseFlowgroupDiscoveryService,
    BaseMonitoringFinalizerService,
)
from .monitoring_pipeline_builder import MonitoringBuildResult


class FlowgroupBootstrapService(BaseFlowgroupBootstrapService):
    """Concrete :class:`BaseFlowgroupBootstrapService`."""

    def __init__(
        self,
        *,
        discovery: BaseFlowgroupDiscoveryService,
        blueprint_discoverer: BlueprintDiscoverer,
        blueprint_expander: BlueprintExpander,
        monitoring: BaseMonitoringFinalizerService,
        logger: logging.Logger,
    ) -> None:
        self._discovery = discovery
        self._blueprint_discoverer = blueprint_discoverer
        self._blueprint_expander = blueprint_expander
        self._monitoring = monitoring
        self._logger = logger
        self._synthetic_contexts: Dict[Tuple[str, str], FlowGroupContext] = {}
        self._monitoring_result: Optional[MonitoringBuildResult] = None

    def discover_all_flowgroups(self) -> List[FlowGroup]:
        """Discover disk-sourced flowgroups, then expand blueprints and monitoring.

        Side effects: populates ``self._synthetic_contexts`` and refreshes
        ``self._monitoring_result`` from the monitoring service's last build.
        """
        with perf_timer("discover_all_flowgroups [orchestrator]"):
            flowgroups = list(self._discovery.discover_flowgroups(pipeline_filter=None))

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
            self._discovery.register_synthetic_sources(  # type: ignore[attr-defined]  # concrete-only method; ABC narrows surface to discover_flowgroups
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

    def make_context(self, fg: FlowGroup) -> FlowGroupContext:
        """Wrap a FlowGroup in its FlowGroupContext for the worker boundary.

        Looks up synthetic provenance (synthetic flag, auxiliary_files) from
        ``self._synthetic_contexts``; disk-sourced flowgroups get default values.
        Source YAML is resolved via the FlowgroupDiscoveryService (threading.Lock'd
        index — must run on the main process before spawn).
        """
        source_yaml = self._discovery.find_source_yaml_for_flowgroup(fg)
        if self._synthetic_contexts:
            existing = self._synthetic_contexts.get((fg.pipeline, fg.flowgroup))
            if existing is not None:
                return replace(existing, flowgroup=fg, source_yaml=source_yaml)
        return FlowGroupContext(flowgroup=fg, source_yaml=source_yaml)

    def _expand_blueprints(
        self,
    ) -> Tuple[List[FlowGroupContext], Dict[Tuple[str, str], BlueprintProvenance]]:
        """Discover and expand blueprints + instances into synthetic FlowGroupContexts.

        Returns an empty result when no blueprint or instance files are present
        in the project (the entire feature is fully opt-in via file presence).
        """
        blueprints = self._blueprint_discoverer.discover_blueprints()
        if not blueprints:
            return [], {}

        instances = self._blueprint_discoverer.discover_instances(blueprints)
        if not instances:
            self._logger.info(
                f"Found {len(blueprints)} blueprint(s) but no instance files; "
                "blueprint expansion produces no flowgroups."
            )
            return [], {}

        return self._blueprint_expander.expand(blueprints, instances)

    def _build_monitoring(
        self, discovered_flowgroups: List[FlowGroup]
    ) -> Optional[MonitoringBuildResult]:
        """Build monitoring artifacts via the monitoring service.

        Delegates to ``self._monitoring.build_flowgroup``; the service stashes
        the full :class:`MonitoringBuildResult` on its own ``last_build_result``
        property, which this service mirrors onto ``self._monitoring_result``
        for callers that read it directly.
        """
        self._monitoring.build_flowgroup(discovered_flowgroups)
        self._monitoring_result = self._monitoring.last_build_result  # type: ignore[attr-defined]  # concrete-only property; ABC narrows surface to build_flowgroup
        return self._monitoring_result
