"""Bootstrap service: discovery → blueprint expansion → monitoring chain.

Owns the synthetic-context provenance table consulted when wrapping a
discovered :class:`FlowGroup` in its :class:`FlowGroupContext` envelope.

:stability: provisional
"""

import logging
from dataclasses import replace
from typing import Dict, List, Optional, Tuple

from lhp.models import FlowGroup, FlowGroupContext

from ...utils.performance_timer import perf_timer
from .._interfaces import (
    BaseFlowgroupBootstrapService,
    BaseFlowgroupDiscoveryService,
    BaseMonitoringFinalizerService,
)
from ..discovery.blueprint_discoverer import BlueprintDiscoverer
from ..processing.blueprint_expander import BlueprintExpander, BlueprintProvenance
from .monitoring_pipeline_builder import MonitoringBuildResult


class FlowgroupBootstrapService(BaseFlowgroupBootstrapService):
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
        # Invocation-scoped discovery memo. Precedent in this codebase:
        # ``ActionOrchestrator._pipeline_slice_cache``. Deliberate divergence:
        # that precedent caches a *mutable dict* used only internally, whereas
        # this result is handed across the public facade, so immutability is
        # load-bearing here -- hence a frozen ``Tuple`` returned by identity
        # rather than a mutable list (guards the aliasing hazard of 149de1dd).
        self._discovery_cache: Optional[Tuple[FlowGroup, ...]] = None

    def discover_all_flowgroups(self) -> Tuple[FlowGroup, ...]:
        """Discover disk-sourced flowgroups, then expand blueprints and monitoring.

        Side effects: populates ``self._synthetic_contexts`` and refreshes
        ``self._monitoring_result`` from the monitoring service's last build.
        Memoized for the invocation (see ``self._discovery_cache``): the list
        and these side-effect fields are produced atomically by the first
        (miss) call; subsequent calls early-return the cached tuple, leaving
        the side-effect fields untouched from that first call.
        """
        if self._discovery_cache is not None:
            return self._discovery_cache

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
        self._discovery_cache = tuple(flowgroups)
        return self._discovery_cache

    def reset_discovery_cache(self) -> None:
        """Drop the invocation-scoped discovery memo and its derived state.

        Forces the next :meth:`discover_all_flowgroups` to re-read the project
        from disk (blueprints and monitoring re-expanded), so an edit made
        outside the current process becomes visible without discarding the
        whole service graph. Clears ONLY discovery-derived state: the persisted
        graph cache and any in-process dependency-graph memo live elsewhere and
        are left untouched.

        Also resets the discovery service's lazily-built source-path index so a
        newly added / renamed / moved flowgroup resolves to its YAML file on the
        next :meth:`find_source_yaml_for_flowgroup` (the index memoizes misses
        as ``None`` and never re-scans on its own).
        """
        self._discovery_cache = None
        self._synthetic_contexts = {}
        self._monitoring_result = None
        self._discovery.reset_source_path_index()  # type: ignore[attr-defined]  # concrete-only method; ABC narrows surface to discover_flowgroups

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

        Instances are always discovered (symmetrically with the validate path,
        per CODING_CONSTITUTION §9.24) so that instance/blueprint validation is
        not silently skipped when no blueprint files are present. Returns an
        empty result only when no instance files are present in the project
        (the entire feature is fully opt-in via file presence).
        """
        blueprints = self._blueprint_discoverer.discover_blueprints()

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
        """Mirrors the monitoring service's ``last_build_result`` onto ``self._monitoring_result`` for callers that read it directly."""
        self._monitoring.build_flowgroup(discovered_flowgroups)
        self._monitoring_result = self._monitoring.last_build_result  # type: ignore[attr-defined]  # concrete-only property; ABC narrows surface to build_flowgroup
        return self._monitoring_result
