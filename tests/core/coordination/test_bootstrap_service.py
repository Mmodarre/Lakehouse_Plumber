"""Unit tests for :class:`FlowgroupBootstrapService`."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from unittest.mock import MagicMock

import pytest

from lhp.core.coordination._interfaces import (
    BaseFlowgroupBootstrapService,
    BaseFlowgroupDiscoveryService,
    BaseMonitoringFinalizerService,
)
from lhp.core.coordination.bootstrap_service import FlowgroupBootstrapService
from lhp.core.coordination.monitoring_pipeline_builder import MonitoringBuildResult
from lhp.core.processing.blueprint_expander import BlueprintProvenance
from lhp.models.config import Blueprint, BlueprintInstance, FlowGroup, FlowGroupContext


class _FakeDiscovery(BaseFlowgroupDiscoveryService):
    """Test fake; tracks calls + returns fixed result."""

    def __init__(self, flowgroups: Tuple[FlowGroup, ...] = ()) -> None:
        self._flowgroups = flowgroups
        self.discover_calls: List[Optional[str]] = []
        self.register_calls: List[Mapping[Tuple[str, str], Path]] = []
        self.source_yaml_lookups: List[FlowGroup] = []
        self.source_yaml_return: Optional[Path] = None

    def discover_flowgroups(
        self, *, pipeline_filter: Optional[str] = None
    ) -> Tuple[FlowGroup, ...]:
        self.discover_calls.append(pipeline_filter)
        return self._flowgroups

    def get_include_patterns(self) -> Tuple[str, ...]:
        return ()

    # Non-ABC method used by FlowgroupBootstrapService.
    def register_synthetic_sources(
        self, sources: Mapping[Tuple[str, str], Path]
    ) -> None:
        self.register_calls.append(sources)

    # Non-ABC method used by FlowgroupBootstrapService.make_context.
    def find_source_yaml_for_flowgroup(self, fg: FlowGroup) -> Optional[Path]:
        self.source_yaml_lookups.append(fg)
        return self.source_yaml_return


class _FakeMonitoring(BaseMonitoringFinalizerService):
    """Test fake."""

    def __init__(self, build_result: Optional[MonitoringBuildResult] = None) -> None:
        self._build_result = build_result
        self.build_calls: List[Tuple[FlowGroup, ...]] = []

    def build_flowgroup(
        self, discovered_flowgroups: Sequence[FlowGroup]
    ) -> Optional[FlowGroup]:
        self.build_calls.append(tuple(discovered_flowgroups))
        if self._build_result and self._build_result.context is not None:
            return self._build_result.context.flowgroup
        return None

    def finalize_artifacts(self, env: str, output_dir: Path) -> None:
        pass

    def cleanup_artifacts(self, env: str, output_dir: Path) -> None:
        pass

    @property
    def last_build_result(self) -> Optional[MonitoringBuildResult]:
        return self._build_result


class _FakeBlueprintDiscoverer:
    """Test fake; mirrors the methods on :class:`BlueprintDiscoverer` that the
    bootstrap service calls."""

    def __init__(
        self,
        blueprints: Optional[Dict[str, Tuple[Blueprint, Path]]] = None,
        instances: Optional[List[Tuple[BlueprintInstance, Path]]] = None,
    ) -> None:
        self._blueprints = blueprints if blueprints is not None else {}
        self._instances = instances if instances is not None else []

    def discover_blueprints(self) -> Dict[str, Tuple[Blueprint, Path]]:
        return self._blueprints

    def discover_instances(
        self, blueprints: Dict[str, Tuple[Blueprint, Path]]
    ) -> List[Tuple[BlueprintInstance, Path]]:
        return self._instances


class _FakeBlueprintExpander:
    """Test fake; mirrors :class:`BlueprintExpander.expand`."""

    def __init__(
        self,
        contexts: Optional[List[FlowGroupContext]] = None,
        provenance: Optional[Dict[Tuple[str, str], BlueprintProvenance]] = None,
    ) -> None:
        self._contexts = contexts if contexts is not None else []
        self._provenance = provenance if provenance is not None else {}
        self.expand_calls: List[Tuple[Any, Any]] = []

    def expand(
        self,
        blueprints: Dict[str, Tuple[Blueprint, Path]],
        instances: List[Tuple[BlueprintInstance, Path]],
    ) -> Tuple[List[FlowGroupContext], Dict[Tuple[str, str], BlueprintProvenance]]:
        self.expand_calls.append((blueprints, instances))
        return self._contexts, self._provenance


def _make_service(
    *,
    discovery: Optional[_FakeDiscovery] = None,
    blueprint_discoverer: Optional[_FakeBlueprintDiscoverer] = None,
    blueprint_expander: Optional[_FakeBlueprintExpander] = None,
    monitoring: Optional[_FakeMonitoring] = None,
    logger: Optional[logging.Logger] = None,
) -> FlowgroupBootstrapService:
    """Build a FlowgroupBootstrapService with typed fakes by default."""
    return FlowgroupBootstrapService(
        discovery=discovery or _FakeDiscovery(),
        blueprint_discoverer=blueprint_discoverer or _FakeBlueprintDiscoverer(),
        blueprint_expander=blueprint_expander or _FakeBlueprintExpander(),
        monitoring=monitoring or _FakeMonitoring(),
        logger=logger or logging.getLogger("test_flowgroup_bootstrap_service"),
    )


@pytest.mark.unit
def test_implements_abc_and_constructs_with_keyword_args() -> None:
    """FlowgroupBootstrapService subclasses BaseFlowgroupBootstrapService and constructs cleanly."""
    assert issubclass(FlowgroupBootstrapService, BaseFlowgroupBootstrapService)
    service = _make_service()
    assert isinstance(service, BaseFlowgroupBootstrapService)
    assert service._synthetic_contexts == {}
    assert service._monitoring_result is None


@pytest.mark.unit
def test_make_context_synthetic_lookup() -> None:
    """When ``_synthetic_contexts`` has an entry, ``make_context`` returns the
    synthetic envelope with ``source_yaml`` replaced from the discovery service."""
    fg = FlowGroup(pipeline="p", flowgroup="fg", actions=[])
    synthetic_ctx = FlowGroupContext(flowgroup=fg, source_yaml=None, synthetic=True)

    discovery = _FakeDiscovery()
    resolved_yaml = MagicMock(name="resolved_source_yaml_path")
    discovery.source_yaml_return = resolved_yaml

    service = _make_service(discovery=discovery)
    service._synthetic_contexts[(fg.pipeline, fg.flowgroup)] = synthetic_ctx

    result = service.make_context(fg)

    assert result.flowgroup is fg
    assert result.source_yaml is resolved_yaml
    assert result.synthetic is True
    assert discovery.source_yaml_lookups == [fg]


@pytest.mark.unit
def test_make_context_fallback_when_no_provenance() -> None:
    """When no synthetic provenance is recorded for ``fg``, ``make_context``
    falls back to a plain :class:`FlowGroupContext`."""
    fg = FlowGroup(pipeline="p", flowgroup="fg", actions=[])
    discovery = _FakeDiscovery()
    discovery.source_yaml_return = None

    service = _make_service(discovery=discovery)

    result = service.make_context(fg)

    assert isinstance(result, FlowGroupContext)
    assert result.flowgroup is fg
    assert result.source_yaml is None
    assert result.synthetic is False


@pytest.mark.unit
def test_discover_all_flowgroups_returns_empty_when_no_disk_flowgroups_no_blueprints() -> (
    None
):
    """No disk flowgroups, no blueprints, no monitoring context produces an empty list."""
    discovery = _FakeDiscovery(flowgroups=())
    blueprint_discoverer = _FakeBlueprintDiscoverer(blueprints={})
    blueprint_expander = _FakeBlueprintExpander()
    monitoring = _FakeMonitoring(build_result=None)

    service = _make_service(
        discovery=discovery,
        blueprint_discoverer=blueprint_discoverer,
        blueprint_expander=blueprint_expander,
        monitoring=monitoring,
    )

    result = service.discover_all_flowgroups()

    assert result == []
    assert discovery.register_calls == []
    assert service._synthetic_contexts == {}
    assert service._monitoring_result is None


@pytest.mark.unit
def test_discover_all_flowgroups_populates_synthetic_contexts_from_blueprint_expansion() -> (
    None
):
    """A blueprint + instance + expander result populates synthetic contexts and
    registers the provenance map with the discovery service."""
    synthetic_fg = FlowGroup(pipeline="p", flowgroup="syn", actions=[])
    synthetic_ctx = FlowGroupContext(
        flowgroup=synthetic_fg, source_yaml=None, synthetic=True
    )
    blueprint_path = Path("/tmp/bp.yaml")
    instance_path = Path("/tmp/instance.yaml")
    blueprint = Blueprint(name="bp", flowgroups=[])
    blueprint_dict: Dict[str, Tuple[Blueprint, Path]] = {
        "bp": (blueprint, blueprint_path)
    }
    instance = BlueprintInstance(blueprint="bp")
    instances = [(instance, instance_path)]
    provenance = {
        (synthetic_fg.pipeline, synthetic_fg.flowgroup): BlueprintProvenance(
            blueprint_name="bp",
            blueprint_path=blueprint_path,
            instance_path=instance_path,
            flowgroup=synthetic_fg,
            spec_index=0,
        )
    }

    discovery = _FakeDiscovery(flowgroups=())
    blueprint_discoverer = _FakeBlueprintDiscoverer(
        blueprints=blueprint_dict, instances=instances
    )
    blueprint_expander = _FakeBlueprintExpander(
        contexts=[synthetic_ctx], provenance=provenance
    )
    monitoring = _FakeMonitoring(build_result=None)

    service = _make_service(
        discovery=discovery,
        blueprint_discoverer=blueprint_discoverer,
        blueprint_expander=blueprint_expander,
        monitoring=monitoring,
    )

    result = service.discover_all_flowgroups()

    assert result == [synthetic_fg]
    assert service._synthetic_contexts == {
        (synthetic_fg.pipeline, synthetic_fg.flowgroup): synthetic_ctx
    }
    assert len(discovery.register_calls) == 1
    assert discovery.register_calls[0] == {
        (synthetic_fg.pipeline, synthetic_fg.flowgroup): blueprint_path
    }


@pytest.mark.unit
def test_discover_all_flowgroups_appends_monitoring_flowgroup_when_monitoring_returns_context() -> (
    None
):
    """A non-None monitoring context is appended to the result and recorded in
    ``_synthetic_contexts``."""
    monitoring_fg = FlowGroup(pipeline="monitoring", flowgroup="mon", actions=[])
    monitoring_ctx = FlowGroupContext(
        flowgroup=monitoring_fg, source_yaml=None, synthetic=True
    )
    monitoring_result = MonitoringBuildResult(
        context=monitoring_ctx,
        template_context={},
        eligible_pipelines=[],
        pipeline_name="monitoring",
    )

    discovery = _FakeDiscovery(flowgroups=())
    monitoring = _FakeMonitoring(build_result=monitoring_result)

    service = _make_service(discovery=discovery, monitoring=monitoring)

    result = service.discover_all_flowgroups()

    assert result == [monitoring_fg]
    assert service._monitoring_result is monitoring_result
    assert service._synthetic_contexts == {
        (monitoring_fg.pipeline, monitoring_fg.flowgroup): monitoring_ctx
    }
    assert monitoring.build_calls == [()]
