"""Unit tests for :class:`BootstrapService`."""

import logging
from unittest.mock import MagicMock

import pytest

from lhp.core.coordination._interfaces import (
    BaseBootstrapService,
    BaseFlowgroupDiscoveryService,
    BaseMonitoringFinalizerService,
)
from lhp.core.coordination.bootstrap_service import BootstrapService
from lhp.core.discovery.blueprint_discoverer import BlueprintDiscoverer
from lhp.core.processing.blueprint_expander import BlueprintExpander
from lhp.models.config import FlowGroup, FlowGroupContext


def _make_service(
    *,
    discovery=None,
    blueprint_discoverer=None,
    blueprint_expander=None,
    monitoring=None,
    logger=None,
) -> BootstrapService:
    """Build a BootstrapService with MagicMock collaborators by default."""
    return BootstrapService(
        discovery=discovery or MagicMock(spec=BaseFlowgroupDiscoveryService),
        blueprint_discoverer=blueprint_discoverer
        or MagicMock(spec=BlueprintDiscoverer),
        blueprint_expander=blueprint_expander or MagicMock(spec=BlueprintExpander),
        monitoring=monitoring or MagicMock(spec=BaseMonitoringFinalizerService),
        logger=logger or logging.getLogger("test.bootstrap_service"),
    )


@pytest.mark.unit
def test_implements_abc_and_constructs_with_keyword_args() -> None:
    """BootstrapService subclasses BaseBootstrapService and constructs cleanly."""
    assert issubclass(BootstrapService, BaseBootstrapService)
    service = _make_service()
    assert isinstance(service, BaseBootstrapService)
    assert service._synthetic_contexts == {}
    assert service._monitoring_result is None


@pytest.mark.unit
def test_make_context_synthetic_lookup() -> None:
    """When ``_synthetic_contexts`` has an entry, ``make_context`` returns the
    synthetic envelope with ``source_yaml`` replaced from the discovery service."""
    fg = FlowGroup(pipeline="p", flowgroup="fg", actions=[])
    synthetic_ctx = FlowGroupContext(flowgroup=fg, source_yaml=None, synthetic=True)

    discovery = MagicMock()
    resolved_yaml = MagicMock(name="resolved_source_yaml_path")
    discovery.find_source_yaml_for_flowgroup.return_value = resolved_yaml

    service = _make_service(discovery=discovery)
    service._synthetic_contexts[(fg.pipeline, fg.flowgroup)] = synthetic_ctx

    result = service.make_context(fg)

    assert result.flowgroup is fg
    assert result.source_yaml is resolved_yaml
    assert result.synthetic is True
    discovery.find_source_yaml_for_flowgroup.assert_called_once_with(fg)


@pytest.mark.unit
def test_make_context_fallback_when_no_provenance() -> None:
    """When no synthetic provenance is recorded for ``fg``, ``make_context``
    falls back to a plain :class:`FlowGroupContext`."""
    fg = FlowGroup(pipeline="p", flowgroup="fg", actions=[])
    discovery = MagicMock()
    discovery.find_source_yaml_for_flowgroup.return_value = None

    service = _make_service(discovery=discovery)

    result = service.make_context(fg)

    assert isinstance(result, FlowGroupContext)
    assert result.flowgroup is fg
    assert result.source_yaml is None
    assert result.synthetic is False
