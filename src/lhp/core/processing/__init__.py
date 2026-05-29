"""Processing services for LakehousePlumber core.

Houses concrete implementations of the resolution and processing-stage
services composed by :class:`lhp.core.coordination.orchestrator.ActionOrchestrator`.

:stability: provisional
"""

from typing import TYPE_CHECKING, Any

from .blueprint_expander import BlueprintExpander
from .dqe import DQEParser
from .local_variables import LocalVariableResolver
from .namespace_normalizer import normalize_namespace_fields
from .substitution import EnhancedSubstitutionManager, SecretReference
from .template_engine import TemplateEngine

if TYPE_CHECKING:
    from .flowgroup_resolver import FlowgroupResolutionService

__all__ = [
    "BlueprintExpander",
    "DQEParser",
    "EnhancedSubstitutionManager",
    "FlowgroupResolutionService",
    "LocalVariableResolver",
    "SecretReference",
    "TemplateEngine",
    "normalize_namespace_fields",
]


def __getattr__(name: str) -> Any:
    # §5.4-compatible lazy resolution: imports through the package init are
    # endorsed, so deferring one entry to module-level __getattr__ (PEP 562)
    # preserves the public surface contract while breaking an import cycle:
    # flowgroup_resolver → core._interfaces → coordination/__init__.py
    # → monitoring_service → core.registry, which is mid-init when
    # registry.factories triggers the chain via core.processing.substitution.
    if name == "FlowgroupResolutionService":
        from .flowgroup_resolver import FlowgroupResolutionService

        return FlowgroupResolutionService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
