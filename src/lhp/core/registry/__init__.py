"""Action registry, generator factories, and the base action generator.

Public re-exports:
- ActionRegistry        — registry of action types → generator classes
- BaseActionGenerator   — base class every action generator extends
- OrchestrationDependencies — DI container for orchestrator services
- SubstitutionFactory   — protocol for substitution factories
- DefaultSubstitutionFactory — default impl

Internal helpers stay in their respective modules.
"""
# Eager re-exports (lightweight; no transitive imports of per-action generators)
from .base_generator import BaseActionGenerator
from .factories import (
    DefaultSubstitutionFactory,
    OrchestrationDependencies,
    SubstitutionFactory,
)

# Lazy re-export of ActionRegistry: importing it eagerly would pull in every
# per-action generator (load/transform/write/test), and those generators
# import BaseActionGenerator from this package — causing a circular import
# when a consumer first touches `lhp.core.registry` via a generator path.
# Deferring to module-level __getattr__ (PEP 562) keeps the public attribute
# contract intact while breaking the cycle.
def __getattr__(name):
    if name == "ActionRegistry":
        from .action_registry import ActionRegistry
        return ActionRegistry
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "ActionRegistry",
    "BaseActionGenerator",
    "DefaultSubstitutionFactory",
    "OrchestrationDependencies",
    "SubstitutionFactory",
]
