"""Action registry, generator factories, and the base action generator.

Public re-exports:
- ActionRegistry        — registry of action types → generator classes
- BaseActionGenerator   — base class every action generator extends
- OrchestrationDependencies — DI container for orchestrator services
- SubstitutionFactory   — protocol for substitution factories
- DefaultSubstitutionFactory — default impl

Internal helpers stay in their respective modules.
"""

# Eager re-exports. ActionRegistry no longer imports the per-action generator
# families (load/transform/write/test) — those are registered into the core
# registry from the ``generators`` layer (ABOVE ``core``) via
# ``lhp.generators.registration``. The former circular import is gone, so the
# eager re-export here is safe.
from .action_registry import ActionRegistry, register_generators
from .base_generator import BaseActionGenerator
from .factories import (
    DefaultSubstitutionFactory,
    OrchestrationDependencies,
    SubstitutionFactory,
)

__all__ = [
    "ActionRegistry",
    "BaseActionGenerator",
    "DefaultSubstitutionFactory",
    "OrchestrationDependencies",
    "SubstitutionFactory",
    "register_generators",
]
