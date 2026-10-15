"""Action registry, generator factories, and the base action generator.

Public re-exports:
- ActionRegistry        — registry of action types → generator classes
- determine_action_subtype — the registry sub-type key for an action
- BaseActionGenerator   — base class every action generator extends
- OrchestrationDependencies — DI container for orchestrator services
- SubstitutionFactory   — protocol for substitution factories
- DefaultSubstitutionFactory — default impl
"""

# Eager re-exports. ActionRegistry no longer imports the per-action generator
# families (load/transform/write/test) — those are registered into the core
# registry from the ``generators`` layer (ABOVE ``core``) via
# ``lhp.generators.registration``. The former circular import is gone, so the
# eager re-export here is safe.
from .action_registry import (
    ActionRegistry,
    determine_action_subtype,
    register_generators,
)
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
    "determine_action_subtype",
    "register_generators",
]
