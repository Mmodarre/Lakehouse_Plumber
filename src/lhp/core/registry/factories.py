"""Factory interfaces for dependency injection in LakehousePlumber orchestrator."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

from ..processing.substitution import EnhancedSubstitutionManager


class SubstitutionFactory(ABC):
    """Factory interface for creating substitution managers.

    Internal contract per constitution §13.8 — implemented as
    :class:`abc.ABC` (not :class:`typing.Protocol`) so that wiring
    failures surface as :class:`TypeError` at construction rather than
    as :class:`AttributeError` on first call. Mirrors the ABC style of
    :mod:`lhp.core._interfaces`.
    """

    @abstractmethod
    def create(self, substitution_file: Path, env: str) -> EnhancedSubstitutionManager:
        raise NotImplementedError


class DefaultSubstitutionFactory(SubstitutionFactory):
    """Default implementation of :class:`SubstitutionFactory`."""

    def create(self, substitution_file: Path, env: str) -> EnhancedSubstitutionManager:
        return EnhancedSubstitutionManager(substitution_file, env)


@dataclass
class OrchestrationDependencies:
    """Dependency container for orchestrator injection."""

    substitution_factory: SubstitutionFactory = field(
        default_factory=lambda: DefaultSubstitutionFactory()
    )

    def create_substitution_manager(
        self, substitution_file: Path, env: str
    ) -> EnhancedSubstitutionManager:
        return self.substitution_factory.create(substitution_file, env)
