"""Factory interfaces for dependency injection in LakehousePlumber orchestrator."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from ..utils.substitution import EnhancedSubstitutionManager


class SubstitutionFactory(Protocol):
    """Factory interface for creating substitution managers."""

    def create(self, substitution_file: Path, env: str) -> EnhancedSubstitutionManager:
        """
        Create a substitution manager for the given environment.

        Args:
            substitution_file: Path to substitution YAML file
            env: Environment name

        Returns:
            EnhancedSubstitutionManager instance
        """
        ...


class DefaultSubstitutionFactory:
    """Default implementation of SubstitutionFactory."""

    def create(self, substitution_file: Path, env: str) -> EnhancedSubstitutionManager:
        """Create default substitution manager."""
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
        """Create substitution manager using factory."""
        return self.substitution_factory.create(substitution_file, env)
