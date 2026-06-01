"""Contract tests for the substitution/orchestration DI factories.

Covers the ABC contract of :class:`SubstitutionFactory`, the concrete
:class:`DefaultSubstitutionFactory`, and the
:class:`OrchestrationDependencies` injection seam. These replace the old
``test_dependency_factories_work`` green-lie smoke test (which only
asserted ``is not None``) with assertions that pin down the real
contract per constitution §13.8 (ABC wiring failures surface as
``TypeError`` at construction).
"""

from pathlib import Path

import pytest

from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import (
    DefaultSubstitutionFactory,
    OrchestrationDependencies,
    SubstitutionFactory,
)


@pytest.fixture
def substitution_file(tmp_path):
    """A minimal substitution YAML file on disk for factory ``create``."""
    path = tmp_path / "test.yaml"
    path.write_text("dev:\n  catalog: test_catalog\n")
    return path


@pytest.mark.unit
class TestSubstitutionFactoryContract:
    """Contract tests for the substitution factory ABC and default impl."""

    def test_abstract_factory_cannot_be_instantiated(self):
        """``SubstitutionFactory`` is an ABC and refuses direct construction.

        Per constitution §13.8 the factory seam is an ``abc.ABC`` (not a
        ``typing.Protocol``) so that wiring failures surface as
        ``TypeError`` at construction rather than ``AttributeError`` on
        first call.
        """
        with pytest.raises(TypeError):
            SubstitutionFactory()  # type: ignore[abstract]

    def test_default_factory_create_returns_substitution_manager(
        self, substitution_file
    ):
        """``DefaultSubstitutionFactory.create`` yields a real manager."""
        factory = DefaultSubstitutionFactory()

        manager = factory.create(substitution_file, "dev")

        assert isinstance(manager, EnhancedSubstitutionManager)
        assert manager.env == "dev"


@pytest.mark.unit
class TestOrchestrationDependenciesInjection:
    """Tests for the ``OrchestrationDependencies`` injection seam."""

    def test_custom_subclass_factory_is_accepted(self):
        """A custom ``SubstitutionFactory`` subclass is injectable.

        Implementing the abstract ``create`` method makes the subclass
        concrete, so passing an instance to the constructor must succeed
        and the instance must be retained on ``substitution_factory``.
        """

        class _CustomFactory(SubstitutionFactory):
            def create(
                self, substitution_file: Path, env: str
            ) -> EnhancedSubstitutionManager:
                return EnhancedSubstitutionManager(substitution_file, env)

        custom = _CustomFactory()

        deps = OrchestrationDependencies(substitution_factory=custom)

        assert deps.substitution_factory is custom

    def test_create_delegates_to_injected_factory(self, substitution_file):
        """``create_substitution_manager`` delegates to the injected factory.

        Injects a spy subclass whose ``create`` records its arguments and
        returns a sentinel; the container must hand that sentinel straight
        back and forward the exact ``(file, env)`` arguments.
        """
        sentinel = object()

        class _SpyFactory(SubstitutionFactory):
            def __init__(self):
                self.calls = []

            def create(self, substitution_file: Path, env: str):
                self.calls.append((substitution_file, env))
                return sentinel

        spy = _SpyFactory()
        deps = OrchestrationDependencies(substitution_factory=spy)

        result = deps.create_substitution_manager(substitution_file, "prod")

        assert result is sentinel
        assert spy.calls == [(substitution_file, "prod")]
