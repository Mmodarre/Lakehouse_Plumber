"""Contract test: the ``DependencyFacade`` sub-facade surface.

``DependencyFacade`` was split out of ``InspectionFacade`` to keep the
latter under the §3.2 method cap. These tests pin the split: the two
dependency operations live on ``facade.dependency`` (not
``facade.inspection``), the sub-facade shares the app's orchestrator,
and the move is a hard move — no aliases remain on the inspection facade.
"""

from __future__ import annotations

import pytest

from lhp.api import DependencyFacade, LakehousePlumberApplicationFacade


@pytest.mark.unit
class TestDependencyFacadeWiring:
    def test_dependency_subfacade_is_wired_and_shares_orchestrator(self) -> None:
        orchestrator = object()
        app = LakehousePlumberApplicationFacade(orchestrator)

        assert isinstance(app.dependency, DependencyFacade)
        # The sub-facade receives the SAME orchestrator the app holds.
        assert app.dependency._orchestrator is orchestrator

    def test_dependency_methods_live_on_dependency_facade(self) -> None:
        app = LakehousePlumberApplicationFacade(object())

        assert hasattr(app.dependency, "analyze_dependencies")
        assert hasattr(app.dependency, "save_dependency_outputs")

    def test_hard_move_leaves_no_alias_on_inspection(self) -> None:
        app = LakehousePlumberApplicationFacade(object())

        assert not hasattr(app.inspection, "analyze_dependencies")
        assert not hasattr(app.inspection, "save_dependency_outputs")
