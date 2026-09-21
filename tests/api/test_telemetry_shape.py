"""Behavioural tests for :mod:`lhp.api._telemetry_shape`.

The project shape is the one piece of a ``cli.command`` telemetry event
that describes the user's project, so the properties that matter are as
much about what it CANNOT carry as about what it counts:

* its keys are exactly ``PROJECT_SHAPE_KEYS`` and its values are
  integers and booleans, so no name, path or free text can ride along —
  the fixture's own pipeline, flowgroup and table names are collected
  from its YAML at test time and asserted absent;
* it is bounded in time: a read that pushes the composition past the
  wall-clock budget abandons the shape instead of slowing the command;
* it never raises: a facade whose read fails yields ``None``.

The happy path runs against a REAL facade over an isolated deep copy of
the e2e fixture project (never mutated); the budget and failure paths
use a stub facade and an injected clock, so neither depends on how fast
this machine reads the fixture.

``enforce_version=False`` relaxes the ``required_lhp_version`` gate so
the fixture is independent of the installed package version.
"""

from __future__ import annotations

import itertools
import os
import shutil
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set

import pytest
import yaml

from lhp.api import LakehousePlumberApplicationFacade
from lhp.api._telemetry_shape import build_project_shape
from lhp.api.responses import StatsResult
from lhp.api.views import BlueprintView, FlowgroupView, ProjectConfigView
from lhp.telemetry import PROJECT_SHAPE_KEYS

pytestmark = pytest.mark.unit

_FIXTURE_PATH = Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"

# YAML keys under which the fixture spells a name of its own.
_NAME_KEYS = frozenset(
    {"pipeline", "flowgroup", "name", "table", "catalog", "schema", "database"}
)

# The vocabulary the allowlist itself is spelled in. A fixture name drawn
# from it (say a flowgroup called "python") is already a substring of a
# legitimate key, so its presence proves nothing either way and it is
# excluded from the leak check rather than asserted on.
_ALLOWLIST_VOCABULARY = " ".join(PROJECT_SHAPE_KEYS)


@pytest.fixture
def fixture_project(tmp_path: Path) -> Iterator[Path]:
    """Isolated deep copy of the e2e fixture, with cwd swapped to its root.

    LHP resolves several relative paths off ``Path.cwd()``, so the copy is
    made the working directory for the duration of the test.
    """
    root = tmp_path / "testing_project"
    shutil.copytree(_FIXTURE_PATH, root)
    original_cwd = os.getcwd()
    os.chdir(root)
    try:
        yield root
    finally:
        os.chdir(original_cwd)


@pytest.fixture
def facade(fixture_project: Path) -> LakehousePlumberApplicationFacade:
    return LakehousePlumberApplicationFacade.for_project(
        fixture_project, enforce_version=False
    )


@pytest.fixture
def shape(
    facade: LakehousePlumberApplicationFacade, fixture_project: Path
) -> Dict[str, Any]:
    composed = build_project_shape(facade, fixture_project)
    assert composed is not None, "the fixture project must compose within budget"
    return composed


def _collect_names(node: Any, found: Set[str]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key in _NAME_KEYS and isinstance(value, str):
                found.add(value)
            _collect_names(value, found)
    elif isinstance(node, list):
        for item in node:
            _collect_names(item, found)


def _fixture_names(root: Path) -> Set[str]:
    """Names the fixture project authors: pipelines, flowgroups, tables.

    Read from the YAML at test time rather than hard-coded, so extending
    the fixture cannot quietly narrow what the leak check covers.
    """
    found: Set[str] = set()
    sources = [root / "lhp.yaml", *sorted((root / "pipelines").rglob("*.yaml"))]
    for path in sources:
        for document in yaml.safe_load_all(path.read_text("utf-8")):
            _collect_names(document, found)
    return {
        name
        for name in found
        if len(name) >= 4 and "$" not in name and name not in _ALLOWLIST_VOCABULARY
    }


def _ticking_clock(step: float) -> Callable[[], float]:
    """A clock that starts at zero and advances by ``step`` per call."""
    ticks = itertools.count()
    return lambda: next(ticks) * step


class _StubInspection:
    """Records the reads a composition makes; optionally fails one of them."""

    def __init__(self, failing: Optional[str] = None) -> None:
        self.calls: List[str] = []
        self._failing = failing

    def _record(self, name: str) -> None:
        self.calls.append(name)
        if name == self._failing:
            raise RuntimeError("the facade read failed")

    def compute_stats(self) -> StatsResult:
        self._record("compute_stats")
        return StatsResult(
            pipeline_count=2,
            flowgroup_count=3,
            total_actions=4,
            action_counts_by_type={"load": 4, "load_delta": 4},
        )

    def get_project_config(self) -> ProjectConfigView:
        self._record("get_project_config")
        return ProjectConfigView(name="stub", version="1.0")

    def list_flowgroups(self) -> tuple[FlowgroupView, ...]:
        self._record("list_flowgroups")
        return ()

    def list_blueprints(
        self, *, include_instances: bool = True
    ) -> tuple[BlueprintView, ...]:
        self._record(f"list_blueprints(include_instances={include_instances})")
        return ()

    def list_presets(self) -> tuple[Any, ...]:
        self._record("list_presets")
        return ()

    def list_templates(self) -> tuple[Any, ...]:
        self._record("list_templates")
        return ()


class _StubFacade:
    def __init__(self, inspection: _StubInspection) -> None:
        self.inspection = inspection


class TestFixtureProjectShape:
    def test_the_shape_is_exactly_the_allowlist(self, shape: Dict[str, Any]) -> None:
        assert set(shape) == set(PROJECT_SHAPE_KEYS)

    def test_every_value_is_an_int_or_a_bool(self, shape: Dict[str, Any]) -> None:
        assert all(isinstance(value, (int, bool)) for value in shape.values())

    def test_the_fixture_counts_are_populated(self, shape: Dict[str, Any]) -> None:
        assert shape["pipelines"] > 0
        assert shape["flowgroups"] > 0
        assert shape["actions"] > 0
        assert shape["tables"] > 0
        assert shape["environments"] == 3
        assert 0 < shape["flowgroups_using_templates"] <= shape["flowgroups"]

    def test_the_counts_agree_with_the_facade_reads(
        self,
        shape: Dict[str, Any],
        facade: LakehousePlumberApplicationFacade,
    ) -> None:
        stats = facade.inspection.compute_stats()
        counts = stats.action_counts_by_type
        assert shape["pipelines"] == stats.pipeline_count
        assert shape["flowgroups"] == stats.flowgroup_count
        assert shape["actions"] == stats.total_actions
        assert shape["transform_sql"] == counts["transform_sql"]
        assert shape["templates"] == len(facade.inspection.list_templates())
        assert shape["presets"] == len(facade.inspection.list_presets())
        blueprints = facade.inspection.list_blueprints(include_instances=False)
        assert shape["blueprints"] == len(blueprints)
        assert shape["blueprint_instances"] == sum(
            view.instance_count for view in blueprints
        )

    def test_the_flags_agree_with_the_project_config(
        self,
        shape: Dict[str, Any],
        facade: LakehousePlumberApplicationFacade,
    ) -> None:
        config = facade.inspection.get_project_config()
        assert shape["has_operational_metadata"] == config.has_operational_metadata
        assert shape["has_event_log"] == config.has_event_log
        assert shape["has_monitoring"] == config.has_monitoring
        assert shape["has_uc_tagging"] == config.has_uc_tagging
        assert shape["has_test_reporting"] == config.has_test_reporting
        assert shape["has_wheel"] == config.has_wheel
        assert shape["has_sandbox"] == config.has_sandbox
        assert shape["apply_formatting"] == config.apply_formatting
        assert shape["has_required_lhp_version"] is True

    def test_no_fixture_name_reaches_the_shape(
        self, shape: Dict[str, Any], fixture_project: Path
    ) -> None:
        names = _fixture_names(fixture_project)
        assert names, (
            "the fixture must contribute names for this check to mean anything"
        )
        rendered = repr(shape)
        leaked = sorted(name for name in names if name in rendered)
        assert leaked == []


class TestWallClockBudget:
    def test_a_read_over_budget_abandons_the_shape(
        self, facade: LakehousePlumberApplicationFacade, fixture_project: Path
    ) -> None:
        composed = build_project_shape(
            facade, fixture_project, clock=_ticking_clock(1.0)
        )

        assert composed is None

    def test_an_over_budget_composition_stops_reading(
        self, fixture_project: Path
    ) -> None:
        inspection = _StubInspection()

        composed = build_project_shape(
            _StubFacade(inspection), fixture_project, clock=_ticking_clock(1.0)
        )

        assert composed is None
        assert inspection.calls == ["compute_stats"]

    def test_a_budget_that_is_never_exceeded_composes(
        self, fixture_project: Path
    ) -> None:
        inspection = _StubInspection()

        composed = build_project_shape(
            _StubFacade(inspection), fixture_project, clock=lambda: 0.0
        )

        assert composed is not None
        assert composed["load_delta"] == 4
        assert "list_blueprints(include_instances=False)" in inspection.calls
        assert inspection.calls[0] == "compute_stats"


class TestTelemetryNeverAffectsTheCommand:
    @pytest.mark.parametrize(
        "failing", ["compute_stats", "get_project_config", "list_templates"]
    )
    def test_a_failing_read_yields_no_shape(
        self, fixture_project: Path, failing: str
    ) -> None:
        composed = build_project_shape(
            _StubFacade(_StubInspection(failing=failing)), fixture_project
        )

        assert composed is None

    def test_a_facade_without_the_expected_surface_yields_no_shape(
        self, fixture_project: Path
    ) -> None:
        composed = build_project_shape(object(), fixture_project)

        assert composed is None
